import contextlib
import io
import random
import sys
import unittest
from pathlib import Path

import numpy as np

SRC = Path(__file__).resolve().parents[1] / "src" / "python"
TASKS = Path(__file__).resolve().parents[1] / "tasks"
# smell_vectors imports its sibling hadamard_rotation module
sys.path.insert(0, str(SRC))

import smell_vectors


def in_memory_dim_stats(samples):
  """The original load-everything-into-RAM per-dim math, kept as the reference for the streaming implementation."""
  num_sample, dim = samples.shape
  mean = samples.mean(axis=0)
  std = samples.std(axis=0)
  pct_zeros = (samples == 0.0).mean(axis=0)
  dominant_val = np.zeros(dim, dtype=np.float32)
  dominant_frac = np.zeros(dim, dtype=np.float64)
  num_unique = np.zeros(dim, dtype=np.int64)
  for d in range(dim):
    vals, counts = np.unique(samples[:, d], return_counts=True)
    top = counts.argmax()
    dominant_val[d] = vals[top]
    dominant_frac[d] = counts[top] / num_sample
    num_unique[d] = len(vals)
  centered = samples - mean
  m3 = (centered**3).mean(axis=0)
  m4 = (centered**4).mean(axis=0)
  cov = (centered.T @ centered) / num_sample
  global_min = float(samples.min())
  global_max = float(samples.max())
  dim_counts = []
  for d in range(dim):
    col = samples[:, d]
    if col.min() == col.max():
      counts = [0] * smell_vectors._NUM_SPARK_BINS
      counts[smell_vectors._NUM_SPARK_BINS // 2] = num_sample
    else:
      counts = np.histogram(col, bins=smell_vectors._NUM_SPARK_BINS, range=(global_min, global_max))[0].tolist()
    dim_counts.append(counts)
  return {
    "mean": mean,
    "std": std,
    "m3": m3,
    "m4": m4,
    "cov": cov,
    "pct_zeros": pct_zeros,
    "dominant_val": dominant_val,
    "dominant_frac": dominant_frac,
    "num_unique": num_unique,
    "dim_counts": dim_counts,
  }


def chunked(x, chunk_size):
  return lambda: [(np.arange(start, min(start + chunk_size, len(x))), x[start : start + chunk_size]) for start in range(0, len(x), chunk_size)]


class TestSmellVectors(unittest.TestCase):
  def setUp(self):
    self.noisy = smell_vectors.NOISY
    smell_vectors.NOISY = False

  def tearDown(self):
    smell_vectors.NOISY = self.noisy

  def assert_same_as_in_memory(self, x, read_chunks):  # noqa: PLR6301 asserts via np.testing
    with contextlib.redirect_stdout(io.StringIO()):
      got = smell_vectors._dim_stats(read_chunks, x.shape[1], "test")
    ref = in_memory_dim_stats(x)
    # float32 in-memory math vs float64 streaming math: agree to float32 noise
    for name in ("mean", "std", "cov"):
      np.testing.assert_allclose(got[name], ref[name], rtol=1e-4, atol=1e-7, err_msg=name)
    # compare skew/kurtosis, which the labels threshold, rather than the tiny raw m3/m4
    non_const = ref["std"] > smell_vectors._THRESH_CONSTANT_STD
    for name, power in (("m3", 3), ("m4", 4)):
      got_std, ref_std = got["std"][non_const], ref["std"][non_const]
      np.testing.assert_allclose(got[name][non_const] / got_std**power, ref[name][non_const] / ref_std**power, rtol=1e-4, atol=1e-3, err_msg=name)
    np.testing.assert_array_equal(got["pct_zeros"], ref["pct_zeros"])
    np.testing.assert_array_equal(got["dim_counts"], ref["dim_counts"])
    cap = smell_vectors._THRESH_LOW_CARDINALITY_MAX_UNIQUE + 1
    np.testing.assert_array_equal(np.minimum(got["num_unique"], cap), np.minimum(ref["num_unique"], cap))
    # the dominant value is only needed (and only exact) when it holds more than half of a dim
    majority = ref["dominant_frac"] > 0.5
    np.testing.assert_array_equal(got["dominant_val"][majority], ref["dominant_val"][majority])
    np.testing.assert_array_equal(got["dominant_frac"][majority], ref["dominant_frac"][majority])

  def test_streaming_matches_in_memory_on_random_samples(self):
    rng = random.Random(17)
    for name, dim in (("vector-task-100d.vec", 100), ("vector-task-minilm.vec", 384)):
      vectors = np.fromfile(TASKS / name, dtype="<f4").reshape(-1, dim)
      for trial in range(3):
        x = vectors[rng.sample(range(len(vectors)), 2000)]
        for chunk_size in (len(x), 997):
          with self.subTest(name=name, trial=trial, chunk_size=chunk_size):
            self.assert_same_as_in_memory(x, chunked(x, chunk_size))

  def test_streaming_matches_in_memory_on_degenerate_dims(self):
    rng = np.random.default_rng(3)
    x = rng.standard_normal((1001, 8)).astype(np.float32)
    x[:, 0] = 0.25
    x[:, 1] = np.where(rng.random(1001) < 0.97, 0.0, x[:, 1])
    x[:, 2] = rng.integers(0, 100, 1001)
    x[:, 3] = rng.integers(0, 101, 1001)
    # a non-zero majority value that loses the vote in the first chunks
    x[:, 4] = 0.5
    x[:400, 4] = 0.1
    for chunk_size in (1, 3, 1000, 1001):
      with self.subTest(chunk_size=chunk_size):
        self.assert_same_as_in_memory(x, chunked(x, chunk_size))

  def test_full_file_reader(self):
    vectors = np.fromfile(TASKS / "vector-task-minilm.vec", dtype="<f4").reshape(-1, 384)
    norms = np.linalg.norm(vectors, axis=1)
    bad_norms = norms[~np.isclose(norms, 1.0, rtol=0.0001, atol=0.0001)]
    num_not_norm = len(bad_norms)
    self.assertGreater(num_not_norm, 8)
    old_chunk_vecs = smell_vectors._FULL_CHUNK_VECS
    # leave a partial last chunk
    smell_vectors._FULL_CHUNK_VECS = 997
    try:
      read_chunks = lambda: smell_vectors._read_all_vectors(TASKS / "vector-task-minilm.vec", 384, len(vectors), "smell:")
      self.assert_same_as_in_memory(vectors, read_chunks)
      out = io.StringIO()
      with contextlib.redirect_stdout(out):
        stats = smell_vectors._dim_stats(read_chunks, 384, "test")
    finally:
      smell_vectors._FULL_CHUNK_VECS = old_chunk_vecs
    self.assertEqual(stats["not_norm_count"], num_not_norm)
    # only the 1st, 2nd, 4th, 8th, ... bad vectors are printed
    printed = [int(line.rsplit("#", 1)[1].rstrip(")")) for line in out.getvalue().splitlines() if "(not normalized" in line]
    self.assertEqual(printed, [1 << i for i in range(num_not_norm.bit_length())])
    self.assertAlmostEqual(stats["not_norm_min"], bad_norms.min(), places=5)
    self.assertAlmostEqual(stats["not_norm_max"], bad_norms.max(), places=5)

  def test_not_normalized_summary(self):
    x = np.zeros((6, 4), dtype=np.float32)
    x[:, 0] = [1.0, 3.0, 1.0, 0.5, 2.0, 1.0]
    with contextlib.redirect_stdout(io.StringIO()):
      stats = smell_vectors._dim_stats(chunked(x, 2), 4, "test")
    self.assertEqual(stats["not_norm_count"], 3)
    self.assertEqual(stats["not_norm_first"], 1)
    self.assertEqual((stats["not_norm_min"], stats["not_norm_max"]), (0.5, 3.0))
    self.assertAlmostEqual(stats["not_norm_mean"], 5.5 / 3)

  def test_nearest_sq_distances_matches_full_distance_matrix(self):  # noqa: PLR6301 asserts via np.testing
    vectors = np.fromfile(TASKS / "vector-task-100d.vec", dtype="<f4").reshape(-1, 100)
    indices = np.arange(len(vectors))
    queries = indices[::5]
    sq_norms = (vectors * vectors).sum(axis=1)
    D2 = sq_norms[queries, None] + sq_norms[None, :] - 2.0 * (vectors[queries] @ vectors.T)
    np.maximum(D2, 0.0, out=D2)
    D2[np.arange(len(queries)), queries] = np.inf
    want = np.sort(np.partition(D2, 19, axis=1)[:, :20], axis=1)
    got = np.sort(smell_vectors._nearest_sq_distances(vectors[queries], queries, chunked(vectors, 300), 20), axis=1)
    np.testing.assert_array_equal(got, want)

  def test_rotation_matrix_matches_rotate_batch(self):  # noqa: PLR6301 asserts via np.testing
    x = np.fromfile(TASKS / "vector-task-100d.vec", dtype="<f4").reshape(-1, 100)[:200]
    want = smell_vectors.HadamardRotation.for_dimension(100).rotate_batch(x)
    np.testing.assert_allclose(x @ smell_vectors._rotation_matrix(100), want, atol=1e-6)


if __name__ == "__main__":
  unittest.main()
