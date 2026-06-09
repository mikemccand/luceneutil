#!/usr/bin/env python3
r"""Smell-test vectors in a `.vec` file: per-dimension distribution stats, degenerate-dim
detection, isotropy / effective-rank, and intrinsic-dimensionality estimation
(TwoNN, MLE-Levina-Bickel, PCA-cumvar).

Quick CLI usage (from luceneutil/util):

    python3 src/python/smell_vectors.py <vec_path> <dim> \
        [--label LABEL] [--io pread|mmap] [--quiet] [--no-intrinsic-dim]

Only <vec_path> and <dim> are required.

Can be used as:
- a library — import and call `smell_vectors(dim, file_name, label)` from another script
- a CLI — `python3 smell_vectors.py <vec_path> <dim>` for ad-hoc analysis of a `.vec` file

This is the source of truth for the vector-smelling logic. `knnPerfTest.py` imports
`smell_vectors` from here.
"""

import argparse
import mmap
import os
import random
import time

import numpy as np

# --- IO and printing knobs (importable; can be overridden at module load by callers) ---

# toggle between 'pread' and 'mmap' for concurrent random vector reads when smelling vectors --
# pread is maybe a bit faster
IO_METHOD = "pread"

# emit progress / per-dim listing
NOISY = True

# whether to also run intrinsic-dim estimation after the per-dim distribution smell.
# Empirically, vector-search performance (HNSW recall scaling, quantization tolerance,
# NN-distance contrast) is governed by the *intrinsic* dimensionality of the data
# manifold, not by the *ambient* dimension of the embedding. Two corpora with ambient
# dim=1024 but ID=8 vs ID=80 will look wildly different at search time.
#
# How it relates to isotropy / effective-rank:
#   - isotropy and effective_rank are LINEAR measures: "how many coordinate axes does
#     the data spread across?" (a property of the covariance spectrum).
#   - intrinsic dim is a MANIFOLD measure: "how many independent latent variables does
#     the data actually vary along?" (can be much smaller when data lives on a curved
#     low-D surface inside the ambient space — common for embeddings).
#   - the gap between PCA-95% and TwoNN reveals manifold curvature.
DO_INTRINSIC_DIM = True


# --- Smell config thresholds ---

_SPARK_CHARS = " ▁▂▃▄▅▆▇█"
_NUM_SPARK_BINS = 20
_NUM_DIM_SAMPLE_VECS = 2000
_THRESH_CONSTANT_STD = 1e-6
_THRESH_SPARSE_PCT_ZEROS = 0.50
_THRESH_SKEWED_ABS = 1.0
_THRESH_HEAVY_TAILS_KURTOSIS = 3.0
_THRESH_FLAT_KURTOSIS = -1.0
_THRESH_OUTLIER_SPREAD_SIGMA = 3.0
# isotropy participation ratio below this triggers a WARNING; values closer to 1.0 are isotropic
_THRESH_ANISOTROPIC = 0.5

# --- Intrinsic-dim config ---

# sample size for ID estimation. TwoNN and MLE-Levina-Bickel both rely on nearest-neighbor
# distance ratios that are noisy at small N. 10k samples gives stable d-hat for ID up to
# ~50 while keeping the all-pairs distance matrix at ~400MB float32 (10k x 10k) and BLAS
# matmul under ~5s on a typical multicore box. At higher ID the estimator needs more
# points; bump this if reported R^2 is consistently low.
_NUM_INTRINSIC_DIM_SAMPLE_VECS = 10_000

# k values for MLE Levina-Bickel. Small k -> more local (closer to true manifold dimension
# under curvature) but noisier. Large k -> smoother but more biased high under non-uniform
# sampling density. Reporting both gives a sense of stability across scales.
_MLE_K_VALUES = (10, 20)

# fraction of TwoNN ratios to discard from the upper tail. Very large mu values come from
# points near density boundaries and from outliers; they bend the regression away from the
# true slope. Facco et al. (2017) recommend ~10%.
_TWONN_DISCARD_FRAC = 0.10

# WARNING thresholds for ID analysis -- triggers loud output if data looks pathological
# for KNN benchmarking:

# TwoNN regression R^2 below this -> the Pareto-tail model doesn't fit, which means the
# data isn't a clean single manifold (mixture of clusters, heavy duplicate population,
# isolated outliers, etc). ID estimate is unreliable.
_THRESH_TWONN_BAD_FIT_R2 = 0.95

# TwoNN ID below this -> vectors are essentially collinear; HNSW and quantization will be
# trivially easy and benchmark numbers won't transfer to real workloads.
_THRESH_TWONN_DEGENERATE_ID = 2.0

# TwoNN ID above this fraction of ambient dim -> vectors look like noise (poorly trained
# / random embeddings); benchmarks will be hard but not meaningfully so.
_THRESH_TWONN_NEAR_AMBIENT_FRAC = 0.5

# PCA-95% / TwoNN ratio above this -> highly curved manifold within a moderate-rank linear
# subspace. Random rotations (RaBitQ) less effective here than learned rotations (PCA, OPQ).
_THRESH_HIGH_CURVATURE_RATIO = 20.0

# max(MLE) / TwoNN (or vice versa) above this -> ID estimators disagree strongly; treat
# reported ID as ~uncertain by 2x.
_THRESH_ID_DISAGREEMENT_RATIO = 2.0

# fraction of duplicate (r1==0) sample points above which we consider the corpus
# problematic for KNN benchmarking.
_THRESH_DUPLICATES_FRAC = 0.001


def _sparklines_2row(counts):
  """Returns (top_row_str, bot_row_str) for a two-row histogram using unicode block chars.

  Block chars fill from the bottom of the cell, so with 8 levels per row the two rows
  connect seamlessly: a partial char in the top row sits at the bottom of its cell,
  flush against the full block below it.  Total resolution: 16 height levels.
  """
  max_count = max(counts) if max(counts) > 0 else 1
  top_chars = []
  bot_chars = []
  for c in counts:
    level = round(c / max_count * 16)
    if level <= 8:
      bot_chars.append(_SPARK_CHARS[level])
      top_chars.append(" ")
    else:
      bot_chars.append("█")
      top_chars.append(_SPARK_CHARS[level - 8])
  return "".join(top_chars), "".join(bot_chars)


def _print_dim_line(d, mean, std, pct_zeros, counts, labels, dim_idx_width):
  """Prints sparkline + stats, with any smell labels (with details) on separate lines below."""
  top_row, bot_row = _sparklines_2row(counts)
  prefix = f"  dim {d:0{dim_idx_width}d} μ={mean:+8.3f} σ={std:8.3f} zeros={pct_zeros * 100:3.0f}% "  # noqa: RUF001 sigma (std deviation) is intentional
  print(f"{' ' * len(prefix)}[{top_row}]")
  print(f"{prefix}[{bot_row}]")
  indent = f"  {' ' * dim_idx_width}  "
  for name, detail in labels:
    print(f"{indent}-> {name}: {detail}")
  print()


def _read_vectors_pread(file_name, sample_indices, vec_size_bytes):
  """Generator that issues concurrent readahead hints and yields vectors via pread."""
  with open(file_name, "rb") as f:
    fd = f.fileno()

    # hint random access for the whole file, to suppress wasteful readahead
    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_RANDOM)

    # concurrently send all requests to the OS as hints
    for vec_idx in sample_indices:
      os.posix_fadvise(fd, vec_idx * vec_size_bytes, vec_size_bytes, os.POSIX_FADV_WILLNEED)

    # yield vectors; they should be pre-fetched by the kernel
    for vec_idx in sample_indices:
      yield vec_idx, np.frombuffer(os.pread(fd, vec_size_bytes, vec_idx * vec_size_bytes), dtype="<f4")

    # hint sequential access for the subsequent (sequential) indexing test
    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_SEQUENTIAL)


def _read_vectors_mmap(file_name, sample_indices, vec_size_bytes, dim):
  """Generator that issues concurrent readahead hints and yields vectors via mmap."""
  with open(file_name, "rb") as f:
    # mmap.PAGESIZE is typically 4096 on Linux
    pagesize = mmap.PAGESIZE

    # map the entire file
    mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

    # hint random access to the whole mmap, to suppress wasteful readahead
    mm.madvise(mmap.MADV_RANDOM)

    try:
      # concurrently send all requests to the OS as hints
      for vec_idx in sample_indices:
        offset = vec_idx * vec_size_bytes
        page_offset = (offset // pagesize) * pagesize
        length = offset + vec_size_bytes - page_offset
        mm.madvise(mmap.MADV_WILLNEED, page_offset, length)

      # yield vectors; they should be pre-fetched by the kernel
      for vec_idx in sample_indices:
        offset = vec_idx * vec_size_bytes
        yield vec_idx, np.frombuffer(mm, dtype="<f4", count=dim, offset=offset)

      # hint sequential access for the subsequent (sequential) indexing test
      mm.madvise(mmap.MADV_SEQUENTIAL)
    finally:
      mm.close()

  print()


def _check_dim_distributions(dim, file_name, num_vectors, vec_size_bytes):
  """Samples vectors and computes per-dim statistics to detect degenerate dimensions."""
  if num_vectors == 0:
    return None

  num_sample = min(_NUM_DIM_SAMPLE_VECS, num_vectors)
  sample_indices = random.sample(range(num_vectors), num_sample)

  if NOISY:
    print(f"smell: sampling {num_sample} of {num_vectors} vectors for per-dim distribution...")

  # load all sampled vectors concurrently into a (num_sample, dim) float32 array
  samples = np.empty((num_sample, dim), dtype=np.float32)
  t0_sec = time.monotonic()

  if IO_METHOD == "pread":
    reader = _read_vectors_pread(file_name, sample_indices, vec_size_bytes)
  elif IO_METHOD == "mmap":
    reader = _read_vectors_mmap(file_name, sample_indices, vec_size_bytes, dim)
  else:
    raise ValueError(f'unknown IO_METHOD "{IO_METHOD}"')

  not_norm_count = 0
  for i, (vec_idx, vec) in enumerate(reader):
    samples[i] = vec

    # CPU work: check norm immediately as vector arrives
    norm = np.linalg.norm(vec)
    if not np.isclose(norm, 1.0, rtol=0.0001, atol=0.0001):
      # print warning on new line so it doesn't get overwritten by next progress \r
      print(f'\nWARNING: vec {vec_idx} in "{file_name}" has norm={norm} (not normalized)')
      not_norm_count += 1

    completed = i + 1
    if completed % max(1, num_sample // 20) == 0 or completed == num_sample:
      elapsed_sec = time.monotonic() - t0_sec
      if NOISY:
        print(f"\rsmell:   {completed}/{num_sample} ({100 * completed / num_sample:3.0f}%) {elapsed_sec:.1f}s", end="", flush=True)

  if NOISY:
    print()

  if not_norm_count > 0:
    print(f'WARNING: dimension or vector file name might be wrong?  {not_norm_count} of {num_sample} randomly checked vectors are not normalized in "{file_name}"')

  # per-dim stats, all vectorized over axis=0, result shape: (dim,)
  mean = samples.mean(axis=0)
  std = samples.std(axis=0)
  pct_zeros = (samples == 0.0).mean(axis=0)

  centered = samples - mean
  m3 = (centered**3).mean(axis=0)
  m4 = (centered**4).mean(axis=0)
  with np.errstate(invalid="ignore", divide="ignore"):
    skewness = m3 / std**3
    excess_kurtosis = m4 / std**4 - 3.0
  # zero out stats that are undefined for constant dims
  non_const = std > _THRESH_CONSTANT_STD
  skewness = np.where(non_const, skewness, 0.0)
  excess_kurtosis = np.where(non_const, excess_kurtosis, 0.0)

  # Isotropy = participation ratio of the covariance spectrum, in [0, 1]:
  #   PR = (tr C)^2 / (dim * ||C||_F^2) = (sum eigvals)^2 / (dim * sum eigvals^2)
  # Roughly: "what fraction of dims does the data actually use?" 1.0 = energy spread
  # evenly across all dims (isotropic, no preconditioning needed); 1/dim = data lives
  # on a single direction (highly anisotropic).
  #
  # We compute it two ways:
  #   - isotropy_full uses the real covariance C = X^T X / N (captures cross-dim
  #     correlations -- the "true" answer)
  #   - isotropy_diag pretends C is diagonal, using only per-dim variances (cheap;
  #     blind to correlations)
  # Always isotropy_full <= isotropy_diag. A large gap means anisotropy is rotational
  # (data on a low-dim subspace at an angle to coord axes) rather than axis-aligned.
  variances = std**2
  sum_var = variances.sum()
  sum_var_sq = (variances**2).sum()
  if sum_var_sq > 0:
    isotropy_diag = (sum_var**2) / (dim * sum_var_sq)
  else:
    isotropy_diag = 0.0

  C = (centered.T @ centered) / num_sample
  tr_C = np.trace(C)
  frob_sq = np.sum(C**2)
  if frob_sq > 0:
    isotropy_full = (tr_C**2) / (dim * frob_sq)
  else:
    isotropy_full = 0.0

  # OUTLIER_SPREAD: flag dims whose std is an outlier across all dims' stds
  mean_of_stds = std.mean()
  std_of_stds = std.std()

  # use global range so histogram widths are directly comparable across dims:
  # a dim with larger sigma spans more bins visually, matching the printed sigma
  global_min = float(samples.min())
  global_max = float(samples.max())

  # per-dim histogram (loop is over dims not over samples, so it's cheap)
  dim_counts = []
  for d in range(dim):
    col = samples[:, d]
    if col.min() == col.max():
      counts = [0] * _NUM_SPARK_BINS
      counts[_NUM_SPARK_BINS // 2] = num_sample
    else:
      counts = np.histogram(col, bins=_NUM_SPARK_BINS, range=(global_min, global_max))[0].tolist()
    dim_counts.append(counts)

  # assign labels per dim -- each label is (name, detail_string)
  dim_labels = []
  for d in range(dim):
    labels = []

    if std[d] <= _THRESH_CONSTANT_STD:
      labels.append(("CONSTANT", f"std={std[d]:.6f}, threshold={_THRESH_CONSTANT_STD}"))

    if pct_zeros[d] > _THRESH_SPARSE_PCT_ZEROS:
      labels.append(("SPARSE", f"{pct_zeros[d] * 100:.1f}% zeros, threshold={_THRESH_SPARSE_PCT_ZEROS * 100:.0f}%"))

    if non_const[d]:
      if abs(skewness[d]) > _THRESH_SKEWED_ABS:
        labels.append(("SKEWED", f"skew={skewness[d]:+.2f}, threshold=|{_THRESH_SKEWED_ABS}|"))
      if excess_kurtosis[d] > _THRESH_HEAVY_TAILS_KURTOSIS:
        labels.append(("HEAVY_TAILS", f"kurtosis={excess_kurtosis[d]:+.2f}, threshold>{_THRESH_HEAVY_TAILS_KURTOSIS}"))
      if excess_kurtosis[d] < _THRESH_FLAT_KURTOSIS:
        labels.append(("FLAT", f"kurtosis={excess_kurtosis[d]:+.2f}, threshold<{_THRESH_FLAT_KURTOSIS}"))

    if std_of_stds > 0 and abs(std[d] - mean_of_stds) > _THRESH_OUTLIER_SPREAD_SIGMA * std_of_stds:
      z = (std[d] - mean_of_stds) / std_of_stds
      labels.append(("OUTLIER_SPREAD", f"this_std={std[d]:.4f}, mean_std={mean_of_stds:.4f}, {z:+.1f}sigma vs threshold={_THRESH_OUTLIER_SPREAD_SIGMA}sigma"))

    dim_labels.append(labels)

  # format and print output
  dim_idx_width = len(str(dim - 1))
  bad_dims = [d for d in range(dim) if len(dim_labels[d]) > 0]

  elapsed_sec = time.monotonic() - t0_sec

  # Estimate effective rank for human-readable context: full*dim ~= number of dims actually used
  eff_rank_full = isotropy_full * dim
  eff_rank_diag = isotropy_diag * dim
  print(f"smell: isotropy={isotropy_full:.3f} full-cov, {isotropy_diag:.3f} diag-only (effective rank ~{eff_rank_full:.1f}/{dim} full, ~{eff_rank_diag:.1f}/{dim} diag)")
  print("  isotropy = (tr C)^2 / (D * ||C||_F^2): 1.0 = energy spread evenly across all dims; 1/D = data on one direction")
  print("  full-cov uses real covariance (sees cross-dim correlations); diag-only uses per-dim variances (blind to correlations)")
  if isotropy_full < _THRESH_ANISOTROPIC:
    if isotropy_diag - isotropy_full > 0.2:
      cause = "ROTATED anisotropy: per-dim variances look balanced but data lives in a lower-dim subspace at an angle to coord axes"
      fix = "consider PCA or a random rotation before quantization/HNSW"
    else:
      cause = "AXIS-ALIGNED anisotropy: a few dims carry most of the variance"
      fix = "consider per-dim standardization (subtract mean, divide by std)"
    print(f"  WARNING: anisotropic vectors (isotropy_full={isotropy_full:.3f} < {_THRESH_ANISOTROPIC})")
    print(f"           {cause}")
    print(f"           {fix}")

  if bad_dims:
    print(f"smell: {len(bad_dims)} degenerate dim(s) found in {elapsed_sec:.1f}s:")
    if any(name == "OUTLIER_SPREAD" for lbs in dim_labels for name, _ in lbs):
      print(f"  (OUTLIER_SPREAD: std of all dims: μ={mean_of_stds:.3f}, σ={std_of_stds:.3f}, threshold={_THRESH_OUTLIER_SPREAD_SIGMA}σ)")  # noqa: RUF001 sigma (std deviation) is intentional

    for d in bad_dims:
      _print_dim_line(d, float(mean[d]), float(std[d]), float(pct_zeros[d]), dim_counts[d], dim_labels[d], dim_idx_width)

    if NOISY:
      print(f"smell: all {dim} dims:")
      for d in range(dim):
        _print_dim_line(d, float(mean[d]), float(std[d]), float(pct_zeros[d]), dim_counts[d], dim_labels[d], dim_idx_width)
  elif NOISY:
    print(f"smell: no degenerate dims found in {elapsed_sec:.1f}s")

  # collect per-label counts for the final summary
  label_counts = {}
  for lbs in dim_labels:
    for name, _ in lbs:
      label_counts[name] = label_counts.get(name, 0) + 1
  anisotropy_kind = None
  if isotropy_full < _THRESH_ANISOTROPIC:
    anisotropy_kind = "ROTATED" if (isotropy_diag - isotropy_full > 0.2) else "AXIS-ALIGNED"
  return {
    "dim": dim,
    "num_sample": num_sample,
    "isotropy_full": float(isotropy_full),
    "isotropy_diag": float(isotropy_diag),
    "num_bad_dims": len(bad_dims),
    "label_counts": label_counts,
    "not_norm_count": not_norm_count,
    "anisotropy_kind": anisotropy_kind,
  }


def _load_id_samples(dim, file_name, num_vectors, vec_size_bytes):
  """Load a fresh, larger sample of vectors for intrinsic-dim estimation.

  Kept separate from the histogram sample (which only needs ~2k vectors) so
  the histogram cost doesn't grow with the bigger ID sample size.  Re-uses
  the same posix_fadvise/pread or mmap path as the histogram loader.
  """
  num_sample = min(_NUM_INTRINSIC_DIM_SAMPLE_VECS, num_vectors)
  sample_indices = random.sample(range(num_vectors), num_sample)

  if NOISY:
    print(f"smell ID: sampling {num_sample} of {num_vectors} vectors for intrinsic-dim estimation...")

  samples = np.empty((num_sample, dim), dtype=np.float32)
  t0_sec = time.monotonic()

  if IO_METHOD == "pread":
    reader = _read_vectors_pread(file_name, sample_indices, vec_size_bytes)
  elif IO_METHOD == "mmap":
    reader = _read_vectors_mmap(file_name, sample_indices, vec_size_bytes, dim)
  else:
    raise ValueError(f'unknown IO_METHOD "{IO_METHOD}"')

  for i, (_vec_idx, vec) in enumerate(reader):
    samples[i] = vec
    completed = i + 1
    if completed % max(1, num_sample // 20) == 0 or completed == num_sample:
      elapsed_sec = time.monotonic() - t0_sec
      if NOISY:
        print(f"\rsmell ID:   {completed}/{num_sample} ({100 * completed / num_sample:3.0f}%) {elapsed_sec:.1f}s", end="", flush=True)

  if NOISY:
    print()

  return samples


def _twonn_estimate(D2):
  """TwoNN intrinsic-dim estimator (Facco, d'Errico, Rodriguez, Laio 2017).

  Theory: if data is sampled uniformly from a smooth manifold of intrinsic
  dimension d, then for each point the ratio mu = r2 / r1 of its 2nd to 1st
  nearest-neighbor distances follows a Pareto distribution:

      P(mu) = d * mu^(-d - 1)         for mu >= 1
      F(mu) = 1 - mu^(-d)             (CDF)
      => -log(1 - F(mu)) = d * log(mu)

  So the slope of -log(1 - F_hat) regressed on log(mu) IS the intrinsic
  dimension.  Beautiful trick: the per-point density cancels out (because
  r1 and r2 are at the same point), so this works even when sampling
  density varies wildly across the manifold.

  We discard the upper tail (Facco et al. recommend ~10%) because very
  large mu values come from points near density boundaries and outliers,
  which bend the regression.

  Returns dict with keys: d, r2, n_kept, n_dup, n_used.
    d      = estimated intrinsic dimension
    r2     = R^2 of the linear fit (1.0 = perfect Pareto, lower = data is
             not really a single manifold)
    n_kept = points kept after tail discard
    n_dup  = points dropped because r1 == 0 (exact duplicates)
    n_used = points with valid r1, r2 before tail discard
  """
  N = D2.shape[0]

  # 2nd and 3rd smallest of each row of D2 (the 1st-smallest is the diagonal,
  # which we set to inf upstream).  partition is O(N) per row so this is
  # O(N^2) total, dwarfed by the matmul that produced D2.
  partitioned = np.partition(D2, 1, axis=1)
  r1_sq = partitioned[:, 0]
  r2_sq = partitioned[:, 1]

  # numerical guard: tiny negatives from float roundoff in the matmul
  r1_sq = np.maximum(r1_sq, 0.0)
  r2_sq = np.maximum(r2_sq, 0.0)
  r1 = np.sqrt(r1_sq)
  r2 = np.sqrt(r2_sq)

  # drop points whose nearest neighbor is a duplicate (r1 == 0): mu is
  # undefined and probably indicates corpus-level duplication.  count them
  # so the caller can warn.
  is_dup = r1 == 0.0
  n_dup = int(is_dup.sum())
  valid = ~is_dup
  if valid.sum() < 100:
    raise RuntimeError(f"TwoNN: too few non-duplicate points ({int(valid.sum())} of {N}); the corpus may be heavily duplicated or the sample too small")

  mu = r2[valid] / r1[valid]
  # mu must be >= 1 by construction (r2 >= r1); float roundoff can produce
  # mu just under 1 -- clamp.
  mu = np.maximum(mu, 1.0)
  n_used = int(mu.size)

  # discard upper tail
  mu_sorted = np.sort(mu)
  n_keep = int(round(n_used * (1.0 - _TWONN_DISCARD_FRAC)))
  if n_keep < 50:
    raise RuntimeError(f"TwoNN: too few kept points ({n_keep}); sample size too small")
  mu_kept = mu_sorted[:n_keep]

  # empirical CDF: F_hat_i = i / (N+1) for i = 1..N (avoids log(0) at the top)
  i_arr = np.arange(1, n_keep + 1, dtype=np.float64)
  f_hat = i_arr / (n_keep + 1)

  x = np.log(mu_kept)
  y = -np.log(1.0 - f_hat)

  # linear fit forced through origin (the model is y = d * x exactly when
  # the Pareto holds).  drop x==0 (mu==1 points) to avoid degeneracies.
  nz = x > 0
  x = x[nz]
  y = y[nz]
  if x.size < 50:
    raise RuntimeError(f"TwoNN: too few non-degenerate points ({x.size}) after dropping mu==1")

  d_hat = float(np.sum(x * y) / np.sum(x * x))

  # R^2 vs the through-origin fit
  y_pred = d_hat * x
  ss_res = float(np.sum((y - y_pred) ** 2))
  ss_tot = float(np.sum((y - np.mean(y)) ** 2))
  r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

  return {"d": d_hat, "r2": r_squared, "n_kept": n_keep, "n_dup": n_dup, "n_used": n_used}


def _mle_estimate(D2, k):
  """MLE Levina-Bickel intrinsic-dim estimator (2004) at neighborhood size k.

  Theory: model the local point process around each query point as a Poisson
  process with intensity rho * r^(d-1).  The MLE for d given the k nearest
  neighbor distances r_1 < r_2 < ... < r_k is:

      d_hat_k(x) = (k - 1) / sum_{j=1}^{k-1} log(r_k / r_j)

  This is a per-point estimate; we report mean and median over all sample
  points.  The harmonic-mean form makes it scale-invariant (good) but
  biased high under non-uniform sampling density (bad).  Comparing across
  k=10 vs k=20 reveals stability.
  """
  N = D2.shape[0]
  if k + 1 > N:
    raise RuntimeError(f"MLE: k={k} requires N>{k}; have N={N}")

  # per-row top-k smallest distances (excluding self, which is inf)
  partitioned = np.partition(D2, k - 1, axis=1)[:, :k]
  partitioned = np.sort(partitioned, axis=1)
  partitioned = np.maximum(partitioned, 0.0)
  r = np.sqrt(partitioned)  # shape (N, k); r[:, 0]..r[:, k-1]

  r_k = r[:, k - 1]  # shape (N,)
  r_inner = r[:, : k - 1]  # shape (N, k-1)

  # need r_k > 0 and all r_inner > 0; drop rows that fail
  r_inner_min = r_inner.min(axis=1)
  valid = (r_k > 0) & (r_inner_min > 0)
  if valid.sum() < 100:
    raise RuntimeError(f"MLE k={k}: too few non-degenerate points ({int(valid.sum())} of {N})")

  log_ratios = np.log(r_k[valid, None] / r_inner[valid])  # (M, k-1)
  inv_d = log_ratios.sum(axis=1) / (k - 1)
  # inv_d must be > 0 (since r_k >= r_j); guard against numerical zero
  good = inv_d > 0
  if good.sum() < 100:
    raise RuntimeError(f"MLE k={k}: too few rows with positive log-ratio sum ({int(good.sum())})")
  d_per_point = 1.0 / inv_d[good]

  return {"mean": float(d_per_point.mean()), "median": float(np.median(d_per_point)), "n": int(d_per_point.size)}


def _pca_id(samples, thresholds=(0.90, 0.95, 0.99)):
  """Linear ID via cumulative variance of the centered covariance.

  Returns a dict {threshold -> num_components_needed}.  This is the LINEAR
  intrinsic dim -- minimum number of orthogonal axes that span X% of the
  total variance.  Comparing PCA-95% to TwoNN reveals manifold curvature:
  if PCA-95% >> TwoNN, the data lives on a curved low-D manifold inside
  a higher-D linear subspace.
  """
  centered = samples - samples.mean(axis=0)
  # use SVD on centered data for numerical stability vs eigh of X^T X
  # (squared-condition-number is bad when some dims are tiny)
  s = np.linalg.svd(centered, compute_uv=False)
  variances = (s**2) / max(samples.shape[0] - 1, 1)
  total = variances.sum()
  if total <= 0:
    raise RuntimeError("PCA ID: total variance is zero; samples are constant")
  cumvar = np.cumsum(variances) / total

  result = {}
  for thresh in thresholds:
    # smallest k such that cumvar[k-1] >= thresh
    k = int(np.searchsorted(cumvar, thresh) + 1)
    k = min(k, len(cumvar))
    result[thresh] = k
  return result


def _estimate_intrinsic_dim(samples, label, dim):
  """Compute and report intrinsic-dimensionality estimates for `samples`.

  Runs three estimators (TwoNN, MLE-Levina-Bickel at multiple k, PCA-cumvar)
  and prints a compact report plus interpretation guidance.  Issues loud
  WARNINGs when the data looks pathological for KNN benchmarking.

  See module-level docstring on DO_INTRINSIC_DIM for the why; the function
  bodies _twonn_estimate / _mle_estimate / _pca_id document the math for
  each estimator.

  Args:
    samples: (N, D) float32 array of sample vectors
    label:   "docs" or "queries", used in printed report
    dim:     ambient dimension (== samples.shape[1])

  """
  N = samples.shape[0]
  assert samples.shape[1] == dim, f"shape mismatch: samples.shape[1]={samples.shape[1]} vs dim={dim}"

  print(f"\nsmell ID: estimating intrinsic dim of {label} vectors (N={N}, ambient dim={dim})...")
  t0_sec = time.monotonic()

  # all-pairs squared euclidean distance via BLAS.  for unit-normalized
  # vectors euclidean ranking is monotone-equivalent to angular ranking and
  # to dot-product ranking, so a single distance covers all metrics in
  # use here (smell_vectors already enforced near-unit norm upstream).
  #
  # ||a - b||^2 = ||a||^2 + ||b||^2 - 2 <a, b>
  sq_norms = (samples * samples).sum(axis=1)
  D2 = sq_norms[:, None] + sq_norms[None, :] - 2.0 * (samples @ samples.T)
  # numerical guard for tiny negatives from float roundoff
  np.maximum(D2, 0.0, out=D2)
  # exclude self-distance from NN search by setting diagonal to +inf
  np.fill_diagonal(D2, np.inf)

  matmul_sec = time.monotonic() - t0_sec
  if NOISY:
    print(f"smell ID:   distance matrix ({N}x{N}) in {matmul_sec:.1f}s")

  # --- TwoNN ---
  twonn = _twonn_estimate(D2)

  # --- MLE Levina-Bickel ---
  mle = {}
  for k in _MLE_K_VALUES:
    mle[k] = _mle_estimate(D2, k)

  # free distance matrix before PCA (PCA only needs samples)
  del D2

  # --- PCA cumulative variance ---
  pca = _pca_id(samples)

  total_sec = time.monotonic() - t0_sec

  # --- report ---
  print(f"smell ID: results for {label} (computed in {total_sec:.1f}s):")
  print(f"  TwoNN:    d={twonn['d']:6.2f}  (R^2={twonn['r2']:.3f} over {twonn['n_kept']} points; {twonn['n_dup']} duplicates dropped)")
  for k in _MLE_K_VALUES:
    print(f"  MLE k={k:<3d} d_mean={mle[k]['mean']:6.2f}  d_median={mle[k]['median']:6.2f}  (over {mle[k]['n']} points)")
  pca_strs = []
  for thresh, k_needed in pca.items():
    pca_strs.append(f"d_{int(thresh * 100)}={k_needed}")
  print(f"  PCA:      {'  '.join(pca_strs)}   (linear ID; ambient={dim})")

  # --- interpretation block ---
  pca95 = pca.get(0.95)
  twonn_d = twonn["d"]
  curvature_ratio = pca95 / twonn_d if (pca95 is not None and twonn_d > 0) else float("inf")
  mle_max = max(m["mean"] for m in mle.values())
  mle_min = min(m["mean"] for m in mle.values())
  disagreement = max(mle_max / twonn_d, twonn_d / max(mle_min, 1e-9)) if twonn_d > 0 else float("inf")

  print("  -- interpretation --")
  print("  TwoNN d is the manifold (nonlinear) intrinsic dim; PCA-95% is the linear-subspace dim.")
  print(f"  PCA-95% / TwoNN = {curvature_ratio:.1f}x measures how curved the manifold is inside")
  print("  that linear subspace.  high ratio -> learned rotations (PCA, OPQ) help quantization;")
  print("  random rotations (RaBitQ) less so.  low ratio (~1) -> data is near-flat; random rotation suffices.")
  print(f"  TwoNN d~={twonn_d:.1f} predicts HNSW recall scaling roughly like dim-{int(round(twonn_d))} data:")
  print("  lower ID -> can afford smaller beamWidth, more aggressive quantization, fewer fanout;")
  print("  higher ID -> need richer beam search and conservative quantization.")

  # --- WARNINGs ---
  warnings = []

  if twonn["n_dup"] > _THRESH_DUPLICATES_FRAC * N:
    warnings.append(
      f"found {twonn['n_dup']} exact-duplicate sample points (>{_THRESH_DUPLICATES_FRAC * 100:.1f}% of {N}); "
      "duplicates inflate recall numbers and may indicate dataset corruption or a leaky train/test split"
    )

  if twonn["r2"] < _THRESH_TWONN_BAD_FIT_R2:
    warnings.append(
      f"TwoNN R^2={twonn['r2']:.3f} < {_THRESH_TWONN_BAD_FIT_R2}; "
      "the Pareto-tail model does not fit the data, which means the sample is not a single clean "
      "manifold (likely a mixture of clusters, heavy duplicates, or extreme outliers).  ID estimate "
      "is unreliable; the dataset may be a poor KNN benchmark."
    )

  if twonn_d < _THRESH_TWONN_DEGENERATE_ID:
    warnings.append(
      f"TwoNN d={twonn_d:.2f} < {_THRESH_TWONN_DEGENERATE_ID}; "
      "vectors are nearly collinear / degenerate.  HNSW and quantization will be trivially easy; "
      "benchmark numbers will not transfer to real-world embedding workloads."
    )

  if twonn_d > _THRESH_TWONN_NEAR_AMBIENT_FRAC * dim:
    warnings.append(
      f"TwoNN d={twonn_d:.1f} > {_THRESH_TWONN_NEAR_AMBIENT_FRAC} * ambient dim {dim}; "
      "vectors look like high-D noise (poorly trained or random embeddings).  benchmarks will be hard "
      "but in a way that does not reflect real embeddings, which typically have ID well below ambient."
    )

  if curvature_ratio > _THRESH_HIGH_CURVATURE_RATIO:
    warnings.append(
      f"PCA-95%/TwoNN ratio {curvature_ratio:.1f} > {_THRESH_HIGH_CURVATURE_RATIO}: highly curved manifold; "
      "expect quantization with random rotation (RaBitQ) to underperform learned rotation (PCA/OPQ)."
    )

  if disagreement > _THRESH_ID_DISAGREEMENT_RATIO:
    warnings.append(
      f"ID estimators disagree by {disagreement:.1f}x (TwoNN={twonn_d:.1f}, MLE range "
      f"[{mle_min:.1f}, {mle_max:.1f}]); treat reported ID as uncertain by ~2x.  "
      "this can happen when sampling density varies a lot across the manifold."
    )

  if warnings:
    print(f"\nWARNING: {len(warnings)} issue(s) flagged for {label} vectors:")
    for w in warnings:
      print(f"  WARNING: {w}")
  elif NOISY:
    print(f"smell ID: no issues flagged for {label} vectors")

  return {
    "twonn_d": float(twonn["d"]),
    "twonn_r2": float(twonn["r2"]),
    "twonn_n_dup": int(twonn["n_dup"]),
    "mle_means": {k: float(mle[k]["mean"]) for k in mle},
    "mle_medians": {k: float(mle[k]["median"]) for k in mle},
    "pca": {float(t): int(k) for t, k in pca.items()},
    "curvature_ratio": float(curvature_ratio) if curvature_ratio != float("inf") else None,
    "disagreement_ratio": float(disagreement) if disagreement != float("inf") else None,
    "warnings": warnings,
    "n_samples": int(N),
    "ambient_dim": int(dim),
  }


def _print_final_summary(label, file_name, dist_summary, id_summary):
  """Consolidate the scattered intermediate output into one easy-to-read SUMMARY block.

  Two columns: METRIC | VALUE | INTERPRETATION. Followed by a verdict line and any
  recommendations.
  """
  rows = []

  # --- distribution / isotropy ---
  iso_full = dist_summary["isotropy_full"]
  iso_diag = dist_summary["isotropy_diag"]
  dim = dist_summary["dim"]
  eff_full = iso_full * dim

  iso_verdict = f"isotropic ({iso_full:.2f} >= {_THRESH_ANISOTROPIC})" if iso_full >= _THRESH_ANISOTROPIC else f"{dist_summary['anisotropy_kind']} anisotropy"
  rows.extend(
    [
      ("isotropy (full-cov)", f"{iso_full:.3f}", iso_verdict),
      ("isotropy (diag-only)", f"{iso_diag:.3f}", "blind to cross-dim correlations"),
      ("effective rank", f"~{eff_full:.0f} / {dim}", "dims actually carrying variance"),
    ]
  )

  bad = dist_summary["num_bad_dims"]
  if bad:
    parts = [f"{n}×{name}" for name, n in sorted(dist_summary["label_counts"].items(), key=lambda x: -x[1])]  # noqa: RUF001 multiplication sign is intentional
    rows.append(("degenerate dims", f"{bad} / {dim}", ", ".join(parts)))
  else:
    rows.append(("degenerate dims", f"0 / {dim}", "no constant/sparse/skewed/heavy-tailed dims"))

  if dist_summary["not_norm_count"]:
    rows.append(("non-unit-norm vectors", f"{dist_summary['not_norm_count']} / {dist_summary['num_sample']}", "wrong dim or wrong file?"))

  # --- intrinsic dim ---
  if id_summary is not None:
    twonn_d = id_summary["twonn_d"]
    twonn_r2 = id_summary["twonn_r2"]
    pca95 = id_summary["pca"].get(0.95)
    mle_means = id_summary["mle_means"]

    rows.append(("TwoNN intrinsic dim", f"{twonn_d:.1f}", f"manifold ID; R²={twonn_r2:.3f}"))
    mle_str = ", ".join(f"k={k}:{m:.1f}" for k, m in sorted(mle_means.items()))
    rows.append(("MLE intrinsic dim", mle_str, "should agree with TwoNN"))
    if pca95 is not None:
      rows.append(("PCA-95% linear dim", f"{pca95}", f"linear subspace dim (ambient={dim})"))

    cr = id_summary["curvature_ratio"]
    if cr is not None:
      cr_verdict = "near-flat (random rotation OK)" if cr < 5 else ("moderately curved" if cr < _THRESH_HIGH_CURVATURE_RATIO else "highly curved (use learned rotation)")
      rows.append(("curvature (PCA-95/TwoNN)", f"{cr:.1f}×", cr_verdict))  # noqa: RUF001 multiplication sign is intentional

    if id_summary["twonn_n_dup"]:
      rows.append(("duplicate sample points", f"{id_summary['twonn_n_dup']} / {id_summary['n_samples']}", "may inflate recall"))

  # render
  m_w = max(len(r[0]) for r in rows)
  v_w = max(len(r[1]) for r in rows)
  bar = "─" * (m_w + v_w + 60)
  print()
  print(bar)
  print(f"SMELL SUMMARY for {label} vectors  ({file_name})")
  print(bar)
  print(f"{'METRIC'.ljust(m_w)}   {'VALUE'.ljust(v_w)}   INTERPRETATION")
  print(f"{'─' * m_w}   {'─' * v_w}   {'─' * 50}")
  for name, val, interp in rows:
    print(f"{name.ljust(m_w)}   {val.ljust(v_w)}   {interp}")
  print(bar)

  # verdict
  problems = []
  if dist_summary["anisotropy_kind"] == "ROTATED":
    problems.append("rotated anisotropy")
  elif dist_summary["anisotropy_kind"] == "AXIS-ALIGNED":
    problems.append("axis-aligned anisotropy")
  if dist_summary["num_bad_dims"]:
    problems.append(f"{dist_summary['num_bad_dims']} degenerate dim(s)")
  if id_summary and id_summary["warnings"]:
    problems.append(f"{len(id_summary['warnings'])} ID-estimator concern(s) (see WARNING lines above)")

  if problems:
    print(f"VERDICT: {len(problems)} concern(s): {'; '.join(problems)}")
  else:
    print("VERDICT: clean — vectors look well-conditioned for KNN benchmarking")
  print(bar)
  print()


def smell_vectors(dim, file_name, label="vectors"):
  """Runs sanity checks on the vector source file: per-dim distribution / isotropy /
  intrinsic dim. The .vec source file has no self-describing metadata, so the caller
  passes in the dim. `label` (e.g. "docs", "queries") is used in printed reports.
  """
  size_bytes = os.path.getsize(file_name)
  vec_size_bytes = dim * 4

  # cool, i didn't know about divmod!
  num_vectors, leftover = divmod(size_bytes, vec_size_bytes)

  if leftover != 0:
    raise RuntimeError(
      f'vector file "{file_name}" cannot be dimension {dim}: its size is not a multiple of each vector\'s size in bytes ({vec_size_bytes}); wrong vector source file or dimensionality?'
    )

  if NOISY:
    print(f"smell vectors from {file_name}")

  dist_summary = _check_dim_distributions(dim, file_name, num_vectors, vec_size_bytes)

  id_summary = None
  if DO_INTRINSIC_DIM:
    samples = _load_id_samples(dim, file_name, num_vectors, vec_size_bytes)
    id_summary = _estimate_intrinsic_dim(samples, label, dim)

  if dist_summary is not None:
    _print_final_summary(label, file_name, dist_summary, id_summary)


def _main():
  ap = argparse.ArgumentParser(description="Smell-test a .vec file: per-dim distributions, degenerate-dim detection, isotropy, intrinsic-dim estimation.")
  ap.add_argument("vec_path", help="path to the .vec file (raw float32 little-endian, no header)")
  ap.add_argument("dim", type=int, help="dimension of each vector")
  ap.add_argument("--io", choices=("pread", "mmap"), default="pread", help="random-read IO method")
  ap.add_argument("--quiet", action="store_true", help="suppress per-dim full listing and progress")
  ap.add_argument("--label", default="vectors", help="label used in intrinsic-dim report (e.g. docs, queries)")
  ap.add_argument("--no-intrinsic-dim", action="store_true", help="skip intrinsic-dim estimation")
  args = ap.parse_args()

  global IO_METHOD, NOISY, DO_INTRINSIC_DIM
  IO_METHOD = args.io
  NOISY = not args.quiet
  if args.no_intrinsic_dim:
    DO_INTRINSIC_DIM = False

  smell_vectors(args.dim, args.vec_path, args.label)


if __name__ == "__main__":
  _main()
