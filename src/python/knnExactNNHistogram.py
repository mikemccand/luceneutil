#!/usr/bin/env python3

# computes brute-force exact nearest neighbor dot-product scores for varying
# numbers of query vectors (N), repeating M times per N, advancing through
# fresh query vectors each run.  writes raw float32 .bin score files that a
# separate tool can turn into histogram PNGs and then a video.

import argparse
import json
import os
import struct
import sys
import time

import numpy as np


def load_vectors(path, dim, count, start_index=0):
  """load count float32 vectors of given dim from a .vec file, starting at start_index.
  wraps around to the beginning of the file if start_index + count exceeds the file."""
  vec_bytes = dim * 4
  file_size = os.path.getsize(path)
  total_vecs = file_size // vec_bytes
  if file_size % vec_bytes != 0:
    raise RuntimeError(f'"{path}" size {file_size} is not a multiple of vector byte size {vec_bytes}; wrong dim?')

  # wrap start_index into range
  start_index = start_index % total_vecs

  with open(path, "rb") as f:
    if start_index + count <= total_vecs:
      # no wrap needed
      f.seek(start_index * vec_bytes)
      data = f.read(count * vec_bytes)
    else:
      # read tail, then wrap and read from beginning
      tail_count = total_vecs - start_index
      f.seek(start_index * vec_bytes)
      data = f.read(tail_count * vec_bytes)
      remaining = count - tail_count
      # may need multiple full wraps if count > total_vecs
      while remaining > 0:
        f.seek(0)
        chunk = min(remaining, total_vecs)
        data += f.read(chunk * vec_bytes)
        remaining -= chunk
      print(f"  WARNING: query vectors wrapped around (start_index={start_index}, count={count}, total={total_vecs})")

  return np.frombuffer(data, dtype="<f4").reshape(count, dim)


def compute_topk_scores(queries, docs, top_k, batch_size=200):
  """compute dot product of each query against all docs, keep top_k scores per query.

  returns float32 array of shape (num_queries, top_k).
  """
  num_queries = queries.shape[0]
  num_docs = docs.shape[0]
  all_scores = np.empty((num_queries, top_k), dtype=np.float32)

  for batch_start in range(0, num_queries, batch_size):
    batch_end = min(batch_start + batch_size, num_queries)
    batch = queries[batch_start:batch_end]

    # (batch, dim) @ (dim, num_docs) -> (batch, num_docs)
    scores = batch @ docs.T

    # partial sort to get top_k largest per row
    if top_k >= num_docs:
      # just sort and take all
      sorted_scores = np.sort(scores, axis=1)[:, ::-1][:, :top_k]
      all_scores[batch_start:batch_end] = sorted_scores
    else:
      # argpartition is O(n) per row
      idx = np.argpartition(scores, -top_k, axis=1)[:, -top_k:]
      # gather the top_k scores
      topk_unsorted = np.take_along_axis(scores, idx, axis=1)
      all_scores[batch_start:batch_end] = topk_unsorted

    if batch_start == 0 or (batch_start % (batch_size * 10)) == 0:
      print(f"  queries {batch_start}..{batch_end} of {num_queries}", flush=True)

  return all_scores


def main():
  parser = argparse.ArgumentParser(
    description="compute brute-force exact NN dot-product scores for varying N, M repeats"
  )
  parser.add_argument("-docs", required=True, help="path to doc vectors .vec file")
  parser.add_argument("-queries", required=True, help="path to query vectors .vec file")
  parser.add_argument("-dim", type=int, required=True, help="vector dimensionality")
  parser.add_argument("-numDocs", type=int, required=True, help="number of doc vectors to load")
  parser.add_argument("-topK", type=int, required=True, help="top-K scores to keep per query")
  parser.add_argument(
    "-N",
    required=True,
    help="comma-separated list of query vector counts, e.g. 100,500,1000,5000",
  )
  parser.add_argument("-M", type=int, required=True, help="number of repeats per N value")
  parser.add_argument("-outputDir", required=True, help="directory to write .bin score files")
  parser.add_argument(
    "-batchSize",
    type=int,
    default=200,
    help="number of queries to matmul at once (controls peak RAM)",
  )
  args = parser.parse_args()

  n_values = [int(x) for x in args.N.split(",")]
  m_repeats = args.M
  dim = args.dim
  num_docs = args.numDocs
  top_k = args.topK
  batch_size = args.batchSize

  total_queries_needed = sum(n * m_repeats for n in n_values)
  query_file_size = os.path.getsize(args.queries)
  total_queries_available = query_file_size // (dim * 4)
  if total_queries_needed > total_queries_available:
    print(
      f"WARNING: need {total_queries_needed} total query vectors but file only has"
      f" {total_queries_available}; will wrap around"
    )

  print(f"doc vectors: {args.docs}")
  print(f"query vectors: {args.queries}")
  print(f"dim={dim} numDocs={num_docs} topK={top_k}")
  print(f"N values: {n_values}")
  print(f"M repeats: {m_repeats}")
  print(f"total query vectors needed: {total_queries_needed} (available: {total_queries_available})")
  print(f"batch size: {batch_size}")
  print(f"output dir: {args.outputDir}")

  os.makedirs(args.outputDir, exist_ok=True)

  # load doc vectors
  print(f"\nloading {num_docs} doc vectors...", flush=True)
  t0 = time.time()
  docs = load_vectors(args.docs, dim, num_docs)
  t1 = time.time()
  print(f"loaded {num_docs} doc vectors in {t1 - t0:.1f}s ({docs.nbytes / 1e9:.2f} GB)")

  # verify BLAS is available
  print(f"numpy version: {np.__version__}")
  np_config = np.__config__
  if hasattr(np_config, "show"):
    # numpy 2.x
    print("numpy config: (run 'python -c \"import numpy; numpy.show_config()\"' for BLAS details)")

  query_start_index = 0
  manifest = []

  for n in n_values:
    for m in range(m_repeats):
      print(f"\n--- N={n}, repeat={m}, queryStartIndex={query_start_index} ---", flush=True)

      t0 = time.time()
      queries = load_vectors(args.queries, dim, n, start_index=query_start_index)
      t1 = time.time()
      print(f"  loaded {n} query vectors in {t1 - t0:.3f}s")

      t0 = time.time()
      scores = compute_topk_scores(queries, docs, top_k, batch_size)
      t1 = time.time()
      elapsed_sec = t1 - t0
      print(f"  computed {n} x {num_docs} dot products in {elapsed_sec:.1f}s")

      # write scores as little-endian float32
      out_file = os.path.join(args.outputDir, f"scores_N{n}_M{m}.bin")
      scores.astype("<f4").tofile(out_file)
      num_scores = scores.size
      print(f"  wrote {num_scores} scores ({num_scores * 4} bytes) to {out_file}")

      manifest.append({
        "n": n,
        "m": m,
        "queryStartIndex": query_start_index,
        "numScores": int(num_scores),
        "elapsedSec": round(elapsed_sec, 3),
        "file": f"scores_N{n}_M{m}.bin",
      })

      query_start_index += n

  # write manifest so the histogram/video tool knows the run order and params
  manifest_path = os.path.join(args.outputDir, "manifest.json")
  manifest_data = {
    "docs": args.docs,
    "queries": args.queries,
    "dim": dim,
    "numDocs": num_docs,
    "topK": top_k,
    "nValues": n_values,
    "mRepeats": m_repeats,
    "runs": manifest,
  }
  with open(manifest_path, "w") as f:
    json.dump(manifest_data, f, indent=2)
  print(f"\nwrote manifest to {manifest_path}")
  print("done.")


if __name__ == "__main__":
  main()
