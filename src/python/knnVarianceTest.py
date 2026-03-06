#!/usr/bin/env python

"""Measure HNSW graph build variance by running identical KNN benchmarks many times.

Runs knnPerfTest.run_single_knn_iteration() N times (optionally in parallel),
with optional doc-vector shuffling, and collects per-run results for analysis.
"""

import argparse
import json
import operator
import os
import shutil
import statistics
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import constants
import knnPerfTest
import numpy as np
from common import getLuceneDirFromGradleProperties


def extract_vectors(src_path, count, dim, dst_path):
  """Extract the first `count` vectors from src_path and write to dst_path."""
  vec_size_bytes = dim * 4
  src_size = os.path.getsize(src_path)
  available = src_size // vec_size_bytes
  if count > available:
    raise RuntimeError(f"requested {count} vectors but {src_path} only has {available} (dim={dim})")

  bytes_to_copy = count * vec_size_bytes
  with open(src_path, "rb") as fin, open(dst_path, "wb") as fout:
    remaining = bytes_to_copy
    while remaining > 0:
      chunk = fin.read(min(remaining, 64 * 1024 * 1024))
      if not chunk:
        raise RuntimeError(f"unexpected EOF reading {src_path}")
      fout.write(chunk)
      remaining -= len(chunk)

  print(f"extracted {count} vectors ({bytes_to_copy} bytes) from {src_path} -> {dst_path}")


def shuffle_vectors_to_file(src_path, dst_path, count, dim, seed):
  """Read count vectors from src_path, shuffle with given seed, write to dst_path.

  Returns the permutation array P where shuffled[i] = original[P[i]].
  """
  data = np.fromfile(src_path, dtype="<f4", count=count * dim).reshape(count, dim)

  rng = np.random.default_rng(seed)
  perm = rng.permutation(count)
  shuffled = data[perm]

  shuffled.tofile(dst_path)
  print(f"shuffled {count} vectors (seed={seed}) -> {dst_path}")
  return perm


def remap_exact_nn_bin(src_bin_path, dst_bin_path, perm, num_queries, top_k):
  """Remap doc IDs in exact-NN .bin file according to shuffle permutation.

  The .bin file has num_queries * top_k little-endian int32s.
  Each doc ID `old_id` maps to `inv_perm[old_id]` in the shuffled index.
  """
  # build inverse permutation: inv_perm[old_id] = new_id
  inv_perm = np.empty(len(perm), dtype=np.int32)
  inv_perm[perm] = np.arange(len(perm), dtype=np.int32)

  raw = np.fromfile(src_bin_path, dtype="<i4")
  if raw.size != num_queries * top_k:
    raise RuntimeError(f"exact-NN .bin has {raw.size} ints, expected {num_queries * top_k} (num_queries={num_queries}, top_k={top_k})")

  remapped = inv_perm[raw]
  remapped.tofile(dst_bin_path)
  print(f"remapped exact-NN doc IDs ({num_queries} queries x {top_k} topK) -> {dst_bin_path}")


def find_exact_nn_files(work_dir):
  """Find the exact-NN .bin and .scores files created by KnnGraphTester in work_dir."""
  exact_nn_dir = Path(work_dir) / "knn-reuse" / "exact-nn"
  if not exact_nn_dir.exists():
    raise RuntimeError(f"no knn-reuse/exact-nn directory in {work_dir}")

  bin_files = list(exact_nn_dir.glob("*.bin"))
  scores_files = list(exact_nn_dir.glob("*.scores"))

  if len(bin_files) != 1:
    raise RuntimeError(f"expected exactly 1 .bin file in {exact_nn_dir}, found {len(bin_files)}: {bin_files}")
  if len(scores_files) > 1:
    raise RuntimeError(f"expected 0 or 1 .scores file in {exact_nn_dir}, found {len(scores_files)}")

  scores_file = scores_files[0] if scores_files else None
  return bin_files[0], scores_file


def seed_exact_nn_cache(canonical_bin, canonical_scores, target_work_dir, perm, num_queries, top_k):
  """Pre-populate target_work_dir's exact-NN cache with remapped files.

  If perm is None (no shuffle), just copies the files.
  """
  exact_nn_dir = Path(target_work_dir) / "knn-reuse" / "exact-nn"
  os.makedirs(exact_nn_dir, exist_ok=True)

  dst_bin = exact_nn_dir / canonical_bin.name
  if perm is not None:
    remap_exact_nn_bin(canonical_bin, dst_bin, perm, num_queries, top_k)
  else:
    shutil.copy2(canonical_bin, dst_bin)

  dst_scores = None
  if canonical_scores is not None:
    dst_scores = exact_nn_dir / canonical_scores.name
    # scores don't depend on doc ID ordering -- the same query-doc pairs produce the same scores
    # but the order within each query's top-K may differ after remap. however, KnnGraphTester
    # doesn't care about score ordering for recall computation, so we just copy scores as-is.
    # (the scores correspond to the same vectors, just with different doc IDs)
    shutil.copy2(canonical_scores, dst_scores)

  # touch cache files so they're newer than the doc/query vectors files
  now_ns = time.time_ns()
  os.utime(dst_bin, ns=(now_ns, now_ns))
  if dst_scores is not None:
    os.utime(dst_scores, ns=(now_ns, now_ns))

  print(f"seeded exact-NN cache in {exact_nn_dir}")


def parse_summary(summary_str):
  """Parse a tab-separated SUMMARY string into a dict keyed by OUTPUT_HEADERS."""
  values = summary_str.split("\t")
  result = {}
  for i, header in enumerate(knnPerfTest.OUTPUT_HEADERS):
    if i < len(values):
      result[header] = values[i]
  return result


def run_one_iteration(iteration, checkout, params, dim, doc_vectors, query_vectors, work_dir, shuffle, canonical_bin, canonical_scores, base_doc_vectors, ndoc, top_k, niter, base_seed):
  """Worker function for a single iteration. Called from main or pool."""
  run_dir = Path(work_dir) / f"run_{iteration:04d}"
  os.makedirs(run_dir, exist_ok=True)

  seed = None
  perm = None

  if shuffle and iteration > 0:
    seed = base_seed + iteration
    (run_dir / "seed.txt").write_text(str(seed))

    # write shuffled doc vectors
    run_docs = run_dir / "docs.vec"
    perm = shuffle_vectors_to_file(base_doc_vectors, run_docs, ndoc, dim, seed)

    # seed exact-NN cache with remapped doc IDs
    seed_exact_nn_cache(canonical_bin, canonical_scores, run_dir, perm, niter, top_k)
    actual_doc_vectors = run_docs
  else:
    # iteration 0, or no shuffle: use the base (unshuffled) doc vectors
    if iteration > 0:
      # no shuffle but still need exact-NN cache to avoid recomputation
      seed_exact_nn_cache(canonical_bin, canonical_scores, run_dir, None, niter, top_k)
    actual_doc_vectors = base_doc_vectors
    (run_dir / "seed.txt").write_text("none")

  t0_sec = time.monotonic()
  summary_str, full_output = knnPerfTest.run_single_knn_iteration(
    checkout,
    params,
    dim,
    actual_doc_vectors,
    query_vectors,
    str(run_dir),
  )
  elapsed_sec = time.monotonic() - t0_sec

  # save all output
  (run_dir / "output.log").write_text(full_output)

  # parse and save structured results
  result = parse_summary(summary_str)
  result["iteration"] = iteration
  result["seed"] = seed
  result["elapsed_sec"] = round(elapsed_sec, 3)
  (run_dir / "results.json").write_text(json.dumps(result, indent=2))

  # clean up index to reclaim disk space
  index_dir = run_dir / "knn-reuse" / "indices"
  if index_dir.exists():
    shutil.rmtree(index_dir)
    print(f"[variance] deleted index dir {index_dir}")

  # clean up shuffled doc vectors (reproducible from seed + base docs)
  shuffled_docs = run_dir / "docs.vec"
  if shuffle and iteration > 0 and shuffled_docs.exists():
    shuffled_docs.unlink()
    print(f"[variance] deleted shuffled docs {shuffled_docs}")

  return result


def print_variance_summary(all_results):
  """Print statistical summary of all iteration results."""
  numeric_fields = ["recall", "latency(ms)", "netCPU", "avgCpuCount", "visited", "index(s)", "force_merge(s)", "index_size(MB)"]

  print("\n" + "=" * 80)
  print(f"VARIANCE SUMMARY ({len(all_results)} iterations)")
  print("=" * 80)

  for field in numeric_fields:
    values = []
    for r in all_results:
      if field in r:
        try:
          values.append(float(r[field]))
        except (ValueError, TypeError):
          pass

    if len(values) < 2:
      continue

    values.sort()
    mean = statistics.mean(values)
    median = statistics.median(values)
    stdev = statistics.stdev(values)
    p5 = values[max(0, int(len(values) * 0.05))]
    p95 = values[min(len(values) - 1, int(len(values) * 0.95))]
    cv = (stdev / mean * 100) if mean != 0 else 0

    print(f"\n  {field}:")
    print(f"    mean={mean:.4f}  median={median:.4f}  stdev={stdev:.4f}  cv={cv:.2f}%")
    print(f"    min={values[0]:.4f}  max={values[-1]:.4f}  p5={p5:.4f}  p95={p95:.4f}")


def main():
  parser = argparse.ArgumentParser(description="measure HNSW build variance by running identical KNN benchmarks repeatedly")
  parser.add_argument("--iterations", type=int, required=True, help="total number of runs")
  parser.add_argument("--concurrency", type=int, default=1, help="max parallel subprocesses (default: 1)")
  parser.add_argument("--ndoc", type=int, default=400_000, help="number of doc vectors to index (default: 400000)")
  parser.add_argument("--niter", type=int, default=10_000, help="number of query vectors to search (default: 10000)")
  parser.add_argument("--dim", type=int, default=1024, help="vector dimensionality (default: 1024)")
  parser.add_argument("--doc-vectors", type=str, default="/lucenedata/enwiki/cohere-v3/cohere-v3-wikipedia-en-scattered-1024d.docs.vec", help="path to source doc vectors file")
  parser.add_argument("--query-vectors", type=str, default="/lucenedata/enwiki/cohere-v3/cohere-v3-wikipedia-en-scattered-1024d.queries.vec", help="path to source query vectors file")
  parser.add_argument("--output-dir", type=str, default=None, help="output directory (default: auto-timestamped)")
  parser.add_argument("--shuffle-docs", action="store_true", help="shuffle doc vectors before each run")
  parser.add_argument("--base-seed", type=int, default=None, help="base random seed (default: random)")
  parser.add_argument("--top-k", type=int, default=100, help="top-K for recall computation (default: 100)")
  parser.add_argument("--max-conn", type=int, default=32, help="HNSW maxConn (default: 32)")
  parser.add_argument("--beam-width-index", type=int, default=100, help="HNSW beamWidthIndex (default: 100)")
  parser.add_argument("--fanout", type=int, default=25, help="search fanout (default: 25)")
  parser.add_argument("--quantize-bits", type=int, default=4, help="quantize bits (default: 4)")
  parser.add_argument("--quantize-compress", action="store_true", help="enable quantize compression")
  parser.add_argument("--force-merge", action="store_true", default=True, help="force merge to 1 segment (default: True)")
  parser.add_argument("--no-force-merge", action="store_false", dest="force_merge")
  parser.add_argument("--encoding", type=str, default="float32", help="vector encoding (default: float32)")
  parser.add_argument("--metric", type=str, default="dot_product", help="similarity metric (default: dot_product)")
  parser.add_argument("--num-merge-worker", type=int, default=24, help="merge workers (default: 24)")
  parser.add_argument("--num-merge-thread", type=int, default=8, help="merge threads (default: 8)")
  parser.add_argument("--num-search-thread", type=int, default=4, help="search threads (default: 4)")

  args = parser.parse_args()

  if args.base_seed is None:
    args.base_seed = int.from_bytes(os.urandom(4), "big")
  print(f"base seed: {args.base_seed}")

  # set up output directory
  if args.output_dir is None:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    args.output_dir = str(Path(constants.LOGS_DIR) / f"knn-variance-{timestamp}")
  os.makedirs(args.output_dir, exist_ok=True)
  print(f"output directory: {args.output_dir}")

  # build flat params dict (single values, not tuples)
  params = {
    "ndoc": args.ndoc,
    "maxConn": args.max_conn,
    "beamWidthIndex": args.beam_width_index,
    "fanout": args.fanout,
    "quantizeBits": args.quantize_bits,
    "quantizeCompress": args.quantize_compress,
    "encoding": args.encoding,
    "metric": args.metric,
    "numMergeWorker": args.num_merge_worker,
    "numMergeThread": args.num_merge_thread,
    "numSearchThread": args.num_search_thread,
    "topK": args.top_k,
    "niter": args.niter,
  }
  if args.force_merge:
    params["forceMerge"] = True

  # print cpu and memory info
  knnPerfTest.print_cpu_info()
  knnPerfTest.print_mem_info()

  lucene_checkout = getLuceneDirFromGradleProperties()

  # save full config
  config = {
    "iterations": args.iterations,
    "concurrency": args.concurrency,
    "ndoc": args.ndoc,
    "niter": args.niter,
    "dim": args.dim,
    "doc_vectors_source": args.doc_vectors,
    "query_vectors_source": args.query_vectors,
    "shuffle_docs": args.shuffle_docs,
    "base_seed": args.base_seed,
    "params": params,
    "lucene_checkout": lucene_checkout,
    "start_time": datetime.now().isoformat(),
  }
  config_path = Path(args.output_dir) / "config.json"
  config_path.write_text(json.dumps(config, indent=2))
  print(f"config saved to {config_path}")

  # step 1: extract doc and query slices
  base_doc_vectors = Path(args.output_dir) / "docs_base.vec"
  query_vectors = Path(args.output_dir) / "queries.vec"

  extract_vectors(args.doc_vectors, args.ndoc, args.dim, base_doc_vectors)
  extract_vectors(args.query_vectors, args.niter, args.dim, query_vectors)

  # step 2: run canonical iteration 0 (unshuffled) to compute exact-NN
  print("\n" + "=" * 80)
  print("ITERATION 0 (canonical, unshuffled) -- computing exact-NN")
  print("=" * 80)

  result_0 = run_one_iteration(
    iteration=0,
    checkout=lucene_checkout,
    params=params,
    dim=args.dim,
    doc_vectors=base_doc_vectors,
    query_vectors=query_vectors,
    work_dir=args.output_dir,
    shuffle=False,
    canonical_bin=None,
    canonical_scores=None,
    base_doc_vectors=base_doc_vectors,
    ndoc=args.ndoc,
    top_k=args.top_k,
    niter=args.niter,
    base_seed=args.base_seed,
  )

  all_results = [result_0]

  # find the exact-NN cache files from iteration 0
  run_0_dir = Path(args.output_dir) / "run_0000"
  canonical_bin, canonical_scores = find_exact_nn_files(run_0_dir)
  print(f"canonical exact-NN: bin={canonical_bin}, scores={canonical_scores}")

  # step 3: run remaining iterations
  remaining = args.iterations - 1
  if remaining <= 0:
    print("only 1 iteration requested, done")
  elif args.concurrency <= 1:
    # sequential
    for i in range(1, args.iterations):
      print(f"\n{'=' * 80}")
      print(f"ITERATION {i}/{args.iterations - 1}")
      print(f"{'=' * 80}")

      result = run_one_iteration(
        iteration=i,
        checkout=lucene_checkout,
        params=params,
        dim=args.dim,
        doc_vectors=base_doc_vectors,
        query_vectors=query_vectors,
        work_dir=args.output_dir,
        shuffle=args.shuffle_docs,
        canonical_bin=canonical_bin,
        canonical_scores=canonical_scores,
        base_doc_vectors=base_doc_vectors,
        ndoc=args.ndoc,
        top_k=args.top_k,
        niter=args.niter,
        base_seed=args.base_seed,
      )
      all_results.append(result)
  else:
    # parallel execution
    print(f"\nrunning {remaining} iterations with concurrency={args.concurrency}")

    # pre-create shuffled docs and seed exact-NN caches before launching parallel workers,
    # because numpy + file I/O in forked workers can be problematic
    run_configs = []
    for i in range(1, args.iterations):
      run_dir = Path(args.output_dir) / f"run_{i:04d}"
      os.makedirs(run_dir, exist_ok=True)

      if args.shuffle_docs:
        seed = args.base_seed + i
        (run_dir / "seed.txt").write_text(str(seed))
        run_docs = run_dir / "docs.vec"
        perm = shuffle_vectors_to_file(base_doc_vectors, run_docs, args.ndoc, args.dim, seed)
        seed_exact_nn_cache(canonical_bin, canonical_scores, run_dir, perm, args.niter, args.top_k)
        actual_doc_vectors = run_docs
      else:
        (run_dir / "seed.txt").write_text("none")
        seed_exact_nn_cache(canonical_bin, canonical_scores, run_dir, None, args.niter, args.top_k)
        actual_doc_vectors = base_doc_vectors

      run_configs.append((i, actual_doc_vectors))

    # now launch the actual Java benchmarks in parallel
    with ProcessPoolExecutor(max_workers=args.concurrency) as executor:
      futures = {}
      for i, actual_doc_vectors in run_configs:
        run_dir = Path(args.output_dir) / f"run_{i:04d}"
        future = executor.submit(
          _run_iteration_worker,
          lucene_checkout,
          params,
          args.dim,
          str(actual_doc_vectors),
          str(query_vectors),
          str(run_dir),
          i,
          args.shuffle_docs,
        )
        futures[future] = i

      for future in as_completed(futures):
        iteration_idx = futures[future]
        try:
          result = future.result()
          all_results.append(result)
          print(f"[variance] iteration {iteration_idx} completed: recall={result.get('recall', '?')}")
        except (RuntimeError, OSError) as e:
          print(f"[variance] iteration {iteration_idx} FAILED: {e}", file=sys.stderr)

  # sort results by iteration number
  all_results.sort(key=operator.itemgetter("iteration"))

  # save all results
  results_path = Path(args.output_dir) / "all_results.json"
  results_path.write_text(json.dumps(all_results, indent=2))
  print(f"\nall results saved to {results_path}")

  # print summary
  print_variance_summary(all_results)

  # save summary to file too
  config["end_time"] = datetime.now().isoformat()
  config_path.write_text(json.dumps(config, indent=2))

  print(f"\ndone. {len(all_results)} iterations completed. output: {args.output_dir}")


def _run_iteration_worker(checkout, params, dim, doc_vectors, query_vectors, run_dir, iteration, shuffle):
  """Worker function for ProcessPoolExecutor -- must be top-level for pickling."""
  t0_sec = time.monotonic()
  summary_str, full_output = knnPerfTest.run_single_knn_iteration(
    checkout,
    params,
    dim,
    doc_vectors,
    query_vectors,
    run_dir,
  )
  elapsed_sec = time.monotonic() - t0_sec

  # save output
  run_path = Path(run_dir)
  (run_path / "output.log").write_text(full_output)

  result = parse_summary(summary_str)
  result["iteration"] = iteration
  # read seed from the file we wrote during pre-creation
  seed_file = run_path / "seed.txt"
  if seed_file.exists():
    seed_text = seed_file.read_text().strip()
    result["seed"] = int(seed_text) if seed_text != "none" else None
  result["elapsed_sec"] = round(elapsed_sec, 3)

  (run_path / "results.json").write_text(json.dumps(result, indent=2))

  # clean up index
  index_dir = Path(run_dir) / "knn-reuse" / "indices"
  if index_dir.exists():
    shutil.rmtree(index_dir)

  # clean up shuffled doc vectors
  shuffled_docs = Path(run_dir) / "docs.vec"
  if shuffle and shuffled_docs.exists():
    shuffled_docs.unlink()

  return result


if __name__ == "__main__":
  main()
