#!/usr/bin/env/python

# TODO
#   - hmm what is "normalized" boolean at KNN indexing time? -- COSINE similarity sets this to true
#   - try turning diversity off -- faster forceMerge?  better recall?
#   - why force merge 12X slower
#   - why only one thread
#   - report net concurrency utilized in the table
#   - report total cpu for all indexing threads too
#   - hmm how come so much faster to compute exact NN at queryStartIndex=0 than 10000, 20000?  60 sec vs ~470 sec!?
#     - not always the first run!  sometimes 2nd run is super-fast

import argparse
import itertools
import mmap
import multiprocessing
import os
import random
import re
import shlex
import shutil
import statistics
import struct
import subprocess
import sys
import time
from pathlib import Path

try:
  import numpy as np
except ImportError:
  print("\nERROR: numpy is required but not installed.\n")
  print("To fix, run from the luceneutil root directory:\n")
  print("  make env")
  print("  source .venv/bin/activate")
  print("  python -u src/python/knnPerfTest.py\n")
  raise SystemExit(1) from None

import autologger
import benchUtil
import constants
import knnExactNN
import ps_head
from benchUtil import GNUPLOT_PATH, PERF_EXE
from common import getLuceneDirFromGradleProperties

# toggle between 'pread' and 'mmap' for concurrent random vector reads when smelling vectors -- pread is
# maybe a bit faster?
IO_METHOD = "pread"
# IO_METHOD = "mmap"


def advise_will_need(file_name, offset_bytes=0, length_bytes=0):
  """Proactively hint to the OS to load a range of file into RAM, using the configured IO_METHOD."""
  if not os.path.exists(file_name):
    return

  file_size = os.path.getsize(file_name)
  if length_bytes <= 0 or offset_bytes + length_bytes > file_size:
    length_bytes = file_size - offset_bytes

  if length_bytes <= 0:
    return

  with open(file_name, "rb") as f:
    if IO_METHOD == "pread":
      os.posix_fadvise(f.fileno(), offset_bytes, length_bytes, os.POSIX_FADV_WILLNEED)
    elif IO_METHOD == "mmap":
      # map the part of the file we need
      mm = mmap.mmap(f.fileno(), length_bytes, offset=offset_bytes, access=mmap.ACCESS_READ)
      try:
        mm.madvise(mmap.MADV_WILLNEED)
      finally:
        mm.close()


# see also https://share.google/aimode/IDYCxtTyGhFUwC1pX for clean-ish
# ways to use io_uring-like async io from Python

# Measure vector search recall and latency while exploring hyperparameters

# SETUP:
### Download and extract data files: Wikipedia line docs + GloVe
# python src/python/initial_setup.py -download    OR    curl -O  https://downloads.cs.stanford.edu/nlp/data/glove.6B.zip -k
# cd ../data
# unzip glove.6B.zip
# unlzma enwiki-20120502-lines-1k.txt.lzma    OR    xz enwiki-20120502-lines-1k.txt.lzma
### Create document and task vectors
# ./gradlew vectors-100
#
# change the parameters below and then run (you can still manually run this file, but using gradle command
# below will auto recompile if you made any changes to java files in luceneutils)
# ./gradlew runKnnPerfTest
#
# for the median result of n runs with the same parameters:
# ./gradlew runKnnPerfTest -Pruns=n
#
# you may want to modify the following settings:

# uses CPUTime sampling (newly available/experimental in Java 25, seems to work on the tasks benchmark)
DO_PROFILING = False
DO_PS = True
DO_VMSTAT = True

# precompute exact NN using numpy (multi-threaded BLAS matmul).  when False,
# KnnGraphTester.java computes exact NN itself (slower, single-threaded Java).
USE_NUMPY_EXACT_NN = True

# Set this to True to use perf tool to record instructions executed and confirm SIMD
# instructions were executed
# TODO: how much overhead / perf impact from this?  can we always run?
CONFIRM_SIMD_ASM_MODE = False

# perf stat SIMD validation: uses hardware counters to confirm SIMD (SSE/AVX2/AVX-512)
# is actually used during vector operations.  negligible overhead -- always on when perf
# is available.
DO_PERF_STAT_SIMD = PERF_EXE is not None

# set this to True to collect all HNSW traversal scores and generate a histogram
DO_HNSW_SCORE_HISTOGRAM = False

# sample 1 in every N HNSW traversal scores when building the histogram (to keep HTML size reasonable)
HNSW_SAMPLE_EVERY_N = 100

# set this to True to compute sampled all query x doc distances and generate a histogram
DO_ALL_DISTANCES_HISTOGRAM = False

# sample 1 in every N all-distances scores (sampling done in Java).
# with 400K docs and 10K queries (4B distances), 1000 -> ~4M samples -> ~40 MB HTML
ALL_DISTANCES_SAMPLE_EVERY_N = 1000

if CONFIRM_SIMD_ASM_MODE and PERF_EXE is None:
  raise RuntimeError("CONFIRM_SIMD_ASM_MODE is True but PERF_EXE is not found; install 'perf' tool and rerun?")

# e.g. to compile KnnIndexer:
#
#   javac -d build -cp /l/trunk/lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar:/l/trunk/lucene/join/build/libs/lucene-join-10.0.0-SNAPSHOT.jar src/main/knn/*.java src/main/WikiVectors.java src/main/perf/VectorDictionary.java
#

NOISY = True

# TODO
#  - can we expose greediness (global vs local queue exploration in KNN search) here?

# test parameters. This script will run KnnGraphTester on every combination of these parameters
PARAMS = {
  # "ndoc": (10_000_000,),
  #'ndoc': (10000, 100000, 200000, 500000),
  #'ndoc': (10000, 100000, 200000, 500000),
  #'ndoc': (2_000_000,),
  #'ndoc': (1_000_000,),
  "ndoc": (500_000,),
  #'ndoc': (50_000,),
  "maxConn": (64,),
  # "maxConn": (64,),
  #'maxConn': (32,),
  "beamWidthIndex": (250,),
  # "beamWidthIndex": (250,),
  #'beamWidthIndex': (50,),
  "fanout": (100,),
  # "fanout": (50,),
  #'quantize': None,
  #'quantizeBits': (32, 7, 4),
  "numMergeWorker": (24,),
  "numMergeThread": (8,),
  "numSearchThread": (4,),
  #'numMergeWorker': (1,),
  #'numMergeThread': (1,),
  "encoding": ("float32",),
  # "metric": ("cosine",),  # default is angular (dot_product)
  "metric": ("dot_product",),
  # 'metric': ('mip',),
  #'quantize': (True,),
  "quantizeBits": (32, 8, 7, 4, 2),
  # "quantizeBits": (1,),
  # "overSample": (5,), # extra ratio of vectors to retrieve, for testing approximate scoring, e.g. quantized indices
  #'fanout': (0,),
  "topK": (100,),
  # "bp": ("false", "true"),
  #'quantizeCompress': (True, False),
  "quantizeCompress": (True,),
  # "indexType": ("flat", "hnsw"), # index type,
  # "queryStartIndex": (0, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000),  # seek to this start vector before searching, to sample different vectors
  # "queryStartIndex": (0, 200000, 400000, 600000),
  "forceMerge": (True,),
  "niter": (10000,),
  # "filterStrategy": ("query-time-pre-filter", "query-time-post-filter", "index-time-filter"),
  # "filterSelectivity": ("0.5", "0.2", "0.1", "0.01",),
  # "searchType": ("radius",),
  # "resultSimilarity": ("0.8", "0.9",),
  # "decay": ("0", "0.5", "0.8",),
}


OUTPUT_HEADERS = [
  "recall",
  "latency(ms)",
  "netCPU",
  "avgCpuCount",
  "nDoc",
  "searchType",
  "topK",
  "fanout",
  "resultSimilarity",
  "decay",
  "resultCount",
  "maxConn",
  "beamWidth",
  "quantized",
  "visited",
  "index(s)",
  "index_docs/s",
  "force_merge(s)",
  "num_segments",
  "index_size(MB)",
  "filterStrategy",
  "filterSelectivity",
  "overSample",
  "vec_disk(MB)",
  "vec_RAM(MB)",
  "bp-reorder",
  "indexType",
]
# TODO:  "bp",


def advance(ix, values):
  for i in reversed(range(len(ix))):
    # scary to rely on dict key enumeration order?  but i guess if dict never changes while we do this, it's stable?
    param = list(values.keys())[i]
    # print("advance " + param)
    if type(values[param]) in (list, tuple) and ix[i] == len(values[param]) - 1:
      ix[i] = 0
    else:
      ix[i] += 1
      return True
  return False


_SPARK_CHARS = " ▁▂▃▄▅▆▇█"
_NUM_SPARK_BINS = 20
_NUM_DIM_SAMPLE_VECS = 2000
_THRESH_CONSTANT_STD = 1e-6
_THRESH_SPARSE_PCT_ZEROS = 0.50
_THRESH_SKEWED_ABS = 1.0
_THRESH_HEAVY_TAILS_KURTOSIS = 3.0
_THRESH_FLAT_KURTOSIS = -1.0
_THRESH_OUTLIER_SPREAD_SIGMA = 3.0


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
    return

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


def smell_vectors(dim, file_name):
  """Runs some simple sanity checks on the vector source file, because we don't store any
  self-describing metadata in the .vec source file.
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

  _check_dim_distributions(dim, file_name, num_vectors, vec_size_bytes)


# all candidate perf stat counters for SIMD validation -- probed at import time
# to discover which ones this CPU actually supports.
#
# two families:
#   fp_arith_inst_retired.*  -- counts float SIMD instructions (float32 vectors)
#   int_vec_retired.*        -- counts integer SIMD instructions (quantized vectors)
#                               available on Ice Lake+ (different naming on older CPUs)
_FP_SIMD_CANDIDATE_COUNTERS = (
  "fp_arith_inst_retired.scalar_single",
  "fp_arith_inst_retired.128b_packed_single",
  "fp_arith_inst_retired.256b_packed_single",
  "fp_arith_inst_retired.512b_packed_single",
)

_INT_SIMD_CANDIDATE_COUNTERS = (
  "int_vec_retired.128bit",
  "int_vec_retired.256bit",
  "int_vec_retired.512bit",
)

# label, counter name, weight (proportional to SIMD width so FP and INT are comparable)
_FP_SIMD_LEVEL_DEFS = (
  ("FP-scalar", "fp_arith_inst_retired.scalar_single", 1),
  ("FP-SSE", "fp_arith_inst_retired.128b_packed_single", 4),
  ("FP-AVX2", "fp_arith_inst_retired.256b_packed_single", 8),
  ("FP-AVX512", "fp_arith_inst_retired.512b_packed_single", 16),
)

_INT_SIMD_LEVEL_DEFS = (
  ("INT-SSE", "int_vec_retired.128bit", 4),
  ("INT-AVX2", "int_vec_retired.256bit", 8),
  ("INT-AVX512", "int_vec_retired.512bit", 16),
)


def _probe_perf_counter(counter):
  """Return True if perf recognizes this counter on the current CPU."""
  try:
    result = subprocess.run(
      [PERF_EXE, "stat", "-e", counter, "--", "true"],
      capture_output=True,
      text=True,
      timeout=5,
      check=False,
    )
    # perf exits 0 and prints the counter (possibly "<not counted>" for short
    # commands) when the counter is valid.  it exits non-zero or prints
    # "<not supported>" when the counter doesn't exist on this CPU.
    return result.returncode == 0 and "<not supported>" not in result.stderr
  except (subprocess.TimeoutExpired, OSError):
    return False


def _probe_simd_perf_counters():
  """Probe which SIMD perf counters this CPU supports.

  Probes each counter individually because perf refuses to run at all if any
  counter name is unrecognized (e.g. 512b on a non-AVX-512 CPU).

  Returns (available_counters, fp_levels, int_levels).
  """
  if PERF_EXE is None:
    return (), (), ()

  available = []
  for counter in _FP_SIMD_CANDIDATE_COUNTERS + _INT_SIMD_CANDIDATE_COUNTERS:
    if _probe_perf_counter(counter):
      available.append(counter)

  available_set = set(available)
  counters = tuple(available)
  fp_levels = tuple(t for t in _FP_SIMD_LEVEL_DEFS if t[1] in available_set)
  int_levels = tuple(t for t in _INT_SIMD_LEVEL_DEFS if t[1] in available_set)
  return counters, fp_levels, int_levels


# probe once at import time
_AVAILABLE_SIMD_COUNTERS, FP_SIMD_LEVELS, INT_SIMD_LEVELS = _probe_simd_perf_counters()
if _AVAILABLE_SIMD_COUNTERS:
  fp_names = [c for c in _AVAILABLE_SIMD_COUNTERS if c.startswith("fp_")]
  int_names = [c for c in _AVAILABLE_SIMD_COUNTERS if c.startswith("int_")]
  parts = []
  if fp_names:
    parts.append(f"FP: {', '.join(fp_names)}")
  if int_names:
    parts.append(f"INT: {', '.join(int_names)}")
  print(f"NOTE: perf SIMD counters available: {'; '.join(parts)}")
  if not int_names:
    print("NOTE: integer SIMD counters (int_vec_retired.*) not available on this CPU; quantized runs will only show FP counters")
elif DO_PERF_STAT_SIMD:
  print("WARNING: perf SIMD counters not available on this CPU; disabling SIMD validation")
  DO_PERF_STAT_SIMD = False


def wrap_cmd_with_perf_stat_simd(cmd, output_file):
  """Prepend perf stat with SIMD counters to a command, writing stats to output_file."""
  return [
    PERF_EXE,
    "stat",
    "-o",
    output_file,
    "-e",
    ",".join(_AVAILABLE_SIMD_COUNTERS),
    "--",
  ] + cmd


def parse_perf_stat_file(path):
  """Parse a perf stat output file and return dict of counter_name -> count (int).

  Returns empty dict if file is missing or unparseable.
  """
  result = {}
  try:
    text = Path(path).read_text()
  except OSError:
    return result
  for line in text.splitlines():
    # format: "     12,345,678      cpu_core/fp_arith_inst_retired.256b_packed_single/    (88.34%)"
    # or:     "     12345678      fp_arith_inst_retired.256b_packed_single"
    # or:     "     12345678      int_vec_retired.256bit"
    m = re.match(r"^\s+([\d,]+)\s+(?:\S+/)?((?:fp_arith_inst_retired|int_vec_retired)\.\S+?)(?:/|\s)", line)
    if m:
      count = int(m.group(1).replace(",", ""))
      counter = m.group(2)
      result[counter] = count
  return result


def _summarize_levels(counters, level_defs):
  """Compute per-level breakdown for a set of SIMD level defs.

  Returns (parts_list, total_ops, dominant_label) where parts_list has
  (label, count, ops) tuples for levels with count > 0.
  """
  parts = []
  total_ops = 0
  dominant_label = None
  dominant_ops = 0
  for label, counter, ops_per_insn in level_defs:
    count = counters.get(counter, 0)
    ops = count * ops_per_insn
    total_ops += ops
    if count > 0:
      parts.append((label, count, ops))
    if ops > dominant_ops:
      dominant_ops = ops
      dominant_label = label
  return parts, total_ops, dominant_label


def format_simd_report(counters, is_quantized=False):
  """Format a one-line SIMD usage summary from parsed perf stat counters.

  Combines FP (fp_arith_inst_retired) and integer (int_vec_retired) counters
  into a single flat percentage breakdown weighted by SIMD width, so all
  percentages sum to 100%.

  Returns (report_string, dominant_level) where dominant_level is
  "FP-scalar", "FP-SSE", "FP-AVX2", "FP-AVX512",
  "INT-SSE", "INT-AVX2", "INT-AVX512", or "none".
  """
  if not counters:
    return "SIMD: no perf data", "none"

  all_levels = list(FP_SIMD_LEVELS) + list(INT_SIMD_LEVELS)
  all_parts, grand_total, dominant = _summarize_levels(counters, all_levels)

  if grand_total == 0:
    if is_quantized and not INT_SIMD_LEVELS:
      return "SIMD: no FP instructions (expected for quantized); integer SIMD counters not available on this CPU", "none"
    return "SIMD: no instructions detected", "none"

  pct_parts = []
  for label, _count, ops in all_parts:
    pct_parts.append(f"{label} {100.0 * ops / grand_total:.1f}%")

  report = f"SIMD: {' | '.join(pct_parts)} (dominant: {dominant})"

  # warn only when the relevant family shows no SIMD
  if dominant == "FP-scalar" and not is_quantized:
    report += " WARNING: no SIMD vectorization detected!"
  elif is_quantized and not any(label.startswith("INT-") for label, _, _ in all_parts):
    if not INT_SIMD_LEVELS:
      report += " (int_vec_retired counters not available on this CPU)"
    else:
      report += " WARNING: no integer SIMD detected!"

  return report, dominant


def get_unique_log_name(log_path, sub_tool):
  log_dir_name, log_base_name = log_path
  upto = 0
  while True:
    log_file_name = f"{log_dir_name}/{log_base_name}-{sub_tool}"
    if upto > 0:
      log_file_name += f"-{upto}"
    log_file_name += ".log"
    if not os.path.exists(log_file_name):
      return log_file_name
    upto += 1


def print_run_summary(values):
  options = []
  fixed = []
  combos = 1

  # print these important params first, in this order:
  print_order = ["forceMerge", "ndoc", "niter", "topK", "quantizeBits"]
  key_to_ord = {}
  other_keys = []

  max_key_len = 0

  for key, value in values.items():
    max_key_len = max(max_key_len, len(key))
    try:
      key_ord = print_order.index(key)
    except ValueError:
      other_keys.append(key)

  for key_ord, key in enumerate(print_order):
    key_to_ord[key] = key_ord
  other_keys.sort()
  for key_ord, key in enumerate(other_keys):
    key_to_ord[key] = len(print_order) + key_ord
  ord_to_key = [-1] * len(key_to_ord)
  for key, key_ord in key_to_ord.items():
    ord_to_key[key_ord] = key

  for key in ord_to_key:
    value = values[key]
    if len(value) > 1:
      # yay, it turns out you can nest {...} in f-strings!
      options.append(f"  {key:<{max_key_len}s}: {','.join(str(v) for v in value)}")
      combos *= len(value)
    else:
      # yay, it turns out you can nest {...} in f-strings!
      fixed.append(f"  {key:<{max_key_len}s}: {value[0]}")

  if len(options) == 0:
    assert combos == 1
    print("NOTE: will run single warmup+test with these params:")
  else:
    print(f"NOTE: will run {combos} total warmups+tests with all combinations of:")
    for s in options:
      print(s)
  for s in fixed:
    print(s)


METRIC_LABELS = {
  "dot_product": ("dot_product similarity", "higher --->"),
  "angular": ("dot_product similarity", "higher --->"),
  "cosine": ("cosine similarity", "higher --->"),
  "euclidean": ("euclidean distance", "<--- lower"),
  "mip": ("max inner product", "higher --->"),
}


def generate_exact_nn_histogram(scores_path, output_dir, log_base_name, metric=None):
  """Read the binary exact NN scores file and generate an HTML histogram using Google Charts."""
  if not os.path.exists(scores_path):
    print(f"WARNING: exact NN scores file not found: {scores_path}")
    return

  file_size = os.path.getsize(scores_path)
  num_floats = file_size // 4
  if num_floats == 0:
    print("WARNING: exact NN scores file is empty")
    return

  all_scores = struct.unpack(f"<{num_floats}f", Path(scores_path).read_bytes())

  metric_name, direction = METRIC_LABELS.get(metric or "", ("similarity score", ""))

  min_score = min(all_scores)
  max_score = max(all_scores)

  if min_score == max_score:
    print(f"WARNING: all exact NN scores are identical ({min_score}); skipping histogram")
    return

  # Sort scores so JS can use binary search for fast range queries
  sorted_scores = sorted(all_scores)

  # Emit as a compact JSON array (6 decimal places keeps file reasonable)
  scores_js_lines = []
  chunk_size = 500
  for i in range(0, len(sorted_scores), chunk_size):
    chunk = sorted_scores[i : i + chunk_size]
    scores_js_lines.append(",".join(f"{s:.6f}" for s in chunk))
  scores_js = ",\n".join(scores_js_lines)

  html = f"""<!DOCTYPE html>
<html>
  <head>
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">
      google.load("visualization", "1", {{packages:["corechart"]}});
      google.setOnLoadCallback(init);

      // sorted scores for fast range queries via binary search
      var scores = [
{scores_js}
      ];
      var globalMin = {min_score:.6f};
      var globalMax = {max_score:.6f};
      var curMin = globalMin;
      var curMax = globalMax;
      var chart, chartDiv;
      var zoomStack = [];

      function lowerBound(arr, val) {{
        var lo = 0, hi = arr.length;
        while (lo < hi) {{
          var mid = (lo + hi) >> 1;
          if (arr[mid] < val) lo = mid + 1; else hi = mid;
        }}
        return lo;
      }}

      function buildHistogram(lo, hi, numBins) {{
        var range = hi - lo;
        var binWidth = range / numBins;
        var bins = new Array(numBins).fill(0);
        var startIdx = lowerBound(scores, lo);
        var endIdx = lowerBound(scores, hi);
        var count = 0;
        for (var i = startIdx; i < scores.length && scores[i] <= hi; i++) {{
          var idx = Math.floor((scores[i] - lo) / binWidth);
          if (idx >= numBins) idx = numBins - 1;
          if (idx < 0) idx = 0;
          bins[idx]++;
          count++;
        }}
        return {{bins: bins, binWidth: binWidth, count: count}};
      }}

      function drawChart(lo, hi) {{
        var numBins = 50;
        var h = buildHistogram(lo, hi, numBins);
        var rows = [['{metric_name} ({direction})', 'Count']];
        for (var i = 0; i < numBins; i++) {{
          var center = lo + (i + 0.5) * h.binWidth;
          rows.push([center, h.bins[i]]);
        }}
        var data = google.visualization.arrayToDataTable(rows);

        var zoomLabel = (lo === globalMin && hi === globalMax) ? '' : ' [zoomed]';
        var options = {{
          title: 'Exact NN {metric_name} distribution (' + h.count + ' of {num_floats} scores)' + zoomLabel,
          legend: {{position: 'none'}},
          hAxis: {{
            title: '{metric_name} ({direction})',
            viewWindow: {{min: lo, max: hi}}
          }},
          vAxis: {{title: 'Count'}},
          bar: {{groupWidth: '95%'}},
          chartArea: {{left: 80, right: 20, top: 40, bottom: 60}}
        }};

        chart.draw(data, options);
        document.getElementById('info').innerHTML =
          'Range: ' + lo.toFixed(6) + ' .. ' + hi.toFixed(6) +
          ' &nbsp; Bin width: ' + h.binWidth.toFixed(6) +
          ' &nbsp; Scores in view: ' + h.count;
      }}

      function init() {{
        chartDiv = document.getElementById('chart_div');
        chart = new google.visualization.ColumnChart(chartDiv);
        drawChart(globalMin, globalMax);

        // drag-to-zoom
        var dragStart = null;
        var overlay = document.getElementById('overlay');

        chartDiv.addEventListener('mousedown', function(e) {{
          var rect = chartDiv.getBoundingClientRect();
          dragStart = {{x: e.clientX - rect.left, clientX: e.clientX}};
          overlay.style.left = dragStart.x + 'px';
          overlay.style.width = '0px';
          overlay.style.display = 'block';
        }});

        chartDiv.addEventListener('mousemove', function(e) {{
          if (!dragStart) return;
          var rect = chartDiv.getBoundingClientRect();
          var curX = e.clientX - rect.left;
          var left = Math.min(dragStart.x, curX);
          var width = Math.abs(curX - dragStart.x);
          overlay.style.left = left + 'px';
          overlay.style.width = width + 'px';
        }});

        chartDiv.addEventListener('mouseup', function(e) {{
          if (!dragStart) return;
          overlay.style.display = 'none';
          var rect = chartDiv.getBoundingClientRect();
          var endX = e.clientX - rect.left;
          var x0 = Math.min(dragStart.x, endX);
          var x1 = Math.max(dragStart.x, endX);
          dragStart = null;

          // need at least 5px drag to count as zoom
          if (x1 - x0 < 5) return;

          var cli = chart.getChartLayoutInterface();
          var val0 = cli.getHAxisValue(x0);
          var val1 = cli.getHAxisValue(x1);
          if (val0 === null || val1 === null) return;
          var newMin = Math.max(Math.min(val0, val1), globalMin);
          var newMax = Math.min(Math.max(val0, val1), globalMax);
          if (newMax - newMin < (globalMax - globalMin) * 0.001) return;

          zoomStack.push({{min: curMin, max: curMax}});
          curMin = newMin;
          curMax = newMax;
          drawChart(curMin, curMax);
          document.getElementById('resetBtn').style.display = 'inline';
          document.getElementById('backBtn').style.display = 'inline';
        }});

        document.getElementById('resetBtn').addEventListener('click', function() {{
          zoomStack = [];
          curMin = globalMin;
          curMax = globalMax;
          drawChart(curMin, curMax);
          this.style.display = 'none';
          document.getElementById('backBtn').style.display = 'none';
        }});

        document.getElementById('backBtn').addEventListener('click', function() {{
          if (zoomStack.length === 0) return;
          var prev = zoomStack.pop();
          curMin = prev.min;
          curMax = prev.max;
          drawChart(curMin, curMax);
          if (zoomStack.length === 0) {{
            document.getElementById('resetBtn').style.display = 'none';
            this.style.display = 'none';
          }}
        }});
      }}
    </script>
    <style>
      #chart_container {{ position: relative; width: 1200px; height: 600px; }}
      #chart_div {{ width: 100%; height: 100%; }}
      #overlay {{ position: absolute; top: 0; height: 100%; background: rgba(66,133,244,0.15);
                  border-left: 1px solid rgba(66,133,244,0.5); border-right: 1px solid rgba(66,133,244,0.5);
                  display: none; pointer-events: none; z-index: 10; }}
      button {{ margin: 4px 4px 4px 0; padding: 4px 12px; }}
    </style>
  </head>
  <body>
    <div id="chart_container">
      <div id="chart_div"></div>
      <div id="overlay"></div>
    </div>
    <button id="backBtn" style="display:none">Back</button>
    <button id="resetBtn" style="display:none">Reset zoom</button>
    <p id="info"></p>
    <p style="color:#888">Click and drag on the chart to zoom in. Source: {scores_path}</p>
  </body>
</html>
"""
  output_file = f"{output_dir}/{log_base_name}-knnDistanceHistogram.html"
  Path(output_file).write_text(html)
  print(f"Wrote exact NN distance histogram to {output_file}")


def generate_all_distances_histogram(scores_path, output_dir, log_base_name, metric=None, sample_every_n=None):
  """Read the sampled all-distances binary file and generate an HTML histogram.

  The binary format is raw little-endian float32 scores (no per-query structure).
  """
  if not os.path.exists(scores_path):
    print(f"WARNING: all-distances scores file not found: {scores_path}")
    return

  file_size = os.path.getsize(scores_path)
  num_floats = file_size // 4
  if num_floats == 0:
    print("WARNING: all-distances scores file is empty")
    return

  all_scores = struct.unpack(f"<{num_floats}f", Path(scores_path).read_bytes())

  sample_label = ""
  if sample_every_n is not None:
    sample_label = f" (sampled 1-in-{sample_every_n})"

  metric_name, direction = METRIC_LABELS.get(metric or "", ("similarity score", ""))

  min_score = min(all_scores)
  max_score = max(all_scores)

  if min_score == max_score:
    print(f"WARNING: all distances scores are identical ({min_score}); skipping histogram")
    return

  sorted_scores = sorted(all_scores)

  scores_js_lines = []
  chunk_size = 500
  for i in range(0, len(sorted_scores), chunk_size):
    chunk = sorted_scores[i : i + chunk_size]
    scores_js_lines.append(",".join(f"{s:.6f}" for s in chunk))
  scores_js = ",\n".join(scores_js_lines)

  html = f"""<!DOCTYPE html>
<html>
  <head>
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">
      google.load("visualization", "1", {{packages:["corechart"]}});
      google.setOnLoadCallback(init);

      var scores = [
{scores_js}
      ];
      var globalMin = {min_score:.6f};
      var globalMax = {max_score:.6f};
      var curMin = globalMin;
      var curMax = globalMax;
      var chart, chartDiv;
      var zoomStack = [];

      function lowerBound(arr, val) {{
        var lo = 0, hi = arr.length;
        while (lo < hi) {{
          var mid = (lo + hi) >> 1;
          if (arr[mid] < val) lo = mid + 1; else hi = mid;
        }}
        return lo;
      }}

      function buildHistogram(lo, hi, numBins) {{
        var range = hi - lo;
        var binWidth = range / numBins;
        var bins = new Array(numBins).fill(0);
        var startIdx = lowerBound(scores, lo);
        var count = 0;
        for (var i = startIdx; i < scores.length && scores[i] <= hi; i++) {{
          var idx = Math.floor((scores[i] - lo) / binWidth);
          if (idx >= numBins) idx = numBins - 1;
          if (idx < 0) idx = 0;
          bins[idx]++;
          count++;
        }}
        return {{bins: bins, binWidth: binWidth, count: count}};
      }}

      function drawChart(lo, hi) {{
        var numBins = 50;
        var h = buildHistogram(lo, hi, numBins);
        var rows = [['{metric_name} ({direction})', 'Count']];
        for (var i = 0; i < numBins; i++) {{
          var center = lo + (i + 0.5) * h.binWidth;
          rows.push([center, h.bins[i]]);
        }}
        var data = google.visualization.arrayToDataTable(rows);

        var zoomLabel = (lo === globalMin && hi === globalMax) ? '' : ' [zoomed]';
        var options = {{
          title: 'All query x doc distances{sample_label} (' + h.count + ' of {num_floats} scores)' + zoomLabel,
          legend: {{position: 'none'}},
          hAxis: {{
            title: '{metric_name} ({direction})',
            viewWindow: {{min: lo, max: hi}}
          }},
          vAxis: {{title: 'Count'}},
          bar: {{groupWidth: '95%'}},
          chartArea: {{left: 80, right: 20, top: 40, bottom: 60}}
        }};

        chart.draw(data, options);
        document.getElementById('info').innerHTML =
          'Range: ' + lo.toFixed(6) + ' .. ' + hi.toFixed(6) +
          ' &nbsp; Bin width: ' + h.binWidth.toFixed(6) +
          ' &nbsp; Scores in view: ' + h.count;
      }}

      function init() {{
        chartDiv = document.getElementById('chart_div');
        chart = new google.visualization.ColumnChart(chartDiv);
        drawChart(globalMin, globalMax);

        var dragStart = null;
        var overlay = document.getElementById('overlay');

        chartDiv.addEventListener('mousedown', function(e) {{
          var rect = chartDiv.getBoundingClientRect();
          dragStart = {{x: e.clientX - rect.left, clientX: e.clientX}};
          overlay.style.left = dragStart.x + 'px';
          overlay.style.width = '0px';
          overlay.style.display = 'block';
        }});

        chartDiv.addEventListener('mousemove', function(e) {{
          if (!dragStart) return;
          var rect = chartDiv.getBoundingClientRect();
          var curX = e.clientX - rect.left;
          var left = Math.min(dragStart.x, curX);
          var width = Math.abs(curX - dragStart.x);
          overlay.style.left = left + 'px';
          overlay.style.width = width + 'px';
        }});

        chartDiv.addEventListener('mouseup', function(e) {{
          if (!dragStart) return;
          overlay.style.display = 'none';
          var rect = chartDiv.getBoundingClientRect();
          var endX = e.clientX - rect.left;
          var x0 = Math.min(dragStart.x, endX);
          var x1 = Math.max(dragStart.x, endX);
          dragStart = null;

          if (x1 - x0 < 5) return;

          var cli = chart.getChartLayoutInterface();
          var val0 = cli.getHAxisValue(x0);
          var val1 = cli.getHAxisValue(x1);
          if (val0 === null || val1 === null) return;
          var newMin = Math.max(Math.min(val0, val1), globalMin);
          var newMax = Math.min(Math.max(val0, val1), globalMax);
          if (newMax - newMin < (globalMax - globalMin) * 0.001) return;

          zoomStack.push({{min: curMin, max: curMax}});
          curMin = newMin;
          curMax = newMax;
          drawChart(curMin, curMax);
          document.getElementById('resetBtn').style.display = 'inline';
          document.getElementById('backBtn').style.display = 'inline';
        }});

        document.getElementById('resetBtn').addEventListener('click', function() {{
          zoomStack = [];
          curMin = globalMin;
          curMax = globalMax;
          drawChart(curMin, curMax);
          this.style.display = 'none';
          document.getElementById('backBtn').style.display = 'none';
        }});

        document.getElementById('backBtn').addEventListener('click', function() {{
          if (zoomStack.length === 0) return;
          var prev = zoomStack.pop();
          curMin = prev.min;
          curMax = prev.max;
          drawChart(curMin, curMax);
          if (zoomStack.length === 0) {{
            document.getElementById('resetBtn').style.display = 'none';
            this.style.display = 'none';
          }}
        }});
      }}
    </script>
    <style>
      #chart_container {{ position: relative; width: 1200px; height: 600px; }}
      #chart_div {{ width: 100%; height: 100%; }}
      #overlay {{ position: absolute; top: 0; height: 100%; background: rgba(66,133,244,0.15);
                  border-left: 1px solid rgba(66,133,244,0.5); border-right: 1px solid rgba(66,133,244,0.5);
                  display: none; pointer-events: none; z-index: 10; }}
      button {{ margin: 4px 4px 4px 0; padding: 4px 12px; }}
    </style>
  </head>
  <body>
    <div id="chart_container">
      <div id="chart_div"></div>
      <div id="overlay"></div>
    </div>
    <button id="backBtn" style="display:none">Back</button>
    <button id="resetBtn" style="display:none">Reset zoom</button>
    <p id="info"></p>
    <p style="color:#888">Click and drag on the chart to zoom in. Source: {scores_path}</p>
  </body>
</html>
"""
  output_file = f"{output_dir}/{log_base_name}-allDistancesHistogram.html"
  Path(output_file).write_text(html)
  print(f"Wrote all-distances histogram to {output_file}")


def generate_hnsw_traversal_histogram(scores_path, output_dir, log_base_name, metric=None):
  """Read the HNSW traversal scores binary file and generate an HTML histogram.

  The binary format is: for each query, a little-endian int32 count followed
  by that many little-endian float32 scores.
  """
  if not os.path.exists(scores_path):
    print(f"WARNING: HNSW traversal scores file not found: {scores_path}")
    return

  all_scores = []
  total_scores = 0
  data = Path(scores_path).read_bytes()

  offset = 0
  while offset < len(data):
    count = struct.unpack_from("<i", data, offset)[0]
    offset += 4
    scores = struct.unpack_from(f"<{count}f", data, offset)
    offset += count * 4
    total_scores += count
    all_scores.extend(scores[::HNSW_SAMPLE_EVERY_N])

  num_sampled = len(all_scores)
  print(f"HNSW traversal: {total_scores} total scores, sampled 1-in-{HNSW_SAMPLE_EVERY_N} -> {num_sampled} scores for histogram")
  if num_sampled == 0:
    print("WARNING: HNSW traversal scores file is empty")
    return

  metric_name, direction = METRIC_LABELS.get(metric or "", ("similarity score", ""))

  min_score = min(all_scores)
  max_score = max(all_scores)

  if min_score == max_score:
    print(f"WARNING: all HNSW traversal scores are identical ({min_score}); skipping histogram")
    return

  sorted_scores = sorted(all_scores)

  scores_js_lines = []
  chunk_size = 500
  for i in range(0, len(sorted_scores), chunk_size):
    chunk = sorted_scores[i : i + chunk_size]
    scores_js_lines.append(",".join(f"{s:.6f}" for s in chunk))
  scores_js = ",\n".join(scores_js_lines)

  html = f"""<!DOCTYPE html>
<html>
  <head>
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">
      google.load("visualization", "1", {{packages:["corechart"]}});
      google.setOnLoadCallback(init);

      var scores = [
{scores_js}
      ];
      var globalMin = {min_score:.6f};
      var globalMax = {max_score:.6f};
      var curMin = globalMin;
      var curMax = globalMax;
      var chart, chartDiv;
      var zoomStack = [];

      function lowerBound(arr, val) {{
        var lo = 0, hi = arr.length;
        while (lo < hi) {{
          var mid = (lo + hi) >> 1;
          if (arr[mid] < val) lo = mid + 1; else hi = mid;
        }}
        return lo;
      }}

      function buildHistogram(lo, hi, numBins) {{
        var range = hi - lo;
        var binWidth = range / numBins;
        var bins = new Array(numBins).fill(0);
        var startIdx = lowerBound(scores, lo);
        var count = 0;
        for (var i = startIdx; i < scores.length && scores[i] <= hi; i++) {{
          var idx = Math.floor((scores[i] - lo) / binWidth);
          if (idx >= numBins) idx = numBins - 1;
          if (idx < 0) idx = 0;
          bins[idx]++;
          count++;
        }}
        return {{bins: bins, binWidth: binWidth, count: count}};
      }}

      function drawChart(lo, hi) {{
        var numBins = 50;
        var h = buildHistogram(lo, hi, numBins);
        var rows = [['{metric_name} ({direction})', 'Count']];
        for (var i = 0; i < numBins; i++) {{
          var center = lo + (i + 0.5) * h.binWidth;
          rows.push([center, h.bins[i]]);
        }}
        var data = google.visualization.arrayToDataTable(rows);

        var zoomLabel = (lo === globalMin && hi === globalMax) ? '' : ' [zoomed]';
        var options = {{
          title: 'HNSW traversal {metric_name} distribution (' + h.count + ' of {num_sampled} sampled from {total_scores} total, 1-in-{HNSW_SAMPLE_EVERY_N})' + zoomLabel,
          legend: {{position: 'none'}},
          hAxis: {{
            title: '{metric_name} ({direction})',
            viewWindow: {{min: lo, max: hi}}
          }},
          vAxis: {{title: 'Count'}},
          bar: {{groupWidth: '95%'}},
          chartArea: {{left: 80, right: 20, top: 40, bottom: 60}}
        }};

        chart.draw(data, options);
        document.getElementById('info').innerHTML =
          'Range: ' + lo.toFixed(6) + ' .. ' + hi.toFixed(6) +
          ' &nbsp; Bin width: ' + h.binWidth.toFixed(6) +
          ' &nbsp; Scores in view: ' + h.count;
      }}

      function init() {{
        chartDiv = document.getElementById('chart_div');
        chart = new google.visualization.ColumnChart(chartDiv);
        drawChart(globalMin, globalMax);

        var dragStart = null;
        var overlay = document.getElementById('overlay');

        chartDiv.addEventListener('mousedown', function(e) {{
          var rect = chartDiv.getBoundingClientRect();
          dragStart = {{x: e.clientX - rect.left, clientX: e.clientX}};
          overlay.style.left = dragStart.x + 'px';
          overlay.style.width = '0px';
          overlay.style.display = 'block';
        }});

        chartDiv.addEventListener('mousemove', function(e) {{
          if (!dragStart) return;
          var rect = chartDiv.getBoundingClientRect();
          var curX = e.clientX - rect.left;
          var left = Math.min(dragStart.x, curX);
          var width = Math.abs(curX - dragStart.x);
          overlay.style.left = left + 'px';
          overlay.style.width = width + 'px';
        }});

        chartDiv.addEventListener('mouseup', function(e) {{
          if (!dragStart) return;
          overlay.style.display = 'none';
          var rect = chartDiv.getBoundingClientRect();
          var endX = e.clientX - rect.left;
          var x0 = Math.min(dragStart.x, endX);
          var x1 = Math.max(dragStart.x, endX);
          dragStart = null;

          if (x1 - x0 < 5) return;

          var cli = chart.getChartLayoutInterface();
          var val0 = cli.getHAxisValue(x0);
          var val1 = cli.getHAxisValue(x1);
          if (val0 === null || val1 === null) return;
          var newMin = Math.max(Math.min(val0, val1), globalMin);
          var newMax = Math.min(Math.max(val0, val1), globalMax);
          if (newMax - newMin < (globalMax - globalMin) * 0.001) return;

          zoomStack.push({{min: curMin, max: curMax}});
          curMin = newMin;
          curMax = newMax;
          drawChart(curMin, curMax);
          document.getElementById('resetBtn').style.display = 'inline';
          document.getElementById('backBtn').style.display = 'inline';
        }});

        document.getElementById('resetBtn').addEventListener('click', function() {{
          zoomStack = [];
          curMin = globalMin;
          curMax = globalMax;
          drawChart(curMin, curMax);
          this.style.display = 'none';
          document.getElementById('backBtn').style.display = 'none';
        }});

        document.getElementById('backBtn').addEventListener('click', function() {{
          if (zoomStack.length === 0) return;
          var prev = zoomStack.pop();
          curMin = prev.min;
          curMax = prev.max;
          drawChart(curMin, curMax);
          if (zoomStack.length === 0) {{
            document.getElementById('resetBtn').style.display = 'none';
            this.style.display = 'none';
          }}
        }});
      }}
    </script>
    <style>
      #chart_container {{ position: relative; width: 1200px; height: 600px; }}
      #chart_div {{ width: 100%; height: 100%; }}
      #overlay {{ position: absolute; top: 0; height: 100%; background: rgba(66,133,244,0.15);
                  border-left: 1px solid rgba(66,133,244,0.5); border-right: 1px solid rgba(66,133,244,0.5);
                  display: none; pointer-events: none; z-index: 10; }}
      button {{ margin: 4px 4px 4px 0; padding: 4px 12px; }}
    </style>
  </head>
  <body>
    <div id="chart_container">
      <div id="chart_div"></div>
      <div id="overlay"></div>
    </div>
    <button id="backBtn" style="display:none">Back</button>
    <button id="resetBtn" style="display:none">Reset zoom</button>
    <p id="info"></p>
    <p style="color:#888">Click and drag on the chart to zoom in. Source: {scores_path}</p>
    <p style="color:#888">These are all scores computed during HNSW graph traversal, not just the final top-K results.</p>
  </body>
</html>
"""
  output_file = f"{output_dir}/{log_base_name}-hnswTraversalHistogram.html"
  Path(output_file).write_text(html)
  print(f"Wrote HNSW traversal score histogram to {output_file}")


def precompute_exact_nn(values, dim, doc_vectors, query_vectors):
  """Precompute exact nearest neighbors using numpy for all parameter combinations."""
  ndocs = values.get("ndoc", (1000,))
  niters = values.get("niter", (1000,))
  metrics = values.get("metric", ("dot_product",))
  top_ks = values.get("topK", (100,))
  query_start_indices = values.get("queryStartIndex", (0,))
  encodings = values.get("encoding", ("float32",))

  # wrap scalar values
  if not isinstance(ndocs, (tuple, list)):
    ndocs = (ndocs,)
  if not isinstance(niters, (tuple, list)):
    niters = (niters,)
  if not isinstance(metrics, (tuple, list)):
    metrics = (metrics,)
  if not isinstance(top_ks, (tuple, list)):
    top_ks = (top_ks,)
  if not isinstance(query_start_indices, (tuple, list)):
    query_start_indices = (query_start_indices,)
  if not isinstance(encodings, (tuple, list)):
    encodings = (encodings,)

  combos = list(itertools.product(ndocs, niters, metrics, top_ks, query_start_indices, encodings))
  print(f"\nprecomputing exact NN for {len(combos)} parameter combination(s) using numpy...")
  knnExactNN.check_blas_config()
  for ndoc, niter, metric, top_k, query_start_index, encoding in combos:
    knnExactNN.run_one(doc_vectors, query_vectors, dim, ndoc, niter, metric, top_k, query_start_index, encoding)
  print()


def run_knn_benchmark(checkout, values, log_path):
  indexes = [0] * len(values.keys())
  indexes[-1] = -1
  args = []
  # dim = 100
  # doc_vectors = "%s/lucene_util/tasks/enwiki-20120502-lines-1k-100d.vec" % constants.BASE_DIR
  # query_vectors = "%s/lucene_util/tasks/vector-task-100d.vec" % constants.BASE_DIR

  # Cohere Wikipedia en vectors - see cohere-v3-README.txt -- download your copy with "initial_setup.py -download"
  v3 = True

  if v3:
    dim = 1024
    doc_vectors = "/lucenedata/enwiki/cohere-v3/cohere-v3-wikipedia-en-scattered-1024d.docs.vec"
    query_vectors = "/lucenedata/enwiki/cohere-v3/cohere-v3-wikipedia-en-scattered-1024d.queries.vec"
  else:
    dim = 768
    doc_vectors = f"/lucenedata/enwiki/cohere-wikipedia-docs-{dim}d.vec"
    query_vectors = f"/lucenedata/enwiki/cohere-wikipedia-queries-{dim}d.vec"

  # dim = 768
  # doc_vectors = '/lucenedata/enwiki/enwiki-20120502-lines-1k-mpnet.vec'
  # query_vectors = '/lucenedata/enwiki/enwiki-20120502.mpnet.vec'
  # dim = 384
  # doc_vectors = '%s/data/enwiki-20120502-lines-1k-minilm.vec' % constants.BASE_DIR
  # query_vectors = '%s/luceneutil/tasks/vector-task-minilm.vec' % constants.BASE_DIR
  # dim = 300
  # doc_vectors = '%s/data/enwiki-20120502-lines-1k-300d.vec' % constants.BASE_DIR
  # query_vectors = '%s/luceneutil/tasks/vector-task-300d.vec' % constants.BASE_DIR

  # Cohere dataset
  # dim = 768
  # doc_vectors = f"{constants.BASE_DIR}/data/cohere-wikipedia-docs-{dim}d.vec"
  # query_vectors = f"{constants.BASE_DIR}/data/cohere-wikipedia-queries-{dim}d.vec"
  # doc_vectors = f"/lucenedata/enwiki/{'cohere-wikipedia'}-docs-{dim}d.vec"
  # query_vectors = f"/lucenedata/enwiki/{'cohere-wikipedia'}-queries-{dim}d.vec"
  # parentJoin_meta_file = f"{constants.BASE_DIR}/data/{'cohere-wikipedia'}-metadata.csv"

  jfr_output = f"{constants.LOGS_DIR}/knn-perf-test.jfr"

  cp = benchUtil.classPathToString(benchUtil.getClassPath(checkout) + (f"{constants.BENCH_BASE_DIR}/build",))
  cmd = constants.JAVA_EXE.split(" ") + [
    "-cp",
    cp,
    "--add-modules",
    "jdk.incubator.vector",  # no need to add these flags -- they are on by default now?
    "--enable-native-access=ALL-UNNAMED",
    f"-Djava.util.concurrent.ForkJoinPool.common.parallelism={multiprocessing.cpu_count()}",  # so that brute force computeNN uses all cores
    "-XX:+UnlockDiagnosticVMOptions",
    "-XX:+DebugNonSafepoints",
  ]

  if DO_PROFILING:
    cmd += [
      f"-XX:StartFlightRecording=jdk.CPUTimeSample#enabled=true,dumponexit=true,maxsize={constants.JFR_MAX_SIZE_MB}M,settings={constants.BENCH_BASE_DIR}/src/python/profiling.jfc,filename={jfr_output}"
    ]

  cmd += ["knn.KnnGraphTester"]

  if NOISY:
    print_run_summary(values)

  smell_vectors(dim, doc_vectors)
  smell_vectors(dim, query_vectors)

  # precompute exact nearest neighbors using numpy (much faster than Java brute force)
  if USE_NUMPY_EXACT_NN:
    precompute_exact_nn(values, dim, doc_vectors, query_vectors)

  index_run = 1
  all_results = []
  all_simd_reports = []
  log_dir_name, log_file_name = log_path
  if DO_VMSTAT and GNUPLOT_PATH is not None:
    vmstat_index_html_path = f"{log_dir_name}/{log_file_name}-vmstats.html"
    print(f"\nNOTE: open {vmstat_index_html_path} in browser to see CPU/IO telemetry of each run")
    vmstat_index_out = open(vmstat_index_html_path, "w")
    vmstat_index_out.write("<h2>vmstat results for each run</h2>\n\n")
  while advance(indexes, values):
    if NOISY:
      print("\nNEXT:")
    pv = {}
    args = []
    quantize_bits = None
    do_quantize_compress = False
    for i, p in enumerate(values.keys()):
      if values[p]:
        value = values[p][indexes[i]]
        if p == "quantizeBits":
          if value != 32:
            pv[p] = value
            print(f"  -{p}={value}")
            print("  -quantize")
            args += ["-quantize"]
            quantize_bits = value
        elif type(value) is bool:
          if p == "quantizeCompress":
            # carefully only add this flag (below) if we are quantizing to 4 bits:
            do_quantize_compress = True
          elif value:
            args += ["-" + p]
            print(f"  -{p}")
        else:
          print(f"  -{p}={value}")
          pv[p] = value
      else:
        args += ["-" + p]
        print(f"  -{p}")

    if quantize_bits == 4 and do_quantize_compress:
      args += ["-quantizeCompress"]
      print("  -quantizeCompress")

    args += [a for (k, v) in pv.items() for a in ("-" + k, str(v)) if a]

    this_cmd = (
      cmd
      + args
      + [
        "-dim",
        str(dim),
        "-docs",
        doc_vectors,
        # "-reindex",
        "-search-and-stats",
        query_vectors,
        "-numIndexThreads",
        "8",
        # "-metric",
        # "mip",
        # "-parentJoin",
        # parentJoin_meta_file,
        # '-numMergeThread', '8', '-numMergeWorker', '8',
        #'-forceMerge',
        #'-stats',
        #'-quiet'
      ]
    )

    if DO_HNSW_SCORE_HISTOGRAM:
      this_cmd += ["-hnswScoreHistogram"]

    if DO_ALL_DISTANCES_HISTOGRAM:
      this_cmd += ["-allDistancesHistogram", "-allDistancesSampleEveryN", str(ALL_DISTANCES_SAMPLE_EVERY_N)]

    if CONFIRM_SIMD_ASM_MODE:
      perf_data_file = f"perf{index_run}.data"
      print(f"NOTE: adding 'perf record' command, to {perf_data_file}, to sample instructions being executed to later confirm SIMD usage")
      this_cmd = [PERF_EXE, "record", "-m", "2M", "-v", "--call-graph", "lbr", "-e", "instructions:u", "-o", perf_data_file, "-g"] + this_cmd

    perf_stat_simd_file = None
    if DO_PERF_STAT_SIMD:
      perf_stat_simd_file = get_unique_log_name(log_path, "perf-simd").replace(".log", ".txt")
      this_cmd = wrap_cmd_with_perf_stat_simd(this_cmd, perf_stat_simd_file)

    if NOISY:
      print(f"  cmd: {this_cmd}")
    else:
      cmd += ["-quiet"]

    # hint that we will read the vectors files, to get the OS starting on the I/O now:
    vec_size_bytes = dim * 4
    query_start_byte = pv.get("queryStartIndex", 0) * vec_size_bytes
    advise_will_need(query_vectors, query_start_byte, pv.get("niter", 0) * vec_size_bytes)
    if "-reindex" in this_cmd or DO_ALL_DISTANCES_HISTOGRAM:
      advise_will_need(doc_vectors, 0, pv.get("ndoc", 0) * vec_size_bytes)

    if DO_PS:
      # TODO: get k=v into log file name instead of confusing/error-prone 0, 1, 2, ...
      ps_log_file_name = get_unique_log_name(log_path, "ps")
      ps_process = ps_head.PSTopN(1, ps_log_file_name)
      print(f"\nsaving top (ps) processes: {ps_process.cmd}")
    else:
      print("WARNING: top (ps) processes is disabled!")
      ps_process = None

    if DO_VMSTAT:
      vmstat_log_file_name = get_unique_log_name(log_path, "vmstat")
      vmstat_cmd = f"{benchUtil.VMSTAT_PATH} --active --wide --timestamp --unit M 1 > {vmstat_log_file_name} 2>/dev/null &"
      print(f'saving vmstat: "{vmstat_cmd}"\n')
      vmstat_process = subprocess.Popen(vmstat_cmd, shell=True, preexec_fn=os.setsid)
    else:
      print("WARNING: vmstat is disabled!")

    try:
      job = subprocess.Popen(this_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding="utf-8")
      re_summary = re.compile(r"^SUMMARY: (.*?)$", re.MULTILINE)
      re_scores_path = re.compile(r"^EXACT_NN_SCORES_PATH: (.+)$")
      re_nn_metric = re.compile(r"^EXACT_NN_METRIC: (.+)$")
      re_hnsw_scores_path = re.compile(r"^HNSW_TRAVERSAL_SCORES_PATH: (.+)$")
      re_hnsw_metric = re.compile(r"^HNSW_TRAVERSAL_METRIC: (.+)$")
      re_all_distances_path = re.compile(r"^ALL_DISTANCES_SCORES_PATH: (.+)$")
      re_all_distances_metric = re.compile(r"^ALL_DISTANCES_METRIC: (.+)$")
      re_all_distances_sample = re.compile(r"^ALL_DISTANCES_SAMPLE_EVERY_N: (.+)$")
      summary = None
      exact_nn_scores_path = None
      exact_nn_metric = None
      hnsw_traversal_scores_path = None
      hnsw_traversal_metric = None
      all_distances_scores_path = None
      all_distances_metric = None
      all_distances_sample_every_n = None
      hit_exception = False
      while job.poll() is None:
        line = job.stdout.readline()
        if not line:
          continue
        if NOISY:
          sys.stdout.write(line)
          sys.stdout.flush()
        m = re_summary.match(line)
        if m is not None:
          summary = m.group(1)
        m = re_scores_path.match(line)
        if m is not None:
          exact_nn_scores_path = m.group(1).strip()
        m = re_nn_metric.match(line)
        if m is not None:
          exact_nn_metric = m.group(1).strip()
        m = re_hnsw_scores_path.match(line)
        if m is not None:
          hnsw_traversal_scores_path = m.group(1).strip()
        m = re_hnsw_metric.match(line)
        if m is not None:
          hnsw_traversal_metric = m.group(1).strip()
        m = re_all_distances_path.match(line)
        if m is not None:
          all_distances_scores_path = m.group(1).strip()
        m = re_all_distances_metric.match(line)
        if m is not None:
          all_distances_metric = m.group(1).strip()
        m = re_all_distances_sample.match(line)
        if m is not None:
          all_distances_sample_every_n = int(m.group(1).strip())
        if "Exception in" in line:
          hit_exception = True
    finally:
      if DO_PS:
        print("now stop ps process...")
        ps_process.stop()

      if DO_VMSTAT:
        print(f"now stop vmstat (pid={vmstat_process.pid})...")
        # TODO: messy!  can we get process group working so we can kill bash and its child reliably?
        subprocess.check_call(["pkill", "-u", benchUtil.get_username(), "vmstat"])
        if vmstat_process.poll() is None:
          raise RuntimeError("failed to kill vmstat child process?  pid={vmstat_process.pid}")

    if DO_VMSTAT and GNUPLOT_PATH is not None:
      vmstat_subdir_name = write_vmstat_pretties(vmstat_log_file_name, this_cmd)
      str_this_cmd = shlex.join(this_cmd)
      # each run creates a new subdir with the N (cpu, io, memory, ...) charts
      vmstat_index_out.write(f'\n<a href="{vmstat_subdir_name}/index.html"><tt>run {index_run}</tt>: <tt>{str_this_cmd}</tt></a><br><br>\n')

    if hit_exception:
      raise RuntimeError("unhandled java exception while running")
    job.wait()
    if job.returncode != 0:
      raise RuntimeError(f"command failed with exit {job.returncode}")
    if summary is None:
      raise RuntimeError("could not find summary line in output! ")

    if exact_nn_scores_path is not None:
      generate_exact_nn_histogram(exact_nn_scores_path, log_dir_name, log_file_name, exact_nn_metric)

    if hnsw_traversal_scores_path is not None:
      generate_hnsw_traversal_histogram(hnsw_traversal_scores_path, log_dir_name, log_file_name, hnsw_traversal_metric)

    if all_distances_scores_path is not None:
      generate_all_distances_histogram(all_distances_scores_path, log_dir_name, log_file_name, all_distances_metric, all_distances_sample_every_n)

    all_results.append((summary, args))
    if DO_PROFILING:
      benchUtil.profilerOutput(constants.JAVA_EXE, jfr_output, benchUtil.checkoutToPath(checkout), 30, (1, 4, 12))

    if perf_stat_simd_file is not None:
      counters = parse_perf_stat_file(perf_stat_simd_file)
      is_quant = quantize_bits is not None and quantize_bits != 32
      report, dominant = format_simd_report(counters, is_quantized=is_quant)
      all_simd_reports.append((index_run, report, dominant))
      print(f"  run {index_run} {report}")
      if dominant in ("FP-scalar", "none"):
        print(f"  raw perf stat output ({perf_stat_simd_file}):")
        try:
          for line in Path(perf_stat_simd_file).read_text().splitlines():
            print(f"    {line}")
        except OSError as e:
          print(f"    (could not read: {e})")

    index_run += 1

  if NOISY:
    print("\nResults:")

  skip_headers = set()

  # skip columns that have the same value for every row
  if len(all_results) > 1:
    for col in range(len(OUTPUT_HEADERS)):
      unique_values = set([result[0].split("\t")[col] for result in all_results])
      if len(unique_values) == 1:
        skip_headers.add(OUTPUT_HEADERS[col])
        print(f"NOTE: {OUTPUT_HEADERS[col]} = {unique_values.pop()} for all runs; skipping column")

  print_fixed_width(all_results, skip_headers)
  print_chart(all_results)

  # SIMD summary across all runs
  if all_simd_reports:
    print("\nSIMD validation summary:")
    for run_num, report, dominant in all_simd_reports:
      print(f"  run {run_num}: {report}")
    # warn if any run had scalar-dominant or no SIMD
    bad_runs = [(r, d) for r, _, d in all_simd_reports if d in ("FP-scalar", "none")]
    if bad_runs:
      print(f"\n  WARNING: {len(bad_runs)} run(s) without SIMD vectorization: runs {', '.join(str(r) for r, _ in bad_runs)}")
      print("  Check that JVM is using --add-modules jdk.incubator.vector and that the Lucene")
      print("  codec supports vectorized similarity functions for your metric/encoding.")
    else:
      levels = {d for _, _, d in all_simd_reports}
      print(f"\n  All {len(all_simd_reports)} run(s) used SIMD: {', '.join(sorted(levels))}")

  if DO_VMSTAT and GNUPLOT_PATH is not None:
    print(f"\nNOTE: open {vmstat_index_html_path} in browser to see CPU/IO telemetry of each run")
  return all_results, skip_headers


def write_vmstat_pretties(vmstat_log_file_name, full_cmd):
  # print(f"write vmstat pretties from log={vmstat_log_file_name}")

  vmstat_log_path = Path(vmstat_log_file_name)
  dir_name = vmstat_log_path.parent
  base_name = vmstat_log_path.stem
  ext = vmstat_log_path.suffix

  vmstat_dir_name = f"{dir_name}/{base_name}-vmstat-charts"
  job_index_html_file = f"{vmstat_dir_name}/index.html"

  # print(f"see {vmstat_dir_name}/index.html for vmstat visualization")
  os.mkdir(vmstat_dir_name)

  # our own little pushd/popd!
  cwd = os.getcwd()
  try:
    # TODO: optimize to single shared copy!
    # TODO: don't hardwire version / full path to this js file!
    shutil.copy("/usr/share/gnuplot/6.0/js/gnuplot_svg.js", vmstat_dir_name)
    shutil.copy(f"{constants.BENCH_BASE_DIR}/src/vmstat/index.html.template", f"{vmstat_dir_name}/index.html")

    # because gnuplot needs to be in this directory (?)
    # the gnuplot script (src/vmstat/vmstat.gpi) writes output to ".":
    # print(f"cd {vmstat_dir_name=}")
    os.chdir(dir_name)
    subprocess.check_call(f"{GNUPLOT_PATH} -c {constants.BENCH_BASE_DIR}/src/vmstat/vmstat.gpi {vmstat_log_file_name} {base_name}-vmstat-charts", shell=True)
  finally:
    os.chdir(cwd)

  return os.path.split(vmstat_dir_name)[1]


def print_fixed_width(all_results, columns_to_skip):
  header = "\t".join(OUTPUT_HEADERS)

  # crazy logic to make everything fixed width so rendering in fixed width font "aligns":
  headers = header.split("\t")
  num_columns = len(headers)
  # print(f'{num_columns} columns')
  max_by_col = [0] * num_columns

  rows_to_print = [header] + [result[0] for result in all_results]

  skip_column_index = {headers.index(h) for h in columns_to_skip}

  for row in rows_to_print:
    by_column = row.split("\t")
    if len(by_column) != num_columns:
      raise RuntimeError(f'wrong number of columns: expected {num_columns} but got {len(by_column)} in row "{row}"')
    for i, s in enumerate(by_column):
      max_by_col[i] = max(max_by_col[i], len(s))

  row_fmt = "  ".join([f"%{max_by_col[i]}s" for i in range(num_columns) if i not in skip_column_index])
  # print(f'using row format {row_fmt}')

  for row in rows_to_print:
    cols = row.split("\t")
    cols = tuple(cols[x] for x in range(len(cols)) if x not in skip_column_index)
    print(row_fmt % cols)


def arglist_to_argmap(arglist):
  """Map args starting with - to the next value in the list unless it starts with -
  in which case map to the empty string
  """
  argmap = dict()
  for i in range(len(arglist)):
    if arglist[i][0] == "-":
      if i < len(arglist) - 1 and arglist[i + 1][0] != "-":
        argmap[arglist[i]] = arglist[i + 1]
      else:
        argmap[arglist[i]] = ""
  return argmap


def remove_common_args(argmaps):
  common_args = argmaps[0].copy()
  for args in argmaps[1:]:
    for k in list(common_args.keys()):
      if k not in args or args[k] != common_args[k]:
        del common_args[k]
  # don't use fanout as a dimension in a data series label
  # TODO: also remove other "minor" dimensions such as beam_width and maxconn?
  # or place under user control somehow
  common_args["-fanout"] = 1
  # everything remaining is in common to all rows, now remove them
  unique_args = []
  for args in argmaps:
    ua = dict()
    for k, v in args.items():
      if k not in common_args:
        ua[k] = v
    unique_args.append(ua)
  return unique_args


CHART_HEADER = """
<!DOCTYPE html>
<html>
  <head>
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">
      google.load("visualization", "1", {packages:["corechart"]});
      google.setOnLoadCallback(drawChart);
      function drawChart() {
        var data = google.visualization.arrayToDataTable([
"""

CHART_FOOTER = """        ]);

        var options = {
          //title: '$TITLE$',
          pointSize: 5,
          //legend: {position: 'none'},
          hAxis: {title: 'Recall'},
          vAxis: {title: 'CPU (msec)', direction: -1},
          interpolateNulls: true
        };

        var chart = new google.visualization.LineChart(document.getElementById('chart_div'));
        chart.draw(data, options);
      }
    </script>
  </head>
  <body>
    <div id="chart_div" style="width: 1200px; height: 600px;"></div>
  </body>
</html>
"""


def print_chart(results):
  # (recall, nCpu) for each result
  argmaps = [arglist_to_argmap(r[1]) for r in results]
  argmaps = remove_common_args(argmaps)
  # TODO: also remove "minor" args that may vary the performance but are not shown on the axes
  # and then show the corresponding value(s) in the tooltips
  output = CHART_HEADER
  labels = dict()
  for argmap in argmaps:
    label = chart_args_label(argmap)
    if label in labels:
      index = labels[label]
    else:
      index = len(labels)
      labels[label] = index

  output += str([""] + list(labels.keys()))
  output += ",\n"

  for i, row in enumerate(results):
    values = row[0].split("\t")
    label = chart_args_label(argmaps[i])
    index = labels[label]
    # recall on x axis, cpu on y axis. nulls for the other label indices
    data_row = [float(values[0])] + ["null"] * index + [float(values[2])] + ["null"] * (len(labels) - index - 1)
    output += str(data_row).replace("'null'", "null")
    output += ",\n"

  output += CHART_FOOTER
  with open("knnPerfChart.html", "w") as fout:
    print(output, file=fout)


def chart_args_label(args):
  if len(args) == 0:
    return "baseline"
  return str(args)


def print_cpu_info():
  """Read and print CPU information from /proc/cpuinfo if present (linux only)"""
  cpuinfo_path = "/proc/cpuinfo"

  if not os.path.exists(cpuinfo_path):
    print("CPU info: /proc/cpuinfo not found (not running on Linux)")
    return

  # parse /proc/cpuinfo - it repeats info for each logical core
  cpu_info = {}
  processor_count = 0

  with open(cpuinfo_path) as f:
    for line in f:
      line = line.strip()
      if not line:
        continue

      if ":" in line:
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()

        # count processors
        if key == "processor":
          processor_count += 1

        # capture these fields (they repeat for each core, so we only need one)
        elif key == "model name" and "model_name" not in cpu_info:
          cpu_info["model_name"] = value
        elif key == "cpu cores" and "cpu_cores" not in cpu_info:
          cpu_info["cpu_cores"] = value
        elif key == "microcode" and "microcode" not in cpu_info:
          cpu_info["microcode"] = value
        elif key == "flags" and "flags" not in cpu_info:
          cpu_info["flags"] = value
        elif key == "vmx flags" and "vmx_flags" not in cpu_info:
          cpu_info["vmx_flags"] = value
        elif key == "bugs" and "bugs" not in cpu_info:
          cpu_info["bugs"] = value

  # print cpu information
  print("\nCPU Information:")
  print(f"  model: {cpu_info.get('model_name', 'unknown')}")
  print(f"  logical cores: {processor_count}")
  print(f"  physical cores per socket: {cpu_info.get('cpu_cores', 'unknown')}")
  print(f"  microcode: {cpu_info.get('microcode', 'unknown')}")

  flags = cpu_info.get("flags", "")
  if flags:
    print(f"  flags: {flags}")
  else:
    print("  flags: unknown")

  vmx_flags = cpu_info.get("vmx_flags", "")
  if vmx_flags:
    print(f"  vmx flags: {vmx_flags}")
  else:
    print("  vmx flags: none")

  bugs = cpu_info.get("bugs", "")
  if bugs:
    print(f"  bugs: {bugs}")
  else:
    print("  bugs: none")

  print()


def format_memory_kb(value_str):
  """Convert memory value from kB to appropriate units (kB, MB, GB, TB)"""
  try:
    # parse value (format is like "65916396 kB")
    parts = value_str.split()
    if len(parts) < 1:
      return value_str

    value_kb = int(parts[0])

    # choose appropriate unit
    if value_kb >= 1024 * 1024 * 1024:  # >= 1 TB
      return f"{value_kb / (1024 * 1024 * 1024):.2f} TB"
    if value_kb >= 1024 * 1024:  # >= 1 GB
      return f"{value_kb / (1024 * 1024):.2f} GB"
    if value_kb >= 1024:  # >= 1 MB
      return f"{value_kb / 1024:.2f} MB"
    return f"{value_kb} kB"
  except (ValueError, IndexError):
    return value_str


def print_mem_info():
  """Read and print memory information from /proc/meminfo if present (linux only)"""
  meminfo_path = "/proc/meminfo"

  if not os.path.exists(meminfo_path):
    print("Memory info: /proc/meminfo not found (not running on Linux)")
    return

  mem_info = {}

  with open(meminfo_path) as f:
    for line in f:
      line = line.strip()
      if not line:
        continue

      if ":" in line:
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()

        # capture memory fields
        if key == "MemTotal":
          mem_info["mem_total"] = value
        elif key == "MemFree":
          mem_info["mem_free"] = value
        elif key == "MemAvailable":
          mem_info["mem_available"] = value
        elif key == "Dirty":
          mem_info["mem_dirty"] = value

  # print memory information
  print("Memory Information:")

  mem_total = mem_info.get("mem_total", "unknown")
  mem_free = mem_info.get("mem_free", "unknown")
  mem_available = mem_info.get("mem_available", "unknown")
  mem_dirty = mem_info.get("mem_dirty", "unknown")

  print(f"  total RAM: {format_memory_kb(mem_total) if mem_total != 'unknown' else 'unknown'}")
  print(f"  free RAM: {format_memory_kb(mem_free) if mem_free != 'unknown' else 'unknown'}")
  print(f"  available RAM: {format_memory_kb(mem_available) if mem_available != 'unknown' else 'unknown'}")

  # calculate used ram if we have total and available
  if "mem_total" in mem_info and "mem_available" in mem_info:
    try:
      # parse values (they come in format like "65916396 kB")
      total_kb = int(mem_info["mem_total"].split()[0])
      available_kb = int(mem_info["mem_available"].split()[0])
      used_kb = total_kb - available_kb
      print(f"  used RAM: {format_memory_kb(f'{used_kb} kB')}")
    except (ValueError, IndexError):
      print("  used RAM: unknown")
  else:
    print("  used RAM: unknown")

  print(f"  dirty RAM: {format_memory_kb(mem_dirty) if mem_dirty != 'unknown' else 'unknown'}")
  print()


def check_knn_compiled():
  """Hard exit if KNN Java classes are missing or out of date vs source files."""
  build_dir = Path(constants.BENCH_BASE_DIR) / "build"
  src_dir = Path(constants.BENCH_BASE_DIR) / "src" / "main"

  marker = build_dir / "knn" / "KnnGraphTester.class"

  source_files = list((src_dir / "knn").glob("*.java"))
  source_files.extend([src_dir / "WikiVectors.java", src_dir / "perf" / "VectorDictionary.java"])

  gradle_cmd = "  JAVA_HOME=/usr/lib/jvm/java-25-openjdk ./gradlew compileKnn"

  if not marker.exists():
    print(f"\nERROR: {marker} does not exist. Run:\n\n{gradle_cmd}\n")
    raise SystemExit(1)

  marker_mtime = marker.stat().st_mtime
  stale = [src for src in source_files if src.exists() and src.stat().st_mtime > marker_mtime]

  if len(stale) > 0:
    print("\nERROR: KNN Java classes are out of date. Stale source files:")
    for src in stale:
      print(f"  {src}")
    print(f"\nRun:\n\n{gradle_cmd}\n")
    raise SystemExit(1)

  lucene_checkout = getLuceneDirFromGradleProperties()
  lucene_path = benchUtil.checkoutToPath(lucene_checkout)
  lucene_gradle_cmd = f"  JAVA_HOME=/usr/lib/jvm/java-25-openjdk ./gradlew compileJava\n  (in {lucene_path})"

  try:
    cp_entries = benchUtil.getClassPath(lucene_checkout)
  except RuntimeError as e:
    print(f"\nERROR: Lucene checkout at {lucene_path} is missing built artifacts: {e}\n\nRun:\n\n{lucene_gradle_cmd}\n")
    raise SystemExit(1) from e

  for entry in cp_entries:
    p = Path(entry)
    if not str(p).startswith(lucene_path):
      continue
    if not p.exists():
      print(f"\nERROR: Lucene classpath entry missing: {p}\n\nRun:\n\n{lucene_gradle_cmd}\n")
      raise SystemExit(1)


def build_java_base_cmd(checkout):
  """Build the base Java command (JVM flags + classpath) for KnnGraphTester."""
  cp = benchUtil.classPathToString(benchUtil.getClassPath(checkout) + (f"{constants.BENCH_BASE_DIR}/build",))
  cmd = constants.JAVA_EXE.split(" ") + [
    "-cp",
    cp,
    "--add-modules",
    "jdk.incubator.vector",
    "--enable-native-access=ALL-UNNAMED",
    f"-Djava.util.concurrent.ForkJoinPool.common.parallelism={multiprocessing.cpu_count()}",
    "-XX:+UnlockDiagnosticVMOptions",
    "-XX:+DebugNonSafepoints",
  ]
  cmd += ["knn.KnnGraphTester"]
  return cmd


def build_knn_args_from_params(params):
  """Convert a flat dict of param_name->value into the arg list for KnnGraphTester."""
  args = []
  quantize_bits = None
  do_quantize_compress = False
  for p, value in params.items():
    if p == "quantizeBits":
      if value != 32:
        args += ["-quantize", "-quantizeBits", str(value)]
        quantize_bits = value
    elif p == "quantizeCompress":
      do_quantize_compress = value
    elif isinstance(value, bool):
      if value:
        args += ["-" + p]
    else:
      args += ["-" + p, str(value)]

  if quantize_bits == 4 and do_quantize_compress:
    args += ["-quantizeCompress"]

  return args


def run_single_knn_iteration(checkout, params, dim, doc_vectors, query_vectors, work_dir, extra_java_args=None):
  """Run a single KNN benchmark iteration in work_dir.  Always reindexes.

  params: flat dict of param_name -> single value (not tuple)
  Returns (summary_string, full_output_string) or raises on failure.
  """
  base_cmd = build_java_base_cmd(checkout)
  knn_args = build_knn_args_from_params(params)

  full_cmd = (
    base_cmd
    + knn_args
    + [
      "-dim",
      str(dim),
      "-docs",
      str(doc_vectors),
      "-reindex",
      "-search-and-stats",
      str(query_vectors),
      "-numIndexThreads",
      "8",
    ]
  )

  if extra_java_args is not None:
    full_cmd += extra_java_args

  perf_stat_simd_file = None
  if DO_PERF_STAT_SIMD:
    perf_stat_simd_file = str(Path(work_dir) / "perf-simd.txt")
    full_cmd = wrap_cmd_with_perf_stat_simd(full_cmd, perf_stat_simd_file)

  print(f"[variance] running in {work_dir}")
  print(f"[variance] cmd: {full_cmd}")

  os.makedirs(work_dir, exist_ok=True)

  job = subprocess.Popen(
    full_cmd,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    encoding="utf-8",
    cwd=str(work_dir),
  )

  output_lines = []
  re_summary = re.compile(r"^SUMMARY: (.*?)$", re.MULTILINE)
  summary = None
  hit_exception = False

  while job.poll() is None:
    line = job.stdout.readline()
    if not line:
      continue
    output_lines.append(line)
    sys.stdout.write(line)
    sys.stdout.flush()
    m = re_summary.match(line)
    if m is not None:
      summary = m.group(1)
    if "Exception in" in line:
      hit_exception = True

  # drain remaining output
  for line in job.stdout:
    output_lines.append(line)
    sys.stdout.write(line)
    sys.stdout.flush()
    m = re_summary.match(line)
    if m is not None:
      summary = m.group(1)
    if "Exception in" in line:
      hit_exception = True

  full_output = "".join(output_lines)

  if hit_exception:
    raise RuntimeError(f"java exception in {work_dir}:\n{full_output}")
  job.wait()
  if job.returncode != 0:
    raise RuntimeError(f"command failed with exit {job.returncode} in {work_dir}:\n{full_output}")
  if summary is None:
    raise RuntimeError(f"could not find SUMMARY line in output from {work_dir}:\n{full_output}")

  if perf_stat_simd_file is not None:
    counters = parse_perf_stat_file(perf_stat_simd_file)
    is_quant = params.get("quantizeBits", 32) != 32
    report, dominant = format_simd_report(counters, is_quantized=is_quant)
    print(f"  {report}")
    if dominant in ("scalar", "none"):
      print(f"  raw perf stat output ({perf_stat_simd_file}):")
      try:
        for line in Path(perf_stat_simd_file).read_text().splitlines():
          print(f"    {line}")
      except OSError as e:
        print(f"    (could not read: {e})")

  return summary, full_output


def run_n_knn_benchmarks(LUCENE_CHECKOUT, PARAMS, n, log_path):
  rec, lat, net, avg = [], [], [], []
  tests = []
  for i in range(n):
    results, skip_headers = run_knn_benchmark(LUCENE_CHECKOUT, PARAMS, log_path)
    tests.append(results)
    first_4_numbers = results[0][0].split("\t")[:4]
    first_4_numbers = [float(num) for num in first_4_numbers]

    # store relevant data points
    rec.append(first_4_numbers[0])
    lat.append(first_4_numbers[1])
    net.append(first_4_numbers[2])
    avg.append(first_4_numbers[3])

  # reconstruct string with median results
  med_results = []
  med_string = ""
  med_string += f"{round(statistics.median(rec), 3)}\t"
  med_string += f"{round(statistics.median(lat), 3)}\t"
  med_string += f"{round(statistics.median(net), 3)}\t"
  med_string += f"{round(statistics.median(avg), 3)}\t"

  split_results = results[0][0].split("\t")
  split_string = "\t".join(split_results[4:])
  med_string += split_string
  med_tuple = (med_string, results[0][1])
  med_results.append(med_tuple)

  # re-print all tables in a row
  print("\nFinal Results:")
  for i in range(n):
    print(f"\nTest {i + 1}:")
    print_fixed_width(tests[i], skip_headers)

  # print median results in table
  print("\nMedian Results:")
  print_chart(med_results)
  print_fixed_width(med_results, skip_headers)


if __name__ == "__main__":
  with autologger.capture_output() as log_path:
    log_path = Path(log_path)
    log_dir_name = log_path.parent
    log_base_name = log_path.stem
    log_ext = log_path.suffix

    # print cpu and memory information at the start
    print_cpu_info()
    print_mem_info()

    parser = argparse.ArgumentParser(description="Run KNN benchmarks")
    parser.add_argument("--runs", type=int, default=1, help="Number of times to run the benchmark (default: 1)")
    n = parser.parse_args()

    check_knn_compiled()

    # Where the version of Lucene is that will be tested. Now this will be sourced from gradle.properties
    LUCENE_CHECKOUT = getLuceneDirFromGradleProperties()
    if n.runs == 1:
      run_knn_benchmark(LUCENE_CHECKOUT, PARAMS, (log_dir_name, log_base_name))
    else:
      run_n_knn_benchmarks(LUCENE_CHECKOUT, PARAMS, n.runs, (log_dir_name, log_base_name))
