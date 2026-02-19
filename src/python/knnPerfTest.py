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
import math
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

import autologger
import benchUtil
import constants
import ps_head
from benchUtil import GNUPLOT_PATH, PERF_EXE
from common import getLuceneDirFromGradleProperties

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

# TODO: support async prorfiler (it can write JFRs as well so it'd fit right in with our simple JFR summarizer output)
# TODO: also support new CPU time profiler (no more sleepy state bias?) in JDK 25 here: https://mostlynerdless.de/blog/2025/06/11/java-25s-new-cpu-time-profiler-1/
DO_PROFILING = False
DO_PS = True
DO_VMSTAT = True

# Set this to True to use perf tool to record instructions executed and confirm SIMD
# instructions were executed
# TODO: how much overhead / perf impact from this?  can we always run?
CONFIRM_SIMD_ASM_MODE = False

# set this to True to collect all HNSW traversal scores and generate a histogram
DO_HNSW_SCORE_HISTOGRAM = True

# sample 1 in every N HNSW traversal scores when building the histogram (to keep HTML size reasonable)
HNSW_SAMPLE_EVERY_N = 100

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
  "ndoc": (400_000,),
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
  "quantizeBits": (8,),
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
}


OUTPUT_HEADERS = [
  "recall",
  "latency(ms)",
  "netCPU",
  "avgCpuCount",
  "nDoc",
  "topK",
  "fanout",
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


def smell_vectors(dim, file_name, do_check_norms=True):
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

  if do_check_norms:
    struct_fmt = f"<{dim}f"

    with open(file_name, "rb") as f:
      # sanity check
      t0 = time.time()
      checked_count = 0
      not_norm_count = 0
      for i in range(100):
        vec_idx = random.randint(0, num_vectors - 1)
        f.seek(vec_idx * vec_size_bytes)
        b = f.read(vec_size_bytes)
        one_vec = struct.unpack(struct_fmt, b)

        sumsq = 0
        for i, v in enumerate(one_vec):
          # print(f"  {i:4d}: {v:g}")
          sumsq += v * v
        norm_euclidean_length = math.sqrt(sumsq)

        if not math.isclose(norm_euclidean_length, 1.0, rel_tol=0.0001, abs_tol=0.0001):
          # not normalized
          print(f'WARNING: vec {vec_idx} in "{file_name}" has norm={norm_euclidean_length} (not normalized)')
          not_norm_count += 1

        t1 = time.time()
        checked_count += 1
        # print(f"  {t1-t0:.1f}: vec[vec_idx] length is {norm_euclidean_length}")
        if t1 - t0 > 1.0 and i >= 10:
          # spend at most 1 second checking, but check at least 10 vectors
          break

      if not_norm_count:
        print(f'WARNING: dimension or vector file name might be wrong?  {not_norm_count} of {checked_count} randomly checked vectors are not normalized in "{file_name}"')


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
      options.append(f"  {key:<{max_key_len}s}: {','.join(value)}")
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

  with open(scores_path, "rb") as f:
    all_scores = struct.unpack(f"<{num_floats}f", f.read())

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
  with open(output_file, "w") as f:
    f.write(html)
  print(f"Wrote exact NN distance histogram to {output_file}")


def generate_hnsw_traversal_histogram(scores_path, output_dir, log_base_name, metric=None):
  """Read the HNSW traversal scores binary file and generate an HTML histogram.

  The binary format is: for each query, a little-endian int32 count followed
  by that many little-endian float32 scores."""
  if not os.path.exists(scores_path):
    print(f"WARNING: HNSW traversal scores file not found: {scores_path}")
    return

  all_scores = []
  total_scores = 0
  with open(scores_path, "rb") as f:
    data = f.read()

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
  with open(output_file, "w") as f:
    f.write(html)
  print(f"Wrote HNSW traversal score histogram to {output_file}")


def run_knn_benchmark(checkout, values, log_path):
  indexes = [0] * len(values.keys())
  indexes[-1] = -1
  args = []
  # dim = 100
  # doc_vectors = "%s/lucene_util/tasks/enwiki-20120502-lines-1k-100d.vec" % constants.BASE_DIR
  # query_vectors = "%s/lucene_util/tasks/vector-task-100d.vec" % constants.BASE_DIR

  do_check_norms = True

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
    cmd += [f"-XX:StartFlightRecording=dumponexit=true,maxsize=250M,settings={constants.BENCH_BASE_DIR}/src/python/profiling.jfc" + f",filename={jfr_output}"]

  cmd += ["knn.KnnGraphTester"]

  if NOISY:
    print_run_summary(values)

  if NOISY:
    print("smell vectors...")

  smell_vectors(dim, doc_vectors, do_check_norms)
  smell_vectors(dim, query_vectors, do_check_norms)

  index_run = 1
  all_results = []
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

    if CONFIRM_SIMD_ASM_MODE:
      perf_data_file = f"perf{index_run}.data"
      print(f"NOTE: adding 'perf record' command, to {perf_data_file}, to sample instructions being executed to later confirm SIMD usage")
      this_cmd = [PERF_EXE, "record", "-m", "2M", "-v", "--call-graph", "lbr", "-e", "instructions:u", "-o", perf_data_file, "-g"] + this_cmd

    if NOISY:
      print(f"  cmd: {this_cmd}")
    else:
      cmd += ["-quiet"]

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
      summary = None
      exact_nn_scores_path = None
      exact_nn_metric = None
      hnsw_traversal_scores_path = None
      hnsw_traversal_metric = None
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

    all_results.append((summary, args))
    if DO_PROFILING:
      benchUtil.profilerOutput(constants.JAVA_EXE, jfr_output, benchUtil.checkoutToPath(checkout), 30, (1,))
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

    # Where the version of Lucene is that will be tested. Now this will be sourced from gradle.properties
    LUCENE_CHECKOUT = getLuceneDirFromGradleProperties()
    if n.runs == 1:
      run_knn_benchmark(LUCENE_CHECKOUT, PARAMS, (log_dir_name, log_base_name))
    else:
      run_n_knn_benchmarks(LUCENE_CHECKOUT, PARAMS, n.runs, (log_dir_name, log_base_name))
