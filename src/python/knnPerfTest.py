#!/usr/bin/env/python

# TODO
#   - hmm what is "normalized" boolean at KNN indexing time? -- COSINE similarity sets this to true
#   - try turning diversity off -- faster forceMerge?  better recall?
#   - why force merge 12X slower
#   - why only one thread
#   - report net concurrency utilized in the table

import multiprocessing
import re
import subprocess
import sys

import benchUtil
import constants
from common import getLuceneDirFromGradleProperties

# Measure vector search recall and latency while exploring hyperparameters

# SETUP:
### Download and extract data files: Wikipedia line docs + GloVe
# python src/python/setup.py -download
# cd ../data
# unzip glove.6B.zip
# unlzma enwiki-20120502-lines-1k.txt.lzma
### Create document and task vectors
# ./gradlew vectors-100
#
# change the parameters below and then run (you can still manually run this file, but using gradle command
# below will auto recompile if you made any changes to java files in luceneutils)
# ./gradlew runKnnPerfTest
#
# you may want to modify the following settings:


# nocommit
DO_PROFILING = False

# e.g. to compile KnnIndexer:
#
#   javac -d build -cp /l/trunk/lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar:/l/trunk/lucene/join/build/libs/lucene-join-10.0.0-SNAPSHOT.jar src/main/knn/*.java src/main/WikiVectors.java src/main/perf/VectorDictionary.java
#

# TODO
#  - can we expose greediness (global vs local queue exploration in KNN search) here?

# test parameters. This script will run KnnGraphTester on every combination of these parameters
PARAMS = {
  "ndoc": (10_000_000,),
  #'ndoc': (10000, 100000, 200000, 500000),
  #'ndoc': (10000, 100000, 200000, 500000),
  #'ndoc': (2_000_000,),
  #'ndoc': (1_000_000,),
  #'ndoc': (50_000,),
  #'maxConn': (32, 64, 96),
  "maxConn": (32,),
  #'maxConn': (32,),
  #'beamWidthIndex': (250, 500),
  "beamWidthIndex": (100,),
  #'beamWidthIndex': (50,),
  #'fanout': (20, 100, 250)
  "fanout": (50,),
  #'quantize': None,
  #'quantizeBits': (32, 7, 4),
  "numMergeWorker": (12,),
  "numMergeThread": (4,),
  #'numMergeWorker': (1,),
  #'numMergeThread': (1,),
  "encoding": ("float32",),
  # 'metric': ('angular',),  # default is angular (dot_product)
  # 'metric': ('mip',),
  #'quantize': (True,),
  "quantizeBits": (32,),
  #'fanout': (0,),
  "topK": (100,),
  "bp": ("false", "true"),
  #'quantizeCompress': (True, False),
  "quantizeCompress": (True,),
  "queryStartIndex": (0,),  # seek to this start vector before searching, to sample different vectors
  "forceMerge": (True, False),
  #'niter': (10,),
}


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


def run_knn_benchmark(checkout, values):
  indexes = [0] * len(values.keys())
  indexes[-1] = -1
  args = []
  # dim = 100
  # doc_vectors = constants.GLOVE_VECTOR_DOCS_FILE
  # query_vectors = '%s/luceneutil/tasks/vector-task-100d.vec' % constants.BASE_DIR
  # dim = 768
  # doc_vectors = '/lucenedata/enwiki/enwiki-20120502-lines-1k-mpnet.vec'
  # query_vectors = '/lucenedata/enwiki/enwiki-20120502.mpnet.vec'
  # dim = 384
  # doc_vectors = '%s/data/enwiki-20120502-lines-1k-minilm.vec' % constants.BASE_DIR
  # query_vectors = '%s/luceneutil/tasks/vector-task-minilm.vec' % constants.BASE_DIR
  # dim = 300
  # doc_vectors = '%s/data/enwiki-20120502-lines-1k-300d.vec' % constants.BASE_DIR
  # query_vectors = '%s/luceneutil/tasks/vector-task-300d.vec' % constants.BASE_DIR

  # dim = 256
  # doc_vectors = '/d/electronics_asin_emb.bin'
  # query_vectors = '/d/electronics_query_vectors.bin'

  # Cohere dataset
  dim = 768
  doc_vectors = f"/lucenedata/enwiki/{'cohere-wikipedia'}-docs-{dim}d.vec"
  query_vectors = f"/lucenedata/enwiki/{'cohere-wikipedia'}-queries-{dim}d.vec"
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

  all_results = []
  while advance(indexes, values):
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
            print(f"  -quantize")
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
        "-reindex",
        "-search-and-stats",
        query_vectors,
        "-numIndexThreads",
        "8",
        #'-metric', 'mip',
        # '-parentJoin', parentJoin_meta_file,
        # '-numMergeThread', '8', '-numMergeWorker', '8',
        "-forceMerge",
        #'-stats',
        #'-quiet'
      ]
    )
    print(f"  cmd: {this_cmd}")
    job = subprocess.Popen(this_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding="utf-8")
    re_summary = re.compile(r"^SUMMARY: (.*?)$", re.MULTILINE)
    summary = None
    while True:
      line = job.stdout.readline()
      if line == "":
        break
      sys.stdout.write(line)
      m = re_summary.match(line)
      if m is not None:
        summary = m.group(1)
    if summary is None:
      raise RuntimeError("could not find summary line in output!")
    job.wait()
    if job.returncode != 0:
      raise RuntimeError(f"command failed with exit {job.returncode}")
    all_results.append(summary)
    if DO_PROFILING:
      benchUtil.profilerOutput(constants.JAVA_EXE, jfr_output, benchUtil.checkoutToPath(checkout), 30, (1,))

  print("\nResults:")

  # TODO: be more careful when we skip/show headers e.g. if some of the runs involve filtering,
  # turn filterType/selectivity back on for all runs
  skip_headers = {"selectivity", "filterType", "visited"}

  if "-forceMerge" not in this_cmd:
    skip_headers.add("force merge s")

  print_fixed_width(all_results, skip_headers)


def print_fixed_width(all_results, columns_to_skip):
  header = "recall\tlatency (ms)\tnDoc\ttopK\tfanout\tmaxConn\tbeamWidth\tquantized\tvisited\tindex s\tindex docs/s\tforce merge s\tnum segments\tindex size (MB)\tselectivity\tfilterType\tvec disk (MB)\tvec RAM (MB)"

  # crazy logic to make everything fixed width so rendering in fixed width font "aligns":
  headers = header.split("\t")
  num_columns = len(headers)
  # print(f'{num_columns} columns')
  max_by_col = [0] * num_columns

  rows_to_print = [header] + all_results

  skip_column_index = {headers.index(h) for h in columns_to_skip}

  for row in rows_to_print:
    by_column = row.split("\t")
    if len(by_column) != num_columns:
      raise RuntimeError(f'wrong number of columns: expected {num_columns} but got {len(by_column)} in "{row}"')
    for i, s in enumerate(by_column):
      max_by_col[i] = max(max_by_col[i], len(s))

  row_fmt = "  ".join([f"%{max_by_col[i]}s" for i in range(num_columns) if i not in skip_column_index])
  # print(f'using row format {row_fmt}')

  for row in rows_to_print:
    cols = row.split("\t")
    cols = tuple(cols[x] for x in range(len(cols)) if x not in skip_column_index)
    print(row_fmt % cols)


if __name__ == "__main__":
  # Where the version of Lucene is that will be tested. Now this will be sourced from gradle.properties
  LUCENE_CHECKOUT = getLuceneDirFromGradleProperties()
  run_knn_benchmark(LUCENE_CHECKOUT, PARAMS)
