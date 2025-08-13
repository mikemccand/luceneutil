#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import glob
import json
import os
import pickle
import random
import re
import shutil
import smtplib
import subprocess
import sys
import tarfile
import time
import traceback
import urllib.request

# local imports:
import benchUtil
import blunders
import competition
import constants
import fillNightlyTotalIndexSizes
import ps_head
import runFacetsBenchmark
import runNightlyKnn
import runStoredFieldsBenchmark
import stats
from notation import KNOWN_CHANGES

"""
This script runs certain benchmarks, once per day, and generates graphs so we can see performance over time:

  * Index all of wikipedia ~ 1 KB docs w/ 512 MB ram buffer

  * Run NRT perf test on this index for 30 minutes (we only plot mean/stddev reopen time)

  * Index all of wikipedia actual (~4 KB) docs w/ 512 MB ram buffer

  * Index all of wikipedia ~ 1 KB docs, flushing by specific doc count to get 5 segs per level

  * Run search test
"""

# TODO
#   - need a tiny docs test?  catch per-doc overhead regressions...
#   - click on graph should go to details page
#   - nrt
#     - chart all reopen times by time...?
#     - chart over-time mean/stddev reopen time
#   - maybe multiple queries on one graph...?

DEBUG = "-debug" in sys.argv

JFR_STACK_SIZES = (1, 2, 4, 8, 12)

if DEBUG:
  NIGHTLY_DIR = "trunk"
else:
  NIGHTLY_DIR = "trunk.nightly"

DIR_IMPL = "MMapDirectory"

# Make sure we exercise Lucene's intra-query concurrency code paths:
SEARCH_CONCURRENCY = 8

INDEXING_RAM_BUFFER_MB = 2048

# "randomly" pick 5 queries for each category, but see validate_nightly_task_count where we
# enforce no more than this number of tasks in each category so that adding a new category
# of tasks will not cause other categories to pick different tasks
COUNTS_PER_CAT = 5

TASK_REPEAT_COUNT = 50

# MED_WIKI_BYTES_PER_DOC = 950.21921304868431
# BIG_WIKI_BYTES_PER_DOC = 4183.3843150398807

NRT_DOCS_PER_SECOND = 1103  # = 1 MB / avg med wiki doc size
NRT_RUN_TIME = 30 * 60
NRT_SEARCH_THREADS = 4
NRT_INDEX_THREADS = 1
NRT_REOPENS_PER_SEC = 1
JVM_COUNT = 20

if DEBUG:
  NRT_RUN_TIME //= 90
  JVM_COUNT = 3

reBytesIndexed = re.compile("^Indexer: net bytes indexed (.*)$", re.MULTILINE)
reIndexingTime = re.compile(r"^Indexer: finished \((.*) msec\)", re.MULTILINE)
reSVNRev = re.compile(r"revision (.*?)\.")
reIndexAtClose = re.compile("Indexer: at close: (.*?)$", re.MULTILINE)
reGitHubPROpen = re.compile(r"\s([0-9,]+) Open")
reGitHubPRClosed = re.compile(r"\s([0-9,]+) Closed")

# luceneutil#205: we accumulate the raw CSV values for every chart here, and write JSON in the end:
all_graph_data = {}

REAL = True


def now():
  return datetime.datetime.now()


def toSeconds(td):
  return td.days * 86400 + td.seconds + td.microseconds / 1000000.0


def message(s):
  print("[%s] %s" % (now(), s))


def runCommand(command):
  if REAL:
    message("RUN: %s" % command)
    t0 = time.time()
    if os.system(command):
      message("  FAILED")
      raise RuntimeError("command failed: %s" % command)
    message("  took %.1f sec" % (time.time() - t0))
  else:
    message("WOULD RUN: %s" % command)


def buildIndex(r, runLogDir, desc, index, logFile):
  message("build %s" % desc)
  # t0 = now()
  indexPath = benchUtil.nameToIndexPath(index.getName())
  if os.path.exists(indexPath):
    shutil.rmtree(indexPath)
  # aggregate at multiple stack depths so we can see patterns like "new BytesRef() is costly regardless of context", for example:
  indexPath, fullLogFile, profilerResults, jfrFile = r.makeIndex("nightly", index, profilerCount=50, profilerStackSize=JFR_STACK_SIZES)

  # indexTime = (now()-t0)

  newLogFileName = "%s/%s" % (runLogDir, logFile)
  if REAL:
    print("move log to %s" % newLogFileName)
    shutil.move(fullLogFile, newLogFileName)

  newVmstatLogFileName = f"{runLogDir}/{logFile.replace('.log', '.vmstat.log')}"
  if REAL:
    print("move vmstat log to %s" % newVmstatLogFileName)
    shutil.move(f"{constants.LOGS_DIR}/nightly.vmstat.log", newVmstatLogFileName)

  newTopLogFileName = f"{runLogDir}/{logFile.replace('.log', '.top.log')}"
  if REAL:
    print("move top log to %s" % newTopLogFileName)
    shutil.move(f"{constants.LOGS_DIR}/nightly.top.log", newTopLogFileName)

  s = open(newLogFileName).read()
  bytesIndexed = int(reBytesIndexed.search(s).group(1))
  m = reIndexAtClose.search(s)
  if m is not None:
    indexAtClose = m.group(1)
  else:
    # we have no index when we don't -waitForCommit
    indexAtClose = None
  indexTimeSec = int(reIndexingTime.search(s).group(1)) / 1000.0

  message("  took %.1f sec" % indexTimeSec)

  if "(fast)" in desc:
    # don't run checkIndex: we rollback in the end
    pass
  else:
    # checkIndex
    checkLogFileName = "%s/checkIndex.%s" % (runLogDir, logFile)
    checkIndex(r, indexPath, checkLogFileName)

  return indexPath, indexTimeSec, bytesIndexed, indexAtClose, profilerResults, jfrFile


def checkIndex(r, indexPath, checkLogFileName):
  message("run CheckIndex")
  cmd = '%s -classpath "%s" -ea org.apache.lucene.index.CheckIndex -threadCount 16 -level 2 "%s" > %s 2>&1' % (
    constants.JAVA_COMMAND,
    benchUtil.classPathToString(benchUtil.getClassPath(NIGHTLY_DIR)),
    indexPath + "/index",
    checkLogFileName,
  )
  runCommand(cmd)
  if open(checkLogFileName, encoding="utf-8").read().find("No problems were detected with this index") == -1:
    raise RuntimeError("CheckIndex failed")


reNRTReopenTime = re.compile("^Reopen: +([0-9.]+) msec$", re.MULTILINE)


def runNRTTest(r, indexPath, runLogDir):
  open("body10.tasks", "w").write("Term: body:10\n")

  cmd = '%s -classpath "%s" perf.NRTPerfTest %s "%s" multi "%s" 17 %s %s %s %s %s update 5 no 0.0 body10.tasks' % (
    constants.JAVA_COMMAND,
    benchUtil.classPathToString(benchUtil.getClassPath(NIGHTLY_DIR)),
    DIR_IMPL,
    indexPath + "/index",
    constants.NIGHTLY_MEDIUM_LINE_FILE,
    NRT_DOCS_PER_SECOND,
    NRT_RUN_TIME,
    NRT_SEARCH_THREADS,
    NRT_INDEX_THREADS,
    NRT_REOPENS_PER_SEC,
  )

  logFile = "%s/nrt.log" % runLogDir
  cmd += "> %s 2>&1" % logFile
  runCommand(cmd)

  times = []
  for s in reNRTReopenTime.findall(open(logFile, encoding="utf-8").read()):
    times.append(float(s))

  # Discard first 10 (JVM warmup)
  times = times[10:]

  # Discard worst 2%
  times.sort()
  numDrop = len(times) // 50
  if numDrop > 0:
    message("drop: %s" % " ".join(["%.1f" % x for x in times[-numDrop:]]))
    times = times[:-numDrop]
  message("times: %s" % " ".join(["%.1f" % x for x in times]))

  min, max, mean, stdDev = stats.getStats(times)
  message("NRT reopen time (msec) mean=%.4f stdDev=%.4f" % (mean, stdDev))

  checkIndex(r, indexPath, "%s/checkIndex.nrt.log" % runLogDir)

  return mean, stdDev


def validate_nightly_task_count(tasks_file, max_count):
  """We don't want tasks to randomly shift in the nightly benchy when we add a new category
  of tasks, as we did/saw for count(*) tasks. so we enforce here that there are NO MORE
  than N tasks in each category in the nightly tasks file.
  """
  re_cat_and_task = re.compile("^([^:]+): (.*?)(?:#.*)?$")

  by_cat = {}
  with open(tasks_file, encoding="utf-8") as f:
    for line in f.readlines():
      line = line.strip()
      if len(line) > 0 and line[0] != "#":
        # not a blank line nor comment line.  it's real!
        m = re_cat_and_task.match(line)
        cat, task = m.groups()
        if cat not in by_cat:
          by_cat[cat] = set()
        by_cat[cat].add(task)

  for cat, tasks in by_cat.items():
    if len(tasks) > max_count:
      tasks_str = "\n  ".join(tasks)
      raise RuntimeError(f"nightly tasks file {tasks_file} must have at most {max_count} tasks in each category, but saw {len(tasks)}:\n  {tasks_str}")


def run():
  openPRCount, closedPRCount = countGitHubPullRequests()

  MEDIUM_INDEX_NUM_DOCS = constants.NIGHTLY_MEDIUM_INDEX_NUM_DOCS
  BIG_INDEX_NUM_DOCS = constants.NIGHTLY_BIG_INDEX_NUM_DOCS

  if DEBUG:
    # Must re-direct all logs/results/reports so we don't overwrite the "production" run's logs:
    constants.LOGS_DIR = "/l/trunk/lucene/benchmark"
    constants.NIGHTLY_LOG_DIR = f"{constants.BASE_DIR}/logs.debug"
    constants.NIGHTLY_REPORTS_DIR = f"{constants.BASE_DIR}/reports.debug"
    MEDIUM_INDEX_NUM_DOCS //= 100
    BIG_INDEX_NUM_DOCS //= 100

  DO_RESET = "-reset" in sys.argv
  if DO_RESET:
    print("will regold results files (command line specified)")

  # TODO: remove this mechanism?  best if we must commit a message to Lucene recording this instead?
  regold_marker_file = f"{constants.BENCH_BASE_DIR}/regold_next_run"
  if os.path.exists(regold_marker_file):
    DO_RESET = True
    os.remove(regold_marker_file)
    print(f"saw regold marker file {regold_marker_file}; will regold results files")

  validate_nightly_task_count(f"{constants.BENCH_BASE_DIR}/tasks/wikinightly.tasks", COUNTS_PER_CAT)

  if not DEBUG:
    # TODO: understand why the attempted removal in Competition.benchmark did not actually run for nightly bench!
    for fileName in glob.glob(f"{constants.LOGS_DIR}/bench-search-*.jfr"):
      print("Removing old JFR %s..." % fileName)
      os.remove(fileName)

  print()
  print()
  print()
  print()
  message("start")
  id = "nightly"
  if not REAL:
    start = datetime.datetime(year=2011, month=5, day=19, hour=23, minute=00, second=0o1)
  else:
    start = now()
  timeStamp = "%04d.%02d.%02d.%02d.%02d.%02d" % (start.year, start.month, start.day, start.hour, start.minute, start.second)
  runLogDir = "%s/%s" % (constants.NIGHTLY_LOG_DIR, timeStamp)
  if REAL:
    os.makedirs(runLogDir)
  message("log dir %s" % runLogDir)

  os.chdir("%s/%s" % (constants.BASE_DIR, NIGHTLY_DIR))
  javaFullVersion = os.popen("%s -fullversion 2>&1" % constants.JAVA_COMMAND).read().strip()
  print("%s" % javaFullVersion)
  javaVersion = os.popen("%s -version 2>&1" % constants.JAVA_COMMAND).read().strip()
  print("%s" % javaVersion)

  p = subprocess.run(f"{constants.JAVA_COMMAND} -XX:+PrintFlagsFinal -version", shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=True)
  open(f"{runLogDir}/java-final-flags.txt", "w").write(p.stdout)

  print("uname -a: %s" % os.popen("uname -a 2>&1").read().strip())
  print("lsb_release -a:\n%s" % os.popen("lsb_release -a 2>&1").read().strip())

  if not REAL:
    os.chdir("%s/%s" % (constants.BASE_DIR, NIGHTLY_DIR))
    svnRev = "1102160"
    luceneUtilRev = "2270c7a8b3ac+ tip"
    print("SVN rev is %s" % svnRev)
    print("luceneutil rev is %s" % luceneUtilRev)

    luceneRev = os.popen("git rev-parse HEAD").read().strip()

  else:
    # nightly_strict.cmd has already git pull'd luceneutil & lucene
    os.chdir(constants.BENCH_BASE_DIR)
    luceneUtilRev = os.popen("git rev-parse HEAD").read().strip()
    print(f"luceneutil rev is {luceneUtilRev}")

    os.chdir("%s/%s" % (constants.BASE_DIR, NIGHTLY_DIR))
    luceneRev = os.popen("git rev-parse HEAD").read().strip()
    print(f"LUCENE rev is {luceneRev}")

    lastRevs = findLastSuccessfulGitHashes()
    if lastRevs is not None:
      lastLuceneRev, lastLuceneUtilRev, lastLogFile = lastRevs
      print(f"last successfull Lucene rev {lastLuceneRev}, luceneutil rev {lastLuceneUtilRev}")

      # parse git log to see if there were any commits requesting regold
      command = ["git", "log", f"{lastLuceneRev}^..{luceneRev}"]
      print(f"run {command}")
      result = subprocess.run(command, check=True, capture_output=True)

      regold_string = "// nightly-benchmarks-results-changed //"
      if regold_string in result.stdout.decode("utf-8"):
        print(f'Saw commit with "{regold_string}" comment from {" ".join(command)}; will regold results files')
        DO_RESET = True
      else:
        print("No commit message asking for regold of results files")

    os.chdir("%s/%s" % (constants.BASE_DIR, NIGHTLY_DIR))
    runCommand("%s clean -xfd" % constants.GIT_EXE)

  if not DEBUG and not DO_RESET:
    print("will NOT regold results files but rather compare to last good result files")

  print("Java command-line: %s" % constants.JAVA_COMMAND)
  try:
    s = open("/sys/kernel/mm/transparent_hugepage/enabled").read()
  except:
    print("Unable to read /sys/kernel/mm/transparent_hugepage/enabled")
  else:
    print("transparent_hugepages: %s" % s)

  runCommand("%s clean > %s/clean-lucene.log 2>&1" % (constants.GRADLE_EXE, runLogDir))
  runCommand("%s jar > %s/jar-lucene.log 2>&1" % (constants.GRADLE_EXE, runLogDir))

  verifyScores = True

  # When intra-query concurrency is used we will not consistently return the same
  # estimated hit counts (I think?)
  verifyCounts = SEARCH_CONCURRENCY == 1

  r = benchUtil.RunAlgs(constants.JAVA_COMMAND, verifyScores, verifyCounts)

  comp = competition.Competition(
    taskRepeatCount=TASK_REPEAT_COUNT,
    taskCountPerCat=COUNTS_PER_CAT,
    verifyCounts=False,  # only verify top hits, not counts
    jvmCount=20,
  )

  mediumSource = competition.Data("wikimedium", constants.NIGHTLY_MEDIUM_LINE_FILE, MEDIUM_INDEX_NUM_DOCS, constants.WIKI_MEDIUM_TASKS_FILE)

  fastIndexMedium = comp.newIndex(
    NIGHTLY_DIR,
    mediumSource,
    analyzer="StandardAnalyzerNoStopWords",
    postingsFormat="Lucene103",
    numThreads=constants.INDEX_NUM_THREADS,
    directory=DIR_IMPL,
    idFieldPostingsFormat="Lucene103",
    ramBufferMB=INDEXING_RAM_BUFFER_MB,
    waitForMerges=False,
    waitForCommit=False,
    grouping=False,
    verbose=False,
    mergePolicy="TieredMergePolicy",
    useCMS=True,
  )

  fastIndexMediumVectors = comp.newIndex(
    NIGHTLY_DIR,
    mediumSource,
    analyzer="StandardAnalyzerNoStopWords",
    postingsFormat="Lucene103",
    numThreads=constants.INDEX_NUM_THREADS,
    directory=DIR_IMPL,
    idFieldPostingsFormat="Lucene103",
    ramBufferMB=INDEXING_RAM_BUFFER_MB,
    waitForMerges=False,
    waitForCommit=False,
    grouping=False,
    verbose=False,
    mergePolicy="TieredMergePolicy",
    useCMS=True,
    vectorFile=constants.VECTORS_DOCS_FILE,
    vectorDimension=constants.VECTORS_DIMENSIONS,
    vectorEncoding=constants.VECTORS_TYPE,
  )

  fastIndexMediumVectorsQuantized = comp.newIndex(
    NIGHTLY_DIR,
    mediumSource,
    analyzer="StandardAnalyzerNoStopWords",
    postingsFormat="Lucene103",
    numThreads=constants.INDEX_NUM_THREADS,
    directory=DIR_IMPL,
    idFieldPostingsFormat="Lucene103",
    ramBufferMB=INDEXING_RAM_BUFFER_MB,
    waitForMerges=False,
    waitForCommit=False,
    grouping=False,
    verbose=False,
    mergePolicy="TieredMergePolicy",
    useCMS=True,
    vectorFile=constants.VECTORS_DOCS_FILE,
    vectorDimension=constants.VECTORS_DIMENSIONS,
    vectorEncoding=constants.VECTORS_TYPE,
    quantizeKNNGraph=True,
  )

  nrtIndexMedium = comp.newIndex(
    NIGHTLY_DIR,
    mediumSource,
    analyzer="StandardAnalyzerNoStopWords",
    postingsFormat="Lucene103",
    numThreads=constants.INDEX_NUM_THREADS,
    directory=DIR_IMPL,
    idFieldPostingsFormat="Lucene103",
    ramBufferMB=INDEXING_RAM_BUFFER_MB,
    waitForMerges=True,
    waitForCommit=True,
    grouping=False,
    verbose=False,
    mergePolicy="TieredMergePolicy",
    useCMS=True,
    addDVFields=True,
  )

  bigSource = competition.Data("wikibig", constants.NIGHTLY_BIG_LINE_FILE, BIG_INDEX_NUM_DOCS, constants.WIKI_MEDIUM_TASKS_FILE)

  fastIndexBig = comp.newIndex(
    NIGHTLY_DIR,
    bigSource,
    analyzer="StandardAnalyzerNoStopWords",
    postingsFormat="Lucene103",
    numThreads=constants.INDEX_NUM_THREADS,
    directory=DIR_IMPL,
    idFieldPostingsFormat="Lucene103",
    ramBufferMB=INDEXING_RAM_BUFFER_MB,
    waitForMerges=False,
    waitForCommit=False,
    grouping=False,
    verbose=False,
    mergePolicy="TieredMergePolicy",
    useCMS=True,
  )

  # Must use only 1 thread so we get same index structure, always:
  index = comp.newIndex(
    NIGHTLY_DIR,
    mediumSource,
    analyzer="StandardAnalyzerNoStopWords",
    postingsFormat="Lucene103",
    numThreads=1,
    useCMS=False,
    directory=DIR_IMPL,
    idFieldPostingsFormat="Lucene103",
    mergePolicy="LogDocMergePolicy",
    facets=(
      ("taxonomy:Date", "Date"),
      ("taxonomy:Month", "Month"),
      ("taxonomy:DayOfYear", "DayOfYear"),
      ("sortedset:Month", "Month"),
      ("sortedset:DayOfYear", "DayOfYear"),
      ("sortedset:Date", "Date"),
      ("taxonomy:RandomLabel", "RandomLabel"),
      ("sortedset:RandomLabel", "RandomLabel"),
    ),
    addDVFields=True,
    vectorFile=constants.VECTORS_DOCS_FILE,
    vectorDimension=constants.VECTORS_DIMENSIONS,
    vectorEncoding=constants.VECTORS_TYPE,
    hnswThreadsPerMerge=constants.HNSW_THREADS_PER_MERGE,
    hnswThreadPoolCount=constants.HNSW_THREAD_POOL_COUNT,
  )

  # this index is gigantic -- if a turd is leftover from a prior failed run, nuke it now!!
  index_path = benchUtil.nameToIndexPath(index.getName())
  if os.path.exists(index_path):
    print(f"NOTE: now delete old leftover ginormous index {index_path}")
    shutil.rmtree(index_path)

  c = comp.competitor(
    id,
    NIGHTLY_DIR,
    index=index,
    # vectorDict=(constants.VECTORS_WORD_TOK_FILE, constants.VECTORS_WORD_VEC_FILE, constants.VECTORS_DIMENSIONS),
    vectorFileName=constants.VECTORS_QUERY_FILE,
    vectorDimension=constants.VECTORS_DIMENSIONS,
    directory=DIR_IMPL,
    commitPoint="multi",
    numConcurrentQueries=1,
    searchConcurrency=SEARCH_CONCURRENCY,
  )

  # c = benchUtil.Competitor(id, 'trunk.nightly', index, DIR_IMPL, 'StandardAnalyzerNoStopWords', 'multi', constants.WIKI_MEDIUM_TASKS_FILE)

  if REAL:
    r.compile(c)

  # stored fields benchy
  if not DEBUG and not DO_RESET:
    os.chdir(constants.BENCH_BASE_DIR)
    try:
      message("now run stored fields benchmark")
      if not REAL or DEBUG:
        doc_limit = 100000
      else:
        # do all docs in the file
        doc_limit = -1
      runStoredFieldsBenchmark.run_benchmark(
        f"{constants.BASE_DIR}/{NIGHTLY_DIR}", constants.GEONAMES_LINE_FILE_DOCS, f"{constants.INDEX_DIR_BASE}/geonames-stored-fields-nightly", runLogDir, doc_limit
      )
      message("done run stored fields benchmark")
    finally:
      os.chdir("%s/%s" % (constants.BASE_DIR, NIGHTLY_DIR))

    # high cardinality facets
    os.chdir(constants.BENCH_BASE_DIR)
    try:
      message("now run NAD facets benchmark")
      if not REAL or DEBUG:
        doc_limit = 100000
        num_iters = 2
      else:
        doc_limit = -1
        num_iters = 30
      index_dir_name = f"{constants.INDEX_DIR_BASE}/nad-facets-nightly"
      runFacetsBenchmark.run_benchmark(f"{constants.BASE_DIR}/{NIGHTLY_DIR}", index_dir_name, f"{constants.BASE_DIR}/data", runLogDir, doc_limit, num_iters)
      message("done run NAD facets benchmark")
      shutil.rmtree(index_dir_name)
    finally:
      os.chdir("%s/%s" % (constants.BASE_DIR, NIGHTLY_DIR))

    # KNN
    os.chdir(constants.BENCH_BASE_DIR)
    try:
      message("now run KNN benchmark")
      runNightlyKnn.run(runLogDir)

      message("now generate KNN nightly charts")
      runNightlyKnn.write_graph()
    except:
      print("Nightly KNN benchy failed; ignoring:")
      traceback.print_exc()
    finally:
      os.chdir("%s/%s" % (constants.BASE_DIR, NIGHTLY_DIR))

  # 1: test indexing speed: small (~ 1KB) sized docs, flush-by-ram
  medIndexPath, medIndexTime, medBytesIndexed, atClose, profilerMediumIndex, profilerMediumJFR = buildIndex(r, runLogDir, "medium index (fast)", fastIndexMedium, "fastIndexMediumDocs.log")
  message("medIndexAtClose %s" % atClose)
  shutil.rmtree(medIndexPath)

  # 2: test indexing speed: small (~ 1KB) sized docs, flush-by-ram, with vectors
  medVectorsIndexPath, medVectorsIndexTime, medVectorsBytesIndexed, atClose, profilerMediumVectorsIndex, profilerMediumVectorsJFR = buildIndex(
    r, runLogDir, "medium vectors index (fast)", fastIndexMediumVectors, "fastIndexMediumDocsWithVectors.log"
  )
  message("medIndexVectorsAtClose %s" % atClose)
  shutil.rmtree(medVectorsIndexPath)

  # 2.5: test indexing speed: small (~ 1KB) sized docs, flush-by-ram, with vectors, quantized
  # TODO: render profiler data for thias run too
  medQuantizedVectorsIndexPath, medQuantizedVectorsIndexTime, medQuantizedVectorsBytesIndexed, atClose, profilerMediumQuantizedVectorsIndex, profilerMediumQuantizedVectorsJFR = buildIndex(
    r, runLogDir, "medium quantized vectors index (fast)", fastIndexMediumVectorsQuantized, "fastIndexMediumDocsWithVectorsQuantized.log"
  )
  message("medIndexVectorsAtClose %s" % atClose)
  shutil.rmtree(medQuantizedVectorsIndexPath)

  # 3: build index for NRT test
  nrtIndexPath, nrtIndexTime, nrtBytesIndexed, atClose, profilerNRTIndex, profilerNRTJFR = buildIndex(r, runLogDir, "nrt medium index", nrtIndexMedium, "nrtIndexMediumDocs.log")
  message("nrtMedIndexAtClose %s" % atClose)
  nrtResults = runNRTTest(r, nrtIndexPath, runLogDir)

  # 4: test indexing speed: medium (~ 4KB) sized docs, flush-by-ram
  bigIndexPath, bigIndexTime, bigBytesIndexed, atClose, profilerBigIndex, profilerBigJFR = buildIndex(r, runLogDir, "big index (fast)", fastIndexBig, "fastIndexBigDocs.log")
  message("bigIndexAtClose %s" % atClose)
  shutil.rmtree(bigIndexPath)

  # 5: test searching speed; first build index, flushed by doc count (so we get same index structure night to night)
  # TODO: switch to concurrent yet deterministic indexer: https://markmail.org/thread/cp6jpjuowbhni6xc
  indexPathNow, ign, ign, atClose, profilerSearchIndex, profilerSearchJFR = buildIndex(r, runLogDir, "search index (fixed segments)", index, "fixedIndex.log")
  message("fixedIndexAtClose %s" % atClose)
  fixedIndexAtClose = atClose

  indexPathPrev = "%s/trunk.nightly.index.prev" % constants.INDEX_DIR_BASE

  if os.path.exists(indexPathPrev) and os.path.exists(benchUtil.nameToIndexPath(index.getName())):
    segCountPrev = benchUtil.getSegmentCount(indexPathPrev)
    segCountNow = benchUtil.getSegmentCount(benchUtil.nameToIndexPath(index.getName()))
    if segCountNow != segCountPrev:
      # raise RuntimeError('different index segment count prev=%s now=%s' % (segCountPrev, segCountNow))
      print("WARNING: different index segment count prev=%s now=%s" % (segCountPrev, segCountNow))

  # Search
  rand = random.Random(714)
  staticSeed = rand.randint(-10000000, 1000000)
  # staticSeed = -1492352

  message("search")
  t0 = now()

  coldRun = False
  comp = c
  comp.tasksFile = f"{constants.BENCH_BASE_DIR}/tasks/wikinightly.tasks"
  comp.printHeap = True
  if REAL:
    vmstatLogFile = f"{runLogDir}/search-tasks.vmstat.log"
    topLogFile = f"{runLogDir}/search-tasks.top.log"

    vmstatCmd = f"{benchUtil.VMSTAT_PATH} --active --wide --timestamp --unit M 1 > {vmstatLogFile} 2>/dev/null &"
    print(f"run vmstat: {vmstatCmd}")
    vmstatProcess = subprocess.Popen(vmstatCmd, shell=True, preexec_fn=os.setsid)

    topProcess = ps_head.PSTopN(10, topLogFile)
    print(f"run {topProcess.cmd} to {topLogFile}")

    resultsNow = []
    for iter in range(JVM_COUNT):
      seed = rand.randint(-10000000, 1000000)
      resultsNow.append(r.runSimpleSearchBench(iter, id, comp, coldRun, seed, staticSeed, filter=None))

    print(f"now kill vmstat: pid={vmstatProcess.pid}")
    # TODO: messy!  can we get process group working so we can kill bash and its child reliably?
    subprocess.check_call(["pkill", "-u", benchUtil.get_username(), "vmstat"])
    if vmstatProcess.poll() is None:
      raise RuntimeError("failed to kill vmstat child process?  pid={vmstatProcess.pid}")
    topProcess.stop()

  else:
    resultsNow = ["%s/%s/modules/benchmark/%s.%s.x.%d" % (constants.BASE_DIR, NIGHTLY_DIR, id, comp.name, iter) for iter in range(20)]
  message("done search (%s)" % (now() - t0))
  resultsPrev = []

  searchResults = searchHeap = None

  for fname in resultsNow:
    prevFName = fname + ".prev"
    if os.path.exists(prevFName):
      resultsPrev.append(prevFName)

  if len(resultsPrev) == 0 and DEBUG:
    # sidestep exception when we can't find any previous results because DEBUG
    resultsPrev = resultsNow

  output = []
  results, cmpDiffs, searchHeaps = r.simpleReport(resultsPrev, resultsNow, False, True, "prev", "now", writer=output.append)

  # generate vmstat pretties
  print("generate vmstat pretties")
  timestampLogDir = f"{constants.NIGHTLY_REPORTS_DIR}/{timeStamp}"
  os.mkdir(timestampLogDir)
  os.chdir(timestampLogDir)
  with open("index.html", "w") as indexOut:
    indexOut.write("<h2>vmstat charts for indexing tasks</h2>\n")
    for vmstatLogFileName in glob.glob(f"{runLogDir}/*.vmstat.log"):
      prefix = os.path.split(vmstatLogFileName)[1][:-11]
      indexOut.write(f"<a href={prefix}>{prefix}</a><br>\n")
      # a new sub-directory per run since each vmstat generates a bunch of pretties
      subDirName = f"{timestampLogDir}/{prefix}"
      os.mkdir(subDirName)
      print(f"  {subDirName}")
      # TODO: optimize to single shared copy!
      shutil.copy("/usr/share/gnuplot/6.0/js/gnuplot_svg.js", subDirName)
      shutil.copy(f"{constants.BENCH_BASE_DIR}/src/vmstat/index.html.template", f"{subDirName}/index.html")
      subprocess.check_call(f"gnuplot -c {constants.BENCH_BASE_DIR}/src/vmstat/vmstat.gpi {vmstatLogFileName} {prefix}", shell=True)

  with open("%s/%s.html" % (constants.NIGHTLY_REPORTS_DIR, timeStamp), "w") as f:
    timeStamp2 = "%s %02d/%02d/%04d" % (start.strftime("%a"), start.month, start.day, start.year)
    w = f.write
    w("<html>\n")
    w("<h1>%s</h1>" % timeStamp2)

    if DO_RESET:
      w("<b>NOTE</b>: this run regolded the results gold files<br><br>")

    if lastRevs is not None:
      w(f'\nLast successful run: <a href="{lastLogFile}">{lastLogFile[:-5]}</a><br>')
      if lastLuceneRev != luceneRev:
        w(f'\nLucene/Solr trunk rev {luceneRev} (<a href="https://github.com/apache/lucene/compare/{lastLuceneRev}...{luceneRev}">commits since last successful run</a>)<br>')
      else:
        w(f"\nLucene/Solr trunk rev {luceneRev} (no changes since last successful run)<br>")
      if lastLuceneUtilRev != luceneUtilRev:
        w(f'\nluceneutil revision {luceneUtilRev} (<a href="https://github.com/mikemccand/luceneutil/compare/{lastLuceneUtilRev}...{luceneUtilRev}">commits since last successful run</a>)<br>')
      else:
        w(f"\nluceneutil revision {luceneUtilRev} (no changes since last successful run)<br>")
    w("%s<br>" % javaVersion)
    w("%s<br>" % javaFullVersion)
    w("Java command-line: %s<br>" % htmlEscape(constants.JAVA_COMMAND))
    w("Index: %s<br>" % fixedIndexAtClose)
    w(f'See the <a href="{timeStamp}/index.html">perty vmstat charts</a>\n')
    w("<br><br><b>Search perf vs day before</b>\n")
    w("".join(output))
    w("<br><br>")
    w('<img src="%s.png"/>\n' % timeStamp)

    w("Jump to profiler results:")
    w("<br>indexing 1KB\n<ul>")
    for stackSize in JFR_STACK_SIZES:
      w(f'<li>stackSize={stackSize}: <a href="#profiler_1kb_indexing_{stackSize}_cpu">cpu</a>, <a href="#profiler_1kb_indexing_{stackSize}_heap">heap</a>')
    w("</ul>")

    w("<br>indexing 1KB (with vectors)\n<ul>")
    for stackSize in JFR_STACK_SIZES:
      w(f'<li>stackSize={stackSize}: <a href="#profiler_1kb_indexing_vectors_{stackSize}_cpu">cpu</a>, <a href="#profiler_1kb_indexing_vectors_{stackSize}_heap">heap</a>')
    w("</ul>")

    w("<br>indexing 4KB\n<ul>")
    for stackSize in JFR_STACK_SIZES:
      w(f'<li>stackSize={stackSize}: <a href="#profiler_4kb_indexing_{stackSize}_cpu">cpu</a>, <a href="#profiler_4kb_indexing_{stackSize}_heap">heap</a>')
    w("</ul>")

    w("<br>indexing near-real-timeB\n<ul>")
    for stackSize in JFR_STACK_SIZES:
      w(f'<li>stackSize={stackSize}: <a href="#profiler_nrt_indexing_{stackSize}_cpu">cpu</a>, <a href="#profiler_nrt_indexing_{stackSize}_heap">heap</a>')
    w("</ul>")

    w("<br>deterministic (single threaded) indexing<ul>")
    for stackSize in JFR_STACK_SIZES:
      w(f'<li>stackSize={stackSize}: <a href="#profiler_deterministic_indexing_{stackSize}_cpu">cpu</a>, <a href="#profiler_deterministic_indexing_{stackSize}_heap">heap</a>')
    w("</ul>")

    w("<br>searching<ul>")
    for stackSize in JFR_STACK_SIZES:
      w(f'<li>stackSize={stackSize}: <a href="#profiler_searching_{stackSize}_cpu">cpu</a>, <a href="#profiler_searching_{stackSize}_heap">heap</a>')
    w("</ul>")

    w("<br><br><h2>Profiler results (indexing)</h2>\n")
    if profilerMediumIndex is not None:
      w("<b>~1KB docs</b>")
      for mode, stackSize, output in profilerMediumIndex:
        w(f'\n<a id="profiler_1kb_indexing_{stackSize}_{mode}"></a>')
        w(f"\n<pre>{output}</pre>\n")
    if profilerBigIndex is not None:
      w("<b>~4KB docs</b>")
      for mode, stackSize, output in profilerBigIndex:
        w(f'\n<a id="profiler_4kb_indexing_{stackSize}_{mode}"></a>')
        w(f"\n<pre>{output}</pre>\n")
    if profilerNRTIndex is not None:
      w("<b>NRT indexing</b>")
      for mode, stackSize, output in profilerNRTIndex:
        w(f'\n<a id="profiler_nrt_indexing_{stackSize}_{mode}"></a>')
        w(f"\n<pre>{output}</pre>\n")
    if profilerSearchIndex is not None:
      w("<b>Deterministic (for search benchmarking) indexing</b>")
      for mode, stackSize, output in profilerSearchIndex:
        w(f'\n<a id="profiler_deterministic_indexing_{stackSize}_{mode}"></a>')
        w(f"\n<pre>{output}</pre>\n")
    if profilerMediumVectorsIndex is not None:
      w("<b>~1KB docs</b>")
      for mode, stackSize, output in profilerMediumVectorsIndex:
        w(f'\n<a id="profiler_1kb_indexing_vectors_{stackSize}_{mode}"></a>')
        w(f"\n<pre>{output}</pre>\n")

    w("<br><br><h2>Profiler results (searching)</h2>\n")
    w('<a id="profiler_searching_cpu"></a>')
    w("<b>CPU:</b><br>")
    w("<pre>\n")
    for stackSize, result in comp.getAggregateProfilerResult(id, "cpu", stackSize=JFR_STACK_SIZES, count=50):
      w(f'\n<a id="profiler_searching_{stackSize}_cpu"></a>')
      w(f"\n<pre>{result}</pre>")

    w("<br><br>")
    w("<b>HEAP:</b><br>")
    w('<a id="profiler_searching_heap"></a>')
    w("<pre>\n")
    for stackSize, result in comp.getAggregateProfilerResult(id, "heap", stackSize=JFR_STACK_SIZES, count=50):
      w(f'\n<a id="profiler_searching_{stackSize}_heap"></a>')
      w(f"\n<pre>{result}</pre>")
    w("</pre>")
    w("</html>\n")

    if not DEBUG and REAL:
      # Blunders upload:
      blunders.upload(
        f"Searching ({timeStamp})",
        f"searching-{timeStamp}",
        f"Profiled results during search benchmarks in Lucene's nightly benchmarks on {timeStamp}.  See <a href='https://home.apache.org/~mikemccand/lucenebench/{timeStamp}.html'>here</a> for full details.",
        # glob.glob(f'{constants.BENCH_BASE_DIR}/bench-search-{id}-{comp.name}-*.jfr'))
        glob.glob(f"{constants.NIGHTLY_LOG_DIR}/bench-search-{id}-{comp.name}-*.jfr"),
      )

      blunders.upload(
        f"Indexing fast ~1 KB docs ({timeStamp})",
        f"indexing-1kb-{timeStamp}",
        f"Profiled results during indexing ~1 KB docs in Lucene's nightly benchmarks on {timeStamp}.  See <a href='https://home.apache.org/~mikemccand/lucenebench/{timeStamp}.html'>here</a> for full details.",
        profilerMediumJFR,
      )

      blunders.upload(
        f"Indexing fast ~1 KB docs with vectors ({timeStamp})",
        f"indexing-1kb-vectors-{timeStamp}",
        f"Profiled results during indexing ~1 KB docs with KNN vectors in Lucene's nightly benchmarks on {timeStamp}.  See <a href='https://home.apache.org/~mikemccand/lucenebench/{timeStamp}.html'>here</a> for full details.",
        profilerMediumVectorsJFR,
      )

      blunders.upload(
        f"Indexing fast ~4 KB docs ({timeStamp})",
        f"indexing-4kb-{timeStamp}",
        f"Profiled results during indexing ~4 KB docs in Lucene's nightly benchmarks on {timeStamp}.  See <a href='https://home.apache.org/~mikemccand/lucenebench/{timeStamp}.html'>here</a> for full details.",
        profilerBigJFR,
      )

      blunders.upload(
        f"Indexing single-threaded ~1 KB docs into fixed searching index ({timeStamp})",
        f"indexing-1kb-fixed-search-single-thread-{timeStamp}",
        f"Profiled results during indexing ~1 KB docs in Lucene's nightly benchmarks, single-threaded, for fixed searching index on {timeStamp}.  See <a href='https://home.apache.org/~mikemccand/lucenebench/{timeStamp}.html'>here</a> for full details.",
        profilerSearchJFR,
      )

    if False:
      blunders.upload(
        f"Indexing NRT ~1 KB docs ({timeStamp})",
        f"indexing-1kb-nrt-{timeStamp}",
        f"Profiled results during indexing ~1 KB near-real-time docs in Lucene's nightly benchmarks on {timeStamp}.  See <a href='https://home.apache.org/~mikemccand/lucenebench/{timeStamp}.html'>here</a> for full details.",
        profilerNRTJFR,
      )

  if os.path.exists("out.png"):
    shutil.move("out.png", "%s/%s.png" % (constants.NIGHTLY_REPORTS_DIR, timeStamp))
  searchResults = results

  print("  heaps: %s" % str(searchHeaps))

  if cmpDiffs is not None:
    warnings, errors, overlap = cmpDiffs
    print("WARNING: search result differences: %s" % str(warnings))
    if len(errors) > 0 and not DO_RESET:
      raise RuntimeError("search result differences: %s" % str(errors))
  else:
    cmpDiffs = None
    searchHeaps = None

  results = (
    start,
    MEDIUM_INDEX_NUM_DOCS,
    medIndexTime,
    medBytesIndexed,
    BIG_INDEX_NUM_DOCS,
    bigIndexTime,
    bigBytesIndexed,
    nrtResults,
    searchResults,
    luceneRev,
    luceneUtilRev,
    searchHeaps,
    medVectorsIndexTime,
    medVectorsBytesIndexed,
    openPRCount,
    closedPRCount,
    medQuantizedVectorsIndexTime,
    medQuantizedVectorsBytesIndexed,
  )

  for fname in resultsNow:
    shutil.copy(fname, runLogDir)
    if os.path.exists(fname + ".stdout"):
      shutil.copy(fname + ".stdout", runLogDir)

  if REAL:
    for fname in resultsNow:
      shutil.move(fname, fname + ".prev")

    if not DEBUG:
      # print 'rename %s to %s' % (indexPathNow, indexPathPrev)
      if os.path.exists(indexPathNow):
        if os.path.exists(indexPathPrev):
          shutil.rmtree(indexPathPrev)
        os.rename(indexPathNow, indexPathPrev)

    os.chdir(runLogDir)
    # tar/bz2 log files, but not results files from separate benchmarks (e.g. stored fields):
    runCommand("tar cjf logs.tar.bz2 --exclude=*.pk *")
    for f in os.listdir(runLogDir):
      if f != "logs.tar.bz2" and not f.endswith(".pk"):
        os.remove(f)

    fillNightlyTotalIndexSizes.extract_one_file(f"{runLogDir}/logs.tar.bz2")

  if DEBUG:
    resultsFileName = "results.debug.pk"
  else:
    resultsFileName = "results.pk"

  open("%s/%s" % (runLogDir, resultsFileName), "wb").write(pickle.dumps(results))

  if REAL:
    if False:
      runCommand("chmod -R a-w %s" % runLogDir)

  message(f"done: total time {now() - start}")


def countGitHubPullRequests():
  with urllib.request.urlopen("https://github.com/apache/lucene/pulls") as response:
    html = response.read().decode("utf-8")

    m = reGitHubPROpen.search(html)
    if m is not None:
      openCount = int(m.group(1).replace(",", ""))
    else:
      openCount = None

    m = reGitHubPRClosed.search(html)
    if m is not None:
      closedCount = int(m.group(1).replace(",", ""))
    else:
      closedCount = None

    print(f"GitHub pull request counts: {openCount} open, {closedCount} closed")

    return openCount, closedCount


def findLastSuccessfulGitHashes():
  # lazy -- we really just need the most recent one:
  logFiles = sorted(os.listdir(constants.NIGHTLY_REPORTS_DIR), reverse=True)

  nightlyBenchResult = re.compile(r"\d\d\d\d\.\d\d\.\d\d\.\d\d\.\d\d\.\d\d\.html")

  for logFile in logFiles:
    if nightlyBenchResult.match(logFile) is not None and os.path.exists(f"{constants.NIGHTLY_LOG_DIR}/{logFile[:-5]}/results.pk"):
      luceneGitHash = None
      luceneUtilGitHash = None

      with open(os.path.join(constants.NIGHTLY_REPORTS_DIR, logFile)) as f:
        html = f.read()

        m = re.search("Lucene/Solr trunk rev ([a-z0-9]+)[< ]", html)
        if m is not None:
          luceneGitHash = m.group(1)
        else:
          raise RuntimeError(f"failed to determine last successful Lucene git hash from file {logFile}")

        m = re.search("luceneutil rev(?:ision)? ([a-z0-9]+)[< ]", html)
        if m is not None:
          luceneUtilGitHash = m.group(1)
        else:
          raise RuntimeError(f"failed to determine last successful luceneutil git hash from file {logFile}")

      return luceneGitHash, luceneUtilGitHash, logFile


reTimeIn = re.compile(r"^\s*Time in (.*?): (\d+) ms")


def getIndexGCTimes(subDir):
  if not os.path.exists("%s/gcTimes.pk" % subDir):
    times = {}
    if os.path.exists("%s/logs.tar.bz2" % subDir):
      cmd = "tar xjf %s/logs.tar.bz2 fastIndexMediumDocs.log" % subDir
      if os.system(cmd):
        raise RuntimeError("%s failed (cwd %s)" % (cmd, os.getcwd()))
      try:
        with open("fastIndexMediumDocs.log", encoding="utf-8") as f:
          for line in f.readlines():
            m = reTimeIn.search(line)
            if m is not None:
              times[m.group(1)] = float(m.group(2)) / 1000.0
      finally:
        os.remove("fastIndexMediumDocs.log")

      open("%s/gcTimes.pk" % subDir, "wb").write(pickle.dumps(times))
    return times
  return pickle.loads(open("%s/gcTimes.pk" % subDir, "rb").read())


reSearchStdoutLog = re.compile(r"nightly\.nightly\.\d+\.stdout")


def getSearchGCTimes(subDir):
  times = {}
  # print("check search gc/jit %s" % ('%s/logs.tar.bz2' % subDir))
  pk_file = "%s/search.gcjit.pk" % subDir
  if os.path.exists(pk_file):
    return pickle.load(open(pk_file, "rb"))

  if os.path.exists("%s/logs.tar.bz2" % subDir):
    with tarfile.open("%s/logs.tar.bz2" % subDir, "r") as t:
      for i in range(20):
        try:
          info = t.getmember("nightly.nightly.%d.stdout" % i)
        except KeyError:
          # we did not always save the .stdout w/ GC/JIT telemetry:
          break

        f = t.extractfile(info)
        try:
          for line in f.readlines():
            m = reTimeIn.search(line.decode("utf-8"))
            if m is not None:
              key = m.group(1)
              times[key] = times.get(key, 0.0) + float(m.group(2)) / 1000.0
        finally:
          f.close()

    open(pk_file, "wb").write(pickle.dumps(times))

  return times


def makeGraphs():
  global annotations
  medIndexChartData = ["Date,GB/hour"]
  medIndexVectorsChartData = ["Date,GB/hour"]
  medIndexQuantizedVectorsChartData = ["Date,GB/hour"]
  bigIndexChartData = ["Date,GB/hour"]
  nrtChartData = ["Date,Reopen Time (msec)"]
  gcIndexTimesChartData = ["Date,JIT (sec),Young GC (sec),Old GC (sec)"]
  fixedIndexSizeChartData = ["Date,Size (GB)"]
  gcSearchTimesChartData = ["Date,JIT (sec),Young GC (sec),Old GC (sec)"]
  searchChartData = {}
  storedFieldsResults = {
    "Index size": ["Date,Index size BEST_SPEED (MB),Index size BEST_COMPRESSION (MB)"],
    "Indexing time": ["Date,Indexing time BEST_SPEED (sec),Indexing time BEST_COMPRESSION (sec)"],
    "Retrieval time": ["Date,Retrieval time BEST_SPEED (msec),Retrieval time BEST_COMPRESSION (msec)"],
  }
  nadFacetsResults = {
    "Instantiating Counts class time": ["Date,Time creating FastTaxonomyCounts (msec),Time creating SSDVFacetCounts (msec)"],
    "getAllDims() time": ["Date,Taxonomy (msec),SSDV (msec)"],
    "getTopDims() time": ["Date,Taxonomy (msec),SSDV (msec)"],
    "getTopChildren() time": ["Date,Taxonomy (msec),SSDV (msec)"],
    "getAllChildren() time": ["Date,Taxonomy (msec),SSDV (msec)"],
  }
  gitHubPRChartData = ["Date,Open PR Count,Closed PR Count"]
  days = []
  annotations = []
  l = os.listdir(constants.NIGHTLY_LOG_DIR)
  l.sort()

  for subDir in l:
    resultsFile = "%s/%s/results.pk" % (constants.NIGHTLY_LOG_DIR, subDir)
    if DEBUG and not os.path.exists(resultsFile):
      resultsFile = "%s/%s/results.debug.pk" % (constants.NIGHTLY_LOG_DIR, subDir)

    if os.path.exists(resultsFile):
      tup = pickle.loads(open(resultsFile, "rb").read(), encoding="bytes")
      # print 'RESULTS: %s' % resultsFile

      timeStamp, medNumDocs, medIndexTimeSec, medBytesIndexed, bigNumDocs, bigIndexTimeSec, bigBytesIndexed, nrtResults, searchResults = tup[:9]
      if len(tup) > 9:
        rev = tup[9]
      else:
        rev = None

      if len(tup) > 10:
        utilRev = tup[10]
      else:
        utilRev = None

      if len(tup) > 11:
        searchHeaps = tup[11]
      else:
        searchHeaps = None

      if len(tup) > 16:
        medQuantizedVectorsIndexTimeSec, medQuantizedVectorsBytesIndexed = tup[16:18]
      else:
        medQuantizedVectorsIndexTimeSec, medQuantizedVectorsBytesIndexed = None, None

      if len(tup) > 12:
        medVectorsIndexTimeSec, medVectorsBytesIndexed = tup[12:14]
      else:
        medVectorsIndexTimeSec, medVectorsBytesIndexed = None, None

      if len(tup) > 14:
        openGitHubPRCount, closedGitHubPRCount = tup[14:16]
      else:
        openGitHubPRCount, closedGitHubPRCount = None, None

      timeStampString = "%04d-%02d-%02d %02d:%02d:%02d" % (timeStamp.year, timeStamp.month, timeStamp.day, timeStamp.hour, timeStamp.minute, int(timeStamp.second))
      date = "%02d/%02d/%04d" % (timeStamp.month, timeStamp.day, timeStamp.year)
      if date in ("09/03/2014",):
        # I was testing disabling THP again...
        continue
      if date in ("05/16/2014"):
        # Bug in luceneutil made it look like 0 qps on all queries
        continue
      if date in ("05/28/2023"):
        # Skip partially successfull first run with Panama -- the next run (05/29) was complete
        continue
      if date in ("06/22/2024", "06/23/2024"):
        # Super confusing: these two runs ran with inter-query concurrency 6 (6 queries in flight at once) and
        # intra-query concurrency 1 (only one worker thread), making the effective QPS measured ~1/6th of true
        # QPS
        continue

      gcIndexTimes = getIndexGCTimes("%s/%s" % (constants.NIGHTLY_LOG_DIR, subDir))
      s = timeStampString
      for h in "JIT compilation", "Young Generation GC", "Old Generation GC":
        v = gcIndexTimes.get(h)
        s += ","
        if v is not None:
          s += "%.4f" % v
      gcIndexTimesChartData.append(s)

      gcSearchTimes = getSearchGCTimes("%s/%s" % (constants.NIGHTLY_LOG_DIR, subDir))
      s = timeStampString
      for h in "JIT compilation", "Young Generation GC", "Old Generation GC":
        v = gcSearchTimes.get(h)
        s += ","
        if v is not None:
          s += "%.4f" % v
      gcSearchTimesChartData.append(s)

      if openGitHubPRCount is not None:
        gitHubPRChartData.append(f"{timeStampString},{openGitHubPRCount},{closedGitHubPRCount}")

      medIndexChartData.append("%s,%.1f" % (timeStampString, (medBytesIndexed / (1024 * 1024 * 1024.0)) / (medIndexTimeSec / 3600.0)))
      if medVectorsBytesIndexed is not None:
        medIndexVectorsChartData.append("%s,%.1f" % (timeStampString, (medVectorsBytesIndexed / (1024 * 1024 * 1024.0)) / (medVectorsIndexTimeSec / 3600.0)))
      if medQuantizedVectorsBytesIndexed is not None:
        medIndexQuantizedVectorsChartData.append("%s,%.1f" % (timeStampString, (medQuantizedVectorsBytesIndexed / (1024 * 1024 * 1024.0)) / (medQuantizedVectorsIndexTimeSec / 3600.0)))
      bigIndexChartData.append("%s,%.1f" % (timeStampString, (bigBytesIndexed / (1024 * 1024 * 1024.0)) / (bigIndexTimeSec / 3600.0)))
      mean, stdDev = nrtResults
      nrtChartData.append("%s,%.3f,%.2f" % (timeStampString, mean, stdDev))
      if searchResults is not None:
        days.append(timeStamp)
        for cat, (minQPS, maxQPS, avgQPS, stdDevQPS) in list(searchResults.items()):
          if isinstance(cat, bytes):
            # TODO: why does this happen!?
            cat = str(cat, "utf-8")

          if cat not in searchChartData:
            searchChartData[cat] = ["Date,QPS"]
          if cat == "PKLookup":
            qpsMult = 4000
          else:
            qpsMult = 1

          if cat == "TermDateFacets":
            if date in ("01/03/2013", "01/04/2013", "01/05/2013", "01/05/2014"):
              # Bug in luceneutil made facets not actually run correctly so QPS was way too high:
              continue
          if cat == "Fuzzy1":
            if date in ("05/06/2012", "05/07/2012", "05/08/2012", "05/09/2012", "05/10/2012", "05/11/2012", "05/12/2012", "05/13/2012", "05/14/2012"):
              # Bug in FuzzyQuery made Fuzzy1 be exact search
              continue
          if cat == "TermDateFacets":
            if date in ("02/02/2013", "02/03/2013"):
              # Bug in luceneutil (didn't actually run faceting on these days)
              continue
          if cat == "IntNRQ":
            if date in ("06/09/2014", "06/10/2014"):
              # Bug in luceneutil (didn't index numeric field properly)
              continue
          if cat in ("CombinedTerm", "CombinedHighMed", "CombinedHighHigh") and timeStamp < datetime.datetime(2022, 10, 19):
            # prior to this date these tasks were matching 0 docs, causing an fake 1000X slowdown!
            # see https://github.com/mikemccand/luceneutil/commit/56729cf341a443fb81148dd25d3d49cb88bc72e8
            continue

          searchChartData[cat].append("%s,%.3f,%.3f" % (timeStampString, avgQPS * qpsMult, stdDevQPS * qpsMult))

        fixed_index_size_file_name = f"{constants.NIGHTLY_LOG_DIR}/{subDir}/fixed_index_bytes.pk"
        if os.path.exists(fixed_index_size_file_name):
          with open(fixed_index_size_file_name, "rb") as f:
            size_in_mb = pickle.load(f)
          fixedIndexSizeChartData.append(f"{timeStampString},{size_in_mb / 1024}")

      label = 0
      for date, desc, fullDesc in KNOWN_CHANGES:
        # e.g. timeStampString: 2021-01-09 13:35:50
        if timeStampString.startswith(date):
          # print('timestamp %s: add annot %s' % (timeStampString, desc))
          annotations.append((date, timeStampString, desc, fullDesc, label))
          # KNOWN_CHANGES.remove((date, desc, fullDesc))
        label += 1

      resultsFile = "%s/%s/geonames-stored-fields-benchmark-results.pk" % (constants.NIGHTLY_LOG_DIR, subDir)
      if os.path.exists(resultsFile):
        print(f"stored: {subDir}, {timeStamp}")
        stored_fields_results = pickle.load(open(resultsFile, "rb"))
        index_size_mb_row = [timeStampString, None, None]
        indexing_time_sec_row = [timeStampString, None, None]
        retrieval_time_msec_row = [timeStampString, None, None]
        for mode, indexing_time_msec, stored_field_size_mb, retrieved_time_msec in stored_fields_results:
          if mode == "BEST_SPEED":
            idx = 1
          elif mode == "BEST_COMPRESSION":
            idx = 2
          else:
            raise RuntimeError(f"unknown stored fields benchmark mode {mode}: expected BEST_SPEED or BEST_COMPRESSION")

          index_size_mb_row[idx] = stored_field_size_mb
          indexing_time_sec_row[idx] = indexing_time_msec / 1000.0
          retrieval_time_msec_row[idx] = retrieved_time_msec

        storedFieldsResults["Index size"].append(",".join([str(x) for x in index_size_mb_row]))
        storedFieldsResults["Indexing time"].append(",".join([str(x) for x in indexing_time_sec_row]))
        storedFieldsResults["Retrieval time"].append(",".join([str(x) for x in retrieval_time_msec_row]))

      resultsFile = "%s/%s/nad-facet-benchmark.pk" % (constants.NIGHTLY_LOG_DIR, subDir)
      if os.path.exists(resultsFile):
        nad_facet_results = pickle.load(open(resultsFile, "rb"))
        create_counts_row = [None, None, None]
        get_all_dims_row = [None, None, None]
        get_top_dims_row = [None, None, None]
        get_children_row = [None, None, None]
        get_all_children_row = [None, None, None]
        for (
          create_fast_taxo_counts,
          create_ssdv_facet_counts,
          taxo_get_all_dims,
          ssdv_get_all_dims,
          taxo_get_top_dims,
          ssdv_get_top_dims,
          taxo_get_children,
          ssdv_get_children,
          taxo_get_all_children,
          ssdv_get_all_children,
        ) in nad_facet_results:
          create_counts_row = [timeStampString, create_fast_taxo_counts, create_ssdv_facet_counts]
          get_all_dims_row = [timeStampString, taxo_get_all_dims, ssdv_get_all_dims]
          get_top_dims_row = [timeStampString, taxo_get_top_dims, ssdv_get_top_dims]
          get_children_row = [timeStampString, taxo_get_children, ssdv_get_children]
          get_all_children_row = [timeStampString, taxo_get_all_children, ssdv_get_all_children]

        nadFacetsResults["Instantiating Counts class time"].append(",".join([str(x) for x in create_counts_row]))
        nadFacetsResults["getAllDims() time"].append(",".join([str(x) for x in get_all_dims_row]))
        nadFacetsResults["getTopDims() time"].append(",".join([str(x) for x in get_top_dims_row]))
        nadFacetsResults["getTopChildren() time"].append(",".join([str(x) for x in get_children_row]))
        nadFacetsResults["getAllChildren() time"].append(",".join([str(x) for x in get_all_children_row]))

      # else:
      #  fixedIndexSizeChartData.append(f'{timeStampString},null')

  sort(gitHubPRChartData)
  sort(medIndexChartData)
  sort(medIndexVectorsChartData)
  sort(medIndexQuantizedVectorsChartData)
  sort(bigIndexChartData)
  sort(gcIndexTimesChartData)
  sort(fixedIndexSizeChartData)
  for k, v in list(searchChartData.items()):
    sort(v)

  # Index time, including GC/JIT times
  writeIndexingHTML(fixedIndexSizeChartData, medIndexChartData, medIndexVectorsChartData, medIndexQuantizedVectorsChartData, bigIndexChartData, gcIndexTimesChartData)

  # CheckIndex time
  writeCheckIndexTimeHTML()

  # NRT
  writeNRTHTML(nrtChartData)

  # GitHub PR open/closed counts

  for k, v in list(searchChartData.items())[:]:
    # Graph does not render right with only one value:
    if len(v) > 1:
      writeOneGraphHTML("Lucene %s queries/sec" % taskRename.get(k, k), "%s/%s.html" % (constants.NIGHTLY_REPORTS_DIR, k), getOneGraphHTML(k, v, "Queries/sec", taskRename.get(k, k), errorBars=True))
    else:
      print("skip %s: %s" % (k, len(v)))
      del searchChartData[k]

  writeIndexHTML(searchChartData, days)

  writeGitHubPRChartHTML(gitHubPRChartData)

  writeSearchGCJITHTML(gcSearchTimesChartData)

  writeStoredFieldsBenchmarkHTML(storedFieldsResults)

  writeNADFacetBenchmarkHTML(nadFacetsResults)

  # publish
  # runCommand('rsync -rv -e ssh %s/reports.nightly mike@10.17.4.9:/usr/local/apache2/htdocs' % constants.BASE_DIR)

  if not DEBUG:
    open(f"{constants.BASE_DIR}/reports.nightly/for_all_time.json", "w").write(json.dumps(all_graph_data, indent=2))
    # we no longer push reports here -- the nightly shell script does git add/commit/push


reTookSec = re.compile("took ([0-9.]+) sec")
reDateTime = re.compile("log dir /lucene/logs.nightly/(.*?)$")


def writeCheckIndexTimeHTML():
  # Messy: parses the .tar.bz2 to find timestamps of each file  Once
  # LUCENE-6233 is in we can more cleanly get this from CheckIndex's output
  # instead:
  chartData = ["Date,CheckIndex time (seconds)"]

  l = os.listdir(constants.NIGHTLY_LOG_DIR)
  l.sort()

  for subDir in l:
    checkIndexTimeFile = "%s/%s/checkIndex.time" % (constants.NIGHTLY_LOG_DIR, subDir)
    if os.path.exists("%s/%s/results.debug.pk" % (constants.NIGHTLY_LOG_DIR, subDir)):
      # Skip debug runs
      continue

    tup = subDir.split(".")
    if len(tup) != 6:
      # print('skip %s' % subDir)
      continue

    if tup[:3] == ["2015", "04", "04"]:
      # Hide disastrously slow CheckIndex time after auto-prefix first landed
      continue

    if os.path.exists(checkIndexTimeFile):
      # Already previously computed & cached:
      seconds = int(open(checkIndexTimeFile).read())
    else:
      # Look at timestamps of each file in the tar file:
      logsFile = "%s/%s/logs.tar.bz2" % (constants.NIGHTLY_LOG_DIR, subDir)
      if os.path.exists(logsFile):
        t = tarfile.open(logsFile, "r:bz2")
        l = []
        while True:
          ti = t.next()
          if ti is None:
            break
          l.append((ti.mtime, ti.name))

        l.sort()
        for i in range(len(l)):
          if l[i][1] == "checkIndex.fixedIndex.log":
            seconds = l[i][0] - l[i - 1][0]
            break
        else:
          continue

        open(checkIndexTimeFile, "w").write("%d" % seconds)
      else:
        continue

    # print("tup %s" % tup)
    chartData.append("%s-%s-%s %s:%s:%s,%s" % (tuple(tup) + (seconds,)))
    # print("added %s" % chartData[-1])

  with open("%s/checkIndexTime.html" % constants.NIGHTLY_REPORTS_DIR, "w", encoding="utf-8") as f:
    w = f.write
    header(w, "Lucene nightly CheckIndex time")
    w("<h1>Seconds to run CheckIndex</h1>\n")
    w("<br>Click and drag to zoom; shift + click and drag to scroll after zooming; hover over an annotation to see details<br>")
    w("<br>")
    w(getOneGraphHTML("CheckIndexTimeSeconds", chartData, "Seconds", "CheckIndex time (seconds)", errorBars=False, pctOffset=15))

    writeKnownChanges(w, pctOffset=85)

    w("<br><br>")
    w("<b>Notes</b>:\n")
    w("<ul>\n")
    w("  <li> Java command-line: <tt>%s</tt>\n" % constants.JAVA_COMMAND)
    w("  <li> Java version: <tt>%s</tt>\n" % htmlEscape(os.popen("java -version 2>&1").read().strip()))
    w("  <li> OS: <tt>%s</tt>\n" % htmlEscape(os.popen("uname -a 2>&1").read().strip()))
    w("  <li> CPU: 2 Xeon X5680, overclocked @ 4.0 Ghz (total 24 cores = 2 CPU * 6 core * 2 hyperthreads)\n")
    w(
      '  <li> IO: index stored on 240 GB <a href="http://www.ocztechnology.com/ocz-vertex-3-sata-iii-2-5-ssd.html">OCZ Vertex 3</a>, starting on 4/25 (previously on traditional spinning-magnets hard drive (Western Digital Caviar Green, 1TB))'
    )
    w('  <li> Source code: <a href="http://code.google.com/a/apache-extras.org/p/luceneutil/source/browse/perf/Indexer.java"><tt>Indexer.java</tt></a>')
    w('  <li> All graphs are interactive <a href="http://dygraphs.com">Dygraphs</a>')
    w("</ul>")
    w('<br><a href="index.html">Back to all results</a><br>')
    footer(w)


def header(w, title):
  w("<html>")
  w("<head>")
  w("<title>%s</title>" % htmlEscape(title))
  # w('<style type="text/css">')
  # w('BODY { font-family:verdana; }')
  # w('</style>')
  w('<script type="text/javascript" src="dygraph-combined-dev.js"></script>\n')
  w("</head>")
  w("<body>")


def footer(w):
  w('<br><em>[last updated: %s; send questions to <a href="mailto:lucene@mikemccandless.com">Mike McCandless</a>]</em>' % now())
  w("</div>")
  w("</body>")
  w("</html>")


def writeOneLine(w, seen, cat, desc):
  seen.add(cat)
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="%s.html">%s</a>' % (cat, desc))


def writeIndexHTML(searchChartData, days):
  f = open("%s/index.html" % constants.NIGHTLY_REPORTS_DIR, "w")
  w = f.write
  header(w, "Lucene nightly benchmarks")
  w("<h1>Lucene nightly benchmarks</h1>")
  w(
    'Each night, an <a href="https://code.google.com/a/apache-extras.org/p/luceneutil/source/browse/src/python/nightlyBench.py">automated Python tool</a> checks out the Lucene/Solr trunk source code and runs multiple benchmarks: indexing the entire <a href="http://en.wikipedia.org/wiki/Wikipedia:Database_download">Wikipedia English export</a> three times (with different settings / document sizes); running a near-real-time latency test; running a set of "hardish" auto-generated queries and tasks.  The tests take around 2.5 hours to run, and the results are verified against the previous run and then added to the graphs linked below.'
  )
  w(
    '<p>The goal is to spot any long-term regressions (or, gains!) in Lucene\'s performance that might otherwise accidentally slip past the committers, hopefully avoiding the fate of the <a href="http://en.wikipedia.org/wiki/Boiling_frog">boiling frog</a>.</p>'
  )
  w('<p>See more details in <a href="http://blog.mikemccandless.com/2011/04/catching-slowdowns-in-lucene.html">this blog post</a>.</p>')
  w('<p>See pretty flame charts from Java Flight Recorder profiling at <a href="https://blunders.io/lucene-bench">blunders.io</a>.</p>')

  done = set()

  w("<br><br><b>Indexing:</b>")
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="indexing.html">Indexing throughput</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="analyzers.html">Analyzers throughput</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="nrt.html">Near-real-time refresh latency</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="indexing.html#MedIndexTime">GB/hour for medium docs index</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="indexing.html#MedVectorsIndexTime">GB/hour for medium docs index with vectors</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="indexing.html#MedQuantizedVectorsIndexTime">GB/hour for medium docs index with int8 quantized vectors</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="indexing.html#BigIndexTime">GB/hour for big docs index</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="indexing.html#GCTimes">Indexing JIT/GC times</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="indexing.html#FixedIndexSize">Index disk usage</a>')

  w("<br><br><b>BooleanQuery:</b>")
  writeOneLine(w, done, "AndHighHigh", "+high-freq +high-freq")
  writeOneLine(w, done, "AndHighMed", "+high-freq +medium-freq")
  writeOneLine(w, done, "OrHighHigh", "high-freq high-freq")
  writeOneLine(w, done, "OrHighMed", "high-freq medium-freq")
  writeOneLine(w, done, "AndHighOrMedMed", "+high-freq +(medium-freq medium-freq)")
  writeOneLine(w, done, "AndMedOrHighHigh", "+medium-freq +(high-freq high-freq)")
  writeOneLine(w, done, "Or2Terms2StopWords", "Disjunction of 2 regular terms and 2 stop words")
  writeOneLine(w, done, "And2Terms2StopWords", "Conjunction of 2 regular terms and 2 stop words")
  writeOneLine(w, done, "OrStopWords", "Disjunction of 2 or more stop words")
  writeOneLine(w, done, "AndStopWords", "Conjunction of 2 or more stop words")
  writeOneLine(w, done, "Or3Terms", "Disjunction of 3 terms")
  writeOneLine(w, done, "And3Terms", "Conjunction of 3 terms")
  writeOneLine(w, done, "OrHighRare", "Disjunction of a very frequent term and a very rare term")
  writeOneLine(w, done, "OrMany", "Disjunction of many terms")

  w("<br><br><b>CombinedFieldQuery:</b>")
  writeOneLine(w, done, "CombinedTerm", "Combined high-freq")
  writeOneLine(w, done, "CombinedOrHighMed", "Combined OR high-freq medium-freq")
  writeOneLine(w, done, "CombinedOrHighHigh", "Combined OR high-freq high-freq")
  writeOneLine(w, done, "CombinedAndHighMed", "Combined AND high-freq medium-freq")
  writeOneLine(w, done, "CombinedAndHighHigh", "Combined AND high-freq high-freq")

  w("<br><br><b>DisjunctionMaxQuery to take the maximum score across the title and body fields:</b>")
  writeOneLine(w, done, "DismaxTerm", "Term query")
  writeOneLine(w, done, "DismaxOrHighMed", "Disjunctive query on a high-frequency term and a medium-frequency term")
  writeOneLine(w, done, "DismaxOrHighHigh", "Disjunctive query on two high-frequency terms")
  writeOneLine(w, done, "FilteredDismaxTerm", "Filtered term query")
  writeOneLine(w, done, "FilteredDismaxOrHighMed", "Filtered disjunctive query on a high-frequency term and a medium-frequency term")
  writeOneLine(w, done, "FilteredDismaxOrHighHigh", "Filtered disjunctive query on two high-frequency terms")

  w("<br><br><b>Proximity queries:</b>")
  writeOneLine(w, done, "Phrase", "Exact phrase")
  writeOneLine(w, done, "SloppyPhrase", "Sloppy (~4) phrase")
  writeOneLine(w, done, "SpanNear", "Span near (~10)")
  writeOneLine(w, done, "IntervalsOrdered", "Ordered intervals (MAXWIDTH/10)")

  w("<br><br><b>FuzzyQuery:</b>")
  writeOneLine(w, done, "Fuzzy1", "Edit distance 1")
  writeOneLine(w, done, "Fuzzy2", "Edit distance 2")

  w("<br><br><b>Count:</b>")
  writeOneLine(w, done, "CountTerm", "Count(Term)")
  writeOneLine(w, done, "CountPhrase", "Count(Phrase)")
  writeOneLine(w, done, "CountAndHighHigh", "Count(+high-freq +high-freq)")
  writeOneLine(w, done, "CountAndHighMed", "Count(+high-freq +med-freq)")
  writeOneLine(w, done, "CountOrHighHigh", "Count(high-freq high-freq)")
  writeOneLine(w, done, "CountOrHighMed", "Count(high-freq med-freq)")
  writeOneLine(w, done, "CountOrMany", "Count(&lt;many terms&gt;)")
  writeOneLine(w, done, "CountFilteredPhrase", "Count(Filtered(Phrase))")
  writeOneLine(w, done, "CountFilteredOrHighHigh", "Count(Filtered(high-freq high-freq))")
  writeOneLine(w, done, "CountFilteredOrHighMed", "Count(Filtered(high-freq med-freq))")
  writeOneLine(w, done, "CountFilteredOrMany", "Count(Filtered(&lt;many terms&gt;))")
  writeOneLine(w, done, "CountFilteredIntNRQ", "Count(Filtered numeric range query)")

  w("<br><br><b>Vector Search:</b>")
  writeOneLine(w, done, "VectorSearch", "VectorSearch (approximate KNN float 768-dimension vector search from word embeddings)")
  writeOneLine(w, done, "PreFilteredVectorSearch", "Likewise, with a pre-filter")
  writeOneLine(w, done, "PostFilteredVectorSearch", "Same filter, but applied as a post-filter rather than a pre-filter")
  writeOneLine(
    w,
    done,
    "knnResults",
    "Nightly charts for <a href='https://github.com/mikemccand/luceneutil/blob/main/src/main/knn/KnnGraphTester.java'><tt>KnnGraphTester</tt></a> on <a href='https://huggingface.co/datasets/Cohere/wikipedia-22-12-en-embeddings'>Cohere Wikipedia en corpus</a>",
  )

  w("<br><br><b>Other queries:</b>")
  writeOneLine(w, done, "Term", "TermQuery")
  writeOneLine(w, done, "Respell", "Respell (DirectSpellChecker)")
  writeOneLine(w, done, "PKLookup", "Primary key lookup")
  writeOneLine(w, done, "Wildcard", "WildcardQuery")
  writeOneLine(w, done, "Prefix3", "PrefixQuery (3 leading characters)")
  writeOneLine(w, done, "IntNRQ", "Numeric range filtering on last-modified-datetime")
  writeOneLine(w, done, "IntSet", "Numeric set filtering on last-modified-datetime")

  w("<br><br><b>Filtered queries where the filter matches 5% of the index:</b>")
  writeOneLine(w, done, "FilteredTerm", "Filtered term query")
  writeOneLine(w, done, "FilteredAndHighHigh", "Filtered conjunctive query on two high-frequency terms")
  writeOneLine(w, done, "FilteredAndHighMed", "Filtered conjunctive query on a high-frequency term and a medium-frequency term")
  writeOneLine(w, done, "FilteredOrHighHigh", "Filtered disjunctive query on two high-frequency terms")
  writeOneLine(w, done, "FilteredOrHighMed", "Filtered disjunctive query on a high-frequency term and a medium-frequency term")
  writeOneLine(w, done, "FilteredPhrase", "Filtered phrase query")
  writeOneLine(w, done, "FilteredOr2Terms2StopWords", "Filtered disjunction of 2 regular terms and 2 stop words")
  writeOneLine(w, done, "FilteredAnd2Terms2StopWords", "Filtered conjunction of 2 regular terms and 2 stop words")
  writeOneLine(w, done, "FilteredOrStopWords", "Filtered disjunction of 2 or more stop words")
  writeOneLine(w, done, "FilteredAndStopWords", "Filtered conjunction of 2 or more stop words")
  writeOneLine(w, done, "FilteredOr3Terms", "Filtered disjunction of 3 terms")
  writeOneLine(w, done, "FilteredAnd3Terms", "Filtered conjunction of 3 terms")
  writeOneLine(w, done, "FilteredOrMany", "Filtered disjunction of many terms")
  writeOneLine(w, done, "FilteredPrefix3", "Filtered prefix query on a prefix of 3 chars")
  writeOneLine(w, done, "FilteredIntNRQ", "Filtered numeric range query")

  w("<br><br><b>Faceting:</b>")
  writeOneLine(w, done, "TermDateFacets", "Term query + date hierarchy")
  writeOneLine(w, done, "BrowseDateTaxoFacets", "All dates hierarchy")
  writeOneLine(w, done, "BrowseDateSSDVFacets", "All dates hierarchy (doc values)")
  writeOneLine(w, done, "BrowseMonthTaxoFacets", "All months")
  writeOneLine(w, done, "BrowseMonthSSDVFacets", "All months (doc values)")
  writeOneLine(w, done, "BrowseDayOfYearTaxoFacets", "All dayOfYear")
  writeOneLine(w, done, "BrowseDayOfYearSSDVFacets", "All dayOfYear (doc values)")
  writeOneLine(w, done, "MedTermDayTaxoFacets", "medium-freq-term +dayOfYear taxo facets")
  writeOneLine(w, done, "OrHighMedDayTaxoFacets", "high-freq medium-freq +dayOfYear taxo facets")
  writeOneLine(w, done, "AndHighHighDayTaxoFacets", "+high-freq +high-freq +dayOfYear taxo facets")
  writeOneLine(w, done, "AndHighMedDayTaxoFacets", "+high-freq +medium-freq +dayOfYear taxo facets")
  writeOneLine(w, done, "BrowseRandomLabelTaxoFacets", "Random labels chosen from each doc")
  writeOneLine(w, done, "BrowseRandomLabelSSDVFacets", "Random labels chosen from each doc (doc values)")
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="nad_facet_benchmarks.html">NAD high cardinality faceting</a>')

  w("<br><br><b>Sorting (on TermQuery):</b>")
  writeOneLine(w, done, "TermDTSort", "Date/time (long, high cardinality)")
  writeOneLine(w, done, "TermTitleSort", "Title (string, high cardinality)")
  writeOneLine(w, done, "TermMonthSort", "Month (string, low cardinality)")
  writeOneLine(w, done, "TermDayOfYearSort", "Day of year (int, medium cardinality)")

  w("<br><br><b>Grouping (on TermQuery):</b>")
  writeOneLine(w, done, "TermGroup100", "100 groups")
  writeOneLine(w, done, "TermGroup10K", "10K groups")
  writeOneLine(w, done, "TermGroup1M", "1M groups")
  writeOneLine(w, done, "TermBGroup1M", "1M groups (two pass block grouping)")
  writeOneLine(w, done, "TermBGroup1M1P", "1M groups (single pass block grouping)")

  w("<br><br><b>Others:</b>")
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="stored_fields_benchmarks.html">Stored fields geonames benchmarks</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="search_gc_jit.html">GC/JIT metrics during search benchmarks</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="geobench.html">Geo spatial benchmarks</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="sparseResults.html">Sparse vs dense doc values performance on NYC taxi ride corpus</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="antcleantest.html">"gradle -p lucene test" and "gradle precommit" time in lucene</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="checkIndexTime.html">CheckIndex time</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="github_pr_counts.html">Lucene GitHub pull-request counts</a>')

  l = list(searchChartData.keys())
  lx = []
  for s in l:
    if s not in done:
      done.add(s)
      v = taskRename.get(s, s)
      lx.append((v, '<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="%s.html">%s</a>' % (htmlEscape(s), htmlEscape(v))))
  lx.sort()
  for ign, s in lx:
    w(s)

  if False:
    w("<br><br>")
    w("<b>Details by day</b>:")
    w("<br>")
    days.sort()
    for t in days:
      timeStamp = "%04d.%02d.%02d.%02d.%02d.%02d" % (t.year, t.month, t.day, t.hour, t.minute, t.second)
      timeStamp2 = "%s %02d/%02d/%04d" % (t.strftime("%a"), t.month, t.day, t.year)
      w('<br>&nbsp;&nbsp;<a href="%s.html">%s</a>' % (timeStamp, timeStamp2))

  w("<br><br>")
  footer(w)


taskRename = {
  "PKLookup": "Primary Key Lookup",
  "Fuzzy1": "FuzzyQuery (edit distance 1)",
  "Fuzzy2": "FuzzyQuery (edit distance 2)",
  "Term": "TermQuery",
  "TermDTSort": "TermQuery (date/time sort)",
  "TermTitleSort": "TermQuery (title sort)",
  "TermBGroup1M": "Term (bgroup)",
  "TermBGroup1M1P": "Term (bgroup, 1pass)",
  "IntNRQ": "NumericRangeQuery (int)",
  "Prefix3": "PrefixQuery (3 characters)",
  "Phrase": "PhraseQuery (exact)",
  "SloppyPhrase": "PhraseQuery (sloppy)",
  "SpanNear": "SpanNearQuery",
  "IntervalsOrdered": "IntervalsQuery (ordered)",
  "AndHighHigh": "BooleanQuery (AND, high freq, high freq term)",
  "AndHighMed": "BooleanQuery (AND, high freq, medium freq term)",
  "OrHighHigh": "BooleanQuery (OR, high freq, high freq term)",
  "OrHighMed": "BooleanQuery (OR, high freq, medium freq term)",
  "CombinedTerm": "CombinedFieldsQuery (term)",
  "CombinedHighMed": "CombinedFieldsQuery (OR, high freq, medium freq term)",
  "CombinedHighHigh": "CombinedFieldsQuery (OR, high freq, high freq term)",
  "Wildcard": "WildcardQuery",
  "BrowseDayOfYearTaxoFacets": "All flat taxonomy facet counts for last-modified day-of-year",
  "BrowseMonthTaxoFacets": "All flat taxonomy facet counts for last-modified month",
  "BrowseDateTaxoFacets": "All hierarchical taxonomy facet counts for last-modified year/month/day",
  "BrowseDateSSDVFacets": "All hierarchical SSDV facet counts for last-modified year/month/day",
  "BrowseDayOfYearSSDVFacets": "All flat sorted-set doc values facet counts for last-modified day-of-year",
  "BrowseMonthSSDVFacets": "All flat sorted-set doc values facet counts for last-modified month",
  "BrowseRandomLabelTaxoFacets": "All flat taxonomy facet counts for a random word chosen from each doc",
  "BrowseRandomLabelSSDVFacets": "All flat sorted-set doc values counts for a random word chosen from each doc",
}


def htmlEscape(s):
  return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def sort(l):
  """Leave header (l[0]) in-place but sort the rest of the list, naturally."""
  x = l[0]
  del l[0]
  l.sort()
  l.insert(0, x)
  return l


def writeOneGraphHTML(title, fileName, chartHTML):
  f = open(fileName, "w")
  w = f.write
  header(w, title)
  w("<br>Click and drag to zoom; shift + click and drag to scroll after zooming; hover over an annotation to see details<br>")
  w(chartHTML)
  w("\n")
  writeKnownChanges(w)
  w("<b>Notes</b>:")
  w("<ul>")
  if title.find("Primary Key") != -1:
    w('<li>Lookup 4000 random documents using unique field "id"')
    w('<li>The "id" field is indexed with Pulsing codec.')
  w(
    '<li> Test runs %s instances of each tasks/query category (auto-discovered with <a href="http://code.google.com/a/apache-extras.org/p/luceneutil/source/browse/perf/CreateQueries.java">this Java tool</a>)'
    % COUNTS_PER_CAT
  )
  w("<li> Each of the %s instances are run %s times per JVM instance; we keep the best (fastest) time per task/query instance" % (COUNTS_PER_CAT, TASK_REPEAT_COUNT))
  w("<li> %s JVM instances are run; we compute mean/stddev from these" % JVM_COUNT)
  w("<li> %d concurrent queries\n" % constants.SEARCH_NUM_CONCURRENT_QUERIES)
  w("<li> One sigma error bars")
  w('<li> Source code: <a href="http://code.google.com/a/apache-extras.org/p/luceneutil/source/browse/perf/SearchPerfTest.java"><tt>SearchPerfTest.java</tt></a>')
  w('<li> All graphs are interactive <a href="http://dygraphs.com">Dygraphs</a>')
  w("</ul>")
  w('<br><a href="index.html"><b>Back to all results...</b></a><br>')
  footer(w)
  f.close()


def writeKnownChanges(w, pctOffset=77):
  # closed in footer()
  w('<div style="position: absolute; top: %d%%">\n' % pctOffset)
  w("<br>")
  w("<b>Known changes:</b>")
  w("<ul>")
  label = 0
  for date, timestamp, desc, fullDesc, label in annotations:
    w("<li><p><b>%s</b> (%s): %s</p>" % (getLabel(label), date, fullDesc))
    label += 1
  w("</ul>")


def writeSearchGCJITHTML(gcTimesChartData):
  with open("%s/search_gc_jit.html" % constants.NIGHTLY_REPORTS_DIR, "w") as f:
    w = f.write
    header(w, "Lucene search GC/JIT times")
    w("<br>Click and drag to zoom; shift + click and drag to scroll after zooming; hover over an annotation to see details<br>")
    w("<br>")
    w(getOneGraphHTML("GCTimes", gcTimesChartData, "Seconds", "JIT/GC times during searching", errorBars=False, pctOffset=10))
    w("\n")
    writeKnownChanges(w, pctOffset=227)
    footer(w)


def writeGitHubPRChartHTML(gitHubPRChartData):
  with open("%s/github_pr_counts.html" % constants.NIGHTLY_REPORTS_DIR, "w") as f:
    w = f.write
    header(w, "Lucene GitHub pull-request counts")
    w(
      '<br>Click and drag to zoom; shift + click and drag to scroll after zooming; hover over an annotation to see details.  This counts Lucene\'s <a href="https://github.com/apache/lucene/pulls">GitHub pull requests</a>. <br>'
    )
    w("<br>")
    w(getOneGraphHTML("GitHubPRCounts", gitHubPRChartData, "Count", "Lucene GitHub pull-request counts", errorBars=False, pctOffset=10))
    w("\n")
    writeKnownChanges(w, pctOffset=100)
    footer(w)


def writeStoredFieldsBenchmarkHTML(storedFieldsBenchmarkData):
  with open("%s/stored_fields_benchmarks.html" % constants.NIGHTLY_REPORTS_DIR, "w") as f:
    w = f.write
    header(w, "Lucene Stored Fields benchmarks")
    w(
      "<br>Click and drag to zoom; shift + click and drag to scroll after zooming; hover over an annotation to see details.  This shows the results of the stored fields benchmark (runStoredFieldsBenchmark.py). <br>"
    )
    w("<br>")
    w(getOneGraphHTML("IndexSize", storedFieldsBenchmarkData["Index size"], "MB", "Index size (MB)", errorBars=False, pctOffset=10))
    w(getOneGraphHTML("IndexingTime", storedFieldsBenchmarkData["Indexing time"], "Sec", "Indexing time (sec)", errorBars=False, pctOffset=90))
    w(getOneGraphHTML("RetrievalTime", storedFieldsBenchmarkData["Retrieval time"], "MSec", "Retrieval time (msec)", errorBars=False, pctOffset=170))
    w("\n")
    writeKnownChanges(w, pctOffset=250)
    footer(w)


def writeNADFacetBenchmarkHTML(nadFacetBenchmarkData):
  with open("%s/nad_facet_benchmarks.html" % constants.NIGHTLY_REPORTS_DIR, "w") as f:
    w = f.write
    header(w, "National Address Database high cardinality faceting benchmarks")
    w(
      '<br>Uses the <a href="https://www.transportation.gov/gis/national-address-database/national-address-database-nad-disclaimer">National Address Database dataset</a>. The data from revision 8 (used for this benchmark), can be downloaded <a href="https://nationaladdressdata.s3.amazonaws.com/NAD_r8_TXT.zip">here</a>'
    )
    w(
      "<br>Click and drag to zoom; shift + click and drag to scroll after zooming; hover over an annotation to see details.  This shows the results of the NAD facets benchmark (runFacetsBenchmark.py). <br>"
    )
    w("<br>")
    w(getOneGraphHTML("InstantatingCountsClassTime", nadFacetBenchmarkData["Instantiating Counts class time"], "msec", "Instantiating Counts class time (msec)", errorBars=False, pctOffset=10))
    w(getOneGraphHTML("getAllDimsTime", nadFacetBenchmarkData["getAllDims() time"], "msec", "getAllDims() time (msec)", errorBars=False, pctOffset=90))
    w(getOneGraphHTML("getTopDimsTime", nadFacetBenchmarkData["getTopDims() time"], "msec", "getTopDims() time (msec)", errorBars=False, pctOffset=170))
    w(getOneGraphHTML("getTopChildrenTime", nadFacetBenchmarkData["getTopChildren() time"], "msec", "getTopChildren() time (msec)", errorBars=False, pctOffset=250))
    w(getOneGraphHTML("getAllChildrenTime", nadFacetBenchmarkData["getAllChildren() time"], "msec", "getAllChildren() time (msec)", errorBars=False, pctOffset=330))
    w("\n")
    writeKnownChanges(w, pctOffset=410)
    footer(w)


def writeIndexingHTML(fixedIndexSizeChartData, medChartData, medVectorsChartData, medQuantizedVectorsChartData, bigChartData, gcTimesChartData):
  f = open("%s/indexing.html" % constants.NIGHTLY_REPORTS_DIR, "w", encoding="utf-8")
  w = f.write
  header(w, "Lucene nightly indexing benchmark")
  w("<br>Click and drag to zoom; shift + click and drag to scroll after zooming; hover over an annotation to see details<br>")
  w("<br>")
  w(getOneGraphHTML("MedIndexTime", medChartData, "Plain text GB/hour", "~1 KB Wikipedia English docs", errorBars=False, pctOffset=10))

  w("<br>")
  w("<br>")
  w("<br>")
  w(getOneGraphHTML("MedVectorsIndexTime", medVectorsChartData, "Plain text GB/hour", "~4 KB Wikipedia English docs, with KNN Vectors", errorBars=False, pctOffset=90))
  w("\n")

  w("<br>")
  w("<br>")
  w("<br>")
  w(
    getOneGraphHTML(
      "MedQuantizedVectorsIndexTime", medQuantizedVectorsChartData, "Plain text GB/hour", "~4 KB Wikipedia English docs, with KNN Scalar Quantized Vectors", errorBars=False, pctOffset=170
    )
  )
  w("\n")

  w("<br>")
  w("<br>")
  w("<br>")
  w(getOneGraphHTML("BigIndexTime", bigChartData, "Plain text GB/hour", "~4 KB Wikipedia English docs", errorBars=False, pctOffset=250))
  w("\n")

  w("<br>")
  w("<br>")
  w("<br>")
  w(getOneGraphHTML("GCTimes", gcTimesChartData, "Seconds", "JIT/GC times indexing ~1 KB docs", errorBars=False, pctOffset=330))

  w("\n")
  w("<br>")
  w("<br>")
  w("<br>")
  w(getOneGraphHTML("FixedIndexSize", fixedIndexSizeChartData, "GB", "Disk usage of fixed search index", errorBars=False, pctOffset=410))
  w("\n")

  writeKnownChanges(w, pctOffset=490)

  w("<br><br>")
  w("<b>Notes</b>:\n")
  w("<ul>\n")
  w("  <li> Test does <b>not wait for merges on close</b> (calls <tt>IW.close(false)</tt>)")
  w("  <li> Analyzer is <tt>StandardAnalyzer</tt>, but we <b>index all stop words</b>")
  w(
    '  <li> Test indexes full <a href="http://en.wikipedia.org/wiki/Wikipedia:Database_download">Wikipedia English XML export</a> (1/15/2011), from a pre-created line file (one document per line), on a different drive from the one that stores the index'
  )
  w("  <li> %d indexing threads\n" % constants.INDEX_NUM_THREADS)
  w("  <li> %s MB RAM buffer\n" % INDEXING_RAM_BUFFER_MB)
  w("  <li> Java command-line: <tt>%s</tt>\n" % constants.JAVA_COMMAND)
  w("  <li> Java version: <tt>%s</tt>\n" % htmlEscape(os.popen("java -version 2>&1").read().strip()))
  w("  <li> OS: <tt>%s</tt>\n" % htmlEscape(os.popen("uname -a 2>&1").read().strip()))
  w("  <li> CPU: 2 Xeon X5680, overclocked @ 4.0 Ghz (total 24 cores = 2 CPU * 6 core * 2 hyperthreads)\n")
  w(
    '  <li> IO: index stored on 240 GB <a href="http://www.ocztechnology.com/ocz-vertex-3-sata-iii-2-5-ssd.html">OCZ Vertex 3</a>, starting on 4/25 (previously on traditional spinning-magnets hard drive (Western Digital Caviar Green, 1TB))'
  )
  w('  <li> Source code: <a href="http://code.google.com/a/apache-extras.org/p/luceneutil/source/browse/perf/Indexer.java"><tt>Indexer.java</tt></a>')
  w('  <li> All graphs are interactive <a href="http://dygraphs.com">Dygraphs</a>')
  w("</ul>")
  w('<br><a href="index.html">Back to all results</a><br>')
  footer(w)
  f.close()


def writeNRTHTML(nrtChartData):
  f = open("%s/nrt.html" % constants.NIGHTLY_REPORTS_DIR, "w", encoding="utf-8")
  w = f.write
  header(w, "Lucene nightly near-real-time latency benchmark")
  w("<br>")
  w(getOneGraphHTML("NRT", nrtChartData, "Milliseconds", "Time (msec) to open a new reader", errorBars=True))
  writeKnownChanges(w)

  w("<b>Notes</b>:\n")
  w("<ul>\n")
  w("  <li> Test starts from full Wikipedia index, then use <tt>IW.updateDocument</tt> (so we stress deletions)")
  w("  <li> Indexing rate: %s updates/second (= ~ 1MB plain text / second)" % NRT_DOCS_PER_SECOND)
  w("  <li> Reopen NRT reader once per second")
  w("  <li> One sigma error bars")
  w("  <li> %s indexing thread" % NRT_INDEX_THREADS)
  w("  <li> 1 reopen thread")
  w("  <li> %s searching threads" % NRT_SEARCH_THREADS)
  w('  <li> Source code: <a href="http://code.google.com/a/apache-extras.org/p/luceneutil/source/browse/perf/NRTPerfTest.java"><tt>NRTPerfTest.java</tt></a>')
  w('<li> All graphs are interactive <a href="http://dygraphs.com">Dygraphs</a>')
  w("</ul>")

  w('<br><a href="index.html">Back to all results</a><br>')
  footer(w)
  w("</body>\n")
  w("</html>\n")


onClickJS = """
  function zp(num,count) {
    var ret = num + '';
    while(ret.length < count) {
      ret = "0" + ret;
    }
    return ret;
  }

  function doClick(ev, msec, pts) {
    d = new Date(msec);
    top.location = d.getFullYear() + "." + zp(1+d.getMonth(), 2) + "." + zp(d.getDate(), 2) + "." + zp(d.getHours(), 2) + "." + zp(d.getMinutes(), 2) + "." + zp(d.getSeconds(), 2) + ".html";
  }
"""


def getOneGraphHTML(id, data, yLabel, title, errorBars=True, pctOffset=5):
  # convert data closer to separate values:

  # TODO: do we have any escaped commans (\,) in our data!!
  values = [x.split(",") for x in data]

  if errorBars:
    # Insert the (missing) stddev headers so the header count matches the value count
    headers = values[0]
    column = 1
    while column < len(headers):
      headers.insert(column + 1, headers[column] + " (stddev)")
      column += 2

  # make numbers numbers again:
  is_int = [True] * len(values[0])
  is_float = [True] * len(values[0])
  for row in values[1:]:
    for i in range(len(row)):
      if row[i] == "":
        # allow missing values
        continue
      try:
        int(row[i])
      except ValueError:
        is_int[i] = False
      try:
        float(row[i])
      except ValueError:
        is_float[i] = False

  for row in values[1:]:
    for i in range(len(row)):
      if row[i] == "":
        # allow missing values, but swap in None/null
        if is_float[i] or is_int[i]:
          row[i] = None
        continue

      if is_int[i]:
        row[i] = int(row[i])
      elif is_float[i]:
        row[i] = float(row[i])

  # TODO: also include all known annotations!
  # TODO: when errorBars is true, the variance(s) is/are extra columns in the data, so the headers
  #       look incorrect now
  # TODO: get all other benches into this -- geo, sparse, github PRs, etc.
  all_graph_data[id] = (title, values)

  l = []
  w = l.append
  series = data[0].split(",")[1]
  w('<style type="text/css">')
  w("  #%s {\n" % id)
  w("    position: absolute;")
  w("    left: 10px;")
  w("    top: %d%%;" % pctOffset)
  w("  }")
  w("</style>")
  w('<div id="%s" style="height:70%%; width: 98%%"></div>' % id)
  w('<script type="text/javascript">')
  w(onClickJS)
  w("  g_%s = new Dygraph(" % id)
  w('    document.getElementById("%s"),' % id)
  seenTimeStamps = set()
  firstTimeStamp = None

  # header
  w('    "%s\\n" +' % data[0])

  for s in data[1:-1]:
    w('    "%s\\n" +' % s)
    timeStamp, theRest = s.split(",", 1)
    if firstTimeStamp is None and len(theRest.strip().replace(",", "").replace("\\n", "")) > 0:
      firstTimeStamp = timeStamp
    if firstTimeStamp is not None:
      # don't show annotations before this chart's data begins
      seenTimeStamps.add(timeStamp)

  s = data[-1]
  w('    "%s\\n",' % s)
  timeStamp = s[: s.find(",")]
  seenTimeStamps.add(timeStamp)
  options = []
  options.append('title: "%s"' % title)
  options.append('xlabel: "Date"')
  options.append('colors: ["#218559", "#192823", "#B0A691", "#06A2CB", "#EBB035", "#DD1E2F"]')
  options.append('ylabel: "%s"' % yLabel)
  options.append("labelsKMB: true")
  options.append("labelsSeparateLines: true")
  # options.append('labelsDivWidth: "50%"')
  options.append("clickCallback: doClick")
  options.append("labelsDivStyles: {'background-color': 'transparent'}")

  # show past 2 years by default -- disabled!  this is irritating because it is not obvious how to get back to the full history?
  if False:
    start = datetime.datetime.now() - datetime.timedelta(days=2 * 365)
    end = datetime.datetime.now() + datetime.timedelta(days=2)
    options.append('dateWindow: [Date.parse("%s/%s/%s"), Date.parse("%s/%s/%s")]' % (start.year, start.month, start.day, end.year, end.month, end.day))
  if False:
    if errorBars:
      maxY = max([float(x.split(",")[1]) + float(x.split(",")[2]) for x in data[1:]])
    else:
      maxY = max([float(x.split(",")[1]) for x in data[1:]])
    options.append("valueRange:[0,%.3f]" % (maxY * 1.25))
  # options.append('includeZero: true')

  if errorBars:
    options.append("errorBars: true")
    options.append("sigma: 1")

  options.append("showRoller: false")

  w("    {%s}" % ", ".join(options))

  # if 0:
  #     if errorBars:
  #         w('    {errorBars: true, valueRange:[0,%.3f], sigma:1, title:"%s", ylabel:"%s", xlabel:"Date"}' % (
  #         maxY * 1.25, title, yLabel))
  #     else:
  #         w('    {valueRange:[0,%.3f], title:"%s", ylabel:"%s", xlabel:"Date"}' % (maxY * 1.25, title, yLabel))
  w("  );")
  w("  g_%s.setAnnotations([" % id)
  descDedup = set()
  for date, timestamp, desc, fullDesc, label in annotations:
    # if this annot's timestamp was not seen in this chart, skip it:
    if timestamp not in seenTimeStamps:
      # print('SKIP: %s, %s, %s:%s' % (timestamp, desc, id, getLabel(label)))
      continue
    # print('KEEP: %s, %s, %s:%s' % (timestamp, desc, id, getLabel(label)))

    # if the same description on the same date was already added to this chart, skip it:
    tup = (desc, date)
    if tup in descDedup:
      continue
    descDedup.add(tup)
    if "JIT/GC" not in title or label >= 33:
      w("    {")
      w('      series: "%s",' % series)
      w('      x: "%s",' % timestamp)
      w('      shortText: "%s",' % getLabel(label))
      w("      width: 20,")
      w('      text: "%s",' % desc)
      w("    },")
  w("  ]);")
  w("</script>")

  if 0:
    f = open("%s/%s.txt" % (constants.NIGHTLY_REPORTS_DIR, id), "w")
    for s in data:
      f.write("%s\n" % s)
    f.close()
  return "\n".join(l)


def getLabel(label):
  if label < 26:
    s = chr(65 + label)
  else:
    s = "%s%s" % (chr(65 + (label // 26 - 1)), chr(65 + (label % 26)))
  return s


def sendEmail(toEmailAddr, subject, messageText):
  try:
    import localpass

    useSendMail = False
  except ImportError:
    useSendMail = True
  if not useSendMail:
    SMTP_SERVER = localpass.SMTP_SERVER
    SMTP_PORT = localpass.SMTP_PORT
    FROM_EMAIL = "admin@mikemccandless.com"
    smtp = smtplib.SMTP(SMTP_SERVER, port=SMTP_PORT)
    smtp.ehlo(FROM_EMAIL)
    smtp.starttls()
    smtp.ehlo(FROM_EMAIL)
    localpass.smtplogin(smtp)
    msg = "From: %s\r\n" % "mail@mikemccandless.com"
    msg += "To: %s\r\n" % toEmailAddr
    msg += "Subject: %s\r\n" % subject
    msg += "\r\n"
    smtp.sendmail("mail@mikemccandless.com", toEmailAddr.split(","), msg)
    smtp.quit()
  else:
    from email.mime.text import MIMEText
    from subprocess import PIPE, Popen

    msg = MIMEText(messageText)
    msg["From"] = "mail@mikemccandless.com"
    msg["To"] = toEmailAddr
    msg["Subject"] = subject
    p = Popen(["/usr/sbin/sendmail", "-t"], stdin=PIPE)
    p.communicate(msg.as_string())


if __name__ == "__main__":
  try:
    if "-run" in sys.argv:
      run()
    makeGraphs()
  except:
    traceback.print_exc()
    if not DEBUG and REAL:
      import socket

      sendEmail("mail@mikemccandless.com", "Nightly Lucene bench FAILED (%s)" % socket.gethostname(), "")

# scp -rp /lucene/reports.nightly mike@10.17.4.9:/usr/local/apache2/htdocs

# TO CLEAN
#   - rm -rf /p/lucene/indices/trunk.nightly.index.prev/
#   - rm -rf /lucene/logs.nightly/*
#   - rm -rf /lucene/reports.nightly/*
#   - rm -f /lucene/trunk.nightly/modules/benchmark/*.x
