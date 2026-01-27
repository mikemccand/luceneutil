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

import multiprocessing
import os

from localconstants import BASE_DIR

# NOTE: you must have a localconstants.py that, minimally, defines
# BASE_DIR; all your checkouts should be under BASE_DIR, ie
# BASE_DIR/aaa BASE_DIR/bbb etc.

if "BENCH_BASE_DIR" not in globals():
  BENCH_BASE_DIR = "%s/util" % BASE_DIR

# wget http://home.apache.org/~mikemccand/enwiki-20100302-pages-articles-lines-1k-shuffled.txt.bz2
# WIKI_MEDIUM_DOCS_LINE_FILE = '%s/data/enwiki-20100302-pages-articles-lines-1k-shuffled.txt' % BASE_DIR

# wget http://home.apache.org/~mikemccand/enwiki-20120502-lines-1k-fixed-utf8-with-random-label.txt.lzma
WIKI_MEDIUM_DOC_BIN_LINE_FILE = "%s/data/enwiki-20120502-lines-1k-fixed-utf8-with-random-label.bin" % BASE_DIR
WIKI_MEDIUM_DOCS_LINE_FILE = "%s/data/enwiki-20120502-lines-1k-fixed-utf8-with-random-label.txt" % BASE_DIR
WIKI_MEDIUM_DOCS_COUNT = 33332620

# Word vectors downloaded from http://nlp.stanford.edu/data/glove.6B.zip (823MB download; 2.1GB unzipped)
# Licensed under Public Domain (http://www.opendatacommons.org/licenses/pddl/1.0/);
# see https://nlp.stanford.edu/projects/glove/
# Thanks to Jeffrey Pennington, Richard Socher, and Christopher D. Manning.

# see vector-test.py for other vector sets / different dimensions:

# the words
VECTORS_WORD_TOK_FILE = "%s/data/enwiki-20120502-mpnet.tok" % BASE_DIR

# the vector for each word
VECTORS_WORD_VEC_FILE = "%s/data/enwiki-20120502-mpnet.vec" % BASE_DIR

# the pre-computed "line docs file" like version for vectors:
VECTORS_DOCS_FILE = "%s/data/enwiki-20120502-lines-1k-mpnet.vec" % BASE_DIR
# For debug/testing, fall back to cohere vectors if mpnet doesn't exist
import os
if not os.path.exists(VECTORS_DOCS_FILE):
    VECTORS_DOCS_FILE = "%s/data/cohere-v3-wikipedia-en-scattered-1024d.docs.first1M.vec" % BASE_DIR
    VECTORS_DIMENSIONS = 1024
VECTORS_QUERY_FILE = VECTORS_DOCS_FILE  # Use same file for queries in nightly bench

# VECTORS_TYPE = 'FLOAT8'
VECTORS_TYPE = "FLOAT32"

VECTORS_DIMENSIONS = 1024  # Changed from 768 to match Cohere v3 vectors

# WIKI_MEDIUM_TASKS_10MDOCS_FILE = '%s/tasks/wikimedium.10M.tasks' % BENCH_BASE_DIR
WIKI_MEDIUM_TASKS_10MDOCS_FILE = "%s/tasks/wikimedium.10M.nostopwords.tasks" % BENCH_BASE_DIR
WIKI_MEDIUM_FACETS_TASKS_10MDOCS_FILE = "%s/tasks/wikimedium.10M.facets.tasks" % BENCH_BASE_DIR
# WIKI_MEDIUM_TASKS_1MDOCS_FILE = '%s/tasks/wikimedium.1M.tasks' % BENCH_BASE_DIR
WIKI_MEDIUM_TASKS_1MDOCS_FILE = "%s/tasks/wikimedium.1M.nostopwords.tasks" % BENCH_BASE_DIR
WIKI_MEDIUM_TASKS_500DOCS_FILE = "%s/tasks/wikimedium500.tasks" % BENCH_BASE_DIR
WIKI_MEDIUM_TASKS_ALL_FILE = "%s/tasks/wikimedium.10M.tasks" % BENCH_BASE_DIR
WIKI_VECTOR_TASKS_FILE = "%s/tasks/vector.tasks" % BENCH_BASE_DIR
SORTED_TASKS_FILE = "%s/tasks/sorted.tasks" % BENCH_BASE_DIR
DISJUNCTION_SIMPLE_TASKS_FILE = "%s/tasks/disjunctionSimple.tasks" % BENCH_BASE_DIR
DISJUNCTION_REALISTIC_TASKS_FILE = "%s/tasks/disjunctionRealistic.tasks" % BENCH_BASE_DIR
DISJUNCTION_INTENSIVE_TASKS_FILE = "%s/tasks/disjunctionIntensive.tasks" % BENCH_BASE_DIR
COMBINED_FIELDS_TASKS_FILE = "%s/tasks/combinedfields.tasks" % BENCH_BASE_DIR
COMBINED_FIELDS_UNEVENLY_WEIGHTED_TASKS_FILE = "%s/tasks/combinedfields.unevenlyweighted.tasks" % BENCH_BASE_DIR

# wget https://pub-6de3254d7180436684278e0ec33ada22.r2.dev/enwiki-20120502-lines-1k-fixed-utf8-with-random-label.txt.lzma"
WIKI_BIG_DOCS_LINE_FILE = "%s/data/enwiki-20120502-lines-1k-fixed-utf8-with-random-label.txt" % BASE_DIR
# WIKI_BIG_DOCS_LINE_FILE = '%s/data/enwiki-20130102-lines.txt' % BASE_DIR

# 33332620 docs in enwiki-20120502-lines-1k.txt'
# 6726515 docs in enwiki-20120502-lines.txt
# enwiki-20110115-lines.txt has 5982049 docs
# enwiki-20110115-lines-1k-fixed.txt has 27625038 docs
# enwiki-20120502-lines-1k.txt has 33332620 docs
# enwiki-20120502-lines.txt has 6726515 docs
# enwiki-20130102-lines.txt has 6647577 docs
WIKI_BIG_DOCS_COUNT = 6726515

DISJUNCTION_DOC_COUNT = 500000
DISJUNCTION_DOCS_LINE_FILE = WIKI_MEDIUM_DOCS_LINE_FILE

# WIKI_FILE = '%s/data/enwiki-20100302-pages-articles.xml.bz2' % BENCH_BASE_DIR

# 5607746 docs:
# wget http://home.apache.org/~mikemccand/europarl.para.lines.txt
EUROPARL_MEDIUM_DOCS_LINE_FILE = "%s/data/europarl.para.lines.txt" % BASE_DIR
EUROPARL_MEDIUM_TASKS_FILE = "%s/data/europarlmedium.tasks" % BASE_DIR

LOGS_DIR = "%s/logs" % BASE_DIR
TOOL_LOGS_DIR = "%s/tool-logs" % BASE_DIR

TRUNK_CHECKOUT = "trunk"

INDEX_DIR_BASE = "%s/indices" % BASE_DIR

GIT_EXE = "git"

# configure java executables
JAVA_HOME = os.environ.get("JAVA_HOME")
java_bin = JAVA_HOME + "/bin/" if JAVA_HOME else ""
if java_bin:
  print("Using java from: %s" % java_bin)
if "JAVA_EXE" not in globals():
  JAVA_EXE = f"{java_bin}java"
if "JAVAC_EXE" not in globals():
  JAVAC_EXE = f"{java_bin}javac"
if "JAVA_COMMAND" not in globals():
  JAVA_COMMAND = "%s -server -Xms2g -Xmx2g --add-modules jdk.incubator.vector -XX:+HeapDumpOnOutOfMemoryError -XX:+UseParallelGC" % JAVA_EXE
else:
  print("use java command %s" % JAVA_COMMAND)  # pyright: ignore[reportUnboundVariable] # TODO: fix how variables are managed here

JRE_SUPPORTS_SERVER_MODE = True
INDEX_NUM_THREADS = 1

# when testing natural query latency we do not want to saturate CPU:
SEARCH_NUM_CONCURRENT_QUERIES = max(2, int(multiprocessing.cpu_count() / 3))

# geonames: http://download.geonames.org/export/dump/
GEONAMES_LINE_FILE_DOCS = "%s/data/allCountries.txt" % BASE_DIR

REPRO_COMMAND_START = "python -u %s/repeatLuceneTest.py -once -verbose -nolog" % BENCH_BASE_DIR
REPRO_COMMAND_END = ""

# SORT_REPORT_BY = 'p-value'
SORT_REPORT_BY = "pctchange"
# SORT_REPORT_BY = 'query'

if "ANALYZER" in locals():
  raise RuntimeError("ANALYZER should now be specified per-index and per-competitor")
# DEFAULTS

POSTINGS_FORMAT_DEFAULT = "Lucene104"
ID_FIELD_POSTINGS_FORMAT_DEFAULT = POSTINGS_FORMAT_DEFAULT
FACET_FIELD_DV_FORMAT_DEFAULT = "Lucene90"  # this field is not used as a default. Change the code in src/main/perf/Indexer.java to use a different DV format
ANALYZER_DEFAULT = "StandardAnalyzer"
SIMILARITY_DEFAULT = "BM25Similarity"
MERGEPOLICY_DEFAULT = "LogDocMergePolicy"

TESTS_LINE_FILE = "/lucene/clean2.svn/lucene/test-framework/src/resources/org/apache/lucene/util/europarl.lines.txt"
TESTS_LINE_FILE = "/lucenedata/from_hudson/hudson.enwiki.random.lines.txt"
# TESTS_LINE_FILE = None

GRADLE_EXE = "./gradlew"

# Set to True to run Linux's "perf stat" tool, but sudo must work w/o a password!
DO_PERF = False

PERF_STATS = (
  "task-clock",
  "cycles",
  "instructions",
  "cache-references",
  "cache-misses",
  "L1-dcache-loads",
  "L1-dcache-load-misses",
  "L1-icache-loads",
  "L1-icache-load-misses",
  "LLC-loads",
  "LLC-load-misses",
  "LLC-stores",
  "LLC-store-misses",
  "LLC-prefetches",
  "LLC-prefetch-misses",
  "faults",
  "minor-faults",
  "major-faults",
  "branches",
  "branch-misses",
  "stalled-cycles-frontend",
  "stalled-cycles-backend",
)

NIGHTLY_REPORTS_DIR = "%s/reports.nightly" % BASE_DIR
NIGHTLY_LOG_DIR = "%s/logs.nightly" % BASE_DIR

# Email notification settings for nightly benchmark
# Set NIGHTLY_EMAIL_ENABLED = False in localconstants.py to disable emails
# NIGHTLY_EMAIL_ENABLED = True
# NIGHTLY_FROM_EMAIL = 'your-email@example.com'
# NIGHTLY_TO_EMAIL = 'your-email@example.com'

# Nightly benchmark document counts
NIGHTLY_MEDIUM_INDEX_NUM_DOCS = 999000  # 999K docs (stay under 1M vector limit to avoid reader thread EOF)
NIGHTLY_BIG_INDEX_NUM_DOCS = 33000000     # 33M docs

# HNSW vector search configuration
HNSW_THREADS_PER_MERGE = 1
HNSW_THREAD_POOL_COUNT = 1

# Nightly KNN benchmark vector files (Cohere v3 Wikipedia embeddings)
# The "first1M" files are downloaded by initial_setup.py -download
# For full 8M vector benchmarks, download from: https://huggingface.co/datasets/Cohere/wikipedia-22-12-en-embeddings
# and use load_cohere_v3.py to generate the full .vec files
# Cohere v3, switched Dec 7 2025:
NIGHTLY_KNN_INDEX_VECTORS_FILE = "%s/data/cohere-v3-wikipedia-en-scattered-1024d.docs.first1M.vec" % BASE_DIR
NIGHTLY_KNN_SEARCH_VECTORS_FILE = "%s/data/cohere-v3-wikipedia-en-scattered-1024d.queries.first200K.vec" % BASE_DIR
NIGHTLY_KNN_VECTORS_DIM = 1024
NIGHTLY_KNN_REPORTS_DIR = "%s/reports.nightly" % BASE_DIR

# Nightly benchmark data files
NIGHTLY_MEDIUM_LINE_FILE = WIKI_MEDIUM_DOCS_LINE_FILE
NIGHTLY_BIG_LINE_FILE = WIKI_BIG_DOCS_LINE_FILE
WIKI_MEDIUM_TASKS_FILE = WIKI_MEDIUM_TASKS_ALL_FILE

PROCESSOR_COUNT = 12

# set this to see flame charts on blunders.io; see blunders.py
BLUNDERS_AUTH_UPLOAD_PASSWORD = None

# import again in case you want to override any of the vars set above
from localconstants import *  # noqa: F403 TODO: rethink how we do this to be more type-safe
