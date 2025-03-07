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
import argparse
import os

import competition
import constants

GLOVE_WORD_VECTORS_FILE = "%s/data/glove.6B.300d.txt" % constants.BASE_DIR
GLOVE_VECTOR_DOCS_FILE = "%s/data/enwiki-20120502-lines-1k-300d.vec" % constants.BASE_DIR
GLOVE_VECTOR8_DOCS_FILE = "%s/data/enwiki-20120502-lines-1k-300d-8bit.vec" % constants.BASE_DIR

MINILM_WORD_TOK_FILE = "%s/data/enwiki-20120502.all-MiniLM-L6-v2.tok" % constants.BASE_DIR
MINILM_WORD_VEC_FILE = "%s/data/enwiki-20120502.all-MiniLM-L6-v2.vec" % constants.BASE_DIR
MINILM_VECTOR_DOCS_FILE = "%s/data/enwiki-20120502-lines-1k-MiniLM-L6.vec" % constants.BASE_DIR

MPNET_WORD_TOK_FILE = "%s/data/enwiki-20120502-mpnet.tok" % constants.BASE_DIR
MPNET_WORD_VEC_FILE = "%s/data/enwiki-20120502-mpnet.vec" % constants.BASE_DIR
MPNET_VECTOR_DOCS_FILE = "%s/data/enwiki-20120502-lines-1k-mpnet.vec" % constants.BASE_DIR

# simple example that runs benchmark with WIKI_MEDIUM source and task files
# Baseline here is ../lucene_baseline versus ../lucene_candidate
if __name__ == "__main__":
  # TODO: Move to competition.DATA ?
  # sourceData = Data('wikivector1m-minilm', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 1000000, constants.WIKI_VECTOR_TASKS_FILE)
  # sourceData = competition.Data('wikivector1m-mpnet', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 1000000, constants.WIKI_VECTOR_TASKS_FILE)
  comp = competition.Competition(taskCountPerCat=20)

  # (vectorFile, vectorDimension, vectorDict) = (GLOVE_VECTOR_DOCS_FILE, 300, GLOVE_WORD_VECTORS_FILE)
  # (vectorFile, vectorDimension, vectorDict) = (MINILM_VECTOR_DOCS_FILE, 384, (MINILM_WORD_TOK_FILE, MINILM_WORD_VEC_FILE, 384))
  (vectorFile, vectorDimension, vectorDict) = (MPNET_VECTOR_DOCS_FILE, 768, (MPNET_WORD_TOK_FILE, MPNET_WORD_VEC_FILE, 768))

  parser = argparse.ArgumentParser(prog="Local Vector Benchmark Run", description="Run a local vector test benchmark on provided source dataset.")
  parser.add_argument("-s", "-source", "--source", default="wikivector1m", help="Data source to run the benchmark on.")
  parser.add_argument("-searchConcurrency", "--searchConcurrency", default="0", type=int, help="Search concurrency, 0 for disabled, -1 for using all cores")
  parser.add_argument("-b", "--baseline", default=os.environ.get("BASELINE") or "lucene_baseline", help="Path to lucene repo to be used for baseline")
  parser.add_argument("-c", "--candidate", default=os.environ.get("CANDIDATE") or "lucene_candidate", help="Path to lucene repo to be used for candidate")
  # parser.add_argument('-r', '--reindex', action='store_true',
  #                     help='Reindex data for candidate run')
  args = parser.parse_args()
  print("Running vector-test benchmarks with the following args: %s" % args)

  sourceData = competition.sourceData(args.source)
  index = comp.newIndex(args.baseline, sourceData, vectorFile=vectorFile, vectorDimension=vectorDimension, vectorEncoding="FLOAT32")

  # create a competitor named baseline with sources in the ../trunk folder
  comp.competitor("baseline", args.baseline, vectorDict=vectorDict, index=index, searchConcurrency=args.searchConcurrency)

  # use a different index
  # index = comp.newIndex(args.candidate, sourceData,
  #                       vectorFile=constants.GLOVE_VECTOR_DOCS_FILE,
  #                       vectorDimension=100,
  #                       vectorEncoding='FLOAT32',
  #                       facets = (('taxonomy:Date', 'Date'),
  #                                 ('taxonomy:Month', 'Month'),
  #                                 ('taxonomy:DayOfYear', 'DayOfYear'),
  #                                 ('sortedset:Month', 'Month'),
  #                                 ('sortedset:DayOfYear', 'DayOfYear')))
  # create a competitor named my_modified_version with sources in the ../patch folder
  # note that we haven't specified an index here, luceneutil will automatically use the index from the base competitor for searching
  # while the codec that is used for running this competitor is taken from this competitor.
  JAVA_COMMAND = "%s -server -Xms2g -Xmx2g --add-modules jdk.incubator.vector -XX:+HeapDumpOnOutOfMemoryError -XX:+UseParallelGC" % constants.JAVA_EXE

  comp.competitor("candidate", args.candidate, javaCommand=JAVA_COMMAND, vectorDict=vectorDict, index=index, searchConcurrency=args.searchConcurrency)

  # start the benchmark - this can take long depending on your index and machines
  comp.benchmark("baseline_vs_candidate")
