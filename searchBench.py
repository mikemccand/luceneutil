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

import time
import sys
import os
import benchUtil
import common
import constants
import random

if '-ea' in sys.argv:
  JAVA_COMMAND += ' -ea:org.apache.lucene...'

INDEX_NUM_THREADS = constants.INDEX_NUM_THREADS
SEARCH_NUM_THREADS = constants.SEARCH_NUM_THREADS

if '-source' in sys.argv:
  source = sys.argv[1+sys.argv.index('-source')]
  if source == 'wikimedium':
    LINE_FILE = constants.WIKI_MEDIUM_DOCS_LINE_FILE
    INDEX_NUM_DOCS = 10000000
    TASKS_FILE = constants.WIKI_MEDIUM_TASKS_FILE
  elif source == 'wikibig':
    LINE_FILE = constants.WIKI_BIG_DOCS_LINE_FILE
    INDEX_NUM_DOCS = 3000000
    TASKS_FILE = constants.WIKI_BIG_TASKS_FILE
  elif source == 'euromedium':
    # TODO: need to be able to swap in new queries
    LINE_FILE = constants.EUROPARL_MEDIUM_DOCS_LINE_FILE
    INDEX_NUM_DOCS = 5000000
    TASKS_FILE = constants.EUROPARL_MEDIUM_TASKS_FILE
  else:
    # TODO: add geonames
    raise RuntimeError('unknown -source "%s" (expected wikimedium, wikibig, euromedium)' % source)
else:
  raise RuntimeError('please specify -source (wikimedium, wikibig, euromedium)')

if '-debug' in sys.argv:
  # 400K docs
  INDEX_NUM_DOCS /= 25

# This is #docs in /lucene/data/enwiki-20110115-lines-1k-fixed.txt
#INDEX_NUM_DOCS = 27625038

# This is #docs in /lucene/data/europarl.para.lines.txt
# INDEX_NUM_DOCS = 5607746

osName = common.osName

def run(id, coldRun, *competitors):

  r = benchUtil.RunAlgs(constants.JAVA_COMMAND)
  if '-noc' not in sys.argv:
    print
    print 'Compile:'
    for c in competitors:
      r.compile(c)
  search = '-search' in sys.argv
  index  = '-index' in sys.argv
  sum = search or '-sum' in sys.argv

  if '-debugs' in sys.argv or '-debug' in sys.argv:
    id += '-debug'
    jvmCount = 20
    if coldRun:
      countPerCat = 20
      repeatCount = 1
    else:
      countPerCat = 5
      repeatCount = 30
  else:
    jvmCount = 20
    if coldRun:
      countPerCat = 500
      repeatCount = 1
    else:
      countPerCat = 5
      repeatCount = 30

  if index:
    seen = set()
    for c in competitors:
      if c.index not in seen:
        seen.add(c.index)

    # if all jobs are going to share single index, use many threads
    numThreads = INDEX_NUM_THREADS

    seen = set()
    indexSegCount = None
    indexCommit = None
    p = False
    for c in competitors:
      if c.index not in seen:
        if not p:
          print
          print 'Create indices:'
          p = True
        seen.add(c.index)
        r.makeIndex(id, c.index)
        segCount = benchUtil.getSegmentCount(c.index)
        if indexSegCount is None:
          indexSegCount = segCount
          indexCommit = c.commitPoint
        elif indexCommit == c.commitPoint and indexSegCount != segCount:
          raise RuntimeError('segment counts differ across indices: %s vs %s' % (indexSegCount, segCount))
          
  logUpto = 0

  if search:

    threads = SEARCH_NUM_THREADS

    randomSeed = random.randint(-10000000, 1000000)

    results = {}
    print
    print 'Search:'

    for c in competitors:
      print '  %s:' % c.name
      t0 = time.time()
      results[c] = r.runSimpleSearchBench(id, c, repeatCount, threads, countPerCat, coldRun, randomSeed, jvmCount, filter=None)
      print '    %.2f sec' % (time.time() - t0)
  else:
    results = {}
    for c in competitors:
      results[c] = r.getSearchLogFiles(id, c, jvmCount)
      
  r.simpleReport(results[competitors[0]],
                 results[competitors[1]],
                 '-jira' in sys.argv,
                 '-html' in sys.argv,
                 cmpDesc=competitors[1].name,
                 baseDesc=competitors[0].name)


class Competitor(object):

  doSort = False

  def __init__(self, name, checkout, index, dirImpl, analyzer, commitPoint, tasksFile):
    self.name = name
    self.index = index
    self.checkout = checkout
    self.commitPoint = commitPoint
    self.dirImpl = dirImpl
    self.analyzer = analyzer
    self.tasksFile = tasksFile

  def compile(self, cp):
    benchUtil.run('javac -classpath "%s" perf/*.java >> compile.log 2>&1' % cp, 'compile.log')

  def setTask(self, task):
    self.searchTask = self.TASKS[task];
    return self

def bushy():

  COLD = False

  index1 = benchUtil.Index('clean.svn', source, 'StandardAnalyzer', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=LINE_FILE)
  index2 = benchUtil.Index('bushy3', source, 'StandardAnalyzer', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=LINE_FILE)
  run('bushy',
      COLD,
      Competitor('base', 'clean.svn', index1, 'MMapDirectory', 'StandardAnalyzer', 'multi', TASKS_FILE),
      Competitor('bushy', 'bushy3', index2, 'MMapDirectory', 'StandardAnalyzer', 'multi', TASKS_FILE),
    )

if __name__ == '__main__':
  bushy()

# NOTE: when running on 3.0, apply this patch:
"""
Index: src/java/org/apache/lucene/search/FuzzyQuery.java
===================================================================
--- src/java/org/apache/lucene/search/FuzzyQuery.java	(revision 1062278)
+++ src/java/org/apache/lucene/search/FuzzyQuery.java	(working copy)
@@ -133,6 +133,8 @@
     }
 
     int maxSize = BooleanQuery.getMaxClauseCount();
+    // nocommit
+    maxSize = 50;
     PriorityQueue<ScoreTerm> stQueue = new PriorityQueue<ScoreTerm>();
     FilteredTermEnum enumerator = getEnum(reader);
     try {
"""
