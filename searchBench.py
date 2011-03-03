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

# TODO
#  - add sort-by-title

if '-ea' in sys.argv:
  JAVA_COMMAND += ' -ea:org.apache.lucene...'

INDEX_NUM_THREADS = constants.INDEX_NUM_THREADS

if '-source' in sys.argv:
  source = sys.argv[1+sys.argv.index('-source')]
  if source == 'wikimedium':
    LINE_FILE = constants.WIKI_MEDIUM_DOCS_LINE_FILE
    INDEX_NUM_DOCS = 10000000
  elif source == 'wikibig':
    LINE_FILE = constants.WIKI_BIG_DOCS_LINE_FILE
    INDEX_NUM_DOCS = 3000000
  elif source == 'euromedium':
    # TODO: need to be able to swap in new queries
    LINE_FILE = constants.EUROPARL_MEDIUM_DOCS_LINE_FILE
    INDEX_NUM_DOCS = 5000000
  else:
    # TODO: add geonames
    raise RuntimeError('unknown -source "%s" (expected wikimedium, wikibig, euromedium)' % source)
else:
  raise RuntimeError('please specify -source (wikimedium, wikibig, euromedium)')

if '-debug' in sys.argv:
  # nocommit
  INDEX_NUM_DOCS /= 1000

# This is #docs in /lucene/data/enwiki-20110115-lines-1k-fixed.txt
#INDEX_NUM_DOCS = 27625038

# This is #docs in /lucene/data/europarl.para.lines.txt
# INDEX_NUM_DOCS = 5607746

osName = common.osName

def run(*competitors):  
  r = benchUtil.RunAlgs(constants.JAVA_COMMAND)
  if '-noc' not in sys.argv:
    for c in competitors:
      r.compile(c)
  search = '-search' in sys.argv
  index  = '-index' in sys.argv

  indexSegCount = None
  indexCommit = None
  
  if index:
    seen = set()
    for c in competitors:
      if c.index not in seen:
        seen.add(c.index)

    if len(seen) == 1:
      # if all jobs are going to share single index, use many threads
      numThreads = INDEX_NUM_THREADS
    else:
      # else we must use 1 thread so indices are identical
      numThreads = 1

    seen = set()
    for c in competitors:
      if c.index not in seen:
        seen.add(c.index)
        r.makeIndex(c.index)
        segCount = benchUtil.getSegmentCount(c.index)
        if indexSegCount is None:
          indexSegCount = segCount
          indexCommit = c.commitPoint
        elif indexCommit == c.commitPoint and indexSegCount != segCount:
          raise RuntimeError('segment counts differ across indices: %s vs %s' % (indexSegCount, segCount))
          

  logUpto = 0
  if not search:
    return
  if '-debugs' in sys.argv or '-debug' in sys.argv:
    iters = 1
    itersPerJVM = 15
    threads = 1
  else:
    iters = 2
    itersPerJVM = 40
    threads = 4

  results = {}
  for c in competitors:
    print 'Search on %s...' % c.checkout
    if osName == 'linux':
      benchUtil.run("sudo %s/dropCaches.sh" % constants.BENCH_BASE_DIR)
    elif osName in ('windows', 'cygwin'):
      # NOTE: requires you have admin priv
      benchUtil.run('%s/dropCaches.bat' % constants.BENCH_BASE_DIR)
    elif osName == 'osx':
      # NOTE: requires you install OSX CHUD developer package
      benchUtil.run('/usr/bin/purge')
    else:
      raise RuntimeError('do not know how to purge buffer cache on this OS (%s)' % osName
      
    t0 = time.time()
    results[c] = r.runSimpleSearchBench(c, iters, itersPerJVM, threads, filter=None)
    print '  %.2f sec' % (time.time() - t0)

  r.simpleReport(results[competitors[0]],
                 results[competitors[1]],
                 '-jira' in sys.argv,
                 '-html' in sys.argv,
                 cmpDesc=competitors[1].name,
                 baseDesc=competitors[0].name)


class Competitor(object):
  TASKS = { "search" : "perf.SearchTask",
          "loadIdFC" : "perf.LoadFieldCacheSearchTask",
          "loadIdDV" : "perf.values.DocValuesSearchTask"}

  def __init__(self, name, checkout, index, dirImpl, analyzer, commitPoint, task='search'):
    self.name = name
    self.index = index
    self.checkout = checkout
    self.searchTask = self.TASKS[task];
    self.commitPoint = commitPoint
    self.dirImpl = dirImpl
    self.analyzer = analyzer

  def compile(self, cp):
    benchUtil.run('javac -classpath "%s" perf/*.java >> compile.log 2>&1' % cp, 'compile.log')

  def setTask(self, task):
    self.searchTask = self.TASKS[task];
    return self

  def taskRunProperties(self):
    return '-Dtask.type=%s' % self.searchTask

class DocValueCompetitor(Competitor):
  def __init__(self, sourceDir, index, task='loadIdDV'):
    Competitor.__init__(self, sourceDir, index, task)
  
  def compile(self, cp):
    command = 'javac -cp %s perf/*.java >> compile.log 2>&1' % cp
    benchUtil.run(command,  'compile.log')
    benchUtil.run('javac -cp %s:./perf perf/values/*.java >> compile.log 2>&1' % cp,  'compile.log')

def bulkVsFOR():
  index1 = benchUtil.Index('bulkbranch', source, 'StandardAnalyzer', 'BulkVInt', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=LINE_FILE, doOptimize=False)
  index2 = benchUtil.Index('bulkbranch', source, 'StandardAnalyzer', 'FrameOfRef', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=LINE_FILE, doOptimize=False)
  run(
    Competitor('bulkvint', 'bulkbranch', index1, 'MMapDirectory', 'StandardAnalyzer', 'multi'),
    Competitor('for', 'bulkbranch', index2, 'MMapDirectory', 'StandardAnalyzer', 'multi'),
    )

def standardVSSimple64():
  index1 = benchUtil.Index('clean.svn', source, 'StandardAnalyzer', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=LINE_FILE, doOptimize=True)
  index2 = benchUtil.Index('bulkbranch', source, 'StandardAnalyzer', 'Simple64', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=LINE_FILE, doOptimize=True)
  run(
    Competitor('standard', 'clean.svn', index1, 'MMapDirectory', 'StandardAnalyzer', 'single'),
    Competitor('bulkvint', 'bulkbranch', index2, 'MMapDirectory', 'StandardAnalyzer', 'single'),
    )

def fst10vs4():
  index1 = benchUtil.Index('clean.svn', source, 'StandardAnalyzer', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=LINE_FILE)
  index2 = benchUtil.Index('clean2.svn', source, 'StandardAnalyzer', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=LINE_FILE)
  run(
    Competitor('fst10', 'clean.svn', index1, 'MMapDirectory', 'StandardAnalyzer', 'multi'),
    Competitor('fst4', 'clean2.svn', index2, 'MMapDirectory', 'StandardAnalyzer', 'multi'),
    )

def seekOpto():
  index = benchUtil.Index('clean.svn', source, 'StandardAnalyzer', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=LINE_FILE)
  run(
    Competitor('base', 'clean.svn', index, 'MMapDirectory', 'StandardAnalyzer', 'multi'),
    Competitor('opto', 'seekopto', index, 'MMapDirectory', 'StandardAnalyzer', 'multi'),
    )

def makeBigIndex():
  index = benchUtil.Index('newmp', source, 'StandardAnalyzer', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=LINE_FILE, doOptimize=False)
  run(
    Competitor('base', 'newmp', index, 'MMapDirectory', 'StandardAnalyzer', 'multi'),
    Competitor('base2', 'newmp', index, 'MMapDirectory', 'StandardAnalyzer', 'multi'),
    )

def pfor4():
  index1 = benchUtil.Index('bulkbranch', source, 'StandardAnalyzer', 'BulkVInt', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=LINE_FILE)
  index2 = benchUtil.Index('bulkbranch', source, 'StandardAnalyzer', 'PatchedFrameOfRef4', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=LINE_FILE)
  run(
    Competitor('BulkVInt', 'bulkbranch', index2, 'MMapDirectory', 'StandardAnalyzer', 'multi'),
    Competitor('PFor4', 'bulkbranch', index1, 'MMapDirectory', 'StandardAnalyzer', 'multi'),
    )

def bulkInterleaved():
  index1 = benchUtil.Index('bulkbranch', source, 'StandardAnalyzer', 'BulkVInt', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=LINE_FILE)
  index2 = benchUtil.Index('bulkbranch.clean', source, 'StandardAnalyzer', 'BulkVInt', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=LINE_FILE)
  run(
    Competitor('bulkint', 'bulkbranch.clean', index2, 'MMapDirectory', 'StandardAnalyzer', 'multi'),
    Competitor('bulk', 'bulkbranch', index1, 'MMapDirectory', 'StandardAnalyzer', 'multi'),
    )

def bushy():
  index1 = benchUtil.Index('clean.svn', source, 'StandardAnalyzer', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=LINE_FILE)
  index2 = benchUtil.Index('bushy3', source, 'StandardAnalyzer', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=LINE_FILE)
  run(
    Competitor('base', 'clean.svn', index1, 'MMapDirectory', 'StandardAnalyzer', 'multi'),
    Competitor('bushy', 'bushy3', index2, 'MMapDirectory', 'StandardAnalyzer', 'multi'),
    )

if __name__ == '__main__':
  #bulkVsFOR()
  #testSimple64Mult()
  #standardVSSimple64()
  #makeBigIndex()
  #fst10vs4()
  #seekOpto()
  bushy()
  #pfor4()
  #bulkInterleaved()

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
