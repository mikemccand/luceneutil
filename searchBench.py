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
if '-debug' in sys.argv:
  INDEX_NUM_DOCS = 100000
else:
  INDEX_NUM_DOCS = 10000000

# This is #docs in /lucene/data/enwiki-20100302-pages-articles-lines-1k.txt:
# INDEX_NUM_DOCS = 24900504

# Must be evenly divisible by number of threads:
INDEX_NUM_DOCS -= INDEX_NUM_DOCS % INDEX_NUM_THREADS

WIKI_LINE_FILE = constants.WIKI_LINE_FILE

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
    benchUtil.run('javac -cp %s perf/*.java >> compile.log 2>&1' % cp,  'compile.log')

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

def standardVsBulkVIntVarGap():
  index1 = benchUtil.Index('bulkbranch', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=True)
  index2 = benchUtil.Index('bulkbranch', 'wiki', 'BulkVInt', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=True)
  run(
    Competitor('base', 'bulkbranch', index1, 'MMapDirectory', 'multi'),
    Competitor('bulk', 'bulkbranch', index2, 'MMapDirectory', 'multi'),
    )

def standardVsFOR():
  index1 = benchUtil.Index('clean.svn', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=True)
  index2 = benchUtil.Index('bulkbranch', 'wiki', 'FrameOfRef', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=True)
  run(
    Competitor('base', 'clean.svn', index1, 'MMapDirectory', 'multi'),
    Competitor('for', 'bulkbranch', index2, 'MMapDirectory', 'multi'),
    )

def bigIndex():
  index = benchUtil.Index('clean.svn', 'wiki', 'StandardAnalyzer', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  run(
    Competitor('base', 'clean.svn', index, 'MMapDirectory', 'StandardAnalyzer', 'multi'),
    Competitor('base2', 'clean.svn', index, 'MMapDirectory', 'StandardAnalyzer', 'multi'),
    )

def standardVsStandard():
  index1 = benchUtil.Index('clean.svn', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=True)
  index2 = benchUtil.Index('blockterms', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=True)
  run(
    Competitor('clean', 'clean.svn', index1, 'NIOFSDirectory', 'multi'),
    Competitor('robspec', 'robspec', index1, 'NIOFSDirectory', 'multi'),
    #Competitor('blockterms', 'blockterms.old', index2, 'MMapDirectory', 'multi'),
    )

def testRT():
  index1 = benchUtil.Index('clean.svn', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  index2 = benchUtil.Index('realtime', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  run(
    Competitor('clean', 'clean.svn', index1, 'MMapDirectory', 'multi'),
    Competitor('realtime', 'realtime', index2, 'MMapDirectory', 'multi'),
    )

def testNIOWrite():
  index1 = benchUtil.Index('clean.svn', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  index2 = benchUtil.Index('niowrite', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  run(
    Competitor('clean', 'clean.svn', index1, 'NIOFSDirectory', 'multi'),
    Competitor('niowrite', 'niowrite', index2, 'NIOFSDirectory', 'multi'),
    )

def sepVsSep():
  index1 = benchUtil.Index('clean.svn', 'wiki', 'MockSep', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  index2 = benchUtil.Index('blockterms', 'wiki', 'MockSep', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  run(
    Competitor('clean', 'clean.svn', index1, 'MMapDirectory', 'multi'),
    Competitor('block', 'blockterms', index2, 'MMapDirectory', 'multi'),
    )

def bulkFixedVsVarGap():
  index1 = benchUtil.Index('bulkbranch.clean', 'wiki', 'BulkVInt', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=True)
  index2 = benchUtil.Index('bulkbranch', 'wiki', 'BulkVInt', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=True)
  run(
    Competitor('bulkfixedgap', 'bulkbranch.clean', index1, 'MMapDirectory', 'multi'),
    Competitor('bulkvargap', 'bulkbranch', index2, 'MMapDirectory', 'multi'),
    )
  
def test30vsTrunk():
  index1 = benchUtil.Index('30x', 'wiki', 'StandardAnalyzer', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  index2 = benchUtil.Index('clean.svn', 'wiki', 'ClassicAnalyzer', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  run(
    Competitor('3.0', '30x', index1, 'NIOFSDirectory', 'StandardAnalyzer', 'multi'),
    Competitor('4.0', 'clean.svn', index2, 'NIOFSDirectory', 'ClassicAnalyzer', 'multi'),
    )

def main():
  index1 = benchUtil.Index('solrcene-clean', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  index2 = benchUtil.Index('solrcene', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  run(
    Competitor('trunk', 'solrcene-clean', index1, 'NIOFSDirectory', 'multi'),
    Competitor('patch', 'solrcene', index2, 'NIOFSDirectory', 'multi'),
    )

if __name__ == '__main__':
  #standardVsBulkVIntVarGap()
  #standardVsFOR()
  #bulkFixedVsVarGap()
  #standardVsStandard()
  #testRT()
  #testNIOWrite()
  #sepVsSep()
  #test30vsTrunk()
  #bigIndex()
  main()



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
