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
from competition import *
import benchUtil
import common
import constants
import random

if '-ea' in sys.argv:
  JAVA_COMMAND += ' -ea:org.apache.lucene...'

osName = common.osName

def run(id, base, challenger, coldRun=False, doCharts=False, search=False, index=False, debug=False, debugs=False, verifyScores=True, taskPatterns=None):
  competitors = [challenger, base]

  #verifyScores = False
  r = benchUtil.RunAlgs(constants.JAVA_COMMAND, verifyScores)
  if '-noc' not in sys.argv:
    print
    print 'Compile:'
    for c in competitors:
      r.compile(c)
  if not search:
    search = '-search' in sys.argv

  if not index:
    index  = '-index' in sys.argv
  sum = search or '-sum' in sys.argv
 
  if debugs or debug or '-debugs' in sys.argv or '-debug' in sys.argv:
    debug = True
    id += '-fast'
    # nocommit
    jvmCount = 4
    if coldRun:
      countPerCat = 20
      repeatCount = 1
    else:
      countPerCat = 4
      repeatCount = 35
  else:
    jvmCount = 20
    if coldRun:
      countPerCat = 500
      repeatCount = 1
    else:
      countPerCat = 5
      repeatCount = 50

  if False:
    jvmCount = 3
    countPerCat = 5
    repeatCount = 35

  if index:
    seen = set()
    
    for c in competitors:
      if c.index not in seen:
        seen.add(c.index)
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
        r.makeIndex(id, c.index, doCharts)
        segCount = benchUtil.getSegmentCount(benchUtil.nameToIndexPath(c.index.getName()))
        if indexSegCount is None:
          indexSegCount = segCount
          indexCommit = c.commitPoint
        elif indexCommit == c.commitPoint and indexSegCount != segCount:
          raise RuntimeError('segment counts differ across indices: %s vs %s' % (indexSegCount, segCount))
          
  logUpto = 0

  if search:
    randomSeed = random.randint(-10000000, 1000000)
    print 'FIXED STATIC SEED'
    randomSeed = 17
    results = {}

    if constants.JAVA_COMMAND.find(' -ea') != -1:
      print
      print 'WARNING: *** assertions are enabled *** JAVA_COMMAND=%s' % constants.JAVA_COMMAND
      print

    print
    print 'Search:'

    taskFiles = {}

    for c in competitors:
      print '  %s:' % c.name
      if taskPatterns is not None:
        print '    tasks file: %s from %s' % (','.join(taskPatterns), c.tasksFile)
      else:
        print '    tasks file: %s' % c.tasksFile
      
      t0 = time.time()
      results[c] = r.runSimpleSearchBench(id, c, repeatCount, c.threads, countPerCat, coldRun, randomSeed, jvmCount, filter=None, taskPatterns=taskPatterns)
      print '    %.2f sec' % (time.time() - t0)
  else:
    results = {}
    for c in competitors:
      results[c] = r.getSearchLogFiles(id, c, jvmCount)

  results, cmpDiffs, cmpHeap = r.simpleReport(results[base],
                                              results[challenger],
                                              '-jira' in sys.argv,
                                              '-html' in sys.argv,
                                              cmpDesc=challenger.name,
                                              baseDesc=base.name)
  if cmpDiffs is not None:
    raise RuntimeError('results differ: %s' % str(cmpDiffs))


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
