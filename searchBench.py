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
import re
import random

from competition import *
import benchUtil
import common
import constants

if '-ea' in sys.argv:
  JAVA_COMMAND += ' -ea:org.apache.lucene...'

osName = common.osName

def run(id, base, challenger, coldRun=False, doCharts=False, search=False, index=False, verifyScores=True, taskPatterns=None, randomSeed=None):
  competitors = [challenger, base]

  if randomSeed is None:
    raise RuntimeError('missing randomSeed')

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

  if index:

    seen = set()
    indexSegCount = None
    indexCommit = None
    p = False
    tasksFile = None
    for c in competitors:
      if tasksFile is None:
        tasksFile = c.tasksFile
      elif tasksFile != c.tasksFile:
        raise RuntimeError('inconsistent taskFile %s vs %s' % (taskFile, c.taskFile))
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

    if taskPatterns is not (None, None):
      pos, neg = taskPatterns
      if pos is None:
        if neg is None:
          print '    tasks file: %s' % tasksFile
        else:
          print '    tasks file: NOT %s from %s' % (','.join(neg), tasksFile)
      elif neg is None:
        print '    tasks file: %s from %s' % (','.join(pos), tasksFile)
      else:
        print '    tasks file: %s, NOT %s from %s' % (','.join(pos), ','.join(neg), tasksFile)
      newTasksFile = '%s/%s.tasks' % (constants.BENCH_BASE_DIR, os.getpid())
      pos, neg = taskPatterns
      if pos is None:
        posPatterns = None
      else:
        posPatterns = [re.compile(x) for x in pos]
      if neg is None:
        negPatterns = None
      else:
        negPatterns = [re.compile(x) for x in neg]

      f = open(c.tasksFile)
      fOut = open(newTasksFile, 'wb')
      for l in f.readlines():
        i = l.find(':')
        if i != -1:
          cat = l[:i]
          if posPatterns is not None:
            for p in posPatterns:
              if p.search(cat) is not None:
                #print 'KEEP: match on %s' % cat
                break
            else:
              continue
          if negPatterns is not None:
            skip = False
            for p in negPatterns:
              if p.search(cat) is not None:
                skip = True
                #print 'SKIP: match on %s' % cat
                break
            if skip:
              continue

        fOut.write(l)
      f.close()
      fOut.close()

      for c in competitors:
        c.tasksFile = newTasksFile
        
    else:
      print '    tasks file: %s' % c.tasksFile
      newTasksFile = None

    try:

      results = {}

      if constants.JAVA_COMMAND.find(' -ea') != -1:
        print
        print 'WARNING: *** assertions are enabled *** JAVA_COMMAND=%s' % constants.JAVA_COMMAND
        print

      print
      print 'Search:'

      taskFiles = {}

      rand = random.Random(randomSeed)
      staticSeed = rand.randint(-10000000, 1000000)

      # Remove old log files:
      for c in competitors:
        for fileName in r.getSearchLogFiles(id, c):
          if os.path.exists(fileName):
            os.remove(fileName)

      for iter in xrange(base.competition.jvmCount):

        print '  iter %d' % iter

        seed = rand.randint(-10000000, 1000000)

        for c in competitors:
          print '    %s:' % c.name
          t0 = time.time()
          if c not in results:
            results[c] = []
          logFile = r.runSimpleSearchBench(iter, id, c,
                                           coldRun, seed, staticSeed,
                                           filter=None, taskPatterns=taskPatterns) 
          results[c].append(logFile)

        print
        print 'Report after iter %d:' % iter
        #print '  results: %s' % results
        details, cmpDiffs, cmpHeap = r.simpleReport(results[base],
                                                    results[challenger],
                                                    '-jira' in sys.argv,
                                                    '-html' in sys.argv,
                                                    cmpDesc=challenger.name,
                                                    baseDesc=base.name)
        if cmpDiffs is not None:
          raise RuntimeError('results differ: %s' % str(cmpDiffs))
        
    finally:
      if newTasksFile is not None and os.path.exists(newTasksFile):
        os.remove(newTasksFile)
          
  else:
    results = {}
    for c in competitors:
      results[c] = r.getSearchLogFiles(id, c)

    details, cmpDiffs, cmpHeap = r.simpleReport(results[base],
                                                results[challenger],
                                                '-jira' in sys.argv,
                                                '-html' in sys.argv,
                                                cmpDesc=challenger.name,
                                                baseDesc=base.name)
    if cmpDiffs is not None:
      raise RuntimeError('results differ: %s' % str(cmpDiffs))
