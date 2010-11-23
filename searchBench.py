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
from constants import *

if '-ea' in sys.argv:
  JAVA_COMMAND += ' -ea:org.apache.lucene...'

if '-debug' in sys.argv:
  INDEX_NUM_DOCS = 100000
else:
  INDEX_NUM_DOCS = 10000000

INDEX_NUM_THREADS = 6

def run(*competitors):  
  r = benchUtil.RunAlgs(JAVA_COMMAND)
  if '-noc' not in sys.argv:
    for c in competitors:
      r.compile(c)
  search = '-search' in sys.argv
  index  = '-index' in sys.argv

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

  logUpto = 0
  if not search:
    return
  if '-debugs' in sys.argv:
    iters = 2
    itersPerJVM = 2
    threads = 1
  else:
    iters = 3
    itersPerJVM = 40
    threads = 4

  results = {}
  for c in competitors:
    print 'Search on %s...' % c.checkout
    benchUtil.run("sudo %s/dropCaches.sh" % BENCH_BASE_DIR)
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

  def __init__(self, name, checkout, index, task='search'):
    self.name = name
    self.index = index
    self.checkout = checkout
    self.searchTask = self.TASKS[task];
    self.commitPoint = 'delmulti'

  def compile(self, cp):
    benchUtil.run('javac -cp %s perf/SearchPerfTest.java >> compile.log 2>&1' % cp,  'compile.log')

  def setTask(self, task):
    self.searchTask = self.TASKS[task];
    return self

  def taskRunProperties(self):
    return '-Dtask.type=%s' % self.searchTask

class DocValueCompetitor(Competitor):
  def __init__(self, sourceDir, index, task='loadIdDV'):
    Competitor.__init__(self, sourceDir, index, task)
  
  def compile(self, cp):
    benchUtil.run('javac -cp %s perf/SearchPerfTest.java >> compile.log 2>&1' % cp,  'compile.log')
    benchUtil.run('javac -cp %s:./perf perf/values/DocValuesSearchTask.java >> compile.log 2>&1' % cp,  'compile.log')
  
    
def main():
  index = benchUtil.Index('clean', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE)
  CLEAN_COMPETITOR = Competitor('base', 'clean', index)
  TERM_STATE_COMPETITOR = Competitor('termstate', 'termstate.2694', index)
  SPEC_COMPETITOR = Competitor('spec', 'docsenumspec', index)
  run(CLEAN_COMPETITOR, TERM_STATE_COMPETITOR)

if __name__ == '__main__':
  main()
