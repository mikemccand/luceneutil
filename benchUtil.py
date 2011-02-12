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

import types
import re
import time
import os
import shutil
import sys
import cPickle
import datetime
import constants
import common

# TODO
#   - add option for testing sorting, applying the SortValues.patch!!
#   - verify step
#   - run searches
#   - get all docs query in here

osName = common.osName

# NOTE: only detects back to 3.0
def getLuceneVersion(checkout):
  checkoutPath = checkoutToPath(checkout)
  if os.path.isdir('%s/contrib/benchmark' % checkoutPath):
    return '3.0'
  elif os.path.isdir('%s/lucene/contrib/benchmark' % checkoutPath):
    return '3.x'
  elif os.path.isdir('%s/modules/benchmark' % checkoutPath):
    return '4.0'
  else:
    raise RuntimeError('cannot determine Lucene version for checkout %s' % checkoutPath)

def checkoutToPath(checkout):
  return '%s/%s' % (constants.BASE_DIR, checkout)

def checkoutToBenchPath(checkout):
  p = checkoutToPath(checkout)
  for subDir in ('contrib/benchmark', # pre-3.1
                 'lucene/contrib/benchmark', # 3.1
                 'modules/benchmark', # 4.0
                 ):
    fullPath = '%s/%s' % (p, subDir)
    if os.path.exists(fullPath):
      return fullPath
  raise RuntimeError('could not locate benchmark under %s' % p)
    
def nameToIndexPath(name):
  return '%s/%s/index' % (constants.INDEX_DIR_BASE, name)

reQuery = re.compile('^q=(.*?) s=(.*?) h=(.*?)$')
reHits = re.compile('^HITS q=(.*?) s=(.*?) tot=(.*?)$')
reHit = re.compile('^(\d+) doc=(?:doc)?(\d+) score=([0-9.]+)$')
reResult = re.compile(r'^(\d+) c=(.*?)$')
reChecksum = re.compile(r'checksum=(\d+)$')

DEBUG = True

# let shell find it:
#JAVA_COMMAND = 'java -Xms1g -Xmx1g -server'
#JAVA_COMMAND = 'java -Xms1024M -Xmx1024M -Xbatch -server -XX:+AggressiveOpts -XX:CompileThreshold=100 -XX:+UseFastAccessorMethods'

LOG_SUB_DIR = 'logs'

# for multi-segment index:
SEGS_PER_LEVEL = 5

def run(cmd, log=None):
  print 'RUN: %s' % cmd
  if os.system(cmd):
    if log is not None:
      print open(log).read()
    raise RuntimeError('failed: %s [wd %s]' % (cmd, os.getcwd()))

class Index:

  def __init__(self, checkout, dataSource, analyzer, codec, numDocs, numThreads, lineDocSource, doOptimize=False, dirImpl='NIOFSDirectory'):
    self.checkout = checkout
    self.analyzer = analyzer
    self.dataSource = dataSource
    self.codec = codec
    self.numDocs = numDocs
    self.numThreads = numThreads
    self.lineDocSource = lineDocSource
    self.doOptimize = doOptimize
    self.dirImpl = dirImpl
    
    mergeFactor = 10
    if SEGS_PER_LEVEL >= mergeFactor:
      raise RuntimeError('SEGS_PER_LEVEL (%s) is greater than mergeFactor (%s)' % (SEGS_PER_LEVEL, mergeFactor))
    

  def getName(self):
    if self.doOptimize:
      s = 'opt.'
    else:
      s = ''
    return '%s.%s.%s.%snd%gM' % (self.dataSource, self.checkout, self.codec, s, self.numDocs/1000000.0)

class RunAlgs:

  def __init__(self, javaCommand):
    self.logCounter = 0
    self.results = []
    self.compiled = set()
    self.javaCommand = javaCommand
    print
    print 'JAVA:\n%s' % os.popen('java -version 2>&1').read()
    
    print
    if osName not in ('windows', 'cygwin'):
      print 'OS:\n%s' % os.popen('uname -a 2>&1').read()
    else:
      print 'OS:\n%s' % sys.platform
    
  def printEnv(self):
    print
    print 'JAVA:\n%s' % os.popen('%s -version 2>&1' % self.javaCommand).read()

    print
    if osName not in ('windows', 'cygwin'):
      print 'OS:\n%s' % os.popen('uname -a 2>&1').read()
    else:
      print 'OS:\n%s' % sys.platform

  def makeIndex(self, index):

    fullIndexPath = nameToIndexPath(index.getName())
    if os.path.exists(fullIndexPath):
      print 'Index %s already exists...' % fullIndexPath
      return fullIndexPath

    maxBufferedDocs = index.numDocs / (SEGS_PER_LEVEL*111)
    # maxBufferedDocs = 10000000
    
    print 'Now create index %s...' % fullIndexPath

    try:
      if index.doOptimize:
        opt = 'yes'
      else:
        opt = 'no'
      
      cmd = '%s -classpath "%s" perf.Indexer %s "%s" %s %s %s %s %s %s %s %s %s' % \
            (self.javaCommand,
             self.classPathToString(self.getClassPath(index.checkout)),
             index.dirImpl,
             fullIndexPath,
             index.analyzer,
             index.lineDocSource,
             index.numDocs,
             index.numThreads,
             opt,
             'yes',
             -1,
             maxBufferedDocs,
             index.codec)
      logDir = '%s/%s' % (checkoutToBenchPath(index.checkout), LOG_SUB_DIR)
      if not os.path.exists(logDir):
        os.makedirs(logDir)
      fullLogFile = '%s/%s.log' % (logDir, index.getName())
      print '  log %s' % fullLogFile
      if DEBUG:
        print 'command=%s' % cmd

      cmd += ' > "%s" 2>&1' % fullLogFile

      t0 = time.time()      
      if os.system(cmd) != 0:
        raise RuntimeError('FAILED')
      t1 = time.time()

    except:
      if os.path.exists(fullIndexPath):
        shutil.rmtree(fullIndexPath)
      raise

    return fullIndexPath

  def getClassPath(self, checkout):
    path = checkoutToPath(checkout)
    cp = []
    version = getLuceneVersion(checkout)
    if version == '3.0':
      buildPath = '%s/build/classes' % path
    else:
      buildPath = '%s/lucene/build/classes' % path
    cp.append('%s/java' % buildPath)
    cp.append('%s/test' % buildPath)
    cp.append('%s/test-framework' % buildPath)

    if version == '4.0':
      cp.append('%s/modules/analysis/build/common/classes/java' % path)
      cp.append('%s/modules/analysis/build/icu/classes/java' % path)
    elif version == '3.x':
      cp.append('%s/lucene/build/contrib/analyzers/common/classes/java' % path)
    else:
      cp.append('%s/build/contrib/analyzers/common/classes/java' % path)

    # need benchmark path so perf.SearchPerfTest is found:
    cp.append(checkoutToBenchPath(checkout))

    return tuple(cp)

  def compile(self,competitor):
    path = checkoutToBenchPath(competitor.checkout)
    print 'COMPILE: %s' % path
    os.chdir(path)
    run('ant compile > compile.log 2>&1', 'compile.log')
    if path.endswith('/'):
      path = path[:-1]
      
    cp = self.classPathToString(self.getClassPath(competitor.checkout))
    if not os.path.exists('perf'):
      version = getLuceneVersion(competitor.checkout)
      if version == '3.0':
        subdir = '/30'
      elif version == '3.1':
        subdir = '/31'
      else:
        subdir = ''
      srcDir = '%s/perf%s' % (constants.BENCH_BASE_DIR, subdir)
      # TODO: change to just compile the code & run directly from util
      if osName in ('windows', 'cygwin'):
        run('cp -r %s perf' % srcDir)
      else:
        run('ln -s %s perf' % srcDir)
    competitor.compile(cp)
    
  def classPathToString(self, cp):
    return common.pathsep().join(cp)

  def runSimpleSearchBench(self, c, iters, itersPerJVM, threadCount, filter=None):
    benchDir = checkoutToBenchPath(c.checkout)
    os.chdir(benchDir)
    cp = self.classPathToString(self.getClassPath(c.checkout))
    logFile = '%s/res-%s.txt' % (benchDir, c.name)
    print 'log %s' % logFile
    if os.path.exists(logFile):
      os.remove(logFile)
    for iter in xrange(iters):
      command = '%s %s -classpath "%s" perf.SearchPerfTest %s %s %s %s %s %s' % \
          (self.javaCommand, c.taskRunProperties(), cp, c.dirImpl, nameToIndexPath(c.index.getName()), c.analyzer, threadCount, itersPerJVM, c.commitPoint)
      if filter is not None:
        command += ' %s %.2f' % filter
      run('%s >> %s' % (command, logFile))
    return logFile

  def simpleReport(self, baseLogFile, cmpLogFile, jira=False, html=False, baseDesc='Standard', cmpDesc=None):
    base, hitsBase = getSimpleResults(baseLogFile)
    cmp, hitsCMP = getSimpleResults(cmpLogFile)

    compareHits(hitsBase, hitsCMP)
    
    allQueries = set()

    for (q, s), t in base.items():
      allQueries.add(q)

    # for s in ('null', '<long: "docdatenum">'):
    for s in ('null',):

      lines = []
      w = sys.stdout.write

      if s == 'null':
        sp = 'score'
      else:
        sp = 'date'
        
      w('\nNOTE: SORT BY %s\n\n' % sp)

      if jira:
        w('||Query||QPS %s||QPS %s||Pct diff||' % (baseDesc, cmpDesc))
      elif html:
        w('<table>')
        w('<tr>')
        w('<th>Query</th>')
        w('<th>QPS %s</th>' % baseDesc)
        w('<th>QPS %s</th>' % cmpDesc)
        w('<th>%% change</th>')
        w('</tr>')
      else:
        w('%20s' % 'Query')
        w('%12s' % ('QPS %s' % baseDesc))
        w('%12s' % ('QPS %s' % cmpDesc))
        w('%10s' % 'Pct diff')

      if jira:
        w('||\n')
      else:
        w('\n')

      l2 = list(allQueries)
      l2.sort()

      # TODO: assert checksums agree across versions

      warnings = []

      lines = []
      wOrig = w

      for q in l2:
        l0 = []
        w = l0.append
        qs = q.replace('body:', '').replace('*:*', '<all>')
        if jira:
          w('|%s' % qs)
        elif html:
          w('<tr>')
          w('<td>%s</td>' % htmlEscape(qs))
        else:
          w('%20s' % qs)

        tCmp, hitCount, check = cmp[(q, s)]
        tBase, hitCount2, check2 = base[(q, s)]

        tCmp /= 1000000.0
        tBase /= 1000000.0

        qpsCmp = 1000.0/tCmp
        qpsBase = 1000.0/tBase

        if hitCount != hitCount2:
          warnings.append('q=%s sort=%s: hit counts differ: %s vs %s' % (q, s, hitCount, hitCount2))
          #raise RuntimeError('hit counts differ: %s vs %s' % (hitCount, hitCount2))
        if qpsCmp > qpsBase:
          color = 'green'
          sign = -1
        else:
          color = 'red'
          sign = 1

        ps = 100.0*(qpsCmp - qpsBase)/qpsBase

        if jira:
          w('|%.2f|%.2f' % (qpsBase, qpsCmp))
        elif html:
          w('<td>%.2f</td><td>%.2f</td>' % (qpsBase, qpsCmp))
        else:
          w('%12.2f%12.2f'% (qpsBase, qpsCmp))

        if jira:
          w('|{color:%s}%.1f%%{color}' % (color, ps))
        elif html:
          w('<td><font color=%s>%.1f%%</font></td>' % (color, ps))
        else:
          w('%10s' % ('%.1f%%' % ps))

        if jira:
          w('|\n')
        else:
          w('\n')

        if constants.SORT_REPORT_BY == 'pctchange':
          lines.append((ps, ''.join(l0)))
        elif constants.SORT_REPORT_BY == 'query':
          lines.append((qs, ''.join(l0)))
        else:
          raise RuntimeError('invalid result sort %s' % constant.SORT_REPORT_BY)

      lines.sort()

      w = wOrig
      for ign, s in lines:
        w(s)

      if html:
        w('</table>')

      for w in warnings:
        print 'WARNING: %s' % w
    
  def compare(self, baseline, newList, *params):

    for new in newList:
      if new.numHits != baseline.numHits:
        raise RuntimeError('baseline found %d hits but new found %d hits' % (baseline[0], new[0]))

      warmOld = baseline.warmTime
      warmNew = new.warmTime
      qpsOld = baseline.bestQPS
      qpsNew = new.bestQPS
      pct = 100.0*(qpsNew-qpsOld)/qpsOld
      #print '  diff: %.1f%%' % pct

      pct = 100.0*(warmNew-warmOld)/warmOld
      #print '  warmdiff: %.1f%%' % pct

    self.results.append([baseline] + [newList] + list(params))

  def save(self, name):
    f = open('%s.pk' % name, 'wb')
    cPickle.dump(self.results, f)
    f.close()

reFuzzy = re.compile(r'body:(.*?)\~(.*?)$')

# converts unit~0.7 -> unit~1
def fixupFuzzy(query):
  m = reFuzzy.search(query)
  if m is not None:
    term, fuzzOrig = m.groups()
    fuzz = float(fuzzOrig)
    if fuzz < 1.0:
      editDistance = int((1.0-fuzz)*len(term))
      query = query.replace('~%s' % fuzzOrig, '~%s.0' % editDistance)
  return query
  
# cleans up s=<long: "docdatenum">(org.apache.lucene.search.cache.LongValuesCreator@286fbcd7) to remove the (...)s
reParens = re.compile(r'\(.*?\)')

def getSimpleResults(fname):
  results = {}

  start = False
  best = None
  count = 0
  lastCheck = None
  hits = {}
  totCS = None
    
  for l in open(fname).readlines():

    l = l.strip()

    m = reHits.match(l)
    if m is not None:
      query, sort, hitCount = m.groups()
      query = fixupFuzzy(query)
      sort = reParens.sub('', sort)
      hitList = []
      hits[(query, sort)] = (hitCount, hitList)

    m = reHit.match(l)
    if m is not None:
      hitList.append(m.groups()[1:])
      
    if not start:
      if l == 'ns by query/coll:':
        start = True
      continue

    m = reChecksum.match(l)
    if m is not None:
      s = m.group(1)
      if totCS is None:
        totCS = s
      elif totCS != s:
        # checksum is sum of all docIDs hit in all searches; make sure
        # all threads match
        raise RuntimeError('internal total checksum diff %s vs %s' % (totCS, s))
      
    if l.startswith('q='):
      if best is not None:
        results[(query, sort)] = best, hitCount, check
        lastCheck = None
        best = None
      query, sort, hitCount = reQuery.match(l).groups()
      sort = reParens.sub('', sort)
    elif l.startswith('t='):
      count = 0
    else:
      if l.endswith(' **'):
        l = l[:-3]
      m = reResult.match(l)
      if m is not None:
        t = long(m.group(1))
        check = long(m.group(2))
        if lastCheck is None:
          lastCheck = check
        elif lastCheck != check:
          raise RuntimeError('internal checksum diff %s vs %s within query=%s sort=%s' % (check, lastCheck, query, sort))
        count += 1
        if count > 3 and (best is None or t < best):
          best = t

  if len(hits) == 0:
    raise RuntimeError("didn't see any hits")
  results[(query, sort)] = best, hitCount, check
  return results, hits

def cleanScores(l):
  for i in range(len(l)):
    pos = l[i].find(' score=')
    l[i] = l[i][:pos].strip()
  
def verify(r1, r2):
  if r1.numHits != r2.numHits:
    raise RuntimeError('different total hits: %s vs %s' % (r1.numHits, r2.numHits))
                       
  h1 = r1.hits
  h2 = r2.hits
  if len(h1) != len(h2):
    raise RuntimeError('different number of results')
  else:
    for i in range(len(h1)):
      s1 = h1[i].replace('score=NaN', 'score=na').replace('score=0.0', 'score=na')
      s2 = h2[i].replace('score=NaN', 'score=na').replace('score=0.0', 'score=na')
      if s1 != s2:
        raise RuntimeError('hit %s differs: %s vs %s' % (i, repr(s1), repr(s2)))

def compareHits(r1, r2):
  for (query, sort), (totCount, hits) in r1.items():
    if (query, sort) not in r2:
      raise RuntimeError('HITS: q=%s s=%s is missing' % (query, sort))
    else:
      #print 'COMPARE q=%s' % query
      totCount2, hits2 = r2[(query, sort)]
      #print '  HITS %s vs %s' % (totCount, totCount2)
      if totCount != totCount2:
        raise RuntimeError('HITS: q=%s s=%s totCount differs: %s vs %s' % (query, sort, totCount, totCount2))

      if len(hits) != len(hits2):
        raise RuntimeError('HITS: q=%s s=%s top N count differs: %s vs %s' % (query, sort, len(hits), len(hits2)))

      for i in range(len(hits)):
        if hits[i][0] != hits2[i][0]:
          raise RuntimeError('HITS: q=%s s=%s: different hit: i=%s hit1=%s hit2=%s' % (query, sort, i, hits[i], hits2[i]))
        if abs(float(hits[i][1]) - float(hits2[i][1])) > 0.00001:
          raise RuntimeError('HITS: q=%s s=%s: different hit: i=%s hit1=%s hit2=%s' % (query, sort, i, hits[i], hits2[i]))
        
def htmlEscape(s):
  return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def getSegmentCount(index):
  segCount = 0
  for fileName in os.listdir(nameToIndexPath(index.getName())):
    if fileName.endswith('.tis') or fileName.endswith('.tib'):
      segCount += 1
  return segCount
