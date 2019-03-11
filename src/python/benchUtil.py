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

import math
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
import random
import signal
import QPSChart
import IndexChart
import subprocess
  
# Skip the first N runs of a given category (cold) or particular task (hot):
WARM_SKIP = 3

# Skip this pctg of the slowest runs:
SLOW_SKIP_PCT = 10

# From the N times we run each task in a single JVM, how do we pick
# the single QPS to represent those results:

# SELECT = 'min'
# SELECT = 'mean'
SELECT = 'median'

MAX_SCORE_DIFF = .00001

VERBOSE = False

DO_PERF = constants.DO_PERF

PERF_STATS = constants.PERF_STATS

osName = common.osName

# returns an array of all java files in a directory; walks the directory tree
def addFiles(root):
  files = []
  for f in os.listdir(root):
    f = os.path.join(root, f).replace("\\","/")
    if os.path.isdir(f):
      files.extend(addFiles(f))
    elif not f.startswith('.#') and f.endswith('.java'):
      files.append(f)
  return files

def htmlColor(v):
  if v < 0:
    return '<font color="red">%d%%</font>' % (-v)
  else:
    return '<font color="green">%d%%</font>' % v

def htmlColor2(v):
  if v < 1.0:
    return '<font color="red">%.1f X</font>' % v
  else:
    return '<font color="green">%.1f X</font>' % v

def jiraColor(v):
  if v < 0:
    return '{color:red}%d%%{color}' % (-v)
  else:
    return '{color:green}%d%%{color}' % v

def getArg(argName, default, hasArg=True):
  try:
    idx = sys.argv.index(argName)
  except ValueError:
    v = default
  else:
    if hasArg:
      v = sys.argv[idx+1]
      del sys.argv[idx:idx+2]
      try:
        sys.argv.index(argName)
      except ValueError:
        # ok
        pass
      else:
        raise RuntimeError('argument %s appears more than once' % argName)
    else:
      v = True
      del sys.argv[idx]
  return v

# NOTE: only detects back to 3.0
def getLuceneVersion(checkout):
  checkoutPath = checkoutToPath(checkout)
  if os.path.isdir('%s/contrib/benchmark' % checkoutPath):
    return '3.0'
  elif os.path.isdir('%s/lucene/contrib/benchmark' % checkoutPath):
    return '3.x'
  elif os.path.isdir('%s/lucene/benchmark' % checkoutPath):
    return '4.0'
  else:
    raise RuntimeError('cannot determine Lucene version for checkout %s' % checkoutPath)

def checkoutToPath(checkout):
  return '%s/%s' % (constants.BASE_DIR, checkout)

def checkoutToBenchPath(checkout):
  return '%s/lucene/benchmark' % checkoutToPath(checkout)

def checkoutToUtilPath(checkout):
  p = checkoutToPath(checkout)
  if os.path.exists('%s/luceneutil' % p):
    # This checkout has a 'private' luceneutil:
    compPath = '%s/luceneutil' % p
  else:
    compPath = constants.BENCH_BASE_DIR
  return compPath

def nameToIndexPath(name):
  return '%s/%s' % (constants.INDEX_DIR_BASE, name)

class SearchTask:
  # TODO: subclass SearchGroupTask

  def verifySame(self, other, verifyScores, verifyCounts):
    if not isinstance(other, SearchTask):
      self.fail('not a SearchTask (%s)' % other)
    if self.query != other.query:
      self.fail('wrong query: %s vs %s' % (self.query, other.query))
    if self.sort != other.sort:
      self.fail('wrong sort: %s vs %s' % (self.sort, other.sort))
    if self.groupField is None:
      if False:
        # TODO: fix SearchPerfTest -- cannot use term count across threads since mutiple threads store in the query
        if self.expandedTermCount != other.expandedTermCount:
          print 'WARNING: expandedTermCounts differ for %s: %s vs %s' % (self, self.expandedTermCount, other.expandedTermCount)
          # self.fail('wrong expandedTermCount: %s vs %s' % (self.expandedTermCount, other.expandedTermCount))

      if False and verifyCounts and self.hitCount != other.hitCount:
        self.fail('wrong hitCount: %s vs %s' % (self.hitCount, other.hitCount))

      if len(self.hits) != len(other.hits):
        self.fail('wrong top hit count: %s vs %s' % (len(self.hits), len(other.hits)))

      if verifyScores:
        # Collapse equals... this is sorta messy, but necessary because we
        # do not dedup by true id in SearchPerfTest
        hitsSelf = collapseDups(self.hits)
        hitsOther = collapseDups(other.hits)

        if verifyCounts and len(hitsSelf) != len(hitsOther):
          self.fail('self=%s: wrong collapsed hit count: %s vs %s\n  %s vs %s\n  %s vs %s' % (self, len(hitsSelf), len(hitsOther), hitsSelf, hitsOther, self.hits, other.hits))

        if verifyScores:
          for i in xrange(len(hitsSelf)):
            if hitsSelf[i][1] != hitsOther[i][1]:
              if False:
                if abs(float(hitsSelf[i][1])-float(hitsOther[i][1])) > MAX_SCORE_DIFF:
                  self.fail('hit %s has wrong field/score value %s vs %s' % (i, hitsSelf[i][1], hitsOther[i][1]))
                else:
                  print 'WARNING: query=%s filter=%s sort=%s: slight score diff %s vs %s' % \
                        (self.query, self.filter, self.sort, hitsSelf[i][1], hitsOther[i][1])
              else:
                self.fail('hit %s has wrong field/score value %s vs %s' % (i, hitsSelf[i][1], hitsOther[i][1]))
            if hitsSelf[i][0] != hitsOther[i][0] and i < len(hitsSelf)-1:
              self.fail('hit %s has wrong id/s %s vs %s' % (i, hitsSelf[i][0], hitsOther[i][0]))
    else:
      # groups
      if self.groupCount != other.groupCount:
        self.fail('wrong groupCount: cat=%s groupField=%s %s vs %s: self=%s, other=%s' % (self.cat, self.groupField, self.groupCount, other.groupCount, self, other))
      for groupIDX in xrange(self.groupCount):
        groupValue1, groupTotHits1, groupTopScore1, groups1 = self.groups[groupIDX]
        groupValue2, groupTotHits2, groupTopScore2, groups2 = other.groups[groupIDX]

        # TODO: if we have 1 pass and 2 pass on the "same" group field, assert same

        # TODO: this is because block grouping doesn't pull group
        # values; conditionalize this on block grouping
        if False and groupValue1 != groupValue2:
          self.fail('group %d has wrong groupValue: %s vs %s' % (groupIDX, groupValue1, groupValue2))

        # iffy: this is a float cmp
        if verifyScores:
          if groupTopScore1 != groupTopScore2:
            self.fail('group %d has wrong groupTopScor: %s vs %s' % (groupIDX, groupTopScore1, groupTopScore2))

          if groupTotHits1 != groupTotHits2:
            self.fail('group %d has wrong totHits: %s vs %s' % (groupIDX, groupTotHits1, groupTotHits2))

          if len(groups1) != len(groups2):
            self.fail('group %d has wrong number of docs: %s vs %s' % (groupIDX, len(groups1), len(groups2)))

          groups1 = collapseDups(groups1)
          groups2 = collapseDups(groups2)

          for docIDX in xrange(len(groups1)):
            if groups1[docIDX][1] != groups2[docIDX][1]:
              self.fail('hit %s has wrong field/score value %s vs %s' % (docIDX, groups1[docIDX][1], groups2[docIDX][1]))
            if groups1[docIDX][0] != groups2[docIDX][0] and docIDX < len(groups1)-1:
              self.fail('hit %s has wrong id/s %s vs %s' % (docIDX, groups1[docIDX][0], groups2[docIDX][0]))

    if self.facets != other.facets:
      if False:
        print
        print '***WARNING*** facet diffs: %s: %s vs %s'% (self, self.facets, other.facets)
        print
      else:
        self.fail('facets differ: %s vs %s' % (self.facets, other.facets))
    
  def fail(self, message):
    s = 'query=%s filter=%s sort=%s groupField=%s hitCount=%s' % (self.query, self.filter, self.sort, self.groupField, self.hitCount)
    raise RuntimeError('%s: %s' % (s, message))

  def __str__(self):
    s = self.query
    if self.sort is not None:
      s += ' [sort=%s]' % self.sort
    if self.groupField is not None:
      s += ' [groupField=%s]' % self.groupField
    if self.facets is not None:
      s += ' [facets=%s]' % self.facets
    return s

  def __eq__(self, other):
    if not isinstance(other, SearchTask):
      return False
    else:
      return self.query == other.query and self.sort == other.sort and self.groupField == other.groupField and self.filter == other.filter and self.facets == other.facets

  def __hash__(self):
    return hash(self.query) + hash(self.sort) + hash(self.groupField) + hash(self.filter) + hash(type(self.facets))
      
  
class RespellTask:
  cat = 'Respell'

  def verifySame(self, other, verifyScores, verifyCounts):
    if not isinstance(other, RespellTask):
      self.fail('not a RespellTask')
    if self.term != other.term:
      self.fail('wrong term: %s vs %s' % (self.term, other.term))
    if self.hits != other.hits:
      self.fail('wrong hits: %s vs %s' % (self.hits, other.hits))

  def fail(self, message):
    raise RuntimeError('respell: term=%s: %s' % (self.term, message))

  def __str__(self):
    return 'Respell %s' % self.term

  def __eq__(self, other):
    if not isinstance(other, RespellTask):
      return False
    else:
      return self.term == other.term

  def __hash__(self):
    return hash(self.term)

    
class PKLookupTask:
  cat = 'PKLookup'

  def verifySame(self, other, verifyScores, verifyCounts):
    # already "verified" in search perf test, ie, that the docID
    # returned in fact has the id that was asked for
    pass

  def __str__(self):
    return 'PK%s' % self.pkOrd

  def __eq__(self, other):
    if not isinstance(other, PKLookupTask):
      return False
    else:
      return self.pkOrd == other.pkOrd

  def __hash__(self):
    return hash(self.pkOrd)

class PointsPKLookupTask:
  cat = 'PointsPKLookup'

  def verifySame(self, other, verifyScores, verifyCounts):
    # already "verified" in search perf test, ie, that the docID
    # returned in fact has the id that was asked for
    pass

  def __str__(self):
    return 'PointsPK%s' % self.pkOrd

  def __eq__(self, other):
    if not isinstance(other, PointsPKLookupTask):
      return False
    else:
      return self.pkOrd == other.pkOrd

  def __hash__(self):
    return hash(self.pkOrd)

def collapseDups(hits):
  newHits = []
  for id, v in hits:
    if len(newHits) == 0 or v != newHits[-1][1]:
      newHits.append(([id], v))
    else:
      newHits[-1][0].append(id)
      newHits[-1][0].sort()
  return newHits

reSearchTaskOld = re.compile('cat=(.*?) q=(.*?) s=(.*?) group=null hits=(null|[0-9]+\+?) facets=(.*?)$')
reSearchGroupTaskOld = re.compile('cat=(.*?) q=(.*?) s=(.*?) group=(.*?) groups=(.*?) hits=([0-9]+\+?) groupTotHits=([0-9]+)(?: totGroupCount=(.*?))? facets=(.*?)$', re.DOTALL)
reSearchTask = re.compile('cat=(.*?) q=(.*?) s=(.*?) f=(.*?) group=null hits=(null|[0-9]+\+?)$')
reSearchGroupTask = re.compile('cat=(.*?) q=(.*?) s=(.*?) f=(.*?) group=(.*?) groups=(.*?) hits=([0-9]+\+?) groupTotHits=([0-9]+)(?: totGroupCount=(.*?))?$', re.DOTALL)
reSearchHitScore = re.compile('doc=(.*?) score=(.*?)$')
reSearchHitField = re.compile('doc=(.*?) .*?=(.*?)$')
reRespellHit = re.compile('(.*?) freq=(.*?) score=(.*?)$')
rePKOrd = re.compile(r'PK(.*?)\[')
reOneGroup = re.compile('group=(.*?) totalHits=(.*?)(?: hits)? groupRelevance=(.*?)$', re.DOTALL)
reHeap = re.compile('HEAP: ([0-9]+)$')

def parseResults(resultsFiles):
  taskIters = []
  heaps = []
  for resultsFile in resultsFiles:
    tasks = []

    if not os.path.exists(resultsFile):
      continue

    if os.path.exists(resultsFile + '.stdout') and os.path.getsize(resultsFile + '.stdout') > 10*1024:
      raise RuntimeError('%s.stdout is %d bytes; leftover System.out.println?' % (resultsFile, os.path.getsize(resultsFile + '.stdout')))
    
    # print 'parse %s' % resultsFile
    f = open(resultsFile, 'rb')
    while True:
      line = f.readline()
      if line == '':
        break
      line = line.strip()

      if line.startswith('HEAP: '):
        m = reHeap.match(line)
        heaps.append(int(m.group(1)))

      if line.startswith('TASK: cat='):
        task = SearchTask()
        task.msec = float(f.readline().strip().split()[0])
        task.threadID = int(f.readline().strip().split()[1])
        task.facets = None

        m = reSearchTask.match(line[6:])

        if m is not None:
          cat, task.query, sort, filter, hitCount = m.groups()
        else:
          m = reSearchTaskOld.match(line[6:])
          if m is not None:
            cat, task.query, sort, hitCount, facets = m.groups()
            filter = None
          else:
            cat = None

        if cat is not None:
          task.cat = cat
          task.groups = None
          task.groupField = None
          task.filter = filter
          # print 'CAT %s' % cat

          if hitCount == 'null':
            task.hitCount = "0"
          else:
            task.hitCount = hitCount
          if sort == '<string: "title">' or sort == '<string: "titleDV">':
            task.sort = 'Title'
          elif sort.startswith('<long: "datenum">') or sort.startswith('<long: "lastModNDV">'):
            task.sort = 'DateTime'
          elif sort == '<string: "monthSortedDV">':
            task.sort = 'Month'
          elif sort == '<int: "dayOfYearNumericDV">':
            task.sort = 'DayOfYear'
          elif sort != 'null':
            raise RuntimeError('could not parse sort: %s' % sort)
          else:
            task.sort = None

          task.hits = []
          task.expandedTermCount = 0

          while True:
            line = f.readline().strip()
            if line == '':
              break

            if task.facets is not None:
              task.facets.append(line)
              continue
            
            if line.find('expanded terms') != -1:
              task.expandedTermCount = int(line.split()[0])
              continue
            if line.find('Zing VM Warning') != -1:
              continue
            if line.find('facets') != -1:
              task.facets = []
              continue
            if line.find('hilite time') != -1:
              task.hiliteMsec = float(line.split()[2])
              continue
            if line.find('getFacetResults time') != -1:
              task.getFacetResultsMsec = float(line.split()[2])
              continue
            
            if line.startswith('HEAP: '):
              m = reHeap.match(line)
              heaps.append(int(m.group(1)))
              break

            if sort == 'null':
              m = reSearchHitScore.match(line)
              id = int(m.group(1))
              score = m.group(2)
              # score stays a string so we can do "precise" ==
              task.hits.append((id, score))
            else:
              m = reSearchHitField.match(line)
              id = int(m.group(1))
              field = m.group(2)
              task.hits.append((id, field))
        else:
          m = reSearchGroupTask.match(line[6:])
          if m is not None:
            cat, task.query, sort, filter, task.groupField, groupCount, hitCount, groupedHitCount, totGroupCount = m.groups()
          else:
            m = reSearchGroupTaskOld.match(line[6:])
            if m is not None:
              cat, task.query, sort, task.groupField, groupCount, hitCount, groupedHitCount, totGroupCount, facets = m.groups()
              filter = None

          if cat is not None:
            task.cat = cat
            task.hits = hitCount
            task.hitCount = hitCount
            task.groupedHitCount = groupedHitCount
            task.groupCount = int(groupCount)
            task.filter = filter
            if totGroupCount in (None, 'null'):
              task.totGroupCount = None
            else:
              task.totGroupCount = int(totGroupCount)
            # TODO: handle different sorts
            task.sort = None
            task.groups = []
            group = None
            while True:
              line = f.readline().strip()
              if line == '':
                break
              if line.find('Zing VM Warning') != -1:
                continue
              if line.startswith('HEAP: '):
                m = reHeap.match(line)
                heaps.append(int(m.group(1)))
                break
              m = reOneGroup.search(line)
              if m is not None:
                groupValue, groupTotalHits, groupTopScore = m.groups()
                group = (groupValue, int(groupTotalHits), float(groupTopScore), [])
                task.groups.append(group)
                continue
              m = reSearchHitScore.search(line)
              if m is not None:
                doc = int(m.group(1))
                score = float(m.group(2))
                group[-1].append((doc, score))
              else:
                # BUG
                raise RuntimeError('result parsing failed: line=%s' % line)
          elif line.find('Zing VM Warning') != -1:
            continue
          else:
            raise RuntimeError('result parsing failed: line=%s' % line)
      elif line.startswith('TASK: respell'):
        task = RespellTask()
        task.msec = float(f.readline().strip().split()[0])
        task.threadID = int(f.readline().strip().split()[1])
        task.term = line[14:]

        task.hits = []
        while True:
          line = f.readline().strip()
          if line == '':
            break
          if line.find('Zing VM Warning') != -1:
            continue
          if line.startswith('HEAP: '):
            m = reHeap.match(line)
            heaps.append(int(m.group(1)))
            break
          m = reRespellHit.search(line)
          suggest, freq, score = m.groups()
          task.hits.append((suggest, int(freq), float(score)))

      elif line.startswith('TASK: PK'):
        task = PKLookupTask()
        task.pkOrd = rePKOrd.search(line).group(1)
        task.msec = float(f.readline().strip().split()[0])
        task.threadID = int(f.readline().strip().split()[1])
      elif line.startswith('TASK: PointsPK'):
        task = PointsPKLookupTask()
        task.pkOrd = rePKOrd.search(line).group(1)
        task.msec = float(f.readline().strip().split()[0])
        task.threadID = int(f.readline().strip().split()[1])
      else:
        task = None
        if line.find('\tat') != -1:
          raise RuntimeError('log has exceptions')

      if task is not None:
        tasks.append(task)

    taskIters.append(tasks)

  return taskIters, heaps

def collateResults(resultIters):
  iters = []
  for results in resultIters:
    # Keyed first by category (Fuzzy1, Respell, ...) and 2nd be exact
    # task mapping to list of tasks.  For a cold run (no task repeats)
    # the 2nd map will always map to a length-1 list.
    byCat = {}
    iters.append(byCat)
    for task in results:
      if isinstance(task, SearchTask):
        key = task.cat, task.sort
      else:
        key = task.cat
      if key not in byCat:
        byCat[key] = ([], {})
      l, d = byCat[key]
      l.append(task)
      if task not in d:
        d[task] = [task]
      else:
        d[task].append(task)

  return iters

def agg(iters, cat, name):

  bestAvgMS = None
  lastHitCount = None

  accumMS = []
  totHitCount = 0
  
  # Iterate over each JVM instance we ran
  for tasksByCat in iters:

    # Maps cat -> actual tasks ran
    if cat not in tasksByCat:
      continue
    tasks = tasksByCat[cat]

    if len(tasks[0]) <= WARM_SKIP:
      raise RuntimeError('only %s tasks in cat %s' % (len(tasks[0]), cat))

    totHitCount = 0
    count = 0
    sumMS = 0.0
    if VERBOSE:
      print 'AGG: cat=%s' % str(cat)

    # Iterate over each category's instances, eg a given category
    # might have 5 different instances:
    for task, results in tasks[1].items():

      allMS = [result.msec for result in results]
      if VERBOSE:
        print '  %s' % task
        print '    before prune:'
        for t in allMS:
          print '      %.4f' % t

      # Skip warmup runs
      allMS = allMS[WARM_SKIP:]

      allMS.sort()
      minMS = allMS[0]
      if VERBOSE:
        print '    after sort:'
        for t in allMS:
          print '      %.4f' % t

      # Skip slowest SLOW_SKIP_PCT runs:
      skipSlowest = int(len(allMS)*SLOW_SKIP_PCT/100.)
      if VERBOSE:
        print 'skipSlowest %s' % skipSlowest

      if skipSlowest > 0:
        pruned = allMS[:-skipSlowest]
      else:
        pruned = allMS

      if VERBOSE:
        print '    after prune:'
        for t in pruned:
          print '      %.4f' % t
          
      if SELECT == 'min':
        sumMS += minMS
        count += 1
      elif SELECT == 'mean':
        sumMS += sum(pruned)
        count += len(pruned)
      elif SELECT == 'median':
        mid = len(pruned)/2
        if len(pruned) % 2 == 0:
          median = (pruned[mid-1] + pruned[mid])/2.0
        else:
          median = pruned[mid]
        if VERBOSE:
          print '  median %.4f' % median
        sumMS += median
        count += 1
      else:
        raise RuntimeError('unrecognized SELECT=%s: should be min, median or mean' % SELECT)
          
      if isinstance(task, SearchTask):
        if task.groupField is None:
          totHitCount = sum_hit_count(totHitCount, task.hitCount)
        else:
          for group in task.groups:
            totHitCount += group[1]

    # AvgMS per query in category, eg if we ran 5 different queries in
    # each cat, then this is AvgMS for query in that cat:
    avgMS = sumMS/count
    accumMS.append(avgMS)
    
    if lastHitCount is None:
      lastHitCount = totHitCount
    elif totHitCount != lastHitCount:
      raise RuntimeError('different hit counts: %s vs %s' % (lastHitCount, totHitCount))

  if VERBOSE:
    #accumMS.sort()
    minValue = min(accumMS)
    #print '  accumMS=%s' % ' '.join(['%5.1f' % x for x in accumMS])
    print '  %s %s: accumMS=%s' % (name, cat[0], ' '.join(['%5.1f' % (100.0*(x-minValue)/minValue) for x in accumMS]))
    
  return accumMS, totHitCount

def sum_hit_count(hc1, hc2):
  lower_bound = False
  if isinstance(hc1, basestring) and hc1.endswith('+'):
    lower_bound = True
    hc1 = int(hc1[:-1])
  else:
    hc1 = int(hc1)
  if isinstance(hc2, basestring) and hc2.endswith('+'):
    lower_bound = True
    hc2 = int(hc2[:-1])
  else:
    hc2 = int(hc2)
  return str(hc1+hc2) + (lower_bound and "+" or "")

def stats(l):
  sum = 0
  sumSQ = 0
  for v in l:
    sum += v
    sumSQ += v*v

  # min, max, mean, stddev
  if len(l) == 0:
    return 0.0, 0.0, 0.0, 0.0
  else:
    return min(l), max(l), sum/len(l), math.sqrt(len(l)*sumSQ - sum*sum)/len(l)

def run(cmd, logFile=None, indent='    '):
  print '%s[RUN: %s, cwd=%s]' % (indent, cmd, os.getcwd())
  if logFile is not None:
    cmd = '%s > "%s" 2>&1' % (cmd, logFile)
  if os.system(cmd):
    if logFile is not None and os.path.getsize(logFile) < 50*1024:
      print open(logFile).read()
    raise RuntimeError('failed: %s [wd %s]; see logFile %s' % (cmd, os.getcwd(), logFile))

reCoreJar = re.compile('lucene-core-[0-9]\.[0-9]\.[0-9](?:-SNAPSHOT)?\.jar')

class RunAlgs:

  def __init__(self, javaCommand, verifyScores, verifyCounts):
    self.logCounter = 0
    self.results = []
    self.compiled = set()
    self.javaCommand = javaCommand
    self.verifyScores = verifyScores
    self.verifyCounts = verifyCounts
    print
    print 'JAVA:\n%s' % os.popen('%s -version 2>&1' % javaCommand).read()
    
    print
    if osName not in ('windows', 'cygwin'):
      print 'OS:\n%s' % os.popen('uname -a 2>&1').read()
    else:
      print 'OS:\n%s' % sys.platform
    
    if not os.path.exists(constants.LOGS_DIR):
      os.makedirs(constants.LOGS_DIR)
    print  
    print 'LOGS:\n%s' % constants.LOGS_DIR

    
  def printEnv(self):
    print
    print 'JAVA:\n%s' % os.popen('%s -version 2>&1' % self.javaCommand).read()

    print
    if osName not in ('windows', 'cygwin'):
      print 'OS:\n%s' % os.popen('uname -a 2>&1').read()
    else:
      print 'OS:\n%s' % sys.platform

  def makeIndex(self, id, index, printCharts=False):

    fullIndexPath = nameToIndexPath(index.getName())
    if os.path.exists(fullIndexPath) and not index.doUpdate:
      print '  %s: already exists' % fullIndexPath
      return fullIndexPath
    elif index.doUpdate:
      if not os.path.exists(fullIndexPath):
        raise RuntimeError('index path does not exists: %s' % fullIndexPath)
      print '  %s: now update' % fullIndexPath
    else:
      print '  %s: now create' % fullIndexPath

    s = checkoutToBenchPath(index.checkout)
    print '    cd %s' % s
    os.chdir(s)
    
    try:

      cmd = []
      w = cmd.append
      w(index.javaCommand)
      w('-classpath "%s"' % self.classPathToString(self.getClassPath(index.checkout)))
      w('perf.Indexer')
      w('-dirImpl %s' % index.directory)
      w('-indexPath "%s"' % fullIndexPath)
      w('-analyzer %s' % index.analyzer)
      w('-lineDocsFile %s' % index.lineDocSource)
      w('-docCountLimit %s' % index.numDocs)
      w('-threadCount %s' % index.numThreads)
      w('-maxConcurrentMerges %s' % index.maxConcurrentMerges)
      
      if index.addDVFields:
        w('-dvfields')

      if index.useCMS:
        w('-useCMS')
        
      if index.optimize:
        w('-forceMerge')

      if index.verbose:
        w('-verbose')

      w('-ramBufferMB %s' % index.ramBufferMB)
      w('-maxBufferedDocs %s' % index.maxBufferedDocs)
      w('-postingsFormat %s' % index.postingsFormat)

      if index.doDeletions:
        w('-deletions')

      if index.printDPS:
        w('-printDPS')

      if index.waitForMerges:
        w('-waitForMerges')

      w('-mergePolicy %s' % index.mergePolicy)

      if index.doUpdate:
        w('-update')

      if index.facets is not None:
        for tup in index.facets:
          w('-facets')
          w('"%s"' % ';'.join(tup))
        w('-facetDVFormat %s' % index.facetDVFormat)
        
      w('-idFieldPostingsFormat %s' % index.idFieldPostingsFormat)

      if index.grouping:
        w('-grouping')
      
      if index.useCFS:
        w('-cfs')

      if index.bodyTermVectors:
        w('-tvs')

      if index.bodyPostingsOffsets:
        w('-bodyPostingsOffsets')

      if index.bodyStoredFields:
        w('-store')

      if index.waitForCommit:
        w('-waitForCommit')

      if index.disableIOThrottle:
        w('-disableIOThrottle')

      cmd = ' '.join(cmd)

      fullLogFile = '%s/%s.%s.log' % (constants.LOGS_DIR, id, index.getName())
      
      print '    log %s' % fullLogFile

      t0 = time.time()
      run(cmd, fullLogFile)
      t1 = time.time()
      if printCharts and IndexChart.Gnuplot is not None:
        chart = IndexChart.IndexChart(fullLogFile, index.getName())
        chart.plot()

      with open(fullLogFile, 'rb') as f:
        while True:
          l = f.readline()
          if l == '':
            break
          if l.lower().find('exception in thread') != -1:
            raise RuntimeError('unhandled exceptions in log "%s"' % fullLogFile)

    except:
      # if we hit any exception/problem building the index, remove the
      # partially built index so we don't accidentally think we can
      # run with it:
      if os.path.exists(fullIndexPath):
        shutil.rmtree(fullIndexPath)
      raise

    return fullIndexPath, fullLogFile

  def addJars(self, cp, path):
    if os.path.exists(path):
      for f in os.listdir(path):
        if f.endswith('.jar'):
          cp.append('%s/%s' % (path, f))

  def getClassPath(self, checkout):
    path = checkoutToPath(checkout)
    cp = []
    version = getLuceneVersion(checkout)

    # We use the jar file for core to leverage the MR JAR
    core_jar_file = None
    #for filename in os.listdir('%s/lucene/build/core' % path):
    #  if reCoreJar.match(filename) is not None:
    #    core_jar_file = '%s/lucene/build/core/%s' % (path, filename)
    #    break
    #if core_jar_file is None:
    #  raise RuntimeError('can\'t find core JAR file in %s' % ('%s/lucene/build/core' % path))

    #cp.append(core_jar_file)
    cp.append('%s/lucene/build/core/classes/test' % path)
    cp.append('%s/lucene/build/sandbox/classes/java' % path)
    cp.append('%s/lucene/build/misc/classes/java' % path)
    cp.append('%s/lucene/build/facet/classes/java' % path)
    cp.append('/home/mike/src/lucene-c-boost/dist/luceneCBoost-SNAPSHOT.jar')
    if version == '4.0':
      cp.append('%s/lucene/build/analysis/common/classes/java' % path)
      cp.append('%s/lucene/build/analysis/icu/classes/java' % path)
      cp.append('%s/lucene/build/queryparser/classes/java' % path)
      cp.append('%s/lucene/build/grouping/classes/java' % path)
      cp.append('%s/lucene/build/suggest/classes/java' % path)
      cp.append('%s/lucene/build/highlighter/classes/java' % path)
      cp.append('%s/lucene/build/codecs/classes/java' % path)
      cp.append('%s/lucene/build/queries/classes/java' % path)
      self.addJars(cp, '%s/lucene/facet/lib' % path)
    elif version == '3.x':
      cp.append('%s/lucene/build/contrib/analyzers/common/classes/java' % path)
      cp.append('%s/lucene/build/contrib/spellchecker/classes/java' % path)
    else:
      cp.append('%s/build/contrib/analyzers/common/classes/java' % path)
      cp.append('%s/build/contrib/spellchecker/classes/java' % path)

    # so perf.* is found:
    lib = os.path.join(checkoutToUtilPath(checkout), "lib")
    for f in os.listdir(lib):
      if f.endswith('.jar'):
        cp.append(os.path.join(lib, f))
    cp.append(os.path.join(checkoutToUtilPath(checkout), "build"))
    
    return tuple(cp)

  compiledCheckouts = set()

  def compile(self, competitor):
    path = checkoutToBenchPath(competitor.checkout)
    cwd = os.getcwd()
    checkoutPath = checkoutToPath(competitor.checkout)
    try:
      if competitor.checkout not in self.compiledCheckouts:
        self.compiledCheckouts.add(competitor.checkout);
        # for core we build a JAR in order to benefit from the MR JAR stuff
        for module in ['core']:
          modulePath = '%s/lucene/%s' % (checkoutPath, module)
          os.chdir(modulePath)
          print '  %s...' % modulePath
          #run('%s jar' % constants.ANT_EXE, '%s/compile.log' % constants.LOGS_DIR)
        for module in ('suggest', 'highlighter', 'misc',
                       'analysis/common', 'grouping',
                       'codecs', 'facet', 'sandbox'):
           modulePath = '%s/lucene/%s' % (checkoutPath, module)
           os.chdir(modulePath)
           print '  %s...' % modulePath
           #run('%s jar' % constants.ANT_EXE, '%s/compile.log' % constants.LOGS_DIR)

          #modulePath = '%s/lucene/%s' % (checkoutPath, module)
          #classesPath = '%s/lucene/build/%s/classes/java' % (checkoutPath, module)
          # Try to be faster than ant; this may miss changes, e.g. a static final constant changed in core that is used in another module:
          #if common.getLatestModTime('%s/src/java' % modulePath) > common.getLatestModTime(classesPath, '.class'):
          #  print '  %s...' % modulePath
          ##  os.chdir(modulePath)
          #  #run('%s compile' % constants.ANT_EXE, '%s/compile.log' % constants.LOGS_DIR)

      print '  %s' % path
      os.chdir(path)      
      if path.endswith('/'):
        path = path[:-1]

      cp = self.classPathToString(self.getClassPath(competitor.checkout))
      competitor.compile(cp)
    finally:
      os.chdir(cwd)
      
  def classPathToString(self, cp):
    return common.pathsep().join(cp)

  def runSimpleSearchBench(self, iter, id, c,
                           coldRun, seed, staticSeed,
                           filter=None, taskPatterns=None):

    if coldRun:
      # flush OS buffer cache
      print 'Drop buffer caches...'
      if osName == 'linux':
        run("sudo %s/dropCaches.sh" % constants.BENCH_BASE_DIR)
      elif osName in ('windows', 'cygwin'):
        # NOTE: requires you have admin priv
        run('%s/dropCaches.bat' % constants.BENCH_BASE_DIR)
      elif osName == 'osx':
        # NOTE: requires you install OSX CHUD developer package
        run('/usr/bin/purge')
      else:
        raise RuntimeError('do not know how to purge buffer cache on this OS (%s)' % osName)

    # randomSeed = random.Random(staticRandomSeed).randint(-1000000, 1000000)
    #randomSeed = random.randint(-1000000, 1000000)

    cp = self.classPathToString(self.getClassPath(c.checkout))
    logFile = '%s/%s.%s.%d' % (constants.LOGS_DIR, id, c.name, iter)

    if c.doSort:
      doSort = '-sort'
    else:
      doSort = ''

    command = []
    command.extend(c.javaCommand.split())
    command.append('-classpath')
    command.append(cp)
    command.append('perf.SearchPerfTest')
    command.append('-dirImpl')
    command.append(c.directory)
    command.append('-indexPath')
    command.append(nameToIndexPath(c.index.getName()))
    if c.index.facets is not None:
      for tup in c.index.facets:
        command.append('-facets')
        command.append(';'.join(tup))
    
    command.append('-analyzer')
    command.append(c.analyzer)
    command.append('-taskSource')
    command.append(c.tasksFile)
    command.append('-searchThreadCount')
    command.append(str(c.numThreads))
    command.append('-taskRepeatCount')
    command.append(str(c.competition.taskRepeatCount))
    command.append('-field')
    command.append('body')
    command.append('-tasksPerCat')
    command.append(str(c.competition.taskCountPerCat))
    if c.doSort:
      command.append('-sort')
    command.append('-staticSeed')
    command.append(str(staticSeed))
    command.append('-seed')
    command.append(str(seed))
    command.append('-similarity')
    command.append(c.similarity)
    command.append('-commit')
    command.append(c.commitPoint)
    command.append('-hiliteImpl')
    command.append(c.hiliteImpl)
    command.append('-log')
    command.append(logFile)
    command.append('-topN')
    command.append('10')
    if filter is not None:
      command.append('-filter')
      command.append('%.2f' % filter)
    if c.printHeap:
      command.append('-printHeap')
    if c.pk:
      command.append('-pk')
    if c.loadStoredFields:
      command.append('-loadStoredFields')
    
    if False:
      command = '%s -classpath "%s" perf.SearchPerfTest -dirImpl %s -indexPath "%s" -analyzer %s -taskSource "%s" -searchThreadCount %s -taskRepeatCount %s -field body -tasksPerCat %s %s -staticSeed %s -seed %s -similarity %s -commit %s -hiliteImpl %s -log %s' % \
          (c.javaCommand, cp, c.directory,
          nameToIndexPath(c.index.getName()), c.analyzer, c.tasksFile,
          c.numThreads, c.competition.taskRepeatCount,
          c.competition.taskCountPerCat, doSort, staticSeed, seed, c.similarity, c.commitPoint, c.hiliteImpl, logFile)
      command += ' -topN 10'
      if filter is not None:
        command += ' %s %.2f' % filter
      if c.printHeap:
        command += ' -printHeap'
      if c.pk:
        command += ' -pk'
      if c.loadStoredFields:
        command += ' -loadStoredFields'

    print '      log: %s + stdout' % logFile
    t0 = time.time()
    print '      run: %s' % ' '.join(command)
    #p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)    
    #print 'command %s' % command
    p = subprocess.Popen(command, shell=False, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.STDOUT)

    if DO_PERF:
      perfCommand = []
      perfCommand.append('sudo')
      perfCommand.append('perf')
      perfCommand.append('stat')
      #perfCommand.append('-v')
      perfCommand.append('-e')
      perfCommand.append(','.join(PERF_STATS))
      perfCommand.append('--pid')
      perfCommand.append(str(p.pid))
      #print 'COMMAND: %s' % perfCommand
      p2 = subprocess.Popen(perfCommand, shell=False, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.STDOUT)
      f = open(logFile + '.stdout', 'wb')
      while True:
        s = p.stdout.readline()
        if s == '':
          break
        f.write(s)
        f.flush()
      f.close()
      p.wait()
      run('sudo kill -INT %s' % p2.pid)
      #os.kill(p2.pid, signal.SIGINT)
      stdout, stderr = p2.communicate()
      print 'PERF: %s' % fixupPerfOutput(stderr)
    else:
      f = open(logFile + '.stdout', 'wbu')
      while True:
        s = p.stdout.readline()
        if s == '':
          break
        f.write(s)
        f.flush()
      f.close()
      if p.wait() != 0:
        print
        print 'SearchPerfTest FAILED:'
        s = open(logFile + '.stdout', 'r')
        for line in s.readlines():
          print line.rstrip()
        raise RuntimeError('SearchPerfTest failed; see log %s.stdout' % logFile)

    #run(command, logFile + '.stdout', indent='      ')
    print '      %.1f s' % (time.time()-t0)

    return logFile

  def getSearchLogFiles(self, id, c):
    logFiles = []
    for iter in xrange(c.competition.jvmCount):
      logFile = '%s/%s.%s.%d' % (constants.LOGS_DIR, id, c.name, iter)
      logFiles.append(logFile)
    return logFiles

  def simpleReport(self, baseLogFiles, cmpLogFiles, jira=False, html=False, baseDesc='Standard', cmpDesc=None, writer=sys.stdout.write):

    baseRawResults, heapBase = parseResults(baseLogFiles)
    cmpRawResults, heapCmp = parseResults(cmpLogFiles)

    # make sure they got identical results
    cmpDiffs = compareHits(baseRawResults, cmpRawResults, self.verifyScores, self.verifyCounts)

    baseResults = collateResults(baseRawResults)
    cmpResults = collateResults(cmpRawResults)

    cats = set()
    for l in (baseResults, cmpResults):
      if len(l) > 0:
        for k in l[0].keys():
          cats.add(k)
    cats = list(cats)
    cats.sort()

    lines = []

    warnings = []

    lines = []

    chartData = []

    resultsByCatCmp = {}

    for cat in cats:

      if type(cat) is types.TupleType:
        if False and cat[1] is not None:
          desc = '%s (sort %s)' % (cat[0], cat[1])
        else:
          desc = cat[0]
      else:
        desc = cat

      if desc == 'TermDateTimeSort':
        desc = 'TermDTSort'

      l0 = []
      w = l0.append
      if jira:
        w('|%s' % desc)
      elif html:
        w('<tr>')
        w('<td>%s</td>' % htmlEscape(desc))
      else:
        w('%24s' % desc)

      # baseMS, cmpMS are lists of milli-seconds of the run-time for
      # this task across the N JVMs:
      baseMS, baseTotHitCount = agg(baseResults, cat, 'base')
      cmpMS, cmpTotHitCount = agg(cmpResults, cat, 'cmp')

      baseQPS = [1000.0/x for x in baseMS]
      cmpQPS = [1000.0/x for x in cmpMS]

      # Aggregate stats over the N JVMs we ran:
      minQPSBase, maxQPSBase, avgQPSBase, qpsStdDevBase = stats(baseQPS)
      minQPSCmp, maxQPSCmp, avgQPSCmp, qpsStdDevCmp = stats(cmpQPS)

      resultsByCatCmp[desc] = (minQPSCmp, maxQPSCmp, avgQPSCmp, qpsStdDevCmp)

      if VERBOSE:
        if type(cat) is types.TupleType:
          print 'cat %s' % cat[0]
        else:
          print 'cat %s' % cat
        print '  baseQPS: %s' % ' '.join('%.1f' % x for x in baseQPS)
        print '    avg %.1f' % avgQPSBase
        print '  cmpQPS: %s' % ' '.join('%.1f' % x for x in cmpQPS)
        print '    avg %.1f' % avgQPSCmp

      qpsBase = avgQPSBase
      qpsCmp = avgQPSCmp

      # print '%s: %s' % (desc, abs(qpsBase-qpsCmp) / ((maxQPSBase-minQPSBase)+(maxQPSCmp-minQPSCmp)))
      # TODO: need a real significance test here
      if qpsStdDevBase != 0 or qpsStdDevCmp != 0:
        significant = (abs(qpsBase-qpsCmp) / (2*qpsStdDevBase+2*qpsStdDevCmp)) > 0.30
      else:
        significant = False

      if self.verifyCounts and baseTotHitCount != cmpTotHitCount:
        warnings.append('cat=%s: hit counts differ: %s vs %s' % (desc, baseTotHitCount, cmpTotHitCount))

      if jira:
        w('|%.2f|%.2f|%.2f|%.2f' %
          (qpsBase, qpsStdDevBase, qpsCmp, qpsStdDevCmp))
      elif html:
        if qpsBase == 0.0:
          p1 = '(n/a%)'
          p2 = '(n/a%)'
        else:
          p1 = '(%.1f%%)' % (100*qpsStdDevBase/qpsBase)
          p2 = '(%.1f%%)' % (100*qpsStdDevCmp/qpsBase)
        w('<td>%.1f</td><td>%s</td><td>%.1f</td><td>%s</td>' %
          (qpsBase, p1, qpsCmp, p2))
      else:
        if qpsBase == 0.0:
          p1 = '(n/a%)'
          p2 = '(n/a%)'
        else:
          p1 = '(%.1f%%)' % (100*qpsStdDevBase/qpsBase)
          p2 = '(%.1f%%)' % (100*qpsStdDevCmp/qpsBase)
        w('%12.2f%12s%12.2f%12s' % (qpsBase, p1, qpsCmp, p2))

      if qpsBase == 0.0:
        psAvg = 0.0
      else:
        psAvg = 100.0*(qpsCmp - qpsBase)/qpsBase

      qpsBaseBest = qpsBase + qpsStdDevBase
      qpsBaseWorst = qpsBase - qpsStdDevBase

      qpsCmpBest = qpsCmp + qpsStdDevCmp
      qpsCmpWorst = qpsCmp - qpsStdDevCmp

      if qpsBaseWorst == 0.0:
        psBest = psWorst = 0
      else:
        psBest = int(100.0 * (qpsCmpBest - qpsBaseWorst)/qpsBaseWorst)
        psWorst = int(100.0 * (qpsCmpWorst - qpsBaseBest)/qpsBaseBest)

      if jira:
        w('|%s-%s' % (jiraColor(psWorst), jiraColor(psBest)))
      elif html:
        #w('<td>%s (%s - %s)</td>' % (htmlColor(psAvg), htmlColor(psWorst), htmlColor(psBest)))
        w('<td>%s</td>' % htmlColor2(1+psAvg/100.))
      else:
        w('%16s' % ('%7.1f%% (%4d%% - %4d%%)' % (psAvg, psWorst, psBest)))

      if jira:
        w('|\n')
      else:
        w('\n')

      if constants.SORT_REPORT_BY == 'pctchange':
        sortBy = psAvg
      elif constants.SORT_REPORT_BY == 'query':
        sortBy = desc
      else:
        raise RuntimeError('invalid result sort %s' % constant.SORT_REPORT_BY)

      lines.append((sortBy, ''.join(l0)))
      if True or significant:
        chartData.append((sortBy,
                          desc,
                          qpsBase-qpsStdDevBase,
                          qpsBase+qpsStdDevBase,
                          qpsCmp-qpsStdDevCmp,
                          qpsCmp+qpsStdDevCmp,
                          ))
      
    lines.sort()
    chartData.sort()
    chartData = [x[1:] for x in chartData]

    if QPSChart.supported:
      QPSChart.QPSChart(chartData, 'out.png')
      print 'Chart saved to out.png... (wd: %s)' % os.getcwd()
                        
    w = writer

    if jira:
      w('||Task||QPS %s||StdDev %s||QPS %s||StdDev %s||Pct diff||' %
        (baseDesc, baseDesc, cmpDesc, cmpDesc))
    elif html:
      w('<table>')
      w('<tr>')
      w('<th>Task</th>')
      w('<th>QPS %s</th>' % baseDesc)
      w('<th>StdDev %s</th>' % baseDesc)
      w('<th>QPS %s</th>' % cmpDesc)
      w('<th>StdDev %s</th>' % cmpDesc)
      w('<th>% change</th>')
      w('</tr>')
    else:
      w('%24s' % 'Task')
      w('%12s' % ('QPS %s' % baseDesc))
      w('%12s' % 'StdDev')
      w('%12s' % ('QPS %s' % cmpDesc))
      w('%12s' % 'StdDev')
      w('%24s' % 'Pct diff')

    if jira:
      w('||\n')
    else:
      w('\n')

    for ign, s in lines:
      w(s)

    if html:
      w('</table>')

    for w in warnings:
      print 'WARNING: %s' % w

    return resultsByCatCmp, cmpDiffs, stats(heapCmp)

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

def tasksToMap(taskIters, verifyScores, verifyCounts):
  d = {}
  if len(taskIters) > 0:
    for task in taskIters[0]:
      d[task] = task
    for tasks in taskIters[1:]:
      for task in tasks:
        if task not in d:
          # BUG
          raise RuntimeError('tasks differ from one iteration to the next: task=%s' % str(task))
        else:
          # Make sure same task returned same results w/in this run:
          task.verifySame(d[task], verifyScores, verifyCounts)
  return d
    
def compareHits(r1, r2, verifyScores, verifyCounts):

  # TODO: must also compare facet results

  # Carefully compare, allowing for the addition of new tasks:
  d1 = tasksToMap(r1, verifyScores, verifyCounts)
  d2 = tasksToMap(r2, verifyScores, verifyCounts)

  checked = 0
  onlyInD1 = 0
  errors = []
  for task in d1.keys():
    if task in d2:
      try:
        task.verifySame(d2.get(task), verifyScores, verifyCounts)
      except RuntimeError, re:
        errors.append(str(re))
      checked += 1
    else:
      onlyInD1 += 1

  onlyInD2 = len(d2) - (len(d1) - onlyInD1)

  warnings = []
  if len(d1) != len(d2):
    # not necessarily an error because we may have added new tasks in nightly bench
    warnings.append('non-overlapping tasks onlyInD1=%s onlyInD2=%s' % (onlyInD1, onlyInD2))

  if checked == 0:
    warnings.append('no results were checked')

  if len(warnings) == 0 and len(errors) == 0:
    return None
  else:
    return warnings, errors
  
def htmlEscape(s):
  return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def getSegmentCount(indexPath):
  segCount = 0
  for fileName in os.listdir(indexPath):
    if fileName.endswith('.fdx') or fileName.endswith('.cfs'):
      segCount += 1
  return segCount

reBrokenLine = re.compile('^\ +#')
def fixupPerfOutput(s):
  lines = s.split('\n')
  linesOut = []
  for line in lines:
    if reBrokenLine.match(line) is not None:
      linesOut[-1] += ' ' + line.rstrip()
    else:
      linesOut.append(line.rstrip())
  return '\n'.join(linesOut)
