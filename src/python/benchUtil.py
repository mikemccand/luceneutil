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
import os
import pickle
import pwd
import re
import shutil
import statistics
import subprocess
import sys
import time
import traceback
import types
from shutil import which

import common
import constants
import IndexChart
import ps_head
import QPSChart

PERF_EXE = which("perf")

if PERF_EXE is None:
  print("no perf executable; will not collect aggregate CPU profiling data")
else:
  print(f"perf executable is {PERF_EXE}; will collect aggregate CPU profiling data")

PYTHON_MAJOR_VER = sys.version_info.major

VMSTAT_PATH = shutil.which("vmstat")

if PYTHON_MAJOR_VER < 3:
  raise RuntimeError("Please run with Python 3.x!  Got: %s" % str(sys.version))

# Skip the first N runs of a given category (cold) or particular task (hot):
WARM_SKIP = 3

# Skip this pctg of the slowest runs:
SLOW_SKIP_PCT = 10

# Disregard first N seconds of query tasks for computing avg QPS:
DISCARD_QPS_WARMUP_SEC = 5

# From the N times we run each task in a single JVM, how do we pick
# the single QPS to represent those results:

# SELECT = 'min'
# SELECT = 'mean'
SELECT = "median"

MAX_SCORE_DIFF = 0.00001

VERBOSE = False

DO_PERF = constants.DO_PERF

PERF_STATS = constants.PERF_STATS

osName = common.osName


# returns an array of all java files in a directory; walks the directory tree
def addFiles(root):
  files = []
  for f in os.listdir(root):
    f = os.path.join(root, f).replace("\\", "/")
    if os.path.isdir(f):
      files.extend(addFiles(f))
    elif not f.startswith(".#") and f.endswith(".java"):
      files.append(f)
  return files


def htmlColor(v):
  if v < 0:
    return colorFormat(-v, "html", "red")
  return colorFormat(v, "html", "green")


def htmlColor2(v):
  vstr = "%.1f X" % v
  if v < 1.0:
    return colorFormat(vstr, "html", "red")
  return colorFormat(vstr, "html", "green")


def jiraColor(v):
  if v < 0:
    return colorFormat(-v, "jira", "red")
  return colorFormat(v, "jira", "green")


def pValueColor(v, form):
  vstr = "%.3f" % v
  if v <= 0.05:
    return colorFormat(vstr, form, "green")
  return colorFormat(vstr, form, "red")


def colorFormat(value, form, color):
  if form == "html":
    return f'<font color="{color}">{value}</font>'
  if form == "jira":
    return f"{{color:{color}}}{value}{{color}}"
  raise RuntimeError(f"unknown format {form}")


def getArg(argName, default, hasArg=True):
  try:
    idx = sys.argv.index(argName)
  except ValueError:
    v = default
  else:
    if hasArg:
      v = sys.argv[idx + 1]
      del sys.argv[idx : idx + 2]
      try:
        sys.argv.index(argName)
      except ValueError:
        # ok
        pass
      else:
        raise RuntimeError("argument %s appears more than once" % argName)
    else:
      v = True
      del sys.argv[idx]
  return v


def get_username():
  uid = os.getuid()
  return pwd.getpwuid(uid).pw_name


def checkoutToName(checkout):
  return checkout.split("/")[-1]


def checkoutToPath(checkout):
  return checkout if "/" in checkout else "%s/%s" % (constants.BASE_DIR, checkout)


def checkoutToBenchPath(checkout):
  return "%s/lucene/benchmark" % checkoutToPath(checkout)


def checkoutToUtilPath(checkout):
  p = checkoutToPath(checkout)
  if os.path.exists("%s/luceneutil" % p):
    # This checkout has a 'private' luceneutil:
    compPath = "%s/luceneutil" % p
  else:
    compPath = constants.BENCH_BASE_DIR
  return compPath


def nameToIndexPath(name):
  return "%s/%s" % (constants.INDEX_DIR_BASE, name)


def decode(str_or_bytes):
  if PYTHON_MAJOR_VER < 3 or isinstance(str_or_bytes, str):
    return str_or_bytes
  return str_or_bytes.decode("utf-8")


class SearchTask:
  # TODO: subclass SearchGroupTask

  countOnlyCount = None
  isCountOnly = False
  facet_request = None

  def verifySame(self, other, verifyScores, verifyCounts):
    if re.match(".*Knn(Float|Byte)VectorQuery:", self.query) is not None:
      # While KNN search is statically randomized (seed 42?), the concurrent HNSW merge alters the order of results
      return
    if not isinstance(other, SearchTask):
      self.fail("not a SearchTask (%s)" % other)
    if self.query != other.query:
      self.fail("wrong query: %s vs %s" % (self.query, other.query))
    if self.sort != other.sort:
      self.fail("wrong sort: %s vs %s" % (self.sort, other.sort))
    if self.groupField is None:
      if False:
        # TODO: fix SearchPerfTest -- cannot use term count across threads since mutiple threads store in the query
        if self.expandedTermCount != other.expandedTermCount:
          print("WARNING: expandedTermCounts differ for %s: %s vs %s" % (self, self.expandedTermCount, other.expandedTermCount))
          # self.fail('wrong expandedTermCount: %s vs %s' % (self.expandedTermCount, other.expandedTermCount))

      if verifyCounts:
        if self.hitCount != other.hitCount:
          self.fail("wrong hitCount: %s vs %s" % (self.hitCount, other.hitCount))
        if self.countOnlyCount != other.countOnlyCount:
          self.fail("wrong countOnlyCount: %s vs %s" % (self.countOnlyCount, other.countOnlyCount))

      if len(self.hits) != len(other.hits):
        self.fail("wrong top hit count: %s vs %s" % (len(self.hits), len(other.hits)))

      if verifyScores:
        # Collapse equals... this is sorta messy, but necessary because we
        # do not dedup by true id in SearchPerfTest
        hitsSelf = collapseDups(self.hits)
        hitsOther = collapseDups(other.hits)

        if verifyCounts and len(hitsSelf) != len(hitsOther):
          self.fail("self=%s: wrong collapsed hit count: %s vs %s\n  %s vs %s\n  %s vs %s" % (self, len(hitsSelf), len(hitsOther), hitsSelf, hitsOther, self.hits, other.hits))

        if verifyScores:
          for i in range(len(hitsSelf)):
            if hitsSelf[i][1] != hitsOther[i][1]:
              if False:
                if abs(float(hitsSelf[i][1]) - float(hitsOther[i][1])) > MAX_SCORE_DIFF:
                  self.fail("hit %s has wrong field/score value %s vs %s" % (i, hitsSelf[i][1], hitsOther[i][1]))
                else:
                  print("WARNING: query=%s filter=%s sort=%s: slight score diff %s vs %s" % (self.query, self.filter, self.sort, hitsSelf[i][1], hitsOther[i][1]))
              else:
                self.fail("hit %s has wrong field/score value %s vs %s" % (i, hitsSelf[i], hitsOther[i]))
            if hitsSelf[i][0] != hitsOther[i][0] and i < len(hitsSelf) - 1:
              self.fail("hit %s has wrong id/s %s vs %s" % (i, hitsSelf[i], hitsOther[i]))
    else:
      # groups
      if self.groupCount != other.groupCount:
        self.fail("wrong groupCount: cat=%s groupField=%s %s vs %s: self=%s, other=%s" % (self.cat, self.groupField, self.groupCount, other.groupCount, self, other))
      for groupIDX in range(self.groupCount):
        groupValue1, groupTotHits1, groupTopScore1, groups1 = self.groups[groupIDX]
        groupValue2, groupTotHits2, groupTopScore2, groups2 = other.groups[groupIDX]

        # TODO: if we have 1 pass and 2 pass on the "same" group field, assert same

        # TODO: this is because block grouping doesn't pull group
        # values; conditionalize this on block grouping
        if False and groupValue1 != groupValue2:
          self.fail("group %d has wrong groupValue: %s vs %s" % (groupIDX, groupValue1, groupValue2))

        # iffy: this is a float cmp
        if verifyScores:
          if groupTopScore1 != groupTopScore2:
            self.fail("group %d has wrong groupTopScore: %s vs %s" % (groupIDX, groupTopScore1, groupTopScore2))

          if groupTotHits1 != groupTotHits2:
            self.fail("group %d has wrong totHits: %s vs %s" % (groupIDX, groupTotHits1, groupTotHits2))

          if len(groups1) != len(groups2):
            self.fail("group %d has wrong number of docs: %s vs %s" % (groupIDX, len(groups1), len(groups2)))

          groups1 = collapseDups(groups1)
          groups2 = collapseDups(groups2)

          for docIDX in range(len(groups1)):
            if groups1[docIDX][1] != groups2[docIDX][1]:
              self.fail("hit %s has wrong field/score value %s vs %s" % (docIDX, groups1[docIDX][1], groups2[docIDX][1]))
            if groups1[docIDX][0] != groups2[docIDX][0] and docIDX < len(groups1) - 1:
              self.fail("hit %s has wrong id/s %s vs %s" % (docIDX, groups1[docIDX][0], groups2[docIDX][0]))
    if self.facets != other.facets:
      if False:
        print()
        print("***WARNING*** facet diffs: %s: %s vs %s" % (self, self.facets, other.facets))
        print()
      else:
        self.fail("facets differ: %s vs %s" % (self.facets, other.facets))

  def fail(self, message):
    s = "query=%s filter=%s sort=%s groupField=%s hitCount=%s" % (self.query, self.filter, self.sort, self.groupField, self.hitCount)
    raise RuntimeError("%s: %s" % (s, message))

  def __repr__(self):
    return self.__str__()

  def __str__(self):
    s = self.query
    if self.isCountOnly:
      s += " [count-only]"
    else:
      if self.sort is not None:
        s += " [sort=%s]" % self.sort
      if self.groupField is not None:
        s += " [groupField=%s]" % self.groupField
      if self.facet_request is not None:
        s += " [facet_request=%s]" % self.facet_request
    return s

  def __eq__(self, other):
    if not isinstance(other, SearchTask):
      return False
    return (
      self.query == other.query
      and self.sort == other.sort
      and self.groupField == other.groupField
      and self.filter == other.filter
      and self.facet_request == other.facet_request
      and self.isCountOnly == other.isCountOnly
    )

  def __hash__(self):
    return hash(self.query) + hash(self.sort) + hash(self.groupField) + hash(self.filter) + hash(type(self.facet_request)) + hash(self.isCountOnly)


class RespellTask:
  cat = "Respell"

  def verifySame(self, other, verifyScores, verifyCounts):
    if not isinstance(other, RespellTask):
      self.fail("not a RespellTask")
    if self.term != other.term:
      self.fail("wrong term: %s vs %s" % (self.term, other.term))
    if self.hits != other.hits:
      self.fail("wrong hits: %s vs %s" % (self.hits, other.hits))

  def fail(self, message):
    raise RuntimeError("respell: term=%s: %s" % (self.term, message))

  def __str__(self):
    return "Respell %s" % self.term

  def __eq__(self, other):
    if not isinstance(other, RespellTask):
      return False
    return self.term == other.term

  def __hash__(self):
    return hash(self.term)


class PKLookupTask:
  cat = "PKLookup"

  def verifySame(self, other, verifyScores, verifyCounts):
    # already "verified" in search perf test, ie, that the docID
    # returned in fact has the id that was asked for
    pass

  def __str__(self):
    return "PK%s" % self.pkOrd

  def __eq__(self, other):
    if not isinstance(other, PKLookupTask):
      return False
    return self.pkOrd == other.pkOrd

  def __hash__(self):
    return hash(self.pkOrd)


class PKLookupWithTermStateTask:
  cat = "PKLookupWithTermState"

  def verifySame(self, other, verifyScores, verifyCounts):
    # already "verified" in search perf test, ie, that the docID
    # returned in fact has the id that was asked for
    pass

  def __str__(self):
    return "PKTS%s" % self.pkOrd

  def __eq__(self, other):
    if not isinstance(other, PKLookupWithTermStateTask):
      return False
    return self.pkOrd == other.pkOrd

  def __hash__(self):
    return hash(self.pkOrd)


class PointsPKLookupTask:
  cat = "PointsPKLookup"

  def verifySame(self, other, verifyScores, verifyCounts):
    # already "verified" in search perf test, ie, that the docID
    # returned in fact has the id that was asked for
    pass

  def __str__(self):
    return "PointsPK%s" % self.pkOrd

  def __eq__(self, other):
    if not isinstance(other, PointsPKLookupTask):
      return False
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


reSearchTaskOld = re.compile("cat=(.*?) q=(.*?) s=(.*?) group=null hits=(null|[0-9]+\\+?) facets=(.*?)$")
reSearchGroupTaskOld = re.compile("cat=(.*?) q=(.*?) s=(.*?) group=(.*?) groups=(.*?) hits=([0-9]+\\+?) groupTotHits=([0-9]+)(?: totGroupCount=(.*?))? facets=(.*?)$", re.DOTALL)
reSearchTask = re.compile("cat=(.*?) q=(.*?) s=(.*?) f=(.*?) group=null hits=(null|[0-9]+\\+?)$")
reSearchGroupTask = re.compile("cat=(.*?) q=(.*?) s=(.*?) f=(.*?) group=(.*?) groups=(.*?) hits=([0-9]+\\+?) groupTotHits=([0-9]+)(?: totGroupCount=(.*?))?$", re.DOTALL)
reCountOnlyTask = re.compile("cat=(.*?) q=(.*?) countOnlyCount=(.*?)?$", re.DOTALL)
reSearchHitScore = re.compile("doc=(.*?) score=(.*?)$")
reSearchHitField = re.compile("doc=(.*?) .*?=(.*?)$")
reRespellHit = re.compile("(.*?) freq=(.*?) score=(.*?)$")
rePKOrd = re.compile(r"PK(?:TS)?(.*?)\[")
reOneGroup = re.compile("group=(.*?) totalHits=(.*?)(?: hits)? groupRelevance=(.*?)$", re.DOTALL)
reHeap = re.compile("HEAP: ([0-9]+)$")
reLatencyAndStartTime = re.compile(r"^([\d.]+) msec @ ([\d.]+) msec$")
reTasksWinddown = re.compile("^Start of tasks winddown: ([0-9.]+) msec$")
reAvgCPUCores = re.compile("^Average CPU cores used: (-?[0-9.]+)$")


def parse_times_line(task, line):
  m = reLatencyAndStartTime.match(line.decode("utf-8"))
  if m is None:
    raise RuntimeError(f"unable to parse {line} into latency & start time")
  task.msec = float(m.group(1))
  task.startMsec = float(m.group(2))


def parseResults(resultsFiles):
  taskIters = []
  heaps = []
  for resultsFile in resultsFiles:
    tasks = []

    if not os.path.exists(resultsFile):
      # nocommit -- why would we pass this file in, if it does not exist?
      continue

    if os.path.exists(resultsFile + ".stdout") and os.path.getsize(resultsFile + ".stdout") > 50 * 1024:
      raise RuntimeError("%s.stdout is %d bytes; leftover System.out.println?" % (resultsFile, os.path.getsize(resultsFile + ".stdout")))

    tasksWindownMS = -1
    avgCPUCores = -1

    # print 'parse %s' % resultsFile
    f = open(resultsFile, "rb")
    while True:
      line = f.readline()
      if line == b"":
        break
      line = line.strip()

      if line.startswith(b"Start of tasks winddown: "):
        tasksWindownMS = float(reTasksWinddown.match(line.decode("utf-8")).group(1))
        continue

      if line.startswith(b"Average CPU cores used: "):
        avgCPUCores = float(reAvgCPUCores.match(line.decode("utf-8")).group(1))
        continue

      if line.startswith(b"HEAP: "):
        m = reHeap.match(decode(line))
        heaps.append(int(m.group(1)))

      if line.startswith(b"TASK: cat="):
        task = SearchTask()
        parse_times_line(task, f.readline().strip())
        task.threadID = int(f.readline().strip().split()[1])
        task.facets = None

        m = reSearchTask.match(decode(line[6:]))

        if m is not None:
          cat, task.query, sort, filter, hitCount = m.groups()
        else:
          m = reSearchTaskOld.match(decode(line[6:]))
          if m is not None:
            cat, task.query, sort, hitCount, task.facet_request = m.groups()
            filter = None
          else:
            m = reCountOnlyTask.search(decode(line))
            if m is not None:
              cat = m.group(1)
              task.query = m.group(2)
              task.isCountOnly = True
              task.countOnlyCount = int(m.group(3))
              hitCount = None
              filter = None
              sort = "null"
            else:
              cat = None

        if cat is not None:
          task.cat = cat
          task.groups = None
          task.groupField = None
          task.filter = filter
          # print 'CAT %s' % cat

          if hitCount == "null":
            task.hitCount = "0"
          else:
            task.hitCount = hitCount
          if sort in ('<string: "title">', '<string: "titleDV">', '<sortedset: "title"> selector=MIN'):
            task.sort = "Title"
          elif sort.startswith('<long: "datenum">') or sort.startswith('<long: "lastModNDV">') or sort.startswith('<sortednumeric: "lastMod"> selector=MIN type=LONG'):
            task.sort = "DateTime"
          elif sort in ('<string: "month">', '<string: "monthSortedDV">', '<sortedset: "month"> selector=MIN'):
            task.sort = "Month"
          elif sort == '<int: "dayOfYearNumericDV">' or sort.startswith('<sortednumeric: "dayOfYear"> selector=MIN type=INT'):
            task.sort = "DayOfYear"
          elif sort == '<string_val: "titleBDV">':
            task.sort = "TitleBinary"
          elif sort != "null":
            raise RuntimeError("could not parse sort: %s" % sort)
          else:
            task.sort = None

          task.hits = []
          task.expandedTermCount = 0

          while True:
            line = f.readline().strip()
            if line == b"":
              break

            if task.facets is not None:
              task.facets.append(line)
              continue

            if line.find(b"expanded terms") != -1:
              task.expandedTermCount = int(line.split()[0])
              continue
            if line.find(b"Zing VM Warning") != -1:
              continue
            if line.find(b"facets") != -1:
              task.facets = []
              continue
            if line.find(b"hilite time") != -1:
              task.hiliteMsec = float(line.split()[2])
              continue
            if line.find(b"getFacetResults time") != -1:
              task.getFacetResultsMsec = float(line.split()[2])
              continue

            if line.startswith(b"HEAP: "):
              m = reHeap.match(decode(line))
              heaps.append(int(m.group(1)))
              break

            if sort == "null":
              m = reSearchHitScore.match(decode(line))
              id = int(m.group(1))
              score = m.group(2)
              # score stays a string so we can do "precise" ==
              task.hits.append((id, score))
            else:
              m = reSearchHitField.match(decode(line))
              id = int(m.group(1))
              field = m.group(2)
              task.hits.append((id, field))
        else:
          m = reSearchGroupTask.match(decode(line[6:]))
          if m is not None:
            cat, task.query, sort, filter, task.groupField, groupCount, hitCount, groupedHitCount, totGroupCount = m.groups()
          else:
            m = reSearchGroupTaskOld.match(decode(line[6:]))
            if m is not None:
              cat, task.query, sort, task.groupField, groupCount, hitCount, groupedHitCount, totGroupCount, task.facet_request = m.groups()
              filter = None

          if cat is not None:
            task.cat = cat
            task.hits = hitCount
            task.hitCount = hitCount
            task.groupedHitCount = groupedHitCount
            task.groupCount = int(groupCount)
            task.filter = filter
            if totGroupCount in (None, "null"):
              task.totGroupCount = None
            else:
              task.totGroupCount = int(totGroupCount)
            # TODO: handle different sorts
            task.sort = None
            task.groups = []
            group = None
            while True:
              line = f.readline().strip()
              if line == b"":
                break
              if line.find(b"Zing VM Warning") != -1:
                continue
              if line.startswith(b"HEAP: "):
                m = reHeap.match(decode(line))
                heaps.append(int(m.group(1)))
                break
              m = reOneGroup.search(decode(line))
              if m is not None:
                groupValue, groupTotalHits, groupTopScore = m.groups()
                group = (groupValue, int(groupTotalHits), float(groupTopScore), [])
                task.groups.append(group)
                continue
              m = reSearchHitScore.search(decode(line))
              if m is not None:
                doc = int(m.group(1))
                score = float(m.group(2))
                group[-1].append((doc, score))
              else:
                # BUG
                raise RuntimeError("result parsing failed: line=%s" % line)
          elif line.find(b"Zing VM Warning") != -1:
            continue
          else:
            raise RuntimeError("result parsing failed: line=%s" % line)
      elif line.startswith(b"TASK: respell"):
        task = RespellTask()
        parse_times_line(task, f.readline().strip())
        task.threadID = int(f.readline().strip().split()[1])
        task.term = line[14:]

        task.hits = []
        while True:
          line = f.readline().strip()
          if line == b"":
            break
          if line.find(b"Zing VM Warning") != -1:
            continue
          if line.startswith(b"HEAP: "):
            m = reHeap.match(decode(line))
            heaps.append(int(m.group(1)))
            break
          m = reRespellHit.search(decode(line))
          suggest, freq, score = m.groups()
          task.hits.append((suggest, int(freq), float(score)))

      elif line.startswith(b"TASK: PKTS"):
        task = PKLookupWithTermStateTask()
        task.pkOrd = rePKOrd.search(decode(line)).group(1)
        parse_times_line(task, f.readline().strip())
        task.threadID = int(f.readline().strip().split()[1])
      elif line.startswith(b"TASK: PK"):
        task = PKLookupTask()
        task.pkOrd = rePKOrd.search(decode(line)).group(1)
        parse_times_line(task, f.readline().strip())
        task.threadID = int(f.readline().strip().split()[1])
      elif line.startswith(b"TASK: PointsPK"):
        task = PointsPKLookupTask()
        task.pkOrd = rePKOrd.search(decode(line)).group(1)
        parse_times_line(task, f.readline().strip())
        task.threadID = int(f.readline().strip().split()[1])
      else:
        task = None
        if line.find(b"\tat") != -1:
          raise RuntimeError("log has exceptions")

      if task is not None:
        tasks.append(task)

    taskIters.append(tasks)

    if tasksWindownMS == -1:
      raise RuntimeError(f'did not find "Start of tasks winddown: " line in results file {resultsFile}')

  # TODO: why are we returning tasksWindownMS (which is per-result-file) here when
  # we were given multiple results files?
  return taskIters, heaps, tasksWindownMS, avgCPUCores


# Collect task latencies segregated by categories across all the runs of the task
# This allows calculating P50, P90, P99 and P100 latencies per task
def collateTaskLatencies(resultIters):
  iters = []
  for results in resultIters:
    byCat = {}
    iters.append(byCat)
    for task in results:
      if isinstance(task, SearchTask):
        key = (task.cat, task.sort)
      else:
        key = (task.cat,)

      if key not in byCat:
        byCat[key] = []

      l = byCat[key]
      l.append(task.msec)

  return iters


def collateResults(resultIters):
  iters = []
  for results in resultIters:
    # Keyed first by category (Fuzzy1, Respell, ...) and 2nd be exact
    # task mapping to list of runs of that task.  For a cold run (no task repeats)
    # the 2nd map will always map to a length-1 list.
    byCat = {}
    iters.append(byCat)
    for task in results:
      if isinstance(task, SearchTask):
        key = (task.cat, task.sort)
      else:
        key = (task.cat,)
      if key not in byCat:
        byCat[key] = ([], {})
      l, d = byCat[key]
      l.append(task)
      if task not in d:
        d[task] = [task]
      else:
        d[task].append(task)

  return iters


def agg(iters, cat, name, verifyCounts):
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
      raise RuntimeError("only %s tasks in cat %s" % (len(tasks[0]), cat))

    totHitCount = 0
    totCountOnlyCount = 0
    count = 0
    sumMS = 0.0
    if VERBOSE:
      print("AGG: cat=%s" % str(cat))

    # Iterate over each category's instances, eg a given category
    # might have 5 different instances:
    for task, results in tasks[1].items():
      allMS = [result.msec for result in results]
      if VERBOSE:
        print("  %s" % task)
        print("    before prune:")
        for t in allMS:
          print("      %.4f" % t)

      if len(allMS) <= WARM_SKIP:
        raise RuntimeError(f"only {len(allMS)} runs (<= warmup={WARM_SKIP}) in cat {cat} for task {task}")

      # Skip warmup runs
      allMS = allMS[WARM_SKIP:]

      allMS.sort()
      minMS = allMS[0]
      if VERBOSE:
        print("    after sort:")
        for t in allMS:
          print("      %.4f" % t)

      # Skip slowest SLOW_SKIP_PCT runs:
      skipSlowest = int(len(allMS) * SLOW_SKIP_PCT / 100.0)
      if VERBOSE:
        print("skipSlowest %s" % skipSlowest)

      if skipSlowest > 0:
        pruned = allMS[:-skipSlowest]
      else:
        pruned = allMS

      if VERBOSE:
        print("    after prune:")
        for t in pruned:
          print("      %.4f" % t)

      if SELECT == "min":
        sumMS += minMS
        count += 1
      elif SELECT == "mean":
        sumMS += sum(pruned)
        count += len(pruned)
      elif SELECT == "median":
        mid = len(pruned) // 2
        if len(pruned) % 2 == 0:
          median = (pruned[mid - 1] + pruned[mid]) / 2.0
        else:
          median = pruned[mid]
        if VERBOSE:
          print("  median %.4f" % median)
        sumMS += median
        count += 1
      else:
        raise RuntimeError("unrecognized SELECT=%s: should be min, median or mean" % SELECT)

      if isinstance(task, SearchTask):
        if task.countOnlyCount is not None:
          totCountOnlyCount += task.countOnlyCount
        elif task.groupField is None:
          totHitCount = sum_hit_count(totHitCount, task.hitCount)
        else:
          for group in task.groups:
            totHitCount += group[1]

    # AvgMS per query in category, eg if we ran 5 different queries in
    # each cat, then this is AvgMS for query in that cat:
    avgMS = sumMS / count
    accumMS.append(avgMS)

    if lastHitCount is None:
      lastHitCount = totHitCount
      lastCountOnlyCount = totCountOnlyCount
    elif verifyCounts:
      if totHitCount != lastHitCount:
        raise RuntimeError("different hit counts: %s vs %s" % (lastHitCount, totHitCount))
      if totCountOnlyCount != lastCountOnlyCount:
        raise RuntimeError("different count only counts: %s vs %s" % (lastCountOnlyCount, totCountOnlyCount))

  if VERBOSE:
    # accumMS.sort()
    minValue = min(accumMS)
    # print '  accumMS=%s' % ' '.join(['%5.1f' % x for x in accumMS])
    print("  %s %s: accumMS=%s" % (name, cat[0], " ".join(["%5.1f" % (100.0 * (x - minValue) / minValue) for x in accumMS])))

  return accumMS, totHitCount


def sum_hit_count(hc1, hc2):
  lower_bound = False
  if isinstance(hc1, str) and hc1.endswith("+"):
    lower_bound = True
    hc1 = int(hc1[:-1])
  else:
    hc1 = int(hc1)
  if isinstance(hc2, str) and hc2.endswith("+"):
    lower_bound = True
    hc2 = int(hc2[:-1])
  else:
    hc2 = int(hc2)
  return str(hc1 + hc2) + ((lower_bound and "+") or "")


def stats(l):
  # min, max, mean, stddev
  if len(l) == 0:
    return 0.0, 0.0, 0.0, 0.0
  mu = statistics.mean(l)
  return min(l), max(l), mu, statistics.stdev(l) if len(l) > 1 else 0


def run(cmd, logFile=None, indent="    ", vmstatLogFile=None, topLogFile=None):
  print("%srun: %s, cwd=%s" % (indent, cmd, os.getcwd()))
  if logFile is not None:
    out = open(logFile, "wb")
  else:
    out = subprocess.STDOUT

  if vmstatLogFile is not None:
    vmstatCmd = f"{VMSTAT_PATH} --active --wide --timestamp --unit M 1 > {vmstatLogFile} 2>&1 &"
    print(f"run vmstat: {vmstatCmd}")
    vmstatProcess = subprocess.Popen(vmstatCmd, shell=True, preexec_fn=os.setsid)

  if topLogFile is not None:
    topProcess = ps_head.PSTopN(10, topLogFile)
    print(f"run {topProcess.cmd} to {topLogFile}")

  p = subprocess.Popen(cmd, stdout=out, stderr=out)
  if p.wait():
    if logFile is not None and os.path.getsize(logFile) < 50 * 1024:
      print(open(logFile).read())
    raise RuntimeError("failed: %s [wd %s]; see logFile %s" % (cmd, os.getcwd(), logFile))
  if vmstatLogFile is not None:
    print(f"now kill vmstat: pid={vmstatProcess.pid}")
    # TODO: messy!  can we get process group working so we can kill bash and its child reliably?
    subprocess.check_call(["pkill", "-u", get_username(), "vmstat"])
    # os.kill(vmstatProcess.pid, signal.SIGKILL)
    if vmstatProcess.poll() is None:
      raise RuntimeError("failed to kill vmstat child process?  pid={vmstatProcess.pid}")
  if topLogFile is not None:
    topProcess.stop()


reCoreJar = re.compile("lucene-core-[0-9]+\\.[0-9]+\\.[0-9]+(?:-SNAPSHOT)?\\.jar")


class RunAlgs:
  def __init__(self, javaCommand, verifyScores, verifyCounts):
    self.logCounter = 0
    self.results = []
    self.compiled = set()
    self.javaCommand = javaCommand
    self.verifyScores = verifyScores
    self.verifyCounts = verifyCounts
    print()
    print("JAVA:\n%s" % os.popen("%s -version 2>&1" % javaCommand).read())

    print()
    if osName not in ("windows", "cygwin"):
      print("OS:\n%s" % os.popen("uname -a 2>&1").read())
    else:
      print("OS:\n%s" % sys.platform)

    if not os.path.exists(constants.LOGS_DIR):
      os.makedirs(constants.LOGS_DIR)
    print()
    print("LOGS:\n%s" % constants.LOGS_DIR)

  def printEnv(self):
    print()
    print("JAVA:\n%s" % os.popen("%s -version 2>&1" % self.javaCommand).read())

    print
    if osName not in ("windows", "cygwin"):
      print("OS:\n%s" % os.popen("uname -a 2>&1").read())
    else:
      print("OS:\n%s" % sys.platform)

  def makeIndex(self, id, index, printCharts=False, profilerCount=30, profilerStackSize=1):
    # we accept a sequence of stack sizes and will re-aggregate JFR results at each
    if type(profilerStackSize) is int:
      profilerStackSize = (profilerStackSize,)

    fullIndexPath = nameToIndexPath(index.getName())
    if os.path.exists(fullIndexPath) and not index.doUpdate:
      print("  %s: already exists" % fullIndexPath)
      return fullIndexPath
    if index.doUpdate:
      if not os.path.exists(fullIndexPath):
        raise RuntimeError("index path does not exists: %s" % fullIndexPath)
      print("  %s: now update" % fullIndexPath)
    else:
      print("  %s: now create" % fullIndexPath)

    s = checkoutToBenchPath(index.checkout)
    print("    cd %s" % s)
    os.chdir(s)

    try:
      cmd = []
      cmd += index.javaCommand.split()
      w = lambda *xs: [cmd.append(str(x)) for x in xs]
      w("-classpath", classPathToString(getClassPath(index.checkout)))

      jfrOutput = f"{constants.LOGS_DIR}/bench-index-{id}-{index.getName()}.jfr"

      # 77: always enable Java Flight Recorder profiling
      w(
        f"-XX:StartFlightRecording=dumponexit=true,maxsize=250M,settings={constants.BENCH_BASE_DIR}/src/python/profiling.jfc" + f",filename={jfrOutput}",
        "-XX:+UnlockDiagnosticVMOptions",
        "-XX:+DebugNonSafepoints",
      )

      w("perf.Indexer")
      w("-dirImpl", index.directory)
      w("-indexPath", fullIndexPath)
      w("-analyzer", index.analyzer)
      w("-lineDocsFile", index.lineDocSource)
      w("-docCountLimit", index.numDocs)
      w("-threadCount", index.numThreads)
      if index.maxConcurrentMerges is not None:
        w("-maxConcurrentMerges", index.maxConcurrentMerges)

      if index.addDVFields:
        w("-dvfields")

      if index.useCMS:
        w("-useCMS")

      if index.vectorFile:
        w("-vectorFile", index.vectorFile)
        w("-vectorDimension", index.vectorDimension)
        w("-vectorEncoding", index.vectorEncoding)

      if index.optimize:
        w("-forceMerge")

      if index.verbose:
        w("-verbose")

      w("-ramBufferMB", index.ramBufferMB)
      w("-maxBufferedDocs", index.maxBufferedDocs)
      w("-postingsFormat", index.postingsFormat)

      if index.doDeletions:
        w("-deletions")

      if index.printDPS:
        w("-printDPS")

      if index.waitForMerges:
        w("-waitForMerges")

      w("-mergePolicy", index.mergePolicy)

      if index.doUpdate:
        w("-update")

      if index.facets is not None:
        for tup in index.facets:
          w("-facets", ";".join(tup))
        w("-facetDVFormat", index.facetDVFormat)

      w("-idFieldPostingsFormat", index.idFieldPostingsFormat)

      w("-idFieldPostingsFormat")
      w(index.idFieldPostingsFormat)

      if index.grouping:
        w("-grouping")

      if index.useCFS:
        w("-cfs")

      if index.bodyTermVectors:
        w("-tvs")

      if index.bodyPostingsOffsets:
        w("-bodyPostingsOffsets")

      if index.bodyStoredFields:
        w("-store")

      if index.waitForCommit:
        w("-waitForCommit")

      if index.ioThrottle is not None:
        w("-ioThrottle", str(index.ioThrottle).lower())

      if index.indexSort:
        w("-indexSort", index.indexSort)

      if index.rearrange != 0:
        w("-rearrange", index.rearrange)

      w("-hnswThreadsPerMerge", index.hnswThreadsPerMerge)
      w("-hnswThreadPoolCount", index.hnswThreadPoolCount)
      if index.quantizeKNNGraph:
        w("-quantizeKNNGraph")

      fullLogFile = "%s/%s.%s.log" % (constants.LOGS_DIR, id, index.getName())

      print("    log %s" % fullLogFile)

      t0 = time.time()
      if VMSTAT_PATH is not None:
        vmstatLogFile = f"{constants.LOGS_DIR}/{id}.vmstat.log"
      else:
        vmstatLogFile = None
      topLogFile = f"{constants.LOGS_DIR}/{id}.top.log"
      run(cmd, fullLogFile, vmstatLogFile=vmstatLogFile, topLogFile=topLogFile)
      t1 = time.time()
      if printCharts and IndexChart.Gnuplot is not None:
        chart = IndexChart.IndexChart(fullLogFile, index.getName())
        chart.plot()

      with open(fullLogFile, "rb") as f:
        while True:
          l = f.readline()
          if l == b"":
            break
          if l.lower().find(b"exception in thread") != -1:
            raise RuntimeError('unhandled exceptions in log "%s"' % fullLogFile)

    except:
      # if we hit any exception/problem building the index, remove the
      # partially built index so we don't accidentally think we can
      # run with it:
      if os.path.exists(fullIndexPath):
        shutil.rmtree(fullIndexPath)
      raise

    profilerResults = profilerOutput(index.javaCommand, jfrOutput, checkoutToPath(index.checkout), profilerCount, profilerStackSize)

    return fullIndexPath, fullLogFile, profilerResults, jfrOutput

  def addJars(self, cp, path):
    if os.path.exists(path):
      for f in os.listdir(path):
        if f.endswith(".jar"):
          cp.append("%s/%s" % (path, f))

  compiledCheckouts = set()

  def compile(self, competitor):
    path = checkoutToBenchPath(competitor.checkout)
    cwd = os.getcwd()
    checkoutPath = checkoutToPath(competitor.checkout)
    if not os.path.exists(os.path.join(checkoutPath, "build.gradle")):
      return self.antCompile(competitor)
    try:
      if competitor.checkout not in self.compiledCheckouts:
        self.compiledCheckouts.add(competitor.checkout)
        # for core we build a JAR in order to benefit from the MR JAR stuff
        os.chdir(checkoutPath)
        for module in ["core"]:
          print("compile lucene:core...")
          run([constants.GRADLE_EXE, "lucene:core:jar"], "%s/compile.log" % constants.LOGS_DIR)
        for module in ("suggest", "highlighter", "misc", "analysis:common", "grouping", "codecs", "facet", "sandbox", "queryparser"):
          # Try to be faster; this may miss changes, e.g. a static final constant changed in core that is used in another module:
          modulePath = "%s/lucene/%s" % (checkoutPath, module.replace(":", "/"))
          classesPath = "%s/build/classes/java" % (modulePath)
          lastCompileTime = common.getLatestModTime(classesPath, ".class")
          if common.getLatestModTime("%s/src/java" % modulePath) > lastCompileTime:
            print("compile lucene:%s..." % module)
            run([constants.GRADLE_EXE, "lucene:%s:compileJava" % module], "%s/compile.log" % constants.LOGS_DIR)

      print("  %s" % path)
      os.chdir(path)
      path = path.removesuffix("/")

      cp = classPathToString(getClassPath(competitor.checkout))
      competitor.compile(cp)
    finally:
      os.chdir(cwd)

  def antCompile(self, competitor):
    path = checkoutToBenchPath(competitor.checkout)
    cwd = os.getcwd()
    checkoutPath = checkoutToPath(competitor.checkout)
    try:
      if competitor.checkout not in self.compiledCheckouts:
        self.compiledCheckouts.add(competitor.checkout)
        # for core we build a JAR in order to benefit from the MR JAR stuff
        for module in ["core"]:
          modulePath = "%s/lucene/%s" % (checkoutPath, module)
          os.chdir(modulePath)
          print("  %s..." % modulePath)
          run([constants.GRADLE_EXE, "jar"], "%s/compile.log" % constants.LOGS_DIR)
        for module in ("suggest", "highlighter", "misc", "analysis/common", "grouping", "codecs", "facet", "sandbox"):
          modulePath = "%s/lucene/%s" % (checkoutPath, module)
          classesPath = "%s/lucene/build/%s/classes/java" % (checkoutPath, module)
          # Try to be faster than ant; this may miss changes, e.g. a static final constant changed in core that is used in another module:
          if common.getLatestModTime("%s/src/java" % modulePath) > common.getLatestModTime(classesPath, ".class"):
            print("  %s..." % modulePath)
            os.chdir(modulePath)
            run([constants.GRADLE_EXE, "compileJava"], "%s/compile.log" % constants.LOGS_DIR)

      print("  %s" % path)
      os.chdir(path)
      path = path.removesuffix("/")

      cp = classPathToString(getClassPath(competitor.checkout))
      competitor.compile(cp)
    finally:
      os.chdir(cwd)

  def runSimpleSearchBench(self, iter, id, c, coldRun, seed, staticSeed, filter=None, taskPatterns=None):
    if coldRun:
      # flush OS buffer cache
      print("Drop buffer caches...")
      if osName == "linux":
        run(["sudo", "%s/dropCaches.sh" % constants.BENCH_BASE_DIR])
      elif osName in ("windows", "cygwin"):
        # NOTE: requires you have admin priv
        run(["%s/dropCaches.bat" % constants.BENCH_BASE_DIR])
      elif osName == "osx":
        # NOTE: requires you install OSX CHUD developer package
        run(["/usr/bin/purge"])
      else:
        raise RuntimeError("do not know how to purge buffer cache on this OS (%s)" % osName)

    # randomSeed = random.Random(staticRandomSeed).randint(-1000000, 1000000)
    # randomSeed = random.randint(-1000000, 1000000)

    cp = classPathToString(getClassPath(c.checkout))
    logFile = "%s/%s.%s.%d" % (constants.LOGS_DIR, id, c.name, iter)

    if c.doSort:
      doSort = "-sort"
    else:
      doSort = ""

    command = []
    if PERF_EXE is not None:
      command += [PERF_EXE, "stat", "-dd"]
    command += c.javaCommand.split()

    # 77: always enable Java Flight Recorder profiling
    command += [
      f"-XX:StartFlightRecording=dumponexit=true,maxsize=250M,settings={constants.BENCH_BASE_DIR}/src/python/profiling.jfc" + f",filename={constants.LOGS_DIR}/bench-search-{id}-{c.name}-{iter}.jfr",
      "-XX:+UnlockDiagnosticVMOptions",
      "-XX:+DebugNonSafepoints",
      # uncomment the line below to enable remote debugging
      # '-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=localhost:7891'
    ]

    w = lambda *xs: [command.append(str(x)) for x in xs]
    w("-classpath", cp)
    w("perf.SearchPerfTest")
    w("-dirImpl", c.directory)
    w("-indexPath", nameToIndexPath(c.index.getName()))
    if c.index.facets is not None:
      for tup in c.index.facets:
        w("-facets", ";".join(tup))

    w("-analyzer", c.analyzer)
    w("-taskSource", c.tasksFile)
    w("-numConcurrentQueries", c.numConcurrentQueries)
    w("-taskRepeatCount", c.competition.taskRepeatCount)
    w("-field", "body")
    w("-tasksPerCat", c.competition.taskCountPerCat)
    if c.competition.groupByCat:
      w("-groupByCat")
    if c.doSort:
      w("-sort")
    w("-searchConcurrency", c.searchConcurrency)
    w("-staticSeed", staticSeed)
    w("-seed", seed)
    w("-similarity", c.similarity)
    w("-commit", c.commitPoint)
    w("-hiliteImpl", c.hiliteImpl)
    w("-log", logFile)
    w("-topN", c.topN)
    w("-context", c.testContext)
    if filter is not None:
      w("-filter", "%.2f" % filter)
    if c.printHeap:
      w("-printHeap")
    if c.pk:
      w("-pk")
    if c.loadStoredFields:
      w("-loadStoredFields")
    if c.vectorDict:
      w("-vectorDict", c.vectorDict)
    elif c.vectorFileName is not None:
      w("-vectorFile", c.vectorFileName)
      w("-vectorDimension", c.vectorDimension)
    if c.vectorScale:
      w("-vectorScale", c.vectorScale)
    if c.exitable:
      w("-exitable")

    print("      log: %s + stdout" % logFile)
    t0 = time.time()
    print("      run: %s" % " ".join(command))
    # p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    p = subprocess.Popen(command, shell=False, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.STDOUT)

    if DO_PERF:
      perfCommand = []
      # why sudo needed!?  also, why not just run perf stat full commandline?
      # perfCommand.append('sudo')
      perfCommand.append("perf")
      perfCommand.append("stat")
      # perfCommand.append('-v')
      perfCommand.append("-e")
      perfCommand.append(",".join(PERF_STATS))
      perfCommand.append("--pid")
      perfCommand.append(str(p.pid))
      # print 'COMMAND: %s' % perfCommand
      p2 = subprocess.Popen(perfCommand, shell=False, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.STDOUT)
      f = open(logFile + ".stdout", "wb")
      while True:
        s = p.stdout.readline()
        if s == b"":
          break
        f.write(s)
        f.flush()
      f.close()
      p.wait()
      # run(['sudo', 'kill', '-INT', p2.pid])
      # why?
      run(["kill", "-INT", p2.pid])
      # os.kill(p2.pid, signal.SIGINT)
      stdout, stderr = p2.communicate()
      print("PERF: %s" % fixupPerfOutput(stderr))
    else:
      mode = "wbu" if PYTHON_MAJOR_VER < 3 else "wb"
      f = open(logFile + ".stdout", mode)
      while True:
        s = p.stdout.readline()
        if s == b"":
          break
        f.write(s)
        f.flush()
      f.close()
      if p.wait() != 0:
        print()
        print("SearchPerfTest FAILED:")
        s = open(logFile + ".stdout")
        for line in s.readlines():
          print(line.rstrip())
        raise RuntimeError("SearchPerfTest failed; see log %s.stdout" % logFile)

    # run(command, logFile + '.stdout', indent='      ')
    print("      %.1f s" % (time.time() - t0))

    # nocommit don't wastefully load/process here too!!
    raw_results, heap_base, tasks_winddown_ms, avg_cpu_cores = parseResults([logFile])
    qpss = self.compute_qps(raw_results, tasks_winddown_ms)
    print("      %.1f actual sustained QPS; %.1f CPU cores used" % (qpss[0], avg_cpu_cores))

    return logFile

  def getSearchLogFiles(self, id, c):
    logFiles = []
    for iter in range(c.competition.jvmCount):
      logFile = "%s/%s.%s.%d" % (constants.LOGS_DIR, id, c.name, iter)
      logFiles.append(logFile)
    return logFiles

  def computeTaskLatencies(self, inputList, catSet):
    resultLatencyMetrics = {}
    for currentRecord in inputList:
      for currentKey in currentRecord.keys():
        catSet.add(currentKey)
        currentCatLatencies = currentRecord[currentKey]
        currentCatLatencies.sort()

        currentLatencyMetricsDict = {}
        resultLatencyMetrics[currentKey] = currentLatencyMetricsDict

        currentP0 = currentCatLatencies[0]
        currentP50 = currentCatLatencies[(len(currentCatLatencies) - 1) // 2]
        currentP90 = currentCatLatencies[int((len(currentCatLatencies) - 1) * 0.9)]
        currentP99 = currentCatLatencies[int((len(currentCatLatencies) - 1) * 0.99)]
        currentP999 = currentCatLatencies[int((len(currentCatLatencies) - 1) * 0.999)]
        currentP100 = currentCatLatencies[len(currentCatLatencies) - 1]

        currentLatencyMetricsDict["p0"] = currentP0
        currentLatencyMetricsDict["p50"] = currentP50
        currentLatencyMetricsDict["p90"] = currentP90
        currentLatencyMetricsDict["p99"] = currentP99
        currentLatencyMetricsDict["p999"] = currentP999
        currentLatencyMetricsDict["p100"] = currentP100

    return resultLatencyMetrics

  def compute_qps(self, raw_results, tasks_winddown_ms):
    # one per JVM iteration
    qpss = []
    for tasks in raw_results:
      # make full copy -- don't mess up sort of incoming tasks
      sorted_tasks = tasks[:]
      sorted_tasks.sort(key=lambda x: x.startMsec)
      # print(f'{len(sorted_tasks)} tasks:')
      by_second = {}
      count = 0
      # nocommit couldn't we dynamically infer warmup by looking for second-by-second QPS
      keep_after_ms = sorted_tasks[0].startMsec + DISCARD_QPS_WARMUP_SEC * 1000
      for task in sorted_tasks:
        # print(f'  {task.startMsec} {task.msec}')
        finish_time_ms = task.startMsec + task.msec
        if finish_time_ms < keep_after_ms:
          # print(f'  task too early {finish_time_ms} vs {keep_after_ms}')
          continue

        if finish_time_ms > tasks_winddown_ms:
          # print(f'  task too late {finish_time_ms} vs {tasks_winddown_ms}')
          continue

        finish_time_sec = int(finish_time_ms / 1000)
        by_second[finish_time_sec] = 1 + by_second.get(finish_time_sec, 0)
        count += 1

      qps = count / ((tasks_winddown_ms - keep_after_ms) / 1000.0)
      qpss.append(qps)

      if False:
        # curious to see how QPS varies second by second...:
        l = list(by_second.items())
        l.sort()
        for sec, count in l:
          print(f"  {sec:3d}: qps={count}")

    return qpss

  if False:

    def only_qps_report(self, baseLogFiles, cmpLogFiles):
      # nocommit must also validate tasks' results validity
      baseRawResults, heapBase, baseTasksWindownMS, baseAvgCpuCores = parseResults(baseLogFiles)
      cmpRawResults, heapCmp, cmpTasksWindownMS, cmpAvgCpuCores = parseResults(cmpLogFiles)

      base_qpss = self.compute_qps(baseRawResults, baseTasksWindownMS)
      cmp_qpss = self.compute_qps(cmpRawResults, cmpTasksWindownMS)

  def simpleReport(self, baseLogFiles, cmpLogFiles, jira=False, html=False, baseDesc="Standard", cmpDesc=None, writer=sys.stdout.write):
    baseRawResults, heapBase, ignore, baseAvgCpuCores = parseResults(baseLogFiles)
    cmpRawResults, heapCmp, ignore, cmpAvgCpuCores = parseResults(cmpLogFiles)

    # make sure they got identical results
    cmpDiffs = compareHits(baseRawResults, cmpRawResults, self.verifyScores, self.verifyCounts)

    baseResults = collateResults(baseRawResults)
    cmpResults = collateResults(cmpRawResults)

    baseTaskLatencies = collateTaskLatencies(baseRawResults)
    cmpTaskLatencies = collateTaskLatencies(cmpRawResults)

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
      _type = types.TupleType if PYTHON_MAJOR_VER < 3 else tuple
      if isinstance(cat, _type):
        if False and cat[1] is not None:
          desc = "%s (sort %s)" % (cat[0], cat[1])
        else:
          desc = cat[0]
      else:
        desc = cat

      if desc == "TermDateTimeSort":
        desc = "TermDTSort"

      l0 = []
      w = l0.append
      if jira:
        w("|%s" % desc)
      elif html:
        w("<tr>")
        w("<td>%s</td>" % htmlEscape(desc))
      else:
        w("%32s" % desc)

      # baseMS, cmpMS are lists of milli-seconds of the run-time for
      # this task across the N JVMs:
      baseMS, baseTotHitCount = agg(baseResults, cat, "base", self.verifyCounts)
      cmpMS, cmpTotHitCount = agg(cmpResults, cat, "cmp", self.verifyCounts)

      baseQPS = [1000.0 / x for x in baseMS]
      cmpQPS = [1000.0 / x for x in cmpMS]

      # Aggregate stats over the N JVMs we ran:
      minQPSBase, maxQPSBase, avgQPSBase, qpsStdDevBase = stats(baseQPS)
      minQPSCmp, maxQPSCmp, avgQPSCmp, qpsStdDevCmp = stats(cmpQPS)

      resultsByCatCmp[desc] = (minQPSCmp, maxQPSCmp, avgQPSCmp, qpsStdDevCmp)

      if VERBOSE:
        print("cat %s" % str(cat))
        print("  baseQPS: %s" % " ".join("%.1f" % x for x in baseQPS))
        print("    avg %.1f" % avgQPSBase)
        print("  cmpQPS: %s" % " ".join("%.1f" % x for x in cmpQPS))
        print("    avg %.1f" % avgQPSCmp)

      qpsBase = avgQPSBase
      qpsCmp = avgQPSCmp

      # print '%s: %s' % (desc, abs(qpsBase-qpsCmp) / ((maxQPSBase-minQPSBase)+(maxQPSCmp-minQPSCmp)))
      if (qpsStdDevBase != 0 or qpsStdDevCmp != 0) and len(baseQPS) == len(cmpQPS):
        # Student's T-Test is often used for distributions with the same underlying variance and number of samples, but
        # with large number of samples, Student's T distribution approaches a normal distribution

        # the "combined" std.dev. is the square root of the average of the two variances.
        # the factor of 2 here cancels out with one below, but is left in for clarity of nomenclature
        qpsStdDev = math.sqrt((qpsStdDevBase * qpsStdDevBase + qpsStdDevCmp * qpsStdDevCmp) / 2.0)

        # t-value is the difference of the means normalized by the combined std.dev.
        if qpsStdDev != 0 and len(baseQPS) != 0:
          tValue = abs(qpsCmp - qpsBase) / (qpsStdDev * math.sqrt(2.0 / len(baseQPS)))
        else:
          tValue = float("inf")

        # then we have tValue = exp(-x/(2 . stddev^2)) and
        # pValue = 1 - 2 * Integral(exp(-x/(2 . stddev^2)) [0 to tValue]) as the probability of the null hypothesis (that the
        # two means are drawn from the same distribution, using a "two-tailed" test).
        # We have no closed form solution for the Gaussian integral, but python has erf() which is its residual
        pValue = 1 - math.erf(tValue / math.sqrt(2))
        # We pick an arbitrary  but typical confidence interval for "significance":
        significant = pValue <= 0.05
      else:
        pValue = 1.0
        significant = False

      if self.verifyCounts and baseTotHitCount != cmpTotHitCount:
        warnings.append("cat=%s: hit counts differ: %s vs %s" % (desc, baseTotHitCount, cmpTotHitCount))

      if jira:
        w("|%.2f|%.2f|%.2f|%.2f" % (qpsBase, qpsStdDevBase, qpsCmp, qpsStdDevCmp))
      elif html:
        if qpsBase == 0.0:
          p1 = "(n/a%)"
          p2 = "(n/a%)"
        else:
          p1 = "(%.1f%%)" % (100 * qpsStdDevBase / qpsBase)
          p2 = "(%.1f%%)" % (100 * qpsStdDevCmp / qpsBase)
        w("<td>%.1f</td><td>%s</td><td>%.1f</td><td>%s</td>" % (qpsBase, p1, qpsCmp, p2))
      else:
        if qpsBase == 0.0:
          p1 = "(n/a%)"
          p2 = "(n/a%)"
        else:
          p1 = "(%.1f%%)" % (100 * qpsStdDevBase / qpsBase)
          p2 = "(%.1f%%)" % (100 * qpsStdDevCmp / qpsBase)
        w("%12.2f%12s%12.2f%12s" % (qpsBase, p1, qpsCmp, p2))

      if qpsBase == 0.0:
        psAvg = 0.0
      else:
        psAvg = 100.0 * (qpsCmp - qpsBase) / qpsBase

      qpsBaseBest = qpsBase + qpsStdDevBase
      qpsBaseWorst = qpsBase - qpsStdDevBase

      qpsCmpBest = qpsCmp + qpsStdDevCmp
      qpsCmpWorst = qpsCmp - qpsStdDevCmp

      if qpsBaseWorst == 0.0:
        psBest = psWorst = 0
      else:
        psBest = int(100.0 * (qpsCmpBest - qpsBaseWorst) / qpsBaseWorst)
        psWorst = int(100.0 * (qpsCmpWorst - qpsBaseBest) / qpsBaseBest)

      if jira:
        w("|%s-%s" % (jiraColor(psWorst), jiraColor(psBest)))
      elif html:
        # w('<td>%s (%s - %s)</td>' % (htmlColor(psAvg), htmlColor(psWorst), htmlColor(psBest)))
        w("<td>%s</td>" % htmlColor2(1 + psAvg / 100.0))
      else:
        w("%16s" % ("%7.1f%% (%4d%% - %4d%%)" % (psAvg, psWorst, psBest)))

      if jira:
        w("|%s" % pValueColor(pValue, "jira"))
      elif html:
        w("<td>%s</td>" % pValueColor(pValue, "html"))
      else:
        w("%6.3f" % pValue)

      if jira:
        w("|\n")
      else:
        w("\n")

      if constants.SORT_REPORT_BY == "p-value":
        sortBy = pValue
      elif constants.SORT_REPORT_BY == "pctchange":
        sortBy = psAvg
      elif constants.SORT_REPORT_BY == "query":
        sortBy = desc
      else:
        raise RuntimeError("invalid result sort %s" % constants.SORT_REPORT_BY)

      lines.append((sortBy, "".join(l0)))
      if True or significant:
        chartData.append(
          (
            sortBy,
            desc,
            qpsBase - qpsStdDevBase,
            qpsBase + qpsStdDevBase,
            qpsCmp - qpsStdDevCmp,
            qpsCmp + qpsStdDevCmp,
            pValue,
          )
        )

    lines.sort()
    chartData.sort()
    chartData = [x[1:] for x in chartData]

    if QPSChart.supported:
      try:
        QPSChart.QPSChart(chartData, "out.png")
      except:
        print(f"WARNING: failed to generate QPS diff chart chartData={chartData}; skipping")
        traceback.print_exc()
      else:
        print(f"Chart saved to out.png... (wd: {os.getcwd()})")

    w = writer

    catSet = set()

    baseLatencyMetrics = self.computeTaskLatencies(baseTaskLatencies, catSet)
    cmpLatencyMetrics = self.computeTaskLatencies(cmpTaskLatencies, catSet)

    for currentCat in catSet:
      if currentCat not in baseLatencyMetrics:
        # When we add a whole new task (e.g. VectorSearch), just skip the comparison for the first nightly run
        # since baseline will not have this task yet:
        continue
      currentBaseMetrics = baseLatencyMetrics[currentCat]
      currentCmpMetrics = cmpLatencyMetrics[currentCat]
      pctP50 = 100 * (currentCmpMetrics["p50"] - currentBaseMetrics["p50"]) / currentBaseMetrics["p50"]
      pctP90 = 100 * (currentCmpMetrics["p90"] - currentBaseMetrics["p90"]) / currentBaseMetrics["p90"]
      pctP99 = 100 * (currentCmpMetrics["p99"] - currentBaseMetrics["p99"]) / currentBaseMetrics["p99"]
      pctP999 = 100 * (currentCmpMetrics["p999"] - currentBaseMetrics["p999"]) / currentBaseMetrics["p999"]
      pctP100 = 100 * (currentCmpMetrics["p100"] - currentBaseMetrics["p100"]) / currentBaseMetrics["p100"]
      print(
        "||Task %s||P50 Base %s||P50 Cmp %s||Pct Diff %s||P90 Base %s||P90 Cmp %s||Pct Diff %s||P99 Base %s||P99 Cmp %s||Pct Diff %s||P999 Base %s||P999 Cmp %s||Pct Diff %s||P100 Base %s||P100 Cmp %s||Pct Diff %s"
        % (
          currentCat,
          currentBaseMetrics["p50"],
          currentCmpMetrics["p50"],
          pctP50,
          currentBaseMetrics["p90"],
          currentCmpMetrics["p90"],
          pctP90,
          currentBaseMetrics["p99"],
          currentCmpMetrics["p99"],
          pctP99,
          currentBaseMetrics["p999"],
          currentCmpMetrics["p999"],
          pctP999,
          currentBaseMetrics["p100"],
          currentCmpMetrics["p100"],
          pctP100,
        )
      )

    if jira:
      w("||Task||QPS %s||StdDev %s||QPS %s||StdDev %s||Pct diff||p-value||" % (baseDesc, baseDesc, cmpDesc, cmpDesc))
    elif html:
      w("<table>")
      w("<tr>")
      w("<th>Task</th>")
      w("<th>QPS %s</th>" % baseDesc)
      w("<th>StdDev %s</th>" % baseDesc)
      w("<th>QPS %s</th>" % cmpDesc)
      w("<th>StdDev %s</th>" % cmpDesc)
      w("<th>% change</th>")
      w("<th>p-value</th>")
      w("</tr>")
    else:
      w("%32s" % "Task")
      w("%12s" % ("QPS %s" % baseDesc))
      w("%12s" % "StdDev")
      w("%12s" % ("QPS %s" % cmpDesc))
      w("%12s" % "StdDev")
      w("%24s" % "Pct diff")
      w("%8s" % "p-value")

    if jira:
      w("||\n")
    else:
      w("\n")

    for ign, s in lines:
      w(s)

    if html:
      w("</table>")

    for w in warnings:
      print("WARNING: %s" % w)

    return resultsByCatCmp, cmpDiffs, stats(heapCmp)

  def compare(self, baseline, newList, *params):
    for new in newList:
      if new.numHits != baseline.numHits:
        raise RuntimeError("baseline found %d hits but new found %d hits" % (baseline[0], new[0]))

      warmOld = baseline.warmTime
      warmNew = new.warmTime
      qpsOld = baseline.bestQPS
      qpsNew = new.bestQPS
      pct = 100.0 * (qpsNew - qpsOld) / qpsOld
      # print '  diff: %.1f%%' % pct

      pct = 100.0 * (warmNew - warmOld) / warmOld
      # print '  warmdiff: %.1f%%' % pct

    self.results.append([baseline] + [newList] + list(params))

  def save(self, name):
    f = open("%s.pk" % name, "wb")
    pickle.dump(self.results, f)
    f.close()


def getAntClassPath(checkout):
  path = checkoutToPath(checkout)
  cp = []

  # We use the jar file for core to leverage the MR JAR
  core_jar_file = None
  for filename in os.listdir("%s/lucene/build/core" % path):
    if reCoreJar.match(filename) is not None:
      core_jar_file = "%s/lucene/build/core/%s" % (path, filename)
      break
  if core_jar_file is None:
    raise RuntimeError("can't find core JAR file in %s" % ("%s/lucene/build/core" % path))

  cp.append(core_jar_file)
  cp.append("%s/lucene/build/sandbox/classes/java" % path)
  cp.append("%s/lucene/build/misc/classes/java" % path)
  cp.append("%s/lucene/build/facet/classes/java" % path)
  cp.append("/home/mike/src/lucene-c-boost/dist/luceneCBoost-SNAPSHOT.jar")
  cp.append("%s/lucene/build/analysis/common/classes/java" % path)
  cp.append("%s/lucene/build/analysis/icu/classes/java" % path)
  cp.append("%s/lucene/build/queryparser/classes/java" % path)
  cp.append("%s/lucene/build/grouping/classes/java" % path)
  cp.append("%s/lucene/build/suggest/classes/java" % path)
  cp.append("%s/lucene/build/highlighter/classes/java" % path)
  cp.append("%s/lucene/build/codecs/classes/java" % path)
  cp.append("%s/lucene/build/queries/classes/java" % path)

  # so perf.* is found:
  lib = os.path.join(checkoutToUtilPath(checkout), "lib")
  for f in os.listdir(lib):
    if f.endswith(".jar"):
      cp.append(os.path.join(lib, f))
  cp.append(os.path.join(checkoutToUtilPath(checkout), "build"))

  return tuple(cp)


def getClassPath(checkout):
  path = checkoutToPath(checkout)
  if not os.path.exists(os.path.join(path, "build.gradle")):
    return getAntClassPath(checkout)

  cp = []

  # We use the jar file for core to leverage the MR JAR
  core_jar_file = None
  for filename in os.listdir("%s/lucene/core/build/libs" % path):
    if reCoreJar.match(filename) is not None:
      core_jar_file = "%s/lucene/core/build/libs/%s" % (path, filename)
      break
  if core_jar_file is None:
    raise RuntimeError("can't find core JAR file in %s" % ("%s/lucene/core/build/libs" % path))

  cp.append(core_jar_file)
  cp.append("%s/lucene/sandbox/build/classes/java/main" % path)
  cp.append("%s/lucene/misc/build/classes/java/main" % path)
  cp.append("%s/lucene/facet/build/classes/java/main" % path)
  cp.append("%s/lucene/analysis/common/build/classes/java/main" % path)
  cp.append("%s/lucene/analysis/icu/build/classes/java/main" % path)
  cp.append("%s/lucene/queryparser/build/classes/java/main" % path)
  cp.append("%s/lucene/grouping/build/classes/java/main" % path)
  cp.append("%s/lucene/suggest/build/classes/java/main" % path)
  cp.append("%s/lucene/highlighter/build/classes/java/main" % path)
  cp.append("%s/lucene/codecs/build/classes/java/main" % path)
  cp.append("%s/lucene/queries/build/classes/java/main" % path)
  cp.append("%s/lucene/join/build/classes/java/main" % path)
  cp.append("%s/lucene/spatial3d/build/classes/java/main" % path)

  # self.addJars(cp, '%s/lucene/facet/lib' % path)

  # so perf.* is found:
  lib = os.path.join(checkoutToUtilPath(checkout), "lib")
  for f in os.listdir(lib):
    if f.endswith(".jar"):
      cp.append(os.path.join(lib, f))
  # TODO: reconcile this!  one or the other?
  cp.append(os.path.join(checkoutToUtilPath(checkout), "src/main/build/classes/java/main"))
  cp.append(os.path.join(checkoutToUtilPath(checkout), "build"))

  return tuple(cp)


def classPathToString(cp):
  return common.pathsep().join(cp)


reFuzzy = re.compile(r"body:(.*?)\~(.*?)$")


# converts unit~0.7 -> unit~1
def fixupFuzzy(query):
  m = reFuzzy.search(query)
  if m is not None:
    term, fuzzOrig = m.groups()
    fuzz = float(fuzzOrig)
    if fuzz < 1.0:
      editDistance = int((1.0 - fuzz) * len(term))
      query = query.replace("~%s" % fuzzOrig, "~%s.0" % editDistance)
  return query


def tasksToMap(taskIters, verifyScores, verifyCounts):
  d = {}
  if len(taskIters) > 0:
    # Make sure same task returned same results w/in this one JVM iter (self-consistent), since
    # each task runs N times for each of M threads:
    for run_iter, tasks in enumerate(taskIters):
      # Gather all task runs for this one JVM iter:
      run_d = {}
      for task in tasks:
        if task not in run_d:
          run_d[task] = [task, 0]
        else:
          run_d[task][1] += 1
          try:
            task.verifySame(run_d[task][0], verifyScores, verifyCounts)
          except RuntimeError as re:
            # BUG
            raise RuntimeError(
              f"ERROR: hits within a single JVM iter are not self-consistent; something is acting non-deterministically within one JVM?  task instance 0 and instance {run_d[task][1]} in JVM {run_iter} differ"
            ) from re

      # now make sure this JVM iter's results match prior JVMs:
      if len(d) == 0:
        d = run_d
      else:
        for task in run_d:
          if task not in d:
            # BUG
            raise RuntimeError(f"ERROR: tasks differ from one iteration to the next: task={task} in JVM {run_iter} is missing from JVM 0")
          # Make sure same task returned same results across JVMs:
          try:
            task.verifySame(d[task][0], verifyScores, verifyCounts)
          except RuntimeError as re:
            # BUG
            raise RuntimeError(f"ERROR: hits across JVMs differ; something is acting non-deterministically across JVMs?  run 0 vs run {run_iter}") from re
        for task in d.keys():
          if task not in run_d:
            # BUG
            raise RuntimeError(f"ERROR: JVM {run_iter} has task {task} not seen in JVM 0?")

  # strip off the task iteration:
  d2 = {}
  for key, val in d.items():
    d2[key] = val[0]

  return d2


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
      except RuntimeError as re:
        errors.append(str(re))
      checked += 1
    else:
      onlyInD1 += 1

  onlyInD2 = len(d2) - (len(d1) - onlyInD1)

  warnings = []
  if len(d1) != len(d2):
    # not necessarily an error because we may have added new tasks in nightly bench
    warnings.append("non-overlapping tasks onlyInD1=%s onlyInD2=%s" % (onlyInD1, onlyInD2))

  if checked == 0:
    warnings.append("no results were checked")

  if len(warnings) == 0 and len(errors) == 0:
    return None
  inBoth = (len(d1) + len(d2) - onlyInD1 - onlyInD2) / 2
  overlap = inBoth / float(max(len(d1), len(d2)))
  return warnings, errors, overlap


def htmlEscape(s):
  return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def getSegmentCount(indexPath):
  segCount = 0
  for fileName in os.listdir(indexPath):
    if fileName.endswith(".fdx") or fileName.endswith(".cfs"):
      segCount += 1
  return segCount


reBrokenLine = re.compile("^\\ +#")


def fixupPerfOutput(s):
  lines = s.split("\n")
  linesOut = []
  for line in lines:
    if reBrokenLine.match(line) is not None:
      linesOut[-1] += " " + line.rstrip()
    else:
      linesOut.append(line.rstrip())
  return "\n".join(linesOut)


def profilerOutput(javaCommand, jfrOutput, checkoutPath, profilerCount, profilerStackSize):
  profilerResults = []

  for mode in "cpu", "heap":
    for stackSize in profilerStackSize:
      profileCommand = javaCommand.split(" ") + [
        "-cp",
        f"{checkoutPath}/build-tools/build-infra/build/classes/java/main",
        f"-Dtests.profile.mode={mode}",
        f"-Dtests.profile.count={profilerCount}",
        f"-Dtests.profile.stacksize={stackSize}",
        "org.apache.lucene.gradle.ProfileResults",
        jfrOutput,
      ]
      print(f"profile command: {profileCommand}")
      try:
        result = subprocess.run(profileCommand, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True)
      except subprocess.CalledProcessError as e:
        print(f"command failed:\n  stderr:\n{e.stderr}\n  stdout:\n{e.stdout}")
        raise
      output = f"\nProfiler for {mode}:\n{result.stdout.decode('utf-8')}"
      print(output)
      profilerResults.append((mode, stackSize, output))
  return profilerResults
