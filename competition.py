#
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
#

import glob
import searchBench
import benchUtil
import constants

class Data(object):
  
  def __init__(self, name, lineFile, numDocs, tasksFile):
    self.name = name
    self.lineFile = lineFile
    self.numDocs = numDocs
    self.tasksFile = tasksFile

WIKI_MEDIUM_10M = Data('wikimedium10m', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 10000000, constants.WIKI_MEDIUM_TASKS_10MDOCS_FILE)
WIKI_MEDIUM_5M = Data('wikimedium5m', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 5000000, constants.WIKI_MEDIUM_TASKS_10MDOCS_FILE)
WIKI_MEDIUM_1M = Data('wikimedium1m', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 1000000, constants.WIKI_MEDIUM_TASKS_1MDOCS_FILE)
WIKI_MEDIUM_2M = Data('wikimedium2m', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 2000000, constants.WIKI_MEDIUM_TASKS_1MDOCS_FILE)

WIKI_BIG = Data('wikibig', constants.WIKI_BIG_DOCS_LINE_FILE, 3000000, constants.WIKI_BIG_TASKS_FILE)
EURO_MEDIUM = Data('euromedium', constants.EUROPARL_MEDIUM_DOCS_LINE_FILE, 5000000, constants.EUROPARL_MEDIUM_TASKS_FILE)

DATA = {'wikimedium10m' : WIKI_MEDIUM_10M,
        'wikimedium1m' : WIKI_MEDIUM_1M,
        'wikimedium5m' : WIKI_MEDIUM_5M,
        'wikimedium2m' : WIKI_MEDIUM_2M,
        'wikibig' : WIKI_BIG,
        'euromedium' : EURO_MEDIUM }

# for multi-segment index:
SEGS_PER_LEVEL = 5

def sourceData(key=None):
  if not key:
    import sys
    if '-source' in sys.argv:
      key = sys.argv[1+sys.argv.index('-source')]
    else:
      raise RuntimeError('please specify -source (wikimedium10m, wikimedium1m, wikibig, euromedium)')
  if key in DATA:
    return DATA[key]
  else:
    raise RuntimeError('unknown data source (valid keys: %s)' % DATA.keys())

class Index(object):

  def __init__(self, checkout, dataSource,
               analyzer = constants.ANALYZER_DEFAULT,
               postingsFormat = constants.POSTINGS_FORMAT_DEFAULT,
               idFieldPostingsFormat = constants.ID_FIELD_POSTINGS_FORMAT_DEFAULT,
               numThreads = constants.INDEX_NUM_THREADS,
               optimize = False,
               directory = 'MMapDirectory',
               doDeletions = False,
               ramBufferMB = -1,
               mergePolicy = constants.MERGEPOLICY_DEFAULT,
               doUpdate = False,
               useCFS = False,
               javaCommand = constants.JAVA_COMMAND,
               grouping = True,
               verbose = False,
               printDPS = False,
               waitForMerges = True):
    self.checkout = checkout
    self.dataSource = dataSource
    self.analyzer = analyzer
    self.postingsFormat = postingsFormat
    self.numThreads = numThreads
    self.optimize = optimize
    self.directory = directory
    self.doDeletions = doDeletions
    self.grouping = grouping
    self.ramBufferMB = ramBufferMB
    self.numDocs = dataSource.numDocs
    if ramBufferMB == -1:
      self.maxBufferedDocs = self.numDocs/ (SEGS_PER_LEVEL*111)
    else:
      self.maxBufferedDocs = -1
    self.mergePolicy = mergePolicy
    self.doUpdate = doUpdate
    self.useCFS = useCFS
    self.javaCommand = javaCommand

    self.lineDocSource = dataSource.lineFile
    self.verbose = verbose
    self.printDPS = printDPS
    self.waitForMerges = waitForMerges
    self.idFieldPostingsFormat = idFieldPostingsFormat

    self.mergeFactor = 10
    if SEGS_PER_LEVEL >= self.mergeFactor:
      raise RuntimeError('SEGS_PER_LEVEL (%s) is greater than mergeFactor (%s)' % (SEGS_PER_LEVEL, mergeFactor))

  def getName(self):
    if self.optimize:
      s = 'opt.'
    else:
      s = ''
    if self.useCFS:
      s2 = 'cfs.'
    else:
      s2 = ''
    return '%s.%s.%s.%s%snd%gM' % (self.dataSource.name, self.checkout, self.postingsFormat, s, s2, self.numDocs/1000000.0)

class Competitor(object):

  doSort = False

  def __init__(self, name, checkout,
               index = None,
               numThreads = constants.SEARCH_NUM_THREADS,
               directory = 'MMapDirectory',
               analyzer = constants.ANALYZER_DEFAULT,
               commitPoint = 'multi',
               similarity = constants.SIMILARITY_DEFAULT,
               javaCommand = constants.JAVA_COMMAND,
               printHeap = False):
    self.name = name
    self.checkout = checkout
    self.numThreads = numThreads
    self.index = index
    self.directory = directory
    self.analyzer = analyzer
    self.commitPoint = commitPoint
    self.similarity = similarity
    self.javaCommand = javaCommand
    self.printHeap = printHeap

  def compile(self, cp):
    files = glob.glob('perf/*.java')
    for skip in ('PKLookupPerfTest', 'PKLookupUpdatePerfTest'):
      try:
        files.remove('perf/%s.java' % skip)
      except ValueError:
        pass
    benchUtil.run('javac -classpath "%s" %s >> compile.log 2>&1' % (cp, ' '.join(files)), 'compile.log')

class Competition(object):

  def __init__(self, cold=False,
               printCharts=False,
               debug=False,
               verifyScores=True,
               # Pass fixed randomSeed so separate runs are comparable (pick the same tasks):
               randomSeed=None):
    self.cold = cold
    self.competitors = []
    self.indices = []
    self.printCharts = printCharts 
    self.debug = debug
    self.benchSearch = True
    self.benchIndex = True
    self.verifyScores = verifyScores
    self.onlyTaskPatterns = None
    if randomSeed is not None:
      self.randomSeed = randomSeed
    else:
      self.randomSeed = random.randint(-10000000, 1000000)

  def addTaskPattern(self, pattern):
    if self.onlyTaskPatterns is None:
      self.onlyTaskPatterns = []
    self.onlyTaskPatterns.append(pattern)

  def newIndex(self, checkout, data, **kwArgs):
    index = Index(checkout, data, **kwArgs)
    self.indices.append(index)
    return index

  def competitor(self, name, checkout=None, **kwArgs):
    if not checkout:
      checkout = name
    c = Competitor(name, checkout, **kwArgs)
    self.competitors.append(c)
    return c
  
  def skipIndex(self):
    self.benchIndex = False

  def skipSearch(self):
    self.benchSearch = False
    
  def benchmark(self, id):
    if len(self.competitors) != 2:
      raise RuntimeError('expected 2 competitors but was %d' % (len(self.competitors)))
    if not self.indices:
      raise RuntimeError('expected at least one index use withIndex(...)')

    # If a competitor is named 'base', use that as base:
    base = None
    for c in self.competitors:
      if c.name == 'base':
        base = c
        break
    if base is None:
      base = self.competitors[0]
      challenger = self.competitors[1]
    else:
      if base == self.competitors[0]:
        challenger = self.competitors[1]
      else:
        challenger = self.competitors[0]

    base.tasksFile = base.index.dataSource.tasksFile
    challenger.tasksFile = challenger.index.dataSource.tasksFile

    searchBench.run(id, base, challenger, coldRun = self.cold, doCharts = self.printCharts,
                    search = self.benchSearch, index = self.benchIndex, debugs = self.debug, debug = self.debug,
                    verifyScores = self.verifyScores, taskPatterns = self.onlyTaskPatterns, randomSeed = self.randomSeed)
    return self

  def clearCompetitors(self):
    self.competitors = []

  def clearIndices(self):
    self.indices = []
