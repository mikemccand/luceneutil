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

import os
import searchBench
import benchUtil
import constants
import random
import common

class Data(object):
  
  def __init__(self, name, lineFile, numDocs, tasksFile):
    self.name = name
    self.lineFile = lineFile
    self.numDocs = numDocs
    self.tasksFile = tasksFile

WIKI_MEDIUM_ALL = Data('wikimediumall', constants.WIKI_MEDIUM_DOCS_LINE_FILE, constants.WIKI_MEDIUM_DOCS_COUNT, constants.WIKI_MEDIUM_TASKS_10MDOCS_FILE)
WIKI_MEDIUM_10M = Data('wikimedium10m', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 10000000, constants.WIKI_MEDIUM_TASKS_10MDOCS_FILE)
WIKI_MEDIUM_5M = Data('wikimedium5m', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 5000000, constants.WIKI_MEDIUM_TASKS_10MDOCS_FILE)
WIKI_MEDIUM_1M = Data('wikimedium1m', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 1000000, constants.WIKI_MEDIUM_TASKS_1MDOCS_FILE)
WIKI_MEDIUM_10K = Data('wikimedium10k', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 10000, constants.WIKI_MEDIUM_TASKS_1MDOCS_FILE)
WIKI_MEDIUM_500K = Data('wikimedium500k', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 500000, constants.WIKI_MEDIUM_TASKS_1MDOCS_FILE)
WIKI_MEDIUM_2M = Data('wikimedium2m', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 2000000, constants.WIKI_MEDIUM_TASKS_1MDOCS_FILE)

MEME_ALL = Data('memeall',
                '/x/lucene/data/memetracker/lines.txt',
                210999824,
                constants.WIKI_MEDIUM_TASKS_10MDOCS_FILE)

WIKI_BIG = Data('wikibigall', constants.WIKI_BIG_DOCS_LINE_FILE, constants.WIKI_BIG_DOCS_COUNT, constants.WIKI_BIG_TASKS_FILE)
WIKI_BIG_10K = Data('wikibig10k', constants.WIKI_BIG_DOCS_LINE_FILE, 10000, constants.WIKI_BIG_TASKS_FILE)
WIKI_BIG_100K = Data('wikibig100k', constants.WIKI_BIG_DOCS_LINE_FILE, 100000, constants.WIKI_BIG_TASKS_FILE)
WIKI_BIG_1M = Data('wikibig1m', constants.WIKI_BIG_DOCS_LINE_FILE, 1000000, constants.WIKI_BIG_TASKS_FILE)

EURO_MEDIUM = Data('euromedium', constants.EUROPARL_MEDIUM_DOCS_LINE_FILE, 5000000, constants.EUROPARL_MEDIUM_TASKS_FILE)

DATA = {'wikimediumall': WIKI_MEDIUM_ALL,
        'wikimedium10m' : WIKI_MEDIUM_10M,
        'wikimedium1m' : WIKI_MEDIUM_1M,
        'wikimedium500k' : WIKI_MEDIUM_500K,
        'wikimedium10k' : WIKI_MEDIUM_10K,
        'wikimedium5m' : WIKI_MEDIUM_5M,
        'wikimedium2m' : WIKI_MEDIUM_2M,
        'memeall': MEME_ALL,
        'wikibigall' : WIKI_BIG,
        'wikibig10k' : WIKI_BIG_10K,
        'wikibig100k' : WIKI_BIG_100K,
        'wikibig1m' : WIKI_BIG_1M,
        'euromedium' : EURO_MEDIUM }

# for multi-segment index:
SEGS_PER_LEVEL = 5

def sourceData(key=None):
  if not key:
    import sys
    if '-source' in sys.argv:
      key = sys.argv[1+sys.argv.index('-source')]
  if key in DATA:
    return DATA[key]
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
               waitForMerges = True,
               waitForCommit = True,
               disableIOThrottle = False,
               bodyTermVectors = False,
               bodyStoredFields = False,
               bodyPostingsOffsets = False,
               useCMS = False,
               facets = None,
               extraNamePart = None,
               facetDVFormat = constants.FACET_FIELD_DV_FORMAT_DEFAULT,
               maxConcurrentMerges = 1,  # use 1 for spinning-magnets and 3 for fast SSD
               addDVFields = False,
               name = None,
               indexSort = None
               ):
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
    self.extraNamePart = extraNamePart
    if ramBufferMB == -1:
      self.maxBufferedDocs = self.numDocs // (SEGS_PER_LEVEL*111)
    else:
      self.maxBufferedDocs = -1
    self.mergePolicy = mergePolicy
    self.doUpdate = doUpdate
    self.useCFS = useCFS
    self.javaCommand = javaCommand
    self.maxConcurrentMerges = maxConcurrentMerges
    self.addDVFields = addDVFields

    self.lineDocSource = dataSource.lineFile
    self.verbose = verbose
    self.printDPS = printDPS
    self.waitForMerges = waitForMerges
    self.waitForCommit = waitForCommit
    self.disableIOThrottle = disableIOThrottle
    self.idFieldPostingsFormat = idFieldPostingsFormat
    self.bodyTermVectors = bodyTermVectors
    self.bodyStoredFields = bodyStoredFields
    self.bodyPostingsOffsets = bodyPostingsOffsets
    self.facets = facets
    self.facetDVFormat = facetDVFormat
    self.assignedName = name
    self.indexSort = indexSort
    
    self.mergeFactor = 10
    if SEGS_PER_LEVEL >= self.mergeFactor:
      raise RuntimeError('SEGS_PER_LEVEL (%s) is greater than mergeFactor (%s)' % (SEGS_PER_LEVEL, mergeFactor))
    self.useCMS = useCMS
    
  def getName(self):
    if self.assignedName is not None:
      return self.assignedName
    
    name = [self.dataSource.name,
            self.checkout]

    if self.extraNamePart is not None:
      name.append(self.extraNamePart)
      
    if self.optimize:
      name.append('opt')

    if self.useCFS:
      name.append('cfs')

    if self.facets is not None:
      name.append('facets')
      for arg in self.facets:
        name.append(arg[0])
      name.append(self.facetDVFormat)

    if self.bodyTermVectors:
      name.append('tv')

    if self.bodyStoredFields:
      name.append('stored')

    if self.bodyPostingsOffsets:
      name.append('offsets')

    name.append(self.postingsFormat)
    if self.postingsFormat != self.idFieldPostingsFormat:
      name.append(self.idFieldPostingsFormat)

    if self.addDVFields:
      name.append('dvfields')

    if self.indexSort:
      name.append('sort=%s' % self.indexSort)
      
    name.append('nd%gM' % (self.numDocs/1000000.0))
    return '.'.join(name)

class Competitor(object):

  doSort = False

  def __init__(self, name, checkout,
               index = None,
               numThreads = constants.SEARCH_NUM_THREADS,
               directory = 'MMapDirectory',
               analyzer = None,
               commitPoint = 'multi',
               similarity = constants.SIMILARITY_DEFAULT,
               javaCommand = constants.JAVA_COMMAND,
               printHeap = False,
               hiliteImpl = 'FastVectorHighlighter',
               pk = True,
               loadStoredFields = False,
               concurrentSearches = False,
               javacCommand = constants.JAVAC_EXE):
    self.name = name
    self.checkout = checkout
    self.numThreads = numThreads
    self.index = index
    self.directory = directory
    if analyzer is None:
      if index is not None:
        analyzer = index.analyzer
      else:
        analyzer = constants.ANALYZER_DEFAULT
    self.analyzer = analyzer
    self.commitPoint = commitPoint
    self.similarity = similarity
    self.javaCommand = javaCommand
    self.printHeap = printHeap
    self.hiliteImpl = hiliteImpl
    self.pk = pk
    self.loadStoredFields = loadStoredFields
    self.javacCommand = javacCommand
    self.concurrentSearches = concurrentSearches

  def compile(self, cp):
    root = benchUtil.checkoutToUtilPath(self.checkout)

    perfSrc = os.path.join(root, "src/main")
      
    buildDir = os.path.join(root, "build")
    if not os.path.exists(buildDir):
      os.makedirs(buildDir)

    # Try to be faster than ant; this may miss changes, e.g. a static final constant changed in core that is used in another module:
    if common.getLatestModTime(perfSrc) <= common.getLatestModTime(buildDir, '.class'):
      print('Skip compiling luceneutil: all .class are up to date')
      return

    files = ['%s/perf/%s' % (perfSrc, x) for x in (
      'Args.java',
      'IndexState.java',
      'IndexThreads.java',
      'NRTPerfTest.java',
      'Indexer.java',
      'KeepNoCommitsDeletionPolicy.java',
      'LineFileDocs.java',
      'LocalTaskSource.java',
      'OpenDirectory.java',
      'PKLookupTask.java',
      'PointsPKLookupTask.java',
      'PerfUtils.java',
      'RandomQuery.java',
      'RemoteTaskSource.java',
      'RespellTask.java',
      'SearchPerfTest.java',
      'SearchTask.java',
      'StatisticsHelper.java',
      'Task.java',
      'TaskParser.java',
      'TaskSource.java',
      'TaskThreads.java',
      )]

    print('files %s' % files)
    
    benchUtil.run('%s -d %s -classpath "%s" %s' % (self.javacCommand, buildDir, cp, ' '.join(files)), os.path.join(constants.LOGS_DIR, 'compile.log'))
    # copy resources/META-INF
    if os.path.exists(os.path.join(perfSrc, 'resources/*')):
      benchUtil.run('cp -r %s %s' % (os.path.join(perfSrc, 'resources/*'), buildDir.replace("\\", "/")))

class Competition(object):

  def __init__(self, cold=False,
               printCharts=False,
               verifyScores=True,
               verifyCounts=True,
               remoteHost=None,
               # Pass fixed randomSeed so separate runs are comparable (pick the same tasks):
               randomSeed=None,
               benchSearch=True,
               taskCountPerCat = 1,
               taskRepeatCount = 20,
               jvmCount = 20):
    self.cold = cold
    self.competitors = []
    self.indices = []
    self.printCharts = printCharts 
    self.benchSearch = benchSearch
    self.benchIndex = True
    self.verifyScores = verifyScores
    self.verifyCounts = verifyCounts
    self.onlyTaskPatterns = None
    self.notTaskPatterns = None
    # TODO: not implemented yet
    self.remoteHost = remoteHost
    if randomSeed is not None:
      self.randomSeed = randomSeed
    else:
      self.randomSeed = random.randint(-10000000, 1000000)

    # How many queries in each category to repeat.  Increasing this
    # beyond 1 doesn't seem to alter relative results, ie most queries
    # within a category behave the same.  Note that even with 1,
    # you'll get a random choice each time you run the competition:
    self.taskCountPerCat = taskCountPerCat

    # How many times to run each query.  Curiously anything higher than
    # ~15 (I've tested up to 1000) doesn't alter results, ie once
    # hotspot compiles (after 1st or 2nd time the query is run) it
    # doesn't seem to re-compile:
    self.taskRepeatCount = taskRepeatCount

    # JVM count: how many times to run the java process for each
    # competitor.  Increase this to get more repeatable results, because each run can compile the
    # code differently.  Often the results are bi or tri modal for a
    # given query.
    self.jvmCount = jvmCount
    

  def addTaskPattern(self, pattern):
    if self.onlyTaskPatterns is None:
      self.onlyTaskPatterns = []
    self.onlyTaskPatterns.append(pattern)

  def addNotTaskPattern(self, pattern):
    if self.notTaskPatterns is None:
      self.notTaskPatterns = []
    self.notTaskPatterns.append(pattern)

  def newIndex(self, checkout, data, **kwArgs):
    index = Index(checkout, data, **kwArgs)
    self.indices.append(index)
    return index

  def competitor(self, name, checkout=None, **kwArgs):
    if not checkout:
      checkout = name
    c = Competitor(name, checkout, **kwArgs)
    c.competition = self
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
                    search = self.benchSearch, index = self.benchIndex,
                    verifyScores = self.verifyScores, verifyCounts = self.verifyCounts,
                    taskPatterns = (self.onlyTaskPatterns, self.notTaskPatterns), randomSeed = self.randomSeed)
    return self

  def clearCompetitors(self):
    self.competitors = []

  def clearIndices(self):
    self.indices = []
