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

import benchUtil
import common
import constants
import glob
import os
import random
import searchBench
import subprocess
import time

if hasattr(constants, 'SEARCH_NUM_THREADS'):
  raise RuntimeError('please rename your localconstants.py SEARCH_NUM_THREADS to SEARCH_NUM_CONCURRENT_QUERIES')

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
WIKI_MEDIUM_500 = Data('wikimedium500', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 500, constants.WIKI_MEDIUM_TASKS_500DOCS_FILE)
WIKI_MEDIUM_500K = Data('wikimedium500k', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 500000, constants.WIKI_MEDIUM_TASKS_1MDOCS_FILE)
WIKI_MEDIUM_2M = Data('wikimedium2m', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 2000000, constants.WIKI_MEDIUM_TASKS_1MDOCS_FILE)

MEME_ALL = Data('memeall',
                '/x/lucene/data/memetracker/lines.txt',
                210999824,
                constants.WIKI_MEDIUM_TASKS_10MDOCS_FILE)

# wikibig uses the same task files as wikimedium
WIKI_BIG = Data('wikibigall', constants.WIKI_BIG_DOCS_LINE_FILE, constants.WIKI_BIG_DOCS_COUNT, constants.WIKI_MEDIUM_TASKS_10MDOCS_FILE)
WIKI_BIG_10K = Data('wikibig10k', constants.WIKI_BIG_DOCS_LINE_FILE, 10000, constants.WIKI_MEDIUM_TASKS_1MDOCS_FILE)
WIKI_BIG_100K = Data('wikibig100k', constants.WIKI_BIG_DOCS_LINE_FILE, 100000, constants.WIKI_MEDIUM_TASKS_1MDOCS_FILE)
WIKI_BIG_1M = Data('wikibig1m', constants.WIKI_BIG_DOCS_LINE_FILE, 1000000, constants.WIKI_MEDIUM_TASKS_1MDOCS_FILE)

EURO_MEDIUM = Data('euromedium', constants.EUROPARL_MEDIUM_DOCS_LINE_FILE, 5000000, constants.EUROPARL_MEDIUM_TASKS_FILE)

WIKI_VECTOR_10K = Data('wikivector10k', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 10000, constants.WIKI_VECTOR_TASKS_FILE)
WIKI_VECTOR_1M = Data('wikivector1m', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 1000000, constants.WIKI_VECTOR_TASKS_FILE)

DISJUNCTION_SIMPLE = Data('disjunctionSimple',
                          constants.DISJUNCTION_DOCS_LINE_FILE,
                          constants.DISJUNCTION_DOC_COUNT,
                          constants.DISJUNCTION_SIMPLE_TASKS_FILE)

DISJUNCTION_REALISTIC = Data('disjunctionRealistic',
                             constants.DISJUNCTION_DOCS_LINE_FILE,
                             constants.DISJUNCTION_DOC_COUNT,
                             constants.DISJUNCTION_REALISTIC_TASKS_FILE)

DISJUNCTION_INTENSIVE = Data('disjunctionIntensive',
                             constants.DISJUNCTION_DOCS_LINE_FILE,
                             constants.DISJUNCTION_DOC_COUNT,
                             constants.DISJUNCTION_INTENSIVE_TASKS_FILE)

COMBINED_FIELDS_BIG = Data('combinedFieldsBig',
                       constants.WIKI_BIG_DOCS_LINE_FILE,  constants.WIKI_BIG_DOCS_COUNT, constants.COMBINED_FIELDS_TASKS_FILE)

COMBINED_FIELDS_UNEVENLY_WEIGHTED_BIG = Data('combinedFieldsUnevenlyWeightedBig',
                       constants.WIKI_BIG_DOCS_LINE_FILE,  constants.WIKI_BIG_DOCS_COUNT, constants.COMBINED_FIELDS_UNEVENLY_WEIGHTED_TASKS_FILE)

COMBINED_FIELDS_MEDIUM_10M = Data('combinedFieldsMedium10M',
                           constants.WIKI_MEDIUM_DOCS_LINE_FILE,  10000000, constants.COMBINED_FIELDS_TASKS_FILE)

COMBINED_FIELDS_UNEVENLY_WEIGHTED_MEDIUM_10M = Data('combinedFieldsUnevenlyWeightedMedium10M',
                                             constants.WIKI_MEDIUM_DOCS_LINE_FILE,  10000000, constants.COMBINED_FIELDS_UNEVENLY_WEIGHTED_TASKS_FILE)


DATA = {'wikimediumall': WIKI_MEDIUM_ALL,
        'wikimedium10m' : WIKI_MEDIUM_10M,
        'wikimedium1m' : WIKI_MEDIUM_1M,
        'wikimedium500' : WIKI_MEDIUM_500,
        'wikimedium500k' : WIKI_MEDIUM_500K,
        'wikimedium10k' : WIKI_MEDIUM_10K,
        'wikimedium5m' : WIKI_MEDIUM_5M,
        'wikimedium2m' : WIKI_MEDIUM_2M,
        'memeall': MEME_ALL,
        'wikibigall' : WIKI_BIG,
        'wikibig10k' : WIKI_BIG_10K,
        'wikibig100k' : WIKI_BIG_100K,
        'wikibig1m' : WIKI_BIG_1M,
        'euromedium' : EURO_MEDIUM,
        'wikivector10k' : WIKI_VECTOR_10K,
        'wikivector1m' : WIKI_VECTOR_1M,
        'disjunctionSimple' : DISJUNCTION_SIMPLE,
        'disjunctionRealistic' : DISJUNCTION_REALISTIC,
        'disjunctionIntensive' : DISJUNCTION_INTENSIVE,
        'combinedFieldsBig' : COMBINED_FIELDS_BIG,
        'combinedFieldsUnevenlyWeightedBig' : COMBINED_FIELDS_UNEVENLY_WEIGHTED_BIG,
        'combinedFieldsMedium10M' : COMBINED_FIELDS_MEDIUM_10M,
        'combinedFieldsUnevenlyWeightedMedium10M' : COMBINED_FIELDS_UNEVENLY_WEIGHTED_MEDIUM_10M
        }

# for multi-segment index:
SEGS_PER_LEVEL = 5

def sourceData(key=None):
  if not key:
    raise RuntimeError('Data source required for benchmark run. '
                       'Use "-s/-source/--source" option to provide data source. \nValid options: %s' % DATA.keys())
  if key in DATA:
    return DATA[key]
  raise RuntimeError('unknown data source "%s" (valid keys: %s)' % (key, DATA.keys()))

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
               ioThrottle = None,
               bodyTermVectors = False,
               bodyStoredFields = False,
               bodyPostingsOffsets = False,
               useCMS = False,
               facets = None,
               extraNamePart = None,
               facetDVFormat = constants.FACET_FIELD_DV_FORMAT_DEFAULT,
               maxConcurrentMerges = None,
               addDVFields = False,
               name = None,
               indexSort = None,
               vectorFile = None,
               vectorDimension = None,
               vectorEncoding = None,
               # an int defining how many segments to rearrange to, 0 represents not performing rearrange
               # rearrange /100 gives how many large segments desired
               # (rearrange % 100) / 10 gives how many medium segments desired
               # rearrange % 10 gives how many small segments desired
               # For example, rearrange = 555 means 5 large segments, 5 medium segments and 5 small segments
               rearrange = 0,
               hnswThreadsPerMerge = 1,
               hnswThreadPoolCount = 1,
               quantizeKNNGraph = False
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
      if self.maxBufferedDocs < 2:
        self.maxBufferedDocs = self.numDocs
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
    self.ioThrottle = ioThrottle
    self.idFieldPostingsFormat = idFieldPostingsFormat
    self.bodyTermVectors = bodyTermVectors
    self.bodyStoredFields = bodyStoredFields
    self.bodyPostingsOffsets = bodyPostingsOffsets
    self.facets = facets
    self.facetDVFormat = facetDVFormat
    self.assignedName = name
    self.indexSort = indexSort
    self.vectorFile = vectorFile
    self.vectorDimension = vectorDimension
    if vectorFile is not None:
      if vectorEncoding not in ('FLOAT32', 'BYTE'):
        raise RuntimeError(f'vectorEncoding must be FLOAT32 or BYTE; got: {vectorEncoding}')
    elif vectorEncoding is not None:
      raise RuntimeError(f'vectorEncoding must be None when there are no vectors to index (vectorFile is None)')
      
    self.vectorEncoding = vectorEncoding
    self.mergeFactor = 10
    if SEGS_PER_LEVEL >= self.mergeFactor:
      raise RuntimeError('SEGS_PER_LEVEL (%s) is greater than mergeFactor (%s)' % (SEGS_PER_LEVEL, mergeFactor))
    self.useCMS = useCMS
    self.rearrange = rearrange
    self.hnswThreadsPerMerge = hnswThreadsPerMerge
    self.hnswThreadPoolCount = hnswThreadPoolCount
    self.quantizeKNNGraph = quantizeKNNGraph

  def getName(self):
    if self.assignedName is not None:
      return self.assignedName

    name = [self.dataSource.name,
            benchUtil.checkoutToName(self.checkout)]

    if self.extraNamePart is not None:
      name.append(self.extraNamePart)

    if self.optimize:
      name.append('opt')

    if self.useCFS:
      name.append('cfs')

    # TODO: adding facets to filename makes it too long and runs into limits on some machines
    # Can we remove this from file name and record it in a different logfile.
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

    if self.vectorFile:
      name.append('vectors=%d' % self.vectorDimension)
      if self.quantizeKNNGraph:
        name.append('int8-quantized')

    name.append('nd%gM' % (self.numDocs/1000000.0))
    return '.'.join(name)

class Competitor(object):

  doSort = False

  def __init__(self, name, checkout,
               index = None,
               numConcurrentQueries = constants.SEARCH_NUM_CONCURRENT_QUERIES,
               directory = 'MMapDirectory',
               analyzer = None,
               commitPoint = 'multi',
               similarity = constants.SIMILARITY_DEFAULT,
               javaCommand = constants.JAVA_COMMAND,
               printHeap = False,
               hiliteImpl = 'FastVectorHighlighter',
               pk = True,
               vectorDict = None,     # query time vectors
               vectorFileName = None, # query time vectors
               vectorDimension = -1,  # query time vectors
               vectorScale = None,    # query time vectors
               loadStoredFields = False,
               exitable = False,
               searchConcurrency = 0,
               javacCommand = constants.JAVAC_EXE,
               topN = 100):
    self.name = name
    self.checkout = checkout
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
    self.exitable = exitable
    if vectorDict is not None and vectorFileName is not None:
      raise RuntimeError('specify either vectorDict or vectorFileName, not both')
    if vectorFileName is not None and vectorDimension < 1:
      raise RuntimeError(f'with vectorFileNamee you must specific vectorDimension > 0 (got: {vectorDimension})')
    if vectorDict is not None and vectorDimension != -1:
      raise RuntimeError(f'with vectorDict, vectorDimension should be -1 (got: {vectorDimension})')
    self.vectorDict = vectorDict
    self.vectorFileName = vectorFileName
    self.vectorDimension = vectorDimension
    self.vectorScale = vectorScale
    self.javacCommand = javacCommand

    # nocommit
    if False and searchConcurrency != 0 and numConcurrentQueries > 1:
      raise RuntimeError(f'when searchConcurrency != 0 (got: {searchConcurrency}) you must specify numConcurrentQueries = 1 (got: {numConcurrentQueries}), otherwise you might see falsely low effective QPS')

    # how many queries are in flight at once (one query per request thread)
    self.numConcurrentQueries = numConcurrentQueries

    # how many "worker threads" are used when executing each query concurrently
    self.searchConcurrency = searchConcurrency

    # TopN: how many hits are retrieved
    self.topN = topN

  def getAggregateProfilerResult(self, id, mode, count=30, stackSize=1):

    # we accept a sequence of stack sizes and will re-aggregate JFR results at each
    if type(stackSize) is int:
      stackSize = (stackSize,)
    
    if mode not in ('cpu', 'heap'):
      raise ValueError(f'mode must be "cpu" or "heap" but got: {mode}')

    results = []

    for size in stackSize:

      command = constants.JAVA_COMMAND.split(' ') + \
        ['-cp',
         f'{benchUtil.checkoutToPath(self.checkout)}/build-tools/build-infra/build/classes/java/main',
         f'-Dtests.profile.mode={mode}',
         f'-Dtests.profile.stacksize={size}',
         f'-Dtests.profile.count={count}',
         'org.apache.lucene.gradle.ProfileResults'] + \
         glob.glob(f'{constants.LOGS_DIR}/bench-search-{id}-{self.name}-*.jfr')

      print(f'JFR aggregation command: {" ".join(command)}')
      t0 = time.time()
      try:
        result = subprocess.run(command,
                                stdout = subprocess.PIPE,
                                stderr = subprocess.STDOUT,
                                check = True)
      except subprocess.CalledProcessError as e:
        print(f'command failed:\n  stderr:\n{e.stderr}\n  stdout:\n{e.stdout}')
        raise
        
      t1 = time.time()
      print(f'Took {t1-t0:.2f} seconds')
      results.append((size, result.stdout.decode('utf-8')))

    return results

  def compile(self, cp):
    root = benchUtil.checkoutToUtilPath(self.checkout)

    perfSrc = os.path.join(root, "src/main")

    buildDir = os.path.join(root, "build")
    if not os.path.exists(buildDir):
      os.makedirs(buildDir)

    # Try to be faster than ant; this may miss changes, e.g. a static final constant changed in core that is used in another module:
    # This is a broken optimization!  E.g. if there is a single .class file, with recent timestamp, and all other .class files are missing,
    # this fails to compile the missing ones!
    if False and common.getLatestModTime(perfSrc) <= common.getLatestModTime(buildDir, '.class'):
      print('Skip compiling luceneutil: all .class are up to date')
      return

    perfSrcDir = os.path.join(perfSrc, 'perf')
    files = list(filter(lambda f: os.path.isfile(f) and f.endswith('.java'),
                        [os.path.join(perfSrcDir, f) for f in os.listdir(perfSrcDir)]))

    print('files %s' % files)

    cmd = [self.javacCommand, '-g', '-d', buildDir, '-classpath', cp]
    cmd += files
    
    print(f'compile: {" ".join(cmd)}')
    
    benchUtil.run(cmd, os.path.join(constants.LOGS_DIR, 'compile.log'))
    # copy resources/META-INF
    if os.path.exists(os.path.join(perfSrc, 'resources/*')):
      benchUtil.run('cp', '-r', os.path.join(perfSrc, 'resources/*'), buildDir.replace("\\", "/"))

class Competition(object):

  def __init__(self, cold=False,
               printCharts=False,
               verifyScores=True,
               verifyCounts=True,
               requireOverlap=1.0,
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
    self.requireOverlap = requireOverlap
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
    for c in self.competitors:
      if c.name == name:
        raise RuntimeError(f'competitor named {name} already added')
    print('Using checkout:[%s] for competitor:[%s]' % (benchUtil.checkoutToPath(checkout), name))
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
    elif base == self.competitors[0]:
      challenger = self.competitors[1]
    else:
      challenger = self.competitors[0]

    for fileName in glob.glob(f'{constants.LOGS_DIR}/bench-search-*.jfr'):
      print('Removing old JFR %s...' % fileName)
      os.remove(fileName)

    base.tasksFile = base.index.dataSource.tasksFile
    challenger.tasksFile = challenger.index.dataSource.tasksFile

    searchBench.run(id, base, challenger,
                    coldRun = self.cold,
                    doCharts = self.printCharts,
                    search = self.benchSearch,
                    index = self.benchIndex,
                    verifyScores = self.verifyScores, verifyCounts = self.verifyCounts,
                    taskPatterns = (self.onlyTaskPatterns, self.notTaskPatterns),
                    requireOverlap = self.requireOverlap,
                    randomSeed = self.randomSeed)
    return self

  def clearCompetitors(self):
    self.competitors = []

  def clearIndices(self):
    self.indices = []
