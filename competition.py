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
WIKI_MEDIUM_1M = Data('wikimedium1m', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 1000000, constants.WIKI_MEDIUM_TASKS_1MDOCS_FILE)

WIKI_BIG = Data('wikibig', constants.WIKI_BIG_DOCS_LINE_FILE, 3000000, constants.WIKI_BIG_TASKS_FILE)
EURO_MEDIUM = Data('euromedium', constants.EUROPARL_MEDIUM_DOCS_LINE_FILE, 5000000, constants.EUROPARL_MEDIUM_TASKS_FILE)

DATA = {'wikimedium10m' : WIKI_MEDIUM_10M,
        'wikimedium1m' : WIKI_MEDIUM_1M,
        'wikibig' : WIKI_BIG,
        'euromedium' : EURO_MEDIUM }

MMAP_DIRECTORY='MMapDirectory'
NIOFS_DIRECTORY='NIOFSDirectory'
MULTI_SEGMENTS_COMMIT='multi'
SINGLE_SEGMENT_COMMIT='single'
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

  doGrouping = True

  def __init__(self, checkout, dataSource, analyzer, codec, numDocs, numThreads,
               lineDocSource, doOptimize=False, dirImpl='NIOFSDirectory',
               doDeletions=False, ramBufferMB=-1, mergePolicy=constants.MERGEPOLICY_DEFAULT, doUpdate='No'):
    self.checkout = checkout
    self.analyzer = analyzer
    self.dataSource = dataSource
    self.codec = codec
    self.numDocs = numDocs
    self.numThreads = numThreads
    self.lineDocSource = lineDocSource
    self.doOptimize = doOptimize
    self.dirImpl = dirImpl
    self.doDeletions = doDeletions
    self.ramBufferMB = ramBufferMB
    if ramBufferMB == -1:
      self.maxBufferedDocs = numDocs/ (SEGS_PER_LEVEL*111)
    else:
      self.maxBufferedDocs = -1
    self.verbose = 'yes'
    self.printDPS = 'yes'
    self.waitForMerges = True
    self.mergePolicy = mergePolicy
    mergeFactor = 10
    self.doUpdate = doUpdate
    self.idFieldUsesPulsing = False
    if SEGS_PER_LEVEL >= mergeFactor:
      raise RuntimeError('SEGS_PER_LEVEL (%s) is greater than mergeFactor (%s)' % (SEGS_PER_LEVEL, mergeFactor))

  def setVerbose(self, verbose):
    if verbose:
      self.verbose = 'yes'
    else:
      self.verbose = 'no'

  def setPrintDPS(self, dps):
    if dps:
      self.printDPS = 'yes'
    else:
      self.printDPS = 'no'

  def getName(self):
    if self.doOptimize:
      s = 'opt.'
    else:
      s = ''
    return '%s.%s.%s.%snd%gM' % (self.dataSource, self.checkout, self.codec, s, self.numDocs/1000000.0)



class Competitor(object):

  doSort = False

  def __init__(self, name, checkout, index, dirImpl, analyzer, commitPoint, tasksFile, threads):
    self.name = name
    self.index = index
    self.checkout = checkout
    self.commitPoint = commitPoint
    self.dirImpl = dirImpl
    self.analyzer = analyzer
    self.tasksFile = tasksFile
    self.threads = threads

  def compile(self, cp):
    benchUtil.run('javac -classpath "%s" perf/*.java >> compile.log 2>&1' % cp, 'compile.log')

  def setTask(self, task):
    self.searchTask = self.TASKS[task];
    return self



class Competition(object):

  def __init__(self, indexThreads=constants.INDEX_NUM_THREADS, searchThreads=constants.SEARCH_NUM_THREADS,
    ramBufferMB=-1, cold=False, verbose=False, printCharts=False):
    self.indexThreads = indexThreads
    self.searchThreads = searchThreads
    self.ramBufferMB = ramBufferMB
    self.cold = cold
    self.competitors = []
    self.indices = []
    self.verbose=verbose
    self.printCharts = printCharts 
    self._debug=False
    self.benchSearch = True
    self.benchIndex = True

  def newIndex(self, checkout, data=WIKI_MEDIUM_10M):
    return IndexBuilder(checkout, self.ramBufferMB, data, self)

  def competitor(self, name, checkout=None ):
    if not checkout:
      checkout = name
    return CompetitorBuilder(name, checkout, self)

  def debug(self):
    self._debug = True
    return self

  def skipIndex(self):
    self.benchIndex=False
    return self

  def skipSearch(self):
    self.benchSearch=False
    return self

  def benchmark(self, id):
    if len(self.competitors) != 2:
      raise RuntimeError('expected 2 competitors but was %d' % (len(self.competitors)))
    if not self.indices:
      raise RuntimeError('expected at least one index use withIndex(...)')
    if len(self.indices) == 1:
      for comp in self.competitors:
        # only one index given? share it!
        if not comp._index:
          comp.withIndex(self.indices[0])
    base = self.competitors[0].build()
    challenger = self.competitors[1].build()
     
    searchBench.run(id, base, challenger, coldRun=self.cold, doCharts=self.printCharts,
                search=self.benchSearch, index=self.benchIndex, debugs=self._debug, debug=self._debug)
    return self

  def clearCompetitors(self):
    self.competitors = []
    return self 
  def clearIndices(self):
    self.indices = []
    return self

class CompetitorBuilder(object):
  
  def __init__(self, name, checkout, competition):
    self._name = name
    self._checkout = checkout
    self._directory = MMAP_DIRECTORY
    self._analyzer = constants.ANALYZER_DEFAULT
    self._ramBufferMB = competition.ramBufferMB
    self._threads = competition.searchThreads
    self._commitPoint = MULTI_SEGMENTS_COMMIT
    self._index = None
    competition.competitors.append(self)
 
  def commitPoint(self, commitPoint): 
    self._commitPoint = commitPoint
    return self

  def numThreads(self, num):
    self._threads = num
    return self

  def analyzer(self, analyzer):
    self._analyzer = analyzer
    return self

  def directory(self, directory):
    self._directory = directory
    return self

  def withIndex(self, index):
    self._index = index
    return self
  
  def build(self):
    if not self._index:
      raise RuntimeError("no index given to competitor %s " % self._name)
    data = self._index._data
    return Competitor(self._name, self._checkout, self._index.build(), self._directory, self._analyzer, self._commitPoint, data.tasksFile, self._threads)

class IndexBuilder(object):
  
  def __init__(self, checkout, ramBufferMB, data, competition):
    self._data = data
    self._checkout = checkout
    self._directory = NIOFS_DIRECTORY
    self._analyzer = constants.ANALYZER_DEFAULT
    self._ramBufferMB = ramBufferMB
    self._threads = competition.indexThreads
    self._maxBufferedDocs = -1
    self._codec = constants.CODEC_DEFAULT
    self._verbose = competition.verbose
    self._printCharts = competition.printCharts
    self._doDeletions = False
    self._doOptimize = False
    self._mergePolicy = constants.MERGEPOLICY_DEFAULT
    self._waitForMerges = True
    self._doUpdate = False
    self._idFieldUsesPulsing = False
    competition.indices.append(self)

  def doUpdate(self):
    self._doUpdate = True 
    return self    
    
  def threads(self, threads):
    self._threads = threads
    return self
  
  def analyzer(self, analyzer):
    self._analyzer = analyzer
    return self

  def directory(self, directory):
    self._directory = directory
    return self

  def ramBufferMB(self, ramBufferMB):
    self._ramBufferMB = ramBufferMB
    return self

  def maxBufferedDocs(self, numDocs):
    self._maxBufferedDocs = numDocs 
    return self

  def optimize(self, doOptimize):
    self._doOptimize = doOptimize
    return self

  def doDeletes(self, doDeletions):
    self._doDeletions = doDeletions
    return self

  def codec(self, codec):
    self._codec = codec
    return self

  def mergePolicy(self, p):
    self._mergePolicy = p
    return self

  def waitForMerges(self, v):
    self._waitForMerges = v
    return self

  def idFieldUsesPulsing(self, v):
    self._idFieldUsesPulsing = v
    return self

  def build(self):

    idx = Index(self._checkout, self._data.name, self._analyzer, self._codec,
          self._data.numDocs, self._threads, self._data.lineFile, doOptimize=self._doOptimize, doDeletions=self._doDeletions, dirImpl=self._directory, ramBufferMB=self._ramBufferMB, doUpdate = self._doUpdate)
    idx.setVerbose(self._verbose)
    idx.setPrintDPS(self._printCharts)
    idx.mergePolicy = self._mergePolicy
    idx.waitForMerges = self._waitForMerges
    idx.idFieldUsesPulsing = self._idFieldUsesPulsing
    return idx
