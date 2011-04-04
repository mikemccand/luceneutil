from searchBench import *
from benchUtil import Index
import constants


class Data(object):
  
  def __init__(self, name, lineFile, numDocs, tasksFile):
    self.name = name
    self.lineFile = lineFile
    self.numDocs = numDocs
    self.tasksFile = tasksFile


WIKI_MEDIUM = Data('wikimedium', constants.WIKI_MEDIUM_DOCS_LINE_FILE, 10000000, constants.WIKI_MEDIUM_TASKS_FILE)
WIKI_BIG = Data('wikibig', constants.WIKI_BIG_DOCS_LINE_FILE, 3000000, constants.WIKI_BIG_TASKS_FILE)
EURO_MEDIUM = Data('euromedium', constants.EUROPARL_MEDIUM_DOCS_LINE_FILE, 5000000, constants.EUROPARL_MEDIUM_TASKS_FILE)
MMAP_DIRECTORY='MMAPDirectory'
NIOFS_DIRECTORY='NOIFSDirectory'
MULTI_SEGMENTS_COMMIT='multi'
SINGLE_SEGMENT_COMMIT='single'

class Competition(object):

  def __init__(self, indexThreads=constants.INDEX_NUM_THREADS, searchThreads=constants.SEARCH_NUM_THREADS, ramBufferMB=-1, cold=False, verbose=False, printCharts=False):
    self.indexThreads = indexThreads
    self.searchThreads = searchThreads
    self.ramBufferMB = ramBufferMB
    self.cold = cold
    self.competitors = []
    self.indices = []
    self.verbose=verbose
    self.printCharts = printCharts 
    self.debug=False
    self.benchSearch = True
    self.benchIndex = True
  def withIndex(self, checkout, data=WIKI_MEDIUM, analyzer=constants.ANALYZER_DEFAULT, codec=constants.CODEC_DEFAULT, indexDir=NIOFS_DIRECTORY, ramBufferMB=0):
    if not ramBufferMB:
      ramBufferMB = self.ramBufferMB
    self.indices.append((checkout, data, analyzer, codec, indexDir, ramBufferMB))    
    return self

  def competitor(self, name, checkout=None, analyzer=constants.ANALYZER_DEFAULT, searchDir=MMAP_DIRECTORY, commitPoint=MULTI_SEGMENTS_COMMIT):
    if not checkout:
      checkout = name
    self.competitors.append((name, checkout, analyzer, searchDir, commitPoint))
    return self

  def debug(self):
    self.debug = True
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
    checkout, data, analyzer, codec, indexDir, ramBufferMB = self.indices[0]
    baseIndex = Index(checkout, data.name, analyzer, codec, data.numDocs, self.indexThreads, data.lineFile, ramBufferMB)
    name, checkout, analyzer, searchDir, commitPoint  =  self.competitors[0]
    base = Competitor(name, checkout, baseIndex, searchDir, analyzer, commitPoint, data.tasksFile)
    if len(self.indices) > 1:
      checkout, data, analyzer, codec, indexDir, ramBufferMB = self.indices[1]
      compIndex = Index(checkout, data.name, analyzer, codec, data.numDocs, self.indexThreads, data.lineFile, ramBufferMB)
    else: 
      compIndex = baseIndex
    name, checkout, analyzer, searchDir, commitPoint  =  self.competitors[1]
    comp = Competitor(name, checkout, baseIndex, searchDir, analyzer, commitPoint, data.tasksFile)
    if self.verbose:
      baseIndex.setVerbose(self.verbose)
      compIndex.setVerbose(self.verbose)
    run(id, base, comp, coldRun=self.cold, doCharts=self.printCharts, search=self.benchSearch, index=self.benchIndex, debug=self.debug)
    return self
  
  def clearCompetitors(self):
    self.competitors = []
    return self 
  def clearIndices(self):
    self.indices = []
    return self

