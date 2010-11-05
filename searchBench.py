import time
import sys
import os
import benchUtil
from luceneutil import *

if '-ea' in sys.argv:
  JAVA_COMMAND += ' -ea:org.apache.lucene...'

if '-debug' in sys.argv:
  INDEX_NUM_DOCS = 100000
else:
  INDEX_NUM_DOCS = 10000000
INDEX_NUM_THREADS = 1


def run(*competitors):  
  r = benchUtil.RunAlgs(INDEX_DIR_BASE, JAVA_COMMAND)
  if '-noc' not in sys.argv:
    for c in competitors:
      r.compile(c)
  search = '-search' in sys.argv
  index  = '-index' in sys.argv

  if index:
    for c in competitors:
      if c.index:
        r.makeIndex(c.defaultCodec, c.benchDir, c.name, c.dataSource, INDEX_NUM_DOCS, INDEX_NUM_THREADS, c.sourceBase, lineDocSource=WIKI_LINE_FILE)

  logUpto = 0
  if not search:
    return
  if '-debugs' in sys.argv:
    iters = 10
    threads = 1
  else:
    iters = 40
    threads = 4

  results = {}
  for c in competitors:
    print 'Search on %s...' % c.name
    benchUtil.run("sudo %s/bench/dropCaches.sh" % BASE_DIR)
    t0 = time.time()
    results[c] = r.runSimpleSearchBench(c, iters, threads, filter=None)
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

  def __init__(self, name, baseDir, dataSource='wiki', defaultCodec='Standard', task='search', index=True):
    self.name = name
    self.dataSource = dataSource
    self.defaultCodec = defaultCodec
    self.sourceBase = '%s/%s' % (baseDir, name)
    self.searchTask = self.TASKS[task];
    self.benchDir = '%s/lucene/contrib/benchmark' % (self.sourceBase)
    self.commitPoint = 'single'
    self.index=index

  def indexName(self, baseDir):
    return ('%s/%s.work.%s.%s.nd%gM' % (baseDir,self.defaultCodec, self.name, self.dataSource, INDEX_NUM_DOCS/1000000.0))
  
  def indexDir(self, baseDir):
    return '%s/index' % self.indexName(baseDir)

  def compile(self, cp):
    benchUtil.run('javac -cp %s perf/SearchPerfTest.java >> compile.log 2>&1' % cp,  'compile.log')

  def setTask(self, task):
    self.searchTask = self.TASKS[task];
    return self

  def taskRunProperties(self):
    return '-Dtask.type=%s' % self.searchTask

class DocValueCompetitor(Competitor):
  def __init__(self, name, baseDir, dataSource='wiki', defaultCodec='Standard', task='loadIdDV'):
    Competitor.__init__(self, name, baseDir, dataSource, defaultCodec, task)
  
  def compile(self, cp):
    benchUtil.run('javac -cp %s perf/SearchPerfTest.java >> compile.log 2>&1' % cp,  'compile.log')
    benchUtil.run('javac -cp %s:./perf perf/values/DocValuesSearchTask.java >> compile.log 2>&1' % cp,  'compile.log')
  
    
TRUNK_COMPETITOR = Competitor('trunk', BASE_DIR)
DOCVALUES_COMPETITOR = DocValueCompetitor('docvalues', BASE_DIR)
PFOR_COMPETITOR = Competitor('LUCENE-2723_trunk', BASE_DIR, defaultCodec="PatchedFrameOfRef")
FOR_COMPETITOR = Competitor('LUCENE-2723_trunk', BASE_DIR, defaultCodec="FrameOfRef")
GROUP_VAR_INT_COMPETITOR = Competitor('LUCENE-2735_trunk', BASE_DIR, defaultCodec="GVInt")

def main():

  if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
#  run(TRUNK_COMPETITOR.setTask('loadIdFC'), DOCVALUES_COMPETITOR)
  run(TRUNK_COMPETITOR, FOR_COMPETITOR)

if __name__ == '__main__':
  main()
