import types
import re
import time
import os
import shutil
import sys
import cPickle
import datetime
from luceneutil import BASE_DIR
from luceneutil import BENCH_BASE_DIR
# TODO
#   - add option for testing sorting, applying the SortValues.patch!!
#   - verify step
#   - run searches
#   - get all docs query in here

if sys.platform.lower().find('darwin') != -1:
  osName = 'osx'
elif sys.platform.lower().find('win') != -1:
  osName = 'windows'
elif sys.platform.lower().find('linux') != -1:
  osName = 'linux'
else:
  osName = 'unix'

reQuery = re.compile('^q=(.*?) s=(.*?) h=(.*?)$')
reHits = re.compile('^HITS q=(.*?) s=(.*?) tot=(.*?)$')
reHit = re.compile('^(\d+) doc=(\d+) score=([0-9.]+)$')
reResult = re.compile(r'^(\d+) c=(.*?)$')
reChecksum = re.compile(r'checksum=(\d+)$')

DEBUG = True

# let shell find it:
#JAVA_COMMAND = 'java -Xms2g -Xmx2g -server'
#JAVA_COMMAND = 'java -Xms1024M -Xmx1024M -Xbatch -server -XX:+AggressiveOpts -XX:CompileThreshold=100 -XX:+UseFastAccessorMethods'

LOG_SUB_DIR = 'logs'

BASE_SEARCH_ALG = '''
analyzer=org.apache.lucene.analysis.en.EnglishWDFAnalyzer
directory=FSDirectory
work.dir = $INDEX$
search.num.hits = $NUM_HITS$
query.maker=org.apache.lucene.benchmark.byTask.feeds.FileBasedQueryMaker
file.query.maker.file = queries.txt
log.queries=true
log.step=100000
print.hits.field=$PRINT_HITS_FIELD$

OpenReader  
{"XSearchWarm" $SEARCH$}
SetProp(print.hits.field,)
$ROUNDS$
CloseReader 
RepSumByPrefRound XSearch
'''

SINGLE_SEG_INDEX_ALG = '''
analyzer=org.apache.lucene.analysis.en.EnglishWDFAnalyzer

$OTHER$

doc.body.stored = false
doc.term.vector = false
doc.tokenized = false

doc.index.props = true
doc.stored = true
doc.body.tokenized = true

sort.rng = 1000000
rand.seed=17

log.step.AddDoc=10000
#writer.info.stream = SystemOut

directory=FSDirectory
compound=false
ram.flush.mb = -1
#max.buffered = 10
#merge.factor = 2

deletion.policy = org.apache.lucene.index.NoDeletionPolicy

work.dir=$WORKDIR$
max.field.length= 2047483647
content.source.forever = true
max.buffered = 7777

ResetSystemErase
CreateIndex

{ "BuildIndex"
  $INDEX_LINE$
  -CommitIndex(multi)
  -CloseIndex
}

RepSumByPrefRound BuildIndex

{ "OptimizeIndex"
  -OpenIndex
  -Optimize
  -CommitIndex(single)
  -CloseIndex
}

RepSumByPrefRound OptimizeIndex
'''

MULTI_COMMIT_INDEX_ALG = '''
analyzer=org.apache.lucene.analysis.en.EnglishAnalyzer

$OTHER$

doc.body.stored = false
doc.term.vector = false
doc.tokenized = false

doc.index.props = true
doc.stored = true
doc.body.tokenized = true

sort.rng = 1000000
rand.seed=17

log.step.AddDoc=10000
#writer.info.stream = SystemOut

directory=FSDirectory
compound=false
ram.flush.mb = 256
max.buffered = 77777

deletion.policy = org.apache.lucene.index.NoDeletionPolicy

work.dir=$WORKDIR$
max.field.length= 2047483647
content.source.forever = true

ResetSystemErase
CreateIndex

{ "BuildIndex"
  $INDEX_LINE$
  -CommitIndex(multi)
  -CloseIndex
}

RepSumByPrefRound BuildIndex

{ "Optimize"
  Optimize
}

RepSumByPrefRound Optimize

CommitIndex(single)
CloseIndex

OpenReader(false,multi)
DeleteByPercent(5)
RepSumByPrefRound DeleteByPercent
CommitIndex(delmulti)
CloseReader

OpenReader(false,single)
DeleteByPercent(5)
RepSumByPrefRound DeleteByPercent
CommitIndex(delsingle)
CloseReader
'''

#BASE_INDEX_ALG = MULTI_COMMIT_INDEX_ALG
BASE_INDEX_ALG = SINGLE_SEG_INDEX_ALG

def run(cmd, log=None):
  print 'RUN: %s' % cmd
  if os.system(cmd):
    if log is not None:
      print open(log).read()
    raise RuntimeError('failed: %s [wd %s]' % (cmd, os.getcwd()))

class SearchResult:

  def __init__(self, job, numHits, warmTime, bestQPS, hits):

    self.job = job
    self.warmTime = warmTime
    self.bestQPS = bestQPS
    self.hits = hits
    self.numHits = numHits

class Job:

  def __init__(self, cat, label, numIndexDocs, alg, queries=None, numRounds=None):

    # index or search
    self.cat = cat

    # eg clean, flex, csf, etc
    self.label = label

    self.queries = queries

    self.numRounds = numRounds

    self.numIndexDocs = numIndexDocs
    self.alg = alg

class SearchJob(Job):
  def __init__(self, label, numIndexDocs, alg, queries, numRounds):
    Job.__init__(self, 'search', label, numIndexDocs, alg, queries, numRounds)

class IndexJob(Job):
  def __init__(self, label, numIndexDocs, alg):
    Job.__init__(self, 'index', label, numIndexDocs, alg)

class RunAlgs:

  def __init__(self, baseDir, javaCommand):
    self.baseDir = baseDir
    self.logCounter = 0
    self.results = []
    self.compiled = set()
    self.javaCommand = javaCommand
    print
    print 'JAVA:\n%s' % os.popen('java -version 2>&1').read()
    
    print
    if osName != 'windows':
      print 'OS:\n%s' % os.popen('uname -a 2>&1').read()
    else:
      print 'OS:\n%s' % sys.platform
    
  def printEnv(self):
    print
    print 'JAVA:\n%s' % os.popen('%s -version 2>&1' % self.javaCommand).read()

    print
    if osName != 'windows':
      print 'OS:\n%s' % os.popen('uname -a 2>&1').read()
    else:
      print 'OS:\n%s' % sys.platform
      
  def makeIndex(self, defaultCodec, workingDir, prefix, source, numDocs, numThreads, lineDocSource=None, xmlDocSource=None):

    if source not in ('wiki', 'random'):
      raise RuntimeError('source must be wiki or random')

    indexName = '%s.work.%s.%s.nd%gM' % (defaultCodec, prefix, source, numDocs/1000000.0)
    fullIndexPath = '%s/%s' % (self.baseDir, indexName)
    
    if os.path.exists(fullIndexPath):
      print 'Index %s already exists...' % fullIndexPath
      return fullIndexPath

    print 'Now create index %s...' % fullIndexPath

    alg = self.getIndexAlg(defaultCodec, fullIndexPath, source, numDocs, numThreads, lineDocSource=lineDocSource, xmlDocSource=xmlDocSource)

    job = IndexJob(prefix, numDocs, alg)

    try:
      self.runOne(workingDir, job, sourceBase, logFileName=indexName+'.log')
    except:
      if os.path.exists(fullIndexPath):
        shutil.rmtree(fullIndexPath)
      raise

    return fullIndexPath

  def getClassPath(self, base):
    baseDict = {'base' : base}
    cp = '.:%(base)s/lucene/build/classes/java'
    cp += ':%(base)s/lucene/build/classes/test'
    if os.path.exists('%(base)s/modules' % baseDict):
      cp += ':%(base)s/modules/analysis/build/common/classes/java'
      cp += ':%(base)s/modules/analysis/build/icu/classes/java'
    else:
      cp += ':%(base)s/lucene/build/contrib/analyzers/common/classes/java'
    return cp % baseDict

  def compile(self,competitor):
    path = '%s/lucene/contrib/benchmark' % (competitor.sourceBase)
    sourceBase = competitor.sourceBase
    print 'COMPILE: %s' % path
    os.chdir(path)
    run('ant compile > compile.log 2>&1', 'compile.log')
    if path.endswith('/'):
      path = path[:-1]
      
    cp = self.getClassPath(sourceBase)
    if not os.path.exists('perf'):
      run('ln -s %s/perf .' % BENCH_BASE_DIR)
    competitor.compile(cp)
    
  def runOne(self, workingDir, job, sourceBase, verify=False, logFileName=None):

    if logFileName is None:
      logFileName = '%s.%d' % (job.label, self.logCounter)
      algFile = '%s.%d.alg' % (job.label, self.logCounter)
      self.logCounter += 1
    else:
      algFile = logFileName + '.alg'

    savDir = os.getcwd()
    os.chdir(workingDir)
    print '    cd %s' % workingDir

    try:

      if job.queries is not None:
        if type(job.queries) in types.StringTypes:
          job.queries = [job.queries]
        open('queries.txt', 'wb').write('\n'.join(job.queries))

      if not os.path.exists(LOG_SUB_DIR):
        os.makedirs(LOG_SUB_DIR)

      algFullFile = '%s/%s' % (LOG_SUB_DIR, algFile)

      open(algFullFile, 'wb').write(job.alg)

      fullLogFileName = '%s/%s' % (LOG_SUB_DIR, logFileName)
      print '    log: %s/%s' % (workingDir, fullLogFileName)

      command = '%s -classpath %s:../../build/contrib/highlighter/classes/java:lib/icu4j-4_4_1_1.jar:lib/icu4j-charsets-4_4_1_1.jar:lib/commons-digester-1.7.jar:lib/commons-collections-3.1.jar:lib/commons-compress-1.0.jar:lib/commons-logging-1.0.4.jar:lib/commons-beanutils-1.7.0.jar:lib/xerces-2.9.0.jar:lib/xml-apis-2.9.0.jar:../../build/contrib/benchmark/classes/java org.apache.lucene.benchmark.byTask.Benchmark %s > "%s" 2>&1' % \
                (self.javaCommand, self.getClassPath(sourceBase), algFullFile, fullLogFileName)

      if DEBUG:
        print 'command=%s' % command

      t0 = time.time()
      if os.system(command) != 0:
        raise RuntimeError('FAILED')
      t1 = time.time()

      if job.cat == 'index':
        s = open(fullLogFileName, 'rb').read()
        if s.find('Exception in thread "') != -1 or s.find('at org.apache.lucene') != -1:
          raise RuntimeError('alg hit exceptions')
        return

      else:

        # Parse results:
        bestQPS = None
        count = 0
        nhits = None
        ndocs = None
        warmTime = None
        r = re.compile('^  ([0-9]+): (.*)$')
        topN = []

        for line in open(fullLogFileName, 'rb').readlines():
          m = r.match(line.rstrip())
          if m is not None:
            topN.append(m.group(2))
          if line.startswith('totalHits ='):
            nhits = int(line[11:].strip())
          if line.startswith('numDocs() ='):
            ndocs = int(line[11:].strip())
          if line.startswith('maxDoc()  ='):
            maxDoc = int(line[11:].strip())
          if line.startswith('XSearchWarm'):
            v = line.strip().split()
            warmTime = float(v[5])
          if line.startswith('XSearchReal'):
            v = line.strip().split()
            # print len(v), v
            upto = 0
            i = 0
            qps = None
            while i < len(v):
              if v[i] == '-':
                i += 1
                continue
              else:
                upto += 1
                i += 1
                if upto == 5:
                  qps = float(v[i-1].replace(',', ''))
                  break

            if qps is None:
              raise RuntimeError('did not find qps')

            count += 1
            if bestQPS is None or qps > bestQPS:
              bestQPS = qps

        if not verify:
          if count != job.numRounds:
            raise RuntimeError('did not find %s rounds (got %s)' % (job.numRounds, count))
          if warmTime is None:
            raise RuntimeError('did not find warm time')
        else:
          bestQPS = 1.0
          warmTime = None

        if nhits is None:
          raise RuntimeError('did not see totalHits line')

        if ndocs is None:
          raise RuntimeError('did not see numDocs line')

        if ndocs != job.numIndexDocs:
          raise RuntimeError('indexNumDocs mismatch: expected %d but got %d' % (job.numIndexDocs, ndocs))

        return SearchResult(job, nhits, warmTime, bestQPS, topN)
    finally:
      os.chdir(savDir)
                           
  def getIndexAlg(self, defaultCodec, fullIndexPath, source, numDocs, numThreads, lineDocSource=None, xmlDocSource=None):

    s = BASE_INDEX_ALG

    if source == 'wiki':
      if lineDocSource is not None:
        s2 = '''
content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource
docs.file=%s
''' % lineDocSource
      elif xmlDocSource is not None:
        s2 = '''
content.source=org.apache.lucene.benchmark.byTask.feeds.EnwikiContentSource
docs.file=%s
''' % xmlDocSource
      else:
        raise RuntimeError('if source is wiki, either lineDocSource or xmlDocSource must be set')
        
    elif source == 'random':
      s2 = '''
content.source=org.apache.lucene.benchmark.byTask.feeds.SortableSingleDocSource
'''
    else:
      raise RuntimeError('source must be wiki or random (got "%s")' % source)

    s2 += 'default.codec = %s\n' % defaultCodec
    
    if numThreads > 1:
      # other += 'doc.reuse.fields=false\n'
      s = s.replace('$INDEX_LINE$', '[ { "AddDocs" AddDoc > : %s } : %s' % \
                    (numDocs/numThreads, numThreads))
    else:
      s = s.replace('$INDEX_LINE$', '{ "AddDocs" AddDoc > : %s' % \
                    (numDocs))

    s = s.replace('$WORKDIR$', fullIndexPath)
    s = s.replace('$OTHER$', s2)

    return s

  def getSearchAlg(self, indexPath, searchTask, numHits, numRounds, verify=False):

    s = BASE_SEARCH_ALG
    
    if not verify:
      s = s.replace('$ROUNDS$',
  '''                
  { "Rounds"
    { "Run"
      { "TestSearchSpeed"
        { "XSearchReal" $SEARCH$ > : 5.0s
      }
      NewRound
    } : %d
  } 
  ''' % numRounds)
    else:
      s = s.replace('$ROUNDS$', '')

    s = s.replace('$INDEX$', indexPath)
    s = s.replace('$SEARCH$', searchTask)
    s = s.replace('$NUM_HITS$', str(numHits))
    
    return s

  def runSimpleSearchBench(self, c, iters, threadCount, filter=None):
    os.chdir(c.benchDir)
    cp = self.getClassPath(c.sourceBase)
    logFile = '%s/res-%s.txt' % (c.benchDir, c.name)
    print 'log %s' % logFile
    command = '%s %s -cp %s perf.SearchPerfTest %s %s %s %s' % \
        (self.javaCommand, c.taskRunProperties(), cp, c.indexDir(self.baseDir), threadCount, iters, c.commitPoint)
    if filter is not None:
      command += ' %s %.2f' % filter
    run('%s > %s' % (command, logFile))
    return logFile

  def simpleReport(self, baseLogFile, cmpLogFile, jira=False, html=False, baseDesc='Standard', cmpDesc=None):
    base, totCS, hitsBase = getSimpleResults(baseLogFile, None)
    cmp, totCS, hitsCMP = getSimpleResults(cmpLogFile, totCS)

    compareHits(hitsBase, hitsCMP)
    
    allQueries = set()

    for (q, s), t in base.items():
      allQueries.add(q)

    s = 'null'
    #s = '<string: "doctitle">'

    lines = []
    w = sys.stdout.write
    w('\nNOTE: SORT BY %s\n\n' % s)
    
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
      if check != check2:
        warnings.append('q=%s sort=%s: check counts differ: %s vs %s' % (q, s, check, check2))
        #raise RuntimeError('check counts differ: %s vs %s' % (check, check2))
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

      lines.append((ps, ''.join(l0)))

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

def getSimpleResults(fname, totCS):
  results = {}

  start = False
  best = None
  count = 0
  hits = {}
    
  for l in open(fname).readlines():

    l = l.strip()

    m = reHits.match(l)
    if m is not None:
      query, sort, hitCount = m.groups()
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
        raise RuntimeError('internal checksum diff %s vs %s' % (totCS, s))
      
    if l.startswith('q='):
      if best is not None:
        results[(query, sort)] = best, hitCount, check
        best = None
      query, sort, hitCount = reQuery.match(l).groups()
    elif l.startswith('t='):
      count = 0
    else:
      if l.endswith(' **'):
        l = l[:-3]
      m = reResult.match(l)
      t = long(m.group(1))
      check = long(m.group(2))
      count += 1
      if count > 6 and (best is None or t < best):
        best = t

  if len(hits) == 0:
    raise RuntimeError("didn't see any hits")
  results[(query, sort)] = best, hitCount, check
  return results, totCS, hits

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
