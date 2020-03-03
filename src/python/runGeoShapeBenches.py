import sys
import pickle
import subprocess
import os
import re
import datetime
import time

#src/python/runGeoShapeBenches.py -compare -reindex -ant

reTotHits = re.compile('totHits=(\d+)$')

nightly = '-nightly' in sys.argv
compareRun = '-compare' in sys.argv

if nightly and compareRun:
  raise RuntimeError('cannot run nightly job with compare flag')

if nightly and '-reindex' not in sys.argv:
  sys.argv.append('-reindex')



####################
# Add here your paths
####################
GEO_UTIL_DIR ='/Users/ivera/forks/luceneutil_cleanfork'
GEO_LUCENE_DIR = '/Users/ivera/forks/lucene-solr-fork/lucene'
BASELINE_LUCENE_DIR = '/Users/ivera/projects/lucene-solr/lucene'
GEO_LOGS_DIR = '/data/geo/'
#######
# add your file name, it needs to be under your data directory (see IndexAndSearchShapes.java)
#######
fileName = "osmdata.wkt"

approaches = ('LatLonShape',)
ops = ('intersects', 'contains', 'within', 'disjoint')
shapes = ('point', 'box', 'distance', 'poly 10', 'polyMedium', 'polyRussia')



def printResults(results, stats, maxDoc):
  print()
  print('Results on %2fM points:' % (maxDoc/1000000.))

  print()

  if '-reindex' in sys.argv or '-reindexFast' in sys.argv:
    print('||Approach||Index time (sec)||Force merge time (sec)||Index size (GB)||Reader heap (MB)||')
    readerHeapMB, indexSizeGB, indexTimeSec, forceMergeTimeSec = stats['LatLonShape']
    print('%s|%.1fs|%.1fs|%.2f|%.2f|' % ('LatLonShape', indexTimeSec, forceMergeTimeSec, indexSizeGB, readerHeapMB))
  else:
    print('||Index size (GB)||Reader heap (MB)||')
    readerHeapMB, indexSizeGB = stats['LatLonShape'][:2]
    print('|%.2f|%.2f|' % (indexSizeGB, readerHeapMB))

  print()
  print('||Approach||Shape||Operation||M hits/sec||QPS||Hit count||')
  for shape in shapes:
    for op in ops:
      tup = shape, op
      if tup in results:
        qps, mhps, totHits = results[tup]
        print('|%s|%s|%s|%.2f|%.2f|%d|' % ('LatLonShape',shape, op, mhps, qps, totHits))

def printCompareResults(results, stats, maxDoc, resultsBase, statsBase):
  print()
  print('Results on %2fM points:' % (maxDoc/1000000.))

  print()

  if '-reindex' in sys.argv or '-reindexFast' in sys.argv:
    print('Index time (sec)||Force merge time (sec)||Index size (GB)||Reader heap (MB)||')
    print('||Dev||Base||Diff ||Dev  ||Base  ||diff   ||Dev||Base||Diff||Dev||Base||Diff ||')
    readerHeapMB, indexSizeGB, indexTimeSec, forceMergeTimeSec = stats['LatLonShape']
    readerHeapMBBase, indexSizeGBBase, indexTimeSecBase, forceMergeTimeSecBase = statsBase['LatLonShape']
    print('|%.1fs|%.1fs|%2.f%%|%.1fs|%.1fs|%2.f%%|%.2f|%.2f|%2.f%%|%.2f|%.2f|%2.f%%|' % (indexTimeSec, indexTimeSecBase, computeDiff(indexTimeSec, indexTimeSecBase), forceMergeTimeSec, forceMergeTimeSecBase, computeDiff(forceMergeTimeSec, forceMergeTimeSecBase), indexSizeGB, indexSizeGBBase, computeDiff(indexSizeGB, indexSizeGBBase), readerHeapMB, readerHeapMBBase, computeDiff(readerHeapMB, readerHeapMBBase)))
  else:
    print('||Index size (GB)||Reader heap (MB)||')
    print('||Dev||Base||Diff||Dev||Base||Diff ||')
    readerHeapMB, indexSizeGB = stats['LatLonShape'][:2]
    readerHeapMBBase, indexSizeGBBase = statsBase['LatLonShape'][:2]
    print('|%.2f|%.2f|%2.f%%|%.2f|%.2f|%2.f%%|' % (indexSizeGB, indexSizeGBBase, computeDiff(indexSizeGB, indexSizeGBBase), readerHeapMB, readerHeapMBBase, computeDiff(readerHeapMB, readerHeapMBBase)))

  print()
  print('||Approach||Shape||M hits/sec      ||QPS            ||Hit count      ||')
  print('                 ||Dev||Base ||Diff||Dev||Base||Diff||Dev||Base||Diff||')
  for op in ops:
    for shape in shapes:
      tup = shape, op
      if tup in results:
        qps, mhps, totHits = results[tup]
        qpsBas, mhpsBas, totHitsBas = resultsBase[tup]
        print('|%s|%s|%.2f|%.2f|%2.f%%|%.2f|%.2f|%2.f%%|%d|%d|%2.f%%|' % (shape, op, mhps, mhpsBas, computeDiff(mhps, mhpsBas), qps, qpsBas, computeDiff(qps, qpsBas), totHits, totHitsBas, computeDiff(totHits, totHitsBas)))

def computeDiff(dev, base):
  if dev == 0:
    return 0
  return 100. * (dev - base) / base

def compile(basedir):
  sources =  '%s/src/main/perf/IndexAndSearchShapes.java' % (GEO_UTIL_DIR)

  testFramework = '%s/build/test-framework/classes/java' % (basedir)
  codecs = '%s/build/codecs/classes/java' % (basedir)
  core = '%s/build/core/classes/java' % (basedir)
  sandbox = '%s/build/sandbox/classes/java' % (basedir)
  compile = 'javac -cp %s:%s:%s:%s %s' % (testFramework, codecs, core, sandbox, sources)
  if os.system(compile):
    raise RuntimeError('compile failed : %s' % basedir)

def execute(results, tup, didReindexParam, indexKey, log, basedir, dev):

  extra = ' -file ' + fileName
  if '-reindex' in sys.argv and indexKey not in didReindexParam:
    extra = extra + ' -reindex'
    didReindexParam.add(indexKey)

  if '-reindexFast' in sys.argv and indexKey not in didReindexParam:
    extra = extra + ' -reindexFast'
    didReindexParam.add(indexKey)

  if dev:
    extra = extra + ' -dev'

  shapeCmd = shape

  utilSrcDir = '%s/src/main' % (GEO_UTIL_DIR)
  testFramework = '%s/build/test-framework/classes/java' % (basedir)
  codecs = '%s/build/codecs/classes/java' % (basedir)
  core = '%s/build/core/classes/java' % (basedir)
  sandbox = '%s/build/sandbox/classes/java' % (basedir)

  run = 'java -Xmx10g -cp %s:%s:%s:%s:%s perf.IndexAndSearchShapes -%s -%s%s' % (utilSrcDir, testFramework, codecs, core,  sandbox,  op, shapeCmd, extra)
  p = subprocess.Popen(run, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

  totHits = None
  indexSizeGB = None
  readerHeapMB = None
  maxDoc = None
  indexTimeSec = 0.0
  forceMergeTimeSec = 0.0
  while True:
    line = p.stdout.readline().decode('utf-8')
    if len(line) == 0:
      break
    line = line.rstrip()
    m = reTotHits.search(line)
    if m is not None:
      x = m.group(1)
      if totHits is None:
        totHits = x
      elif totHits != x:
        raise RuntimeError('total hits changed from %s to %s' % (totHits, x))
    log.write('%7.1fs: %s, %s: %s\n' % (time.time()-t0, op, shape, line))
    doPrintLine = False
    if line.find('...') != -1 or line.find('ITER') != -1 or line.find('***') != -1:
      doPrintLine = True
    if line.startswith('BEST QPS: '):
      doPrintLine = True
      results[(shape, op)] = (float(line[10:]), bestMHPS, int(totHits))
      pickle.dump((rev, stats, results), open(resultsFileName, 'wb'))
    if line.startswith('BEST M hits/sec: '):
      doPrintLine = True
      bestMHPS = float(line[17:])
    if line.startswith('INDEX SIZE: '):
      doPrintLine = True
      indexSizeGB = float(line[12:-3])
    if line.startswith('READER MB: '):
      doPrintLine = True
      readerHeapMB = float(line[11:])
    if line.startswith('numPoints='):
      doPrintLine = True
    if line.startswith('maxDoc='):
      maxDoc = int(line[7:])
      doPrintLine = True
    i = line.find(' sec to index part ')
    if i != -1:
      doPrintLine = True
      indexTimeSec += float(line[:i])
    i = line.find(' sec to force merge part ')
    if i != -1:
      doPrintLine = True
      forceMergeTimeSec += float(line[:i])

    if doPrintLine:
      print('%7.1fs: %s, %s: %s' % (time.time()-t0, op, shape, line))

  tup[0] = readerHeapMB
  tup[1] = indexSizeGB
  tup[2] = indexTimeSec
  tup[3] = forceMergeTimeSec
  return maxDoc

def antCompile(basedir):
 if os.chdir(basedir):
     raise RuntimeError('cannot change working directory: %s' % basedir)
 print ('ant compile on %s...' %basedir)
 if os.system('ant compile > %s/compile.log' % GEO_LOGS_DIR):
     raise RuntimeError('ant compile failed > %s/compile.log' % GEO_LOGS_DIR)
 if os.chdir(GEO_UTIL_DIR):
     raise RuntimeError('cannot change working directory: %s' % GEO_LOGS_DIR)


if nightly:
  # paths for nightly run
  GEO_UTIL_DIR = '/l/util.nightly/'
  GEO_LUCENE_DIR = '/l/trunk.nightly/lucene/'
  GEO_LOGS_DIR = '/l/logs.nightly/geoshape'
  if '-timeStamp' in sys.argv:
    timeStamp = sys.argv[sys.argv.index('-timeStamp')+1]
    year, month, day, hour, minute, second = (int(x) for x in timeStamp.split('.'))
    timeStampDateTime = datetime.datetime(year, month, day, hour, minute, second)
  else:
    start = datetime.datetime.now()
    timeStamp = '%04d.%02d.%02d.%02d.%02d.%02d' % (start.year, start.month, start.day, start.hour, start.minute, start.second)
  resultsFileName = '%s/%s.pk' % (GEO_LOGS_DIR, timeStamp)
else:
  resultsFileName = 'geo.results.pk'

# nocommit should we "ant jar"?

if nightly:
  logFileName = '%s/%s.log.txt' % (GEO_LOGS_DIR, timeStamp)
else:
  logFileName = '%s/geoShapeBenchLog.txt' % (GEO_LOGS_DIR)

os.chdir(GEO_LUCENE_DIR)
rev = os.popen('git rev-parse HEAD').read().strip()
print('git head revision %s' % rev)
print('\nNOTE: logging all output to %s; saving results to %s\n' % (logFileName, resultsFileName))



# nocommit should we "ant jar"?
if '-ant' in sys.argv:
  antCompile(GEO_LUCENE_DIR)
  if compareRun:
    antCompile(BASELINE_LUCENE_DIR)


compile(GEO_LUCENE_DIR)
if compareRun:
   compile(BASELINE_LUCENE_DIR)

results = {}
stats = {}
theMaxDoc = None

resultsBase = {}
statsBase = {}
theMaxDocBase = None

didReIndex = set()
didReIndexBase = set()

t0 = time.time()


rev = os.popen('git rev-parse HEAD').read().strip()
print('git head revision %s' % rev)
print('\nNOTE: logging all output to %s; saving results to %s\n' % (logFileName, resultsFileName))

# TODO: filters
with open(logFileName, 'w') as log:
  log.write('\ngit head revision %s' % rev)
  for op in ops:
    for shape in shapes:
      indexKey = 'LatLonShape'
      tup =[None, None, None, None]
      maxDoc = execute(results, tup, didReIndex, indexKey, log, GEO_LUCENE_DIR, False)

      if maxDoc is None:
        raise RuntimeError('did not see maxDoc')


      if indexKey not in stats:
        stats[indexKey] = tup
      elif stats[indexKey][:2] != tup[:2]:
        raise RuntimeError('stats changed for %s: %s vs %s' % (indexKey, stats[indexKey], tup))

      if theMaxDoc == None:
        theMaxDoc = maxDoc
      elif maxDoc != theMaxDoc:
        raise RuntimeError('maxDoc changed from %s to %s' % (theMaxDoc, maxDoc))

      if compareRun:
        tupBase =[None, None, None, None]
        maxDocBase = execute(resultsBase, tupBase, didReIndexBase, indexKey, log, BASELINE_LUCENE_DIR, True)
        if maxDocBase is None:
          raise RuntimeError('did not see maxDoc')
        if maxDocBase != maxDoc:
           raise RuntimeError('different count from current and baseline projects')

        if indexKey not in statsBase:
          statsBase[indexKey] = tupBase
        elif statsBase[indexKey][:2] != tupBase[:2]:
          raise RuntimeError('stats changed for %s: %s vs %s' % (indexKey, statsBase[indexKey], tupBase))
        printCompareResults(results, stats, maxDoc, resultsBase, statsBase)

      else:
        printResults(results, stats, maxDoc)

if nightly:
  os.system('bzip2 --best %s' % logFileName)
  
print('Took %.1f sec to run all geo benchmarks' % (time.time()-t0))
