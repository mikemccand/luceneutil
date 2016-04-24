import sys
import pickle
import subprocess
import os
import re
import datetime
import time

reTotHits = re.compile('totHits=(\d+)$')

GEO_LOGS_DIR = '/l/logs.nightly/geo'

nightly = '-nightly' in sys.argv

if nightly and '-reindex' not in sys.argv:
  sys.argv.append('-reindex')

def printResults(results, stats, maxDoc):
  print()
  print('Results on %2fM points:' % (maxDoc/1000000.))

  print()

  if '-reindex' in sys.argv:
    print('||Approach||Index time (sec)||Force merge time (sec)||Index size (GB)||Reader heap (MB)||')
    for approach in ('geo3d', 'points', 'geopoint'):
      if approach in stats:
        readerHeapMB, indexSizeGB, indexTimeSec, forceMergeTimeSec = stats[approach]
        print('|%s|%.1fs|%.1fs|%.2f|%.2f|' % (approach, indexTimeSec, forceMergeTimeSec, indexSizeGB, readerHeapMB))
  else:
    print('||Approach||Index size (GB)||Reader heap (MB)||')
    for approach in ('geo3d', 'points', 'geopoint'):
      if approach in stats:
        readerHeapMB, indexSizeGB = stats[approach][:2]
        print('|%s|%.2f|%.2f|' % (approach, indexSizeGB, readerHeapMB))
    
  print()
  print('||Shape||Approach||M hits/sec||QPS||Hit count||')
  for shape in ('distance', 'box', 'poly 10', 'polyMedium', 'polyRussia', 'nearest 10', 'sort'):
    for approach in ('geo3d', 'points', 'geopoint'):
      tup = shape, approach
      if tup in results:
        qps, mhps, totHits = results[tup]
        print('|%s|%s|%.2f|%.2f|%d|' % (shape, approach, mhps, qps, totHits))
      else:
        print('|%s|%s||||' % (shape, approach))

haveNearest = True
haveGeo3DNewPolyAPI = True
haveGeo3DPoly = False

if nightly:
  if '-timeStamp' in sys.argv:
    timeStamp = sys.argv[sys.argv.index('-timeStamp')+1]
    year, month, day, hour, minute, second = (int(x) for x in timeStamp.split('.'))
    timeStampDateTime = datetime.datetime(year, month, day, hour, minute, second)
    if timeStampDateTime < datetime.datetime(year=2016, month=4, day=14):
      haveNearest = False
    if timeStampDateTime < datetime.datetime(year=2016, month=4, day=5):
      haveGeo3DNewPolyAPI = False
    if timeStampDateTime < datetime.datetime(year=2016, month=4, day=9):
      # something badly wrong before this...
      haveGeo3DPoly = False
  else:
    start = datetime.datetime.now()
    timeStamp = '%04d.%02d.%02d.%02d.%02d.%02d' % (start.year, start.month, start.day, start.hour, start.minute, start.second)        
  resultsFileName = '%s/%s.pk' % (GEO_LOGS_DIR, timeStamp)
else:
  resultsFileName = 'geo.results.pk'
  
# nocommit should we "ant jar"?

if os.system('javac -cp build/test-framework/classes/java:build/codecs/classes/java:build/core/classes/java:build/sandbox/classes/java:build/spatial/classes/java:build/spatial3d/classes/java /l/util/src/main/perf/IndexAndSearchOpenStreetMaps.java /l/util/src/main/perf/RandomQuery.java'):
  raise RuntimeError('compile failed')

results = {}
stats = {}
theMaxDoc = None

didReIndex = set()

t0 = time.time()

if nightly:
  logFileName = '%s/%s.log.txt' % (GEO_LOGS_DIR, timeStamp)
else:
  logFileName = '/l/logs/geoBenchLog.txt'

print('git head revision %s' % os.popen('git rev-parse HEAD').read().strip())
print('\nNOTE: logging all output to %s; saving results to %s\n' % (logFileName, resultsFileName))

# TODO: filters
with open(logFileName, 'w') as log:

  for shape in ('nearest 10', 'sort', 'distance', 'box', 'poly 10', 'polyMedium', 'polyRussia'):
    for approach in ('points', 'geopoint', 'geo3d'):

      if shape == 'polyRussia' and approach != 'points':
        continue

      if shape == 'nearest 10' and not haveNearest:
        # we are back-testing, and got back before nearest was pushed
        continue

      if shape == 'poly 10' and approach == 'geo3d' and (not haveGeo3DNewPolyAPI or not haveGeo3DPoly):
        # we are back-testing, and got back before geo3d had the .newPolygonQuery API
        continue

      if shape == 'nearest 10' and approach != 'points':
        # KNN only implemented for LatLonPoint now
        continue

      if shape == 'sort' and approach not in ('points', 'geopoint'):
        # distance sort only implemented for LatLonPoint now
        continue

      if shape == 'polyMedium' and approach == 'geo3d':
        continue

      if shape == 'sort' and approach == 'points':
        indexKey = 'points-dvs'
      else:
        indexKey = approach

      if '-reindex' in sys.argv and indexKey not in didReIndex:
        extra = ' -reindex'
        didReIndex.add(indexKey)
      else:
        extra = ''

      if shape == 'sort':
        shapeCmd = 'sort -box'
      else:
        shapeCmd = shape

      p = subprocess.Popen('java -Xmx10g -cp /l/util/src/main:build/test-framework/classes/java:build/codecs/classes/java:build/core/classes/java:build/sandbox/classes/java:build/spatial/classes/java:build/spatial3d/classes/java perf.IndexAndSearchOpenStreetMaps -%s -%s%s' % (approach, shapeCmd, extra), shell=True, stdout=subprocess.PIPE)

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
        log.write('%7.1fs: %s, %s: %s\n' % (time.time()-t0, approach, shape, line))
        doPrintLine = False
        if line.find('...') != -1 or line.find('ITER') != -1 or line.find('***') != -1:
          doPrintLine = True
        if line.startswith('BEST QPS: '):
          doPrintLine = True
          results[(shape, approach)] = (float(line[10:]), bestMHPS, int(totHits))
          pickle.dump((stats, results), open(resultsFileName, 'wb'))
        if line.startswith('BEST M hits/sec: '):
          doPrintLine = True
          bestMHPS = float(line[17:])
        if line.startswith('INDEX SIZE: '):
          doPrintLine = True
          indexSizeGB = float(line[12:-3])
        if line.startswith('READER MB: '):
          doPrintLine = True
          readerHeapMB = float(line[11:])
        if line.startswith('maxDoc='):
          maxDoc = int(line[7:])
        i = line.find(' sec to index part ')
        if i != -1:
          doPrintLine = True
          indexTimeSec += float(line[:i])
        i = line.find(' sec to force merge part ')
        if i != -1:
          doPrintLine = True
          forceMergeTimeSec += float(line[:i])
          
        if doPrintLine:
          print('%7.1fs: %s, %s: %s' % (time.time()-t0, approach, shape, line))

      tup = readerHeapMB, indexSizeGB, indexTimeSec, forceMergeTimeSec
      if approach not in stats:
        stats[approach] = tup
      elif stats[approach][:2] != tup[:2]:
        raise RuntimeError('stats changed for %s: %s vs %s' % (approach, stats[approach], tup))

      if theMaxDoc is None:
        theMaxDoc = maxDoc
      elif maxDoc != theMaxDoc:
        raise RuntimeError('maxDoc changed from %d to %d' % (themaxDoc, maxDoc))

      printResults(results, stats, maxDoc)

if nightly:
  os.system('bzip2 --best %s' % logFileName)
  
print('Took %.1f sec to run all geo benchmarks' % (time.time()-t0))
