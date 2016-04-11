import sys
import pickle
import subprocess
import os
import re
import time

reTotHits = re.compile('totHits=(\d+)$')

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
  for shape in ('distance', 'box', 'poly 10'):
    for approach in ('geo3d', 'points', 'geopoint'):
      tup = shape, approach
      if tup in results:
        qps, mhps, totHits = results[tup]
        print('|%s|%s|%.2f|%d|' % (shape, approach, mhps, qps, totHits))
      else:
        print('|%s|%s||||' % (shape, approach))
  
# nocommit should we "ant jar"?

if os.system('javac -cp build/test-framework/classes/java:build/codecs/classes/java:build/core/classes/java:build/sandbox/classes/java:build/spatial/classes/java:build/spatial3d/classes/java /l/util/src/main/perf/IndexAndSearchOpenStreetMaps.java'):
  raise RuntimeError('compile failed')

results = {}
stats = {}
theMaxDoc = None

didReIndex = set()

t0 = time.time()

logFileName = '/l/logs/geoBenchLog.txt'
print('\nNOTE: logging all output to %s\n' % logFileName)

with open(logFileName, 'w') as log:

  for shape in ('distance', 'box', 'poly 10'):
    for approach in ('geopoint', 'points', 'geo3d'):

      if '-reindex' in sys.argv and approach not in didReIndex:
        extra = ' -reindexSlow'
        didReIndex.add(approach)
      else:
        extra = ''

      p = subprocess.Popen('java -Xmx10g -cp /l/util/src/main:build/test-framework/classes/java:build/codecs/classes/java:build/core/classes/java:build/sandbox/classes/java:build/spatial/classes/java:build/spatial3d/classes/java perf.IndexAndSearchOpenStreetMaps -%s -%s%s' % (approach, shape, extra), shell=True, stdout=subprocess.PIPE)

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
          pickle.dump(results, open('results.pk', 'wb'))
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
