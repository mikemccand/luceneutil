import os
import shutil
import time
import re

# run from lucene subdir in trunk checkout

reIter = re.compile(': (\d+) msec; totalHits=(\d+) hash=(\d+)')

def run(cmd):
  if os.system(cmd):
    raise RuntimeError('%s failed' % cmd)

def parseLog():
  global totalHits
  global hash
  
  totalTerms = None
  bestMS = None
  indexTimeSec = None
  indexSizeBytes = None
  
  f = open('log.txt', 'r')
  while True:
    line = f.readline()
    if line == '':
      break
    line = line.strip()
    if line.startswith('total terms:'):
      totalTerms = int(line[12:])
    elif line.startswith('iter '):
      m = reIter.search(line)
      ms = int(m.group(1))
      x = int(m.group(2))
      y = int(m.group(3))
      if bestMS is None or ms < bestMS:
        bestMS = ms
        totalHits = x
        hash = y
      elif totalHits != x or hash != y:
        raise RuntimeError('wrong totalHits/hash')
    elif line.startswith('After close: '):
      indexTimeSec = float(line[13:-4])
    elif line.startswith('Total index size: '):
      indexSizeBytes = int(line[18:-6])
      
  f.close()

  return indexTimeSec, indexSizeBytes, totalTerms, bestMS

print('Compile...')
os.chdir('core')
run('ant jar > compile.log 2>&1')
os.chdir('..')
run('javac -d /l/util/build -cp build/core/classes/java:build/analysis/common/classes/java /l/util/src/main/perf/AutoPrefixPerf.java > compile.log 2>&1')

# Used to validate each run got the same hits:
totalHits = None
hash = None

for precStep in (4, 8, 12, 16):
  print
  print('  NF precStep=%d' % precStep)
  if os.path.exists('/l/indices/numbers'):
    shutil.rmtree('/l/indices/numbers')
  run('java -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.AutoPrefixPerf /lucenedata/numbers/randlongs.10m.txt /lucenedata/numbers/randlongs.queries.txt /l/indices/numbers %d 0 0 > log.txt 2>&1' % precStep)

  indexTimeSec, indexSizeBytes, totalTerms, bestMS = parseLog()
  print('    index sec %.2f' % indexTimeSec)
  print('    index MB %.2f' % (indexSizeBytes/1024/1024.))
  print('    term count %s' % totalTerms)
  print('    search msec %s' % bestMS)  

for minItemsInPrefix in 5, 10, 15, 20, 25, 30, 35, 40:
  for mult in 2, 3, 4, 5, None:
    print
    print('AP min=%d max=%d' % (minItemsInPrefix, maxItemsInPrefix))
    if mult is None:
      maxItemsInPrefix = (1<<31)-1
    else:
      maxItemsInPrefix = (minItemsInPrefix-1) * mult
    if os.path.exists('/l/indices/numbers'):
      shutil.rmtree('/l/indices/numbers')
    run('java -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.AutoPrefixPerf /lucenedata/numbers/randlongs.10m.txt /lucenedata/numbers/randlongs.queries.txt /l/indices/numbers 0 %d %d > log.txt 2>&1' % (minItemsInPrefix, maxItemsInPrefix))

    indexTimeSec, indexSizeBytes, totalTerms, bestMS = parseLog()
    print('    index sec %.2f' % indexTimeSec)
    print('    index MB %.2f' % (indexSizeBytes/1024/1024.))
    print('    term count %d' % totalTerms)
    print('    search msec %d' % bestMS)  
