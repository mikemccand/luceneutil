import os
import sys
import constants
import re
import benchUtil
from competition import *
import stats

def run(command):
  if os.system(command):
    raise RuntimeError('%s failed' % command)

DIR_IMPL = 'NIOFSDirectory'
INDEX = 'd:/dev/lucene/indices/wikimedium500k.lucene-trunk.Lucene41.Memory.dvfields.nd0.5M/index'
SEED = 17
SEARCH_NUM_THREADS = 24
RUN_TIME_SEC = 60
VERBOSE = '-verbose' in sys.argv
ADDS_ONLY = '-adds' in sys.argv
FIELD_UPDATES = '-fupdates' in sys.argv
STATS_EVERY_SEC = 1
REOPEN_RATE = 0.5

def main():
  if not os.path.exists(constants.LOGS_DIR):
    os.makedirs(constants.LOGS_DIR)

  r = benchUtil.RunAlgs(constants.JAVA_COMMAND, False)
  c = Competitor("base", "lucene-trunk")
  r.compile(c)
  cp = r.classPathToString(r.getClassPath(c.checkout))
  
  constants.INDEX_NUM_THREADS = 1
  # 1 update/sec
  indexRate = 1 * constants.INDEX_NUM_THREADS
  
#  for reopenRate in (0.1, 0.5, 1.0, 5.0, 10.0, 20.0):
#  for reopenRate in (0.1, 0.5, 1.0):
  for reopenRate in (1,):
    logFileName = '%s/dps%s_reopen%s.txt' % (constants.LOGS_DIR, indexRate, reopenRate)
    docCount, searchCount, readerCount, runTimeSec = runOne(cp, indexRate, reopenRate, logFileName)
    reopenStats(logFileName)
    print 'Index rate target=%s/sec: %.2f docs/sec; %.2f reopens/sec; %.2f searches/sec' % (indexRate, docCount/float(runTimeSec), readerCount/float(runTimeSec), searchCount/float(runTimeSec))

reNRTReopenTime = re.compile('^Reopen: +([0-9.]+) msec$', re.MULTILINE)
reByTime = re.compile('  (\d+) searches=(\d+) docs=(\d+) reopens=(\d+)$')

def runOne(claspath, docsPerSec, reopensPerSec, logFileName):
  if ADDS_ONLY:
    mode = 'add'
  elif FIELD_UPDATES:
    mode = 'fieldUpdates'
  else:
    mode = 'update'
  command = constants.JAVA_COMMAND
  command += ' -cp "%s"' % claspath
  command += ' perf.NRTPerfTest'
  command += ' %s' % DIR_IMPL
  command += ' %s' % INDEX
  command += ' multi'
  command += ' %s' % constants.WIKI_MEDIUM_DOCS_LINE_FILE
  command += ' %s' % SEED
  command += ' %s' % docsPerSec
  command += ' %s' % RUN_TIME_SEC
  command += ' %s' % SEARCH_NUM_THREADS
  command += ' %s' % constants.INDEX_NUM_THREADS
  command += ' %s' % reopensPerSec
  command += ' %s' % mode
  command += ' %s' % STATS_EVERY_SEC
  command += " no 0.0"
  command += ' > %s 2>&1' % logFileName

  if VERBOSE:
    print
    print 'run: %s' % command
  os.system(command)
  result = open(logFileName, 'rb').read()
  if VERBOSE:
    print result

  try:
    perTimeQ = []
    qtCount = 0
    totSearches = 0
    totDocs = 0
    totReopens = 0
    for line in result.split('\n'):
      m = reByTime.match(line.rstrip())
      if m is not None:
        t = int(m.group(1))
        searches = int(m.group(2))
        docs = int(m.group(3))
        reopens = int(m.group(4))
        perTimeQ.append((t, searches, docs, reopens))
        # discard first 5 seconds -- warmup
        if t >= 10:
          totSearches += searches
          totDocs += docs
          totReopens += reopens
          qtCount += 1
  
    return totDocs, totSearches, totReopens, qtCount * STATS_EVERY_SEC
  except:
    print 'FAILED -- output:\n%s' % result
    raise

def reopenStats(logFileName):
  times = []
  for line in open(logFileName, 'rb').read().split('\n'):
    m = reNRTReopenTime.match(line.rstrip())
    if m is not None:
      times.append(float(m.group(1)))
    
  # Discard first 10% (JVM warmup): minimum of 1 but no more than 10
  times = times[min(10, max(1, len(times) / 10)):]

  # Discard worst 2%
  times.sort()
  numDrop = len(times)/50
  if numDrop > 0:
    print 'drop: %s' % ' '.join(['%.1f' % x for x in times[-numDrop:]])
    times = times[:-numDrop]
  print 'times: %s' % ' '.join(['%.1f' % x for x in times])

  minVal, maxVal, mean, stdDev = stats.getStats(times)
  print 'NRT reopen time (msec) mean=%.4f stdDev=%.4f' % (mean, stdDev)
  
if __name__ == '__main__':
  main()

