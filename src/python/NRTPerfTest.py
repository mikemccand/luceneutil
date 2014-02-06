import os
import sys
import constants
import re

def run(command):
  if os.system(command):
    raise RuntimeError('%s failed' % command)

DIR_IMPL = 'NIOFSDirectory'
INDEX = '/lucene/indices/clean.svn.Standard.nd24.9005M/index'
COMMIT = 'multi'
SEED = 17
INDEX_NUM_THREADS = 6
SEARCH_NUM_THREADS = 24
RUN_TIME_SEC = 60
VERBOSE = '-verbose' in sys.argv
ADDS_ONLY = '-adds' in sys.argv
STATS_EVERY_SEC = 1
REOPEN_RATE = 0.5
LOG_DIR = 'logs'

def main():
  if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
    
  run('ant compile >> compile.log 2>&1')
  run('%s -cp ../modules/analysis/build/common/classes/java:build/classes/java:build/classes/test perf/NRTPerfTest.java' % constants.JAVAC_EXE)

  # fix reopen rate, vary indexing rate
  #for indexRate in (100, 200, 500, 1000, 2000, 5000, 10000):
  indexRate = 2000
  for reopenRate in (0.1, 0.5, 1.0, 5.0, 10.0, 20.0):
    logFileName = '%s/dps%s_reopen%s.txt' % (LOG_DIR, indexRate, reopenRate)
    docCount, searchCount, readerCount, runTimeSec = runOne(indexRate, reopenRate, logFileName)
    print 'Index rate target=%s/sec: %.2f docs/sec; %.2f reopens/sec; %.2f searches/sec' % (indexRate, docCount/float(runTimeSec), readerCount/float(runTimeSec), searchCount/float(runTimeSec))
  
def runOne(docsPerSec, reopensPerSec, logFileName):
  if ADDS_ONLY:
    mode = 'add'
  else:
    mode = 'update'
  command = constants.JAVA_COMMAND
  command += ' -classpath .:lib/junit-4.7.jar:../modules/analysis/build/common/classes/java:build/classes/java:build/classes/test'
  command += ' perf.NRTPerfTest'
  command += ' %s %s %s %s %s %s %s %s %s %s %s %s' % \
             (DIR_IMPL, INDEX, COMMIT, constants.WIKI_LINE_FILE, SEED, docsPerSec, RUN_TIME_SEC, SEARCH_NUM_THREADS, INDEX_NUM_THREADS, reopensPerSec, mode, STATS_EVERY_SEC)
  if VERBOSE:
    print
    print 'run: %s' % command
  result = os.popen(command, 'rb').read()
  open(logFileName, 'wb').write(result)
  if VERBOSE:
    print result

  try:
    perTimeQ = []
    reByTime = re.compile('  (\d+) searches=(\d+) docs=(\d+) reopens=(\d+)$')
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

  
if __name__ == '__main__':
  main()

