import os
import sys
import constants
import re
import benchUtil
from competition import *

def run(command):
  if os.system(command):
    raise RuntimeError('%s failed' % command)

DIR_IMPL = 'NIOFSDirectory'
INDEX = 'd:/dev/lucene/indices/wikimedium10k.lucene-trunk.facets.Date.Direct.Lucene41.nd0.01M/index'
SEED = 17
INDEX_NUM_THREADS = 6
SEARCH_NUM_THREADS = 24
RUN_TIME_SEC = 60
VERBOSE = '-verbose' in sys.argv
ADDS_ONLY = '-adds' in sys.argv
STATS_EVERY_SEC = 1
REOPEN_RATE = 0.5

def main():
  if not os.path.exists(constants.LOGS_DIR):
    os.makedirs(constants.LOGS_DIR)

  r = benchUtil.RunAlgs(constants.JAVA_COMMAND, False)
  c = Competitor("base", "lucene-trunk")
  r.compile(c)
  cp = r.classPathToString(r.getClassPath(c.checkout))
  
  # fix reopen rate, vary indexing rate
  #for indexRate in (100, 200, 500, 1000, 2000, 5000, 10000):
  indexRate = 2000
  for reopenRate in (0.1, 0.5, 1.0, 5.0, 10.0, 20.0):
    logFileName = '%s/dps%s_reopen%s.txt' % (constants.LOGS_DIR, indexRate, reopenRate)
    docCount, searchCount, readerCount, runTimeSec = runOne(cp, indexRate, reopenRate, logFileName)
    print 'Index rate target=%s/sec: %.2f docs/sec; %.2f reopens/sec; %.2f searches/sec' % (indexRate, docCount/float(runTimeSec), readerCount/float(runTimeSec), searchCount/float(runTimeSec))
  
def runOne(claspath, docsPerSec, reopensPerSec, logFileName):
  if ADDS_ONLY:
    mode = 'add'
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
  command += ' %s' % INDEX_NUM_THREADS
  command += ' %s' % reopensPerSec
  command += ' %s' % mode
  command += ' %s' % STATS_EVERY_SEC
  command += " yes 0.0"
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

