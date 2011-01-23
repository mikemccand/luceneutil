import os
import sys
import constants
import re

def run(command):
  if os.system(command):
    raise RuntimeError('%s failed' % command)

DIR_IMPL = 'MMapDirectory'
INDEX = '/lucene/indices/clean.svn.Standard.opt.nd24.9005M/index'
COMMIT = 'multi'
SEED = 17
INDEX_NUM_THREADS = 6
SEARCH_NUM_THREADS = 24
RUN_TIME_SEC = 1200
VERBOSE = '-verbose' in sys.argv
ADDS_ONLY = '-adds' in sys.argv

def main():
  run('ant compile >> compile.log 2>&1')
  run('javac -cp ../modules/analysis/build/common/classes/java:build/classes/java:build/classes/test perf/NRTPerfTest.java')

  # fix reopen rate, vary indexing rate
  #for indexRate in (10, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000):
  for indexRate in (500,):
    docCount, searchCount, readerCount, runTimeMS = runOne(indexRate, 25)
    runTimeSec = runTimeMS/1000.0
    print 'Index rate target=%s/sec: %.2f docs/sec; %.2f reopens/sec; %.2f searches/sec' % (indexRate, docCount/float(runTimeSec), readerCount/float(runTimeSec), searchCount/float(runTimeSec))
  
def runOne(docsPerSec, reopensPerSec):
  if ADDS_ONLY:
    mode = 'add'
  else:
    mode = 'update'
  command = constants.JAVA_COMMAND
  command += ' -classpath .:lib/junit-4.7.jar:../modules/analysis/build/common/classes/java:build/classes/java:build/classes/test'
  command += ' perf.NRTPerfTest'
  command += ' %s %s %s %s %s %s %s %s %s %s %s' % (DIR_IMPL, INDEX, COMMIT, constants.WIKI_LINE_FILE, SEED, docsPerSec, RUN_TIME_SEC, SEARCH_NUM_THREADS, INDEX_NUM_THREADS, reopensPerSec, mode)
  if VERBOSE:
    print
    print 'run: %s' % command
  result = os.popen(command, 'rb').read()
  if VERBOSE:
    print result
  try:
    runTimeMS = float(re.search('Ran for ([0-9.]+) ms', result).group(1))
    docCount = int(re.search('Indexed (\d+) docs', result).group(1))
    searchCount = int(re.search('Finished (\d+) searches', result).group(1))
    readerCount = int(re.search('Opened (\d+) readers', result).group(1))
    return docCount, searchCount, readerCount, runTimeMS
  except:
    print 'FAILED -- output:\n%s' % result
    raise

  
if __name__ == '__main__':
  main()

