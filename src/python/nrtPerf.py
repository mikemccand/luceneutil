import os
import sys
import constants
import re
import benchUtil
import competition
import stats

def run(command):
  if os.system(command):
    raise RuntimeError('%s failed' % command)

VERBOSE = '-verbose' in sys.argv

def main():
  sourceData = competition.sourceData()
  comp = competition.Competition(randomSeed=0)

  index = comp.newIndex(constants.TRUNK_CHECKOUT, sourceData,
                        postingsFormat='Lucene41',
                        idFieldPostingsFormat='Memory',
                        grouping=False,
                        doDeletions=False,
                        addDVFields=True,
                        )
  
  c = competition.Competitor('base', constants.TRUNK_CHECKOUT)
 
  r = benchUtil.RunAlgs(constants.JAVA_COMMAND, False)
  r.compile(c)
  r.makeIndex(c.name, index, False)

  if not os.path.exists(constants.LOGS_DIR):
    os.makedirs(constants.LOGS_DIR)

  indexRate = 1000
  runTimeSec = 10
  numSearchThreads = 10
  numIndexThreads = 4
  
#  for reopenRate in (0.1, 0.5, 1.0, 5.0, 10.0, 20.0):
#  for reopenRate in (0.1, 0.5, 1.0):
  for reopenRate in (1, 0.5):
    restats = runOne(classpath='%s' % r.classPathToString(r.getClassPath(c.checkout)),
                     mode=benchUtil.getArg('-mode', 'update', True),
                     docsPerSec=indexRate,
                     reopensPerSec=reopenRate,
                     fullIndexPath='%s/index' % benchUtil.nameToIndexPath(index.getName()),
                     runTimeSec=runTimeSec,
                     numSearchThreads=numSearchThreads,
                     numIndexThreads=numIndexThreads)
    print 'Index rate target=%s/sec: %.2f docs/sec; %.2f reopens/sec; %.2f searches/sec' % \
          (indexRate,
           restats.totalDocs/float(runTimeSec),
           restats.totalReopens/float(runTimeSec),
           restats.totalSearches/float(runTimeSec))
    
reNRTReopenTime = re.compile('^Reopen: +([0-9.]+) msec$', re.MULTILINE)
reByTime = re.compile('  (\d+) searches=(\d+) docs=(\d+) reopens=(\d+) totUpdateTime=(\d+)$')

def runOne(classpath, docsPerSec, reopensPerSec, fullIndexPath, 
           mode='update',
           dir='MMapDirectory',
           seed=17,
           runTimeSec=60,
           numSearchThreads=4,
           numIndexThreads=constants.INDEX_NUM_THREADS,
           statsEverySec=1,
           commit="no",
           docsFile=constants.WIKI_MEDIUM_DOCS_LINE_FILE):
  logFileName = '%s/dps%s_reopen%s.txt' % (constants.LOGS_DIR, docsPerSec, reopensPerSec)
  command = constants.JAVA_COMMAND
  command += ' -cp "%s"' % classpath
  command += ' perf.NRTPerfTest'
  command += ' %s' % dir
  command += ' %s' % fullIndexPath
  command += ' multi'
  command += ' %s' % docsFile
  command += ' %s' % seed
  command += ' %s' % docsPerSec
  command += ' %s' % runTimeSec
  command += ' %s' % numSearchThreads
  command += ' %s' % numIndexThreads
  command += ' %s' % reopensPerSec
  command += ' %s' % mode
  command += ' %s' % statsEverySec
  command += ' %s' % commit
  command += " 0.0"
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
    reopenStats = ReopenStats()
    for line in result.split('\n'):
      m = reByTime.match(line.rstrip())
      if m is not None:
        t = int(m.group(1))
        searches = int(m.group(2))
        docs = int(m.group(3))
        reopens = int(m.group(4))
        updateTime = int(m.group(5))
        perTimeQ.append((t, searches, docs, reopens, updateTime))
        # discard first 5 seconds -- warmup
        if t >= 5 * reopensPerSec:
          reopenStats.totalSearches += searches
          reopenStats.totalDocs += docs
          reopenStats.totalReopens += reopens
          reopenStats.totalUpdateTime += updateTime
          reopenStats.qtCount += 1

    # Reopen times
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
    reopenStats.meanReopenTime = mean
    reopenStats.stddevReopenTime = stdDev

    if VERBOSE:
      print 'reopen stats:'
      reopenStats.toString()
      print
          
    return reopenStats
  except:
    print 'FAILED -- output:\n%s' % result
    raise

class ReopenStats:
  
  def __init__(self):
    self.meanReopenTime = 0
    self.stddevReopenTime = 0
    self.totalSearches = 0
    self.totalDocs = 0
    self.totalReopens = 0
    self.totalUpdateTime = 0
    self.qtCount = 0

  def toString(self):
    print 'meanReopenTime=%s stdReopenTime=%s qtCount=%s totalDocs=%s totalReopen=%s totalSearches=%s totalUpdateTime=%s' % \
            (self.meanReopenTime,
             self.stddevReopenTime,
             self.qtCount,
             self.totalDocs,
             self.totalReopens,
             self.totalSearches,
             self.totalUpdateTime)
    
if __name__ == '__main__':
  main()
