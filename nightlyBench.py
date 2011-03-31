import time
import datetime
import os
import shutil
import benchUtil
import constants

# TODO
#   - checkout separate util.nightly and run from there
#   - after 1 night, verify against prior night -- catch regressions

# nocommit
DEBUG = True

if DEBUG:
  INDEX_NUM_DOCS = 100000
else:
  INDEX_NUM_DOCS = 27625038

LINE_FILE = '/p/lucene/data/enwiki-20110115-lines-1k-fixed.txt'
NIGHTLY_LOG_DIR = '/lucene/logs.nightly'
NIGHTLY_DIR = 'trunk.nightly'

def now():
  return datetime.datetime.now()

def message(s):
  print '[%s] %s' % (now(), s)

def runCommand(command):
  message('RUN: %s' % command)
  t0 = time.time()
  if os.system(command):
    message('  FAILED')
    raise RuntimeError('command failed: %s' % command)
  message('  took %.1f sec' % (time.time()-t0))
                                
def run():

  start = now()
  print
  print
  print
  print
  message('start')
  id = 'nightly'
  upto = None
  while True:
    runLogDir = '%s/%04d.%02d.%02d' % (NIGHTLY_LOG_DIR, start.year, start.month, start.day)
    if upto is not None:
      runLogDir += '.%d' % upto
      upto += 1
    else:
      upto = 0
    if not os.path.exists(runLogDir):
      os.makedirs(runLogDir)
      break
  message('log dir %s' % runLogDir)

  os.chdir('/lucene/%s' % NIGHTLY_DIR)
  for i in range(30):
    try:
      runCommand('svn update')
    except RuntimeError:
      message('  retry...')
      time.sleep(60.0)
    else:
      break

  r = benchUtil.RunAlgs(constants.JAVA_COMMAND)
  index = benchUtil.Index(NIGHTLY_DIR, 'wikimedium', 'StandardAnalyzer', 'Standard', INDEX_NUM_DOCS, constants.INDEX_NUM_THREADS, lineDocSource=LINE_FILE)

  c = benchUtil.Competitor(id, 'trunk.nightly', index, 'MMapDirectory', 'StandardAnalyzer', 'multi', constants.WIKI_MEDIUM_TASKS_FILE)
  r.compile(c)

  message('build index')
  t0 = now()
  indexPathNow = benchUtil.nameToIndexPath(index.getName())
  if os.path.exists(indexPathNow):
    print 'WARNING: removing leftover index at %s' % indexPathNow
    shutil.rmtree(indexPathNow)
  indexPathNow, fullLogFile = r.makeIndex('nightly', index)
  os.rename(fullLogFile, '%s/index.log' % runLogDir)

  message('done build index (%s)' % (now()-t0))
          
  indexPrev = benchUtil.Index(NIGHTLY_DIR + '.prev', 'wikimedium', 'StandardAnalyzer', 'Standard', INDEX_NUM_DOCS, constants.INDEX_NUM_THREADS, lineDocSource=LINE_FILE)
  indexPathPrev = '%s/trunk.nightly.index.prev' % constants.INDEX_DIR_BASE
                                                 
  if os.path.exists(indexPathPrev):
    segCountPrev = benchUtil.getSegmentCount(indexPrev)
    segCountNow = benchUtil.getSegmentCount(index)
    if segCountNow != segCountPrev:
      raise RuntimeError('segment counts differ: prev=%s now=%s' % (segCountPrev, segCountNow))

  # Search
  if DEBUG:
    countPerCat = 10
    repeatCount = 50
    jvmCount = 3
  else:
    countPerCat = 10
    repeatCount = 50
    jvmCount = 20

  randomSeed = 714

  message('search')
  t0 = now()

  coldRun = False
  resultsNow = r.runSimpleSearchBench(id, c, repeatCount, constants.SEARCH_NUM_THREADS, countPerCat, coldRun, randomSeed, jvmCount, filter=None)  
  message('done search (%s)' % (now()-t0))
  resultsPrev = []
  for fname in resultsNow:
    prevFName = fname + '.prev'
    if os.path.exists(prevFName):
      resultsPrev.append(prevFName)
    else:
      break
  else:
    r.simpleReport(resultsPrev,
                   resultsNow,
                   False, False,
                   'prev', 'now')
  for fname in resultsNow:
    shutil.copy(fname, fname + '.prev')
    shutil.move(fname, runLogDir)

  if os.path.exists(indexPathPrev):
    shutil.rmtree(indexPathPrev)
  # print 'rename %s to %s' % (indexPathNow, indexPathPrev)
  os.rename(indexPathNow, indexPathPrev)
  runCommand('bzip2 %s/index.log' % runLogDir)
  message('done: total time %s' % (now()-startTime))

if __name__ == '__main__':
  run()
