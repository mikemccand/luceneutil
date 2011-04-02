#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import cPickle
import traceback
import time
import datetime
import os
import sys
import shutil
import smtplib
import re

# local imports:
import benchUtil
import constants
import localpass
import stats

"""
This script runs certain benchmarks, once per day, and generates graphs so we can see performance over time:

  * Index all of wikipedia ~ 1 KB docs w/ 512 MB ram buffer

  * Run NRT perf test on this index for 30 minutes (we only plot mean/stddev reopen time)
  
  * Index all of wikipedia actual (~4 KB) docs w/ 512 MB ram buffer

  * Index all of wikipedia ~ 1 KB docs, flushing by specific doc count to get 5 segs per level

  * Run search test
"""

# TODO
#   - make blog post -- full disclosure; link to it from the pages
#   - nrt
#     - chart all reopen times by time...?
#     - chart over-time mean/stddev reopen time
#   - add annotations, over time, when Lucene fixes/breaks stuff
#   - maybe tiny docs vs big docs (not medium...)?
#   - break out commit vs indexing vs merging time?
#   - also run NRT perf test (so deletions are exercised)
#   - hmm put graphs all on one page...?
#   - cutover to new SSD
#   - make sure this finishes before 3:30 am (backups)
#   - maybe multiple queries on one graph...?
#   - measure index size?

DEBUG = '-debug' in sys.argv

DIR_IMPL = 'MMapDirectory'

MEDIUM_INDEX_NUM_DOCS = 27625038
BIG_INDEX_NUM_DOCS = 5982049
INDEXING_RAM_BUFFER_MB = 512

#MED_WIKI_BYTES_PER_DOC = 950.21921304868431
#BIG_WIKI_BYTES_PER_DOC = 4183.3843150398807

MEDIUM_LINE_FILE = '/lucene/data/enwiki-20110115-lines-1k-fixed.txt'
BIG_LINE_FILE = '/lucene/data/enwiki-20110115-lines.txt'
NIGHTLY_LOG_DIR = '/lucene/logs.nightly'
NIGHTLY_REPORTS_DIR = '/lucene/reports.nightly'
NIGHTLY_DIR = 'trunk.nightly'

NRT_DOCS_PER_SECOND = 1000
NRT_RUN_TIME = 30*60
NRT_SEARCH_THREADS = 4
NRT_INDEX_THREADS = 1
NRT_REOPENS_PER_SEC = 1

if DEBUG:
  MEDIUM_INDEX_NUM_DOCS /= 20
  BIG_INDEX_NUM_DOCS /= 20
  NRT_RUN_TIME /= 20

reBytesIndexed = re.compile('^Indexer: net bytes indexed (.*)$', re.MULTILINE)
reIndexingTime = re.compile(r'^Indexer: finished \((.*) msec\)$', re.MULTILINE)

def now():
  return datetime.datetime.now()

def toSeconds(td):
  return td.days * 86400 + td.seconds + td.microseconds/1000000.

def message(s):
 print '[%s] %s' % (now(), s)

def runCommand(command):
  message('RUN: %s' % command)
  t0 = time.time()
  if os.system(command):
    message('  FAILED')
    raise RuntimeError('command failed: %s' % command)
  message('  took %.1f sec' % (time.time()-t0))

def buildIndex(r, runLogDir, desc, index, logFile):
  message('build %s' % desc)
  #t0 = now()
  indexPath = benchUtil.nameToIndexPath(index.getName())
  if os.path.exists(indexPath):
    shutil.rmtree(indexPath)
  indexPath, fullLogFile = r.makeIndex('nightly', index)
  #indexTime = (now()-t0)
  
  s = open(fullLogFile).read()
  bytesIndexed = int(reBytesIndexed.search(s).group(1))

  indexTimeSec = int(reIndexingTime.search(s).group(1))/1000.0
  os.rename(fullLogFile, '%s/%s' % (runLogDir, logFile))
  message('  took %.1f sec' % indexTimeSec)

  # run checkIndex
  checkLogFileName = '%s/checkIndex.%s' % (runLogDir, logFile)
  checkIndex(r, indexPath, checkLogFileName)

  return indexPath, indexTimeSec, bytesIndexed

def checkIndex(r, indexPath, checkLogFileName):
  message('run CheckIndex')
  cmd = '%s -classpath "%s" -ea org.apache.lucene.index.CheckIndex "%s" > %s 2>&1' % \
        (constants.JAVA_COMMAND,
         r.classPathToString(r.getClassPath(NIGHTLY_DIR)),
         indexPath,
         checkLogFileName)
  runCommand(cmd)

reNRTReopenTime = re.compile('^Reopen: +([0-9.]+) msec$', re.MULTILINE)

def runNRTTest(r, indexPath, runLogDir):

  cmd = '%s -classpath "%s" perf.NRTPerfTest %s "%s" multi "%s" 17 %s %s %s %s %s update 5 yes' % \
        (constants.JAVA_COMMAND,
         r.classPathToString(r.getClassPath(NIGHTLY_DIR)),
         DIR_IMPL,
         indexPath,
         MEDIUM_LINE_FILE,
         NRT_DOCS_PER_SECOND,
         NRT_RUN_TIME,
         NRT_SEARCH_THREADS,
         NRT_INDEX_THREADS,
         NRT_REOPENS_PER_SEC)

  logFile = '%s/nrt.log' % runLogDir
  cmd += '> %s 2>&1' % logFile
  runCommand(cmd)

  checkIndex(r, indexPath, 'checkIndex.nrt.log')
  
  times = []
  for s in reNRTReopenTime.findall(open(logFile, 'rb').read()):
    times.append(float(s))

  # Discard first 10 (JVM warmup)
  times = times[10:]

  # Discard worst 2%
  times.sort()
  numDrop = len(times)/50
  if numDrop > 0:
    message('drop: %s' % ' '.join(['%.1f' % x for x in times[-numDrop:]]))
    times = times[:-numDrop]
  message('times: %s' % ' '.join(['%.1f' % x for x in times]))

  min, max, mean, stdDev = stats.getStats(times)
  message('NRT reopen time (msec) mean=%.4f stdDev=%.4f' % (mean, stdDev))
  
  return mean, stdDev

def run():

  DO_RESET = '-reset' in sys.argv

  start = now()
  print
  print
  print
  print
  message('start')
  id = 'nightly'
  timeStamp = '%04d.%02d.%02d.%02d.%02d.%02d' % (start.year, start.month, start.day, start.hour, start.minute, start.second)
  runLogDir = '%s/%s' % (NIGHTLY_LOG_DIR, timeStamp)
  os.makedirs(runLogDir)
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

  fastIndexMedium = benchUtil.Index(NIGHTLY_DIR, 'wikimedium', 'StandardAnalyzer', 'Standard', MEDIUM_INDEX_NUM_DOCS, constants.INDEX_NUM_THREADS, lineDocSource=MEDIUM_LINE_FILE, ramBufferMB=INDEXING_RAM_BUFFER_MB, dirImpl=DIR_IMPL)
  fastIndexMedium.setVerbose(False)
  fastIndexMedium.waitForMerges = False
  fastIndexMedium.printDPS = 'no'
  fastIndexMedium.mergePolicy = 'LogByteSizeMergePolicy'

  fastIndexBig = benchUtil.Index(NIGHTLY_DIR, 'wikimedium', 'StandardAnalyzer', 'Standard', BIG_INDEX_NUM_DOCS, constants.INDEX_NUM_THREADS, lineDocSource=BIG_LINE_FILE, ramBufferMB=INDEXING_RAM_BUFFER_MB, dirImpl=DIR_IMPL)
  fastIndexBig.setVerbose(False)
  fastIndexBig.waitForMerges = False
  fastIndexBig.printDPS = 'no'
  fastIndexBig.mergePolicy = 'LogByteSizeMergePolicy'
  
  index = benchUtil.Index(NIGHTLY_DIR, 'wikimedium', 'StandardAnalyzer', 'Standard', MEDIUM_INDEX_NUM_DOCS, constants.INDEX_NUM_THREADS, lineDocSource=MEDIUM_LINE_FILE)
  index.printDPS = 'no'

  c = benchUtil.Competitor(id, 'trunk.nightly', index, DIR_IMPL, 'StandardAnalyzer', 'multi', constants.WIKI_MEDIUM_TASKS_FILE)
  r.compile(c)

  # 1: test indexing speed: small (~ 1KB) sized docs, flush-by-ram
  medIndexPath, medIndexTime, medBytesIndexed = buildIndex(r, runLogDir, 'medium index (fast)', fastIndexMedium, 'fastIndexMediumDocs.log')

  # 2: NRT test
  nrtResults = runNRTTest(r, medIndexPath, runLogDir)

  # 3: test indexing speed: medium (~ 4KB) sized docs, flush-by-ram
  ign, bigIndexTime, bigBytesIndexed = buildIndex(r, runLogDir, 'big index (fast)', fastIndexBig, 'fastIndexBigDocs.log')

  # 4: test searching speed; first build index, flushed by doc count (so we get same index structure night to night)
  indexPathNow, ign, ign = buildIndex(r, runLogDir, 'search index (fixed segments)', index, 'fixedIndex.log')

  indexPathPrev = '%s/trunk.nightly.index.prev' % constants.INDEX_DIR_BASE
                                                 
  if os.path.exists(indexPathPrev) and not DO_RESET:
    segCountPrev = benchUtil.getSegmentCount(indexPathPrev)
    segCountNow = benchUtil.getSegmentCount(benchUtil.nameToIndexPath(index.getName()))
    if segCountNow != segCountPrev:
      raise RuntimeError('segment counts differ: prev=%s now=%s' % (segCountPrev, segCountNow))

  # Search
  if DEBUG:
    countPerCat = 5
    repeatCount = 50
    jvmCount = 3
  else:
    countPerCat = 5
    repeatCount = 50
    jvmCount = 20

  randomSeed = 714

  message('search')
  t0 = now()

  coldRun = False
  resultsNow = r.runSimpleSearchBench(id, c, repeatCount, constants.SEARCH_NUM_THREADS, countPerCat, coldRun, randomSeed, jvmCount, filter=None)  
  message('done search (%s)' % (now()-t0))
  resultsPrev = []

  searchResults = None
  
  for fname in resultsNow:
    prevFName = fname + '.prev'
    if os.path.exists(prevFName):
      resultsPrev.append(prevFName)
    else:
      break
  else:
    if not DO_RESET:
      output = []
      results = r.simpleReport(resultsPrev,
                               resultsNow,
                               False, True,
                               'prev', 'now',
                               writer=output.append)
      f = open('%s/%s.html' % (NIGHTLY_REPORTS_DIR, timeStamp), 'wb')
      timeStamp2 = '%s %02d/%02d/%04d' % (start.strftime('%a'), start.day, start.month, start.year)
      w = f.write
      w('<html>\n')
      w('<h1>%s</h1>' % timeStamp2)
      w('<br><br><b>Search perf vs day before</b>\n')
      w(''.join(output))
      w('<br><br>')
      w('<img src="%s.png"/>\n' % timeStamp)
      w('</html>\n')
      f.close()

      shutil.move('out.png', '%s/%s.png' % (NIGHTLY_REPORTS_DIR, timeStamp))
      searchResults = results

  for fname in resultsNow:
    shutil.copy(fname, fname + '.prev')
    shutil.move(fname, runLogDir)

  if os.path.exists(indexPathPrev):
    shutil.rmtree(indexPathPrev)
  # print 'rename %s to %s' % (indexPathNow, indexPathPrev)
  os.rename(indexPathNow, indexPathPrev)
  os.chdir(runLogDir)
  runCommand('tar cjf logs.tar.bz2 *')
  for f in os.listdir(runLogDir):
    if f != 'logs.tar.bz2':
      os.remove(f)
  results = (start,
             MEDIUM_INDEX_NUM_DOCS, medIndexTime, medBytesIndexed,
             BIG_INDEX_NUM_DOCS, bigIndexTime, bigBytesIndexed,
             nrtResults,
             searchResults)
  open('results.pk', 'wb').write(cPickle.dumps(results))
  message('done: total time %s' % (now()-start))

def makeGraphs():
  medIndexChartData = ['Date,GB/hour']
  bigIndexChartData = ['Date,GB/hour']
  nrtChartData = ['Date,Reopen Time (msec)']
  searchChartData = {}
  days = []
  for subDir in os.listdir(NIGHTLY_LOG_DIR):
    resultsFile = '%s/%s/results.pk' % (NIGHTLY_LOG_DIR, subDir)
    if os.path.exists(resultsFile):
      timeStamp, \
                 medNumDocs, medIndexTimeSec, medBytesIndexed, \
                 bigNumDocs, bigIndexTimeSec, bigBytesIndexed, \
                 nrtResults, searchResults = cPickle.loads(open(resultsFile).read())
      timeStampString = '%04d-%02d-%02d %02d:%02d:%02d' % \
                        (timeStamp.year,
                         timeStamp.month,
                         timeStamp.day,
                         timeStamp.hour,
                         timeStamp.minute,
                         int(timeStamp.second))
      medIndexChartData.append('%s,%.1f' % (timeStampString, (medBytesIndexed / (1024*1024*1024.))/(medIndexTimeSec/3600.)))
      bigIndexChartData.append('%s,%.1f' % (timeStampString, (bigBytesIndexed / (1024*1024*1024.))/(bigIndexTimeSec/3600.)))
      mean, stdDev = nrtResults
      nrtChartData.append('%s,%.3f,%.2f' % (timeStampString, mean, stdDev))
      if searchResults is not None:
        days.append(timeStamp)
        for cat, (minQPS, maxQPS, avgQPS, stdDevQPS) in searchResults.items():
          if cat not in searchChartData:
            searchChartData[cat] = ['Date,QPS']
          searchChartData[cat].append('%s,%.3f,%.3f' % (timeStampString, avgQPS, stdDevQPS))

  sort(medIndexChartData)
  sort(bigIndexChartData)
  for k, v in searchChartData.items():
    sort(v)

  # Index time
  writeIndexingHTML(medIndexChartData, bigIndexChartData)

  # NRT
  writeNRTHTML(nrtChartData)

  for k, v in searchChartData.items():
    writeOneGraphHTML('%s QPS' % k,
                      '%s/%s.html' % (NIGHTLY_REPORTS_DIR, k),
                      getOneGraphHTML(k, v, "Queries/sec", k, errorBars=True))

  writeIndexHTML(searchChartData, days)
  runCommand('scp -rp /lucene/reports.nightly mike@10.17.4.9:/usr/local/apache2/htdocs')
  
def writeIndexHTML(searchChartData, days):
  f = open('%s/index.html' % NIGHTLY_REPORTS_DIR, 'wb')
  w = f.write
  w('<html>')
  w('<h1>Lucene nightly benchmarks</h1>')
  w('<br><a href="indexing.html">Indexing performance</a>')
  w('<br><a href="nrt.html">Near-real-time performance</a>')
  w('<br><br>')
  w('<b>Queries</b>:')
  l = searchChartData.keys()
  l.sort()
  for s in l:
    w('<br>&nbsp;&nbsp;<a href="%s.html">%s</a>' % \
      (htmlEscape(s), htmlEscape(s)))
  w('<br><br>')
  w('<b>By day</b>:')
  days.sort()
  for t in days:
    timeStamp = '%04d.%02d.%02d.%02d.%02d.%02d' % (t.year, t.month, t.day, t.hour, t.minute, t.second)
    timeStamp2 = '%s %02d/%02d/%04d' % (t.strftime('%a'), t.day, t.month, t.year)
    w('<br>&nbsp;&nbsp;<a href="%s.html">%s</a>' % (timeStamp, timeStamp2))
  w('<br><br>')
  w('<em>[last updated: %s]</em>' % now())
  w('</html>')

def htmlEscape(s):
  return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def sort(l):
  x = l[0]
  del l[0]
  l.sort()
  l.insert(0, x)
  return l

def writeOneGraphHTML(title, fileName, chartHTML):
  f = open(fileName, 'wb')
  w = f.write
  w('<html>\n')
  w('<head>\n')
  w('<script type="text/javascript" src="dygraph-combined.js"></script>\n')
  w('</head>\n')
  w('<body>\n')
  w('<h1>%s</h1>\n' % htmlEscape(title))
  w(chartHTML)
  w('\n')
  footer(w)
  w('</body>\n')
  w('</html>\n')
  f.close()

def writeIndexingHTML(medChartData, bigChartData):
  f = open('%s/indexing.html' % NIGHTLY_REPORTS_DIR, 'wb')
  w = f.write
  w('<html>\n')
  w('<head>\n')
  w('<script type="text/javascript" src="dygraph-combined.js"></script>\n')
  w('</head>\n')
  w('<body>\n')
  w('<h1>Indexing</h1>\n')
  w('<br>')
  w(getOneGraphHTML('MedIndexTime', medChartData, "Plain text GB/hour", "~1 KB docs", errorBars=False))

  w('<br>')
  w('<br>')
  w('<br>')
  w(getOneGraphHTML('BigIndexTime', bigChartData, "Plain text GB/hour", "~4 KB docs", errorBars=False))
  w('\n')
  w('<b>Notes</b>:\n')
  w('<ul>\n')
  w('  <li> Test does not wait for merges on close (calls <tt>IW.close(false)</tt>)')
  w('  <li> Documents created from 1/15/2011 <a href="http://en.wikipedia.org/wiki/Wikipedia:Database_download">Wikipedia English XML export</a>\n')
  w('  <li> %d indexing threads\n' % constants.INDEX_NUM_THREADS)
  w('  <li> Flush at %s MB\n' % INDEXING_RAM_BUFFER_MB)
  w('  <li> OS: %s\n' % htmlEscape(os.popen('uname -a 2>&1').read().strip()))
  w('  <li> Java command-line: %s\n' % constants.JAVA_COMMAND)
  w('  <li> Java version: %s\n' % htmlEscape(os.popen('java -version 2>&1').read().strip()))
  w('  <li> 2 Xeon X5680, overclocked @ 4.0 Ghz (total 24 cores 2 CPU * 6 core * 2 hyperthreads)\n')
  w('</ul>')
  footer(w)
  w('</body>\n')
  w('</html>\n')
  f.close()

def writeNRTHTML(nrtChartData):
  f = open('%s/nrt.html' % NIGHTLY_REPORTS_DIR, 'wb')
  w = f.write
  w('<html>\n')
  w('<head>\n')
  w('<script type="text/javascript" src="dygraph-combined.js"></script>\n')
  w('</head>\n')
  w('<body>\n')
  w('<h1>Near-real-time reader reopen time (msec)</h1>\n')
  w('<br>')
  w(getOneGraphHTML('NRT', nrtChartData, "Milliseconds", "Near-real-time reopen time", errorBars=True))
  footer(w)
  w('</body>\n')
  w('</html>\n')

def footer(w):
  w('<br><em>[last updated: %s; send questions to <a href="mailto:lucene@mikemccandless.com">Mike McCandless</a>]</em>' % now())

def getOneGraphHTML(id, data, yLabel, title, errorBars=True):
  l = []
  w = l.append
  w('<table><tr><td><b>%s</b></td><td>' % htmlEscape(yLabel))
  w('<center><b>%s</b></center><br>' % htmlEscape(title))
  w('<div id="%s" style="width:600px;height:300px"></div>' % id)
  w('</td></tr></table>')
  w('<script type="text/javascript">')
  w('  g_%s = new Dygraph(' % id)
  w('    document.getElementById("%s"),' % id)
  w('    "%s.txt",' % id)
  if errorBars:
    w('    {errorBars: true}')
  else:
    w('    {}')
  w('  );')
  w('</script>')

  f = open('%s/%s.txt' % (NIGHTLY_REPORTS_DIR, id), 'wb')
  for s in data:
    f.write('%s\n' % s)
  f.close()
  return '\n'.join(l)

def sendEmail(emailAddress, message):
  SMTP_SERVER = localpass.SMTP_SERVER
  SMTP_PORT = localpass.SMTP_PORT
  FROM_EMAIL = 'admin@mikemccandless.com'
  smtp = smtplib.SMTP(SMTP_SERVER, port=SMTP_PORT)
  smtp.ehlo(FROM_EMAIL)
  smtp.starttls()
  smtp.ehlo(FROM_EMAIL)
  localpass.smtplogin(smtp)
  smtp.sendmail(FROM_EMAIL, emailAddress.split(','), message)
  smtp.quit()

if __name__ == '__main__':
  try:
    if '-run' in sys.argv:
      run()
    makeGraphs()
  except:
    traceback.print_exc()
    if not DEBUG:
      emailAddr = 'mail@mikemccandless.com'
      message = 'From: %s\r\n' % localpass.FROM_EMAIL
      message += 'To: %s\r\n' % emailAddr
      message += 'Subject: Nightly Lucene bench FAILED!!\r\n'
      sendEmail(emailAddr, message)
    
# scp -rp /lucene/reports.nightly mike@10.17.4.9:/usr/local/apache2/htdocs

# TO CLEAN
#   - rm -rf /p/lucene/indices/trunk.nightly.index.prev/
#   - rm -rf /lucene/logs.nightly/*
#   - rm -rf /lucene/reports.nightly/*
#   - rm -f /lucene/trunk.nightly/modules/benchmark/*.x
