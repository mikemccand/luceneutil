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
import benchUtil
import constants
import smtplib
import localpass
import re

# TODO
#   - maybe do not wait for merges / close(false) for fast indexing test?
#   - break out commit vs indexing vs merging time?
#   - do indexing rate as MB/sec
#     - maybe build BIG docs too...?
#   - also run NRT perf test
#   - hmm put graphs all on one page...?
#   - cutover to new SSD
#   - make sure this finishes before 3:30 am (backups)
#   - maybe multiple queries on one graph...?
#   - measure index size?

DEBUG = False

MEDIUM_INDEX_NUM_DOCS = 27625038
BIG_INDEX_NUM_DOCS = 5982049

if DEBUG:
  MEDIUM_INDEX_NUM_DOCS /= 20
  BIG_INDEX_NUM_DOCS /= 20

#MED_WIKI_BYTES_PER_DOC = 950.21921304868431
#BIG_WIKI_BYTES_PER_DOC = 4183.3843150398807

MEDIUM_LINE_FILE = '/lucene/data/enwiki-20110115-lines-1k-fixed.txt'
BIG_LINE_FILE = '/lucene/data/enwiki-20110115-lines.txt'
NIGHTLY_LOG_DIR = '/lucene/logs.nightly'
NIGHTLY_REPORTS_DIR = '/lucene/reports.nightly'
NIGHTLY_DIR = 'trunk.nightly'

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
  message('done: %s' % indexTimeSec)

  return indexPath, indexTimeSec, bytesIndexed

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

  # First test: med docs, full indexing @ 256 MB RAM buffer
  fastIndexMedium = benchUtil.Index(NIGHTLY_DIR, 'wikimedium', 'StandardAnalyzer', 'Standard', MEDIUM_INDEX_NUM_DOCS, constants.INDEX_NUM_THREADS, lineDocSource=MEDIUM_LINE_FILE, ramBufferMB=256)
  fastIndexMedium.setVerbose(False)
  fastIndexMedium.waitForMerges = False
  fastIndexMedium.printDPS = 'no'

  # Second test: big docs, full indexing @ 256 MB RAM buffer
  fastIndexBig = benchUtil.Index(NIGHTLY_DIR, 'wikimedium', 'StandardAnalyzer', 'Standard', BIG_INDEX_NUM_DOCS, constants.INDEX_NUM_THREADS, lineDocSource=BIG_LINE_FILE, ramBufferMB=256)
  fastIndexBig.setVerbose(False)
  fastIndexBig.waitForMerges = False
  fastIndexBig.printDPS = 'no'
  
  # 3rd test: build index, flushed by doc count (so we get same index structure night to night)
  index = benchUtil.Index(NIGHTLY_DIR, 'wikimedium', 'StandardAnalyzer', 'Standard', MEDIUM_INDEX_NUM_DOCS, constants.INDEX_NUM_THREADS, lineDocSource=MEDIUM_LINE_FILE)
  index.printDPS = 'no'

  c = benchUtil.Competitor(id, 'trunk.nightly', index, 'MMapDirectory', 'StandardAnalyzer', 'multi', constants.WIKI_MEDIUM_TASKS_FILE)
  r.compile(c)

  ign, medIndexTime, medBytesIndexed = buildIndex(r, runLogDir, 'medium index (fast)', fastIndexMedium, 'fastIndexMediumDocs.log')
  ign, bigIndexTime, bigBytesIndexed = buildIndex(r, runLogDir, 'big index (fast)', fastIndexBig, 'fastIndexBigDocs.log')
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
  resultsPK = None
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

      dailyResults = (start,
                      MEDIUM_INDEX_NUM_DOCS, medIndexTime, medBytesIndexed,
                      BIG_INDEX_NUM_DOCS, bigIndexTime, bigBytesIndexed,
                      results)
      resultsPK = cPickle.dumps(dailyResults)

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
  if resultsPK is not None:
    open('results.pk', 'wb').write(resultsPK)
  message('done: total time %s' % (now()-start))

def makeGraphs():
  medIndexChartData = ['Date,GB/hour']
  bigIndexChartData = ['Date,GB/hour']
  searchChartData = {}
  days = []
  for subDir in os.listdir(NIGHTLY_LOG_DIR):
    resultsFile = '%s/%s/results.pk' % (NIGHTLY_LOG_DIR, subDir)
    if os.path.exists(resultsFile):
      timeStamp, medNumDocs, medIndexTimeSec, medBytesIndexed, bigNumDocs, bigIndexTimeSec, bigBytesIndexed, searchResults = cPickle.loads(open(resultsFile).read())
      days.append(timeStamp)
      timeStampString = '%04d-%02d-%02d %02d:%02d:%02d' % \
                        (timeStamp.year,
                         timeStamp.month,
                         timeStamp.day,
                         timeStamp.hour,
                         timeStamp.minute,
                         int(timeStamp.second))
      medIndexChartData.append('%s,%.1f' % (timeStampString, (medBytesIndexed / (1024*1024*1024.))/(medIndexTimeSec/3600.)))
      bigIndexChartData.append('%s,%.1f' % (timeStampString, (bigBytesIndexed / (1024*1024*1024.))/(bigIndexTimeSec/3600.)))
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

  for k, v in searchChartData.items():
    writeOneGraphHTML('%s QPS' % k,
                      '%s/%s.html' % (NIGHTLY_REPORTS_DIR, k),
                      getOneGraphHTML(k, v, errorBars=True))

  writeIndexHTML(searchChartData, days)
  runCommand('scp -rp /lucene/reports.nightly mike@10.17.4.9:/usr/local/apache2/htdocs')
  
def writeIndexHTML(searchChartData, days):
  f = open('%s/index.html' % NIGHTLY_REPORTS_DIR, 'wb')
  w = f.write
  w('<html>')
  w('<h1>Lucene nightly performance tests</h1>')
  w('<br><a href="indexing.html">Indexing performance</a>')
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
  w('<h1>Ingest rate (GB/hour)</h1>\n')
  w('<br><br><b>Small (~1 KB doc) Wikipedia English docs</b>:')
  w('<br>')
  w(getOneGraphHTML('MedIndexTime', medChartData, errorBars=False))

  w('<br><br><b>Medium (~4 KB doc) Wikipedia English docs</b>:')
  w('<br>')
  w(getOneGraphHTML('BigIndexTime', bigChartData, errorBars=False))
  w('\n')
  w('<b>Notes</b>:\n')
  w('<ul>\n')
  w('  <li> Do not wait for merges on close')
  w('  <li> Documents created from <a href="http://en.wikipedia.org/wiki/Wikipedia:Database_download">Wikipedia English XML export</a> from 01/15/2011\n')
  w('  <li> OS: %s\n' % htmlEscape(os.popen('uname -a 2>&1').read().strip()))
  w('  <li> Java command-line: %s\n' % constants.JAVA_COMMAND)
  w('  <li> Java version: %s\n' % htmlEscape(os.popen('java -version 2>&1').read().strip()))
  w('  <li> 24 cores (2 CPU * 6 core * 2 hyperthreads)\n')
  w('  <li> %d indexing threads\n' % constants.INDEX_NUM_THREADS)
  w('  <li> 256 MB RAM buffer\n')
  w('</ul>')
  w('<em>[last updated: %s]</em>' % now())
  w('</body>\n')
  w('</html>\n')
  f.close()

def getOneGraphHTML(id, data, errorBars=True):
  l = []
  w = l.append
  w('<div id="%s" style="width:600px;height:300px"></div>' % id)
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
