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

# TODO
#   - crontab to launch 11 pm
#   - hmm put graphs all on one page...?
#   - cutover to new SSD
#   - make sure this finishes before 3:30 am (backups)
#   - on daily html show graph
#   - send email if things fail
#   - maybe multiple queries on one graph...?
#   - be able to regen all HTML
#   - measure index size?
#   - checkout separate util.nightly and run from there
#   - after 1 night, verify against prior night -- catch regressions

DEBUG = False

if DEBUG:
  INDEX_NUM_DOCS = 1000000
else:
  INDEX_NUM_DOCS = 27625038

LINE_FILE = '/p/lucene/data/enwiki-20110115-lines-1k-fixed.txt'
NIGHTLY_LOG_DIR = '/lucene/logs.nightly'
NIGHTLY_REPORTS_DIR = '/lucene/reports.nightly'
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

  indexTime = (now()-t0)
  message('done build index (%s)' % indexTime)

  indexPathPrev = '%s/trunk.nightly.index.prev' % constants.INDEX_DIR_BASE
                                                 
  if os.path.exists(indexPathPrev) and not DO_RESET:
    segCountPrev = benchUtil.getSegmentCount(indexPathPrev)
    segCountNow = benchUtil.getSegmentCount(benchUtil.nameToIndexPath(index.getName()))
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
      w('<b>Indexing time</b>: %s\n' % indexTime)
      w('<br><br><b>Search perf vs day before</b>\n')
      w(''.join(output))
      w('<br><br>')
      w('<img src="%s.png"/>\n' % timeStamp)
      w('</html>\n')
      f.close()

      shutil.move('out.png', '%s/%s.png' % (NIGHTLY_REPORTS_DIR, timeStamp))

      indexTimeSec = indexTime.days * 86400 + indexTime.seconds + indexTime.microseconds/1000000.          
      dailyResults = start, indexTimeSec, results, INDEX_NUM_DOCS
      open('%s/results.pk' % runLogDir, 'wb').write(cPickle.dumps(dailyResults))

  for fname in resultsNow:
    shutil.copy(fname, fname + '.prev')
    shutil.move(fname, runLogDir)

  if os.path.exists(indexPathPrev):
    shutil.rmtree(indexPathPrev)
  # print 'rename %s to %s' % (indexPathNow, indexPathPrev)
  os.rename(indexPathNow, indexPathPrev)
  runCommand('bzip2 %s/index.log' % runLogDir)
  message('done: total time %s' % (now()-start))

def makeGraphs():
  indexChartData = ['Date,Docs/Sec']
  searchChartData = {}
  days = []
  for subDir in os.listdir(NIGHTLY_LOG_DIR):
    resultsFile = '%s/%s/results.pk' % (NIGHTLY_LOG_DIR, subDir)
    if os.path.exists(resultsFile):
      timeStamp, indexTime, searchResults, numDocs = cPickle.loads(open(resultsFile).read())
      days.append(timeStamp)
      timeStampString = '%04d-%02d-%02d %02d:%02d:%02d' % \
                        (timeStamp.year,
                         timeStamp.month,
                         timeStamp.day,
                         timeStamp.hour,
                         timeStamp.minute,
                         int(timeStamp.second))
      indexChartData.append('%s,%.1f' % (timeStampString, float(numDocs)/indexTime))
      for cat, (minQPS, maxQPS, avgQPS, stdDevQPS) in searchResults.items():
        if cat not in searchChartData:
          searchChartData[cat] = ['Date,QPS']
        searchChartData[cat].append('%s,%.3f,%.3f' % (timeStampString, avgQPS, stdDevQPS))

  sort(indexChartData)
  for k, v in searchChartData.items():
    sort(v)

  # Index time
  writeOneGraphHTML('Indexing throughput (docs/sec)',
                    '%s/indexing.html' % NIGHTLY_REPORTS_DIR,
                    getOneGraphHTML('IndexTime', indexChartData, errorBars=False))

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

