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

import random
import codecs
import socket
import sys
import time
import threading
import cPickle
import gc

# We don't create cyclic garbage, and we want no hiccups:
gc.disable()

MAX_BYTES = 70

# TODO
#   - generalize this to send requests via http too
#   - run hiccup thread here?
#   - sweep to find capacity...
#   - test PK lookup, NRT as well

# python -u perf/sendTasks.py /l/util/wikimedium500.tasks localhost 7777 10 10 10

count = 0
sumLatencyMS = 0
sumQueueTimeMS = 0

def gatherResponses(sent, s, results):
  global count, sumLatencyMS, sumQueueTimeMS
  
  while True:
    result = s.recv(14)
    taskID, queueTimeMS = result.split(':')
    taskID = int(taskID)
    queueTimeMS = float(queueTimeMS)
    endTime = time.time()
    try:
      startTime, taskString = sent[taskID]
    except KeyError:
      print 'WARNING: ignore bad return taskID=%s' % taskID
      continue
    del sent[taskID]
    latencyMS = (endTime-startTime)*1000
    results.append((startTime-globalStartTime, taskString, latencyMS, queueTimeMS))
    count += 1
    sumLatencyMS += latencyMS
    sumQueueTimeMS += queueTimeMS
    #print '  recv %s' % taskString

def pruneTasks(taskStrings, numTasksPerCat):
  byCat = {}
  for s in taskStrings:
    cat = s.split(':', 1)[0]
    if cat not in byCat:
      byCat[cat] = []
    l = byCat[cat]
    if len(l) < numTasksPerCat:
      l.append(s)

  prunedTasks = []
  for cat, l in byCat.items():
    # nocommit
    if cat != 'Term':
      continue
    prunedTasks.extend(l)

  return prunedTasks
  
def runTest():
  global globalStartTime
  
  tasksFile = sys.argv[1]
  serverHost = sys.argv[2]
  serverPort = int(sys.argv[3])
  meanQPS = float(sys.argv[4])
  numTasksPerCat = int(sys.argv[5])
  repeatCount = int(sys.argv[6])
  savFile = sys.argv[7]

  print 'Mean QPS %s' % meanQPS

  f = open(tasksFile, 'rb')
  taskStrings = []
  while True:
    l = f.readline()
    if l == '':
      break
    idx = l.find('#')
    if idx != -1:
      l = l[:idx]
    l = l.strip()
    if l == '':
      continue
    s = l
    if len(s) > MAX_BYTES:
      raise RuntimeError('task is > 50 bytes: %s' % l)
    s = s + ((MAX_BYTES-len(s))*' ')
    taskStrings.append(s)

  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.connect((serverHost, serverPort))

  r = random.Random(0)
  r.shuffle(taskStrings)

  taskStrings = pruneTasks(taskStrings, numTasksPerCat)

  taskStrings *= repeatCount
  r.shuffle(taskStrings)
  
  sent = {}
  results = []

  t = threading.Thread(target=gatherResponses,
                       args=(sent, s, results))
  t.setDaemon(True)
  t.start()

  globalStartTime = time.time()
  lastPrint = globalStartTime
  startTime = None
  for taskID, task in enumerate(taskStrings):
    #print task.strip()
    now = time.time()
    if now - lastPrint > 2.0 and count > 0:
      print '%.1f s: %5.1f%%: %.1f qps; %4.1f/%5.2f ms' % \
            (now - globalStartTime, 100.0*(1+taskID)/float(len(taskStrings)), count/(now-globalStartTime), sumLatencyMS/count, sumQueueTimeMS/count)
      lastPrint = now

    pause = r.expovariate(meanQPS)

    if startTime is not None:
      # Correct for any time taken in our "overhead" here...:
      pause = startTime + pause - time.time()

    if pause > 0:
      #print 'sent %s; sleep %.3f sec' % (origTask, pause)
      time.sleep(pause)
      
    startTime = time.time()
    if s.send(task) != len(task):
      raise RuntimeError('failed to send all bytes')
    sendTime = time.time()
    if sendTime - startTime > .001:
      print 'WARNING: took %.1f msec to send request' % (1000*(sendTime - startTime))
    #origTask = task
    sent[taskID] = (startTime, task)

  print '%8.1f sec: Done sending tasks...' % (time.time()-globalStartTime)
  while len(sent) != 0:
    time.sleep(0.1)
  print '%8.1f sec: Done...' % (time.time()-globalStartTime)

  results.sort()

  open(savFile, 'wb').write(cPickle.dumps(results))

  # printResults(results)

logPoints = ((50, 2),
             (75, 4),
             (90, 10),
             (95, 20),
             (97.5, 40),
             (99, 100),
             (99.9, 1000),
             (99.99, 10000),
             (99.999, 100000),
             (99.9999, 1000000))

def printResults(results):
  for startTime, taskString, latencyMS, queueTimeMS in results:
    print '%8.3f sec: latency %8.1f msec; queue msec %.1f; task %s' % (startTime, latencyMS, queueTimeMS, taskString)

def setRowCol(rows, row, pct, col, val):
  if len(rows) <= row:
    assert len(rows) == row
    rows.append(["'%g%%'" % pct])
  assert len(rows[row]) == 1+col, '%s vs %s; row=%s' % (len(rows[row]), 1+col, row)
  rows[row].append('%.1f' % val)
  #print '  now %s; row=%s' % (len(rows[row]), row)
  
def createGraph(fileNames):
  rows = []
  names = []
  col = 0
  for name, file in fileNames.items():
    results = cPickle.loads(open(file, 'rb').read())
    print '%s has %d results' % (file, len(results))
    # printResults(results)

    responseTimes = [x[2] for x in results]
    responseTimes.sort()

    setRowCol(rows, 0, 0.0, col, responseTimes[0])
    for row, (pct, minCount) in enumerate(logPoints):
      if len(responseTimes) < minCount:
        endRow = row+1
        break
      idx = int(((100.0-pct)/100.0)*len(responseTimes))
      # TODO: should we take linear blend of the two points...?  Else
      # we have a sparseness problem...
      setRowCol(rows, 1+row, pct, col, responseTimes[-idx-1])
      
    setRowCol(rows, endRow, 100.0, col, responseTimes[-1])
    col += 1
    names.append(name)
    
  chart = []
  w = chart.append
  w(chartHeader)
  l = ['Percentile'] + names
  w("          [%s],\n" % (','.join("'%s'" % x for x in l)))
  for row in rows:
    w('          [%s],\n' % (','.join(row)))
  w(chartFooter)
  return ''.join(chart)
  
chartHeader = '''
<html>
  <head>
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">
      google.load("visualization", "1", {packages:["corechart"]});
      google.setOnLoadCallback(drawChart);
      function drawChart() {
        var data = google.visualization.arrayToDataTable([
'''

chartFooter = '''        ]);

        var options = {
          title: 'Query Time',
          pointSize: 5,
          //legend: {position: 'none'},
          hAxis: {title: 'Percentile', format: '# %'},
          vAxis: {title: 'Query time (msec)'},
        };

        var chart = new google.visualization.LineChart(document.getElementById('chart_div'));
        chart.draw(data, options);
      }
    </script>
  </head>
  <body>
    <div id="chart_div" style="width: 1000px; height: 500px;"></div>
  </body>
</html>
'''

# silly emacs: '

if __name__ == '__main__':
  runTest()
  if True:
    html = createGraph({
      'Results': 'results.pk',
      })
  if False:
    html = createGraph({
      'Oops': 'oops.pk',
      'No oops': 'nooops.pk',
      })
  if False:
    html = createGraph({'100 QPS': 'results100qps.pk',
                        '150 QPS': 'results150qps.pk'})
    
  open('/x/tmp4/out2.html', 'wb').write(html)

  
