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
import sys
import re
import struct
import os

logPoints = ((50, 2),
             (75, 4),
             (90, 10),
             (95, 20),
             (97.5, 40),
             (99, 100),
             (99.9, 1000),
             (99.99, 10000),
             (99.999, 100000),
             (99.9999, 1000000),
             (99.99999, 10000000))

def cleanName(name):
  name = name.replace('Directory', '')
  name = name.replace('Oracle', '')
  name = name.replace('.Lucene40', '')
  name = name.replace('.RAM.Direct', '.Direct')
  return name

def setRowCol(rows, row, pct, col, val):
  if len(rows) <= row:
    assert len(rows) == row
    rows.append(["'%g%%'" % pct])
  
  while len(rows[row]) < 1+col:
    rows[row].append(rows[row-1][col])
  rows[row].append('%.1f' % val)
  #print '  now %s; row=%s' % (len(rows[row]), row)

resultsCache = {}

def loadResults(file, sort=True):
  if file not in resultsCache:
    if file.endswith('.pk'):
      results = cPickle.loads(open(file, 'rb').read())
    else:
      f = open(file, 'rb')
      results = []
      netHitCount = 0
      while True:
        b = f.read(17)
        if len(b) == 0:
          break
        timestamp, latencyMS, queueTimeMS, totalHitCount, taskLen = struct.unpack('fffIB', b)
        taskString = f.read(taskLen)
        results.append((timestamp, taskString, latencyMS, queueTimeMS))
        netHitCount += totalHitCount
      f.close()
      results.sort()

    # Find time when test finally finished:
    endTime = None
    for tup in results:
      endTime = max(tup[0] + tup[2]/1000.0, endTime)

    actualQPS = len(results) / endTime
      
    print '%s: actualQPS=%s endTime=%s len(results)=%d netHitCount=%d' % (file, actualQPS, endTime, len(results), netHitCount)
    
    resultsCache[file] = results, actualQPS
    
  return resultsCache[file]
  
def createGraph(fileNames, warmupSec):
  cols = []
  names = []
  maxRows = 0

  reQPS = re.compile(r'\.qps(\d+)$')

  qps = int(reQPS.search(fileNames[0][0]).group(1))

  for name, file in fileNames:
    results, actualQPS = loadResults(file, sort=False)

    # Discard first warmupSec seconds:
    upto = 0
    while results[upto][0] < warmupSec:
      upto += 1

    print '%s has %d results (less %d = first %.1f seconds warmup); %.1f sec' % \
          (file, len(results), upto, warmupSec, results[-1][0]-warmupSec)
    results = results[upto:]

    responseTimes = [x[2] for x in results]
    responseTimes.sort()

    if responseTimes[-1] > 100000:
      print '  discard %s: max responseTime=%s' % (name, responseTimes[-1])
      continue

    col = []
    cols.append(col)
    col.append(responseTimes[0])
    didMax = False
    for row, (pct, minCount) in enumerate(logPoints):
      if len(responseTimes) < minCount:
        break
      idx = int(((100.0-pct)/100.0)*len(responseTimes))
      # TODO: should we take linear blend of the two points...?  Else
      # we have a sparseness problem...
      col.append(responseTimes[-idx-1])
      if idx == 0:
        didMax = True

    # Max:
    if not didMax:
      #print 'max %s vs %s' % (responseTimes[-1], col[-1])
      col.append(responseTimes[-1])
    print '%s has %d rows' % (name, len(col))
    print col

    maxRows = max(maxRows, len(col))

    i = name.find('.qps')
    if i != -1:
      name = name[:i]
    names.append(name)
    
  chart = []
  w = chart.append
  w(chartHeader)

  l = ['Percentile'] + names
  w("          [%s],\n" % (','.join("'%s'" % cleanName(x) for x in l)))
  rowID = 0
  while True:
    row = []
    for col in cols:
      if len(col) > rowID:
        row.append('%.1f' % col[rowID])
      else:
        row.append('%.1f' % col[-1])

    if rowID == maxRows-1:
      label = '100%'
    elif rowID == 0:
      label = '0%'
    else:
      #print 'rowID %s vs %s, %s' % (rowID, len(logPoints), maxRows)
      label = '%g' % logPoints[rowID-1][0]

    w('          ["%s",%s],\n' % (label, ','.join(row)))

    rowID += 1

    if rowID == maxRows:
      break

        
  w(chartFooter.replace('$QPS$', str(qps)))
  
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
          title: 'Query Time @ $QPS$ qps',
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
    <div id="chart_div" style="width: 1200px; height: 600px;"></div>
  </body>
</html>
'''

# silly emacs: '

if __name__ == '__main__':

  if False:
    results = loadResults('/x/azul/phase1/ec2logs.reprozingslow.again/logs/Zing.qps175/results.bin')
    upto = 0
    while results[upto][0] < 300.0:
      upto += 1
    print 'upto %s' % upto
    for tup in results[upto:]:
      print tup
      if tup[2] > 50000:
        print '  ***'
    sys.exit(0)

  logsDir = sys.argv[1]
  warmupSec = float(sys.argv[2])
  fileNameOut = sys.argv[3]

  d = []
  for name in sys.argv[4:]:
    resultsFile = '%s/%s/results.pk' % (logsDir, name)
    if not os.path.exists(resultsFile):
      resultsFile = '%s/%s/results.bin' % (logsDir, name)
    d.append((name, resultsFile))

  html = createGraph(d, warmupSec)
  open(fileNameOut, 'wb').write(html)
  print 'Saved to %s' % fileNameOut
    
