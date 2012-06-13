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

def setRowCol(rows, row, pct, col, val):
  if len(rows) <= row:
    assert len(rows) == row
    rows.append(["'%g%%'" % pct])
  
  while len(rows[row]) < 1+col:
    rows[row].append(rows[row-1][col])
  rows[row].append('%.1f' % val)
  #print '  now %s; row=%s' % (len(rows[row]), row)
  
def createGraph(fileNames, warmupSec):
  cols = []
  names = []
  maxRows = 0

  for name, file in fileNames.items():
    results = cPickle.loads(open(file, 'rb').read())

    # Discard first warmupSec seconds:
    upto = 0
    while results[upto][0] < warmupSec:
      upto += 1

    print '%s has %d results (less %d = first %.1f seconds warmup); %.1f sec' % \
          (file, len(results), upto, warmupSec, results[-1][0]-warmupSec)
    results = results[upto:]

    responseTimes = [x[2] for x in results]
    responseTimes.sort()

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
      print 'max %s vs %s' % (responseTimes[-1], col[-1])
      col.append(responseTimes[-1])
    print '%s has %d rows' % (name, len(col))
    print col

    maxRows = max(maxRows, len(col))
      
    names.append(name)
    
  chart = []
  w = chart.append
  w(chartHeader)
  l = ['Percentile'] + names
  w("          [%s],\n" % (','.join("'%s'" % x for x in l)))
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
      label = '%g' % logPoints[rowID-1][0]

    w('          ["%s",%s],\n' % (label, ','.join(row)))

    rowID += 1

    if rowID == maxRows:
      break

        
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

  warmupSec = float(sys.argv[1])
  fileNameOut = sys.argv[2]

  d = {}
  for name in sys.argv[3:]:
    d[name] = 'logs/%s/results.pk' % name

  html = createGraph(d, warmupSec)
  open(fileNameOut, 'wb').write(html)
  print 'Saved to %s' % fileNameOut
    
