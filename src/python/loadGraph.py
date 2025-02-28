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

import sys
import os
import re
import responseTimeGraph

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

def graph(rowPoint, logsDir, warmupSec, names, fileName, maxQPS=None):

  allQPS = set()

  points = {}

  for name in names:

    reQPS = re.compile(r'^%s\.qps(\d+)$' % re.escape(name))

    for f in os.listdir(logsDir):
      m = reQPS.match(f)
      resultsFile = '%s/%s/results.pk' % (logsDir, f)
      if not os.path.exists(resultsFile):
        resultsFile = '%s/%s/results.bin' % (logsDir, f)
        
      if m is not None and os.path.exists(resultsFile):
        qps = int(m.group(1))
        if maxQPS is not None and qps > maxQPS:
          print('SKIPPING %s qps' % qps)
          continue

        allQPS.add(qps)
        results = responseTimeGraph.loadResults(resultsFile)

        # Discard first warmupSec seconds:
        upto = 0
        while results[upto][0] < warmupSec:
          upto += 1

        results = results[upto:]

        responseTimes = [x[2] for x in results]
        responseTimes.sort()

        if rowPoint == 'min':
          t = responseTimes[0]
        elif rowPoint == 'max':
          t = responseTimes[-1]
        else:
          pct, minCount = logPoints[rowPoint]
          if len(responseTimes) < minCount:
            raise RuntimeError('%s doesn\'t have enough enough data' % name)
          idx = int(((100.0-pct)/100.0)*len(responseTimes))
          # TODO: should we take linear blend of the two points...?  Else
          # we have a sparseness problem...
          t = responseTimes[-idx-1]

        points[(name, qps)] = t
        if sla is not None and t <= sla:
          passesSLA.add(name)

  qpsList = list(allQPS)
  qpsList.sort()

  cleanName = {'OracleCMS': 'CMS',
               'OracleCMSMMap': 'CMS + MMap',
               'OracleCMSMMapDir': 'CMS + MMap'}

  print('names: %s; cleaned %s' % (names, (', '.join("'%s'" % cleanName.get(x, x) for x in names))))
  
  l = []
  w = l.append
  w("['QPS', %s],\n" % (', '.join("'%s'" % cleanName.get(x, x) for x in names)))
  for qps in qpsList:
    row = ['%d' % qps]
    for name in names:
      try:
        s = '%.1f' % points[(name, qps)]
      except KeyError:
        s = ''
      row.append(s)
    w('[%s],\n' % ','.join(row))

  if rowPoint == 'max':
    p = 'Max'
  elif rowPoint == 'min':
    p = 'Min'
  else:
    p = '%g%%' % logPoints[rowPoint][0]
    
  html = graphHeader + ''.join(l) + graphFooter % p
  open(fileName, 'wb').write(html)
  print('  saved %s' % fileName)

graphHeader = '''<html>
  <head>
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">
      google.load("visualization", "1", {packages:["corechart"]});
      google.setOnLoadCallback(drawChart);
      function drawChart() {
        var data = google.visualization.arrayToDataTable([
        '''

graphFooter = '''
        ]);

        var options = {
          title: '%s Response Time vs QPS',
          hAxis: {'title': 'QPS'},
          vAxis: {'title': 'Response Time (msec)'},
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

if __name__ == '__main__':
  logsDir = sys.argv[1]
  warmupSec = float(sys.argv[2])
  reportDir = sys.argv[3]
  for idx in xrange(len(logPoints)):
    graph(idx, logsDir, warmupSec, sys.argv[4:], '%s/load%spct.html' % (reportDir, logPoints[idx][0]))
  graph('max', logsDir, warmupSec, sys.argv[4:], '%s/loadmax.html' % reportDir)
  
