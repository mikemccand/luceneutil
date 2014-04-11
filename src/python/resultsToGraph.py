import os
import sys
import re
import time

r = re.compile(r'^(\d+): ([0-9\.]+) sec$')

def loadPoints(fileName):
  points = []
  with open(fileName, 'r') as f:
    for line in f.readlines():
      #print('line %s' % line)
      m = r.match(line.strip())
      if m is not None:
        points.append((int(m.group(1)), float(m.group(2))))
  return points

def gen():
  data = []
  for name in os.listdir('/l/trunk/lucene'):
    if name.startswith('results') and name.endswith('.txt'):
      label = 'base:%s' % name[:-4]
      data.append((label, loadPoints('/l/trunk/lucene/%s' % name)))
  for name in os.listdir('/l/fastindexingchain/lucene'):
    if name.startswith('results') and name.endswith('.txt'):
      label = 'comp:%s' % name[:-4]
      data.append((label, loadPoints('/l/fastindexingchain/lucene/%s' % name)))

  f = open('/x/tmp/results.html', 'w')
  f.write('''
  <html>
    <head>
      <script type="text/javascript" src="https://www.google.com/jsapi"></script>
      <script type="text/javascript">
        google.load("visualization", "1", {packages:["corechart"]});
        google.setOnLoadCallback(drawChart);
        function drawChart() {
          var data = google.visualization.arrayToDataTable([
          ''')
  f.write("['Doc count',%s],\n" % ','.join("'%s'" % x[0] for x in data))
  upto = 0
  while True:
    row = []
    docCount = None
    for label, points in data:
      if len(points) > upto:
        docCount, sec = points[upto]
        row.append('%.2f' % sec)
      else:
        row.append('0.0')

    if docCount is None:
      break

    f.write('[%s,%s],\n' % (docCount, ','.join(row)))
    upto += 1

  f.write('''
          ]);

          var options = {
            title: 'Performance'
          };

          var chart = new google.visualization.LineChart(document.getElementById('chart_div'));
          chart.draw(data, options);
        }
      </script>
    </head>
    <body>
      <div id="chart_div" style="width: 900px; height: 500px;"></div>
    </body>
  </html>
  ''')
  f.close()

  #os.rename('/x/tmp/results.html.new', '/x/tmp/results.html')

while True:
  print('regen...')
  gen()
  time.sleep(1.0)
