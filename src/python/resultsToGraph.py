import os
import sys
import re
import time

r = re.compile(r'^(\d+): ([0-9\.]+) sec$')
r2 = re.compile(r'^Indexer: (\d+) docs... \(([0-9\.]+) msec\)$')

def loadPoints(fileName):
  points = []
  with open(fileName, 'r') as f:
    for line in f.readlines():
      #print('line %s' % line)
      m = r.match(line.strip())
      if m is not None:
        points.append((int(m.group(1)), float(m.group(2))))
      m = r2.match(line.strip())
      if m is not None:
        #print('%s, %s' % (int(m.group(1)), m.group(2)))
        points.append((int(m.group(1)), float(m.group(2))/1000.))
  return points

def gen():
  f = open('/x/tmp/results.html', 'w')
  f.write('<html>')

  for prefix in 'wiki', 'geo':
    #print('prefix %s' % prefix)
    if prefix == 'wiki':
      continue
    data = []
    root = '/l/results'
    for name in os.listdir(root):
      if name.startswith('results.%s' % prefix) and name.endswith('.txt'):
        #print('  %s' % name)
        label = name[(9+len(prefix)):-4]
        data.append((label, loadPoints('%s/%s' % (root, name))))

    f.write('''
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

            var chart = new google.visualization.LineChart(document.getElementById('chart_div_%s'));
            chart.draw(data, options);
          }
        </script>
        <br><br>
        %s:
        <div id="chart_div_%s" style="width: 900px; height: 500px;"></div>
    ''' % (prefix, prefix, prefix))

  f.write('</html>')
  f.close()

while True:
  print('regen...')
  gen()
  time.sleep(1.0)
