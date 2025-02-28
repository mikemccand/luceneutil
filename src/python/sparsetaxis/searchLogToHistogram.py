import sys
import re

reHits = re.compile('T(.) (.*?) sort=(.*?): ([0-9]+) hits in ([.0-9]+) msec')
reHeapUsagePart = re.compile(r'^  ([a-z ]+) \[.*?\]: ([0-9.]+) (.B)$')
def extractSearchStats(searchLog, byQuerySort={}):
  
  heapBytes = None
  heapBytesByPart = {}
  byThread = {}
  
  with open(searchLog, 'r', encoding='utf-8') as f:
    while True:
      line = f.readline()
      if line == '':
        break
      line = line.rstrip()
      if line.startswith('HEAP: '):
        heapBytes = int(line[6:])
      else:
        m = reHits.match(line)
        if m is not None:
          threadID, queryDesc, sortDesc, hitCount, msec = m.groups()
          if threadID not in byThread:
            byThread[threadID] = []
          if sortDesc == 'null':
            sortDesc = None
          else:
            sortDesc = 'longitude'
          byThread[threadID].append((queryDesc, sortDesc, int(hitCount), float(msec)))
        else:
          m = reHeapUsagePart.match(line)
          if m is not None:
            part, size, unit = m.groups()
            size = float(size)
            if unit == 'GB':
              size *= 1024*1024*1024
            elif unit == 'MB':
              size *= 1024*1024
            elif unit == 'KB':
              size *= 1024
            else:
              raise RuntimeError('uhandled unit %s' % unit)
            heapBytesByPart[part] = heapBytesByPart.get(part, 0.0) + size
            

  for threadID, results in byThread.items():
    # discard warmup
    print('count %s' % len(results))
    results = results[10:]
    for queryDesc, sortDesc, hitCount, msec in results:
      tup = (queryDesc, sortDesc)
      if tup not in byQuerySort:
        byQuerySort[tup] = []
      byQuerySort[tup].append(msec)

  return byQuerySort

stats = {}
for fileName in sys.argv[1:]:
  extractSearchStats(fileName, stats)

with open('/x/tmp/histo.html', 'w') as f:
  w = f.write
  w('''
<html>
  <head>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript">
      google.charts.load("current", {packages:["corechart"]});
    </script>
  </head>
  <body>
''')  

  count = 0
  for key, value in stats.items():
    w('''
    
    <script type="text/javascript">
      // Chart %s:
      google.charts.setOnLoadCallback(drawChart%s);
      function drawChart%s() {
        var data = google.visualization.arrayToDataTable(
''' % (str(key), count, count))
    l = []
    l.append(['Iter', 'MSec'])
    maxMS = 0
    for idx, msec in enumerate(value):
      l.append([str(idx), msec])
      maxMS = max(maxMS, msec)
    w(repr(l) + ');')
    if key == ('cab_color:g', None):
      title = 'Green taxis'
    elif key == ('cab_color:g', 'longitude'):
      title = 'Green taxis, sort by Longitude'
    elif key == ('cab_color:y', None):
      title = 'Yellow taxis'
    elif key == ('cab_color:y', 'longitude'):
      title = 'Yellow taxis, sort by Longitude'
    elif key == ('cab_color:y cab_color:g', None):
      title = 'Green + Yellow taxis'
    elif key == ('pickup_latitude:[40.75 TO 40.9]', None):
      title = 'Latitude range'
    else:
      title = str(key)
      
    w('''
        var options = {
          title: "MSec for %s",
          legend: { position: "none" },
          histogram: { bucketSize: %s, lastBucketPercentile: 10 },

        };

        var chart = new google.visualization.Histogram(document.getElementById("chart_div%s"));
        chart.draw(data, options);
      }
    </script>
''' % (title, int(maxMS/60.), count))
    w('<div id="chart_div%s" style="width: 900px; height: 500px;"></div>' % count)
    count += 1

  w('''
  </body>
</html>
''')
