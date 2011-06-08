import math
import datetime
import re
import sys

reReopen = re.compile(r'^Reopen:\s+([0-9.]+) msec')
reQT = re.compile('^QT (\d+) searches=(\d+) docs=(\d+) reopens=(\d+)')

qt = 0
reopen = None
results = []
sum = 0
sumSQ = 0
count = 0
for line in open(sys.argv[1], 'rb').readlines():
  m = reReopen.match(line)
  if m is not None:
    reopen = float(m.group(1))
  else:
    m = reQT.search(line)
    if m is not None and reopen is not None:
      qt = int(m.group(1))
      searches = int(m.group(2))
      results.append((qt, reopen, searches))
      sum += reopen
      sumSQ += reopen*reopen
      count += 1

print 'reopen: mean %.2f stddev %.2f' % \
      (sum/count, math.sqrt(count*sumSQ - sum*sum)/count)

# discard warmup
results = results[10:]

js = []
t = datetime.datetime(year=2011, month=4, day=25)
for idx, (qt, reopen, searches) in enumerate(results):
  js.append('"%s,%d,%.2f\\n"' % (t + datetime.timedelta(seconds=qt), searches, reopen))

open('nrt.csv', 'wb').write('Date,Searches/sec,Reopen (msec)\n%s' % '\n'.join(js))
open('nrt.html', 'wb').write('''
<html>
<head>
<script type="text/javascript"
  src="dygraph-combined.js"></script>
</head>
<body>
<div id="graphdiv" style="width:1000px; height:600px;"></div>
<script type="text/javascript">
  g = new Dygraph(

    // containing div
    document.getElementById("graphdiv"),

    // CSV or path to a CSV file.
    "nrt.csv",

    {showRoller: true,
     xlabel: "Run Time (HH:MM)",
     ylabel: "QPS, NRT Latency (msec)",
     title: "QPS and NRT Latency over time"}
  );
</script>
</body>
</html>
''')
