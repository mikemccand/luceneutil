import subprocess
import sys

import responseTimeGraph

results = responseTimeGraph.loadResults(sys.argv[1])[0]
htmlOut = sys.argv[2]

lastTick = None

HDR_HISTOGRAM_PATH = "/l/HdrHistogram"

EVERY_SEC = 10

PCT = 0.99

pending = []

graphData = []

chartHeader = """
<html>
  <head>
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">
      google.load("visualization", "1", {packages:["corechart"]});
      google.setOnLoadCallback(drawChart);
      function drawChart() {
        var data = google.visualization.arrayToDataTable([
"""

chartFooter = """        ]);

        var options = {
          title: '%d%% pct response times',
          pointSize: 5,
          hAxis: {title: 'Elapsed time', format: '# sec'},
          vAxis: {title: '%d%% response time (msec)'},
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
""" % (PCT * 100, PCT * 100)

# silly emacs: '

for timestamp, taskString, latencyMS, queueTimeMS in results:
  tick = int(timestamp / EVERY_SEC)
  if tick != lastTick:
    if lastTick is not None:
      # print 'MAX %s' % (max(pending)/1000)
      p = subprocess.Popen("java -cp .:%s/src ToHGRM %g > out.hgrm 2>&1" % (HDR_HISTOGRAM_PATH, max(pending) / 1000.0), shell=True, stdin=subprocess.PIPE)
      # print '%d values to write' % len(pending)
      for ms in pending:
        p.stdin.write("%g\n" % (ms / 1000.0))
      p.stdin.close()
      p.communicate()

      with open("out.hgrm", "rb") as f:
        # Skip header:
        f.readline()
        f.readline()

        best = None
        bestSec = None
        for l in f.readlines():
          if l.startswith("#[Mean"):
            break

          tup = l.strip().split()
          pct = float(tup[1])
          sec = float(tup[0])
          if best is None:
            best = pct
            bestSec = sec
          elif abs(pct - PCT) < abs(best - PCT):
            best = pct
            bestSec = sec

      graphData.append((lastTick * EVERY_SEC, bestSec))

    pending = [latencyMS]
    lastTick = tick
  else:
    pending.append(latencyMS)

chart = []
w = chart.append
w(chartHeader)
w("          ['Sec', '%s%% response time'],\n" % (PCT * 100))
for t, msec in graphData:
  w("          [%d,%s],\n" % (t, msec))
w(chartFooter)

open(htmlOut, "wb").write("".join(chart))
print("Saved to %s" % htmlOut)
