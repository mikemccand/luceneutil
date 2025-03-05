import re
import sys

html = """
<html>
  <head>
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">
      google.load("visualization", "1", {packages:["corechart"]});
      google.setOnLoadCallback(drawChart);
      function drawChart() {
        var data = google.visualization.arrayToDataTable(
        %s
        );

        var options = {
          title: 'UUID K lookups/sec, 1 thread',
          titleTextStyle: {fontSize: 20},
          // vAxis: {title: 'Algorithm',  titleTextStyle: {fontSize: 20}},
          // hAxis: {title: 'K lookups/sec',  titleTextStyle: {fontSize: 20}},
          legend: {position: 'none'}
        };

        var chart = new google.visualization.BarChart(document.getElementById('chart_div'));
        chart.draw(data, options);
      }
    </script>
  </head>
  <body>
    <div id="chart_div" style="width: 900px; height: 500px;"></div>
  </body>
</html>
"""

results = []
with open(sys.argv[1], "r") as f:
  while True:
    line = f.readline()
    if line == "":
      break
    line = line.strip()
    if line.startswith("best result: "):
      m = re.search("best result: (.*?): lookup=(.*?)K IDs/sec", line)
      label = m.group(1)
      if label.find(", base 256") != -1 or label in ("flake", "UUID v4 (random)", "UUID v1 (time, node, counter)", "uuids v4 (random)"):
        label = label.replace(", base 256", " [binary]")
        label = label.replace("simple sequential", "sequential")
        label = label.replace("uuids v1 (time, node, counter)", "uuids v1")
        label = label.replace("uuids v4 (random)", "uuids v4")
        if label == "nanotime [binary]":
          label = "nanotime"
        elif label == "sequential [binary]":
          label = "sequential"
        elif label == "zero pad sequential [binary]":
          label = "zero pad sequential"
        results.append([label, float(m.group(2))])

results.sort(key=lambda x: -x[1])
results = [["ID Source", "K lookups/sec, 1 thread"]] + results
open("/x/tmp/ids.html", "w").write(html % str(results))
