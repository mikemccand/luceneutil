import bisect
import datetime
import os
import pickle

nextGraph = 500
graphCount = 0

KNOWN_CHANGES = ()


def toString(timeStamp):
  return "%04d-%02d-%02d %02d:%02d:%02d" % (timeStamp.year, timeStamp.month, timeStamp.day, timeStamp.hour, timeStamp.minute, int(timeStamp.second))


def writeGraphHeader(f, id):
  global nextGraph
  global graphCount

  graphCount += 1

  idNoSpace = id.replace(" ", "_")

  f.write(
    """
<a name="%s" id="%s"></a>
<style>
  a#%s {
    display: block;
    position: relative;
    top: %spx;
    visibility: hidden;
  }
</style>
<div id="chart_%s" style="height:500px; position: absolute; left: 0px; right: 260px; top: %spx"></div>
<div id="chart_%s_labels" style="width: 250px; position: absolute; right: 0px; top: %spx"></div>"""
    % (idNoSpace, idNoSpace, idNoSpace, nextGraph - 210 - 0 * graphCount, id, nextGraph, id, nextGraph + 30)
  )
  nextGraph += 550


def writeGraphFooter(f, id, title, yLabel, series):
  colors = ["#DD1E2F", "#EBB035", "#06A2CB", "#218559"]  # , "#218559"] #, "#B0A691", "#192823"],

  # colors = []
  # if 'LatLonShape' in series:
  #  colors.append('#218559')

  idNoSpace = id.replace(" ", "_")

  f.write(
    """,
{ "title": "<a href=\'#%s\'><font size=+2>%s</font></a>",
  "colors": %s,
  "includeZero": true,
  "xlabel": "Date",
  "ylabel": "%s",
  "connectSeparatedPoints": true,
  "hideOverlayOnMouseOut": false,
  "labelsDiv": "chart_%s_labels",
  "labelsSeparateLines": true,
  "legend": "always",
  "clickCallback": onClick,
  //"drawPointCallback": drawPoint,
  "drawPoints": true,
  }
  );
"""
    % (idNoSpace, title, colors, yLabel, id)
  )


def writeOneGraph(data, chartID, chartTitle, yLabel, allTimes):
  writeGraphHeader(f, chartID)
  f.write(
    """
  <script type="text/javascript">
    g = new Dygraph(

      // containing div
      document.getElementById("chart_%s"),
  """
    % chartID
  )

  series = list(data.keys())
  series.sort()

  headers = ["Date"] + series

  # Records valid timestamps by series:
  chartTimes = {}

  f.write('    "%s\\n"\n' % ",".join(headers))

  for timeStamp in allTimes:
    l = [toString(timeStamp)]
    for seriesName in series:
      if timeStamp in data[seriesName]:
        if seriesName not in chartTimes:
          chartTimes[seriesName] = []
        chartTimes[seriesName].append(timeStamp)
        value = data[seriesName][timeStamp]
        l.append("%.2f" % value)
      else:
        l.append("")
    f.write('    + "%s\\n"\n' % ",".join(l))
  writeGraphFooter(f, chartID, chartTitle, yLabel, series)
  print("writeAnnots for %s" % chartTitle)
  writeAnnots(f, chartTimes, KNOWN_CHANGES, "LatLonShape")
  f.write("</script>")


def prettyName(approach):
  return approach


def getLabel(label):
  if label < 26:
    s = chr(65 + label)
  else:
    s = "%s%s" % (chr(65 + (label // 26 - 1)), chr(65 + (label % 26)))
  return s


def writeAnnots(f, chartTimes, annots, defaultSeries):
  if len(chartTimes) == 0:
    return

  # First, just aggregate by the timeStamp we need to attach each annot to, so in
  # case more than one change lands on one point, we can show all of them under a
  # single annot:

  if type(chartTimes) is list:
    firstTimeStamp = chartTimes[0]
  else:
    firstTimeStamp = None
    for l in chartTimes.values():
      if len(l) > 0 and (firstTimeStamp is None or l[0] < firstTimeStamp):
        firstTimeStamp = l[0]

  label = 0
  byTimeStamp = {}
  for annot in annots:
    date, reason = annot[:2]
    if len(annot) > 2:
      series = annot[2]
    else:
      series = defaultSeries

    parts = list(int(x) for x in date.split("-"))
    year, month, day = parts[:3]
    if len(parts) == 6:
      hour, min, sec = parts[3:]
    else:
      if len(parts) != 3:
        raise RuntimeError('invalid timestamp "%s": should be YYYY-MM-DD[-HH-MM-SS]' % date)
      hour = min = sec = 0
    timeStamp = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=min, second=sec)

    if timeStamp < firstTimeStamp - datetime.timedelta(days=1):
      # If this annot is from before this chart started, skip it:
      # print('skip annot %s %s: %s' % (date, series, reason))
      continue

    # Place the annot on the next data point that's <= the annot timestamp, in this chart:
    if type(chartTimes) is dict:
      if series not in chartTimes:
        print("skip annot %s %s: %s (series not in chartTimes: %s)" % (date, series, reason, chartTimes.keys()))
        continue
      l = chartTimes[series]
    else:
      l = chartTimes

    idx = bisect.bisect_left(l, timeStamp)

    if idx is not None and idx < len(l):
      # This is the timestamp, on or after when the annot was, that exists in the particular
      # series we are annotating:
      bestTimeStamp = l[idx]
      if bestTimeStamp not in byTimeStamp:
        byTimeStamp[bestTimeStamp] = {}
      if series not in byTimeStamp[bestTimeStamp]:
        byTimeStamp[bestTimeStamp][series] = [label]
        label += 1
      byTimeStamp[bestTimeStamp][series].append(reason)

  # Then render the annots:
  f.write("g.ready(function() {g.setAnnotations([")
  for timeStamp, d in byTimeStamp.items():
    for series, items in d.items():
      messages = r"\n\n".join(items[1:])
      f.write('{series: "%s", x: "%s", shortText: "%s", text: "%s"},\n' % (series, toString(timeStamp), getLabel(items[0]), messages.replace('"', '\\"')))
  f.write("]);});\n")


def loadAllResults():
  allResults = {}
  for name in os.listdir("/l/logs.nightly/geoshape"):
    if name.endswith(".pk"):
      # print('load results: %s' % name)
      year, month, day, hour, minute, second = (int(x) for x in name[:-3].split("."))
      results = pickle.loads(open("/l/logs.nightly/geoshape/%s" % name, "rb").read())
      allResults[datetime.datetime(year, month, day, hour, minute, second)] = results
  return allResults


def add(data, key, timeStamp, value):
  # print data
  if key not in data:
    print("key %s is not in data" % key)
    data[key] = {}
  data[key][timeStamp] = value


with open("/l/reports.nightly/geoshapebench.html", "w") as f:
  f.write("""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="content-type" content="text/html; charset=UTF-8">
<title>Lucene Nightly GeoShape Benchmarks</title>
<script type="text/javascript" src="dygraph-combined.js"></script>
<style>
  a:hover * {
    text-decoration: underline;
  }
  html *
  {
    #font-size: 1em !important;
    text-decoration: none;
    #color: #000 !important;
    font-family: Helvetica !important;
  }
</style>

<script type="text/javascript">

  // surely there is a format operator I could use instead ;)
  function zp(num,count) {
    var ret = num + '';
    while(ret.length < count) {
      ret = "0" + ret;
    }
    return ret;
  }

  function onClick(ev, msec, pts) {
    var d = new Date(msec);
    var key = d.getFullYear() + "-" + zp(1+d.getMonth(), 2) + "-" + zp(d.getDate(), 2);
    key += " " + zp(d.getHours(), 2) + ":" + zp(d.getMinutes(), 2) + ":" + zp(d.getSeconds(), 2);
    gitHash = gitHashes[key];

    top.location = "https://gitbox.apache.org/repos/asf?p=lucene.git;a=log;h=" + gitHash;
  }
</script>

</head>
<body>
<h2>Lucene Geo Shape Benchmarks</h2>
<div style="min-width: 800px">
<p>Below are the results of the Lucene nightly geo shape benchmarks based on the <a href="https://gitbox.apache.org/repos/asf/lucene.git">main</a> branch as of that point in time.</p>
<p>This test indexes a 13M polygons subset exported from the full (as of 23/05/2019) <a href="http://openstreetmaps.org">OpenStreetMaps corpus</a>, including every polygon inside the UK, and 10% of polygons from remaining areas, using vector based LatLonShape to test search performance for different spatial operations using various shapes.  The London boroughs polygons <a href="http://data.london.gov.uk/2011-boundary-files">come from here</a> (33 polygons, average 5.6K vertices).</p>
<p>On each chart, you can click + drag (vertically or horizontally) to zoom in and then shift + drag to move around, and double-click to reset.  Hover over an annotation to see known changes.  Click on a point to see the exact source git revision that was run.</p>
</div>
    """)

  byQuery = {}
  indexKDPS = {}
  readerHeapMB = {}
  indexMB = {}
  maxDoc = 13165248
  gitHashes = {}
  allTimes = set()
  for timeStamp, (gitRev, stats, results) in loadAllResults().items():
    gitHashes[toString(timeStamp)] = gitRev
    allTimes.add(timeStamp)
    for approach in stats.keys():
      add(indexKDPS, prettyName(approach), timeStamp, maxDoc / stats[approach][2] / 1000.0)
      add(readerHeapMB, prettyName(approach), timeStamp, stats[approach][0])
      add(indexMB, prettyName(approach), timeStamp, stats[approach][1] * 1024)

    for tup in results.keys():
      shape, operation = tup

      if shape not in byQuery:
        byQuery[shape] = {}

      qps, mhps, totHits = results[tup]
      metric = qps
      add(byQuery[shape], prettyName(operation), timeStamp, metric)

  f.write('<script type="text/javascript">\n')
  f.write("  var gitHashes = {};\n")
  for key, value in gitHashes.items():
    f.write('  gitHashes["%s"] = "%s";\n' % (key, value))
  f.write("</script>\n")
  allTimes = list(allTimes)
  allTimes.sort()
  for key in "poly 10", "polyMedium", "box", "point", "polyRussia":
    if key not in byQuery:
      continue
    data = byQuery[key]
    # print('write graph for %s' % key)
    if key == "distance":
      title = "Distance Filter"
    elif key == "poly 10":
      title = "Regular 10-gon Query"
    elif key == "point":
      title = "Point Query"
    elif key == "polyMedium":
      title = "London Boroughs Polygons Query (avg 5.6K vertices)"
    elif key == "polyRussia":
      title = "Russia Polygon (11.6K vertices)"
    elif key == "box":
      title = "Box Query"
    elif key == "nearest 10":
      title = "10 Nearest Points"
    elif key == "sort":
      title = "Box Query, Sort by Distance"
    else:
      raise RuntimeError("unknown chart %s" % key)
    if key == "sort":
      try:
        # This data is a lie ... was running GeoPoint box query but doing messed up sort as if LatLonDVField had been used at indexing time:
        del data["GeoPoint"]
      except KeyError:
        pass
    writeOneGraph(data, "search-%s" % key, title, "QPS", allTimes)

  writeOneGraph(indexKDPS, "index-times", "Indexing K docs/sec", "K docs/sec", allTimes)
  writeOneGraph(readerHeapMB, "reader-heap", "Searcher Heap Usage", "MB", allTimes)
  writeOneGraph(indexMB, "index-size", "Index Size", "MB", allTimes)

  f.write("""
</body>
</html>
""")
