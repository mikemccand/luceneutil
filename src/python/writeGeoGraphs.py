import bisect
import os
import datetime
import pickle
import pysftp

nextGraph = 300

KNOWN_CHANGES = (
  ('2016-04-14', 'LUCENE-7214: remove two-phase support from 2D points distance query', 'LatLonPoint'),
  ('2016-04-22', 'LUCENE-7249: polygon queries should use the grid\'s relate during recursion', 'LatLonPoint'),
)

def toString(timeStamp):
  return '%04d-%02d-%02d %02d:%02d:%02d' % \
                    (timeStamp.year,
                     timeStamp.month,
                     timeStamp.day,
                     timeStamp.hour,
                     timeStamp.minute,
                     int(timeStamp.second))

def writeGraphHeader(f, id):
  global nextGraph
  f.write('''
<a name="%s"></a>
<div id="chart_%s" style="height:500px; position: absolute; left: 0px; right: 260px; top: %spx"></div>
<div id="chart_%s_labels" style="width: 250px; position: absolute; right: 0px; top: %spx"></div>''' % (id, id, nextGraph, id, nextGraph+30))
  nextGraph += 550

def writeGraphFooter(f, id, title, yLabel):
  f.write(''',
{ "title": "<a href=\'#%s\'><font size=+2>%s</font></a>",
  "colors": ["#DD1E2F", "#EBB035", "#06A2CB", "#218559", "#B0A691", "#192823"],
  "includeZero": true,
  "xlabel": "Date",
  "ylabel": "%s",
  "connectSeparatedPoints": true,
  "hideOverlayOnMouseOut": false,
  "labelsDiv": "chart_%s_labels",
  "labelsSeparateLines": true,
  "legend": "always",
  //"clickCallback": onClick,
  //"drawPointCallback": drawPoint,
  //"drawPoints": true,
  }
  );
''' % (id, title, yLabel, id))

def writeOneGraph(data, chartID, chartTitle, yLabel):
  writeGraphHeader(f, chartID)

  f.write('''
  <script type="text/javascript">
    g = new Dygraph(

      // containing div
      document.getElementById("chart_%s"),
  ''' % chartID)

  series = data.keys()
  
  headers = ['Date'] + list(series)

  allTimes = set()
  for seriesName, points in data.items():
    for timeStamp, point in points.items():
      allTimes.add(timeStamp)

  allTimes = list(allTimes)
  allTimes.sort()

  # Records valid timestamps by series:
  chartTimes = {}
  
  f.write('    "%s\\n"\n' % ','.join(headers))
  for timeStamp in allTimes:
    l = [toString(timeStamp)]
    for seriesName in series:
      if timeStamp in data[seriesName]:
        if seriesName not in chartTimes:
          chartTimes[seriesName] = []
        chartTimes[seriesName].append(timeStamp)
        value = data[seriesName][timeStamp]
        l.append('%.2f' % value)
      else:
        l.append('')
    f.write('    + "%s\\n"\n' % ','.join(l))
  writeGraphFooter(f, chartID, chartTitle, yLabel)
  writeAnnots(f, chartTimes, KNOWN_CHANGES, 'LatLonPoint')
  f.write('</script>')

def prettyName(approach):
  if approach == 'points':
    return 'LatLonPoint'
  elif approach == 'geopoint':
    return 'GeoPoint'
  elif approach == 'geo3d':
    return 'Geo3D'
  else:
    raise RuntimeError('unhandled approach %s' % approach)

def getLabel(label):
  if label < 26:
    s = chr(65+label)
  else:
    s = '%s%s' % (chr(65+(label//26 - 1)), chr(65 + (label%26)))
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
    
    parts = list(int(x) for x in date.split('-'))
    year, month, day = parts[:3]
    if len(parts) == 6:
      hour, min, sec = parts[3:]
    else:
      if len(parts) != 3:      
        raise RuntimeError('invalid timestamp "%s": should be YYYY-MM-DD[-HH-MM-SS]' % date)
      hour = min = sec = 0
    timeStamp = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=min, second=sec)

    if timeStamp < firstTimeStamp:
      # If this annot is from before this chart started, skip it:
      # print('skip annot %s %s: %s' % (date, series, reason))
      continue

    # Place the annot on the next data point that's <= the annot timestamp, in this chart:
    if type(chartTimes) is dict:
      if series not in chartTimes:
        print('skip annot %s %s: %s (series not in chartTimes: %s)' % (date, series, reason, chartTimes.keys()))
        continue
      l = chartTimes[series]
    else:
      l = chartTimes

    idx = bisect.bisect_left(l, timeStamp)
    
    if idx is not None:
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
  f.write('g.ready(function() {g.setAnnotations([')
  for timeStamp, d in byTimeStamp.items():
    for series, items in d.items():
      messages = r'\n\n'.join(items[1:])
      f.write('{series: "%s", x: "%s", shortText: "%s", text: "%s"},\n' % \
              (series, toString(timeStamp), getLabel(items[0]), messages.replace('"', '\\"')))
  f.write(']);});\n')

def loadAllResults():
  allResults = {}
  for name in os.listdir('/l/logs.nightly/geo'):
    if name.endswith('.pk'):
      #print('load results: %s' % name)
      year, month, day, hour, minute, second = (int(x) for x in name[:-3].split('.'))
      results = pickle.loads(open('/l/logs.nightly/geo/%s' % name, 'rb').read())
      allResults[datetime.datetime(year, month, day, hour, minute, second)] = results
  return allResults

def add(data, key, timeStamp, value):
  if key not in data:
    data[key] = {}
  data[key][timeStamp] = value

with open('/x/tmp/geobench.html', 'w') as f:
  f.write('''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="content-type" content="text/html; charset=UTF-8">
<title>Lucene Nightly Geo Benchmarks</title>
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
</head>
<body>
<h2>Lucene Geo benchmarks</h2>
<p>Below are the results of the Lucene nightly geo benchmarks based on the <a href="https://git-wip-us.apache.org/repos/asf/lucene-solr.git">master</a> branch as of that point in time.</p>
<p>This test indexes a 6.1M point subset exported from the full (as of 3/7/2016) 3.2B point <a href="http://openstreetmaps.org">OpenStreetMaps corpus</a>, including every point inside London, UK, and 2.5% of the remaining points, using three different approaches, and then tests search and sorting performance of various shapes.  The London boroughs polygons <a href="http://data.london.gov.uk/2011-boundary-files">come from here</a> (33 polygons, average 5.6K vertices).</p>
<p>On each chart, you can click + drag (vertically or horizontally) to zoom in and then shift + drag to move around, and double-click to reset.  Hover over an annotation to see known changes.</p>
    ''')

  byQuery = {}
  indexKDPS = {}
  readerHeapMB = {}
  indexMB = {}
  maxDoc = 60844404
  for timeStamp, (stats, results) in loadAllResults().items():
    for approach in stats.keys():
      add(indexKDPS, prettyName(approach), timeStamp, maxDoc/stats[approach][2]/1000.0)
      add(readerHeapMB, prettyName(approach), timeStamp, stats[approach][0])
      add(indexMB, prettyName(approach), timeStamp, stats[approach][1]*1024)
      
    for tup in results.keys():
      if tup[0] not in byQuery:
        byQuery[tup[0]] = {}
        
      key = prettyName(tup[1])
      if key not in byQuery[tup[0]]:
        byQuery[tup[0]][key] = {}

      qps, mhps, totHits = results[tup]
      if tup[0] == 'nearest 10':
        metric = qps
      else:
        metric = mhps
      byQuery[tup[0]][key][timeStamp] = metric

  for key in 'distance', 'poly 10', 'polyMedium', 'box', 'nearest 10', 'sort':
    data = byQuery[key]
    #print('write graph for %s' % key)
    if key == 'distance':
      title = 'Distance Filter'
    elif key == 'poly 10':
      title = 'Regular 10-gon Filter'
    elif key == 'polyMedium':
      title = 'London Boroughs Polygons Filter (avg 5.6K vertices)'
    elif key == 'box':
      title = 'Box Filter'
    elif key == 'nearest 10':
      title = '10 Nearest Points'
    elif key == 'sort':
      title = 'Box Filter, Sort by Distance'
    else:
      raise RuntimeError('unknown chart %s' % key)
    writeOneGraph(data, 'search-%s' % key, title, 'MHPS')

  writeOneGraph(indexKDPS, 'index-times', 'Indexing K docs/sec', 'K docs/sec')
  writeOneGraph(readerHeapMB, 'reader-heap', 'Searcher Heap Usage', 'MB')
  writeOneGraph(indexMB, 'index-size', 'Index Size', 'MB')

  f.write('''
</body>
</html>
''')

with pysftp.Connection('home.apache.org', username='mikemccand') as c:
  with c.cd('public_html'):
    c.put('/x/tmp/geobench.html', 'geobench.html')
