import bisect
import os
import datetime
import pickle
import pysftp

nextGraph = 500
graphCount = 0

KNOWN_CHANGES = (
  ('2016-03-22', 'LUCENE-7130: fold recent LatLonPoint optimizations into GeoPoint', 'GeoPoint'),
  ('2016-03-29', 'LUCENE-7147: more accurate disjoint check for distance filters', 'LatLonPoint'),
  ('2016-03-31', 'LUCENE-7150: add common shape query APIs to geo3d', 'Geo3D'),
  ('2016-04-11', 'LUCENE-7199: speed up how polygon\'s sideness is computed', 'Geo3D'),
  ('2016-04-14', 'LUCENE-7214: remove two-phase support from 2D points distance query', 'LatLonPoint'),
  ('2016-04-15', 'LUCENE-7221: speed up large polygons', 'Geo3D'),
  ('2016-04-15', 'LUCENE-7069: add nearest neighbor search to LatLonPoint', 'LatLonPoint'),
  ('2016-04-19', 'LUCENE-7229: improve Polygon.relate for faster tree traversal/grid construction', 'LatLonPoint'),
  ('2016-04-22', 'LUCENE-7239: polygon queries now use an interval tree for fast point-in-polygon testing', 'LatLonPoint'),
  ('2016-04-23', 'LUCENE-7249: polygon queries should use the interval tree for fast relate during recursion', 'LatLonPoint'),
  ('2016-04-25', 'LUCENE-7240: don\'t use doc values with LatLonPoint unless sorting is needed', 'LatLonPoint'),
  ('2016-04-25', 'Implement grow() for polygon queries', 'LatLonPoint'),
  ('2016-04-26', 'LUCENE-7251: speed up polygons with many sub-polygons', 'LatLonPoint'),
  ('2016-04-27', 'LUCENE-7254: optimization: pick a sparse or non-sparse bit set up front for collection', 'LatLonPoint'),
  ('2016-04-28', 'LUCENE-7249: optimization: remove per-hit add instruction', 'LatLonPoint'),
  ('2016-05-30-22-00-00', 'LUCENE-7306: use radix sort rather than quicksort', 'LatLonPoint'),
  ('2016-07-04', 'LUCENE-7351: compress doc ids', 'LatLonPoint'),
  ('2016-07-11', 'Upgrade beast2 OS from Ubuntu 15.04 to 16.04'),
  ('2016-07-12', 'LUCENE-7371: use run-length compression for points (BKD) values'),
  ('2016-07-16', 'Upgrade Linux kernel from 4.4.x to 4.6.x'),
  ('2016-08-17', 'Upgrade Linux kernel from 4.6.x to 4.7.0'),
  ('2016-09-05', 'Upgrade Linux kernel from 4.7.0 to 4.7.2'),
  ('2016-09-21', 'LUCENE-7407: Change doc values from random access to iterator API'),
  ('2016-11-01', 'LUCENE-7135: This issue accidentally caused FSDirectory.open to use NIOFSDirectory instead of MMapDirectory'),
  ('2016-11-03', 'LUCENE-7135: Fixed this issue so we use MMapDirectory again'),
  ('2017-01-27', 'LUCENE-7656: Speed up LatLonPointDistanceQuery by dividing the bounding box around the circle into a grid of smaller boxes and precomputing their relation with the distance query'),
  ('2017-01-31', 'LUCENE-7661: Speed up LatLonPointInPolygonQuery by dividing the bounding box around the polygons into a grid of smaller boxes and precomputing their relation with the polygon'),
  ('2018-11-03', 'LUCENE-8554: Check if either end of a segment is contained in the target rectangle before applying the determinant formula'),
  ('2018-11-27', 'LUCENE-8562: Speed up merging segments of points with data dimensions by only sorting on the indexed dimensions', 'LatLonShape'),
  ('2018-12-19', 'LUCENE-8581: Change LatLonShape encoding to use 4 BYTES Per Dimension', 'LatLonShape'),
  ('2019-01-15', 'LUCENE-8623: Decrease I/O pressure when merging high dimensional points'),
  ('2019-02-08', 'LUCENE-8673: Use radix partitioning when merging dimensional points'),
  ('2019-02-12', 'LUCENE-8687: Optimise radix partitioning for points on heap'),
  ('2019-02-21', 'LUCENE-8699: Change HeapPointWriter to use a single byte array instead to a list of byte arrays'),
  ('2019-04-12', 'LUCENE-8736: Improved line detection'),
  ('2019-04-23', 'Switched to OpenJDK 11'),
  ('2019-04-30', 'Switched GC back to ParallelGC (away from default G1GC)'),
  ('2019-08-27', 'LUCENE-8955: Move compare logic to IntersectVisitor in NearestNeighbor'),
  ('2019-10-12', 'LUCENE-8928: Compute exact bounds every N splits'),
  ('2019-10-26', 'LUCENE-8932: Move BKDReader\'s index off-heap when the input is a ByteBufferIndexInput'),
  ('2020-01-13', 'Switch to OpenJDK 13'),
  ('2020-01-14', 'Switch to OpenJDK 12'),
  ('2020-02-05', 'LUCENE-9147: Off-heap stored fields index'),
  ('2020-05-02', 'LUCENE-9087: Always write full leaves'),
  ('2020-09-28', 'LUCENE-10125: Optimize primitive writes in OutputStreamIndexOutput'),
  ('2020-10-05', 'LUCENE-10145: Speed up byte[] comparisons using VarHandles'),
  ('2020-10-07', 'LUCENE-10153: Speed up BKDWriter using VarHandles'),

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
  global graphCount

  graphCount += 1

  idNoSpace = id.replace(' ', '_')

  f.write('''
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
<div id="chart_%s_labels" style="width: 250px; position: absolute; right: 0px; top: %spx"></div>''' % (idNoSpace, idNoSpace, idNoSpace, nextGraph-210-0*graphCount, id, nextGraph, id, nextGraph+30))
  nextGraph += 550

def writeGraphFooter(f, id, title, yLabel, series):

  # "colors": ["#DD1E2F", "#EBB035", "#06A2CB", "#218559", "#B0A691", "#192823"],

  colors = []
  if 'Geo3D' in series:
    colors.append('#DD1E2F')
  if 'GeoPoint' in series:
    colors.append('#EBB035')
  if 'LatLonPoint' in series:
    colors.append('#06A2CB')
  if 'LatLonPoint+DV' in series:
    colors.append('#218559')
  if 'LatLonShape' in series:
    colors.append('#8A2BE2')
    
  idNoSpace = id.replace(' ', '_')

  f.write(''',
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
''' % (idNoSpace, title, colors, yLabel, id))

def writeOneGraph(data, chartID, chartTitle, yLabel, allTimes):
  writeGraphHeader(f, chartID)

  f.write('''
  <script type="text/javascript">
    g = new Dygraph(

      // containing div
      document.getElementById("chart_%s"),
  ''' % chartID)

  series = list(data.keys())
  series.sort()
  
  headers = ['Date'] + series

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
  writeGraphFooter(f, chartID, chartTitle, yLabel, series)
  print('writeAnnots for %s' % chartTitle)
  writeAnnots(f, chartTimes, KNOWN_CHANGES, 'LatLonPoint')
  f.write('</script>')

def prettyName(approach):
  if approach == 'points':
    return 'LatLonPoint'
  elif approach == 'points-withdvs':
    return 'LatLonPoint+DV'
  elif approach == 'geopoint':
    return 'GeoPoint'
  elif approach == 'geo3d':
    return 'Geo3D'
  elif approach == 'shapes':
    return "LatLonShape"
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

    if timeStamp < firstTimeStamp - datetime.timedelta(days=1):
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

    top.location = "https://git-wip-us.apache.org/repos/asf?p=lucene-solr.git;a=log;h=" + gitHash;
  }
</script>

</head>
<body>
<h2>Lucene Geo benchmarks</h2>
<div style="min-width: 800px">
<p>Below are the results of the Lucene nightly geo benchmarks based on the <a href="https://git-wip-us.apache.org/repos/asf/lucene-solr.git">main</a> branch as of that point in time.</p>
<p>This test indexes a 60.8M point subset exported from the full (as of 3/7/2016) 3.2B point <a href="http://openstreetmaps.org">OpenStreetMaps corpus</a>, including every point inside London, UK, and 2.5% of the remaining points, using three different approaches, and then tests search and sorting performance of various shapes.  The London boroughs polygons <a href="http://data.london.gov.uk/2011-boundary-files">come from here</a> (33 polygons, average 5.6K vertices).</p>
<p>On each chart, you can click + drag (vertically or horizontally) to zoom in and then shift + drag to move around, and double-click to reset.  Hover over an annotation to see known changes.  Click on a point to see the exact source git revision that was run.</p>
</div>
    ''')

  byQuery = {}
  indexKDPS = {}
  readerHeapMB = {}
  indexMB = {}
  maxDoc = 60844404
  gitHashes = {}
  allTimes = set()
  for timeStamp, (gitRev, stats, results) in loadAllResults().items():

    gitHashes[toString(timeStamp)] = gitRev
    allTimes.add(timeStamp)
    
    for approach in stats.keys():
      add(indexKDPS, prettyName(approach), timeStamp, maxDoc/stats[approach][2]/1000.0)
      add(readerHeapMB, prettyName(approach), timeStamp, stats[approach][0])
      add(indexMB, prettyName(approach), timeStamp, stats[approach][1]*1024)
      
    for tup in results.keys():
      shape, approach = tup

      if shape == 'poly 10' and approach == 'geo3d' and timeStamp < datetime.datetime(year=2016, month=4, day=9):
        continue
      
      if shape not in byQuery:
        byQuery[shape] = {}
        
      qps, mhps, totHits = results[tup]
      if shape == 'nearest 10':
        metric = qps
      else:
        metric = mhps
      add(byQuery[shape], prettyName(approach), timeStamp, metric)

  f.write('<script type="text/javascript">\n')
  f.write('  var gitHashes = {};\n')
  for key, value in gitHashes.items():
    f.write('  gitHashes["%s"] = "%s";\n' % (key, value))
  f.write('</script>\n')

  allTimes = list(allTimes)
  allTimes.sort()

  for key in 'distance', 'poly 10', 'polyMedium', 'box', 'nearest 10', 'sort', 'polyRussia':
    if key not in byQuery:
      continue
    data = byQuery[key]
    #print('write graph for %s' % key)
    if key == 'distance':
      title = 'Distance Filter'
    elif key == 'poly 10':
      title = 'Regular 10-gon Filter'
    elif key == 'polyMedium':
      title = 'London Boroughs Polygons Filter (avg 5.6K vertices)'
    elif key == 'polyRussia':
      title = 'Russia Polygon (11.6K vertices)'
    elif key == 'box':
      title = 'Box Filter'
    elif key == 'nearest 10':
      title = '10 Nearest Points'
    elif key == 'sort':
      title = 'Box Filter, Sort by Distance'
    else:
      raise RuntimeError('unknown chart %s' % key)
    if key == 'sort':
      try:
        # This data is a lie ... was running GeoPoint box query but doing messed up sort as if LatLonDVField had been used at indexing time:
        del data['GeoPoint']
      except KeyError:
        pass
    writeOneGraph(data, 'search-%s' % key, title, 'M hits/sec', allTimes)

  writeOneGraph(indexKDPS, 'index-times', 'Indexing K docs/sec', 'K docs/sec', allTimes)
  writeOneGraph(readerHeapMB, 'reader-heap', 'Searcher Heap Usage', 'MB', allTimes)
  writeOneGraph(indexMB, 'index-size', 'Index Size', 'MB', allTimes)

  f.write('''
</body>
</html>
''')

with pysftp.Connection('home.apache.org', username='mikemccand') as c:
  with c.cd('public_html'):
    c.put('/x/tmp/geobench.html', 'geobench.html')
