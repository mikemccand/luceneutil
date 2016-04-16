import datetime
import pickle

nextGraph = 300

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
  //"colors": ["#DD1E2F", "#EBB035", "#06A2CB", "#218559", "#B0A691", "#192823"],
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
  f.write('</script>')

with open('/x/tmp/test.html', 'w') as f:
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
<p>This test indexes 6.1M points subset exported from the <a href="http://openstreetmaps.org">OpenStreetMaps corpus</a>, including all points inside London, UK, and 2.5% of the remaining points.</p>
<p>Below are the results of the Lucene nightly geo benchmarks based on the <a href="https://git-wip-us.apache.org/repos/asf/lucene-solr.git">master</a> branch as of that point in time.</p>
<p>On each chart, you can click + drag (vertically or horizontally) to zoom in and then shift + drag to move around.</p>
    ''')

  t0 = datetime.datetime(year=2016, month=4, day=2)
  t1 = t0 + datetime.timedelta(days=1)
  t2 = t1 + datetime.timedelta(days=1)
  data = {
    'Points2D': {t0: 5.0, t1: 6.0, t2: 6.5}
    }

  writeOneGraph(data, 'search', 'Distance filter', 'QPS')

  f.write('''
</body>
</html>
''')
