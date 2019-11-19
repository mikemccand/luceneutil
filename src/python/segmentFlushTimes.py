import random
import time
import os
import pickle
import re
import subprocess
import shutil

THREAD_COUNT = 4
DATA_FILE = '/l/data/enwiki-20110115-lines-1k-fixed.txt'
# DATA_FILE = '/b/lucenedata/enwiki-20120502-lines-1k.txt'

chartHTML = '''
<html>
  <head>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript">
      google.charts.load('current', {'packages':['corechart', 'line']});
      google.charts.setOnLoadCallback(drawChart);
      function drawChart() {
        var data = google.visualization.arrayToDataTable([
          ['Segment size (MB)', 'Flush time (msec)'],
          %s
        ]);

        var options = {
          title: 'Flush Time vs. Segment Size',
          hAxis: {title: 'Segment size (MB)'},
          vAxis: {title: 'Flush time (msec)'},
          legend: 'none',
          pointSize: 2,
        };

        var chart = new google.visualization.ScatterChart(document.getElementById('chart_div'));

        chart.draw(data, options);
      }
    </script>

    <script type="text/javascript">
      google.charts.setOnLoadCallback(drawChart2);
      function drawChart2() {
        var data = google.visualization.arrayToDataTable([
          ['Segment size (MB)', 'RAM/Disk efficiency (%%)'],
          %s
        ]);

        var options = {
          title: 'RAM/Disk efficiency vs. Segment Size',
          hAxis: {title: 'Segment size (MB)'},
          vAxis: {title: 'RAM/Disk efficiency (%%)'},
          legend: 'none',
          pointSize: 2,
        };

        var chart = new google.visualization.ScatterChart(document.getElementById('chart_div2'));

        chart.draw(data, options);
      }
    </script>

    <script type="text/javascript">
      google.charts.setOnLoadCallback(drawChart3);
      function drawChart3() {
        var data = google.visualization.arrayToDataTable([
          ['Segment size (MB)', 'Term count (M)'],
          %s
        ]);

        var options = {
          title: 'Term Count vs. Segment Size',
          hAxis: {title: 'Segment size (MB)'},
          vAxis: {title: 'Term count (M terms)'},
          legend: 'none',
          pointSize: 2,
        };

        var chart = new google.visualization.ScatterChart(document.getElementById('chart_div3'));

        chart.draw(data, options);
      }
    </script>

    <script type="text/javascript">
      google.charts.setOnLoadCallback(drawChart4);
      function drawChart4() {
        var data = google.visualization.arrayToDataTable([
          ['Buffer size (MB)', 'Segment Size (MB)'],
          %s
        ]);

        var options = {
          title: 'Buffer Size vs. Segment Size',
          hAxis: {title: 'Buffer size (MB)'},
          vAxis: {title: 'Segment Size (MB)'},
          legend: 'none',
          pointSize: 2,
        };

        var chart = new google.visualization.ScatterChart(document.getElementById('chart_div4'));

        chart.draw(data, options);
      }
    </script>

    <script type="text/javascript">
      google.charts.setOnLoadCallback(drawChart5);
      function drawChart5() {
        var data = google.visualization.arrayToDataTable([
          ['Term count (M)', 'Sort time (msec)'],
          %s
        ]);

        var options = {
          title: 'Sort Time vs. Term Count',
          hAxis: {title: 'Term Count (M)'},
          vAxis: {title: 'Sort Time (msec)'},
          legend: 'none',
          pointSize: 2,
        };

        var chart = new google.visualization.ScatterChart(document.getElementById('chart_div5'));

        chart.draw(data, options);
      }
    </script>

    <script type="text/javascript">
      google.charts.setOnLoadCallback(drawChart6);

      function drawChart6() {
        var data = google.visualization.arrayToDataTable([
        %s
        ]);

        var options = {
          title: 'Docs indexed over time',
          hAxis: {
            title: 'Time'
          },
          vAxis: {
            title: 'Doc Count'
          },
          interpolateNulls: true,
        };

        var chart = new google.visualization.LineChart(document.getElementById('chart_div6'));
        chart.draw(data, options);
      }
    </script>
  </head>
  <body>
    <table>
    <tr>
    <td><div id="chart_div" style="width: 700px; height: 400px;"></div></td>
    <td><div id="chart_div2" style="width: 700px; height: 400px;"></div></td>
    <td><div id="chart_div3" style="width: 700px; height: 400px;"></div></td>
    </tr>
    <tr>
    <td><div id="chart_div4" style="width: 700px; height: 400px;"></div></td>
    <td><div id="chart_div5" style="width: 700px; height: 400px;"></div></td>
    <td><div id="chart_div6" style="width: 700px; height: 400px;"></div></td>
    </tr>
  </body>
</html>
'''

def writeChart(results):

  byMB = {}

  for tup in results:
    mb = tup[0]
    if mb not in byMB:
      byMB[mb] = []
    byMB[mb].append(tup[1:])

  dataLines1 = []
  dataLines2 = []
  dataLines3 = []
  dataLines4 = []
  dataLines5 = []

  allMS = {}
  allMB = []

  for mb, l in byMB.items():
    # randomly pick one point:
    #x = random.choice(l)
    #print(x)
    for x in l:
      ramSizeMB, diskSizeMB, flushTimeMS, termCount, sortTimeMS, docsCount = x
      dataLines1.append('          [%.2f, %.2f],\n' % (diskSizeMB, flushTimeMS))
      dataLines2.append('          [%.2f, %.2f],\n' % (diskSizeMB, 100.0*diskSizeMB/ramSizeMB))
      dataLines3.append('          [%.2f, %.5f],\n' % (diskSizeMB, termCount/1000000.))
      dataLines4.append('          [%.2f, %.2f],\n' % (mb, diskSizeMB))
      dataLines5.append('          [%.5f, %.2f],\n' % (termCount/1000000., sortTimeMS))

    # Collate ingest rate at exact msec:
    for docCount, msec in docsCount:
      if msec not in allMS:
        allMS[msec] = []
      allMS[msec].append((mb, docCount))

    allMB.append(mb)

  allMB.sort()
  cols = ['Time (s)'] + ['%d MB' % x for x in allMB]
  allMSKeys = list(allMS.keys())
  allMSKeys.sort()
  dataLines6 = [str(cols) + ',\n']
  for ms in allMSKeys:
    l = [ms/1000.]
    for mb in allMB:
      value = None
      for i, (mb2, docCount) in enumerate(allMS[ms]):
        if mb2 == mb:
          value = docCount
          del allMS[ms][i]
          break
      if value is None:
        s = 'null'
      else:
        s = value
      l.append(s)
    dataLines6.append('        %s,\n' % str(l).replace("'null'", 'null'))
        
  with open('/x/tmp/flushTimes.html', 'w') as f:
    f.write(chartHTML % (''.join(dataLines1).strip(),
                         ''.join(dataLines2).strip(),
                         ''.join(dataLines3).strip(),
                         ''.join(dataLines4).strip(),
                         ''.join(dataLines5).strip(),
                         ''.join(dataLines6).strip(),
                         ))

if not os.path.exists('results.pk'):
  cmd = 'java -verbose:gc -Xms8g -Xmx8g -server -classpath "/l/trunk/lucene/build/core/classes/java:/l/trunk/lucene/build/core/classes/test:/l/trunk/lucene/build/sandbox/classes/java:/l/trunk/lucene/build/misc/classes/java:/l/trunk/lucene/build/facet/classes/java:/home/mike/src/l-c-boost/dist/lCBoost-SNAPSHOT.jar:/l/trunk/lucene/build/analysis/common/classes/java:/l/trunk/lucene/build/analysis/icu/classes/java:/l/trunk/lucene/build/queryparser/classes/java:/l/trunk/lucene/build/grouping/classes/java:/l/trunk/lucene/build/suggest/classes/java:/l/trunk/lucene/build/highlighter/classes/java:/l/trunk/lucene/build/codecs/classes/java:/l/trunk/lucene/build/queries/classes/java:/l/util/lib/HdrHistogram.jar:/l/util/build" perf.Indexer -dirImpl MMapDirectory -indexPath "/l/tmp/index" -analyzer StandardAnalyzerNoStopWords -lineDocsFile %s -docCountLimit 33332620 -threadCount %s -maxConcurrentMerges 1 -ramBufferMB %%s -maxBufferedDocs -1 -postingsFormat Lucene84 -mergePolicy NoMergePolicy -idFieldPostingsFormat Lucene84 -verbose -repeatDocs -store -tvs' % (DATA_FILE, THREAD_COUNT)

  reSegSizeMB = re.compile('ramUsed=([0-9,.]+) MB newFlushedSize=([0-9,.]+) MB')
  reFlushTime = re.compile('flush time ([0-9.]+) msec')
  reTermCount = re.compile('has ([0-9]+) unique terms ([0-9.]+) msec to sort')
  reIndexedCount = re.compile(r'Indexer: ([0-9]+) docs... \(([0-9]+) msec\)')

  results = []

  for iter in range(200):

    if os.path.exists('/l/tmp/index'):
      while True:
        try:
          shutil.rmtree('/l/tmp/index')
        except OSError:
          # hmm why :)
          time.sleep(1.0)
        else:
          break

    mb = random.randint(8, 1536)
    # nocommit
    #mb = random.randint(8, 64)

    print('IW buffer: %.1f MB' % mb)
    
    p = subprocess.Popen(cmd % mb, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    termCount = 0
    flushCount = 0
    sortTimeMS = 0.0
    startTime = time.time()
    docsCount = []
    while flushCount < 5 or (time.time()-startTime) < 90.0:
      line = p.stdout.readline().decode('utf-8')
      if line == '':
        print('FAILED:')
        break

      print(line.strip())
      
      m = reIndexedCount.search(line)
      if m is not None:
        docsCount.append((int(m.group(1)), int(m.group(2))))

      m = reSegSizeMB.search(line)
      if m is not None:
        ramSizeMB = float(m.group(1).replace(',', ''))
        diskSizeMB = float(m.group(2).replace(',', ''))

      m = reTermCount.search(line)
      if m is not None:
        termCount += int(m.group(1))
        sortTimeMS += float(m.group(2))

      m = reFlushTime.search(line)
      if m is not None:
        flushTimeMS = float(m.group(1))
        print('RESULT: %.1fs: %s MB IW buffer: %.1f MB, %.1f MB, %.1f msec, %.1f MB/sec, %.2f M unique terms, %.2f ms sort time' % \
              (time.time()-startTime, mb, ramSizeMB, diskSizeMB, flushTimeMS, diskSizeMB/(flushTimeMS/1000.0), termCount/1000000.0, sortTimeMS))
        flushCount += 1
        if flushCount > 2 and time.time()-startTime >= 10.0:
          results.append((mb, ramSizeMB, diskSizeMB, flushTimeMS, termCount, sortTimeMS, docsCount))
          pickle.dump(results, open('results.pk.new', 'wb'))
          if os.path.exists('results.pk'):
            os.remove('results.pk')
          os.rename('results.pk.new', 'results.pk')
          writeChart(results)
        termCount = 0
        sortTimeMS = 0

    p.kill()
    p.wait()
else:
  results = pickle.load(open('results.pk', 'rb'))
  writeChart(results)

