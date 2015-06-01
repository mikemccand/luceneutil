import time
import os
import datetime

DEBUG = False

BASE_DIR = '/lucene'

if DEBUG:
  NIGHTLY_DIR = 'clean2.svn'
else:
  NIGHTLY_DIR = 'trunk.nightly'

LOGS_DIR = '%s/logs.nightly/ant_test' % BASE_DIR

KNOWN_CHANGES_ANT_TEST = []

def runOneDay(logFile):

  # nocommit svn up to the timestamp:
  os.chdir('%s/%s/lucene' % (BASE_DIR, NIGHTLY_DIR))

  print('  log: %s' % logFile)
  open(logFile + '.tmp', 'w').write('svn rev: %s\n\n' % os.popen('svnversion 2>&1').read())
  open(logFile + '.tmp', 'a').write('\n\njava version: %s\n\n' % os.popen('java -fullversion 2>&1').read())
  
  t0 = time.time()
  if not os.system('ant clean test -Dtests.jvms=12 >> %s.tmp 2>&1' % logFile):
    # Success
    t1 = time.time()
    open(logFile + '.tmp', 'a').write('\nTOTAL SEC: %s' % (t1-t0))
    os.rename(logFile + '.tmp', logFile)
    print('  took: %.1f min' % ((t1-t0)/60.0))
  else:
    print('FAILED; see %s.tmp' % logFile)

def writeGraph():
  logFiles = os.listdir(LOGS_DIR)
  logFiles.sort()

  results = []
  for logFile in logFiles:
    if logFile.endswith('.txt.tmp'):
      continue
    if not logFile.endswith('.txt'):
      raise RuntimeError('unexpected file "%s"' % logFile)
    tup = tuple(int(x) for x in logFile[:-4].split('.'))
    timestamp = datetime.datetime(year = tup[0],
                                  month = tup[1],
                                  day = tup[2])
    with open('%s/%s' % (LOGS_DIR, logFile)) as f:
      for line in f.readlines():
        if line.startswith('TOTAL SEC: '):
          results.append((timestamp, float(line[11:].strip())))
          break
      else:
        raise RuntimeError("couldn't find total seconds for %s/%s" % (LOGS_DIR, logFile))

  results.sort()

  with open('/x/tmp/anttest.html', 'w') as f:
    w = f.write
    
    w('<html>')
    w('<head>')
    w('<title>Minutes for "ant clean test" in lucene</title>')
    w('<style type="text/css">')
    w('BODY { font-family:verdana; }')
    w('</style>')
    w('<script type="text/javascript" src="dygraph-combined.js"></script>\n')
    w('</head>')
    w('<body>')
    
    w('''
    <a name="antcleantest"></a>
    <table><tr>
    <td><div id="chart_ant_clean_test_time" style="width:800px; height:500px"></div></td>
    <td><br><br><div id="chart_ant_clean_test_time_labels" style="width:250px; height:500px"></div></td>
    </tr></table>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("chart_ant_clean_test_time"),
    ''')

    headers = ['Date', 'Time (minutes)']
    w('    "%s\\n"\n' % ','.join(headers))

    for date, seconds in results:
      w('    + "%4d-%02d-%02d,%s\\n"\n' % (date.year, date.month, date.day, seconds/60.0))

    w(''',
    { "title": "Time for \'ant clean test\'",
      "includeZero": true,
      "xlabel": "Date",
      "ylabel": "Minutes",
      "connectSeparatedPoints": true,
      "hideOverlayOnMouseOut": false,
      "labelsDiv": "chart_ant_clean_test_time_labels",
      "labelsSeparateLines": true,
      "legend": "always",
      }
      );
      ''')

    if True:
      w('g.ready(function() {g.setAnnotations([')
      label = 0
      for tup in KNOWN_CHANGES_ANT_TEST:
        date, reason = tup[:2]
        series = 'Time (minutes)'
        shortText = getLabel(label)
        label += 1
        w('{series: "%s", x: "%s", shortText: "%s", text: "%s"},' % \
                (series, date, shortText, reason))
      w(']);});\n')

    w('</script>')
    w('<br><em>[last updated: %s; send questions to <a href="mailto:lucene@mikemccandless.com">Mike McCandless</a>]</em>' % datetime.datetime.now())
    w('</body>')
    w('</html>')

def run(cmd):
  if os.system(cmd):
    raise RuntimeError('%s failed' % cmd)

def getLogFile(then):
  return '%s/%4d.%02d.%02d.txt' % \
         (LOGS_DIR,
          then.year,
          then.month,
          then.day)

def backTest():
  then = datetime.datetime.now().date()
  os.chdir('%s/%s' % (BASE_DIR, NIGHTLY_DIR))
  
  while True:
    print('\n%s: now back-test %s' % (datetime.datetime.now(), then))
    logFile = getLogFile(then)
    if not os.path.exists(logFile):
      run('python -u /home/mike/src/util/svnClean.py .')
      run('svn up -r {%s}' % then.strftime('%Y-%m-%d'))
      runOneDay(logFile)
    else:
      print('  already done')

    then = then - datetime.timedelta(days=1)
    
if __name__ == '__main__':
  print('\nNow run nightly ant test')
  #backTest()
  #runOneDay(getLogFile(datetime.datetime.now()))
  writeGraph()
