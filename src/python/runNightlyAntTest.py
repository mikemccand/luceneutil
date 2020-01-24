import pysftp
import re
import shutil
import time
import os
import datetime
import sys
import constants

reCompletedTestCount = re.compile(r', (\d+) tests')

DEBUG = False

BASE_DIR = constants.BASE_DIR

if DEBUG:
  NIGHTLY_DIR = 'clean2.svn'
else:
  NIGHTLY_DIR = 'trunk.nightly'

LOGS_DIR = '%s/logs.nightly/ant_test' % BASE_DIR
#LOGS_DIR = '/x/tmp/beast.logs/logs.nightly/ant_test'

KNOWN_CHANGES_ANT_TEST = [
  ('2014-05-04', 'Switched from Java 1.7.0_65 to 1.8.0_40'),
  ('2014-10-15', 'The Great Test Slowdown of 2014')
  ]

def runOneDay(logFile):

  # nocommit svn up to the timestamp:
  #os.chdir('%s/%s/lucene' % (BASE_DIR, NIGHTLY_DIR))
  os.chdir('%s/%s' % (BASE_DIR, NIGHTLY_DIR))

  print('  ant test log: %s' % logFile)
  open(logFile + '.tmp', 'w').write('svn rev: %s\n\n' % os.popen('svnversion 2>&1').read())
  open(logFile + '.tmp', 'a').write('\n\njava version: %s\n\n' % os.popen('java -fullversion 2>&1').read())

  if os.system('git clean -xfd >> %s.tmp 2>&1' % logFile):
    raise RuntimeError('git clean -xfd failed!')
  
  t0 = time.time()
  # Goodbye ant, hello gradle!
  #if not os.system('ant clean test -Dtests.jvms=%s >> %s.tmp 2>&1' % (constants.PROCESSOR_COUNT, logFile)):

  # There is some weird gradle bootstrapping bug: if we do not run this "help" first, then the test run fails w/ cryptic error:
  os.system('./gradlew help >> %s.tmp 2>&1' % logFile)
  
  if not os.system('./gradlew -p lucene test >> %s.tmp 2>&1' % logFile):
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
    if logFile.startswith('#'):
      continue
    if not logFile.endswith('.txt'):
      raise RuntimeError('unexpected file "%s"' % logFile)
    tup = tuple(int(x) for x in logFile[:-4].split('.'))
    timestamp = datetime.datetime(year = tup[0],
                                  month = tup[1],
                                  day = tup[2])
    totalTests = 0
    with open('%s/%s' % (LOGS_DIR, logFile)) as f:
      for line in f.readlines():
        m = reCompletedTestCount.search(line)
        if m is not None:
          totalTests += int(m.group(1))
        if line.startswith('TOTAL SEC: '):
          results.append((timestamp, totalTests, float(line[11:].strip())))
          break
      else:
        raise RuntimeError("couldn't find total seconds for %s/%s" % (LOGS_DIR, logFile))

  results.sort()

  with open('/x/tmp/antcleantest.html', 'w') as f:
    w = f.write
    
    w('<html>')
    w('<head>')
    w('<title>Minutes for "ant clean test" in lucene</title>')
    w('<style type="text/css">')
    w('BODY { font-family:verdana; }')
    w('#chart_ant_clean_test_time {\n')
    w('  position: absolute;\n')
    w('  left: 10px;\n')
    w('}\n')
    w('#chart_ant_clean_test_time_labels {\n')
    w('  position: absolute;\n')
    w('  left: 100px;\n')
    w('  top: 100px;\n')
    w('}\n')
    w('</style>')
    w('<script type="text/javascript" src="dygraph-combined.js"></script>\n')
    w('</head>')
    w('<body>')
    
    w('''
    <a name="antcleantest"></a>
    <table><tr>
    <td><div id="chart_ant_clean_test_time" style="height:70%; width: 98%"></div></td>
    <td><br><br><div id="chart_ant_clean_test_time_labels" style="width:250px; height:500px"></div></td>
    </tr></table>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("chart_ant_clean_test_time"),
    ''')

    if False:
      headers = ['Date', 'Count (thousands)', 'Time (minutes)']
      w('    "%s\\n"\n' % ','.join(headers))
    else:
      w('    ""\n')

    for date, totalTests, seconds in results:
      w('    + "%4d-%02d-%02d,%s,%s,%.2f\\n"\n' % (date.year, date.month, date.day, totalTests/1000.0, seconds/60.0, float(totalTests)/(seconds/60.0)/1000.))

    w(''',
    { "title": "Time for \'ant clean test\'",
      "labels": ["Date", "Count (thousands)", "Minutes", "Tests per minute (thousands)"],
      "series": {
        "Count (thousands)": {
          "axis": "y2"
        },
        "Tests per minute (thousands)": {
          "axis": "y2"
        }
      },
      "axes": {
        "y2": {
          "axisLabelFormatter": function(x) {
            return x.toFixed(0);
          }
        }
      },
      "includeZero": true,
      "xlabel": "Date",
      "ylabel": "Minutes",
      "y2label": "Count (thousands)",
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
        series = 'Minutes'
        shortText = getLabel(label)
        label += 1
        w('{series: "%s", x: "%s", shortText: "%s", text: "%s"},' % \
                (series, date, shortText, reason))
      w(']);});\n')

    w('</script>')
    w('<div style="position: absolute; top: 77%">\n')
    w('<br><em>[last updated: %s; send questions to <a href="mailto:lucene@mikemccandless.com">Mike McCandless</a>]</em>' % datetime.datetime.now())
    w('</div>')
    w('</body>')
    w('</html>')

  if constants.NIGHTLY_REPORTS_DIR != '/x/tmp':
    shutil.copy('/x/tmp/antcleantest.html', '%s/antcleantest.html' % constants.NIGHTLY_REPORTS_DIR)

def getLabel(label):
  if label < 26:
    s = chr(65+label)
  else:
    s = '%s%s' % (chr(65+(label/26 - 1)), chr(65 + (label%26)))
  return s
              
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
  #then = datetime.datetime(year=2014, month=5, day=24)
  os.chdir('%s/%s' % (BASE_DIR, NIGHTLY_DIR))
  
  while True:
    print('\n%s: now back-test %s' % (datetime.datetime.now(), then))
    logFile = getLogFile(then)
    if not os.path.exists(logFile):
      run('python -u /home/mike/src/util/svnClean.py .')
      run('svn up -r {%s}' % then.strftime('%Y-%m-%d'))
      runOneDay(logFile)
      writeGraph()
    else:
      print('  already done')
    
    then = then - datetime.timedelta(days=1)

def copyChart():
  with pysftp.Connection('home.apache.org', username='mikemccand') as c:
    with c.cd('public_html/lucenebench'):
      #c.mkdir('lucenebench')
      # TODO: this is not incremental...
      c.put('%s/antcleantest.html' % constants.NIGHTLY_REPORTS_DIR, 'antcleantest.html')

if __name__ == '__main__':
  if '-chart' in sys.argv: 
    writeGraph()
    copyChart()
  elif '-backTest' in sys.argv:
    print('\nNow run nightly ant test')
    backTest()
  else:
    runOneDay(getLogFile(datetime.datetime.now()))
    writeGraph()
    copyChart()
