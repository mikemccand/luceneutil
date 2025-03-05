import datetime
import os
import re
import shutil
import sys
import time

import constants

reCompletedTestCountGradle = re.compile(r": (\d+) test\(s\)")
reCompletedTestCountAnt = re.compile(r", (\d+) tests")

DEBUG = False

BASE_DIR = constants.BASE_DIR

if DEBUG:
  NIGHTLY_DIR = "clean2.svn"
else:
  NIGHTLY_DIR = "trunk.nightly"

LOGS_DIR = "%s/logs.nightly/ant_test" % BASE_DIR
# LOGS_DIR = '/x/tmp/beast.logs/logs.nightly/ant_test'

KNOWN_CHANGES_ANT_TEST = [
  ("2014-05-04", "Lucene Tests Minutes", "Switched from Java 1.7.0_65 to 1.8.0_40"),
  ("2014-10-15", "Lucene Tests Minutes", "The Great Test Slowdown of 2014"),
  ("2021-10-19", "Precommit Minutes", "Add -release option to ecj"),
  ("2021-11-30", "Precommit Minutes", "Switch to counting aggregate task time instead of wall clock elapsed time"),
  ("2021-11-30", "Lucene Tests Minutes", "Switch to counting aggregate task time instead of wall clock elapsed time"),
]


def clean(logFile):
  if os.system(f"git clean -xfd lucene solr >> {logFile}.tmp 2>&1"):
    raise RuntimeError(f"git clean -xfd lucene solr failed!  see {logFile}")

  # nightlyBench.py leaves this:
  cleanLogFile = f"{BASE_DIR}/{NIGHTLY_DIR}/clean.log"
  if os.path.exists(cleanLogFile):
    os.remove(cleanLogFile)


def runLuceneTests(logFile):
  # nocommit svn up to the timestamp:
  # os.chdir('%s/%s/lucene' % (BASE_DIR, NIGHTLY_DIR))
  os.chdir("%s/%s" % (BASE_DIR, NIGHTLY_DIR))

  print("  gradle test log: %s" % logFile)
  open(logFile + ".tmp", "w").write("git rev: %s\n\n" % os.popen("git rev-parse HEAD").read().strip())
  open(logFile + ".tmp", "a").write("\n\njava version: %s\n\n" % os.popen("java -fullversion 2>&1").read())

  # Goodbye ant, hello gradle!
  # if not os.system('ant clean test -Dtests.jvms=%s >> %s.tmp 2>&1' % (constants.PROCESSOR_COUNT, logFile)):
  clean(logFile)

  # There is some weird gradle bootstrapping bug: if we do not run this "help" first, then the test run fails w/ cryptic error:
  os.system("./gradlew help >> %s.tmp 2>&1" % logFile)

  t0 = time.time()

  if not os.system("./gradlew --no-daemon -Ptask.times=true -p lucene test >> %s.tmp 2>&1" % logFile):
    # Success
    t1 = time.time()
    open(logFile + ".tmp", "a").write("\nTOTAL SEC: %s" % (t1 - t0))
    os.rename(logFile + ".tmp", logFile)
    print("  took: %.1f min" % ((t1 - t0) / 60.0))
  else:
    print("FAILED; see %s.tmp" % logFile)


def runPrecommit(logFile):
  os.chdir("%s/%s" % (BASE_DIR, NIGHTLY_DIR))

  print("  gradle precommit log: %s" % logFile)
  open(logFile + ".tmp", "w").write("git rev: %s\n\n" % os.popen("git rev-parse HEAD").read().strip())
  open(logFile + ".tmp", "a").write("\n\njava version: %s\n\n" % os.popen("java -fullversion 2>&1").read())

  clean(logFile)

  # See https://issues.apache.org/jira/browse/LUCENE-9670 -- retry up to 5 times in case this weird IOException: Stream Closed issue struck:

  for i in range(5):
    if os.system("git clean -xfd lucene solr >> %s.tmp 2>&1" % logFile):
      raise RuntimeError("git clean -xfd lucene solr failed!")

    t0 = time.time()
    if not os.system("./gradlew --stacktrace precommit -Ptask.times=true >> %s.tmp 2>&1" % logFile):
      # Success
      t1 = time.time()
      open(logFile + ".tmp", "a").write("\nTOTAL SEC: %s" % (t1 - t0))
      os.rename(logFile + ".tmp", logFile)
      print("  took: %.1f min" % ((t1 - t0) / 60.0))
      break
    elif i < 4:
      print(f"FAILED; see {logFile}.tmp; will try again ({5 - i - 1} attempts remain)")
    else:
      print("FAILED; see %s.tmp" % logFile)


def getTestCount(line, regexp):
  m = regexp.search(line)
  if m is not None:
    return int(m.group(1))
  else:
    return -1


def pick_seconds(tup):
  elapsed_seconds, aggregate_task_seconds = tup[2:]
  # on Nov 30 2021 we switched to summing aggregate_tasks_seconds so the many-cored beast3 doesn't hide progress:
  if aggregate_task_seconds is not None:
    return aggregate_task_seconds
  else:
    return elapsed_seconds


def writeGraph():
  logFiles = os.listdir(LOGS_DIR)
  logFiles.sort()

  reLogFile = re.compile(r"(\d\d\d\d)\.(\d\d)\.(\d\d)(?:\.(.*?))?\.txt")

  test_results = []
  precommit_results = []

  for logFile in logFiles:
    if logFile.endswith(".txt.tmp"):
      continue
    if logFile.startswith("#"):
      continue
    if not logFile.endswith(".txt"):
      raise RuntimeError('unexpected file "%s"' % logFile)

    m = reLogFile.match(logFile)
    if m is None:
      raise RuntimeError(f'could not understand log file "{LOGS_DIR}/{logFile}"')

    timestamp = datetime.datetime(year=int(m.group(1)), month=int(m.group(2)), day=int(m.group(3)))

    what = m.group(4)

    if what in (None, "lucene-tests"):
      # back compat!
      what = "lucene-tests"
      results = test_results
    else:
      results = precommit_results

    aggregate_time_sec = None
    with open("%s/%s" % (LOGS_DIR, logFile)) as f:
      totalTests = 0
      in_aggregate_task_times = False
      for line in f.readlines():
        if what == "lucene-tests":
          testCount = getTestCount(line, reCompletedTestCountGradle)
          if testCount <= 0:
            testCount = getTestCount(line, reCompletedTestCountAnt)
            if testCount < 0:
              testCount = 0
            elif "Tests summary" in line:
              # do not over-count ant "summary" output lines:
              testCount = 0
          totalTests += testCount

        if line.startswith("Aggregate task times "):
          # e.g.:
          """
Aggregate task times (possibly running in parallel!):
 496.12 sec.  test
  27.65 sec.  compileJava
  13.11 sec.  compileTestJava
   2.97 sec.  jar
   1.55 sec.  processResources
   0.80 sec.  copyTestResources
   0.47 sec.  gitStatus
   0.17 sec.  processTestResources
   0.07 sec.  compileTestFixturesJava
   0.04 sec.  wipeTaskTemp
   0.02 sec.  randomizationInfo
   0.01 sec.  testFixturesJar
   0.00 sec.  errorProneSkipped

"""
          in_aggregate_task_times = True
          aggregate_time_sec = 0
          continue
        elif in_aggregate_task_times:
          if len(line.strip()) == 0:
            # ends with empty line
            in_aggregate_task_times = False
          else:
            m = re.match("([0-9.]+) sec.*", line.strip())
            if m is None:
              raise RuntimeError(f'failed to parse "{line}" as an aggregate task time entry (file={LOGS_DIR}/{logFile})')
            aggregate_time_sec += float(m.group(1))

        if line.startswith("TOTAL SEC: "):
          results.append((timestamp, totalTests, float(line[11:].strip()), aggregate_time_sec))
          break
      else:
        raise RuntimeError("couldn't find total seconds for %s/%s" % (LOGS_DIR, logFile))

  test_results.sort()
  precommit_results.sort()

  with open("/x/tmp/antcleantest.html", "w") as f:
    w = f.write

    w("<html>")
    w("<head>")
    w('<title>Minutes for "gradle -p lucene test" and "gradle precommit" in lucene</title>')
    w('<style type="text/css">')
    w("BODY { font-family:verdana; }")
    w("#chart_ant_clean_test_time {\n")
    w("  position: absolute;\n")
    w("  left: 10px;\n")
    w("}\n")
    w("#chart_ant_clean_test_time_labels {\n")
    w("  position: absolute;\n")
    w("  left: 100px;\n")
    w("  top: 100px;\n")
    w("}\n")
    w("</style>")
    w('<script type="text/javascript" src="dygraph-combined.js"></script>\n')
    w("</head>")
    w("<body>")

    w("""
    <a name="antcleantest"></a>
    <table><tr>
    <td><div id="chart_ant_clean_test_time" style="height:70%; width: 98%"></div></td>
    <td><br><br><div id="chart_ant_clean_test_time_labels" style="width:250px; height:500px"></div></td>
    </tr></table>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("chart_ant_clean_test_time"),
    """)

    if False:
      headers = ["Date", "Count (thousands)", "Time (minutes)"]
      w('    "%s\\n"\n' % ",".join(headers))
    else:
      w('    ""\n')

    precommit_upto = 0
    tests_upto = 0

    while precommit_upto < len(precommit_results) or tests_upto < len(test_results):
      # merge sort tests time / precommit time:
      if tests_upto < len(test_results):
        date, total_tests = test_results[tests_upto][:2]

        tests_seconds = pick_seconds(test_results[tests_upto])

        if date == precommit_results[precommit_upto][0]:
          precommit_minutes_str = f"{pick_seconds(precommit_results[precommit_upto]) / 60.0:.3f}"
          precommit_upto += 1
          tests_upto += 1
        elif date > precommit_results[precommit_upto][0]:
          precommit_minutes_str = f"{pick_seconds(precommit_results[precommit_upto]) / 60.0:.3f}"
          date = precommit_results[precommit_upto][0]
          total_tests = None
          precommit_upto += 1
          tests_seconds = None
        else:
          precommit_minutes_str = ""
          tests_upto += 1
      else:
        date = precommit_results[precommit_upto][0]
        total_tests = None
        tests_seconds = None
        precommit_minutes_str = f"{pick_seconds(precommit_results[precommit_upto]) / 60.0:.3f}"
        precommit_upto += 1

      if total_tests is None:
        total_tests_str = ""
        total_tests_rate_str = ""
      else:
        total_tests_str = "%.3f" % (total_tests / 1000.0)
        total_tests_rate_str = "%.3f" % (float(total_tests) / (tests_seconds / 60.0) / 1000.0)

      if tests_seconds is None:
        tests_seconds_str = ""
      else:
        tests_seconds_str = "%.3f" % (tests_seconds / 1000.0)

      w('    + "%4d-%02d-%02d,%s,%s,%s,%s\\n"\n' % (date.year, date.month, date.day, total_tests_str, tests_seconds_str, precommit_minutes_str, total_tests_rate_str))

    w(""",
    { "title": "Time for \'gradle -p lucene test\'",
      "labels": ["Date", "Count (thousands)", "Lucene Tests Minutes", "Precommit Minutes", "Tests per minute (thousands)"],
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
            return x.toFixed(1);
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
      """)

    if True:
      w("g.ready(function() {g.setAnnotations([")
      label = 0
      for date, series, reason in KNOWN_CHANGES_ANT_TEST:
        shortText = getLabel(label)
        label += 1
        w('{series: "%s", x: "%s", shortText: "%s", text: "%s"},' % (series, date, shortText, reason))
      w("]);});\n")

    w("</script>")
    w('<div style="position: absolute; top: 77%">\n')
    w('<br><em>[last updated: %s; send questions to <a href="mailto:lucene@mikemccandless.com">Mike McCandless</a>]</em>' % datetime.datetime.now())
    w("</div>")
    w("</body>")
    w("</html>")

  if constants.NIGHTLY_REPORTS_DIR != "/x/tmp":
    shutil.copy("/x/tmp/antcleantest.html", "%s/antcleantest.html" % constants.NIGHTLY_REPORTS_DIR)


def getLabel(label):
  if label < 26:
    s = chr(65 + label)
  else:
    s = "%s%s" % (chr(65 + (label / 26 - 1)), chr(65 + (label % 26)))
  return s


def run(cmd):
  if os.system(cmd):
    raise RuntimeError("%s failed" % cmd)


def getLogFile(then, what):
  return "%s/%4d.%02d.%02d.%s.txt" % (LOGS_DIR, then.year, then.month, then.day, what)


if __name__ == "__main__":
  if "-chart" in sys.argv:
    writeGraph()
  else:
    runLuceneTests(getLogFile(datetime.datetime.now(), "lucene-tests"))
    runPrecommit(getLogFile(datetime.datetime.now(), "precommit"))
    writeGraph()
