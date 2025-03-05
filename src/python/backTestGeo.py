import datetime
import os
import shutil

oneDay = datetime.timedelta(days=1)

GEO_LOGS_DIR = "/l/logs.nightly/geo"

os.chdir("/l/trunk.nightly/lucene")

commits = []
with os.popen("git log --format=fuller", "r") as f:
  while True:
    line = f.readline()
    if len(line) == 0:
      break
    line = line.rstrip()
    if line.startswith("commit "):
      commitHash = line[7:]
      # Author
      f.readline()
      # AuthorDate
      f.readline()
      # Committer
      f.readline()
      # Committer
      line = f.readline()
      if line.startswith("CommitDate: "):
        s = line[12:].strip()
        commitTime = datetime.datetime.strptime(s, "%a %b %d %H:%M:%S %Y %z")
        # if len(commits) > 0 and commitTime > commits[-1][1]:
        #  print('wrong order: %s -> %s vs %s, %s' % (s, commitTime, commits[-1][1], commits[-1]))
        commits.append((commitHash, commitTime))
        # print('got: %s, %s' % (commitHash, commitTime))


def run(cmd):
  if os.system(cmd):
    raise RuntimeError('command "%s" failed' % cmd)


datesTested = set()
for name in os.listdir(GEO_LOGS_DIR):
  if name.endswith(".log.txt.bz2"):
    tup = list(int(x) for x in name.split(".")[:3])
    date = datetime.date(year=tup[0], month=tup[1], day=tup[2])
    # print('already done: %s' % date)
    datesTested.add(date)

lastTestedDate = None

upto = 0
print("HERE: %s" % str(commits[0]))

while upto < len(commits):
  # Go back to last commit the day before:
  if lastTestedDate is not None:
    while True:
      hash, commitTime = commits[upto]
      oldSrc = "/l/util/src/extra/perf/backtest/IndexAndSearchOpenStreetMaps.java.%s" % hash
      if os.path.exists(oldSrc):
        print("  switch to %s" % oldSrc)
        shutil.copy(oldSrc, "/l/util/src/extra/perf/IndexAndSearchOpenStreetMaps.java")

      utc = commitTime.utctimetuple()
      date = datetime.date(year=utc[0], month=utc[1], day=utc[2])
      # print('cmp %s vs %s' % (date, lastTestedDate))
      if date != lastTestedDate:
        break
      upto += 1

  hash, commitTimeStamp = commits[upto]
  upto += 1

  utc = commitTimeStamp.utctimetuple()

  ts = "%04d-%02d-%02d %02d:%02d:%02d" % utc[:6]
  lastTestedDate = datetime.date(year=utc[0], month=utc[1], day=utc[2])

  print("\n%s TEST: %s, %s" % (datetime.datetime.now(), hash, commitTimeStamp))
  if lastTestedDate in datesTested:
    print("  already done: %s" % lastTestedDate)
  else:
    run("git checkout %s" % hash)
    run("git clean -xfd")
    run("ant jar")
    run("python3 -u /l/util/src/python/runGeoBenches.py -nightly -timeStamp %s" % (ts.replace("-", ".").replace(":", ".").replace(" ", ".")))
    run("python3 -u /l/util/src/python/writeGeoGraphs.py")
