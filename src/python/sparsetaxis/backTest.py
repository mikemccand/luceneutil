import datetime
import mailbox
import os
import re
import shutil

import writeGraph

TAXIS_LOGS_DIR = "/l/logs.nightly/taxis"


def run(cmd):
  if os.system(cmd):
    raise RuntimeError('command "%s" failed' % cmd)


os.chdir("/l/sparseTaxis/sparseTaxis/lucene.master/lucene")
run("git checkout master")
run("git pull origin master")


def findCommitsToTest():
  # Get the lucene-commits mbox archives, e.g.:
  #
  #   wget http://mail-archives.apache.org/mod_mbox/lucene-commits/201611.mbox

  reMaster = re.compile("^  refs/heads/master ([0-9a-f]+) -> ([0-9a-f]+)$", re.MULTILINE)

  print("  parse commit hashes from lucene-commits mbox archives...")

  # master commit hashes pushed
  masterCommits = set()

  for month in 6, 7, 8, 9, 10, 11:
    m = mailbox.mbox("/lucenedata/apache-lucene-commits-mbox/2016%02d.mbox" % month)

    for message in m:
      p = message.get_payload()
      if type(p) is str:
        p = [p]
      for x in p:
        matches = reMaster.findall(str(x))
        for fromHash, toHash in matches:
          # print("fromHash %s" % fromHash)
          masterCommits.add(toHash)

  masterCommitsAndTimes = []

  # now match up those commit hashes we found in the emails, to timestamps:

  print("  match to timestamps...")

  with os.popen("git log --format=fuller --parents", "r") as f:
    line = f.readline().strip()

    commits = {}
    childCount = {}
    firstCommit = None

    while True:
      # print("got: %s" % line)
      tup = line.split()
      if tup[0] != "commit":
        raise RuntimeError("expected commit but saw %s" % line)

      hash = tup[1]
      if hash in commits:
        raise RuntimeError("duplicate commit hash %s" % hash)
      parentHashes = tup[2:]
      if len(parentHashes) not in (0, 1, 2):
        raise RuntimeError("expected 0 or 1 or 2 parents but saw %s" % line)
      author = f.readline().strip()
      if author.startswith("Merge: "):
        author = f.readline().strip()
      authorDate = f.readline().strip()
      commitUser = f.readline().strip()
      commitDate = f.readline()[11:].strip()
      comments = []

      commits[hash] = (commitUser, commitDate, parentHashes, comments)

      f.readline()
      while True:
        line = f.readline()
        if line == "":
          break
        if line.rstrip().startswith("commit "):
          break
        comments.append(line)

      if firstCommit is None:
        firstCommit = hash

      if hash[:9] in masterCommits:
        # parse commit time, and convert to UTC
        t = datetime.datetime.strptime(commitDate, "%a %b %d %H:%M:%S %Y %z")
        t = datetime.datetime(*t.utctimetuple()[:6])
        masterCommitsAndTimes.append((t, hash, "".join(comments).rstrip()))

      if line == "":
        break

  # This was pushed along with another change but we want to separately test them if possible:
  masterCommitsAndTimes.append((datetime.datetime(2016, 8, 3, 12, 34, 6), "234ea3ef8954325923f4e85c5c0aa72c3bb15baa", "LUCENE-7403: Use blocks of exactly maxPointsInLeafNodes values in the 1D case."))

  # So was this one:
  masterCommitsAndTimes.append((datetime.datetime(2016, 7, 4, 7, 13, 41), "d66e9935c39ed859659de46d3d5cfb66f2279bd4", "LUCENE-7351: Doc id compression for points"))

  print("found %d commit + times vs %d commits only" % (len(masterCommitsAndTimes), len(masterCommits)))

  masterCommitsAndTimes.sort(reverse=True)
  return [(y, x, z) for x, y, z in masterCommitsAndTimes]


commits = findCommitsToTest()

upto = 0
timeToCommitIndex = {}

for upto, (hash, commitTimeStamp, comments) in enumerate(commits):
  timestamp = datetime.datetime(*commitTimeStamp.utctimetuple()[:6])

  print("%s -> %s, %s\n%s" % (timestamp, upto, hash, comments))
  timeToCommitIndex[timestamp] = upto

timesAlreadyDone = set()
for name in os.listdir(TAXIS_LOGS_DIR):
  if os.path.isdir("%s/%s" % (TAXIS_LOGS_DIR, name)):
    if os.path.exists("%s/%s/results.pk" % (TAXIS_LOGS_DIR, name)):
      tup = list(int(x) for x in name.split("."))
      timestamp = datetime.datetime(*tup)
      print("already done: %s" % timestamp)
      timesAlreadyDone.add(timestamp)


def getTimesToTest():
  """Carefully enumerates commit timestamps in such a way that we test large gaps first, then smaller
  gaps, and eventually all commits in the range.
  """
  # First, make sure we test all explicitly annotated commits in the graph:
  for timestampString, desc in writeGraph.CHANGES:
    t = datetime.datetime.strptime(timestampString, "%Y-%m-%d %H:%M:%S")
    if t not in timeToCommitIndex:
      raise RuntimeError("timestamp %s is missing from commits but is in CHANGES: %s" % (t, desc))

  for timestampString, desc in writeGraph.CHANGES:
    t = datetime.datetime.strptime(timestampString, "%Y-%m-%d %H:%M:%S")
    if t not in timesAlreadyDone:
      yield t

  while True:
    totalMaxMinTimestamp = None
    totalMaxMinDistance = None
    for hash, timestamp, comment in commits:
      if timestamp in timesAlreadyDone:
        continue

      # consider choosing timestamp; find the closest timestamp already done, to this candidate:
      minDistance = None
      for timestamp2 in timesAlreadyDone:
        distance = abs((timestamp - timestamp2).total_seconds())
        if minDistance is None or distance < minDistance:
          minDistance = distance

      if totalMaxMinDistance is None or minDistance > totalMaxMinDistance:
        totalMaxMinDistance = minDistance
        totalMaxMinTimestamp = timestamp

    if totalMaxMinTimestamp is None:
      break

    yield totalMaxMinTimestamp


for timestamp in getTimesToTest():
  hash, commitTimeStamp, comment = commits[timeToCommitIndex[timestamp]]

  utc = commitTimeStamp.utctimetuple()

  ts = "%04d.%02d.%02d.%02d.%02d.%02d" % utc[:6]

  print("\n%s TEST: %s, %s" % (datetime.datetime.now(), hash, commitTimeStamp))
  timesAlreadyDone.add(timestamp)
  logDir = "/l/logs.nightly/taxis/%s" % ts
  if os.path.exists(logDir):
    print("  remove old partial log dir %s" % logDir)
    shutil.rmtree(logDir)

  run("git checkout %s > /dev/null 2>&1" % hash)
  run("git clean -xfd")

  luceneMaster = "/l/sparseTaxis/sparseTaxis/lucene.master"

  if os.path.exists("%s/lucene/core/src/java/org/apache/lucene/codecs/lucene70" % luceneMaster):
    print("  use DiskUsage.70.java")
    shutil.copy("/l/util/src/extra/perf/DiskUsage.70.java", "/l/util/src/extra/perf/DiskUsage.java")
  elif "DocIdSetIterator" in open("%s/lucene/core/src/java/org/apache/lucene/index/NumericDocValues.java" % luceneMaster).read():
    print("  use DiskUsage.62.java")
    shutil.copy("/l/util/src/extra/perf/DiskUsage.62.java", "/l/util/src/extra/perf/DiskUsage.java")
  elif os.path.exists("%s/lucene/core/src/java/org/apache/lucene/codecs/lucene62" % luceneMaster):
    print("  use DiskUsage.62.pre-iterators.java")
    shutil.copy("/l/util/src/extra/perf/DiskUsage.62.pre-iterators.java", "/l/util/src/extra/perf/DiskUsage.java")
  else:
    print("  use DiskUsage.60.java")
    shutil.copy("/l/util/src/extra/perf/DiskUsage.60.java", "/l/util/src/extra/perf/DiskUsage.java")

  run("python3 -u /l/util/src/python/sparsetaxis/runBenchmark.py -rootDir /l/sparseTaxis -logDir %s -luceneMaster %s" % (logDir, luceneMaster))
  run("python3 -u /l/util/src/python/sparsetaxis/writeGraph.py")
