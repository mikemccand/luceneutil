import shutil
import datetime
import os

TAXIS_LOGS_DIR = '/l/logs.nightly/taxis'

def run(cmd):
  if os.system(cmd):
    raise RuntimeError('command "%s" failed' % cmd)

os.chdir('/l/sparseTaxis/sparseTaxis/lucene.master/lucene')
run('git checkout master')
run('git pull origin master')

commits = []
with os.popen('git log --format=fuller', 'r') as f:
  while True:
    line = f.readline()
    if len(line) == 0:
      break
    line = line.rstrip()
    if line.startswith('commit '):
      commitHash = line[7:]
      # Author
      f.readline()
      # AuthorDate
      f.readline()
      # Committer
      f.readline()
      # Committer
      line = f.readline()
      if line.startswith('CommitDate: '):
        s = line[12:].strip()
        commitTime = datetime.datetime.strptime(s, '%a %b %d %H:%M:%S %Y %z')
        #if len(commits) > 0 and commitTime > commits[-1][1]:
        #  print('wrong order: %s -> %s vs %s, %s' % (s, commitTime, commits[-1][1], commits[-1]))
        commits.append((commitHash, datetime.datetime(*commitTime.utctimetuple()[:6])))
        #print('got: %s, %s' % (commitHash, commitTime))

dateStart = datetime.datetime(2016, 9, 15)

upto = 0
lastTestedDate = None
timeToCommitIndex = {}
while True:

  hash, commitTimeStamp = commits[upto]

  timestamp = datetime.datetime(*commitTimeStamp.utctimetuple()[:6])

  print('%s -> %s' % (timestamp, upto))
  timeToCommitIndex[timestamp] = upto
  upto += 1

  if timestamp < dateStart:
    del commits[upto:]
    break

timesAlreadyDone = set()
for name in os.listdir(TAXIS_LOGS_DIR):
  if os.path.isdir('%s/%s' % (TAXIS_LOGS_DIR, name)):
    if os.path.exists('%s/%s/results.pk' % (TAXIS_LOGS_DIR, name)):
      tup = list(int(x) for x in name.split('.'))
      timestamp = datetime.datetime(*tup)
      print('already done: %s' % timestamp)
      timesAlreadyDone.add(timestamp)

def getTimesToTest():

  """
  Carefully enumerates commit timestamps in such a way that we test large gaps first, then smaller
  gaps, and eventually all commits in the range.
  """

  while True:

    totalMaxMinTimestamp = None
    totalMaxMinDistance = None
    for hash, timestamp in commits:

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
          

lastTestedDate = None

for timestamp in getTimesToTest():

  hash, commitTimeStamp = commits[timeToCommitIndex[timestamp]]

  utc = commitTimeStamp.utctimetuple()

  ts = '%04d.%02d.%02d.%02d.%02d.%02d' % utc[:6]
  
  print('\n%s TEST: %s, %s' % (datetime.datetime.now(), hash, commitTimeStamp))
  timesAlreadyDone.add(timestamp)
  logDir = '/l/logs.nightly/taxis/%s' % ts
  if os.path.exists(logDir):
    print('  remove old partial log dir %s' % logDir)
    shutil.rmtree(logDir)

  run('git checkout %s > /dev/null 2>&1' % hash)
  run('git clean -xfd')
  run('python3 -u /l/util/src/python/sparsetaxis/runBenchmark.py -rootDir /l/sparseTaxis -logDir %s -luceneMaster /l/sparseTaxis/sparseTaxis/lucene.master' % logDir)
  run('python3 -u /l/util/src/python/sparsetaxis/writeGraph.py')

