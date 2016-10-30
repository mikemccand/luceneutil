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
        commits.append((commitHash, commitTime))
        #print('got: %s, %s' % (commitHash, commitTime))

dateStart = datetime.date(2016, 9, 20)
dateEnd = datetime.date(2016, 10, 30)

upto = 0
lastTestedDate = None
dateToCommitIndex = {}
while True:

  # Go back to last commit the day before:
  if lastTestedDate is not None:
    while True:
      hash, commitTime = commits[upto]
      utc = commitTime.utctimetuple()
      date = commitTime.date()
      if date != lastTestedDate:
        break
      upto += 1

  hash, commitTimeStamp = commits[upto]

  utc = commitTimeStamp.utctimetuple()

  lastTestedDate = datetime.date(year=utc[0], month=utc[1], day=utc[2])

  print('%s -> %s' % (lastTestedDate, upto))
  dateToCommitIndex[lastTestedDate] = upto
  upto += 1

  if lastTestedDate < dateStart:
    break

datesAlreadyDone = set()
for name in os.listdir(TAXIS_LOGS_DIR):
  if os.path.isdir('%s/%s' % (TAXIS_LOGS_DIR, name)):
    if os.path.exists('%s/%s/results.pk' % (TAXIS_LOGS_DIR, name)):
      tup = list(int(x) for x in name.split('.')[:3])
      date = datetime.date(year=tup[0], month=tup[1], day=tup[2])
      print('already done: %s' % date)
      datesAlreadyDone.add(date)

oneDay = datetime.timedelta(days=1)
 
def getDatesToTest():
  """
  Carefully enumerates dates in such a way that we test large gaps first, then smaller
  gaps, and eventually all dates in range.
  """

  if dateStart not in datesAlreadyDone:
    yield dateStart

  if dateEnd not in datesAlreadyDone:
    yield dateEnd

  totalMaxMinDistance = None
  totalMaxMinDate = None

  while True:
    date = dateStart
    while date <= dateEnd:
      date += oneDay
      if date in datesAlreadyDone:
        continue

      # consider choosing date next

      # find the closest date already done, to this candidate date:
      minDistance = None
      for date2 in datesAlreadyDone:
        distance = abs((date - date2).total_seconds())
        if minDistance is None or distance < minDistance:
          minDistance = distance

      if totalMaxMinDistance is None or minDistance > totalMaxMinDistance:
        totalMaxMinDistance = minDistance
        totalMaxMinDate = date

    if totalMaxMinDate is None:
      break

    yield totalMaxMinDate
          

lastTestedDate = None

for date in getDatesToTest():

  if date not in dateToCommitIndex:
    # no commits on this date:
    datesAlreadyDone.add(date)
    print('  skip date %s: no commits' % date)
    continue

  hash, commitTimeStamp = commits[dateToCommitIndex[date]]

  utc = commitTimeStamp.utctimetuple()

  ts = '%04d.%02d.%02d.%02d.%02d.%02d' % utc[:6]
  
  print('\n%s TEST: %s, %s' % (datetime.datetime.now(), hash, commitTimeStamp))
  if date in datesAlreadyDone:
    print('  already done: %s' % date)
  else:
    datesAlreadyDone.add(date)
    logDir = '/l/logs.nightly/taxis/%s' % ts
    if os.path.exists(logDir):
      print('  remove old partial log dir %s' % logDir)
      shutil.rmtree(logDir)
      
    run('git checkout %s > /dev/null 2>&1' % hash)
    run('git clean -xfd')
    run('python3 -u /l/util/src/python/sparsetaxis/runBenchmark.py -rootDir /l/sparseTaxis -logDir %s -luceneMaster /l/sparseTaxis/sparseTaxis/lucene.master' % logDir)
    run('python3 -u /l/util/src/python/sparsetaxis/writeGraph.py')

