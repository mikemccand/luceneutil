import shutil
import datetime
import os

TAXIS_LOGS_DIR = '/l/logs.nightly/taxis'

def run(cmd):
  if os.system(cmd):
    raise RuntimeError('command "%s" failed' % cmd)

os.chdir('/l/sparseTaxis/sparseTaxis/lucene.master/lucene')
run('git checkout master')

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

datesTested = set()
for name in os.listdir(TAXIS_LOGS_DIR):
  if os.path.isdir('%s/%s' % (TAXIS_LOGS_DIR, name)):
    if os.path.exists('%s/%s/results.pk' % (TAXIS_LOGS_DIR, name)):
      tup = list(int(x) for x in name.split('.')[:3])
      date = datetime.date(year=tup[0], month=tup[1], day=tup[2])
      print('already done: %s' % date)
      datesTested.add(date)

lastTestedDate = None

upto = 0
print('HERE: %s' % str(commits[0]))

while upto < len(commits):

  # Go back to last commit the day before:
  if lastTestedDate is not None:
    while True:
      hash, commitTime = commits[upto]
      utc = commitTime.utctimetuple()
      date = datetime.date(year=utc[0], month=utc[1], day=utc[2])
      if date != lastTestedDate:
        break
      upto += 1

  hash, commitTimeStamp = commits[upto]
  upto += 1

  utc = commitTimeStamp.utctimetuple()

  lastTestedDate = datetime.date(year=utc[0], month=utc[1], day=utc[2])
  ts = '%04d.%02d.%02d.%02d.%02d.%02d' % utc[:6]
  
  print('\n%s TEST: %s, %s' % (datetime.datetime.now(), hash, commitTimeStamp))
  if lastTestedDate in datesTested:
    print('  already done: %s' % lastTestedDate)
  else:
    logDir = '/l/logs.nightly/taxis/%s' % ts
    if os.path.exists(logDir):
      print('  remove old partial log dir %s' % logDir)
      shutil.rmtree(logDir)
      
    run('git checkout %s > /dev/null 2>&1' % hash)
    run('git clean -xfd')
    run('python3 -u /l/util/src/python/sparsetaxis/runBenchmark.py -rootDir /l/sparseTaxis -logDir %s -luceneMaster /l/sparseTaxis/sparseTaxis/lucene.master' % logDir)
        

