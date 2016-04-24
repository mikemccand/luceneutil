import shutil
import datetime
import os

oneDay = datetime.timedelta(days=1)

GEO_LOGS_DIR = '/l/logs.nightly/geo'

os.chdir('/l/trunk.nightly/lucene')

def run(cmd):
  if os.system(cmd):
    raise RuntimeError('command "%s" failed' % cmd)

datesTested = set()
for name in os.listdir(GEO_LOGS_DIR):
  if name.endswith('.log.txt.bz2'):
    tup = list(int(x) for x in name.split('.')[:3])
    date = datetime.date(year=tup[0], month=tup[1], day=tup[2])
    #print('already done: %s' % date)
    datesTested.add(date)

then = datetime.datetime.now()

while True:
  ts = '%04d-%02d-%02d %02d:%02d:%02d' % (then.year, then.month, then.day, then.hour, then.minute, then.second)
  tsDate = datetime.date(year=then.year, month=then.month, day=then.day)
  print('\n%s TEST: %s' % (datetime.datetime.now(), ts))
  if tsDate in datesTested:
    print('  already done')
  else:
    oldSrc = '/l/util/src/main/perf/backtest/IndexAndSearchOpenStreetMaps.java.%04d%02d%02d' % (then.year, then.month, then.day)
    print('check oldSrc %s' % oldSrc)
    if os.path.exists(oldSrc):
      print('  switch to %s' % oldSrc)
      shutil.copy(oldSrc, '/l/util/src/main/perf/IndexAndSearchOpenStreetMaps.java')
    run('git checkout `git rev-list -1 --before="%s" master`' % ts)
    run('git clean -xfd')
    run('ant jar')
    run('python3 -u /l/util/src/python/runGeoBenches.py -nightly -timeStamp %s' % (ts.replace('-', '.').replace(':', '.').replace(' ', '.')))
    run('python3 -u /l/util/src/python/writeGeoGraphs.py')
  then -= oneDay
