import datetime
import os

oneDay = datetime.timedelta(days=1)

os.chdir('/l/trunk.nightly/lucene')

def run(cmd):
  if os.system(cmd):
    raise RuntimeError('command "%s" failed' % cmd)

then = datetime.datetime.now()
then -= oneDay

while True:
  ts = '%04d-%02d-%02d %02d:%02d:%02d' % (then.year, then.month, then.day, then.hour, then.minute, then.second)
  print('\nTEST: %s' % ts)
  run('git checkout `git rev-list -1 --before="%s" master`' % ts)
  run('git clean -xfd')
  run('ant jar')
  run('python3 -u /l/util/src/python/runGeoBenches.py -nightly -timeStamp %s' % (ts.replace('-', '.').replace(':', '.').replace(' ', '.')))
  then -= oneDay
