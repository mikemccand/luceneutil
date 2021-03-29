import os
import datetime

now = datetime.datetime.now()

os.chdir('/l/sparseTaxis.nightly/sparseTaxis/lucene.main')
if os.system('git checkout main'):
  raise RuntimeError('git checkout main failed')

if os.system('git pull origin main'):
  raise RuntimeError('git pull failed')

if os.system('python3 -u /l/util.nightly/src/python/sparsetaxis/runBenchmark.py -rootDir /l/sparseTaxis.nightly -logDir /l/logs.nightly/taxis/%4d.%02d.%02d.%02d.%02d.%02d -luceneMain /l/sparseTaxis.nightly/sparseTaxis/lucene.main' % \
             (now.year, now.month, now.day, now.hour, now.minute, now.second)):
  raise RuntimeError('bench failed')

if os.system('python3 -u /l/util.nightly/src/python/sparsetaxis/writeGraph.py'):
  raise RuntimeError('bench failed')
