import os
import datetime

now = datetime.datetime.now()

os.chdir('/l/sparseTaxis.nightly/sparseTaxis/lucene.master/')
if os.system('git checkout master'):
  raise RuntimeError('git checkout master failed')

if os.system('git pull origin master'):
  raise RuntimeError('git pull failed')

if os.system('python3 -u /l/util.nightly/src/python/sparsetaxis/runBenchmark.py -rootDir /l/sparseTaxis.nightly -logDir /l/logs.nightly/taxis/%4d.%02d.%02d.%02d.%02d.%02d -luceneMaster /l/sparseTaxis.nightly/sparseTaxis/lucene.master' % \
             (now.year, now.month, now.day, now.hour, now.minute, now.second)):
  raise RuntimeError('bench failed')

if os.system('python3 -u /l/util.nightly/src/python/sparsetaxis/writeGraph.py'):
  raise RuntimeError('bench failed')
