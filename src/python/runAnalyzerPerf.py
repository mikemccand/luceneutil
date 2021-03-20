# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import os
import constants

LUCENE_ROOT = '/l/trunk.analyzers.nightly/lucene'
LOGS_ROOT = os.path.join(constants.LOGS_DIR, 'analyzers')
LOGS_JA_ROOT = os.path.join(constants.LOGS_DIR, 'analyzers_ja')

for path in (LOGS_ROOT, LOGS_JA_ROOT):
  if not os.path.exists(path):
    os.makedirs(path)

def run(cmd):
  print('RUN: %s' % cmd)
  if os.system(cmd):
    raise RuntimeError('%s failed' % cmd)

os.chdir(LUCENE_ROOT)

t = datetime.datetime.now()
ymd = t.strftime('%Y-%m-%d')
print('\nrunAnalyzerPerf.py: %s' % ymd)

#run('python -u /home/mike/src/util/svnClean.py %s/..' % LUCENE_ROOT)
#run('svn cleanup')
#run('svn up')
run('git clean -xfd')
run('git pull origin main')
print('Compile...')
run('../gradlew clean compileJava > compile.log 2>&1')

logFile = '%s/%s.log' % (LOGS_ROOT, ymd)
logFileJa = '%s/%s.log' % (LOGS_JA_ROOT, ymd)

with open(logFile + '.tmp', 'w') as lf:
  #lf.write('svnversion: %s\n' % os.popen('svnversion').read().strip())
  lf.write('lucene main version: %s\n' % os.popen('git rev-parse HEAD').read().strip())
  os.chdir(constants.BENCH_BASE_DIR)
  lf.write('git version: %s\n' % os.popen('git rev-parse HEAD').read().strip())
  lf.write('java version: %s\n' % os.popen('java -fullversion 2>&1').read().strip())
  os.chdir(LUCENE_ROOT)

with open(logFileJa + '.tmp', 'w') as lf:
  #lf.write('svnversion: %s\n' % os.popen('svnversion').read().strip())
  lf.write('lucene main version: %s\n' % os.popen('git rev-parse HEAD').read().strip())
  os.chdir(constants.BENCH_BASE_DIR)
  lf.write('git version: %s\n' % os.popen('git rev-parse HEAD').read().strip())
  lf.write('java version: %s\n' % os.popen('java -fullversion 2>&1').read().strip())
  os.chdir(LUCENE_ROOT)

run('javac -d %s/build -cp core/build/classes/java/main:analysis/common/build/classes/java/main:analysis/kuromoji/build/classes/java/main %s/src/main/perf/TestAnalyzerPerf.java' % (constants.BENCH_BASE_DIR, constants.BENCH_BASE_DIR))

print('  now run en perf')
run('java -XX:+UseParallelGC -cp %s/build:core/build/classes/java/main:analysis/common/build/classes/java/main:analysis/kuromoji/build/classes/java/main perf.TestAnalyzerPerf /l/data/enwiki-20130102-lines.txt >> %s.tmp 2>&1' % (constants.BENCH_BASE_DIR, logFile))
os.rename(logFile+'.tmp', logFile)
print('  done en ')

print('  now run ja')
run('java -XX:+UseParallelGC -cp %s/build:core/build/classes/java/main:analysis/common/build/classes/java/main:analysis/kuromoji/build/classes/java/main:analysis/kuromoji/src/resources perf.TestAnalyzerPerf /l/data/jawiki-20200620-lines.txt ja >> %s.tmp 2>&1' % (constants.BENCH_BASE_DIR, logFileJa))
os.rename(logFileJa+'.tmp', logFileJa)
print('  done ja')
