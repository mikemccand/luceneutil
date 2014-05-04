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

LUCENE_ROOT = '/lucene/4x.analyzers/lucene'
LOGS_ROOT = os.path.join(constants.LOGS_DIR, 'analyzers')

def run(cmd):
  if os.system(cmd):
    raise RuntimeError('%s failed' % cmd)

os.chdir(LUCENE_ROOT)

t = datetime.datetime.now()
ymd = t.strftime('%Y-%m-%d')
print('\n%s' % ymd)

run('python -u /home/mike/src/util/svnClean.py %s/..' % LUCENE_ROOT)
run('svn cleanup')
run('svn up')
run('ant clean compile > compile.log 2>&1')

logFile = '%s/%s.log' % (LOGS_ROOT, ymd)

with open(logFile + '.tmp', 'w') as lf:
  lf.write('svnversion: %s\n' % os.popen('svnversion').read().strip())
  lf.write('hgversion: %s\n' % os.popen('hg id %s' % constants.BENCH_BASE_DIR).read().strip())

run('javac -d %s/build -cp build/core/classes/java:build/analysis/common/classes/java %s/src/main/perf/TestAnalyzerPerf4x.java' % (constants.BENCH_BASE_DIR, constants.BENCH_BASE_DIR))

print('  now run')
run('java -cp %s/build:build/core/classes/java:build/analysis/common/classes/java perf.TestAnalyzerPerf4x /lucenedata/enwiki/enwiki-20130102-lines.txt >> %s.tmp 2>&1' % (constants.BENCH_BASE_DIR, logFile))
os.rename(logFile+'.tmp', logFile)
print('  done')
