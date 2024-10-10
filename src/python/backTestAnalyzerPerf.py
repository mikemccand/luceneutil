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
import time
import constants

LUCENE_ROOT = '/l/trunk.analyzers.nightly/lucene'
LOGS_ROOT = os.path.join(constants.LOGS_DIR, 'analyzers')

def fixCtors():
  if len(os.popen('grep matchVersion %s/analysis/common/src/java/org/apache/lucene/analysis/miscellaneous/WordDelimiterFilter.java' % LUCENE_ROOT).readlines()) == 0:
    print('  remove matchVersion from WDF...')
    s = open('%s/src/extra/perf/TestAnalyzerPerf4x.java' % constants.BENCH_BASE_DIR).read()
    s = s.replace('new WordDelimiterFilter(Version.LUCENE_CURRENT, ', 'new WordDelimiterFilter(')
    open('%s/src/extra/perf/TestAnalyzerPerf4x.java' % constants.BENCH_BASE_DIR, 'w').write(s)

  # Doens't work, too simplistic: EdgeNGramTokenFilter ctor took Side args before 2013-05-07:
  if False and len(os.popen('grep EdgeNGramTokenFilter\\(Version %s/analysis/common/src/java/org/apache/lucene/analysis/ngram/EdgeNGramTokenFilter.java' % LUCENE_ROOT).readlines()) == 0:
    print('  remove matchVersion from EdgeNGramTokenFilter...')
    s = open('%s/src/extra/perf/TestAnalyzerPerf4x.java' % constants.BENCH_BASE_DIR).read()
    s = s.replace('new EdgeNGramTokenFilter(Version.LUCENE_CURRENT, ', 'new EdgeNGramTokenFilter(')
    open('%s/src/extra/perf/TestAnalyzerPerf4x.java' % constants.BENCH_BASE_DIR, 'w').write(s)

def run(cmd):
  if os.system(cmd):
    raise RuntimeError('%s failed' % cmd)

os.chdir(LUCENE_ROOT)

then = datetime.datetime.now()

while True:
  ymd = then.strftime('%Y-%m-%d')
  logFile = '%s/%s.log' % (LOGS_ROOT, ymd)
  print('\n%s' % ymd)
  if not os.path.exists(logFile):

    t0 = time.time()
    run('python -u /home/mike/src/util/svnClean.py %s/..' % LUCENE_ROOT)
    run('svn up -r {%s}' % ymd)

    with open(logFile + '.tmp', 'w') as lf:
      lf.write('svnversion: %s\n' % os.popen('svnversion').read().strip())
      os.chdir(constants.BENCH_BASE_DIR)
      lf.write('git version: %s\n' % os.popen('git rev-parse HEAD').read().strip())
      lf.write('java version: %s\n' % os.popen('java -fullversion 2>&1').read().strip())
      os.chdir(LUCENE_ROOT)

    run('ant clean compile > compile.log 2>&1')

    fixCtors()
    run('javac -d %s/build -cp build/core/classes/java:build/analysis/common/classes/java %s/src/extra/perf/TestAnalyzerPerf.java' % (constants.BENCH_BASE_DIR, constants.BENCH_BASE_DIR))
    print('  now run')
    run('java -cp %s/build:build/core/classes/java:build/analysis/common/classes/java perf.TestAnalyzerPerf /lucenedata/enwiki/enwiki-20130102-lines.txt >> %s.tmp 2>&1' % (constants.BENCH_BASE_DIR, logFile))
    os.rename('%s.tmp' % logFile, logFile)
    print('  took %.1f sec' % (time.time()-t0))
  else:
    print('  already done; skip')
  then -= datetime.timedelta(days=1)

