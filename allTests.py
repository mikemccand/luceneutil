#!/usr/bin/env python

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

import time
import signal
import os
import subprocess
import sendTasks

# TODO
#   - make graph across load rates w/ curves @ percentiles
#   - if log already exists (hmm, or "completed marker file") don't regen?

LOGS_DIR = 'logs'

RUN_TIME_SEC = 60

WARMUP_SEC = 10

LUCENE_HOME = '/l/direct/lucene'

SERVER_PORT = 7777

#INDEX_PATH = '/l/scratch/indices/wikimedium2m.direct.Direct.opt.nd2M/index'
INDEX_PATH = '/l/scratch/indices/wikimedium2m.direct.Lucene40.opt.nd2M/index'

DIR_IMPL = 'MMapDirectory'

SEARCH_THREAD_COUNT = 4

MAX_HEAP_GB = 2

DOCS_PER_SEC_PER_THREAD = 500.0

LINE_DOCS_FILE = '/x/lucene/data/enwiki/enwiki-20120502-lines-1k.txt'

POSTINGS_FORMAT = 'Lucene40'

JHICCUP_PATH = '/x/tmp4/jHiccup.1.1.4/jHiccup'

TASKS_FILE = 'hiliteterms500.tasks'

for desc, javaOpts in (
  ('G1', '-XX:+UnlockExperimentalVMOptions -XX:+UseG1GC'),
  ('CMS', '-XX:+UnlockExperimentalVMOptions -XX:+UseConcMarkSweepGC'),
  ('Parallel', '')):

  for targetQPS in (50, 100, 150, 200, 250, 300):
    command = []
    w = command.append
    w('java')
    w(javaOpts)
    w('-Xmx%dg' % MAX_HEAP_GB)
    w('-verbose:gc')
    w('-cp')
    w('.:$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/highlighter/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java'.replace('$LUCENE_HOME', LUCENE_HOME))
    w('perf.SearchPerfTest')
    w('-indexPath %s' % INDEX_PATH)
    w('-dirImpl %s' % DIR_IMPL)
    w('-cloneDocs')
    w('-analyzer StandardAnalyzer')
    w('-taskSource server:localhost:%s' % SERVER_PORT)
    w('-searchThreadCount %d' % SEARCH_THREAD_COUNT)
    w('-field body')
    w('-similarity DefaultSimilarity')
    w('-commit single')
    w('-seed 0')
    w('-staticSeed 0')
    w('-nrt')
    w('-indexThreadCount 1')
    w('-docsPerSecPerThread %s' % DOCS_PER_SEC_PER_THREAD)
    w('-lineDocsFile %s' % LINE_DOCS_FILE)
    w('-reopenEverySec 1.0')
    w('-store')
    w('-tvs')
    w('-postingsFormat %s' % POSTINGS_FORMAT)
    w('-idFieldPostingsFormat %s' % POSTINGS_FORMAT)

    logsDir = '%s/%s.qps%s' % (LOGS_DIR, desc, targetQPS)

    if not os.path.exists(logsDir):
      os.makedirs(logsDir)

    serverLog = '%s/server.log' % logsDir

    command = '%s -d %s -l %s/hiccups %s > %s 2>&1' % \
              (JHICCUP_PATH, WARMUP_SEC*1000, logsDir, ' '.join(command), serverLog)

    print
    print '%s, QPS=%d' % (desc, targetQPS)

    doneFile = '%s/done' % logsDir
    if os.path.exists(doneFile):
      print '  skip: already done'
      continue

    try:

      p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)

      while True:
        try:
          if open(serverLog).read().find('  ready for client...'):
            break
        except IOError:
          pass
        time.sleep(1.0)

      time.sleep(2.0)

      f = open('%s/client.log' % logsDir, 'wb')
      sendTasks.run(TASKS_FILE, 'localhost', SERVER_PORT, targetQPS, 1000, RUN_TIME_SEC, '%s/results.pk' % logsDir, f, False)
      f.close()
    finally:
      print '  now find pid...'
      pid = int(os.popen('ps ww | grep SearchPerfTest | grep -v grep | grep -v /bin/sh').read().strip().split()[0])
      print '  kill %s' % pid
      os.kill(pid, signal.SIGKILL)
      print '  now poll'
      p.poll()
    print '  done'
    open(doneFile, 'wb').close()
    
