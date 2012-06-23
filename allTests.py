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
import datetime

# TODO
#   - check for excs in the logs

# Indexing rate ~ 178 MB/minute @ 500 docs/sec; 71.2 MB/minute @ 200 docs/sec

LOGS_DIR = 'logs'

RUN_TIME_SEC = 60

WARMUP_SEC = 10

#CLIENT_HOST = '10.17.4.10'
#SERVER_HOST = '10.17.4.91'

CLIENT_HOST = None
SERVER_HOST = 'localhost'

LUCENE_HOME = '/l/direct/lucene'

REMOTE_CLIENT = 'sendTasks.py'

SERVER_PORT = 7777

#INDEX_PATH = '/l/scratch/indices/wikimedium2m.direct.Direct.opt.nd2M/index'
INDEX_PATH = '/l/scratch/indices/wikimedium2m.direct.Lucene40.opt.nd2M/index'

#DIR_IMPL = 'RAMDirectory'
DIR_IMPL = 'MMapDirectory'

SEARCH_THREAD_COUNT = 4

MAX_HEAP_GB = 13

DOCS_PER_SEC_PER_THREAD = 100.0

LINE_DOCS_FILE = '/x/lucene/data/enwiki/enwiki-20120502-lines-1k.txt'

POSTINGS_FORMAT = 'Lucene40'

JHICCUP_PATH = '/x/tmp4/jHiccup.1.1.4/jHiccup'

TASKS_FILE = 'hiliteterms500.tasks'

def kill(name, p):
  for l in os.popen('ps ww | grep %s | grep -v grep | grep -v /bin/sh' % name).readlines():
    l = l.strip().split()
    pid = int(l[0])
    print '  stop %s process %s' % (name, pid)
    os.kill(pid, signal.SIGKILL)
  if p is not None:
    p.poll()
        
for targetQPS in (100, 200, 300, 400, 500,600, 700, 750, 800, 850, 900, 950, 1000):

  for desc, javaOpts in (
    ('G1', '-XX:+UnlockExperimentalVMOptions -XX:+UseG1GC'),
    ('CMS', '-XX:+UnlockExperimentalVMOptions -XX:+UseConcMarkSweepGC'),
    #('Parallel', ''),
    ):

    print
    print '%s: %s, QPS=%d' % (datetime.datetime.now(), desc, targetQPS)

    logsDir = '%s/%s.qps%s' % (LOGS_DIR, desc, targetQPS)
    doneFile = '%s/done' % logsDir

    javaCommand = 'java'

    if not os.path.exists(logsDir):
      os.makedirs(logsDir)

    if os.path.exists(doneFile):
      print '  skip: already done'
      continue

    command = []
    w = command.append
    w('java')
    w(javaOpts)
    if desc.find('MMap') != -1:
      w('-Xmx4g')
    else:
      w('-Xmx%dg' % MAX_HEAP_GB)
    w('-verbose:gc')
    w('-XX:+PrintGCDetails')
    w('-cp')
    w('.:$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/highlighter/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java'.replace('$LUCENE_HOME', LUCENE_HOME))
    w('perf.SearchPerfTest')
    w('-indexPath %s' % INDEX_PATH)
    w('-dirImpl %s' % DIR_IMPL)
    w('-cloneDocs')
    w('-analyzer StandardAnalyzer')
    w('-taskSource server:%s:%s' % (SERVER_HOST, SERVER_PORT))
    w('-searchThreadCount %d' % SEARCH_THREAD_COUNT)
    w('-field body')
    w('-similarity DefaultSimilarity')
    w('-commit multi')
    w('-seed 0')
    w('-staticSeed 0')
    w('-nrt')
    w('-indexThreadCount 1')
    w('-docsPerSecPerThread %s' % DOCS_PER_SEC_PER_THREAD)
    w('-lineDocsFile %s' % LINE_DOCS_FILE)
    w('-reopenEverySec 1.0')
    #w('-store')
    #w('-tvs')
    w('-postingsFormat %s' % POSTINGS_FORMAT)
    w('-idFieldPostingsFormat %s' % POSTINGS_FORMAT)

    serverLog = '%s/server.log' % logsDir
    if os.path.exists(serverLog):
      os.remove(serverLog)

    command = '%s -d %s -l %s/hiccups %s > %s 2>&1' % \
              (JHICCUP_PATH, WARMUP_SEC*1000, logsDir, ' '.join(command), serverLog)

    # print command

    p = None
    vmstatProcess = None
    
    try:

      print '  clean index'
      touchCmd = '%s -cp .:$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/highlighter/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java perf.OpenCloseIndexWriter %s'.replace('$LUCENE_HOME', LUCENE_HOME) % (javaCommand, INDEX_PATH)
      #print '  run %s' % touchCmd
      if os.system(touchCmd):
        raise RuntimeError('OpenCloseIndexWriter failed')

      t0 = time.time()
      vmstatProcess = subprocess.Popen('vmstat 1 > %s/vmstat.log 2>&1' % logsDir, shell=True)
      p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)

      while True:
        try:
          if open(serverLog).read().find('  ready for client...') != -1:
            break
        except IOError:
          pass
        time.sleep(0.5)

      print '  %.1f sec to start' % (time.time()-t0)

      time.sleep(2.0)

      if CLIENT_HOST is not None:
        # Remote client:
        command = 'python -u %s %s %s %s %.1f 1000 %.1f results.pk' % \
                  (REMOTE_CLIENT, TASKS_FILE, SERVER_HOST, SERVER_PORT, targetQPS, RUN_TIME_SEC)

        if os.system('ssh %s %s > %s/client.log 2>&1' % (CLIENT_HOST, command, logsDir)):
          raise RuntimeError('client failed; see %s/client.log' % logsDir)

        if os.system('scp %s:results.pk %s > /dev/null 2>&1' % (CLIENT_HOST, logsDir)):
          raise RuntimeError('scp results.pk failed')

        if os.system('ssh %s rm -f results.pk' % CLIENT_HOST):
          raise RuntimeError('rm results.pk failed')
          
      else:
        f = open('%s/client.log' % logsDir, 'wb')
        sendTasks.run(TASKS_FILE, 'localhost', SERVER_PORT, targetQPS, 1000, RUN_TIME_SEC, '%s/results.pk' % logsDir, f, False)
        f.close()

    finally:
      kill('SearchPerfTest', p)
      kill('vmstat', vmstatProcess)
      
    print '  done'
    open(doneFile, 'wb').close()
    
