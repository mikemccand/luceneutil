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

import re
import shutil
import time
import sys
import signal
import os
import subprocess
import sendTasks
import datetime
import traceback
import threading

# TODO: pull these from localconstants.py:

SMOKE_TEST = True

if True:
  # Home
  env = 'home'
  LUCENE_HOME = '/l/4x.beta.azul/lucene'
  LUCENE40_INDEX_PATH = '/l/scratch/indices/Lucene40'
  DIRECT_INDEX_PATH = '/l/scratch/indices/Direct'
  LINE_DOCS_FILE = '/x/lucene/data/enwiki/enwiki-20120502-lines-1k.txt'
  JHICCUP_PATH = '/x/tmp4/jHiccup.1.1.4/jHiccup'
  ORACLE_JVM = '/usr/local/src/jdk1.7.0_05/bin/java'
  # nocommit
  ZING_JVM = '/usr/local/src/zingLX-jdk1.6.0_31-5.2.1.0-3/bin/java'
  #ZING_JVM = ORACLE_JVM
  DO_STOP_START_ZST = True
  MAX_HEAP_GB = 10
  SEARCH_THREAD_COUNT = 6
  DO_ZV_ROBOT = False
  ZV_ROBOT_ROOT = '/root/ZVRobot'
  QPS_START = 100
  QPS_INC = 50
  QPS_END = None
  CLIENT_HOST = '10.17.4.10'
  CLIENT_USER = 'mike'
  SERVER_HOST = '10.17.4.91'
  COMMIT_POINT = 'multi'
elif False:
  # EC2:
  env = 'ec2'
  LUCENE_HOME = '/root/lucene4x/lucene'
  LUCENE40_INDEX_PATH = '/large/indices/wikimediumall.lucene4x.Lucene40.nd33.3326M/index'
  DIRECT_INDEX_PATH = '/large/indices/fullwiki'
  LINE_DOCS_FILE = '/large/enwiki-20120502-lines-1k.txt'
  JHICCUP_PATH = '/root/jHiccup.1.1.4/jHiccup'
  ZING_JVM = '/opt/zing/zingLX-jdk1.6.0_31-5.2.0.0-18-x86_64/bin/java'
  ORACLE_JVM = '/root/jdk1.6.0_31/bin/java'
  DO_STOP_START_ZST = True
  MAX_HEAP_GB = 40
  SEARCH_THREAD_COUNT = 20
  DO_ZV_ROBOT = False
  ZV_ROBOT_ROOT = '/root/ZVRobot'
  QPS_START = 25
  QPS_INC = 25
  QPS_END = None
  CLIENT_HOST = None
  CLIENT_USER = 'root'
  SERVER_HOST = 'localhost'
  COMMIT_POINT = 'multi'
else:
  # Lab box:
  env = 'lab'
  LUCENE_HOME = '/localhome/lucene4x/lucene'
  LUCENE40_INDEX_PATH = '/localhome/indices/wikimediumall.lucene4x.Lucene40.nd33.3326M/index'
  DIRECT_INDEX_PATH = '/localhome/indices/direct'
  LINE_DOCS_FILE = '/localhome/data/enwiki-20120502-lines-1k.txt'
  JHICCUP_PATH = '/localhome/jHiccup.1.1.4/jHiccup'
  ZING_JVM = '/opt/zing/zingLX-jdk1.6.0_31-5.2.0.0-18-x86_64/bin/java'
  ORACLE_JVM = '/localhome/jdk1.6.0_32/bin/java'
  DO_STOP_START_ZST = True
  MAX_HEAP_GB = 250
  SEARCH_THREAD_COUNT = 64
  DO_ZV_ROBOT = False
  ZV_ROBOT_ROOT = '/root/ZVRobot'
  QPS_START = 50
  QPS_INC = 50
  QPS_END = None
  CLIENT_HOST = 'isvx40'
  SERVER_HOST = 'isvx512'
  CLIENT_USER = 'root'
  COMMIT_POINT = 'multi'

LOGS_DIR = 'logs'

if SMOKE_TEST:
  RUN_TIME_SEC = 30
  WARMUP_SEC = 10
else:
  RUN_TIME_SEC = 3600
  WARMUP_SEC = 5 * 60

REMOTE_CLIENT = 'sendTasks.py'

SERVER_PORT = 7777

DOCS_PER_SEC_PER_THREAD = 100.0

#TASKS_FILE = 'hiliteTermsNoStopWords.tasks'
#TASKS_FILE = 'termsNoStopWords.tasks'
TASKS_FILE = 'single.tasks'

reSVNRev = re.compile(r'revision (.*?)\.')

class Tee(object):
  def __init__(self, file, att):
    self.file = file
    self.att = att
    self.orig = getattr(sys, att)
    setattr(sys, att, self)

  def __del__(self):
    setattr(sys, self.att, self.orig)

  def write(self, data):
    self.file.write(data)
    self.file.flush()
    self.orig.write(data)

def captureEnv(logsDir):
  print 'Python version: %s' % sys.version
  svnRev = os.popen('svnversion %s' % LUCENE_HOME).read().strip()
  print 'Lucene svn rev is %s (%s)' % (svnRev, LUCENE_HOME)
  if svnRev.endswith('M'):
    if system('svn diff %s > %s/lucene.diffs' % (LUCENE_HOME, logsDir)):
      raise RuntimeError('svn diff failed')
    os.chmod('%s/lucene.diffs' % logsDir, 0444)

  luceneUtilDir = os.path.abspath(os.path.split(sys.argv[0])[0])

  luceneUtilRev = os.popen('hg id %s' % luceneUtilDir).read().strip()  
  print 'Luceneutil hg rev is %s (%s)' % (luceneUtilRev, luceneUtilDir)
  if luceneUtilRev.find('+') != -1:
    if system('hg diff %s > %s/luceneutil.diffs' % (luceneUtilDir, logsDir)):
      raise RuntimeError('hg diff failed')
    os.chmod('%s/luceneutil.diffs' % logsDir, 0444)
    
  shutil.copy('%s/responseTimeTests.py' % luceneUtilDir,
              '%s/responseTimeTests.py' % logsDir)
  os.chmod('%s/responseTimeTests.py' % logsDir, 0444)
              
def kill(name, p):
  for l in os.popen('ps ww | grep %s | grep -v grep | grep -v /bin/sh' % name).readlines():
    l = l.strip().split()
    pid = int(l[0])
    print '  stop %s process %s' % (name, pid)
    try:
      os.kill(pid, signal.SIGKILL)
    except OSError:
      pass
  if p is not None:
    p.poll()

stopPSThread = False

def runPSThread(logFileName):

  startTime = time.time()
  f = open(logFileName, 'wb')
  try:
    while not stopPSThread:
      # ps axuw | sed "1 d" | sort -n -r -k3 | head
      for i in xrange(10):
        if stopPSThread:
          break
        time.sleep(0.5)

      # TODO: top instead?
      f.write('\n\nTime %.1f s:\n' % (time.time() - startTime))
      #p = os.popen('ps axuw | sed "1 d" | sort -n -r -k3')
      sawHeader = False
      p = os.popen('top -b -n1')
      try:
        keep = []
        for l in p.readlines():
          l = l.strip()
          if l == '':
            continue
          if not sawHeader:
            if l.find('PID') != -1:
              sawHeader = True
              tup = l.split()
              cpuIDX = tup.index('%CPU')
              memIDX = tup.index('%MEM')
            keep.append(l)
            continue
          tup = l.split()
          if float(tup[cpuIDX]) > 0 or float(tup[memIDX]) > 0.1:
            keep.append(l)
        f.write('\n'.join(keep))
      finally:
        p.close()
      
      f.write('\n')
      
  finally:
    f.close()

def system(command):
  #print '  run: %s' % command
  p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
  output = p.communicate()[0].strip()
  if len(output) > 0:
    print '  %s' % output.replace('\n', '\n  ')
  return p.returncode

def run():

  global stopPSThread

  if SMOKE_TEST:
    print
    print '***SMOKE_TEST***'
    print

  captureEnv(LOGS_DIR)

  # Which tasks exceeded capacity:
  finished = set()

  targetQPS = QPS_START

  JOBS =  (
    #('Zing', 'MMapDirectory', 'Lucene40'),
    #('OracleCMS', 'MMapDirectory', 'Lucene40'),
    ('Zing', 'MMapDirectory', 'Lucene40'),
    ('OracleCMS', 'MMapDirectory', 'Lucene40'),
    ('Zing', 'MMapDirectory', 'Direct'),
    ('OracleCMS', 'MMapDirectory', 'Direct'),
    )


  if CLIENT_HOST is not None:
    print 'Copy sendTasks.py to client host %s' % CLIENT_HOST
    if system('scp sendTasks.py %s@%s: > /dev/null 2>&1' % (CLIENT_USER, CLIENT_HOST)):
      raise RuntimeError('copy sendTasks.py failed')

  startTime = datetime.datetime.now()
  
  while len(finished) != len(JOBS):

    for job in JOBS:

      if job in finished:
        continue
    
      desc, dirImpl, postingsFormat = job
      
      print
      print '%s: config=%s, dir=%s, postingsFormat=%s, QPS=%d' % \
            (datetime.datetime.now(), desc, dirImpl, postingsFormat, targetQPS)

      logsDir = '%s/%s.%s.%s.qps%s' % (LOGS_DIR, desc, dirImpl, postingsFormat, targetQPS)

      if postingsFormat == 'Lucene40':
        indexPath = LUCENE40_INDEX_PATH
      else:
        indexPath = DIRECT_INDEX_PATH

      if SMOKE_TEST:
        indexPath += '.1M'

      os.makedirs(logsDir)

      if desc.startswith('Zing'):
        if DO_STOP_START_ZST:
          while True:
            if system('sudo service zing-memory start 2>&1'):
              print 'Failed to start zing-memory... retry; java processes:'
              system('ps axuw | grep java')
              time.sleep(2.0)
            else:
              break
        javaCommand = ZING_JVM
      else:
        if DO_STOP_START_ZST:
          while True:
            if system('sudo service zing-memory stop 2>&1'):
              print 'Failed to stop zing-memory... retry; java processes:'
              system('ps axuw | grep java')
              time.sleep(2.0)
            else:
              break
        javaCommand = ORACLE_JVM

      command = []
      w = command.append
      w(javaCommand)

      # nocommit
      # w('-agentlib:yjpagent=sampling,disablej2ee,alloceach=10')
      
      if desc.find('CMS') != -1:
        w('-XX:+UseConcMarkSweepGC')

      if dirImpl == 'MMapDirectory' and postingsFormat == 'Lucene40':
        w('-Xmx4g')
      else:
        w('-Xmx%dg' % MAX_HEAP_GB)

      w('-Xloggc:%s/gc.log' % logsDir)
      
      if DO_ZV_ROBOT and desc.startswith('Zing'):
        w('-XX:ARTAPort=8111')
        
      w('-verbose:gc')
      w('-XX:+PrintGCDetails')
      w('-cp')
      w('.:$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/highlighter/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java'.replace('$LUCENE_HOME', LUCENE_HOME))
      w('perf.SearchPerfTest')
      w('-indexPath %s' % indexPath)
      if dirImpl == 'RAMDirectory' and postingsFormat == 'Direct':
        w('-dirImpl RAMExceptDirectPostingsDirectory')
      else:
        w('-dirImpl %s' % dirImpl)
      w('-analyzer StandardAnalyzer')
      w('-taskSource server:%s:%s' % (SERVER_HOST, SERVER_PORT))
      w('-searchThreadCount %d' % SEARCH_THREAD_COUNT)
      w('-field body')
      w('-similarity DefaultSimilarity')
      w('-commit %s' % COMMIT_POINT)
      w('-seed 0')
      w('-staticSeed 0')
      w('-hiliteImpl FastVectorHighlighter')

      # Do indexing/NRT reopens:
      if True:
        w('-nrt')
        w('-indexThreadCount 1')
        w('-docsPerSecPerThread %s' % DOCS_PER_SEC_PER_THREAD)
        w('-lineDocsFile %s' % LINE_DOCS_FILE)
        w('-reopenEverySec 1.0')
        w('-store')
        w('-tvs')
        w('-postingsFormat %s' % postingsFormat)
        w('-idFieldPostingsFormat %s' % postingsFormat)
        w('-cloneDocs')
      
      serverLog = '%s/server.log' % logsDir

      command = '%s -d %s -l %s/hiccups %s > %s 2>&1' % \
                (JHICCUP_PATH, WARMUP_SEC*1000, logsDir, ' '.join(command), serverLog)

      p = None
      vmstatProcess = None
      zvRobotProcess = None
      psThread = None
      success = False

      try:

        print '  clean index'
        touchCmd = '%s -cp .:$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/highlighter/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java perf.OpenCloseIndexWriter %s'.replace('$LUCENE_HOME', LUCENE_HOME) % (javaCommand, indexPath)
        #print '  run %s' % touchCmd
        if system(touchCmd):
          raise RuntimeError('OpenCloseIndexWriter failed')

        t0 = time.time()
        vmstatProcess = subprocess.Popen('vmstat 1 > %s/vmstat.log 2>&1' % logsDir, shell=True)
        print '  server command: %s' % command
        p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)

        if DO_ZV_ROBOT and desc.startswith('Zing'):
          cmd = '%s -Xmx1g -jar %s/ZVRobot-5.2.0.0-18.jar any %s/ZVRobot.prop > /dev/null 2>&1' % \
                (ORACLE_JVM, ZV_ROBOT_ROOT, ZV_ROBOT_ROOT)
          print 'run %s' % cmd
          zvRobotProcess = subprocess.Popen(cmd, shell=True)
          del cmd

        print '  wait for server startup...'

        time.sleep(2.0)
        
        while True:
          try:
            if open(serverLog).read().find('  ready for client...') != -1:
              break
          except IOError:
            pass
          time.sleep(1.0)

        print '  %.1f sec to start; start test now' % (time.time()-t0)

        time.sleep(2.0)

        stopPSThread = False
        psThread = threading.Thread(target=runPSThread, args=('%s/top.log' % logsDir,))
        psThread.start()

        t0 = time.time()
        if CLIENT_HOST is not None:
          # Remote client:
          command = 'python -u %s %s %s %s %.1f 1000 %.1f results.bin' % \
                    (REMOTE_CLIENT, TASKS_FILE, SERVER_HOST, SERVER_PORT, targetQPS, RUN_TIME_SEC)

          if system('ssh %s@%s %s > %s/client.log 2>&1' % (CLIENT_USER, CLIENT_HOST, command, logsDir)):
            raise RuntimeError('client failed; see %s/client.log' % logsDir)

          print '  copy results.bin back...'
          if system('scp %s@%s:results.bin %s > /dev/null 2>&1' % (CLIENT_USER, CLIENT_HOST, logsDir)):
            raise RuntimeError('scp results.bin failed')

          if system('ssh %s@%s rm -f results.bin' % (CLIENT_USER, CLIENT_HOST)):
            raise RuntimeError('rm results.bin failed')

        else:
          f = open('%s/client.log' % logsDir, 'wb')
          sendTasks.run(TASKS_FILE, 'localhost', SERVER_PORT, targetQPS, 1000, RUN_TIME_SEC, '%s/results.bin' % logsDir, f, False)
          f.close()
          
        t1 = time.time()
        print '  test done (%.1f total sec)' % (t1-t0)

        if (t1 - t0) > RUN_TIME_SEC * 1.3:
          print '  marking this job finished!'
          finished.add(job)

        success = True
        
      finally:
        kill('SearchPerfTest', vmstatProcess)
        kill('vmstat', vmstatProcess)
        kill('ZVRobot', zvRobotProcess)
        kill('java', p)
        if not success:
          system('rm -rf ZVRobot_2012*')
        if psThread is not None:
          stopPSThread = True
          psThread.join()
        
      if DO_ZV_ROBOT and desc.startswith('Zing'):
        found = False
        l = os.listdir('.')
        for fileName in l:
          if fileName.find('ZVRobot') != -1:
            if not found:
              os.rename(fileName, '%s/%s' % (logsDir, fileName))
              found = True
            else:
              raise RuntimeError('more than one ZVRobot log dir')
        if not found:
          raise RuntimeError('could not find ZVRobot log dir')

      print '  done'
      open('%s/done' % logsDir, 'wb').close()

    if QPS_END is not None and targetQPS >= QPS_END:
      break

    targetQPS += QPS_INC

  now = datetime.datetime.now()

  print
  print '%s: ALL DONE (elapsed time %s)' % (now, now - startTime)
  print

def main():
  if os.path.exists(LOGS_DIR):
    raise RuntimeError('please move last logs dir away')

  os.makedirs(LOGS_DIR)

  logOut = open('%s/log.txt' % LOGS_DIR, 'wb')
  teeStdout = Tee(logOut, 'stdout')
  teeStderr = Tee(logOut, 'stderr')

  try:
    run()
  except:
    traceback.print_exc()
  finally:
    logOut.close()
    os.chmod('%s/log.txt' % LOGS_DIR, 0444)
    del teeStdout
    del teeStderr
    
if __name__ == '__main__':
  main()
