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
import email.mime.text
import smtplib

def usage():
  print
  print('Usage: python -u %s -config <config>.py [-smoke]' % sys.argv[0])
  print
  sys.exit(1)

SMOKE_TEST = '-smoke' in sys.argv

if '-help' in sys.argv:
  usage()
  
try:
  idx = sys.argv.index('-config')
except ValueError:
  configFile = 'localconfig.py'
else:
  configFile = sys.argv[idx+1]

exec(open(configFile).read())

LOGS_DIR = 'logs'

REMOTE_CLIENT = 'sendTasks.py'

SERVER_PORT = 7777

LUCENE_HOME = '/please/configure/this'
TASKS_FILE = '/please/configure/this'
LINE_DOCS_FILE = '/please/configure/this'
DIRECT_INDEX_PATH = '/please/configure/this'
LUCENE41_INDEX_PATH = '/please/configure/this'
ZING_JVM = '/please/configure/this'
ORACLE_JVM = '/please/configure/this'
ZV_ROBOT_JAR = '/please/configure/this'
JHICCUP_PATH = '/please/configure/this'
ANALYZER = 'please.configure.This'
HIGHLIGHT_IMPL = 'please.configure.This'
COMMIT_POINT = 'please.configure.this'
SERVER_HOST = 'please.configure.this'
DO_EMAIL = False
DO_STOP_START_ZST = False
DO_NRT = False
DO_ZV_ROBOT = False
DO_AUTO_QPS = False
USE_SMTP = False
VERBOSE_INDEXING = False
REOPEN_EVERY_SEC = False
SEARCH_THREAD_COUNT = 10
DOCS_PER_SEC_PER_THREAD = 10
WARMUP_SEC = 10
TOP_N = 10
RUN_TIME_SEC = 60
FRAGGER_ALLOC_MB_PER_SEC = 10
AUTO_QPS_START = 10
AUTO_QPS_PERCENT_POINTS = 5
QPS_START = 10
QPS_END = 20
QPS_INC = 1
TASKS_PER_CAT = 10
ENABLE_THP = False
MAX_HEAP_GB = None
FRAGGER_JAR = None
CMS_NEW_GEN_SIZE = None
CLIENT_HOST = None
CLIENT_USER = 'somebody'
JOBS = []

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
  print()
  print('Started: %s' % datetime.datetime.now())
  print('Python version: %s' % sys.version)
  svnRev = os.popen('svnversion %s' % LUCENE_HOME).read().strip()
  print('Lucene svn rev is %s (%s)' % (svnRev, LUCENE_HOME))
  if svnRev.endswith('M'):
    if system('svn diff %s > %s/lucene.diffs 2>&1' % (LUCENE_HOME, logsDir)):
      raise RuntimeError('svn diff failed')
    os.chmod('%s/lucene.diffs' % logsDir, 0o444)

  luceneUtilDir = os.path.abspath(os.path.split(sys.argv[0])[0])

  luceneUtilRev = os.popen('hg id %s' % luceneUtilDir).read().strip()  
  print('Luceneutil hg rev is %s (%s)' % (luceneUtilRev, luceneUtilDir))
  if luceneUtilRev.find('+') != -1:
    if system('hg diff %s > %s/luceneutil.diffs 2>&1' % (luceneUtilDir, logsDir)):
      raise RuntimeError('hg diff failed')
    os.chmod('%s/luceneutil.diffs' % logsDir, 0o444)

  for fileName in ('responseTimeTests.py', TASKS_FILE, configFile):
    shutil.copy('%s/%s' % (luceneUtilDir, fileName),
                '%s/%s' % (logsDir, fileName))
    os.chmod('%s/%s' % (logsDir, fileName), 0o444)

  for fileName in ('/sys/kernel/mm/transparent_hugepage/enabled',
                   '/sys/kernel/mm/redhat_transparent_hugepage/enabled'):
    if os.path.exists(fileName):
      s = open(fileName, 'rb').read().strip()
      print('Transparent huge pages @ %s: currently %s' % (fileName, s))
      if not ENABLE_THP:
        if s.find('[never]') == -1:
          open(fileName, 'wb').write('never')
          print('  now setting to [never]...')
        else:
          print('  already disabled')
      else:
        if s.find('[always]') == -1:
          open(fileName, 'wb').write('always')
          print('  now setting to [always]...')
        else:
          print('  already enabled')
        
def kill(name, p):
  while True:
    for l in os.popen('ps ww | grep %s | grep -v grep | grep -v /bin/sh' % name).readlines():
      l2 = l.strip().split()
      pid = int(l2[0])
      print('  stop %s process %s: %s' % (name, pid, l.strip()))
      try:
        os.kill(pid, signal.SIGKILL)
      except OSError as e:
        print('    OSError: %s' % str(e))
    if p.poll() is not None:
      print('  done killing "%s"' % name)
      return
    time.sleep(2.0)
    
class TopThread(threading.Thread):

  def __init__(self, logFileName):
    threading.Thread.__init__(self)
    self.logFileName = logFileName
    self.stop = False

  def run(self):

    startTime = time.time()
    f = open(self.logFileName, 'wb')
    try:
      while not self.stop:
        # ps axuw | sed "1 d" | sort -n -r -k3 | head

        # Run top every 3 sec:
        for _ in range(6):
          if self.stop:
            break
          time.sleep(0.5)

        f.write('\n\nTime %.1f s:\n' % (time.time() - startTime))
        #p = os.popen('ps axuw | sed "1 d" | sort -n -r -k3')
        sawHeader = False
        p = os.popen('COLUMNS=10000 top -c -b -n1')
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
        f.flush()

    finally:
      f.close()

def system(command):
  #print '  run: %s' % command
  p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
  output = p.communicate()[0].strip()
  if len(output) > 0:
    print('  %s' % output.replace('\n', '\n  '))
  return p.returncode

def runOne(startTime, desc, dirImpl, postingsFormat, targetQPS, pct=None):

  if pct is not None:
    details = ' autoPct=%s' % pct
  else:
    details = ''

  print()
  print('%s: config=%s, dir=%s, postingsFormat=%s, QPS=%s %s' % \
        (datetime.datetime.now(), desc, dirImpl, postingsFormat, targetQPS, details))

  logsDir = '%s/%s.%s.%s.qps%s' % (LOGS_DIR, desc, dirImpl, postingsFormat, targetQPS)
  if pct is not None:
    logsDir += '.pct%s' % pct

  if postingsFormat == 'Lucene41':
    indexPath = LUCENE41_INDEX_PATH
  else:
    indexPath = DIRECT_INDEX_PATH

  os.makedirs(logsDir)

  finished = False
    
  if desc.startswith('Zing'):
    if DO_STOP_START_ZST:
      while True:
        if system('sudo service zing-memory start 2>&1'):
          print('Failed to start zing-memory... retry; java processes:')
          system('ps axuw | grep java')
          time.sleep(2.0)
        else:
          break
    javaCommand = ZING_JVM
  else:
    if DO_STOP_START_ZST:
      while True:
        if system('sudo service zing-memory stop 2>&1'):
          print('Failed to stop zing-memory... retry; java processes:')
          system('ps axuw | grep java')
          time.sleep(2.0)
        else:
          break
    javaCommand = ORACLE_JVM

  command = []
  w = command.append
  w(javaCommand)

  # w('-agentlib:yjpagent=sampling,disablej2ee,alloceach=10')

  if desc.find('CMS') != -1:
    w('-XX:+UseConcMarkSweepGC')
    #w('-XX:PrintFLSStatistics=1')
    if CMS_NEW_GEN_SIZE is not None:
      w('-XX:NewSize=%s' % CMS_NEW_GEN_SIZE)
  elif desc.find('G1') != -1:
    w('-XX:+UnlockExperimentalVMOptions -XX:+UseG1GC')

  if dirImpl == 'MMapDirectory' and postingsFormat == 'Lucene41':
    w('-Xmx4g')
  elif MAX_HEAP_GB is not None:
    w('-Xms%sg' % MAX_HEAP_GB)
    w('-Xmx%sg' % MAX_HEAP_GB)

  w('-Xloggc:%s/gc.log' % logsDir)

  if DO_ZV_ROBOT and desc.startswith('Zing'):
    w('-XX:ARTAPort=8111')

  w('-verbose:gc')
  w('-XX:+PrintGCDetails')
  w('-XX:+PrintGCTimeStamps')
  w('-XX:+PrintHeapAtGC')
  w('-XX:+PrintTenuringDistribution')
  w('-XX:+PrintGCApplicationStoppedTime')
  w('-XX:PrintCMSStatistics=2')
  if desc.startswith('Zing'):
    w('-XX:+PrintCommandLine')
  w('-XX:+PrintCommandLineFlags')
  #w('-XX:+PrintFlagsFinal')
  cp = '.:$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/memory/classes/java:$LUCENE_HOME/build/codecs/classes/java:$LUCENE_HOME/build/highlighter/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java'.replace('$LUCENE_HOME', LUCENE_HOME)
  if FRAGGER_JAR is not None:
    cp = FRAGGER_JAR + ':' + cp
  w('-cp')
  w(cp)
  w('perf.SearchPerfTest')
  w('-indexPath %s' % indexPath)
  if dirImpl == 'RAMDirectory' and postingsFormat == 'Direct':
    # Leaves postings on disk (since they will be turned into
    # DirectPF in RAM), and loads everything else into RAM:
    w('-dirImpl RAMExceptDirectPostingsDirectory')
  else:
    w('-dirImpl %s' % dirImpl)
  w('-analyzer %s' % ANALYZER)
  w('-taskSource server:%s:%s' % (SERVER_HOST, SERVER_PORT))
  w('-searchThreadCount %d' % SEARCH_THREAD_COUNT)
  w('-field body')
  w('-similarity DefaultSimilarity')
  w('-commit %s' % COMMIT_POINT)
  w('-seed 0')
  w('-staticSeed 0')
  w('-hiliteImpl %s' % HIGHLIGHT_IMPL)
  w('-topN %d' % TOP_N)

  serverLog = '%s/server.log' % logsDir
  w('-log %s' % serverLog)

  # Do indexing/NRT reopens:
  if DO_NRT:
    if VERBOSE_INDEXING:
      w('-verbose')
    w('-nrt')
    w('-indexThreadCount 1')
    w('-docsPerSecPerThread %s' % DOCS_PER_SEC_PER_THREAD)
    w('-lineDocsFile %s' % LINE_DOCS_FILE)
    w('-reopenEverySec %g' % REOPEN_EVERY_SEC)
    w('-store')
    w('-tvs')
    w('-postingsFormat %s' % postingsFormat)
    w('-idFieldPostingsFormat %s' % postingsFormat)
    w('-cloneDocs')

  stdLog = '%s/std.log' % logsDir

  if FRAGGER_JAR is not None:
    idx = command.index('perf.SearchPerfTest')
    command = '%s org.managedruntime.perftools.Fragger -v -a %s -exec %s' % (' '.join(command[:idx]), FRAGGER_ALLOC_MB_PER_SEC, ' '.join(command[idx:]))
  else:
    command = ' '.join(command)
  
  command = '%s -d %s -l %s/hiccups %s > %s 2>&1' % \
            (JHICCUP_PATH, WARMUP_SEC*1000, logsDir, command, stdLog)

  p = None
  vmstatProcess = None
  zvRobotProcess = None
  clientProcess = None
  topThread = None
  success = False

  try:

    touchCmd = '%s -Xmx1g -cp .:$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/codecs/classes/java:$LUCENE_HOME/build/highlighter/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java perf.OpenCloseIndexWriter %s 2>&1'.replace('$LUCENE_HOME', LUCENE_HOME) % (javaCommand, indexPath)
    #print '  run %s' % touchCmd
    while True:
      print('  clean index')
      if system(touchCmd):
        print('   failed .. retry')
        time.sleep(2.0)
      else:
        break

    t0 = time.time()
    vmstatProcess = subprocess.Popen('vmstat 1 > %s/vmstat.log 2>&1' % logsDir, shell=True)
    print('  server command: %s' % command)
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)

    if DO_ZV_ROBOT and desc.startswith('Zing'):
      cmd = '%s -Xmx1g -jar %s %s/ZVRobot %s/ZVRobot.prop > %s/ZVRobot.log 2>&1' % \
            (ORACLE_JVM, ZV_ROBOT_JAR, logsDir, os.path.split(ZV_ROBOT_JAR)[0], logsDir)
      print('  ZVRobot command: %s' % cmd)
      zvRobotProcess = subprocess.Popen(cmd, shell=True)
      del cmd
    else:
      zvRobotProcess = None

    print('  wait for server startup...')

    time.sleep(2.0)

    while True:
      try:
        if open(stdLog).read().find('  ready for client...') != -1:
          break
        v = p.poll()
        if p.poll() is not None:
          raise RuntimeError('  failed to start:\n\n%s' % open(stdLog).read())
      except IOError:
        pass
      time.sleep(1.0)

    print('  %.1f sec to start; start test now' % (time.time()-t0))

    time.sleep(2.0)

    topThread = TopThread('%s/top.log' % logsDir)
    topThread.setDaemon(True)
    topThread.start()

    t0 = time.time()
    if CLIENT_HOST is not None:
      # Remote client:
      command = 'python -u %s %s %s %s %s %d %.1f results.bin' % \
                (REMOTE_CLIENT, TASKS_FILE, SERVER_HOST, SERVER_PORT, targetQPS, TASKS_PER_CAT, RUN_TIME_SEC)
      command = 'ssh %s@%s %s > %s/client.log 2>&1' % (CLIENT_USER, CLIENT_HOST, command, logsDir)

      print('  client command: %s' % command)
      clientProcess = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
      output = clientProcess.communicate()[0].strip()
      if len(output) > 0:
        print('  %s' % output.replace('\n', '\n  '))
      if clientProcess.returncode:
        raise RuntimeError('client failed; see %s/client.log' % logsDir)

      print('  copy results.bin back...')
      if system('scp %s@%s:results.bin %s > /dev/null 2>&1' % (CLIENT_USER, CLIENT_HOST, logsDir)):
        raise RuntimeError('scp results.bin failed')

      if system('ssh %s@%s rm -f results.bin' % (CLIENT_USER, CLIENT_HOST)):
        raise RuntimeError('rm results.bin failed')

    else:
      clientProcess = None
      f = open('%s/client.log' % logsDir, 'wb')
      sendTasks.run(TASKS_FILE, 'localhost', SERVER_PORT, targetQPS, TASKS_PER_CAT, RUN_TIME_SEC, '%s/results.bin' % logsDir, f, False)
      f.close()

    t1 = time.time()
    print('  test done (%.1f total sec)' % (t1-t0))

    if not SMOKE_TEST and (t1 - t0) > RUN_TIME_SEC * 1.30:
      print('  marking this job finished')
      finished = True

  finally:
    kill('SearchPerfTest', p)
    
    kill('vmstat', vmstatProcess)
    if clientProcess is not None:
      kill('sendTasks.py', clientProcess)
      if not os.path.exists('%s/results.bin' % logsDir):
        print('  copy results.bin back...')
        system('scp %s@%s:results.bin %s > /dev/null 2>&1' % (CLIENT_USER, CLIENT_HOST, logsDir))
      
    if DO_ZV_ROBOT and zvRobotProcess is not None:
      kill('ZVRobot', zvRobotProcess)
    if topThread is not None:
      topThread.stop = True
      topThread.join()
      print('  done stopping top')

  try:
    printAvgCPU('%s/top.log' % logsDir)
  except:
    print('WARNING: failed to compute avg CPU usage:')
    traceback.print_exc()

  print('  done')
  open('%s/done' % logsDir, 'wb').close()
  if DO_EMAIL and os.path.getsize('%s/log.txt' % LOGS_DIR) < 5*1024*1024:
    try:
      emailResult(open('%s/log.txt' % LOGS_DIR).read(), 'Test RUNNING [%s]' % (datetime.datetime.now() - startTime))
    except:
      print('  send email failed')
      traceback.print_exc()

  return logsDir, finished


def run():

  if SMOKE_TEST:
    print()
    print('***SMOKE_TEST***')
    print()

  captureEnv(LOGS_DIR)

  print('Compile java sources...')
  cmd = '%sc -Xlint -Xlint:deprecation -cp $LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/highlighter/classes/java:$LUCENE_HOME/build/codecs/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java perf/Args.java perf/IndexThreads.java perf/OpenCloseIndexWriter.java perf/Task.java perf/CreateQueries.java perf/LineFileDocs.java perf/PKLookupPerfTest.java perf/RandomQuery.java perf/SearchPerfTest.java perf/TaskParser.java perf/Indexer.java perf/LocalTaskSource.java perf/PKLookupTask.java perf/RemoteTaskSource.java perf/SearchTask.java perf/TaskSource.java perf/IndexState.java perf/NRTPerfTest.java perf/RespellTask.java perf/ShowFields.java perf/TaskThreads.java perf/KeepNoCommitsDeletionPolicy.java' % ORACLE_JVM
  cmd = cmd.replace('$LUCENE_HOME', LUCENE_HOME)

  if system(cmd):
    raise RuntimeError('compile failed')

  if CLIENT_HOST is not None:
    print('Copy sendTasks.py to client host %s' % CLIENT_HOST)
    if system('scp sendTasks.py %s@%s: > /dev/null 2>&1' % (CLIENT_USER, CLIENT_HOST)):
      raise RuntimeError('copy sendTasks.py failed')
    print('Copy tasks file "%s" to client host %s' % (TASKS_FILE, CLIENT_HOST))
    if system('scp %s %s@%s: > /dev/null 2>&1' % (TASKS_FILE, CLIENT_USER, CLIENT_HOST)):
      raise RuntimeError('copy sendTasks.py failed')

  startTime = datetime.datetime.now()

  finished = set()

  if DO_AUTO_QPS:
    maxQPS = {}
    reQPSOut = re.compile(r'; +([0-9\.]+) qps out')
    reQueueSize = re.compile(r'\[(\d+), (\d+)\]$')
    print()
    print('Find max QPS per job:')
    for job in JOBS:
      desc, dirImpl, postingsFormat = job
      logsDir = runOne(startTime, desc, dirImpl, postingsFormat, 'sweep')[0]
      qpsOut = []
      with open('%s/client.log' % logsDir) as f:
        for line in f.readlines():
          m = reQPSOut.search(line)
          m2 = reQueueSize.search(line)
          if m is not None and m2 is not None and int(m2.group(2)) > 200:
            qpsOut.append(float(m.group(1)))
      if len(qpsOut) < 10:
        raise RuntimeError("couldn't find enough 'qps out' lines: got %d" % len(qpsOut))
      # QPS out is avg of last 5 seconds ... make sure we only measure actual saturation
      qpsOut = qpsOut[5:]
      maxQPS[job] = sum(qpsOut)/len(qpsOut)
      print('  QPS throughput=%.1f' % maxQPS[job])
      if maxQPS[job] < 2*AUTO_QPS_START:
        raise RuntimeError('max QPS for job %s (= %s) is < 2*AUTO_QPS_START (= %s)' % \
                           (desc, maxQPS[job], AUTO_QPS_START))

    for pctPoint in AUTO_QPS_PERCENT_POINTS:
      realJobsLeft = False
      for job in JOBS:
        if job in finished:
          continue

        desc, dirImpl, postingsFormat = job

        targetQPS = AUTO_QPS_START + (pctPoint/100.)*(maxQPS[job] - AUTO_QPS_START)

        if runOne(startTime, desc, dirImpl, postingsFormat, targetQPS, pct=pctPoint)[1]:
          if desc.lower().find('warmup') == -1:
            finished.add(job)
        elif desc.lower().find('warmup') == -1:
          realJobsLeft = True

      if not realJobsLeft:
        break

  else:
    # Which tasks exceeded capacity:
    targetQPS = QPS_START

    while len(finished) != len(JOBS):

      realJobsLeft = False

      for job in JOBS:

        if job in finished:
          continue

        desc, dirImpl, postingsFormat = job

        if runOne(startTime, desc, dirImpl, postingsFormat, targetQPS)[1]:
          if desc.lower().find('warmup') == -1:
            finished.add(job)
        elif desc.lower().find('warmup') == -1:
          realJobsLeft = True

      if QPS_END is not None and targetQPS >= QPS_END:
        break

      if not realJobsLeft:
        break

      targetQPS += QPS_INC

  now = datetime.datetime.now()

  print()
  print('%s: ALL DONE (elapsed time %s)' % (now, now - startTime))
  print()

def printAvgCPU(topLog):

  cpuCoreCount = int(os.popen('grep processor /proc/cpuinfo | wc').read().strip().split()[0])

  entCount = 0
  
  with open(topLog) as f:

    byPid = {}

    cpuCol = None
    for line in f.readlines():
      line = line.strip()
      if line.startswith('Time'):
        cpuCol = None
      elif line.startswith('PID'):
        cpuCol = line.split().index('%CPU')
        entCount += 1
      elif cpuCol is not None and entCount > 20:
        cols = line.split()
        if len(cols) > cpuCol:
          pid = int(cols[0])
          cpu = float(cols[cpuCol])
          if pid not in byPid:
            # sum, min, max, count
            byPid[pid] = [0.0, None, None, 0]
          
          l = byPid[pid]
          l[0] += cpu
          l[3] += 1

          if l[1] is None:
            l[1] = cpu
          else:
            l[1] = min(cpu, l[1])

          if l[2] is None:
            l[2] = cpu
          else:
            l[2] = max(cpu, l[2])

  pids = []
  for pid, (sum, minCPU, maxCPU, count) in byPid.items():
    pids.append((sum/count, minCPU, maxCPU, pid))

  pids.sort(reverse=True)
  print('  CPU usage [%d CPU cores]' % cpuCoreCount)
  for avgCPU, minCPU, maxCPU, pid in pids:
    if maxCPU > 20:
      print('    avg %7.2f%% CPU, min %7.2f%%, max %7.2f%% pid %s' % (avgCPU, minCPU, maxCPU, pid))

def emailResult(body, subject):
  fromAddress = toAddress = 'mail@mikemccandless.com'

  msg = email.mime.text.MIMEText(body)
  msg["From"] = fromAddress
  msg["To"] = toAddress
  msg["Subject"] = subject

  message = msg.as_string()

  if USE_SMTP:
    if False:
      s = smtplib.SMTP('localhost')
    else:
      import localpass
      s = smtplib.SMTP(localpass.SMTP_SERVER, port=localpass.SMTP_PORT)
      s.ehlo(fromAddress)
      s.starttls()
      s.ehlo(fromAddress)
      localpass.smtplogin(s)
    print('sending mail...')
    s.sendmail(fromAddress, (toAddress,), message)
    print('quitting smtp...')
    s.quit()
  else:
    p = subprocess.Popen(["/usr/sbin/sendmail", "-t"], stdin=subprocess.PIPE)
    p.communicate(message)

def main():
  if os.path.exists(LOGS_DIR):
    raise RuntimeError('please move last logs dir away')

  os.makedirs(LOGS_DIR)

  logOut = open('%s/log.txt' % LOGS_DIR, 'wb')
  teeStdout = Tee(logOut, 'stdout')
  teeStderr = Tee(logOut, 'stderr')

  failed = False
  
  try:
    run()
  except:
    traceback.print_exc()
    failed = True
  finally:
    if os.path.exists('/localhome/ftpit.sh'):
      system('/localhome/ftpit.sh')
    logOut.flush()
    if DO_EMAIL and os.path.getsize('%s/log.txt' % LOGS_DIR) < 5*1024*1024:
      if failed:
        subject = 'Test FAIL'
      else:
        subject = 'Test SUCCESS'
      emailResult(open('%s/log.txt' % LOGS_DIR).read(), subject)
    logOut.close()
    os.chmod('%s/log.txt' % LOGS_DIR, 0o444)
    del teeStdout
    del teeStderr
    
if __name__ == '__main__':
  main()
