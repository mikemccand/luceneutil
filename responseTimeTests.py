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
  print 'Usage: python -u %s -config <config>.py [-smoke]' % sys.argv[0]
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
  print
  print 'Started: %s' % datetime.datetime.now()
  print 'Python version: %s' % sys.version
  svnRev = os.popen('svnversion %s' % LUCENE_HOME).read().strip()
  print 'Lucene svn rev is %s (%s)' % (svnRev, LUCENE_HOME)
  if svnRev.endswith('M'):
    if system('svn diff %s > %s/lucene.diffs 2>&1' % (LUCENE_HOME, logsDir)):
      raise RuntimeError('svn diff failed')
    os.chmod('%s/lucene.diffs' % logsDir, 0444)

  luceneUtilDir = os.path.abspath(os.path.split(sys.argv[0])[0])

  luceneUtilRev = os.popen('hg id %s' % luceneUtilDir).read().strip()  
  print 'Luceneutil hg rev is %s (%s)' % (luceneUtilRev, luceneUtilDir)
  if luceneUtilRev.find('+') != -1:
    if system('hg diff %s > %s/luceneutil.diffs 2>&1' % (luceneUtilDir, logsDir)):
      raise RuntimeError('hg diff failed')
    os.chmod('%s/luceneutil.diffs' % logsDir, 0444)

  for fileName in ('responseTimeTests.py', TASKS_FILE):
    shutil.copy('%s/%s' % (luceneUtilDir, fileName),
                '%s/%s' % (logsDir, fileName))
    os.chmod('%s/%s' % (logsDir, fileName), 0444)

  for fileName in ('/sys/kernel/mm/transparent_hugepage/enabled',
                   '/sys/kernel/mm/redhat_transparent_hugepage/enabled'):
    if os.path.exists(fileName):
      s = open(fileName, 'rb').read().strip()
      print 'Transparent huge pages @ %s: currently %s' % (fileName, s)
      if not ENABLE_THP:
        if s.find('[never]') == -1:
          open(fileName, 'wb').write('never')
          print '  now setting to [never]...'
        else:
          print '  already disabled'
      else:
        if s.find('[always]') == -1:
          open(fileName, 'wb').write('always')
          print '  now setting to [always]...'
        else:
          print '  already enabled'
        
def kill(name, p):
  while True:
    for l in os.popen('ps ww | grep %s | grep -v grep | grep -v /bin/sh' % name).readlines():
      l2 = l.strip().split()
      pid = int(l2[0])
      print '  stop %s process %s: %s' % (name, pid, l.strip())
      try:
        os.kill(pid, signal.SIGKILL)
      except OSError, e:
        print '    OSError: %s' % str(e)
    if p.poll() is not None:
      print '  done killing "%s"' % name
      return
    time.sleep(2.0)
    
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
      f.flush()
      
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

  print 'Compile java sources...'
  cmd = '%sc -Xlint -Xlint:deprecation -cp $LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/highlighter/classes/java:$LUCENE_HOME/build/codecs/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java perf/Args.java perf/IndexThreads.java perf/OpenCloseIndexWriter.java perf/Task.java perf/CreateQueries.java perf/LineFileDocs.java perf/PKLookupPerfTest.java perf/RandomFilter.java perf/SearchPerfTest.java perf/TaskParser.java perf/Indexer.java perf/LocalTaskSource.java perf/PKLookupTask.java perf/RemoteTaskSource.java perf/SearchTask.java perf/TaskSource.java perf/IndexState.java perf/NRTPerfTest.java perf/RespellTask.java perf/ShowFields.java perf/TaskThreads.java perf/KeepNoCommitsDeletionPolicy.java perf/RateLimitingRAMDirectory.java perf/RateLimitingRAMOutputStream.java perf/RateLimitingRAMFile.java' % ORACLE_JVM
  cmd = cmd.replace('$LUCENE_HOME', LUCENE_HOME)

  if system(cmd):
    raise RuntimeError('compile failed')

  if CLIENT_HOST is not None:
    print 'Copy sendTasks.py to client host %s' % CLIENT_HOST
    if system('scp sendTasks.py %s@%s: > /dev/null 2>&1' % (CLIENT_USER, CLIENT_HOST)):
      raise RuntimeError('copy sendTasks.py failed')
    print 'Copy tasks file "%s" to client host %s' % (TASKS_FILE, CLIENT_HOST)
    if system('scp %s %s@%s: > /dev/null 2>&1' % (TASKS_FILE, CLIENT_USER, CLIENT_HOST)):
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
      elif desc.find('G1') != -1:
        w('-XX:+UnlockExperimentalVMOptions -XX:+UseG1GC')

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
      w('.:$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/memory/classes/java:$LUCENE_HOME/build/codecs/classes/java:$LUCENE_HOME/build/highlighter/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java'.replace('$LUCENE_HOME', LUCENE_HOME))
      w('perf.SearchPerfTest')
      w('-indexPath %s' % indexPath)
      if dirImpl == 'RAMDirectory' and postingsFormat == 'Direct':
        # Leaves postings on disk (since they will be turned into
        # DirectPF in RAM), and loads everything else into RAM:
        w('-dirImpl RAMExceptDirectPostingsDirectory')
      else:
        w('-dirImpl %s' % dirImpl)
        #w('-ramMergeMaxMBPerSec %s' % RAM_MERGE_MAX_MB_PER_SEC)
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
      
      command = '%s -d %s -l %s/hiccups %s > %s 2>&1' % \
                (JHICCUP_PATH, WARMUP_SEC*1000, logsDir, ' '.join(command), stdLog)

      p = None
      vmstatProcess = None
      zvRobotProcess = None
      psThread = None
      success = False

      try:

        touchCmd = '%s -Xmx1g -cp .:$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/codecs/classes/java:$LUCENE_HOME/build/highlighter/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java perf.OpenCloseIndexWriter %s 2>&1'.replace('$LUCENE_HOME', LUCENE_HOME) % (javaCommand, indexPath)
        #print '  run %s' % touchCmd
        while True:
          print '  clean index'
          if system(touchCmd):
            print '   failed .. retry'
            time.sleep(2.0)
          else:
            break

        t0 = time.time()
        vmstatProcess = subprocess.Popen('vmstat 1 > %s/vmstat.log 2>&1' % logsDir, shell=True)
        print '  server command: %s' % command
        p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)

        if DO_ZV_ROBOT and desc.startswith('Zing'):
          cmd = '%s -Xmx1g -jar %s %s/ZVRobot %s/ZVRobot.prop > %s/ZVRobot.log 2>&1' % \
                (ORACLE_JVM, ZV_ROBOT_JAR, logsDir, os.path.split(ZV_ROBOT_JAR)[0], logsDir)
          print '  ZVRobot command: %s' % cmd
          zvRobotProcess = subprocess.Popen(cmd, shell=True)
          del cmd
        else:
          zvRobotProcess = None
          
        print '  wait for server startup...'

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

        print '  %.1f sec to start; start test now' % (time.time()-t0)

        time.sleep(2.0)

        stopPSThread = False
        psThread = threading.Thread(target=runPSThread, args=('%s/top.log' % logsDir,))
        psThread.start()

        t0 = time.time()
        if CLIENT_HOST is not None:
          # Remote client:
          command = 'python -u %s %s %s %s %.1f %d %.1f results.bin' % \
                    (REMOTE_CLIENT, TASKS_FILE, SERVER_HOST, SERVER_PORT, targetQPS, TASKS_PER_CAT, RUN_TIME_SEC)

          print '  client command: %s' % command
          if system('ssh %s@%s %s > %s/client.log 2>&1' % (CLIENT_USER, CLIENT_HOST, command, logsDir)):
            raise RuntimeError('client failed; see %s/client.log' % logsDir)

          print '  copy results.bin back...'
          if system('scp %s@%s:results.bin %s > /dev/null 2>&1' % (CLIENT_USER, CLIENT_HOST, logsDir)):
            raise RuntimeError('scp results.bin failed')

          if system('ssh %s@%s rm -f results.bin' % (CLIENT_USER, CLIENT_HOST)):
            raise RuntimeError('rm results.bin failed')

        else:
          f = open('%s/client.log' % logsDir, 'wb')
          sendTasks.run(TASKS_FILE, 'localhost', SERVER_PORT, targetQPS, TASKS_PER_CAT, RUN_TIME_SEC, '%s/results.bin' % logsDir, f, False)
          f.close()
          
        t1 = time.time()
        print '  test done (%.1f total sec)' % (t1-t0)

        if not SMOKE_TEST and (t1 - t0) > RUN_TIME_SEC * 1.3:
          print '  marking this job finished!'
          finished.add(job)

        success = True
        
      finally:
        kill('SearchPerfTest', p)
        kill('vmstat', vmstatProcess)
        if DO_ZV_ROBOT and zvRobotProcess is not None:
          kill('ZVRobot', zvRobotProcess)
        if psThread is not None:
          stopPSThread = True
          psThread.join()
        
      print '  done'
      open('%s/done' % logsDir, 'wb').close()
      if DO_EMAIL and os.path.getsize('%s/log.txt' % LOGS_DIR) < 5*1024*1024:
        try:
          emailResult(open('%s/log.txt' % LOGS_DIR).read(), 'Test RUNNING [%s]' % (datetime.datetime.now() - startTime))
        except:
          print '  send email failed'
          traceback.print_exc()

    if QPS_END is not None and targetQPS >= QPS_END:
      break

    targetQPS += QPS_INC

  now = datetime.datetime.now()

  print
  print '%s: ALL DONE (elapsed time %s)' % (now, now - startTime)
  print

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
    print 'sending mail...'
    s.sendmail(fromAddress, (toAddress,), message)
    print 'quitting smtp...'
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
    os.chmod('%s/log.txt' % LOGS_DIR, 0444)
    del teeStdout
    del teeStderr
    
if __name__ == '__main__':
  main()
