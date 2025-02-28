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

import shutil
import signal
import time
import datetime
import os
import sys
import common
import constants
import re
import threading
import subprocess
import json

# NOTE
#   - only works in the module's working directory,
#     e.g. "lucene/core", or "lucene/analyzers/common"

# TODO
#   - we currently cannot detect if a test did not in fact run because
#     it was @Ignore, @Nightly, or hit an AssumptionViolatedExc ... we
#     should fail in that case, if nothing actually ran

ROOT = common.findRootDir(os.getcwd())

# True to use simple JUnit test runner; False to use
# randomizedtesting's runner:
USE_JUNIT = True

ASSERTS = True

osName = common.osName

#JAVA_ARGS = '-Xmx512m -Xms512m -XX:+UseG1GC'
#JAVA_ARGS = '-Xmx512m -Xms512m -server -XX:+UseSerialGC'
#JAVA_ARGS += ' -XX:+PrintCompilation -XX:+UnlockDiagnosticVMOptions -XX:CompileCommand=print,*Version.init'
# print
# print 'WARNING: *** running java w/ 8 GB heap ***'
# print
# JAVA_ARGS = '-Xmx8g -Xms8g'
JAVA_ARGS = '-Xmx512m -Xms512m'

def getArg(argName, default, hasArg=True):
  try:
    idx = sys.argv.index(argName)
  except ValueError:
    v = default
  else:
    if hasArg:
      v = sys.argv[idx+1]
      del sys.argv[idx:idx+2]
      try:
        sys.argv.index(argName)
      except ValueError:
        # ok
        pass
      else:
        raise RuntimeError('argument %s appears more than once' % argName)
    else:
      v = True
      del sys.argv[idx]
  return v

def error(message):
  beep()
  print(('ERROR: %s' % message))
  print()
  sys.exit(1)

def beep():
  print('\a\a\a')  

reRepro = re.compile('NOTE: reproduce with(.*?)$', re.MULTILINE)
reDefines = re.compile('-D(.*?)=(.*?)(?: |$)')
def printReproLines(logFileName, iters):
  with open(logFileName, 'r') as f:
    print()
    while True:
      line = f.readline()
      if line == '':
        break
      m = reRepro.search(line)
      if m is not None:
        parseReproLine(line, iters)
        break

def parseReproLine(line, iters):

  codec = None
  mult = 1
  
  for x in reDefines.findall(line):
    k, v = x
    if k == 'testcase':
      testCase = v
    elif k == 'testmethod':
      testCase += '.%s' % v
    elif k == 'tests.method':
      testCase += '.%s' % v
    elif k == 'tests.seed':
      seed = v
    elif k == 'tests.codec':
      codec = v
    elif k == 'tests.multiplier':
      mult = v
    else:
      print('WARNING: don\'t know how to repro k/v=%s' % str(x))

  extra = []
  if codec is not None:
    extra.append('-codec %s' % codec)

  if mult != 1:
    extra.append('-mult %s' % mult)

  s = 'REPRO: %s %s -seed %s %s'%  (constants.REPRO_COMMAND_START, testCase, seed, ' '.join(extra))
  if constants.REPRO_COMMAND_END != '':
    s += ' %s' % constants.REPRO_COMMAND_END

  if iters != 1:
    s = s.strip() + ' -iters %s' % iters
    
  print(('\n%s\n' % s))

tup = os.path.split(os.getcwd())

sub = os.path.split(tup[0])[0]
sub = os.path.split(sub)[1]

logDirName = getArg('-logDir', '%s/lucene/build' % ROOT)

doCompile = not getArg('-noc', False, False)
doLog = not getArg('-nolog', False, False)
jvmCount = int(getArg('-jvms', 1))

if jvmCount != 1:
  doLog = True
  
if doLog:
  print('\nLogging to dir %s' % logDirName)

if not os.path.exists(logDirName):      
  os.makedirs(logDirName)

onlyOnce = getArg('-once', False, False)
mult = int(getArg('-mult', 1))
locale = getArg('-locale', None)
postingsFormat = getArg('-pf', 'random')
codec = getArg('-codec', 'random')
sim = getArg('-sim', 'random')
dir = getArg('-dir', 'random')
monsters = getArg('-monsters', False, False)
verbose = getArg('-verbose', False, False)
iters = int(getArg('-iters', 1))
seed = getArg('-seed', None)
dvFormat = getArg('-dvFormat', None)
nightly = getArg('-nightly', None, False)
keepLogs = getArg('-keeplogs', False, False)
slow = getArg('-slow', True, False)
# -Dtests.heapsize=XXX if running ant
heap = getArg('-heap', None, True)
if heap is not None:
  JAVA_ARGS = JAVA_ARGS.replace('512m', heap)

testTmpDir = getArg('-tmpDir', None)
if testTmpDir is None:
  testTmpDir = '%s/lucene/build/core/test' % ROOT
else:
  print('test temp dir %s' % testTmpDir)

#print('args: %s' % sys.argv)

# sys.argv also contains the name of the script, so if it's 
# length is 1, it means no test was specified
if len(sys.argv) == 1:
  print('\nERROR: no test specified\n')
  sys.exit(1)

tests = []
testLogFile = None

for test in sys.argv[1:]:
  if test.startswith('/dev/shm/'):
    testLogFile = test
  elif not test.startswith('org.'):
    tup = common.locateTest(test)
    if tup is None:
      print('\nERROR: cannot find test class %s.java\n' % test)
      sys.exit(1)
    testClass, testMethod = tup
    tests.append((testClass, testMethod))

OLD_JUNIT = os.path.exists('lib/junit-3.8.2.jar')

if doCompile:
  # Compile, but only send output to stdout if it fails:
  print('Compile...')
  try:
    if os.getcwd().endswith('lucene'):
      res = os.system('%s compile-core compile-test > %s/compile.log 2>&1' % (constants.ANT_EXE, logDirName))
    else:
      res = os.system('%s compile-test > %s/compile.log 2>&1' % (constants.ANT_EXE, logDirName))
    if res:
      print(open('%s/compile.log' % logDirName, 'r').read())
      sys.exit(1)
  finally:
    os.remove('%s/compile.log' % logDirName)

#JAVA_ARGS += ' -XX:+HeapDumpOnOutOfMemoryError -agentlib:yjpagent=alloceach=10,allocsizelimit=1024 -cp "%s"' % common.pathsep().join(common.getLuceneTestClassPath(ROOT))
JAVA_ARGS += ' -XX:+HeapDumpOnOutOfMemoryError -cp "%s"' % common.pathsep().join(common.getLuceneTestClassPath(ROOT))

failed = False

iterLock = threading.Lock()
iter = 0

def nextIter(threadID, logFileName, secLastIter):
  global iter
  
  with iterLock:
    print('')
    if logFileName is None:
      print('%s [%d, jvm %d, %5.1fs]:' % (datetime.datetime.now(), iter, threadID, secLastIter))
    else:
      print('%s [%d, jvm %d, %5.1fs]: %s' % (datetime.datetime.now(), iter, threadID, secLastIter, logFileName))
    iter += 1
    return iter

def eventToLog(eventsFileIn, fileOut):

  """
  Appends all stdout/stderr from the events file, to human readable form.
  """

  r = re.compile('^    "chunk": "(.*?)"$')
  with open(eventsFileIn, 'r') as f:
    with open(fileOut, 'wb') as fOut:
      while True:
        line = f.readline()
        if line == '':
          break
        m = r.match(line)
        if m is not None:
          s = m.group(1)
          s = s.replace('\\"', '"')
          s = s.replace('%0A', '\n')
          s = s.replace('%09', '\t')
          fOut.write(s)
  
def run(threadID):
  global failed
  success = False
  try:
    _run(threadID)
    success = True
  finally:
    if not success:
      failed = True

def _run(threadID):
  global failed
  TEST_TEMP_DIR = '%s/reruns.%s.%s.t%d' % (testTmpDir, tests[0][0].split('.')[-1], tests[0][1], threadID)

  upto = 0
  first = True
  secLastIter = 0.0
  while not failed:
    for testClass, testMethod in tests:
      if testMethod is not None:
        s = '%s#%s' % (testClass, testMethod)
      else:
        s = testClass

      if not USE_JUNIT:
        eventsFile = '%s/lucene/build/C%d.events' % (ROOT, threadID)

      if testLogFile is not None:
        logFileName = testLogFile
      elif doLog:
        logFileName = '%s/%s.%s.%d.t%d.log' % (logDirName, tests[0][0].split('.')[-1], tests[0][1], upto, threadID)
      else:
        logFileName = None

      if not onlyOnce:
        iter = nextIter(threadID, logFileName, secLastIter)

      command = '%s %s -DtempDir=%s' % (constants.JAVA_EXE, JAVA_ARGS, TEST_TEMP_DIR)
      if ASSERTS:
        command += ' -ea:org.apache.lucene... -ea:org.apache.solr...'
      #command += ' -Dtests.locale=random'
      #command += ' -Dtests.timezone=random'
      #command += ' -Dtests.lockdir=build'
      command += ' -Dtests.monster=true'
      command += ' -Dtests.cleanthreads=perMethod'
      command += ' -Djava.util.logging.config.file=%s/lucene/tools/junit4/logging.properties' % ROOT
      command += ' -Dtests.timeoutSuite=2147483647'
      command += ' -Dtests.verbose=%s' % str(verbose).lower()
      command += ' -Dtests.infostream=%s' % str(verbose).lower()
      command += ' -Dtests.multiplier=%s' % mult
      command += ' -Dtests.iters=%s' % iters
      command += ' -Dtests.maxfailures=1'
      command += ' -Dtests.postingsformat=%s' % postingsFormat
      command += ' -Dtests.codec=%s' % codec
      command += ' -Dtests.similarity=%s' % sim
      command += ' -Dtests.directory=%s' % dir
      if dvFormat is not None:
        command += ' -Dtests.docvaluesformat=%s' % dvFormat
      command += ' -Dtests.luceneMatchVersion=4.0'
      if constants.TESTS_LINE_FILE is not None:
        command += ' -Dtests.linedocsfile=%s' % constants.TESTS_LINE_FILE
      if nightly:
        command += ' -Dtests.nightly=true'
      if seed is not None:
        command += ' -Dtests.seed=%s' % seed
      if testMethod is not None:
        command += ' -Dtests.method=%s*' % testMethod
      if locale is not None:
        command += ' -Dtests.locale=%s' % locale

      command += ' -Dtests.slow=%s' % str(slow).lower()
      command += ' -Djetty.testMode=1'
      command += ' -Djetty.insecurerandom=1'
      command += ' -Dtests.asserts.gracious=false'
      if ASSERTS:
        command += ' -Dtests.asserts=true'
      else:
        command += ' -Dtests.asserts=false'
      #command += ' -Djava.security.policy=%s/lucene/tools/junit4/tests.policy' % ROOT
      #command += ' -Djava.security.manager=org.apache.lucene.util.TestSecurityManager'

      # nocommit
      #command += ' -Dtests.leaveTemporary=true'
      
      if USE_JUNIT:
        command += ' org.junit.runner.JUnitCore'
      else:
        command += ' com.carrotsearch.ant.tasks.junit4.slave.SlaveMainSafe -flush'
        command += ' -eventsfile %s' % eventsFile

      command += ' %s' % testClass

      if doLog:
        with open(logFileName, 'w') as f:
          f.write('RUN: %s\n\n' % command)
        command += ' >> %s 2>&1' % logFileName
      else:
        #print('RUN: %s\n\n' % command)
        pass
        
      # print('command: %s' % command)

      if os.path.exists(TEST_TEMP_DIR):
        #print '  remove %s' % TEST_TEMP_DIR
        try:
          shutil.rmtree(TEST_TEMP_DIR)
        except OSError:
          pass
      os.makedirs(TEST_TEMP_DIR)

      if False and first:
        print('  RUN: %s' % command)
        first = False

      t0 = time.time()
      if doLog:
        res = os.system(command)
        if USE_JUNIT:
          with open(logFileName) as f:
            noTestsRun = False
            while True:
              line = f.readline()
              if line == '':
                break
              if line.find('OK (0 tests') != -1:
                noTestsRun = True
                break
      else:

        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)

        noTestsRun = False
        while True:
          line = p.stdout.readline()
          if line == '':
            break
          if line.find('OK (0 tests)') != -1:
            noTestsRun = True
            break
          print(line.rstrip())
          if p.returncode is not None:
            break

        res = p.wait()
      secLastIter = (time.time()-t0)

      if res == signal.SIGINT:
        # Ctrl+C
        failed = True
        break

      if not USE_JUNIT:
        # Parse the events file:
        decoder = json.JSONDecoder()
        s = open(eventsFile).read()
        idx = 0
        testCount = 0
        failCount = 0
        sawTest = False
        ignored = True
        ignoreCount = 0
        while True:
          obj, idx = decoder.raw_decode(s, json.decoder.WHITESPACE.match(s, idx).end())
          # print 'obj %s' % obj
          if obj[0] == 'TEST_STARTED':
            testName = obj[1]['description']
            ignored = False
            sawTest = True
          elif obj[0] == 'TEST_IGNORED':
            ignoreCount += 1
            ignored = True
          elif obj[0] == 'TEST_IGNORED_ASSUMPTION':
            if not onlyOnce:
              print(('\n  TEST SKIPPED: %s' % obj[1]['failure']['message']))
            else:
              print(('\n  TEST SKIPPED: %s' % obj[1]['failure']['trace']))
            ignored = True
          elif obj[0] == 'TEST_FINISHED':
            testName = None
            if not ignored:
              testCount += 1
          elif obj[0] == 'TEST_FAILURE':
            if doLog:
              print('\nERROR: test %s failed; see %s' % (testName, logFileName))
            else:
              print('\nERROR: test %s failed' % testName)
              print(obj[1]['failure']['trace'])
            failCount += 1
            failed = True
          elif obj[0] == 'APPEND_STDERR':
            if obj[1]['chunk'].startswith('NOTE: reproduce with'):
              parseReproLine(obj[1]['chunk'])
            else:
              #print(obj[1]['chunk'])
              pass
          elif obj[0] == 'QUIT':
            break

        if not sawTest:
          # No tests matched:
          print()
          if ignoreCount != 0:
            error('all test cases were marked @Ignore')
          else:
            error('no test matches method "%s" in class %s' % (testMethod, testClass))

        noTestsRun = testCount == 0

      if res:
        if logFileName is None:
          print('  FAILED')
        else:
          print('  FAILED [log %s]' % logFileName)
          
        if doLog:
          printReproLines(logFileName, iters)
        failed = True
        beep()
        break
      elif noTestsRun:
        if onlyOnce:
          failed = True
          error('no test actually ran, due to an Assume or @Ignore/@Nightly/@Slow/etc.')
        else:
          if USE_JUNIT:
            print('  WARNING: no test actually ran, due to typo or an Assume or @Ignore/@Nightly/@Slow/etc.')
          else:
            print('  WARNING: no test actually ran, due to an Assume or @Ignore/@Nightly/@Slow/etc.')
      else:
        if onlyOnce and not USE_JUNIT and not failed:
          print(('  OK [%d tests]\n' % testCount))
        if doLog and USE_JUNIT:
          if not keepLogs:
            pass
            #os.remove(logFileName)
          else:
            upto += 1

    if onlyOnce:
      break

print('\nRun test(s)...')
if jvmCount > 1:
  threads = []
  for threadID in range(jvmCount):
    t = threading.Thread(target=run, args=(threadID,))
    t.start()
    threads.append(t)
  while not failed:
    try:
      time.sleep(.1)
    except KeyboardInterrupt:
      failed = True
      raise
else:
  run(0)

if failed:
  sys.exit(1)
else:
  sys.exit(0)
