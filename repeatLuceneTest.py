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
import datetime
import os
import sys
import random
import common
import constants
import re
import threading

# NOTE
#   - only works in the lucene subdir, ie this runs equivalent of "ant test-core"

ROOT = common.findRootDir(os.getcwd())

try:
  idx = sys.argv.index('-jvms')
except ValueError:
  jvmCount = 1
else:
  jvmCount = int(sys.argv[1+idx])
  del sys.argv[idx:idx+2]

osName = common.osName

JAVA_ARGS = '-Xmx512m -Xms512m'
# print
# print 'WARNING: *** running java w/ 8 GB heap ***'
# print
# JAVA_ARGS = '-Xmx8g -Xms8g'

def getArg(argName, default, hasArg=True):
  try:
    idx = sys.argv.index(argName)
  except ValueError:
    v = default
  else:
    if hasArg:
      v = sys.argv[idx+1]
      del sys.argv[idx:idx+2]
    else:
      v = True
      del sys.argv[idx]
  return v

reRepro = re.compile('NOTE: reproduce with(.*?)$', re.MULTILINE)
reDefines = re.compile('-D(.*?)=(.*?)(?: |$)')
def printReproLines(logFileName):
  f = open(logFileName, 'rb')
  print
  while True:
    line = f.readline()
    if line == '':
      break
    m = reRepro.search(line)
    codec = None
    mult = 1
    if m is not None:
      for x in reDefines.findall(line):
        k, v = x
        if k == 'testcase':
          testCase = v
        elif k == 'testmethod':
          testCase += '.%s' % v
        elif k == 'tests.seed':
          seed = v
        elif k == 'tests.codec':
          codec = v
        elif k == 'tests.multiplier':
          mult = v
        else:
          print 'WARNING: don\'t know how to repro k/v=%s' % str(x)

      if codec is not None:
        extra = '-codec %s' % codec
      else:
        extra = ''
      if extra != '':
        extra = ' ' + extra

      if mult != 1:
        if extra == '':
          extra = '-mult %s' % mult
        else:
          extra += ' -mult %s' % mult

      s = 'REPRO: %s %s -seed %s %s'%  (constants.REPRO_COMMAND_START, testCase, seed, extra)
      if constants.REPRO_COMMAND_END != '':
        s += ' %s' % constants.REPRO_COMMAND_END
      print s

tup = os.path.split(os.getcwd())

sub = os.path.split(tup[0])[0]
sub = os.path.split(sub)[1]

if os.path.exists('/dev/shm'):
  logDirName = '/dev/shm/logs/%s' % sub
else:
  logDirName = '/tmp/logs/%s' % sub
  if osName == 'windows':
    logDirName = 'c:' + logDirName

doLog = not getArg('-nolog', False, False)
doCompile = not getArg('-noc', False, False)

print 'Logging to dir %s' % logDirName

if doLog:
  if False:
    if os.path.exists(logDirName):
      shutil.rmtree(logDirName)
  if not os.path.exists(logDirName):      
    os.makedirs(logDirName)

if doCompile:
  print 'Compile...'
  try:
    if os.getcwd().endswith('lucene'):
      #res = os.system('ant compile-core compile-test common.compile-test > compile.log 2>&1')
      res = os.system('%s compile-core compile-test > compile.log 2>&1' % constants.ANT_EXE)
    else:
      res = os.system('%s compile-test > compile.log 2>&1' % constants.ANT_EXE)
    if res:
      print open('compile.log', 'rb').read()
      sys.exit(1)
  finally:
    os.remove('compile.log')

onlyOnce = getArg('-once', False, False)
mult = int(getArg('-mult', 1))
postingsFormat = getArg('-pf', 'random')
codec = getArg('-codec', 'random')
sim = getArg('-sim', 'random')
dir = getArg('-dir', 'random')
verbose = getArg('-verbose', False, False)
iters = int(getArg('-iters', 1))
seed = getArg('-seed', None)
dvFormat = getArg('-dvFormat', None)
nightly = getArg('-nightly', None, False)
keepLogs = getArg('-keeplogs', False, False)
# -Dtests.heapsize=XXX if running ant
heap = getArg('-heap', None, True)
if heap is not None:
  JAVA_ARGS = JAVA_ARGS.replace('512m', heap)

if len(sys.argv) == 1:
  print '\nERROR: no test specified\n'
  sys.exit(1)

tests = []
for test in sys.argv[1:]:
  if not test.startswith('org.'):
    tup = common.locateTest(test)
    if tup is None:
      print '\nERROR: cannot find test %s\n' % test
      sys.exit(1)
    testClass, testMethod = tup
    tests.append((testClass, testMethod))

JAVA_ARGS += ' -cp "%s"' % common.pathsep().join(common.getLuceneTestClassPath(ROOT))
OLD_JUNIT = os.path.exists('lib/junit-3.8.2.jar')

failed = False

iterLock = threading.Lock()
iter = 0

def nextIter(threadID, logFileName):
  global iter
  
  with iterLock:
    print
    if logFileName is None:
      print '%s [%d, thread %d]:' % (datetime.datetime.now(), iter, threadID)
    else:
      print '%s [%d, thread %d]: %s' % (datetime.datetime.now(), iter, threadID, logFileName)
    iter += 1
    return iter
  
def run(threadID):

  global failed

  TEST_TEMP_DIR = '%s/lucene/build/core/test/reruns.%s.%s.t%d' % (ROOT, tests[0][0].split('.')[-1], tests[0][1], threadID)

  upto = 0
  first = True
  while not failed:
    for testClass, testMethod in tests:
      if testMethod is not None:
        s = '%s#%s' % (testClass, testMethod)
      else:
        s = testClass

      if doLog:
        logFileName = '%s/%s.%s.%d.t%d.log' % (logDirName, tests[0][0].split('.')[-1], tests[0][1], upto, threadID)
      else:
        logFileName = None

      iter = nextIter(threadID, logFileName)

      if False:
        if doLog:
          print 'iter %s %s TEST: %s -> %s' % (iter, datetime.datetime.now(), s, logFileName)
        else:
          print 'iter %s %s TEST: %s' % (iter, datetime.datetime.now(), s)

      command = '%s %s -DtempDir=%s -ea' % (constants.JAVA_EXE, JAVA_ARGS, TEST_TEMP_DIR)
      if False and constants.JRE_SUPPORTS_SERVER_MODE and random.randint(0, 1) == 1:
        command += ' -server'
      if False and random.randint(0, 1) == 1 and not onlyOnce:
        command += ' -Xbatch'
      #command += ' -Dtests.locale=random'
      #command += ' -Dtests.timezone=random'
      #command += ' -Dtests.lockdir=build'
      command += ' -Dtests.cleanthreads=perMethod'
      command += ' -Djava.util.logging.config.file=%s/lucene/tools/junit4/logging.properties' % ROOT
      command += ' -Dtests.timeoutSuite=2147483647'
      command += ' -Dtests.verbose=%s' % str(verbose).lower()
      command += ' -Dtests.infostream=%s' % str(verbose).lower()
      command += ' -Dtests.multiplier=%s' % mult
      command += ' -Dtests.iters=%s' % iters
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

      command += ' -Djetty.testMode=1'
      command += ' -Djetty.insecurerandom=1'
      command += ' -Dtests.asserts.gracious=false'
      #command += ' -Djava.security.manager=org.apache.lucene.util.TestSecurityManager'
      command += ' -Djava.security.policy=%s/lucene/tools/junit4/tests.policy' % ROOT

      if OLD_JUNIT:
        command += ' junit.textui.TestRunner'
      else:
        command += ' org.junit.runner.JUnitCore'

      command += ' %s' % testClass

      if doLog:
        command += ' > %s 2>&1' % logFileName

      if os.path.exists(TEST_TEMP_DIR):
        #print '  remove %s' % TEST_TEMP_DIR
        try:
          shutil.rmtree(TEST_TEMP_DIR)
        except OSError:
          pass

      if first:
        print '  RUN: %s' % command
        first = False
        
      res = os.system(command)

      if res:
        if logFileName is None:
          print '  FAILED'
        else:
          print '  FAILED [log %s]' % logFileName
          
        if doLog:
          printReproLines(logFileName)
        failed = True
        raise RuntimeError('hit fail')
      elif doLog:
        if not keepLogs:
          os.remove(logFileName)
        else:
          upto += 1

    if onlyOnce:
      break

if jvmCount > 1:
  threads = []
  for threadID in xrange(jvmCount):
    t = threading.Thread(target=run, args=(threadID,))
    t.start()
    threads.append(t)
else:
  run(0)
