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

# NOTE
#   - only works in the lucene subdir, ie this runs equivalent of "ant test-core"

osName = common.osName

# nocommit
#JAVA_ARGS = '-Xmx512m -Xms512m'
JAVA_ARGS = '-Xmx2g -Xms2g'

allTests = {}
def locateTest(test):
  cwd = os.getcwd()
  tup = test.split('.')
  if len(tup) == 1:
    method = None
  else:
    test, method = tup
  if len(allTests) == 0:
    for root, dirs, files in os.walk('src/test'):
      for f in files:
        if f.endswith('.java') and (f.startswith('Test') or f.endswith('Test.java')):
          path = root[len('src/test/'):].replace(os.sep, '.')
          allTests[f[:-5]] = '%s.%s' % (path, f[:-5])
          #print '%s.%s' % (path, f[:-5])
  if test not in allTests:
    return None
  else:
    return allTests[test], method

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
    
tup = os.path.split(os.getcwd())

if tup[1] != 'lucene' and os.path.exists('lucene'):
  os.chdir('lucene')

sub = os.path.split(tup[0])[1]

if os.path.exists('/dev/shm'):
  logDirName = '/dev/shm/logs/%s' % sub
else:
  logDirName = '/tmp/logs/%s' % sub
  if osName == 'windows':
    logDirName = 'c:' + logDirName
doLog = True

print 'Logging to dir %s' % logDirName

if doLog:
  if os.path.exists(logDirName):
    shutil.rmtree(logDirName)
  os.makedirs(logDirName)

print 'Compile...'
try:
  if os.system('ant compile-core compile-test common.compile-test > compile.log 2>&1'):
    print open('compile.log', 'rb').read()
    sys.exit(1)
finally:
  os.remove('compile.log')
  
OLD_JUNIT = os.path.exists('lib/junit-3.8.2.jar')

mult = int(getArg('-mult', 1))
codec = getArg('-codec', 'random')
dir = getArg('-dir', 'random')
verbose = getArg('-verbose', False, False)
iters = int(getArg('-iters', 1))
seed = getArg('-seed', None)
nightly = getArg('-nightly', None)
keepLogs = getArg('-keeplogs', False, False)

if len(sys.argv) == 1:
  print '\nERROR: no test specified\n'
  sys.exit(1)

tests = []
for test in sys.argv[1:]:
  if not test.startswith('org.'):
    tup = locateTest(test)
    if tup is None:
      print '\nERROR: cannot find test %s\n' % test
      sys.exit(1)
    testClass, testMethod = tup
    tests.append((testClass, testMethod))
  
CP = []
CP.append('build/classes/test')
CP.append('build/classes/java')
if not OLD_JUNIT:
  JUNIT_JAR = './lib/junit-4.7.jar'
else:
  JUNIT_JAR = './lib/junit-3.8.2.jar'
CP.append(JUNIT_JAR)
if os.path.exists('build/classes/demo'):
  CP.append('build/classes/demo')

JAVA_ARGS += ' -cp "%s"' % common.pathsep().join(CP)

TEST_TEMP_DIR = 'build/test/reruns'

upto = 0
while True:
  for testClass, testMethod in tests:
    print
    if testMethod is not None:
      s = '%s#%s' % (testClass, testMethod)
    else:
      s = testClass

    if doLog:
      print '%s TEST: %s -> %s/%d.log' % (datetime.datetime.now(), s, logDirName, upto)
    else:
      print '%s TEST: %s' % (datetime.datetime.now(), s)
      
    command = 'java %s -DtempDir=%s -ea' % (JAVA_ARGS, TEST_TEMP_DIR)
    if constants.JRE_SUPPORTS_SERVER_MODE and random.randint(0, 1) == 1:
      command += ' -server'
    if random.randint(0, 1) == 1:
      command += ' -Xbatch'
    command += ' -Dtests.verbose=%s' % verbose
    command += ' -Drandom.multiplier=%s' % mult
    command += ' -Dtests.iter=%s' % iters
    command += ' -Dtests.codec=%s' % codec
    command += ' -Dtests.directory=%s' % dir
    if nightly:
      command += ' -Dtests.nightly=true'
    if seed is not None:
      command += ' -Dtests.seed=%s' % seed
    if testMethod is not None:
      command += ' -Dtestmethod=%s' % testMethod
      
    if OLD_JUNIT:
      command += ' junit.textui.TestRunner'
    else:
      command += ' org.junit.runner.JUnitCore'

    command += ' %s' % testClass

    if doLog:
      command += ' > %s/%d.log 2>&1' % (logDirName, upto)
      
    if os.path.exists(TEST_TEMP_DIR):
      print '  remove %s' % TEST_TEMP_DIR
      try:
        shutil.rmtree(TEST_TEMP_DIR)
      except OSError:
        pass
    print '  RUN: %s' % command
    res = os.system(command)

    if res:
      print '  FAILED'
      raise RuntimeError('hit fail')
    elif doLog:
      if not keepLogs:
        os.remove('%s/%d.log' % (logDirName, upto))
      pass
      
    upto += 1

