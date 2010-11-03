import shutil
import datetime
import os
import sys

if 0:
  tests = ('org.apache.lucene.index.TestThreadedOptimize',
           'org.apache.lucene.index.TestConcurrentMergeScheduler',
           'org.apache.lucene.index.TestTransactions',
           'org.apache.lucene.index.TestAddIndexesNoOptimize',
           'org.apache.lucene.index.stresstests.TestIndexWriterConcurrent',
           'org.apache.lucene.index.TestStressIndexing',
           'org.apache.lucene.index.TestStressIndexing2',
           'org.apache.lucene.index.TestIndexWriter',
           'org.apache.lucene.index.TestAtomicUpdate',
           'org.apache.lucene.index.TestIndexWriterDelete',
           'org.apache.lucene.index.TestIndexWriterExceptions',
           'org.apache.lucene.TestSnapshotDeletionPolicy',
           'org.apache.lucene.index.TestCrash')

  tests = ('org.apache.lucene.search.TestRegexpRandom2',
           'org.apache.lucene.search.TestRegexpRandom',
           'org.apache.lucene.search.TestPrefixRandom',
           'org.apache.lucene.search.TestAutomatonQuery',
           'org.apache.lucene.util.automaton.TestUTF32ToUTF8',
           'org.apache.lucene.index.codecs.preflex.TestSurrogates')
  doLog = False
else:
  #tests = ('org.apache.lucene.index.TestAtomicUpdate',)
  #tests = ('org.apache.lucene.index.TestBackwardsCompatibility',)
  #tests = ('org.apache.lucene.search.TestPrefixRandom',)
  #tests = ('org.apache.lucene.index.codecs.preflex.TestSurrogates',)
  #logDirName = 'atomic'
  #tests = ('org.apache.lucene.index.TestPayloads',)
  tests = ('org.apache.lucene.index.TestIndexWriterExceptions',)
  #tests = ('org.apache.lucene.index.TestConcurrentMergeScheduler',)
  logDirName = '/tmp/logs/%s' % os.path.split(os.getcwd())[-1]
  doLog = True

if os.system('ant compile-core compile-test common.compile-test'):
  sys.exit(0)
  
if doLog:
  if os.path.exists(logDirName):
    shutil.rmtree(logDirName)
  os.makedirs(logDirName)

IS29 = False

p = sys.platform.lower()
if not IS29:
  JUNIT_JAR = './lib/junit-4.7.jar'
  #JUNIT_JAR = '/Volumes/root/usr/local/src/junit-4.4.jar'
  CP = '-cp .:%s:./build/classes/test:./build/classes/java' % JUNIT_JAR
else:
  CP = '-cp .:./lib/junit-3.8.2.jar:./build/classes/test:./build/classes/java:./build/classes/demo'
#ARGS = '-server -Xmx1024m -Xms1024m -Xbatch %s' % CP
ARGS = '-Xmx512m -Xms512m %s' % CP
#ARGS = CP

ranMult = 1
verbose = 'false'
codec = 'random'
#codec = 'PreFlex'
#codec = '"MockVariableIntBlock(29)"'
upto = 0
while True:
  for test in tests:
    print
    if doLog:
      print '%s TEST: %s -> %s/%d.log' % (datetime.datetime.now(), test, logDirName, upto)
    else:
      print '%s TEST: %s' % (datetime.datetime.now(), test)
      
    if doLog:
      if IS29:
        command = 'java %s -DtempDir=build/test -ea junit.textui.TestRunner %s > %s/%d.log 2>&1' % (ARGS, test, logDirName, upto)
      else:
        #command = 'java %s -DtempDir=build/test -Dtests.codec=%s -Dtestmethod=testRandomStoredFields -Dtests.verbose=%s -Drandom.multiplier=%d -Dtests.iter=50 -ea org.junit.runner.JUnitCore %s > %s/%d.log 2>&1' % (ARGS, codec, verbose, ranMult, test, logDirName, upto)
        #command = 'java %s -DtempDir=build/test -Dtestmethod=testRandomStoredFields -Dtests.verbose=%s -Drandom.multiplier=%d -Dtests.iter=10 -ea org.junit.runner.JUnitCore %s > %s/%d.log 2>&1' % (ARGS, verbose, ranMult, test, logDirName, upto)
        #command = 'java %s -DtempDir=build/test -Dtests.directory=SimpleFSDirectory -Dtests.verbose=%s -Drandom.multiplier=%d -Dtests.iter=100 -ea org.junit.runner.JUnitCore %s > %s/%d.log 2>&1' % (ARGS, verbose, ranMult, test, logDirName, upto)
        #command = 'java %s -DtempDir=build/test -Drandom.multiplier=%d -ea org.junit.runner.JUnitCore %s > %s/%d.log 2>&1' % (ARGS, ranMult, test, logDirName, upto)
        command = 'java %s -DtempDir=build/test -Dtests.verbose=%s -Drandom.multiplier=%d -Dtests.iter=10 -Dtests.codec=%s -Dtests.directory=RAMDirectory -ea org.junit.runner.JUnitCore %s > %s/%d.log 2>&1' % (ARGS, verbose, ranMult, codec, test, logDirName, upto)
      print 'CLEAN'
      if os.path.exists('build/test'):
        shutil.rmtree('build/test')
      print 'RUN: %s' % command
      res = os.system(command)
      #res = os.system('ant test-core -Dtestcase=%s' % test.split('.')[-1])
    else:
      #res = os.system('java -server -agentlib:yjpagent -Xmx1024m -Xms1024m -Xbatch -DtempDir=build/test -ea org.junit.runner.JUnitCore %s' % test)
      command = 'java %s -DtempDir=build/test -Drandom.multiplier=%s -ea org.junit.runner.JUnitCore %s' % (ARGS, ranMult, test)
      #command = 'ant test-core -Dtestcase=%s' 
      print 'RUN: %s' % command
      res = os.system(command)

    if res:
      print '  FAILED'
      raise RuntimeError('hit fail')
    elif doLog:
      os.remove('%s/%d.log' % (logDirName, upto))
      pass
      
    upto += 1

