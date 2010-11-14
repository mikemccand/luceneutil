import shutil
import datetime
import os
import sys

# NOTE
#   - only works in the lucene subdir, ie this runs equivalent of "ant test-core"

IS_WINDOWS = sys.platform.find('win') != -1 and sys.platform.find('darwin') == -1

JAVA_ARGS = '-server -Xmx512m -Xms512m'

if 0:
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
    doLog = True

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

if tup[1] != 'lucene':
  print '\nERROR: you need to be in lucene subdir when running this\n'
  sys.exit(1)

sub = os.path.split(tup[0])[1]
  
logDirName = '/tmp/logs/%s' % sub
if IS_WINDOWS:
  logDirName = 'c:' + logDirName
doLog = True

print 'Logging to dir %s' % logDirName

if doLog:
  if os.path.exists(logDirName):
    shutil.rmtree(logDirName)
  os.makedirs(logDirName)

print 'Compile...'
if os.system('ant compile-core compile-test common.compile-test > compile.log 2>&1'):
  print open('compile.log', 'rb').read()
  sys.exit(1)
  
OLD_JUNIT = os.path.exists('lib/junit-3.8.2.jar')

mult = int(getArg('-mult', 1))
codec = getArg('-codec', 'random')
dir = getArg('-dir', 'random')
verbose = getArg('-verbose', False, False)
iters = int(getArg('-iters', 1))
seed = getArg('-seed', None)

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

JAVA_ARGS += ' -cp "%s"' % os.pathsep.join(CP)



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
      
    command = 'java %s -DtempDir=build/test -ea' % JAVA_ARGS
    command += ' -Dtests.verbose=%s' % verbose
    command += ' -Drandom.multiplier=%s' % mult
    command += ' -Dtests.iter=%s' % iters
    command += ' -Dtests.codec=%s' % codec
    command += ' -Dtests.directory=%s' % dir
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
      
    if os.path.exists('build/test'):
      print '  remove build/test'
      try:
        shutil.rmtree('build/test')
      except OSError:
        pass
    print '  RUN: %s' % command
    res = os.system(command)

    if res:
      print '  FAILED'
      raise RuntimeError('hit fail')
    elif doLog:
      os.remove('%s/%d.log' % (logDirName, upto))
      pass
      
    upto += 1

