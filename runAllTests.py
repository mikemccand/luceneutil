import shutil
import re
import subprocess
import traceback
import cPickle
import threading
import os
import heapq
import subprocess
import time
import sys
import constants
import copy
import common

# TODO
#   - how come quiet logging doesn't "take"???
#   - hmm an i 'compile-contrib' while letting other threads run core-only jobs?
#   - must test modules/analyzers too!!
#   - learn over time which tests are slowest and run those first
#   - threads!
#   - do all that ant build files do -- randomness, random codec, etc.
#   - print total # testcases
#   - verify i can "ant clean" @ top then run this; eg I'm not compiling Solr tests correctly yet

ROOT = common.findRootDir(os.getcwd())

# We bundle up tests that take roughly this many seconds, together, to reduce JRE startup time:
DO_GATHER_TIMES = '-setTimes' in sys.argv

COST_PER_JOB = 30.0

TEST_TIMES_FILE = '%s/TEST_TIMES.pk' % constants.BASE_DIR

NUM_THREAD = 18

RAN_MULT = 1

VERBOSE = 'false'

LUCENE_VERSION = '4.0-SNAPSHOT'

CLASSPATH = ['../lucene/lib/junit-4.7.jar',
             '../lucene/build/classes/test',
             '../lucene/build/classes/test-framework',
             '../solr/build/test-framework',
             '../lucene/build/classes/java',
             '../modules/analysis/build/common/classes/java',
             '../modules/suggest/build/classes/java',
             '../modules/grouping/build/classes/java',
             '/usr/share/java/ant.jar']

try:
  CODEC = sys.argv[1+sys.argv.index('-codec')]
except ValueError:
  CODEC = 'randomPerField'

try:
  DIR = sys.argv[1+sys.argv.index('-dir')]
except ValueError:
  DIR = 'random'
  
TEST_ARGS = ' -server -Djetty.insecurerandom=1 -Djetty.testMode=1 -Dchecksum.algorithm=md5 -Djava.compat.version=1.6 -Djava.vm.info="mixed mode" -Dsun.java.launcher=SUN_STANDARD -Dtests.codec="%s" -Dtests.verbose=%s -Dtests.directory=%s -Drandom.multiplier=%s -Dweb.xml=/lucene/clean/solr/src/webapp/web/WEB-INF/web.xml' % (CODEC, VERBOSE, DIR, RAN_MULT)

reTime = re.compile(r'^Time: ([0-9\.]+)$', re.M)

try:
  testTimes = cPickle.loads(open(TEST_TIMES_FILE, 'rb').read())
except:
  print 'WARNING: no test times:'
  traceback.print_exc()
  testTimes = {}

testTimes['org.apache.lucene.search.TestPhraseQuery'] = 45.0

doLucene = doModules = doSolr = False

if '-lucene' in sys.argv:
  doLucene = True
if '-solr' in sys.argv:
  doSolr = True
if '-modules' in sys.argv:
  doModules = True

if not doLucene and not doSolr and not doModules:
  doLucene = doModules = doSolr = True

def addCP(dirName):
  if os.path.exists(dirName):
    # print 'ADD %s' % dirName
    CLASSPATH.append(dirName)
    
def fixCP():
  for i in range(len(CLASSPATH)):
    s = CLASSPATH[i]
    if s.startswith('../'):
      s = ROOT + '/' + s[3:]
      CLASSPATH[i] = s

def jarOK(jar):
  return jar != 'log4j-1.2.14.jar'

def addJARs(path):
  if os.path.exists(path):
    for f in os.listdir(path):
      if f.endswith('.jar') and jarOK(f):
        CLASSPATH.append('%s/%s' % (path, f))
  
def run(comment, cmd, logFile):
  print comment
  if os.system('%s > %s 2>&1' % (cmd, logFile)):
    print open(logFile).read()
    raise RuntimeError('FAILED: %s' % cmd)

def estimateCost(testClass):
  try:
    #print '%.1f sec: %s' % (testTimes[testClass], testClass)
    t = testTimes[testClass]-0.15
    if t < 0:
      t = 0.05
    return t
  except KeyError:
    print 'NO COST: %s' % testClass
    return 1.0

prLock = threading.Lock()

def pr(s):
  with prLock:
    sys.stdout.write(s)

def aggTests(workQ, tests):
  tests0 = []

  pendingCost = 0
  lastWD = None
  for cost, wd, test, classpath in tests:

    if len(tests0) > 0 and (lastWD != wd or DO_GATHER_TIMES or cost + pendingCost > COST_PER_JOB):
      # print 'JOB: %s, %s' % (pendingCost, ' '.join(tests))
      workQ.add(Job(lastWD, tests0, pendingCost, CLASSPATH))
      tests0 = []
      pendingCost = 0

    lastWD = wd

    pendingCost += cost
    tests0.append(test)

  if len(tests0) > 0:
    workQ.add(Job(lastWD, tests0, pendingCost, CLASSPATH))

class Job:
  def __init__(self, wd, tests, cost, classpath):
    self.wd = wd
    self.tests = tests
    self.cost = cost
    self.classpath = classpath

  # highest cost compares lowest
  def __cmp__(self, other):
    return cmp(-self.cost, -other.cost)

class WorkQueue:
  def __init__(self):
    self.lock = threading.Lock()
    self.q = []

  def add(self, job):
    heapq.heappush(self.q, job)

  def pop(self):
    with self.lock:
      if len(self.q) == 0:
        if '-repeat' in sys.argv:
          pr('t')
          aggTests(self, tests)
        else:
          return None

      v = heapq.heappop(self.q)
      # print 'WQ: %s %s' % (v.cost, ' '.join(v.tests))
      return v

class RunThread:

  def __init__(self, id, work):
    self.id = id
    self.tempDir = '%s/lucene/build/test/%d' % (ROOT, self.id)
    self.cleanup()
    self.work = work
    try:
      os.remove('%s/%s.log' % (ROOT, self.id))
    except OSError:
      pass
    self.suiteCount = 0
    self.failed = False
    self.myEnv = copy.copy(os.environ)
    self.t = threading.Thread(target=self.run)
    self.t.setDaemon(True)
    self.t.start()

  def cleanup(self):
    if os.path.exists(self.tempDir):
      shutil.rmtree(self.tempDir)
    os.makedirs(self.tempDir)

  def run(self):
    while True:
      job = self.work.pop()
      if job is None:
        #pr('%s: DONE' % self.id)
        pr('d')
        break
      else:
        #pr('%s: RUN' % self.id)
        pr('.')
        logFile = '%s/%s.log' % (ROOT, self.id)
        cmd = 'java -Xmx512m -Xms512m %s -Dlucene.version=%s' % (TEST_ARGS, LUCENE_VERSION)
        if constants.TESTS_LINE_FILE is not None:
          cmd += ' -Dtests.linedocsfile=%s' % constants.TESTS_LINE_FILE
        cmd += ' -DtempDir=%s -Djava.util.logging.config=%s/solr/testlogging.properties -Dtests.luceneMatchVersion=4.0 -ea:org.apache.lucene... -ea:org.apache.solr... org.junit.runner.JUnitCore %s' % \
              (self.tempDir, ROOT, ' '.join(job.tests))

        if 0:
          print
          print 'wd %s' % job.wd
          print 'cp %s' % ':'.join(job.classpath)
          print 'cmd %s' % cmd

        #open(logFile, 'ab').write('\nTESTS: cost=%.3f %s\n  CWD: %s\n  RUN: %s\n' % (job.cost, ' '.join(job.tests), job.wd, cmd))
        open(logFile, 'ab').write('\nTESTS: cost=%.3f %s\n  CWD: %s\n' % (job.cost, ' '.join(job.tests), job.wd))

        self.myEnv['CLASSPATH'] = ':'.join(job.classpath)
        
        if 0:
          if job.tests[0].find('.solr.') != -1:
            wd = '%s/solr/src/test/test-files' % ROOT
          else:
            wd = '%s/lucene' % ROOT

        cmd = 'cd %s; %s' % (job.wd, cmd)
        self.suiteCount += len(job.tests)

        if not DO_GATHER_TIMES:
          cmd += ' >> %s 2>&1' % logFile
        #print 'CP=%s' % (':'.join(job.classpath))
        #print 'CMD %s' % cmd
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.myEnv)
        output = p.communicate()[0]
        if DO_GATHER_TIMES:
          open(logFile, 'ab').write(output + '\n')
        if p.returncode != 0:
          pr('\n%s: FAILED %s [see %s]\n' % (self.id, job.tests[0], logFile))
          self.failed = True

        if DO_GATHER_TIMES:
          m = reTime.search(output)
          if m is None:
            print 'FAILED to parse time %s' % output
            testTime = 1.0
          else:
            testTime = float(m.group(1))
          
          # take max
          if job.tests[0] not in testTimes or \
             testTimes[job.tests[0]] < testTime:
            testTimes[job.tests[0]] = testTime

  def join(self):
    self.t.join()
    
t0 = time.time()
os.chdir('%s/lucene' % ROOT)

if '-noc' not in sys.argv:

  if os.path.exists('%s/modules' % ROOT):
    os.chdir('%s/modules' % ROOT)
    run('Compile modules...', 'ant compile compile-test', 'compile.log')

  os.chdir('%s/lucene' % ROOT)
  #run('Compile Lucene...', 'ant compile-test', 'compile.log')

  if True:
    run('Compile Lucene contrib...', 'ant build-contrib', 'compile-contrib.log')

  if True and doSolr:
    os.chdir('%s/solr' % ROOT)
    run('Compile Solr...', 'ant compileTests build-contrib', 'compile.log')

testDir = '%s/lucene/build/test' % ROOT
if not os.path.exists(testDir):
  os.makedirs(testDir)
  
tests = []
strip = len(ROOT) + len('/lucene/src/test/')

# lucene core tests
for dir, subDirs, files in os.walk('%s/lucene/src/test' % ROOT):
  for file in files:
    if file.endswith('.java') and (file.startswith('Test') or file.endswith('Test.java')):
      fullFile = '%s/%s' % (dir, file)
      testClass = fullFile[strip:-5].replace('/', '.')
      if doLucene:
        tests.append((estimateCost(testClass), '%s/lucene' % ROOT, testClass, CLASSPATH))

# lucene contrib tests
for contrib in list(os.listdir('%s/lucene/contrib' % ROOT)) + ['db/bdb', 'db/bdb-je']:
  #print 'contrib/%s' % contrib
  strip = len(ROOT) + len('/lucene/contrib/%s/src/test/' % contrib)
  addCP(('%s/lucene/build/contrib/%s/classes/java' % (ROOT, contrib)))
  addCP('%s/lucene/build/contrib/%s/classes/test' % (ROOT, contrib))
  libDir = '%s/lucene/contrib/%s/lib' % (ROOT, contrib)
  addJARs(libDir)
  for dir, subDirs, files in os.walk('%s/lucene/contrib/%s/src/test' % (ROOT, contrib)):
    for file in files:
      if file.endswith('.java') and (file.startswith('Test') or file.endswith('Test.java')):
        fullFile = '%s/%s' % (dir, file)
        testClass = fullFile[strip:-5].replace('/', '.')
        # print '  %s' % testClass
        if testClass == 'org.apache.lucene.store.db.DbStoreTest':
          continue
        if doLucene:
          tests.append((estimateCost(testClass), '%s/lucene' % ROOT, testClass, CLASSPATH))

# modules tests
if os.path.exists('%s/modules' % ROOT):
  for path in os.listdir('%s/modules' % ROOT):
    fullPath = '%s/modules/%s' % (ROOT, path)
    if os.path.isdir(fullPath):
      module = path
      if module == 'analysis':
        # sub-projects
        for package in os.listdir('%s/modules/analysis' % ROOT):
          subDir = '%s/modules/analysis/%s' % (ROOT, package)
          if os.path.isdir(subDir):
            CLASSPATH.append('../modules/analysis/build/%s/classes/java' % package)
            CLASSPATH.append('../modules/analysis/build/%s/classes/test' % package)
            libDir = '../modules/analysis/%s/lib' % package
            if os.path.exists(libDir):
              for f in os.listdir(libDir):
                if f.endswith('.jar'):
                  CLASSPATH.append('%s/%s' % (libDir, f))
            strip = len(ROOT) + len('/modules/analysis/%s/src/test/' % package)
            for dir, subDirs, files in os.walk('%s/modules/analysis/%s/src/test' % (ROOT, package)):
              for file in files:
                if file.endswith('.java') and (file.startswith('Test') or file.endswith('Test.java')):
                  fullFile = '%s/%s' % (dir, file)
                  testClass = fullFile[strip:-5].replace('/', '.')
                  # print '  %s' % testClass
                  if doModules:
                    tests.append((estimateCost(testClass), '%s/modules/analysis' % ROOT, testClass, CLASSPATH))
      else:
        CLASSPATH.append('../modules/%s/build/classes/java' % module)
        CLASSPATH.append('../modules/%s/build/classes/test' % module)
        libDir = '../modules/%s/lib' % module
        if os.path.exists(libDir):
          for f in os.listdir(libDir):
            if f.endswith('.jar'):
              CLASSPATH.append('%s/%s' % (libDir, f))
        strip = len(ROOT) + len('/modules/%s/src/test/' % module)
        for dir, subDirs, files in os.walk('%s/modules/%s/src/test' % (ROOT, module)):
          for file in files:
            if file.endswith('.java') and (file.startswith('Test') or file.endswith('Test.java')):
              fullFile = '%s/%s' % (dir, file)
              testClass = fullFile[strip:-5].replace('/', '.')
              # print '  %s' % testClass
              if doModules:
                tests.append((estimateCost(testClass), '%s/modules/%s' % (ROOT, module), testClass, CLASSPATH))

# solr core tests
if doSolr:
  addJARs('../solr/lib')
  addJARs('../solr/example/lib')
  addJARs('../solr/example/lib/jsp-2.1')
  CLASSPATH.append('../solr/build/solr')
  CLASSPATH.append('../solr/build/tests')
  CLASSPATH.append('../solr/build/solrj')

  # guess!  to load solrconfig.xml
  #CLASSPATH.append('../solr/src/test/test-files/solr/conf')
  #CLASSPATH.append('../solr/src/test/test-files/solr/lib/classes')

  fixCP()
      
  strip = len(ROOT) + len('/solr/src/test/')
  for dir, subDirs, files in os.walk('%s/solr/src/test' % ROOT):
    for file in files:
      if file.endswith('.java') and (file.startswith('Test') or file.endswith('Test.java')):
        
        fullFile = '%s/%s' % (dir, file)
        testClass = fullFile[strip:-5].replace('/', '.')
        #if testClass in ('org.apache.solr.client.solrj.embedded.TestSolrProperties', 'org.apache.solr.cloud.CloudStateUpdateTest'):
        if testClass in ('org.apache.solr.cloud.CloudStateUpdateTest',):
          print 'WARNING: skipping test %s' % testClass
          continue
        # print '  %s' % testClass
        tests.append((estimateCost(testClass), '%s/solr/src/test/test-files' % ROOT, testClass, CLASSPATH))

# solr contrib tests
if doSolr:
  for contrib in os.listdir('%s/solr/contrib' % ROOT):
    if contrib == 'clustering':
      continue
    #print 'contrib/%s' % contrib
    strip = len(ROOT) + len('/solr/contrib/%s/src/test/java/' % contrib)
    addCP('%s/solr/contrib/%s/target/test-classes' % (ROOT, contrib))
    addCP('%s/solr/contrib/%s/target/classes' % (ROOT, contrib))
    addCP('%s/solr/contrib/%s/target/extras/classes' % (ROOT, contrib))
    addCP('%s/solr/contrib/%s/build/test-classes' % (ROOT, contrib))
    addCP('%s/solr/contrib/%s/build/classes' % (ROOT, contrib))
    addCP('%s/solr/contrib/%s/src/test/resources' % (ROOT, contrib))
    addCP('%s/solr/contrib/%s/src/main/resources' % (ROOT, contrib))
    if 0:
      CLASSPATH.append('../solr/build/contrib/%s/classes/java' % contrib)
      CLASSPATH.append('../lucene/build/contrib/%s/classes/test' % contrib)
      CLASSPATH.append('../lucene/build/contrib/%s/classes' % contrib)
    libDir = '%s/solr/contrib/%s/lib' % (ROOT, contrib)
    addJARs(libDir)
    for dir, subDirs, files in os.walk('%s/solr/contrib/%s/src/test/java' % (ROOT, contrib)):
      for file in files:
        if file.endswith('.java') and (file.startswith('Test') or file.endswith('Test.java')):
          fullFile = '%s/%s' % (dir, file)
          testClass = fullFile[strip:-5].replace('/', '.')
          # print '  %s' % testClass
          tests.append((estimateCost(testClass), '%s/solr/contrib/%s/src/test/resources' % (ROOT, contrib), testClass, CLASSPATH))

tests.sort(reverse=True)
repeat = '-repeat' in sys.argv

pendingCost = 0
workQ = WorkQueue()

aggTests(workQ, tests)

threads = []
for i in range(NUM_THREAD):
  threads.append(RunThread(i, workQ))

totSuites = 0
failed = False
for i in range(NUM_THREAD):
  threads[i].join()
  totSuites += threads[i].suiteCount

if DO_GATHER_TIMES:
  open(TEST_TIMES_FILE, 'wb').write(cPickle.dumps(testTimes))

print '\n%.1f sec [%d test suites]' % (time.time()-t0, totSuites)
