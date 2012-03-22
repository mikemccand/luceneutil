import shutil
import re
import subprocess
import traceback
import cPickle
import threading
import os
import heapq
import time
import sys
import constants
import copy
import common

# TODO
#   - hmm make some effort to assing "hard" jobs to the "fast" resources
#   - do a better job getting the failure details out
#   - hmm an i 'compile-contrib' while letting other threads run core-only jobs?
#   - must test modules/analyzers too!!
#   - learn over time which tests are slowest and run those first
#   - threads!
#   - do all that ant build files do -- randomness, random codec, etc.
#   - print total # testcases
#   - verify i can "ant clean" @ top then run this; eg I'm not compiling Solr tests correctly yet

ROOT = common.findRootDir(os.getcwd())

HEAP = '512m'
# HEAP = '14g'

# We bundle up tests that take roughly this many seconds, together, to reduce JRE startup time:
DO_GATHER_TIMES = '-setTimes' in sys.argv

COST_PER_JOB = 40.0

TEST_TIMES_FILE = '%s/TEST_TIMES.pk' % constants.BASE_DIR

if DO_GATHER_TIMES:
  NUM_THREAD = [('local', 14)]
elif '-repeat' in sys.argv:
  NUM_THREAD = [('local', 16)]
else:
  NUM_THREAD = [('local', 20),
                #('10.17.4.90', 3),
                #('10.17.4.5', 3),
                ]

RAN_MULT = 1

VERBOSE = 'false'

LUCENE_VERSION = '4.0-SNAPSHOT'

CLASSPATH = ['../lucene/lib/junit-4.10.jar',
             '../lucene/build/test-framework/classes/java',
             '../lucene/build/core/classes/java',
             '../lucene/build/core/classes/test',
             '../solr/build/solr-test-framework/classes/java',
             '../lucene/build/classes/java',
             '../modules/analysis/build/common/classes/java',
             '../modules/queries/build/common/classes/java',
             '../modules/facet/build/classes/java',
             '../modules/facet/build/classes/examples',
             '../modules/join/build/classes/java',
             '../modules/suggest/build/classes/java',
             '../modules/grouping/build/classes/java',
             '/usr/share/java/ant.jar']

try:
  POSTINGS_FORMAT = sys.argv[1+sys.argv.index('-pf')]
except ValueError:
  POSTINGS_FORMAT = 'random'

try:
  CODEC = sys.argv[1+sys.argv.index('-codec')]
except ValueError:
  CODEC = 'random'

try:
  DIR = sys.argv[1+sys.argv.index('-dir')]
except ValueError:
  DIR = 'random'
  
#TEST_ARGS = ' -server -Djetty.insecurerandom=1 -Djetty.testMode=1 -Dchecksum.algorithm=md5 -Djava.compat.version=1.6 -Djava.vm.info="mixed mode" -Dsun.java.launcher=SUN_STANDARD -Dtests.codec="%s" -Dtests.verbose=%s -Dtests.directory=%s -Drandom.multiplier=%s -Dweb.xml=/lucene/clean/solr/src/webapp/web/WEB-INF/web.xml' % (CODEC, VERBOSE, DIR, RAN_MULT)

#TEST_ARGS = ' -server -Dtestmethod= -Dtests.nightly=false -Dtests.iter=1 -Dtests.iter.min=1 -Dtests.locale=random -Dtests.timezone=random -Dtests.seed=random -Dtests.cleanthreads=perMethod -Dsolr.directoryFactory=org.apache.solr.core.MockDirectoryFactory -Djetty.insecurerandom=1 -Djetty.testMode=1 -Dchecksum.algorithm=md5 -Djava.compat.version=1.6 -Djava.vm.info="mixed mode" -Dsun.java.launcher=SUN_STANDARD -Dtests.postingsformat=random -Dtests.postingsformat=%s -Dtests.verbose=%s -Dtests.directory=%s -Drandom.multiplier=%s -Dweb.xml=/lucene/clean/solr/src/webapp/web/WEB-INF/web.xml -Dtests.luceneMatchVersion=4.0 -ea:org.apache.lucene... -ea:org.apache.solr...' % (POSTINGS_FORMAT, VERBOSE, DIR, RAN_MULT)

TEST_ARGS = ' -Dtests.nightly=false -Dtests.iter=1 -Dtests.iter.min=1 -Dtests.locale=random -Dtests.timezone=random -Dtests.seed=random -Dtests.cleanthreads=perMethod -DsolrudirectoryFactory=org.apache.solr.core.MockDirectoryFactory -Djetty.insecurerandom=1 -Djetty.testMode=1 -Dchecksum.algorithm=md5 -Djava.compat.version=1.6 -Dsun.java.launcher=SUN_STANDARD -Dtests.postingsformat=%s -Dtests.codec=%s -Dtests.verbose=%s -Dtests.directory=%s -Drandom.multiplier=%s -Dweb.xml=/lucene/clean/solr/src/webapp/web/WEB-INF/web.xml -Dtests.luceneMatchVersion=4.0 -ea:org.apache.lucene... -ea:org.apache.solr...' % (POSTINGS_FORMAT, CODEC, VERBOSE, DIR, RAN_MULT)

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
    #print 'CP: add %s' % dirName
    CLASSPATH.append(dirName)

def addCPIfExists(dirName):
  if os.path.exists(dirName):
    CLASSPATH.append(dirName)
    
def fixCP():
  for i in range(len(CLASSPATH)):
    s = CLASSPATH[i]
    if s.startswith('../'):
      s = ROOT + '/' + s[3:]
      CLASSPATH[i] = s
    # print 'cp: %s' % s

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
      #print 'JOB: %s, %s' % (pendingCost, ' '.join(tests0))
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

  # XXX higher cost compares as lower
  # fewer number of tests compares as lower
  def __cmp__(self, other):
    c = cmp(len(self.tests), len(other.tests))
    #c = -cmp(self.cost, other.cost)
    #print 'cmp %d vs %d: ret %s' % (len(self.tests), len(other.tests), c)
    return c

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
          pr('t ')
          aggTests(self, tests)
        else:
          return None

      #print
      #print 'NOW HP'
      v = heapq.heappop(self.q)
      #print 'WQ: %s %s' % (v.cost, ' '.join(v.tests))
      return v

class RunThread:

  def __init__(self, resource, resourceID, id, work):
    self.resource = resource
    self.resourceID = resourceID
    self.id = id
    self.tempDir = '%s/lucene/build/test/%d' % (ROOT, self.id)
    self.cleanup()
    self.work = work
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
        pr('d%d:%d ' % (self.resourceID, self.id))
        break
      else:
        #pr('%s: RUN' % self.id)
        pr('%d:%d ' % (self.resourceID, self.id))
        logFile = '%s/%s.log' % (ROOT, self.id)
        cmd = 'java -Xmx%s -Xms%s %s -Dlucene.version=%s' % (HEAP, HEAP, TEST_ARGS, LUCENE_VERSION)
        if constants.TESTS_LINE_FILE is not None:
          cmd += ' -Dtests.linedocsfile=%s' % constants.TESTS_LINE_FILE

        if self.resource == 'local':
          self.myEnv['CLASSPATH'] = ':'.join(job.classpath)
        else:
          cmd += ' -classpath %s' % ':'.join(job.classpath)

        cmd += ' -DtempDir=%s -Djava.util.logging.config.file=%s/solr/testlogging.properties org.junit.runner.JUnitCore %s' % \
              (self.tempDir, ROOT, ' '.join(job.tests))

        if 0:
          print
          print 'wd %s' % job.wd
          print 'cp %s' % ':'.join(job.classpath)
          print 'cmd %s' % cmd

        #open(logFile, 'ab').write('\nTESTS: cost=%.3f %s\n  CWD: %s\n  RUN: %s\n' % (job.cost, ' '.join(job.tests), job.wd, cmd))
        s = '\nTESTS: cost=%.3f %s\n  CWD: %s\n' % (job.cost, ' '.join(job.tests), job.wd)
        if self.resource == 'local':
          open(logFile, 'ab').write(s)
        else:
          p = subprocess.Popen('ssh %s "cat >> %s"' % (self.resource, logFile), bufsize=-1, shell=True, stdin=subprocess.PIPE)
          p.stdin.write(s)
          p.stdin.flush()
          
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
        if self.resource == 'local':
          p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.myEnv)
        else:
          # nocommit should i just have stdout come back over ssh...?
          # print 'CMD %s' % cmd
          cmd = 'ssh -Tx %s "%s"' % (self.resource, cmd)
          p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.myEnv)
          
        output = p.communicate()[0]
        if DO_GATHER_TIMES:
          open(logFile, 'ab').write(output + '\n')
        if p.returncode != 0:
          pr('\n%s: FAILED %s [see %s:%s]\n' % (self.id, job.tests[0], self.resource, logFile))
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
    
if '-noc' not in sys.argv:

  os.chdir('%s/lucene/core' % ROOT)
  run('Compile Lucene core...', 'ant compile-test', 'compile.log')

  os.chdir('%s/lucene' % ROOT)
  run('Compile Lucene contrib...', 'ant build-contrib', 'compile.log')
  
  if os.path.exists('%s/modules' % ROOT):
    os.chdir('%s/modules' % ROOT)
    run('Compile modules...', 'ant compile compile-test', 'compile.log')

  if True and doSolr:
    os.chdir('%s/solr' % ROOT)
    #run('Compile Solr...', 'ant compile-test', 'compile.log')
    run('Compile Solr...', 'ant compile compile-test', 'compile.log')

os.chdir('%s/lucene' % ROOT)

testDir = '%s/lucene/build/core/test' % ROOT
if not os.path.exists(testDir):
  os.makedirs(testDir)
  
tests = []
strip = len(ROOT) + len('/lucene/core/src/test/')

# lucene core tests
for dir, subDirs, files in os.walk('%s/lucene/core/src/test' % ROOT):
  for file in files:
    if file.endswith('.java') and (file.startswith('Test') or file.endswith('Test.java')):
      fullFile = '%s/%s' % (dir, file)
      testClass = fullFile[strip:-5].replace('/', '.')
      if testClass in ('org.apache.lucene.util.junitcompat.TestExceptionInBeforeClassHooks',):
        print 'WARNING: skipping test %s' % testClass
        continue
      
      if doLucene:
        wd = '%s/lucene' % ROOT
        #wd = ROOT
        tests.append((estimateCost(testClass), wd, testClass, CLASSPATH))

# lucene contrib tests
if True:
  for contrib in list(os.listdir('%s/lucene/contrib' % ROOT)) + ['db/bdb', 'db/bdb-je']:
    #print 'contrib/%s' % contrib
    strip = len(ROOT) + len('/lucene/contrib/%s/src/test/' % contrib)
    if contrib in ('queries', 'queryparser'):
      contrib2 = '%s-contrib' % contrib
    else:
      contrib2 = contrib
    addCP(('%s/lucene/build/contrib/%s/classes/java' % (ROOT, contrib2)))
    addCP('%s/lucene/build/contrib/%s/classes/test' % (ROOT, contrib2))
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
            wd = '%s/lucene' % ROOT
            #wd = ROOT
            tests.append((estimateCost(testClass), wd, testClass, CLASSPATH))
else:
  print '***WARNING***: skipping lucene contrib tests'
  
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
            addCPIfExists('../modules/analysis/%s/src/test-files' % package)
            addCPIfExists('../modules/analysis/%s/src/resources' % package)
            strip = len(ROOT) + len('/modules/analysis/%s/src/test/' % package)
            for dir, subDirs, files in os.walk('%s/modules/analysis/%s/src/test' % (ROOT, package)):
              for file in files:
                if file.endswith('.java') and (file.startswith('Test') or file.endswith('Test.java')):
                  fullFile = '%s/%s' % (dir, file)
                  testClass = fullFile[strip:-5].replace('/', '.')
                  # print '  %s' % testClass
                  if doModules:
                    # wd = '%s/modules/analysis' % ROOT
                    wd = ROOT
                    tests.append((estimateCost(testClass), wd, testClass, CLASSPATH))
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
                # wd = '%s/modules/%s' % (ROOT, module)
                wd = ROOT
                tests.append((estimateCost(testClass), wd, testClass, CLASSPATH))

# solr core tests
if doSolr:
  addJARs('../solr/lib')
  addJARs('../solr/example/lib')
  addJARs('../solr/example/lib/jsp-2.1')
  CLASSPATH.append('../solr/build/solr-core/classes/java')
  CLASSPATH.append('../solr/build/solr-core/classes/test')
  CLASSPATH.append('../solr/build/solr-solrj/classes/test')
  CLASSPATH.append('../solr/build/solr-solrj/classes/java')
  CLASSPATH.append('../solr/core/src/test-files')

  # guess!  to load solrconfig.xml
  #CLASSPATH.append('../solr/src/test/test-files/solr/conf')
  #CLASSPATH.append('../solr/src/test/test-files/solr/lib/classes')

  fixCP()
      
  strip = len(ROOT) + len('/solr/core/src/test/')
  for dir, subDirs, files in os.walk('%s/solr/core/src/test' % ROOT):
    for file in files:
      if file.endswith('.java') and (file.startswith('Test') or file.endswith('Test.java')):
        
        fullFile = '%s/%s' % (dir, file)
        testClass = fullFile[strip:-5].replace('/', '.')
        if testClass in ('org.apache.solr.cloud.CloudStateUpdateTest', 'org.apache.solr.search.TestRealTimeGet', 'org.apache.solr.servlet.CacheHeaderTest', 'org.apache.solr.request.TestRemoteStreaming', 'org.apache.solr.handler.dataimport.TestSqlEntityProcessorDelta2'):
          print 'WARNING: skipping test %s' % testClass
          continue
        # print '  %s' % testClass
        wd = '%s/solr/core/src/test/test-files' % ROOT
        # wd = '%s/solr' % ROOT
        tests.append((estimateCost(testClass), wd, testClass, CLASSPATH))

  # solrj
  strip = len(ROOT) + len('/solr/solrj/src/test/')
  for dir, subDirs, files in os.walk('%s/solr/solrj/src/test' % ROOT):
    for file in files:
      if file.endswith('.java') and (file.startswith('Test') or file.endswith('Test.java')):
        
        fullFile = '%s/%s' % (dir, file)
        testClass = fullFile[strip:-5].replace('/', '.')
        if testClass in ('org.apache.solr.cloud.CloudStateUpdateTest',
                         'org.apache.solr.client.solrj.TestBatchUpdate',
                         'org.apache.solr.client.solrj.embedded.SolrExampleJettyTest',
                         'org.apache.solr.client.solrj.embedded.SolrExampleStreamingTest',
                         'org.apache.solr.client.solrj.SolrExampleBinaryTest',
                         'org.apache.solr.client.solrj.embedded.SolrExampleStreamingBinaryTest',
                         'org.apache.solr.client.solrj.embedded.TestSolrProperties'):
          print 'WARNING: skipping test %s' % testClass
          continue
        # print '  %s' % testClass
        wd = '%s/solr/solrj/src/test-files' % ROOT
        # wd = '%s/solr' % ROOT
        tests.append((estimateCost(testClass), wd, testClass, CLASSPATH))

  # solr contrib tests
  for contrib in os.listdir('%s/solr/contrib' % ROOT):
    if False and contrib == 'clustering':
      continue
    if not os.path.isdir('%s/solr/contrib/%s' % (ROOT, contrib)) or contrib in ('.svn',):
      continue
    # print 'contrib/%s' % contrib
    strip = len(ROOT) + len('/solr/contrib/%s/src/test/' % contrib)
    if contrib == 'extraction':
      contrib2 = 'cell'
    else:
      contrib2 = contrib
    addCP('%s/solr/build/contrib/solr-%s/classes/java' % (ROOT, contrib2))
    addCP('%s/solr/build/contrib/solr-%s/classes/test' % (ROOT, contrib2))
    if contrib == 'dataimporthandler':
      s = 'dih'
    else:
      s = contrib
    #addCP('%s/solr/contrib/%s/src/test-files/solr-%s' % (ROOT, contrib, s))
    addCP('%s/solr/contrib/%s/src/test-files' % (ROOT, contrib))
    #addCP('%s/solr/contrib/%s/target/classes' % (ROOT, contrib))
    #addCP('%s/solr/contrib/%s/target/extras/classes' % (ROOT, contrib))
    #addCP('%s/solr/contrib/%s/build/test-classes' % (ROOT, contrib))
    #addCP('%s/solr/contrib/%s/build/classes' % (ROOT, contrib))
    #addCP('%s/solr/contrib/%s/src/test/resources' % (ROOT, contrib))
    #addCP('%s/solr/contrib/%s/src/main/resources' % (ROOT, contrib))
    addCP('%s/solr/contrib/%s/src/resources' % (ROOT, contrib))
      
    if 0:
      addCP('../solr/build/contrib/%s/classes/java' % contrib)
      addCP('../lucene/build/contrib/%s/classes/test' % contrib)
      addCP('../lucene/build/contrib/%s/classes' % contrib)
    libDir = '%s/solr/contrib/%s/lib' % (ROOT, contrib)
    addJARs(libDir)
    os.chdir('%s/solr/contrib/%s' % (ROOT, contrib))
    if '-noc' not in sys.argv:
      run('Compile Solr contrib %s...' % contrib, 'ant compile-test', 'compile-contrib-test.log')
    for dir, subDirs, files in os.walk('%s/solr/contrib/%s/src/test' % (ROOT, contrib)):
      for file in files:
        if file.endswith('.java') and (file.startswith('Test') or file.endswith('Test.java')):
          fullFile = '%s/%s' % (dir, file)
          testClass = fullFile[strip:-5].replace('/', '.')
          if testClass in ('org.apache.solr.handler.clustering.DistributedClusteringComponentTest',):
            print 'WARNING: skipping test %s' % testClass
            continue
          # print '  %s' % testClass
          #wd = '%s/solr/contrib/%s/src/test/resources' % (ROOT, contrib)
          wd = '%s/solr' % ROOT
          tests.append((estimateCost(testClass), wd, testClass, CLASSPATH))

def cmpTests(test1, test2):
  if test1[1] != test2[1]:
    # different wd
    return cmp(test1[1], test2[1])
  else:
    return -cmp(test1[0], test2[0])

tests.sort(cmpTests)

if False:
  newTests = []
  for test in tests:
    if test[2].find('uima') != -1:
      newTests.append(test)
      #print 'KEEP: %s' % str(test)
  tests = newTests

#for test in tests:
#  print '%s: %s' % (test[0], test[2])
repeat = '-repeat' in sys.argv

if 0:
  for cost, path, test, cp in tests:
    print 'TEST: %s' % test
  print 'Total %d tests' % len(tests)

pendingCost = 0
workQ = WorkQueue()

aggTests(workQ, tests)

for resource, threadCount in NUM_THREAD:
  if resource != 'local':
    cmd = '/usr/bin/rsync --delete -rtS %s -e "ssh -x -c arcfour -o Compression=no" --exclude=.svn/ --exclude="*.log" mike@%s:%s' % (ROOT, resource, constants.BASE_DIR)
    print 'Copy to %s: %s' % (resource, cmd)
    t = time.time()
    if os.system(cmd):
      raise RuntimeError('rsync failed')
    print '  %.1f sec' % (time.time()-t)
    os.system('ssh %s "rm -f %s/*.log"' % (resource, ROOT))
    os.system('ssh %s "killall java >& /dev/null"' % resource)
  else:
    os.system('rm -f %s/*.log' % ROOT)

threads = []
t0 = time.time()
for resourceID, (resource, threadCount) in enumerate(NUM_THREAD):
  for threadID in xrange(threadCount):
    threads.append(RunThread(resource, resourceID, threadID, workQ))
  # Let beast kick off jobs/threads first
  time.sleep(0.2)

totSuites = 0
failed = False
for thread in threads:
  thread.join()
  totSuites += thread.suiteCount

if DO_GATHER_TIMES:
  open(TEST_TIMES_FILE, 'wb').write(cPickle.dumps(testTimes))

print '\n%.1f sec [%d test suites]' % (time.time()-t0, totSuites)
