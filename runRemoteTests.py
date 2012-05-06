import sys
import threading
import time
import subprocess
import socket
import constants
import codecs
import common
import cPickle
import os

# TODO
#   - make sure no leftover processes!
#   - need -repeat
#   - need to run 'ant compile'
#   - may need separate instances per WD, per CLASSPATH?
#   - different java per env?
#   - solr tests?

# NOTE: you must have passwordless ssh to all these machines:

RESOURCES = (
  ('vine', 4),
  ('beast', 12),
  ('scratch', 2),
  ('mikedesktop', 4),
  )

USERNAME = 'mike'

VERBOSE = '-verbose' in sys.argv

printLock = threading.Lock()

tStart = time.time()

def msg(message):
  with printLock:
    if VERBOSE:
      print '%.3fs: %s' % (time.time()-tStart, message)
    else:
      print message

def run(comment, cmd, logFile, printTime=False):
  t0 = time.time()
  print comment
  if os.system('%s > %s 2>&1' % (cmd, logFile)):
    print open(logFile).read()
    raise RuntimeError('FAILED: %s' % cmd)
  if printTime:
    print '  %.1f sec' % (time.time()-t0)

class Remote(threading.Thread):

  """
  Handles interactions with one remote machine.
  """

  def __init__(self, stats, jobs, command, classpath, rootDir, hostName, processCount):
    threading.Thread.__init__(self)
    self.stats = stats
    self.jobs = jobs
    self.command = command
    self.classpath = classpath
    self.hostName = hostName
    self.processCount = processCount
    self.rootDir = rootDir
    self.anyFails = False

  def run(self):
    if self.hostName != socket.gethostname():
      cmd = '/usr/bin/rsync --delete -rtS %s -e "ssh -x -c arcfour -o Compression=no" --exclude=".#*" --exclude="C*.events" --exclude=.svn/ --exclude="*.log" %s@%s:%s' % \
            (self.rootDir, USERNAME, self.hostName, constants.BASE_DIR)
      t = time.time()
      if os.system(cmd):
        msg('local: %s: WARNING rsync failed' % self.hostName)
      msg('local: %s: rsync took %.1f sec' % (self.hostName, time.time()-t))
      os.system('ssh %s "killall java >& /dev/null"' % self.hostName)
        
    cmd = 'ssh -Tx %s@%s python -u %s/remoteTestServer.py %s %s %s %s \'"%s"\'' % \
              (USERNAME,
               self.hostName,
               constants.BENCH_BASE_DIR,
               self.hostName,
               self.processCount,
               self.rootDir,
               self.classpath,
               self.command)

    # msg('local: %s: start cmd: %s' % (self.hostName, cmd))

    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)

    # Skip login banner:
    while True:
      s = p.stdout.readline()
      if s == '':
        raise RuntimeError('failed to start remoteTestServer.py')
      elif s.strip() == 'REMOTE SERVER STARTED':
        break
      
    # msg('local: %s: started' % self.hostName)
    
    while True:
      command = p.stdout.read(5)
      if command == 'PRINT':
        numBytes = int(p.stdout.read(8))
        msg('%s: %s' % (self.hostName, codecs.getdecoder('UTF8')(p.stdout.read(numBytes))[0]))
      elif command == 'READY':
        # TODO: pull new job here
        bytes = cPickle.dumps(self.jobs.nextJob())
        p.stdin.write('%8d' % len(bytes))
        p.stdin.write(bytes)
      elif command == 'RESUL':
        numBytes = int(p.stdout.read(8))
        job, msec, errors = cPickle.loads(p.stdout.read(numBytes))
        if len(errors) != 0:

          s = '\n\nFAILURE: %s' % job
          for error in errors:
            s = s + '\n' + error
          if job in ('org.apache.solr.TestGroupingSearch',
                     'org.apache.solr.TestDistributedGrouping') and \
                     s.find('Caused by: org.apache.lucene.util.ThreadInterruptedException') != -1:
            # OK
            pass
          else:
            msg(s)
            self.anyFails = True
        self.stats.update(job, msec/1000.0)
        if VERBOSE:
          msg('%s: %d msec for %s' % (self.hostName, msec, job))
        else:
          sys.stdout.write('.')
          sys.stdout.flush()
      elif command == '':
        break

TEST_TIMES_FILE = '%s/TEST_TIMES.pk' % constants.BASE_DIR

class Stats:

  def __init__(self):
    try:
      self.testTimes = cPickle.loads(open(TEST_TIMES_FILE, 'rb').read())
    except:
      print 'WARNING: no test times:'
      self.testTimes = {}

  def update(self, className, msec):
    if className not in self.testTimes:
      self.testTimes[className] = [0.0, 0.0, 0]
    l = self.testTimes[className]
    l[0] += msec
    l[1] += msec*msec
    l[2] += 1

  def save(self):
    #print 'Saved stats...'
    open(TEST_TIMES_FILE, 'wb').write(cPickle.dumps(self.testTimes))

  def estimateCost(self, className):
    try:
      #print '%.1f sec: %s' % (testTimes[testClass], testClass)
      l = self.testTimes[className]
      # TODO: use variance too!
      meanTime = l[0] / l[2]
      return meanTime
    except KeyError:
      return 100.0

FLAKY_TESTS = set([
  # Not flaky, just gets angry about polluted classpaths!:
  'org.apache.lucene.analysis.core.TestRandomChains',

  # requires a certain cwd because it writes to a relative path:
  'org.apache.solr.handler.dataimport.TestSolrEntityProcessorEndToEnd',
  ])

class Jobs:

  def __init__(self, tests):
    self.tests = tests
    self.upto = 0
    self.lock = threading.Lock()

  def nextJob(self):
    with self.lock:
      if self.upto == len(self.tests):
        #msg('no more tests')
        test = None
      else:
        test = self.tests[self.upto][1]
        self.upto += 1
      return test
    
def jarOK(jar):
  return jar != 'log4j-1.2.14.jar'

def addJARs(cp, path):
  if os.path.exists(path):
    for f in os.listdir(path):
      if f.endswith('.jar') and jarOK(f):
        cp.append('%s/%s' % (path, f))

def gatherTests(stats, rootDir):

  cp = []
  addCP = cp.append

  addCP('lucene/test-framework/lib/junit-4.10.jar')
  addCP('lucene/test-framework/lib/randomizedtesting-runner-1.4.0.jar')
  addCP('lucene/test-framework/lib/junit4-ant-1.4.0.jar')
  addCP('lucene/build/test-framework/classes/java')
  addCP('solr/build/solr-test-framework/classes/java')

  testDir = '%s/lucene/build/core/test' % rootDir
  if not os.path.exists(testDir):
    os.makedirs(testDir)

  tests = []

  modules = []

  print 'ROOT %s' % rootDir

  if '-noc' not in sys.argv:
    os.chdir('%s/lucene' % rootDir)
    run('Compile Lucene...', 'ant compile-test', 'compile.log', printTime=True)

    if '-solr' in sys.argv:
      os.chdir('%s/solr' % rootDir)
      run('Compile Solr...', 'ant compile-test', 'compile.log', printTime=True)

    os.chdir(rootDir)
    
  # lucene tests
  for ent in os.listdir('%s/lucene' % rootDir):
    if os.path.isdir('%s/lucene/%s' % (rootDir, ent)):

      if ent == 'test-framework':
        # Who tests the tester?  (test-framework has no src/test)
        continue

      if ent == 'analysis':
        # has sub-modules:
        for ent2 in os.listdir('%s/lucene/analysis' % rootDir):
          if os.path.isdir('%s/lucene/analysis/%s' % (rootDir, ent2)):
            modules.append('analysis/%s' % ent2)
      else:
        modules.append(ent)

  for module in modules:

    path = '%s/lucene/%s' % (rootDir, module)
    addCP('lucene/build/%s/classes/java' % module)
    addCP('lucene/build/%s/classes/test' % module)
    addCP('lucene/build/%s/classes/examples' % module)
    addCP('lucene/%s/src/test-files' % module)
    addCP('lucene/%s/src/resources' % module)

    libDir = '%s/lib' % module
    if os.path.exists('lucene/%s' % libDir):
      for f in os.listdir('lucene/%s' % libDir):
        if f.endswith('.jar'):
          addCP('lucene/%s/%s' % (libDir, f))

    strip = len(rootDir) + len('/lucene/%s/src/test/' % module)
    for dir, subDirs, files in os.walk('%s/src/test' % path):
      for file in files:
        if file.endswith('.java') and (file.startswith('Test') or file.endswith('Test.java')):
          fullFile = '%s/%s' % (dir, file)
          testClass = fullFile[strip:-5].replace('/', '.')
          if testClass in FLAKY_TESTS:
            print 'WARNING: skipping test %s' % testClass
            continue

          tests.append((stats.estimateCost(testClass), testClass))

  doSolr = '-solr' in sys.argv
  
  if doSolr:
    addJARs(cp, 'solr/lib')
    addJARs(cp, 'solr/example/lib')
    addCP('solr/build/solr-core/classes/java')
    addCP('solr/build/solr-core/classes/test')
    addCP('solr/build/solr-core/test-files')
    addCP('solr/build/solr-solrj/classes/java')
    addCP('solr/build/solr-solrj/classes/test')
    addCP('solr/build/solr-solrj/test-files')
    addCP('solr/core/src/test-files')

    strip = len(rootDir) + len('/solr/core/src/test/')
    for dir, subDirs, files in os.walk('%s/solr/core/src/test' % rootDir):
      for file in files:
        if file.endswith('.java') and (file.startswith('Test') or file.endswith('Test.java')):

          fullFile = '%s/%s' % (dir, file)
          testClass = fullFile[strip:-5].replace('/', '.')
          if testClass in FLAKY_TESTS:
            print 'WARNING: skipping test %s' % testClass
            continue
          tests.append((stats.estimateCost(testClass), testClass))

    # solrj
    strip = len(rootDir) + len('/solr/solrj/src/test/')
    for dir, subDirs, files in os.walk('%s/solr/solrj/src/test' % rootDir):
      for file in files:
        if file.endswith('.java') and (file.startswith('Test') or file.endswith('Test.java')):

          fullFile = '%s/%s' % (dir, file)
          testClass = fullFile[strip:-5].replace('/', '.')
          if testClass in FLAKY_TESTS:
            print 'WARNING: skipping test %s' % testClass
            continue
          # print '  %s' % testClass
          tests.append((stats.estimateCost(testClass), testClass))

    # solr contrib tests
    for contrib in os.listdir('%s/solr/contrib' % rootDir):
      if not os.path.isdir('%s/solr/contrib/%s' % (rootDir, contrib)) or contrib in ('.svn',):
        continue
      # print 'contrib/%s' % contrib
      strip = len(rootDir) + len('/solr/contrib/%s/src/test/' % contrib)
      if contrib == 'extraction':
        contrib2 = 'cell'
      else:
        contrib2 = contrib

      addCP('solr/build/contrib/solr-%s/classes/java' % contrib2)
      addCP('solr/build/contrib/solr-%s/classes/test' % contrib2)
      addCP('solr/build/contrib/solr-%s/test-files' % contrib2)
      addCP('solr/contrib/%s/src/resources' % contrib)

      s = contrib
      libDir = 'solr/contrib/%s/lib' % contrib
      addJARs(cp, libDir)
      for dir, subDirs, files in os.walk('%s/solr/contrib/%s/src/test' % (rootDir, contrib)):
        for file in files:
          if file.endswith('.java') and (file.startswith('Test') or file.endswith('Test.java')):
            fullFile = '%s/%s' % (dir, file)
            testClass = fullFile[strip:-5].replace('/', '.')
            if testClass in FLAKY_TESTS:
              print 'WARNING: skipping test %s' % testClass
              continue
            # print '  %s' % testClass
            #wd = '%s/solr/contrib/%s/src/test/resources' % (ROOT, contrib)
            tests.append((stats.estimateCost(testClass), testClass))

  tests.sort(reverse=True)
  #for idx in xrange(10):
  #  print 'top test: %s' % str(tests[idx])
  return cp, tests

testTimes = {}

def main():
  global tTestsStart
  
  rootDir = common.findRootDir(os.getcwd())
  localHostName = socket.gethostname()

  stats = Stats()
  
  classpath, tests = gatherTests(stats, rootDir)

  # nocommit
  #tests = [(1.0, 'org.apache.solr.client.solrj.embedded.SolrExampleStreamingTest')]

  # TODO: solr has tests.cleanthreads=perClass but lucene has
  # perMethod... maybe I need dedicated solr vs lucene jvms
  command = 'java -Dtests.prefix=tests -Xmx512M -Dtests.iters= -Dtests.verbose=false -Dtests.infostream=false -Dtests.lockdir=%s/lucene/build -Dtests.codec=random -Dtests.postingsformat=random -Dtests.locale=random -Dtests.timezone=random -Dtests.directory=random -Dtests.linedocsfile=europarl.lines.txt.gz -Dtests.luceneMatchVersion=4.0 -Dtests.cleanthreads=perClass -Djava.util.logging.config.file=solr/testlogging.properties -Dtests.nightly=false -Dtests.weekly=false -Dtests.slow=false -Dtests.asserts.gracious=false -Dtests.multiplier=1 -DtempDir=. -Dlucene.version=4.0-SNAPSHOT -Djetty.testMode=1 -Djetty.insecurerandom=1 -Dsolr.directoryFactory=org.apache.solr.core.MockDirectoryFactory -ea:org.apache.lucene... -ea:org.apache.solr... com.carrotsearch.ant.tasks.junit4.slave.SlaveMainSafe -flush -stdin' % rootDir

  # Tests first chdir to lucene/build:
  classpath = ':'.join(['../../%s' % x for x in classpath])
  #print 'CP: %s' % classpath
  jobs = Jobs(tests)

  tTestsStart = time.time()
  
  # Launch local first since it can immediately start working, and, it
  # will pull the hardest jobs...:
  workers = []
  for hostName, processCount in RESOURCES:
    if hostName == localHostName:
      remote = Remote(stats, jobs, command, classpath, rootDir, hostName, processCount)
      remote.start()
      workers.append(remote)
      break

  for hostName, processCount in RESOURCES:
    if hostName != localHostName:
      remote = Remote(stats, jobs, command, classpath, rootDir, hostName, processCount)
      remote.start()
      workers.append(remote)

  anyFails = False
  for worker in workers:
    worker.join()
    if worker.anyFails:
      anyFails = True

  stats.save()
  print
  if anyFails:
    print 'FAILED'
  else:
    print 'SUCCESS'
  print
  print '%.1f sec' % (time.time()-tTestsStart)
    
if __name__ == '__main__':
  main()
