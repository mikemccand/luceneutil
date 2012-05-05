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
#   - make sure no turd processes!
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

# nocommit
#RESOURCES = (
#  ('vine', 1),
#  )
  
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
      cmd = '/usr/bin/rsync --delete -rtS %s -e "ssh -x -c arcfour -o Compression=no" --exclude=.svn/ --exclude="*.log" %s@%s:%s' % \
            (self.rootDir, USERNAME, self.hostName, constants.BASE_DIR)
      t = time.time()
      if os.system(cmd):
        raise RuntimeError('rsync failed')
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
      # nocommit
      # print 'NO COST: %s' % testClass
      return 1.0

    
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

  os.chdir('%s/lucene' % rootDir)

  # lucene tests
  for ent in os.listdir('.'):
    if os.path.isdir(ent):

      if ent == 'test-framework':
        # Who tests the tester?  (test-framework has no src/test)
        continue

      if ent == 'analysis':
        # has sub-modules:
        for ent2 in os.listdir('analysis'):
          if os.path.isdir('analysis/%s' % ent2):
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
    if os.path.exists(libDir):
      for f in os.listdir(libDir):
        if f.endswith('.jar'):
          addCP('lucene/%s/%s' % (libDir, f))

    strip = len(rootDir) + len('/lucene/%s/src/test/' % module)
    for dir, subDirs, files in os.walk('%s/src/test' % path):
      for file in files:
        if file.endswith('.java') and (file.startswith('Test') or file.endswith('Test.java')):
          fullFile = '%s/%s' % (dir, file)
          testClass = fullFile[strip:-5].replace('/', '.')
          if testClass in ('org.apache.lucene.util.junitcompat.TestExceptionInBeforeClassHooks',
                           'org.apache.lucene.analysis.core.TestRandomChains'):
            print 'WARNING: skipping test %s' % testClass
            continue

          tests.append((stats.estimateCost(testClass), testClass))

  tests.sort(reverse=True)
  #for idx in xrange(10):
  #  print 'top test: %s' % str(tests[idx])
  return cp, tests

testTimes = {}

def main():

  rootDir = common.findRootDir(os.getcwd())
  localHostName = socket.gethostname()

  stats = Stats()
  
  classpath, tests = gatherTests(stats, rootDir)

  # nocommit
  #tests = [(1.0, 'org.apache.lucene.TestDemo')]
  
  command = 'java -Dtests.prefix=tests -Xmx512M -Dtests.iters= -Dtests.verbose=false -Dtests.infostream=false -Dtests.lockdir=%s/lucene/build -Dtests.codec=random -Dtests.postingsformat=random -Dtests.locale=random -Dtests.timezone=random -Dtests.directory=random -Dtests.linedocsfile=europarl.lines.txt.gz -Dtests.luceneMatchVersion=4.0 -Dtests.cleanthreads=perMethod -Djava.util.logging.config.file=/dev/null -Dtests.nightly=false -Dtests.weekly=false -Dtests.slow=false -Dtests.asserts.gracious=false -Dtests.multiplier=1 -DtempDir=. -Dlucene.version=4.0-SNAPSHOT -Djetty.testMode=1 -Djetty.insecurerandom=1 -Dsolr.directoryFactory=org.apache.solr.core.MockDirectoryFactory -ea:org.apache.lucene... -ea:org.apache.solr... com.carrotsearch.ant.tasks.junit4.slave.SlaveMainSafe -flush -stdin' % rootDir

  classpath = ':'.join(classpath)
  # print 'CP %s' % classpath

  jobs = Jobs(tests)
  
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
  print '%.1f sec' % (time.time()-tStart)
    
if __name__ == '__main__':
  main()
