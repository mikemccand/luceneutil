import copy
import os
import codecs
import random
import time
import socket
import threading
import subprocess
import cPickle
import traceback
import sys

"""
One instance of this runs, per machine that will launch tests.  This
instance spawns multiple sub-processes and manages communications with
them, back to the main server.
"""

"""
/usr/local/src/jdk1.7.0_04/jre/bin/java -Dtests.prefix=tests -Dtests.seed=771F118CC53F329 -Xmx512M -Dtests.iters= -Dtests.verbose=false -Dtests.infostream=false -Dtests.lockdir=/l/lucene.trunk/lucene/build -Dtests.codec=random -Dtests.postingsformat=random -Dtests.locale=random -Dtests.timezone=random -Dtests.directory=random -Dtests.linedocsfile=europarl.lines.txt.gz -Dtests.luceneMatchVersion=4.0 -Dtests.cleanthreads=perMethod -Djava.util.logging.config.file=/dev/null -Dtests.nightly=false -Dtests.weekly=false -Dtests.slow=false -Dtests.asserts.gracious=false -Dtests.multiplier=1 -DtempDir=. -Dlucene.version=4.0-SNAPSHOT -Djetty.testMode=1 -Djetty.insecurerandom=1 -Dsolr.directoryFactory=org.apache.solr.core.MockDirectoryFactory -classpath /l/lucene.trunk/lucene/build/test-framework/classes/java:/l/lucene.trunk/lucene/test-framework/lib/junit-4.10.jar:/l/lucene.trunk/lucene/test-framework/lib/randomizedtesting-runner-1.4.0.jar:/l/lucene.trunk/lucene/build/core/classes/java:/l/lucene.trunk/lucene/build/core/classes/test:/usr/local/src/apache-ant-1.8.3/lib/ant-launcher.jar:/home/mike/.ant/lib/ivy-2.2.0.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-netrexx.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-jmf.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-commons-net.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-testutil.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-junit.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-apache-bcel.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-apache-regexp.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-jai.jar:/usr/local/src/apache-ant-1.8.3/lib/ant.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-javamail.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-apache-bsf.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-antlr.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-apache-oro.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-apache-resolver.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-apache-xalan2.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-jsch.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-apache-log4j.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-swing.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-jdepend.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-commons-logging.jar:/usr/local/src/apache-ant-1.8.3/lib/ant-junit4.jar:/usr/local/src/jdk1.7.0_04/lib/tools.jar:/l/lucene.trunk/lucene/test-framework/lib/junit4-ant-1.4.0.jar -ea:org.apache.lucene... -ea:org.apache.solr... com.carrotsearch.ant.tasks.junit4.slave.SlaveMainSafe -flush -eventsfile /l/lucene.trunk/lucene/build/core/test/junit4-J0-0819129977b5076df.events @/l/lucene.trunk/lucene/build/core/test/junit4-J0-1916253054fa0d84f.suites
"""

class Child(threading.Thread) :

  """
  Interacts with one child test runner.
  """

  def __init__(self, id, parent):
    threading.Thread.__init__(self)
    self.id = id
    self.parent = parent

  def run(self):

    eventsFile = '%s/lucene/build/C%d.events' % (self.parent.rootDir, self.id)
    if os.path.exists(eventsFile):
      os.remove(eventsFile)
    
    cmd = '%s -eventsfile %s' % (self.parent.command, eventsFile)

    # TODO
    #   - add -Dtests.seed=XXX, eg -Dtests.seed=771F118CC53F329
    #   - add -eventsfile /l/lucene.trunk/lucene/build/core/test/junit4-J0-0819129977b5076df.events @/l/lucene.trunk/lucene/build/core/test/junit4-J0-1916253054fa0d84f.suites

    try:
      #self.parent.remotePrint('C%d init' % self.id)

      # TODO
      p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE, env=self.parent.env)
      #self.parent.remotePrint('C%d subprocess started' % self.id)

      events = ReadEvents(eventsFile, self.parent)
      #self.parent.remotePrint('C%d startup0 done' % self.id)
      events.waitIdle()

      #self.parent.remotePrint('startup done C%d' % self.id)
      
      while True:
        job = self.parent.nextJob()
        if job is None:
          #self.parent.remotePrint('C%d no more jobs' % self.id)
          p.stdin.close()
          break
        #self.parent.remotePrint('C%d: job %s' % (self.id, job))
        p.stdin.write(job + '\n')
        results = events.waitIdle()

        # nocommit how to detect failure!
        endSuite = False
        output = []
        failed = False
        msec = None
        for l in results:
          #if l.find('"chunk": ') != -1 or l.find('"bytes": ') != -1:
          if l.find('"chunk": ') != -1:
            chunk = l.strip().split()[-1][1:-1]
            #self.parent.remotePrint('C%d: chunk=%s' % (self.id, chunk))
            bytes = []
            idx = 0
            while idx < len(chunk):
              bytes.append(chr(int(chunk[idx:idx+2], 16)))
              idx += 2
            output.append(codecs.getdecoder('UTF8')(''.join(bytes))[0])

          if l.find('"trace": ') != -1:
            chunk = l.strip().replace('"trace": "', '')[:-2]
            chunk = chunk.replace('\\n', '\n')
            chunk = chunk.replace('\\t', '\t')
            output.append(chunk)
            if chunk.find('AssumptionViolatedException') == -1:
              failed = True
            
          if l.find('"SUITE_COMPLETED"') != -1:
            endSuite = True
          elif endSuite and l.find('"executionTime"') != -1:
            msec = int(l.strip()[:-1].split()[1])
            break

        if not failed:
          output = []
        self.parent.sendResult((job, msec, output))
        
    except:
      self.parent.remotePrint('C%d: EXC:\n%s' % (self.id, traceback.format_exc()))

class ReadEvents:

  def __init__(self, fileName, parent):
    self.fileName = fileName
    self.parent = parent
    while True:
      try:
        self.f = open(self.fileName, 'rb')
      except IOError:
        time.sleep(.001)
      else:
        break
    self.f.seek(0)
    
  def readline(self):
    while True:
      pos = self.f.tell()
      l = self.f.readline()
      if l == '' or not l.endswith('\n'):
        time.sleep(.01)
        self.f.seek(pos)
      else:
        #self.parent.remotePrint('readline=%s' % l.strip())
        return l

  def waitIdle(self):
    lines = []
    while True:
      l = self.readline()
      if l.find('"IDLE",') != -1:
        return lines
      else:
        lines.append(l)
    
class Parent:

  def __init__(self, rootDir, id, processCount, env, command):

    self.rootDir = rootDir
    self.id = id
    self.env = env
    self.command = command
    self.children = []
    self.jobLock = threading.Lock()

    print 'REMOTE SERVER STARTED'

    for childID in xrange(processCount):
      child = Child(childID, self)
      child.start()
      # Silly: subprocess.Popen seems to hang if we launch too quickly
      time.sleep(.001)
      self.children.append(child)

    for child in self.children:
      child.join()

    # self.remotePrint('all children done')

  def remotePrint(self, message):
    with self.jobLock:
      sys.stdout.write('PRINT')
      bytes = codecs.getencoder('UTF8')(message)[0]
      sys.stdout.write('%8d' % len(bytes))
      sys.stdout.write(bytes)

  def sendResult(self, result):
    with self.jobLock:
      bytes = cPickle.dumps(result)
      sys.stdout.write('RESUL')
      sys.stdout.write('%8d' % len(bytes))
      sys.stdout.write(bytes)
      
  def nextJob(self):
    with self.jobLock:
      sys.stdout.write('READY')
      len = int(sys.stdin.read(8))
      if len == -1:
        return None
      else:
        return cPickle.loads(sys.stdin.read(len))

def main():
  myID = sys.argv[1]
  processCount = int(sys.argv[2])
  rootDir = sys.argv[3]
  classPath = sys.argv[4]
  command = sys.argv[5]

  os.chdir('%s/lucene/build' % rootDir)
  env = copy.copy(os.environ)
  env['CLASSPATH'] = classPath

  Parent(rootDir, myID, processCount, env, command)

if __name__ == '__main__':
  main()
