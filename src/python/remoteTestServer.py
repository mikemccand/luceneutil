import copy
import os
import codecs
import time
import threading
import subprocess
import cPickle
import traceback
import sys
import signal

signal.signal(signal.SIGCHLD, signal.SIG_IGN)

"""
One instance of this runs, per machine that will launch tests.  This
instance spawns multiple sub-processes and manages communications with
them, back to the main server.
"""

class Child(threading.Thread) :

  """
  Interacts with one child test runner.
  """

  def __init__(self, id, parent, startedEvent):
    threading.Thread.__init__(self)
    self.id = id
    self.startedEvent = startedEvent
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
      p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.STDOUT, env=self.parent.env)
      #p = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, env=self.parent.env)
      #self.parent.remotePrint('C%d subprocess started' % self.id)
      self.startedEvent.set()
      #self.parent.remotePrint('HERE: %s' % p.stdout.read(1))
      events = ReadEvents(p, eventsFile, self.parent)
      #self.parent.remotePrint('C%d startup0 done' % self.id)
      events.waitIdle()

      #self.parent.remotePrint('startup done C%d' % self.id)
      
      while True:
        #self.parent.remotePrint('C%d get job' % self.id)
        job = self.parent.nextJob()
        if job[1] is None:
          #self.parent.remotePrint('C%d no more jobs' % self.id)
          #p.stdin.close()
          p.kill()
          break
        #self.parent.remotePrint('C%d: job %s' % (self.id, job))
        p.stdin.write(job[1] + '\n')
        results = events.waitIdle()

        endSuite = False
        output = []
        failed = False
        msec = None
        for l in results:
          #if l.find('"chunk": ') != -1 or l.find('"bytes": ') != -1:
          if l.find('"chunk": ') != -1:
            if False:
              chunk = l.strip().split()[-1][1:-1]
              #self.parent.remotePrint('C%d: chunk=%s' % (self.id, chunk))
              bytes = []
              idx = 0
              while idx < len(chunk):
                bytes.append(chr(int(chunk[idx:idx+2], 16)))
                idx += 2
              try:
                # Spooky I must replace!!!  eg chunk=383637205432313220433720524551205B636F6C6C656374696F6E315D207765626170703D6E756C6C20706174683D6E756C6C20706172616D733D7B736F72743D69642B61736326666C3D696426713D736F72745F74725F63616E6F6E3A22492B57696C6C2B5573652B5475726B6973682B436173F56E67227D20686974733D33207374617475733D30205154696D653D31200A
                output.append(codecs.getdecoder('UTF8')(''.join(bytes), errors='replace')[0])
              except:
                self.parent.remotePrint('C%d: EXC:\n%s\nchunk=%s' % (self.id, traceback.format_exc(), chunk))
            else:
              l = l.strip()[14:-1]
              
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

  def __init__(self, process, fileName, parent):
    self.process = process
    self.fileName = fileName
    self.parent = parent
    while True:
      try:
        self.f = open(self.fileName, 'rb')
      except IOError:
        time.sleep(.01)
      else:
        break
    self.f.seek(0)
    
  def readline(self):
    while True:
      pos = self.f.tell()
      l = self.f.readline()
      if l == '' or not l.endswith('\n'):
        time.sleep(.01)
        p = self.process.poll()
        if p is not None:
          raise RuntimeError('process exited with status %s' % str(p))
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

    print('REMOTE SERVER STARTED')
    #self.remotePrint('python version is %s' % sys.version)

    for childID in xrange(processCount):
      #self.remotePrint('start child %d' % childID)
      startedEvent = threading.Event()
      child = Child(childID, self, startedEvent)
      child.start()
      # Silly: subprocess.Popen seems to hang if we launch too quickly
      startedEvent.wait()
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

def dumpstacks(signal, frame):
  id2name = dict([(th.ident, th.name) for th in threading.enumerate()])
  code = []
  for threadId, stack in sys._current_frames().items():
    code.append("\n# Thread: %s(%d)" % (id2name.get(threadId,""), threadId))
    for filename, lineno, name, line in traceback.extract_stack(stack):
      code.append('File: "%s", line %d, in %s' % (filename, lineno, name))
      if line:
        code.append("  %s" % (line.strip()))
  open('/tmp/%s.stacks' % os.getpid(), 'w').write('\n'.join(code))

signal.signal(signal.SIGUSR1, dumpstacks)

if __name__ == '__main__':
  main()
