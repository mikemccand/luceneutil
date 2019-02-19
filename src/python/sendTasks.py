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

import cStringIO
import random
import codecs
import socket
import Queue
import sys
import time
import threading
import cPickle
import gc
import struct

# If targetQPS is 'sweep' then we start at this QPS:
SWEEP_START_QPS = 10

# ... and every this many seconds we see if we can increase the target:
SWEEP_CHECK_EVERY_SEC = 60

# More frequent thread switching:
sys.setcheckinterval(10)

# We don't create cyclic garbage, and we want no hiccups:
gc.disable()

MAX_BYTES = 70

# TODO
#   - generalize this to send requests via http too
#   - run hiccup thread here?
#   - sweep to find capacity...
#   - test PK lookup, NRT as well

# python -u perf/sendTasks.py /l/util/wikimedium500.tasks localhost 7777 10 10 10

class RollingStats:

  def __init__(self, count):
    self.buffer = [0] * count
    self.sum = 0
    self.upto = 0

  def add(self, value):
    if value < 0:
      raise RuntimeError('values should be positive')
    idx = self.upto % len(self.buffer)
    self.sum += value - self.buffer[idx]
    self.buffer[idx] = value
    self.upto += 1

  def get(self):
    if self.upto == 0:
      return -1.0
    else:
      if self.upto < len(self.buffer):
        v = self.sum/self.upto
      else:
        v = self.sum/len(self.buffer)
      # Don't let roundoff error manifest as -0.0:
      return max(0.0, v)

class Results:

  def __init__(self, savFile):
    self.buffers = []
    self.current = cStringIO.StringIO()
    self.fOut = open(savFile, 'wb')

  def add(self, taskString, totalHitCount, timestamp, latencyMS, queueTimeMS):
    self.current.write(struct.pack('fffIB', timestamp, latencyMS, queueTimeMS, totalHitCount, len(taskString)))
    self.current.write(taskString)
    if self.current.tell() >= 64*1024:
      self.fOut.write(self.current.getvalue())
      self.fOut.flush()
      self.current = cStringIO.StringIO()
      
  def finish(self):
    self.fOut.close()
  
class SendTasks:

  def __init__(self, serverHost, serverPort, out, runTimeSec, savFile):
    self.startTime = time.time()

    self.out = out
    self.runTimeSec = runTimeSec
    self.results = Results(savFile)
    self.sent = {}
    self.queue = Queue.Queue()

    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.sock.connect((serverHost, serverPort))

    t = threading.Thread(target=self.gatherResponses,
                         args=())
    t.setDaemon(True)
    t.start()

    t = threading.Thread(target=self.sendRequests,
                         args=())
    t.setDaemon(True)
    t.start()

    self.taskID = 0

  def send(self, startTime, task):
    self.sent[self.taskID] = (startTime, task)
    self.queue.put(task)
    self.taskID += 1

  def gatherResponses(self):

    '''
    Runs as dedicated thread gathering results coming from the server.
    '''

    startTime = self.startTime
    lastPrint = self.startTime

    lastSec = None
    queriesThisSec = 0
    
    queueTimeStats = RollingStats(100)
    totalTimeStats = RollingStats(100)
    actualQPSStats = RollingStats(5)
    latenciesInMS = []
    
    while True:
      result = ''
      while len(result) < 30:
        result = result + self.sock.recv(30 - len(result))
      taskID, totalHitCount, queueTimeMS = result.split(':')
      taskID = int(taskID)
      totalHitCount = int(totalHitCount)
      queueTimeMS = float(queueTimeMS)
      endTime = time.time()
      intSec = int(endTime)
      if intSec != lastSec:
        if intSec - self.startTime >= 1:
          actualQPSStats.add(float(queriesThisSec))
        queriesThisSec = 1
        lastSec = intSec
      else:
        queriesThisSec += 1
      
      try:
        taskStartTime, taskString = self.sent[taskID]
      except KeyError:
        print 'WARNING: ignore bad return taskID=%s' % taskID
        continue
      del self.sent[taskID]
      latencyMS = (endTime-taskStartTime)*1000
      queueTimeStats.add(queueTimeMS)
      totalTimeStats.add(latencyMS)
      latenciesInMS.append(latencyMS)
      self.results.add(taskString.strip(),
                       totalHitCount,
                       taskStartTime-startTime,
                       latencyMS,
                       queueTimeMS)

      now = time.time()
      if now - lastPrint > 2.0:
        pctDone = 100.0*(now - startTime) / self.runTimeSec
        if pctDone > 100.0:
          pctDone = 100.0

        latenciesInMS.sort()

        self.out.write('p0: ' + str(latenciesInMS[0]))
        self.out.write('p50: ' + str(latenciesInMS[(len(latenciesInMS)-1)/2]))
        self.out.write('p90: ' + str(times[int((len(latenciesInMS)-1)*0.9)]))
        self.out.write('p100: ' + str(times[len(latenciesInMS)-1]))

        self.out.write('%6.1f s: %5.1f%%: %5.1f qps in; %5.1f qps out; %6.1f/%6.1f ms [%d, %d]\n' % \
                       (now - startTime, pctDone,
                        self.taskID/(now-startTime),
                        actualQPSStats.get(),
                        totalTimeStats.get(),
                        queueTimeStats.get(),
                        self.queue.qsize(),
                        len(self.sent)))
        #self.out.flush()
        lastPrint = now
                  
  def sendRequests(self):

    '''
    Runs as dedicated thread, sending requests from the queue to the
    server.
    '''

    while True:
      task = self.queue.get()
      startTime = time.time()
      while len(task) > 0:
        sent = self.sock.send(task)
        if sent <= 0:
          raise RuntimeError('failed to send task "%s"' % task)
        task = task[sent:]

def pruneTasks(taskStrings, numTasksPerCat):
  byCat = {}
  for s in taskStrings:
    cat = s.split(':', 1)[0]
    if cat not in byCat:
      byCat[cat] = []
    l = byCat[cat]
    if len(l) < numTasksPerCat:
      l.append(s)

  prunedTasks = []
  for cat, l in byCat.items():
    prunedTasks.extend(l)

  return prunedTasks
  
def run(tasksFile, serverHost, serverPort, meanQPS, numTasksPerCat, runTimeSec, savFile, out, handleCtrlC):

  recentLatencyMS = 0
  recentQueueTimeMS = 0
  
  out.write('Mean QPS %s\n' % meanQPS)

  f = open(tasksFile, 'rb')
  taskStrings = []
  while True:
    l = f.readline()
    if l == '':
      break
    orig = l
    idx = l.find('#')
    if idx != -1:
      l = l[:idx]
    l = l.strip()
    if l == '':
      continue
    s = l
    if len(s) > MAX_BYTES:
      raise RuntimeError('task is > %d bytes: %s' % (MAX_BYTES, l))
    s = s + ((MAX_BYTES-len(s))*' ')
    taskStrings.append(s)

  r = random.Random(0)
  r.shuffle(taskStrings)
  out.write('%d tasks\n' % len(taskStrings))

  taskStrings = pruneTasks(taskStrings, numTasksPerCat)
  out.write('%d tasks after prune\n' % len(taskStrings))
  
  # Shuffle again (pruneTasks collates):
  r.shuffle(taskStrings)

  tasks = SendTasks(serverHost, serverPort, out, runTimeSec, savFile)

  targetTime = tasks.startTime

  if meanQPS == 'sweep':
    doSweep = True
    print 'Sweep: start at %s QPS' % SWEEP_START_QPS
    meanQPS = SWEEP_START_QPS
    lastSweepCheck = time.time()
  else:
    doSweep = False
    
  try:

    warned = False
    iters = 0
    
    while True:

      iters += 1

      for task in taskStrings:

        targetTime += r.expovariate(meanQPS)

        pause = targetTime - time.time()

        if pause > 0:
          #print 'sent %s; sleep %.3f sec' % (origTask, pause)
          time.sleep(pause)
          warned = False
          startTime = time.time()
        else:
          # Pretend query was issued back when we wanted it to be;
          # this way a system-wide hang is still "counted":
          startTime = targetTime
          if not warned and pause < -.005:
            out.write('WARNING: hiccup %.1f msec\n' % (-1000*pause))
            warned = True
          
        #origTask = task
        tasks.send(startTime, task)

      t = time.time()

      if doSweep:
        if t - lastSweepCheck > SWEEP_CHECK_EVERY_SEC:
          if meanQPS == SWEEP_START_QPS and len(tasks.sent) > 4:
            print 'Sweep: stay @ %s QPS for warmup...' % SWEEP_START_QPS
          elif len(tasks.sent) < 10000:
            # Still not saturated
            meanQPS *= 2
            print 'Sweep: set target to %.1f QPS' % meanQPS
          else:
            break
          lastSweepCheck = t
      elif t - tasks.startTime > runTimeSec:
        break

    print 'Sent all tasks %d times.' % iters

  except KeyboardInterrupt:
    if not handleCtrlC:
      raise
    # Ctrl-c to stop the test
    print
    print 'Ctrl+C: stopping now...'
    print
  
  out.write('%8.1f sec: Done sending tasks...\n' % (time.time()-tasks.startTime))
  out.flush()
  try:
    while len(tasks.sent) != 0:
      time.sleep(0.1)
  except KeyboardInterrupt:
    if not handleCtrlC:
      raise
    pass

  out.write('%8.1f sec: Done...\n' % (time.time()-tasks.startTime))
  out.flush()

  tasks.results.finish()

  # printResults(results)

def printResults(results):
  for startTime, taskString, latencyMS, queueTimeMS in results:
    print '%8.3f sec: latency %8.1f msec; queue msec %.1f; task %s' % (startTime, latencyMS, queueTimeMS, taskString)

if __name__ == '__main__':
  tasksFile = sys.argv[1]
  serverHost = sys.argv[2]
  serverPort = int(sys.argv[3])
  s = sys.argv[4]
  if s == 'sweep':
    meanQPS = s
  else:
    meanQPS = float(s)
  numTasksPerCat = int(sys.argv[5])
  runTimeSec = float(sys.argv[6])
  savFile = sys.argv[7]
  
  run(tasksFile, serverHost, serverPort, meanQPS, numTasksPerCat, runTimeSec, savFile, sys.stdout, True)

  
