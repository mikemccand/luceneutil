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
    idx = self.upto % len(self.buffer)
    self.sum += value - self.buffer[idx]
    self.buffer[idx] = value
    self.upto += 1

  def get(self):
    if self.upto == 0:
      return -1.0
    elif self.upto < len(self.buffer):
      return self.sum/self.upto
    else:
      return self.sum/len(self.buffer)

class Results:

  def __init__(self):
    self.buffers = []
    self.current = cStringIO.StringIO()

  def add(self, taskString, totalHitCount, timestamp, latencyMS, queueTimeMS):
    self.current.write(struct.pack('fffIB', timestamp, latencyMS, queueTimeMS, totalHitCount, len(taskString)))
    self.current.write(taskString)
    if self.current.tell() >= 256*1024:
      self.buffers.append(self.current.getvalue())
      self.current = cStringIO.StringIO()

  def finish(self, fileNameOut):
    self.buffers.append(self.current.getvalue())
    f = open(fileNameOut, 'wb')
    for buffer in self.buffers:
      f.write(buffer)
    f.close()
  
class SendTasks:

  def __init__(self, serverHost, serverPort, out, runTimeSec):
    self.startTime = time.time()

    self.out = out
    self.runTimeSec = runTimeSec
    self.queueTimeStats = RollingStats(100)
    self.totalTimeStats = RollingStats(100)
    self.results = Results()
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
    
    while True:
      result = ''
      while len(result) < 30:
        result = result + self.sock.recv(30 - len(result))
      taskID, totalHitCount, queueTimeMS = result.split(':')
      taskID = int(taskID)
      totalHitCount = int(totalHitCount)
      queueTimeMS = float(queueTimeMS)
      endTime = time.time()
      try:
        taskStartTime, taskString = self.sent[taskID]
      except KeyError:
        print 'WARNING: ignore bad return taskID=%s' % taskID
        continue
      del self.sent[taskID]
      latencyMS = (endTime-taskStartTime)*1000
      self.queueTimeStats.add(queueTimeMS)
      self.totalTimeStats.add(latencyMS)
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
        self.out.write('%6.1f s: %5.1f%%: %5.1f qps; %6.1f/%6.1f ms [%d, %d]\n' % \
                       (now - startTime, pctDone,
                        self.taskID/(now-startTime),
                        self.totalTimeStats.get(),
                        self.queueTimeStats.get(),
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
    # nocommit
    if cat != 'Term':
      continue
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

  tasks = SendTasks(serverHost, serverPort, out, runTimeSec)

  targetTime = tasks.startTime
  
  try:

    done = False
    warned = False
    
    while not done:

      for task in taskStrings:

        now = time.time()
        if now - tasks.startTime > runTimeSec:
          done = True
          break

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

  tasks.results.finish(savFile)

  # printResults(results)

def printResults(results):
  for startTime, taskString, latencyMS, queueTimeMS in results:
    print '%8.3f sec: latency %8.1f msec; queue msec %.1f; task %s' % (startTime, latencyMS, queueTimeMS, taskString)

if __name__ == '__main__':
  tasksFile = sys.argv[1]
  serverHost = sys.argv[2]
  serverPort = int(sys.argv[3])
  meanQPS = float(sys.argv[4])
  numTasksPerCat = int(sys.argv[5])
  runTimeSec = float(sys.argv[6])
  savFile = sys.argv[7]
  
  run(tasksFile, serverHost, serverPort, meanQPS, numTasksPerCat, runTimeSec, savFile, sys.stdout, True)

  
