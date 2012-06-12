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

import random
import codecs
import socket
import Queue
import sys
import time
import threading
import cPickle
import gc

# We don't create cyclic garbage, and we want no hiccups:
gc.disable()

MAX_BYTES = 70

# TODO
#   - generalize this to send requests via http too
#   - run hiccup thread here?
#   - sweep to find capacity...
#   - test PK lookup, NRT as well

# python -u perf/sendTasks.py /l/util/wikimedium500.tasks localhost 7777 10 10 10

count = 0
count2 = 0
sumLatencyMS = 0
sumQueueTimeMS = 0

def gatherResponses(sent, s, results):
  global count, count2, sumLatencyMS, sumQueueTimeMS
  
  while True:
    result = ''
    while len(result) < 14:
      result = result + s.recv(14 - len(result))
    taskID, queueTimeMS = result.split(':')
    taskID = int(taskID)
    queueTimeMS = float(queueTimeMS)
    endTime = time.time()
    try:
      startTime, taskString = sent[taskID]
    except KeyError:
      print 'WARNING: ignore bad return taskID=%s' % taskID
      continue
    del sent[taskID]
    latencyMS = (endTime-startTime)*1000
    results.append((startTime-globalStartTime, taskString.strip(), latencyMS, queueTimeMS))
    count += 1
    count2 += 1
    sumLatencyMS += latencyMS
    sumQueueTimeMS += queueTimeMS
    #print '  recv %s' % taskString

def sendRequests(queue, s):
  while True:
    task = queue.get()
    startTime = time.time()
    while len(task) > 0:
      sent = s.send(task)
      if sent == 0:
        raise RuntimeError('failed to send task "%s"' % task)
      task = task[sent:]
    sendTime = time.time()
    #if sendTime - startTime > .001:
    #print 'WARNING: took %.1f msec to send request' % (1000*(sendTime - startTime))

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
  global globalStartTime
  global count, count2, sumLatencyMS, sumQueueTimeMS

  count = 0
  count2 = 0
  sumLatencyMS = 0
  sumQueueMS = 0
  
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
      raise RuntimeError('task is > 50 bytes: %s' % l)
    s = s + ((MAX_BYTES-len(s))*' ')
    taskStrings.append(s)

  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.connect((serverHost, serverPort))

  r = random.Random(0)
  r.shuffle(taskStrings)

  taskStrings = pruneTasks(taskStrings, numTasksPerCat)

  sent = {}
  results = []

  t = threading.Thread(target=gatherResponses,
                       args=(sent, s, results))
  t.setDaemon(True)
  t.start()

  queue = Queue.Queue()
  t = threading.Thread(target=sendRequests,
                       args=(queue, s))
  t.setDaemon(True)
  t.start()

  globalStartTime = time.time()
  lastPrint = globalStartTime
  startTime = None
  
  r.shuffle(taskStrings)

  try:

    taskID = 0

    done = False
    
    while not done:

      for task in taskStrings:
        #print task.strip()
        now = time.time()
        if now - globalStartTime > runTimeSec:
          done = True
          break

        if now - lastPrint > 2.0 and count > 0:
          pctDone = 100.0*((time.time() - globalStartTime) / runTimeSec)
          if pctDone > 100.0:
            pctDone = 100.0

          if count2 == 0:
            s = 'n/a'
          else:
            s = '%5.2f' % (sumQueueTimeMS/count2)

          out.write('%.1f s: %5.1f%%: %.1f qps; %4.1f/%s ms\n' % \
                    (now - globalStartTime, pctDone, count/(now-globalStartTime), sumLatencyMS/count, s))
          out.flush()
          sumQueueTimeMS = 0
          count2 = 0
          lastPrint = now

        pause = r.expovariate(meanQPS)

        if startTime is not None:
          # Correct for any time taken in our "overhead" here...:
          pause = (startTime + pause) - time.time()

        # nocommit print warning if there's a hiccup here:
        
        if pause > 0:
          #print 'sent %s; sleep %.3f sec' % (origTask, pause)
          time.sleep(pause)
        elif pause < -.001:
          out.write('WARNING: hiccup %.1f msec\n' % (-1000*pause))

        #origTask = task
        startTime = time.time()
        sent[taskID] = (startTime, task)
        queue.put(task)
        taskID += 1

  except KeyboardInterrupt:
    if not handleCtrlC:
      raise
    # Ctrl-c to stop the test
    print
    print 'Ctrl+C: stopping now...'
    print
  
  out.write('%8.1f sec: Done sending tasks...\n' % (time.time()-globalStartTime))
  out.flush()
  try:
    while len(sent) != 0:
      time.sleep(0.1)
  except KeyboardInterrupt:
    if not handleCtrlC:
      raise
    pass

  out.write('%8.1f sec: Done...\n' % (time.time()-globalStartTime))
  out.flush()

  # Sort by query startTime:
  results.sort()

  open(savFile, 'wb').write(cPickle.dumps(results))

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
  runTimeSec = float(s[:-1])
  savFile = sys.argv[7]
  
  run(tasksFile, sererHost, serverPort, meanQPS, numTasksPerCat, runTimeSec, savFile, sys.stdout, True)
  
  open('out.html', 'wb').write(html)

  
