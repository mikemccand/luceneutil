import datetime
import os
import random
import time
import urllib.request

# TODO
#   - test 4x too
#   - email on failure

LOG_FILE = "/l/logs/testLuceneFSync.log.txt"

# INDEX_PATH = '/l/indices/trunk'
INDEX_PATH = "/home/l/indices/trunk"

INSTEON_ID = "06.21.88"


def log(message):
  if message.startswith("\n"):
    message = "\n%s: %s" % (datetime.datetime.now(), message[1:])
  else:
    message = "%s: %s" % (datetime.datetime.now(), message)
  open(LOG_FILE, "ab").write((message + "\n").encode("utf-8"))
  print(message)


def run(command, wd, bg=False):
  log("RUN: %s, wd=%s" % (command, wd))
  command = 'ssh joel@10.17.4.6 "cd %s; %s" >> %s 2>&1' % (wd, command, LOG_FILE)
  if bg:
    command += " &"
  if os.system(command):
    raise RuntimeError("FAILED: %s" % command)


first = True

while True:
  log("\ncycle")
  try:
    run("svn up", "/l/trunk")
  except:
    log("WARNING: svn up failed; ignoring")

  run("ant clean jar >& compile.log", "/l/trunk/lucene")

  if not first:
    log("check index")
    cmd = "java -ea -classpath build/core/classes/java:build/codecs/classes/java org.apache.lucene.index.CheckIndex %s/index" % INDEX_PATH
    run(cmd, "/l/trunk/lucene")

  log("start indexing")
  indexCmd = (
    'java -Xms2g -Xmx2g -classpath "build/core/classes/java:build/core/classes/test:build/sandbox/classes/java:build/misc/classes/java:build/facet/classes/java:/home/mike/src/lucene-c-boost/dist/luceneCBoost-SNAPSHOT.jar:build/analysis/common/classes/java:build/analysis/icu/classes/java:build/queryparser/classes/java:build/grouping/classes/java:build/suggest/classes/java:build/highlighter/classes/java:build/codecs/classes/java:build/queries/classes/java:/l/util/lib/HdrHistogram.jar:/l/util/build" perf.Indexer -dirImpl MMapDirectory -indexPath "%s" -analyzer StandardAnalyzerNoStopWords -lineDocsFile /l/data/enwiki-20110115-lines-1k-fixed.txt -docCountLimit 27625038 -threadCount 2 -maxConcurrentMerges 3 -randomCommit -verbose -postingsFormat Lucene41 -mergePolicy TieredMergePolicy -idFieldPostingsFormat Memory'
    % INDEX_PATH
  )

  if first:
    first = False
  else:
    indexCmd += " -update"

  if random.randint(0, 1) == 0:
    indexCmd += " -cfs"

  if random.randint(0, 1) == 0:
    indexCmd += " -store"

  if random.randint(0, 1) == 0:
    indexCmd += " -tvs"

  if random.randint(0, 1) == 0:
    indexCmd += " -bodyPostingsOffsets"

  x = random.randint(0, 3)
  if x == 0:
    dirImpl = "MMapDirectory"
  elif x == 1:
    dirImpl = "NIOFSDirectory"
  elif x == 2:
    dirImpl = "SimpleFSDirectory"
  else:
    dirImpl = None

  if dirImpl is not None:
    indexCmd += " -dirImpl %s" % dirImpl

  if random.randint(0, 1) == 1:
    indexCmd += " -ramBufferMB -1 -maxBufferedDocs %s" % random.randint(1000, 30000)
  else:
    indexCmd += " -maxBufferedDocs -1 -ramBufferMB %s" % random.randint(10, 200)

  run(indexCmd, "/l/trunk/lucene", bg=True)

  sleepTimeSec = 100 + random.randint(0, 200)
  log("wait for %s sec" % sleepTimeSec)
  time.sleep(sleepTimeSec)

  log("cut power")
  while True:
    u = urllib.request.urlopen("http://10.17.4.73:8000/commands?device=%s&command=Turn+Off" % INSTEON_ID)
    data = u.read().decode("utf-8")
    if data.find("SUCCESS") == -1:
      log("FAILED; will retry:\n%s" % data)
      time.sleep(2.0)
    else:
      log("got: %s" % data)
      break

  time.sleep(5.0)

  if not os.system("ssh joel@10.17.4.6 ls"):
    raise RuntimeError("machine is not shutdown")

  log("restore power")
  while True:
    u = urllib.request.urlopen("http://10.17.4.73:8000/commands?device=%s&command=Turn+On" % INSTEON_ID)
    if data.find("SUCCESS") == -1:
      log("FAILED; will retry:\n%s" % data)
      time.sleep(2.0)
    else:
      log("got: %s" % data)
      break

  # Wait for maching to come back:
  log("wait for restart")
  while True:
    log("  check")
    if os.system("ssh joel@10.17.4.6 ls"):
      time.sleep(5.0)
    else:
      break
