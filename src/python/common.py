import os
import sys

import constants

if sys.platform.lower().find("darwin") != -1:
  osName = "osx"
elif sys.platform.lower().find("cygwin") != -1:
  osName = "cygwin"
elif sys.platform.lower().find("win") != -1:
  osName = "windows"
elif sys.platform.lower().find("linux") != -1:
  osName = "linux"
else:
  osName = "unix"


def pathsep():
  if osName in ("windows", "cygwin"):
    return ";"
  else:
    return os.pathsep


allTests = {}


def locateTest(test):
  if not os.path.exists("src/test"):
    raise RuntimeError("no src/test in cwd")

  cwd = os.getcwd()
  tup = test.split(".")
  if len(tup) == 1:
    method = None
  else:
    test, method = tup
  if len(allTests) == 0:
    for root, dirs, files in os.walk("src/test"):
      for f in files:
        if f.endswith(".java") and (f.startswith("Test") or f.endswith("Test.java")):
          path = root[len("src/test/") :].replace(os.sep, ".")
          allTests[f[:-5]] = "%s.%s" % (path, f[:-5])
          # print '%s.%s' % (path, f[:-5])
  if test not in allTests:
    return None
  else:
    return allTests[test], method


def findRootDir(s):
  if not s.replace(os.sep, "/").startswith(constants.BASE_DIR):
    raise RuntimeError("directory is not under constants.BASE_DIR?")

  s = s[len(constants.BASE_DIR) :]
  if s.startswith(os.sep):
    s = s[1:]

  checkout = s.split(os.sep)[0]
  if checkout == "future":
    checkout = os.sep.join(s.split(os.sep)[:2])
  del s

  return "%s/%s" % (constants.BASE_DIR, checkout)


def jarOK(jar):
  return jar != "log4j-1.2.14.jar"


def addJARs(cp, path):
  if os.path.exists(path):
    for f in os.listdir(path):
      if f.endswith(".jar") and jarOK(f):
        cp.append("%s/%s" % (path, f))


def getLuceneMatchVersion(ROOT):
  fileName = "%s/lucene/version.properties" % ROOT
  if os.path.exists(fileName):
    for line in open(fileName).readlines():
      if line.startswith("version.base="):
        return line[13:].strip()
    raise RuntimeError("could not locate lucene version")
  else:
    return "4.10.4"


def getLuceneTestClassPath(ROOT):
  CP = []
  if os.path.exists(ROOT + "/lucene/build/core"):
    CP.append(ROOT + "/lucene/build/test-framework/classes/java")
    CP.append(ROOT + "/lucene/build/core/classes/test")
    CP.append(ROOT + "/lucene/build/core/classes/java")
  else:
    CP.append(ROOT + "/lucene/build/classes/test-framework")
    CP.append(ROOT + "/lucene/build/classes/test")
    CP.append(ROOT + "/lucene/build/classes/java")

  addJARs(CP, ROOT + "/lucene/test-framework/lib")
  if False:
    if not os.path.exists(ROOT + "/lucene/test-framework/lib/junit-3.8.2.jar"):
      if not os.path.exists(ROOT + "/lucene/test-framework/lib/junit-4.7.jar"):
        JUNIT_JAR = "junit-4.10.jar"
      else:
        JUNIT_JAR = "junit-4.7.jar"
    else:
      JUNIT_JAR = "junit-3.8.2.jar"
    CP.append(ROOT + "/lucene/test-framework/lib/" + JUNIT_JAR)

  if os.path.exists(ROOT + "/lucene/build/classes/demo"):
    CP.append(ROOT + "/lucene/build/classes/demo")

  CP.append("/home/mike/src/lucene-c-boost/dist/luceneCBoost-SNAPSHOT.jar")
  CP.append(ROOT + "/lucene/build/test-framework/classes/java")
  CP.append(ROOT + "/lucene/build/test-framework/classes/test")
  CP.append(ROOT + "/lucene/build/queries/classes/java")
  CP.append(ROOT + "/lucene/build/queries/classes/test")
  CP.append(ROOT + "/lucene/build/spatial/classes/test")
  CP.append(ROOT + "/lucene/build/spatial/classes/java")
  CP.append(ROOT + "/lucene/build/spatial-extras/classes/test")
  CP.append(ROOT + "/lucene/build/spatial-extras/classes/java")
  CP.append(ROOT + "/lucene/build/spatial3d/classes/test")
  CP.append(ROOT + "/lucene/build/spatial3d/classes/java")
  CP.append(ROOT + "/lucene/spatial/src/test-files")
  addJARs(CP, ROOT + "/lucene/spatial/lib")
  CP.append(ROOT + "/lucene/build/server/classes/java")
  CP.append(ROOT + "/lucene/build/server/classes/test")
  addJARs(CP, ROOT + "/lucene/server/lib")
  CP.append(ROOT + "/lucene/build/classification/classes/java")
  CP.append(ROOT + "/lucene/build/classification/classes/test")
  CP.append(ROOT + "/lucene/build/expressions/classes/java")
  CP.append(ROOT + "/lucene/build/expressions/classes/test")
  addJARs(CP, ROOT + "/lucene/expressions/lib")
  CP.append(ROOT + "/lucene/build/replicator/classes/java")
  CP.append(ROOT + "/lucene/build/replicator/classes/test")
  addJARs(CP, ROOT + "/lucene/replicator/lib")
  CP.append(ROOT + "/lucene/build/demo/classes/java")
  CP.append(ROOT + "/lucene/build/demo/classes/test")
  CP.append(ROOT + "/lucene/build/codecs/classes/java")
  CP.append(ROOT + "/lucene/build/codecs/classes/test")
  CP.append(ROOT + "/lucene/build/backward-codecs/classes/java")
  CP.append(ROOT + "/lucene/build/backward-codecs/classes/test")
  CP.append(ROOT + "/lucene/build/benchmark/classes/java")
  CP.append(ROOT + "/lucene/build/benchmark/classes/test")
  addJARs(CP, ROOT + "/lucene/benchmark/lib")
  CP.append(ROOT + "/lucene/build/join/classes/java")
  CP.append(ROOT + "/lucene/build/join/classes/test")
  CP.append(ROOT + "/lucene/build/memory/classes/java")
  CP.append(ROOT + "/lucene/build/memory/classes/test")
  CP.append(ROOT + "/lucene/build/highlighter/classes/java")
  CP.append(ROOT + "/lucene/build/highlighter/classes/test")
  CP.append(ROOT + "/lucene/build/queryparser/classes/java")
  CP.append(ROOT + "/lucene/build/queryparser/classes/test")
  CP.append(ROOT + "/lucene/build/misc/classes/java")
  CP.append(ROOT + "/lucene/build/misc/classes/test")
  CP.append(ROOT + "/lucene/build/grouping/classes/test")
  CP.append(ROOT + "/lucene/build/grouping/classes/java")
  CP.append(ROOT + "/lucene/build/analysis/common/classes/java")
  CP.append(ROOT + "/lucene/build/analysis/common/classes/test")
  CP.append(ROOT + "/lucene/build/analysis/phonetic/classes/java")
  CP.append(ROOT + "/lucene/build/analysis/phonetic/classes/test")
  addJARs(CP, ROOT + "/lucene/analysis/phonetic/lib")
  CP.append(ROOT + "/lucene/build/analysis/icu/classes/java")
  CP.append(ROOT + "/lucene/build/analysis/icu/classes/test")
  CP.append(ROOT + "/lucene/analysis/icu/lib/icu4j-4_8_1_1.jar")
  CP.append(ROOT + "/lucene/build/analysis/kuromoji/classes/java")
  CP.append(ROOT + "/lucene/build/analysis/kuromoji/classes/test")
  CP.append(ROOT + "/lucene/build/join/classes/test")
  CP.append(ROOT + "/lucene/build/join/classes/java")
  CP.append(ROOT + "/lucene/build/facet/classes/test")
  CP.append(ROOT + "/lucene/build/facet/classes/java")
  CP.append(ROOT + "/lucene/build/facet/classes/examples")
  CP.append(ROOT + "/lucene/build/suggest/classes/test")
  CP.append(ROOT + "/lucene/build/suggest/classes/java")
  CP.append(ROOT + "/lucene/build/sandbox/classes/test")
  CP.append(ROOT + "/lucene/build/sandbox/classes/java")

  return CP


def filterCWD(l):
  l2 = []
  d = os.getcwd() + os.sep
  for e in l:
    if e.startswith(d):
      e = d[len(d) :]
    l2.append(e)
  return l2


def getLatestModTime(path, extension=None):
  """
  Returns latest modified time of all files with the specified extension under the specified path.
  """
  modTime = 0
  for root, dirs, files in os.walk(path):
    for file in files:
      if extension is None or file.endswith(extension):
        fullPath = "%s/%s" % (root, file)
        if os.path.isfile(fullPath):
          modTime = max(os.path.getmtime(fullPath), modTime)
  return modTime


def getLuceneDirFromGradleProperties():
  try:
    with open(os.path.join(constants.BENCH_BASE_DIR, "gradle.properties"), "r") as f:
      for line in f:
        if line.find("=") == -1:
          continue

        key, value = line.strip().split("=")
        if key == "external.lucene.repo":
          return value
  except FileNotFoundError as e:
    print("gradle.properties not found, try run ./gradlew :localSettings")
    raise e

  raise ValueError("Cannot find lucene repo, please define 'external.lucene.repo' in gradle.properties")
