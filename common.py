import sys
import os
import constants

if sys.platform.lower().find('darwin') != -1:
  osName = 'osx'
elif sys.platform.lower().find('cygwin') != -1:
  osName = 'cygwin'
elif sys.platform.lower().find('win') != -1:
  osName = 'windows'
elif sys.platform.lower().find('linux') != -1:
  osName = 'linux'
else:
  osName = 'unix'

def pathsep():
  if osName in ('windows', 'cygwin'):
    return ';'
  else:
    return os.pathsep

allTests = {}

def locateTest(test):
  if not os.path.exists('src/test'):
    raise RuntimeError('no src/test in cwd')

  cwd = os.getcwd()
  tup = test.split('.')
  if len(tup) == 1:
    method = None
  else:
    test, method = tup
  if len(allTests) == 0:
    for root, dirs, files in os.walk('src/test'):
      for f in files:
        if f.endswith('.java') and (f.startswith('Test') or f.endswith('Test.java')):
          path = root[len('src/test/'):].replace(os.sep, '.')
          allTests[f[:-5]] = '%s.%s' % (path, f[:-5])
          #print '%s.%s' % (path, f[:-5])
  if test not in allTests:
    return None
  else:
    return allTests[test], method

def findRootDir(s):

  if not s.startswith(constants.BASE_DIR):
    raise RuntimeError('directory is not under constants.BASE_DIR?')

  s = s[len(constants.BASE_DIR):]
  if s.startswith(os.sep):
    s = s[1:]

  checkout = s.split(os.sep)[0]
  if checkout == 'future':
    checkout = os.sep.join(s.split(os.sep)[:2])
  del s

  return '%s/%s' % (constants.BASE_DIR, checkout)
  
def jarOK(jar):
  return jar != 'log4j-1.2.14.jar'

def addJARs(cp, path):
  if os.path.exists(path):
    for f in os.listdir(path):
      if f.endswith('.jar') and jarOK(f):
        cp.append('%s/%s' % (path, f))

def getLuceneTestClassPath(ROOT):
  CP = []
  if os.path.exists(ROOT+'/lucene/build/core'):
    CP.append(ROOT+'/lucene/build/test-framework/classes/java')
    CP.append(ROOT+'/lucene/build/core/classes/test')
    CP.append(ROOT+'/lucene/build/core/classes/java')
  else:
    CP.append(ROOT+'/lucene/build/classes/test-framework')
    CP.append(ROOT+'/lucene/build/classes/test')
    CP.append(ROOT+'/lucene/build/classes/java')

  addJARs(CP, ROOT+'/lucene/test-framework/lib')
  if False:
    if not os.path.exists(ROOT + '/lucene/test-framework/lib/junit-3.8.2.jar'):
      if not os.path.exists(ROOT + '/lucene/test-framework/lib/junit-4.7.jar'):
        JUNIT_JAR = 'junit-4.10.jar'
      else:
        JUNIT_JAR = 'junit-4.7.jar'
    else:
      JUNIT_JAR = 'junit-3.8.2.jar'
    CP.append(ROOT + '/lucene/test-framework/lib/' + JUNIT_JAR)
    CP.append(ROOT + '/lucene/test-framework/lib/randomizedtesting-runner-1.4.0.jar')

  if os.path.exists(ROOT + '/lucene/build/classes/demo'):
    CP.append(ROOT + '/lucene/build/classes/demo')

  CP.append(ROOT + '/lucene/build/queries/classes/java')
  CP.append(ROOT + '/lucene/build/queries/classes/test')
  CP.append(ROOT + '/lucene/build/benchmark/classes/java')
  CP.append(ROOT + '/lucene/build/benchmark/classes/test')
  addJARs(CP, ROOT + '/lucene/benchmark/lib')
  CP.append(ROOT + '/lucene/build/join/classes/java')
  CP.append(ROOT + '/lucene/build/join/classes/test')
  CP.append(ROOT + '/lucene/build/memory/classes/java')
  CP.append(ROOT + '/lucene/build/memory/classes/test')
  CP.append(ROOT + '/lucene/build/misc/classes/java')
  CP.append(ROOT + '/lucene/build/misc/classes/test')
  CP.append(ROOT + '/lucene/build/grouping/classes/test')
  CP.append(ROOT + '/lucene/build/grouping/classes/java')
  CP.append(ROOT + '/lucene/build/analysis/common/classes/java')
  CP.append(ROOT + '/lucene/build/analysis/common/classes/test')
  CP.append(ROOT + '/lucene/build/analysis/icu/classes/java')
  CP.append(ROOT + '/lucene/build/analysis/icu/classes/test')
  CP.append(ROOT + '/lucene/analysis/icu/lib/icu4j-4_8_1_1.jar')
  CP.append(ROOT + '/lucene/build/analysis/kuromoji/classes/java')
  CP.append(ROOT + '/lucene/build/analysis/kuromoji/classes/test')
  CP.append(ROOT + '/lucene/build/join/classes/test')
  CP.append(ROOT + '/lucene/build/join/classes/java')
  CP.append(ROOT + '/lucene/build/facet/classes/test')
  CP.append(ROOT + '/lucene/build/facet/classes/java')
  CP.append(ROOT + '/lucene/build/suggest/classes/test')
  CP.append(ROOT + '/lucene/build/suggest/classes/java')

  # return filterCWD(CP)
  return CP

def filterCWD(l):
  l2 = []
  d = os.getcwd() + os.sep
  for e in l:
    if e.startswith(d):
      e = d[len(d):]
    l2.append(e)
  return l2

