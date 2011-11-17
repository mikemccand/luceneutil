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
  del s

  return '%s/%s' % (constants.BASE_DIR, checkout)
  
def getLuceneTestClassPath(ROOT):
  CP = []
  CP.append(ROOT+'/lucene/build/classes/test-framework')
  CP.append(ROOT+'/lucene/build/classes/test')
  CP.append(ROOT+'/lucene/build/classes/java')

  if not os.path.exists(ROOT + '/lucene/lib/junit-3.8.2.jar'):
    JUNIT_JAR = 'junit-4.7.jar'
  else:
    JUNIT_JAR = 'junit-3.8.2.jar'
  CP.append(ROOT + '/lucene/lib/' + JUNIT_JAR)
  if os.path.exists(ROOT + '/lucene/build/classes/demo'):
    CP.append(ROOT + '/lucene/build/classes/demo')

  CP.append(ROOT + '/lucene/build/contrib/queries/classes/java')
  CP.append(ROOT + '/lucene/build/contrib/queries/classes/test')
  CP.append(ROOT + '/lucene/build/contrib/misc/classes/java')
  CP.append(ROOT + '/lucene/build/contrib/misc/classes/test')
  CP.append(ROOT + '/modules/grouping/build/classes/test')
  CP.append(ROOT + '/modules/grouping/build/classes/java')
  CP.append(ROOT + '/modules/analysis/build/common/classes/java')
  CP.append(ROOT + '/modules/analysis/build/common/classes/test')
  CP.append(ROOT + '/modules/join/build/classes/test')
  CP.append(ROOT + '/modules/join/build/classes/java')
  CP.append(ROOT + '/modules/facet/build/classes/test')
  CP.append(ROOT + '/modules/facet/build/classes/java')

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

