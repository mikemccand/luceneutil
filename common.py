import sys
import os

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
    raise RuntimeError('cwd must be $ROOT/lucene of src checkout')

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

def getTestClassPath(ROOT):
  CP = []
  CP.append(ROOT+'build/classes/test')
  CP.append(ROOT+'build/classes/test-framework')
  CP.append(ROOT+'build/classes/java')

  if not os.path.exists('lib/junit-3.8.2.jar'):
    JUNIT_JAR = 'lib/junit-4.7.jar'
  else:
    JUNIT_JAR = 'lib/junit-3.8.2.jar'
  CP.append(ROOT + JUNIT_JAR)
  if os.path.exists(ROOT + 'build/classes/demo'):
    CP.append(ROOT + 'build/classes/demo')

  if ROOT != '':
    CP.append(ROOT + 'build/contrib/queries/classes/java')
    CP.append(ROOT + 'build/contrib/queries/classes/test')

  return CP

