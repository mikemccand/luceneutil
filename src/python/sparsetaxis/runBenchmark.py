import pickle
import time
import re
import multiprocessing
import subprocess
import os
import argparse
import urllib.request

# TODO
#  - back test
#  - nightly
#    - git pull lucene, luceneutil
#    - save nightly logs away
#    - save git commit hash

luceneUtilPythonPath = os.path.split(os.path.abspath(__file__))[0]

def toGB(x):
  return x/1024./1024./1024.

def downloadDocumentsSource(dataDir):
  fileName = '50M.subset.nyctaxis.csv.blocks'
  destFileName = '%s/%s' % (dataDir, fileName)
  if not os.path.exists(destFileName):
    urlSource = 'http://home.apache.org/~mikemccand/%s.bz2' % fileName
    print('  download %s -> %s.bz2...' % (urlSource, destFileName))
    try:
      with open(destFileName + '.bz2', 'wb') as fOut, urllib.request.urlopen(urlSource) as fIn:
        expectedSize = int(fIn.info()['Content-Length'])
        lastPrint = 0
        byteCount = 0
        while True:
          data = fIn.read(16384)
          if len(data) == 0:
            print('  %.2f GB of %.2f GB...done!' % (toGB(byteCount), toGB(expectedSize)))
            if byteCount != expectedSize:
              raise RuntimeError('got wrong byte count: actual %s vs expected %s' % (byteCount, expectedSize))
            break
          fOut.write(data)
          byteCount += len(data)
          now = time.time()
          if now - lastPrint > 3.0:
            print('  %.2f GB of %.2f GB...' % (toGB(byteCount), toGB(expectedSize)))
            lastPrint = now
      print('  uncompress...')
      run('bunzip2 %s.bz2' % destFileName)
      print('  done!  %.2f GB uncompressed' % (toGB(os.path.getsize(destFileName))))
    except:
      print('Failed; removing partial file')
      if os.path.exists(destFileName):
        os.remove(destFileName)
      if os.path.exists(destFileName + '.bz2'):
        os.remove(destFileName + '.bz2')
      raise

def run(command, logFile=None):
  print('  run: "%s" in cwd=%s' % (command, os.getcwd()))
  if logFile is not None:
    command += ' > %s 2>&1' % logFile
    print('    log: %s' % logFile)
  if os.system(command) != 0:
    if logFile is not None:
      print(open(logFile, 'r', encoding='utf-8').read())
    raise RuntimeError('command "%s" failed' % command)

reIndexingRate = re.compile('([.0-9]+) sec: (\d+) docs; ([.0-9]+) docs/sec; ([.0-9]+) MB/sec')
def runIndexing(args, cpuCount, sparseOrNot):

  docsSource = '%s/data/50M.subset.nyctaxis.csv.blocks' % rootDir

  indexPath = '%s/indices/index.%dthreads.%s' % (rootDir, cpuCount, sparseOrNot)
  if os.path.exists(indexPath):
    print('  remove old index at %s' % indexPath)
    shutil.rmtree(indexPath)

  command = 'java -Xmx2g -Xms2g -cp %s/lucene/build/core/lucene-core-7.0.0-SNAPSHOT.jar:%s/../../main perf.IndexTaxis %s %d %s %s' % \
            (args.luceneMaster, luceneUtilPythonPath, indexPath, cpuCount, docsSource, sparseOrNot)

  logFile = '%s/index.%dthreads.%s.log' % (logDir, cpuCount, sparseOrNot)

  warm(docsSource)
  
  print('  run: "%s" in cwd=%s' % (command, os.getcwd()))
  print('  log: %s' % logFile)
  with open(logFile, 'wb') as fOut:
    lastPrint = time.time()
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while True:
      line = p.stdout.readline()
      if line == b'':
        break
      fOut.write(line)
      fOut.flush()
      now = time.time()
      m = reIndexingRate.search(line.decode('utf-8'))
      if m is not None:
        lastMatch = m
        if now > lastPrint + 3.0:
          print('  %6.1f sec: %5.1f M docs; %5.1f K docs/sec' % \
                (float(m.group(1)), int(m.group(2))/1000000.0, float(m.group(3))/1000.0))
          lastPrint = now
    p.poll()
    if p.returncode != 0:
      print(open(logFile, 'r', encoding='utf-8').read())
      raise RuntimeError('indexing failed: %s' % p.returncode)
  print('  %6.1f sec: %5.1f M docs; %5.1f K docs/sec' % \
        (float(lastMatch.group(1)), int(lastMatch.group(2))/1000000.0, float(lastMatch.group(3))/1000.0))

  # indexPath, docs/sec, index size in bytes, merge times
  sizeOnDiskInBytes = int(os.popen('du -s %s' % indexPath).read().split()[0])*1024
  
  return indexPath, sizeOnDiskInBytes

def warm(fileName):
  print('  warm %s...' % fileName)
  numBytes = 0
  with open(fileName, 'rb') as f:
    while True:
      s = f.read(16384)
      if s == b'':
        break
      numBytes += len(s)
  print('  done warm %.1f GB' % (numBytes/1024./1024./1024.))

def makedirs(path):
  print('creating directories %s...' % path)
  os.makedirs(path)

def main():
  global rootDir, logDir
  
  parser = argparse.ArgumentParser()
  parser.add_argument('-rootDir', help='directory where documents source files and indices are stored', default='.')
  parser.add_argument('-luceneMaster', help='directory where lucene\'s master is cloned', default='./lucene.master')
  parser.add_argument('-logDir', help='where to write all indexing and searching logs')
  args = parser.parse_args()
  args.rootDir = os.path.abspath(args.rootDir)
  if not os.path.exists('%s/sparseTaxis' % args.rootDir):
    makedirs('%s/sparseTaxis' % args.rootDir)

  rootDir = '%s/sparseTaxis' % args.rootDir

  if args.logDir is not None:
    logDir = args.logDir
  else:
    logDir = '%s/logs' % rootDir

  for subDir in ('data', 'logs', 'indices'):
    if not os.path.exists('%s/%s' % (rootDir, subDir)):
      os.makedirs('%s/%s' % (rootDir, subDir))

  downloadDocumentsSource('%s/data' % rootDir)

  # nocommit
  if False:
    cwd = os.getcwd()
    if not os.path.exists(args.luceneMaster):
      raise RuntimeError('%s default lucene master clone does not exist; please specify -luceneMaster')
    os.chdir('%s/lucene/core' % args.luceneMaster)
    print('\nCompile lucene core...')
    run('ant clean jar', '%s/antclean.jar.log' % logDir)
    os.chdir(cwd)

  print('\nCompile java benchmark sources...')
  run('javac -cp %s/lucene/build/core/lucene-core-7.0.0-SNAPSHOT.jar %s/../../main/perf/IndexTaxis.java %s/../../main/perf/SearchTaxis.java' % \
      (args.luceneMaster, luceneUtilPythonPath, luceneUtilPythonPath))

  results = []

  # nocommit
  if False:
    cpuCount = int(3*multiprocessing.cpu_count()/4.)
    print('\nIndex "fast" nonsparse with %d threads...' % cpuCount)
    fastIndexPath, sizeOnDiskBytes = runIndexing(args, cpuCount, 'nonsparse')
    print('  index is %.2f GB' % toGB(sizeOnDiskBytes))

    print('\nIndex "slow" nonsparse with 1 thread, SMS...')
    nonSparseIndexPath, sizeOnDiskBytes = runIndexing(args, 1, 'nonsparse')
    print('  index is %.2f GB' % toGB(sizeOnDiskBytes))
    results.append(sizeOnDiskBytes)

    print('\nIndex "slow" sparse with 1 thread, SMS...')
    sparseIndexPath, sizeOnDiskBytes = runIndexing(args, 1, 'sparse')
    print('  index is %.2f GB' % toGB(sizeOnDiskBytes))
    results.append(sizeOnDiskBytes)

    print('\nCheckIndex nonsparse')
    t0 = time.time()
    run('java -Xmx2g -Xms2g -cp %s/lucene/build/core/lucene-core-7.0.0-SNAPSHOT.jar org.apache.lucene.index.CheckIndex %s' % \
        (args.luceneMaster, nonSparseIndexPath), '%s/checkIndexNonSparse.log' % logDir)
    sec = time.time() - t0
    print('  took %.1f sec' % sec)
    results.append(sec)

    print('\nCheckIndex sparse')
    t0 = time.time()
    run('java -Xmx2g -Xms2g -cp %s/lucene/build/core/lucene-core-7.0.0-SNAPSHOT.jar org.apache.lucene.index.CheckIndex %s' % \
        (args.luceneMaster, sparseIndexPath), '%s/checkIndexSparse.log' % logDir)
    sec = time.time() - t0
    print('  took %.1f sec' % sec)
    results.append(sec)

    print(results)
  else:
    nonSparseIndexPath = '/l/taxisroot/sparseTaxis/indices/index.1threads.nonsparse'
    sparseIndexPath = '/l/taxisroot/sparseTaxis/indices/index.1threads.sparse'

  print('\nSearch nonsparse...')
  run('java -Xmx2g -Xms2g -cp %s/lucene/build/core/lucene-core-7.0.0-SNAPSHOT.jar:%s/../../main perf.SearchTaxis %s nonsparse' % \
      (args.luceneMaster, luceneUtilPythonPath, nonSparseIndexPath), '%s/searchNonSparse.log' % logDir)

  print('\nSearch sparse...')
  run('java -Xmx2g -Xms2g -cp %s/lucene/build/core/lucene-core-7.0.0-SNAPSHOT.jar:%s/../../main perf.SearchTaxis %s sparse' % \
      (args.luceneMaster, luceneUtilPythonPath, sparseIndexPath), '%s/searchSparse.log' % logDir)

  open('%s/results.pk' % logDir, 'wb').write(pickle.dumps(results))
  
if __name__ == '__main__':
  main()
  
