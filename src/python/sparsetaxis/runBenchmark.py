import shutil
import pickle
import time
import re
import multiprocessing
import subprocess
import os
import argparse
import urllib.request

# TODO
#  - nightly
#    - git pull lucene, luceneutil
#    - save nightly logs away
#    - save git commit hash

# mount ramfs:

# sudo mkdir /mnt/ramdisk
# sudo mount -t ramfs -o size=1g ramfs /mnt/ramdisk
# sudo chmod a+rwX /mnt/ramdisk/

luceneUtilPythonPath = os.path.split(os.path.abspath(__file__))[0]

#JAVA_CMD = 'java -server -Xbatch -Xmx2g -Xms2g -XX:-TieredCompilation -XX:+HeapDumpOnOutOfMemoryError'
JAVA_CMD = 'java -server -Xmx2g -Xms2g -XX:+HeapDumpOnOutOfMemoryError'

def toGB(x):
  return x/1024./1024./1024.

def downloadDocumentsSource(dataDir):
  fileName = '20M.subset.nyctaxis.csv.blocks'
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
def runIndexing(args, cpuCount, sparseOrNot, sortOrNot):

  docsSource = '%s/data/20M.subset.nyctaxis.csv.blocks' % rootDir

  if sortOrNot:
    sortDesc = '.sorted'
  else:
    sortDesc = ''

  indexPath = '%s/indices/index.%dthreads.%s%s' % (rootDir, cpuCount, sparseOrNot, sortDesc)
  if os.path.exists(indexPath):
    print('  remove old index at %s' % indexPath)
    shutil.rmtree(indexPath)

  command = '%s -cp %s perf.IndexTaxis %s %d %s %s %s' % \
            (JAVA_CMD, classPath, indexPath, cpuCount, docsSource, sparseOrNot, sortOrNot)

  logFile = '%s/index.%dthreads.%s%s.log' % (logDir, cpuCount, sparseOrNot, sortDesc)

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
  global rootDir, logDir, classPath
  
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

  for subDir in ('data', 'indices'):
    if not os.path.exists('%s/%s' % (rootDir, subDir)):
      makedirs('%s/%s' % (rootDir, subDir))
  if not os.path.exists(logDir):
    makedirs(logDir)

  downloadDocumentsSource('%s/data' % rootDir)

  results = []

  if True:
    cwd = os.getcwd()
    if not os.path.exists(args.luceneMaster):
      raise RuntimeError('%s default lucene master clone does not exist; please specify -luceneMaster' % args.luceneMaster)
    os.chdir('%s/lucene/core' % args.luceneMaster)
    luceneRev = os.popen('git rev-parse HEAD').read().strip()
    print('Lucene git hash: %s' % luceneRev)
    results.append(luceneRev)
    print('\nCompile lucene core...')
    # nocommit
    #run('ant clean jar', '%s/antclean.jar.log' % logDir)
    run('ant jar', '%s/antclean.jar.log' % logDir)
    # nocommit
    if False:
      print('\nCompile lucene analysis/common...')
      os.chdir('%s/lucene/analysis/common' % args.luceneMaster)
      run('ant clean jar', '%s/antclean.jar.log' % logDir)

    os.chdir(cwd)

  print('\nCompile java benchmark sources...')

  classPath = '%s/lucene/build/core/lucene-core-7.0.0-SNAPSHOT.jar:%s/lucene/build/analysis/common/lucene-analyzers-common-7.0.0-SNAPSHOT.jar:%s/../../main' % \
              (args.luceneMaster, args.luceneMaster, luceneUtilPythonPath)

  run('javac -cp %s %s/../../main/perf/SearchTaxis.java %s/../../main/perf/IndexTaxis.java %s/../../main/perf/DiskUsage.java' % (classPath, luceneUtilPythonPath, luceneUtilPythonPath, luceneUtilPythonPath))

  for sparse, sort in (('sparse', True),
                       ('sparse', False),
                       ('nonsparse', False)):
    if sort:
      desc = '%s-sorted' % sparse
    else:
      desc = sparse

    print('\nIndex "slow" %s with 1 thread, SMS...' % desc)
    indexPath, sizeOnDiskBytes = runIndexing(args, 1, sparse, sort)
    print('  index is %.2f GB' % toGB(sizeOnDiskBytes))
    results.append(sizeOnDiskBytes)

    print('\nCheckIndex %s' % desc)
    t0 = time.time()
    run('%s -cp %s org.apache.lucene.index.CheckIndex %s' % \
        (JAVA_CMD, classPath, indexPath), '%s/checkIndex%s.log' % (logDir, desc))
    sec = time.time() - t0
    print('  took %.1f sec' % sec)
    results.append(sec)

    print('\nDiskUsage %s' % desc)
    t0 = time.time()
    if os.path.exists('/mnt/ramdisk'):
      extra = ' -Djava.io.tmpdir=/mnt/ramdisk'
    else:
      extra = ''
    run('%s -cp %s %s perf.DiskUsage %s' % (JAVA_CMD, classPath, extra, indexPath), '%s/diskUsage%s.log' % (logDir, desc))
    sec = time.time() - t0
    print('  took %.1f sec' % sec)
    results.append(sec)

    #for iter in range(5):
    for iter in range(1):
      print('\nSearch %s iter %s...' % (desc, iter))
      run('%s -cp %s perf.SearchTaxis %s %s' % \
          (JAVA_CMD, classPath, indexPath, sparse), '%s/search%s.%s.log' % (logDir, desc, iter))

  open('%s/results.pk' % logDir, 'wb').write(pickle.dumps(results))
  
if __name__ == '__main__':
  main()
  
