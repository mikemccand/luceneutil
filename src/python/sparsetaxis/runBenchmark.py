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

def downloadDocumentsSource(dataDir):
  fileName = '50M.mergedSubset.csv.blocks'
  destFileName = '%s/%s' % (dataDir, fileName)
  if not os.path.exists(destFileName):
    try:
      with open(destFileName, 'wb') as fOut, urllib.request.urlopen('http://home.apache.org/~mikemccand/%s' % fileName) as fIn:
        while True:
          data = fIn.read(16384)
          if len(data) == 0:
            break
          fOut.write(data)
    except:
      print('Failed; removing partial file')
      if os.path.exists(destFileName):
        os.remove(destFileName)
      raise

def run(command):
  print('  run: "%s" in cwd=%s' % (command, os.getcwd()))
  if os.system(command) != 0:
    raise RuntimeError('command "%s" failed' % command)

reIndexingRate = re.compile('([.0-9]+) sec: (\d+) docs; ([.0-9]+) docs/sec; ([.0-9]+) MB/sec')
def runIndexing(command, logFile):
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
          print('  %.1f sec: %.1f M docs; %.1f K docs/sec' % \
                (float(m.group(1)), int(m.group(2))/1000000.0, float(m.group(3))/1000.0))
          lastPrint = now
    p.poll()
    if p.returncode != 0:
      print(open(logFile, 'r', encoding='utf-8').read())
      raise RuntimeError('indexing failed: %s' % p.returncode)
  print('  %.1f sec: %.1f M docs; %.1f K docs/sec' % \
        (float(lastMatch.group(1)), int(lastMatch.group(2))/1000000.0, float(lastMatch.group(3))/1000.0))
  return float(lastMatch.group(3))/1000.0
        

def indexStats(indexPath, indexLog):
  sizeInBytes = int(os.popen('du -s %s' % indexPath).read().split(' ')[0])

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

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('-rootDir', help='directory where documents source files and indices are stored', default='.')
  parser.add_argument('-luceneMaster', help='directory where lucene\'s master is cloned', default='./lucene.master')
  args = parser.parse_args()
  args.rootDir = os.path.abspath(args.rootDir)

  for subDir in ('sparseTaxisData', 'sparseTaxisLogs', 'sparseTaxisIndices'):
    if not os.path.exists('%s/%s' % (args.rootDir, subDir)):
      os.makedirs('%s/%s' % (args.rootDir, subDir))

  downloadDocumentsSource('%s/sparseTaxisData' % args.rootDir)

  cwd = os.getcwd()
  os.chdir('%s/lucene/core' % args.luceneMaster)
  print('\nCompile lucene core...')
  run('ant clean jar > %s/sparseTaxisLogs/antclean.jar.log 2>&1' % args.rootDir)
  os.chdir(cwd)

  luceneUtilPythonPath = os.path.split(os.path.abspath(__file__))[0]

  print('\nCompile IndexTaxis.java...')
  run('javac -cp %s/lucene/build/core/lucene-core-7.0.0-SNAPSHOT.jar %s/../../main/perf/IndexTaxis.java' % (args.luceneMaster, luceneUtilPythonPath))

  cpuCount = int(3*multiprocessing.cpu_count()/4.)
  print('\nIndex nonsparse with %d threads...' % cpuCount)
  warm('%s/sparseTaxisData/50M.mergedSubset.csv.blocks' % args.rootDir)
  kdps = runIndexing('java -Xmx2g -Xms2g -cp %s/lucene/build/core/lucene-core-7.0.0-SNAPSHOT.jar:%s/../../main perf.IndexTaxis %s/sparseTaxisIndices/index.%dthreads.nonsparse %d %s/sparseTaxisData/50M.mergedSubset.csv.blocks nonsparse' % (args.luceneMaster, luceneUtilPythonPath, args.rootDir, cpuCount, cpuCount, args.rootDir),
                    '%s/sparseTaxisLogs/%dthreads.nonsparse.log' % (args.rootDir, cpuCount))

  print('\nIndex nonsparse with 1 thread...')
  warm('%s/sparseTaxisData/50M.mergedSubset.csv.blocks' % args.rootDir)
  kdps = runIndexing('java -Xmx2g -Xms2g -cp %s/lucene/build/core/lucene-core-7.0.0-SNAPSHOT.jar:%s/../../main perf.IndexTaxis %s/sparseTaxisIndices/index.%dthreads.nonsparse %d %s/sparseTaxisData/50M.mergedSubset.csv.blocks nonsparse' % (args.luceneMaster, luceneUtilPythonPath, args.rootDir, 1, 1, args.rootDir),
                    '%s/sparseTaxisLogs/%dthreads.nonsparse.log' % (args.rootDir, 1))

  print('\nIndex sparse with 1 thread...')
  warm('%s/sparseTaxisData/50M.mergedSubset.csv.blocks' % args.rootDir)
  kdps = runIndexing('java -Xmx2g -Xms2g -cp %s/lucene/build/core/lucene-core-7.0.0-SNAPSHOT.jar:%s/../../main perf.IndexTaxis %s/sparseTaxisIndices/index.%dthreads.sparse %d %s/sparseTaxisData/50M.mergedSubset.csv.blocks sparse' % (args.luceneMaster, luceneUtilPythonPath, args.rootDir, 1, 1, args.rootDir),
                     '%s/sparseTaxisLogs/%dthreads.sparse.log' % (args.rootDir, 1))
  
if __name__ == '__main__':
  main()
  
