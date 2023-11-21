import os
import bz2
import tarfile
import pickle
import multiprocessing

lock = multiprocessing.Lock()

def extract_one_file(tar_file_name):
  indexSizeFile = os.path.split(tar_file_name)[0] + '/fixed_index_bytes.pk'
  if not os.path.exists(indexSizeFile):
    try:
      with tarfile.open(fileobj=bz2.open(tar_file_name)) as t:
        if 'checkIndex.fixedIndex.log' in t.getnames():
          with t.extractfile('checkIndex.fixedIndex.log') as f:
            tot_mb = 0
            for line in f.readlines():
              line = line.strip().decode('utf-8')
              if line.startswith('size (MB)='):
                #print(f'  {line}')
                mb = float(line[10:].replace(',', ''))
                tot_mb += mb
            lock.acquire()
            try:
              print(f'\n{tar_file_name}: {tot_mb:.2f} MB')
            finally:
              lock.release()
            with open(indexSizeFile, 'wb') as out:
              pickle.dump(tot_mb, out)
    except:
      print(f'FAILED on {tar_file_name}')
      raise

if __name__ == '__main__':
  todo = []
  for dirName in sorted(os.listdir('/l/logs.nightly')):
    resultsFile = f'/l/logs.nightly/{dirName}/logs.tar.bz2'
    if os.path.exists(resultsFile):
      todo.append(resultsFile)

  with multiprocessing.Pool(64) as p:
    p.map(extract_one_file, todo)
