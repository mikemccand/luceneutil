import subprocess
import shutil
import os
import constants
import nightlyBench
import sys

# Simple tool that attempts to regold the last (partially failed) nightly benchy run.  It can
# only succeed if the prior run nearly succeeded, i.e. ran all 20 JVM iterations, got results,
# but then failed in the comparison to the prior nightly build. The tool tries to detect this
# "near success" and fails/refuses to regold otherwise.

def main():

  p = subprocess.run(f'ls -ltrd {constants.LOGS_DIR}/????.??.??.??.??.??', check=True, capture_output=True, shell=True)
  last_dir_name = p.stdout.splitlines()[-1].decode('utf-8').split()[-1]

  if os.path.exists(f'{last_dir_name}/logs.tar.bz2'):
    raise RuntimeError(f'last run {last_dir_name} succeeded?')

  print(f'will regold from last run: {last_dir_name}')

  fixed_index_log = f'{last_dir_name}/fixedIndex.log'
  if not os.path.exists(fixed_index_log):
    raise RuntimeError(f'{fixed_index_log} is missing; cannot regold')

  fixed_lucene_index_path = None
  
  with open(fixed_index_log, 'r', encoding='utf-8') as f:
    while True:
      line = f.readline()
      if line == '':
        break
      if line.startswith('Index path: '):
        fixed_lucene_index_path = line[11:].strip()
        break
    if fixed_lucene_index_path is None:
      raise RuntimeError(f'could not find index path in {fixed_index_log}; cannot regold')

  if not os.path.exists(fixed_lucene_index_path):
    raise RuntimeError(f'fixed index {fixed_lucene_index_path} does not exist; cannot regold')

  index_path_prev = f'{constants.INDEX_DIR_BASE}/trunk.nightly.index.prev'
  if not os.path.exists(index_path_prev):
    # paranoia
    raise RuntimeError(f'previous index {index_path_prev} does not exist; cannot regold')

  for i in range(nightlyBench.JVM_COUNT):
    file_name = f'{constants.LOGS_DIR}/nightly.nightly.{i}'
    if not os.path.exists(file_name):
      raise RuntimeError(f'result file {file_name} is missing; cannot regold')

  if '-real' not in sys.argv:
    print('\nregold is possible; please re-run with -real to actually regold\n')
  else:

    # rename fixed index to prev index:
    shutil.rmtree(index_path_prev)
    shutil.move(fixed_lucene_index_path, index_path_prev)

    # rename the 20 JVM iter results files to .prev
    for i in range(nightlyBench.JVM_COUNT):
      file_name = f'{constants.LOGS_DIR}/nightly.nightly.{i}'
      shutil.move(file_name, f'{file_name}.prev')

if __name__ == '__main__':
  main()
