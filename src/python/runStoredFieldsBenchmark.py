import os
import glob
import sys
import subprocess
import localconstants
import pickle

def main():
  if len(sys.argv) != 4:
    print('\nUsage: python3 src/python/runStoredFieldsBenchmark.py /path/to/lucene/main /path/to/geonames.txt /path/to/index\n')
    sys.exit(1)

  lucene_dir = sys.argv[1]
  lucene_core_jar = glob.glob(f'{lucene_dir}/lucene/core/build/libs/lucene-core-*.jar')
  if len(lucene_core_jar) == 0:
    raise RuntimeError(f'please build Lucene core JAR first:\n  cd {lucene_dir}/lucene/core\n  ../../gradlew build')
  if len(lucene_core_jar) != 1:
    raise RuntimeError('WTF?')
  lucene_core_jar = lucene_core_jar[0]

  geonames_csv_in = sys.argv[2]
  index_dir = sys.argv[3]

  # compile
  cmd = f'javac -cp {lucene_core_jar} -d build src/main/perf/StoredFieldsBenchmark.java'
  print(f'RUN: {cmd}, cwd={os.getcwd()}')
  subprocess.check_call(cmd, shell=True)

  # run in each mode
  for mode in 'BEST_SPEED', 'BEST_COMPRESSION':
    print(f'Now run {mode}:')
    results = subprocess.run(f'{localconstants.JAVA_EXE} -cp {lucene_core_jar}:build perf.StoredFieldsBenchmark {geonames_csv_in} {localconstants.INDEX_DIR_BASE}/geonames-stored-fields {mode}',
                             shell=True, capture_output=True)
    print(f'{results.stdout}')
    print(f'{results.stderr}')
        
if __name__ == '__main__':
  main()
