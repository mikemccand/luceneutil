import glob
import localconstants
import time
import os
import pickle
import re
import subprocess
import sys

def main():
  if len(sys.argv) != 6:
    print('\nUsage: python3 src/python/runStoredFieldsBenchmark.py /path/to/lucene/main /path/to/geonames.txt /path/to/index /path/to/nightly/logs/timestamp/dir doc_limit\n')
    sys.exit(1)

  lucene_dir = sys.argv[1]
  geonames_csv_in = sys.argv[2]
  index_dir = sys.argv[3]
  nightly_log_dir = sys.argv[4]
  doc_limit = int(sys.argv[5])

  run_benchmark(lucene_dir, geonames_csv_in, index_dir, nightly_log_dir, doc_limit)

def run_benchmark(lucene_dir, geonames_csv_in, index_dir, nightly_log_dir, doc_limit):

  lucene_core_jar = glob.glob(f'{lucene_dir}/lucene/core/build/libs/lucene-core-*.jar')
  if len(lucene_core_jar) == 0:
    raise RuntimeError(f'please build Lucene core JAR first:\n  cd {lucene_dir}/lucene/core\n  ../../gradlew build')
  if len(lucene_core_jar) != 1:
    raise RuntimeError('WTF?')
  lucene_core_jar = lucene_core_jar[0]

  # compile
  cmd = f'javac -cp {lucene_core_jar} -d build src/main/perf/StoredFieldsBenchmark.java'
  print(f'RUN: {cmd}, cwd={os.getcwd()}')
  subprocess.check_call(cmd, shell=True)

  all_results = []

  # run in each mode, recording stats into results
  start_time_sec = time.time()
  for mode in 'BEST_SPEED', 'BEST_COMPRESSION':
    print(f'Now run {mode} with doc_limit={doc_limit}:')
    results = subprocess.run(f'{localconstants.JAVA_EXE} -cp {lucene_core_jar}:build perf.StoredFieldsBenchmark {geonames_csv_in} {localconstants.INDEX_DIR_BASE}/geonames-stored-fields {mode} {doc_limit}',
                             shell=True, capture_output=True)
    stdout = results.stdout.decode('utf-8')
    stderr = results.stderr.decode('utf-8')

    open(f'{nightly_log_dir}/geoname-stored-fields-benchmark-{mode}.stdout.txt', 'w').write(stdout)
    open(f'{nightly_log_dir}/geoname-stored-fields-benchmark-{mode}.sterr.txt', 'w').write(stdout)
    
    if results.returncode != 0:
      print(f'stdout: {stdout}\n')
      print(f'stderr: {stderr}\n')
      raise RuntimeError(f'failed errorcode={results.returncode}!')

    m = re.search('Indexing time: (.*?) msec', stdout)
    if m is None:
      raise RuntimeError(f'could not find "Indexing time" in output; see {nightly_log_dir}/geonames-stored-fields-benchmark.std{{out,err}}.txt')
    indexing_time_msec = float(m.group(1))

    m = re.search('Stored fields size: (.*?) MB', stdout)
    if m is None:
      raise RuntimeError(fr'could not find "Stored fields size" in output; see {nightly_log_dir}/geonames-stored-fields-benchmark.std{{out,err}}.txt')
    stored_field_size_mb = float(m.group(1))

    m = re.search('Retrieved time: (.*?) msec', stdout)
    if m is None:
      raise RuntimeError(fr'could not find "Retrieved time" in output; see {nightly_log_dir}/geonames-stored-fields-benchmark.std{{out,err}}.txt')
    retrieved_time_msec = float(m.group(1))

    all_results.append((mode, indexing_time_msec, stored_field_size_mb, retrieved_time_msec))
    print(f'Mode: {mode}\n  indexing_time_msec: {indexing_time_msec:.2f}\n  stored_field_size_mb: {stored_field_size_mb:.2f}\n  retrieved_time_msec: {retrieved_time_msec:.2f}')

  results_file_name = f'{nightly_log_dir}/geonames-stored-fields-benchmark-results.pk'
  open(results_file_name, 'wb').write(pickle.dumps(all_results))
  end_time_sec = time.time()
  print(f'Done: took {(end_time_sec - start_time_sec):.2f} seconds, saved to {results_file_name}')
  
if __name__ == '__main__':
  main()
