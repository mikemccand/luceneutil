import glob
import os
import pickle
import re
import subprocess
import sys
import time

import localconstants


def main():
  if len(sys.argv) != 6:
    print("\nUsage: python3 src/python/runOrdinalMapBenchmark.py /path/to/lucene/main /path/to/geonames.txt /path/to/index /path/to/nightly/logs/timestamp/dir doc_limit\n")
    sys.exit(1)

  lucene_dir = sys.argv[1]
  geonames_csv_in = sys.argv[2]
  index_dir = sys.argv[3]
  nightly_log_dir = sys.argv[4]
  doc_limit = int(sys.argv[5])

  run_benchmark(lucene_dir, geonames_csv_in, index_dir, nightly_log_dir, doc_limit)


def run_benchmark(lucene_dir, geonames_csv_in, index_dir, nightly_log_dir, doc_limit):
  lucene_core_jar = glob.glob(f"{lucene_dir}/lucene/core/build/libs/lucene-core-*.jar")
  if len(lucene_core_jar) == 0:
    raise RuntimeError(f"please build Lucene core JAR first:\n  cd {lucene_dir}/lucene/core\n  ../../gradlew build")
  if len(lucene_core_jar) != 1:
    raise RuntimeError("WTF?")
  lucene_core_jar = lucene_core_jar[0]

  # compile
  cmd = f"javac -cp {lucene_core_jar} -d build src/extra/perf/OrdinalMapBenchmark.java"
  print(f"RUN: {cmd}, cwd={os.getcwd()}")
  subprocess.check_call(cmd, shell=True)

  all_results = []

  start_time_sec = time.time()
  print(f"Now run with doc_limit={doc_limit}:")

  cmd = f"{localconstants.JAVA_EXE} -cp {lucene_core_jar}:build perf.OrdinalMapBenchmark {geonames_csv_in} {localconstants.INDEX_DIR_BASE}/geonames-ordinal-map {doc_limit}"
  print(f'cmd ="{cmd}"')
  results = subprocess.run(cmd, shell=True, capture_output=True, check=False)
  stdout = results.stdout.decode("utf-8")
  stderr = results.stderr.decode("utf-8")

  open(f"{nightly_log_dir}/geoname-ordinal-map-benchmark.stdout.txt", "w").write(stdout)
  open(f"{nightly_log_dir}/geoname-ordinal-map-benchmark.sterr.txt", "w").write(stdout)

  if results.returncode != 0:
    print(f"stdout: {stdout}\n")
    print(f"stderr: {stderr}\n")
    raise RuntimeError(f"failed errorcode={results.returncode}!")

  matches = re.findall("^(.*?): (.*?) msec$", stdout, re.MULTILINE)
  if len(matches) == 0:
    raise RuntimeError(f"could not find any runtimes in output; see {nightly_log_dir}/geonames-stored-fields-benchmark.std{{out,err}}.txt")

  for name, msec in matches:
    all_results.append((name, float(msec)))
    print(f"  {name}: {msec} msec")

  results_file_name = f"{nightly_log_dir}/geonames-ordinal-map-benchmark-results.pk"
  open(results_file_name, "wb").write(pickle.dumps(all_results))
  end_time_sec = time.time()
  print(f"Done: took {(end_time_sec - start_time_sec):.2f} seconds, saved to {results_file_name}")


if __name__ == "__main__":
  main()
