import sys
import glob
import os
import subprocess
import re
import pickle
import time

def main():
    if len(sys.argv) != 7:
        print('\nUsage: python3 src/python/runFacetsBenchmark.py /path/to/lucene/main /path/to/index /path/to/data /path/to/nightly/logs doc_limit num_iters\n')
        sys.exit(1)

    lucene_dir = sys.argv[1]
    index_dir = sys.argv[2]
    data_dir = sys.argv[3]
    nightly_log_dir = sys.argv[4]
    doc_limit = int(sys.argv[5])
    num_iters = int(sys.argv[6])

    run_benchmark(lucene_dir, index_dir, data_dir, nightly_log_dir, doc_limit, num_iters)

def run_benchmark(lucene_dir, index_dir, data_dir, nightly_log_dir, doc_limit, num_iters):

    start_time_sec = time.time()

    lucene_core_jar = glob.glob(f'{lucene_dir}/lucene/core/build/libs/lucene-core-*.jar')
    if len(lucene_core_jar) == 0:
        raise RuntimeError(f'please build Lucene core JAR first:\n  cd {lucene_dir}/lucene/core\n  ../../gradlew build')
    if len(lucene_core_jar) != 1:
        raise RuntimeError('WTF?')
    lucene_core_jar = lucene_core_jar[0]

    lucene_facet_jar = glob.glob(f'{lucene_dir}/lucene/facet/build/libs/lucene-facet-*.jar')
    if len(lucene_facet_jar) == 0:
        raise RuntimeError(f'please build Lucene facet JAR first:\n  cd {lucene_dir}/lucene/facet\n  ../../gradlew build')
    if len(lucene_facet_jar) != 1:
        raise RuntimeError('WTF?')
    lucene_facet_jar = lucene_facet_jar[0]

    # compile indexer
    index_compile_cmd = f'javac -cp {lucene_core_jar}:{lucene_facet_jar}:build -d build src/main/perf/facets/IndexFacets.java'
    print(f'RUN: {index_compile_cmd}, cwd={os.getcwd()}')
    subprocess.check_call(index_compile_cmd, shell=True)

    # compile searcher
    searcher_compile_cmd = f'javac -cp {lucene_core_jar}:{lucene_facet_jar}:build -d build src/main/perf/facets/BenchmarkFacets.java'
    print(f'RUN: {searcher_compile_cmd}, cwd={os.getcwd()}')
    subprocess.check_call(searcher_compile_cmd, shell=True)

    # # run indexing
    index_cmd = f'java -cp {lucene_core_jar}:{lucene_facet_jar}:build perf.facets.IndexFacets {data_dir}/NAD_taxonomy.txt.gz {index_dir} {doc_limit}'
    print(f'RUN: {index_cmd}, cwd={os.getcwd()}')
    print('Indexing documents...\n')
    subprocess.check_call(index_cmd, shell=True)

    hppc_path = None
    for root_path, dirs, files in os.walk(os.path.expanduser('~/.gradle/caches/modules-2/files-2.1/com.carrotsearch/hppc')):
        for file in files:
            if file == 'hppc-0.9.0.jar':
                hppc_path = f'{root_path}/{file}'

    if hppc_path is None:
        raise RuntimeError('unable to locate hppc-0.9.0-jar dependency for lucene/facet!')

    # run benchmark
    benchmark_cmd = f'java -cp {lucene_core_jar}:{lucene_facet_jar}:{hppc_path}:build perf.facets.BenchmarkFacets {index_dir} {num_iters}'
    print(f'RUN: {benchmark_cmd}, cwd={os.getcwd()}')
    results = subprocess.run(benchmark_cmd, shell=True, capture_output=True)

    stdout = results.stdout.decode('utf-8')
    stderr = results.stderr.decode('utf-8')

    open(f'{nightly_log_dir}/nad-facet-benchmark.stdout.txt', 'w').write(stdout)
    open(f'{nightly_log_dir}/nad-facet-benchmark.sterr.txt', 'w').write(stdout)

    if results.returncode != 0:
        print(f'stdout: {stdout}\n')
        print(f'stderr: {stderr}\n')
        raise RuntimeError(f'failed errorcode={results.returncode}!')

    find_arr = re.findall('Time \(ms\) taken to instanciate FastTaxonomyFacetCounts: (.*\d)', stdout)
    if len(find_arr) == 0:
        raise RuntimeError(f'could not find FastTaxonomyFacetCounts instanciation time in output; see {nightly_log_dir}/nad-facet-benchmark.std{{out,err}}.txt')
    total_time_create_fast_taxo_counts = float(find_arr[-1])

    find_arr = re.findall('Time \(ms\) taken to instanciate SSDVFacetCounts: (.*\d)', stdout)
    if len(find_arr) == 0:
        raise RuntimeError(f'could not find SSDVFacetCounts instanciation time in output; see {nightly_log_dir}/nad-facet-benchmark.std{{out,err}}.txt')
    total_time_create_ssdv_facet_counts = float(find_arr[-1])

    find_arr = re.findall('Time \(ms\) taken to get all dims for taxonomy: (.*\d)', stdout)
    if len(find_arr) == 0:
        raise RuntimeError(f'could not find time taken to get all dims for taxonomy in output; see {nightly_log_dir}/nad-facet-benchmark.std{{out,err}}.txt')
    taxo_get_all_dims_time = float(find_arr[-1])

    find_arr = re.findall('Time \(ms\) taken to get all dims for SSDV: (.*\d)', stdout)
    if len(find_arr) == 0:
        raise RuntimeError(f'could not find time taken to get all dims for SSDV in output; see {nightly_log_dir}/nad-facet-benchmark.std{{out,err}}.txt')
    ssdv_get_all_dims_time = float(find_arr[-1])

    find_arr = re.findall('Time \(ms\) taken to get top dims for taxonomy: (.*\d)', stdout)
    if len(find_arr) == 0:
        raise RuntimeError(f'could not find time taken to get top dims for taxonomy in output; see {nightly_log_dir}/nad-facet-benchmark.std{{out,err}}.txt')
    taxo_get_top_dims_time = float(find_arr[-1])

    find_arr = re.findall('Time \(ms\) taken to get top dims for SSDV: (.*\d)', stdout)
    if len(find_arr) == 0:
        raise RuntimeError(f'could not find time taken to get top dims for SSDV in output; see {nightly_log_dir}/nad-facet-benchmark.std{{out,err}}.txt')
    ssdv_get_top_dims_time = float(find_arr[-1])

    find_arr = re.findall('Time \(ms\) taken to get top "address\.taxonomy\/TX" children for taxonomy: (.*\d)', stdout)
    if len(find_arr) == 0:
        raise RuntimeError(f'could not find time taken to top "address.taxonomy/TX" children in output; see {nightly_log_dir}/nad-facet-benchmark.std{{out,err}}.txt')
    taxo_get_children_time = float(find_arr[-1])

    find_arr = re.findall('Time \(ms\) taken to get top "address\.sortedset\/TX" children for SSDV: (.*\d)', stdout)
    if len(find_arr) == 0:
        raise RuntimeError(f'could not find time taken to top "address.sortedset/TX" children in output; see {nightly_log_dir}/nad-facet-benchmark.std{{out,err}}.txt')
    ssdv_get_children_time = float(find_arr[-1])

    find_arr = re.findall('Time \(ms\) taken to get all \"address.taxonomy/TX\" children for taxonomy: (.*\d)', stdout)
    if len(find_arr) == 0:
        raise RuntimeError(f'could not find time taken to top "address.taxonomy/TX" children in output; see {nightly_log_dir}/nad-facet-benchmark.std{{out,err}}.txt')
    taxo_get_all_children_time = float(find_arr[-1])

    find_arr = re.findall('Time \(ms\) taken to get all \"address.sortedset/TX\" children for SSDV: (.*\d)', stdout)
    if len(find_arr) == 0:
        raise RuntimeError(f'could not find time taken to top "address.sortedset/TX" children in output; see {nightly_log_dir}/nad-facet-benchmark.std{{out,err}}.txt')
    ssdv_get_all_children_time = float(find_arr[-1])

    results = [(total_time_create_fast_taxo_counts,
                total_time_create_ssdv_facet_counts,
                taxo_get_all_dims_time,
                ssdv_get_all_dims_time,
                taxo_get_top_dims_time,
                ssdv_get_top_dims_time,
                taxo_get_children_time,
                ssdv_get_children_time,
                taxo_get_all_children_time,
                ssdv_get_all_children_time)]

    print(f'Total time creating FastTaxonomyCounts: {total_time_create_fast_taxo_counts} ms\n'
          f'Total time creating SSDVFacetCounts: {total_time_create_ssdv_facet_counts} ms\n'
          f'Total time taken to get all dims for taxonomy: {taxo_get_all_dims_time} ms\n'
          f'Total time taken to get all dims for SSDV: {ssdv_get_all_dims_time} ms\n'
          f'Total time to get top dims for taxonomy: {taxo_get_top_dims_time} ms\n'
          f'Total time to get top dims for SSDV: {ssdv_get_top_dims_time} ms\n'
          f'Total time to get top "address.taxonomy/TX" children: {taxo_get_children_time} ms\n'
          f'Total time to get top "address.sortedset/TX" children: {ssdv_get_children_time} ms\n'
          f'Total time to get all "address.taxonomy/TX" children: {taxo_get_all_children_time} ms\n'
          f'Total time to get all "address.sortedset/TX" children: {ssdv_get_all_children_time} ms')

    results_file_name = f'{nightly_log_dir}/nad-facet-benchmark.pk'
    open(results_file_name, 'wb').write(pickle.dumps(results))

    end_time_sec = time.time()
    print(f'Done: took {(end_time_sec - start_time_sec):.2f} seconds, saved to {results_file_name}')

if __name__ == '__main__':
    main()