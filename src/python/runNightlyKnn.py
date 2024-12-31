import os
import re
import sys
import time
import constants
import benchUtil
import subprocess
import pickle
import datetime
import shutil
import glob
from collections import namedtuple
import knnPerfTest

# TODO
#   - remove all cached indices / .bin files from nightly area
#     - hmm maybe don't remove the exact KNN?  takes a long time to recompute!
#   - test different distance metrics / vector sources?
#   - compile lucene too?
#   - test parent join too
#   - make nightly charts
#   - test filters, pre and post

# Cohere v2?  768 dims

#INDEX_VECTORS_FILE = constants.NIGHTLY_INDEX_VECTORS_FILE
#SEARCH_VECTORS_FILE = constants.NIGHTLY_SEARCH_VECTORS_FILE
#VECTORS_DIM = constants.NIGHTLY_VECTORS_DIM
#VECTORS_ENCODING = constants.NIGHTLY_VECTORS_ENCODING
#LUCENE_CHECKOUT = constants.LUCENE_CHECKOUT

# e.g. Cohere v2
INDEX_VECTORS_FILE = '/lucenedata/enwiki/cohere-wikipedia-docs-768d.vec'
SEARCH_VECTORS_FILE = '/lucenedata/enwiki/cohere-wikipedia-queries-768d.vec'
VECTORS_DIM = 768
VECTORS_ENCODING = 'float32'
LUCENE_CHECKOUT = '/l/trunk.nightly'
INDEX_THREAD_COUNT = 8

INDEX_NUM_VECTORS = 8_000_000
HNSW_MAX_CONN = 32
HNSW_INDEX_BEAM_WIDTH = 100
HNSW_SEARCH_FANOUT = 50
HNSW_TOP_K = 100

# set to False to quickly debug things:
REAL = True

if not REAL:
  INDEX_NUM_VECTORS //= 10

re_summary = re.compile(r'^SUMMARY: (.*?)$', re.MULTILINE)
re_graph_level = re.compile(r'^Graph level=(\d) size=(\d+), Fanout')
re_leaf_docs = re.compile(r'^Leaf (\d+) has (\d+) documents')
re_leaf_layers = re.compile(r'^Leaf (\d+) has (\d+) layers')
re_index_path = re.compile(r'^Index Path = (.*)$')

KNNResultV0 = namedtuple('KNNResult',
                         ['lucene_git_rev',
                          'luceneutil_git_rev',
                          'index_vectors_file',
                          'search_vectors_file',
                          'vector_dims',
                          'do_force_merge',
                          'recall',
                          'cpu_time_ms',
                          'num_docs',
                          'top_k',
                          'fanout',
                          'max_conn',
                          'beam_width',
                          'quantize_desc',
                          'total_visited',
                          'index_time_sec',
                          'index_docs_per_sec',
                          'force_merge_time_sec',
                          'index_num_segments',
                          'index_size_on_disk_mb',
                          'selectivity',
                          'pre_post_filter',
                          'vec_disk_mb',
                          'vec_ram_mb',
                          'graph_level_conn_p_values',
                          'combined_run_time'])

def get_git_revision(git_clone_path):
  # git is so weird, like this is intuitive?
  git_rev = subprocess.run(['git', '-C', git_clone_path, 'rev-parse', 'HEAD'],
                           check=True,
                           capture_output=True,
                           encoding='utf-8').stdout.strip()
  # i don't trust git:
  if len(git_rev) != 40:
    raise RuntimeError(f'failed to parse HEAD git hash for {git_clone_path}?  got {git_rev}, not 40 characters')

  return git_rev

def is_git_clone_dirty(git_clone_path):
  p = subprocess.run(['git', '-C', git_clone_path, 'diff', '--quiet'],
                     check=False,  # we will interpret return code ourselves
                     capture_output=True,
                     encoding='utf-8')
  return bool(p.returncode)

def main():
  all_results = run('.')
  if len(sys.argv) == 1:
    pass

def run(results_dir):
  try:
    return _run(results_dir)
  finally:
    # these get quite large (~78 GB for three indices):
    indicesDir = f'{constants.BENCH_BASE_DIR}/knnIndices'
    if os.path.exists(indicesDir):
      print(f'removing KNN indices dir {indicesDir}')
      shutil.rmtree(indicesDir)
    
def _run(results_dir):
  start_time_epoch_secs = time.time()
  lucene_git_rev = get_git_revision(LUCENE_CHECKOUT)
  lucene_clone_is_dirty = is_git_clone_dirty(LUCENE_CHECKOUT)
  print(f'Lucene HEAD git revision at {LUCENE_CHECKOUT}: {lucene_git_rev} dirty?={lucene_clone_is_dirty}')

  luceneutil_git_rev = get_git_revision(constants.BENCH_BASE_DIR)
  luceneutil_clone_is_dirty = is_git_clone_dirty(constants.BENCH_BASE_DIR)
  print(f'luceneutil HEAD git revision at {constants.BENCH_BASE_DIR}: {luceneutil_git_rev} dirty?={luceneutil_clone_is_dirty}')

  if REAL:
    if lucene_clone_is_dirty:
      raise RuntimeError(f'lucene clone {LUCENE_CHECKOUT} is git-dirty')
    if luceneutil_clone_is_dirty:
      raise RuntimeError(f'luceneutil clone {constants.BENCH_BASE_DIR} is git-dirty')

  # make sure nothing is shared from prior runs
  indicesDir = f'{constants.BENCH_BASE_DIR}/knnIndices'
  if os.path.exists(indicesDir):
    print(f'removing KNN indices dir {indicesDir}')
    shutil.rmtree(indicesDir)
    
  for cacheFileName in glob.glob(f'{constants.BENCH_BASE_DIR}/*.bin'):
    print(f'removing cached KNN results {cacheFileName}')
    os.remove(cacheFileName)
    
  print(f'compile Lucene jars at {LUCENE_CHECKOUT}')
  os.chdir(LUCENE_CHECKOUT)
  subprocess.check_call(['./gradlew', 'jar'])

  print(f'compile luceneutil KNN at {constants.BENCH_BASE_DIR}')
  os.chdir(constants.BENCH_BASE_DIR)
  subprocess.check_call(['./gradlew', 'compileKnn'])

  cp = benchUtil.classPathToString(benchUtil.getClassPath(LUCENE_CHECKOUT) + (f'{constants.BENCH_BASE_DIR}/build',))

  cmd = constants.JAVA_EXE.split(' ') + \
      ['-cp', cp,
       '--add-modules', 'jdk.incubator.vector',  # are these still needed in Lucene 11+, java 23+?
       '--enable-native-access=ALL-UNNAMED',
       'knn.KnnGraphTester',
       '-maxConn', str(HNSW_MAX_CONN),
       '-dim', str(VECTORS_DIM),
       '-docs', INDEX_VECTORS_FILE,
       '-ndoc', str(INDEX_NUM_VECTORS),
       '-topK', str(HNSW_TOP_K),
       '-numIndexThreads', str(INDEX_THREAD_COUNT),
       '-beamWidthIndex', str(HNSW_INDEX_BEAM_WIDTH),
       '-search-and-stats', SEARCH_VECTORS_FILE,
       '-metric', 'mip',
       '-numMergeThread', '16',
       '-numMergeWorker', '48',
       #'-forceMerge'
       ]

  all_results = []
  all_summaries = []
  
  for quantize_bits in (4, 7, 32):

    for do_force_merge in (False, True):

      this_cmd = cmd[:]

      if not do_force_merge:
        this_cmd.append('-reindex')
      else:
        # reuse previous index, but force merge it first
        this_cmd.append('-forceMerge')

      if quantize_bits != 32:
        this_cmd += ['-quantize', '-quantizeBits', str(quantize_bits)]
        if quantize_bits <= 4:
          this_cmd += ['-quantizeCompress']

      print(f'  cmd: {this_cmd}')

      t0 = datetime.datetime.now()

      job = subprocess.Popen(this_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='utf-8')
      summary = None
      all_graph_level_conn_p_values = []
      layer_count = None
      index_path = None

      while True:
        line = job.stdout.readline()
        if line == '':
          break
        sys.stdout.write(line)
        m = re_index_path.match(line)
        if m is not None:
          index_path = m.group(1)

        m = re_summary.match(line)
        if m is not None:
          summary = m.group(1)

        m = re_leaf_layers.match(line)
        if m is not None:
          leaf_count = int(m.group(1))
          next_layer_count = int(m.group(2))

        m = re_leaf_docs.match(line)
        if m is not None:

          if next_layer_count is None:
            raise RuntimeError('missed layer count?')

          if len(all_graph_level_conn_p_values) > 0:
            assert graph_level_conn_p_values == all_graph_level_conn_p_values[-1][2]
            if len(graph_level_conn_p_values) < layer_count:
              raise RuntimeError(f'failed to parse graph level connection stats for leaf {all_graph_level_conn_p_values[-1][0]}?  {graph_level_conn_p_values} vs {layer_count=}')

          graph_level_conn_p_values = {}
          # leaf-number, doc-count, dict mapping layer to node connectedness p values
          if leaf_count != int(m.group(1)):
            raise RuntimeError('leaf count disagrees?  {leaf_count=} vs {int(m.group(1))}')
          all_graph_level_conn_p_values.append((leaf_count, int(m.group(2)), graph_level_conn_p_values))
          layer_count = next_layer_count

        m = re_graph_level.match(line)
        if m is not None:
          graph_level = int(m.group(1))
          graph_level_size = int(m.group(2))

          line = job.stdout.readline().strip()
          sys.stdout.write(line)
          sys.stdout.write('\n')

          if line != '%   0  10  20  30  40  50  60  70  80  90 100':
            raise RuntimeError(f'unexpected line after graph level output: {line}')
          line = job.stdout.readline()
          sys.stdout.write(line)
          sys.stdout.write('\n')

          if graph_level_size == 0:
            # no p-values
            d = {}
          else:
            p_values = [int(x) for x in line.split()]
            if len(p_values) != 11:
              raise RuntimeError(f'expected 11 p-values for graph level but got {len(p_values)}: {p_values}')
            d = {}
            p = 0
            for v in p_values:
              d[p] = v
              p += 10

          graph_level_conn_p_values[graph_level] = (graph_level_size, d)

      t1 = datetime.datetime.now()
      combined_run_time = t1 - t0
      print(f'  took {combined_run_time} to run KnnGraphTester')

      if summary is None:
        raise RuntimeError('could not find summary line in output!')

      if index_path is None:
        raise RuntimeError('could not find Index Path line in output!')

      job.wait()

      if job.returncode != 0:
        raise RuntimeError(f'command failed with exit {job.returncode}')

      all_summaries.append(summary)

      cols = summary.split('\t')

      result = KNNResultV0(lucene_git_rev,
                           luceneutil_git_rev,
                           INDEX_VECTORS_FILE,
                           SEARCH_VECTORS_FILE,
                           VECTORS_DIM,
                           do_force_merge,
                           float(cols[0]),  # recall
                           float(cols[1]),  # cpu_time_ms
                           int(cols[2]),    # num_docs
                           int(cols[3]),    # top_k
                           int(cols[4]),    # fanout
                           int(cols[5]),    # max_conn
                           int(cols[6]),    # beam_width
                           cols[7],         # quantize_desc
                           int(cols[8]),    # total_visited
                           float(cols[9]),  # index_time_sec
                           float(cols[10]), # index_docs_per_sec
                           float(cols[11]), # force_merge_time_sec
                           int(cols[12]),   # index_num_segments
                           float(cols[13]), # index_size_on_disk_mb
                           float(cols[14]), # selectivity
                           cols[15],        # pre_post_filter
                           float(cols[16]), # vec_disk_mb
                           float(cols[17]), # vec_ram_mb
                           graph_level_conn_p_values,  # graph_level_conn_p_values
                           combined_run_time,  # time to run KnnGraphTester (indexing + force-merging + searching)
                           )
      print(f'result: {result}')
      all_results.append(result)

      if do_force_merge:
        # clean as we go -- these indices are biggish (~25-30 GB):
        print(f'now remove KNN index {index_path}')
        shutil.rmtree(index_path)

  skip_headers = {'selectivity', 'filterType', 'visited'}
  knnPerfTest.print_fixed_width(all_summaries, skip_headers)

  results_file = f'{results_dir}/knn_results.pk'
  print(f'saving all results to {results_file}')
  open(results_file, 'wb').write(pickle.dumps(all_results))

  end_time_epoch_secs = time.time()
  print(f'done!  took {(end_time_epoch_secs - start_time_epoch_secs):.1f} seconds total')

  return all_results

if __name__ == '__main__':
  main()
