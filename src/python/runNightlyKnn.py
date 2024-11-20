import os
import re
import sys
import constants
import benchUtil
import subprocess
import pickle
import datetime
from collections import namedtuple
import knnPerfTest

# TODO
#   - test different distance metrics / vector sources?
#   - test with and without force merge?
#   - need "hot searcher RAM"
#   - compile lucene too?
#   - test parent join too
#   - suck metrics out of -stats too?
#   - make nightly charts
#   - test filters, pre and post

# Cohere v2?  768 dims

#INDEX_VECTORS_FILE = constants.NIGHTLY_INDEX_VECTORS_FILE
#SEARCH_VECTORS_FILE = constants.NIGHTLY_SEARCH_VECTORS_FILE
#VECTORS_DIM = constants.NIGHTLY_VECTORS_DIM
#VECTORS_ENCODING = constants.NIGHTLY_VECTORS_ENCODING
#LUCENE_CHECKOUT = constants.LUCENE_CHECKOUT

# e.g. Cohere v2
INDEX_VECTORS_FILE = '/l/data/cohere-wikipedia-docs-768d.vec'
SEARCH_VECTORS_FILE = '/l/data/cohere-wikipedia-queries-768d.vec'
VECTORS_DIM = 768
VECTORS_ENCODING = 'float32'
LUCENE_CHECKOUT = '/l/trunk'


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

KNNResult = namedtuple('KNNResult',
                       ['lucene_git_rev',
                        'luceneutil_git_rev',
                        'index_vectors_file',
                        'search_vectors_file',
                        'vector_dims',
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
                        'pre_or_post_filter',
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
  lucene_git_rev = get_git_revision(LUCENE_CHECKOUT)
  lucene_clone_is_dirty = is_git_clone_dirty(LUCENE_CHECKOUT)
  print(f'Lucene HEAD git revision at {LUCENE_CHECKOUT}: {lucene_git_rev} dirty?={lucene_clone_is_dirty}')

  luceneutil_git_rev = get_git_revision(constants.BENCH_BASE_DIR)
  luceneutil_clone_is_dirty = is_git_clone_dirty(constants.BENCH_BASE_DIR)
  print(f'luceneutil HEAD git revision at {constants.BENCH_BASE_DIR}: {luceneutil_git_rev} dirty?={luceneutil_clone_is_dirty}')

  # nocommit turn on!
  if False and REAL:
    if lucene_clone_is_dirty:
      raise RuntimeError(f'lucene clone {LUCENE_CHECKOUT} is git-dirty')
    if luceneutil_clone_is_dirty:
      raise RuntimeError(f'luceneutil clone {constants.BENCH_BASE_DIR} is git-dirty')

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
       '-reindex',
       '-beamWidthIndex', str(HNSW_INDEX_BEAM_WIDTH),
       '-search-and-stats', SEARCH_VECTORS_FILE,
       '-metric', 'mip',
       '-numMergeThread', str(8),
       '-numMergeWorker', str(12),
       #'-forceMerge'
       ]

  all_results = []
  all_summaries = []
  
  for quantize_bits in (4, 7, 32):

    this_cmd = cmd[:]

    if quantize_bits != 32:
      this_cmd += ['-quantize', '-quantizeBits', str(quantize_bits)]
      if quantize_bits <= 4:
        this_cmd += ['-quantizeCompress']

    print(f'  cmd: {this_cmd}')

    t0 = datetime.datetime.now()

    job = subprocess.Popen(this_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='utf-8')
    summary = None
    graph_level_conn_p_values = {}
    while True:
      line = job.stdout.readline()
      if line == '':
        break
      sys.stdout.write(line)
      m = re_summary.match(line)
      if m is not None:
        summary = m.group(1)
      m = re_graph_level.match(line)
      if m is not None:
        graph_level = int(m.group(1))
        graph_level_size = int(m.group(2))

        line = job.stdout.readline().strip()
        sys.stdout.write(line)
        
        if line != '%   0  10  20  30  40  50  60  70  80  90 100':
          raise RuntimeError(f'unexpected line after graph level output: {line}')
        line = job.stdout.readline()
        sys.stdout.write(line)
        
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

    if len(graph_level_conn_p_values) < 4:
      raise RuntimeError(f'failed to parse graph level connection stats?  {graph_level_conn_p_values}')

    if summary is None:
      raise RuntimeError('could not find summary line in output!')
    job.wait()
    if job.returncode != 0:
      raise RuntimeError(f'command failed with exit {job.returncode}')

    all_summaries.append(summary)
    
    cols = summary.split('\t')

    result = KNNResult(lucene_git_rev,
                       luceneutil_git_rev,
                       INDEX_VECTORS_FILE,
                       SEARCH_VECTORS_FILE,
                       VECTORS_DIM,
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
                       cols[15],        # pre_or_post_filter
                       graph_level_conn_p_values,  # graph_level_conn_p_values
                       combined_run_time,  # time to run KnnGraphTester (indexing + force-merging + searching)
                       )
    all_results.append((quantize_bits, result))

  skip_headers = {'selectivity', 'filterType', 'visited'}
  knnPerfTest.print_fixed_width(all_summaries, skip_headers)

  return all_results

if __name__ == '__main__':
  main()
