import datetime
import glob
import os
import pickle
import re
import shutil
import subprocess
import sys
import time
from collections import namedtuple

import benchUtil
import constants
import knnPerfTest

# TODO
#   - graphs
#     - get gitHashes / clicking working
#     - get annots working
#   - remove all cached indices / .bin files from nightly area
#     - hmm maybe don't remove the exact KNN?  takes a long time to recompute!
#   - test different distance metrics / vector sources?
#   - compile lucene too?
#   - test parent join too
#   - make nightly charts
#   - test filters, pre and post

# Cohere v2?  768 dims

# INDEX_VECTORS_FILE = constants.NIGHTLY_INDEX_VECTORS_FILE
# SEARCH_VECTORS_FILE = constants.NIGHTLY_SEARCH_VECTORS_FILE
# VECTORS_DIM = constants.NIGHTLY_VECTORS_DIM
# VECTORS_ENCODING = constants.NIGHTLY_VECTORS_ENCODING
# LUCENE_CHECKOUT = constants.LUCENE_CHECKOUT

# e.g. Cohere v2
# INDEX_VECTORS_FILE = "/lucenedata/enwiki/cohere-wikipedia-docs-768d.vec"
# SEARCH_VECTORS_FILE = "/lucenedata/enwiki/cohere-wikipedia-queries-768d.vec"
# VECTORS_DIM =  768

# Cohere v3, switched Dec 7 2025:
INDEX_VECTORS_FILE = "/big/cohere-v3-wikipedia-en-scattered-1024d.docs.vec"
SEARCH_VECTORS_FILE = "/lucenedata/enwiki/cohere-v3/cohere-v3-wikipedia-en-scattered-1024d.queries.vec"
VECTORS_DIM = 1024

VECTORS_ENCODING = "float32"
LUCENE_CHECKOUT = "/l/trunk.nightly"
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

re_summary = re.compile(r"^SUMMARY: (.*?)$", re.MULTILINE)
re_graph_level = re.compile(r"^Graph level=(\d) size=(\d+), Fanout")
re_leaf_docs = re.compile(r"^Leaf (\d+) has (\d+) documents")
re_leaf_layers = re.compile(r"^Leaf (\d+) has (\d+) layers")
re_index_path = re.compile(r"^Index Path = (.*)$")

KNNResultV0 = namedtuple(
  "KNNResultV0",
  [
    "lucene_git_rev",
    "luceneutil_git_rev",
    "index_vectors_file",
    "search_vectors_file",
    "vector_dims",
    "do_force_merge",
    "recall",
    "cpu_time_ms",
    "num_docs",
    "top_k",
    "fanout",
    "max_conn",
    "beam_width",
    "quantize_desc",
    "total_visited",
    "index_time_sec",
    "index_docs_per_sec",
    "force_merge_time_sec",
    "index_num_segments",
    "index_size_on_disk_mb",
    "selectivity",
    "pre_post_filter",
    "vec_disk_mb",
    "vec_ram_mb",
    "graph_level_conn_p_values",
    "combined_run_time",
  ],
)

KNNResultV1 = namedtuple(
  "KNNResultV1",
  [
    "lucene_git_rev",
    "luceneutil_git_rev",
    "index_vectors_file",
    "search_vectors_file",
    "vector_dims",
    "do_force_merge",
    "recall",
    "cpu_time_ms",
    "net_cpu_ms",
    "avg_cpu_core_count",
    "num_docs",
    "top_k",
    "fanout",
    "max_conn",
    "beam_width",
    "quantize_desc",
    "total_visited",
    "index_time_sec",
    "index_docs_per_sec",
    "force_merge_time_sec",
    "index_num_segments",
    "index_size_on_disk_mb",
    "selectivity",
    "pre_post_filter",
    "vec_disk_mb",
    "vec_ram_mb",
    "graph_level_conn_p_values",
    "combined_run_time",
  ],
)


CHANGES = [
  ("2016-07-04 07:13:41", "LUCENE-7351: Doc id compression for dimensional points"),
  ("2016-07-07 08:02:29", "LUCENE-7369: Similarity.coord and BooleanQuery.disableCoord are removed"),
  ("2016-07-12 15:57:56", "LUCENE-7371: Better compression of dimensional points values"),
  ("2016-07-29 08:23:54", "LUCENE-7396: speed up flush of points"),
  ("2016-08-03 12:34:06", "LUCENE-7403: Use blocks of exactly maxPointsInLeafNode in the 1D points case"),
  ("2016-08-03 12:35:48", "LUCENE-7399: Speed up flush of points, v2"),
  ("2016-08-12 17:54:33", "LUCENE-7409: improve MMapDirectory's IndexInput to detect if a clone is being used after its parent was closed"),
  ("2016-09-21 13:41:41", "LUCENE-7407: Switch doc values to iterator API"),
  ("2016-10-04 17:00:53", "LUCENE-7474: Doc values writers should use sparse encoding"),
  ("2016-10-17 07:28:20", "LUCENE-7489: Better sparsity support for Lucene70DocValuesFormat"),
  ("2016-10-18 13:05:50", "LUCENE-7501: Save one heap byte per index node in the dimensional points index for the 1D case"),
  ("2016-10-18 14:08:29", "LUCENE-7489: Wrap only once in case GCD compression is used"),
  ("2016-10-24 08:51:23", "LUCENE-7462: Give doc values an advanceExact method"),
  ("2016-10-31 00:04:37", "LUCENE-7135: This issue accidentally caused FSDirectory.open to use NIOFSDirectory instead of MMapDirectory"),
  ("2016-11-02 10:48:29", "LUCENE-7135: Fixed this issue so we use MMapDirectory again"),
  ("2016-11-10 13:04:15", "LUCENE-7545: Dense norms/doc-values should not consume memory for the IW buffer"),
  ("2016-11-23", "Take best of 5 JVM runs for each search benchmark to reduce noise"),
  ("2016-12-04", "LUCENE-7563: Compress the in-memory BKD points index"),
  ("2016-12-07", "LUCENE-7583: buffer small leaf-block writes in BKDWriter"),
  ("2016-12-11", "Re-enable JVM's background and tiered compilation"),
  ("2016-12-15", "LUCENE-7589: Prevent outliers from raising number of doc-values bits for all documents"),
  ("2016-12-20", "LUCENE-7579: Sort segments on flush, not merge"),
  ("2019-04-23", "Switched to OpenJDK 11"),
  ("2019-04-30", "Switched GC back to ParallelGC (away from default G1GC)"),
  ("2020-11-06", "Move to new beast 3 Ryzen Threadripper 3990X hardware for all nightly benchmarks"),
  ("2021-06-18", "LUCENE-9996: Reduced RAM usage per DWPT"),
  ("2021-06-24", "LUCENE-9613: Encode ordinals like numerics"),
  ("2021-07-29", "LUCENE-10031: Faster merging with index sorting enabled"),
  ("2021-08-16", "LUCENE-10014: Fixed GCD compression"),
  ("2021-09-28", "LUCENE-10125: Optimize primitive writes in OutputStreamIndexOutput"),
  ("2021-10-07", "LUCENE-10153: Speed up BKDWriter using VarHandles"),
  ("2021-10-21", "LUCENE-10165: Implement Lucene90DocValuesProducer#getMergeInstance"),
  ("2021-11-01", "LUCENE-10196: Improve IntroSorter with 3-ways partitioning"),
  ("2022-12-23", "Cut over numeric fields to LongField / DoubleField"),
  ("2022-12-28", "GITHUB#12037: Optimize flush of SORTED_NUMERIC fields in conjunction with index sorting"),
  ("2023-02-13", "Cut over keyword fields to KeywordField"),
  ("2023-02-21", "GITHUB#12139: Skip the TokenStream overhead for simple keywords"),
]


def get_git_revision(git_clone_path):
  # git is so weird, like this is intuitive?
  git_rev = subprocess.run(["git", "-C", git_clone_path, "rev-parse", "HEAD"], check=True, capture_output=True, encoding="utf-8").stdout.strip()
  # i don't trust git:
  if len(git_rev) != 40:
    raise RuntimeError(f"failed to parse HEAD git hash for {git_clone_path}?  got {git_rev}, not 40 characters")

  return git_rev


def is_git_clone_dirty(git_clone_path):
  p = subprocess.run(
    ["git", "-C", git_clone_path, "diff", "--quiet"],
    check=False,  # we will interpret return code ourselves
    capture_output=True,
    encoding="utf-8",
  )
  return bool(p.returncode)


re_date_time = re.compile(r"/(\d\d\d\d)\.(\d\d)\.(\d\d)\.(\d\d)\.(\d\d)\.(\d\d)/")


def convert_v0_to_v1(v0_result):
  """Convert a KNNResultV0 result to KNNResultV1 format."""
  # Extract all fields from v0
  fields = list(v0_result)

  # total cpu and net cpu cores added in https://github.com/mikemccand/luceneutil/commit/20ad142136f38360ac6c5b54c954d1575527ce2b
  assert len(fields) == 26, f"got {len(fields)}"

  # Insert placeholder values for the new fields after cpu_time_ms
  cpu_time_index = 7  # Index of cpu_time_ms in KNNResultV0
  fields.insert(cpu_time_index + 1, 0.0)  # netCPU
  fields.insert(cpu_time_index + 2, 0.0)  # avgCpuCount

  return KNNResultV1(*fields)


def main():
  if "-write_graph" in sys.argv:
    write_graph()
  else:
    all_results = run(".")


def write_graph():
  series = {}
  timestamps = []
  luceneGitHashes = []
  luceneUtilGitHashes = []
  for file_name in sorted(glob.glob(f"{constants.LOGS_DIR}/*/knn_results.pk")):
    # for file_name in sorted(glob.glob(f'/l/logs.nightly/*/knn_results.pk')):
    m = re_date_time.search(file_name)
    year, month, day, hour, minute, second = [int(x) for x in m.groups()]
    t = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)
    timestamps.append(t)
    result = pickle.load(open(file_name, "rb"))
    row = []

    # quantize_bits=(4, 7, 32) X forceMerge=(False, True)
    recalls = {}
    lucene_git_rev = None
    luceneutil_git_rev = None
    for run in result:
      # TODO: messy, messy -- how can I check that the class type is exactly KNNResultV0?
      # Unpickle does something weird... type(run) is runNightlyKnn.KNNResultV0 but KNNResultV0 is __main_)_.KNResultV0
      if str(type(run)).endswith(".KNNResultV0'>"):
        run = convert_v0_to_v1(run)

      if lucene_git_rev is None:
        lucene_git_rev = run.lucene_git_rev
      elif lucene_git_rev != run.lucene_git_rev:
        # should be the same for all runs
        raise RuntimeError(f"lucene git rev changed?  {lucene_git_rev=} vs {run.lucene_git_rev=}")
      if luceneutil_git_rev is None:
        luceneutil_git_rev = run.luceneutil_git_rev
      elif luceneutil_git_rev != run.luceneutil_git_rev:
        # should be the same for all runs
        raise RuntimeError(f"luceneutil git rev changed?  {luceneutil_git_rev=} vs {run.luceneutil_git_rev=}")
      desc = run.quantize_desc
      if run.do_force_merge:
        desc += ".force_merge"
      add(series, desc, run.recall)
      add(series, f"{desc} net_cpu_time_ms", run.net_cpu_ms)
      add(series, f"{desc} avg_cpu_core_count", run.avg_cpu_core_count)
      add(series, f"{desc} cpu_time_ms", run.cpu_time_ms)
      add(series, f"{desc} RAM", run.vec_ram_mb)
      add(series, f"{desc} disk", run.vec_disk_mb)
      add(series, f"{desc} index-time-sec", run.index_time_sec)
      add(series, f"{desc} force-merge-time-sec", run.force_merge_time_sec)
      add(series, f"{desc} index K docs/sec", run.index_docs_per_sec / 1000.0)
      add(series, f"{desc} vec_ram_gb", run.vec_ram_mb / 1024.0)
      add(series, f"{desc} vec_disk_gb", run.vec_disk_mb / 1024.0)
      add(series, f"{desc} k_total_visited", run.total_visited / 1000.0)

    luceneGitHashes.append(lucene_git_rev)
    luceneUtilGitHashes.append(luceneutil_git_rev)

  if False:
    # too verbose!
    for i, githash in enumerate(luceneGitHashes):
      print(f"{timestamps[i]=} {githash=}")

    print("\n\nluceneutil:")
    for i, githash in enumerate(luceneUtilGitHashes):
      print(f"{timestamps[i]=} {githash=}")

    for label, points in series.items():
      print(f"{label} -> {points}")

  with open("/l/lucenenightly/docs/knnResults.html", "w") as f:
    f.write("""
<html>
<head>
<link rel="stylesheet" href="dygraph.css"></link>
<title>Lucene Nightly KNN benchmarks</title>
<script type="text/javascript" src="dygraph.min.js"></script>
<script type="text/javascript">
""")
    f.write("luceneGitHashes = %s;\n" % repr(luceneGitHashes))
    f.write("luceneUtilGitHashes = %s;\n" % repr(luceneUtilGitHashes))
    f.write("""
function onPointClick(e, p) {
  // TODO: make this controllable via end-user GUI
  var which = "luceneutil";
  // var which = "lucene";
  if (p.idx > 0) {
    if (which === "lucene") {
      top.location = "https://github.com/apache/lucene/compare/" + luceneGitHashes[p.idx-1] + "..." + luceneGitHashes[p.idx];
    } else {
      top.location = "https://github.com/mikemccand/luceneutil/compare/" + luceneUtilGitHashes[p.idx-1] + "..." + luceneUtilGitHashes[p.idx];
    }
  } else {
    if (which === "lucene") {
      top.location = "https://github.com/apache/lucene/commit/" + luceneGitHashes[p.idx];
    } else {
      top.location = "https://github.com/mikemccand/luceneutil/commit/" + luceneUtilGitHashes[p.idx];
    }
  }
}
</script>
<style type="text/css">
a:hover * {
  text-decoration: underline;
}

html * {
  #font-size: 1em !important;
  text-decoration: none;
  #color: #000 !important;
  font-family: Verdana !important;
}

.dygraph-legend > span.highlight {
  border: 2px solid black;
  font-weight: bold;
  font-size: 12;
  width: 500px;
}

.dygraph-legend > span {
  font-weight: bold;
  font-size: 12;
  width: 500px;
}
</style>

</head>
<body>
""")

    f.write("""
<style type="text/css">
#summary {
  left: 10px;
}
</style>

<div id="summary" style="height:17%%; width:95%%">
This benchmark indexes 8.0M and searches Cohere 768 dimension vectors from https://huggingface.co/datasets/Cohere/wikipedia-22-12-en-embeddings.
</div>
""")

    write_one_graph(
      f,
      timestamps,
      (series["no"], series["no.force_merge"], series["7 bits"], series["7 bits.force_merge"], series["4 bits"], series["4 bits.force_merge"]),
      "knn_recall",
      "Recall",
      headers=("Date", "float32", "float32 1seg", "7 bits", "7 bits 1seg", "4 bits", "4 bits 1seg"),
      ylabel="Recall",
    )

    write_one_graph(
      f,
      timestamps,
      (
        series["no cpu_time_ms"],
        series["no.force_merge cpu_time_ms"],
        series["7 bits cpu_time_ms"],
        series["7 bits.force_merge cpu_time_ms"],
        series["4 bits cpu_time_ms"],
        series["4 bits.force_merge cpu_time_ms"],
      ),
      "knn_cpu_time_ms",
      "Wall clock latency (msec)",
      headers=("Date", "float32", "float32 1seg", "7 bits", "7 bits 1seg", "4 bits", "4 bits 1seg"),
      ylabel="Wall clock latency (msec)",
    )

    write_one_graph(
      f,
      timestamps,
      (
        series["no net_cpu_time_ms"],
        series["no.force_merge net_cpu_time_ms"],
        series["7 bits net_cpu_time_ms"],
        series["7 bits.force_merge net_cpu_time_ms"],
        series["4 bits net_cpu_time_ms"],
        series["4 bits.force_merge net_cpu_time_ms"],
      ),
      "knn_net_cpu_time_ms",
      "Net CPU time per query (msec)",
      headers=("Date", "float32", "float32 1seg", "7 bits", "7 bits 1seg", "4 bits", "4 bits 1seg"),
      ylabel="Net CPU time per query (msec)",
    )

    write_one_graph(
      f,
      timestamps,
      (
        series["no avg_cpu_core_count"],
        series["no.force_merge avg_cpu_core_count"],
        series["7 bits avg_cpu_core_count"],
        series["7 bits.force_merge avg_cpu_core_count"],
        series["4 bits avg_cpu_core_count"],
        series["4 bits.force_merge avg_cpu_core_count"],
      ),
      "knn_avg_cpu_core_count",
      "CPU cores per query",
      headers=("Date", "float32", "float32 1seg", "7 bits", "7 bits 1seg", "4 bits", "4 bits 1seg"),
      ylabel="CPU cores per query",
    )

    write_one_graph(
      f,
      timestamps,
      (series["no index K docs/sec"], series["7 bits index K docs/sec"], series["4 bits index K docs/sec"]),
      "knn_indexing_docs_per_sec",
      "Indexing K docs/sec",
      headers=("Date", "float32", "7 bits", "4 bits"),
      ylabel="Indexing K doc/sec",
    )

    write_one_graph(
      f,
      timestamps,
      (series["no.force_merge force-merge-time-sec"], series["7 bits.force_merge force-merge-time-sec"], series["4 bits.force_merge force-merge-time-sec"]),
      "force_merge_time_sec",
      "Force Merge time (sec)",
      headers=("Date", "float32", "7 bits", "4 bits"),
      ylabel="sec",
    )

    write_one_graph(
      f,
      timestamps,
      (
        series["no vec_ram_gb"],
        series["no.force_merge vec_ram_gb"],
        series["7 bits vec_ram_gb"],
        series["7 bits.force_merge vec_ram_gb"],
        series["4 bits vec_ram_gb"],
        series["4 bits.force_merge vec_ram_gb"],
        series["no vec_disk_gb"],
        series["no.force_merge vec_disk_gb"],
        series["7 bits vec_disk_gb"],
        series["7 bits.force_merge vec_disk_gb"],
        series["4 bits vec_disk_gb"],
        series["4 bits.force_merge vec_disk_gb"],
      ),
      "knn_ram_disk_gb",
      "GB RAM and disk",
      headers=(
        "Date",
        "RAM float32",
        "RAM float32 1seg",
        "RAM 7 bits",
        "RAM 7 bits 1seg",
        "RAM 4 bits",
        "RAM 4 bits 1seg",
        "Disk float32",
        "Disk float32 1seg",
        "Disk 7 bits",
        "Disk 7 bits 1seg",
        "Disk 4 bits",
        "Disk 4 bits 1seg",
      ),
      ylabel="GB",
    )

    write_one_graph(
      f,
      timestamps,
      (
        series["no k_total_visited"],
        series["no.force_merge k_total_visited"],
        series["7 bits k_total_visited"],
        series["7 bits.force_merge k_total_visited"],
        series["4 bits k_total_visited"],
        series["4 bits.force_merge k_total_visited"],
      ),
      "k_total_visited",
      "Visited Node Count (K)",
      headers=("Date", "float32", "float32 1seg", "7 bits", "7 bits 1seg", "4 bits", "4 bits 1seg"),
      ylabel="K Nodes Visited",
    )

    f.write("</body>\n</html>")


topPct = 0


def getLabel(label):
  if label < 26:
    s = chr(65 + label)
  else:
    s = "%s%s" % (chr(65 + (label // 26 - 1)), chr(65 + (label % 26)))
  return s


def write_one_graph(f, timestamps, series, id, title, headers=None, ylabel=None):
  global topPct

  assert len(timestamps) == len(series[0])

  topPct += 55

  if headers is None:
    headers = ("Date", "Dense", "Sparse", "Sparse (sorted)")

  f.write(
    """
<div id="%s" style="height:50%%; width:95%%"></div>
<script type="text/javascript">
  g = new Dygraph(

  // containing div
  document.getElementById("%s"),
  "%s\\n"
"""
    % (id, id, ",".join(headers))
  )

  for i in range(len(timestamps)):
    timestamp = timestamps[i]
    values = [series[x][i] for x in range(len(series))]
    f.write('  + "%04d-%02d-%02d %02d:%02d:%02d' % (timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute, timestamp.second))
    f.write(',%s\\n"\n' % ",".join([str(x) for x in values]))

  f.write(f'''
  , {{ "title": "<center><a href=\'#{id}\'><font size=+2>{title}</font></a></center>",
    // "colors": ["#DD1E2F", "#EBB035", "#06A2CB", "#218559", "#B0A691", "#192823"],
    "colors": ["#00BFB3", "#FED10A", "#0078A0", "#DF4998", "#93C90E", "#00A9E5", "#222", "#AAA", "#777"],
    "drawGapEdgePoints": true,
    "xlabel": "Date",
    "ylabel": "{ylabel}",
    "pointClickCallback": onPointClick,
    //"labelsDivWidth": 500,
    "labelsSeparateLines": true,
    "pointSize": 2,
    "gridLineColor": "#BBB",
    "colorSaturation": 0.5,
    "highlightCircleSize": 4,
    //"strokeWidth": 2.0,
    "connectSeparatedPoints": true,
    "drawPoints": true,
    "includeZero": true,
    "axisLabelColor": "#555",
    "axisLineColor": "#555",
    highlightSeriesOpts: {{
      strokeWidth: 3,
      strokeBorderWidth: 1,
      highlightCircleSize: 5
    }},
  }});
  ''')

  f.write("g.ready(function() {g.setAnnotations([")
  for i in range(len(CHANGES)):
    change = CHANGES[i]
    if len(change) == 3:
      timeStamp = change[2]
      f.write('{series: "%s", x: "%s", shortText: "%s", text: "%s"},\n' % (headers[2], timeStamp, getLabel(i), change[1].replace('"', '\\"')))
  f.write("]);});\n")

  f.write("</script>\n")


def add(series, desc, value):
  if desc not in series:
    series[desc] = []
  series[desc].append(value)


def run(results_dir):
  try:
    return _run(results_dir)
  finally:
    # these get quite large (~78 GB for three indices):
    indicesDir = f"{constants.BENCH_BASE_DIR}/knn-reuse/indices"
    if os.path.exists(indicesDir):
      print(f"removing KNN indices dir {indicesDir}")
      shutil.rmtree(indicesDir)


def _run(results_dir):
  start_time_epoch_secs = time.time()
  lucene_git_rev = get_git_revision(LUCENE_CHECKOUT)
  lucene_clone_is_dirty = is_git_clone_dirty(LUCENE_CHECKOUT)
  print(f"Lucene HEAD git revision at {LUCENE_CHECKOUT}: {lucene_git_rev} dirty?={lucene_clone_is_dirty}")

  luceneutil_git_rev = get_git_revision(constants.BENCH_BASE_DIR)
  luceneutil_clone_is_dirty = is_git_clone_dirty(constants.BENCH_BASE_DIR)
  print(f"luceneutil HEAD git revision at {constants.BENCH_BASE_DIR}: {luceneutil_git_rev} dirty?={luceneutil_clone_is_dirty}")

  if REAL:
    if lucene_clone_is_dirty:
      raise RuntimeError(f"lucene clone {LUCENE_CHECKOUT} is git-dirty")
    if luceneutil_clone_is_dirty:
      raise RuntimeError(f"luceneutil clone {constants.BENCH_BASE_DIR} is git-dirty")

  # make sure nothing is shared from prior runs
  indicesDir = f"{constants.BENCH_BASE_DIR}/knn-reuse/indices"
  if os.path.exists(indicesDir):
    print(f"removing KNN indices dir {indicesDir}")
    shutil.rmtree(indicesDir)

  if False:
    # we should be able to safely reuse the "answer key" -- the hash/key should work --
    # if we change something here, it should recompute, and if not, it's buggy, and we should
    # fix it!
    for cacheFileName in glob.glob(f"{constants.BENCH_BASE_DIR}/knn-reuse/exact-nn/*.bin"):
      print(f"removing cached KNN results {cacheFileName}")
      os.remove(cacheFileName)

  print(f"compile Lucene jars at {LUCENE_CHECKOUT}")
  os.chdir(LUCENE_CHECKOUT)
  subprocess.check_call(["./gradlew", "jar"])

  print(f"compile luceneutil KNN at {constants.BENCH_BASE_DIR}")
  os.chdir(constants.BENCH_BASE_DIR)
  subprocess.check_call(["./gradlew", "compileKnn"])

  cp = benchUtil.classPathToString(benchUtil.getClassPath(LUCENE_CHECKOUT) + (f"{constants.BENCH_BASE_DIR}/build",))

  cmd = constants.JAVA_EXE.split(" ") + [
    "-cp",
    cp,
    "--add-modules",
    "jdk.incubator.vector",  # are these still needed in Lucene 11+, java 23+?
    "--enable-native-access=ALL-UNNAMED",
    "knn.KnnGraphTester",
    "-maxConn",
    str(HNSW_MAX_CONN),
    "-dim",
    str(VECTORS_DIM),
    "-docs",
    INDEX_VECTORS_FILE,
    "-ndoc",
    str(INDEX_NUM_VECTORS),
    "-topK",
    str(HNSW_TOP_K),
    "-numIndexThreads",
    str(INDEX_THREAD_COUNT),
    "-beamWidthIndex",
    str(HNSW_INDEX_BEAM_WIDTH),
    "-search-and-stats",
    SEARCH_VECTORS_FILE,
    "-metric",
    "dot_product",
    "-numMergeThread",
    "16",
    "-numMergeWorker",
    "48",
    #'-forceMerge'
  ]

  # print cpu and memory information at the start
  knnPerfTest.print_cpu_info()
  knnPerfTest.print_mem_info()

  # sanity check vectors
  knnPerfTest.smell_vectors(VECTORS_DIM, INDEX_VECTORS_FILE, True)
  knnPerfTest.smell_vectors(VECTORS_DIM, SEARCH_VECTORS_FILE, True)

  all_results = []
  all_summaries = []
  graph_level_conn_p_values = []

  for quantize_bits in (4, 7, 32):
    for do_force_merge in (False, True):
      this_cmd = cmd[:]

      if not do_force_merge:
        this_cmd.append("-reindex")
      else:
        # reuse previous index, but force merge it first
        this_cmd.append("-forceMerge")

      if quantize_bits != 32:
        this_cmd += ["-quantize", "-quantizeBits", str(quantize_bits)]
        if quantize_bits <= 4:
          this_cmd += ["-quantizeCompress"]

      print(f"  cmd: {this_cmd}")

      t0 = datetime.datetime.now()

      job = subprocess.Popen(this_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding="utf-8")
      summary = None
      all_graph_level_conn_p_values = []
      layer_count = None
      index_path = None

      while True:
        line = job.stdout.readline()
        if line == "":
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
            raise RuntimeError("missed layer count?")

          if len(all_graph_level_conn_p_values) > 0:
            assert graph_level_conn_p_values == all_graph_level_conn_p_values[-1][2]
            if len(graph_level_conn_p_values) < layer_count:
              raise RuntimeError(f"failed to parse graph level connection stats for leaf {all_graph_level_conn_p_values[-1][0]}?  {graph_level_conn_p_values} vs {layer_count=}")

          graph_level_conn_p_values = {}
          # leaf-number, doc-count, dict mapping layer to node connectedness p values
          if leaf_count != int(m.group(1)):
            raise RuntimeError("leaf count disagrees?  {leaf_count=} vs {int(m.group(1))}")
          all_graph_level_conn_p_values.append((leaf_count, int(m.group(2)), graph_level_conn_p_values))
          layer_count = next_layer_count

        m = re_graph_level.match(line)
        if m is not None:
          graph_level = int(m.group(1))
          graph_level_size = int(m.group(2))

          line = job.stdout.readline().strip()
          sys.stdout.write(line)
          sys.stdout.write("\n")

          if line != "%   0  10  20  30  40  50  60  70  80  90 100":
            raise RuntimeError(f"unexpected line after graph level output: {line}")
          line = job.stdout.readline()
          sys.stdout.write(line)
          sys.stdout.write("\n")

          if graph_level_size == 0:
            # no p-values
            d = {}
          else:
            p_values = [int(x) for x in line.split()]
            if len(p_values) != 11:
              raise RuntimeError(f"expected 11 p-values for graph level but got {len(p_values)}: {p_values}")
            d = {}
            p = 0
            for v in p_values:
              d[p] = v
              p += 10

          graph_level_conn_p_values[graph_level] = (graph_level_size, d)

      t1 = datetime.datetime.now()
      combined_run_time = t1 - t0
      print(f"  took {combined_run_time} to run KnnGraphTester")

      if summary is None:
        raise RuntimeError("could not find summary line in output!")

      if index_path is None:
        raise RuntimeError("could not find Index Path line in output!")

      job.wait()

      if job.returncode != 0:
        raise RuntimeError(f"command failed with exit {job.returncode}")

      all_summaries.append(summary)

      cols = summary.split("\t")

      assert len(cols) >= 21

      if cols[17] == "N/A":
        selectivity = None
      else:
        selectivity = float(cols[17])

      result = KNNResultV1(
        lucene_git_rev,
        luceneutil_git_rev,
        INDEX_VECTORS_FILE,
        SEARCH_VECTORS_FILE,
        VECTORS_DIM,
        do_force_merge,
        float(cols[0]),  # recall
        float(cols[1]),  # cpu_time_ms
        float(cols[2]),  # netCPU
        float(cols[3]),  # avgCpuCount
        int(cols[4]),  # num_docs
        int(cols[5]),  # top_k
        int(cols[6]),  # fanout
        int(cols[7]),  # max_conn
        int(cols[8]),  # beam_width
        cols[9],  # quantize_desc
        int(cols[10]),  # total_visited
        float(cols[11]),  # index_time_sec
        float(cols[12]),  # index_docs_per_sec
        float(cols[13]),  # force_merge_time_sec
        int(cols[14]),  # index_num_segments
        float(cols[15]),  # index_size_on_disk_mb
        selectivity,  # selectivity
        cols[16],  # filter-strategy
        float(cols[18]),  # vec_disk_mb
        float(cols[19]),  # vec_ram_mb
        graph_level_conn_p_values,  # graph_level_conn_p_values
        combined_run_time,  # time to run KnnGraphTester
      )
      print(f"result: {result}")
      all_results.append(result)

      if do_force_merge:
        # clean as we go -- these indices are biggish (~25-30 GB):
        print(f"now remove KNN index {index_path}")
        shutil.rmtree(index_path)

  skip_headers = {"filterSelectivity", "filterStrategy", "visited"}
  knnPerfTest.print_fixed_width([(x, None) for x in all_summaries], skip_headers)

  results_file = f"{results_dir}/knn_results.pk"
  print(f"saving all results to {results_file}")
  open(results_file, "wb").write(pickle.dumps(all_results))

  end_time_epoch_secs = time.time()
  print(f"done!  took {(end_time_epoch_secs - start_time_epoch_secs):.1f} seconds total")

  return all_results


if __name__ == "__main__":
  main()
