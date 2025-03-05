import datetime
import os
import shutil

now = datetime.datetime.now()

os.chdir("/l/sparseTaxis.nightly/sparseTaxis/lucene.main")
if os.system("git checkout main"):
  raise RuntimeError("git checkout main failed")

if os.system("git pull origin main"):
  raise RuntimeError("git pull failed")

if os.system(
  "python3 -u /l/util.nightly/src/python/sparsetaxis/runBenchmark.py -rootDir /l/sparseTaxis.nightly -logDir /l/logs.nightly/taxis/%4d.%02d.%02d.%02d.%02d.%02d -luceneMain /l/sparseTaxis.nightly/sparseTaxis/lucene.main"
  % (now.year, now.month, now.day, now.hour, now.minute, now.second)
):
  raise RuntimeError("bench failed")

if os.system("python3 -u /l/util.nightly/src/python/sparsetaxis/writeGraph.py"):
  raise RuntimeError("bench failed")

for thing_name in os.listdir("/l/sparseTaxis.nightly/sparseTaxis/indices"):
  if thing_name.startswith("index."):
    full_path = f"/l/sparseTaxis.nightly/sparseTaxis/indices/{thing_name}"
    print(f"sparseTaxis: now remove {full_path}")
    shutil.rmtree(full_path)
