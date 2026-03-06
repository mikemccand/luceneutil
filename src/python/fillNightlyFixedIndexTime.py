import bz2
import multiprocessing
import os
import pickle
import re
import tarfile

lock = multiprocessing.Lock()

RE_FINISHED = re.compile(r'^Indexer: finished \((.*) msec\)')
RE_BYTES = re.compile(r'^Indexer: net bytes indexed (.*)$')


def extract_one_file(tar_file_name):
  cache_file = os.path.split(tar_file_name)[0] + "/fixed_index_time.txt"
  if os.path.exists(cache_file):
    return

  # First try to get values from results.pk (available since 2026-02-20)
  results_pk = os.path.split(tar_file_name)[0] + "/results.pk"
  if os.path.exists(results_pk):
    try:
      tup = pickle.loads(open(results_pk, "rb").read(), encoding="bytes")
      if len(tup) > 19:
        fixed_index_time = tup[18]
        fixed_index_bytes = tup[19]
        if fixed_index_time is not None and fixed_index_bytes is not None:
          with open(cache_file, "w") as out:
            out.write(f"timeSec={fixed_index_time}\n")
            out.write(f"bytesIndexed={fixed_index_bytes}\n")
          lock.acquire()
          try:
            print(f"\n{tar_file_name}: fixed index time {fixed_index_time:.1f}s, bytes {fixed_index_bytes} (from pickle)")
          finally:
            lock.release()
          return
    except Exception:
      pass

  # Fallback: parse fixedIndex.log from the tar archive
  try:
    with tarfile.open(fileobj=bz2.open(tar_file_name)) as t:
      if "fixedIndex.log" in t.getnames():
        with t.extractfile("fixedIndex.log") as f:
          time_msec = None
          bytes_indexed = None
          for line in f.readlines():
            line = line.strip().decode("utf-8")
            m = RE_FINISHED.match(line)
            if m is not None:
              time_msec = float(m.group(1))
            m = RE_BYTES.match(line)
            if m is not None:
              bytes_indexed = int(m.group(1))
          if time_msec is not None and bytes_indexed is not None:
            time_sec = time_msec / 1000.0
            lock.acquire()
            try:
              print(f"\n{tar_file_name}: fixed index time {time_sec:.1f}s, bytes {bytes_indexed} (from log)")
            finally:
              lock.release()
            with open(cache_file, "w") as out:
              out.write(f"timeSec={time_sec}\n")
              out.write(f"bytesIndexed={bytes_indexed}\n")
  except Exception:
    print(f"FAILED on {tar_file_name}")
    raise


def read_cache_file(cache_file):
  """Read a fixed_index_time.txt cache file and return (timeSec, bytesIndexed) or None."""
  try:
    with open(cache_file) as f:
      vals = {}
      for line in f:
        line = line.strip()
        if "=" in line:
          key, value = line.split("=", 1)
          vals[key] = value
      time_sec = float(vals["timeSec"])
      bytes_indexed = int(vals["bytesIndexed"])
      return time_sec, bytes_indexed
  except Exception:
    return None


if __name__ == "__main__":
  todo = []
  for dirName in sorted(os.listdir("/l/logs.nightly")):
    resultsFile = f"/l/logs.nightly/{dirName}/logs.tar.bz2"
    if os.path.exists(resultsFile):
      todo.append(resultsFile)

  with multiprocessing.Pool(64) as p:
    p.map(extract_one_file, todo)
