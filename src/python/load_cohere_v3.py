import struct
import subprocess
import time
from datasets import load_dataset
import csv
import struct
import numpy as np

# TODO
#   - finish/publish these new v3 vec sources, send email
#     - babysit nightly, then add annot
#   - run nightly knn
#   - KnnIndexer should support two commit points in one index -- not-force-merged, force-merged
#   - create auto-committer Lucene "plugin" -- wraps Directory and MergeScheduler?
#     - open upstream issue
#     - could be used here to have many commit points in-between not-force-merged and force-merged, to
#       make it easier to see effect of N segment on perf/recall ROC
#   - is corpus natively stored/downloaded as float32 or maybe float64?
#   - build IO profiler directory wrapper
#   - build off-heap version mapping
#   - upgrade to v3
#   - what is "split"
#   - write meta file for parent/child join
#   - write title/id/other-features
#   - shuffle
#   - then make separate query + index files
#   - derive dims from the model

DIMENSIONS = 1024

LANG = 'en'

def run(command):
  print(f'RUN: {command}')
  t0 = time.time()
  subprocess.run(command, shell=True, check=True)
  print(f'  took {time.time() - t0:.1f} sec')

def main():
  csv_source_file = '/b3/cohere-wikipedia-v3.csv'
  vec_source_file = '/b3/cohere-wikipedia-v3.vec'
  
  if True:
    docs = load_dataset("Cohere/wikipedia-2023-11-embed-multilingual-v3", LANG, split="train", streaming=True)
    print(f'columns: {docs.column_names}')
    print(f'features: {docs.column_names}')

  if False:
    count = 0
    dimensions = 1024
    headers = ['id', 'title', 'text', 'url']
    start_time_sec = time.time()
    # print('%s' % dir(docs['emb']))
    # should be 41488110 rows + 1 header
    with open(csv_source_file, 'w') as meta_out, open(vec_source_file, 'wb') as vec_out:
      meta_csv_out = csv.writer(meta_out)
      meta_csv_out.writerow(headers)
      for doc in docs:
        meta_csv_out.writerow([doc['_id'], doc['title'], doc['text']])
        emb = np.array(doc['emb'], dtype=np.float32)
        if len(emb) != DIMENSIONS:
          raise RuntimeError(f'planned on {DIMENSIONS} dims but corpus is {len(emb)}!')
        # print(f'{type(emb)}')
        emb.tofile(vec_out)
        count += 1
        if count % 10000 == 0:
          print(f'{time.time() - start_time_sec:6.1f} sec: {count}... {vec_out.tell()} and {meta_out.tell()}')
          
  print(f'now insert line numbers')
  run(f'nl -v 0 -ba {csv_source_file} > {csv_source_file}.num')

  print(f'now shuffle')
  run(f'shuf {csv_source_file}.num > {csv_source_file}.num.shuf')

  print(f'now remove line numbers')
  run(f'cut -f 2- {csv_source_file}.num.shuf > {csv_source_file}.final')

  print(f'now sort to get reverse mapping')
  run(f'nl -v 0 -ba {csv_source_file}.num.shuf | sort -nk2 > {csv_source_file}.mapping')

  print(f'now cut to just the one new-position column')
  run(f'cut -f1 {csv_source_file}.mapping > {csv_source_file}.only_mapping')

  print(f'now shuffle vectors matching')
  run(f'python3 -u src/python/shuffle_vec_file.py {vec_source_file} {vec_source_file}.shuffled {DIMENSIONS} {csv_source_file}.only_mapping')

  with open(f'{vec_source_file}.shuffled', 'rb') as f:
    b = f.read(DIMENSIONS * 4)
    one_vec = struct.unpack(f'<{DIMENSIONS}f', b)
    print(f'vec is length {len(one_vec)}')
    sumsq = 0
    for i, v in enumerate(one_vec):
      print(f'  {i:4d}: {v:g}')
      sumsq += v*v
    print(f'  sumsq={sumsq}')

if __name__ == '__main__':
  main()
