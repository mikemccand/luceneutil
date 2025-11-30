import csv
import hashlib
import os
import struct
import subprocess
import time

import datasets
import numpy as np

from shuffle_wiki_ids import split_id, split_non_prefix_id, read_exact, TOTAL_DOC_COUNT, TOTAL_PARAGRAPH_COUNT

# else we hit: _csv.Error: field larger than field limit (131072)
csv.field_size_limit(1024 * 1024 * 40)

# TODO
#   - write up overview ocf how this tool works / how corpus is designed
#   - how to use the text?  make matching wiki linefiledocs source?
#   - strip common prefix from ids
#   - ugh -- need to fix shuffle to keep pages (all adjacent rows with same wiki_id) together
#   - validator: aslo check paragraph counts
#   - finish/publish these new v3 vec sources, send email
#     - babysit nightly, then add annot
#   - test knn
#   - install for nightly knn, babysit
#   - what is "split"
#   - write meta file for parent/child join
#   - write title/id/other-features
#   - shuffle
#   - then make separate query + index files
#   - add random end validator: confirm the right vectors ended with the right lines

"""
This tool downloads all metadata + vectors from
https://huggingface.co/datasets/Cohere/wikipedia-2023-11-embed-multilingual-v3
(Cohere v3 Wikipedia embeddings).

We download just lang="en" and split="train".

This is 41_488_110 rows, but each row is one paragraph from a wiki
page and (in general) multiple paragraphs per page.  There are
5_854_887 unique pages, so ~7.1 paragraphs per page on average.

Vectors are 1024 dimensions (up from 768 in Cohere v2), float32, and
seem to be unit-sphere normalized (at least the first 10 vectors are).

Unlike the v2 Cohere Wikipedia vectors
(https://huggingface.co/datasets/Cohere/wikipedia-22-12), these docs
do not have a stated sort order.  Still, we shuffle them to remove any
possible hidden compass bias (see
https://github.com/mikemccand/luceneutil/issues/494).
"""

# stats: row_count=41488110 total_doc_count=5854887 total_text_chars=15_562_116_893 total_title_chars=851_864_089

DIMENSIONS = 1024

LANG = "en"

DO_INIT_LOAD = False
DO_SHUFFLE = False
DO_PARTITION = False
DO_VALIDATE = True

def run(command):
  print(f"RUN: {command}")
  t0 = time.time()
  subprocess.run(command, shell=True, check=True)
  print(f"  took {time.time() - t0:.1f} sec")

def to_gb(b):
  return b / 1024 / 1024 / 1024

def main():
  # where we will download and write our shuffled vectors, before splitting into queries and docs
  csv_source_file = '/b3/take2/cohere-wikipedia-v3.csv'
  vec_source_file = '/b3/take2/cohere-wikipedia-v3.vec'

  # on a different mount point so the heavy read / write are split across hard drives
  csv_shuffled_file = '/b2/coherev3/shuffled.csv'
  vec_shuffled_file = '/b2/coherev3/shuffled.vec'
  
  if DO_INIT_LOAD:
    # takes a long time!  ~3.2 hours
    docs = datasets.load_dataset("Cohere/wikipedia-2023-11-embed-multilingual-v3", LANG, split="train", streaming=True)
    # print(f'columns: {docs.column_names}')

    features = docs.features
    print("\nfeatures:")
    for feature_name, feature_details in features.items():
      print(f"  {feature_name}: {feature_details}")
    print("\n")

    if False:
      for feature_name, feature_details in features.items():
        print(f"  Feature Name: {feature_name}")
        print(f"  Feature Type: {type(feature_details).__name__}")
        if hasattr(feature_details, "dtype"):
          print(f"  Data Type: {feature_details.dtype}")

        # Check for List type (also handles older Sequence type if needed)
        if isinstance(feature_details, datasets.Array2D):
          print(f"- Column: '{feature_name}' is a fixed-size Array.")
          # The shape attribute will provide the dimensions
          print(f"  - Shape: {feature_details.shape}, Dtype: {feature_details.dtype}")

        elif isinstance(feature_details, (datasets.List, datasets.Sequence)):
          inner_type = feature_details.feature
          # Check if the inner type is a Value and get its dtype
          if isinstance(inner_type, datasets.Value):
            print(f"  - Type: List (inner dtype: {inner_type.dtype})")
          else:
            # Handle nested structures within the list (e.g., List of dicts)
            print(f"  - Type: List (inner type details: {inner_type})")

        # Check for simple Value type (like float32, int32, string)
        elif isinstance(feature_details, datasets.Value):
          if feature_details.dtype in ["float32", "float"]:
            print(f"  - Type: Value (dtype: {feature_details.dtype})")
          else:
            print(f"  - Type: Value (dtype: {feature_details.dtype})")

        else:
          # Handle other types like ClassLabel, Array, etc.
          print(f"  - Type: Other (details: {type(feature_details).__name__})")

        # You can add more specific checks for nested features or specific types if needed
        print("-" * 20)
    
    if True:
      # do the actual (slow!) slurping of the full corpus from HuggingFace, down to local csv/vec file:
      row_count = 0
      dimensions = 1024
      headers = ["id", "title", "text", "url"]
      start_time_sec = time.time()
      # print('%s' % dir(docs['emb']))

      total_text_chars = 0
      total_title_chars = 0

      total_doc_count = 0
      cur_wiki_id = None

      next_print_time_sec = start_time_sec
      with open(csv_source_file, "w", newlines='') as meta_out, open(vec_source_file, "wb") as vec_out:
        meta_csv_out = csv.writer(meta_out, lineterminator='\n')
        meta_csv_out.writerow(headers)
        for doc in docs:
          meta_csv_out.writerow([doc["_id"], doc["title"], doc["text"], doc["url"]])
          total_text_chars += len(doc["text"])
          total_title_chars += len(doc["title"])
          wiki_id, paragraph_id = split_id(doc["_id"], row_count)

          if wiki_id != cur_wiki_id:
            total_doc_count += 1
            cur_wiki_id = wiki_id

          emb = np.array(doc["emb"], dtype=np.float32)

          if len(emb) != DIMENSIONS:
            raise RuntimeError(f"planned on {DIMENSIONS} dims but corpus is {len(emb)}!")
          # print(f'{type(emb)}')
          emb.tofile(vec_out)
          row_count += 1
          now_sec = time.time()
          if now_sec > next_print_time_sec:
            pct = row_count * 100 / TOTAL_PARAGRAPH_COUNT
            print(f'{now_sec - start_time_sec:6.1f} sec: {pct:.2f}% ({row_count} rows) ({total_doc_count} wiki docs)... vec {to_gb(vec_out.tell()):.2f} G, meta {to_gb(meta_out.tell()):.2f} G')
            next_print_time_sec += 5.0

        print(f'{now_sec - start_time_sec:6.1f} sec: {row_count} ({total_doc_count} wiki docs)... {vec_out.tell()} and {meta_out.tell()}')

      print(f"Done initial download!\n  {row_count=} {total_doc_count=} {total_text_chars=} {total_title_chars=}")
      print(f"{csv_source_file} is {os.path.getsize(csv_source_file) / 1024 / 1024 / 1024:.2f} GB")
      print(f"{vec_source_file} is {os.path.getsize(vec_source_file) / 1024 / 1024 / 1024:.2f} GB")

      os.chmod(csv_source_file, 0o444)
      os.chmod(vec_source_file, 0o444)

      shutil.copyfile(csv_source_file, '/lucenedata/enwiki/cohere-v3/init.csv')
      shutil.copyfile(vec_source_file, '/lucenedata/enwiki/cohere-v3/init.vec')
      
      os.chmod('/lucenedata/enwiki/cohere-v3/init.csv', 0o444)
      os.chmod('/lucenedata/enwiki/cohere-v3/init.vec', 0o444)

      if row_count != TOTAL_DOC_COUNT:
        raise RuntimeError(f"expected {TOTAL_DOC_COUNT=} but saw {row_count=}")

  if DO_SHUFFLE:

    if False:
      print("strip csv header")
      run(f"sed '1d' {csv_source_file} > {csv_source_file}.noheader")
      
      print("now insert line numbers")
      run(f"nl -v 0 -ba {csv_source_file}.noheader > {csv_source_file}.num")

      print("now shuffle")
      run(f"shuf {csv_source_file}.num > {csv_source_file}.num.shuf")
 
      # this is the actual output meta CSV, post shuffle
      print("now remove line numbers")
      run(f"cut -f 2- {csv_source_file}.num.shuf > {csv_source_file}.final")

      print("now sort to get reverse mapping")
      run(f"nl -v 0 -ba {csv_source_file}.num.shuf | sort -nk2 > {csv_source_file}.mapping")

      print("now cut to just the one new-position column")
      run(f"cut -f1 {csv_source_file}.mapping > {csv_source_file}.only_mapping")

      # this is the actual output vectors, post same shuffle -- this took FOREVER (took 42733.5 sec)
      print("now shuffle vectors to match")
      run(f"python3 -u src/python/shuffle_vec_file.py {vec_source_file} {vec_source_file}.shuffled {DIMENSIONS} {csv_source_file}.only_mapping")

    # Shuffle wiki_ids while keeping all paragraphs coalesced (each wiki_id keeps all of its paragraphs together),
    # for realisic testing of parent/join or multi-valued vectors:
    print(f'Shuffling wiki_ids (keeping paragraphs together)...')

    run(f'python3 -u src/python/shuffle_wiki_ids.py {csv_source_file} {vec_source_file} {DIMENSIONS} {csv_shuffled_file} {vec_shuffled_file}')

    with open(vec_shuffled_file, "rb") as f:
      # sanity check: print first 10 vectors
      for i in range(10):
        b = f.read(DIMENSIONS * 4)
        one_vec = struct.unpack(f"<{DIMENSIONS}f", b)
        print(f"vec {i} is length {len(one_vec)}")
        sumsq = 0
        for i, v in enumerate(one_vec):
          # print(f"  {i:4d}: {v:g}")
          sumsq += v * v
        print(f"  sumsq={sumsq}")

  if DO_PARTITION:
    # split into queries/docs -- files are now shuffled so we can safely take first
    # N wiki_ids as queries and remainder as docs, while keeping all paragraphs of
    # each wiki_id together

    query_wiki_id_count = 250_000
    partition_documents(csv_shuffled_file, vec_shuffled_file, csv_source_file, vec_source_file, query_wiki_id_count)

  if DO_VALIDATE:
    queries_csv_file = csv_source_file.replace(".csv", ".queries.csv")
    queries_vec_file = vec_source_file.replace(".vec", ".queries.vec")
    docs_csv_file = csv_source_file.replace(".csv", ".docs.csv")
    docs_vec_file = vec_source_file.replace(".vec", ".docs.vec")
    validate_vector_integrity(csv_source_file, vec_source_file,
                              [(queries_csv_file, queries_vec_file),
                               (docs_csv_file, docs_vec_file)])

def compute_wiki_id_vector_hashes(csv_file, vec_file, is_full_id=False, expected_total_vectors=None):
  """compute SHA256 hash of all vectors grouped by wiki_id for integrity checking.

  Args:
    csv_file: path to CSV file (with wiki_id in first column)
    vec_file: path to binary vector file
    expected_total_vectors: optional expected total vector count for percentage reporting

  Returns:
    dict mapping wiki_id -> SHA256 hash of concatenated vectors for that wiki_id
  """
  vector_size_bytes = DIMENSIONS * 4
  wiki_id_hashes = {}

  start_time_sec = time.time()
  next_progress_time_sec = start_time_sec + 5

  with open(csv_file, 'r', encoding='utf-8', newline='') as csv_f, \
       open(vec_file, 'rb') as vec_f:
    csv_reader = csv.reader(csv_f, lineterminator='\n')

    # skip header
    header = next(csv_reader)
    if is_full_id:
      # original input CSV
      assert header == ['id', 'title', 'text', 'url']

    current_wiki_id = None
    wiki_id_vector_bytes = b''
    vector_count_for_wiki = 0
    total_vectors = 0

    for csv_row in csv_reader:
      # extract wiki_id from first column
      wiki_id = csv_row[0]
      if is_full_id:
        wiki_id, para_id = split_id(wiki_id, line_num = csv_reader.line_num)

      # read corresponding vector bytes
      vec_bytes = read_exact(vec_f, vector_size_bytes, 'vector')
      total_vectors += 1

      # if we've hit a new wiki_id, hash and store the previous one
      if current_wiki_id is not None and wiki_id != current_wiki_id:
        # hash all vectors for this wiki_id
        wiki_id_hash = hashlib.sha256(wiki_id_vector_bytes).hexdigest()

        assert current_wiki_id not in wiki_id_hashes
        wiki_id_hashes[current_wiki_id] = wiki_id_hash

        now_sec = time.time()
        if now_sec >= next_progress_time_sec:
          elapsed_sec = now_sec - start_time_sec
          if expected_total_vectors is not None:
            pct = 100.0 * total_vectors / expected_total_vectors
            print(f'  hashing: {len(wiki_id_hashes)} wiki_ids, {total_vectors} vectors ({pct:.1f}%) ({elapsed_sec:.1f} sec)...')
          else:
            print(f'  hashing: {len(wiki_id_hashes)} wiki_ids, {total_vectors} vectors ({elapsed_sec:.1f} sec)...')
          next_progress_time_sec = now_sec + 5

        wiki_id_vector_bytes = b''
        vector_count_for_wiki = 0

      current_wiki_id = wiki_id
      wiki_id_vector_bytes += vec_bytes
      vector_count_for_wiki += 1

    # hash the last wiki_id
    if current_wiki_id is not None:
      wiki_id_hash = hashlib.sha256(wiki_id_vector_bytes).hexdigest()
      assert current_wiki_id not in wiki_id_hashes
      wiki_id_hashes[current_wiki_id] = wiki_id_hash

    elapsed_sec = time.time() - start_time_sec
    if expected_total_vectors is not None:
      pct = 100.0 * total_vectors / expected_total_vectors
      print(f'  hashing: {len(wiki_id_hashes)} wiki_ids, {total_vectors} vectors ({pct:.1f}%) ({elapsed_sec:.1f} sec)')
    else:
      print(f'  hashing: {len(wiki_id_hashes)} wiki_ids, {total_vectors} vectors (100.0%) ({elapsed_sec:.1f} sec)')

  return wiki_id_hashes

def validate_vector_integrity(input_csv, input_vec, output_files_list):
  """validate that all vectors were preserved correctly through shuffling/partitioning."""
  print(f'Validating vector integrity...')

  # compute hashes from input (expect full corpus)
  print(f'  computing input file hashes...')
  input_hashes = compute_wiki_id_vector_hashes(input_csv, input_vec, is_full_id=True, expected_total_vectors=TOTAL_PARAGRAPH_COUNT)
  print(f'  input: {len(input_hashes)} unique wiki_ids')

  # compute hashes from all output files (no total expected since they may be partitioned)
  print(f'  computing output file hashes...')
  output_hashes = {}
  for csv_file, vec_file in output_files_list:
    if os.path.exists(csv_file) and os.path.exists(vec_file):
      file_hashes = compute_wiki_id_vector_hashes(csv_file, vec_file, is_full_id=False)
      # TODO: fails to catch dups across query/docs?
      inter = set(output_hashes.keys()) & set(file_hashes.keys())
      if len(inter) > 0:
        raise RuntimeError(f'output file {csv_file} has {len(inter)} overlapping keys with previous output file')
      output_hashes.update(file_hashes)
  print(f'  output: {len(output_hashes)} unique wiki_ids')

  # verify all wiki_ids are present
  input_wiki_ids = set(input_hashes.keys())
  output_wiki_ids = set(output_hashes.keys())

  missing = input_wiki_ids - output_wiki_ids
  extra = output_wiki_ids - input_wiki_ids

  if missing:
    raise RuntimeError(f'ERROR: missing {len(missing)} wiki_ids from output')

  if extra:
    raise RuntimeError(f'ERROR: {len(extra)} extra wiki_ids in output')

  # verify hashes match
  mismatches = []
  for wiki_id in input_wiki_ids:
    if input_hashes[wiki_id] != output_hashes[wiki_id]:
      mismatches.append(wiki_id)

  if mismatches:
    print(f'ERROR: {len(mismatches)} wiki_ids have mismatched vector hashes')
    if len(mismatches) <= 10:
      for wiki_id in mismatches:
        print(f'  {wiki_id}: input={input_hashes[wiki_id][:16]}... output={output_hashes[wiki_id][:16]}...')
    raise RuntimeError(f'ERROR: {len(mismatches)} wiki_ids have mismatched vector hashes')

  print(f'SUCCESS: all {len(input_wiki_ids)} wiki_ids with correct vector hashes preserved')
  return True

def partition_documents(csv_shuffled_file, vec_shuffled_file, csv_source_file, vec_source_file, query_wiki_id_count):
  """partition shuffled corpus into queries and docs in a single pass, keeping all paragraphs of each wiki_id together."""
  print(f'partitioning into queries (first {query_wiki_id_count} wiki_ids) and docs (remainder)...')

  queries_csv_file = csv_source_file.replace(".csv", ".queries.csv")
  queries_vec_file = vec_source_file.replace(".vec", ".queries.vec")
  docs_csv_file = csv_source_file.replace(".csv", ".docs.csv")
  docs_vec_file = vec_source_file.replace(".vec", ".docs.vec")

  vector_size_bytes = DIMENSIONS * 4
  new_headers = ("wiki_id", "paragraph_count", "paragraph_id", "title", "text", "url")

  start_time_sec = time.time()
  next_progress_time_sec = start_time_sec + 5

  # Open all files once
  with open(csv_shuffled_file, "r", newline='') as csv_in, \
       open(vec_shuffled_file, "rb") as vec_in, \
       open(queries_csv_file, "w", newline='') as queries_csv_out, \
       open(queries_vec_file, "wb") as queries_vec_out, \
       open(docs_csv_file, "w", newline='') as docs_csv_out, \
       open(docs_vec_file, "wb") as docs_vec_out:

    csv_in_reader = csv.reader(csv_in, lineterminator='\n')

    # skip header in input
    next(csv_in_reader)

    # write headers to both outputs
    queries_csv_writer = csv.writer(queries_csv_out, lineterminator='\n')
    queries_csv_writer.writerow(new_headers)
    docs_csv_writer = csv.writer(docs_csv_out, lineterminator='\n')
    docs_csv_writer.writerow(new_headers)

    current_wiki_id = None
    wiki_id_count = 0
    query_row_count = 0
    docs_row_count = 0
    total_row_count = 0
    writing_to_queries = True

    for csv_row in csv_in_reader:
      # parse wiki_id from full id
      full_id = csv_row[0]
      wiki_id, paragraph_id = split_non_prefix_id(full_id, total_row_count + 1)

      # if we hit a new wiki_id, increment the counter
      if wiki_id != current_wiki_id:
        wiki_id_count += 1
        current_wiki_id = wiki_id

        # if we've crossed the threshold, switch to docs
        if wiki_id_count > query_wiki_id_count and writing_to_queries:
          writing_to_queries = False
          print(f'  switched to docs after {wiki_id_count - 1} wiki_ids ({query_row_count} rows)')

      # read corresponding vector
      vec_bytes = read_exact(vec_in, vector_size_bytes, 'vector')

      # write to appropriate output
      output_row = [wiki_id, csv_row[1], paragraph_id] + csv_row[2:]

      if writing_to_queries:
        queries_csv_writer.writerow(output_row)
        queries_vec_out.write(vec_bytes)
        query_row_count += 1
      else:
        docs_csv_writer.writerow(output_row)
        docs_vec_out.write(vec_bytes)
        docs_row_count += 1

      total_row_count += 1

      now_sec = time.time()
      if now_sec >= next_progress_time_sec:
        elapsed_sec = now_sec - start_time_sec
        if writing_to_queries:
          pct = 100.0 * (wiki_id_count - 1) / query_wiki_id_count
        else:
          pct = 100.0 * (wiki_id_count - query_wiki_id_count) / (TOTAL_DOC_COUNT - query_wiki_id_count)
        print(f'  {wiki_id_count} wiki_ids ({pct:.1f}%), {total_row_count} total rows ({elapsed_sec:.1f} sec)...')
        next_progress_time_sec = now_sec + 5

  elapsed_sec = time.time() - start_time_sec
  total_wiki_ids = wiki_id_count

  print(f'  done! {total_wiki_ids} total wiki_ids: {query_wiki_id_count} queries ({query_row_count} rows), {total_wiki_ids - query_wiki_id_count} docs ({docs_row_count} rows) ({elapsed_sec:.1f} sec)')
  print(f'  queries:')
  print(f'    csv: {os.path.getsize(queries_csv_file) / 1024.0 / 1024.0 / 1024.0:.2f} GB')
  print(f'    vec: {os.path.getsize(queries_vec_file) / 1024.0 / 1024.0 / 1024.0:.2f} GB')
  print(f'  docs:')
  print(f'    csv: {os.path.getsize(docs_csv_file) / 1024.0 / 1024.0 / 1024.0:.2f} GB')
  print(f'    vec: {os.path.getsize(docs_vec_file) / 1024.0 / 1024.0 / 1024.0:.2f} GB')

if __name__ == "__main__":
  main()
