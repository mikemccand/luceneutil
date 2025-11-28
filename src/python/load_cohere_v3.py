import csv
import os
import struct
import subprocess
import time

import datasets
import numpy as np

csv.field_size_limit(1024 * 1024 * 40)

# TODO
#   - ugh -- need to fix shuffle to keep pages (all adjacent rows with same wiki_id) together
#   - finish/publish these new v3 vec sources, send email
#     - babysit nightly, then add annot
#   - test knn
#   - install for nightly knn, babysit
#   - what is "split"
#   - write meta file for parent/child join
#   - write title/id/other-features
#   - shuffle
#   - then make separate query + index files

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

ID_PREFIX = "20231101.en_"

DIMENSIONS = 1024

# total 15_562_116_893 chars in text
# total 851_864_089 chars in title

TOTAL_DOC_COUNT = 41_488_110  # + 1 header in the initial CSV


TOTAL_DOC_COUNT = 5_854_887

LANG = "en"


def run(command):
  print(f"RUN: {command}")
  t0 = time.time()
  subprocess.run(command, shell=True, check=True)
  print(f"  took {time.time() - t0:.1f} sec")


def main():
  # where we will download and write our shuffled vectors, before splitting into queries and docs
  csv_source_file = "/b3/cohere-wikipedia-v3.csv"
  vec_source_file = "/b3/cohere-wikipedia-v3.vec"

  if False:
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

  if False:
    row_count = 0
    dimensions = 1024
    headers = ["id", "title", "text", "url"]
    start_time_sec = time.time()
    # print('%s' % dir(docs['emb']))

    total_text_chars = 0
    total_title_chars = 0

    total_doc_count = 0
    cur_wiki_id = None

    with open(csv_source_file, "w") as meta_out, open(vec_source_file, "wb") as vec_out:
      meta_csv_out = csv.writer(meta_out)
      meta_csv_out.writerow(headers)
      for doc in docs:
        meta_csv_out.writerow([doc["_id"], doc["title"], doc["text"]])
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
        if row_count % 10000 == 0:
          print(f"{time.time() - start_time_sec:6.1f} sec: {row_count} ({total_doc_count} wiki docs)... {vec_out.tell()} and {meta_out.tell()}")

    print(f"Done initial download!\n  {row_count=} {total_doc_count=} {total_text_chars=} {total_title_chars=}")
    print(f"{csv_source_file} is {os.path.getsize(csv_source_file) / 1024 / 1024 / 1024:.2f} GB")
    print(f"{vec_source_file} is {os.path.getsize(vec_source_file) / 1024 / 1024 / 1024:.2f} GB")

    os.chmod(csv_source_file, 0o444)
    os.chmod(vec_source_file, 0o444)

    if row_count != TOTAL_DOC_COUNT:
      raise RuntimeError(f"expected {TOTAL_DOC_COUNT=} but saw {row_count=}")

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

    with open(f"{vec_source_file}.shuffled", "rb") as f:
      # sanity check: print first 10 vectors
      for i in range(10):
        b = f.read(DIMENSIONS * 4)
        one_vec = struct.unpack(f"<{DIMENSIONS}f", b)
        print(f"vec {i} is length {len(one_vec)}")
        sumsq = 0
        for i, v in enumerate(one_vec):
          print(f"  {i:4d}: {v:g}")
          sumsq += v * v
        print(f"  sumsq={sumsq}")

  if True:
    # split into docs/queries -- files are now shuffled so we can safely take first
    # N as queries and remainder as docs and we are pulling from the same well stirred
    # chicken soup (hmm, but gravity ... analogy doesn't fully work)

    # 1.5 M queries, the balance (TOTAL_DOC_COUNT - 1.5 M) = 39_988_110
    query_count = 1_500_000

    with open(csv_source_file + ".final") as meta_in, open(vec_source_file + ".shuffled", "rb") as vec_in:
      meta_csv_in = csv.reader(meta_in)

      # first, queries:
      copy_meta_and_vec(csv_source_file, vec_source_file, meta_csv_in, vec_in, "queries", query_count)

      # then docs:
      copy_meta_and_vec(csv_source_file, vec_source_file, meta_csv_in, vec_in, "docs", TOTAL_DOC_COUNT - query_count)

      if meta_in.tell() != os.path.getsize(csv_source_file):
        raise RuntimeError(f"did not consume all metadata file rows?  {meta_in.tell()} vs {os.path.getsize(csv_source_file)}")
      if vec_in.tell() != os.path.getsize(vec_source_file):
        raise RuntimeError(f"did not consume all vector file vectors?  {vec_in.tell()} vs {os.path.getsize(vec_source_file)}")


def split_id(id, line_num):
  if not id.startswith(ID_PREFIX):
    raise RuntimeError(f"all wiki_id should start with {ID_PREFIX=} but saw {id} at row {line_num}")
  tup = id[len(ID_PREFIX) :].split("_")
  if len(tup) != 2:
    raise RuntimeError(f"all wiki_id should start have form wiki-id_paragraph-id but saw {id[len(ID_PREFIX) :]} at row {line_num}")
  # TODO: should we further valdiate \d+ for each?  coalesced correctly ("see once" each wiki_id)
  return tup


def copy_meta_and_vec(csv_dest_file, vec_dest_file, meta_csv_in, vec_in, name, doc_copy_count):
  vector_size_bytes = DIMENSIONS * 4

  print(f'\nnow create subset "{name}" with {doc_copy_count} docs')

  # also remove common prefix from id (20231101.en_), and split the remainder into wiki_id and paragraph_id
  subset_csv_dest_file = csv_dest_file.replace(".csv", f".{name}.csv")
  subset_vec_dest_file = vec_dest_file.replace(".vec", f".{name}.vec")

  new_headers = ("wiki_id", "paragraph_id", "title", "text", "url")

  with open(subset_csv_dest_file, "w") as meta_out, open(subset_vec_dest_file, "wb") as vec_out:
    meta_csv_out = csv.writer(meta_out)
    meta_csv_out.writerow(new_headers)

    for i in range(doc_copy_count):
      # id, title, text, url
      row = next(meta_csv_in)

      wiki_id, paragraph_id = split_id(row[0], meta_csv_in.line_num)

      meta_csv_out.writerow([wiki_id, paragraph_id] + row[1:])
      vec = vec_in.read(vector_size_bytes)
      if len(vec) != vector_size_bytes:
        raise RuntimeError(f"failed to read expected {vector_size_bytes=}")
      vec_out.write(vec)

  print(
    f"done!\n  meta file {subset_csv_dest_file} is {os.path.getsize(subset_csv_dest_file) / 1024.0 / 1024.0} MB\n  vec file {subset_vec_dest_file} is {os.path.getsize(subset_vec_dest_file) / 1024.0 / 1024.0} MB"
  )


if __name__ == "__main__":
  main()
