#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import os
import random

import datasets
import localconstants
import numpy as np

"""
Generate document and query vectors for the vector search task from
the Cohere/wikipedia-2023-11-embed-multilingual-v3 dataset.
https://huggingface.co/datasets/Cohere/wikipedia-2023-11-embed-multilingual-v3/viewer/en

Usage:

python src/python/infer_token_vectors_cohere.py -d <num_docs> -q <num_queries>
python src/python/infer_token_vectors_cohere.py -n <filename_prefix> -d <num_docs> -q <num_queries>

For help:
python src/python/infer_token_vectors_cohere.py -h
"""

# v2?
# DATASET_PATH = "Cohere/wikipedia-22-12-en-embeddings"
# DIMENSIONS = 768

# new V3:
LANG = "en"
DATASET_PATH = "Cohere/wikipedia-2023-11-embed-multilingual-v3"
# en has 41,488,110 vectors
# simple has 646,424 vectors
# _id, url, title, text, emb

DIMENSIONS = 1024
LANG = "en"

# Note: Shuffling will break the parent-join benchmarks because the metadata file expects entries to be ordered
DO_SHUFFLE = True
SHUFFLE_BUFFER_SIZE = 25000
SEED = random.randint(1, 512)


def fetch_cohere_vectors():
  parser = argparse.ArgumentParser(
    prog="Fetch Wikipedia Cohere Embeddings", description="Generate document and query vectors for the vector search task from HuggingFace Cohere/wikipedia-22-12-en-embeddings"
  )
  parser.add_argument("-n", "--name", default="cohere-wikipedia", help="Dataset name, used as a filename prefix for generated files.")
  parser.add_argument("-d", "--numDocs", default="1_000_000", help="Number of documents")
  parser.add_argument("-q", "--numQueries", default="10_000", help="Number of queries")
  args = parser.parse_args()
  print("Fetching Cohere embeddings with the following args: %s" % args)

  doc_file = f"{localconstants.BASE_DIR}/data/{args.name}-docs-{DIMENSIONS}d.vec"
  query_file = f"{localconstants.BASE_DIR}/data/{args.name}-queries-{DIMENSIONS}d.vec"
  meta_file = f"{localconstants.BASE_DIR}/data/{args.name}-metadata.csv"
  num_docs = int(args.numDocs)
  num_queries = int(args.numQueries)

  for name in (doc_file, query_file, meta_file):
    print(f"checking if file:{name} exists...")
    if os.path.exists(name):
      raise RuntimeError(f"please remove {name} first")

  ds = datasets.load_dataset(DATASET_PATH, LANG, split="train", streaming=True)
  shuffled_ds = ds.shuffle(buffer_size=SHUFFLE_BUFFER_SIZE, seed=SEED)
  ds_iter = iter(shuffled_ds)

  written = 0
  status_check = min(0.1 * num_docs, 100_000)
  with open(meta_file, "w") as meta, open(doc_file, "wb") as out_f, open(query_file, "wb") as out_f_queries:
    meta.write("wiki_id,para_id\n")
    for row in ds_iter:
      # assert dataset sanity with first row
      if written == 0:
        emb_dims = len(row["emb"])
        print(f"embedding dimensions = {emb_dims}")
        assert emb_dims == DIMENSIONS, f"Dataset embedding dimensions: {emb_dims} do not match configured dimensions: {DIMENSIONS}"
      emb = np.array(row["emb"], dtype=np.float32)
      emb.tofile(out_f)
      id_vals = row["_id"].split("_")
      wiki_id = id_vals[1]
      para_id = id_vals[2]
      meta.write(f"{wiki_id},{para_id}\n")
      written += 1
      if written % status_check == 0:
        print(f"written {written} embeddings")
      if written == num_docs:
        break
    print(f"completed writing {written} document embeddings")

    written = 0
    for row in ds_iter:
      q_emb = np.array(row["emb"], dtype=np.float32)
      q_emb.tofile(out_f_queries)
      written += 1
      if written == num_queries:
        break
    print(f"completed writing {num_queries} query embeddings")

  print("download completed. verifying artifacts...")
  embs_docs = np.fromfile(doc_file, dtype=np.float32)
  embs_docs = embs_docs.reshape(num_docs, DIMENSIONS)
  print(f"reading docs of shape: {embs_docs.shape}")
  print(f"{embs_docs[0]}")
  assert embs_docs.shape[0] == num_docs and embs_docs.shape[1] == DIMENSIONS

  embs_queries = np.fromfile(query_file, dtype=np.float32)
  embs_queries = embs_queries.reshape(num_queries, DIMENSIONS)
  print(f"reading queries shape: {embs_queries.shape}")
  print(f"{embs_queries[0]}")
  assert embs_queries.shape[0] == num_queries and embs_queries.shape[1] == DIMENSIONS

  print("reading metadata file")
  with open(meta_file) as meta:
    md = meta.readlines()
  print(f"metadata file row_count: {len(md)}, includes 1 header line")
  assert len(md) == num_docs + 1
  for line in md[:11]:
    print(line.rstrip("\n"))  # print() adds another newline


if __name__ == "__main__":
  fetch_cohere_vectors()
