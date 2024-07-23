import argparse
import os
import datasets
import numpy as np
import sys

import localconstants

"""
Generate document and query vectors for the vector search task from 
the Cohere/wikipedia-22-12-en-embeddings https://huggingface.co/datasets/Cohere/wikipedia-22-12-en-embeddings dataset. 

Usage: 

python src/python/infer_token_vectors_cohere.py <result_docs_vector_file> <num_docs> <result_queries_vector_file> <num_queries>

python src/python/infer_token_vectors_cohere.py ../data/cohere-wikipedia-768.vec 1000000 \
    ../data/cohere-wikipedia-queries-768.vec 10000
"""

# filename = sys.argv[1]
# num_docs = int(sys.argv[2])
# filename_queries= sys.argv[3]
# num_queries = int(sys.argv[4])
DATASET_PATH = 'Cohere/wikipedia-22-12-en-embeddings'
DIMENSIONS = 768

def fetch_cohere_vectors():
  parser = argparse.ArgumentParser(prog='Fetch Wikipedia Cohere Embeddings',
                                     description='Generate document and query vectors for the vector search task '
                                                 'from HuggingFace Cohere/wikipedia-22-12-en-embeddings')
  parser.add_argument('-n', '--name', default='cohere-wikipedia',
                      help='Dataset name, used as a filename prefix for generated files.')
  parser.add_argument('-d', '--numDocs', default='1_000_000', help='Number of documents')
  parser.add_argument('-q', '--numQueries', default='10_000', help='Number of queries')
  args = parser.parse_args()
  print('Fetching Cohere embeddings with the following args: %s' % args)

  doc_file = f"{localconstants.BASE_DIR}/data/{args.name}-docs-{DIMENSIONS}d.vec"
  query_file = f"{localconstants.BASE_DIR}/data/{args.name}-queries-{DIMENSIONS}d.vec"
  meta_file = f"{localconstants.BASE_DIR}/data/{args.name}-metadata.csv"
  num_docs = int(args.numDocs)
  num_queries = int(args.numQueries)

  for name in (doc_file, query_file, meta_file):
    print(f'checking if file:{name} exists...')
    if os.path.exists(name):
        raise RuntimeError(f'please remove {name} first')

  ds = datasets.load_dataset(DATASET_PATH, split="train")
  print(f'features: {ds.features}')
  print(f"total number of rows: {len(ds)}")
  embedding_dims = len(ds[0]['emb'])
  print(f"embeddings dims: {embedding_dims}")
  assert embedding_dims == DIMENSIONS , f'Dataset embedding dimensions: {embedding_dims} do not match configured dimensions: {DIMENSIONS}'

  with open(meta_file, "at") as meta:
    meta.write(f'wiki_id,para_id\n')

  # do this in windows, else the RAM usage is crazy (OOME even with 256
  # GB RAM since I think this step makes 2X copy of the dataset?)
  doc_upto = 0
  window_num_docs = 1000000
  # window_num_docs = 100000

  # Fetch Document Embeddings
  while doc_upto < num_docs:
    next_doc_upto = min(doc_upto + window_num_docs, num_docs)
    ds_embs = ds[doc_upto:next_doc_upto]['emb']
    # wiki_id, paragraph_id, emb
    ds_wiki_id = ds[doc_upto:next_doc_upto]['wiki_id']
    ds_para_id = ds[doc_upto:next_doc_upto]['paragraph_id']
    batch_size = next_doc_upto - doc_upto
    print(f'batch size = {batch_size}')
    #print(f'\nds_embs: {ds_embs}')
    embs = np.array(ds_embs, dtype=np.float32)
    print(f'embs: {embs.dtype} {embs.size} {embs.itemsize} {embs.shape}')
    print(f'wiki_id: {ds_wiki_id.shape}')
    print(f'para_id: {ds_para_id.shape}')

    print(f"saving docs[{doc_upto}:{next_doc_upto}] of shape: {embs.shape} to file")
    with open(doc_file, "ab") as out_f:
        embs.tofile(out_f)

    print(f'writing associated metadata')
    with open(meta_file, "at") as meta:
      for i in range(batch_size):
        meta.write(f'{ds_wiki_id[i]},{ds_para_id[i]}\n')

    doc_upto = next_doc_upto

  # Fetch Query Embeddings
  ds_embs_queries = ds[num_docs : num_docs + num_queries]['emb']
  embs_queries = np.array(ds_embs_queries, dtype=np.float32)
  print(f"saving queries of shape: {embs_queries.shape} to file")
  with open(query_file, "w") as out_f_queries:
      embs_queries.tofile(out_f_queries)

  ### check saved datasets
  embs_docs = np.fromfile(doc_file, dtype=np.float32)
  embs_docs = embs_docs.reshape(num_docs, DIMENSIONS)
  print(f"reading docs of shape: {embs_docs.shape}")
  print(f'{embs_docs[0]}')

  embs_queries = np.fromfile(query_file, dtype=np.float32)
  embs_queries = embs_queries.reshape(num_queries, DIMENSIONS)
  print(f"reading queries shape: {embs_queries.shape}")
  print(f'{embs_queries[0]}')

  print(f'reading metadata file')
  with open(meta_file, 'rt') as meta:
    md = meta.readlines()
  print(f'metadata file row_count: {len(md)}, includes 1 header line')
  print(md[:4])

if __name__ == '__main__':
  fetch_cohere_vectors()