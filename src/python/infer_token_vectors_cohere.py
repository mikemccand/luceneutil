import os
import datasets
import numpy as np
import sys


"""
Generate document and query vectors for the vector search task from 
the Cohere/wikipedia-22-12-en-embeddings https://huggingface.co/datasets/Cohere/wikipedia-22-12-en-embeddings dataset. 

Usage: 

python src/python/infer_token_vectors_cohere.py <result_docs_vector_file> <num_docs> <result_queries_vector_file> <num_queries>

python src/python/infer_token_vectors_cohere.py ../data/cohere-wikipedia-768.vec 1000000 \
    ../data/cohere-wikipedia-queries-768.vec 10000
"""

filename = sys.argv[1]
num_docs = int(sys.argv[2])
filename_queries= sys.argv[3]
num_queries = int(sys.argv[4])
dims = 768

for name in (filename, filename_queries):
  if os.path.exists(name):
      raise RuntimeError(f'please remove {name} first')

ds = datasets.load_dataset("Cohere/wikipedia-22-12-en-embeddings",
                           split="train")
print(f'features: {ds.features}')
print(f"total number of rows: {len(ds)}")
print(f"embeddings dims: {len(ds[0]['emb'])}")

# do this in windows, else the RAM usage is crazy (OOME even with 256
# GB RAM since I think this step makes 2X copy of the dataset?)
doc_upto = 0
window_num_docs = 1000000
while doc_upto < num_docs:
  next_doc_upto = min(doc_upto + window_num_docs, num_docs)
  ds_embs = ds[doc_upto:next_doc_upto]['emb']
  embs = np.array(ds_embs, dtype=np.single)
  print(f"saving docs[{doc_upto}:{next_doc_upto}] of shape: {embs.shape} to file")
  with open(filename, "ab") as out_f:
      embs.tofile(out_f)
  doc_upto = next_doc_upto

ds_embs_queries = ds[num_docs : num_docs + num_queries]['emb']
embs_queries = np.array(ds_embs_queries, dtype=np.single)
print(f"saving queries of shape: {embs_queries.shape} to file")
with open(filename_queries, "w") as out_f_queries:
    embs_queries.tofile(out_f_queries)

### check saved datasets
embs_docs = np.fromfile(filename, dtype=np.float32)
embs_docs = embs_docs.reshape(num_docs, dims)
print(f"reading docs of shape: {embs_docs.shape}")

embs_queries = np.fromfile(filename_queries, dtype=np.float32)
embs_queries = embs_queries.reshape(num_queries, dims)
print(f"reading queries shape: {embs_queries.shape}")
