import sys
import numpy as np
import re
import torch
# from https://www.sbert.net/
# to install: pip install -U sentence-transformers
from sentence_transformers import SentenceTransformer

"""
read text from input_file
write unique tokens to token_file and corresponding vectors to vector_file
EG:

python src/python/infer_token_vectors.py ../data/enwiki-20120502-lines-1k-fixed-utf8-with-random-label.txt \
    ../data/enwiki-20120502.all-MiniLM-L6-v2.tok \
    ../data/enwiki-20120502.all-MiniLM-L6-v2.vec \
    all-MiniLM-L6-v2


Here we just get embeddings for single tokens out of context. This is kind of abusive use of
sentence transformers which use sliding "attention" windows to take context into account, but
for our performance-testing it seems adequate.

known models: 
all-MiniLM-L6-v2 -> 384 dimension
all-mpnet-base-v2 -> 768 dimension
 """

input_file = sys.argv[1]
token_file = sys.argv[2]
vector_file = sys.argv[3]
model_name = sys.argv[4]

dic = dict()
TOKENIZE_RE = re.compile('[][ \t,\-()!@#$%^&*\'"{}<>/?\\|+=_:;.]+')
with open(input_file, 'rt') as input:
    for line in input:
        if len(dic) > 400000:
            break
        tokens = TOKENIZE_RE.split(line[:-1])
        for t in tokens:
            tl = t.lower()
            if tl in dic:
                dic[tl] += 1
            else:
                dic[tl] = 1

BATCH_SIZE = 256
model = SentenceTransformer(model_name)
model.eval()

with open(token_file, 'wt') as tokens_out:
    with open(vector_file, 'wb') as vectors_out:
        tokens = []
        for token, count in dic.items():
            if count > 1:
                print(token, file=tokens_out)
                tokens.append(token)
            if len(tokens) == BATCH_SIZE:
                outputs = model.encode(tokens)
                outputs.tofile(vectors_out)
                tokens = []
        if tokens:
            outputs = model.encode(tokens)
            outputs.tofile(vectors_out)
            tokens = []
