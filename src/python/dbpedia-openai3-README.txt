DBpedia OpenAI embeddings corpus
================================

load_dbpedia_openai3.py converts this Hugging Face dataset into luceneutil
.vec files:

  https://huggingface.co/datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-large-3072-1M

The dataset has 1M DBpedia entity abstracts. Each row holds the entity id,
title and text, plus two embeddings of the same text:

  - OpenAI text-embedding-3-large, 3072 dims
  - OpenAI text-embedding-ada-002, 1536 dims

All vectors are unit-length, so use -metric dot_product.


Running
-------

  pip install numpy pyarrow
  python src/python/load_dbpedia_openai3.py --out-dir /path/to/dbpedia

The script downloads the 63 parquet shards (24.8 GB) into OUT_DIR/parquet,
skipping shards already there. It then shuffles all rows once with a seed
(printed as SEED, or set with --seed) and holds out the first --num-queries
rows (default 10000) as queries. Docs and queries do not overlap.

Use --max-shards 2 for a quick test run.

Peak disk use is about 70 GB: the parquet shards, an unshuffled copy of the
vectors in OUT_DIR/raw (removed at the end unless --keep-raw), and the
outputs.


Output files
------------

Row i of every docs file is the same entity, and the same holds for the
queries files.

  dbpedia.docs.csv, dbpedia.queries.csv
      id,title,text

  dbpedia-openai3-large-3072d.{docs,queries}.vec
      text-embedding-3-large vectors as little-endian float32. The parquet
      column is float64, but every value converts to float32 exactly; the
      script fails if one does not.

  dbpedia-openai3-large-trunc{1024,1536}d.{docs,queries}.vec
      The first N dims of the 3072d vectors, re-normalized to unit length.
      OpenAI documents this as a supported way to shorten text-embedding-3
      vectors (the dimensions parameter, and manual truncation followed by
      re-normalization): https://developers.openai.com/api/docs/guides/embeddings
      Change the dims with --truncate-dims, or pass an empty value to skip
      them.

  dbpedia-ada002-1536d.{docs,queries}.vec
      text-embedding-ada-002 vectors as little-endian float32.

Lucene's vector formats accept at most 1024 dims by default, so the 3072d and
1536d files need a vectors format that overrides getMaxDimensions(). The
trunc1024d files work as-is.


License
-------

The dataset card lists Apache-2.0. The text comes from BEIR dbpedia-entity
(CC-BY-SA-4.0), which is built from DBpedia abstracts of Wikipedia articles,
so the .csv files should carry CC-BY-SA attribution.
