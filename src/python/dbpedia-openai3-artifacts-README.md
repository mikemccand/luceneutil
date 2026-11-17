# DBpedia OpenAI embeddings: generated artifacts

This README records one run of `src/python/load_dbpedia_openai3.py`. That tool
converts Qdrant's 1M-row DBpedia OpenAI embeddings dataset into luceneutil
`.vec` files. For the full file format and option list, see
`src/python/dbpedia-openai3-README.txt`.

Source dataset (Apache-2.0 card; text is CC-BY-SA-4.0 from BEIR dbpedia-entity):
https://huggingface.co/datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-large-3072-1M

## Where the files are

    /lucenedata/qdrant/dbpedia-openai3/

A copy of this README sits next to them, along with `run.log` (the full
stdout/stderr of the run) and `SHA256SUMS`.

## Command that was run

On host `raptorlake`, 2026-11-17, luceneutil commit `106d34ac`, with Python 3.13.7,
numpy 2.2.6 and pyarrow 22.0.0:

    mkdir -p /lucenedata/qdrant/dbpedia-openai3
    OUT=/lucenedata/qdrant/dbpedia-openai3
    /usr/bin/time -v python3 -u src/python/load_dbpedia_openai3.py \
        --out-dir $OUT --seed 20261117 > $OUT/run.log 2>&1

All other options kept their defaults:

* `--num-queries 10000`
* `--truncate-dims 1024,1536`
* all 63 shards
* raw files removed at the end

The seed is fixed, so rerunning this exact command on the same parquet
shards gives the same shuffle and byte-identical outputs. Check them with
`sha256sum -c SHA256SUMS`.

The run took 13.5 minutes wall clock, including about 7 minutes to download
the 24 GB of parquet. Peak RSS was 14.9 GB.

## Artifacts

All vectors are little-endian float32, unit-length (use `-metric dot_product`),
and headerless: the vector count is file size / (dim * 4).

The docs and queries come from a single shuffle of all 1M rows: 10,000 queries
and 990,000 docs, with no overlap. Row *i* of every `*.docs.*` file is the same
DBpedia entity, and the same holds for the `*.queries.*` files.

| File | Rows | Dims | Size |
|---|---:|---:|---:|
| `dbpedia-openai3-large-3072d.docs.vec` | 990,000 | 3072 | 12.2 GB |
| `dbpedia-openai3-large-3072d.queries.vec` | 10,000 | 3072 | 123 MB |
| `dbpedia-openai3-large-trunc1536d.docs.vec` | 990,000 | 1536 | 6.1 GB |
| `dbpedia-openai3-large-trunc1536d.queries.vec` | 10,000 | 1536 | 61 MB |
| `dbpedia-openai3-large-trunc1024d.docs.vec` | 990,000 | 1024 | 4.1 GB |
| `dbpedia-openai3-large-trunc1024d.queries.vec` | 10,000 | 1024 | 41 MB |
| `dbpedia-ada002-1536d.docs.vec` | 990,000 | 1536 | 6.1 GB |
| `dbpedia-ada002-1536d.queries.vec` | 10,000 | 1536 | 61 MB |
| `dbpedia.docs.csv` | 990,000 | – | 377 MB |
| `dbpedia.queries.csv` | 10,000 | – | 3.8 MB |
| `parquet/` (63 source shards, kept for reruns) | 1,000,000 | – | 24 GB |

* **openai3-large-3072d**: OpenAI `text-embedding-3-large` vectors.
* **openai3-large-trunc{1024,1536}d**: the first N dims of the 3072d vectors,
  re-normalized to unit length (Matryoshka-style truncation).
* **ada002-1536d**: OpenAI `text-embedding-ada-002` vectors of the same text.
* **dbpedia.{docs,queries}.csv**: columns `id,title,text`, row-aligned with the
  `.vec` files. Some text values contain quoted newlines, so read these files
  with a CSV parser, not line by line.

Lucene's default vector formats accept at most 1024 dims. The 3072d and
1536d files need a format that overrides `getMaxDimensions()`. The trunc1024d
files work as-is.

## Checks done after the run

* The script's own checks passed:
  * every row has the expected dim
  * no NaN or infinite values
  * every vector is unit-length to within 1e-3
  * float64 to float32 conversion is exact
  * `_id` values are unique
  * the total row count is 1,000,000
* For every `.vec` file, the size is an exact multiple of dim*4. Vectors
  sampled from each file have norm within 1.2e-7 of 1.0.
* `trunc1024d.queries.vec` equals the re-normalized first 1024 dims of
  `3072d.queries.vec`, which confirms the files are row-aligned.
* `dbpedia.queries.csv` parses to 10,000 rows. The first row is
  `<dbpedia:Missouri_Route_105>`.

## SHA-256

    81029a1509874c3746022780c88a989f7d1a742d5bc2b5a0a0d0a6d814f16fd3  dbpedia-ada002-1536d.docs.vec
    f071601199faaa597a2516db19103827697ddf5bf14a615934efae729096a953  dbpedia-ada002-1536d.queries.vec
    995d887e8515175dc5c78770427ebfa85a7636c79adf2cde720cc26a97632aac  dbpedia-openai3-large-3072d.docs.vec
    b946baa94d768e6d9a53c932c21d7052f11efa3c3392ec6ecfd1d3982cf8dc56  dbpedia-openai3-large-3072d.queries.vec
    083839621738f094a7eb80fd3d1e9e3a371d4d934c47fc59cd7203e049d96366  dbpedia-openai3-large-trunc1024d.docs.vec
    6a956647458f045eb02bc79800b107e2cc0bf0d28eae3decd257717f079a9772  dbpedia-openai3-large-trunc1024d.queries.vec
    b5f6822631b2a3aa51f13e184d9c7a85170d760e1957077e2677554efdaabe74  dbpedia-openai3-large-trunc1536d.docs.vec
    bc89c8d626487f64390e09b16cab6211231d242cbe50918a46cdc69513ea7840  dbpedia-openai3-large-trunc1536d.queries.vec
    759e135c0c0d95d3e70350af4422024e9fac8da76d5714c4c127b0aecab4515b  dbpedia.docs.csv
    c966dae385f7c434dd00fbce55e8e2f4638a634ccdaa5901f82daf6ca7cef978  dbpedia.queries.csv
