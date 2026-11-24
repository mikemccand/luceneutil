# DBpedia OpenAI embeddings: generated artifacts

This README records one run of `src/python/load_dbpedia_openai3.py`. That tool
converts Qdrant's 1M-row DBpedia OpenAI embeddings dataset into luceneutil
`.vec` files. For the full file format and option list, see
`src/python/dbpedia-openai3-README.txt`.

Source dataset (Apache-2.0 card; text is CC-BY-SA-4.0 from BEIR dbpedia-entity):
https://huggingface.co/datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-large-3072-1M

## Where the files are

Public download (Cloudflare R2), one URL per file:

    https://pub-6de3254d7180436684278e0ec33ada22.r2.dev/dbpedia-openai3/<file name>

`python src/python/initial_setup.py --dbpedia <variant>` downloads a variant
(`3072d`, `trunc1536d`, `trunc1024d`, `ada002`, or `all`) plus the two CSVs
into `../data`.

The original copy is on host `raptorlake` in

    /lucenedata/qdrant/dbpedia-openai3/

A copy of this README sits next to the files, along with `run.log` (the full
stdout/stderr of the run) and `SHA256SUMS`.

## Command that was run

On host `raptorlake`, 2026-11-23, luceneutil commit `2ddc2ee7`, with Python 3.13.7,
numpy 2.2.6 and pyarrow 22.0.0:

    OUT=/lucenedata/qdrant/dbpedia-openai3
    /usr/bin/time -v python3 -u src/python/load_dbpedia_openai3.py \
        --out-dir $OUT --seed 20261117 --num-queries 50000 > $OUT/run.log 2>&1

All other options kept their defaults:

* `--truncate-dims 1024,1536`
* all 63 shards
* raw files removed at the end

(`--num-queries 50000` is now also the default.)

The seed is fixed, so rerunning this exact command on the same parquet
shards gives the same shuffle and byte-identical outputs. Check them with
`sha256sum -c SHA256SUMS`.

The run took 11.8 minutes wall clock. The 24 GB of parquet shards were
already on disk from an earlier run, so nothing was downloaded; a fresh
download adds about 7 minutes. Peak RSS was 15.2 GB.

## Artifacts

All vectors are little-endian float32, unit-length (use `-metric dot_product`),
and headerless: the vector count is file size / (dim * 4).

The docs and queries come from a single shuffle of all 1M rows: 50,000 queries
and 950,000 docs, with no overlap. Row *i* of every `*.docs.*` file is the same
DBpedia entity, and the same holds for the `*.queries.*` files.

| File | Rows | Dims | Size |
|---|---:|---:|---:|
| `dbpedia-openai3-large-3072d.docs.vec` | 950,000 | 3072 | 11.7 GB |
| `dbpedia-openai3-large-3072d.queries.vec` | 50,000 | 3072 | 614 MB |
| `dbpedia-openai3-large-trunc1536d.docs.vec` | 950,000 | 1536 | 5.8 GB |
| `dbpedia-openai3-large-trunc1536d.queries.vec` | 50,000 | 1536 | 307 MB |
| `dbpedia-openai3-large-trunc1024d.docs.vec` | 950,000 | 1024 | 3.9 GB |
| `dbpedia-openai3-large-trunc1024d.queries.vec` | 50,000 | 1024 | 205 MB |
| `dbpedia-ada002-1536d.docs.vec` | 950,000 | 1536 | 5.8 GB |
| `dbpedia-ada002-1536d.queries.vec` | 50,000 | 1536 | 307 MB |
| `dbpedia.docs.csv` | 950,000 | – | 362 MB |
| `dbpedia.queries.csv` | 50,000 | – | 19 MB |
| `parquet/` (63 source shards, kept for reruns, not uploaded) | 1,000,000 | – | 24 GB |

* **openai3-large-3072d**: OpenAI `text-embedding-3-large` vectors.
* **openai3-large-trunc{1024,1536}d**: the first N dims of the 3072d vectors,
  re-normalized to unit length (Matryoshka-style truncation).
* **ada002-1536d**: OpenAI `text-embedding-ada-002` vectors of the same text.
* **dbpedia.{docs,queries}.csv**: columns `id,title,text`, row-aligned with the
  `.vec` files. Some text values contain quoted newlines, so read these files
  with a CSV parser, not line by line.

Lucene's default vector formats accept at most 1024 dims. The 3072d and
1536d files need a format that overrides `getMaxDimensions()`. The trunc1024d
files work as-is. KnnGraphTester (and so `knnPerfTest.py`) wraps its formats to
allow up to 4096 dims, so all variants work there.

### Relation to the earlier 10,000-query run

An earlier run (2026-11-17) used the same seed with 10,000 queries and
990,000 docs. Because the shuffle is the same, the first 10,000 rows of each
new `*.queries.*` file are byte-identical to that run's query files. The new
docs are the old docs minus the 40,000 entities that moved into the queries.

## Checks done after the run

* The script's own checks passed:
  * every row has the expected dim
  * no NaN or infinite values
  * every vector is unit-length to within 1e-3
  * float64 to float32 conversion is exact
  * `_id` values are unique
  * the total row count is 1,000,000
* Every `.vec` file has exactly 950,000 or 50,000 rows, with no partial row.
  Vectors sampled from each file have norm within 1.2e-7 of 1.0.
* `trunc1024d.queries.vec` equals the re-normalized first 1024 dims of
  `3072d.queries.vec` (max difference 0), which confirms the files are
  row-aligned.
* `dbpedia.docs.csv` and `dbpedia.queries.csv` parse to 950,000 and 50,000
  rows. The first query row is `<dbpedia:Missouri_Route_105>`.
* The first 10,000 rows of `trunc1024d.queries.vec` and
  `ada002-1536d.queries.vec` hash the same as the earlier 10,000-query files.

## SHA-256

    baf36f07e30ea32dfc02bab0222255c157e121fa52ce30bd419cac0fec43b9f3  dbpedia-ada002-1536d.docs.vec
    ad5c59a0643748934793652c78234d978f5fc47742cf430248cf411a9389e281  dbpedia-ada002-1536d.queries.vec
    12c0b491970c4d5f9005e95eb120a25f54145911b1c7ad4aa9dfd64d2dddcfca  dbpedia-openai3-large-3072d.docs.vec
    3f15f1aa18e01f8e78cbb13ed244cd0067bbcdca22e0afc2f422a9a7eab85205  dbpedia-openai3-large-3072d.queries.vec
    0b2e4662d5a5e0e7b1933190c8c728afd57d765cf30a86e92f2c727ea05f9ade  dbpedia-openai3-large-trunc1024d.docs.vec
    addfc539d346ee28239ac836e16d1babc32a53788d64296739f84aff5faa6806  dbpedia-openai3-large-trunc1024d.queries.vec
    f87b2792d32bee8be555b105ddc27edfb0f682a2542a97374fe046ae26500449  dbpedia-openai3-large-trunc1536d.docs.vec
    823c3c61402f30d7db4ab783181fd7449a979ef0c91512712c2a33515ffb594c  dbpedia-openai3-large-trunc1536d.queries.vec
    084bad6cdeb08919b0ac7231965296ccf7c9f2f90c4176aeda4d8362a3997a64  dbpedia.docs.csv
    f55b7b737c4c27d32ae6fc30e74a75d68f893e8b8648eb910805a81e3736400d  dbpedia.queries.csv
