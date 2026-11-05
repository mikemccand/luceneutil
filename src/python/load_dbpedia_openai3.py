#!/usr/bin/env python3

"""Converts Qdrant's 1M DBpedia OpenAI embeddings into luceneutil .vec files.

See dbpedia-openai3-README.txt.
"""

import argparse
import csv
import os
import shutil
import time
import urllib.request
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

DATASET_NAME = "Qdrant/dbpedia-entities-openai3-text-embedding-3-large-3072-1M"
SHARD_COUNT = 63
TOTAL_ROW_COUNT = 1_000_000

# output name -> (parquet column, dimensions)
EMBEDDINGS = {
  "openai3-large": ("text-embedding-3-large-3072-embedding", 3072),
  "ada002": ("text-embedding-ada-002-1536-embedding", 1536),
}

# each shard download is tried this many times; a stalled read fails after the timeout
DOWNLOAD_ATTEMPTS = 3
DOWNLOAD_TIMEOUT_SEC = 60

# rows are copied in chunks of this many vectors
CHUNK_ROWS = 10_000

# vectors whose length differs from 1.0 by more than this are counted as not unit-length
NORM_TOLERANCE = 1e-3


def shard_url(shard):
  return f"https://huggingface.co/datasets/{DATASET_NAME}/resolve/main/data/train-{shard:05d}-of-{SHARD_COUNT:05d}.parquet"


def download_shards(parquet_dir, shard_count):
  """Downloads the parquet shards, skipping ones already on disk. Returns their paths."""
  os.makedirs(parquet_dir, exist_ok=True)
  paths = []
  for shard in range(shard_count):
    path = os.path.join(parquet_dir, f"train-{shard:05d}-of-{SHARD_COUNT:05d}.parquet")
    if not os.path.exists(path):
      t0 = time.time()
      tmp_path = path + ".part"
      for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        try:
          with urllib.request.urlopen(shard_url(shard), timeout=DOWNLOAD_TIMEOUT_SEC) as response, open(tmp_path, "wb") as out:
            shutil.copyfileobj(response, out, 1024 * 1024)
            expected_bytes = response.headers.get("Content-Length")
          if expected_bytes is not None and os.path.getsize(tmp_path) != int(expected_bytes):
            raise OSError(f"got {os.path.getsize(tmp_path)} bytes, expected {expected_bytes}")
          break
        except OSError as e:
          if attempt == DOWNLOAD_ATTEMPTS:
            raise
          print(f"  download of {os.path.basename(path)} failed ({e}), retrying")
      os.rename(tmp_path, path)
      print(f"  downloaded {os.path.basename(path)} ({os.path.getsize(path) / 1024 / 1024:.1f} MB) in {time.time() - t0:.1f} sec")
    paths.append(path)
  return paths


def column_to_matrix(table, column, dim):
  """Returns a parquet list column as a (rows, dim) float32 matrix, failing if float32 loses precision."""
  values = table.column(column).combine_chunks().flatten().to_numpy()
  if values.size != table.num_rows * dim:
    raise RuntimeError(f"{column}: expected {table.num_rows * dim} values, got {values.size}")
  matrix = values.reshape(table.num_rows, dim)
  if matrix.dtype != np.float32:
    matrix32 = matrix.astype(np.float32)
    if not np.array_equal(matrix32.astype(matrix.dtype), matrix):
      raise RuntimeError(f"{column}: values change when converted to float32")
    matrix = matrix32
  return matrix


def load_shards(shard_paths, raw_dir):
  """Writes each embedding in source order to a raw .vec file and returns the (id, title, text) rows."""
  os.makedirs(raw_dir, exist_ok=True)
  raw_files = {name: open(os.path.join(raw_dir, f"{name}.raw.vec"), "wb") for name in EMBEDDINGS}
  bad_counts = dict.fromkeys(EMBEDDINGS, 0)
  rows = []
  try:
    for path in shard_paths:
      table = pq.read_table(path)
      for name, (column, dim) in EMBEDDINGS.items():
        matrix = column_to_matrix(table, column, dim)
        norms = np.linalg.norm(matrix, axis=1)
        bad = ~np.isfinite(norms) | (np.abs(norms - 1.0) > NORM_TOLERANCE)
        bad_counts[name] += int(bad.sum())
        matrix.tofile(raw_files[name])
      rows.extend(zip(table.column("_id").to_pylist(), table.column("title").to_pylist(), table.column("text").to_pylist(), strict=True))
      print(f"  loaded {os.path.basename(path)}: {table.num_rows} rows, {len(rows)} total")
  finally:
    for f in raw_files.values():
      f.close()

  for name, count in bad_counts.items():
    if count > 0:
      raise RuntimeError(f"{name}: {count} vectors are NaN, infinite, or not unit-length")
  ids = [row[0] for row in rows]
  if len(set(ids)) != len(ids):
    raise RuntimeError(f"found {len(ids) - len(set(ids))} duplicate _id values")
  return rows


def write_vectors(raw, order, out_path, truncate_dim=None):
  """Writes rows of raw in the given order, optionally truncated to the first truncate_dim dims and re-normalized."""
  with open(out_path, "wb") as out:
    for start in range(0, len(order), CHUNK_ROWS):
      chunk = order[start : start + CHUNK_ROWS]
      # read the memmap in increasing offset order, then put the rows back in shuffled order
      sort = np.argsort(chunk)
      vectors = np.empty((len(chunk), raw.shape[1]), dtype=np.float32)
      vectors[sort] = raw[chunk[sort]]
      if truncate_dim is not None:
        vectors = vectors[:, :truncate_dim]
        vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
        if not np.isfinite(vectors).all():
          raise RuntimeError(f"{out_path}: a truncated vector has a zero-length prefix")
      vectors.tofile(out)
  dim = truncate_dim or raw.shape[1]
  expected_bytes = len(order) * dim * 4
  if os.path.getsize(out_path) != expected_bytes:
    raise RuntimeError(f"{out_path}: expected {expected_bytes} bytes, got {os.path.getsize(out_path)}")
  print(f"  wrote {out_path}: {len(order)} x {dim}d")


def write_csv(rows, order, out_path):
  """Writes the (id, title, text) rows in the given order, with a header line."""
  with open(out_path, "w", encoding="utf-8", newline="") as out:
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(["id", "title", "text"])
    for i in order:
      writer.writerow(rows[i])
  print(f"  wrote {out_path}: {len(order)} rows")


def main():
  parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument("--out-dir", required=True, help="where to put the downloaded shards and the output files")
  parser.add_argument("--num-queries", type=int, default=10_000, help="rows held out as query vectors (default 10000)")
  parser.add_argument("--seed", type=int, default=None, help="shuffle seed (default: time based, printed)")
  parser.add_argument("--truncate-dims", default="1024,1536", help="comma separated dims for truncated openai3-large variants, or empty for none")
  parser.add_argument("--max-shards", type=int, default=SHARD_COUNT, help="only use the first N shards, for testing")
  parser.add_argument("--keep-raw", action="store_true", help="keep the unshuffled raw .vec files")
  args = parser.parse_args()

  if args.num_queries < 1:
    parser.error("--num-queries must be at least 1")
  truncate_dims = [int(d) for d in args.truncate_dims.split(",") if d.strip()]
  for d in truncate_dims:
    if not 0 < d < EMBEDDINGS["openai3-large"][1]:
      parser.error(f"--truncate-dims value {d} must be between 1 and 3071")
  seed = args.seed if args.seed is not None else int(time.time() * 1000)
  print(f"SEED: {seed}")

  t0 = time.time()
  print("downloading shards")
  shard_paths = download_shards(os.path.join(args.out_dir, "parquet"), args.max_shards)

  print("loading shards")
  raw_dir = os.path.join(args.out_dir, "raw")
  rows = load_shards(shard_paths, raw_dir)
  row_count = len(rows)
  if args.max_shards == SHARD_COUNT and row_count != TOTAL_ROW_COUNT:
    raise RuntimeError(f"expected {TOTAL_ROW_COUNT} rows, got {row_count}")
  if not 0 < args.num_queries < row_count:
    raise RuntimeError(f"--num-queries {args.num_queries} must be between 1 and {row_count - 1}")

  # one shuffle shared by all embeddings, so row i of every output file is the same entity
  order = np.random.default_rng(seed).permutation(row_count)
  splits = {"queries": order[: args.num_queries], "docs": order[args.num_queries :]}

  print("writing shuffled outputs")
  for split, split_order in splits.items():
    write_csv(rows, split_order, os.path.join(args.out_dir, f"dbpedia.{split}.csv"))
  for name, (_, dim) in EMBEDDINGS.items():
    raw_path = os.path.join(raw_dir, f"{name}.raw.vec")
    raw = np.memmap(raw_path, dtype=np.float32, mode="r", shape=(row_count, dim))
    for split, split_order in splits.items():
      write_vectors(raw, split_order, os.path.join(args.out_dir, f"dbpedia-{name}-{dim}d.{split}.vec"))
      if name == "openai3-large":
        for d in truncate_dims:
          write_vectors(raw, split_order, os.path.join(args.out_dir, f"dbpedia-{name}-trunc{d}d.{split}.vec"), truncate_dim=d)
    del raw
    if not args.keep_raw:
      os.remove(raw_path)

  if not args.keep_raw:
    Path(raw_dir).rmdir()
  print(f"done: {splits['docs'].size} docs, {splits['queries'].size} queries, seed {seed}, {time.time() - t0:.1f} sec")


if __name__ == "__main__":
  main()
