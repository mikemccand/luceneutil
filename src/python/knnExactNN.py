#!/usr/bin/env python

# standalone tool to compute exact (brute-force) nearest neighbors using numpy,
# writing results in the same binary format and directory layout that
# KnnGraphTester.java uses under knn-reuse/exact-nn/.
#
# usage:
#   python src/python/knnExactNN.py \
#     -docVectors /path/to/vectors.bin \
#     -queryVectors /path/to/queries.bin \
#     -dim 768 \
#     -numDocs 400000 \
#     -numQueryVectors 10000 \
#     -metric dot_product \
#     [-topK 100] \
#     [-queryStartIndex 0] \
#     [-encoding float32]

import argparse
import os
import time
from pathlib import Path

import numpy as np


def load_float32_vectors(path, dim, count, start_index=0):
    """Load count vectors of dimension dim from a raw float32 little-endian file, starting at start_index."""
    bytes_per_vector = dim * 4
    offset = start_index * bytes_per_vector
    file_size = os.path.getsize(path)
    if file_size % bytes_per_vector != 0:
        raise ValueError(f"file size {file_size} not divisible by bytes_per_vector {bytes_per_vector} (dim={dim})")
    total_vectors = file_size // bytes_per_vector
    if start_index + count > total_vectors:
        raise ValueError(
            f"requested {count} vectors starting at index {start_index}, "
            f"but file only has {total_vectors} vectors"
        )
    return np.memmap(path, dtype="<f4", mode="r", offset=offset, shape=(count, dim))


def load_byte_vectors(path, dim, count, start_index=0):
    """Load count vectors of dimension dim from a raw byte (int8) file, starting at start_index."""
    bytes_per_vector = dim
    offset = start_index * bytes_per_vector
    file_size = os.path.getsize(path)
    if file_size % bytes_per_vector != 0:
        raise ValueError(f"file size {file_size} not divisible by bytes_per_vector {bytes_per_vector} (dim={dim})")
    total_vectors = file_size // bytes_per_vector
    if start_index + count > total_vectors:
        raise ValueError(
            f"requested {count} vectors starting at index {start_index}, "
            f"but file only has {total_vectors} vectors"
        )
    raw = np.memmap(path, dtype="int8", mode="r", offset=offset, shape=(count, dim))
    return raw.astype(np.float32)


def compute_exact_nn(doc_vectors, query_vectors, metric, top_k):
    """Compute exact nearest neighbors using numpy.

    returns (ids, scores) each of shape (num_queries, top_k).
    scores match lucene's VectorSimilarityFunction encoding.
    """
    num_queries = query_vectors.shape[0]
    result_ids = np.empty((num_queries, top_k), dtype=np.int32)
    result_scores = np.empty((num_queries, top_k), dtype=np.float32)

    # process queries in batches to control memory
    # each batch computes a (batch_size, num_docs) score matrix
    batch_size = max(1, min(256, num_queries))

    for batch_start in range(0, num_queries, batch_size):
        batch_end = min(batch_start + batch_size, num_queries)
        batch_queries = query_vectors[batch_start:batch_end]

        if metric == "dot_product":
            # lucene: (1 + dot(a, b)) / 2
            raw = batch_queries @ doc_vectors.T
            scores = (1.0 + raw) / 2.0
        elif metric == "cosine":
            # lucene: (1 + cosine(a, b)) / 2
            q_norms = np.linalg.norm(batch_queries, axis=1, keepdims=True)
            d_norms = np.linalg.norm(doc_vectors, axis=1, keepdims=True)
            # avoid division by zero
            q_norms = np.maximum(q_norms, 1e-30)
            d_norms = np.maximum(d_norms, 1e-30)
            raw = (batch_queries / q_norms) @ (doc_vectors / d_norms).T
            scores = (1.0 + raw) / 2.0
        elif metric == "euclidean":
            # lucene: 1 / (1 + squaredDistance(a, b))
            # squared distance = ||a||^2 + ||b||^2 - 2*a.b
            q_sq = np.sum(batch_queries**2, axis=1, keepdims=True)
            d_sq = np.sum(doc_vectors**2, axis=1, keepdims=True).T
            sq_dist = q_sq + d_sq - 2.0 * (batch_queries @ doc_vectors.T)
            # clamp negative values from floating point error
            sq_dist = np.maximum(sq_dist, 0.0)
            scores = 1.0 / (1.0 + sq_dist)
        elif metric == "mip":
            # lucene maximum_inner_product:
            #   if dp >= 0: 1 + dp
            #   else: 1 / (1 - dp)
            raw = batch_queries @ doc_vectors.T
            scores = np.where(raw >= 0, 1.0 + raw, 1.0 / (1.0 - raw))
        else:
            raise ValueError(f"unknown metric: {metric}")

        # cast to float32 to match lucene's float precision
        scores = scores.astype(np.float32)

        for i in range(batch_end - batch_start):
            query_idx = batch_start + i
            row = scores[i]
            # argpartition is faster than full sort for top-k
            if top_k < len(row):
                top_indices = np.argpartition(row, -top_k)[-top_k:]
            else:
                top_indices = np.arange(len(row))
            # sort the top-k by descending score
            top_indices = top_indices[np.argsort(row[top_indices])[::-1]]
            result_ids[query_idx] = top_indices[:top_k]
            result_scores[query_idx] = row[top_indices[:top_k]]

        done = batch_end
        print(f"\r  {done}/{num_queries} queries computed", end="", flush=True)

    print()
    return result_ids, result_scores


def format_exact_nn_key(doc_path, query_path, num_docs, num_query_vectors, metric, query_start_index, top_k):
    """Replicate KnnGraphTester.formatExactNNKey naming convention."""
    doc_name = Path(doc_path).name
    query_name = Path(query_path).name

    parts = [
        metric,
        f"i{doc_name}",
        str(num_docs),
        f"q{query_name}",
        str(num_query_vectors),
    ]
    if query_start_index != 0:
        parts.append(f"qs{query_start_index}")
    parts.append(metric)

    return f"{doc_name}-" + "-".join(parts)


def write_ids(ids, path):
    """Write int32 little-endian ids file, matching KnnGraphTester.writeExactNN."""
    num_queries, top_k = ids.shape
    with open(path, "wb") as f:
        f.writelines(ids[i].astype("<i4").tobytes() for i in range(num_queries))
    print(f"  wrote {num_queries * top_k} ids to {path}")


def write_scores(scores, path):
    """Write float32 little-endian scores file, matching KnnGraphTester.writeExactNNScores."""
    num_queries, top_k = scores.shape
    with open(path, "wb") as f:
        f.writelines(scores[i].astype("<f4").tobytes() for i in range(num_queries))
    print(f"  wrote {num_queries * top_k} scores to {path}")


def main():
    parser = argparse.ArgumentParser(description="compute exact nearest neighbors using numpy")
    parser.add_argument("-docVectors", required=True, help="path to document vectors file")
    parser.add_argument("-queryVectors", required=True, help="path to query vectors file")
    parser.add_argument("-dim", type=int, required=True, help="vector dimension")
    parser.add_argument("-numDocs", type=int, required=True, help="number of document vectors")
    parser.add_argument("-numQueryVectors", type=int, required=True, help="number of query vectors")
    parser.add_argument("-metric", required=True, choices=["dot_product", "cosine", "euclidean", "mip"],
                        help="similarity metric")
    parser.add_argument("-topK", type=int, default=100, help="number of nearest neighbors (default 100)")
    parser.add_argument("-queryStartIndex", type=int, default=0, help="start index in query vectors file (default 0)")
    parser.add_argument("-encoding", default="float32", choices=["float32", "byte"],
                        help="vector encoding (default float32)")
    args = parser.parse_args()

    doc_path = Path(args.docVectors).resolve()
    query_path = Path(args.queryVectors).resolve()

    if not doc_path.exists():
        raise FileNotFoundError(f"doc vectors file not found: {doc_path}")
    if not query_path.exists():
        raise FileNotFoundError(f"query vectors file not found: {query_path}")

    print(f"loading {args.numDocs} doc vectors from {doc_path} (dim={args.dim}, encoding={args.encoding})")
    start_ns = time.monotonic_ns()
    if args.encoding == "float32":
        doc_vectors = load_float32_vectors(doc_path, args.dim, args.numDocs)
    else:
        doc_vectors = load_byte_vectors(doc_path, args.dim, args.numDocs)
    load_doc_ms = (time.monotonic_ns() - start_ns) / 1e6
    print(f"  loaded doc vectors in {load_doc_ms:.1f} ms")

    print(f"loading {args.numQueryVectors} query vectors from {query_path} "
          f"(startIndex={args.queryStartIndex})")
    start_ns = time.monotonic_ns()
    if args.encoding == "float32":
        query_vectors = load_float32_vectors(query_path, args.dim, args.numQueryVectors, args.queryStartIndex)
    else:
        query_vectors = load_byte_vectors(query_path, args.dim, args.numQueryVectors, args.queryStartIndex)
    load_query_ms = (time.monotonic_ns() - start_ns) / 1e6
    print(f"  loaded query vectors in {load_query_ms:.1f} ms")

    print(f"computing exact top-{args.topK} nearest neighbors using metric={args.metric}")
    start_ns = time.monotonic_ns()
    ids, scores = compute_exact_nn(doc_vectors, query_vectors, args.metric, args.topK)
    compute_ms = (time.monotonic_ns() - start_ns) / 1e6
    print(f"  computed exact NN in {compute_ms:.1f} ms ({compute_ms / 1000:.3f} sec)")

    # build output key and paths matching KnnGraphTester convention
    # use original (non-resolved) paths for filename extraction, matching java's Path.getFileName()
    key = format_exact_nn_key(args.docVectors, args.queryVectors,
                              args.numDocs, args.numQueryVectors,
                              args.metric, args.queryStartIndex, args.topK)
    out_dir = Path("knn-reuse") / "exact-nn"
    out_dir.mkdir(parents=True, exist_ok=True)

    nn_path = out_dir / f"{key}.bin"
    scores_path = out_dir / f"{key}.scores"

    print(f"exact nn key = {key}")
    write_ids(ids, nn_path)
    write_scores(scores, scores_path)

    print("\noutput files:")
    print(f"  {nn_path}")
    print(f"  {scores_path}")
    print(f"EXACT_NN_SCORES_PATH: {scores_path.resolve()}")
    print(f"EXACT_NN_METRIC: {args.metric}")


if __name__ == "__main__":
    main()
