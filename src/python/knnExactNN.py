#!/usr/bin/env python

# standalone tool to compute exact (brute-force) nearest neighbors using numpy,
# writing results in the same binary format and directory layout that
# KnnGraphTester.java uses under knn-reuse/exact-nn/.
#
# two modes:
#
#   1) explicit args:
#     python src/python/knnExactNN.py \
#       -docVectors /path/to/vectors.bin \
#       -queryVectors /path/to/queries.bin \
#       -dim 768 -numDocs 400000 -numQueryVectors 10000 \
#       -metric dot_product [-topK 100] [-queryStartIndex 0] [-encoding float32]
#
#   2) auto-detect from knnPerfTest.py:
#     python src/python/knnExactNN.py --from-knnPerfTest [path/to/knnPerfTest.py]

import argparse
import ast
import itertools
import os
import re
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


def _compute_scores_batch(batch_queries, doc_vectors, metric):
    """Compute similarity scores for a batch of queries against all docs."""
    if metric == "dot_product":
        # lucene: (1 + dot(a, b)) / 2
        raw = batch_queries @ doc_vectors.T
        return (1.0 + raw) / 2.0
    if metric == "cosine":
        # lucene: (1 + cosine(a, b)) / 2
        q_norms = np.linalg.norm(batch_queries, axis=1, keepdims=True)
        d_norms = np.linalg.norm(doc_vectors, axis=1, keepdims=True)
        # avoid division by zero
        q_norms = np.maximum(q_norms, 1e-30)
        d_norms = np.maximum(d_norms, 1e-30)
        raw = (batch_queries / q_norms) @ (doc_vectors / d_norms).T
        return (1.0 + raw) / 2.0
    if metric == "euclidean":
        # lucene: 1 / (1 + squaredDistance(a, b))
        # squared distance = ||a||^2 + ||b||^2 - 2*a.b
        q_sq = np.sum(batch_queries**2, axis=1, keepdims=True)
        d_sq = np.sum(doc_vectors**2, axis=1, keepdims=True).T
        sq_dist = q_sq + d_sq - 2.0 * (batch_queries @ doc_vectors.T)
        # clamp negative values from floating point error
        sq_dist = np.maximum(sq_dist, 0.0)
        return 1.0 / (1.0 + sq_dist)
    if metric == "mip":
        # lucene maximum_inner_product:
        #   if dp >= 0: 1 + dp
        #   else: 1 / (1 - dp)
        raw = batch_queries @ doc_vectors.T
        return np.where(raw >= 0, 1.0 + raw, 1.0 / (1.0 - raw))
    raise ValueError(f"unknown metric: {metric}")


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

    total_matmul_sec = 0.0
    total_topk_sec = 0.0
    start_sec = time.monotonic()
    next_report_sec = start_sec

    for batch_start in range(0, num_queries, batch_size):
        batch_end = min(batch_start + batch_size, num_queries)
        batch_queries = query_vectors[batch_start:batch_end]

        t0 = time.monotonic()
        scores = _compute_scores_batch(batch_queries, doc_vectors, metric)
        # cast to float32 to match lucene's float precision
        scores = scores.astype(np.float32)
        t1 = time.monotonic()
        total_matmul_sec += t1 - t0

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

        t2 = time.monotonic()
        total_topk_sec += t2 - t1

        # report progress every 5 seconds, matching KnnGraphTester format
        now_sec = time.monotonic()
        if now_sec >= next_report_sec or batch_end == num_queries:
            elapsed_sec = now_sec - start_sec
            pct = 100.0 * batch_end / num_queries
            print(f"  {elapsed_sec:6.1f} s: {pct:5.1f} % ({batch_end:5d} / {num_queries}) vectors "
                  f"[matmul {total_matmul_sec:.1f}s, topK {total_topk_sec:.1f}s]")
            next_report_sec = now_sec + 5.0

    return result_ids, result_scores


# see KnnGraphTester.formatExactNNKey (KnnGraphTester.java:954)
def format_exact_nn_key(doc_path, query_path, num_docs, num_query_vectors, metric, query_start_index):
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


def is_newer(path, *others):
    """Return True if path exists and is newer than all others."""
    if not os.path.exists(path):
        return False
    path_mtime = os.path.getmtime(path)
    return all(os.path.getmtime(other) < path_mtime for other in others)


def run_one(doc_vectors_path, query_vectors_path, dim, num_docs, num_query_vectors,
            metric, top_k, query_start_index, encoding):
    """Compute exact NN for one parameter combination and write cache files."""
    key = format_exact_nn_key(doc_vectors_path, query_vectors_path,
                              num_docs, num_query_vectors,
                              metric, query_start_index)
    out_dir = Path("knn-reuse") / "exact-nn"
    out_dir.mkdir(parents=True, exist_ok=True)

    nn_path = out_dir / f"{key}.bin"
    scores_path = out_dir / f"{key}.scores"

    # check expected file size: numQueryVectors * topK * 4 bytes
    expected_size = num_query_vectors * top_k * 4

    # skip if cache is already valid (exists, right size, newer than source vectors)
    if (nn_path.exists()
            and scores_path.exists()
            and os.path.getsize(nn_path) == expected_size
            and os.path.getsize(scores_path) == expected_size
            and is_newer(nn_path, doc_vectors_path, query_vectors_path)):
        print(f"  SKIP (cached): {key}")
        return

    print(f"\n--- computing: {key} ---")
    print(f"  docs={doc_vectors_path} queries={query_vectors_path}")
    print(f"  dim={dim} numDocs={num_docs} numQueries={num_query_vectors} metric={metric} topK={top_k}")

    print(f"  loading {num_docs} doc vectors (encoding={encoding})")
    start_ns = time.monotonic_ns()
    if encoding == "float32":
        doc_vectors = load_float32_vectors(doc_vectors_path, dim, num_docs)
    else:
        doc_vectors = load_byte_vectors(doc_vectors_path, dim, num_docs)
    load_doc_ms = (time.monotonic_ns() - start_ns) / 1e6
    print(f"    loaded in {load_doc_ms:.1f} ms")

    print(f"  loading {num_query_vectors} query vectors (startIndex={query_start_index})")
    start_ns = time.monotonic_ns()
    if encoding == "float32":
        query_vectors = load_float32_vectors(query_vectors_path, dim, num_query_vectors, query_start_index)
    else:
        query_vectors = load_byte_vectors(query_vectors_path, dim, num_query_vectors, query_start_index)
    load_query_ms = (time.monotonic_ns() - start_ns) / 1e6
    print(f"    loaded in {load_query_ms:.1f} ms")

    print(f"  computing exact top-{top_k} NN (metric={metric})")
    start_ns = time.monotonic_ns()
    ids, scores = compute_exact_nn(doc_vectors, query_vectors, metric, top_k)
    compute_ms = (time.monotonic_ns() - start_ns) / 1e6
    print(f"  computed exact NN in {compute_ms:.1f} ms ({compute_ms / 1000:.3f} sec)")

    print(f"  exact nn key = {key}")
    write_ids(ids, nn_path)
    write_scores(scores, scores_path)
    print(f"  EXACT_NN_SCORES_PATH: {scores_path.resolve()}")
    print(f"  EXACT_NN_METRIC: {metric}")


def parse_params_from_source(source_text):
    """Extract the PARAMS dict from knnPerfTest.py source using ast."""
    tree = ast.parse(source_text)
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "PARAMS":
                    return ast.literal_eval(node.value)
    raise ValueError("could not find PARAMS dict in knnPerfTest.py")


def parse_vector_config_from_source(source_text):
    """Extract v3 flag, dim, doc_vectors, query_vectors from run_knn_benchmark in knnPerfTest.py.

    Parses the active (uncommented) v3 assignment and the corresponding if/else block.
    """
    # find the active v3 = True/False line (not commented out)
    v3_match = re.search(r"^\s+v3\s*=\s*(True|False)\s*$", source_text, re.MULTILINE)
    if v3_match is None:
        raise ValueError("could not find 'v3 = True/False' in knnPerfTest.py")
    v3 = v3_match.group(1) == "True"

    # extract the if/else block for vector paths
    # pattern: find the active (non-commented) assignments for dim, doc_vectors, query_vectors
    # in the v3 branch (if v3: ... else: ...)
    if v3:
        # look for the "if v3:" block assignments
        block_pattern = r"if v3:\s*\n((?:\s+.*\n)*?)(?:\s+else:)"
    else:
        # look for the "else:" block assignments
        block_pattern = r"else:\s*\n((?:\s+.*\n)*?)(?:\n\s*#|\n\s*\n|\n\s+[a-zA-Z])"

    block_match = re.search(block_pattern, source_text)
    if block_match is None:
        raise ValueError(f"could not find v3={'True' if v3 else 'False'} block in knnPerfTest.py")

    block = block_match.group(1)

    # extract dim
    dim_match = re.search(r"^\s+dim\s*=\s*(\d+)", block, re.MULTILINE)
    if dim_match is None:
        raise ValueError(f"could not find dim assignment in v3={v3} block")
    dim = int(dim_match.group(1))

    # extract doc_vectors (handles both plain strings and f-strings)
    doc_match = re.search(r'^\s+doc_vectors\s*=\s*[f]?["\'](.+?)["\']', block, re.MULTILINE)
    if doc_match is None:
        raise ValueError(f"could not find doc_vectors assignment in v3={v3} block")
    doc_vectors = doc_match.group(1)
    # resolve f-string {dim} references from the parsed source
    dim_str = str(dim)
    doc_vectors = doc_vectors.replace("{dim}", dim_str)  # noqa: RUF027

    # extract query_vectors
    query_match = re.search(r'^\s+query_vectors\s*=\s*[f]?["\'](.+?)["\']', block, re.MULTILINE)
    if query_match is None:
        raise ValueError(f"could not find query_vectors assignment in v3={v3} block")
    query_vectors = query_match.group(1)
    query_vectors = query_vectors.replace("{dim}", dim_str)  # noqa: RUF027

    return dim, doc_vectors, query_vectors


def run_from_knn_perf_test(perf_test_path):
    """Parse knnPerfTest.py and precompute all needed exact NN files."""
    source_text = Path(perf_test_path).read_text()

    params = parse_params_from_source(source_text)
    dim, doc_vectors, query_vectors = parse_vector_config_from_source(source_text)

    print(f"parsed from {perf_test_path}:")
    print(f"  dim={dim}")
    print(f"  doc_vectors={doc_vectors}")
    print(f"  query_vectors={query_vectors}")
    print("  PARAMS (exact-nn relevant):")

    # extract the parameters that affect exact NN computation
    # each param is a tuple of values to iterate over
    ndocs = params.get("ndoc", (1000,))
    niters = params.get("niter", (1000,))
    metrics = params.get("metric", ("dot_product",))
    top_ks = params.get("topK", (100,))
    query_start_indices = params.get("queryStartIndex", (0,))
    encodings = params.get("encoding", ("float32",))

    # if any param is not a tuple/list, wrap it
    if not isinstance(ndocs, (tuple, list)):
        ndocs = (ndocs,)
    if not isinstance(niters, (tuple, list)):
        niters = (niters,)
    if not isinstance(metrics, (tuple, list)):
        metrics = (metrics,)
    if not isinstance(top_ks, (tuple, list)):
        top_ks = (top_ks,)
    if not isinstance(query_start_indices, (tuple, list)):
        query_start_indices = (query_start_indices,)
    if not isinstance(encodings, (tuple, list)):
        encodings = (encodings,)

    print(f"    ndoc={ndocs}")
    print(f"    niter={niters}")
    print(f"    metric={metrics}")
    print(f"    topK={top_ks}")
    print(f"    queryStartIndex={query_start_indices}")
    print(f"    encoding={encodings}")

    combos = list(itertools.product(ndocs, niters, metrics, top_ks, query_start_indices, encodings))
    print(f"\n{len(combos)} parameter combination(s) to precompute:")

    for ndoc, niter, metric, top_k, query_start_index, encoding in combos:
        run_one(doc_vectors, query_vectors, dim, ndoc, niter,
                metric, top_k, query_start_index, encoding)

    print("\ndone.")


def main():
    parser = argparse.ArgumentParser(description="compute exact nearest neighbors using numpy")
    subparsers = parser.add_subparsers(dest="command")

    # sub-command: auto mode reading from knnPerfTest.py
    auto_parser = subparsers.add_parser("auto", help="precompute exact NN from knnPerfTest.py config")
    auto_parser.add_argument("perf_test_path", nargs="?", default="src/python/knnPerfTest.py",
                             help="path to knnPerfTest.py (default: src/python/knnPerfTest.py)")

    # sub-command: explicit args mode
    run_parser = subparsers.add_parser("run", help="compute exact NN with explicit parameters")
    run_parser.add_argument("-docVectors", required=True, help="path to document vectors file")
    run_parser.add_argument("-queryVectors", required=True, help="path to query vectors file")
    run_parser.add_argument("-dim", type=int, required=True, help="vector dimension")
    run_parser.add_argument("-numDocs", type=int, required=True, help="number of document vectors")
    run_parser.add_argument("-numQueryVectors", type=int, required=True, help="number of query vectors")
    run_parser.add_argument("-metric", required=True, choices=["dot_product", "cosine", "euclidean", "mip"],
                            help="similarity metric")
    run_parser.add_argument("-topK", type=int, default=100, help="number of nearest neighbors (default 100)")
    run_parser.add_argument("-queryStartIndex", type=int, default=0,
                            help="start index in query vectors file (default 0)")
    run_parser.add_argument("-encoding", default="float32", choices=["float32", "byte"],
                            help="vector encoding (default float32)")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        raise SystemExit(1)

    if args.command == "auto":
        run_from_knn_perf_test(args.perf_test_path)
    elif args.command == "run":
        doc_path = Path(args.docVectors).resolve()
        query_path = Path(args.queryVectors).resolve()
        if not doc_path.exists():
            raise FileNotFoundError(f"doc vectors file not found: {doc_path}")
        if not query_path.exists():
            raise FileNotFoundError(f"query vectors file not found: {query_path}")
        run_one(args.docVectors, args.queryVectors, args.dim, args.numDocs, args.numQueryVectors,
                args.metric, args.topK, args.queryStartIndex, args.encoding)


if __name__ == "__main__":
    main()
