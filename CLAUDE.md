# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

In Python sources:
  - use lower case for comments
  - do not import modules inside functions.  put them at the top of the source.
  - when naming variables that measure something always put the units into the name, for example elapsed_time_sec

## Project Overview

Luceneutil is a comprehensive benchmarking suite for Apache Lucene. It includes benchmarks for indexing, searching, KNN (HNSW vector search), faceted search, and geo-spatial queries. The project compares a baseline version of Lucene against candidate versions with proposed changes.

## Architecture

### Core Components

1. **Benchmarking Framework** (`benchUtil.py`, `competition.py`)
   - Orchestrates benchmark runs comparing baseline vs candidate Lucene versions
   - Manages index building, warmup, and performance measurement
   - Results are analyzed and compared using statistics

2. **KNN Benchmarking** (`knnPerfTest.py`, `runNightlyKnn.py`)
   - Vector search benchmarks using HNSW indexes
   - Supports multiple vector sources: MiniLM, MPNet, and Cohere Wikipedia v3 embeddings
   - Tracks segment metrics and merging during indexing

3. **Vector Data Processing**
   - **`shuffle_wiki_ids.py`**: Shuffles Cohere v3 Wikipedia embeddings while preserving wiki document boundaries
   - **`load_cohere_v3.py`**: Downloads and processes Cohere Wikipedia v3 vectors, partitions into queries/docs
   - **`shuffle_vec_file.py`**: Shuffles binary vector files using write-side random access for performance
   - All vector operations use sequential reads with random writes to optimize disk I/O

4. **Dataset Utilities**
   - Wikipedia line doc creation from dumps
   - OSM (OpenStreetMap) data extraction and indexing
   - Taxis CSV data generation

### Key Design Patterns

1. **Sequential Read + Random Write I/O**: Vector and CSV shuffling use sequential input reads paired with random/shuffled output writes. This pattern is optimized by:
   - Reading input files sequentially (cache-friendly)
   - Using write plans that map input positions to shuffled output positions
   - Pre-allocating output files with `os.posix_fallocate()` for efficiency
   - Opening output files in `wb` mode for performance

2. **Wiki Document Grouping**: When processing Cohere v3 Wikipedia vectors:
   - Multiple paragraphs belong to the same wiki document (wiki_id)
   - All paragraphs of a wiki document must stay together during shuffling
   - Build an index mapping each wiki_id to its byte ranges
   - Shuffle at the wiki_id level, not the paragraph level

3. **Progress Reporting**: Uses time-based progress reporting (every 5 seconds) rather than count-based, with percentage tracking where total counts are known

4. **CSV Handling**:
   - Use Python's `csv` module for proper field parsing (handles quoted fields with newlines)
   - Set `field_size_limit(1024 * 1024 * 40)` to handle large text fields
   - Use `newline=''` when opening CSV files for writing (csv module requirement)
   - Specify explicit `lineterminator='\n'` to control line endings

## Important Constants

- `TOTAL_DOC_COUNT = 5_854_887`: Total unique wiki documents in Cohere v3
- `TOTAL_PARAGRAPH_COUNT = 41_488_110`: Total paragraphs across all wiki documents
- `DIMENSIONS = 1024`: Vector dimensionality for Cohere v3 embeddings
- Defined in `shuffle_wiki_ids.py` and imported by `load_cohere_v3.py`

## Common Development Tasks

### Building and Testing

```bash
# Gradle build (Java/Lucene indexing benchmarks)
./gradlew build

# Run all Python tests
python src/python/runAllTests.py

# Generate vectors for KNN testing
python src/python/infer_token_vectors_cohere.py -d 10000000 -q 10000

# Run KNN benchmark
python src/python/knnPerfTest.py

# Run main benchmarks with test corpus
python src/python/localrun.py -source wikimedium10k

# Run benchmarks with full corpus
python src/python/localrun.py -source wikimediumall
```

### Working with Cohere v3 Vector Data

```bash
# Add paragraph_count column and shuffle wiki documents while preserving paragraph grouping
python src/python/shuffle_wiki_ids.py <input_csv> <input_vec> 1024 <output_csv> <output_vec>

# Download Cohere v3 Wikipedia embeddings and partition into queries/docs
python src/python/load_cohere_v3.py
# This will:
# 1. Download from Hugging Face (41.5M rows, ~5.8M unique wiki documents)
# 2. Add paragraph_count column to track paragraphs per wiki document
# 3. Shuffle at wiki_id level to remove bias
# 4. Partition into queries (first 250K wiki_ids) and docs (remainder)
```

### Debugging Vector Processing

When developing or debugging the vector pipeline:
- Check for CSV corruption by examining first/last rows
- Verify header row is present in output files
- Use `lineterminator='\n'` consistently across CSV writers
- Remember to skip the header when building indexes from CSV files
- File mode matters: use `'wb'` for binary writes, `'r'` for text reads with proper encoding

## Python Environment

- Uses virtual environment in `.venv/`
- Key dependencies: numpy, datasets (Hugging Face), csv (stdlib)
- All vector files are binary float32 format (4 bytes per value)

## File Structure

```
src/python/
  ├── shuffle_wiki_ids.py       # Core shuffling with wiki document grouping
  ├── load_cohere_v3.py         # Download and partition Cohere v3 data
  ├── shuffle_vec_file.py       # Vector file shuffling utility
  ├── benchUtil.py              # Core benchmarking framework
  ├── competition.py            # Benchmark orchestration
  ├── knnPerfTest.py            # KNN benchmark runner
  ├── constants.py              # Global configuration
  └── ... (other utilities)
```

## Recent Changes and Ongoing Work

The vector processing pipeline (`shuffle_wiki_ids.py`, `load_cohere_v3.py`) was recently refactored to:
- Use Python's `csv` module instead of manual string parsing for robustness
- Properly handle CSV headers in shuffled output
- Use time-based progress reporting with percentage tracking
- Extract the `partition_documents()` function to split queries/docs in a single pass without data loss
- Import shared constants from `shuffle_wiki_ids.py` to avoid duplication

Key insight: CSV iteration can be tricky when the csv module's `next()` function disables file position tracking—use `wiki_page_count / TOTAL_DOC_COUNT` for progress instead of `f.tell()`.
