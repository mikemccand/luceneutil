# Exact NN Score Distribution Video

Thank you Claude Opus 4.6!

Generate a video showing how brute-force exact nearest neighbor score distributions change as the number of query vectors (N) increases.

Two tools:
1. **knnExactNNHistogram.py** -- computes dot-product scores via NumPy/BLAS, writes raw `.bin` files
2. **knnExactNNVideo.py** -- reads `.bin` files, renders histogram PNGs with matplotlib, stitches into mp4 with ffmpeg

## Prerequisites

- Python 3 with `numpy` and `matplotlib`
- `ffmpeg` on PATH (for video generation)

## Step 1: Compute scores

```bash
python src/python/knnExactNNHistogram.py \
  -docs /lucenedata/enwiki/cohere-v3/cohere-v3-wikipedia-en-scattered-1024d.docs.vec \
  -queries /lucenedata/enwiki/cohere-v3/cohere-v3-wikipedia-en-scattered-1024d.queries.vec \
  -dim 1024 \
  -numDocs 400000 \
  -topK 100 \
  -N 100,500,1000,5000,10000 \
  -M 10 \
  -outputDir /tmp/nn_scores
```

Arguments:
- `-docs` / `-queries` -- paths to `.vec` files (flat little-endian float32, no header)
- `-dim` -- vector dimensionality
- `-numDocs` -- how many doc vectors to load from the doc file
- `-topK` -- keep this many best scores per query vector
- `-N` -- comma-separated list of query vector counts (no spaces)
- `-M` -- number of repeats per N value
- `-outputDir` -- where to write `.bin` score files and `manifest.json`
- `-batchSize` (optional, default 200) -- queries per matmul batch; controls peak RAM (`batchSize * numDocs * 4` bytes for the score matrix)

Each run uses fresh query vectors -- the query start index advances globally across all (N, M) pairs.

### Output

- `scores_N{n}_M{m}.bin` -- `N * topK` little-endian float32 scores
- `manifest.json` -- metadata for all runs (paths, params, timing, file names)

## Step 2: Generate histograms and video

```bash
python src/python/knnExactNNVideo.py \
  -inputDir /tmp/nn_scores \
  -fps 2
```

Arguments:
- `-inputDir` -- directory containing `.bin` files and `manifest.json` from step 1
- `-outputDir` (optional) -- where to write PNGs and video; defaults to inputDir
- `-fps` (optional, default 2) -- frames per second in output video
- `-bins` (optional, default 100) -- number of histogram bins
- `-noVideo` -- skip ffmpeg, only generate PNGs

### Output

- `frame_0000.png`, `frame_0001.png`, ... -- one histogram PNG per run
- `exact_nn_histogram.mp4` -- video of all frames

Both axes are fixed across all frames (global score range for x, global max bin percentage for y) so changes in distribution shape are visually comparable.

## How it works

The score computation uses NumPy's `@` (matmul) operator which delegates to BLAS (OpenBLAS/MKL) for SIMD-optimized dot products. For each batch of queries, it computes `queries @ docs.T` and extracts topK scores per query via `np.argpartition` (O(n) partial sort).

RAM usage for the score matrix: `batchSize * numDocs * 4` bytes. With defaults (batchSize=200, numDocs=400K) that's ~320 MB.


## Example from Cohere v3

See [this histogram example](https://githubsearch.mikemccandless.com/exact_nn_histogram.html) (NOTE: zoomable on horizontal axis), and [this histogram video example](https://githubsearch.mikemccandless.com/exact_nn_histogram_law_of_large_numbers.mp4) showcasing law of large numbers, or not (anti-Poisson rogue element).

To create the video I first ran this (to generate all scores; it took a loooong time on `beast3` and created 6.3 GB of cached `.bin` score files!):

```
python src/python/knnExactNNHistogram.py -docs /lucenedata/enwiki/cohere-v3/cohere-v3-wikipedia-en-scattered-1024d.docs.vec -queries /lucenedata/enwiki/cohere-v3/cohere-v3-wikipedia-en-scattered-1024d.queries.vec -dim 1024 -numDocs 400000 -topK 100 -N 10,50,100,500,1000,5000,10000,50000,100000 -M 100 -outputDir video_nn_scores
```

and this to create the resulting video:

```
python src/python/knnExactNNVideo.py -inputDir video_nn_scores -fps 10
```