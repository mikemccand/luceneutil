# Luceneutil: Lucene benchmarking utilities

![Benchmarking Lucene Duke -- thank you @mocobeta!](/Benchmarking-Duke-from-Tomoko.png)

### Setting up luceneutil

First, pick a root directory, under which luceneutil will be checked out,
datasets exist, indices are built, Lucene source code is checked out,
etc.. We'll refer to this directory as `$LUCENE_BENCH_HOME` here.

```
# 1. checkout luceneutil:
# Choose a suitable directory, e.g. ~/Projects/lucene/benchmarks.
mkdir $LUCENE_BENCH_HOME && cd $LUCENE_BENCH_HOME
git clone https://github.com/mikemccand/luceneutil.git util

# 2. Run the initial setup script
cd util
python src/python/initial_setup.py -download

# you can run with -h option for help
python src/python/initial_setup.py -h
```
  
In the second step, the setup procedure creates all necessary directories in the clones parent directory and downloads
datasets to run the benchmarks on. By default, it downloads a 6 GB compressed Wikipedia line doc file, and a 13 GB vectors
file from Apache mirrors. If you don't want to download the large data files,
just remove the `-download` flag from the commandline.

After the download has completed, extract the lzma file in `$LUCENE_BENCH_HOME/data`. You can do this using the `xz` tool,
or the `lmza` tool, or any other tool of your choice. For example:
```bash
cd $LUCENE_BENCH_HOME/data
# using xz
xz -d enwiki-20120502-lines-1k-fixed-utf8-with-random-label.txt.lzma
# using lmza
lzma -d enwiki-20120502-lines-1k-fixed-utf8-with-random-label.txt.lzma
```

### (Optional, for development) set up IntelliJ
Should be able to open by IntelliJ automatically. The gradle will write a local configuration file `gradle.properties` in
which you can configure your local lucene repository so that intellij will use it as external library and code suggestion
will work. Also because the compilation is looking for jar so you have to build your lucene repo (run `./gradlew jar`) manually if you haven't 
done so.
Note the gradle build will NOT be able to compile the whole project because
some codes do have errors so we still need to filter which files to compile (see competitions.py). So you still
need to follow the rest procedure.

### Preparing the benchmark candidates

The benchmark compares a baseline version of Lucene to a patched one. Therefore we need two checkouts of Lucene, for example:

* `$LUCENE_BENCH_HOME/lucene_baseline`: contains a complete git clone of Lucene, this is the baseline for comparison
* `$LUCENE_BENCH_HOME/lucene_candidate`: contains a complete git clone of Lucene with some change applied that should be benchmarked against the baseline.

The main branch of Lucene can be checked out with

```
cd $LUCENE_BENCH_HOME
git clone https://github.com/apache/lucene.git lucene_baseline
```

Adjust the command accordingly for `lucene_candidate`.

### Running a first benchmark

`initial_setup.py` has created two files: `localconstants.py`, and `localrun.py` in `$LUCENE_BENCH_HOME/util/src/python/`. 

The file `localconstants.py` should be used to override any existing constants in `constants.py`, for example if you want to change the Java commandline used to run benchmarks. To run an initial benchmark you don't need to modify this file.

Now you can start editing `localrun.py` to define your comparison, at the
bottom near its `__main__`:

This file is a copy of `example.py` and should be used to define your
comparisons. You don't have to build 2 separate indexes; you can make
one and pass it to the two different competitors if you are only benching
some code difference but not a file format change.

To run the benchmark you first test like this:

```
cd $LUCENE_BENCH_HOME/util
python src/python/localrun.py -source wikimedium10k
```

Then once you confirm that everything works, use the `wikimediumall` corpus for all subsequent runs.
Using this much much larger corpus (33+ million docs) is necessary to draw conclusions from your benchmark results.
```
python src/python/localrun.py -source wikimediumall
```

If you get ClassNotFound exceptions, your Lucene checkouts may need to be rebuilt. Run `./gradlew jar` in both `lucene_candidate/` and `lucene_baseline/` dirs.

If your benchmark fails with "facetDim Date was not indexed" or similar, try adding

    facets = (('taxonomy:Date', 'Date'),('sortedset:Month', 'Month'),('sortedset:DayOfYear', 'DayOfYear'))
    index = comp.newIndex('lucene_baseline', sourceData, facets=facets, indexSort='dayOfYearNumericDV:long')

in `localrun.py`, and use that index in your benchmarks.

### Additional Run Options
You can also make the benchmark use baseline or candidate repository that exists outside of the directory structure above. 
Simply use `-b <Baseline repo path>` or `-c <Candidate repo path>` as shown below:
```bash
python src/python/localrun.py -source wikimediumall -b /Users/vigyas/repos/lucene -c /Users/vigyas/forks/lucene
```

While benchmarking an indexing side change, you might want to recreate the index for your candidate run. Use the `-r / --reindex` arg as follows:
```bash
python src/python/localrun.py -source wikimediumall -r
```

For details on all the available options, use the `-h` or `--help` parameter.

# Running the geo benchmark

This one is different and self-contained. Read the command-line examples at the top of src/main/perf/IndexAndSearchOpenStreetMaps.java

# Creating line doc file from an arbitrary Wikimedia dump data

You can create your own line doc file from an arbitrary Wikimedia dump by following steps.  Note that the `src/python/createJapaneseWikipediaLineDocsFile.py` helper tool does these steps:

1. Download Wikimedia dump (XML) from https://dumps.wikimedia.org/ and decompress it on `$YOUR_DATA_DIR`.

    e.g.:
    ```
    bunzip2 -d /data/jawiki/jawiki-20200620-pages-articles-multistream.xml.bz2
    ```

2. Run `src/python/wikiXMLToText.py` to extract attributes such as title and timestamp from the XML dump.

    e.g.:
    ```
    python src/python/wikiXMLToText.py /data/jawiki/jawiki-20200620-pages-articles-multistream.xml /data/jawiki/jawiki-20200620-text.txt
    ```

3. Run `src/python/WikipediaExtractor.py` to extract cleaned body text from the XML dump. This may take long time!

    e.g.:
    ```
    cat /data/jawiki/jawiki-20200620-pages-articles-multistream.xml | python -u src/python/WikipediaExtractor.py -b102400m -o /data/jawiki
    ```

4a. Combine the outputs of 2. and 3. by running `src/python/combineWikiFiles.py`.

    e.g.:
    ```
    python src/python/combineWikiFiles.py /data/jawiki/jawiki-20200620-text.txt /data/jawiki/AA/wiki_00 /data/jawiki/jawiki-20200620-lines.txt
    ```

4b. (Optional) If you want to strip all but the last three columns from the combined file, pass the `-only-three-columns` to combineWikiFiles.py:

    e.g.:
    ```
    python src/python/combineWikiFiles.py /data/jawiki/jawiki-20200620-text.txt /data/jawiki/AA/wiki_00 /data/jawiki/jawiki-20200620-lines.txt -only-three-columns
    ```

    Alternatively, use the Unix `cut` tool:

    ```
    # extract titie, timestamp and body text
    cat /data/jawiki/jawiki-20200620-lines.txt | cut -f1,2,3
    ```
# Running the KNN benchmark

Some knn-related tasks are included in the main benchmarks. If you specifically want to test
KNN/HNSW there is a script dedicated to that in src/python/knnPerfTest.py which has instructions on
how to run it in its comments.

## Testing with higher dimension vectors

By default we use 100/300 dimension vectors, to use higher dimension vectors (more than 384, check `highDimDataSets` in `gradle/knn.gradle`), you need to:

1. run `./gradlew vectors-mpnet` or `./gradlew vectors-minilm` depend on your needs (this step will run `infer_token_vectors.py` for you, and then generate task and document vectors)
2. run `src/python/localrun.py` (see instructions inside `src/python/vector-test.py`) or `src/python/knnPerfTest.py` (see instructions inside the file) of your choice, 

To test vector search with [Cohere/wikipedia-22-12-en-embeddings](https://huggingface.co/datasets/Cohere/wikipedia-22-12-en-embeddings) dataset, you need to do:
1. run `python src/python/infer_token_vectors_cohere.py -d 10000000 -q 10000` to generate vectors
in the format that luceneutil vector search can understand. Instead of `10000000` increase the number of documents to `100000000` if you want to run vector search on 10M documents.
2. In `src/python/knnPerfTest.py` uncomment lines that define doc and query vectors for cohere dataset.
3. run `src/python/knnPerfTest.py` 

# Running the facets benchmark

## Compare facet implementations

There are currently two facets implementations - one that first collects document IDs and then computes facets
in a separate phase, and a new implementation that computes facets during collection.

To compare performance for the two implementations run

```
python src/python/localrunFacets.py -source facetsWikimediumAll
```

Note that only comparison of taxonomy based facets is supported at the moment. We need to add SSDV facets support
to the sandbox facets module, as well as add support for other facet types to this package.
