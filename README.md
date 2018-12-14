# Luceneutil: Lucene benchmarking utilities

### Setting up luceneutil

First, pick a root directory, under which luceneutil will be checked out,
datasets exist, indices are built, Lucene source code is checked out,
etc.. We'll refer to this directory as `$LUCENE_BENCH_HOME` here.

```
# 1. checkout luceneutil:
# Choose a suitable directory, e.g. ~/Projects/lucence/benchmarks.
mkdir $LUCENE_BENCH_HOME && cd $LUCENE_BENCH_HOME
git clone https://github.com/mikemccand/luceneutil.git util

# 2. Run the setup script
cd util
python src/python/setup.py -download
```
  
In the second step, the setup procedure creates all necessary directories in the clones parent directory and downloads a
6 GB compressed Wikipedia line doc file from an Apache mirror. If you don't want to
download the large data file just remove the `-download` flag from the commandline. 

After the download has completed, extract the lzma file in `$LUCENE_BENCH_HOME/data`.

### Preparing the benchmark candidates

The benchmark compares a baseline version of Lucene to a patched one. Therefore we need two checkouts of Lucene, for example:

* `$LUCENE_BENCH_HOME/lucene_baseline`: contains a complete svn checkout of Lucene, this is the baseline for comparison
* `$LUCENE_BENCH_HOME/lucene_candidate`: contains a complete svn checkout of Lucene with some change applied that should be benchmarked against the baseline.

A trunk version of Lucene can be checked out with

```
cd $LUCENE_BENCH_HOME
svn checkout https://svn.apache.org/repos/asf/lucene/dev/trunk lucene_baseline
```

Adjust the command accordingly for `lucene_candidate`.

### Running a first benchmark

`setup.py` has created two files: `localconstants.py`, and `localrun.py` in `$LUCENE_BENCH_HOME/util/src/python/`. 

The file `localconstants.py` should be used to override any existing constants in `constants.py`, for example if you want to change the Java commandline used to run benchmarks. To run an inintal benchmark you don't need to modify this file. 

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

If you get ClassNotFound exceptions, your Lucene checkouts may need to be rebuilt. Run `ant clean jar` in both `lucene_candidate/lucene` and `lucene_baseline/lucene` dirs.

If your benchmark fails with "facetDim Date was not indexed" or similar, try adding

    facets = (('taxonomy:Date', 'Date'),('sortedset:Month', 'Month'),('sortedset:DayOfYear', 'DayOfYear'))
    index = comp.newIndex('lucene_baseline', sourceData, facets=facets, indexSort='dayOfYearNumericDV:long')

in `localrun.py`, and use that index in your benchmarks.

# Running the geo benchmark

This one is different and self-contained. Read the command-line examples at the top of src/main/perf/IndexAndSearchOpenStreetMaps.java
