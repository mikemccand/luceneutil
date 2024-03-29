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

```

This project uses Ant + Ivy to build and has been test to work correctly with ant version >= 1.10.13.
Install Apache ant and configure your path to use ant binaries. You may find [this page](https://ant.apache.org/manual/install.html) useful for the same.

Next, configure the project to use Ivy by

```
# 2. Initialize Ivy
ant bootstrap
```
Use `ant -projecthelp` to know more about all the targets that can be used.

Update the `lucene.checkout` path in the `build.properties` file which can be used by Ant to compile java class in this 
project. By default it is set the lucene_baseline path as also mentioned in the section
[Preparing the benchmark candidates](#Preparing the benchmark candidates)


This will download ivy jars in the $USER_HOME/.ant folder.

```
# 3. Run the setup script
cd util
python src/python/setup.py -download

# you can run with -h option for help
python src/python/setup.py -h
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

### Preparing the benchmark candidates

The benchmark compares a baseline version of Lucene to a patched one. Therefore we need two checkouts of Lucene, for example:

* `$LUCENE_BENCH_HOME/lucene_baseline`: contains a complete svn checkout of Lucene, this is the baseline for comparison
* `$LUCENE_BENCH_HOME/lucene_candidate`: contains a complete svn checkout of Lucene with some change applied that should be benchmarked against the baseline.

A trunk version of Lucene can be checked out with

```
cd $LUCENE_BENCH_HOME
git clone https://github.com/apache/lucene.git lucene_baseline
```

Adjust the command accordingly for `lucene_candidate`.

Compile and build the lucene_baseline and lucene_candidate projects as per its README.txt

If you checkout Lucene in a different directory, update the same in the build.properties to use the right lucene jars in
this project for compilation. 

### (Optional, for development) set up IntelliJ
You can open the project as usual in IntelliJ. 

Select src/main folder as Source Root. `Right Click src/main --> Mark Directory As --> Source Root`

Select src/python folder as source Root. `Right Click src/python --> Mark Directory As --> Source Root`

For configuring the right jar files you will have to do the following.
The jars that you need are present in at 3 places.
1. Apache Lucene jars. Which are present in your ${lucene.checkout}/lucene folder. 
2. Some jars will be downloaded by Ant-Ivy build system of this project into the `./libs` folder of this package.
3. Some jars are shipped with this project which are present in the `./lib` folder.

IntelliJ Idea does not automatically detect these jars in the classpath. So you will have to do following steps to 
include them. Also, IntelliJ Idea by default do not recursively include all the jars in sub folders, so follow the below
instructions to enable recursive search in folder for both the above jar paths.

``
IntelliJ IDEA --> File --> Project Structure --> Modules --> luceneutil-root --> Dependencies --> + 
--> Jars And Directories -->  Select the path to ${lucene.checkout}/lucene --> Open --> Choose Roots (Unselect All) --> 
OK --> Choose Categories of Selected Files --> Jar Directory --> OK --> Apply --> OK
``

After this you will have to update the file in .idea folder manually to enable 'recursive' jar discovery. Open your 
project's `.iml` file and change the `<jarDirectory>` xml tag's recursive attribute to `true`
` vim $luceneutil.root/.idea/luceneutil.iml `

Change line like below from recursive="false" to recursive="true"
``` xml
<library>
  ...
  <jarDirectory url="file://$MODULE_DIR$/../../lucene/lucene" recursive="false" />
</library>
  ```

This will make idea search jar files recursively in lucene project.
Alternatively, you can configure idea with the source code of Lucene as a library, by not doing "unselecting all" in the `Choose Roots` step above.

Repeat the same process for lib and libs folder in $luceneutil.root folder.

### Running a first benchmark

`setup.py` has created two files: `localconstants.py`, and `localrun.py` in `$LUCENE_BENCH_HOME/util/src/python/`. 

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

By default we use 100 dimension vectors, to use higher dimension vectors, you need to:

1. run `src/python/infer_token_vectors.py` to get `xxx.vec` and `xxx.tok` file, also do not forget to set the model you need by editing `infer_token_vectors.py`, The supported models are listed there in comments. E.g. for 768 dimensions you need `enwiki-20120502-mpnet.vec` and `enwiki-20120502-mpnet.tok` as output file and you need to set the model to `model = SentenceTransformer('all-mpnet-base-v2')` by edit `infer_token_vectors.py` (which is already the default).
2. run corresponding ant tasks to generate embeddings for docs and queries. E.g. for 768 dimensions you need to run `ant vectors-mpnet-docs` and `vectors-mpnet-tasks`.
3. run `src/python/localrun.py` (see instructions inside `src/python/vector-test.py`) or `src/python/knnPerTest.py` (see instructions inside the file) of your choice, 

To test vector search with [Cohere/wikipedia-22-12-en-embeddings](https://huggingface.co/datasets/Cohere/wikipedia-22-12-en-embeddings) dataset, you need to do:
1. run `python src/python/infer_token_vectors_cohere.py ../data/cohere-wikipedia-768.vec 10000000 ../data/cohere-wikipedia-queries-768.vec 10000` to generate vectors
in the format that luceneutil vector search can understand. Instead of `10000000` increase the number of documents to `100000000` if you want to run vector search on 10M documents.
2. In `src/python/knnPerTest.py` uncomment lines that define doc and query vectors for cohere dataset.
3. run `src/python/knnPerTest.py` 