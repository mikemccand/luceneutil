Luceneutil: benchmarking utilities

Pick a root directory, under which luceneutil will be checked out,
datasets exist, indices are built, Lucene source code is checked out,
etc.  I use /lucene below.

The way the benchmark works, is you create a hierarchy like this:
/somewhere/lucene1 <-- a complete svn checkout
/somewhere/lucene2 <-- another complete svn checkout

First, checkout luceneutil:

  * cd /lucene

  * hg clone https://hg.codespot.com/a/apache-extras.org/luceneutil/ util

Copy down the dataset we've been using:

  * mkdir /lucene/data

  * cd /lucene/data

  * wget http://people.apache.org/~mikemccand/enwiki-20100302-pages-articles-lines-1k.txt.bz2

Create indices directory:

  * mkdir /lucene/indices

Then, check stuff out under /lucene, eg:

  * cd /lucene

  * svn checkout https://svn.apache.org/repos/asf/lucene/dev/trunk trunk

  * svn checkout https://svn.apache.org/repos/asf/lucene/dev/trunk patch 
    - and apply some patch in this checkout

Create /lucene/util/localconstants.py and put this line (change
/lucene to your root path):

  BASE_DIR = '/lucene'

You can also override anything you see in constants.py in your
localconstants.py, for example if you want to change the Java
commandline.

Now you can edit searchBench.py to define your comparison, at the
bottom near its __main__:

You don't have to build 2 separate indexes; you can make one and pass
it to the two different competitors if you are only benching some code
difference but not a file format change.

To run the benchmark you first test like this:

  python searchBench.py -index -search -debug <-- builds a tiny version of any indexes for testing and searches

Note that results are probably not reliable!

Then run the "real" test:

  python searchBench.py -index -search <-- builds a big version of the indexes and searches
