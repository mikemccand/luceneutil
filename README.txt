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

Run the setup script from the clone:

  * python setup.py -download -prepareTrunk
  
this will setup all necessary directories in the clones parent directory and downloads a
5GB compressed Wikipedia line doc file from an Apache Mirror. If you don't want to
download the large data file just remove the -download flag from the commandline. 

The -prepareTrunk checks out the latest Apache Lucene trunk into the clones
parent directory. If you have an existing checkout you can skip this option as well.
  
To run luceneutils benchmark scrips you also need a competitor that runs against 
the checked out trunk.

You can either copy the trunk folder and apply a patch to it or check it out 
again with a different name:

  * svn checkout https://svn.apache.org/repos/asf/lucene/dev/trunk patch 
    - and apply some patch in this checkout

# Running a first benchmark

setup.py has created a localconstants.py file in the clones top level directory 
(/lucene/util/localconstants.py). This file should be used to override any existing
constants in constants.py, for example if you want to change the Java commandline
used to run benchmarks. To run an inintal benchmark you don't need to modify this file. 

Now you can start editing a your localrun.py to define your comparison, at the
bottom near its __main__:

This file is a copy of the example.py and should be used to define your
comparisons. You don't have to build 2 separate indexes; you can make
one and pass it to the two different competitors if you are only benching
some code difference but not a file format change.

To run the benchmark you first test like this:

  python localrun.py 

Note: the defaul localrun.py has debug mode enabled, with debug mode on results are probably not reliable!
