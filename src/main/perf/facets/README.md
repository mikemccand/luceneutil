#How to use this benchmark

##1. Downloading the data
This benchmark uses the National Address Database dataset, which can be found 
[here](https://www.transportation.gov/gis/national-address-database/national-address-database-nad-disclaimer). The
actual download link for NAD Release 8, which this benchmark was tested with, can be found
[here](https://nationaladdressdata.s3.amazonaws.com/NAD_r8_TXT.zip). You can try with a newer release version, but there
is no guarantee it will work with this benchmark.

Once you have downloaded the data, put it into the `benchmarks/data` folder and then run the `generateNADTaxonomies.py`
script in `src/python`. This script takes no arguments and will look for the `NAD_r8_TXT.zip` file you downloaded in the
`benchmarks/data` folder. It will then create an `NAD_taxonomy.txt.gz` file which will be what we actually use as the
source data for this benchmark.

##2. Running the benchmark

###Building the index

Compile `IndexFacets.java` with the following command:
```commandline
javac -d ../../../../build -cp ../../../../../../lucene/lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar:../../../../../../lucene/lucene/facet/build/libs/lucene-facet-10.0.0-SNAPSHOT.jar:../../../../build IndexFacets.java
```
To index, we need to specify the source data and the index destination in the arguments (Note: this will delete the NAD
index if already built). Use the following command to do
so:
```commandline
java -cp ../../../../../../lucene/lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar:../../../../../../lucene/lucene/facet/build/libs/lucene-facet-10.0.0-SNAPSHOT.jar:../../../../build perf.facets.IndexFacets ../../../../../data/NAD_taxonomy.txt.gz ../../../../../indices/NADFacets
```

###Running the benchmark
Compile `BenchmarkFacets.java` with the following command:
```commandline
javac -d ../../../../build -cp ../../../../../../lucene/lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar:../../../../../../lucene/lucene/facet/build/libs/lucene-facet-10.0.0-SNAPSHOT.jar:../../../../build BenchmarkFacets.java
```
And then run the benchmark (make sure the index is already built). The command takes an argument to specify where the
index is built:
```commandline
java -cp ../../../../../../lucene/lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar:../../../../../../lucene/lucene/facet/build/libs/lucene-facet-10.0.0-SNAPSHOT.jar:../../../../build perf.facets.BenchmarkFacets ../../../../../indices/NADFacets
```