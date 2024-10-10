rm -rf /l/scratch/indices/geonames*

cd core
ant jar
cd ..

javac -d /l/util/build -cp build/core/classes/java:build/analysis/common/classes/java /l/util/src/extra/perf/IndexGeoNames.java

javac -d /l/util/build -cp build/core/classes/java:build/analysis/common/classes/java /l/util/src/extra/perf/SearchGeoNames.java

java -Xmx4g -Xms4g -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.IndexGeoNames /lucenedata/geonames/allCountries.txt /l/scratch/indices/geonames4 1 4 >& geo.index4.log

java -cp build/core/classes/java org.apache.lucene.index.CheckIndex /l/scratch/indices/geonames4 >& geo.checkindex4.log

java -Xmx4g -Xms4g -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.IndexGeoNames /lucenedata/geonames/allCountries.txt /l/scratch/indices/geonames8 1 8 >& geo.index8.log

java -cp build/core/classes/java org.apache.lucene.index.CheckIndex /l/scratch/indices/geonames8 >& geo.checkindex8.log

java -Xmx4g -Xms4g -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.IndexGeoNames /lucenedata/geonames/allCountries.txt /l/scratch/indices/geonames16 1 16 >& geo.index16.log

java -cp build/core/classes/java org.apache.lucene.index.CheckIndex /l/scratch/indices/geonames16 >& geo.checkindex16.log

java -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.SearchGeoNames /l/scratch/indices/geonames4 4 > geo.search4.log

java -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.SearchGeoNames /l/scratch/indices/geonames8 8 > geo.search8.log

java -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.SearchGeoNames /l/scratch/indices/geonames16 16 > geo.search16.log
