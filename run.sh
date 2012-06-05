#!/bin/bash

# You must set $LUCENE_HOME to /path/to/checkout/lucene:

java -XX:-UseCompressedOops -Xmx6G -cp .:$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/highlighter/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java perf.SearchPerfTest \
    -indexPath /indices/testwiki \
    -dirImpl MMapDirectory \
    -analyzer EnglishAnalyzer \
    -taskSource server:localhost:7777 \
    -searchThreadCount 15 \
    -field body \
    -similarity DefaultSimilarity \
    -commit multi \
    -seed 0 \
    -staticSeed 0
