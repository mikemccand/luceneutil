#!/bin/bash

# You must set $LUCENE_HOME to /path/to/checkout/lucene:

java -XX:-UseCompressedOops -Xmx6G -cp .:$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/highlighter/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java perf.SearchPerfTest \
    -indexPath /q/lucene/indices/testwiki \
    -dirImpl MMapDirectory \
    -analyzer StandardAnalyzer \
    -taskSource server:localhost:7777 \
    -searchThreadCount 15 \
    -field body \
    -similarity DefaultSimilarity \
    -commit multi \
    -seed 0 \
    -staticSeed 0 \
    -nrt \
    -indexThreadCount 1 \
    -docsPerSecPerThread 200.0 \
    -lineDocsFile /x/lucene/data/enwiki/enwiki-20120502-lines-1k.txt \
    -reopenEverySec 1.0 \
    -store \
    -tvs \
    -postingsFormat Lucene40 \
    -idFieldPostingsFormat Lucene40

    
