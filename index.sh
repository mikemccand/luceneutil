#!/bin/bash

# You must set $LUCENE_HOME to /path/to/checkout/lucene:

java -XX:-UseCompressedOops -Xmx6G -cp .:$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java perf.Indexer \
    -indexPath /q/lucene/indices/memterms \
    -dirImpl MMapDirectory \
    -analyzer StandardAnalyzer \
    -lineDocsFile /x/lucene/data/enwiki/enwiki-20120502-lines-1k.txt \
    -docCountLimit -1 \
    -threadCount 5 \
    -ramBufferMB 1024 \
    -maxBufferedDocs 60058 \
    -postingsFormat MemoryTerms \
    -idFieldPostingsFormat MemoryTerms \
    -waitForMerges \
    -mergePolicy TieredMergePolicy \
    -verbose

#    -store \
#    -tvs \
