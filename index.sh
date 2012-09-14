#!/bin/bash

# You must set $LUCENE_HOME to /path/to/checkout/lucene:

LUCENE_HOME=/l/4x.beta.azul/lucene
LINE_DOCS_FILE=/x/lucene/data/enwiki/enwiki-20120502-lines-1k.txt
THREAD_COUNT=6
DOC_COUNT_LIMIT=1000000
JAVA=/usr/local/src/jdk1.7.0_05/bin/java
#JAVA=/usr/local/src/zingLX-jdk1.6.0_31-5.2.1.0-3/bin/java
HEAP=-Xmx10g
#PF=Lucene40
#INDEX_PATH=/l/scratch/indices/lucene40.1M
PF=Direct
INDEX_PATH=/l/scratch/indices/Direct.1M

#LUCENE_HOME=/localhome/lucene4x/lucene
#INDEX_PATH=/localhome/indices/direct.1M
#LINE_DOCS_FILE=/localhome/data/enwiki-20120502-lines-1k.txt
#THREAD_COUNT=16
#DOC_COUNT_LIMIT=-1
#DOC_COUNT_LIMIT=1000000
#JAVA=/opt/zing/zingLX-jdk1.6.0_31-5.2.0.0-18-x86_64/bin/java
#HEAP=-Xmx400g
#PF=Lucene40

$JAVA $HEAP -cp .:$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java perf.Indexer \
    -indexPath $INDEX_PATH \
    -dirImpl MMapDirectory \
    -analyzer StandardAnalyzer \
    -lineDocsFile $LINE_DOCS_FILE \
    -docCountLimit $DOC_COUNT_LIMIT \
    -threadCount $THREAD_COUNT \
    -ramBufferMB 1024 \
    -maxBufferedDocs 60058 \
    -postingsFormat $PF \
    -idFieldPostingsFormat $PF \
    -waitForMerges \
    -mergePolicy LogDocMergePolicy \
    -verbose \
    -store \
    -tvs
