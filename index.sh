#!/bin/bash

# You must set $LUCENE_HOME to /path/to/checkout/lucene:

LUCENE_HOME=/lucene/clean2.svn/lucene

#./compile.sh



INDEX_PATH=/q/lucene/indices/all
LINE_DOCS_FILE=/lucenedata/enwiki/enwiki-20120502-lines-1k.txt

THREAD_COUNT=12

DOC_COUNT_LIMIT=33332620
#MAX_BUFFERED_DOCS=60058
MAX_BUFFERED_DOCS=-1

#DOC_COUNT_LIMIT=1000000
#MAX_BUFFERED_DOCS=2703

#DOC_COUNT_LIMIT=6000000
#MAX_BUFFERED_DOCS=21622

JAVA=/usr/local/src/jdk1.7.0_07/bin/java

HEAP=-Xmx10g
PF=Lucene41

#LUCENE_HOME=/localhome/lucene4x/lucene
#INDEX_PATH=/localhome/indices/direct.1M
#LINE_DOCS_FILE=/localhome/data/enwiki-20120502-lines-1k.txt
#THREAD_COUNT=16
#DOC_COUNT_LIMIT=-1
#DOC_COUNT_LIMIT=1000000
#JAVA=/opt/zing/zingLX-jdk1.6.0_31-5.2.0.0-18-x86_64/bin/java
#HEAP=-Xmx400g
#PF=Lucene40

$JAVA $HEAP -cp .:$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/facet/classes/java:$LUCENE_HOME/build/codecs/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java perf.Indexer \
    -indexPath $INDEX_PATH \
    -dirImpl MMapDirectory \
    -analyzer StandardAnalyzerNoStopWords \
    -lineDocsFile $LINE_DOCS_FILE \
    -docCountLimit $DOC_COUNT_LIMIT \
    -threadCount $THREAD_COUNT \
    -ramBufferMB 350 \
    -maxBufferedDocs $MAX_BUFFERED_DOCS \
    -postingsFormat $PF \
    -idFieldPostingsFormat $PF \
    -waitForMerges \
    -mergePolicy LogDocMergePolicy \
    -dateFacets


#rm -rf /q/lucene/indices/test.10M

#java -Xms10g -Xmx10g -server -classpath "$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/core/classes/test:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/contrib/misc/classes/java:$LUCENE_HOME/build/facet/classes/java:$LUCENE_HOME/test-framework/lib/ant-1.8.2.jar:$LUCENE_HOME/test-framework/lib/junit-4.10.jar:$LUCENE_HOME/test-framework/lib/randomizedtesting-runner-2.0.4.jar:$LUCENE_HOME/test-framework/lib/junit4-ant-2.0.4.jar:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/analysis/icu/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/grouping/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/highlighter/classes/java:$LUCENE_HOME/build/codecs/classes/java:/lucene/util.trunk" perf.Indexer -dirImpl MMapDirectory -indexPath "/q/lucene/indices/wikimedium10m.clean2.svn.facets.Lucene41.nd10M" -analyzer StandardAnalyzerNoStopWords -lineDocsFile /lucenedata/enwiki/enwiki-20120502-lines-1k.txt -docCountLimit 10000000 -threadCount 12 -ramBufferMB 350 -maxBufferedDocs -1 -postingsFormat Lucene41 -waitForMerges -mergePolicy LogDocMergePolicy -dateFacets -idFieldPostingsFormat Lucene41