#!/bin/bash

# You must set $LUCENE_HOME to /path/to/checkout/lucene:

LUCENE_HOME=/lucene/clean2.svn/lucene
#LUCENE_HOME=/l/lucene.trunk/lucene
#LUCENE_HOME=/localhome/lucene4x/lucene

javac -Xlint -Xlint:deprecation -cp $LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/highlighter/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java perf/Args.java perf/IndexThreads.java perf/OpenCloseIndexWriter.java perf/Task.java perf/CreateQueries.java perf/LineFileDocs.java perf/PKLookupPerfTest.java perf/RandomFilter.java perf/SearchPerfTest.java perf/TaskParser.java perf/Indexer.java perf/LocalTaskSource.java perf/PKLookupTask.java perf/RemoteTaskSource.java perf/SearchTask.java perf/TaskSource.java perf/IndexState.java perf/NRTPerfTest.java perf/RespellTask.java perf/ShowFields.java perf/TaskThreads.java perf/KeepNoCommitsDeletionPolicy.java
