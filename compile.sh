#!/bin/bash

# try to make the script Windows-friendly, if running from Cygwin
if [ `uname -o` = "Cygwin" ]; then
  WINDOWS="true"
  CLASSPATH_SEP=";"
else
  WINDOWS="false"
  CLASSPATH_SEP=":"
fi

# You must set $LUCENE_HOME to /path/to/checkout/lucene:

#LUCENE_HOME=d:/dev/lucene/lucene-trunk/lucene
LUCENE_HOME=/lucene/trunk.nightly/lucene
#LUCENE_HOME=/localhome/lucene4x/lucene

CLASSPATH=$LUCENE_HOME/build/core/classes/java,$LUCENE_HOME/build/misc/classes/java,$LUCENE_HOME/build/facet/classes/java,$LUCENE_HOME/build/facet/classes/java,$LUCENE_HOME/build/highlighter/classes/java,$LUCENE_HOME/build/test-framework/classes/java,$LUCENE_HOME/build/queryparser/classes/java,$LUCENE_HOME/build/suggest/classes/java,$LUCENE_HOME/build/analysis/common/classes/java,$LUCENE_HOME/build/grouping/classes/java,$LUCENE_HOME/build/sandbox/classes/java

CLASSPATH=`echo $CLASSPATH | tr "," "$CLASSPATH_SEP"`

$JAVA_HOME/bin/javac -Xlint -Xlint:deprecation -target 1.6 -source 1.6 -cp $CLASSPATH perf/Args.java perf/IndexThreads.java perf/OpenCloseIndexWriter.java perf/Task.java perf/CreateQueries.java perf/LineFileDocs.java perf/PKLookupPerfTest.java perf/RandomFilter.java perf/SearchPerfTest.java perf/TaskParser.java perf/Indexer.java perf/LocalTaskSource.java perf/PKLookupTask.java perf/RemoteTaskSource.java perf/SearchTask.java perf/TaskSource.java perf/IndexState.java perf/NRTPerfTest.java perf/RespellTask.java perf/ShowFields.java perf/TaskThreads.java perf/KeepNoCommitsDeletionPolicy.java perf/FacetGroup.java perf/OpenDirectory.java perf/StatisticsHelper.java
