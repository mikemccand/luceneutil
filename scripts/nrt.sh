#!/bin/bash

# You must set $LUCENE_HOME to /path/to/checkout/lucene:

# -XX:+UseConcMarkSweepGC

# /opt/zing/zingLX-jdk1.6.0_31-5.2.0.0-18-x86_64/bin/java
# /root/jdk1.6.0_31/bin/java -XX:+UnlockExperimentalVMOptions -XX:+UseG1GC

/opt/zing/zingLX-jdk1.6.0_31-5.2.0.0-18-x86_64/bin/java -verbose:gc -Xms40G -Xmx40G -cp .:$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/highlighter/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java perf.SearchPerfTest \
    -indexPath /large/indices/wikimediumall.lucene4x.nd33.3326M/index \
    -dirImpl RAMDirectory \
    -analyzer StandardAnalyzer \
    -taskSource server:localhost:7777 \
    -searchThreadCount 20 \
    -field body \
    -similarity DefaultSimilarity \
    -commit multi \
    -seed 0 \
    -staticSeed 0 \
    -nrt \
    -indexThreadCount 1 \
    -docsPerSecPerThread 500.0 \
    -lineDocsFile /large/enwiki-20120502-lines-1k.txt \
    -reopenEverySec 1.0 \
    -postingsFormat Lucene40 \
    -idFieldPostingsFormat Memory

    
