#!/bin/bash
export LUCENE_HOME=/opt/dev/lucene/dev/branches/branch_5x/lucene

java -XX:-UseCompressedOops -Xmx1G -cp .:./target/classes:$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/sandbox/classes/java org.apache.lucene.search.GeoCalculator

