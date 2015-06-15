import datetime
import os
import sys

import constants
import competition
import benchUtil

# NOTE: relative to localconstants.BASE_DIR:
LUCENE_TRUNK_ROOT = 'trunk.nightly'

# Where to write indices:
INDEX_PATH = '/q/lucene/indices/corruption'

# Customize the java command-line:
JAVA_CMD = 'java'

# nocommit
DOC_COUNT = 27625038
# DOC_COUNT = 100000

print('Compile luceneutil and %s/%s...' % (constants.BASE_DIR, LUCENE_TRUNK_ROOT))
r = benchUtil.RunAlgs(JAVA_CMD, False)
c = competition.Competitor('foo', LUCENE_TRUNK_ROOT)
c.compile(r.classPathToString(r.getClassPath(c.checkout)))

while True:
  print
  print('%s: create index' % datetime.datetime.now())

  if os.system('java -classpath "ROOT/lucene/build/core/classes/java:ROOT/lucene/build/core/classes/test:ROOT/lucene/build/sandbox/classes/java:ROOT/lucene/build/misc/classes/java:ROOT/lucene/build/facet/classes/java:/home/mike/src/lucene-c-boost/dist/luceneCBoost-SNAPSHOT.jar:ROOT/lucene/build/analysis/common/classes/java:ROOT/lucene/build/analysis/icu/classes/java:ROOT/lucene/build/queryparser/classes/java:ROOT/lucene/build/grouping/classes/java:ROOT/lucene/build/suggest/classes/java:ROOT/lucene/build/highlighter/classes/java:ROOT/lucene/build/codecs/classes/java:ROOT/lucene/build/queries/classes/java:lib/HdrHistogram.jar:build" perf.Indexer -dirImpl MMapDirectory -indexPath "%s" -analyzer StandardAnalyzerNoStopWords -lineDocsFile /lucene/data/enwiki-20110115-lines-1k-fixed.txt -docCountLimit %s -threadCount 1 -maxConcurrentMerges 3 -dvfields -ramBufferMB -1 -maxBufferedDocs 49774 -postingsFormat Lucene50 -waitForMerges -mergePolicy LogDocMergePolicy -facets Date -facetDVFormat Lucene50 -idFieldPostingsFormat Memory'.replace('ROOT', '%s/%s' % (constants.BASE_DIR, LUCENE_TRUNK_ROOT)) % (INDEX_PATH, DOC_COUNT)):
    raise RuntimeError('failed to build index')

  print('%s: check index' % datetime.datetime.now())
  if os.system('java -cp ROOT/lucene/build/core/classes/java:ROOT/lucene/build/codecs/classes/java org.apache.lucene.index.CheckIndex -fast %s/index'.replace('ROOT', '%s/%s' % (constants.BASE_DIR, LUCENE_TRUNK_ROOT)) % INDEX_PATH):
    raise RuntimeError('CheckIndex failed')

