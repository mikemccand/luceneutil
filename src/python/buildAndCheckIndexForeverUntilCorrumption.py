import shutil
import datetime
import os

import constants
import competition
import benchUtil

# NOTE: relative to localconstants.BASE_DIR:
LUCENE_TRUNK_ROOT = 'trunk.nightly'

# Where to write indices:
INDEX_PATH = '/q/lucene/indices/corruption'

# Docs to index:
LINE_DOCS_FILE = '/lucene/data/enwiki-20110115-lines-1k-fixed.txt'

# Customize the java command-line:
JAVA_CMD = '/usr/local/src/jdk1.8.0_45/bin/java -Xms4g -Xmx4g'

# Should we use ConcurrentMergeScheduler?
USE_CMS = True

# How many indexing threads to use
INDEX_THREADS = 4

# MAX_BUFFERED_DOCS = 49774
# INDEXING_BUFFER_MB = -1

MAX_BUFFERED_DOCS = 5000
INDEXING_BUFFER_MB = -1

BODY_FIELD_TERM_VECTORS = True

# DOC_COUNT = 27625038
# DOC_COUNT = 100000
DOC_COUNT = 10000000

print('Compile luceneutil and %s/%s...' % (constants.BASE_DIR, LUCENE_TRUNK_ROOT))
r = benchUtil.RunAlgs(JAVA_CMD, False)
c = competition.Competitor('foo', LUCENE_TRUNK_ROOT)
c.compile(benchUtil.classPathToString(benchUtil.getClassPath(c.checkout)))

while True:
  print
  print('%s: create index' % datetime.datetime.now())

  shutil.rmtree(INDEX_PATH)

  cmd = '%s -classpath "ROOT/lucene/build/core/classes/java:ROOT/lucene/build/core/classes/test:ROOT/lucene/build/sandbox/classes/java:ROOT/lucene/build/misc/classes/java:ROOT/lucene/build/facet/classes/java:/home/mike/src/lucene-c-boost/dist/luceneCBoost-SNAPSHOT.jar:ROOT/lucene/build/analysis/common/classes/java:ROOT/lucene/build/analysis/icu/classes/java:ROOT/lucene/build/queryparser/classes/java:ROOT/lucene/build/grouping/classes/java:ROOT/lucene/build/suggest/classes/java:ROOT/lucene/build/highlighter/classes/java:ROOT/lucene/build/codecs/classes/java:ROOT/lucene/build/queries/classes/java:lib/HdrHistogram.jar:build" perf.Indexer -dirImpl MMapDirectory -indexPath "%s" -analyzer StandardAnalyzerNoStopWords -lineDocsFile %s -docCountLimit %s -threadCount %d -maxConcurrentMerges 3 -dvfields -ramBufferMB %s -maxBufferedDocs %d -postingsFormat Lucene84 -waitForMerges -mergePolicy LogDocMergePolicy -facets Date -facetDVFormat Lucene84 -idFieldPostingsFormat Memory'.replace('ROOT', '%s/%s' % (constants.BASE_DIR, LUCENE_TRUNK_ROOT)) % (JAVA_CMD, INDEX_PATH, LINE_DOCS_FILE, DOC_COUNT, INDEX_THREADS, INDEXING_BUFFER_MB, MAX_BUFFERED_DOCS)

  if USE_CMS:
    cmd += ' -useCMS'

  if BODY_FIELD_TERM_VECTORS:
    cmd += ' -tvs'

  print('  run: %s' % cmd)
  if os.system(cmd):
    raise RuntimeError('failed to build index')

  print('%s: check index' % datetime.datetime.now())
  cmd = '%s -cp ROOT/lucene/build/core/classes/java:ROOT/lucene/build/codecs/classes/java org.apache.lucene.index.CheckIndex -crossCheckTermVectors %s/index'.replace('ROOT', '%s/%s' % (constants.BASE_DIR, LUCENE_TRUNK_ROOT)) % (JAVA_CMD, INDEX_PATH)
  print('  run: %s' % cmd)
  if os.system(cmd):
    raise RuntimeError('CheckIndex failed')

