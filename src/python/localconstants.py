BASE_DIR = '/l'
BENCH_BASE_DIR = '/l/util'
# nocommit
#JAVA_COMMAND = 'java -Xmx14g -server -XX:-UseCompressedOops -XX:+UnlockExperimentalVMOptions -XX:+UseG1GC -Xbatch'
#JAVA_COMMAND = 'java -XX:+PrintCompilation -Xms2g -Xmx2g -server'
#JAVA_COMMAND = 'java -Xms4g -Xmx4g -XX:-TieredCompilation -XX:+HeapDumpOnOutOfMemoryError -server -Xbatch'
#JAVA_COMMAND = 'java -Xms4g -Xmx4g -server'
#JAVA_COMMAND = '/usr/local/src/ibmj9.20111111_094827/bin/java -Xmx2g -server'
#JAVA_COMMAND = 'java -Xms2g -Xmx2g -server -XX:CICompilerCount=1 -Xbatch'
#JAVA_COMMAND = 'java -Xbatch -Xms2g -Xmx2g -server'
#JAVA_EXE = JAVA_COMMAND = 'java -XX:-UseCompressedOops -XX:+UseSerialGC'
#JAVA_EXE = JAVA_COMMAND = '/usr/local/src/jdk1.8.0_45/bin/java -Xmx2g -Xms2g -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC'
#JAVA_EXE = JAVA_COMMAND = '/usr/local/src/jdk1.8.0_45/bin/java -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -Xcomp -XX:+DeoptimizeRandom'
JAVA_EXE = JAVA_COMMAND = '/usr/local/src/jdk1.8.0_51/bin/java -Xmx2g -Xms2g -server -XX:+UseParallelGC -XX:+UseCompressedOops'
INDEX_NUM_THREADS = 4
SEARCH_NUM_THREADS = 3

# 25033.8 MB, 27625038 docs:
#WIKI_MEDIUM_DOCS_LINE_FILE = '/p/lucene/data/enwiki-20110115-lines-1k-fixed.txt'

# 23865.9 MB, 5982049 docs:
#WIKI_BIG_DOCS_LINE_FILE = '/p/lucene/data/enwiki-20110115-lines.txt'

# INDEX_DIR_BASE = '/p/lucene/indices'

# 3319.6 MB, 5607746 docs:
EUROPARL_MEDIUM_DOCS_LINE_FILE = '%s/data/europarl.para.lines.txt' % BASE_DIR

#WIKI_MEDIUM_DOCS_LINE_FILE = '/p/lucene/data/enwiki-20100302-pages-articles-lines-1k.txt'
#WIKI_MEDIUM_DOCS_LINE_FILE = '/x/lucene/data/enwiki/enwiki-20100302-pages-articles-lines-1k.txt'

# enwiki-20120502-lines-1k.txt has 33332620 docs
# enwiki-20120502-lines.txt has 6726515 docs

WIKI_MEDIUM_DOCS_COUNT = 33332620

WIKI_MEDIUM_DOCS_LINE_FILE = '/lucenedata/enwiki/enwiki-20120502-lines-1k.txt'
#WIKI_MEDIUM_DOCS_LINE_FILE = '/lucenedata/enwiki/enwiki-tiny-lines-1k.txt'
WIKI_MEDIUM_TASKS_FILE = '/p/lucene/data/wikimedium500.tasks'

NIGHTLY_MEDIUM_LINE_FILE = WIKI_MEDIUM_DOCS_LINE_FILE
NIGHTLY_MEDIUM_INDEX_NUM_DOCS = WIKI_MEDIUM_DOCS_COUNT

#WIKI_BIG_DOCS_LINE_FILE = '/lucenedata/enwiki/enwiki-20120502-lines.txt'
WIKI_BIG_DOCS_LINE_FILE = '/lucenedata/enwiki/enwiki-20130102-lines.txt'
WIKI_BIG_DOCS_COUNT = 6647577

# Cleaned out silly "just a redirect" docs:
#WIKI_BIG_DOCS_LINE_FILE = '/lucenedata/enwiki/enwiki-20130102-lines-cleaned.txt'
WIKI_BIG_DOCS_LINE_FILE = '/l/enwiki-20110115-lines.txt'
WIKI_BIG_DOCS_COUNT = 3978548

# nocommit
#INDEX_DIR_BASE = '/p/lucene/indices'
INDEX_DIR_BASE = '/l/indices'

REPRO_COMMAND_START = 'ot'
#REPRO_COMMAND_END = '>& /dev/shm/out.x'

if True:
  print
  print('***USING BIG LINE FILE***')
  print
  TESTS_LINE_FILE = '/lucenedata/hudson.enwiki.random.lines.txt.fixed'

RESOURCES = (
  #('vine', 6),
  ('beast', 12),
  ('scratch', 2),
  #('mikelaptop', 2),
  #('janedesktop', 2),
  ('haswell', 6),
  ('10.17.4.190', 40),
  #('10.17.4.166', 3),
  )

#RESOURCES = (
#  ('vine', 1),
#)

ANT_EXE = '/usr/local/src/apache-ant-1.9.5/bin/ant'

# SORT_REPORT_BY = 'query'

# DO_PERF = True

BUILD_FST_PATH = '/l/util'

LUCENE_JAR = '/l/buildfst/lucene/build/core/lucene-core-4.4-SNAPSHOT.jar'

NIGHTLY_LOG_DIR = '%s/logs.nightly' % BASE_DIR
NIGHTLY_REPORTS_DIR = '%s/reports.nightly' % BASE_DIR
