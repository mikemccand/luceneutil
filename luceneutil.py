# TODO rename to local.py?
ANALYZER='org.apache.lucene.analysis.en.EnglishWDFAnalyzer'
BASE_DIR="/lucene"

# all your checkouts should be under BASE_DIR, ie BASE_DIR/aaa BASE_DIR/bbb etc

BENCH_BASE_DIR = '%s/util/' % BASE_DIR
WIKI_LINE_FILE = '%s/data/enwiki-20100302-lines-1k.txt' % BASE_DIR
INDEX_DIR_BASE = '%s/indices' % BENCH_BASE_DIR
LOG_DIR = '%s/logs' % BENCH_BASE_DIR
WIKI_FILE = '/%s/data/enwiki-20100302-pages-articles.xml.bz2' % BENCH_BASE_DIR
JAVA_COMMAND = 'java -Xbatch -Xms2g -Xmx2g -server'
