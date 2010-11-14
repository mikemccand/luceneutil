
# NOTE: you must have a localconstants.py that, minimally, defines
# BASE_DIR; all your checkouts should be under BASE_DIR, ie
# BASE_DIR/aaa BASE_DIR/bbb etc.
from localconstants import *

ANALYZER='org.apache.lucene.analysis.en.EnglishWDFAnalyzer'

BENCH_BASE_DIR = '%s/util/' % BASE_DIR
WIKI_LINE_FILE = '%s/data/enwiki-20100302-lines-1k.txt' % BENCH_BASE_DIR
WIKI_FILE = '%s/data/enwiki-20100302-pages-articles.xml.bz2' % BENCH_BASE_DIR
INDEX_DIR_BASE = '%s/indices' % BENCH_BASE_DIR
LOG_DIR = '%s/logs' % BENCH_BASE_DIR
JAVA_COMMAND = 'java -Xbatch -Xms2g -Xmx2g -server'

# import again in case you want to override any of the vars set above
from localconstants import *
