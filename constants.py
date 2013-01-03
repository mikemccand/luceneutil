#!/usr/bin/env python


# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# NOTE: you must have a localconstants.py that, minimally, defines
# BASE_DIR; all your checkouts should be under BASE_DIR, ie
# BASE_DIR/aaa BASE_DIR/bbb etc.
from localconstants import *

BENCH_BASE_DIR = '%s/util' % BASE_DIR

# wget http://people.apache.org/~mikemccand/enwiki-20100302-pages-articles-lines-1k-shuffled.txt.bz2
#WIKI_MEDIUM_DOCS_LINE_FILE = '%s/data/enwiki-20100302-pages-articles-lines-1k-shuffled.txt' % BASE_DIR

# wget http://people.apache.org/~mikemccand/enwiki-20120502-lines-1k.txt.lzma
WIKI_MEDIUM_DOCS_LINE_FILE = '%s/data/enwiki-20120502-lines-1k.txt' % BASE_DIR
WIKI_MEDIUM_DOCS_COUNT = 33332621

#WIKI_MEDIUM_TASKS_10MDOCS_FILE = '%s/wikimedium.10M.tasks' % BENCH_BASE_DIR
WIKI_MEDIUM_TASKS_10MDOCS_FILE = '%s/wikimedium.10M.nostopwords.tasks' % BENCH_BASE_DIR
WIKI_MEDIUM_TASKS_1MDOCS_FILE = '%s/wikimedium.1M.tasks' % BENCH_BASE_DIR
WIKI_MEDIUM_TASKS_ALL_FILE = '%s/wikimedium.10M.tasks' % BENCH_BASE_DIR

# wget http://people.apache.org/~mikemccand/enwiki-20100302-pages-articles-lines.txt.bz2
WIKI_BIG_DOCS_LINE_FILE = '%s/data/enwiki-20100302-pages-articles-lines.txt' % BASE_DIR
WIKI_BIG_TASKS_FILE = '%s/data/wikibig.tasks' % BASE_DIR

# enwiki-20120502-lines-1k.txt has 33332620 docs
# enwiki-20120502-lines.txt has 6726515 docs
WIKI_BIG_DOCS_COUNT = 6726515

#WIKI_FILE = '%s/data/enwiki-20100302-pages-articles.xml.bz2' % BENCH_BASE_DIR

# 5607746 docs:
# wget http://people.apache.org/~mikemccand/europarl.para.lines.txt
EUROPARL_MEDIUM_DOCS_LINE_FILE = '%s/data/europarl.para.lines.txt' % BASE_DIR
EUROPARL_MEDIUM_TASKS_FILE = '%s/data/europarlmedium.tasks' % BASE_DIR

INDEX_DIR_BASE = '%s/indices' % BASE_DIR
JAVA_COMMAND = 'java -server -Xms2g -Xmx2g'
JRE_SUPPORTS_SERVER_MODE = True
INDEX_NUM_THREADS = 2
SEARCH_NUM_THREADS = 2
# geonames: http://download.geonames.org/export/dump/

REPRO_COMMAND_START = 'python -u %s/repeatLuceneTest.py -once -verbose -nolog' % BENCH_BASE_DIR
REPRO_COMMAND_END = ''

SORT_REPORT_BY = 'pctchange'
#SORT_REPORT_BY = 'query'

if 'ANALYZER' in locals():
  raise RuntimeException('ANALYZER should now be specified per-index and per-competitor')
#DEFAULTS

POSTINGS_FORMAT_DEFAULT='Lucene41'
ID_FIELD_POSTINGS_FORMAT_DEFAULT='Memory'
ANALYZER_DEFAULT='StandardAnalyzer'
SIMILARITY_DEFAULT='DefaultSimilarity'
MERGEPOLICY_DEFAULT='LogDocMergePolicy'

#TESTS_LINE_FILE = '/lucene/clean2.svn/lucene/test-framework/src/resources/org/apache/lucene/util/europarl.lines.txt'
TESTS_LINE_FILE = None

ANT_EXE = 'ant'

# import again in case you want to override any of the vars set above
from localconstants import *

