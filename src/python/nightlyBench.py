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

# TODO AMD
#   - where to publish
#   - outgoing smtp

import cPickle
import tarfile
import traceback
import time
import datetime
import os
import sys
import shutil
import smtplib
import re
import random
import pysftp

# local imports:
import benchUtil
import constants
import competition
import stats

"""
This script runs certain benchmarks, once per day, and generates graphs so we can see performance over time:

  * Index all of wikipedia ~ 1 KB docs w/ 512 MB ram buffer

  * Run NRT perf test on this index for 30 minutes (we only plot mean/stddev reopen time)
  
  * Index all of wikipedia actual (~4 KB) docs w/ 512 MB ram buffer

  * Index all of wikipedia ~ 1 KB docs, flushing by specific doc count to get 5 segs per level

  * Run search test
"""

KNOWN_CHANGES = [
  ('2011-04-25',
   'Switched to 240 GB OCZ Vertex III',
   """
   Switched from a traditional spinning-magnets hard drive (Western Digital Caviar Green, 1TB) to a 240 GB <a href="http://www.ocztechnology.com/ocz-vertex-3-sata-iii-2-5-ssd.html">OCZ Vertex III SSD</a>; this change gave a small increase in indexing rate, drastically reduced variance on the NRT reopen time (NRT is IO intensive), and didn't affect query performance (which is expected since the postings are small enough to fit into the OS's IO cache.
   """),

  ('2011-05-02',
   'LUCENE-3023: concurrent flushing (DocWriterPerThread)',
   """
   Concurrent flushing, a major improvement to Lucene, was committed.  Before this change, flushing a segment in IndexWriter was single-threaded and blocked all other indexing threads; after this change, each indexing thread flushes its own segment without blocking indexing of other threads.  On highly concurrent hardware (the machine running these tests has 24 cores) this can result in a tremendous increase in Lucene\'s indexing throughput.  See <a href="http://blog.mikemccandless.com/2011/05/265-indexing-speedup-with-lucenes.html">this post</a> for details.

   <p> Some queries did get slower, because the index now has more segments.  Unfortunately, the index produced by concurrent flushing will vary, night to night, in how many segments it contains, so this is a further source of noise in the search results."""),

  ('2011-05-06',
   'Make search index consistent',
   """
   Changed how I build the index used for searching, to only use one thread.  This results in exactly the same index structure (same segments, same docs per segment) from night to night, to avoid the added noise from change B.
   """),

  ('2011-05-07',
   'Change to 20 indexing threads (from 6), 350 MB RAM buffer (from 512)',
   """
   Increased number of indexing threads from 6 to 20 and dropped the IndexWriter RAM buffer from 512 MB to 350 MB.  See <a href="http://blog.mikemccandless.com/2011/05/265-indexing-speedup-with-lucenes.html">this post</a> for details.
   """),

  ('2011-05-11',
   'Add TermQuery with sorting',
   """
   Added TermQuery, sorting by date/time and title fields.
   """),

  ('2011-05-14',
   'Add TermQuery with grouping',
   """
   Added TermQuery, grouping by fields with 100, 10K, 1M unique values.
   """),

  ('2011-06-03',
   'Add single-pass grouping',
   """
   Added Term (bgroup) and Term (bgroup, 1pass) using the BlockGroupingCollector for grouping into 1M unique groups.
   """),

  ('2011-06-26',
   'Use MemoryCodec for id field; switched to NRTCachingDirectory for NRT test',
   '''
   Switched to MemoryCodec for the primary-key 'id' field so that lookups (either for PKLookup test or for deletions during reopen in the NRT test) are fast, with no IO.  Also switched to NRTCachingDirectory for the NRT test, so that small new segments are written only in RAM.
   '''),

  ('2011-07-04',
   'Switched from Java 1.6.0_21 to 1.6.0_26',
   '''
   Switched from Java 1.6.0_21 to 1.6.0_26
   '''),

  ('2011-07-11',
   'LUCENE-3233: arc array optimizations to FST',
   '''
   <a href="https://issues.apache.org/jira/browse/LUCENE-3233">LUCENE-3233</a>: fast SynonymFilter using an FST, including an optimization to the FST representation allowing array arcs even when some arcs have large outputs; this resulted in a good speedup for MemoryCodec, which also speeds up the primary key lookup performance.
   '''),

  ('2011-07-22',
   'LUCENE-3328: specialize code for AND of TermQuery',
   '''
   <a href="https://issues.apache.org/jira/browse/LUCENE-3328">LUCENE-3328</a>: If all clauses of a BooleanQuery are MUST and are TermQuery then create a specialized scorer for scoring this common case.
   '''),

  ('2011-07-30',
   'Switched back to Java 1.6.0_21 from 1.6.0_26',
   '''
   Switched back to Java 1.6.0_21 from 1.6.0_26 because _26 would sometimes deadlock threads.
   '''),

  ('2011-08-20',
   'LUCENE-3030: cutover to more efficient BlockTree terms dict',
   '''
   <a href="https://issues.apache.org/jira/browse/LUCENE-3030">LUCENE-3030</a>: cutover to more efficient BlockTree terms dict.
   '''),

  ('2011-09-22',
   'LUCENE-3215: sloppy PhraseQuery speedups',
   '''
   <a href="https://issues.apache.org/jira/browse/LUCENE-3215">LUCENE-3215</a>: more efficient scoring for sloppy PhraseQuery.
   '''),

  ('2011-11-30',
   'LUCENE-3584: make postings bulk API codec-private',
   '''
   <a href="https://issues.apache.org/jira/browse/LUCENE-3584">LUCENE-3584</a>: make postings bulk API codec-private
   '''),

  ('2011-12-07',
   'Switched to Java 1.7.0_01',
   'Switched to Java 1.7.0_01'),

  ('2011-12-16',
   'LUCENE-3648: JIT optimizations to Lucene40 DocsEnum',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-3648">LUCENE-3648</a>: JIT optimizations to Lucene40 DocsEnum'),

  ('2012-01-30',
   'LUCENE-2858: Split IndexReader in AtomicReader and CompositeReader',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-2858">LUCENE-2858</a>: Split IndexReader in AtomicReader and CompositeReader'),

  ('2012-03-18',
   'LUCENE-3738: Be consistent about negative vInt/vLong',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-3738">LUCENE-3738</a>: Be consistent about negative vInt/vLong'),
   
  ('2012-05-25',
   'LUCENE-4062: new aligned packed-bits implementations',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-4062">LUCENE-4062</a>: new aligned packed-bits implementations'),

  ('2012-05-28',
   'Disable Java\'s compressed OOPS, and LUCENE-4055: refactor SegmentInfos/FieldInfos',
   'Disable Java\'s compressed OOPS (-XX:-UseCompressedOops), and <a href="https://issues.apache.org/jira/browse/LUCENE-4055">LUCENE-4055</a>: refactor SegmentInfos/FieldInfos'),

  ('2012-05-06',
   'LUCENE-4024: FuzzyQuery never does edit distance > 2',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-4024">LUCENE-4024</a>: FuzzyQuery never does edit distance > 2'),

  ('2012-05-15',
   'LUCENE-4024: (rev 1338668) fixed ob1 bug causing FuzzyQ(1) to be TermQuery',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-4024">LUCENE-4024</a>: (rev <a href="http://svn.apache.org/viewvc?view=revision&revision=1338668">1338668</a>) fixed ob1 bug causing FuzzyQ(1) to be TermQuery'),

  ('2012-06-02',
   'Re-enable Java\'s compressed OOPS',
   'Re-enable Java\'s compressed OOPS'),

  ('2012-06-06',
   'Switched to Java 1.7.0_04',
   'Switched to Java 1.7.0_04'),

  ('2012-06-26',
   'Fixed silly performance bug in PKLookupTask.java',
   'Fixed silly performance bug in PKLookupTask.java'),

  ('2012-10-06',
   'Stopped overclocking the computer running benchmarks.',
   'Stopped overclocking the computer running benchmarks.'),

  ('2012-10-15',
   'LUCENE-4446: switch to BlockPostingsFormat',
   'LUCENE-4446: switch to BlockPostingsFormat'),

  ('2012-12-10',
   'LUCENE-4598: small optimizations to facet aggregation',
   'LUCENE-4598: small optimizations to facet aggregation'),

  ('2013-01-11',
   'LUCENE-4620: IntEncoder/Decoder bulk API',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-4620">LUCENE-4620</a>: IntEncoder/Decoder bulk API'),

  ('2013-01-17',
   'Facet performance improvements',
   'Facet performance improvements: LUCENE-4686, LUCENE-4620, LUCENE-4602'),

  ('2013-01-21',
   'Facet performance improvements',
   'Facet performance improvements: LUCENE-4600'),

  ('2013-01-24',
   'Switched to NO_PARENTS faceting',
   'Switched to NO_PARENTS faceting'),

  ('2013-02-07',
   'DocValues improvements (LUCENE-4547) and facets API improvements (LUCENE-4757)',
   'DocValues improvements (<a href="https://issues.apache.org/jira/browse/LUCENE-4547">LUCENE-4547</a>) and facets API improvements (<a href="https://issues.apache.org/jira/browse/LUCENE-4757">LUCENE-4757</a>)'),

  ('2013-02-12',
   'LUCENE-4764: new Facet42DocValuesFormat for faster but more RAM-consuming DocValues',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-4764">LUCENE-4764</a>: new Facet42DocValuesFormat for faster but more RAM-consuming DocValues'),

  ('2013-02-22',
   'LUCENE-4791: optimize ConjunctionTermScorer to use skipping on first term',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-4791">LUCENE-4791</a>: optimize ConjunctionTermScorer to use skipping on first term'),

  ('2013-03-14',
   'LUCENE-4607: add DISI/Spans.cost',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-4607">LUCENE-4607</a>: add DISI/Spans.cost'),

  ('2013-05-03',
   'LUCENE-4946: SorterTemplate',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-4946">LUCENE-4946</a>: SorterTemplate'),
   
  ('2013-06-20',
   'LUCENE-5063: compress int and long FieldCache entries',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-5063">LUCENE-5063</a>: compress int and long FieldCache entries'),

  ('2013-07-31',
   'LUCENE-5140: recover slowdown in span queries and exact phrase query',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-5140">LUCENE-5140</a>: recover slowdown in span queries and exact phrase query'),

  ('2013-09-10',
   'Switched to Java 1.7.0_40',
   'Switched to Java 1.7.0_40'),

  ('2013-11-09',
   'Switched to DirectDocValuesFormat for the Date facets field.',
   'Switched to DirectDocValuesFormat for the Date facets field.'),

  ('2014-02-06',
   'LUCENE-5425: performance improvement for FixedBitSet.iterator',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-5425">LUCENE-5425: performance improvement for FixedBitSet.iterator</a>',),

  ('2014-04-05',
   'LUCENE-5527: LeafCollector (made CachingCollector slower)',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-5527">LUCENE-527: LeafCollector (made CachingCollector slower)</a>',),

  ('2014-04-25',
   'Upgraded to Ubuntu 14.04 LTS (kernel 3.13.0-32-generic #57)',
   'Upgraded to Ubuntu 14.04 LTS (kernel 3.13.0-32-generic #57)'),
  
  ('2014-06-10',
   'Switched from DirectDVFormat to Lucene\'s default for Date facet field',
   'Switched from DirectDVFormat to Lucene\'s default for Date facet field'),

  ('2014-07-25',
   'Disabled transparent huge pages',
   'Disabled transparent huge pages'),   

  ('2014-08-30',
   'Re-enabled transparent huge pages',
   'Re-enabled transparent huge pages'),

  ('2014-03-11',
   'LUCENE-5487: add BulkScorer',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-5487">LUCENE-5487: add BulkScorer</a>'),

  ('2014-11-01',
   'LUCENE-6030: norms compression',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-6030">LUCENE-6030: norms compression</a>'),

  ('2014-11-22',
   'Upgrade from java 1.7.0_55-b13 to java 1.8.0_20-ea-b05',
   'Upgrade from java 1.7.0_55-b13 to java 1.8.0_20-ea-b05'),

  ('2015-01-15',
   'LUCENE-6179: remove out-of-order scoring',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-6179">LUCENE-6179: remove out-of-order scoring</a>'),

  ('2015-01-19',
   'LUCENE-6184: BooleanScorer better deals with sparse clauses',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-6184">LUCENE-6184: BooleanScorer better deals with sparse clauses</a>'),

  ('2015-02-13',
   'LUCENE-6198: Two phase intersection',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-6198">LUCENE-6198: Two phase intersection</a> (approximations are not needed by any query in this benchmark, but the change refactored ConjunctionScorer a bit)'),

  ('2015-02-23',
   'LUCENE-6275: SloppyPhraseScorer reuses ConjunctionDISI',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-6275">LUCENE-6275: SloppyPhraseScorer reuses ConjunctionDISI</a>'),

  ('2015-03-02',
   'LUCENE-6320: Speed up CheckIndex',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-6320">LUCENE-6320: Speed up CheckIndex</a>'),

  ('2015-03-06',
   'Upgrade JDK from 1.8.0_25-b17 to 1.8.0_40-b25',
   'Upgrade JDK from 1.8.0_25-b17 to 1.8.0_40-b25'),

  ('2015-04-02',
   'LUCENE-6308: span queries support two-phased iteration',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-6308">LUCENE-6308: span queries support two-phased iteration</a>'),

  ('2015-04-04',
   'LUCENE-5879: add auto-prefix terms',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-5879">LUCENE-5879: add auto-prefix terms</a>'),

  ('2015-06-24',
   'LUCENE-6548: some optimizations to block tree intersect',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-6548">LUCENE-6548: some optimizations to block tree intersect</a>'),

  ('2015-09-15',
   'LUCENE-6789: switch to BM25 scoring by default',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-6789">LUCENE-6789 switch to BM25 scoring by default</a>'),

  ('2015-10-05',
   'Randomize what time of day benchmark runs',
   'Randomize what time of day benchmark runs'),

  ('2015-12-02',
   'Upgrade to beast2 (72 cores, 256 GB RAM)',
   'Upgrade to beast2 (72 cores, 256 GB RAM)'),

  ('2015-12-10',
   'LUCENE-6919: Change the Scorer API to expose an iterator instead of extending DocIdSetIterator',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-6919">LUCENE-6919: Change the Scorer API to expose an iterator instead of extending DocIdSetIterator</a>'),

  ('2015-12-14',
   'LUCENE-6917: Change from LegacyNumericRangeQuery to DimensionalRangeQuery',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-6917">LUCENE-6917: Change from LegacyNumericRangeQuery to DimensionalRangeQuery</a>'),

  ('2016-05-23',
   'Fix silly benchmark bottlenecks; increase indexing heap to 8 GB; increase indexing buffer to 2 GB; increase indexing threads to 20; use default postings format for all fields; do not wait for commit in the end (just rollback); turn off merge IO throttling',
   '<a href="https://github.com/mikemccand/luceneutil/commit/b24e28dd1bf9a9fcacd693c4162d5ebb03d4afe1">Fix silly benchmark bottlenecks and re-tune for high indexing throughput</a>'),

  ('2016-05-25',
   'Fix another benchmark bottleneck for 1 KB docs (but this added a bug in TermDateFacets, fixed on 10/18)',
   'Fix another benchmark bottleneck for 1 KB docs (but this added a bug in TermDateFacets, fixed on 10/18)'),

  ('2016-06-13',
   'LUCENE-7330: Speed up conjunctions',
   '<a href="https://issues.apache.org/jira/browse/LUCENE-7330">LUCENE-7330: Speed up conjunctions</a>'),

  ('2016-07-11',
   'Upgrade beast2 OS from Ubuntu 15.04 to 16.04',
   'Upgrade beast2 OS from Ubuntu 15.04 to 16.04'),

  ('2016-07-16',
   'Upgrade beast2 kernel from 4.4.x to 4.6.x',
   'Upgrade beast2 kernel from 4.4.x to 4.6.x'),

  ('2016-08-17',
   'Upgrade beast2 kernel from 4.6.x to 4.7.0',
   'Upgrade beast2 kernel from 4.6.x to 4.7.0'),

  ('2016-09-04',
   'Upgrade beast2 kernel from 4.7.0 to 4.7.2',
   'Upgrade beast2 kernel from 4.7.0 to 4.7.2'),

  ('2016-09-21',
   'LUCENE-7407: Change doc values from random access to iterator API',
   'LUCENE-7407: Change doc values from random access to iterator API',
   ),

  ('2016-10-18',
   'Fix silly TermDateFacets bug causing single date facet to be indexed for all docs, added on 5/25',
   'Fix silly TermDateFacets bug causing single date facet to be indexed for all docs, added on 5/25',
   ),

  ('2016-10-24',
   'LUCENE-7462: give doc values an advanceExact API',
   'LUCENE-7462: give doc values an advanceExact API',
   ),

  ('2016-10-26',
   'LUCENE-7519: optimize computing browse-only facets, LUCENE-7489: Remove one layer of abstraction in binary doc values and single-valued numerics',
   'LUCENE-7519: optimize computing browse-only facets, LUCENE-7489: Remove one layer of abstraction in binary doc values and single-valued numerics',
   ),

  ('2016-10-27',
   'Re-enable transparent huge pages in Linux',
   'Re-enable transparent huge pages in Linux',
   ),

  ('2016-10-31',
   'LUCENE-7135: This issue accidentally caused FSDirectory.open to use NIOFSDirectory instead of MMapDirectory for e.g. CheckIndex',
   'LUCENE-7135: This issue accidentally caused FSDirectory.open to use NIOFSDirectory instead of MMapDirectory for e.g. CheckIndex'),

  ('2016-11-02',
   'LUCENE-7135: Fixed this issue so we use MMapDirectory again',
   'LUCENE-7135: Fixed this issue so we use MMapDirectory again'),

  ('2017-01-18',
   'LUCENE-7641: Speed up point ranges that match most documents',
   'LUCENE-7641: Speed up point ranges that match most documents'),

  ('2017-10-27',
   'LUCENE-7997: BM25 to use doubles instead of floats',
   'LUCENE-7997: BM25 to use doubles instead of floats'),

  ('2018-01-31',
   'LUCENE-4198: Allow codecs to index term impacts',
   'LUCENE-4198: Allow codecs to index term impacts'),

  ('2018-02-20',
   'LUCENE-8153: CheckIndex spends less time checking impacts',
   'LUCENE-8153: CheckIndex spends less time checking impacts'),

  ('2018-05-02',
   'LUCENE-8279: CheckIndex now cross-checks terms with norms',
   'LUCENE-8279: CheckIndex now cross-checks terms with norms'),

  ('2018-05-25',
   'LUCENE-8312: Leverage impacts for SynonymQuery (introduced regression for non-scoring term queries)',
   'LUCENE-8312: Leverage impacts for SynonymQuery (introduced regression for non-scoring term queries)'),

  ('2018-08-01',
   'LUCENE-8312: Fixed regression with non-scoring term queries',
   'LUCENE-8312: Fixed regression with non-scoring term queries'),

  ('2018-08-01',
   'LUCENE-8060: Stop counting total hits by default',
   'LUCENE-8060: Stop counting total hits by default'),

]

# TODO
#   - need a tiny docs test?  catch per-doc overhead regressions...
#   - click on graph should go to details page
#   - nrt
#     - chart all reopen times by time...?
#     - chart over-time mean/stddev reopen time
#   - maybe multiple queries on one graph...?

DEBUG = '-debug' in sys.argv

if DEBUG:
  NIGHTLY_DIR = 'trunk'
else:
  NIGHTLY_DIR = 'trunk.nightly'

DIR_IMPL = 'MMapDirectory'

INDEXING_RAM_BUFFER_MB = 2048

COUNTS_PER_CAT = 5
TASK_REPEAT_COUNT = 50

#MED_WIKI_BYTES_PER_DOC = 950.21921304868431
#BIG_WIKI_BYTES_PER_DOC = 4183.3843150398807

NRT_DOCS_PER_SECOND = 1103  # = 1 MB / avg med wiki doc size
NRT_RUN_TIME = 30*60
NRT_SEARCH_THREADS = 4
NRT_INDEX_THREADS = 1
NRT_REOPENS_PER_SEC = 1
JVM_COUNT = 20

if DEBUG:
  NRT_RUN_TIME /= 90
  JVM_COUNT = 3

reBytesIndexed = re.compile('^Indexer: net bytes indexed (.*)$', re.MULTILINE)
reIndexingTime = re.compile(r'^Indexer: finished \((.*) msec\)', re.MULTILINE)
reSVNRev = re.compile(r'revision (.*?)\.')
reIndexAtClose = re.compile('Indexer: at close: (.*?)$', re.M)

REAL = True

def now():
  return datetime.datetime.now()

def toSeconds(td):
  return td.days * 86400 + td.seconds + td.microseconds/1000000.

def message(s):
  print '[%s] %s' % (now(), s)

def runCommand(command):
  if REAL:
    message('RUN: %s' % command)
    t0 = time.time()
    if os.system(command):
      message('  FAILED')
      raise RuntimeError('command failed: %s' % command)
    message('  took %.1f sec' % (time.time()-t0))
  else:
    message('WOULD RUN: %s' % command)
    
def buildIndex(r, runLogDir, desc, index, logFile):
  message('build %s' % desc)
  #t0 = now()
  indexPath = benchUtil.nameToIndexPath(index.getName())
  if os.path.exists(indexPath):
    shutil.rmtree(indexPath)
  if REAL:
    indexPath, fullLogFile = r.makeIndex('nightly', index)
  #indexTime = (now()-t0)

  if REAL:
    print('Move log to %s/%s' % (runLogDir, logFile))
    os.rename(fullLogFile, '%s/%s' % (runLogDir, logFile))

  s = open('%s/%s' % (runLogDir, logFile)).read()
  bytesIndexed = int(reBytesIndexed.search(s).group(1))
  m = reIndexAtClose.search(s)
  if m is not None:
    indexAtClose = m.group(1)
  else:
    # we have no index when we don't -waitForCommit
    indexAtClose = None
  indexTimeSec = int(reIndexingTime.search(s).group(1))/1000.0

  message('  took %.1f sec' % indexTimeSec)

  if '(fast)' in desc:
    # don't run checkIndex: we rollback in the end
    pass
  else:
    # checkIndex
    checkLogFileName = '%s/checkIndex.%s' % (runLogDir, logFile)
    checkIndex(r, indexPath, checkLogFileName)

  return indexPath, indexTimeSec, bytesIndexed, indexAtClose

def checkIndex(r, indexPath, checkLogFileName):
  message('run CheckIndex')
  cmd = '%s -classpath "%s" -ea org.apache.lucene.index.CheckIndex "%s" > %s 2>&1' % \
        (constants.JAVA_COMMAND,
         r.classPathToString(r.getClassPath(NIGHTLY_DIR)),
         indexPath + '/index',
         checkLogFileName)
  runCommand(cmd)
  if open(checkLogFileName, 'rb').read().find('No problems were detected with this index') == -1:
    raise RuntimeError('CheckIndex failed')

reNRTReopenTime = re.compile('^Reopen: +([0-9.]+) msec$', re.MULTILINE)

def runNRTTest(r, indexPath, runLogDir):

  open('body10.tasks', 'w').write('Term: body:10\n')

  cmd = '%s -classpath "%s" perf.NRTPerfTest %s "%s" multi "%s" 17 %s %s %s %s %s update 5 no 0.0 body10.tasks' % \
        (constants.JAVA_COMMAND,
         r.classPathToString(r.getClassPath(NIGHTLY_DIR)),
         DIR_IMPL,
         indexPath + '/index',
         constants.NIGHTLY_MEDIUM_LINE_FILE,
         NRT_DOCS_PER_SECOND,
         NRT_RUN_TIME,
         NRT_SEARCH_THREADS,
         NRT_INDEX_THREADS,
         NRT_REOPENS_PER_SEC)

  logFile = '%s/nrt.log' % runLogDir
  cmd += '> %s 2>&1' % logFile
  runCommand(cmd)

  times = []
  for s in reNRTReopenTime.findall(open(logFile, 'rb').read()):
    times.append(float(s))

  # Discard first 10 (JVM warmup)
  times = times[10:]

  # Discard worst 2%
  times.sort()
  numDrop = len(times)/50
  if numDrop > 0:
    message('drop: %s' % ' '.join(['%.1f' % x for x in times[-numDrop:]]))
    times = times[:-numDrop]
  message('times: %s' % ' '.join(['%.1f' % x for x in times]))

  min, max, mean, stdDev = stats.getStats(times)
  message('NRT reopen time (msec) mean=%.4f stdDev=%.4f' % (mean, stdDev))
  
  checkIndex(r, indexPath, '%s/checkIndex.nrt.log' % runLogDir)
  
  return mean, stdDev

def run():

  MEDIUM_INDEX_NUM_DOCS = constants.NIGHTLY_MEDIUM_INDEX_NUM_DOCS
  BIG_INDEX_NUM_DOCS = constants.NIGHTLY_BIG_INDEX_NUM_DOCS

  if DEBUG:
    # Must re-direct all logs so we don't overwrite the "production" run's logs:
    constants.LOGS_DIR = '/l/trunk/lucene/benchmark'
    MEDIUM_INDEX_NUM_DOCS /= 100
    BIG_INDEX_NUM_DOCS /= 100

  DO_RESET = '-reset' in sys.argv

  print
  print
  print
  print
  message('start')
  id = 'nightly'
  if not REAL:
    start = datetime.datetime(year=2011, month=5, day=19, hour=23, minute=00, second=01)
  else:
    start = now()
  timeStamp = '%04d.%02d.%02d.%02d.%02d.%02d' % (start.year, start.month, start.day, start.hour, start.minute, start.second)
  runLogDir = '%s/%s' % (constants.NIGHTLY_LOG_DIR, timeStamp)
  if REAL:
    os.makedirs(runLogDir)
  message('log dir %s' % runLogDir)

  if not REAL:
    os.chdir('%s/%s' % (constants.BASE_DIR, NIGHTLY_DIR))
    svnRev = '1102160'
    luceneUtilRev = '2270c7a8b3ac+ tip'
    print 'SVN rev is %s' % svnRev
    print 'luceneutil rev is %s' % luceneUtilRev
  else:
    os.chdir(constants.BENCH_BASE_DIR)

    iters = 30
    for i in range(iters):
      try:
        runCommand('git checkout master; git pull origin master > %s/gitupdate.log' % runLogDir)
      except RuntimeError:
        message('  retry...')
        time.sleep(60.0)
      else:
        s = open('%s/gitupdate.log' % runLogDir).read()
        if s.find('not updating') != -1:
          raise RuntimeError('git pull failed: %s' % s)
        break
    else:
      raise RuntimeError('failed to run git pull after %d tries' % iters)

    os.chdir(constants.BENCH_BASE_DIR)
    luceneUtilRev = os.popen('git rev-parse HEAD').read().strip()

    os.chdir('%s/%s' % (constants.BASE_DIR, NIGHTLY_DIR))
    #runCommand('%s cleanup' % constants.SVN_EXE)
    runCommand('%s clean -xfd' % constants.GIT_EXE)
    for i in range(iters):
      try:
        #runCommand('%s update > %s/update.log' % (constants.SVN_EXE, runLogDir))
        runCommand('%s checkout master; %s pull origin master > %s/update.log' % (constants.GIT_EXE, constants.GIT_EXE, runLogDir))
      except RuntimeError:
        message('  retry...')
        time.sleep(60.0)
      else:
        luceneRev = os.popen('git rev-parse HEAD').read().strip()
        #svnRev = int(reSVNRev.search(open('%s/update.log' % runLogDir, 'rb').read()).group(1))
        print 'LUCENE rev is %s' % luceneRev
        break
    else:
      raise RuntimeError('failed to run git pull after %d tries' % iters)

    print 'luceneutil rev is %s' % luceneUtilRev
    javaVersion = os.popen('%s -fullversion 2>&1' % constants.JAVA_COMMAND).read().strip()
    print '%s' % javaVersion
    print 'uname -a: %s' % os.popen('uname -a 2>&1').read().strip()
    print 'lsb_release -a:\n%s' % os.popen('lsb_release -a 2>&1').read().strip()

  print 'Java command-line: %s' % constants.JAVA_COMMAND
  try:
    s = open('/sys/kernel/mm/transparent_hugepage/enabled').read()
  except:
    print('Unable to read /sys/kernel/mm/transparent_hugepage/enabled')
  else:
    print('transparent_hugepages: %s' % s)
  
  runCommand('%s clean > clean.log 2>&1' % constants.ANT_EXE)

  r = benchUtil.RunAlgs(constants.JAVA_COMMAND, True, True)

  comp = competition.Competition(taskRepeatCount=TASK_REPEAT_COUNT,
                                 taskCountPerCat=COUNTS_PER_CAT)

  mediumSource = competition.Data('wikimedium',
                                  constants.NIGHTLY_MEDIUM_LINE_FILE,
                                  MEDIUM_INDEX_NUM_DOCS,
                                  constants.WIKI_MEDIUM_TASKS_FILE)

  fastIndexMedium = comp.newIndex(NIGHTLY_DIR, mediumSource,
                                  analyzer='StandardAnalyzerNoStopWords',
                                  postingsFormat='Lucene50',
                                  numThreads=constants.INDEX_NUM_THREADS,
                                  directory=DIR_IMPL,
                                  idFieldPostingsFormat='Lucene50',
                                  ramBufferMB=INDEXING_RAM_BUFFER_MB,
                                  waitForMerges=False,
                                  waitForCommit=False,
                                  disableIOThrottle=True,
                                  grouping=False,
                                  verbose=False,
                                  mergePolicy='TieredMergePolicy',
                                  maxConcurrentMerges=3,
                                  useCMS=True)

  nrtIndexMedium = comp.newIndex(NIGHTLY_DIR, mediumSource,
                                  analyzer='StandardAnalyzerNoStopWords',
                                  postingsFormat='Lucene50',
                                  numThreads=constants.INDEX_NUM_THREADS,
                                  directory=DIR_IMPL,
                                  idFieldPostingsFormat='Lucene50',
                                  ramBufferMB=INDEXING_RAM_BUFFER_MB,
                                  waitForMerges=True,
                                  waitForCommit=True,
                                  disableIOThrottle=True,
                                  grouping=False,
                                  verbose=False,
                                  mergePolicy='TieredMergePolicy',
                                  maxConcurrentMerges=3,
                                  useCMS=True)

  bigSource = competition.Data('wikibig',
                               constants.NIGHTLY_BIG_LINE_FILE,
                               BIG_INDEX_NUM_DOCS,
                               constants.WIKI_MEDIUM_TASKS_FILE)

  fastIndexBig = comp.newIndex(NIGHTLY_DIR, bigSource,
                               analyzer='StandardAnalyzerNoStopWords',
                               postingsFormat='Lucene50',
                               numThreads=constants.INDEX_NUM_THREADS,
                               directory=DIR_IMPL,
                               idFieldPostingsFormat='Lucene50',
                               ramBufferMB=INDEXING_RAM_BUFFER_MB,
                               waitForMerges=False,
                               waitForCommit=False,
                               disableIOThrottle=True,
                               grouping=False,
                               verbose=False,
                               mergePolicy='TieredMergePolicy',
                               maxConcurrentMerges=3,
                               useCMS=True)

  # Must use only 1 thread so we get same index structure, always:
  index = comp.newIndex(NIGHTLY_DIR, mediumSource,
                        analyzer='StandardAnalyzerNoStopWords',
                        postingsFormat='Lucene50',
                        numThreads=1,
                        directory=DIR_IMPL,
                        idFieldPostingsFormat='Lucene50',
                        mergePolicy='LogDocMergePolicy',
                        facets = (('taxonomy:Date', 'Date'),
                                  ('taxonomy:Month', 'Month'),
                                  ('taxonomy:DayOfYear', 'DayOfYear'),
                                  ('sortedset:Month', 'Month'),
                                  ('sortedset:DayOfYear', 'DayOfYear')),
                        maxConcurrentMerges=3,
                        addDVFields=True)

  c = comp.competitor(id, NIGHTLY_DIR,
                      index=index,
                      directory=DIR_IMPL,
                      commitPoint='multi')
  
  #c = benchUtil.Competitor(id, 'trunk.nightly', index, DIR_IMPL, 'StandardAnalyzerNoStopWords', 'multi', constants.WIKI_MEDIUM_TASKS_FILE)

  if REAL:
    r.compile(c)

  # 1: test indexing speed: small (~ 1KB) sized docs, flush-by-ram
  medIndexPath, medIndexTime, medBytesIndexed, atClose = buildIndex(r, runLogDir, 'medium index (fast)', fastIndexMedium, 'fastIndexMediumDocs.log')
  message('medIndexAtClose %s' % atClose)

  # 2: NRT test
  nrtIndexPath, nrtIndexTime, nrtBytesIndexed, atClose = buildIndex(r, runLogDir, 'nrt medium index', nrtIndexMedium, 'nrtIndexMediumDocs.log')
  message('nrtMedIndexAtClose %s' % atClose)
  nrtResults = runNRTTest(r, medIndexPath, runLogDir)

  # 3: test indexing speed: medium (~ 4KB) sized docs, flush-by-ram
  ign, bigIndexTime, bigBytesIndexed, atClose = buildIndex(r, runLogDir, 'big index (fast)', fastIndexBig, 'fastIndexBigDocs.log')
  message('bigIndexAtClose %s' % atClose)

  # 4: test searching speed; first build index, flushed by doc count (so we get same index structure night to night)
  indexPathNow, ign, ign, atClose = buildIndex(r, runLogDir, 'search index (fixed segments)', index, 'fixedIndex.log')
  message('fixedIndexAtClose %s' % atClose)
  fixedIndexAtClose = atClose

  indexPathPrev = '%s/trunk.nightly.index.prev' % constants.INDEX_DIR_BASE

  if os.path.exists(indexPathPrev) and os.path.exists(benchUtil.nameToIndexPath(index.getName())):
    segCountPrev = benchUtil.getSegmentCount(indexPathPrev)
    segCountNow = benchUtil.getSegmentCount(benchUtil.nameToIndexPath(index.getName()))
    if segCountNow != segCountPrev:
      # raise RuntimeError('different index segment count prev=%s now=%s' % (segCountPrev, segCountNow))
      print 'WARNING: different index segment count prev=%s now=%s' % (segCountPrev, segCountNow)

  # Search
  rand = random.Random(714)
  staticSeed = rand.randint(-10000000, 1000000)
  #staticSeed = -1492352

  message('search')
  t0 = now()

  coldRun = False
  comp = c
  comp.tasksFile = '%s/tasks/wikinightly.tasks' % constants.BENCH_BASE_DIR
  comp.printHeap = True
  if REAL:
    resultsNow = []
    for iter in xrange(JVM_COUNT):
      seed = rand.randint(-10000000, 1000000)      
      resultsNow.append(r.runSimpleSearchBench(iter, id, comp, coldRun, seed, staticSeed, filter=None))
  else:
    resultsNow = ['%s/%s/modules/benchmark/%s.%s.x.%d' % (constants.BASE_DIR, NIGHTLY_DIR, id, comp.name, iter) for iter in xrange(20)]
  message('done search (%s)' % (now()-t0))
  resultsPrev = []

  searchResults = searchHeap = None
  
  for fname in resultsNow:
    prevFName = fname + '.prev'
    if os.path.exists(prevFName):
      resultsPrev.append(prevFName)

  if not DO_RESET:
    output = []
    results, cmpDiffs, searchHeaps = r.simpleReport(resultsPrev,
                                                    resultsNow,
                                                    False, True,
                                                    'prev', 'now',
                                                    writer=output.append)
    f = open('%s/%s.html' % (constants.NIGHTLY_REPORTS_DIR, timeStamp), 'wb')
    timeStamp2 = '%s %02d/%02d/%04d' % (start.strftime('%a'), start.month, start.day, start.year)
    w = f.write
    w('<html>\n')
    w('<h1>%s</h1>' % timeStamp2)
    w('Lucene/Solr trunk rev %s<br>' % luceneRev)
    w('luceneutil rev %s<br>' % luceneUtilRev)
    w('%s<br>' % javaVersion)
    w('Java command-line: %s<br>' % htmlEscape(constants.JAVA_COMMAND))
    w('Index: %s<br>' % fixedIndexAtClose)
    w('<br><br><b>Search perf vs day before</b>\n')
    w(''.join(output))
    w('<br><br>')
    w('<img src="%s.png"/>\n' % timeStamp)
    w('</html>\n')
    f.close()

    if os.path.exists('out.png'):
      shutil.move('out.png', '%s/%s.png' % (constants.NIGHTLY_REPORTS_DIR, timeStamp))
    searchResults = results

    print '  heaps: %s' % str(searchHeaps)

    if cmpDiffs is not None:
      warnings, errors = cmpDiffs
      print 'WARNING: search result differences: %s' % str(warnings)
      if len(errors) > 0:
        raise RuntimeError('search result differences: %s' % str(errors))
  else:
    cmpDiffs = None
    searchHeaps = None

  results = (start,
             MEDIUM_INDEX_NUM_DOCS, medIndexTime, medBytesIndexed,
             BIG_INDEX_NUM_DOCS, bigIndexTime, bigBytesIndexed,
             nrtResults,
             searchResults,
             luceneRev,
             luceneUtilRev,
             searchHeaps)
  for fname in resultsNow:
    shutil.copy(fname, runLogDir)
    if os.path.exists(fname + '.stdout'):
      shutil.copy(fname + '.stdout', runLogDir)

  if REAL:
    for fname in resultsNow:
      shutil.move(fname, fname + '.prev')

    if not DEBUG:
      # print 'rename %s to %s' % (indexPathNow, indexPathPrev)
      if os.path.exists(indexPathNow):
        if os.path.exists(indexPathPrev):
          shutil.rmtree(indexPathPrev)
        os.rename(indexPathNow, indexPathPrev)

    os.chdir(runLogDir)
    runCommand('tar cjf logs.tar.bz2 *')
    for f in os.listdir(runLogDir):
      if f != 'logs.tar.bz2':
        os.remove(f)

  if DEBUG:
    resultsFileName = 'results.debug.pk'
  else:
    resultsFileName = 'results.pk'

  open('%s/%s' % (runLogDir, resultsFileName), 'wb').write(cPickle.dumps(results))

  if REAL:
    if False:
      runCommand('chmod -R a-w %s' % runLogDir)

  message('done: total time %s' % (now()-start))

def getGCTimes(subDir):
  if not os.path.exists('%s/gcTimes.pk' % subDir):
    times = {}
    print("check %s" % ('%s/logs.tar.bz2' % subDir))
    if os.path.exists('%s/logs.tar.bz2' % subDir):
      reTimeIn = re.compile('^\s*Time in (.*?): (\d+) ms')
      cmd = 'tar xjf %s/logs.tar.bz2 fastIndexMediumDocs.log' % subDir
      if os.system(cmd):
        raise RuntimeError('%s failed (cwd %s)' % (cmd, os.getcwd()))

      with open('fastIndexMediumDocs.log') as f:
        for line in f.readlines():
          m = reTimeIn.search(line)
          if m is not None:
            times[m.group(1)] = float(m.group(2))/1000.

      open('%s/gcTimes.pk' % subDir, 'wb').write(cPickle.dumps(times))
    return times
  else:
    return cPickle.loads(open('%s/gcTimes.pk' % subDir, 'rb').read())

def makeGraphs():
  global annotations
  medIndexChartData = ['Date,GB/hour']
  bigIndexChartData = ['Date,GB/hour']
  nrtChartData = ['Date,Reopen Time (msec)']
  gcTimesChartData = ['Date,JIT (sec), Young GC (sec), Old GC (sec)']
  searchChartData = {}
  days = []
  annotations = []
  l = os.listdir(constants.NIGHTLY_LOG_DIR)
  l.sort()

  for subDir in l:
    resultsFile = '%s/%s/results.pk' % (constants.NIGHTLY_LOG_DIR, subDir)
    if DEBUG and not os.path.exists(resultsFile):
      resultsFile = '%s/%s/results.debug.pk' % (constants.NIGHTLY_LOG_DIR, subDir)

    if os.path.exists(resultsFile):

      tup = cPickle.loads(open(resultsFile).read())
      # print 'RESULTS: %s' % resultsFile
      
      timeStamp, \
                 medNumDocs, medIndexTimeSec, medBytesIndexed, \
                 bigNumDocs, bigIndexTimeSec, bigBytesIndexed, \
                 nrtResults, searchResults = tup[:9]
      if len(tup) > 9:
        rev = tup[9]
      else:
        rev = None

      if len(tup) > 10:
        utilRev = tup[10]
      else:
        utilRev = None

      if len(tup) > 11:
        searchHeaps = tup[11]
      else:
        searchHeaps = None
        
      timeStampString = '%04d-%02d-%02d %02d:%02d:%02d' % \
                        (timeStamp.year,
                         timeStamp.month,
                         timeStamp.day,
                         timeStamp.hour,
                         timeStamp.minute,
                         int(timeStamp.second))
      date = '%02d/%02d/%04d' % (timeStamp.month, timeStamp.day, timeStamp.year)
      if date in ('09/03/2014',):
        # I was testing disabling THP again...
        continue
      if date in ('05/16/2014'):
        # Bug in luceneutil made it look like 0 qps on all queries
        continue

      gcTimes = getGCTimes('%s/%s' % (constants.NIGHTLY_LOG_DIR, subDir))
      s = timeStampString
      for h in 'JIT compilation', 'Young Generation GC', 'Old Generation GC':
        v = gcTimes.get(h)
        s += ','
        if v is not None:
          s += '%.4f' % v
      gcTimesChartData.append(s)

      medIndexChartData.append('%s,%.1f' % (timeStampString, (medBytesIndexed / (1024*1024*1024.))/(medIndexTimeSec/3600.)))
      bigIndexChartData.append('%s,%.1f' % (timeStampString, (bigBytesIndexed / (1024*1024*1024.))/(bigIndexTimeSec/3600.)))
      mean, stdDev = nrtResults
      nrtChartData.append('%s,%.3f,%.2f' % (timeStampString, mean, stdDev))
      if searchResults is not None:
        days.append(timeStamp)
        for cat, (minQPS, maxQPS, avgQPS, stdDevQPS) in searchResults.items():
          if cat not in searchChartData:
            searchChartData[cat] = ['Date,QPS']
          if cat == 'PKLookup':
            qpsMult = 4000
          else:
            qpsMult = 1

          if cat == 'TermDateFacets':
            if date in ('01/03/2013', '01/04/2013', '01/05/2013', '01/05/2014'):
              # Bug in luceneutil made facets not actually run correctly so QPS was way too high:
              continue
          if cat == 'Fuzzy1':
            if date in ('05/06/2012',
                        '05/07/2012',
                        '05/08/2012',
                        '05/09/2012',
                        '05/10/2012',
                        '05/11/2012',
                        '05/12/2012',
                        '05/13/2012',
                        '05/14/2012'):
              # Bug in FuzzyQuery made Fuzzy1 be exact search
              continue
          if cat == 'TermDateFacets':
            if date in ('02/02/2013',
                        '02/03/2013'):
              # Bug in luceneutil (didn't actually run faceting on these days)
              continue
          if cat == 'IntNRQ':
            if date in ('06/09/2014',
                        '06/10/2014'):
              # Bug in luceneutil (didn't index numeric field properly)
              continue
          searchChartData[cat].append('%s,%.3f,%.3f' % (timeStampString, avgQPS*qpsMult, stdDevQPS*qpsMult))

      label = 0
      for date, desc, fullDesc in KNOWN_CHANGES:
        if timeStampString.startswith(date):
          #print('timestamp %s: add annot %s' % (timeStampString, desc))
          annotations.append((date, timeStampString, desc, fullDesc, label))
          #KNOWN_CHANGES.remove((date, desc, fullDesc))
        label += 1

  sort(medIndexChartData)
  sort(bigIndexChartData)
  for k, v in searchChartData.items():
    sort(v)

  # Index time, including GC/JIT times
  writeIndexingHTML(medIndexChartData, bigIndexChartData, gcTimesChartData)

  # CheckIndex time
  writeCheckIndexTimeHTML()

  # NRT
  writeNRTHTML(nrtChartData)

  for k, v in searchChartData.items()[:]:
    # Graph does not render right with only one value:
    if len(v) > 1:
      writeOneGraphHTML('Lucene %s queries/sec' % taskRename.get(k, k),
                        '%s/%s.html' % (constants.NIGHTLY_REPORTS_DIR, k),
                        getOneGraphHTML(k, v, "Queries/sec", taskRename.get(k, k), errorBars=True))
    else:
      print('skip %s: %s' % (k, len(v)))
      del searchChartData[k]

  writeIndexHTML(searchChartData, days)

  # publish
  #runCommand('rsync -rv -e ssh %s/reports.nightly mike@10.17.4.9:/usr/local/apache2/htdocs' % constants.BASE_DIR)

  if not DEBUG:
    #runCommand('rsync -r -e ssh %s/reports.nightly/ %s' % (constants.BASE_DIR, constants.NIGHTLY_PUBLISH_LOCATION))
    pushReports()

def pushReports():
  print('Copy reports...')
  with pysftp.Connection('home.apache.org', username='mikemccand') as c:
    with c.cd('public_html'):
      #c.mkdir('lucenebench')
      # TODO: this is not incremental...
      c.put_r('%s/reports.nightly' % constants.BASE_DIR, 'lucenebench')

reTookSec = re.compile('took ([0-9.]+) sec')
reDateTime = re.compile('log dir /lucene/logs.nightly/(.*?)$')

def writeCheckIndexTimeHTML():
  # Messy: parses the .tar.bz2 to find timestamps of each file  Once
  # LUCENE-6233 is in we can more cleanly get this from CheckIndex's output
  # instead:
  chartData = []

  l = os.listdir(constants.NIGHTLY_LOG_DIR)
  l.sort()

  for subDir in l:
    checkIndexTimeFile = '%s/%s/checkIndex.time' % (constants.NIGHTLY_LOG_DIR, subDir)
    if os.path.exists('%s/%s/results.debug.pk' % (constants.NIGHTLY_LOG_DIR, subDir)):
      # Skip debug runs
      continue
    
    tup = subDir.split('.')
    if len(tup) != 6:
      #print('skip %s' % subDir)
      continue

    if tup[:3] == ['2015', '04', '04']:
      # Hide disastrously slow CheckIndex time after auto-prefix first landed
      continue
    
    if os.path.exists(checkIndexTimeFile):
      # Already previously computed & cached:
      seconds = int(open(checkIndexTimeFile, 'r').read())
    else:
      # Look at timestamps of each file in the tar file:
      logsFile = '%s/%s/logs.tar.bz2' % (constants.NIGHTLY_LOG_DIR, subDir)
      if os.path.exists(logsFile):
        t = tarfile.open(logsFile, 'r:bz2')
        l = []
        while True:
          ti = t.next()
          if ti is None:
            break
          l.append((ti.mtime, ti.name))

        l.sort()
        for i in range(len(l)):
          if l[i][1] == 'checkIndex.fixedIndex.log':
            seconds = l[i][0] - l[i-1][0]
            break
        else:
          continue

        open(checkIndexTimeFile, 'w').write('%d' % seconds)
      else:
        continue

    #print("tup %s" % tup)
    chartData.append('%s-%s-%s %s:%s:%s,%s' % (tuple(tup) + (seconds,)))
    #print("added %s" % chartData[-1])
                
  with open('%s/checkIndexTime.html' % constants.NIGHTLY_REPORTS_DIR, 'wb') as f:
    w = f.write
    header(w, 'Lucene nightly CheckIndex time')
    w('<h1>Seconds to run CheckIndex</h1>\n')
    w('<br>Click and drag to zoom; shift + click and drag to scroll after zooming; hover over an annotation to see details<br>')
    w('<br>')
    w(getOneGraphHTML('CheckIndexTimeSeconds', chartData, "Seconds", "CheckIndex time (seconds)", errorBars=False))

    writeKnownChanges(w)

    w('<br><br>')
    w('<b>Notes</b>:\n')
    w('<ul>\n')
    w('  <li> Java command-line: <tt>%s</tt>\n' % constants.JAVA_COMMAND)
    w('  <li> Java version: <tt>%s</tt>\n' % htmlEscape(os.popen('java -version 2>&1').read().strip()))
    w('  <li> OS: <tt>%s</tt>\n' % htmlEscape(os.popen('uname -a 2>&1').read().strip()))
    w('  <li> CPU: 2 Xeon X5680, overclocked @ 4.0 Ghz (total 24 cores = 2 CPU * 6 core * 2 hyperthreads)\n')
    w('  <li> IO: index stored on 240 GB <a href="http://www.ocztechnology.com/ocz-vertex-3-sata-iii-2-5-ssd.html">OCZ Vertex 3</a>, starting on 4/25 (previously on traditional spinning-magnets hard drive (Western Digital Caviar Green, 1TB))')
    w('  <li> Source code: <a href="http://code.google.com/a/apache-extras.org/p/luceneutil/source/browse/perf/Indexer.java"><tt>Indexer.java</tt></a>')
    w('  <li> All graphs are interactive <a href="http://dygraphs.com">Dygraphs</a>')
    w('</ul>')
    w('<br><a href="index.html">Back to all results</a><br>')
    footer(w)
    
def header(w, title):
  w('<html>')
  w('<head>')
  w('<title>%s</title>' % htmlEscape(title))
  w('<style type="text/css">')
  w('BODY { font-family:verdana; }')
  w('</style>')
  w('<script type="text/javascript" src="dygraph-combined-dev.js"></script>\n')
  w('</head>')
  w('<body>')
  
def footer(w):
  w('<br><em>[last updated: %s; send questions to <a href="mailto:lucene@mikemccandless.com">Mike McCandless</a>]</em>' % now())
  w('</div>')
  w('</body>')
  w('</html>')

def writeOneLine(w, seen, cat, desc):
  seen.add(cat)
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="%s.html">%s</a>' % (cat, desc))
  
def writeIndexHTML(searchChartData, days):
  f = open('%s/index.html' % constants.NIGHTLY_REPORTS_DIR, 'wb')
  w = f.write
  header(w, 'Lucene nightly benchmarks')
  w('<h1>Lucene nightly benchmarks</h1>')
  w('Each night, an <a href="https://code.google.com/a/apache-extras.org/p/luceneutil/source/browse/src/python/nightlyBench.py">automated Python tool</a> checks out the Lucene/Solr trunk source code and runs multiple benchmarks: indexing the entire <a href="http://en.wikipedia.org/wiki/Wikipedia:Database_download">Wikipedia English export</a> three times (with different settings / document sizes); running a near-real-time latency test; running a set of "hardish" auto-generated queries and tasks.  The tests take around 2.5 hours to run, and the results are verified against the previous run and then added to the graphs linked below.')
  w('<p>The goal is to spot any long-term regressions (or, gains!) in Lucene\'s performance that might otherwise accidentally slip past the committers, hopefully avoiding the fate of the <a href="http://en.wikipedia.org/wiki/Boiling_frog">boiling frog</a>.</p>')
  w('<p>See more details in <a href="http://blog.mikemccandless.com/2011/04/catching-slowdowns-in-lucene.html">this blog post</a>.</p>')

  done = set()

  w('<br><br><b>Indexing:</b>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="indexing.html">Indexing throughput</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="analyzers.html">Analyzers throughput</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="nrt.html">Near-real-time refresh latency</a>')

  w('<br><br><b>BooleanQuery:</b>')
  writeOneLine(w, done, 'AndHighHigh', '+high-freq +high-freq')
  writeOneLine(w, done, 'AndHighMed', '+high-freq +medium-freq')
  writeOneLine(w, done, 'OrHighHigh', 'high-freq high-freq')
  writeOneLine(w, done, 'OrHighMed', 'high-freq medium-freq')
  writeOneLine(w, done, 'AndHighOrMedMed', '+high-freq +(medium-freq medium-freq)')
  writeOneLine(w, done, 'AndMedOrHighHigh', '+medium-freq +(high-freq high-freq)')

  w('<br><br><b>Proximity queries:</b>')
  writeOneLine(w, done, 'Phrase', 'Exact phrase')
  writeOneLine(w, done, 'SloppyPhrase', 'Sloppy (~4) phrase')
  writeOneLine(w, done, 'SpanNear', 'Span near (~10)')

  w('<br><br><b>FuzzyQuery:</b>')
  writeOneLine(w, done, 'Fuzzy1', 'Edit distance 1')
  writeOneLine(w, done, 'Fuzzy2', 'Edit distance 2')

  w('<br><br><b>Other queries:</b>')
  writeOneLine(w, done, 'Term', 'TermQuery')
  writeOneLine(w, done, 'Respell', 'Respell (DirectSpellChecker)')
  writeOneLine(w, done, 'PKLookup', 'Primary key lookup')
  writeOneLine(w, done, 'Wildcard', 'WildcardQuery')  
  writeOneLine(w, done, 'Prefix3', 'PrefixQuery (3 leading characters)')  
  writeOneLine(w, done, 'IntNRQ', 'Numeric range filtering on last-modified-datetime')  
  
  w('<br><br><b>Faceting:</b>')
  writeOneLine(w, done, 'TermDateFacets', 'Term query + date hierarchy')
  writeOneLine(w, done, 'BrowseDateTaxoFacets', 'All dates hierarchy')
  writeOneLine(w, done, 'BrowseMonthTaxoFacets', 'All months')
  writeOneLine(w, done, 'BrowseMonthSSDVFacets', 'All months (doc values)')
  writeOneLine(w, done, 'BrowseDayOfYearTaxoFacets', 'All dayOfYear')
  writeOneLine(w, done, 'BrowseDayOfYearSSDVFacets', 'All dayOfYear (doc values)')

  w('<br><br><b>Sorting (on TermQuery):</b>')
  writeOneLine(w, done, 'TermDTSort', 'Date/time (long, high cardinality)')
  writeOneLine(w, done, 'TermTitleSort', 'Title (string, high cardinality)')
  writeOneLine(w, done, 'TermMonthSort', 'Month (string, low cardinality)')
  writeOneLine(w, done, 'TermDayOfYearSort', 'Day of year (int, medium cardinality)')

  w('<br><br><b>Grouping (on TermQuery):</b>')
  writeOneLine(w, done, 'TermGroup100', '100 groups')
  writeOneLine(w, done, 'TermGroup10K', '10K groups')
  writeOneLine(w, done, 'TermGroup1M', '1M groups')
  writeOneLine(w, done, 'TermBGroup1M', '1M groups (two pass block grouping)')
  writeOneLine(w, done, 'TermBGroup1M1P', '1M groups (single pass block grouping)')

  w('<br><br><b>Others:</b>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="../geobench.html">Geo spatial benchmarks</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="sparseResults.html">Sparse vs dense doc values performance on NYC taxi ride corpus</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="antcleantest.html">"ant clean test" time in lucene</a>')
  w('<br>&nbsp;&nbsp;&nbsp;&nbsp;<a href="checkIndexTime.html">CheckIndex time</a>')
  
  l = searchChartData.keys()
  lx = []
  for s in l:
    if s not in done:
      done.add(s)
      v = taskRename.get(s, s)
      lx.append((v, '<br>&nbsp;&nbsp;<a href="%s.html">%s</a>' % \
                 (htmlEscape(s), htmlEscape(v))))
  lx.sort()
  for ign, s in lx:
    w(s)

  if False:
    w('<br><br>')
    w('<b>Details by day</b>:')
    w('<br>')
    days.sort()
    for t in days:
      timeStamp = '%04d.%02d.%02d.%02d.%02d.%02d' % (t.year, t.month, t.day, t.hour, t.minute, t.second)
      timeStamp2 = '%s %02d/%02d/%04d' % (t.strftime('%a'), t.month, t.day, t.year)
      w('<br>&nbsp;&nbsp;<a href="%s.html">%s</a>' % (timeStamp, timeStamp2))

  w('<br><br>')
  footer(w)

taskRename = {
  'PKLookup': 'Primary Key Lookup',
  'Fuzzy1': 'FuzzyQuery (edit distance 1)',
  'Fuzzy2': 'FuzzyQuery (edit distance 2)',
  'Term': 'TermQuery',
  'TermDTSort': 'TermQuery (date/time sort)',
  'TermTitleSort': 'TermQuery (title sort)',
  'TermBGroup1M': 'Term (bgroup)',
  'TermBGroup1M1P': 'Term (bgroup, 1pass)',
  'IntNRQ': 'NumericRangeQuery (int)',
  'Prefix3': 'PrefixQuery (3 characters)',
  'Phrase': 'PhraseQuery (exact)',
  'SloppyPhrase': 'PhraseQuery (sloppy)',
  'SpanNear': 'SpanNearQuery',
  'AndHighHigh': 'BooleanQuery (AND, high freq, high freq term)',
  'AndHighMed': 'BooleanQuery (AND, high freq, medium freq term)',
  'OrHighHigh': 'BooleanQuery (OR, high freq, high freq term)',
  'OrHighMed': 'BooleanQuery (OR, high freq, medium freq term)',
  'Wildcard': 'WildcardQuery',
  'BrowseDayOfYearTaxoFacets': 'All flat taxonomy facet counts for last-modified day-of-year',
  'BrowseMonthTaxoFacets': 'All flat taxonomy facet counts for last-modified month',
  'BrowseDateTaxoFacets': 'All hierarchical taxonomy facet counts for last-modified year/month/day',
  'BrowseDayOfYearSSDVFacets': 'All flat sorted-set doc values facet counts for last-modified day-of-year',
  'BrowseMonthSSDVFacets': 'All flat sorted-set doc values facet counts for last-modified month',
  }

def htmlEscape(s):
  return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def sort(l):
  x = l[0]
  del l[0]
  l.sort()
  l.insert(0, x)
  return l

def writeOneGraphHTML(title, fileName, chartHTML):
  f = open(fileName, 'wb')
  w = f.write
  header(w, title)
  w('<br>Click and drag to zoom; shift + click and drag to scroll after zooming; hover over an annotation to see details<br>')
  w(chartHTML)
  w('\n')
  writeKnownChanges(w)
  w('<b>Notes</b>:')
  w('<ul>')
  if title.find('Primary Key') != -1:
    w('<li>Lookup 4000 random documents using unique field "id"')
    w('<li>The "id" field is indexed with Pulsing codec.')
  w('<li> Test runs %s instances of each tasks/query category (auto-discovered with <a href="http://code.google.com/a/apache-extras.org/p/luceneutil/source/browse/perf/CreateQueries.java">this Java tool</a>)' % COUNTS_PER_CAT)
  w('<li> Each of the %s instances are run %s times per JVM instance; we keep the best (fastest) time per task/query instance' % (COUNTS_PER_CAT, TASK_REPEAT_COUNT))
  w('<li> %s JVM instances are run; we compute mean/stddev from these' % JVM_COUNT)
  w('<li> %d searching threads\n' % constants.SEARCH_NUM_THREADS)
  w('<li> One sigma error bars')
  w('<li> Source code: <a href="http://code.google.com/a/apache-extras.org/p/luceneutil/source/browse/perf/SearchPerfTest.java"><tt>SearchPerfTest.java</tt></a>')
  w('<li> All graphs are interactive <a href="http://dygraphs.com">Dygraphs</a>')
  w('</ul>')
  w('<br><a href="index.html"><b>Back to all results...</b></a><br>')
  footer(w)
  f.close()

def writeKnownChanges(w, pctOffset=77):
  # closed in footer()
  w('<div style="position: absolute; top: %d%%">\n' % pctOffset)
  w('<br>')
  w('<b>Known changes:</b>')
  w('<ul>')
  label = 0
  for date, timestamp, desc, fullDesc, label in annotations:
    w('<li><p><b>%s</b> (%s): %s</p>' % (getLabel(label), date, fullDesc))
    label += 1
  w('</ul>')

def writeIndexingHTML(medChartData, bigChartData, gcTimesChartData):
  f = open('%s/indexing.html' % constants.NIGHTLY_REPORTS_DIR, 'wb')
  w = f.write
  header(w, 'Lucene nightly indexing benchmark')
  w('<br>Click and drag to zoom; shift + click and drag to scroll after zooming; hover over an annotation to see details<br>')
  w('<br>')
  w(getOneGraphHTML('MedIndexTime', medChartData, "Plain text GB/hour", "~1 KB Wikipedia English docs", errorBars=False, pctOffset=10))

  w('<br>')
  w('<br>')
  w('<br>')
  w(getOneGraphHTML('BigIndexTime', bigChartData, "Plain text GB/hour", "~4 KB Wikipedia English docs", errorBars=False, pctOffset=80))
  w('\n')

  w('<br>')
  w('<br>')
  w('<br>')
  w(getOneGraphHTML('GCTimes', gcTimesChartData, "Seconds", "JIT/GC times indexing ~1 KB docs", errorBars=False, pctOffset=150))
  w('\n')

  writeKnownChanges(w, pctOffset=227)

  w('<br><br>')
  w('<b>Notes</b>:\n')
  w('<ul>\n')
  w('  <li> Test does <b>not wait for merges on close</b> (calls <tt>IW.close(false)</tt>)')
  w('  <li> Analyzer is <tt>StandardAnalyzer</tt>, but we <b>index all stop words</b>')
  w('  <li> Test indexes full <a href="http://en.wikipedia.org/wiki/Wikipedia:Database_download">Wikipedia English XML export</a> (1/15/2011), from a pre-created line file (one document per line), on a different drive from the one that stores the index')
  w('  <li> %d indexing threads\n' % constants.INDEX_NUM_THREADS)  
  w('  <li> %s MB RAM buffer\n' % INDEXING_RAM_BUFFER_MB)
  w('  <li> Java command-line: <tt>%s</tt>\n' % constants.JAVA_COMMAND)
  w('  <li> Java version: <tt>%s</tt>\n' % htmlEscape(os.popen('java -version 2>&1').read().strip()))
  w('  <li> OS: <tt>%s</tt>\n' % htmlEscape(os.popen('uname -a 2>&1').read().strip()))
  w('  <li> CPU: 2 Xeon X5680, overclocked @ 4.0 Ghz (total 24 cores = 2 CPU * 6 core * 2 hyperthreads)\n')
  w('  <li> IO: index stored on 240 GB <a href="http://www.ocztechnology.com/ocz-vertex-3-sata-iii-2-5-ssd.html">OCZ Vertex 3</a>, starting on 4/25 (previously on traditional spinning-magnets hard drive (Western Digital Caviar Green, 1TB))')
  w('  <li> Source code: <a href="http://code.google.com/a/apache-extras.org/p/luceneutil/source/browse/perf/Indexer.java"><tt>Indexer.java</tt></a>')
  w('  <li> All graphs are interactive <a href="http://dygraphs.com">Dygraphs</a>')
  w('</ul>')
  w('<br><a href="index.html">Back to all results</a><br>')
  footer(w)
  f.close()

def writeNRTHTML(nrtChartData):
  f = open('%s/nrt.html' % constants.NIGHTLY_REPORTS_DIR, 'wb')
  w = f.write
  header(w, 'Lucene nightly near-real-time latency benchmark')
  w('<br>')
  w(getOneGraphHTML('NRT', nrtChartData, "Milliseconds", "Time (msec) to open a new reader", errorBars=True))
  writeKnownChanges(w)
  
  w('<b>Notes</b>:\n')
  w('<ul>\n')
  w('  <li> Test starts from full Wikipedia index, then use <tt>IW.updateDocument</tt> (so we stress deletions)')
  w('  <li> Indexing rate: %s updates/second (= ~ 1MB plain text / second)' % NRT_DOCS_PER_SECOND)
  w('  <li> Reopen NRT reader once per second')
  w('  <li> One sigma error bars')
  w('  <li> %s indexing thread' % NRT_INDEX_THREADS)
  w('  <li> 1 reopen thread')
  w('  <li> %s searching threads' % NRT_SEARCH_THREADS)
  w('  <li> Source code: <a href="http://code.google.com/a/apache-extras.org/p/luceneutil/source/browse/perf/NRTPerfTest.java"><tt>NRTPerfTest.java</tt></a>')
  w('<li> All graphs are interactive <a href="http://dygraphs.com">Dygraphs</a>')
  w('</ul>')

  w('<br><a href="index.html">Back to all results</a><br>')
  footer(w)
  w('</body>\n')
  w('</html>\n')

onClickJS = '''
  function zp(num,count) {
    var ret = num + '';
    while(ret.length < count) {
      ret = "0" + ret;
    }
    return ret;
  }

  function doClick(ev, msec, pts) {
    d = new Date(msec);
    top.location = d.getFullYear() + "." + zp(1+d.getMonth(), 2) + "." + zp(d.getDate(), 2) + "." + zp(d.getHours(), 2) + "." + zp(d.getMinutes(), 2) + "." + zp(d.getSeconds(), 2) + ".html";
  }
'''

def getOneGraphHTML(id, data, yLabel, title, errorBars=True, pctOffset=5):
  l = []
  w = l.append
  series = data[0].split(',')[1]
  w('<style type="text/css">')
  w('  #%s {\n' % id)
  w('    position: absolute;')
  w('    left: 10px;')
  w('    top: %d%%;' % pctOffset)
  w('  }')
  w('</style>')
  w('<div id="%s" style="height:70%%; width: 98%%"></div>' % id)
  w('<script type="text/javascript">')
  w(onClickJS)
  w('  g_%s = new Dygraph(' % id)
  w('    document.getElementById("%s"),' % id)
  seenTimeStamps = set()
  for s in data[:-1]:
    w('    "%s\\n" +' % s)
    timeStamp = s[:s.find(',')]
    seenTimeStamps.add(timeStamp)
  s = data[-1]
  w('    "%s\\n",' % s)
  timeStamp = s[:s.find(',')]
  seenTimeStamps.add(timeStamp)
  options = []
  options.append('title: "%s"' % title)
  options.append('xlabel: "Date"')
  options.append('colors: ["#218559", "#192823", "#B0A691", "#06A2CB", "#EBB035", "#DD1E2F"]')
  options.append('ylabel: "%s"' % yLabel)
  options.append('labelsKMB: true')
  options.append('labelsSeparateLines: true')
  #options.append('labelsDivWidth: "50%"')
  options.append('clickCallback: doClick')
  options.append("labelsDivStyles: {'background-color': 'transparent'}")

  # show past 2 years by default:
  start = datetime.datetime.now() - datetime.timedelta(days=2*365)
  end = datetime.datetime.now() + datetime.timedelta(days=2)
  options.append('dateWindow: [Date.parse("%s/%s/%s"), Date.parse("%s/%s/%s")]' % \
                 (start.year, start.month, start.day,
                  end.year, end.month, end.day))
  if False:
    if errorBars:
      maxY = max([float(x.split(',')[1])+float(x.split(',')[2]) for x in data[1:]])
    else:
      maxY = max([float(x.split(',')[1]) for x in data[1:]])
    options.append('valueRange:[0,%.3f]' % (maxY*1.25))
  #options.append('includeZero: true')
                 
  if errorBars:
    options.append('errorBars: true')
    options.append('sigma: 1')

  options.append('showRoller: false')

  w('    {%s}' % ', '.join(options))
    
  if 0:
    if errorBars:
      w('    {errorBars: true, valueRange:[0,%.3f], sigma:1, title:"%s", ylabel:"%s", xlabel:"Date"}' % (maxY*1.25, title, yLabel))
    else:
      w('    {valueRange:[0,%.3f], title:"%s", ylabel:"%s", xlabel:"Date"}' % (maxY*1.25, title, yLabel))
  w('  );')
  w('  g_%s.setAnnotations([' % id)
  descDedup = set()
  for date, timestamp, desc, fullDesc, label in annotations:
    # if this annot's timestamp was not seen in this chart, skip it:
    if timestamp not in seenTimeStamps:
      #print('SKIP: %s, %s, %s:%s' % (timestamp, desc, id, getLabel(label)))
      continue
    #print('KEEP: %s, %s, %s:%s' % (timestamp, desc, id, getLabel(label)))

    # if the same description on the same date was already added to this chart, skip it:
    tup = (desc, date)
    if tup in descDedup:
      continue
    descDedup.add(tup)
    if 'JIT/GC' not in title or label >= 33:
      w('    {')
      w('      series: "%s",' % series)
      w('      x: "%s",' % timestamp)
      w('      shortText: "%s",' % getLabel(label))
      w('      width: 20,')
      w('      text: "%s",' % desc)
      w('    },')
  w('  ]);')
  w('</script>')

  if 0:
    f = open('%s/%s.txt' % (constants.NIGHTLY_REPORTS_DIR, id), 'wb')
    for s in data:
      f.write('%s\n' % s)
    f.close()
  return '\n'.join(l)

def getLabel(label):
  if label < 26:
    s = chr(65+label)
  else:
    s = '%s%s' % (chr(65+(label/26 - 1)), chr(65 + (label%26)))
  return s

def sendEmail(toEmailAddr, subject, messageText):
  try:
    import localpass
    useSendMail = False
  except ImportError:
    useSendMail = True
  if not useSendMail:
    SMTP_SERVER = localpass.SMTP_SERVER
    SMTP_PORT = localpass.SMTP_PORT
    FROM_EMAIL = 'admin@mikemccandless.com'
    smtp = smtplib.SMTP(SMTP_SERVER, port=SMTP_PORT)
    smtp.ehlo(FROM_EMAIL)
    smtp.starttls()
    smtp.ehlo(FROM_EMAIL)
    localpass.smtplogin(smtp)
    msg = 'From: %s\r\n' % 'mail@mikemccandless.com'
    msg += 'To: %s\r\n' % toEmailAddr
    msg += 'Subject: %s\r\n' % subject
    msg += '\r\n'
    smtp.sendmail('mail@mikemccandless.com', toEmailAddr.split(','), msg)
    smtp.quit()
  else:
    from email.mime.text import MIMEText
    from subprocess import Popen, PIPE

    msg = MIMEText(messageText)
    msg["From"] = 'mail@mikemccandless.com'
    msg["To"] = toEmailAddr
    msg["Subject"] = subject
    p = Popen(["/usr/sbin/sendmail", "-t"], stdin=PIPE)
    p.communicate(msg.as_string())

if __name__ == '__main__':
  try:
    if '-run' in sys.argv:
      run()
    makeGraphs()
  except:
    traceback.print_exc()
    if not DEBUG and REAL:
      import socket
      sendEmail('mail@mikemccandless.com', 'Nightly Lucene bench FAILED (%s)' % socket.gethostname(), '')
    
# scp -rp /lucene/reports.nightly mike@10.17.4.9:/usr/local/apache2/htdocs

# TO CLEAN
#   - rm -rf /p/lucene/indices/trunk.nightly.index.prev/
#   - rm -rf /lucene/logs.nightly/*
#   - rm -rf /lucene/reports.nightly/*
#   - rm -f /lucene/trunk.nightly/modules/benchmark/*.x

