import time
import sys
import os
import benchUtil

JAVA_COMMAND = 'java -Xbatch -Xms2g -Xmx2g -server'
#JAVA_COMMAND = 'java -Xbatch -Xms2g -Xmx2g'

if '-debug' in sys.argv:
  JAVA_COMMAND += ' -ea:org.apache.lucene...'

WIKI_FILE = '/x/lucene/enwiki-20100302-pages-articles.xml.bz2'
WIKI_LINE_FILE = '/lucene/enwiki-20100302-lines-1k.txt'
INDEX_DIR_BASE = '/lucene/indices'
LOG_DIR = './logs'

if '-debug' in sys.argv:
  INDEX_NUM_DOCS = 100000
else:
  INDEX_NUM_DOCS = 10000000

INDEX_NUM_THREADS = 1

BASE = ('clean', 'Standard')
CMP = ('bulk', 'FrameOfRef')
#CMP = ('clean', 'MockSep')

#BASE = ('clean', 'MockSep')
#CMP = ('leanterms', 'MockSep')

def run():
    
  r = benchUtil.RunAlgs(INDEX_DIR_BASE, JAVA_COMMAND)

  if '-noc' not in sys.argv:
    for wd, codec in (BASE, CMP):
      r.compile('/lucene/%s/lucene/contrib/benchmark' % wd)

  indices = {}
  for wd, codec in (CMP, BASE):
    indices[(wd, codec)] = r.makeIndex(codec, '/lucene/%s/lucene/contrib/benchmark' % wd, wd + '-enwdf', 'wiki', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE)

  logUpto = 0

  if '-debugs' in sys.argv or '-debug' in sys.argv:
    iters = 5
    threads = 1
  else:
    iters = 40
    threads = 4

  results = {}
  for wd, codec in (BASE, CMP):
    print 'Search on %s...' % wd
    t0 = time.time()
    results[(wd,codec)] = r.runSimpleSearchBench('/lucene/%s/lucene/contrib/benchmark' % wd, codec, 'single', '%s/index' % indices[(wd, codec)], iters, threads)
    print '  %.2f sec' % (time.time() - t0)

  r.simpleReport(results[BASE],
                 results[CMP],
                 '-jira' in sys.argv,
                 '-html' in sys.argv,
                 cmpDesc='%s-%s' % CMP,
                 baseDesc='%s-%s' % BASE)
    
def main():

  if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

  run()

if __name__ == '__main__':
  main()
