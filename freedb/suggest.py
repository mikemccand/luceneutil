import time
import os
import sys
import subprocess
import getch

#LUCENE_HOME = '/l/lucene.trunk2'
LUCENE_HOME = '/l/predictivesuggest2'

cp = []
cp.append('.')
cp.append('%s/lucene/build/core/classes/java' % LUCENE_HOME)
cp.append('%s/lucene/build/suggest/classes/java' % LUCENE_HOME)
cp.append('%s/lucene/build/analysis/common/classes/java' % LUCENE_HOME)
cp.append('%s/lucene/build/analysis/icu/classes/java' % LUCENE_HOME)
cp.append('%s/lucene/analysis/icu/lib/icu4j-49.1.jar' % LUCENE_HOME)

cmd = 'java -Xmx3g -cp %s FreeDBSuggest %s -server' % (':'.join(cp), sys.argv[1])

p = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
print p.stdout.readline().strip()

getch = getch._Getch()

query = []
while True:
  c = getch()
  if c == '':
    break
  if c == '\r':
    break
  elif c == '\x7f':
    query = query[:-1]
  else:
    query.append(c)

  q = ''.join(query)
  t0 = time.time()
  p.stdin.write('%2d' % len(q))
  p.stdin.write(q)
  x = int(p.stdout.read(5))
  result = p.stdout.read(x)
  t1 = time.time()

  #print '\x9b0m\x1bc\x1b[2J%s [%.1f msec]\n\n%s' % (''.join(query), (t1-t0)*1000, result)
  print '\x9b0m\x1b[He\x1b[2J%s [%.1f msec]\n\n%s' % (''.join(query), (t1-t0)*1000, result)
  
