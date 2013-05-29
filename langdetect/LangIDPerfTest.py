import time
import langid
import sys
import io

testData = []
totBytes = 0
f = io.open(sys.argv[1], 'r', encoding='utf-8')
while True:
  line = f.readline()
  if line == '':
    break
  idx = line.find('\t')
  if idx == -1:
    continue
  test = line[idx+1:].strip()
  totBytes += len(test)
  testData.append(test.encode('utf-8'))

print 'Tot bytes %s' % totBytes

best = -1
for i in range(10):
  answers = []
  t0 = time.time()
  for test in testData:
    answers.append(langid.classify(test)[0])
  t = time.time() - t0
  print '%.1f msec' % (1000*t)
  if best == -1 or t < best:
    best = t
    print '  **'

print 'Best %.1f msec; totBytes=%d MB/sec=%.1f' % \
      (1000*best, totBytes, totBytes/1024./1024./best)

  
