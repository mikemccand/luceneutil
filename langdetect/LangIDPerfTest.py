import time
import langid
import sys
import io

testData = []
totBytesUnicode = 0
totBytesUTF8 = 0
f = io.open(sys.argv[1], 'r', encoding='utf-8')
while True:
  line = f.readline()
  if line == '':
    break
  idx = line.find('\t')
  if idx == -1:
    continue
  test = line[idx+1:].strip()
  totBytesUnicode += len(test)
  utf8 = test.encode('utf-8')
  totBytesUTF8 += len(utf8)
  testData.append(utf8)

print 'Tot bytes %s vs %s' % (totBytesUnicode, totBytesUTF8)

best = -1
for i in range(10):
  answers = []
  t0 = time.time()
  for test in testData:
    #answers.append(langid.classify(test)[0])
    langid.classify(test)
  t = time.time() - t0
  print '%.1f msec' % (1000*t)
  if best == -1 or t < best:
    best = t
    print '  **'

print 'Best %.1f msec; totBytes=%d MB/sec=%.1f' % \
      (1000*best, totBytes, totBytes/1024./1024./best)

  
