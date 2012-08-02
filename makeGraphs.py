import responseTimeGraph
import loadGraph
import sys
import re
import os

reQPS = re.compile(r'\.qps(\d+)$')

def main(maxQPS = None):
  logsDir = sys.argv[1]
  warmupSec = int(sys.argv[2])
  reportsDir = sys.argv[3]

  if maxQPS is None:
    if os.path.exists(reportsDir):
      print 'ERROR: please move existing reportsDir (%s) out of the way' % reportsDir
      sys.exit(1)

    os.makedirs(reportsDir)

  qps = set()
  names = set()
  
  for f in os.listdir(logsDir):
    m = reQPS.search(f)
    if m is not None:
      qps.add(int(m.group(1)))
      names.add(f[:f.find('.qps')])

  l = list(qps)
  l.sort()

  if maxQPS is None:
    indexOut = open('%s/index.html' % reportsDir, 'wb')
  else:
    indexOut = open('%s/index.html' % reportsDir, 'ab')
    indexOut.write('\n<br>')
    
  w = indexOut.write

  try:
    if maxQPS is None:
      w('<h2>By QPS:</h2>')

      for qps in l:
        # python -u /l/util.azul/responseTimeGraph.py /x/azul/0712.phase3.test1/logs 300 /x/tmp4/phase3/graph.html Zing.qps350 OracleCMS.qps350

        d = []
        for name in names:
          resultsFile = '%s/%s.qps%s/results.pk' % (logsDir, name, qps)
          if not os.path.exists(resultsFile):
            resultsFile = '%s/%s.qps%s/results.bin' % (logsDir, name, qps)
          if os.path.exists(resultsFile):
            d.append(('%s.qps%s' % (name, qps), resultsFile))

        if len(d) != 0:
          html = responseTimeGraph.createGraph(d, warmupSec)
          open('%s/%sqps.html' % (reportsDir, qps), 'wb').write(html)
          w('<a href="%sqps.html">%d queries/sec</a>' % (qps, qps))
          w('<br>\n')

    if maxQPS is None:
      w('<h2>By percentile:</h2>')
    else:
      w('<h2>By percentile (max QPS %d):</h2>' % maxQPS)

    for idx in xrange(len(loadGraph.logPoints)):
      if maxQPS is not None:
        fileName = 'load%spct_max%s.html' % (loadGraph.logPoints[idx][0], maxQPS)
      else:
        fileName = 'load%spct.html' % (loadGraph.logPoints[idx][0])
      try:
        loadGraph.graph(idx, logsDir, warmupSec, list(names), '%s/%s' % (reportsDir, fileName), maxQPS=maxQPS)
      except RuntimeError:
        break
      w('<a href="%s">%s %%</a>' % (fileName, loadGraph.logPoints[idx][0]))
      w('<br>\n')

    fileName = '%s/loadmax.html' % reportsDir      
    loadGraph.graph('max', logsDir, warmupSec, list(names), fileName, maxQPS=maxQPS)
    w('<a href="%s">100%%</a>' % fileName)
    w('<br>')

  finally:
    indexOut.close()

if __name__ == '__main__':
  if '-again' in sys.argv:
    idx = sys.argv.index('-again')
    maxQPS = int(sys.argv[1+idx])
    del sys.argv[idx:idx+2]
  else:
    maxQPS = None

  main()
  if maxQPS is not None:
    main(maxQPS)
