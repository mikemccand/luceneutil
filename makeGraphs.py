import responseTimeGraph
import loadGraphActualQPS
import sys
import re
import os

reQPS = re.compile(r'\.qps([\.0-9]+)$')

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

      qpsString = m.group(1)
      qpsValue = float(qpsString)

      if False and qpsValue > 200:
        continue
        
      qps.add((qpsValue, qpsString))
      name = f[:f.find('.qps')]
      if name.lower().find('warmup') == -1:
        names.add(name)

  if len(names) == 0:
    raise RuntimeError('no logs found @ %s' % logsDir)

  l = list(qps)
  l.sort()

  if maxQPS is None:
    indexOut = open('%s/index.html' % reportsDir, 'wb')
  else:
    indexOut = open('%s/index.html' % reportsDir, 'ab')
    indexOut.write('\n<br>')
    
  w = indexOut.write

  names = list(names)
  names.sort()
  
  try:
    if maxQPS is None:
      w('<h2>By QPS:</h2>')

      for idx, (qps, qpsString) in enumerate(l):
        # python -u /l/util.azul/responseTimeGraph.py /x/azul/0712.phase3.test1/logs 300 /x/tmp4/phase3/graph.html Zing.qps350 OracleCMS.qps350

        d = []
        validNames = []
        for name in names:
          resultsFile = '%s/%s.qps%s/results.bin' % (logsDir, name, qpsString)
          if os.path.exists(resultsFile):
            d.append(('%s.qps%s' % (name, qpsString), resultsFile))
            validNames.append(name)

        if len(d) == 0:
          raise RuntimeError('nothing matches qps=%s' % qpsString)

        print
        print 'Create response-time graph @ qps=%s: d=%s' % (qps, d)

        if len(d) != 0:
          try:
            html = responseTimeGraph.createGraph(d, warmupSec)
          except IndexError:
            raise
          open('%s/%sqps.html' % (reportsDir, qps), 'wb').write(html)
          w('<a href="%sqps.html">%d queries/sec [%s]</a>' % (qpsString, qps, ', '.join(responseTimeGraph.cleanName(x) for x in validNames)))
          w('<br>\n')

    if maxQPS is None:
      w('<h2>By percentile:</h2>')
    else:
      w('<h2>By percentile (max QPS %d):</h2>' % maxQPS)

    allPassesSLA = None

    for idx in xrange(len(loadGraphActualQPS.logPoints)):

      print
      print 'Create pct graph @ %s%%' % loadGraphActualQPS.logPoints[idx][0]

      if maxQPS is not None:
        fileName = 'load%spct_max%s.html' % (loadGraphActualQPS.logPoints[idx][0], maxQPS)
      else:
        fileName = 'load%spct.html' % (loadGraphActualQPS.logPoints[idx][0])

      stop = False
      try:
        passesSLA, maxActualQPS = loadGraphActualQPS.graph(idx, logsDir, warmupSec, names, '%s/%s' % (reportsDir, fileName), maxQPS=maxQPS)
      except RuntimeError:
        stop = True

      if passesSLA is not None:
        if allPassesSLA is None:
          allPassesSLA = passesSLA
        else:
          for x in list(allPassesSLA):
            if x not in passesSLA:
              allPassesSLA.remove(x)

      if stop:
        break
      
      w('<a href="%s">%s %%</a>' % (fileName, loadGraphActualQPS.logPoints[idx][0]))
      w('<br>\n')

    fileName = '%s/loadmax.html' % reportsDir      
    passesSLA, maxActualQPS = loadGraphActualQPS.graph('max', logsDir, warmupSec, names, fileName, maxQPS=maxQPS)
    if passesSLA is not None:
      for x in list(allPassesSLA):
        if x not in passesSLA:
          allPassesSLA.remove(x)

    highest = {}
    for qps, name in allPassesSLA:
      if name not in highest or qps > highest[name]:
        highest[name] = qps

    l = [(y, x) for x, y in highest.items()]
    l.sort()

    print
    print 'Max actual QPS:'
    for name, qps in maxActualQPS.items():
      print '  %s: %.1f QPS' % (name, qps)

    print
    print 'Highest QPS w/ SLA met:'
    for qps, name in l:
      print '  %s: %s QPS' % (responseTimeGraph.cleanName(name), qps)

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
