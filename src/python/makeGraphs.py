import responseTimeGraph
import loadGraphActualQPS
import sys
import re
import os

reQPS = re.compile(r'\.qps([\.0-9]+)(\.|$)')
rePCT = re.compile(r'\.pct([\.0-9]+)$')

def main(maxQPS = None):
  logsDir = sys.argv[1]
  warmupSec = int(sys.argv[2])
  reportsDir = sys.argv[3]

  if maxQPS is None:
    if os.path.exists(reportsDir):
      print('ERROR: please move existing reportsDir (%s) out of the way' % reportsDir)
      sys.exit(1)

      # print('getInterpValue: list=%s interpDate=%s' % (valueList, interpDate))

    os.makedirs(reportsDir)

  qps = set()
  names = set()
  byName = {}
  byPCT = {}
  
  for f in os.listdir(logsDir):
    m = reQPS.search(f)
    if m is not None:

      qpsString = m.group(1)
      qpsValue = float(qpsString)

      if False and qpsValue > 200:
        continue
        
      qps.add((qpsValue, qpsString))
      name = f[:f.find('.qps')]

      # nocommit
      if name.find('G1') != -1:
        continue
      
      if name.lower().find('warmup') == -1:
        names.add(name)
        if name not in byName:
          byName[name] = []
        byName[name].append((qpsValue, qpsString, f))

        m = rePCT.search(f)
        if m is not None:
          pct = int(m.group(1))
          if pct not in byPCT:
            byPCT[pct] = []
          byPCT[pct].append((name, qpsValue, qpsString, f))

  if len(names) == 0:
    raise RuntimeError('no logs found @ %s' % logsDir)

  # if False:
  #   for name, l in byName.items():
  #     l.sort()
  #     for idx, (qpsS, qps, dirName) in enumerate(l):
  #       if dirName.find('.pct') == -1:
  #         os.rename('%s/%s' % (logsDir, dirName),
  #                   '%s/%s.pct%s' % (logsDir, dirName, pcts[idx]))
  #     print('%s -> %s' % (name, l))

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

      if len(byPCT) != 0:
        l = list(byPCT.keys())
        l.sort()
        
        # Collate by PCT:
        for idx, pct in enumerate(l):

          # Sort by name so we get consistent colors:
          l2 = byPCT[pct]
          l2.sort()
          
          d = []
          for name, qpsValue, qpsString, resultsFile in l2:
            d.append(('%s.qps%s' % (name, qpsString), '%s/%s/results.bin' % (logsDir, resultsFile)))

          if len(d) == 0:
            raise RuntimeError('nothing matches pct=%d' % pct)

          print()
          print('Create response-time graph @ pct=%d: d=%s' % (pct, d))

          if len(d) != 0:
            try:
              html = responseTimeGraph.createGraph(d, warmupSec, title='Query time at %d%% load' % pct)
            except IndexError:
              raise
            fileName = '%s/responseTime%sPCT.html' % (reportsDir, pct)
            open(fileName, 'wb').write(html)
            w('<a href="%s">at %d%% load</a>' % (fileName, pct))
            w('<br>\n')
        
      else:
        # Collate by QPS:
        l = list(qps)
        l.sort()

        for idx, (qps, qpsString) in enumerate(l):

          d = []
          validNames = []
          for name in names:
            resultsFile = '%s/%s.qps%s/results.bin' % (logsDir, name, qpsString)
            if os.path.exists(resultsFile):
              d.append(('%s.qps%s' % (name, qpsString), resultsFile))
              validNames.append(name)

          if len(d) == 0:
            #raise RuntimeError('nothing matches qps=%s' % qpsString)
            continue

          print()
          print('Create response-time graph @ qps=%s: d=%s' % (qps, d))

          if len(d) != 0:
            try:
              html = responseTimeGraph.createGraph(d, warmupSec)
            except IndexError:
              raise
            open('%s/%sqps.html' % (reportsDir, qpsString), 'wb').write(html)
            w('<a href="%sqps.html">%d queries/sec [%s]</a>' % (qpsString, qps, ', '.join(responseTimeGraph.cleanName(x) for x in validNames)))
            w('<br>\n')

    if maxQPS is None:
      w('<h2>By percentile:</h2>')
    else:
      w('<h2>By percentile (max QPS %d):</h2>' % maxQPS)

    allPassesSLA = None

    for idx in range(len(loadGraphActualQPS.logPoints)):

      print()
      print('Create pct graph @ %s%%' % loadGraphActualQPS.logPoints[idx][0])

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

    print()
    print('Max actual QPS:')
    for name, qps in maxActualQPS.items():
      print('  %s: %.1f QPS' % (name, qps))

    print()
    print('Highest QPS w/ SLA met:')
    for qps, name in l:
      print('  %s: %s QPS' % (responseTimeGraph.cleanName(name), qps))

    w('<a href="loadmax.html">100%%</a>')
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
