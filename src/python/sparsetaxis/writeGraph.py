import re

reMergeTime = re.compile(r': (\d+) msec to merge ([a-z ]+) \[(\d+) docs\]')
reFlushTime = re.compile(r': flush time ([.0-9]+) msec')
reDocsPerMB = re.compile('newFlushedSize.*? docs/MB=([.,0-9]+)$')
reIndexingRate = re.compile('([.0-9]+) sec: (\d+) docs; ([.0-9]+) docs/sec; ([.0-9]+) MB/sec')
        
def indexStats(indexLog):
  mergeTimesSec = {}
  flushTimeSec = 0
  docsPerMB = 0
  flushCount = 0
  lastDPSMatch = None
  with open(indexLog, 'r', encoding='utf-8') as f:
    while True:
      line = f.readline()
      if line == '':
        break
      line = line.strip()
      m = reMergeTime.search(line)
      if m is not None:
        msec, part, docCount = m.groups()
        msec = int(msec)
        docCount = int(docCount)
        if part not in mergeTimesSec:
          mergeTimesSec[part] = [0, 0]
        l = mergeTimesSec[part]
        l[0] += msec/1000.0
        l[1] += docCount
      m = reFlushTime.search(line)
      if m is not None:
        flushTimeSec += float(m.group(1))/1000.
      m = reDocsPerMB.search(line)
      if m is not None:
        docsPerMB += float(m.group(1).replace(',', ''))
        flushCount += 1
      m = reIndexingRate.search(line)
      if m is not None:
        lastDPSMatch = m
      
  return float(lastDPSMatch.group(3)), mergeTimesSec, flushTimeSec, docsPerMB/flushCount

reHits = re.compile('T(.) (.): ([0-9]+) hits in ([.0-9]+) msec')
def searchStats(searchLog):
  
  heapBytes = None
  byThread = {}
  
  with open(searchLog, 'r', encoding='utf-8') as f:
    while True:
      line = f.readline()
      if line == '':
        break
      line = line.strip()
      if line.startswith('HEAP: '):
        heapBytes = int(line[6:])
      else:
        m = reHits.match(line)
        if m is not None:
          threadID, color, hitCount, msec = m.groups()
          if threadID not in byThread:
            byThread[threadID] = []
          byThread[threadID].append((color, int(hitCount), float(msec)))

  byColor = {'y': [], 'g': []}
  for threadID, results in byThread.items():
    # discard warmup
    results = results[10:]
    for color, hitCount, msec in results:
      byColor[color].append(msec)

  results = [heapBytes]
  byColor['g'].sort()
  byColor['y'].sort()

  g = byColor['g']
  results.append(g[len(g)//2])

  y = byColor['y']
  results.append(y[len(y)//2])

  return results

print(searchStats('/l/taxisroot/sparseTaxis/logs/searchSparse.log'))
