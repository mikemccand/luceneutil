import time
import sys
import re
import datetime
import math

# see http://people.apache.org/~mikemccand/lucenebench/iw.html as an example

# TODO
#   - add chart showing %tg deletes
#   - combine "segment count in index" with "segments being merged"
#   - parse date too
#   - separate "stalled merge MB" from running
#   - flush sizes/frequency
#   - commit frequency

reDateTime = re.compile('(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)(,\d\d\d)?')
reFindMerges = re.compile('findMerges: (\d+) segments')
reMergeSize = re.compile(r'[cC](\d+) size=(.*?) MB')
reMergeSizeWithDel = re.compile(r'[cC](\d+)/(\d+):delGen=(\d+) size=(.*?) MB')
reMergeStart = re.compile(r'merge seg=(.*?) ')
reMergeEnd = re.compile(r'merged segment size=(.*?) MB')
reGetReader = re.compile(r'getReader took (\d+) msec')
# Straight lucene log:
reThreadName = re.compile(r'^IW \d+ \[.*?; (.*?)\]:')

# Two variations from Elasticsearch:
reThreadNameES = re.compile(r' elasticsearch\[.*?\](\[.*?\]\[.*?\])')
reThreadNameES2 = re.compile(r' elasticsearch\[.*?\]\[\[.*?\]\[.*?\]: (.*?)\] ')

reIndexedDocCount = re.compile(r'^Indexer: (\d+) docs: ([0-9\.]+) sec')
reShardName = re.compile(r'\[lucene.iw\s*\] \[(.*?)\]\[(.*?)\]\[(\d+)\]')

def parseDateTime(line):
  m = reDateTime.search(line)
  if m is None:
    return None
  
  # year, month, day, hours, minutes, seconds:
  t = list(m.groups())
  if len(t) == 7:
    if t[-1] is None:
      t = [int(x) for x in t[:6]]
    else:
      t2 = [int(x) for x in t[:6]]
      # ES logs have msec:
      t2[-1] += float(t[6][1:])/1000.0
      t = t2
  elif len(t) == 6:
    t = [int(x) for x in t[:3]]

  #print('LINE %s ->\n  %s' % (line, t))
  return t

class RollingTimeWindow:

  def __init__(self, windowTime):
    self.window = []
    self.windowTime = windowTime
    self.pruned = False;
    
  def add(self, t, value):
    self.window.append((t, value))
    while len(self.window) > 0 and t - self.window[0][0] > self.windowTime:
      del self.window[0]
      self.pruned = True

# WARNING: thread [[entry_20140702][0] missing from mergeThreads
# WARNING: thread [[entry_20140702][1] missing from mergeThreads

def parseThreadName(line):
  for r in reThreadNameES2, reThreadNameES, reThreadName:
    m = r.search(line)
    if m is not None:
      return m.group(1)

def main():
  segCounts = []
  pendingSegCounts = {}
  merges = []
  segsPerFullFlush = []

  inFindMerges = False
  maxSegs = 0
  maxRunningMerges = 0
  runningMerges = 0
  mergeThreads = {}
  commitCount = 0
  flushCount = 0
  minTime = None
  maxTime = None
  startFlushCount = None
  getReaderTimes = []
  commitTimes = []
  runningCommits = {}
  indexDocTimes = []
  allShards = {}
  lineCount = 0

  i = 1
  onlyShard = None
  while i < len(sys.argv):
    if sys.argv[i] == '-shard':
      onlyShard = tuple(sys.argv[i+1].split(':'))
      del sys.argv[i:i+2]
    else:
      i += 1

  if onlyShard is not None:
    print('only: %s' % ':'.join(onlyShard))

  with open(sys.argv[1]) as f:
    for line in f.readlines():
      line = line.strip()

      t = parseDateTime(line)
      if t is not None:
        if minTime is None:
          minTime = t
        maxTime = t

      m = reShardName.search(line)
      if m is not None:
        shardTup = m.groups()
      else:
        print('NO SHARD: %s' % line)
        continue

      if onlyShard is not None and shardTup != onlyShard:
        continue

      threadName = parseThreadName(line)
      if threadName is None:
        print('NO THREAD: %s' % line)
        continue
        
      if line.find('startCommit(): start') != -1:
        commitCount += 1
        runningCommits[threadName] = len(commitTimes)
        commitTimes.append(t)

      if line.find('commit: wrote segments file') != -1:
        # Might not be present if IW infoStream was enabled "mid flight":
        if threadName in runningCommits:
          commitTimes[runningCommits[threadName]].append(t)
          del runningCommits[threadName]
        
      if line.find('flush postings as segment') != -1:
        flushCount += 1

      if line.find('prepareCommit: flush') != -1 or line.find('flush at getReader') != -1:
        if startFlushCount is not None:
          #print('%s: %d' % (startFlushTime, flushCount - startFlushCount))
          segsPerFullFlush.append(startFlushTime + [flushCount - startFlushCount])
        startFlushCount = flushCount
        startFlushTime = t

      m = reIndexedDocCount.search(line)
      if m is not None:
        docCount, t = m.groups()
        indexDocTimes.append((int(docCount), float(t)))

      m = reGetReader.search(line)
      if m is not None:
        getReaderTimes.append(t + [int(m.group(1))])
        
      m = reMergeStart.search(line)
      if m is not None:
        # A merge kicked off
        mergeThreads[threadName] = len(merges)
        merges.append(['start', threadName] + t)
        runningMerges += 1
        # print("mergeStart: %s, running=%d" % (threadName, runningMerges))
        maxRunningMerges = max(maxRunningMerges, runningMerges)

      m = reMergeEnd.search(line)
      if m is not None:
        # A merge finished
        mergeSize = float(m.group(1))

        # print("mergeFinish: %s" % threadName)

        # Might not be present if IW infoStream was enabled "mid flight":
        if threadName in mergeThreads:
          merges[mergeThreads[threadName]].append(mergeSize)
          del mergeThreads[threadName]
          merges.append(['end', threadName] + t + [mergeSize])
          runningMerges -= 1
        else:
          print('WARNING: thread %s missing from mergeThreads' % threadName)
        
      m = reFindMerges.search(line)
      key = shardTup + (threadName,)
      #if m is not None and line.find('[es1][bulk]') != -1:
      if m is not None:
        segCount = int(m.group(1))
        # mergeMB, mergeSegCount, indexSizeMB, indexSizeDocs, delDocCount
        maxSegs = max(maxSegs, segCount)
        pendingSegCounts[key] = [0.0, 0, 0.0, 0.0, 0, len(segCounts)]
        segCounts.append(list(t) + [segCount])
        # print('start segCount %s' % (segCounts[-1]))
      elif key in pendingSegCounts:
        sizeDocs = None
        m = reMergeSize.search(line)
        if m is not None:
          # print('line: %s' % line.rstrip())
          sizeDocs = int(m.group(1))
          sizeMB = float(m.group(2))
          delDocs = 0
        else:
          m = reMergeSizeWithDel.search(line)
          if m is not None:
            sizeDocs = int(m.group(1))
            delDocs = int(m.group(2))
            sizeMB = float(m.group(4))

        if sizeDocs is not None:
          l = pendingSegCounts[key]
          l[2] += sizeMB
          l[3] += sizeDocs
          l[4] += delDocs
          if line.find(' [merging]') != -1:
            l[0] += sizeMB
            l[1] += 1
        elif line.find('allowedSegmentCount=') != -1:
          idx = pendingSegCounts[key][5]
          segCounts[idx].extend(pendingSegCounts[key][:5])
          allShards[shardTup] = pendingSegCounts[key][2]
          #print('finish segCount %s' % (segCounts[idx]))
          #print('  index MB %s' % segCounts[idx][9])
          del pendingSegCounts[key]

      lineCount += 1
      if False and lineCount == 200000:
        break
      if lineCount % 10000 == 0:
        print('%d lines...' % lineCount)

  now = datetime.datetime.now()
  globalStartTime = toDateTime(minTime)
  globalEndTime = toDateTime(maxTime)
  totSec = (globalEndTime-globalStartTime).total_seconds()
  print('elapsed time %s: %s - %s' % (globalEndTime-globalStartTime, globalStartTime, globalEndTime))
  print('max concurrent merges %s' % maxRunningMerges)
  print('commit count %s (avg every %.1f sec)' % \
        (commitCount, totSec/commitCount))
  print('flush count %s (avg every %.1f sec)' % \
        (flushCount, totSec/flushCount))
  print('total shard count: %s' % len(allShards))
  l = list(allShards.items())
  l.sort(key=lambda x: (-x[1], x[0]))
  for tup, mb in l:
    print('  %.3f GB: %s' % (mb/1024., ':'.join(tup)))
  
  with open('iw.html', 'w') as f:

    w = f.write

    w('''
    <html>
    <head>
    <script type="text/javascript"
      src="http://dygraphs.com/1.0.1/dygraph-combined.js"></script>
    </head>
    <body>
    ''')

    w('<table>')

    if len(indexDocTimes) > 10:
      startChart(w, 'indexedDocs60Sec', 'Indexed K docs per sec, avg over past 10 seconds')

      # Index rate past 30 seconds
      headers = ['Date', 'KDocsPerSec']
      w('    "%s\\n" + \n"' % ','.join(headers))

      r = RollingTimeWindow(30.0)

      for docCount, t in indexDocTimes:
        #print('%d, %.2f' % (docCount, t))
        r.add(t, docCount)
        if len(r.window) > 5:
          t0 = globalStartTime + datetime.timedelta(seconds=t)
          if r.pruned:
            windowTime = r.window[-1][0] - r.window[0][0]
          else:
            windowTime = r.window[-1][0]
          docCount = r.window[-1][1] - r.window[0][1]
          #print('count %s, win time %s' % (docCount, windowTime))
          w('%s:%02.3f,%.2f\\n' % (formatTime(t0.year, t0.month, t0.day, t0.hour, t0.minute, t0.second+t0.microsecond/1000000.), docCount/1000./windowTime))

      endChart(w)

    # Segment counts
    startChart(w, 'segCounts', 'Seg counts')

    headers = ['Date', 'SegCount', 'MergingSegCount']
    w('    "%s\\n" + \n"' % ','.join(headers))

    w('%s,0,0\\n' % (formatTime(minTime[0], minTime[1], minTime[2], minTime[3], minTime[4], minTime[5])))
    for tup in segCounts:
      if len(tup) >= 9:
        year, month, day, hr, min, sec, count, mergeMB, mergeSegCount = tup[:9]
        w('%s,%d,%d\\n' % (formatTime(year, month, day, hr, min, sec), count, mergeSegCount))

    endChart(w)


    # Segs per full flush
    startChart(w, 'segsFullFlush', 'Segments per full flush (client concurrency)')

    headers = ['Date', 'SegsFullFlush']
    w('    "%s\\n" + \n"' % ','.join(headers))

    w('%s,0\\n' % formatTime(minTime[0], minTime[1], minTime[2], minTime[3], minTime[4], minTime[5]))
    for tup in segsPerFullFlush:
      year, month, day, hr, min, sec, count = tup
      w('%s,%d\\n' % (formatTime(year, month, day, hr, min, sec), count))

    endChart(w)

    # Merging GB
    startChart(w, 'mergingGB', 'Total Merging GB')

    headers = ['Date', 'MergingGB']
    w('    "%s\\n" + \n"' % ','.join(headers))

    w('%s,%.2f\\n' % (formatTime(minTime[0], minTime[1], minTime[2], minTime[3], minTime[4], minTime[5]), 0.0))
    for tup in segCounts:
      if len(tup) >= 8:
        year, month, day, hr, min, sec, count, mergeMB = tup[:8]
        # print("mergeMB %s" % mergeMB)
        w('%s,%.2f\\n' % (formatTime(year, month, day, hr, min, sec), mergeMB/1024.))

    endChart(w)

    # Running merges
    startChart(w, 'runningMerges', 'Running Merges (GB)')

    headers = ['Date'] + ['Merge%s' % x for x in range(maxRunningMerges)]
    w('    "%s\\n" + \n"' % ','.join(headers))

    # Thread name -> id, size
    runningMerges = {}

    w('%s,%s\\n' % (formatTime(minTime[0], minTime[1], minTime[2], minTime[3], minTime[4], minTime[5]), ','.join(['0'] * maxRunningMerges)))
    ids = set(range(maxRunningMerges))
    for tup in merges:
      if len(tup) == 9:
        event, threadName, year, month, day, hr, min, sec, mergeMB = tup
        #if mergeMB > 0:
        #  mergeMB = math.log(mergeMB)/math.log(2.0)

        l = ['0'] * maxRunningMerges
        for ign, (id, size) in runningMerges.items():
          l[id] = '%.3f' % (size/1024.)
        w('%s,%s\\n' % (formatTime(year, month, day, hr, min, sec), ','.join(l)))

        if event == 'end':
          ids.add(runningMerges[threadName][0])
          del runningMerges[threadName]
        else:
          id = ids.pop()
          runningMerges[threadName] = id, mergeMB

        l = ['0'] * maxRunningMerges
        for ign, (id, size) in runningMerges.items():
          l[id] = '%.3f' % (size/1024.)
        w('%s,%s\\n' % (formatTime(year, month, day, hr, min, sec), ','.join(l)))

    endChart(w)

    # Index size GB
    startChart(w, 'indexSizeGB', 'Index Size GB')

    headers = ['Date', 'IndexSizeGB']
    w('    "%s\\n" + \n"' % ','.join(headers))

    w('%s,%.2f\\n' % (formatTime(minTime[0], minTime[1], minTime[2], minTime[3], minTime[4], minTime[5]), 0.0))
    for tup in segCounts:
      if len(tup) >= 10:
        year, month, day, hr, min, sec, count, mergeMB, mergingSegCount, indexSizeMB = tup[:10]
        w('%s,%.2f\\n' % (formatTime(year, month, day, hr, min, sec), indexSizeMB/1024.))

    endChart(w)

    # Pct deletes
    startChart(w, 'pctDel', 'Percent deleted docs')

    headers = ['Date', 'Deletes %']
    w('    "%s\\n" + \n"' % ','.join(headers))

    w('%s,%.2f\\n' % (formatTime(minTime[0], minTime[1], minTime[2], minTime[3], minTime[4], minTime[5]), 0.0))
    for tup in segCounts:
      if len(tup) >= 12:
        year, month, day, hr, min, sec, count, mergeMB, mergingSegCount, indexSizeMB, indexDocCount, deleteDocCount = tup[:12]
        w('%s,%.2f\\n' % (formatTime(year, month, day, hr, min, sec), (100.*deleteDocCount)/indexDocCount))

    endChart(w)

    if False:
      # Running merge count
      startChart(w, 'runningMergeCount', 'Segments being merged')

      headers = ['Date'] + ['MergeCount']
      w('    "%s\\n" + \n"' % ','.join(headers))

      w('%s,0\\n' % formatTime(minTime[0], minTime[1], minTime[2], minTime[3], minTime[4], minTime[5]))
      ids = set(range(maxRunningMerges))
      for tup in segCounts:
        if len(tup) >= 9:
          year, month, day, hr, min, sec, count, mergeMB, mergingSegCount = tup[:9]
          w('%s,%d\\n' % (formatTime(year, month, day, hr, min, sec), mergingSegCount))

      endChart(w)
    

    if False:
      # Index size Docs
      startChart(w, 'indexSizeMDocs', 'Index Size MDocs')

      headers = ['Date', 'IndexSizeMDocs']
      w('    "%s\\n" + \n"' % ','.join(headers))

      for tup in segCounts:
        if len(tup) >= 11:
          year, month, day, hr, min, sec, count, mergeMB, mergingSegCount, indexSizeMB, indexSizeDocs = tup[:11]
          w('%s,%.2f\\n' % (formatTime(year, month, day, hr, min, sec), indexSizeDocs/1000000.0))
      endChart(w)

    # Refresh times
    startChart(w, 'refreshTimes', 'Time (msec) to refresh')

    headers = ['Date', 'RefreshMS']
    w('    "%s\\n" + \n"' % ','.join(headers))

    w('%s,0\\n' % formatTime(minTime[0], minTime[1], minTime[2], minTime[3], minTime[4], minTime[5]))
    for tup in getReaderTimes:
      year, month, day, hr, min, sec, ms = tup
      w('%s,%d\\n' % (formatTime(year, month, day, hr, min, sec), ms))

    endChart(w)

    if len(getReaderTimes) != 0:
      startChart(w, 'refreshRate', 'Refreshes in past 10 sec')

      headers = ['Date', 'RefreshRate']
      w('    "%s\\n" + \n"' % ','.join(headers))

      w('%s,0\\n' % formatTime(minTime[0], minTime[1], minTime[2], minTime[3], minTime[4], minTime[5]))

      startIndex = 0
      l = getReaderTimes[0]
      startTime = toDateTime(l[:-1])

      for i, tup in enumerate(getReaderTimes):
        t = toDateTime(tup[:-1])
        year, month, day, hr, min, sec = l[:-1]
        ms = tup[-1]
        # Rolling window of past 10 seconds:
        while t - startTime > TEN_SEC:
          startIndex += 1
          l = getReaderTimes[startIndex]
          startTime = toDateTime(l[:-1])

        w('%s,%d\\n' % (formatTime(year, month, day, hr, min, sec), i-startIndex+1))

      endChart(w)

    # Commit times
    startChart(w, 'commitTime', 'Time (sec) to commit')

    headers = ['Date', 'CommitSec']
    w('    "%s\\n" + \n"' % ','.join(headers))
    w('%s,0.0\\n' % formatTime(minTime[0], minTime[1], minTime[2], minTime[3], minTime[4], minTime[5]))

    for i, tup in enumerate(commitTimes):
      if len(tup) == 7:
        # print("tup %s" % str(tup))
        year, month, day, hr, min, sec, (endYear, endMonth, endDay, endHr, endMin, endSec) = tup
        t0 = toDateTime(tup[:6])
        t1 = toDateTime(tup[-1])
        commitSec = (t1-t0).total_seconds()
        # print('commitSec %s; %s %s' % (commitSec, t0, t1))
        w('%s,%g\\n' % (formatTime(year, month, day, hr, min, sec), commitSec))

    endChart(w)

    # Commit rate
    startChart(w, 'commitRate', 'Commits in past 60 sec')

    headers = ['Date', 'CommitRate']
    w('    "%s\\n" + \n"' % ','.join(headers))
    w('%s,0\\n' % formatTime(minTime[0], minTime[1], minTime[2], minTime[3], minTime[4], minTime[5]))

    startIndex = 0
    l = commitTimes[0]
    startTime = toDateTime(l[:-1])
    
    for i, tup in enumerate(commitTimes):
      if len(tup) >= 6:
        year, month, day, hr, min, sec = tup[:6]
        t = toDateTime(tup[:6])
        # Rolling window of past 60 seconds:
        while t - startTime > SIXTY_SEC:
          startIndex += 1
          l = commitTimes[startIndex]
          startTime = toDateTime(l[:6])

        w('%s,%d\\n' % (formatTime(year, month, day, hr, min, sec), i-startIndex+1))

    endChart(w)

    w('</table>')
    
    w('''
    </body>
    </html>
    ''')

globalChartCount = 0

def startChart(w, idName, title):
  global globalChartCount
  if globalChartCount % 3 == 0:
    if globalChartCount > 0:
      w('</tr>')
    w('<tr>')

  w('<td>')
  w('''
    <br><b>%s</b>
    <div id="%s" style="width:500px; height:300px"></div>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("%s"),
    ''' % (title, idName, idName))
  globalChartCount += 1

def endChart(w):
  w('''"\n,
  {
    axes: {
      x: {
        valueFormatter: Dygraph.dateString_,
        valueParser: parseInt,
        ticker: Dygraph.dateTicker,
        axisLabelFormatter: function(msec) {
        return new Date(msec).strftime('%H:%M:%S');
        }
      }
    }
  }
  );
  </script>
  </td>
  ''')
  
def toDateTime(tup):
  usec = int((tup[5] % 1) * 1000000)
  return datetime.datetime(year=tup[0], month=tup[1], day=tup[2],
                           hour=tup[3], minute=tup[4],
                           second=int(tup[5]),
                           microsecond=usec)

def utcShift():
  now = time.time()
  offset = datetime.datetime.fromtimestamp(now) - datetime.datetime.utcfromtimestamp(now)
  return offset

EPOCH = datetime.datetime(year=1970, month=1, day=1) + utcShift()

def formatTime(year, month, day, hr, min, sec):
  # return %04d-%02d-%02d %02d:%02d % (year, month, day, hr, min, int(sec)
  usec = int((sec % 1) * 1000000)
  t = datetime.datetime(year=year, month=month, day=day,
                        hour=hr, minute=min,
                        second=int(sec),
                        microsecond=usec)
  return int(1000.0 * (t-EPOCH).total_seconds())

  
TEN_SEC = datetime.timedelta(seconds=10.0)
SIXTY_SEC = datetime.timedelta(seconds=60.0)

if __name__ == '__main__':
  main()
