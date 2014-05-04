import sys
import re
import datetime
import math

# see http://people.apache.org/~mikemccand/lucenebench/iw.html as an example

# TODO
#   - parse date too
#   - separate "stalled merge MB" from running
#   - flush sizes/frequency
#   - commit frequency

reTime = re.compile('(\d\d):(\d\d):(\d\d)(,\d\d\d)?')
reFindMerges = re.compile('findMerges: (\d+) segments')
reMergeSize = re.compile(r'[cC](\d+) size=(.*?) MB')
reMergeStart = re.compile(r'merge seg=(.*?) ')
reMergeEnd = re.compile(r'merged segment size=(.*?) MB')
reGetReader = re.compile(r'getReader took (\d+) msec')
reThreadName = re.compile(r'^IW \d+ \[.*?; (.*?)\]:')
reThreadNameES = re.compile(r'[lucene.iw\s*].*? elasticsearch\[.*?\](\[.*?\]\[.*?\]) IW')

def parseTime(line):
  m = reTime.search(line)
  if m is None:
    return None
  
  # hours, minutes, seconds:
  t = list(m.groups())
  if len(t) == 4:
    if t[-1] is None:
      t = [int(x) for x in t[:3]]
    else:
      t2 = [int(x) for x in t[:3]]
      # ES logs have msec:
      t2[-1] += float(t[3][1:])/1000.0
      t = t2
  else:
    t = [int(x) for x in t[:3]]
    
  return t

def parseThreadName(line):
  m = reThreadName.search(line)
  if m is not None:
    threadName = m.group(1)
  else:
    return reThreadNameES.search(line).group(1)

def main():
  segCounts = []
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
  
  with open(sys.argv[1]) as f:
    for line in f.readlines():
      line = line.strip()

      t = parseTime(line)
      if t is not None:
        if minTime is None:
          minTime = t
        maxTime = t
        
      if line.find('startCommit(): start') != -1:
        commitCount += 1
        threadName = parseThreadName(line)
        runningCommits[threadName] = len(commitTimes)
        commitTimes.append(t)

      if line.find('commit: wrote segments file') != -1:
        threadName = parseThreadName(line)
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

      m = reGetReader.search(line)
      if m is not None:
        getReaderTimes.append(t + [int(m.group(1))])
        
      m = reMergeStart.search(line)
      if m is not None:
        # A merge kicked off
        threadName = parseThreadName(line)
        mergeThreads[threadName] = len(merges)
        merges.append(['start', threadName] + parseTime(line))
        runningMerges += 1
        maxRunningMerges = max(maxRunningMerges, runningMerges)

      m = reMergeEnd.search(line)
      if m is not None:
        # A merge finished
        threadName = parseThreadName(line)
        mergeSize = float(m.group(1))
        merges[mergeThreads[threadName]].append(mergeSize)
        del mergeThreads[threadName]
        merges.append(['end', threadName] + parseTime(line) + [mergeSize])
        runningMerges -= 1
        
      m = reFindMerges.search(line)
      #if m is not None and line.find('[es1][bulk]') != -1:
      if m is not None:
        segCount = int(m.group(1))
        # hack to not include marvel index; once we get .setInfoStream
        # properly integrated we can differentiate the IW instances:
        if segCount >= maxSegs/2.5:
          inFindMerges = True
          mergeMB = 0.0
          indexSizeMB = 0.0
          indexSizeDocs = 0.0
          maxSegs = max(maxSegs, segCount)
          segCounts.append(list(parseTime(line)) + [segCount])
          #print('segCount %s' % (segCounts[-1]))
          if segCounts[-1][:3] == [14, 59, 15]:
            doP = True
      elif inFindMerges:
        m = reMergeSize.search(line)
        if m is not None:
          sizeDocs = int(m.group(1))
          sizeMB = float(m.group(2))
          indexSizeMB += sizeMB
          indexSizeDocs += sizeDocs
          if line.find(' [merging]') != -1:
            mergeMB += sizeMB
        elif line.find('allowedSegmentCount=') != -1:
          inFindMerges = False
          segCounts[-1].extend([mergeMB, indexSizeMB, indexSizeDocs])

  now = datetime.datetime.now()
  t0 = datetime.datetime(year=now.year, month=now.month, day=now.day,
                         hour=minTime[0], minute=minTime[1],
                         second=int(minTime[2]))
  t1 = datetime.datetime(year=now.year, month=now.month, day=now.day,
                         hour=maxTime[0], minute=maxTime[1],
                         second=int(minTime[2]))
  print('elapsed time %s' % (t1-t0))
  print('max concurrent merges %s' % maxRunningMerges)
  print('commit count %s (avg every %.1f sec)' % \
        (commitCount, (t1-t0).total_seconds()/commitCount))
  print('flush count %s (avg every %.1f sec)' % \
        (flushCount, (t1-t0).total_seconds()/flushCount))
  
  with open('iw.html', 'w') as f:

    w = f.write

    w('''
    <html>
    <head>
    <script type="text/javascript"
      src="dygraph-combined.js"></script>
    </head>
    <body>
    ''')

    w('<table><tr>')

    # Segment counts
    w('''
    <td><b>Seg counts</b>
    <div id="segCounts" style="width:500px; height:300px"></div>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("segCounts"),
    ''')

    headers = ['Date', 'SegCount']
    w('    "%s\\n" + \n"' % ','.join(headers))

    for tup in segCounts:
      if len(tup) >= 4:
        hr, min, sec, count = tup[:4]
        w('2014-04-22 %02d:%02d:%02d,%d\\n' % (hr, min, sec, count))
        #w(',%d,%.1f' % (count, mergeMB/1024.))

    w('"\n')
    w('''
      );
    </script>
    </td>
    ''')


    # Merging GB
    w('''
    <td><br><b>Merging GB</b>
    <div id="mergingGB" style="width:500px; height:300px"></div>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("mergingGB"),
    ''')

    headers = ['Date', 'MergingGB']
    w('    "%s\\n" + \n"' % ','.join(headers))

    for tup in segCounts:
      if len(tup) >= 5:
        hr, min, sec, count, mergeMB = tup[:5]
        w('2014-04-22 %02d:%02d:%02d,%.2f\\n' % (hr, min, sec, mergeMB/1024.))

    w('"\n')
    w('''
    );
    </script>
    </td>
    ''')


    # Index size GB
    w('''
    <td><br><b>Index Size GB</b>
    <div id="indexSizeGB" style="width:500px; height:300px"></div>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("indexSizeGB"),
    ''')

    headers = ['Date', 'IndexSizeGB']
    w('    "%s\\n" + \n"' % ','.join(headers))

    for tup in segCounts:
      if len(tup) >= 6:
        hr, min, sec, count, mergeMB, indexSizeMB = tup[:6]
        w('2014-04-22 %02d:%02d:%02d,%.2f\\n' % (hr, min, sec, indexSizeMB/1024.))

    w('"\n')
    w('''
    );
    </script>
    </td>
    ''')

    w('</tr>')
    w('<tr>')

    # Running merges
    w('''
    <td><br><b>Running merges</b>
    <div id="runningMerges" style="width:500px; height:300px"></div>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("runningMerges"),
    ''')

    headers = ['Date'] + ['Merge%s' % x for x in range(maxRunningMerges)]
    w('    "%s\\n" + \n"' % ','.join(headers))

    # Thread name -> id, size
    runningMerges = {}

    ids = set(range(maxRunningMerges))
    for tup in merges:
      if len(tup) == 6:
        event, threadName, hr, min, sec, mergeMB = tup
        #if mergeMB > 0:
        #  mergeMB = math.log(mergeMB)/math.log(2.0)
        
        l = ['0'] * maxRunningMerges
        for ign, (id, size) in runningMerges.items():
          l[id] = '%.1f' % size
        w('2014-04-22 %02d:%02d:%02d,%s\\n' % (hr, min, sec, ','.join(l)))

        if event == 'end':
          ids.add(runningMerges[threadName][0])
          del runningMerges[threadName]
        else:
          id = ids.pop()
          runningMerges[threadName] = id, mergeMB

        l = ['0'] * maxRunningMerges
        for ign, (id, size) in runningMerges.items():
          l[id] = '%.1f' % size
        w('2014-04-22 %02d:%02d:%02d,%s\\n' % (hr, min, sec, ','.join(l)))

    w('"\n')
    w('''
    );
    </script>
    </td>
    ''')

    # Index size Docs
    w('''
    <td><br><b>Index Size MDocs</b>
    <div id="indexSizeMDocs" style="width:500px; height:300px"></div>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("indexSizeMDocs"),
    ''')

    headers = ['Date', 'IndexSizeMDocs']
    w('    "%s\\n" + \n"' % ','.join(headers))

    for tup in segCounts:
      if len(tup) >= 7:
        hr, min, sec, count, mergeMB, indexSizeMB, indexSizeDocs = tup[:7]
        w('2014-04-22 %02d:%02d:%02d,%.2f\\n' % (hr, min, sec, indexSizeDocs/1000000.0))

    w('"\n')
    w('''
    );
    </script>
    </td>
    ''')


    # Segs per full flush
    w('''
    <td><br><b>Segments per full flush (client concurrency)</b>
    <div id="segsFullFlush" style="width:500px; height:300px"></div>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("segsFullFlush"),
    ''')

    headers = ['Date', 'SegsFullFlush']
    w('    "%s\\n" + \n"' % ','.join(headers))

    for tup in segsPerFullFlush:
      hr, min, sec, count = tup
      w('2014-04-22 %02d:%02d:%02d,%d\\n' % (hr, min, sec, count))

    w('"\n')
    w('''
    );
    </script>
    </td>
    ''')

    w('</tr>')
    w('<tr>')

    # Refresh times
    w('''
    <td><br><b>MS to refresh</b>
    <div id="refreshTimes" style="width:500px; height:300px"></div>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("refreshTimes"),
    ''')

    headers = ['Date', 'RefreshMS']
    w('    "%s\\n" + \n"' % ','.join(headers))

    for tup in getReaderTimes:
      hr, min, sec, ms = tup
      w('2014-04-22 %02d:%02d:%02d,%d\\n' % (hr, min, sec, ms))

    w('"\n')
    w('''
    );
    </script>
    </td>
    ''')


    # Refresh rate
    w('''
    <td><br><b>Refreshes in past 10 sec</b>
    <div id="refreshRate" style="width:500px; height:300px"></div>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("refreshRate"),
    ''')

    headers = ['Date', 'RefreshRate']
    w('    "%s\\n" + \n"' % ','.join(headers))

    startIndex = 0
    l = getReaderTimes[0]
    startTime = l[0]*3600 + l[1]*60 + l[2]
    
    for i, tup in enumerate(getReaderTimes):
      hr, min, sec, ms = tup
      t = hr*3600 + min*60 + sec
      # Rolling window of past 10 seconds:
      while t - startTime > 10.0:
        startIndex += 1
        l = getReaderTimes[startIndex]
        startTime = l[0]*3600 + l[1]*60 + l[2]
      
      w('2014-04-22 %02d:%02d:%02d,%d\\n' % (hr, min, sec, i-startIndex+1))

    w('"\n')
    w('''
    );
    </script>
    </td>
    ''')


    # Commit times
    w('''
    <td><br><b>Time to commit</b>
    <div id="commitTime" style="width:500px; height:300px"></div>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("commitTime"),
    ''')

    headers = ['Date', 'CommitSec']
    w('    "%s\\n" + \n"' % ','.join(headers))

    for i, tup in enumerate(commitTimes):
      if len(tup) == 4:
        hr, min, sec, (endHr, endMin, endSec) = tup
        commitSec = (endHr-hr)*3600 + (endMin-min)*60 + endSec-sec
        w('2014-04-22 %02d:%02d:%02d,%g\\n' % (hr, min, sec, commitSec))

    w('"\n')
    w('''
    );
    </script>
    </td>
    ''')

    w('</tr>')
    w('<tr>')

    # Commit rate
    w('''
    <td><br><b>Commits in past 60 sec</b>
    <div id="commitRate" style="width:500px; height:300px"></div>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("commitRate"),
    ''')

    headers = ['Date', 'CommitRate']
    w('    "%s\\n" + \n"' % ','.join(headers))

    startIndex = 0
    l = commitTimes[0]
    startTime = l[0]*3600 + l[1]*60 + l[2]
    
    for i, tup in enumerate(commitTimes):
      if len(tup) >= 3:
        hr, min, sec = tup[:3]
        t = hr*3600 + min*60 + sec
        # Rolling window of past 60 seconds:
        while t - startTime > 60.0:
          startIndex += 1
          l = commitTimes[startIndex]
          startTime = l[0]*3600 + l[1]*60 + l[2]

        w('2014-04-22 %02d:%02d:%02d,%d\\n' % (hr, min, sec, i-startIndex+1))

    w('"\n')
    w('''
    );
    </script>
    </td>
    ''')

    w('</tr>')
    w('</table>')
    
    w('''
    </body>
    </html>
    ''')
  
if __name__ == '__main__':
  main()
