import sys
import re
import datetime

# TODO
#   - parse date too
#   - flush sizes/frequency

reTime = re.compile('(\d\d):(\d\d):(\d\d)')
reFindMerges = re.compile('findMerges: (\d+) segments')
reMergeSize = re.compile(r'[cC](\d+) size=(.*?) MB')
reMergeStart = re.compile(r'^IW \d+ \[.*?; (.*?)\]: merge seg=(.*?)')
reMergeEnd = re.compile(r'^IW \d+ \[.*?; (.*?)\]: merged segment size=(.*?) MB')

def parseTime(line):
  m = reTime.search(line)
  if m is None:
    return None
  
  # hours, minutes, seconds:
  return [int(x) for x in m.groups()]

def main():
  segCounts = []
  merges = []

  inFindMerges = False
  maxSegs = 0
  maxRunningMerges = 0
  runningMerges = 0
  mergeThreads = {}
  commitCount = 0
  flushCount = 0
  minTime = None
  maxTime = None
  
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

      if line.find('flush postings as segment') != -1:
        flushCount += 1
        
      m = reMergeStart.match(line)
      if m is not None:
        # A merge kicked off
        threadName = m.group(1)
        mergeThreads[threadName] = len(merges)
        merges.append(['start', threadName] + parseTime(line))
        runningMerges += 1
        maxRunningMerges = max(maxRunningMerges, runningMerges)

      m = reMergeEnd.match(line)
      if m is not None:
        # A merge finished
        threadName = m.group(1)
        mergeSize = float(m.group(2))
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
                         second=minTime[2])
  t1 = datetime.datetime(year=now.year, month=now.month, day=now.day,
                         hour=maxTime[0], minute=maxTime[1],
                         second=minTime[2])
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


    # Merging MB
    w('''
    <td><br><b>Merging MB</b>
    <div id="mergingMB" style="width:500px; height:300px"></div>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("mergingMB"),
    ''')

    headers = ['Date', 'MergingMB']
    w('    "%s\\n" + \n"' % ','.join(headers))

    for tup in segCounts:
      if len(tup) >= 5:
        hr, min, sec, count, mergeMB = tup[:5]
        w('2014-04-22 %02d:%02d:%02d,%.1f\\n' % (hr, min, sec, mergeMB))

    w('"\n')
    w('''
    );
    </script>
    </td>
    ''')


    # Index size MB
    w('''
    <td><br><b>Index Size MB</b>
    <div id="indexSizeMB" style="width:500px; height:300px"></div>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("indexSizeMB"),
    ''')

    headers = ['Date', 'IndexSizeMB']
    w('    "%s\\n" + \n"' % ','.join(headers))

    for tup in segCounts:
      if len(tup) >= 6:
        hr, min, sec, count, mergeMB, indexSizeMB = tup[:6]
        w('2014-04-22 %02d:%02d:%02d,%.1f\\n' % (hr, min, sec, indexSizeMB))

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
    <td><br><b>Index Size Docs</b>
    <div id="indexSizeDocs" style="width:500px; height:300px"></div>
    <script type="text/javascript">
      g = new Dygraph(

        // containing div
        document.getElementById("indexSizeDocs"),
    ''')

    headers = ['Date', 'IndexSizeDocs']
    w('    "%s\\n" + \n"' % ','.join(headers))

    for tup in segCounts:
      if len(tup) >= 7:
        hr, min, sec, count, mergeMB, indexSizeMB, indexSizeDocs = tup[:7]
        w('2014-04-22 %02d:%02d:%02d,%d\\n' % (hr, min, sec, indexSizeDocs))

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
