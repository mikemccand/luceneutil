import re
import os

# TODO: need annot when hash changes

def renameAnalyzers(l):
  l2 = []
  for x in l:
    if x == 'WordDelimiterFilter':
      x = 'WDF'
    l2.append(x)
  return l2

reResult = re.compile('^(.*?) time=(.*?) msec hash=(.*?) tokens=(.*?)$')

reYMD = re.compile(r'^(\d\d\d\d)-(\d\d)-(\d\d)\.log$')
rootDir = '/l/logs/analyzers'
allResults = []
analyzers = set()
for fileName in os.listdir(rootDir):
  m = reYMD.match(fileName)
  if m is not None:
    year = int(m.group(1))
    month = int(m.group(2))
    day = int(m.group(3))
    with open('%s/%s' % (rootDir, fileName)) as f:
      svnRev = f.readline().strip()

      results = {'rev': svnRev}
      
      for line in f.readlines():
        m = reResult.match(line)
        if m is not None:
          analyzer = m.group(1)
          # tot msec, hash, token count
          results[analyzer] = float(m.group(2)), int(m.group(3)), int(m.group(4))

      if len(results) == 6:
        print("keep %s" % str(results))
        for key in results.keys():
          if key != 'rev':
            analyzers.add(key)
        allResults.append((year, month, day, results))
      
# Sort by date:
allResults.sort()

with open('analyzers.html', 'w') as f:

  f.write('''
  <html>
  <head>
  <script type="text/javascript"
    src="dygraph-combined.js"></script>
  </head>
  <body>
  <div id="chart" style="width:800px; height:500px"></div>
  <script type="text/javascript">
    g = new Dygraph(

      // containing div
      document.getElementById("chart"),
  ''')

  headers = ['Date'] + list(analyzers)
  f.write('    "%s\\n"\n' % ','.join(renameAnalyzers(headers)))

  for year, month, day, results in allResults:
    f.write('    + "%4d-%02d-%02d' % (year, month, day))
    for analyzer in headers[1:]:
      if analyzer in results:
        totMS, hash, tokenCount = results[analyzer]
        mTokPerSec = tokenCount / totMS / 1000.0
        f.write(',%.1f' % mTokPerSec)
      else:
        f.write(',')
    f.write('\\n"\n')

  f.write(''',
  { "title": "Analyzer performance over time",
    "xlabel": "Date",
    "ylabel": "Million Tokens/sec"}
    );
  </script>
  </body>
  </html>
  ''')
