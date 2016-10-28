import re
import os
import constants

# TODO:
#   - click to see log file from that day's run

LOGS_ROOT = os.path.join(constants.LOGS_DIR, 'analyzers')

KNOWN_CHANGES = {
  # Known behavior changes
  (2014, 2, 19, 'Standard'): 'LUCENE-5447: StandardAnalyzer should split on certain punctuation',
  (2014, 2, 20, 'Standard'): 'LUCENE-5447: StandardAnalyzer should split on certain punctuation',
  (2013, 12, 7, 'Standard'): 'LUCENE-5357: Upgrade StandardTokenizer to Unicode 6.3',
  
  # Known perf changes
  (2014, 3, 19, 'WordDelimiterFilter'): 'LUCENE-5111: Fix WordDelimiterFilter offsets',

  (2014, 5, 4, 'Standard'): 'LUCENE-5638: pack the core attributes into one impl by default',
  (2014, 5, 4, 'Shingles'): 'LUCENE-5638: pack the core attributes into one impl by default',
  (2014, 5, 4, 'EdgeNGrams'): 'LUCENE-5638: pack the core attributes into one impl by default',
  (2014, 5, 4, 'Lowercase'): 'LUCENE-5638: pack the core attributes into one impl by default',
  }
  

def renameAnalyzer(x):
  if x == 'WordDelimiterFilter':
    x = 'WDF'
  return x

def getLabel(label):
  if label < 26:
    s = chr(65+label)
  else:
    s = '%s%s' % (chr(65+(label/26 - 1)), chr(65 + (label%26)))
  return s

print
print('run sumAnalyzerPerf.py')
reResult = re.compile('^(.*?) time=(.*?) msec hash=(.*?) tokens=(.*?)$')

reYMD = re.compile(r'^(\d\d\d\d)-(\d\d)-(\d\d)\.log$')
allResults = []
analyzers = set()
for fileName in os.listdir(LOGS_ROOT):
  m = reYMD.match(fileName)
  if m is not None:
    year = int(m.group(1))
    month = int(m.group(2))
    day = int(m.group(3))
    with open('%s/%s' % (LOGS_ROOT, fileName)) as f:
      svnRev = f.readline().strip()

      results = {'rev': svnRev}
      
      for line in f.readlines():
        m = reResult.match(line)
        if m is not None:
          analyzer = m.group(1)
          if analyzer != 'WordDelimiterFilter':
            # tot msec, hash, token count
            results[analyzer] = float(m.group(2)), int(m.group(3)), int(m.group(4))

      if len(results) == 5:
        #print("keep %s" % str(results))
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
    <style type="text/css">
    #chart {
      position: absolute;
      left: 10px;
      top: 5%;
    }
    </style>
  </head>
  <body>
  <div id="chart" style="height:70%; width:98%"></div>
  <script type="text/javascript">
    g = new Dygraph(

      // containing div
      document.getElementById("chart"),
  ''')

  headers = ['Date'] + list(analyzers)
  f.write('    "%s\\n"\n' % ','.join([renameAnalyzer(x) for x in headers]))

  lastHash = {}

  annots = []
  for year, month, day, results in allResults:
    f.write('    + "%4d-%02d-%02d' % (year, month, day))
    for analyzer in headers[1:]:
      if analyzer in results:
        totMS, hash, tokenCount = results[analyzer]
        oldHash = lastHash.get(analyzer)
        lastHash[analyzer] = hash
        if oldHash is not None and oldHash != hash:
          # Record that analyzer changed:
          annots.append((year, month, day, analyzer))
          
        mTokPerSec = tokenCount / totMS / 1000.0
        f.write(',%.1f' % mTokPerSec)
      else:
        f.write(',')
    f.write('\\n"\n')

  f.write(''',
  { "title": "Analyzer performance over time in Lucene trunk",
    "xlabel": "Date",
    "ylabel": "Million Tokens/sec"}
    );
    ''')

  if len(annots) > 0:
    f.write('g.ready(function() {g.setAnnotations([')
    label = 0
    for tup in annots:
      reason = KNOWN_CHANGES.get(tup)
      if reason is None:
        shortText = '?'
        reason = '%s: Analyzer changed its behavior for unknown reasons' % tup[3]
      else:
        del KNOWN_CHANGES[tup]
        shortText = getLabel(label)
        label += 1
      year, month, day, analyzer = tup
      f.write('{series: "%s", x: "%04d-%02d-%02d", shortText: "%s", text: "%s"},' % (analyzer, year, month, day, shortText, reason))

    for tup, reason in KNOWN_CHANGES.items():
      year, month, day, analyzer = tup
      shortText = getLabel(label)
      f.write('{series: "%s", x: "%04d-%02d-%02d", shortText: "%s", text: "%s"},' % (renameAnalyzer(analyzer), year, month, day, shortText, reason))

    f.write(']);});')

  f.write('''    
  </script>
  </body>
  </html>
  ''')

import pysftp
with pysftp.Connection('home.apache.org', username='mikemccand') as c:
  with c.cd('public_html/lucenebench'):
    c.put('analyzers.html', 'analyzers.html')
