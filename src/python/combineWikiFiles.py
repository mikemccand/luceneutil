import sys
import re

# Takes plain text output from WikipediaExtractor, and a previously
# created line file, and just replaces body text with the clean
# one...

onlyThreeColumns = '-only-three-columns' in sys.argv

if onlyThreeColumns:
  sys.argv.remove('-only-three-columns')

# Line file docs:
f1 = open(sys.argv[1], 'r')
line = f1.readline()

# WikipediaExtractor output:
f2 = open(sys.argv[2], 'r')

fOut = open(sys.argv[3], 'w')

if onlyThreeColumns:
  tup = line.strip().split('\t')
  tup = tup[:3]
  line = '\t'.join(tup) + '\n'

fOut.write(line)

reTitle = re.compile('title="(.*?)">$')
reHTMLEscape = re.compile('&.*?;')

lineCount = 0

while True:
  l1 = f1.readline()
  if l1 == '':
    break

  tup = l1.strip().split('\t')
  #print tup[0]
  
  l2 = f2.readline().strip()
  if not l2.startswith('<doc id'):
    raise RuntimeError('unexpected line: %s' % l2)

  m = reTitle.search(l2)
  if m is None:
    raise RuntimeError('could not find title: %s' % l2)

  #tup[0] = WikipediaExtractor.unescape(tup[0])
  title = m.group(1)
  
  if title != tup[0]:
    raise RuntimeError('line %d: title mismatch: %s vs %s' % (lineCount, title, tup[0]))

  lineCount += 1

  l = []
  while True:
    l2 = f2.readline()
    if l2.startswith('</doc>'):
      break
    l.append(l2.strip())

  text = ' '.join(l)

  tup[0] = title
  oldText = tup[2]
  tup[2] = text

  if tup[0].startswith('Category:'):
    continue
  if tup[0].startswith('Wikipedia:'):
    continue
  if tup[0].startswith('Template:'):
    continue
  if tup[0].startswith('File:'):
    continue
  if tup[0].startswith('Book:'):
    continue
  if tup[0].startswith('MediaWiki:'):
    continue
  if tup[0].startswith('Portal:'):
    continue
  if tup[0].startswith('Help:'):
    continue
  lowerText = oldText.lower()
  if lowerText.find('{{disambig') != -1 or \
    lowerText.find('{{disambiguation') != -1 or \
    lowerText.find('{{dab') != -1 or \
    lowerText.find('{{hndis') != -1:
    #print 'SKIP: %s' % oldText
    continue
  if tup[0].find('(disambiguation)') != -1:
    continue

  PARAGRAPH_SEP = u'\u2029'
  #print 'titletype %s' % type(title)
  #print 'texttype %s' % type(text)
  prefix = title + ' ' + PARAGRAPH_SEP
  if text.startswith(prefix):
    #print 'strip...'
    text = text[len(prefix):].strip()
    tup[2] = text

  if onlyThreeColumns:
    tup = tup[:3]
    
  fOut.write('\t'.join(tup))
  fOut.write('\n')

f1.close()
f2.close()
fOut.close()
