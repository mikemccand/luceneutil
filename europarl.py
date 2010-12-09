import sys
import glob
import datetime
import tarfile
import re

try:
  sys.argv.remove('-verbose')
  VERBOSE = True
except ValueError:
  VERBOSE = False

try:
  sys.argv.remove('-docPerParagraph')
  docPerParagraph = True
except ValueError:
  docPerParagraph = False

reChapterOnly = re.compile('^<CHAPTER ID=.*?>$')
reTagOnly = re.compile('^<.*?>$')
reNumberOnly = re.compile(r'^\d+\.?$')

docCount = 0

def write(date, title, pending, fOut):
  global docCount
  body = ' '.join(pending).replace('\t', ' ').strip()
  if len(body) > 0:
    line = '%s\t%s\t%s\n' % (title, date, body)
    fOut.write(line)
    docCount += 1
    del pending[:]
    if VERBOSE:
      print len(body)

def processTar(fileName, fOut):

  t = tarfile.open(fileName, 'r:gz')
  for ti in t:
    if ti.isfile():

      tup = ti.name.split('/')
      lang = tup[1]
      year = int(tup[2][3:5])
      if year < 20:
        year += 2000
      else:
        year += 1900

      month = int(tup[2][6:8])
      day = int(tup[2][9:11])
      date = datetime.date(year=year, month=month, day=day)

      if VERBOSE:
        print
        print '%s: %s' % (ti.name, date)
      nextIsTitle = False
      title = None
      pending = []
      for line in t.extractfile(ti).readlines():
        line = line.strip()
        if reChapterOnly.match(line) is not None:
          if title is not None:
            write(date, title, pending, fOut)
          nextIsTitle = True
          continue
        if nextIsTitle:
          if not reNumberOnly.match(line) and not reTagOnly.match(line):
            title = line
            nextIsTitle = False
            if VERBOSE:
              print '  title %s' % line
          continue
        if line.lower() == '<p>':
          if docPerParagraph:
            write(date, title, pending, fOut)
          else:
            pending.append('PARSEP')
        elif not reTagOnly.match(line):
          pending.append(line)
      if title is not None and len(pending) > 0:
        write(date, title, pending, fOut)


# '/x/lucene/data/europarl/all.lines.txt'
dirIn = sys.argv[1]
fileOut = sys.argv[2]
  
fOut = open(fileOut, 'wb')

for fileName in glob.glob('%s/??-??.tgz' % dirIn):
  if fileName.endswith('.tgz'):
    print 'process %s; %d docs so far...' % (fileName, docCount)
    processTar(fileName, fOut)

print 'TOTAL: %s' % docCount

#run something like this:
"""

# Europarl V5 makes 140,190 docs, avg 37.0 KB per
python -u europarl.py /x/lucene/data/europarl /x/lucene/data/europarl/tmp.lines.txt
shuf /x/lucene/data/europarl/tmp.lines.txt > /x/lucene/data/europarl/full.lines.txt
rm /x/lucene/data/europarl/tmp.lines.txt

# ~5 MB gzip'd
head -392 /x/lucene/data/europarl/full.lines.txt > /x/lucene/data/europarl/full.subset.lines.txt
gzip --best /x/lucene/data/europarl/full.subset.lines.txt


# Run again, this time each paragraph is a doc:

# Europarl V5 makes 10,139,605 paragraphs (one paragraph per line), avg 587 bytes per:
python -u europarl.py /x/lucene/data/europarl /x/lucene/data/europarl/tmp.lines.txt -docPerParagraph
shuf /x/lucene/data/europarl/tmp.lines.txt > /x/lucene/data/europarl/para.lines.txt
rm /x/lucene/data/europarl/tmp.lines.txt

# ~5 MB gzip'd
head -21000 /x/lucene/data/europarl/para.lines.txt > /x/lucene/data/europarl/para.subset.lines.txt
gzip --best /x/lucene/data/europarl/para.subset.lines.txt

"""
