import datetime
import codecs
import sys
import time
import xml.etree.ElementTree as ET
import re
import random

# TODO
#  - put header on
#  - print how many lines

# Removes lines like [[wa:Finlande]]
#reOtherLangs = re.compile(r'^\[\[.*?:.*?\]\]$', re.MULTILINE)
utf8Encoder = codecs.getencoder('UTF8')

reCategory = re.compile(r'\[\[Category:(.*?)\]\]')
reFile = re.compile(r'\[\[File:(.*?)\]\]')
reSection = re.compile('^==([^=]*?)==$', re.MULTILINE)
reSubSection = re.compile('^===([^=]*?)===$', re.MULTILINE)
reSubSubSection = re.compile('^====([^=]*?)====$', re.MULTILINE)
reRef = re.compile('<ref>.*?</ref>')

def toUTF8(s):
  return utf8Encoder(s)[0]

def extractAttrs(text, attrs):
  attrs['characterCount'] = str(len(text))
  attrs['categories'] = '|'.join([x.replace('|', '').strip() for x in reCategory.findall(text)])
  attrs['imageCount'] = str(len(reFile.findall(text)))
  attrs['sectionCount'] = str(len(reSection.findall(text)))
  attrs['subSectionCount'] = str(len(reSubSection.findall(text)))
  attrs['subSubSectionCount'] = str(len(reSubSubSection.findall(text)))
  attrs['refCount'] = str(len(reRef.findall(text)))
  
class AllText:

  def __init__(self):
    self.allText = []

  def start(self, name):
    print
    print 'TEXT: %s' % name
    self.text = []
    self.name = name
    self.len = 0
    self.lastPrintTime = time.time()

  def finish(self):
    self.allText.append((self.name, self.len, self.text))

  def add(self, text):
    self.len += len(text)
    self.allText[-1][1].append(text)
    t = time.time()
    if t - self.lastPrintTime > 5.0:
      print '  %.1f MB...' % (self.len/1024./1024.)
      self.lastPrintTime = t

  def finish(self, mb):
    # even MB per section
    mbPerPart = float(mb)/len(self.allText)

reSpace = re.compile('\s+')

def clean(s):
  s = reSpace.sub(' ', s)
  s = s.replace('&lt;', '<')
  s = s.replace('&gt;', '>')
  s = s.replace('&amp;', '&')
  return s

def fixDate(s):
  if s.find('T') != -1:
    timestamp = s.replace('T', ' ').replace('Z', '')
    dateString, timeString = timestamp.split()
    year, month, day = [int(x) for x in dateString.split('-')]
    d = datetime.date(year=year, month=month, day=day)
    s = '%s %s' % (d.strftime('%d-%b-%Y'), timeString)
  return s

def convert(fIn, fOut):

  # get an iterable
  context = ET.iterparse(fIn, events=("start", "end"))

  # turn it into an iterator
  context = iter(context)

  # get the root element
  event, root = context.next()

  attrs = {}
  isRedirect = False
  count = 0

  ATTS = (
    'title',
    'timestamp',
    'text',
    'username', # who did the last revision
    'characterCount',
    'categories',
    'imageCount',
    'sectionCount',
    'subSectionCount',
    'subSubSectionCount',
    'refCount')

  fOut.write('FIELDS_HEADER_INDICATOR###	%s\n' % ('\t'.join(ATTS)))
  
  for evt, elem in context:

    idx = elem.tag.rfind('}')
    if idx == -1:
      raise RuntimeError('missing } in %s' % elem.tag)
    tag = elem.tag[idx+1:]

    if evt == 'end' and tag == 'page':
      if len(attrs) == 11 and not isRedirect:
        atts = get(attrs, *ATTS)
        fOut.write('\t'.join(atts))
        fOut.write('\n')
        if False:
          print '%s' % attrs['title']
          print '  username: %s' % attrs['username']
          print '  cats: %s' % attrs['categories']
          print '  charCount: %s' % attrs['characterCount']
          print '  imageCount: %s' % attrs['imageCount']
          print '  sections: %s' % attrs['sectionCount']
          print '  refs: %s' % attrs['refCount']
        count += 1
        if count % 100000 == 0:
          print '%d...' % count
        
      attrs = {}
      isRedirect = False

    if tag in ('timestamp', 'text', 'title', 'username'):
      if evt == 'end' and elem.text is not None:
        if tag == 'text':
          extractAttrs(elem.text, attrs)
        text = clean(elem.text)
        if len(text) > 0:
          if tag == 'timestamp':
            text = fixDate(text)
          attrs[tag] = text

    if tag == 'redirect':
      isRedirect = True

    # Else massive memory leak:
    root.clear()

  print '%d articles extracted' % count

def get(attrs, *names):
  return [toUTF8(attrs[x]) for x in names]

if __name__ == '__main__':
  f = open(sys.argv[1], 'rb')
  fOut = open(sys.argv[2], 'wb')
  convert(f, fOut)
  f.close()
  fOut.close()
  
