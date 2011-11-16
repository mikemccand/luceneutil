# Copyright (c) 2011 Michael McCandless. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import os
import sys
import time
import codecs
import subprocess
import cld
import unicodedata
import cPickle

# Cool Python language detection lib: https://github.com/saffsd/langid.py
import langid

ROOT = '/lucene/util/langdetect'
TIKA_ROOT = '/lucene/tika.clean'
LD_ROOT = '/home/mike/src/langdetect'

# TODO
#   - maybe also eval https://github.com/vcl/cue.language

# Download corpus at http://code.google.com/p/language-detection/downloads/detail?name=europarl-test.zip

# For a description of the corpus see http://shuyo.wordpress.com/2011/09/29/langdetect-is-updatedadded-profiles-of-estonian-lithuanian-latvian-slovene-and-so-on/

class DetectTika:
  TIKA_COMMAND = 'java -cp %s:%s/tika-app/target/tika-app-1.0-SNAPSHOT.jar TikaDetectLanguageEmbedded' % (ROOT, TIKA_ROOT)
  name = 'Tika'
  def __init__(self):
    if os.system('javac -cp %s/tika-app/target/tika-app-1.0-SNAPSHOT.jar %s/TikaDetectLanguageEmbedded.java' % (TIKA_ROOT, ROOT)):
      raise RuntimeError('compile failed')

    # talk to Tika via pipes
    self.tika = subprocess.Popen(self.TIKA_COMMAND,
                                 shell=True,
                                 stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE)

  def detect(self, utf8):
    self.tika.stdin.write('%7d' % len(utf8))
    self.tika.stdin.write(utf8)
    size = int(self.tika.stdout.read(7).strip())
    lang, reliable = self.tika.stdout.read(size).split(' ')
    reliable = reliable == 'true'
    return lang, reliable

class DetectCLD:
  name = 'CLD'

  allLangs = set()

  def detect(self, utf8):
    name, code, reliable, numBytes, details = cld.detect(utf8, isPlainText=True, removeWeakMatches=False, pickSummaryLanguage=False)
    for tup in details:
      self.allLangs.add(tup[0])
    return code, reliable

class DetectPyLangID:
  name = 'PyLangID'

  def detect(self, utf8):
    #lang, conf = langid.classify(codecs.getdecoder('UTF-8')(utf8)[0])
    lang, conf = langid.classify(utf8)
    # nocommit conf to bool
    return lang, conf

class DetectLD:
  name = 'LangDetect'
  
  LD_COMMAND = 'java -cp %s/lib/langdetect.jar:%s/lib/jsonic-1.2.0.jar:%s LDDetectEmbedded' % (LD_ROOT, LD_ROOT, ROOT)
  def __init__(self):
    if os.system('javac -cp %s/lib/langdetect.jar %s/LDDetectEmbedded.java' % (LD_ROOT, ROOT)):
      raise RuntimeError('compile failed')

    # talk to Tika via pipes
    self.ld = subprocess.Popen(self.LD_COMMAND,
                               shell=True,
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE)

  def detect(self, utf8):
    self.ld.stdin.write('%7d' % len(utf8))
    self.ld.stdin.write(utf8)
    size = int(self.ld.stdout.read(7).strip())
    lang = self.ld.stdout.read(size)
    reliable = True
    return lang, reliable

def loadTestData(corpusFile):
  f = open(corpusFile)
  testData = []
  bytes = 0
  for line in f.readlines():
    line = line.strip()
    answer, text = line.split('\t')
    if False and answer in ('bg', 'cs', 'lv'):
      continue
    bytes += len(text)
    if 0:
      # This corpus never differs
      uText = codecs.getdecoder('UTF-8')(text)[0]
      text2 = unicodedata.normalize('NFC', uText)
      text2 = codecs.getencoder('UTF-8')(text2)[0]
      if text2 != text:
        print 'DIFFS'
    testData.append((answer, text))
  return testData, bytes

def accTest():
  if len(sys.argv) == 1:
    corpusFile = '%s/europarl.17.test' % ROOT
  else:
    corpusFile = sys.argv[1]
    
  testData, bytes = loadTestData(corpusFile)

  if False:
    f = open('/lucene/util/europarl.18.test', 'wb')
    for answer, text in testData:
      f.write('%s\t%s\n' % (answer, text))
    f.close()

  print '%d texts; total %d bytes' % (len(testData), bytes)

  resultsFile = '%s/results.%s.pk' % (ROOT, os.path.split(corpusFile)[1])

  if not os.path.exists(resultsFile):

    tika = DetectTika()
    cld = DetectCLD()
    ld = DetectLD()
    pylid = DetectPyLangID()

    allResults = {}
    for detector in (pylid, cld, ld, tika):
    # for detector in (cld, ld):
      print '  run %s...' % detector.name
      results = allResults[detector.name] = []
      startTime = time.time()
      for answer, text in testData:
        results.append(detector.detect(text))
      print '    %.3f sec' % (time.time()-startTime)
      if False and detector == cld:
        l = list(cld.allLangs)
        l.sort()
        print '%d langs: %s' % (len(l), l)
    print 'Save all results to %s...' % resultsFile
    f = open(resultsFile, 'wb')
    cPickle.dump(allResults, f)
    f.close()
  else:
    print 'Loading previous results %s...' % resultsFile
    allResults = cPickle.load(open(resultsFile, 'rb'))
    
  processResults(testData, allResults)

def perfTest():
  # NOTE: not really fair because cld is embedded in python but the others communicate through pipes
  testData, totBytes = loadTestData(sys.argv[1])

  tika = DetectTika()
  cld = DetectCLD()
  ld = DetectLD()
  pylid = DetectPYLangID()

  for detector in (pylid, cld, ld, tika):
    print '  run %s...' % detector.name
    best = None
    for iter in xrange(10):
      print '    iter %s...' % iter
      tStart = time.time()
      for answer, text in testData:
        detector.detect(text)
      elapsedSec = time.time() - tStart
      print '      %.3f sec' % elapsedSec
      if best is None or elapsedSec < best:
        print '      **'
        best = elapsedSec
    print '    best %.1f sec %.3f MB/sec' % (best, totBytes/1024./1024./best)

def processResults(testData, allResults):

  byDetector = {}
  for name in allResults.keys():
    byDetector[name] = {}
  byDetector['majority'] = {}

  cldAlsoWrong = 0
  tikaAlsoWrong = 0
  ldWrongCount = 0
  count = 0
  minLen = None
  for idx in xrange(len(testData)):
    answer, text = testData[idx]
    if minLen is None or len(text) < minLen:
      minLen = len(text)
    if False and len(text) <= 30:
      continue
    count += 1
    if False:
      print '%s: %d bytes' % (answer, len(text))
    diffs = []
    cldWrong = False
    ldWrong = False
    tikaWrong = False
    tally = {}
    for name, results in allResults.items():
      byLang = byDetector[name]
      if answer not in byLang:
        byLang[answer] = {}
      hits = byLang[answer]
      detected = results[idx][0]
      hits[detected] = 1+hits.get(detected, 0)
      tally[detected] = 1+tally.get(detected, 0)
      if detected != answer:
        diffs.append('%s:%s' % (name, detected))
        if name == 'LangDetect':
          ldWrong = True
        elif name == 'CLD':
          cldWrong = True
        elif name == 'Tika':
          tikaWrong = True
        if False:
          print '  %s: wrong (got %s)' % (name, detected)

    tallies = [(ct, name) for name, ct in tally.items()]
    tallies.sort(reverse=True)
    if tallies[0][0] > 1:
      vote = tallies[0][1]
    else:
      vote = allResults['LangDetect'][idx][0]
    
    byLang = byDetector['majority']
    if answer not in byLang:
      byLang[answer] = {}
    hits = byLang[answer]
    hits[vote] = 1+hits.get(vote, 0)

    if False and len(diffs) == 3:
      print 'line %d all 3 wrong: %s' % (idx+1, diffs)
    if ldWrong:
      print 'line %d [%s]: %s' % (idx+1, answer, diffs)
      ldWrongCount += 1
      if cldWrong:
        cldAlsoWrong += 1
      if tikaWrong:
        tikaAlsoWrong += 1

  print 'When ld is wrong (%d): tika also wrong %d (%.2f%%); cld also wrong %d (%.2f%%)' % \
        (ldWrongCount,
         tikaAlsoWrong, 100.*tikaAlsoWrong/ldWrongCount,
         cldAlsoWrong, 100.*cldAlsoWrong/ldWrongCount)

  HTML = True
  if HTML:
    print '<html>'
  for name, byLang in byDetector.items():
    l = byLang.items()
    l.sort()

    totFail = 0
    maxOther = None
    for lang, hits in l:
      sum = 0
      if maxOther is None or len(hits) > maxOther:
        maxOther = len(hits)
      for k, ct in hits.items():
        sum += ct
        if k != lang:
          totFail += ct

    if HTML:
      print '<br/>'
      print '<br/>'
    else:
      print
    print '%s results (total %.2f%% = %d / %d):' % (name, 100.0 * (count - totFail)/count, count-totFail, count)
    if HTML:
      print '<br/>'
      print '<font face=Courier>'
      print '<table border=0>'
    
    for lang, hits in l:
      sum = 0
      for k, ct in hits.items():
        sum += ct
        if k != lang:
          totFail += ct
      correctCT = hits.get(lang, 0)
      langAcc = 100.0 * correctCT / sum
      l2 = [(ct, lang2) for lang2, ct in hits.items()]
      l2.sort(reverse=True)
      if HTML:
        print '<tr>'
        print '<td>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>%s</b>&nbsp;</td>' % lang
        print '<td align=right>%.1f%%&nbsp;</td>' % langAcc
        for ct, lang2 in l2:
          print '<td>&nbsp;%s=%d</td>' % (lang2, ct)
        for i in xrange(len(l2), maxOther):
          print '<td>&nbsp;</td>'
        print '</tr>'
      else:
        s0 = '%s=%4d' % (l2[0][1], l2[0][0])
        print '  %s %5.1f%%: %s %s' % (lang, langAcc, s0, ' '.join('%s=%d' % (lang2, ct) for ct, lang2 in l2[1:]))

    if HTML:
      print '</table>'
      print '</font>'
      print '</html>'
    else:
      print '  total %.2f%% (= %d/%d)' % (100.0*(count-totFail)/count, count-totFail, count)

if __name__ == '__main__':
  accTest()
  #perfTest()
