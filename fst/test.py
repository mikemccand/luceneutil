# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import bisect
import array
import traceback
import time
import codecs
import sys
import random
import types
import copy

# allterms3.txt = 9.8 M wikipedia terms dict
# termsDump.txt = from dawid
# termsDumpUnix.txt = from dawid w/ dos stripped

import builder

DEBUG = '-debug' in sys.argv

if '-seed' in sys.argv:
  seed = int(sys.argv[1+sys.argv.index('-seed')])
else:
  seed = random.randint(0, sys.maxint)
#seed = 4269902312347254832
print 'seed %s' % seed

if '-fst' in sys.argv:
  fstFileName = sys.argv[1+sys.argv.index('-fst')]
else:
  fstFileName = None

# only do first N terms with -lex
if '-limit' in sys.argv:
  LIMIT = int(sys.argv[1+sys.argv.index('-limit')])
else:
  LIMIT = None
  
RANDOM = random.Random(seed)

if '-prune' in sys.argv:
  PRUNE_COUNT = int(sys.argv[1+sys.argv.index('-prune')])
else:
  PRUNE_COUNT = None

# NOTES
#   - do compact rep
#     - pull FSA5 from morfologik
#     - http://portal.acm.org/citation.cfm?id=75622
#     - impl patricia tree: http://xw2k.nist.gov/dads/HTML/patriciatree.html
#     - take advantage of convergent vs divergent parts
#   - in FST mode, note "first output" edge going backwards then stop minimizing
#   - go back & re-run orig algo on a "known" dict to make sure I don't cause too many states!
#   - make "full walk" to enum all accepted strings/outputs, to validate
#   - explain why node & edge count DO NOT AGREE w/ morfologik
#   - how to verify fst is minimal?
#     - run opefst
#   - HMMM: just spelling out bytes from string '%d' % idx gives fewer states!!!
#     - maybe we really should pre-code as vint?

# to run mofologik:
#   cd ~/src/morfo
#   ant
#   cd tmp/bin
#   java -jar morfologik-stemming-trunk.jar fsa_build -i INPUT -o OUTPUT
#   java -jar morfologik-stemming-trunk.jar fsa_dump -d OUTPUT

def toBytes(s):
  return tuple(ord(x) for x in s.encode('utf8'))

def toWord(bytes):
  return (''.join(chr(b) for b in bytes)).decode('utf8')
  #return ''.join(chr(b) for b in bytes)

def toAscii(s):
  return s.encode('ascii', 'replace')

def getOutput(mode, idx):
  if mode.startswith('DFA'):
    output = None
  elif mode.startswith('FSTNUM'):
    output = idx
  elif mode == 'FSTIDX':
    output = (idx,)
  else:
    raise RuntimeError('unknown mode %s' % mode)
  return output

def doAdd(b, word, idx, mode):
  output = getOutput(mode, idx)
  b.add(toBytes(word), output)

def getOutputs(mode):
  if mode in ('FSTIDX', 'FSTDEC'):
    outputs = builder.FSTByteSequenceOutput()
  elif mode.startswith('FSTNUM'):
    outputs = builder.FSTMonotonicPositiveIntOutput()
  else:
    outputs = builder.FSTNoOutput()
  return outputs

def getBuilder(mode, pruneCount):
  return builder.Builder(pruneCount is None, None, pruneCount, getOutputs(mode))
  
def tinyTest():
  global mode
  #words = ['it', 'is', 'not', 'easy', 'to', 'wreck', 'a', 'nice', 'beach']
  #words = ['fo', 'fe']
  #words = ['fo', 'footion', 'lotion', 'zzz', 'foz', 'ffftion']
  #words = ['mia', 'kyra', 'joel', 'kyle', 'jane', 'mike', 'audrey', 'vivian', 'fay', 'ike']
  #words = ['dalmation', 'dalmotion', 'motion', 'commotion', 'comation']
  #words = ['foo', 'foobar', 'fofee', 'foofee']
  #words = ['aa', 'ab', 'ba', 'bb']
  words = ['station', 'commotion', 'elation', 'elastic', 'plastic', 'stop']
  #words = ['aa', 'ab', 'ac', 'aca', 'acb', 'acc', 'bx', 'by', 'm', 'mc', 'mca', 'mcb', 'mcc']
  #words = ['aaa', 'aab', 'baa', 'bab', 'caa', 'cab']
  words.sort()
  if PRUNE_COUNT is not None:
    mode += 'PRUNE'
  b = getBuilder(mode, PRUNE_COUNT)
  for idx, word in enumerate(words):
    doAdd(b, word, idx, mode)
  b.finish()
  packed = b.packedFST
  numState, numEdge, numEdgeWithOutput, numSingle = getStats(packed)
  print '%d states [%d single], %d bytes, %d edges (%d w/ output)' % (numState, numSingle, len(packed.bytes), numEdge, numEdgeWithOutput)

  rePacked = builder.repack(packed)
  print 'Saved to out.dot'
  open('out.dot', 'wb').write(toDot(rePacked))
  print 'after re-pack: %d bytes' % len(rePacked.bytes)
  packed = rePacked
  
  verifyAllWords(packed, words, mode)

def randomWord(r, maxLen):
  #return ''.join([chr(r.randint(97, 122)) for x in xrange(r.randint(1, maxLen))])
  return ''.join([chr(r.randint(97, 102)) for x in xrange(r.randint(1, maxLen))])
  
def lexTest(fileName):
  global mode

  if PRUNE_COUNT is not None:
    mode += 'PRUNE'

  if fstFileName is not None:
    # pre-computed packed fst
    packed = builder.loadSerializedFST(open(fstFileName, 'rb'), getOutputs(mode))
    print 'PACKED: %s [read from fst.bin]' % len(packed.bytes)
    allWords = None
    b = None
  else:
  
    b = getBuilder(mode, PRUNE_COUNT)

    f = codecs.open(fileName, 'r', 'utf-8')
    #f = open(fileName, 'r')
    count = 0
    idx = 0
    tStart = time.time()
    if LIMIT is not None and LIMIT <= 100000:
      allWords = []
    else:
      allWords = None

    while True:
      l = f.readline()
      if l == '':
        break
      l = l.strip()
      # print '%s: add %s' % (idx, toAscii(l))
      if allWords is not None:
        allWords.append(l)
      if True or count % 1 == 10:
        doAdd(b, l, idx, mode)
        idx += 1
      if count % 25000 == 0:
        print '%7.2fs: %d terms, %d states, %d mapped, %.2f MB packed [%s]' % (time.time()-tStart, count, b.getTotStateCount(), b.getMappedStateCount(), len(b.packedFST.bytes)/1024./1024., l)
        if False and count == 10:
          open('out.dot', 'w').write(b.toDot())
          raise RuntimeError()
      count += 1
      if idx == LIMIT:
        break
    b.finish()
    print '%d words' % idx
    print '%7.2fs: %s: %d terms, %d states' % (time.time()-tStart, l, count, b.getTotStateCount())
    oldSize =len(b.packedFST.bytes)
    print 'packed %.2f MB' % (oldSize/1024./1024.)
    packed = builder.repack(b.packedFST)
    newSize = len(packed.bytes)
    print 'repacked %.2f MB [%.1f%% smaller]' % (newSize/1024./1024., 100.*(oldSize-newSize)/oldSize)
    
    numState, numEdge, numEdgeWithOutput, numSingle = getStats(packed)
    print '%d states [%d single], %d edges (%d w/ output)' % (numState, numSingle, numEdge, numEdgeWithOutput)
    if numState <= 100:
      open('out.dot', 'wb').write(toDot(b.packedFST))
      print 'Saved to out.dot'

    f.close()

    fOut = open('fst.bin', 'wb')
    packed.write(fOut)
    fOut.close()
    print 'PACKED: %s [saved to fst.bin]' % len(packed.bytes)

  if 0:
    e = serial.FSTEnum(packed, True)
    #print 'HERE: %s' % toWord(e.advance(toBytes('fee')))
    print 'HERE: %s' % toWord(e.advance(toBytes('ratche')))
    #print 'HERE: %s' % toWord(e.advance(toBytes('ratchford')))
    print 'HERE: %s' % toWord(e.next())
    open('out.dot', 'wb').write(toDot(packed, 471526))
    sys.exit(0)
  
  print '\nNow verify...'
  try:

    if False and allWords is not None:
      verifyAllWords(packed, allWords, mode)
    else:
      f = codecs.open(fileName, 'r', 'utf-8')

      e = FSTEnum(packed)

      tStart = time.time()
      idx = 0
      while True:
        l = f.readline()
        if l == '':
          break
        word = l.strip()
        # print '\nnow run %s' % word
        verify(packed, idx, word, mode)
        verifyEnum(idx, word, e, mode)
        idx += 1
        if idx % 25000 == 0:
          print '%7.2fs: %s: %d terms' % (time.time()-tStart, toAscii(word), idx)
        if idx == LIMIT:
          break

  except KeyboardInterrupt:
    raise
  except:
    raise
  
def randomTest():

  # nocommit
  NUM_ITER = 1
  NUM_WORDS = 10000

  r = RANDOM

  for iter in xrange(NUM_ITER):
    print
    print 'TEST: iter %s' % iter
    seed = r.randint(0, sys.maxint)
    print '  seed %s' % seed
    r2 = random.Random(seed)

    # nocommit
    #for mode in ('DFA', 'DFAPRUNE', 'FSTNUM', 'FSTNUMPRUNE'):
    for mode in ('DFA',):

      b = None

      if mode.endswith('PRUNE'):
        pruneCount = r2.randint(0, 20)
        print '  mode %s [%d]' % (mode, pruneCount)
      else:
        print '  mode %s' % mode

      # make sure no dups:
      words = set()
      while len(words) < NUM_WORDS:
        words.add(randomWord(r2, 10))
      words = list(words)
      words.sort()

      # nocommit
      f = open("words.txt", 'wb')
      for w in words:
        f.write('%s\n' % w)
      f.close()

      try:
        if mode.startswith('FSTNUM'):
          outputs = builder.FSTMonotonicPositiveIntOutput()
        else:
          outputs = builder.FSTNoOutput()

        if mode.endswith('PRUNE'):
          doMin = False
          pc = pruneCount
        else:
          doMin = True
          pc = None

        b = builder.Builder(doMin, None, pc, outputs=outputs)
        lastW = None
        for idx, w in enumerate(words):
          doAdd(b, w, idx, mode)
          lastW = w
        b.finish()

        if b.initState is None:
          continue

        packed = b.packedFST
        print '      packed: %d bytes' % len(packed.bytes)
        packed = builder.repack(packed)
        print '      repacked: %d bytes' % len(packed.bytes)

        # nocommit
        open('out.dot', 'wb').write(toDot(packed))
        print 'Wrote to out.dot'

        numState, numEdge, numEdgeWithOutput, numSingle = getStats(packed)
        print '      %d states, %d edges (%d w/ output), %d single' % (numState, numEdge, numEdgeWithOutput, numSingle)

        verifyAllWords(packed, words, mode, r2)

        if False and iter == 0:
          open('out.dot', 'wb').write(toDot(packed))
      except KeyboardInterrupt:
        raise
      except:
        if b is not None and b.initState is not None:
          print 'Saved to out.dot'
          open('out.dot', 'wb').write(toDot(packed))
        print 'FAILED: seed %s, iter %s' % (seed, iter)
        for idx, w in enumerate(words):
          print '  %s -> %d' % (w, idx)
        raise

def verifyAllWords(packed, words, mode, r=RANDOM):

  if not __debug__:
    raise RuntimeError('please run Python without -O')

  assert packed is not None

  if not mode.endswith('PRUNE'):
    # TODO: fix verifyEnum to work w/ prune
    packedEnum = FSTEnum(packed, DEBUG=DEBUG)
    for idx, word in enumerate(words):
      verify(packed, idx, word, mode)
      verifyEnum(idx, word, packedEnum, mode)
    # confirm enum ends here
    verifyEnum(idx, None, packedEnum, mode)

    # test randomly mixed next / seek in the enum
    for iter in xrange(100):
      if DEBUG:
        print 'TEST: iter=%s' % iter
      packedEnum = FSTEnum(packed, DEBUG)
      upto = -1
      while upto < len(words):
        if DEBUG:
          print '  cycle upto=%d of %d' % (upto, len(words))
        if r.randint(0, 10) <= 7:
          # next
          upto += 1
          v, output = packedEnum.next()
          if upto == len(words):
            assert v is None
            assert output is None
          else:
            assert v is not None
            assert toWord(v) == words[upto], 'upto %d: got word %s but expected word %s' % \
                   (upto, toWord(v), words[upto])
            expected = getOutput(mode, upto)
            assert output == expected, 'wrong output for word "%s": got %s but expected %s' % \
                   (words[upto],
                    packed.outputs.outputToString(output),
                    packed.outputs.outputToString(expected))

        else:
          # seek
          inc = r.randint(0, max(1,len(words)/20))
          upto += inc
          if upto == -1:
            upto = 0
          if upto >= len(words):
            upto = len(words)-1
          v, output = packedEnum.advance(toBytes(words[upto]))
          assert toWord(v) == words[upto]
          expected = getOutput(mode, upto)
          assert output == expected

    # make up random words and make sure FSTEnum.advance goes to the
    # right place:
    if DEBUG:
      print 'TEST: random words'
    for iter in xrange(100):
      packedEnum = FSTEnum(packed, DEBUG)
      word = randomWord(r, 10)
      idx = bisect.bisect_left(words, word)
      if idx > 0 and word.startswith(words[idx-1]) and (idx == len(words) or not words[idx].startswith(words[idx-1])):
        idx -= 1
      if DEBUG:
        print '    seek %s; idx %s vs %s' % (word, idx, len(words))
      v, output = packedEnum.advance(toBytes(word))
      if idx == len(words):
        assert v is None, 'got v=%s for word=%s idx=len(words)' % (toWord(v), word)
        assert output is None
      else:
        assert toWord(v) == words[idx], 'got %s expected %s' % (toWord(v), words[idx])
        expected = getOutput(mode, idx)
        assert output == expected

    # TODO: make sure output is right here:
    # TODO: do this test for prefix too
    # neg test: pick random word accepted by the FST and make sure
    # it's in the words
    wordSet = set(words)
    for iter in xrange(100):
      bytes, wordOutput = getAcceptedWord(r, packed)
      word = toWord(bytes)
      assert word in wordSet, 'random word "%s" is not accepted' % word
      
    verifyNot(packed, r, words)

def getAcceptedWord(r, fst):
  input = []
  totOutput = fst.outputs.NO_OUTPUT
  state = fst.getStartState()
  # print 'acc word'
  while True:
    label, toState, output, nextFinalOutput, edgeIsFinal = r.choice(list(fst.getEdges(state)))
    input.append(label)
    totOutput = fst.outputs.add(totOutput, output)
    # print '  label %s; state=%s' % (chr(label), toState)
    if edgeIsFinal:
      if not fst.anyEdges(toState) or r.randint(0,1) == 0:
        totOutput = fst.outputs.add(totOutput, nextFinalOutput)
        break
    state = toState
  return tuple(input), output

def runPacked(fst, bytes):
  state = fst.getStartState()
  netOutput = fst.outputs.NO_OUTPUT
  for i in xrange(len(bytes)):
    label = bytes[i]
    toState, output, nextFinalOutput, edgeIsFinal = fst.findEdge(state, label)
    if toState is None:
      raise RuntimeError('no match')
    else:
      #print '    + %s' % fst.outputs.outputToString(output)
      netOutput = fst.outputs.add(netOutput, output)
      if edgeIsFinal and i == len(bytes)-1:
        #print '    + final output %s' % fst.outputs.outputToString(nextFinalOutput)
        return fst.outputs.add(netOutput, nextFinalOutput)
    state = toState
  raise RuntimeError('no match')
  

def runPrefixPacked(fst, bytes):
  state = fst.getStartState()
  netOutput = fst.outputs.NO_OUTPUT
  for i in xrange(len(bytes)):
    byte = bytes[i]
    toState, output, nextFinalOutput = fst.findEdge(state, byte)
    if toState is None:
      return state, netOutput, i

    netOutput = fst.outputs.add(netOutput, output)
    state = toState

    if fst.isFinal(toState):
      if i == len(bytes)-1:
        netOutput = fst.outputs.add(netOutput, nextFinalOutput)
        return state, netOutput, i+1
    elif not fst.anyEdges(state):
      return state, netOutput, i+1
      
  return state, netOutput, len(bytes)

def verifyEnum(idx, word, e, mode):
  
  if mode == 'DFA':
    eword, out = e.next()
    if word is None:
      assert eword is None
      return
    assert eword is not None
    assert out is None
    assert word == toWord(eword), 'word %s, enum %s' % (word, toWord(eword))
  elif mode == 'DFAPRUNE':
    if DEBUG:
      print 'TEST: verifyEnum word=%s' % word.encode('ascii', 'replace')

    if len(e.input) == 0:
      e.next()

    p, out = e.current()
    assert out is None

    b = toBytes(word)
    if not startsWith(b, p) or (len(b) > len(p) and e.lastFinal):
      if DEBUG:
        print '  do next'
      p, out = e.next()
      assert out is None
      assert p is not None
      
      if DEBUG:
        print '  p=%s' % toWord(p)
      #print '  prefix=%s' % toWord(p)
    if DEBUG:
      print '  enum=%s' % toWord(p)
    assert startsWith(b, p), 'enum=%s word=%s' % (toWord(p), word)
  elif mode == 'FSTNUM':
    eword, out = e.next()
    if word is None:
      assert eword is None
      return
    assert toWord(eword) == word, 'got input %s but expected %s' % (toWord(eword), word)
    assert out == getOutput(mode, idx), 'input %s: got output %s but expected %s' % (word, out, idx)
  # TODO: FSTNUMPRUNE
  

def startsWith(b1, b2):
  idx = 0
  while idx < len(b1) and idx < len(b2):
    if b1[idx] != b2[idx]:
      return False
    idx += 1
  return idx == len(b2)
    
def verify(packed, idx, word, mode):
  
  # confirm each word is accepted and produces the right ord as output
  # print '  test %s' % toAscii(word)
  if mode.endswith('PRUNE'):
    bytes = toBytes(word)
    endState, netOutput, count = runPrefixPacked(packed, bytes)
    if endState != -1 and packed.numEdges(endState) > 0 and count < len(bytes):
      raise RuntimeError('word %s accepted prefix %d but state %s has %d edges' % (word, count, endState, packed.numEdges(endState)))
  else:
    try:
      output = runPacked(packed, toBytes(word))
    except RuntimeError:
      raise RuntimeError('word %s wasn\'t accepted' % word)

    # print '    got output %s' % str(output)
    expected = getOutput(mode, idx)
    if output != expected:
      raise RuntimeError('input %s returned output %s not %s' % (word, output, expected))

def verifyNot(packed, r, words):
  # confirm new random words are not accepted
  wordSet = set()
  for w in words:
    wordSet.add(w)
  NUM_NON_WORD = 100
  for i in xrange(NUM_NON_WORD):
    word = randomWord(r, 10)
    if word not in wordSet:
      try:
        output = runPacked(packed, toBytes(word))
      except RuntimeError:
        pass
      else:
        raise RuntimeError('input %s returned output %s but should not have matched' % \
                           (word, packed.outputs.outputToString(output)))

def toDot(fst, startState=None):

  if startState is None:
    startState = fst.getStartState()
    
  l = []
  w = l.append
  w('digraph Out {')
  w('  rankdir = LR;')
  q = [startState]
  seen = set()
  seen.add(startState)

  w('  %s [shape=circle,label="%s"];' % (q[0], q[0]))
  w('  initial [shape=plaintext,label=""];')
  w('  initial -> %s' % q[0])
  while len(q) > 0:
    s = q.pop()
    #print '  pop s=%s' % s
    for label, toStateNumber, output, nextFinalOutput, edgeIsFinal in fst.getEdges(s):
      #print '    label=%s, to=%s, output=%s, nextFinalOutput=%s' % \
      #      (chr(label), toStateNumber, fst.outputs.outputToString(output), fst.outputs.outputToString(nextFinalOutput))
      if toStateNumber not in seen:
        if nextFinalOutput != fst.outputs.NO_OUTPUT:
          outs = '/%s' % fst.outputs.outputToString(nextFinalOutput)
        else:
          outs = ''
        w('  %s [label="%s%s"];' % (toStateNumber, toStateNumber, outs))
        seen.add(toStateNumber)
        q.append(toStateNumber)
      label = chr(label)
      if output != fst.outputs.NO_OUTPUT:
        outs = '/%s' % fst.outputs.outputToString(output)
      else:
        outs = ''
      opts = ['label="%s%s"' % (label, outs)]
      if edgeIsFinal:
        opts.append('arrowhead="tee"')
      w('  %s -> %s [%s];' % (s, toStateNumber, ' '.join(opts)))
  w('}')

  return '\n'.join(l)
  
def getStats(fst):
  #print 'getStats'
  startState = fst.getStartState()
  #print '  startState=%s' % startState
  q = [startState]
  seen = set()
  seen.add(startState)
  totEdges = 0
  totEdgesWithOutput = 0
  numSingleOutState = 0
  while len(q) > 0:
    s = q.pop()
    #print '    pop %s' % s
    numEdge = fst.numEdges(s)
    #print '      %d edges' % numEdge
    totEdges += numEdge
    if numEdge == 1:
      numSingleOutState += 1
    for label, toStateNumber, output, nextFinalOutput, edgeIsFinal in fst.getEdges(s):
      #print '        %s -> %d' % (chr(label), toStateNumber)
      if output != fst.outputs.NO_OUTPUT:
        totEdgesWithOutput += 1
      if toStateNumber not in seen:
        seen.add(toStateNumber)
        q.append(toStateNumber)

  return len(seen), totEdges, totEdgesWithOutput, numSingleOutState


class FSTEnum:

  # TODO
  #   - really this should be decoupled from the "packed" FST impl,
  #     ie, this enum should be runnable on any FST using a common FST
  #     api
  
  """
  Lets you step through input/output pairs accepted by this FST,
  forward only.  It's initiully unpositioned so you must call next or
  seek first.
  """

  def __init__(self, fst, DEBUG=False):
    self.DEBUG = DEBUG
    self.fst = fst
    self.input = []
    # outputs are cumulative
    self.output = []
    self.edges = []
    self.lastFinal = False

  def push(self, newEdge):

    assert newEdge > 1

    if self.DEBUG:
      print 'push: newEdge=%s' % newEdge
    
    while True:

      label, output, toState, toFinalOutput, edgeIsFinal = self.fst.getEdge(newEdge)

      if self.DEBUG:
        print '    label=%s output=%s toState=%s toFinalOutput=%s' % \
              (chr(label), self.fst.outputs.outputToString(output), toState,
               self.fst.outputs.outputToString(toFinalOutput))
        
      self.input.append(label)
      self.appendOutput(output)
      self.edges.append(newEdge)

      if not self.fst.anyEdges(toState):
        if self.DEBUG:
          print '    end state [%s]' % toState
        break

      if edgeIsFinal:
        self.appendFinalOutput(toFinalOutput)
        self.lastFinal = True
        break

      newEdge = self.fst.firstEdge(toState)

  def appendFinalOutput(self, output):
    if len(self.output) == 0:
      newOutput = output
    else:
      newOutput = self.fst.outputs.add(self.output[-1], output)
    self.lastFinalOutput = newOutput

  def appendOutput(self, output):
    if len(self.output) == 0:
      newOutput = output
    else:
      newOutput = self.fst.outputs.add(self.output[-1], output)
    self.output.append(newOutput)

  def pop(self):
    #print 'pop %s' % len(self.edges)
    #print '  now %s' % self.edges[-1]
    while len(self.edges) > 0 and self.fst.isLastEdge(self.edges[-1]):
      self.edges.pop()
      self.input.pop()
      self.output.pop()
      #print '  pop'
      #print '  now %s' % self.edges[-1]

  def current(self):
    if self.lastFinal:
      output = self.lastFinalOutput
    elif len(self.output) > 0:
      output = self.output[-1]
    else:
      assert False
      output = self.outputs.NO_OUTPUT
    return self.input, output
      
  def next(self):
    if self.DEBUG:
      print '  enum.next'
    if len(self.edges) == 0:
      self.push(self.fst.getStartState())
    elif self.lastFinal:
      self.lastFinal = False
      # "resume" pushing:
      lastEdge = self.edges[-1]
      toState = self.fst.getToState(lastEdge)
      self.push(self.fst.firstEdge(toState))
    else:
      self.pop()
      if len(self.edges) == 0:
        return None, None
      else:
        lastEdge = self.edges.pop()
        self.input.pop()
        self.output.pop()
        self.push(self.fst.nextEdge(lastEdge))
    cur = self.current()
    if self.DEBUG:
      print '    return %s, output %s' % (cur[0], self.fst.outputs.outputToString(cur[1]))
    return cur

  def advance(self, target):
    '''
    NOTE: target must be >= where we are already positioned to!
    '''

    if self.DEBUG:
      print '  enum.advance target=%s' % toWord(target)

    # TODO: possibly caller could/should provide common prefix length
    # -- this is wasted computate wrt what a "true" (not subject to
    # clean API req't) intersection would do
    
    # find common prefix vs current
    idx = 0
    while idx < len(self.input) and idx < len(target) and self.input[idx] == target[idx]:
      idx += 1

    if self.DEBUG:
      print '    prefix len %d' % idx

    # find the "deepest" edge that needs to now be advanced:
    if idx < len(self.input):
      edge = self.edges[idx]
      self.lastFinal = False
      del self.input[idx:]
      del self.output[idx:]
      del self.edges[idx:]
    elif idx == len(target):
      # degen case -- seek to term we are already "on"
      if self.DEBUG:
        print '    degen same lf=%s' % self.lastFinal
      return self.current()
    else:
      edge = None

    recursed = False
    while True:
      if self.DEBUG:
        print '    cycle edge %s' % str(edge)
      if edge is None:
        if self.lastFinal:
          assert len(self.edges) > 0
          self.lastFinal = False
          toState = self.fst.getToState(self.edges[-1])
          edge = self.fst.firstEdge(toState)
          assert edge is not None
        elif len(self.input) != 0:
          edge = self.edges.pop()
          self.input.pop()
          self.output.pop()
        else:
          # we were just created
          assert len(self.input) == 0
          edge = self.fst.firstEdge(self.fst.getStartState())
      elif recursed:
        recursed = False
      else:
        edge = self.fst.nextEdge(edge)
        if self.DEBUG:
          print '      next edge %s' % str(edge)
        
      if edge is None:
        # return
        if len(self.edges) == 0:
          return None, None
        edge = self.edges.pop()
        self.input.pop()
        self.output.pop()
      else:
        label, output, toNode, nextFinalOutput, edgeIsFinal = self.fst.getEdge(edge)
        targetLabel = target[len(self.edges)]
        if self.DEBUG:
          print '      label %s vs target %s' % (chr(label), chr(targetLabel))
        if label == targetLabel:
          toState = self.fst.getToState(edge)
          if len(self.input) == len(target)-1 and not edgeIsFinal:
            self.push(edge)
            break
          # recurse
          self.edges.append(edge)
          self.input.append(label)
          self.appendOutput(output)
          if edgeIsFinal:
            if len(self.input) == len(target) or not self.fst.anyEdges(toState):
              if self.DEBUG:
                print '    now stop'
              self.lastFinal = self.fst.anyEdges(toState)
              if self.lastFinal:
                self.appendFinalOutput(nextFinalOutput)
              break
            elif not self.fst.anyEdges(toState):
              self.edges.pop()
              self.input.pop()
              self.output.pop()
              if self.DEBUG:
                print '      push then pop edge=%s' % str(edge)
            else:
              edge = self.fst.firstEdge(toState)
              recursed = True
              if self.DEBUG:
                print '      recurse new edge=%s' % str(edge)
          else:
            edge = self.fst.firstEdge(toState)
            recursed = True
            if self.DEBUG:
              print '      recurse new edge=%s' % str(edge)

        elif label > targetLabel:
          self.push(edge)
          break

    cur = self.current()
    if self.DEBUG:
      print '    return %s, output %s' % (cur[0], self.fst.outputs.outputToString(cur[1]))
    return cur

if __name__ == '__main__':
  if '-mode' in sys.argv:
    mode = sys.argv[1+sys.argv.index('-mode')]
  else:
    mode = 'DFA'
  if '-test' in sys.argv:
    randomTest()
  elif '-lex' in sys.argv:
    #import cProfile
    #cProfile.run('lexTest(sys.argv[1+sys.argv.index("-lex")])')
    lexTest(sys.argv[1+sys.argv.index("-lex")])
  else:
    tinyTest()

