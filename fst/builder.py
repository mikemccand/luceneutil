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

import sys
import types
import array
import copy
import struct
import threading

# FST PAPER: http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.24.3698
# DFA PAPER: /x/archive/pseudodog.pdf

# /x/tmp/allterm3.txt
#  - correct DFA (matches morfologik): 8,013,021 states [5,600,403 single], 15,194,589 edges (0 w/ output)
#    - packed 81.82 MB
#    - repacked 61.74 MB [24.5% smaller]
#  - FSTNUM 8,133,463 states [5,686,533 single], 15,357,402 edges (8,387,056 w/ output)

# TODO
#   - document the format, and why we write backwards
#   - should we make byte[] writing "pages" -- minimize overalloc cost
#   - clean up the final state vs edge confusion...
#   - get empty string working!  ugh
#   - hmm store max prefix len in header
#   - compression ideas
#     - make direct array lookup for nodes w/ many edges?  faster lookup
#     - or maybe just fully expand to depth N
#     - patricia trie
#   - later
#     - hmm: factor out this prune count 1/2 to a subclassable prune strategy!?
#     - packing opto: not only FLAG_NODE_NEXT we can also make it eg
#       skip N arcs.  ie really we are laying out all the arcs,
#       serialized, so an arc can say "my target is +N arcs away"; for N
#       small the scan cost might be acceptable
#       - also: maybe use vint encoding for relative addressing, only
#         for acyclic fsts
#     - in FST mode if the output is unique per word, we can fix
#       minimize to simply copy as it moves backwards past the full output
#   - post java
#     - pruning: improve it to be RAM based not count
#     - can i somehow make this pruning "work" with min suffix enabled?
#       - needs to suddenly be RAM based not term count based?
#     - how about recording minTermLengthFromHere?  fuzzy could use this
#       to skip whole nodes?

# NOTES
#   - fst is NOT minimal in the numeric case -- eg aa/0, aab/1, bbb/2
#     will produce 6 states when a 5 state fst is also possible

# POSSIBLE USES IN LUCENE
#   - silly in-ram terms in SimpleText
#   - FieldCache terms/terms index
#   - prefix trie as terms index and as fastmatch for AQ
#   - full top (multi-reader) DFA/FST
#     - fst could map to bitset of which segs have the term
#   - make FST only for certain tokens eg proper names


BIT_FINAL_ARC = 1 << 0
BIT_LAST_ARC = 1 << 1
BIT_TARGET_NEXT = 1 << 2
BIT_STOP_STATE = 1 << 3
BIT_ARC_HAS_OUTPUT = 1 << 4
BIT_FINAL_STATE_HAS_OUTPUT = 1 << 5

FINAL_END_STATE = -1
NON_FINAL_END_STATE = 0

class FSTOutput:
  def common(self, output1, output2):
    pass

  def subtract(self, output, inc):
    pass

  def add(self, prefix, output):
    pass

  def write(self, output, bytesOut):
    pass

  def read(self, bytes, pos):
    return None, pos

  def getNoOutput(self):
    pass

  def outputToString(self, o):
    return ''

  def validOutput(self, o):
    return False

class FSTByteSequenceOutput:

  def common(self, output1, output2):
    idx = 0
    while idx < len(output1) and idx < len(output2) and output1[idx] == output2[idx]:
      idx += 1
    return output1[:idx]

  def subtract(self, output, inc):
    assert len(inc) <= len(output)
    return output[len(inc):]

  def add(self, prefix, output):
    return prefix + output

  def write(self, output, bytesOut):
    assert len(output) < 256
    bytesOut.write(len(output))
    for v in output:
      assert v < 256
      bytesOut.write(v)

  def read(self, bytes, pos):
    count = bytes[pos]
    pos -= 1
    l = []
    for idx in xrange(count):
      l.append(bytes[pos])
      pos -= 1
    return tuple(l), pos

  NO_OUTPUT = ()

  def outputToString(self, output):
    return ''.join([str(x) for x in output])    

  def validOutput(self, o):
    return type(o) is types.TupleType

class FSTPositiveIntOutput:

  def common(self, output1, output2):
    return min(output1, output2)

  def subtract(self, output, inc):
    assert inc <= output
    return output - inc

  def add(self, prefix, output):
    return prefix + output

  def write(self, output, bytesOut):
    assert output > 0
    while output > 0x7F:
      bytesOut.write(0x80 | (output & 0x7F))
      output = output >> 7
    bytesOut.write(output)

  def read(self, bytes, pos):
    b = bytes[pos]
    pos -= 1
    value = b & 0x7F
    shift = 7
    while b & 0x80 != 0:
      b = bytes[pos]
      pos -= 1
      value |= (b & 0x7F) << shift
      shift += 7
    return value, pos

  NO_OUTPUT = 0

  def outputToString(self, output):
    return '%d' % output

  def validOutput(self, o):
    return type(o) in (types.IntType, types.LongType)

class FSTNoOutput:

  def common(self, output1, output2):
    return None

  def subtract(self, output, inc):
    return None

  def add(self, prefix, output):
    return None

  def write(self, output, bytesOut):
    return

  def read(self, bytes, pos):
    return None, pos

  NO_OUTPUT = None

  def validOutput(self, o):
    return o is None

  def outputToString(self, o):
    return 'None'

class State:
  isFinal = False
  
  def __init__(self):
    # self.to is list of (label, toState, output, nextFinalOutput, edgeIsFinal)
    self.to = []
    self.stateOutput = self.outputs.NO_OUTPUT
    self.termCount = 0

  def freeze(self):
    return self.builder.packedFST.addState(self)
      
  def clear(self):
    self.to = []
    self.isFinal = False
    self.stateOutput = self.outputs.NO_OUTPUT
    self.termCount = 0

  def getLastOutput(self, labelToMatch):
    assert len(self.to) > 0
    assert labelToMatch == self.to[-1][0]
    return self.to[-1][2]

  def __hash__(self):
    h = len(self.to)
    for label, toState, output, nextFinalOutput, edgeIsFinal in self.to:
      h = 31*h + hash(label)
      assert isinstance(toState, FrozenState)
      h = 31*h + hash(toState.address)
      h = 31*h + hash(output)
      h = 31*h + hash(nextFinalOutput)
      h = 31*h + hash(edgeIsFinal)
    return h & sys.maxint

  def appendTo(self, label, toState, output):
    assert isinstance(toState, State)
    assert len(self.to) == 0 or label > self.to[-1][0]
    self.to.append((label, toState, output, None, False))

  def replaceLast(self, label, toState, nextFinalOutput, edgeIsFinal):
    #assert isinstance(toState, FrozenState)
    assert label == self.to[-1][0]
    label, oldToState, output, oldNextFinalOutput, oldEdgeIsFinal = self.to[-1]
    assert oldNextFinalOutput is None
    self.to[-1] = (label, toState, output, nextFinalOutput, edgeIsFinal)

  def deleteLast(self, label, toState):
    assert isinstance(toState, State)
    assert label == self.to[-1][0]
    assert toState == self.to[-1][1]
    self.to.pop()

  def setLastOutput(self, labelToMatch, newOutput):
    assert len(self.to) > 0
    label, toState, output, oldNextFinalOutput, oldEdgeIsFinal = self.to[-1]
    assert label == labelToMatch
    self.to[-1] = (label, toState, newOutput, oldNextFinalOutput, oldEdgeIsFinal)

  def prependOutput(self, outputPrefix):
    """
    Insert this output in front of all current outputs.
    """
    for i, (label, toState, output, nextFinalOutput, edgeIsFinal) in enumerate(self.to):
      self.to[i] = (label, toState, self.outputs.add(outputPrefix, output), nextFinalOutput, edgeIsFinal)

def hashFrozen(address):
  # NOTE: must match State.__hash__!!!
  to = State.builder.packedFST.getEdges(address)
  h = len(to)
  for label, toState, output, nextFinalOutput, edgeIsFinal in to:
    h = 31*h + hash(label)
    h = 31*h + hash(toState)
    h = 31*h + hash(output)
    h = 31*h + hash(nextFinalOutput)
    h = 31*h + hash(edgeIsFinal)
  return h & sys.maxint

def stateEquals(state, address):
  to1 = state.to
  edge2 = address
  for i in xrange(len(to1)):
    if i > 0 and nextEdge2 is None:
      return False
    label1, toState1, output1, nextFinalOutput1, edgeIsFinal1 = to1[i]
    assert isinstance(toState1, FrozenState)
    label2, output2, toState2, nextFinalOutput2, edgeIsFinal2, nextEdge2 = State.builder.packedFST.getEdge(edge2)
    if label1 != label2:
      return False
    if output1 != output2:
      return False
    if toState1.address != toState2:
      return False
    if nextFinalOutput1 != nextFinalOutput2:
      return False
    if edgeIsFinal1 != edgeIsFinal2:
      return False
    edge2 = nextEdge2
  if nextEdge2 is not None:
    return False
  return True

class MinStateHash2:

  def __init__(self):
    self.table = array.array('i', [-1]*16)
    self.count = 0

  def add(self, state):
    h = origH = hash(state)
    h2 = ((h >> 8) + h) | 1

    while True:
      pos = h%len(self.table)
      v = self.table[pos]
      if v == -1:
        # freeze & add
        address = state.freeze()
        assert hashFrozen(address) == origH, '%s vs %s' % (hashFrozen(address), origH)
        self.count += 1
        self.table[pos] = address
        if len(self.table) < 2*self.count:
          self.rehash()
        return address
      elif stateEquals(state, v):
        # already present
        return v

      # probe
      h += h2
      
  def addNew(self, newTable, ent):
    h = hashFrozen(ent)
    h2 = ((h >> 8) + h) | 1
    while True:
      pos = h%len(newTable)
      if newTable[pos] == -1:
        newTable[pos] = ent
        break
      # probe
      h += h2

  def rehash(self):
    newTable = array.array('i', [-1]*(len(self.table)*2))
    for ent in self.table:
      if ent != -1:
        self.addNew(newTable, ent)
    self.table = newTable

class FrozenState:

  def __init__(self, address):
    # address where this node's edges start
    # if this is -1, then this is the universal final state
    # if it's negative, then it's a final state and the real address is the negation
    # else it's the real address
    assert type(address) is types.IntType
    self.address = address

class Builder:

  initState = None

  def __init__(self, doMinSuffix=True, suffixMinCount=None, suffixMinCount2=None, outputs=FSTNoOutput()):
    """
    suffixMinCount is simple -- keep the state only if it leads to >= N terms
    suffixMinCount2 is off by 1 (leafier): keeps state if prior state leads to = N terms
    """
    State.builder = self
    State.outputs = outputs
    self.outputs = outputs
    self.first = True
    self.lastBytesIn = ()
    self.minMap = MinStateHash2()
    self.tempStates = []
    self.doMinSuffix = doMinSuffix
    self.suffixMinCount = suffixMinCount
    self.suffixMinCount2 = suffixMinCount2

    self.packedFST = SerializedFST(self.outputs)
    self.termCount = 0

  def getTotStateCount(self):
    return self.packedFST.count
  
  def getMappedStateCount(self):
    return self.minMap.count

  def freezeState(self, state):
    if self.doMinSuffix:
      # dedup
      if len(state.to) == 0:
        if state.isFinal:
          address = FINAL_END_STATE
        else:
          address = NON_FINAL_END_STATE
      else:
        address = self.minMap.add(state)
    else:
      # just freeze
      address = state.freeze()
    # reuse
    state.clear()
    return address

  def replacePrevTail(self, prefixLenPlus1):

    #print '  doReplace prefixLenPlus1=%d' % prefixLenPlus1
    for i in xrange(len(self.lastBytesIn), prefixLenPlus1-1, -1):
      #print '  replace tempState[%d] termCount=%d' % (i, self.tempStates[i].termCount)
      
      if self.suffixMinCount is not None:
        # simple, local pruning
        if state.termCount < self.suffixMinCount:
          doPrune = True
        else:
          doPrune = False
        doPruneNext = False
        doFreeze = True
      elif self.suffixMinCount2 is not None:
        # prune if count < threshold and count of state arriving to me
        # also < threshold
        if i > prefixLenPlus1:
          #print '     prev termCount=%s' % self.tempStates[i-1].termCount
          if self.tempStates[i-1].termCount < self.suffixMinCount2:
            # my parent, about to be frozen, doesn't make the cut, so
            # I'm definitely pruned 
            doPrune = True
          elif self.suffixMinCount2 == 1 and self.tempStates[i-1].termCount == 1:
            # special case -- if pruneCount2 is 1, we keep only up
            # until the 'distinguished edge', ie we keep only the
            # 'divergent' part of the FST. if my parent, about to be
            # frozen, has termCount 1 then we are already past the
            # distinguished edge
            doPrune = True
          else:
            # my parent, about to be frozen, does make the cut, so
            # I'm definitely not pruned 
            doPrune = False

            # TODO: assert no to states are unfrozen
          doFreeze = True
        else:
          # this is the head state in the to-be-frozen tail
          assert i == prefixLenPlus1
          doPrune = False
          doFreeze = False
          #print '      first freeze'

        if self.tempStates[i].termCount < self.suffixMinCount2:
          doPruneNext = True
        elif self.suffixMinCount2 == 1 and self.tempStates[i].termCount == 1:
          # keep only 'divergent' part of the FST
          doPruneNext = True          
        else:
          doPruneNext = False
      else:
        doPruneNext = doPrune = False
        doFreeze = True

      #print '    doPrune=%s doPruneNext=%s doFreeze=%s' % (doPrune, doPruneNext, doFreeze)

      if doPruneNext:
        state = self.tempStates[i]
        if __debug__:
          for j in xrange(len(state.to)):
            assert isinstance(state.to[j][1], State)
        state.to = []
        
      if doPrune:
        self.tempStates[i].clear()
        self.tempStates[i-1].deleteLast(self.lastBytesIn[i-1], self.tempStates[i])
      elif doFreeze:
        #print '    now freeze'
        # Now must freeze any to-states that were were previously undecided on
        self.freezeAllToStates(self.tempStates[i])
        nextFinalOutput = self.tempStates[i].stateOutput
        edgeIsFinal = self.tempStates[i].isFinal
        #print '      now freeze state %d' % i
        frozen = FrozenState(self.freezeState(self.tempStates[i]))
        #print '        got %d' % frozen.address
        #print '    now freeze addr=%s' % frozen.address
        self.tempStates[i-1].replaceLast(self.lastBytesIn[i-1], frozen, nextFinalOutput, edgeIsFinal)
      else:
        # must allocate new state since we are leaving last one in play, for now
        self.freezeAllToStates(self.tempStates[i])
        edgeIsFinal = self.tempStates[i].isFinal
        nextFinalOutput = self.tempStates[i].stateOutput
        self.tempStates[i-1].replaceLast(self.lastBytesIn[i-1], self.tempStates[i], nextFinalOutput, edgeIsFinal)
        self.tempStates[i] = State()

  def freezeAllToStates(self, state):
    #print '  freeze to states'
    for i, (label, toState, output, nextFinalOutput, edgeIsFinal) in enumerate(state.to):
      #print '    edge %s -> %s' % (chr(label), toState)
      if isinstance(toState, State):
        # TODO: we could recycle toState at this point
        toState = FrozenState(self.freezeState(toState))
      state.to[i] = (label, toState, output, nextFinalOutput, edgeIsFinal)
          
  def add(self, bytesIn, output):
    
    assert type(bytesIn) is types.TupleType
    assert self.outputs.validOutput(output)
    assert self.first or bytesIn != self.lastBytesIn, '%s vs %s' % (bytesIn, self.lastBytesIn)
    self.first = False
    self.termCount += 1
    
    #print '\nadd %s -> %s' % (toAscii(toWord(bytesIn)), self.outputs.outputToString(output))
    assert bytesIn > self.lastBytesIn, 'words are added out of order: %s prev vs %s now' % \
           (toWord(self.lastBytesIn), toWord(bytesIn))

    # compute length of longest shared prefix w/ previous term
    i = 0
    while i < len(bytesIn) and i < len(self.lastBytesIn) and bytesIn[i] == self.lastBytesIn[i]:
      i += 1
      self.tempStates[i].termCount += 1
      #print '  tempState[%d] termCount=%d' % (i, self.tempStates[i].termCount)
    prefixLenPlus1 = i+1

    while len(self.tempStates) <= len(bytesIn):
      self.tempStates.append(State())

    # minimize states from previous term's orphan'd suffix
    self.replacePrevTail(prefixLenPlus1)

    # print '  prefPlus1 = %s' % prefixLenPlus1

    # init tail states for current term
    for i in xrange(prefixLenPlus1, len(bytesIn)+1):
      assert self.tempStates[i] is not None
      self.tempStates[i-1].appendTo(bytesIn[i-1], self.tempStates[i], self.outputs.NO_OUTPUT)
      self.tempStates[i].termCount += 1
      # print '  tail %s, to label %s' % (i, bytesIn[i-1])

    lastState = self.tempStates[len(bytesIn)]
    lastState.isFinal = True
    lastState.stateOutput = self.outputs.NO_OUTPUT

    # push conflicting outputs forward, only as far as needed
    for j in xrange(1, prefixLenPlus1):
      lastOutput = self.tempStates[j-1].getLastOutput(bytesIn[j-1])
      #print '  push output @ %d: lastOutput %s output %s' % (j, self.outputs.outputToString(lastOutput), self.outputs.outputToString(output))
      if lastOutput != self.outputs.NO_OUTPUT:
        commonOutputPrefix = self.outputs.common(output, lastOutput)
        #print '    common=%s' % self.outputs.outputToString(commonOutputPrefix)
        wordSuffix = self.outputs.subtract(lastOutput, commonOutputPrefix)
        #print '    suffix %s' % str(wordSuffix)
        self.tempStates[j-1].setLastOutput(bytesIn[j-1], commonOutputPrefix)
        #print '    edge %s' % str(self.tempStates[j].to)
        self.tempStates[j].prependOutput(wordSuffix)
      else:
        commonOutputPrefix = self.outputs.NO_OUTPUT
        wordSuffix = self.outputs.NO_OUTPUT

      lastTempState = self.tempStates[j]
      if lastTempState.isFinal and wordSuffix != self.outputs.NO_OUTPUT:
        #print '  output across final wordSuffix=%s current=%s' % (wordSuffix, str(self.tempStates[j].outputs))
        lastTempState.stateOutput = self.outputs.add(wordSuffix, lastTempState.stateOutput)
        #print '  push state output = %s' % self.outputs.outputToString(lastTempState.stateOutput)
        
      output = self.outputs.subtract(output, commonOutputPrefix)

    if bytesIn == self.lastBytesIn:
      # TODO: remove this:
      assert len(bytesIn) == 0
      if output != self.outputs.NO_OUTPUT:
        self.tempStates[len(bytesIn)].stateOutput = output
        #print 'now state output = %s' % self.outputs.outputToString(self.tempStates[len(bytesIn)].stateOutput)
    else:
      # print '  now set edge output state %s label %s bytes %s' % (self.tempStates[prefixLenPlus1-1].number, bytesIn[prefixLenPlus1-1], bytesOut)
      self.tempStates[prefixLenPlus1-1].setLastOutput(bytesIn[prefixLenPlus1-1], output)

    self.lastBytesIn = bytesIn
    self.tempStates[0].termCount += 1

  def finish(self):
    if self.initState is not None:
      raise RuntimeError('already finalized')
    # minimize states in the last word
    #print '\nnow finish'
    self.replacePrevTail(1)
    if (self.suffixMinCount is not None and self.tempStates[0].termCount < self.suffixMinCount) or \
       (self.suffixMinCount2 is not None and self.tempStates[0].termCount < self.suffixMinCount2):
      # the whole shebang got pruned!!
      print '  all pruned!'
      self.initState = None
    else:
      self.freezeAllToStates(self.tempStates[0])
      self.initState = self.freezeState(self.tempStates[0])
    self.packedFST.initState = self.initState

def toBytes(s):
  return tuple(ord(x) for x in s.encode('utf8'))

def toWord(bytes):
  return ''.join(chr(b) for b in bytes)

def toAscii(word):
  return word.encode('ascii', 'replace')


def writeInt(fOut, v):
  fOut.write(struct.pack('i', v))
 
def readInt(fIn):
  return struct.unpack('i', fIn.read(4))[0]

def loadSerializedFST(fIn, outputs):
  fst = SerializedFST(outputs)
  # TODO: messy, messy!
  fst.out = None
  fst.initState = readInt(fIn)
  fst.bytes = array.array('B', fIn.read())
  return fst

class SerializedFST:

  def __init__(self, outputs):
    self.outputs = outputs
    self.out = BytesWriter()
    self.bytes = self.out.bytes
    self.initState = None
    self.lastFrozenState = None
    self.count = 0

    # temporary pad -- ensure no node gets address 0, which is deadly
    # if it's a final node (-0 == 0), or addres 1, which is deadly if
    # it's final because -1 is reserved
    self.out.write(0)
    self.out.write(0)

  def write(self, fOut):
    writeInt(fOut, self.initState)
    fOut.write(self.bytes)

  def addState(self, state):

    """
    Freezes the incoming state, appending its bytes to the end of the
    byte array.  All to states must already be frozen.  Returns the
    address of this state.
    """

    NO_OUTPUT = State.outputs.NO_OUTPUT

    if len(state.to) == 0:
      if state.isFinal:
        return FINAL_END_STATE
      else:
        return NON_FINAL_END_STATE

    self.count += 1
    startAddress = len(self.bytes)

    #print 'SFST.addState newAddr=%s' % address

    lastEdge = len(state.to)-1
    for i, (label, toState, output, nextFinalOutput, edgeIsFinal) in enumerate(state.to):

      assert isinstance(toState, FrozenState), 'got %s' % toState

      #print '  edge: label=%s toAddr=%s' % (chr(label), toState.address)
      flags = 0

      if i == lastEdge:
        flags += BIT_LAST_ARC

      if self.lastFrozenState == toState.address:
        # TODO: maybe don't do this if arc is "too far" from the end?
        flags += BIT_TARGET_NEXT

      if edgeIsFinal:
        flags += BIT_FINAL_ARC
        if nextFinalOutput != NO_OUTPUT:
          flags += BIT_FINAL_STATE_HAS_OUTPUT
      else:
        assert nextFinalOutput == self.outputs.NO_OUTPUT, 'got output %s' % self.outputs.outputToString(nextFinalOutput)

      targetHasEdges = self.hasEdges(toState.address)
      if not targetHasEdges:
        flags += BIT_STOP_STATE

      if output != NO_OUTPUT:
        flags += BIT_ARC_HAS_OUTPUT

      self.out.write(flags)
      self.out.write(label)
      if output != NO_OUTPUT:
        State.outputs.write(output, self.out)
      if nextFinalOutput != NO_OUTPUT:
        State.outputs.write(nextFinalOutput, self.out)

      if (not flags & BIT_TARGET_NEXT) and targetHasEdges:
        encodeAddress(self.out, abs(toState.address))

    # reverse bytes in place
    endAddress = len(self.bytes)
    stopAt = (endAddress - startAddress) / 2
    upto = 0
    while upto < stopAt:
      # swap
      self.bytes[startAddress+upto], self.bytes[endAddress-upto-1] = self.bytes[endAddress-upto-1], self.bytes[startAddress+upto]
      upto += 1
    self.lastFrozenState = endAddress - 1
    
    #print '    start node node %d' % (endAddress-1)
    return endAddress - 1

  def getStartState(self):
    return self.initState

  def anyEdges(self, node):
    return node != FINAL_END_STATE

  def getEdges(self, node, includeFlags=False):
    if node == FINAL_END_STATE or node == NON_FINAL_END_STATE:
      return ()

    bytes = self.bytes
    pos = node

    edges = []
    lastIndex = []
    while True:
      flags = bytes[pos]
      pos -= 1
      label = bytes[pos]
      pos -= 1

      if flags & BIT_ARC_HAS_OUTPUT:
        output, pos = self.outputs.read(bytes, pos)
      else:
        output = self.outputs.NO_OUTPUT

      if flags & BIT_FINAL_STATE_HAS_OUTPUT:
        nextFinalOutput, pos = self.outputs.read(bytes, pos)
      else:
        nextFinalOutput = self.outputs.NO_OUTPUT

      if flags & BIT_STOP_STATE:
        if flags & BIT_FINAL_ARC:
          toState = FINAL_END_STATE
        else:
          toState = NON_FINAL_END_STATE
      elif flags & BIT_TARGET_NEXT:
        if not flags & BIT_LAST_ARC:
          toState = 0
          lastIndex.append(len(edges))
        else:
          toState = pos
      else:
        toState = decodeAddress(bytes, pos)
        pos -= 4

      if includeFlags:
        tup = label, toState, output, nextFinalOutput, flags & BIT_FINAL_ARC != 0, flags
      else:
        tup = label, toState, output, nextFinalOutput, flags & BIT_FINAL_ARC != 0
      edges.append(tup)
      if flags & BIT_LAST_ARC:
        break

    if len(lastIndex) > 0:
      for idx in lastIndex:
        l = list(edges[idx])
        l[1] = pos
        edges[idx] = tuple(l)

    return edges

  def hasEdges(self, node):
    return node != FINAL_END_STATE and node != NON_FINAL_END_STATE
    
  def findEdge(self, node, labelToMatch):

    """
    Returns toState.
    """

    if node == FINAL_END_STATE:
      return None, None, None, None

    pos = node
    bytes = self.bytes
    while True:
      flags = bytes[pos]
      pos -= 1
      label = bytes[pos]
      pos -= 1
      if flags & BIT_ARC_HAS_OUTPUT:
        output, pos = self.outputs.read(bytes, pos)
      else:
        output = self.outputs.NO_OUTPUT

      if flags & BIT_FINAL_STATE_HAS_OUTPUT:
        nextFinalOutput, pos = self.outputs.read(bytes, pos)
      else:
        nextFinalOutput = self.outputs.NO_OUTPUT
        
      if labelToMatch == label:
        if flags & BIT_STOP_STATE:
          target = FINAL_END_STATE
        elif flags & BIT_TARGET_NEXT:
          if not flags & BIT_LAST_ARC:
            pos = self.seekToNextNode(pos)
          target = pos
        else:
          target = decodeAddress(bytes, pos)
          pos -= 4

        return target, output, nextFinalOutput, flags & BIT_FINAL_ARC != 0

      if flags & BIT_LAST_ARC:
        break
      
      if not flags & BIT_STOP_STATE and not flags & BIT_TARGET_NEXT:
        pos -= 4

    return None, None, None, None

  def seekToNextNode(self, pos):
    bytes = self.bytes
    while True:
      flags = bytes[pos]
      pos -= 1
      label = bytes[pos]
      pos -= 1
      
      if flags & BIT_ARC_HAS_OUTPUT:
        output, pos = self.outputs.read(bytes, pos)
      else:
        output = self.outputs.NO_OUTPUT

      if flags & BIT_FINAL_STATE_HAS_OUTPUT:
        nextFinalOutput, pos = self.outputs.read(bytes, pos)
      else:
        nextFinalOutput = self.outputs.NO_OUTPUT
        
      if not flags & BIT_STOP_STATE and not flags & BIT_TARGET_NEXT:
        pos -= 4

      if flags & BIT_LAST_ARC:
        break
    return pos

  def isLastEdge(self, edge):
    return self.bytes[edge] & BIT_LAST_ARC

  def getEdge(self, edge):

    pos = edge
    bytes = self.bytes
    
    flags = bytes[pos]
    pos -= 1
    label = bytes[pos]
    pos -= 1

    #print '    edge label %s' % chr(label)
    if flags & BIT_ARC_HAS_OUTPUT:
      output, pos = self.outputs.read(bytes, pos)
    else:
      output = self.outputs.NO_OUTPUT

    if flags & BIT_FINAL_STATE_HAS_OUTPUT:
      nextFinalOutput, pos = self.outputs.read(bytes, pos)
    else:
      nextFinalOutput = self.outputs.NO_OUTPUT

    if flags & BIT_STOP_STATE:
      #print '    flag stop'
      toNode = FINAL_END_STATE
      nextEdge = pos
    elif flags & BIT_TARGET_NEXT:
      #print '    flag next'
      nextEdge = pos
      if not flags & BIT_LAST_ARC:
        pos = self.seekToNextNode(pos)
      toNode = pos
    else:
      #print '    flag no'
      toNode = decodeAddress(bytes, pos)
      pos -= 4
      nextEdge = pos

    if flags & BIT_LAST_ARC:
      nextEdge = None

    return label, output, toNode, nextFinalOutput, flags & BIT_FINAL_ARC != 0, nextEdge
  
  def getToState(self, edge):
    pos = edge
    bytes = self.bytes
    
    flags = bytes[pos]
    pos -= 1
    label = bytes[pos]
    pos -= 1
    
    if flags & BIT_STOP_STATE:
      return FINAL_END_STATE
    else:
      if flags & BIT_ARC_HAS_OUTPUT:
        pos = self.outputs.read(bytes, pos)[1]
      if flags & BIT_FINAL_STATE_HAS_OUTPUT:
        pos = self.outputs.read(bytes, pos)[1]
      if flags & BIT_TARGET_NEXT:
        if not flags & BIT_LAST_ARC:
          pos = self.seekToNextNode(pos)
        return pos
      else:
        return decodeAddress(bytes, pos)

  def anyEdges(self, state):
    return state != FINAL_END_STATE

  def nextEdge(self, edge):
    assert edge >= 0
    bytes = self.bytes
    pos = edge
    flags = bytes[pos]
    pos -= 1
    if flags & BIT_LAST_ARC != 0:
      return None

    label = bytes[pos]
    pos -= 1
    
    if flags & BIT_ARC_HAS_OUTPUT:
      pos = self.outputs.read(bytes, pos)[1]
    if flags & BIT_FINAL_STATE_HAS_OUTPUT:
      pos = self.outputs.read(bytes, pos)[1]

    if flags & BIT_STOP_STATE or flags & BIT_TARGET_NEXT:
      return pos
    else:
      return pos - 4

      
class BytesWriter:
  def __init__(self):
    self.bytes = array.array('B')

  def write(self, b):
    assert b >= 0 and b <= 0xFF
    self.bytes.append(b)

  def reset(self):
    del self.bytes[:]

  def getPosition(self):
    return len(self.bytes)
  
def encodeAddress(bytes, v):
  bytes.write(v&0xFF)
  bytes.write((v>>8)&0xFF)
  bytes.write((v>>16)&0xFF)
  bytes.write((v>>24)&0xFF)

def decodeAddress(bytes, loc):
  return bytes[loc] + \
         (bytes[loc-1]<<8) + \
         (bytes[loc-2]<<16) + \
         (bytes[loc-3]<<24)
