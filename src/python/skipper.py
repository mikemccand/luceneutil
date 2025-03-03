# TODO
#   - delta-code docCount/ptr?  i can do vs prev L0?  but then must
#     provide prev L0 in L1 for when we skip from higher levels
#   - interaction @ read time w/ codec is a little messy?
#   - need a numBytes packet header for each skip packet...?  only for
#     the inlined case?  ie, so when not using skip data you can jump
#     over it quickly?  need a SkipSkipper class for this...

# FUTURE
#   - frq should have its own skip data, for cases where caller didn't
#     want freqs
#   - should postings interleave position blocks in w/ doc/freq blocks...?
#   - we could have arbitrary skipInterval per level... would it help?
#   - we could have arbitrary skipInterval per posting list... would it help?
#   - we can use this to skip w/in positions too
#   - we could use compact byte format in first pass when recording
#     skip towers, because on reverse we can then efficiently collate
#     them into the nextTower pointers as we write backwards
#   - can we avoid actually writing the EOF tower?  that's a 5 byte
#     vint, per level

# NOTE: from paper "Compressed Perfect Embedded Skip Lists for Quick Inverted-Index Lookups"
#       http://vigna.dsi.unimi.it/ftp/.../CompressedPerfectEmbeddedSkipLists.pdf

import sys
import random
import struct
import types
import math

VERBOSE = '-debug' in sys.argv

NO_MORE_DOCS = (1 << 31) - 1

class SkipTower:

  def __init__(self, docCount, lastDocID, pointer, prevTower):
    assert docCount <= lastDocID+1
    self.docCount = docCount
    self.lastDocID = lastDocID
    self.pointer = pointer
    self.nextTowers = []
    self.writePointer = 0
    self.prevTower = prevTower

  def write(self, b, inlined, isFixed):

    if VERBOSE:
      print('SkipTower.write skipPos=%s numNext=%d lastDocID=%s' % \
            (b.pos, len(self.nextTowers), self.lastDocID))

    # TODO: we can avoid writing this when codec is fixed block size!:
    #       if not... we can usually use only maybe 2-3 bits?
    b.writeVInt(len(self.nextTowers))

    downTo = len(self.nextTowers)-1
    while downTo >= 0:
      nextTower = self.nextTowers[downTo]
      downTo -= 1
      if VERBOSE:
        print('  nextTower skipPos=%s nextLastDocID=%d' % (nextTower.writePointer, nextTower.lastDocID))
      delta = nextTower.writePointer - self.writePointer
      assert delta > 0
      b.writeVLong(delta)
      delta = nextTower.lastDocID - self.lastDocID
      assert delta > 0
      b.writeVInt(delta)

    # TODO: can we delta-code...?
    if not isFixed:
      b.writeVInt(self.docCount)
    if not inlined:
      b.writeVLong(self.pointer)


class SkipWriter:

  def __init__(self, skipInterval):
    #print 'skipInterval %d' % skipInterval
    self.skipInterval = skipInterval
    self.lastSkipDocCount = 0
    # TODO: should we pass -1 for lastDocID...?
    self.tower0 = SkipTower(0, 0, 0, None)

    self.levelSkipCounts = [0]
    self.levelLastTowers = [self.tower0]

  def finish(self, numPostingsBytes, numDocs):
    if VERBOSE:
      print('numPostingsBytes=%s' % numPostingsBytes)

    # Add EOF tower:
    endTower = SkipTower(numDocs, NO_MORE_DOCS, numPostingsBytes,
                         self.levelLastTowers[0])

    for level in range(len(self.levelSkipCounts)):
      self.levelLastTowers[level].nextTowers.append(endTower)
      self.levelLastTowers[level] = endTower

  def visit(self, docCount, lastDocID, pointer):
    if docCount - self.lastSkipDocCount >= self.skipInterval:
      if VERBOSE:
        print('    save skip: docCount=%d lastDocID=%s pointer=%s' % \
              (docCount, lastDocID, pointer))

      tower = SkipTower(docCount, lastDocID, pointer, self.levelLastTowers[0])
      level = 0
      while True:
        self.levelLastTowers[level].nextTowers.append(tower)
        self.levelLastTowers[level] = tower
        self.levelSkipCounts[level] += 1
        if self.levelSkipCounts[level] == self.skipInterval:
          # Recurse
          level += 1
          if level == len(self.levelLastTowers):
            self.levelLastTowers.append(self.tower0)
            self.levelSkipCounts.append(0)
          else:
            self.levelSkipCounts[level] = 0
        else:
          break
        
      self.lastSkipDocCount = docCount


def writeTowers(skipWriter, fixedDocGap, postingsBytes):
  global VERBOSE

  if VERBOSE:
    print('FIXED:')
    print('  %s' % fixedDocGap)

  isFixed = fixedDocGap > 0
  
  inlined = postingsBytes is not None

  # Write the towers, backwards, assigning each tower's writePointer.
  # The writePointer starts at 0 and decreases... this works because
  # the towers only write delta pointers.

  nextTower = skipWriter.levelLastTowers[0]
  tower = nextTower.prevTower

  # Maybe make an unused "startTower" guard, matching endTower...?

  towerNumBytes = [1] * (1+len(skipWriter.tower0.nextTowers))
  
  writePointer = 0
  b = ByteBufferWriter()
  if VERBOSE:
    print('WRITE')
  while tower is not None:

    if inlined:
      chunk = nextTower.pointer - tower.pointer
      assert chunk > 0
      writePointer -= chunk
      if VERBOSE:
        print('  postings chunk %s' % chunk)

    if VERBOSE:
      print('  tower @ pointer=%s lastDocID=%s docCount=%s' % \
            (tower.pointer, tower.lastDocID, tower.docCount))

    # Guess:
    guessTowerNumBytes = towerNumBytes[len(tower.nextTowers)]
    tower.writePointer = writePointer - guessTowerNumBytes
    while True:
      tower.write(b, inlined, isFixed)
      tower.bytes = b.getBytes()
      b.reset()
      actualWritePointer = writePointer - len(tower.bytes)
      if actualWritePointer == tower.writePointer:
        break
      else:
        if VERBOSE:
          print('    tower retry')
        tower.writePointer = actualWritePointer
        towerNumBytes[len(tower.nextTowers)] = len(tower.bytes)

    writePointer -= len(tower.bytes)
    if VERBOSE:
      print('    towerNumBytes=%s' % len(tower.bytes))
      print('    tower.writePointer=%s' % writePointer)

    nextTower = tower
    tower = tower.prevTower

  # Now pull all bytes together, forwards:
  totalBytes = -writePointer

  if VERBOSE:
    print('totalBytes=%s' % totalBytes)
    print('FINAL WRITE')
    
  tower = skipWriter.tower0

  while True:

    if inlined and tower.prevTower is not None:
      # Interleave postings bytes in:
      chunk = postingsBytes[tower.prevTower.pointer : tower.pointer]
      if VERBOSE:
        print('  postings: chunk %d bytes @ wp=%d' % (len(chunk), b.pos))
      assert len(chunk) > 0
      b.writeBytes(chunk)

    if tower == skipWriter.levelLastTowers[0]:
      break
    
    if VERBOSE:
      print('  tower @ pointer=%s lastDocID=%s docCount=%s @ wp=%s (tower.wp=%s)' % \
            (tower.pointer, tower.lastDocID, tower.docCount, b.pos, tower.writePointer))

    assert b.pos == totalBytes + tower.writePointer, \
           'wp=%s expected=%s tower.wp=%d' % (b.pos, totalBytes +
                                                 tower.writePointer, tower.writePointer)
    b.writeBytes(tower.bytes)
    if VERBOSE:
      print('    tower %d bytes' % len(tower.bytes))
    tower.bytes = None
    tower = tower.nextTowers[0]

  assert b.pos == totalBytes
  
  return b.getBytes()

class SkipReader:

  """
  Reads serialized Towers.
  """

  def __init__(self, b, inlined, skipInterval, fixedDocGap):
    self.b = b
    self.pendingDocCount = 0
    self.inlined = inlined
    self.skipInterval = skipInterval
    self.fixedDocGap = fixedDocGap
    firstTowerPos = b.pos
    self.maxNumLevels = b.readVInt()
    if VERBOSE:
      print('skipInterval %d' % self.skipInterval)
      print('%d max skip levels' % self.maxNumLevels)

    self.nextTowerLastDocIDs = []
    self.nextTowerPositions = []
    self.nextTowerDocCounts = []
    self.fixedDocGaps = []
    docGap = self.fixedDocGap
    for i in range(self.maxNumLevels):
      self.nextTowerLastDocIDs.append(0)
      self.nextTowerPositions.append(0)
      self.fixedDocGaps.append(docGap)
      self.nextTowerDocCounts.append(0)
      docGap *= self.skipInterval
    self.lastDocID = 0
    self.b.pos = 0
    self.readTower(firstTowerPos, 0, 0)

  def readTower(self, pos, lastDocID, targetDocID):
    
    if VERBOSE:
      print('READ TOWER: pos=%s lastDocID=%s' % \
            (pos, lastDocID))

    self.lastDocID = lastDocID
    self.pendingDocCount = 0

    assert pos >= self.b.pos

    self.b.seek(pos)

    isFixed = self.fixedDocGap > 0

    numLevels = self.b.readVInt()
    assert numLevels <= self.maxNumLevels, \
           'numLevels %s vs maxNumLevels %s' % (numLevels, self.maxNumLevels)
    
    if VERBOSE:
      print('  %d levels' % numLevels)

    # Towers are written highest to lowest:
    level = numLevels - 1
    while level >= 0:
      self.nextTowerPositions[level] = pos + self.b.readVLong()
      self.nextTowerLastDocIDs[level] = nextTowerLastDocID = lastDocID + self.b.readVInt()
      if isFixed:
        if level == numLevels-1:
          self.docCount = self.nextTowerDocCounts[level]
        else:
          self.nextTowerDocCounts[level] = self.docCount
        self.nextTowerDocCounts[level] += self.fixedDocGaps[level]
      if VERBOSE:
        if isFixed:
          print('  nextPos=%s nextLastDocId=%d nextDocCount=%d' % \
                (self.nextTowerPositions[level], nextTowerLastDocID, self.nextTowerDocCounts[level]))
        else:
          print('  nextPos=%s nextLastDocId=%d' % \
                (self.nextTowerPositions[level], nextTowerLastDocID))
      if nextTowerLastDocID < targetDocID:
        # Early exit: we know we will skip on this level, so don't
        # bother decoding tower entries for any levels lower:
        return level
      level -= 1

    if not isFixed:
      self.docCount = self.b.readVInt()
    if not self.inlined:
      self.pointer = self.b.readVLong()

    if VERBOSE:
      print('  docCount=%d' % self.docCount)
      if not self.inlined:
        print('  pointer=%s' % self.pointer)

    return -1

  def skipSkipData(self, count, lastDocID):
    if self.inlined:
      self.pendingDocCount += count
      if self.pendingDocCount >= self.skipInterval:
        assert self.fixedDocGap == 0 or self.fixedDocGap == self.pendingDocCount
        if VERBOSE:
          print('  now skip tower pos=%s pendingDocCount=%s' % (self.b.pos, self.pendingDocCount))
        self.readTower(self.b.pos, lastDocID, lastDocID)

  def skip(self, targetDocID):

    # First find highest skip level we could jump on:
    level = 0
    while level < self.maxNumLevels and self.nextTowerLastDocIDs[level] < targetDocID:
      if VERBOSE:
        print('  up to level %d' % (1+level))
      level += 1

    if level == 0:
      # No skipping
      return False
    else:
      level -= 1
      while True:
        # Jump on this level / move down a level:
        level = self.readTower(self.nextTowerPositions[level],
                               self.nextTowerLastDocIDs[level],
                               targetDocID)
        if level == -1:
          break
          
      return True

def makeDocs(r, count):
  docID = 0
  docs = []
  while len(docs) < count:
    inc = r.randint(1, 10)
    docID += inc
    docs.append(docID)
  return docs

class ByteBufferWriter:
  def __init__(self):
    self.reset()

  def getBytes(self):
    return ''.join(self.bytes)

  def writeBytes(self, bytes):
    self.bytes.append(bytes)
    self.pos += len(bytes)

  def writeByte(self, b):
    assert b >= 0 and b <= 255
    self.bytes.append(chr(b))
    self.pos += 1

  def reset(self):
    self.bytes = []
    self.pos = 0    

  def writeVInt(self, i):
    while i & ~0x7F != 0:
      self.writeByte((i & 0x7F) | 0x80)
      i = i >> 7
    self.writeByte(i & 0x7F)
  writeVLong = writeVInt

  def writeInt(self, i):
    self.writeBytes(struct.pack('i', i))

class ByteBufferReader:
  def __init__(self, bytes):
    self.bytes = bytes
    self.pos = 0

  def readBytes(self, numBytes):
    v = self.bytes[self.pos:self.pos+numBytes]
    self.pos += numBytes
    return v

  def seek(self, pos):
    assert type(pos) is types.IntType
    assert pos >= self.pos, 'currentPos=%s newPos=%s' % (self.pos, pos)
    self.pos = pos

  def readByte(self):
    v = self.bytes[self.pos]
    self.pos += 1
    return ord(v)

  def readVInt(self):
    b = self.readByte()
    i = b & 0x7F
    shift = 7
    while b & 0x80 != 0:
      b = self.readByte()
      i |= (b & 0x7F) << shift
      shift += 7
    return i
  readVLong = readVInt

  def readInt(self):
    return struct.unpack('i', self.readBytes(4))[0]

class WholeIntAbsCodec:
  """
  Each absolute docID is written as 4 bytes.
  """
  lastReadCount = 0

  def writeDoc(self, b, docID):
    b.writeInt(docID)

  def readDoc(self, b, lastDocID):
    self.skipper.skipSkipData(self.lastReadCount, lastDocID)
    self.lastReadCount = 1
    return b.readInt()

  def flush(self, _):
    pass

  def reset(self):
    self.lastReadCount = 0

  def afterSeek(self):
    self.lastReadCount = 0

class WholeIntDeltaCodec:

  """
  Each delta docID is written as 4 bytes.
  """

  lastDocID = 0
  lastReadCount = 0

  def writeDoc(self, b, docID):
    b.writeInt(docID-self.lastDocID)
    self.lastDocID = docID

  def readDoc(self, b, lastDocID):
    self.skipper.skipSkipData(self.lastReadCount, lastDocID)
    self.lastReadCount = 1
    return lastDocID + b.readInt()

  def flush(self, _):
    pass

  def reset(self):
    self.lastReadCount = 0

  def afterSeek(self):
    self.lastReadCount = 0

class VIntDeltaCodec:

  """
  Each delta docID is written as 4 bytes.
  """

  lastDocID = 0
  lastReadCount = 0

  def writeDoc(self, b, docID):
    delta = docID - self.lastDocID
    b.writeVInt(delta)
    self.lastDocID = docID

  def readDoc(self, b, lastDocID):
    self.skipper.skipSkipData(self.lastReadCount, lastDocID)
    self.lastReadCount = 1
    return lastDocID + b.readVInt()

  def flush(self, _):
    pass

  def reset(self):
    self.lastReadCount = 0

  def afterSeek(self):
    self.lastReadCount = 0

class FixedBlockVIntDeltaCodec:

  def __init__(self, blockSize, inlinedSkipData):
    self.blockSize = blockSize
    self.reset()
    self.inlinedSkipData = inlinedSkipData
    self.buffer = ByteBufferWriter()

  def reset(self):
    self.upto = 0
    self.pending = []
    self.lastReadCount = 0

  lastDocID = 0

  def writeDoc(self, b, docID):
    self.pending.append(docID - self.lastDocID)
    self.lastDocID = docID
    if len(self.pending) == self.blockSize:
      self.flush(b)

  def readDoc(self, b, lastDocID):
    if self.upto == len(self.pending):
      self.readBlock(b, lastDocID)
      self.upto = 0
    delta = self.pending[self.upto]
    if VERBOSE:
      print('  readDoc lastDocID=%d delta=%d' % (lastDocID, delta))
    self.upto += 1
    return lastDocID + delta

  def readBlock(self, b, lastDocID):
    if self.inlinedSkipData:
      self.skipper.skipSkipData(self.lastReadCount, lastDocID)
    if VERBOSE:
      print('  readBlock @ b.pos=%s lastDocID=%s' % (b.pos, lastDocID))
    numBytes = b.readVInt()
    if VERBOSE:
      print('    numBytes=%d' % numBytes)
    self.pending = []
    posStart = b.pos
    for _ in range(self.blockSize):
      delta = b.readVInt()
      assert delta > 0
      self.pending.append(delta)
      if VERBOSE:
        print('    delta=%d' % self.pending[-1])
    self.lastReadCount = len(self.pending)
    assert b.pos-posStart == numBytes

  def afterSeek(self):
    self.reset()

  def flush(self, b):
    # print 'flush'
    if VERBOSE:
      print('  writeBlock @ b.pos=%s' % b.pos)
    for i in self.pending:
      if VERBOSE:
        print('    delta=%d' % i)
      self.buffer.writeVInt(i)
    for i in range(len(self.pending), self.blockSize):
      # not used:
      self.buffer.writeVInt(1)
    self.pending = []
    b.writeVInt(self.buffer.pos)
    if VERBOSE:
      print('    numBytes=%d' % self.buffer.pos)
    b.writeBytes(''.join(self.buffer.bytes))
    self.buffer.reset()


class VariableBlockVIntDeltaCodec:

  def __init__(self, r, inlinedSkipData):
    self.r = r
    self.reset()
    self.inlinedSkipData = inlinedSkipData
    self.buffer = ByteBufferWriter()

  def reset(self):
    self.upto = 0
    self.pending = []
    self.blockSize = self.r.randint(1, 50)
    self.lastReadCount = 0

  lastDocID = 0

  def writeDoc(self, b, docID):
    self.pending.append(docID - self.lastDocID)
    self.lastDocID = docID
    if len(self.pending) == self.blockSize:
      self.flush(b)

  def readDoc(self, b, lastDocID):
    if self.upto == len(self.pending):
      self.readBlock(b, lastDocID)
      self.upto = 0
    delta = self.pending[self.upto]
    self.upto += 1
    return lastDocID + delta

  def readBlock(self, b, lastDocID):
    if self.inlinedSkipData:
      self.skipper.skipSkipData(self.lastReadCount, lastDocID)
    numBytes = b.readVInt()
    self.pending = []
    posEnd = b.pos + numBytes
    while b.pos < posEnd:
      delta = b.readVInt()
      assert delta > 0
      self.pending.append(delta)
    self.lastReadCount = len(self.pending)

  def afterSeek(self):
    self.reset()

  def flush(self, b):
    # print 'flush'
    for i in self.pending:
      self.buffer.writeVInt(i)
    self.pending = []
    b.writeVInt(self.buffer.pos)
    b.writeBytes(''.join(self.buffer.bytes))
    self.buffer.reset()
    self.blockSize = self.r.randint(1, 50)
    
def main():

  if '-seed' in sys.argv:
    seed = int(sys.argv[sys.argv.index('-seed')+1])
  else:
    seed = random.randint(0, sys.maxint)
  
  print('SEED %s' % seed)
  r = random.Random(seed)

  NUM_DOCS = r.randint(30000, 100000)

  if False and VERBOSE:
    NUM_DOCS = 534

  docList = makeDocs(r, NUM_DOCS)

  b = ByteBufferWriter()

  skipInterval = r.randint(2, 300)
  if False and VERBOSE:
    skipInterval = 32

  #skipInterval = 16
  #inlined = False

  sw = SkipWriter(skipInterval)

  inlined = r.randint(0, 1) == 1
  print('INLINED %s' % inlined)

  i = r.randint(0, 4)

  #i = 3
  
  if i == 0:
    codec = WholeIntAbsCodec()
  elif i == 1:
    codec = WholeIntDeltaCodec()
  elif i == 2:
    codec = VIntDeltaCodec()
  elif i == 3:
    blockSize = r.randint(2, 200)
    if False and VERBOSE:
      blockSize = 32
    #blockSize = 64
    print('blockSize %d' % blockSize)
    
    codec = FixedBlockVIntDeltaCodec(blockSize, inlined)
  else:
    codec = VariableBlockVIntDeltaCodec(r, inlined)

  print('CODEC %s' % codec)

  if isinstance(codec, VariableBlockVIntDeltaCodec):
    fixedDocGap = 0
  elif isinstance(codec, FixedBlockVIntDeltaCodec):
    fixedDocGap = blockSize * int(math.ceil(float(skipInterval) / codec.blockSize))
  else:
    fixedDocGap = skipInterval

  print('numDocs %d' % NUM_DOCS)
  print('skipInterval %d' % skipInterval)

  # Non-block coded, fixed 4 byte per docID:
  docCount = 0
  for docID in docList:
    if VERBOSE:
      print('  write docID=%d' % docID)
    oldPos = b.pos
    codec.writeDoc(b, docID)
    docCount += 1
    if b.pos != oldPos:
      # Codec wrote something.  NOTE: this simple logic fails w/
      # codecs that buffer, ie, we assume here that the codec fully
      # wrote through this last docID:
      sw.visit(docCount, docID, b.pos)

  codec.flush(b)
  postingsBytes = b.getBytes()

  sw.finish(len(postingsBytes), docCount)
  
  if inlined:
    allBytes = writeTowers(sw, fixedDocGap, postingsBytes)
    reader = ByteBufferReader(allBytes)
    skipBytes = allBytes
    skipBytesReader = reader

    pct = 100.0*(len(allBytes)-len(postingsBytes))/len(postingsBytes)
    print('  %.1f%% skip (%d skip bytes; %d postings bytes)' % \
          (pct, len(allBytes)-len(postingsBytes), len(postingsBytes)))
  else:
    skipBytes = writeTowers(sw, fixedDocGap, None)
    pct = 100.0*(len(skipBytes))/len(postingsBytes)
    print('  %.1f%% skip (%d skip bytes; %d postings bytes)' % \
          (pct, len(skipBytes), len(postingsBytes)))
    reader = ByteBufferReader(postingsBytes)
    skipBytesReader = ByteBufferReader(skipBytes)
  
  for iter in range(100):
    if VERBOSE:
      print
      print('ITER %s' % iter)
    reader.pos = 0
    skipBytesReader.pos = 0

    doSkipping = r.randint(0, 3) != 2
    if VERBOSE:
      print('  doSkipping %s' % doSkipping)
    sr = SkipReader(skipBytesReader, inlined, skipInterval, fixedDocGap)
    codec.skipper = sr
    docIDX = 0
    lastDocID = 0
    codec.reset()
    while docIDX < len(docList):

      if VERBOSE:
        print('cycle docIDX=%d of %d, pos=%s' % (docIDX, len(docList), reader.pos))

      if doSkipping and r.randint(0, 1) == 1:
        # randomly jump
        if r.randint(0, 10) == 7:
          # big jump
          targetDocID = docList[min(len(docList)-1, docIDX+r.randint(50, 2000))]
        else:
          targetDocID = docList[min(len(docList)-1, docIDX+r.randint(1, 50))]

        # TODO: also sometimes target a non-existent docID!
        
        if VERBOSE:
          print('  try jump targetDocID=%d' % targetDocID)
        if sr.skip(targetDocID):
          # did jump
          if inlined or sr.pointer >= reader.pos:
            docIDX = sr.docCount
            lastDocID = sr.lastDocID
            if not inlined:
              reader.seek(sr.pointer)
            if reader.pos >= len(reader.bytes):
              raise RuntimeError('jumped to pos=%d > length=%d' % \
                                 (reader.pos, len(reader.bytes)))
            codec.afterSeek()
            if VERBOSE:
              print('  jumped!  lastDocID=%d pointer=%s docIDX=%s' % (lastDocID, reader.pos, docIDX))

            if lastDocID >= targetDocID:
              raise RuntimeError('jumped docID=%d is >= targetDocID=%d' % (lastDocID, targetDocID))

            # Make sure we are in fact w/ in skipInterval of the target:
            assert lastDocID == docList[docIDX-1], 'lastDocID=%s expectedLastDocID=%s docIDX=%s' % \
                   (lastDocID, docList[docIDX-1], docIDX)
            
            idx = docIDX
            while docList[idx] != targetDocID:
              idx += 1

            assert fixedDocGap == 0 or idx - docIDX <= fixedDocGap, 'fixedDocGap=%d but skipper was %d away from target' % \
                   (fixedDocGap, idx - docIDX)
            del idx
            
      # nextDoc
      docID = codec.readDoc(reader, lastDocID)

      if VERBOSE:
        print('  docID=%d' % docID)

      if docID != docList[docIDX]:
        raise RuntimeError('FAILED: docID %d but expected %d' % (docID, docList[docIDX]))

      lastDocID = docID
      docIDX += 1

if __name__ == '__main__':
  if not __debug__:
    raise RuntimeError('no')
  main()
