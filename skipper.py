# TODO
#   - hmm: if skip data is inlined, we must always init a skipper,
#     even if we will not use the skip data, so we can skip the skip
#     packets?  unless we tag them / put numbytes header
#   - add no skipping test!
#   - write backwards for faster convergence!?
#     - skipInterval=2, vint delta codec = slow
#   - interaction @ read time w/ codec is a little messy?
#   - write towers from top down not bottom up, and when skipping if
#     as soon as we read an entry we want to follow, jump; no need to
#     read lower entries in that tower
#   - assert net IO ops is really "log(N)" when we skip N
#   - see how separate frq file can be packed in too
#   - what about skipping w/in positions?
#   - we could have arbitrary skipLevel per level...?
#   - maybe we should interleave position blocks in w/ doc/freq blocks?
#   - run random stress test
#   - make sure we sometimes test recursion case
#   - hmm need different versions if we know the docCount will be 'regular' (eg every 128 docs)


# NOTE: from paper "Compressed Perfect Embedded Skip Lists for Quick Inverted-Index Lookups"
#       http://vigna.dsi.unimi.it/ftp/.../CompressedPerfectEmbeddedSkipLists.pdf

import sys
import random
import struct
import types

VERBOSE = '-debug' in sys.argv

NO_MORE_DOCS = (1 << 31) - 1

class SkipTower:

  def __init__(self, docCount, lastDocID, pointer):
    self.docCount = docCount
    self.lastDocID = lastDocID
    self.pointer = pointer
    self.nextTowers = []
    self.writePointer = 0

  def write(self, b, inlined):

    if VERBOSE:
      print 'SkipTower.write skipPos=%s numNext=%d lastDocID=%s' % \
            (b.pos, len(self.nextTowers), self.lastDocID)

    # TODO: we can avoid writing this when codec is fixed block size!:
    #       if not... we can usually use only maybe 2-3 bits?
    b.writeVInt(len(self.nextTowers))

    downTo = len(self.nextTowers)-1
    while downTo >= 0:
      nextTower = self.nextTowers[downTo]
      downTo -= 1
      if VERBOSE:
        print '  nextTower skipPos=%s nextLastDocID=%d' % (nextTower.writePointer, nextTower.lastDocID)
      # TODO: delta can be against the END of our tower, not the
      # start?  Hmm can't do fast path decode then...
      delta = nextTower.writePointer - self.writePointer
      if delta < 0:
        delta = 1000
      b.writeVLong(delta)
      delta = nextTower.lastDocID - self.lastDocID
      b.writeVInt(delta)

    # TODO: can we delta-code...?
    # TODO: we can avoid writing this when codec is fixed block size!:
    b.writeVInt(self.docCount)
    if not inlined:
      b.writeVLong(self.pointer)

class SkipWriter:

  def __init__(self, skipInterval, tower0=None, level=0):
    #print 'skipInterval %d' % skipInterval
    self.skipInterval = skipInterval
    self.lastSkipItemCount = 0
    if tower0 is None:
      tower0 = SkipTower(0, 0, 0)
      print 'TOWER0 %s' % tower0
    self.tower0 = tower0
    self.lastTower = tower0
    self.parent = None
    self.level = level
    self.numSkips = 0
    self.fixedItemGap = None

  def getDepth(self):
    if self.parent is None:
      return 0
    else:
      return 1 + self.parent.getDepth()
        
  def visit(self, itemCount, lastDocID, pointer):
    if itemCount - self.lastSkipItemCount >= self.skipInterval:
      if self.fixedItemGap is None:
        self.fixedItemGap = itemCount - self.lastSkipItemCount
      elif itemCount - self.lastSkipItemCount != self.fixedItemGap:
        self.fixedItemGap = 0
      if self.level == 0:
        tower = SkipTower(itemCount, lastDocID, pointer)
        assert pointer > self.lastTower.pointer
      else:
        tower = pointer
      self.numSkips += 1
      assert len(self.lastTower.nextTowers) == self.level
      self.lastTower.nextTowers.append(tower)
      if VERBOSE:
        if isinstance(pointer, SkipTower):
          print '%s    record skip itemCount=%d' % (self.getDepth()*'  ', itemCount)
        else:
          print '%s    record skip itemCount=%d lastDocID=%s pointer=%s' % (self.getDepth()*'  ', itemCount, lastDocID, pointer)
      self.lastTower = tower
      self.lastSkipItemCount = itemCount
      if self.numSkips == self.skipInterval:
        # Lazily add another skip level:
        self.parent = SkipWriter(self.skipInterval, self.tower0, 1+self.level)

      if self.parent is not None:
        self.parent.visit(self.numSkips, lastDocID, tower)

def writeTowers(skipWriter, postings=None):
  global VERBOSE

  print 'FIXED:'
  print '  %s' % skipWriter.fixedItemGap

  inlined = postings is not None
  sav = VERBOSE
  VERBOSE = False

  if postings is not None:
    endPostings = len(postings)
  else:
    endPostings = 0
  print '  END postings %s' % endPostings
  endTower = SkipTower(0, NO_MORE_DOCS, endPostings)

  # Add EOF markers:
  sw = skipWriter
  while sw is not None:
    sw.lastTower.nextTowers.append(endTower)
    sw = sw.parent

  err = 0
  # Iterate until the pointers converge:
  while True:
    if True or VERBOSE:
      print
      print 'WRITE: cycle [err=%s]' % err
    tower = skipWriter.tower0
    b = ByteBufferWriter()
    b.writeVInt(skipWriter.skipInterval)
    v = skipWriter.fixedItemGap
    if v is None:
      v = 0
    b.writeVInt(v)
    writePointer = b.pos
    print '  start pos=%s' % writePointer
    b.reset()
    changed = False
    err = 0
    while True:
      if tower.writePointer != writePointer:
        changed = True
        err += abs(tower.writePointer - writePointer)
        #print '%s vs %s' % (tower.writePointer, writePointer)
        tower.writePointer = writePointer
      # print 'tower %s, %d nextTowers' % (tower, len(tower.nextTowers))
      tower.write(b, inlined)
      if len(tower.nextTowers) == 0:
        break
      nextTower = tower.nextTowers[0]
      writePointer += b.pos
      if inlined:
        writePointer += nextTower.pointer - tower.pointer
      tower = nextTower
      b.reset()
    # print 'cycle: %d' % writePointer
    if not changed:
      break

  VERBOSE = sav

  print
  print 'FINAL WRITE'
  # Now write for real
  if inlined:
    pb = ByteBufferReader(postings)
  b = ByteBufferWriter()
  tower = skipWriter.tower0
  b.writeVInt(skipWriter.skipInterval)
  v = skipWriter.fixedItemGap
  if v is None:
    v = 0
  b.writeVInt(v)
  writePointer = b.pos
  while tower != endTower:
    assert b.pos == tower.writePointer, '%d vs %d' % (b.pos, tower.writePointer)
    tower.write(b, inlined)
    if len(tower.nextTowers) == 0:
      if inlined:
        chunk = pb.readBytes(len(postings)-tower.pointer)
        if VERBOSE:
          print '  write final postings chunk %d bytes pos=%s (from pointer=%d)' % (len(chunk), b.pos, pb.pos-len(chunk))
        assert len(chunk) > 0
        b.writeBytes(chunk)
      break
    if inlined:
      postingsChunk = tower.nextTowers[0].pointer - tower.pointer
      if VERBOSE:
        print '  write postings chunk %d bytes @ pos=%s (from pointer=%d)' % (postingsChunk, b.pos, pb.pos)
      assert postingsChunk > 0
      b.writeBytes(pb.readBytes(postingsChunk))

    tower = tower.nextTowers[0]

  return ''.join(b.bytes)

class SkipReader:

  """
  Reads serialized Towers.
  """

  def __init__(self, b, inlined):
    self.b = b
    self.pendingCount = 0
    self.inlined = inlined
    self.skipInterval = b.readVInt()
    self.fixedDocGap = b.readVInt()
    firstTowerPos = b.pos
    self.maxNumLevels = b.readVInt()
    if VERBOSE:
      print 'skipInterval %d' % self.skipInterval
      print ' %d max skip levels' % self.maxNumLevels

    self.nextTowers = []
    for i in xrange(self.maxNumLevels):
      self.nextTowers.append((0, 0))
    self.lastDocID = 0
    self.b.pos = 0
    self.readTower(firstTowerPos, 0, 0)

  def readTower(self, pos, lastDocID, targetDocID):
    
    if VERBOSE:
      print 'READ TOWER: pos=%s lastDocID=%s' % \
            (pos, lastDocID)

    self.lastDocID = lastDocID
    self.pendingCount = 0
    self.b.seek(pos)

    numLevels = self.b.readVInt()
    assert numLevels <= self.maxNumLevels
    if VERBOSE:
      print '  %d levels' % numLevels

    # Towers are written highest to lowest:
    nextIDX = numLevels - 1
    for idx in xrange(numLevels):
      nextTowerPos = pos + self.b.readVLong()
      nextTowerLastDocID = lastDocID + self.b.readVInt()
      self.nextTowers[nextIDX] = (nextTowerLastDocID, nextTowerPos)
      if nextTowerLastDocID < targetDocID:
        # Early exit: we know we will skip on this level, so don't
        # bother decoding tower entries for any levels lower:
        return nextIDX
      nextIDX -= 1
      if VERBOSE:
        print '  nextPos=%s nextLastDocId=%d' % (nextTowerPos, nextTowerLastDocID)

    self.docCount = self.b.readVInt()
    if not self.inlined:
      self.pointer = self.b.readVLong()

    if VERBOSE:
      print '  docCount=%d' % self.docCount
      if not self.inlined:
        print '  pointer=%s' % self.pointer

    return -1

  def skipSkipData(self, count, lastDocID):
    self.pendingCount += count
    if self.pendingCount >= self.skipInterval:
      if VERBOSE:
        print '  now skip tower pos=%s pendingCount=%s' % (self.b.pos, self.pendingCount)
      self.readTower(self.b.pos, lastDocID, lastDocID)

  def skip(self, targetDocID):

    # Up, first:
    level = 0
    while level < self.maxNumLevels and self.nextTowers[level][0] < targetDocID:
      if VERBOSE:
        print '  up to level %d' % (1+level)
      level += 1

    if level == 0:
      # no skipping
      return False
    else:
      level -= 1
      # Then down:
      while True:
        level = self.readTower(self.nextTowers[level][1],
                               self.nextTowers[level][0],
                               targetDocID)
        if level == -1 or self.nextTowers[level][0] >= targetDocID:
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

  def flush(self, b):
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

  def flush(self, b):
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

  def flush(self, b):
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
      print '  readDoc lastDocID=%d delta=%d' % (lastDocID, delta)
    self.upto += 1
    return lastDocID + delta

  def readBlock(self, b, lastDocID):
    # TODO: numBytes is unused...
    if self.inlinedSkipData:
      self.skipper.skipSkipData(self.lastReadCount, lastDocID)
    if VERBOSE:
      print '  readBlock @ b.pos=%s lastDocID=%s' % (b.pos, lastDocID)
    numBytes = b.readVInt()
    if VERBOSE:
      print '    numBytes=%d' % numBytes
    self.pending = []
    for idx in xrange(self.blockSize):
      delta = b.readVInt()
      assert delta > 0
      self.pending.append(delta)
      if VERBOSE:
        print '    delta=%d' % self.pending[-1]
    self.lastReadCount = len(self.pending)

  def afterSeek(self):
    self.reset()

  def flush(self, b):
    # print 'flush'
    if VERBOSE:
      print '  writeBlock @ b.pos=%s' % b.pos
    for i in self.pending:
      if VERBOSE:
        print '    delta=%d' % i
      self.buffer.writeVInt(i)
    for i in xrange(len(self.pending), self.blockSize):
      # not used:
      self.buffer.writeVInt(1)
    self.pending = []
    b.writeVInt(self.buffer.pos)
    if VERBOSE:
      print '    numBytes=%d' % self.buffer.pos
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
    seed = sys.argv[sys.argv.index('-seed')+1]
  else:
    seed = random.randint(0, sys.maxint)
  
  print 'SEED %s' % seed
  r = random.Random(seed)

  NUM_DOCS = r.randint(30000, 100000)

  if False and VERBOSE:
    NUM_DOCS = 534

  docList = makeDocs(r, NUM_DOCS)

  b = ByteBufferWriter()

  skipInterval = r.randint(2, 300)
  if False and VERBOSE:
    skipInterval = 32

  # nocommit
  skipInterval = 32
    
  sw = SkipWriter(skipInterval)

  inlined = r.randint(0, 1) == 1
  print 'INLINED %s' % inlined

  i = r.randint(0, 4)
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
    print 'blockSize %d' % blockSize
    codec = FixedBlockVIntDeltaCodec(blockSize, inlined)
  else:
    codec = VariableBlockVIntDeltaCodec(r, inlined)

  print 'CODEC %s' % codec

  print 'numDocs %d' % NUM_DOCS
  print 'skipInterval %d' % skipInterval

  # Non-block coded, fixed 4 byte per docID:
  docCount = 0
  for docID in docList:
    if VERBOSE:
      print '  write docID=%d' % docID
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
  
  if inlined:
    allBytes = writeTowers(sw, postingsBytes)
    reader = ByteBufferReader(allBytes)
    skipBytes = allBytes
    skipBytesReader = reader

    pct = 100.0*(len(allBytes)-len(postingsBytes))/len(postingsBytes)
    print '  %.1f%% skip (%d skip bytes; %d postings bytes)' % \
          (pct, len(allBytes)-len(postingsBytes), len(postingsBytes))
  else:
    skipBytes = writeTowers(sw)
    pct = 100.0*(len(skipBytes))/len(postingsBytes)
    print '  %.1f%% skip (%d skip bytes; %d postings bytes)' % \
          (pct, len(skipBytes), len(postingsBytes))
    reader = ByteBufferReader(postingsBytes)
    skipBytesReader = ByteBufferReader(skipBytes)
  
  for iter in xrange(100):
    if VERBOSE:
      print
      print 'ITER %s' % iter
    reader.pos = 0
    skipBytesReader.pos = 0

    doSkipping = r.randint(0, 3) != 2
    if VERBOSE:
      print '  doSkipping %s' % doSkipping
    sr = SkipReader(skipBytesReader, inlined=inlined)
    codec.skipper = sr
    docIDX = 0
    lastDocID = 0
    codec.reset()
    while docIDX < len(docList):

      if VERBOSE:
        print 'cycle docIDX=%d of %d, pos=%s' % (docIDX, len(docList), reader.pos)

      if doSkipping and r.randint(0, 1) == 1:
        # randomly jump
        if r.randint(0, 10) == 7:
          # big jump
          targetDocID = docList[min(len(docList)-1, docIDX+r.randint(50, 2000))]
        else:
          targetDocID = docList[min(len(docList)-1, docIDX+r.randint(1, 50))]
        if VERBOSE:
          print '  try jump targetDocID=%d' % targetDocID
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
              print '  jumped!  lastDocID=%d pointer=%s docIDX=%s' % (lastDocID, reader.pos, docIDX)

            if lastDocID >= targetDocID:
              raise RuntimeError('jumped docID=%d is >= targetDocID=%d' % (lastDocID, targetDocID))
        
      # nextDoc
      docID = codec.readDoc(reader, lastDocID)

      if VERBOSE:
        print '  docID=%d' % docID

      if docID != docList[docIDX]:
        raise RuntimeError('FAILED: docID %d but expected %d' % (docID, docList[docIDX]))

      lastDocID = docID
      docIDX += 1

if __name__ == '__main__':
  main()
