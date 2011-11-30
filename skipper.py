# TODO
#   - assert net IO ops is really "log(N)" when we skip N
#   - see how separate frq file can be packed in too
#   - what about skipping w/in positions?
#   - handle inlining
#   - we could have arbitrary skipLevel per level...?
#   - run random stress test
#   - test delta coded fixed int
#   - test delta coded var int
#   - test block coded
#   - test inlined or not
#   - make sure we sometimes test recursion case
#   - hmm need different versions if we know the docCount will be 'regular' (eg every 128 docs)
#   - assert in the inlined case that the skip reader never 'goes
#     backward', eg, if we've nextDoc'd a number of times... and then
#     we skip... this might mean we must handle any skipData packet we
#     hit while nextDoc'ing?
#   - assert that numLevels in any tower is never more than first tower?

import sys
import random
import struct

VERBOSE = '-debug' in sys.argv

class SkipTower:

  def __init__(self, docCount, lastDocID, pointer):
    self.docCount = docCount
    self.lastDocID = lastDocID
    self.pointer = pointer
    self.nextTowers = []
    self.writePointer = 0

  def write(self, b):

    if VERBOSE:
      print 'SkipTower.write skipPos=%s numNext=%d docCount=%d pointer=%s lastDocID=%s' % \
            (b.pos, len(self.nextTowers), self.docCount, self.pointer, self.lastDocID)

    # TODO: we can avoid writing this when codec is fixed block size!:
    b.writeVInt(len(self.nextTowers))

    # TODO: can we delta-code...?
    # TODO: we can avoid writing this when codec is fixed block size!:
    b.writeVInt(self.docCount)
    b.writeVLong(self.pointer)

    for nextTower in self.nextTowers:
      if VERBOSE:
        print '  nextTower skipPos=%s nextLastDocID=%d self.lastDocID=%s' % (nextTower.writePointer, nextTower.lastDocID, self.lastDocID)
      delta = nextTower.writePointer - self.writePointer
      if delta < 0:
        delta = 1000
      b.writeVLong(delta)
      delta = nextTower.lastDocID - self.lastDocID
      b.writeVInt(delta)

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

  def write(self, inlined=False):

    # Iterate until the pointers converge:
    while True:
      tower = self.tower0
      b = ByteBufferWriter()
      writePointer = 0
      changed = False
      while True:
        if tower.writePointer != writePointer:
          changed = True
          tower.writePointer = writePointer
        # print 'tower %s, %d nextTowers' % (tower, len(tower.nextTowers))
        tower.write(b)
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

    print
    print 'FINAL WRITE'
    # Now write for real
    b = ByteBufferWriter()
    tower = self.tower0
    writePointer = 0
    while True:
      # print 'tower %s, %d nextTowers' % (tower, len(tower.nextTowers))
      tower.write(b)
      if len(tower.nextTowers) == 0:
        break
      tower = tower.nextTowers[0]

    return ''.join(b.bytes)

  def getDepth(self):
    if self.parent is None:
      return 0
    else:
      return 1 + self.parent.getDepth()
        
  def visit(self, itemCount, lastDocID, pointer):
    if itemCount - self.lastSkipItemCount >= self.skipInterval:
      if self.level == 0:
        tower = SkipTower(itemCount, lastDocID, pointer)
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

class DirectSkipReader:

  """
  Reads non-serialized Towers directly from the writer.
  """

  def __init__(self, w, level=0):
    self.tower = w.tower0
    self.level = level
    if w.parent is not None:
      self.parent = DirectSkipReader(w.parent, level+1)
    else:
      self.parent = None

  def skip(self, targetDocID):
    skipped = False
    if self.parent is not None:
      tower = self.parent.skip(targetDocID)
      if tower is not None:
        self.tower = tower
        skipped = True
    
    tower = self.tower
    while len(tower.nextTowers) > self.level:
      nextTower = tower.nextTowers[self.level]
      if nextTower.lastDocID < targetDocID:
        tower = nextTower
        skipped = True
      else:
        break
    if skipped:
      self.tower = tower
      return self.tower
    else:
      return None

class SkipReader:

  """
  Reads serialized Towers.
  """

  def __init__(self, b=None, baseSkipper=None, level=0, numLevels=None):
    self.level = level
    if level == 0:
      assert b is not None
      numLevels = b.readVInt()
      baseSkipper = self
      self.b = b
      if VERBOSE:
        print '%d skip levels' % numLevels
    self.baseSkipper = baseSkipper
    self.lastDocID = 0
    self.nextTowerPos = 0
    self.nextTowerLastDocID = 0
    if numLevels > level+1:
      self.parent = SkipReader(baseSkipper=baseSkipper, level=level+1, numLevels=numLevels)
    else:
      self.parent = None
    if level == 0:
      self.readTower(0, 0)

  def readTower(self, pos, lastDocID, left=None):
    b = self.baseSkipper.b
    
    self.lastDocID = lastDocID
    self.lastPos = pos
    if left is None:
      if VERBOSE:
        print 'READ TOWER: pos=%s lastDocID=%s' % (pos, lastDocID)
      b.seek(pos)
      numLevels = b.readVInt()
      if VERBOSE:
        print '  %d levels' % numLevels
      self.docCount = b.readVInt()
      self.pointer = b.readVLong()
      if VERBOSE:
        print '  docCount=%d' % self.docCount
        print '  pointer=%s' % self.pointer
      if numLevels == 0:
        return
      left = numLevels-1
    else:
      if VERBOSE:
        print '  recurse read tower'

    self.nextTowerPos = pos + b.readVLong()
    delta = b.readVInt()
    self.nextTowerLastDocID = lastDocID + delta
    if VERBOSE:
      print '  nextPos=%s nextLastDocId=%d' % (self.nextTowerPos, self.nextTowerLastDocID)
      
    if left != 0:
      self.parent.readTower(pos, lastDocID, left=left-1)

  def skip(self, targetDocID):
    skipped = False
    if self.parent is not None:
      skipped = self.parent.skip(targetDocID)

    while self.nextTowerLastDocID < targetDocID and self.nextTowerPos > self.baseSkipper.lastPos:
      self.baseSkipper.readTower(self.nextTowerPos, self.nextTowerLastDocID)
      skipped = True
      
    return skipped

def makeDocs(r, count):
  docID = 0
  docs = []
  while len(docs) < count:
    inc = r.randint(0, 10)
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

  def writeDoc(self, b, docID):
    b.writeInt(docID)

  def readDoc(self, b, lastDocID):
    return b.readInt()

class WholeIntDeltaCodec:

  """
  Each delta docID is written as 4 bytes.
  """

  lastDocID = 0

  def writeDoc(self, b, docID):
    b.writeInt(docID-self.lastDocID)
    self.lastDocID = docID

  def readDoc(self, b, lastDocID):
    return lastDocID + b.readInt()

class VIntDeltaCodec:

  """
  Each delta docID is written as 4 bytes.
  """

  lastDocID = 0

  def writeDoc(self, b, docID):
    delta = docID - self.lastDocID
    b.writeVInt(delta)
    self.lastDocID = docID

  def readDoc(self, b, lastDocID):
    return lastDocID + b.readVInt()

  def flush(self, b):
    pass

  def reset(self):
    pass

  def afterSeek(self):
    pass

class FixedBlockVIntDeltaCodec:

  def __init__(self, blockSize):
    self.blockSize = blockSize
    self.reset()
    self.buffer = ByteBufferWriter()

  def reset(self):
    self.upto = 0
    self.pending = []

  lastDocID = 0

  def writeDoc(self, b, docID):
    self.pending.append(docID - self.lastDocID)
    self.lastDocID = docID
    if len(self.pending) == self.blockSize:
      self.flush(b)

  def readDoc(self, b, lastDocID):
    if self.upto == len(self.pending):
      self.readBlock(b)
      self.upto = 0
    delta = self.pending[self.upto]
    self.upto += 1
    return lastDocID + delta

  def readBlock(self, b):
    # TODO: numBytes is unused...
    numBytes = b.readInt()
    self.pending = []
    for idx in xrange(self.blockSize):
      self.pending.append(b.readVInt())

  def afterSeek(self):
    self.reset()

  def flush(self, b):
    # print 'flush'
    for i in self.pending:
      self.buffer.writeVInt(i)
    for i in xrange(len(self.pending), self.blockSize):
      self.buffer.writeVInt(0)
    self.pending = []
    b.writeInt(self.buffer.pos)
    b.writeBytes(''.join(self.buffer.bytes))
    self.buffer.reset()


class VariableBlockVIntDeltaCodec:

  def __init__(self, r):
    self.r = r
    self.reset()
    self.buffer = ByteBufferWriter()

  def reset(self):
    self.upto = 0
    self.pending = []
    self.blockSize = self.r.randint(1, 50)

  lastDocID = 0

  def writeDoc(self, b, docID):
    self.pending.append(docID - self.lastDocID)
    self.lastDocID = docID
    if len(self.pending) == self.blockSize:
      self.flush(b)

  def readDoc(self, b, lastDocID):
    if self.upto == len(self.pending):
      self.readBlock(b)
      self.upto = 0
    delta = self.pending[self.upto]
    self.upto += 1
    return lastDocID + delta

  def readBlock(self, b):
    numBytes = b.readInt()
    self.pending = []
    posEnd = b.pos + numBytes
    while b.pos < posEnd:
      self.pending.append(b.readVInt())

  def afterSeek(self):
    self.reset()

  def flush(self, b):
    # print 'flush'
    for i in self.pending:
      self.buffer.writeVInt(i)
    self.pending = []
    b.writeInt(self.buffer.pos)
    b.writeBytes(''.join(self.buffer.bytes))
    self.buffer.reset()
    
def main():

  seed = random.randint(0, sys.maxint)
  if VERBOSE:
    seed = 17
  print 'SEED %s' % seed
  r = random.Random(seed)

  NUM_DOCS = r.randint(10000, 20000)

  if VERBOSE:
    NUM_DOCS = 77

  docList = makeDocs(r, NUM_DOCS)

  b = ByteBufferWriter()

  skipInterval = r.randint(2, 50)
  if VERBOSE:
    skipInterval = 3
    
  sw = SkipWriter(skipInterval)

  #codec = WholeIntAbsCodec()
  #codec = WholeIntDeltaCodec()
  #codec = VIntDeltaCodec()
  #blockSize = r.randint(2, 200)
  #print 'blockSize %d' % blockSize
  #codec = FixedBlockVIntDeltaCodec(blockSize)
  codec = VariableBlockVIntDeltaCodec(r)

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
  
  print 'WRITE not inlined:'
  skipBytes = sw.write()
  print '  %d skip bytes; %d postings bytes' % (len(skipBytes), b.pos)

  if False:
    print 'WRITE inlined:'
    sw.write(True)
  
  reader = ByteBufferReader(b.getBytes())
  skipBytes = ByteBufferReader(skipBytes)
  for iter in xrange(100):
    if VERBOSE:
      print
      print 'ITER %s' % iter
    reader.pos = 0
    #sr = DirectSkipReader(sw)
    skipBytes.pos = 0
    sr = SkipReader(skipBytes)
    docIDX = 0
    lastDocID = 0
    codec.reset()
    while docIDX < len(docList):

      if VERBOSE:
        print 'cycle docIDX=%d of %d, pos=%s' % (docIDX, len(docList), reader.pos)

      if r.randint(0, 1) == 1:
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
          docIDX = sr.docCount
          lastDocID = sr.lastDocID
          reader.pos = sr.pointer
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
