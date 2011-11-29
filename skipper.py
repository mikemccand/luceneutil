# TODO
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

import sys
import random
import struct

VERBOSE = '-debug' in sys.argv

class SkipTower:

  def __init__(self, docCount, lastDocID, pointer):
    self.docCount = docCount
    self.lastDocID = lastDocID
    self.pointer = pointer
    self.entries = []

class SkipWriter:

  def __init__(self, skipInterval, tower0=None, level=0):
    #print 'skipInterval %d' % skipInterval
    self.skipInterval = skipInterval
    self.lastSkipItemCount = 0
    if tower0 is None:
      tower0 = SkipTower(0, -1, 0)
    self.tower0 = tower0
    self.lastTower = tower0
    self.parent = None
    self.level = level
    self.numSkips = 0
    
  def visit(self, itemCount, lastDocID, pointer):
    if itemCount - self.lastSkipItemCount >= self.skipInterval:
      if self.level == 0:
        tower = SkipTower(itemCount, lastDocID, pointer)
      else:
        tower = pointer
      self.numSkips += 1
      assert len(self.lastTower.entries) == self.level
      self.lastTower.entries.append(tower)
      self.lastTower = tower
      self.lastSkipItemCount = itemCount
      if self.numSkips == self.skipInterval:
        # Lazily add another skip level:
        self.parent = SkipWriter(self.skipInterval, self.tower0, 1+self.level)

      if self.parent is not None:
        self.parent.visit(self.numSkips, lastDocID, tower)

class SkipReader:

  def __init__(self, w, level=0):
    self.tower = w.tower0
    self.level = level
    if w.parent is not None:
      self.parent = SkipReader(w.parent, level+1)
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
    while len(tower.entries) > self.level:
      nextTower = tower.entries[self.level]
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

  def writeInt(self, i):
    self.writeBytes(struct.pack('i', i))

class ByteBufferReader:
  def __init__(self, writer):
    self.bytes = ''.join(writer.bytes)
    self.pos = 0

  def readBytes(self, numBytes):
    v = self.bytes[self.pos:self.pos+numBytes]
    self.pos += numBytes
    return v

  def readByte(self):
    v = self.bytes[self.pos]
    self.pos += 1
    return ord(v)

  def readVInt(self):
    i = b = self.readByte()
    shift = 7
    while b & 0x80 != 0:
      b = self.readByte()
      i |= (b & 0x7F) << shift
      shift += 7
    return i

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
    NUM_DOCS = 55

  docList = makeDocs(r, NUM_DOCS)

  b = ByteBufferWriter()

  skipInterval = r.randint(2, 50)
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
  
  reader = ByteBufferReader(b)
  for iter in xrange(100):
    if VERBOSE:
      print
      print 'ITER %s' % iter
    reader.pos = 0
    sr = SkipReader(sw)
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
        tower = sr.skip(targetDocID)
        if tower is not None:
          # did jump
          docIDX = tower.docCount
          lastDocID = tower.lastDocID
          reader.pos = tower.pointer
          if reader.pos >= len(reader.bytes):
            raise RuntimeError('jumped to pos=%d > length=%d' % \
                               (reader.pos, len(reader.bytes)))
          codec.afterSeek()
          if VERBOSE:
            print '  jumped!  lastDocID=%d pointer=%s' % (lastDocID, reader.pos)

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
