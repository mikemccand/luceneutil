package perf;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.codecs.BlockTreeTermsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.Version;

// TODO
//   - profile
//   - PFs/bloom
//   - measure terms index size
//   - sometimes lookup ID that doesn't exist
//   - does length matter
//   - indexing threads
//   - merge policy
//   - batch lookup vs singleton
//   - nrt reader along the way

// javac -cp /x/tmp/uuid-3.4.jar:build/core/lucene-core-4.8-SNAPSHOT.jar:build/analysis/common/lucene-analyzers-common-4.8-SNAPSHOT.jar /l/util/src/main/perf/IDPerfTest.java /l/util/src/main/perf/FlakeID.java; java -Xmx10g -Xms10g -cp /x/tmp/uuid-3.4.jar:/l/util/src/main:build/core/lucene-core-4.8-SNAPSHOT.jar:build/analysis/common/lucene-analyzers-common-4.8-SNAPSHOT.jar perf.IDPerfTest /l/scratch/indices/ids 

public class IDPerfTest {

  // 50 million:
  private static final int ID_COUNT = 100000000;

  // 2 million
  private static final int ID_SEARCH_COUNT = 2000000;

  public static void main(String[] args) throws IOException {
    String indexPath = args[0];
    Result r;

    r = testOne(indexPath, "flake", flakeIDs());
    System.out.println("  best result: " + r);

    for(int i=0;i<5;i++) {
      int base = getBase(i);
      r = testOne(indexPath, "flake, base " + base, flakeIDs(base));
      System.out.println("  best result: " + r);
    }

    r = testOne(indexPath, "uuids v1 (time, node, counter)", type1UUIDs());
    System.out.println("  best result: " + r);

    for(int i=0;i<5;i++) {
      int base = getBase(i);
      r = testOne(indexPath, "uuids v1 (time, node, counter), base " + base, type1UUIDsBase(base));
      System.out.println("  best result: " + r);
    }

    for(int i=0;i<5;i++) {
      int base = getBase(i);
      r = testOne(indexPath, "nanotime, base " + base, nanoTimeIDs(base));
      System.out.println("  best result: " + r);
    }

    for(int i=0;i<5;i++) {
      int base = getBase(i);
      r = testOne(indexPath, "simple sequential, base " + base, simpleSequentialIDs(base));
      System.out.println("  best result: " + r);
    }

    for(int i=0;i<5;i++) {
      int base = getBase(i);
      r = testOne(indexPath, "zero pad sequential, base " + base, zeroPadSequentialIDs(base));
      System.out.println("  best result: " + r);
    }

    /*
    for(int i=0;i<5;i++) {
      int base = getBase(i);
      r = testOne(indexPath, "zero pad random, base " + base, zeroPadRandomIDs(base));
      System.out.println("  best result: " + r);
    }
    */

    r = testOne(indexPath, "uuids v4 (random)", javaUUIDs());
    System.out.println("  best result: " + r);

    for(int i=0;i<5;i++) {
      int base = getBase(i);
      r = testOne(indexPath, " uuids v4 (random), base " + base, javaUUIDsBase(base));
      System.out.println("  best result: " + r);
    }
  }

  private static int getBase(int i) {
    int base;
    if (i == 0) {
      base = 10;
    } else if (i == 1) {
      base = 16;
    } else if (i == 2) {
      base = 36;
    } else if (i == 3) {
      base = 64;
    } else if (i == 4) {
      base = 256;
    } else {
      throw new RuntimeException("missing base");
    }
    return base;
  }


  /** Generates IDs forever */
  private static abstract class IDIterator {
    public abstract void next(BytesRef result);
  }

  private static IDIterator flakeIDs() {
    final FlakeID flakeID = new FlakeID();
    return new IDIterator() {
      @Override
      public void next(BytesRef result) {
        result.copyChars(flakeID.getStringId());
      }
    };
  }

  private static IDIterator flakeIDs(final int base) {
    final FlakeID flakeID = new FlakeID();
    return new IDIterator() {
      private final BytesRef scratch = new BytesRef(64);
      @Override
      public void next(BytesRef result) {
        FlakeID.TwoLongs uuid = flakeID.getTwoLongsId();
        longToBytesRef(uuid.high, result, base);
        longToBytesRef(uuid.low, scratch, base);
        result.append(scratch);
      }
    };
  }

  private static IDIterator nanoTimeIDs(final int base) {
    return new IDIterator() {
      @Override
      public void next(BytesRef result) {
        longToBytesRef(System.nanoTime(), result, base);
      }
    };
  }

  private static IDIterator javaUUIDs() {
    return new IDIterator() {
      @Override
      public void next(BytesRef result) {
        result.copyChars(UUID.randomUUID().toString());
      }
    };
  }

  // uuid-3.4.jar from http://johannburkard.de/software/uuid/
  private static IDIterator type1UUIDs() {
    return new IDIterator() {
      @Override
      public void next(BytesRef result) {
        result.copyChars(new com.eaio.uuid.UUID().toString());
      }
    };
  }

  private static IDIterator type1UUIDsBase(final int base) {
    return new IDIterator() {

      private final BytesRef scratch = new BytesRef(64);

      @Override
      public void next(BytesRef result) {
        com.eaio.uuid.UUID uuid = new com.eaio.uuid.UUID();
        longToBytesRef(uuid.getTime(), result, base);
        longToBytesRef(uuid.getClockSeqAndNode(), scratch, base);
        result.append(scratch);
      }
    };
  }

  private static IDIterator javaUUIDsBase(final int base) {
    return new IDIterator() {

      private final BytesRef scratch = new BytesRef(64);

      @Override
      public void next(BytesRef result) {
        UUID uuid = UUID.randomUUID();
        longToBytesRef(uuid.getMostSignificantBits(), result, base);
        longToBytesRef(uuid.getLeastSignificantBits(), scratch, base);
        result.append(scratch);
      }
    };
  }

  /** 0000, 0001, 0002, ... */
  private static IDIterator zeroPadSequentialIDs(final int base) {
    final int zeroPadDigits = getZeroPadDigits(base, ID_COUNT-1);

    return new IDIterator() {
      int counter = 0;
      @Override
      public void next(BytesRef result) {
        longToBytesRef(counter++, result, base, zeroPadDigits);
      }
    };
  }

  /** Random digits, zero padded */
  private static IDIterator zeroPadRandomIDs(final int base) {
    final int zeroPadDigits = getZeroPadDigits(base, Long.MAX_VALUE);

    final Random random = new Random(23);
    return new IDIterator() {
      @Override
      public void next(BytesRef result) {
        longToBytesRef(random.nextLong() & 0x7fffffffffffffffL, result, base, zeroPadDigits);
      }
    };
  }

  // 0, 1, 2, 3...
  private static IDIterator simpleSequentialIDs(final int base) {
    return new IDIterator() {
      int counter = 0;
      @Override
      public void next(BytesRef result) {
        longToBytesRef(counter++, result, base);
      }
    };
  }

  private static Result testOne(String indexPath, String desc, IDIterator ids) throws IOException {
    System.out.println("\ntest: " + desc);
    Directory dir = FSDirectory.open(new File(indexPath));
    IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_48, new StandardAnalyzer(Version.LUCENE_48));
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    // So I can walk the files and get the *.tip sizes:
    iwc.setUseCompoundFile(false);

    /// 7/7/7 segment structure:
    iwc.setMaxBufferedDocs(ID_COUNT/777);
    iwc.setRAMBufferSizeMB(-1);
    //iwc.setInfoStream(new PrintStreamInfoStream(System.out));
    ((TieredMergePolicy) iwc.getMergePolicy()).setFloorSegmentMB(.1);
    ((TieredMergePolicy) iwc.getMergePolicy()).setNoCFSRatio(0.0);

    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();

    FieldType ft = new FieldType(StringField.TYPE_NOT_STORED);
    ft.setTokenized(true);
    ft.freeze();

    BytesRef idValue = new BytesRef(64);
    Field idField = new Field("id", new BinaryTokenStream(idValue), ft);
    doc.add(idField);

    long t0 = System.nanoTime();
    BytesRef[] lookupIDs = new BytesRef[ID_SEARCH_COUNT];
    Random random = new Random(17);
    int lookupCount = 0;
    double rate = 1.001 * ((double) ID_SEARCH_COUNT)/ID_COUNT;
    for(int i=0;i<ID_COUNT;i++) {
      ids.next(idValue);
      if (lookupCount < lookupIDs.length && random.nextDouble() <= rate) {
        lookupIDs[lookupCount++] = BytesRef.deepCopyOf(idValue);
      }
      // Trickery: the idsIter changed the idValue which the BinaryTokenStream reuses for each added doc
      w.addDocument(doc);
    }

    if (lookupCount < lookupIDs.length) {
      throw new RuntimeException("didn't get enough lookup ids");
    }

    long indexTime = System.nanoTime()-t0;

    System.out.println("  indexing done; waitForMerges...");
    w.waitForMerges();

    IndexReader r = DirectoryReader.open(w, true);
    System.out.println("  reader=" + r);

    shuffle(random, lookupIDs);
    shuffle(random, lookupIDs);

    long bestTime = Long.MAX_VALUE;
    long checksum = 0;

    List<AtomicReaderContext> leaves = r.leaves();
    TermsEnum[] termsEnums = new TermsEnum[leaves.size()];
    DocsEnum[] docsEnums = new DocsEnum[leaves.size()];
    int[] docBases = new int[leaves.size()];
    for(int i=0;i<leaves.size();i++) {
      termsEnums[i] = leaves.get(i).reader().fields().terms("id").iterator(null);
      docBases[i] = leaves.get(i).docBase;
    }

    BlockTreeTermsReader.seekExactFastNotFound = 0;
    long rawLookupCount = 0;

    int countx = 0;
    for(int iter=0;iter<5;iter++) {
      t0 = System.nanoTime();
      for(BytesRef id : lookupIDs) {
        if (countx++ < 20) {
          System.out.println("    id=" + id);
        }
        boolean found = false;
        for(int seg=0;seg<termsEnums.length;seg++) {
          rawLookupCount++;
          if (termsEnums[seg].seekExact(id)) {
            docsEnums[seg] = termsEnums[seg].docs(null, docsEnums[seg], 0);
            int docID = docsEnums[seg].nextDoc();
            if (docID == DocsEnum.NO_MORE_DOCS) {
              // uh-oh!
              throw new RuntimeException("id not found");
            }
            // paranoia:
            checksum += docID + docBases[seg];

            found = true;

            // Optimization vs MultiFields: we don't need to check any more segments since id is PK
            break;
          }
        }
        if (found == false) {
          // uh-oh!
          throw new RuntimeException("id not found");
        }
      }
      long lookupTime = System.nanoTime() - t0;
      System.out.println(String.format(Locale.ROOT, "  iter=" + iter + " lookupTime=%.3f sec", lookupTime/1000000000.0));
      if (lookupTime < bestTime) {
        bestTime = lookupTime;
        System.out.println("    **");
      }
    }

    r.close();
    w.close();

    long totalBytes = 0;
    long termsIndexTotalBytes = 0;
    for(String fileName : dir.listAll()) { 
      long bytes = dir.fileLength(fileName);
      totalBytes += bytes;
      if (fileName.endsWith(".tip")) {
        termsIndexTotalBytes += bytes;
      }
    }
    dir.close();

    return new Result(desc,
                      ID_COUNT / (indexTime/1000000.0),
                      lookupIDs.length / (bestTime/1000000.0),
                      totalBytes,
                      termsIndexTotalBytes,
                      checksum,
                      BlockTreeTermsReader.seekExactFastNotFound,
                      rawLookupCount);
  }

  private static class Result {
    final String desc;
    final double indexKPS;
    final double lookupKPS;
    final long indexSizeBytes;
    final long termsIndexSizeBytes;
    final long checksum;
    final long rawLookupsNoSeek;
    final long rawLookups;

    public Result(String desc, double indexKPS, double lookupKPS, long indexSizeBytes, long termsIndexSizeBytes, long checksum, long rawLookupsNoSeek, long rawLookups) {
      this.desc = desc;
      this.indexKPS = indexKPS;
      this.lookupKPS = lookupKPS;
      this.indexSizeBytes = indexSizeBytes;
      this.termsIndexSizeBytes = termsIndexSizeBytes;
      this.checksum = checksum;
      this.rawLookupsNoSeek = rawLookupsNoSeek;
      this.rawLookups = rawLookups;
    }

    @Override
    public String toString() {
      return String.format(Locale.ROOT, "%s: lookup=%.1fK IDs/sec indexing=%.1fK IDs/sec index=%.1f MB termsIndex=%.1f MB checksum=%d no-seek lookups=%d of %d (%.1f%%)",
                           desc,
                           lookupKPS,
                           indexKPS,
                           indexSizeBytes/1024/1024.,
                           termsIndexSizeBytes/1024/1024.,
                           checksum,
                           rawLookupsNoSeek,
                           rawLookups,
                           (100.*rawLookupsNoSeek)/rawLookups);
    }
  }

  private static final class BinaryTokenStream extends TokenStream {
    private final ByteTermAttribute bytesAtt = addAttribute(ByteTermAttribute.class);
    private final BytesRef bytes;
    private boolean available = true;
  
    public BinaryTokenStream(BytesRef bytes) {
      this.bytes = bytes;
    }
  
    @Override
    public boolean incrementToken() {
      if (available) {
        clearAttributes();
        available = false;
        bytesAtt.setBytesRef(bytes);
        return true;
      }
      return false;
    }
  
    @Override
    public void reset() {
      available = true;
    }

    public interface ByteTermAttribute extends TermToBytesRefAttribute {
      public void setBytesRef(BytesRef bytes);
    }
  
    public static class ByteTermAttributeImpl extends AttributeImpl implements ByteTermAttribute,TermToBytesRefAttribute {
      private BytesRef bytes;
    
      @Override
      public void fillBytesRef() {
        // no-op: the bytes was already filled by our owner's incrementToken
      }
    
      @Override
      public BytesRef getBytesRef() {
        return bytes;
      }

      @Override
      public void setBytesRef(BytesRef bytes) {
        this.bytes = bytes;
      }
    
      @Override
      public void clear() {}
    
      @Override
      public void copyTo(AttributeImpl target) {
        ByteTermAttributeImpl other = (ByteTermAttributeImpl) target;
        other.bytes = bytes;
      }
    }
  }

  // bytes must already be grown big enough!
  private static void longToBytesRef(long value, BytesRef bytes, int base) {
    long valueStart = value;
    bytes.length = 0;
    while (value != 0) {
      bytes.bytes[bytes.length++] = (byte) (value % base);
      value = value / base;
    }

    // Reverse in place
    for(int i=0;i<bytes.length/2;i++) {
      byte x = bytes.bytes[i];
      bytes.bytes[i] = bytes.bytes[bytes.length-i-1];
      bytes.bytes[bytes.length-i-1] = x;
    }
    //System.out.println(valueStart + " -> " + bytes);
  }

  // bytes must already be grown big enough!
  private static void longToBytesRef(long value, BytesRef bytes, int base, int zeroPadDigits) {
    if (value < 0) {
      throw new IllegalArgumentException("value=" + value + " is not >= 0; base=" + base);
    }
    int downTo = zeroPadDigits-1;
    while (value != 0) {
      bytes.bytes[downTo--] = (byte) (value % base);
      value = value / base;
    }
    while (downTo >= 0) {
      bytes.bytes[downTo--] = 0;
    }

    bytes.length = zeroPadDigits;
  }

  /** Fisher–Yates shuffle */
  private static void shuffle(Random random, BytesRef[] ids) {
    for (int i=ids.length - 1; i > 0;i--) {
      int index = random.nextInt(i + 1);
      // swap
      BytesRef a = ids[index];
      ids[index] = ids[i];
      ids[i] = a;
    }
  }

  private static int getZeroPadDigits(int base, long maxValue) {
    int count = 0;
    while (maxValue != 0) {
      count++;
      maxValue = maxValue / base;
    }
    return count;
  }
}
