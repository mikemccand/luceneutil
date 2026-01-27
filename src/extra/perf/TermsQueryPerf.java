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

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.automaton.Automata;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

// TODO
//   - sometimes mixin ids that don't exist

// rm -rf /l/tmp/idsindex; pushd core; ant jar; popd; pushd queries; ant jar; popd; javac -cp build/core/lucene-core-6.0.0-SNAPSHOT.jar:build/analysis/common/lucene-analyzers-common-6.0.0-SNAPSHOT.jar:build/queries/lucene-queries-6.0.0-SNAPSHOT.jar /l/util/src/main/perf/TermsQueryPerf.java; java -Xmx1g -Xms1g -cp /l/util/src/main:build/core/lucene-core-6.0.0-SNAPSHOT.jar:build/analysis/common/lucene-analyzers-common-6.0.0-SNAPSHOT.jar:build/queries/lucene-queries-6.0.0-SNAPSHOT.jar perf.TermsQueryPerf /l/tmp/idsindex

public class TermsQueryPerf {

  // 100M:
  static final int ID_INDEX_COUNT = 100000000;

  // Make 10 queries, each searching a random 50K:
  static final int ID_SEARCH_COUNT = 50000;
  static final int NUM_QUERIES = 10;

  public static void main(String[] args) throws Exception {

    List<BytesRef> lookupIDs = new ArrayList<>();
    Random random = new Random(17);
    double rate = 1.01 * ((double) NUM_QUERIES * ID_SEARCH_COUNT)/ID_INDEX_COUNT;

    Path indexPath = Paths.get(args[0]);

    boolean doIndex = Files.exists(indexPath) == false;

    Directory dir = FSDirectory.open(indexPath);

    if (doIndex) {
      IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
      iwc.setMergeScheduler(new SerialMergeScheduler());
      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

      // So I can walk the files and get the *.tip sizes:
      iwc.setUseCompoundFile(false);

      /// 7/7/7 segment structure:
      iwc.setMaxBufferedDocs(ID_INDEX_COUNT/777);
      iwc.setRAMBufferSizeMB(-1);

      iwc.getCodec().compoundFormat().setShouldUseCompoundFile(false);
      ((TieredMergePolicy) iwc.getMergePolicy()).setFloorSegmentMB(.001);

      IndexWriter w = new IndexWriter(dir, iwc);
      // IDIterator ids = zeroPadSequentialIDs(10);
      IDIterator ids = randomIDs(10, random);

      BytesRef idValue = new BytesRef(64);
      for(int i=0;i<ID_INDEX_COUNT;i++) {
        ids.next(idValue);
        Document doc = new Document();
        doc.add(new StringField("id", idValue, Field.Store.NO));
        w.addDocument(doc);
        if (random.nextDouble() <= rate && lookupIDs.size() < NUM_QUERIES * ID_SEARCH_COUNT) {
          lookupIDs.add(BytesRef.deepCopyOf(idValue));
        }
        if (i % 100000 == 0) {
          System.out.println(i + " docs...");
        }
      }
      w.close();
    }

    IndexReader r = DirectoryReader.open(dir);

    if (doIndex == false) {
      System.out.println("Build lookup ids");
      TermsEnum termsEnum = MultiFields.getTerms(r, "id").iterator();
      BytesRef idValue;
      while ((idValue = termsEnum.next()) != null) {
        if (random.nextDouble() <= rate && lookupIDs.size() < NUM_QUERIES * ID_SEARCH_COUNT) {
          lookupIDs.add(BytesRef.deepCopyOf(idValue));
          //System.out.println("add: " + idValue);
        }
      }
      shuffle(random, lookupIDs);
      System.out.println("Done build lookup ids");
    }

    IndexSearcher s = new IndexSearcher(r);

    if (lookupIDs.size() < NUM_QUERIES * ID_SEARCH_COUNT) {
      throw new RuntimeException("didn't get enough lookup ids: " + (NUM_QUERIES * ID_SEARCH_COUNT) + " vs " + lookupIDs.size());
    }

    List<Query> queries = new ArrayList<Query>();
    for(int i=0;i<NUM_QUERIES;i++) {

      List<BytesRef> sortedTermBytes = new ArrayList<>();
      for(BytesRef term : lookupIDs.subList(i*ID_SEARCH_COUNT, (i+1)*ID_SEARCH_COUNT)) {
        sortedTermBytes.add(term);
      }
      Collections.sort(sortedTermBytes);

      // nocommit only do this if term count is high enough?
      // nocommit: we can be more efficient here, go straight to binary:
      Query query = new AutomatonQuery(new Term("id", "manyterms"),
                                       Automata.makeStringUnion(sortedTermBytes));
      //((MultiTermQuery) query).setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
      //Query query = new TermsQuery("id", lookupIDs.subList(i*ID_SEARCH_COUNT, (i+1)*ID_SEARCH_COUNT));
      queries.add(query);
    }

    // TODO: also include construction time of queries
    long best = Long.MAX_VALUE;
    for(int iter=0;iter<100;iter++) {
      long t0 = System.nanoTime();
      long totCount = 0;
      for(int i=0;i<NUM_QUERIES;i++) {
        //Query query = new TermsQuery("id", lookupIDs.subList(i*ID_SEARCH_COUNT, (i+1)*ID_SEARCH_COUNT));
        Query query = queries.get(i);
        totCount += s.search(query, 10).totalHits;
      }
      if (totCount != NUM_QUERIES * ID_SEARCH_COUNT) {
        throw new RuntimeException("totCount=" + totCount + " but expected " + (NUM_QUERIES * ID_SEARCH_COUNT));
      }
      long t = System.nanoTime() - t0;
      System.out.println("ITER: " + iter + ": " + (t/1000000.) + " msec");
      if (t < best) {
        System.out.println("  **");
        best = t;
      }
    }

    IOUtils.close(r, dir);
  }

  private static long[] topBitByBase = new long[257];
  static {
    BigInteger x = new BigInteger(1, new byte[] {(byte) 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00});
    for(int base=2;base<257;base++) {
      topBitByBase[base] = x.divide(BigInteger.valueOf(base)).longValue();
      //System.out.println("  base=" + base + " div=" + topBitByBase[base]);
    }
  }

  // bytes must already be grown big enough!
  private static void longToBytesRef(long value, BytesRef bytes, int base) {
    //System.out.println("valueStart=" + value);
    boolean topBit = value < 0;
    value = value & 0x7fffffffffffffffL;
    //System.out.println("  topBit=" + topBit + " valueNow=" + value);
    long valueStart = value;
    bytes.length = 0;
    while (value != 0) {
      bytes.bytes[bytes.length++] = (byte) (value % base);
      value = value / base;
      if (topBit && bytes.length == 1) {
        value += topBitByBase[base];
      }
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
    boolean topBit = value < 0;
    value = value & 0x7fffffffffffffffL;
    int downTo = zeroPadDigits-1;
    while (value != 0) {
      bytes.bytes[downTo--] = (byte) (value % base);
      value = value / base;
      if (topBit && downTo == zeroPadDigits-2) {
        value += topBitByBase[base];
      }
    }
    while (downTo >= 0) {
      bytes.bytes[downTo--] = 0;
    }

    bytes.length = zeroPadDigits;
  }

  /** Generates IDs forever */
  private static abstract class IDIterator {
    public abstract void next(BytesRef result);
  }

  /** 0000, 0001, 0002, ... */
  private static IDIterator zeroPadSequentialIDs(final int base) {
    final int zeroPadDigits = getZeroPadDigits(base, ID_INDEX_COUNT-1);
    return new IDIterator() {
      int counter = 0;
      @Override
      public void next(BytesRef result) {
        longToBytesRef(counter++, result, base, zeroPadDigits);
      }
    };
  }

  private static IDIterator randomIDs(final int base, final Random random) {
    return new IDIterator() {
      @Override
      public void next(BytesRef result) {
        longToBytesRef(random.nextLong() & Long.MAX_VALUE, result, base);
      }
    };
    
  }

  private static int getZeroPadDigits(int base, long maxValue) {
    int count = 0;
    while (maxValue != 0) {
      count++;
      maxValue = maxValue / base;
    }
    return count;
  }

  /** Fisher–Yates shuffle */
  private static void shuffle(Random random, List<BytesRef> ids) {
    for (int i=ids.size() - 1; i > 0;i--) {
      int index = random.nextInt(i + 1);
      // swap
      BytesRef a = ids.get(index);
      ids.set(index, ids.get(i));
      ids.set(i, a);
    }
  }
}
