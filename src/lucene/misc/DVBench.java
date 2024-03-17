/*
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
import java.util.Collections;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.uninverting.UninvertingReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

public class DVBench {
  public static void main(String args[]) throws Exception {
    for (int i = 1; i <= 64; i++) {
      doBench(i);
    }
  }

  static void doBench(int bpv) throws Exception {
    File file = new File("/data/indices/dvbench");
    file.mkdirs();
    Directory dir = FSDirectory.open(file);
    IndexWriterConfig config = new IndexWriterConfig(null);
    config.setOpenMode(OpenMode.CREATE);
    config.setMergeScheduler(new SerialMergeScheduler());
    config.setMergePolicy(new LogDocMergePolicy());
    config.setMaxBufferedDocs(25000);
    IndexWriter writer = new IndexWriter(dir, config);
    
    MyRandom r = new MyRandom();
    int numdocs = 400000;
    Document doc = new Document();
    Field dv = new NumericDocValuesField("dv", 0);
    Field inv = new LongField("inv", 0, Field.Store.NO);
    Field boxed = new BinaryDocValuesField("boxed", new BytesRef(8));
    Field boxed2 = new BinaryDocValuesField("boxed2", new BytesRef(8));
    
    doc.add(dv);
    doc.add(inv);
    doc.add(boxed);
    doc.add(boxed2);
    for (int i = 0; i < numdocs; i++) {
      // defeat blockpackedwriter
      final long value;
      if (i % 8192 == 0) {
        value = bpv == 64 ? Long.MIN_VALUE : 0;
      } else if (i % 8192 == 1) {
        value = bpv == 64 ? Long.MAX_VALUE : (1L<<bpv)-1;
      } else {
        value = r.nextLong(bpv);
      }
      dv.setLongValue(value);
      inv.setLongValue(value);
      box(value, boxed.binaryValue());
      box(value, boxed2.binaryValue());
      boxed2.binaryValue().length = (bpv + 7) / 8; // fixed length
      writer.addDocument(doc);
    }
    
    writer.close();
    
    // run dv search tests
    String description = "dv (bpv=" + bpv + ")";
    DirectoryReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null); // don't bench the cache
    
    int hash = 0;
    // warmup
    hash += search(description, searcher, "dv", 300, true);
    hash += search(description, searcher, "dv", 300, false);

    // Uninverting
    Map<String,UninvertingReader.Type> mapping = Collections.singletonMap("inv", UninvertingReader.Type.LONG);
    DirectoryReader uninv = UninvertingReader.wrap(reader, mapping);
    IndexSearcher searcher2 = new IndexSearcher(uninv);
    searcher2.setQueryCache(null); // don't bench the cache
    
    description = "fc (bpv=" + bpv + ")";
    // warmup
    hash += search(description, searcher2, "inv", 300, true);
    hash += search(description, searcher2, "inv", 300, false);

    // Boxed inside binary
    DirectoryReader boxedReader = new BinaryAsVLongReader(reader);
    IndexSearcher searcher3 = new IndexSearcher(boxedReader);
    searcher3.setQueryCache(null); // don't bench the cache
    description = "boxed (bpv=" + bpv + ")";
    // warmup
    hash += search(description, searcher3, "boxed", 300, true);
    hash += search(description, searcher3, "boxed", 300, false);

    description = "boxed fixed-length (bpv=" + bpv + ")";
    // warmup
    hash += search(description, searcher3, "boxed2", 300, true);
    hash += search(description, searcher3, "boxed2", 300, false);

    if (hash == 3) {
      // wont happen
      System.out.println("hash=" + hash);
    }
    reader.close();
    dir.close();
  }
  
  static long search(String description, IndexSearcher searcher, String field, int iters, boolean quiet) throws Exception {
    int count = 0;
    Query query = new MatchAllDocsQuery();
    Sort sort = new Sort(new SortField(field, SortField.Type.LONG));
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < iters; i++) {
      TopDocs td = searcher.search(query, 20, sort);
      count += td.totalHits;
    }
    long endTime = System.currentTimeMillis();
    if (!quiet) {
      double delta = endTime - startTime;
      double avg = delta / iters;
      double QPS = 1000d / avg;
      System.out.println(description + ": " + QPS);
    }
    return count;
  }
  
  // java.util.Random only returns 48bits of randomness in nextLong...
  static class MyRandom extends Random {
    byte buffer[] = new byte[8];
    ByteArrayDataInput input = new ByteArrayDataInput();
    
    public synchronized long nextLong(int bpv) {
      nextBytes(buffer);
      input.reset(buffer);
      long bits = input.readLong();
      return bits >>> (64-bpv);
    }
  }

  private static class BinaryAsVLongReader extends FilterDirectoryReader {

    public BinaryAsVLongReader(DirectoryReader in) {
      super(in, new FilterDirectoryReader.SubReaderWrapper() {
        @Override
        public AtomicReader wrap(AtomicReader reader) {
          return new BinaryAsVLongAtomicReader(reader);
        }
      });
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
      return new BinaryAsVLongReader(in);
    }

  }

  private static class BinaryAsVLongAtomicReader extends FilterAtomicReader {

    public BinaryAsVLongAtomicReader(AtomicReader in) {
      super(in);
    }

    @Override
    public NumericDocValues getNumericDocValues(String field)
        throws IOException {
      return BinaryAsVLongNumeric.of(getBinaryDocValues(field));
    }

  }

  private static class BinaryAsVLongNumeric extends NumericDocValues {

    public static NumericDocValues of(BinaryDocValues values) {
      return values == null ? null : new BinaryAsVLongNumeric(values);
    }

    private final BinaryDocValues values;
    private BinaryAsVLongNumeric(BinaryDocValues values) {
      this.values = values;
    }

    @Override
    public long get(int docID) {
      final BytesRef bytes = values.get(docID);
      return unbox(bytes);
    }
    
  }

  private static void box(long value, BytesRef bytes) {
    for (int j = 0; j < 8; ++j) {
      final long bits = value >>> (j * 8);
      if (bits == 0) {
        bytes.length = j;
        break;
      }
      bytes.bytes[j] = (byte) bits;
    }
  }

  private static long unbox(BytesRef bytes) {
    long l = 0;
    for (int i = 0; i < bytes.length; ++i) {
      l |= ((bytes.bytes[bytes.offset + i] & 0xFF) << (i * 8));
    }
    return l;
  }

}
