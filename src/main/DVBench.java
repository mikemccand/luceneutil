import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LogDocMergePolicy;
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
import org.apache.lucene.util.Version;


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
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_CURRENT, null);
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
    doc.add(dv);
    doc.add(inv);
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
      writer.addDocument(doc);
    }
    
    writer.shutdown();
    
    // run dv search tests
    String description = "dv (bpv=" + bpv + ")";
    DirectoryReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);
    
    int hash = 0;
    // warmup
    hash += search(description, searcher, "dv", 300, true);
    hash += search(description, searcher, "dv", 300, false);
    
    Map<String,UninvertingReader.Type> mapping = Collections.singletonMap("inv", UninvertingReader.Type.LONG);
    DirectoryReader uninv = UninvertingReader.wrap(reader, mapping);
    IndexSearcher searcher2 = new IndexSearcher(uninv);
    
    //description = "fc (bpv=" + bpv + ")";
    // warmup
    //hash += search(description, searcher2, "inv", 300, true);
    //hash += search(description, searcher2, "inv", 300, false);

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
}
