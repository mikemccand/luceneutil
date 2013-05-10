
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
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.BlockTreeTermsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;

// javac -cp ../build/core/classes/java:../build/analysis/common/classes/java /l/util.trunk/perf/TestBenchNRTPKLookup.java 
// java -cp .:../build/core/classes/java:../build/analysis/common/classes/java:/l/util.trunk/perf TestBenchNRTPKLookup /tmp/index
public class TestBenchNRTPKLookup {

  public static void main(String[] args) throws IOException {
    Directory dir = new MMapDirectory(new File(args[0]));
    //Directory dir = new NIOFSDirectory(new File(args[0]));
    IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_50,
        new StandardAnalyzer(Version.LUCENE_50));
    iwc.setRAMBufferSizeMB(250);
    IndexWriter writer = new IndexWriter(dir, iwc);
    final SearcherManager manager = new SearcherManager(writer, true, new SearcherFactory() {
        @Override
        public IndexSearcher newSearcher(IndexReader r) {
          return new IndexSearcher(r);
        }
      });
    FieldType type = new FieldType();
    type.setIndexed(true);
    type.setTokenized(false);
    type.setStored(false);
    type.freeze();

    HashMap<Object, TermsEnum> cachedTermsEnum = new HashMap<Object,TermsEnum>();
    long time = System.currentTimeMillis();
    long lastTime = time;
    int num = 2500000;
    Random r = new Random(16);
    for (int i = 0; i < num; i++) {
      //Term t = new Term("_id", Integer.toString(i));
      String id = String.format("%010d", r.nextInt(Integer.MAX_VALUE));
      Term t = new Term("_id", id);
      IndexSearcher acquire = manager.acquire();
      try {
        IndexReader indexReader = acquire.getIndexReader();
        List<AtomicReaderContext> leaves = indexReader.leaves();
        for (AtomicReaderContext atomicReaderContext : leaves) {
          AtomicReader reader = atomicReaderContext.reader();
          TermsEnum termsEnum = cachedTermsEnum.get(reader.getCombinedCoreAndDeletesKey());
          if (termsEnum == null) {
            termsEnum = reader.fields().terms("_id").iterator(null);
            //cachedTermsEnum.put(reader.getCombinedCoreAndDeletesKey(), termsEnum); // uncomment this line to see improvements
          }
          // MKM
          //System.out.println("\nlookup seg=: " + reader + " term=" + t);
          if (termsEnum.seekExact(t.bytes(), false)) {
            DocsEnum termDocsEnum = termsEnum.docs(reader.getLiveDocs(), null);
            if (termDocsEnum != null) {
              break;
            }
          }
        }
      } finally {
        manager.release(acquire);
      }
      Document d = new Document();

      d.add(new Field("_id", id, type));
      writer.updateDocument(t, d);
      //writer.addDocument(d);
      if (i % 50000 == 0) {
        long t1 = System.currentTimeMillis();
        System.out.println(i + " " + (t1 - lastTime) + " ms");
        lastTime = t1;
      }
      if ((i+1) % 250000 == 0) {
        System.out.println("Reopen...");
        manager.maybeRefresh();
        IndexSearcher s = manager.acquire();
        try {
          System.out.println("  got: " + s);
        } finally {
          manager.release(s);
        }
      }
    }

    System.out.println("\nTotal: " + (System.currentTimeMillis() - time) + " msec");
    //System.out.println("loadBlockCount: " + BlockTreeTermsReader.loadBlockCount);

    manager.close();
    writer.close();


    dir.close();
  }
}
