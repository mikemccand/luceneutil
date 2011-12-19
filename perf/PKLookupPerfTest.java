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
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.Version;

// TODO
//  - also test GUID based IDs
//  - also test base-N (32/36) IDs

// javac -cp build/classes/java:../modules/analysis/build/common/classes/java perf/PKLookupPerfTest.java
// java -Xbatch -server -Xmx2g -Xms2g -cp .:build/classes/java:../modules/analysis/build/common/classes/java perf.PKLookupPerfTest MMapDirectory /p/lucene/indices/pk 1000000 1000 17

public class PKLookupPerfTest {

  // If true, we pull a DocsEnum to get the matching doc for
  // the PK; else we only verify the PK is found exactly
  // once (in one segment)
  private static boolean DO_DOC_LOOKUP = true;

  private static void createIndex(final Directory dir, final int docCount)
    throws IOException {
    System.out.println("Create index... " + docCount + " docs");
 
    final IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_40,
                                                        new WhitespaceAnalyzer(Version.LUCENE_40));
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

    final Codec codec = new Lucene40Codec() {
      @Override
      public PostingsFormat getPostingsFormatForField(String field) {
        return PostingsFormat.forName("Pulsing40");
      }
    };

    iwc.setCodec(codec);
    // 5 segs per level in 3 levels:
    int mbd = docCount/(5*111);
    iwc.setMaxBufferedDocs(mbd);
    iwc.setRAMBufferSizeMB(-1.0);
    ((LogMergePolicy) iwc.getMergePolicy()).setUseCompoundFile(false);
    final IndexWriter w = new IndexWriter(dir, iwc);
    InfoStream.setDefault(new PrintStreamInfoStream(System.out));
   
    final Document doc = new Document();
    final Field field = new Field("id", "", StringField.TYPE_STORED);
    doc.add(field);

    for(int i=0;i<docCount;i++) {
      field.setValue(String.format("%09d", i));
      w.addDocument(doc);
      if ((i+1)%1000000 == 0) {
        System.out.println((i+1) + "...");
      }
    }
    w.waitForMerges();
    w.close();
  }

  public static void main(String[] args) throws IOException {

    final Directory dir;
    final String dirImpl = args[0];
    final String dirPath = args[1];
    final int numDocs = Integer.parseInt(args[2]);
    final int numLookups = Integer.parseInt(args[3]);
    final long seed = Long.parseLong(args[4]);

    if (dirImpl.equals("MMapDirectory")) {
      dir = new MMapDirectory(new File(dirPath));
    } else if (dirImpl.equals("NIOFSDirectory")) {
      dir = new NIOFSDirectory(new File(dirPath));
    } else if (dirImpl.equals("SimpleFSDirectory")) {
      dir = new SimpleFSDirectory(new File(dirPath));
    } else {
      throw new RuntimeException("unknown directory impl \"" + dirImpl + "\"");
    }

    if (!new File(dirPath).exists()) {
      createIndex(dir, numDocs);
    }

    final IndexReader r = IndexReader.open(dir);
    System.out.println("Reader=" + r);

    final IndexReader[] subs = r.getSequentialSubReaders();
    final DocsEnum[] docsEnums = new DocsEnum[subs.length];
    final TermsEnum[] termsEnums = new TermsEnum[subs.length];
    for(int subIdx=0;subIdx<subs.length;subIdx++) {
      termsEnums[subIdx] = subs[subIdx].fields().terms("id").iterator(null);
    }

    final int maxDoc = r.maxDoc();
    final Random rand = new Random(seed);

    for(int cycle=0;cycle<10;cycle++) {
      System.out.println("Cycle: " + (cycle==0 ? "warm" : "test"));
      System.out.println("  Lookup...");
      final BytesRef[] lookup = new BytesRef[numLookups];
      final int[] docIDs = new int[numLookups];
      for(int iter=0;iter<numLookups;iter++) {
        lookup[iter] = new BytesRef(String.format("%09d", rand.nextInt(maxDoc)));
      }
      Arrays.fill(docIDs, -1);

      final AtomicBoolean failed = new AtomicBoolean(false);

      final Term t = new Term("id", "");

      final long tStart = System.currentTimeMillis();
      for(int iter=0;iter<numLookups;iter++) {
        //System.out.println("lookup " + lookup[iter].utf8ToString());
        int base = 0;
        int found = 0;
        for(int subIdx=0;subIdx<subs.length;subIdx++) {
          final IndexReader sub = subs[subIdx];
          final TermsEnum termsEnum = termsEnums[subIdx];
          if (termsEnum.seekExact(lookup[iter], false)) { 
            if (DO_DOC_LOOKUP) {
              final DocsEnum docs = docsEnums[subIdx] = termsEnum.docs(null, docsEnums[subIdx], false);
              final int docID = docs.nextDoc();
              if (docID == DocsEnum.NO_MORE_DOCS) {
                failed.set(true);
              }
              if (docIDs[iter] != -1) {
                failed.set(true);
              }
              docIDs[iter] = base + docID;
              if (docs.nextDoc() != DocsEnum.NO_MORE_DOCS) {
                failed.set(true);
              }
            } else {
              found++;
              if (found > 1) {
                failed.set(true);
              }
            }
          }
          base += sub.maxDoc();
        }
      }
      final long tLookup = (System.currentTimeMillis() - tStart);
      
      // cycle 0 is for warming
      //System.out.println("  " + (cycle == 0 ? "WARM: " : "") + tLookup + " msec for " + numLookups + " lookups (" + (1000*tLookup/numLookups) + " us per lookup) + totSeekMS=" + (BlockTermsReader.totSeekNanos/1000000.));
      System.out.println("  " + (cycle == 0 ? "WARM: " : "") + tLookup + " msec for " + numLookups + " lookups (" + (1000*tLookup/numLookups) + " us per lookup)");

      if (failed.get()) {
        throw new RuntimeException("at least one lookup produced more than one result");
      }

      if (DO_DOC_LOOKUP) {
        System.out.println("  Verify...");
        for(int iter=0;iter<numLookups;iter++) {
          if (docIDs[iter] == -1) {
            throw new RuntimeException("lookup of " + lookup[iter] + " failed iter=" + iter);
          }
          final String found = r.document(docIDs[iter]).get("id");
          if (!found.equals(lookup[iter].utf8ToString())) {
            throw new RuntimeException("lookup of docid=" + lookup[iter].utf8ToString() + " hit wrong docid=" + found);
          }
        }
      }
    }

   // System.out.println("blocks=" + BlockTermsReader.totBlockReadCount + " scans=" + BlockTermsReader.totScanCount + " " + (((float) BlockTermsReader.totScanCount))/(BlockTermsReader.totBlockReadCount) + " scans/block");

    r.close();
    dir.close();
  }
}
