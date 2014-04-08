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

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.*;

// TODO
//  - also test GUID based IDs
//  - also test base-N (32/36) IDs

// javac -cp build/classes/java:../modules/analysis/build/common/classes/java perf/PKLookupPerfTest3X.java
// java -Xbatch -server -Xmx2g -Xms2g -cp .:build/classes/java:../modules/analysis/build/common/classes/java perf.PKLookupPerfTest3X MMapDirectory /p/lucene/indices/pk 1000000 1000 17

public class PKLookupPerfTest3X {

  // If true, we pull a DocsEnum to get the matching doc for
  // the PK; else we only verify the PK is found exactly
  // once (in one segment)
  private static boolean DO_DOC_LOOKUP = false;

  private static void createIndex(final Directory dir, final int docCount)
    throws IOException {
    System.out.println("Create index... " + docCount + " docs");
 
    final IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_35,
                                                        new WhitespaceAnalyzer(Version.LUCENE_35));
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    // 5 segs per level in 3 levels:
    int mbd = docCount/(5*111);
    iwc.setMaxBufferedDocs(mbd);
    iwc.setRAMBufferSizeMB(-1.0);
    ((TieredMergePolicy) iwc.getMergePolicy()).setUseCompoundFile(false);
    final IndexWriter w = new IndexWriter(dir, iwc);
    //w.setInfoStream(System.out);
   
    final Document doc = new Document();
    final Field field = new Field("id", "", Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
    field.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY);
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
    final TermDocs[] termDocsArr = new TermDocs[subs.length];
    for(int subIdx=0;subIdx<subs.length;subIdx++) {
      termDocsArr[subIdx] = subs[subIdx].termDocs();
    }

    final int maxDoc = r.maxDoc();
    final Random rand = new Random(seed);

    for(int cycle=0;cycle<10;cycle++) {
      System.out.println("Cycle: " + (cycle==0 ? "warm" : "test"));
      System.out.println("  Lookup...");
      final Term[] lookup = new Term[numLookups];
      final int[] docIDs = new int[numLookups];
      final Term protoTerm = new Term("id");
      for(int iter=0;iter<numLookups;iter++) {
        // Base 36, prefixed with 0s to be length 6 (= 2.2 B)
        lookup[iter] = protoTerm.createTerm(String.format("%6s", Integer.toString(rand.nextInt(maxDoc), Character.MAX_RADIX)).replace(' ', '0'));
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
          if (!DO_DOC_LOOKUP) { 
            final int df = sub.docFreq(lookup[iter]);
            if (df != 0) {
              if (df != 1) {
                // Only 1 doc should be found
                failed.set(true);
              }
              found++;
              if (found > 1) {
                // Should have been found only once across segs
                System.out.println("FAIL0");
                failed.set(true);
              }
            }
          } else {
            final TermDocs termDocs = termDocsArr[subIdx];
            termDocs.seek(lookup[iter]);
            if (termDocs.next()) {
              found++;
              if (found > 1) {
                // Should have been found only once across segs
                failed.set(true);
              }
              final int docID = termDocs.doc();
              if (docIDs[iter] != -1) {
                // Same doc should only be seen once
                failed.set(true);
              }
              docIDs[iter] = base + docID;
              if (termDocs.next()) {
                // Only 1 doc should be found
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
      System.out.println("  " + (cycle == 0 ? "WARM: " : "") + tLookup + " msec for " + numLookups + " lookups (" + (1000.0*tLookup/numLookups) + " us per lookup)");

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
          if (!found.equals(lookup[iter].text())) {
            throw new RuntimeException("lookup of docid=" + lookup[iter].text() + " hit wrong docid=" + found);
          }
        }
      }
    }

   // System.out.println("blocks=" + BlockTermsReader.totBlockReadCount + " scans=" + BlockTermsReader.totScanCount + " " + (((float) BlockTermsReader.totScanCount))/(BlockTermsReader.totBlockReadCount) + " scans/block");

    r.close();
    dir.close();
  }
}
