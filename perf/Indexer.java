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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.codecs.CoreCodecProvider;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.*;

// javac -Xlint:deprecation -cp ../modules/analysis/build/common/classes/java:build/classes/java:build/classes/test perf/Indexer.java perf/LineFileDocs.java

// Usage: dirImpl dirPath analyzer /path/to/line/file numDocs numThreads doOptimize:yes|no verbose:yes|no ramBufferMB maxBufferedDocs codec

// EG:
//
//  java -cp .:../modules/analysis/build/common/classes/java:build/classes/java:build/classes/test perf.Indexer NIOFSDirectory /lucene/indices/test StandardAnalyzer /p/lucene/data/enwiki-20110115-lines-1k-fixed.txt 1000000 6 no yes 256.0 -1 Standard

public final class Indexer {
  public static void main(String[] args) throws Exception {

    final String dirImpl = args[0];
    final String dirPath = args[1];

    final Directory dir;
    if (dirImpl.equals("MMapDirectory")) {
      dir = new MMapDirectory(new File(dirPath));
    } else if (dirImpl.equals("NIOFSDirectory")) {
      dir = new NIOFSDirectory(new File(dirPath));
    } else if (dirImpl.equals("SimpleFSDirectory")) {
      dir = new SimpleFSDirectory(new File(dirPath));
    } else {
      throw new RuntimeException("unknown directory impl \"" + dirImpl + "\"");
    }
      
    final String analyzer = args[2];
    final Analyzer a;
    if (analyzer.equals("EnglishAnalyzer")) {
      a = new EnglishAnalyzer(Version.LUCENE_31);
    } else if (analyzer.equals("ClassicAnalyzer")) {
      a = new ClassicAnalyzer(Version.LUCENE_30);
    } else if (analyzer.equals("StandardAnalyzer")) {
      a = new StandardAnalyzer(Version.LUCENE_40);
    } else {
      throw new RuntimeException("unknown analyzer " + analyzer);
    } 

    final String lineFile = args[3];

    // -1 means all docs in the line file:
    final int docCount = Integer.parseInt(args[4]);
    final int numThreads = Integer.parseInt(args[5]);

    final boolean doOptimize = args[6].equals("yes");
    final boolean verbose = args[7].equals("yes");

    final double ramBufferSizeMB = Double.parseDouble(args[8]);
    final int maxBufferedDocs = Integer.parseInt(args[9]);

    final String codec = args[10];

    System.out.println("Dir: " + dirImpl);
    System.out.println("Index path: " + dirPath);
    System.out.println("Analyzer: " + analyzer);
    System.out.println("Line file: " + lineFile);
    System.out.println("Doc count: " + (docCount == -1 ? "all docs" : ""+docCount));
    System.out.println("Threads: " + numThreads);
    System.out.println("Optimize: " + (doOptimize ? "yes" : "no"));
    System.out.println("Verbose: " + (verbose ? "yes" : "no"));
    System.out.println("RAM Buffer MB: " + ramBufferSizeMB);
    System.out.println("Max buffered docs: " + maxBufferedDocs);
    System.out.println("Codec: " + codec);

    final IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_40, a);
    iwc.setMaxBufferedDocs(maxBufferedDocs);
    iwc.setRAMBufferSizeMB(ramBufferSizeMB);

    // We want deterministic merging, since we target a multi-seg index w/ 5 segs per level:
    iwc.setMergePolicy(new LogDocMergePolicy());
    ((LogMergePolicy) iwc.getMergePolicy()).setUseCompoundFile(false);

    // Keep all commit points:
    iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);

    final CoreCodecProvider cp = new CoreCodecProvider();
    cp.setDefaultFieldCodec(codec);
    iwc.setCodecProvider(cp);

    final IndexWriter w = new IndexWriter(dir, iwc);
    w.setInfoStream(verbose ? System.out : null);

    final LineFileDocs docs = new LineFileDocs(lineFile, false);

    System.out.println("\nIndexer: start");
    final long t0 = System.currentTimeMillis();
    final Thread[] threads = new Thread[numThreads];
    for(int thread=0;thread<numThreads;thread++) {
      threads[thread] = new IndexThread(w, docs, docCount);
      threads[thread].start();
    }
    for(int thread=0;thread<numThreads;thread++) {
      threads[thread].join();
    }
    docs.close();

    final long t1 = System.currentTimeMillis();
    System.out.println("\nIndexer: indexing done (" + (t1-t0) + " msec); total " + w.maxDoc() + " docs");

    if (docCount != -1 && w.maxDoc() != docCount) {
      throw new RuntimeException("w.maxDoc()=" + w.maxDoc() + " but expected " + docCount);
    }

    w.waitForMerges();
    final long t2 = System.currentTimeMillis();
    System.out.println("\nIndexer: waitForMerges done (" + (t2-t1) + " msec)");

    final Map<String,String> commitData = new HashMap<String,String>();
    commitData.put("userData", "multi");
    w.commit(commitData);
    final long t3 = System.currentTimeMillis();
    System.out.println("\nIndexer: commit multi (took " + (t3-t2) + " msec)");

    // Randomly delete 5% of the docs
    final Set<Integer> deleted = new HashSet<Integer>();
    final int maxDoc = w.maxDoc();
    final int toDeleteCount = (int) (maxDoc * 0.05);
    System.out.println("\nIndexer: delete " + toDeleteCount + " docs");
    final Random rand = new Random(17);
    while(deleted.size() < toDeleteCount) {
      final int id = rand.nextInt(maxDoc);
      if (!deleted.contains(id)) {
        deleted.add(id);
        w.deleteDocuments(new Term("id", Integer.toString(id)));
      }
    }
    final long t4 = System.currentTimeMillis();
    System.out.println("\nIndexer: deletes done (took " + (t4-t3) + " msec)");

    commitData.put("userData", "delmulti");
    w.commit(commitData);
    final long t5 = System.currentTimeMillis();
    System.out.println("\nIndexer: commit delmulti done (took " + (t5-t4) + " msec)");

    if (w.numDocs() != maxDoc - toDeleteCount) {
      throw new RuntimeException("count mismatch: w.numDocs()=" + w.numDocs() + " but expected " + (maxDoc - toDeleteCount));
    }

    if (doOptimize) {
      w.optimize();
      final long t6 = System.currentTimeMillis();
      System.out.println("\nIndexer: optimize done (took " + (t6-t5) + " msec)");

      commitData.put("userData", "single");
      w.commit();
      final long t7 = System.currentTimeMillis();
      System.out.println("\nIndexer: commit single done (took " + (t7-t6) + " msec)");

      final int maxDoc2 = w.maxDoc();
      if (maxDoc2 != maxDoc - toDeleteCount) {
        throw new RuntimeException("count mismatch: w.maxDoc()=" + w.maxDoc() + " but expected " + (maxDoc - toDeleteCount));
      }
      final int toDeleteCount2 = (int) (maxDoc2 * 0.05);
      System.out.println("\nIndexer: delete " + toDeleteCount + " docs");
      while(deleted.size() < toDeleteCount) {
        final int id = rand.nextInt(maxDoc);
        if (!deleted.contains(id)) {
          deleted.add(id);
          w.deleteDocuments(new Term("id", Integer.toString(id)));
        }
      }
      final long t8 = System.currentTimeMillis();
      System.out.println("\nIndexer: deletes done (took " + (t8-t7) + " msec)");

      commitData.put("userData", "delsingle");
      w.commit(commitData);
      final long t9 = System.currentTimeMillis();
      System.out.println("\nIndexer: commit delsingle done (took " + (t9-t8) + " msec)");
    }

    w.close();
    dir.close();
  }

  private static class IndexThread extends Thread {
    private final LineFileDocs docs;
    private final int numTotalDocs;
    private final IndexWriter w;
    private final LineFileDocs.DocState docState;

    public IndexThread(IndexWriter w, LineFileDocs docs, int numTotalDocs) {
      this.w = w;
      this.docs = docs;
      this.docState = docs.newDocState();
      this.numTotalDocs = numTotalDocs;
    }

    @Override
    public void run() {
      final Field idField = docState.id;
      while(true) {
        try {
          final Document doc = docs.nextDoc(docState);
          if (doc == null ||
              (numTotalDocs != -1 && Integer.parseInt(idField.stringValue()) >= numTotalDocs)) {
            break;
          }
          w.addDocument(doc);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
