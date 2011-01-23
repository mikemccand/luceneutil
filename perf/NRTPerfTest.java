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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.apache.lucene.document.*;
import org.apache.lucene.util.*;

// cd /a/lucene/trunk/checkout
// ln -s /path/to/lucene/util/perf .
// ant compile; javac -cp ../modules/analysis/build/common/classes/java:build/classes/java:build/classes/test perf/NRTPerfTest.java
// java -cp .:lib/junit-4.7.jar:../modules/analysis/build/common/classes/java:build/classes/java:build/classes/test perf.NRTPerfTest MMapDirectory /lucene/indices/clean.svn.Standard.opt.nd10M/index multi /lucene/clean.svn/lucene/src/test/org/apache/lucene/util/europarl.lines.txt.gz 17 100 10 2 2 10 update

public class NRTPerfTest {

  // TODO: share w/ SearchPerfTest
  private static IndexCommit findCommitPoint(String commit, Directory dir) throws IOException {
    Collection<IndexCommit> commits = IndexReader.listCommits(dir);
    for (final IndexCommit ic : commits) {
      Map<String,String> map = ic.getUserData();
      String ud = null;
      if (map != null) {
        ud = map.get("userData");
        if (ud != null && ud.equals(commit)) {
          return ic;
        }
      }
    }
    throw new RuntimeException("could not find commit '" + commit + "'");
  }

  public static class IndexThread extends Thread {
    private final LineFileDocs docs;
    private final double docsPerSec;
    private final IndexWriter w;
    private final double runTimeSec;
    private final Random random;
    public volatile int indexedCount;
    private final boolean doUpdate;

    public IndexThread(IndexWriter w, LineFileDocs docs, double docsPerSec, double runTimeSec, Random random, boolean doUpdate) {
      this.w = w;
      this.docs = docs;
      this.docsPerSec = docsPerSec;
      this.runTimeSec = runTimeSec;
      this.random = random;
      this.doUpdate = doUpdate;
    }

    @Override
    public void run() {
      try {
        final long startNS = System.nanoTime();
        final long stopNS = startNS + (long) (runTimeSec * 1000000000);
        final int maxDoc = w.maxDoc();
        int count = 0;;
        while(true){ 
          count++;
          final Document doc = docs.nextDoc();
          if (doUpdate) {
            final String id = Integer.toString(random.nextInt(maxDoc));
            doc.add(new Field("docid", id, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO));
            w.updateDocument(new Term("docid", id), doc);
          } else {
            w.addDocument(doc);
          }
          final long t = System.nanoTime();
          if (t >= stopNS) {
            break;
          }
          final long sleepNS = startNS + (long) (1000000000*(count/docsPerSec)) - t;
          if (sleepNS > 0) {
            final long sleepMS = sleepNS/1000000;
            final int sleepNS2 = (int) (sleepNS - sleepMS*1000000);
            Thread.sleep(sleepMS, sleepNS2);
          }
        }
        indexedCount = count;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static IndexSearcher searcher;

  static synchronized IndexSearcher getSearcher() {
    searcher.getIndexReader().incRef();
    return searcher;
  }

  static synchronized void setSearcher(IndexSearcher newSearcher) throws IOException {
    if (searcher != null) {
      searcher.getIndexReader().decRef();
    }
    searcher = newSearcher;
  }

  static public void releaseSearcher(IndexSearcher s) throws IOException {
    s.getIndexReader().decRef();
  }

  public static class SearchThread extends Thread {
    private final double runTimeSec;
    public volatile int searchCount;

    public SearchThread(double runTimeSec) {
      this.runTimeSec = runTimeSec;
    }

    @Override
    public void run() {
      try {
        final long stopMS = System.currentTimeMillis() + (long) (runTimeSec*1000);
        int count = 0;
        final Query q = new TermQuery(new Term("body", "state"));
        while(System.currentTimeMillis() < stopMS) {
          IndexSearcher s = getSearcher();
          s.search(q, 10);
          releaseSearcher(s);
          count++;
          // Burn: no pause
        }
        searchCount = count;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static void main(String[] args) throws Exception {

    final Directory dir;
    final String dirImpl = args[0];
    final String dirPath = args[1];
    final String commit = args[2];
    final String lineDocFile = args[3];
    final long seed = Long.parseLong(args[4]);
    final double docsPerSec = Double.parseDouble(args[5]);
    final double runTimeSec = Double.parseDouble(args[6]);
    final int numSearchThreads = Integer.parseInt(args[7]);
    final int numIndexThreads = Integer.parseInt(args[8]);
    final double reopenPerSec = Double.parseDouble(args[9]);
    final boolean doUpdates = args[10].equals("update");

    System.out.println("DIR=" + dirImpl);
    System.out.println("Index=" + dirPath);
    System.out.println("Commit=" + commit);
    System.out.println("LineDocs=" + lineDocFile);
    System.out.println("Docs/sec=" + docsPerSec);
    System.out.println("Run time sec=" + runTimeSec);
    System.out.println("NumSearchThreads=" + numSearchThreads);
    System.out.println("NumIndexThreads=" + numIndexThreads);
    System.out.println("Reopen/sec=" + reopenPerSec);
    System.out.println("Mode=" + (doUpdates ? "updateDocument" : "addDocument"));

    final Random random = new Random(seed);
    
    final LineFileDocs docs = new LineFileDocs(null, lineDocFile);

    if (dirImpl.equals("MMapDirectory")) {
      dir = new MMapDirectory(new File(dirPath));
    } else if (dirImpl.equals("NIOFSDirectory")) {
      dir = new NIOFSDirectory(new File(dirPath));
    } else if (dirImpl.equals("SimpleFSDirectory")) {
      dir = new SimpleFSDirectory(new File(dirPath));
    } else {
      throw new RuntimeException("unknown directory impl \"" + dirImpl + "\"");
    }

    // Open an IW on the requested commit point, but, don't
    // delete other (past or future) commit points:
    final IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_40, new StandardAnalyzer(Version.LUCENE_40))
      .setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE)
      .setIndexCommit(findCommitPoint(commit, dir));
    final IndexWriter w = new IndexWriter(dir, iwc);
    //w.setInfoStream(System.out);

    // TODO: maybe more than 1 thread if we can't hit target
    // rate
    final IndexThread[] indexThreads = new IndexThread[numIndexThreads];
    for(int i=0;i<numIndexThreads;i++) {
      indexThreads[i] = new IndexThread(w, docs, docsPerSec/numIndexThreads, runTimeSec, random, doUpdates);
      indexThreads[i].setPriority(Thread.currentThread().getPriority()+1);
      indexThreads[i].start();
    }

    // Open initial reader/searcher
    IndexReader r = IndexReader.open(w);
    System.out.println("Reader=" + r);
    setSearcher(new IndexSearcher(r));
    
    final SearchThread[] searchThreads = new SearchThread[numSearchThreads];

    final long startNS = System.nanoTime();
    for(int i=0;i<numSearchThreads;i++) {
      searchThreads[i] = new SearchThread(runTimeSec);
      searchThreads[i].start();
    }

    Thread.currentThread().setPriority(2+Thread.currentThread().getPriority());
    final long startMS = System.currentTimeMillis();
    final long stopMS = startMS + (long) (runTimeSec * 1000);

    // Main thread does reopens
    int reopenCount = 1;
    while(true) {
      final long t = System.currentTimeMillis();
      if (t >= stopMS) {
        break;
      }
      final long sleepMS = startMS + (long) (1000*(reopenCount/reopenPerSec)) - System.currentTimeMillis();
      if (sleepMS > 0) {
        Thread.sleep(sleepMS);
      }

      final IndexReader newR = r.reopen();
      if (newR != r) {
        //r.close();
        setSearcher(new IndexSearcher(newR));
        r = newR;
        reopenCount++;
      } else {
        System.out.println("WARNING: no changes on reopen");
      }
    }

    int numDocs = 0;
    for(IndexThread t : indexThreads) {
      t.join();
      numDocs += t.indexedCount;
    }

    int numSearches = 0;
    for(SearchThread t : searchThreads) {
      t.join();
      numSearches += t.searchCount;
    }
    final long endNS = System.nanoTime();

    System.out.println("Ran for " + ((endNS - startNS)/1000000.0) + " ms");
    System.out.println("Indexed " + numDocs + " docs");
    System.out.println("Opened " + reopenCount + " readers");
    System.out.println("Finished " + numSearches + " searches");
    r.close();
    w.rollback();
  }
}
