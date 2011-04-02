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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.apache.lucene.document.*;
import org.apache.lucene.util.Version;

// cd /a/lucene/trunk/checkout
// ln -s /path/to/lucene/util/perf .
// ant compile; javac -cp ../modules/analysis/build/common/classes/java:build/classes/java:build/classes/test perf/NRTPerfTest.java perf/LineFileDocs.java
// java -Xmx2g -Xms2g -server -Xbatch -cp .:lib/junit-4.7.jar:../modules/analysis/build/common/classes/java:build/classes/java:build/classes/test perf.NRTPerfTest MMapDirectory /lucene/indices/clean.svn.Standard.opt.nd24.9005M/index multi /lucene/clean.svn/lucene/src/test/org/apache/lucene/util/europarl.lines.txt.gz 17 100 10 2 2 10 update 1

public class NRTPerfTest {

  private static boolean NEW_INDEX = false;

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
    private final LineFileDocs.DocState docState;

    public IndexThread(IndexWriter w, LineFileDocs docs, double docsPerSec, double runTimeSec, Random random, boolean doUpdate) {
      this.w = w;
      this.docs = docs;
      docState = docs.newDocState();
      this.docsPerSec = docsPerSec;
      this.runTimeSec = runTimeSec;
      this.random = new Random(random.nextInt());
      this.doUpdate = doUpdate;
    }

    @Override
    public void run() {
      try {
        final long startNS = System.nanoTime();
        final long stopNS = startNS + (long) (runTimeSec * 1000000000);
        //System.out.println("IW.maxDoc=" + maxDoc);
        int count = 0;;
        while(true) {
          count++;
          int maxDoc = w.maxDoc();
          final Document doc = docs.nextDoc(docState);
          //System.out.println("maxDoc=" + maxDoc + " vs " + doc.get("docid"));
          if (doUpdate && (!NEW_INDEX || (maxDoc > 0 && random.nextInt(4) != 2))) {
            final String id = String.format("%09d", random.nextInt(maxDoc));
            docState.id.setValue(id);
            w.updateDocument(new Term("id", id), doc);
          } else {
            w.addDocument(doc);
          }
          final long t = System.nanoTime();
          if (docsIndexedByTime != null) {
            int qt = (int) ((t-startNS)/statsEverySec/1000000000);
            docsIndexedByTime[qt].incrementAndGet();
          }
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
        final long startMS = System.currentTimeMillis();
        final long stopMS = startMS + (long) (runTimeSec*1000);
        int count = 0;
        while(true) {
          final long t = System.currentTimeMillis();
          if (t >= stopMS) {
            break;
          }
          for(Query query: queries) {
            int qt = (int) ((t-startMS)/statsEverySec/1000);
            IndexSearcher s = getSearcher();
            try {
              s.search(query, 10);
            } finally {
              releaseSearcher(s);
            }
            searchesByTime[qt].addAndGet(queries.length);
          }
          count += queries.length;
          // Burn: no pause
        }
        searchCount = count;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  static AtomicInteger[] docsIndexedByTime;
  static AtomicInteger[] searchesByTime;
  static int statsEverySec;

  static Query[] queries;

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
    statsEverySec = Integer.parseInt(args[11]);
    final boolean doCommit = args[12].equals("yes");

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

    System.out.println("Record stats every " + statsEverySec + " seconds");
    final int count = (int) ((runTimeSec / statsEverySec) + 2);
    docsIndexedByTime = new AtomicInteger[count];
    searchesByTime = new AtomicInteger[count];
    final AtomicInteger reopensByTime[] = new AtomicInteger[count];
    for(int i=0;i<count;i++) {
      docsIndexedByTime[i] = new AtomicInteger();
      searchesByTime[i] = new AtomicInteger();
      reopensByTime[i] = new AtomicInteger();
    }

    final Random random = new Random(seed);
    
    final LineFileDocs docs = new LineFileDocs(lineDocFile, true);

    if (dirImpl.equals("MMapDirectory")) {
      dir = new MMapDirectory(new File(dirPath));
    } else if (dirImpl.equals("NIOFSDirectory")) {
      dir = new NIOFSDirectory(new File(dirPath));
    } else if (dirImpl.equals("SimpleFSDirectory")) {
      dir = new SimpleFSDirectory(new File(dirPath));
    } else {
      throw new RuntimeException("unknown directory impl \"" + dirImpl + "\"");
    }

    queries = new Query[50];
    for(int idx=0;idx<queries.length;idx++) {
      queries[idx] = new TermQuery(new Term("body", ""+idx));
    }

    // Open an IW on the requested commit point, but, don't
    // delete other (past or future) commit points:
    final IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_40, new StandardAnalyzer(Version.LUCENE_40))
      .setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE).setRAMBufferSizeMB(256.0);

    //iwc.setMergePolicy(new LogByteSizeMergePolicy());
    //((LogMergePolicy) iwc.getMergePolicy()).setUseCompoundFile(false);

    iwc.setMergePolicy(new TieredMergePolicy());
    ((TieredMergePolicy) iwc.getMergePolicy()).setUseCompoundFile(false);

    if (!commit.equals("none")) {
      iwc.setIndexCommit(findCommitPoint(commit, dir));
    }

    // Make sure merges run @ higher prio than indexing:
    ((ConcurrentMergeScheduler) iwc.getMergeScheduler()).setMergeThreadPriority(Thread.currentThread().getPriority()+2);

    iwc.setMergedSegmentWarmer(new IndexWriter.IndexReaderWarmer() {
        @Override
        public void warm(IndexReader reader) throws IOException {
          //System.out.println("DO WARM: " + reader.maxDoc());
          IndexSearcher s = new IndexSearcher(reader);
          for(Query query : queries) {
            s.search(query, 10);
          }
        }
      });

    final IndexWriter w = new IndexWriter(dir, iwc);
    //w.setInfoStream(System.out);

    final IndexThread[] indexThreads = new IndexThread[numIndexThreads];
    for(int i=0;i<numIndexThreads;i++) {
      indexThreads[i] = new IndexThread(w, docs, docsPerSec/numIndexThreads, runTimeSec, random, doUpdates);
      indexThreads[i].setPriority(Thread.currentThread().getPriority()+1);
      indexThreads[i].setName("IndexThread " + i);
      indexThreads[i].start();
    }

    // Open initial reader/searcher
    final IndexReader startR = IndexReader.open(w, true);
    System.out.println("Reader=" + startR);
    setSearcher(new IndexSearcher(startR));
    
    final SearchThread[] searchThreads = new SearchThread[numSearchThreads];

    final long startNS = System.nanoTime();
    for(int i=0;i<numSearchThreads;i++) {
      searchThreads[i] = new SearchThread(runTimeSec);
      //System.out.println("SEARCH PRI=" + searchThreads[i].getPriority() + " MIN=" + Thread.MIN_PRIORITY + " MAX=" + Thread.MAX_PRIORITY);
      searchThreads[i].setName("SearchThread " + i);
      searchThreads[i].start();
    }

    Thread reopenThread = new Thread() {
      @Override
      public void run() {
        try {
          final long startMS = System.currentTimeMillis();
          final long stopMS = startMS + (long) (runTimeSec * 1000);

          IndexReader r = startR;
          int reopenCount = 1;
          while(true) {
            final long t = System.currentTimeMillis();
            if (t >= stopMS) {
              break;
            }
            
            final long sleepMS = (long) Math.max(500/reopenPerSec, startMS + (long) (1000*(reopenCount/reopenPerSec)) - System.currentTimeMillis());

            /*
            final long sleepMS;
            if (random.nextBoolean()) {
              sleepMS = random.nextInt(200);
            } else if (random.nextBoolean()) {
              sleepMS = random.nextInt(1000);
            } else {
              sleepMS = random.nextInt(2000);
            }
            */

            Thread.sleep(sleepMS);

            int qt = (int) ((t-startMS)/statsEverySec/1000);
            if (reopenCount > 1) {
              reopensByTime[qt].incrementAndGet();
            }

            final long tStart = System.nanoTime();
            final IndexReader newR = r.reopen();

            if (newR != r) {
              System.out.println("Reopen: " + String.format("%9.4f", (System.nanoTime() - tStart)/1000000.0) + " msec");
              setSearcher(new IndexSearcher(newR));
              r = newR;
              reopenCount++;
            } else {
              System.out.println("WARNING: no changes on reopen");
            }
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
    reopenThread.setName("ReopenThread");
    reopenThread.setPriority(2+Thread.currentThread().getPriority());
    //System.out.println("REOPEN PRI " + reopenThread.getPriority());
    reopenThread.start();

    Thread.currentThread().setPriority(3+Thread.currentThread().getPriority());
    //System.out.println("TIMER PRI " + Thread.currentThread().getPriority());

    final long startMS = System.currentTimeMillis();
    final long stopMS = startMS + (long) (runTimeSec * 1000);
    int lastQT = -1;
    while(true) {
      final long t = System.currentTimeMillis();
      if (t >= stopMS) {
        break;
      }
      int qt = (int) ((t-startMS)/statsEverySec/1000);
      if (qt != lastQT) {
        if (lastQT != -1) {
          System.out.println("QT " + lastQT + " searches=" + searchesByTime[lastQT].get() + " docs=" + docsIndexedByTime[lastQT].get() + " reopens=" + reopensByTime[lastQT].get());
        }
        lastQT = qt;
      }
      Thread.sleep(25);
    }

    for(IndexThread t : indexThreads) {
      t.join();
    }

    for(SearchThread t : searchThreads) {
      t.join();
    }

    reopenThread.join();

    System.out.println("By time:");
    for(int i=0;i<searchesByTime.length-2;i++) {
      System.out.println("  " + (i*statsEverySec) + " searches=" + searchesByTime[i].get() + " docs=" + docsIndexedByTime[i].get() + " reopens=" + reopensByTime[i]);
    }
    setSearcher(null);
    if (NEW_INDEX) {
      w.waitForMerges();
      w.close();
    } else if (doCommit) {
      w.close(false);
    } else {
      w.rollback();
    }
  }
}
