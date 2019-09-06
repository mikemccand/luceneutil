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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import java.nio.file.Paths;
import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.util.Version;
import org.apache.lucene.util._TestUtil;
import perf.LineFileDocs;

import com.foundationdb.lucene.*;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;

// javac -Xlint:deprecation -cp build/core/classes/java:build/test-framework/classes/java:build/core/classes/test:build/contrib/analyzers/common/classes/java:build/contrib/misc/classes/java perf/Indexer.java perf/LineFileDocs.java

// Usage: dirImpl dirPath analyzer /path/to/line/file numDocs numThreads doFullMerge:yes|no verbose:yes|no ramBufferMB maxBufferedDocs codec doDeletions:yes|no printDPS:yes|no waitForMerges:yes|no mergePolicy doUpdate idFieldUsesPulsingCodec

// EG:
//
// java -Xms2g -Xmx2g -server -classpath ".:build/core/classes/java:build/test-framework/classes/java:build/core/classes/test:build/contrib/analyzers/common/classes/java:build/contrib/misc/classes/java" perf.Indexer MMapDirectory "/q/lucene/indices/wikimedium.3x.nightly.nd27.625M/index" StandardAnalyzer /lucene/data/enwiki-20110115-lines-1k-fixed.txt 27625038 1 no no -1 49774 Lucene40 no no yes LogDocMergePolicy no Memory yes no

public final class Indexer {


  public static final String DEFAULT_ROOT_PREFIX = "lucene";
  public static final String DEFAULT_TEST_ROOT_PREFIX = "test_" + DEFAULT_ROOT_PREFIX;

  // NOTE: returned array might have dups
  private static String[] randomStrings(int count, Random random) {
    final String[] strings = new String[count];
    int i = 0;
    while(i < count) {
      final String s = "asdfassdfdafdafafdafasda"; //_TestUtil.randomSimpleString(random);
      if (s.length() >= 7) {
        strings[i++] = s;
      }
    }

    return strings;
  }

  public static void main(String[] clArgs) throws Exception {

    StatisticsHelper stats = new StatisticsHelper();
    stats.startStatistics();
    try {
      _main(clArgs);
    } finally {
      stats.stopStatistics();
    }
  }

  private static void _main(String[] clArgs) throws Exception {

    Args args = new Args(clArgs);

    final String dirImpl = args.getString("-dirImpl");
    final String dirPath = args.getString("-indexPath") + "/index";

    //final Directory dir = FSDirectory.open(new File(dirPath));

    FDB fdb = FDB.selectAPIVersion(600);
    Database db = fdb.open();

     FDBDirectory dir = new FDBDirectory(Tuple.from(DEFAULT_TEST_ROOT_PREFIX, "subidr"), db);

    final String analyzer = args.getString("-analyzer");
    final Analyzer a;
    if (analyzer.equals("StandardAnalyzer")) {
      a = new StandardAnalyzer(Version.LUCENE_46, CharArraySet.EMPTY_SET);
    }  else {
      throw new RuntimeException("unknown analyzer " + analyzer);
    }

    final boolean doFullMerge = false;

    final String lineFile = args.getString("-lineDocsFile");

    // -1 means all docs in the line file:
    final int docCountLimit = args.getInt("-docCountLimit");
    final int numThreads = args.getInt("-threadCount");

    final boolean doForceMerge = args.getFlag("-forceMerge");
    final boolean verbose = args.getFlag("-verbose");

    String indexSortField = null;

    final double ramBufferSizeMB = args.getDouble("-ramBufferMB");
    final int maxBufferedDocs = args.getInt("-maxBufferedDocs");

    final String defaultPostingsFormat = args.getString("-postingsFormat");
    final boolean doDeletions = args.getFlag("-deletions");
    final boolean printDPS = args.getFlag("-printDPS");
    final boolean waitForMerges = args.getFlag("-waitForMerges");
    final boolean waitForCommit = args.getFlag("-waitForCommit");
    final String mergePolicy = args.getString("-mergePolicy");
    final boolean doUpdate = args.getFlag("-update");

    final String idFieldPostingsFormat = args.getString("-idFieldPostingsFormat");
    final boolean addGroupingFields = args.getFlag("-grouping");
    final boolean useCFS = args.getFlag("-cfs");
    final boolean storeBody = args.getFlag("-store");
    final boolean tvsBody = args.getFlag("-tvs");
    final boolean bodyPostingsOffsets = args.getFlag("-bodyPostingsOffsets");
    final int maxConcurrentMerges = args.getInt("-maxConcurrentMerges");
    final boolean addDVFields = args.getFlag("-dvfields");
    final boolean doRandomCommit = args.getFlag("-randomCommit");
    final boolean useCMS = args.getFlag("-useCMS");
    final boolean disableIOThrottle = args.getFlag("-disableIOThrottle");

    //if (waitForCommit == false && waitForMerges) {
    //  throw new RuntimeException("pass -waitForCommit if you pass -waitForMerges");
    //}

    //if (waitForCommit == false && doForceMerge) {
    //  throw new RuntimeException("pass -waitForCommit if you pass -forceMerge");
    //}

    //if (waitForCommit == false && doDeletions) {
    //  throw new RuntimeException("pass -waitForCommit if you pass -deletions");
    //}

    //if (useCMS == false && disableIOThrottle) {
     // throw new RuntimeException("-disableIOThrottle only makes sense with -useCMS");
    //}

    final double nrtEverySec;
    if (args.hasArg("-nrtEverySec")) {
      nrtEverySec = args.getDouble("-nrtEverySec");
    } else {
      nrtEverySec = -1.0;
    }

    // True to start back at the beginning if we run out of
    // docs from the line file source:
    final boolean repeatDocs = args.getFlag("-repeatDocs");

    final String facetDVFormatName;

    if (addGroupingFields && docCountLimit == -1) {
      a.close();
      throw new RuntimeException("cannot add grouping fields unless docCount is set");
    }

    //args.check();

    System.out.println("Dir: " + dirImpl);
    System.out.println("Index path: " + dirPath);
    System.out.println("Analyzer: " + analyzer);
    System.out.println("Line file: " + lineFile);
    System.out.println("Doc count limit: " + (docCountLimit == -1 ? "all docs" : ""+docCountLimit));
    System.out.println("Threads: " + numThreads);
    System.out.println("Force merge: " + (doForceMerge ? "yes" : "no"));
    System.out.println("Verbose: " + (verbose ? "yes" : "no"));
    System.out.println("RAM Buffer MB: " + ramBufferSizeMB);
    System.out.println("Max buffered docs: " + maxBufferedDocs);
    System.out.println("Default postings format: " + defaultPostingsFormat);
    System.out.println("Do deletions: " + (doDeletions ? "yes" : "no"));
    System.out.println("Wait for merges: " + (waitForMerges ? "yes" : "no"));
    System.out.println("Wait for commit: " + (waitForCommit ? "yes" : "no"));
    System.out.println("IO throttle: " + (disableIOThrottle ? "no" : "yes"));
    System.out.println("Merge policy: " + mergePolicy);
    System.out.println("ID field postings format: " + idFieldPostingsFormat);
    System.out.println("Add grouping fields: " + (addGroupingFields ? "yes" : "no"));
    System.out.println("Compound file format: " + (useCFS ? "yes" : "no"));
    System.out.println("Store body field: " + (storeBody ? "yes" : "no"));
    System.out.println("Term vectors for body field: " + (tvsBody ? "yes" : "no"));
    //System.out.println("Facet DV Format: " + facetDVFormatName);
    System.out.println("Body postings offsets: " + (bodyPostingsOffsets ? "yes" : "no"));
    System.out.println("Max concurrent merges: " + maxConcurrentMerges);
    System.out.println("Add DocValues fields: " + addDVFields);
    System.out.println("Use ConcurrentMergeScheduler: " + useCMS);
    if (nrtEverySec > 0.0) {
      System.out.println("Open & close NRT reader every: " + nrtEverySec + " sec");
    } else {
      System.out.println("Open & close NRT reader every: never");
    }
    System.out.println("Repeat docs: " + repeatDocs);

    IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_46, a);

    if (doUpdate) {
      iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
    } else {
      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    }

    iwc.setMaxBufferedDocs(maxBufferedDocs);
    iwc.setRAMBufferSizeMB(ramBufferSizeMB);
    iwc.setCodec(new FDBCodec());

    final Random random = new Random(17);
    final AtomicInteger groupBlockIndex;
    if (addGroupingFields) {
      IndexThread.group100 = randomStrings(100, random);
      IndexThread.group10K = randomStrings(10000, random);
      IndexThread.group100K = randomStrings(100000, random);
      IndexThread.group1M = randomStrings(1000000, random);
      groupBlockIndex = new AtomicInteger();
    } else {
      groupBlockIndex = null;
    }

    final LogMergePolicy mp;
    if (mergePolicy.equals("LogDocMergePolicy")) {
      mp = new LogDocMergePolicy();
    } else if (mergePolicy.equals("LogByteSizeMergePolicy")) {
      mp = new LogByteSizeMergePolicy();
    } else if (mergePolicy.equals("NoMergePolicy")) {
      final MergePolicy nmp = useCFS ? NoMergePolicy.COMPOUND_FILES : NoMergePolicy.NO_COMPOUND_FILES;
      iwc.setMergePolicy(nmp);
      mp = null;
    } else if (mergePolicy.equals("TieredMergePolicy")) {
      final TieredMergePolicy tmp = new TieredMergePolicy();
      iwc.setMergePolicy(tmp);
      tmp.setMaxMergedSegmentMB(1000000.0);
      //tmp.setUseCompoundFile(useCFS);
      tmp.setNoCFSRatio(1.0);
      mp = null;
      //    } else if (mergePolicy.equals("BalancedSegmentMergePolicy")) {
      //      mp = new BalancedSegmentMergePolicy();
    } else {
      throw new RuntimeException("unknown MergePolicy " + mergePolicy);
    }

    if (mp != null) {
      iwc.setMergePolicy(mp);
      //mp.setUseCompoundFile(useCFS);
      mp.setNoCFSRatio(1.0);
    }

    // Keep all commit points:
    iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);

    /*
    final Codec codec = new Lucene40Codec() {
      @Override
      public PostingsFormat getPostingsFormatForField(String field) {
        if (field.equals("id")) {
          if (idFieldCodec.equals("Pulsing40")) {
            return PostingsFormat.forName("Pulsing40");
          } else if (idFieldCodec.equals("Memory")) {
            return PostingsFormat.forName("Memory");
          } else if (idFieldCodec.equals("Lucene40")) {
            return PostingsFormat.forName("Lucene40");
          } else {
            throw new RuntimeException("unknown id field codec " + idFieldCodec);
          }
        } else {
          return PostingsFormat.forName(defaultPostingsFormat);
        }
      }
    };

    iwc.setCodec(codec);
    */

    System.out.println("IW config=" + iwc);
    final IndexWriter w = new IndexWriter(dir, iwc);

    if (verbose) {
      //InfoStream.setDefault(new PrintStreamInfoStream(System.out));
      //w.setInfoStream(System.out);
    }
    final LineFileDocs docs = new LineFileDocs(lineFile, false);

    System.out.println("\nIndexer: start");
    final long t0 = System.currentTimeMillis();
    final Thread[] threads = new Thread[numThreads];
    final AtomicInteger count = new AtomicInteger();
    for(int thread=0;thread<numThreads;thread++) {
      threads[thread] = new IndexThread(w, docs, docCountLimit, count, doUpdate, groupBlockIndex);
      threads[thread].start();
    }
    AtomicBoolean stop = null;
    IngestRatePrinter printer = null;
    if (printDPS) {
      stop = new AtomicBoolean(false);
      printer = new IngestRatePrinter(count, stop);
      printer.start();
    }
    for(int thread=0;thread<numThreads;thread++) {
      threads[thread].join();
    }
    if (printer != null) {
      stop.getAndSet(true);
      printer.join();
    }
    docs.close();

    final long t1 = System.currentTimeMillis();
    System.out.println("\nIndexer: indexing done (" + (t1-t0) + " msec); total " + w.maxDoc() + " docs");
    // if we update we can not tell how many docs
    if (!doUpdate && docCountLimit != -1 && w.maxDoc() != docCountLimit) {
      throw new RuntimeException("w.maxDoc()=" + w.maxDoc() + " but expected " + docCountLimit);
    }

    final long t2;
    if (waitForMerges) {
      w.waitForMerges();
      t2 = System.currentTimeMillis();
      System.out.println("\nIndexer: waitForMerges done (" + (t2-t1) + " msec)");
    } else {
      t2 = System.currentTimeMillis();
    }

    final Map<String,String> commitData = new HashMap<String,String>();
    commitData.put("userData", "multi");
    w.setCommitData(commitData);
    w.commit();
    final long t3 = System.currentTimeMillis();
    System.out.println("\nIndexer: commit multi (took " + (t3-t2) + " msec)");

    if (doFullMerge) {
      w.forceMerge(1);
      final long t4 = System.currentTimeMillis();
      System.out.println("\nIndexer: full merge done (took " + (t4-t3) + " msec)");

      commitData.put("userData", "single");
      w.setCommitData(commitData);
      w.commit();
      final long t5 = System.currentTimeMillis();
      System.out.println("\nIndexer: commit single done (took " + (t5-t4) + " msec)");
    }

    if (doDeletions) {
      final long t5 = System.currentTimeMillis();
      // Randomly delete 5% of the docs
      final Set<Integer> deleted = new HashSet<Integer>();
      final int maxDoc = w.maxDoc();
      final int toDeleteCount = (int) (maxDoc * 0.05);
      System.out.println("\nIndexer: delete " + toDeleteCount + " docs");
      while(deleted.size() < toDeleteCount) {
        final int id = random.nextInt(maxDoc);
        if (!deleted.contains(id)) {
          deleted.add(id);
          w.deleteDocuments(new Term("id", LineFileDocs.intToID(id)));
        }
      }
      final long t6 = System.currentTimeMillis();
      System.out.println("\nIndexer: deletes done (took " + (t6-t5) + " msec)");

      commitData.put("userData", doFullMerge ? "delsingle" : "delmulti");
      w.setCommitData(commitData);
      w.commit();
      final long t7 = System.currentTimeMillis();
      System.out.println("\nIndexer: commit delmulti done (took " + (t7-t6) + " msec)");

      if (doUpdate || w.numDocs() != maxDoc - toDeleteCount) {
        throw new RuntimeException("count mismatch: w.numDocs()=" + w.numDocs() + " but expected " + (maxDoc - toDeleteCount));
      }
    }

    // TODO: delmulti isn't done if doFullMerge is yes: we have to go back and open the multi commit point and do deletes against it:

    /*
    if (doFullMerge) {
      final int maxDoc2 = w.maxDoc();
      final int expected = doDeletions ? maxDoc : maxDoc - toDeleteCount;
      if (maxDoc2 != expected {
        throw new RuntimeException("count mismatch: w.maxDoc()=" + w.maxDoc() + " but expected " + expected);
      }
      final int toDeleteCount2 = (int) (maxDoc2 * 0.05);
      System.out.println("\nIndexer: delete " + toDeleteCount + " docs");
      while(deleted.size() < toDeleteCount) {
        final int id = rand.nextInt(maxDoc);
        if (!deleted.contains(id)) {
          deleted.add(id);
          w.deleteDocuments(new Term("id", LineFileDocs.intToID(id)));
        }
      }
      final long t8 = System.currentTimeMillis();
      System.out.println("\nIndexer: deletes done (took " + (t8-t7) + " msec)");

      commitData.put("userData", "delsingle");
      w.commit(commitData);
      final long t9 = System.currentTimeMillis();
      System.out.println("\nIndexer: commit delsingle done (took " + (t9-t8) + " msec)");
    }
    */

    System.out.println("\nIndexer: at close: " + w.segString());
    final long tCloseStart = System.currentTimeMillis();
    w.close(waitForMerges);
    System.out.println("\nIndexer: close took " + (System.currentTimeMillis() - tCloseStart) + " msec");
    dir.close();
    final long tFinal = System.currentTimeMillis();
    System.out.println("\nIndexer: finished (" + (tFinal-t0) + " msec)");
    System.out.println("\nIndexer: net bytes indexed " + docs.getBytesIndexed());
    System.out.println("\nIndexer: " + (docs.getBytesIndexed()/1024./1024./1024./((tFinal-t0)/3600000.)) + " GB/hour plain text");
  }

  private static class IngestRatePrinter extends Thread {

    private final AtomicInteger count;
    private final AtomicBoolean stop;
    public IngestRatePrinter(AtomicInteger count, AtomicBoolean stop){
      this.count = count;
      this.stop = stop;
    }

    public void run() {
      long time = System.currentTimeMillis();
      System.out.println("startIngest: " + time);
      final long start = time;
      int lastCount = count.get();
      while(!stop.get()) {
        try {
          Thread.sleep(200);
        } catch(Exception ex) {
        }
        int numDocs = count.get();

        double current = numDocs - lastCount;
        long now = System.currentTimeMillis();
        double seconds = (now-time) / 1000.0d;
        System.out.println("ingest: " + (current / seconds) + " " + (now - start));
        time = now;
        lastCount = numDocs;
      }
    }
  }

  // TODO: is there a pre-existing way to do this!!!
  static Document cloneDoc(Document doc1) {
    return doc1;
  }

  private static class IndexThread extends Thread {
    public static String[] group100;
    public static String[] group100K;
    public static String[] group10K;
    public static String[] group1M;
    private final LineFileDocs docs;
    private final int numTotalDocs;
    private final IndexWriter w;
    private final AtomicInteger count;
    private final AtomicInteger groupBlockIndex;
    private final boolean doUpdate;

    public IndexThread(IndexWriter w, LineFileDocs docs, int numTotalDocs, AtomicInteger count, boolean doUpdate, AtomicInteger groupBlockIndex) {
      this.w = w;
      this.docs = docs;
      this.numTotalDocs = numTotalDocs;
      this.count = count;
      this.doUpdate = doUpdate;
      this.groupBlockIndex = groupBlockIndex;
    }

    @Override
    public void run() {
      final LineFileDocs.DocState docState = docs.newDocState();
      final Field idField = docState.id;
      final long tStart = System.currentTimeMillis();
      Term delTerm = null;
      final Field group100Field;
      final Field group100KField;
      final Field group10KField;
      final Field group1MField;
      final Field groupBlockField;
      final Field groupEndField;
      if (group100 != null) {
        group100Field = new Field("group100", "", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);
        docState.doc.add(group100Field);
        group10KField = new Field("group10K", "", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);
        docState.doc.add(group10KField);
        group100KField = new Field("group100K", "", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);
        docState.doc.add(group100KField);
        group1MField = new Field("group1M", "", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);
        docState.doc.add(group1MField);
        groupBlockField = new Field("groupblock", "", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);
        docState.doc.add(groupBlockField);
        // Binary marker field:
        groupEndField = new Field("groupend", "x", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS);
      } else {
        group100Field = null;
        group100KField = null;
        group10KField = null;
        group1MField = null;
        groupBlockField = null;
        groupEndField = null;
      }

      try {
        if (group100 != null) {

          // Add docs in blocks:

          final String[] groupBlocks;
          if (numTotalDocs >= 5000000) {
            groupBlocks = group1M;
          } else if (numTotalDocs >= 500000) {
            groupBlocks = group100K;
          } else {
            groupBlocks = group10K;
          }
          final double docsPerGroupBlock = numTotalDocs / (double) groupBlocks.length;

          final List<Document> docsGroup = new ArrayList<Document>();
          while(true) {
            final int groupCounter = groupBlockIndex.getAndIncrement();
            if (groupCounter >= groupBlocks.length) {
              break;
            }
            final int numDocs;
            if (groupCounter == groupBlocks.length-1) {
              // Put all remaining docs in this group
              numDocs = 10000;
            } else {
              // This will toggle between X and X+1 docs,
              // converging over time on average to the
              // floating point docsPerGroupBlock:
              numDocs = ((int) ((1+groupCounter)*docsPerGroupBlock)) - ((int) (groupCounter*docsPerGroupBlock));
            }
            groupBlockField.setStringValue(groupBlocks[groupCounter]);
            for(int docCount=0;docCount<numDocs;docCount++) {
              final Document doc = docs.nextDoc(docState);
              if (doc == null) {
                break;
              }
              final int id = LineFileDocs.idToInt(idField.stringValue());
              if (id >= numTotalDocs) {
                break;
              }
              if (((1+id) % 1000000) == 0) {
                System.out.println("Indexer: " + (1+id) + " docs... (" + (System.currentTimeMillis() - tStart) + " msec)");
              }
              group100Field.setStringValue(group100[id%100]);
              group10KField.setStringValue(group10K[id%10000]);
              group100KField.setStringValue(group100K[id%100000]);
              group1MField.setStringValue(group1M[id%1000000]);
              docsGroup.add(cloneDoc(doc));
            }
            final int docCount = docsGroup.size();
            docsGroup.get(docCount-1).add(groupEndField);
            //System.out.println("nd=" + docCount);
            if (docCount > 0) {
              w.addDocuments(docsGroup);
              count.addAndGet(docCount);
              docsGroup.clear();
            } else {
              break;
            }
          }
        } else {

          while(true) {
            final Document doc = docs.nextDoc(docState);
            if (doc == null) {
              break;
            }
            final int id = LineFileDocs.idToInt(idField.stringValue());
            if (numTotalDocs != -1 && id >= numTotalDocs) {
              break;
            }
            if (((1+id) % 1000000) == 0) {
              System.out.println("Indexer: " + (1+id) + " docs... (" + (System.currentTimeMillis() - tStart) + " msec)");
            }
            if (doUpdate) {
              delTerm = new Term("id", idField.stringValue());
            }
            w.updateDocument(delTerm, doc);
            count.incrementAndGet();
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
