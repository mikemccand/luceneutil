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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene41.Lucene41Codec;
import org.apache.lucene.document.*;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.*;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.*;

// javac -Xlint:deprecation -cp ../modules/analysis/build/common/classes/java:build/classes/java:build/classes/test-framework:build/classes/test:build/contrib/misc/classes/java perf/Indexer.java perf/LineFileDocs.java

public final class Indexer {

  public static void main(String[] clArgs) throws Exception {

    Args args = new Args(clArgs);

    final boolean dateFacets = args.getFlag("-dateFacets");

    final String dirImpl = args.getString("-dirImpl");
    final String dirPath = args.getString("-indexPath") + "/index";
    final String facetsDirPath = args.getString("-indexPath") + "/facets";

    final Directory dir;
    Directory facetsDir = null;
    if (dirImpl.equals("MMapDirectory")) {
      dir = new MMapDirectory(new File(dirPath));
      if (dateFacets) {
        facetsDir = new MMapDirectory(new File(facetsDirPath));
      }
    } else if (dirImpl.equals("NIOFSDirectory")) {
      dir = new NIOFSDirectory(new File(dirPath));
      if (dateFacets) {
        facetsDir = new NIOFSDirectory(new File(facetsDirPath));
      }
    } else if (dirImpl.equals("SimpleFSDirectory")) {
      dir = new SimpleFSDirectory(new File(dirPath));
      if (dateFacets) {
        facetsDir = new SimpleFSDirectory(new File(facetsDirPath));
      }
    } else {
      throw new RuntimeException("unknown directory impl \"" + dirImpl + "\"");
    }
      
    final String analyzer = args.getString("-analyzer");
    final Analyzer a;
    if (analyzer.equals("EnglishAnalyzer")) {
      a = new EnglishAnalyzer(Version.LUCENE_50);
    } else if (analyzer.equals("StandardAnalyzer")) {
      a = new StandardAnalyzer(Version.LUCENE_50);
    } else if (analyzer.equals("StandardAnalyzerNoStopWords")) {
      a = new StandardAnalyzer(Version.LUCENE_50, CharArraySet.EMPTY_SET);
    } else if (analyzer.equals("ShingleStandardAnalyzer")) {
      a = new ShingleAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_50),
                                     2, 2);
    } else if (analyzer.equals("ShingleStandardAnalyzerNoStopWords")) {
      a = new ShingleAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_50, CharArraySet.EMPTY_SET),
                                     2, 2);
    } else {
      throw new RuntimeException("unknown analyzer " + analyzer);
    } 

    final String lineFile = args.getString("-lineDocsFile");

    // -1 means all docs in the line file:
    final int docCountLimit = args.getInt("-docCountLimit");
    final int numThreads = args.getInt("-threadCount");

    final boolean doForceMerge = args.getFlag("-forceMerge");
    final boolean verbose = args.getFlag("-verbose");

    final double ramBufferSizeMB = args.getDouble("-ramBufferMB");
    final int maxBufferedDocs = args.getInt("-maxBufferedDocs");

    final String defaultPostingsFormat = args.getString("-postingsFormat");
    final boolean doDeletions = args.getFlag("-deletions");
    final boolean printDPS = args.getFlag("-printDPS");
    final boolean waitForMerges = args.getFlag("-waitForMerges");
    final String mergePolicy = args.getString("-mergePolicy");
    final boolean doUpdate = args.getFlag("-update");
    final String idFieldPostingsFormat = args.getString("-idFieldPostingsFormat");
    final boolean addGroupingFields = args.getFlag("-grouping");
    final boolean useCFS = args.getFlag("-cfs");
    final boolean storeBody = args.getFlag("-store");
    final boolean tvsBody = args.getFlag("-tvs");
    final boolean bodyPostingsOffsets = args.getFlag("-bodyPostingsOffsets");
    final int maxConcurrentMerges = args.getInt("-maxConcurrentMerges");

    if (addGroupingFields && docCountLimit == -1) {
      throw new RuntimeException("cannot add grouping fields unless docCount is set");
    }

    args.check();

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
    System.out.println("Merge policy: " + mergePolicy);
    System.out.println("Update: " + doUpdate);
    System.out.println("ID field postings format: " + idFieldPostingsFormat);
    System.out.println("Add grouping fields: " + (addGroupingFields ? "yes" : "no"));
    System.out.println("Compound file format: " + (useCFS ? "yes" : "no"));
    System.out.println("Store body field: " + (storeBody ? "yes" : "no"));
    System.out.println("Term vectors for body field: " + (tvsBody ? "yes" : "no"));
    System.out.println("Date facets: " + (dateFacets ? "yes" : "no"));
    System.out.println("Body postings offsets: " + (bodyPostingsOffsets ? "yes" : "no"));
    System.out.println("Max concurrent merges: " + maxConcurrentMerges);
    
    if (verbose) {
      InfoStream.setDefault(new PrintStreamInfoStream(System.out));
    }

    final IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_50, a);

    if (doUpdate) {
      iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
    } else {
      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    }

    iwc.setMaxBufferedDocs(maxBufferedDocs);
    iwc.setRAMBufferSizeMB(ramBufferSizeMB);

    // Increase number of concurrent merges since we are on SSD:
    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
    iwc.setMergeScheduler(cms);
    cms.setMaxMergeCount(maxConcurrentMerges+2);
    cms.setMaxThreadCount(maxConcurrentMerges);

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
      tmp.setUseCompoundFile(useCFS);
      tmp.setNoCFSRatio(1.0);
      mp = null;
    } else {
      throw new RuntimeException("unknown MergePolicy " + mergePolicy);
    }

    if (mp != null) {
      iwc.setMergePolicy(mp);
      mp.setUseCompoundFile(useCFS);
      mp.setNoCFSRatio(1.0);
    }

    // Keep all commit points:
    iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);

    final Codec codec = new Lucene41Codec() {
      @Override
      public PostingsFormat getPostingsFormatForField(String field) {
        return PostingsFormat.forName(field.equals("id") ?
                                      idFieldPostingsFormat : defaultPostingsFormat);
      }
    };

    iwc.setCodec(codec);

    System.out.println("IW config=" + iwc);

    final IndexWriter w = new IndexWriter(dir, iwc);
    final TaxonomyWriter facetWriter;
    if (dateFacets) {
      facetWriter = new DirectoryTaxonomyWriter(facetsDir, IndexWriterConfig.OpenMode.CREATE);
    } else {
      facetWriter = null;
    }

    // Fixed seed so group field values are always consistent:
    final Random random = new Random(17);

    IndexThreads threads = new IndexThreads(random, w, facetWriter, lineFile, storeBody, tvsBody, bodyPostingsOffsets,
                                            numThreads, docCountLimit, addGroupingFields, printDPS,
                                            doUpdate, -1.0f, false);

    System.out.println("\nIndexer: start");
    final long t0 = System.currentTimeMillis();

    threads.start();

    while (!threads.done()) {
      Thread.sleep(100);
    }

    threads.stop();

    final long t1 = System.currentTimeMillis();
    System.out.println("\nIndexer: indexing done (" + (t1-t0) + " msec); total " + w.maxDoc() + " docs");
    // if we update we can not tell how many docs
    if (!doUpdate && docCountLimit != -1 && w.maxDoc() != docCountLimit) {
      throw new RuntimeException("w.maxDoc()=" + w.maxDoc() + " but expected " + docCountLimit);
    }
    if (threads.failed.get()) {
      throw new RuntimeException("exceptions during indexing");
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

    if (doForceMerge) {
      w.forceMerge(1);
      final long t4 = System.currentTimeMillis();
      System.out.println("\nIndexer: force merge done (took " + (t4-t3) + " msec)");

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

      commitData.put("userData", doForceMerge ? "delsingle" : "delmulti");
      w.setCommitData(commitData);
      w.commit();
      final long t7 = System.currentTimeMillis();
      System.out.println("\nIndexer: commit delmulti done (took " + (t7-t6) + " msec)");

      if (doUpdate || w.numDocs() != maxDoc - toDeleteCount) {
        throw new RuntimeException("count mismatch: w.numDocs()=" + w.numDocs() + " but expected " + (maxDoc - toDeleteCount));
      }
    }

    if (facetWriter != null) {
      facetWriter.commit();
      facetWriter.close();
    }

    System.out.println("\nIndexer: at close: " + w.segString());
    final long tCloseStart = System.currentTimeMillis();
    w.close(waitForMerges);
    System.out.println("\nIndexer: close took " + (System.currentTimeMillis() - tCloseStart) + " msec");
    dir.close();
    final long tFinal = System.currentTimeMillis();
    System.out.println("\nIndexer: finished (" + (tFinal-t0) + " msec)");
    System.out.println("\nIndexer: net bytes indexed " + threads.getBytesIndexed());
    System.out.println("\nIndexer: " + (threads.getBytesIndexed()/1024./1024./1024./((tFinal-t0)/3600000.)) + " GB/hour plain text");
  }
}
