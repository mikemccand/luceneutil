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
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene46.Lucene46Codec;
import org.apache.lucene.document.*;
import org.apache.lucene.facet.FacetsConfig;
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

    // EG: -facets Date -facets characterCount ...
    FacetsConfig facetsConfig = new FacetsConfig();
    facetsConfig.setHierarchical("Date", true);
    final Set<String> facetFields = new HashSet<String>();
    if (args.hasArg("-facets")) {
      for(String arg : args.getStrings("-facets")) {
        facetFields.add(arg);
      }
    }

    final String dirImpl = args.getString("-dirImpl");
    final String dirPath = args.getString("-indexPath") + "/index";

    final Directory dir;
    OpenDirectory od = OpenDirectory.get(dirImpl);

    dir = od.open(new File(dirPath));

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

    final String facetDVFormatName;
    if (facetFields.isEmpty()) {
      facetDVFormatName = "Lucene45";
    } else {
      facetDVFormatName = args.getString("-facetDVFormat");
    }

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
    System.out.println("Facet DV Format: " + facetDVFormatName);
    System.out.println("Facet fields: " + facetFields);
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

    // So flushed segments do/don't use CFS:
    iwc.setUseCompoundFile(useCFS);

    // Increase number of concurrent merges since we are on SSD:
    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
    iwc.setMergeScheduler(cms);
    cms.setMaxMergesAndThreads(maxConcurrentMerges, maxConcurrentMerges);

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
      tmp.setNoCFSRatio(useCFS ? 1.0 : 0.0);
      mp = null;
    } else {
      throw new RuntimeException("unknown MergePolicy " + mergePolicy);
    }

    if (mp != null) {
      iwc.setMergePolicy(mp);
      mp.setNoCFSRatio(useCFS ? 1.0 : 0.0);
    }

    // Keep all commit points:
    if (doDeletions || doForceMerge) {
      iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
    }
    
    final Codec codec = new Lucene46Codec() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
          return PostingsFormat.forName(field.equals("id") ?
                                        idFieldPostingsFormat : defaultPostingsFormat);
        }

        private final DocValuesFormat facetsDVFormat = DocValuesFormat.forName(facetDVFormatName);
        //private final DocValuesFormat lucene42DVFormat = DocValuesFormat.forName("Lucene42");
        //private final DocValuesFormat diskDVFormat = DocValuesFormat.forName("Disk");
        private final DocValuesFormat lucene45DVFormat = DocValuesFormat.forName("Lucene45");
        private final DocValuesFormat directDVFormat = DocValuesFormat.forName("Direct");

        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
          if (facetFields.contains(field)) {
            return facetsDVFormat;
            //} else if (field.equals("$facets_sorted_doc_values")) {
            //return diskDVFormat;
          } else {
            return directDVFormat;
          }
        }
      };

    iwc.setCodec(codec);

    System.out.println("IW config=" + iwc);

    final IndexWriter w = new IndexWriter(dir, iwc);
    final TaxonomyWriter taxoWriter;
    if (!facetFields.isEmpty()) {
      taxoWriter = new DirectoryTaxonomyWriter(od.open(new File(args.getString("-indexPath"), "facets")),
                                               IndexWriterConfig.OpenMode.CREATE);
    } else {
      taxoWriter = null;
    }

    // Fixed seed so group field values are always consistent:
    final Random random = new Random(17);

    IndexThreads threads = new IndexThreads(random, w, taxoWriter, facetFields, facetsConfig, lineFile, storeBody, tvsBody, bodyPostingsOffsets,
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

    if (taxoWriter != null) {
      System.out.println("Taxonomy has " + taxoWriter.getSize() + " ords");
      taxoWriter.commit();
      taxoWriter.close();
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
