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

package knn;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.MergeRateLimiter;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.misc.index.BPReorderingMergePolicy;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BpVectorReorderer;
import org.apache.lucene.util.PrintStreamInfoStream;

import static knn.KnnGraphTester.DOCTYPE_CHILD;
import static knn.KnnGraphTester.DOCTYPE_PARENT;

public class KnnIndexer implements FormatterLogger {

  // use smaller ram buffer so we get to merging sooner, making better use of
  // many cores
  private static final double WRITER_BUFFER_MB = 128;

  private final Path docsPath;
  private final Path indexPath;
  private final VectorEncoding vectorEncoding;
  private final int dim;
  private final VectorSimilarityFunction similarityFunction;
  private final Codec codec;
  private final int numDocs;
  private final int docsStartIndex;
  private final int numIndexThreads;
  private final boolean quiet;
  private final boolean parentJoin;
  private final Path parentJoinMetaPath;
  private final boolean useBp;
  private final FilterScheme filterScheme;
  private final TrackingConcurrentMergeScheduler tcms;

  public KnnIndexer(Path docsPath, Path indexPath, Codec codec, int numIndexThreads,
                    VectorEncoding vectorEncoding, int dim,
                    VectorSimilarityFunction similarityFunction, int numDocs, int docsStartIndex, boolean quiet,
                    boolean parentJoin, Path parentJoinMetaPath, boolean useBp, FilterScheme filterScheme) {
    this.docsPath = docsPath;
    this.indexPath = indexPath;
    this.codec = codec;
    this.numIndexThreads = numIndexThreads;
    this.vectorEncoding = vectorEncoding;
    this.dim = dim;
    this.similarityFunction = similarityFunction;
    this.numDocs = numDocs;
    this.docsStartIndex = docsStartIndex;
    this.quiet = quiet;
    this.parentJoin = parentJoin;
    this.parentJoinMetaPath = parentJoinMetaPath;
    this.useBp = useBp;
    this.filterScheme = filterScheme;
    this.tcms = new TrackingConcurrentMergeScheduler();
  }

  public int createIndex() throws IOException, InterruptedException {
    IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    iwc.setCodec(codec);
    iwc.setMergeScheduler(tcms);
    // iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    iwc.setRAMBufferSizeMB(WRITER_BUFFER_MB);
    
    iwc.setUseCompoundFile(false);
    // iwc.setMaxBufferedDocs(10000);

    // sidestep an apparent IW bug that causes merges kicked off during commit to be aborted on later commit/close instead of waited on, hrmph
    iwc.setMaxFullFlushMergeWaitMillis(0);

    // aim for more compact/realistic index:
    TieredMergePolicy tmp = (TieredMergePolicy) iwc.getMergePolicy();
    tmp.setFloorSegmentMB(256);
    tmp.setNoCFSRatio(0);
    // tmp.setSegmentsPerTier(5);
    if (useBp) {
      iwc.setMergePolicy(new BPReorderingMergePolicy(iwc.getMergePolicy(), new BpVectorReorderer(KnnGraphTester.KNN_FIELD)));
    }

    ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler) iwc.getMergeScheduler();
    // cms.setMaxMergesAndThreads(24, 12);

    FieldType fieldType =
        switch (vectorEncoding) {
          case BYTE -> KnnByteVectorField.createFieldType(dim, similarityFunction);
          case FLOAT32 -> KnnFloatVectorField.createFieldType(dim, similarityFunction);
        };
    if (quiet == false) {
      // iwc.setInfoStream(new PrintStreamInfoStream(System.out));
      System.out.println("creating index in " + indexPath);
    }

    if (indexPath.toFile().exists() == false) {
      indexPath.toFile().mkdirs();
    }

    long startNS = System.nanoTime(), elapsedNS;
    try (FSDirectory dir = FSDirectory.open(indexPath);
         IndexWriter iw = new IndexWriter(dir, iwc);
         FileChannel in = FileChannel.open(docsPath)) {
      long docsPathSizeInBytes = in.size();
      if (docsPathSizeInBytes % (dim * vectorEncoding.byteSize) != 0) {
        throw new IllegalArgumentException("docsPath \"" + docsPath + "\" does not contain a whole number of vectors?  size=" + docsPathSizeInBytes);
      }
      System.out.println((int) (docsPathSizeInBytes / (dim * vectorEncoding.byteSize)) + " doc vectors in docsPath \"" + docsPath + "\"");
        
      VectorReader vectorReader = VectorReader.create(in, dim, vectorEncoding, docsStartIndex);
      log("parentJoin=%s\n", parentJoin);
      if (parentJoin == false) {
        ExecutorService exec = Executors.newFixedThreadPool(numIndexThreads);
        AtomicInteger numDocsIndexed = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();
        for (int i=0;i<numIndexThreads;i++) {
          Thread t = new IndexerThread(iw, dim, vectorReader, vectorEncoding, fieldType, numDocsIndexed, numDocs, filterScheme);
          t.setDaemon(true);
          t.start();
          threads.add(t);
        }
        for (Thread t : threads) {
          t.join();
        }
      } else {
        // TODO: multi-threaded!
        // create parent-block join documents
        try (BufferedReader br = Files.newBufferedReader(parentJoinMetaPath)) {
          String[] headers = br.readLine().trim().split(",");
          if (headers.length != 2) {
            throw new IllegalStateException("Expected two columns in parentJoinMetadata csv. Found: " + headers.length);
          }
          log("Parent join metaFile columns: %s | %s\n", headers[0], headers[1]);
          int childDocs = 0;
          int parentDocs = 0;
          int docId = 0;
          String prevWikiId = "null";
          String currWikiId;
          List<Document> block = new ArrayList<>();
          do {
            String[] line = br.readLine().trim().split(",");
            currWikiId = line[0];
            String currParaId = line[1];
            Document doc = new Document();
            switch (vectorEncoding) {
              case BYTE -> {
                byte[] vector = ((VectorReaderByte) vectorReader).nextBytes();
                if (filterScheme == null || filterScheme.keepUnfiltered) {
                  doc.add(new KnnByteVectorField(KnnGraphTester.KNN_FIELD, vector, fieldType));
                }
                if (filterScheme != null && filterScheme.filter.get(docId)) {
                  doc.add(new KnnByteVectorField(KnnGraphTester.KNN_FIELD_FILTERED, vector, fieldType));
                }
              }
              case FLOAT32 -> {
                float[] vector = vectorReader.next();
                if (filterScheme == null || filterScheme.keepUnfiltered) {
                  doc.add(new KnnFloatVectorField(KnnGraphTester.KNN_FIELD, vector, fieldType));
                }
                if (filterScheme != null && filterScheme.filter.get(docId)) {
                  doc.add(new KnnFloatVectorField(KnnGraphTester.KNN_FIELD_FILTERED, vector, fieldType));
                }
              }
            }
            doc.add(new StoredField(KnnGraphTester.ID_FIELD, docId++));
            doc.add(new StringField(KnnGraphTester.WIKI_ID_FIELD, currWikiId, Field.Store.YES));
            doc.add(new StringField(KnnGraphTester.WIKI_PARA_ID_FIELD, currParaId, Field.Store.YES));
            doc.add(new StringField(KnnGraphTester.DOCTYPE_FIELD, DOCTYPE_CHILD, Field.Store.NO));
            childDocs++;

            // Close block and create a new one when wiki article changes.
            if (!currWikiId.equals(prevWikiId) && !"null".equals(prevWikiId)) {
              Document parent = new Document();
              parent.add(new StoredField(KnnGraphTester.ID_FIELD, docId++));
              parent.add(new StringField(KnnGraphTester.DOCTYPE_FIELD, DOCTYPE_PARENT, Field.Store.NO));
              parent.add(new StringField(KnnGraphTester.WIKI_ID_FIELD, prevWikiId, Field.Store.YES));
              parent.add(new StringField(KnnGraphTester.WIKI_PARA_ID_FIELD, "_", Field.Store.YES));
              block.add(parent);
              iw.addDocuments(block);
              parentDocs++;
              // create new block for the next article
              block = new ArrayList<>();
              block.add(doc);
            } else {
              block.add(doc);
            }
            prevWikiId = currWikiId;
            if (childDocs % 25000 == 0) {
              log("indexed %d child documents, with %d parents\n", childDocs, parentDocs);
            }
          } while (childDocs < numDocs);
          if (!block.isEmpty()) {
            Document parent = new Document();
            parent.add(new StoredField(KnnGraphTester.ID_FIELD, docId++));
            parent.add(new StringField(KnnGraphTester.DOCTYPE_FIELD, DOCTYPE_PARENT, Field.Store.NO));
            parent.add(new StringField(KnnGraphTester.WIKI_ID_FIELD, prevWikiId, Field.Store.YES));
            parent.add(new StringField(KnnGraphTester.WIKI_PARA_ID_FIELD, "_", Field.Store.YES));
            block.add(parent);
            iw.addDocuments(block);
          }
          log("Indexed %d documents with %d parent docs. now flush\n", childDocs, parentDocs);
        }
      }

      // give merges a chance to kick off and finish:
      log("now IndexWriter.commit()\n");
      iw.commit();

      elapsedNS = System.nanoTime() - startNS;

      waitForMergesWithStatus(tcms, this);
    }
    log("Indexed %d docs in %d seconds\n", numDocs, TimeUnit.NANOSECONDS.toSeconds(elapsedNS));
    return (int) TimeUnit.NANOSECONDS.toMillis(elapsedNS);
  }

  public static void waitForMergesWithStatus(TrackingConcurrentMergeScheduler tcms, FormatterLogger log) throws InterruptedException {
    long startNS = System.nanoTime();
    
    // wait for running merges to complete, and print coarse status updates
    log.log("now wait for already running merges to finish\n");

    // silliness to just be able to print progress in waiting (so long...) for merges:
    long nextPrintNS = System.nanoTime();
    long lastNonZeroNS = -1;
    
    while (true) {
      long nowNS = System.nanoTime();
      int mergeCount = tcms.mergeThreadCount();
      if (mergeCount > 0) {
        lastNonZeroNS = nowNS;
      } else if (nowNS - lastNonZeroNS > 1_000_000_000) {
        // hackity -- in case merges finish and new merges kick off but that is not atomic, we wait
        // for 1 second period of no merges.  we are still calling CMS.sync below, so worst case that will
        // accurately wait for all merges in that call.
        break;
      }
      if (nowNS > nextPrintNS) {
        log.log("%.1f sec: %d merges\n%s\n",
                (nowNS - startNS)/1_000_000_000.,
                mergeCount, tcms.whatsUp());
        // print status every 5 sec
        nextPrintNS += 5_000_000_000L;
      }
      Thread.sleep(100);
    }
    log.log("done first pass waiting for merges\n");
    tcms.sync();
    log.log("done second pass waiting for merges (CMS.sync())\n");
  }

  public void log(String msg, Object... args) {
    if (quiet == false) {
      System.out.printf(msg, args);
      System.out.flush();
    }
  }

  private static record OneMergeAndStartTime(long startTimeNS, OneMerge oneMerge) {};

  // Simple little class to track progress in merges...
  static class TrackingConcurrentMergeScheduler extends ConcurrentMergeScheduler {

    private final List<OneMergeAndStartTime> running = new ArrayList<>();
    
    @Override
    protected void doMerge(MergeSource mergeSource, OneMerge merge) throws IOException {
      OneMergeAndStartTime mos = new OneMergeAndStartTime(System.nanoTime(), merge);
      synchronized(running) {
        running.add(mos);
      }
      try {
        super.doMerge(mergeSource, merge);
      } finally {
        synchronized(running) {
          running.remove(mos);
        }
      }
    }

    public String whatsUp() {
      List<String> addEm = new ArrayList<>();
      long nowNS = System.nanoTime();
      synchronized(running) {
        for (OneMergeAndStartTime mos : running) {
          addEm.add(String.format(Locale.ROOT, "%6.1fs %3s: %2d segs, %.1f K docs, %6.1f MB",
                                  (nowNS - mos.startTimeNS)/1000000000.,
                                  mos.oneMerge.getMergeInfo().info.name,
                                  mos.oneMerge.segments.size(),
                                  mos.oneMerge.totalNumDocs()/1000.,
                                  mos.oneMerge.totalBytesSize()/1024./1024.));
        }
      }
      return "  " + String.join("\n  ", addEm);
    }
  }

  record FilterScheme(Bits filter, boolean keepUnfiltered) { }
}
