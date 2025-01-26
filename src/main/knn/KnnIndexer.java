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

//package knn;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.misc.index.BPReorderingMergePolicy;
import org.apache.lucene.misc.index.BpVectorReorderer;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.PrintStreamInfoStream;

import static KnnGraphTester.DOCTYPE_CHILD;
import static KnnGraphTester.DOCTYPE_PARENT;

public class KnnIndexer {
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
  private final KnnBenchmarkType benchmarkType;
  private final Path metaDataFilePath;
  private final boolean useBp;

  public KnnIndexer(Path docsPath, Path indexPath, Codec codec, int numIndexThreads,
                    VectorEncoding vectorEncoding, int dim,
                    VectorSimilarityFunction similarityFunction, int numDocs, int docsStartIndex, boolean quiet,
                    KnnBenchmarkType benchmarkType, Path metaDataFilePath, boolean useBp) {
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
    this.benchmarkType = benchmarkType;
    this.metaDataFilePath = metaDataFilePath;
    this.useBp = useBp;
  }

  public int createIndex() throws IOException, InterruptedException {
    IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    iwc.setCodec(codec);
    // iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    iwc.setRAMBufferSizeMB(WRITER_BUFFER_MB);
    iwc.setUseCompoundFile(false);
    // iwc.setMaxBufferedDocs(10000);

    // sidestep an apparent IW bug that causes merges kicked off during commit to be aborted on later commit/close instead of waited on, hrmph
    iwc.setMaxFullFlushMergeWaitMillis(0);

    // aim for more compact/realistic index:
    TieredMergePolicy tmp = (TieredMergePolicy) iwc.getMergePolicy();
    tmp.setFloorSegmentMB(256);
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

    long start = System.nanoTime();
    try (FSDirectory dir = FSDirectory.open(indexPath);
         IndexWriter iw = new IndexWriter(dir, iwc);
         FileChannel in = FileChannel.open(docsPath)) {
      long docsPathSizeInBytes = in.size();
      if (docsPathSizeInBytes % (dim * vectorEncoding.byteSize) != 0) {
        throw new IllegalArgumentException("docsPath \"" + docsPath + "\" does not contain a whole number of vectors?  size=" + docsPathSizeInBytes);
      }
      System.out.println((int) (docsPathSizeInBytes / (dim * vectorEncoding.byteSize)) + " doc vectors in docsPath \"" + docsPath + "\"");
        
      VectorReader vectorReader = VectorReader.create(in, dim, vectorEncoding, docsStartIndex);
      log("benchmarkType = %s", benchmarkType);
      switch (benchmarkType) {
        case DEFAULT -> {
          ExecutorService exec = Executors.newFixedThreadPool(numIndexThreads);
          AtomicInteger numDocsIndexed = new AtomicInteger();
          List<Thread> threads = new ArrayList<>();
          for (int i=0;i<numIndexThreads;i++) {
            Thread t = new IndexerThread(iw, dim, vectorReader, vectorEncoding, fieldType, numDocsIndexed, numDocs);
            t.setDaemon(true);
            t.start();
            threads.add(t);
          }
          for (Thread t : threads) {
            t.join();
          }
        }
        case PARENT_JOIN -> indexParentChildDocs(iw, vectorReader, fieldType);
        case MULTI_VECTOR -> indexMultiVectors(iw, vectorReader, fieldType);
      }

      // give merges a chance to kick off and finish:
      log("now IndexWriter.commit()");
      iw.commit();

      // wait for running merges to complete -- not sure why this is needed -- IW should wait for merges on close by default
      cms.sync();
      log("done ConcurrentMergeScheduler.sync()");
    }
    long elapsed = System.nanoTime() - start;
    log("Indexed %d docs in %d seconds", numDocs, TimeUnit.NANOSECONDS.toSeconds(elapsed));
    return (int) TimeUnit.NANOSECONDS.toMillis(elapsed);
  }

  private void indexParentChildDocs(IndexWriter iw, VectorReader vectorReader, FieldType fieldType) throws IOException {
    // TODO: make multi-threaded?
    try (BufferedReader metaReader = Files.newBufferedReader(metaDataFilePath)) {
      String[] headers = metaReader.readLine().trim().split(",");
      if (headers.length != 2) {
        throw new IllegalStateException("Expected two columns in Metadata csv. Found: " + headers.length);
      }
      log("Parent join metaFile columns: %s | %s", headers[0], headers[1]);
      int childDocs = 0;
      int parentDocs = 0;
      int docIds = 0;
      String prevWikiId = "null";
      String currWikiId;
      List<Document> block = new ArrayList<>();
      do {
        String[] line = metaReader.readLine().trim().split(",");
        currWikiId = line[0];
        String currParaId = line[1];
        Document doc = new Document();
        switch (vectorEncoding) {
          case BYTE -> doc.add(
              new KnnByteVectorField(
                  KnnGraphTester.KNN_FIELD, ((VectorReaderByte) vectorReader).nextBytes(), fieldType));
          case FLOAT32 -> doc.add(
              new KnnFloatVectorField(KnnGraphTester.KNN_FIELD, vectorReader.next(), fieldType));
        }
        doc.add(new StoredField(KnnGraphTester.ID_FIELD, docIds++));
        doc.add(new StringField(KnnGraphTester.WIKI_ID_FIELD, currWikiId, Field.Store.YES));
        doc.add(new StringField(KnnGraphTester.WIKI_PARA_ID_FIELD, currParaId, Field.Store.YES));
        doc.add(new StringField(KnnGraphTester.DOCTYPE_FIELD, DOCTYPE_CHILD, Field.Store.NO));
        childDocs++;

        // Close block and create a new one when wiki article changes.
        if (!currWikiId.equals(prevWikiId) && !"null".equals(prevWikiId)) {
          Document parent = new Document();
          parent.add(new StoredField(KnnGraphTester.ID_FIELD, docIds++));
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
          log("indexed %d child documents, with %d parents", childDocs, parentDocs);
        }
      } while (childDocs < numDocs);
      if (!block.isEmpty()) {
        Document parent = new Document();
        parent.add(new StoredField(KnnGraphTester.ID_FIELD, docIds++));
        parent.add(new StringField(KnnGraphTester.DOCTYPE_FIELD, DOCTYPE_PARENT, Field.Store.NO));
        parent.add(new StringField(KnnGraphTester.WIKI_ID_FIELD, prevWikiId, Field.Store.YES));
        parent.add(new StringField(KnnGraphTester.WIKI_PARA_ID_FIELD, "_", Field.Store.YES));
        block.add(parent);
        iw.addDocuments(block);
      }
      log("Indexed %d documents with %d parent docs. now flush", childDocs, parentDocs);
    }
  }

  private void indexMultiVectors(IndexWriter iw, VectorReader vectorReader, FieldType fieldType) throws IOException {
    // TODO: make multi-threaded?
    try (BufferedReader metaReader = Files.newBufferedReader(metaDataFilePath)) {
      String[] headers = metaReader.readLine().trim().split(",");
      if (headers.length != 2) {
        throw new IllegalStateException("Expected two columns in metadata csv. Found: " + headers.length);
      }
      log("metaFile columns: %s | %s", headers[0], headers[1]);
      int docId = 0;
      int minVectorsPerDoc = Integer.MAX_VALUE;
      int maxVectorsPerDoc = Integer.MIN_VALUE;
      int vectorsInDoc = 0;
      String prevWikiId = "null";
      String currWikiId;
      Document doc = new Document();
      for (int i = 0; i < numDocs; i++) {
        String[] line = metaReader.readLine().trim().split(",");
        currWikiId = line[0];
        String currParaId = line[1];
        if (!currWikiId.equals(prevWikiId)) {
          // add current document and create a new one
          if (!"null".equals(prevWikiId)) {
            iw.addDocument(doc);
          }
          doc = new Document();
          doc.add(new StoredField(KnnGraphTester.ID_FIELD, docId++));
          doc.add(new StringField(KnnGraphTester.WIKI_ID_FIELD, currWikiId, Field.Store.YES));
          vectorsInDoc = 0;
          prevWikiId = currWikiId;
        }
        // add field to document
        switch (vectorEncoding) {
          case BYTE -> doc.add(
              new KnnByteVectorField(
                  KnnGraphTester.KNN_FIELD, ((VectorReaderByte) vectorReader).nextBytes(), fieldType));
          case FLOAT32 -> doc.add(
              new KnnFloatVectorField(KnnGraphTester.KNN_FIELD, vectorReader.next(), fieldType));
        }
        vectorsInDoc++;
        minVectorsPerDoc = Math.min(minVectorsPerDoc, vectorsInDoc);
        maxVectorsPerDoc = Math.max(maxVectorsPerDoc, vectorsInDoc);
        if (i % 25000 == 0) {
          log("indexed %d vectors in %d docs. maxVectorsPerDoc = %d, minVectorsPerDoc = %d",
              i, docId, maxVectorsPerDoc, minVectorsPerDoc);
        }
      }
      // index the last doc
      iw.addDocument(doc);
      log("done indexing multivectors...");
      log("\tindexed %d vectors in %d docs. maxVectorsPerDoc = %d, minVectorsPerDoc = %d",
          numDocs, docId, maxVectorsPerDoc, minVectorsPerDoc);
    }
  }

  private void log(String msg, Object... args) {
    if (quiet == false) {
      System.out.printf((msg) + "%n", args);
    }
  }
}
