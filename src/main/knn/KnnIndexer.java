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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static knn.KnnGraphTester.DOCTYPE_CHILD;
import static knn.KnnGraphTester.DOCTYPE_PARENT;

public class KnnIndexer {
  // use smaller ram buffer so we get to merging sooner, making better use of
  // many cores (TODO: use multiple indexing threads):
  // private static final double WRITER_BUFFER_MB = 1994d;
  private static final double WRITER_BUFFER_MB = 64;

  Path docsPath;
  Path indexPath;
  VectorEncoding vectorEncoding;
  int dim;
  VectorSimilarityFunction similarityFunction;
  Codec codec;
  int numDocs;
  int docsStartIndex;
  boolean quiet;
  boolean parentJoin;
  Path parentJoinMetaPath;

  public KnnIndexer(Path docsPath, Path indexPath, Codec codec, VectorEncoding vectorEncoding, int dim,
                    VectorSimilarityFunction similarityFunction, int numDocs, int docsStartIndex, boolean quiet,
                    boolean parentJoin, Path parentJoinMetaPath) {
    this.docsPath = docsPath;
    this.indexPath = indexPath;
    this.codec = codec;
    this.vectorEncoding = vectorEncoding;
    this.dim = dim;
    this.similarityFunction = similarityFunction;
    this.numDocs = numDocs;
    this.docsStartIndex = docsStartIndex;
    this.quiet = quiet;
    this.parentJoin = parentJoin;
    this.parentJoinMetaPath = parentJoinMetaPath;
  }

  public int createIndex() throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    iwc.setCodec(codec);
    // iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    iwc.setRAMBufferSizeMB(WRITER_BUFFER_MB);
    iwc.setUseCompoundFile(false);
    // iwc.setMaxBufferedDocs(10000);

    FieldType fieldType =
        switch (vectorEncoding) {
          case BYTE -> KnnByteVectorField.createFieldType(dim, similarityFunction);
          case FLOAT32 -> KnnFloatVectorField.createFieldType(dim, similarityFunction);
        };
    if (quiet == false) {
//      iwc.setInfoStream(new PrintStreamInfoStream(System.out));
      System.out.println("creating index in " + indexPath);
    }

    if (!indexPath.toFile().exists()) {
      indexPath.toFile().mkdirs();
    }

    long start = System.nanoTime();
    try (FSDirectory dir = FSDirectory.open(indexPath);
         IndexWriter iw = new IndexWriter(dir, iwc)) {
      try (FileChannel in = FileChannel.open(docsPath)) {
        if (docsStartIndex > 0) {
          seekToStartDoc(in, dim, vectorEncoding, docsStartIndex);
        }
        VectorReader vectorReader = VectorReader.create(in, dim, vectorEncoding);
        log("parentJoin=%s", parentJoin);
        if (parentJoin == false) {
          for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            switch (vectorEncoding) {
              case BYTE -> doc.add(
                  new KnnByteVectorField(
                      KnnGraphTester.KNN_FIELD, ((VectorReaderByte) vectorReader).nextBytes(), fieldType));
              case FLOAT32 -> doc.add(
                  new KnnFloatVectorField(KnnGraphTester.KNN_FIELD, vectorReader.next(), fieldType));
            }
            doc.add(new StoredField(KnnGraphTester.ID_FIELD, i));
            iw.addDocument(doc);

            if ((i + 1) % 25000 == 0) {
              System.out.println("Done indexing " + (i + 1) + " documents.");
            }
          }
        } else {
          // create parent-block join documents
          try (BufferedReader br = Files.newBufferedReader(parentJoinMetaPath)) {
            String[] headers = br.readLine().trim().split(",");
            if (headers.length != 2) {
              throw new IllegalStateException("Expected two columns in parentJoinMetadata csv. Found: " + headers.length);
            }
            log("Parent join metaFile columns: %s | %s", headers[0], headers[1]);
            int childDocs = 0;
            int parentDocs = 0;
            int docIds = 0;
            String prevWikiId = "null";
            String currWikiId;
            List<Document> block = new ArrayList<>();
            do {
              String[] line = br.readLine().trim().split(",");
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
      }
    }
    long elapsed = System.nanoTime() - start;
    log("Indexed %d docs in %d seconds", numDocs, TimeUnit.NANOSECONDS.toSeconds(elapsed));
    return (int) TimeUnit.NANOSECONDS.toMillis(elapsed);
  }

  private void seekToStartDoc(FileChannel in, int dim, VectorEncoding vectorEncoding, int docsStartIndex) throws IOException {
    int startByte = docsStartIndex * dim * vectorEncoding.byteSize;
    in.position(startByte);
  }

  private void log(String msg, Object... args) {
    if (quiet == false) {
      System.out.printf((msg) + "%n", args);
    }
  }
}
