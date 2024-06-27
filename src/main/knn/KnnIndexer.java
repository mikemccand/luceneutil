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

import knn.KnnGraphTester;
import knn.VectorReader;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.PrintStreamInfoStream;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

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

  public KnnIndexer(Path docsPath, Path indexPath, Codec codec, VectorEncoding vectorEncoding, int dim,
                    VectorSimilarityFunction similarityFunction, int numDocs, int docsStartIndex, boolean quiet) {
    this.docsPath = docsPath;
    this.indexPath = indexPath;
    this.codec = codec;
    this.vectorEncoding = vectorEncoding;
    this.dim = dim;
    this.similarityFunction = similarityFunction;
    this.numDocs = numDocs;
    this.docsStartIndex = docsStartIndex;
    this.quiet = quiet;
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
      iwc.setInfoStream(new PrintStreamInfoStream(System.out));
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

          if ((i+1) % 25000 == 0) {
            System.out.println("Done indexing " + (i + 1) + " documents.");
          }
        }
        if (quiet == false) {
          System.out.println("Done indexing " + numDocs + " documents; now flush");
        }
      }
    }
    long elapsed = System.nanoTime() - start;
    if (quiet == false) {
      System.out.println(
          "Indexed " + numDocs + " documents in " + TimeUnit.NANOSECONDS.toSeconds(elapsed) + "s");
    }
    return (int) TimeUnit.NANOSECONDS.toMillis(elapsed);
  }

  private void seekToStartDoc(FileChannel in, int dim, VectorEncoding vectorEncoding, int docsStartIndex) throws IOException {
    int startByte = docsStartIndex * dim * vectorEncoding.byteSize;
    in.position(startByte);
  }
}
