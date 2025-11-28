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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.util.Bits;

class IndexerThread extends Thread {
  private final IndexWriter iw;
  private final VectorReader vectorReader;
  private final AtomicInteger numDocsIndexed;
  private final int numDocsToIndex;
  private final FieldType fieldType;
  private final VectorEncoding vectorEncoding;
  private final byte[] byteVectorBuffer;
  private final float[] floatVectorBuffer;
  private final KnnIndexer.FilterScheme filterScheme;

  public IndexerThread(IndexWriter iw, int dims, VectorReader vectorReader, VectorEncoding vectorEncoding, FieldType fieldType,
                       AtomicInteger numDocsIndexed, int numDocsToIndex, KnnIndexer.FilterScheme filterScheme) {
    this.iw = iw;
    this.vectorReader = vectorReader;
    this.vectorEncoding = vectorEncoding;
    this.fieldType = fieldType;
    this.numDocsIndexed = numDocsIndexed;
    this.numDocsToIndex = numDocsToIndex;
    this.filterScheme = filterScheme;
    switch (vectorEncoding) {
      case BYTE -> {
        byteVectorBuffer = new byte[dims];
        floatVectorBuffer = null;
      }
      case FLOAT32 -> {
        floatVectorBuffer = new float[dims];
        byteVectorBuffer = null;
      }
      default -> {
        throw new IllegalArgumentException("unexpected vector encoding: " + vectorEncoding);
      }
    }
  }

  @Override
  public void run() {
    try {
      _run();
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }
  }

  private void _run() throws IOException {
    while (true) {
      int id;
      Document doc = new Document();
      synchronized (vectorReader) {
        id = numDocsIndexed.getAndIncrement();
        if (id >= numDocsToIndex) {
          // yay, done!
          break;
        }
        switch (vectorEncoding) {
          case BYTE -> {
            byte[] bytes = ((VectorReaderByte) vectorReader).nextBytes();
            System.arraycopy(bytes, 0, byteVectorBuffer, 0, bytes.length);
            if (filterScheme == null || filterScheme.keepUnfiltered()) {
              doc.add(new KnnByteVectorField(KnnGraphTester.KNN_FIELD, byteVectorBuffer, fieldType));
            }
            if (filterScheme != null && filterScheme.filter().get(id)) {
              doc.add(new KnnByteVectorField(KnnGraphTester.KNN_FIELD_FILTERED, byteVectorBuffer, fieldType));
            }
          }
          case FLOAT32 -> {
            float[] floats = vectorReader.next();
            System.arraycopy(floats, 0, floatVectorBuffer, 0, floats.length);
            if (filterScheme == null || filterScheme.keepUnfiltered()) {
              doc.add(new KnnFloatVectorField(KnnGraphTester.KNN_FIELD, floatVectorBuffer, fieldType));
            }
            if (filterScheme != null && filterScheme.filter().get(id)) {
              doc.add(new KnnFloatVectorField(KnnGraphTester.KNN_FIELD_FILTERED, floatVectorBuffer, fieldType));
            }
          }
        }

        // paranoia: a bit of a lie (we didn't index OUR doc yet), but do it in sync block to prevent sysouts from stomping
        // each other ... not sure if line buffering / atomicity would do this for free?
        if ((id + 1) % 25000 == 0) {
          System.out.println("Done indexing " + (id + 1) + " documents.");
        }
      }
      doc.add(new StoredField(KnnGraphTester.ID_FIELD, id));
      iw.addDocument(doc);
    }
  }
}
