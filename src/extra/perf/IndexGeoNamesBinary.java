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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PrintStreamInfoStream;

/**
 * Indexes GeoNames data from a pre-built binary file (created by buildBinaryGeoNames.py).
 * This avoids the overhead of tab-splitting and string parsing at indexing time.
 *
 * <p>Usage:
 * <pre>
 *   java perf.IndexGeoNamesBinary &lt;geonames.bin&gt; &lt;indexPath&gt; &lt;numThreads&gt;
 * </pre>
 *
 * <p>Binary format per chunk (little-endian):
 * <pre>
 *   [int32: docCount] [int32: chunkByteLength] [chunk bytes...]
 * </pre>
 *
 * <p>Each doc within a chunk:
 * <pre>
 *   int32  geoNameID
 *   double latitude     (NaN if missing)
 *   double longitude    (NaN if missing)
 *   int64  population   (-1 if missing)
 *   int64  elevation    (Long.MIN_VALUE if missing)
 *   int32  dem          (Integer.MIN_VALUE if missing)
 *   int64  modifiedMsec (-1 if missing)
 *   [int16 + bytes] x 12 string fields: name, asciiName, alternateNames,
 *       featureClass, featureCode, countryCode, cc2, admin1..4, timezone
 * </pre>
 */
public class IndexGeoNamesBinary {

  private static final Field.Store STORE = Field.Store.NO;

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: IndexGeoNamesBinary <geonames.bin> <indexPath> <numThreads>");
      System.exit(1);
    }

    String binaryFile = args[0];
    Path indexPath = Paths.get(args[1]);
    int numThreads = Integer.parseInt(args[2]);

    Directory dir = FSDirectory.open(indexPath);
    IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
    iwc.setOpenMode(OpenMode.CREATE);
    iwc.setRAMBufferSizeMB(128);
    iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    iwc.setInfoStream(new PrintStreamInfoStream(System.out));
    final IndexWriter w = new IndexWriter(dir, iwc);

    final ArrayBlockingQueue<ByteBuffer> workQueue = new ArrayBlockingQueue<>(1000);
    final AtomicBoolean done = new AtomicBoolean();
    final AtomicInteger docsIndexed = new AtomicInteger();
    final long startMS = System.currentTimeMillis();

    // Worker threads: read chunks from queue and index documents
    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread(() -> {
        try {
          while (true) {
            ByteBuffer buffer = workQueue.poll(100, TimeUnit.MILLISECONDS);
            if (buffer == null) {
              if (done.get()) {
                break;
              }
              continue;
            }

            while (buffer.hasRemaining()) {
              Document doc = readOneDoc(buffer);
              w.addDocument(doc);

              int count = docsIndexed.incrementAndGet();
              if (count % 200000 == 0) {
                long ms = System.currentTimeMillis();
                System.out.println(count + ": " + ((ms - startMS) / 1000.0) + " sec");
              }
            }
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }

    for (Thread thread : threads) {
      thread.start();
    }

    // Reader thread: read binary file and enqueue chunks
    byte[] headerBytes = new byte[8];
    ByteBuffer header = ByteBuffer.wrap(headerBytes).order(ByteOrder.LITTLE_ENDIAN);

    try (SeekableByteChannel channel = Files.newByteChannel(Paths.get(binaryFile), StandardOpenOption.READ)) {
      while (true) {
        header.clear();
        int bytesRead = channel.read(header);
        if (bytesRead == -1) {
          break;
        }
        if (bytesRead != 8) {
          throw new RuntimeException("Expected 8 header bytes but read " + bytesRead);
        }
        header.flip();
        int docCount = header.getInt();
        int chunkLength = header.getInt();

        ByteBuffer chunk = ByteBuffer.allocate(chunkLength).order(ByteOrder.LITTLE_ENDIAN);
        bytesRead = channel.read(chunk);
        if (bytesRead != chunkLength) {
          throw new RuntimeException("Expected " + chunkLength + " chunk bytes but read " + bytesRead);
        }
        chunk.flip();
        workQueue.put(chunk);
      }
    }

    done.set(true);
    for (Thread thread : threads) {
      thread.join();
    }

    w.close();
    dir.close();

    long ms = System.currentTimeMillis();
    System.out.println("Total: " + docsIndexed.get() + " docs in " + ((ms - startMS) / 1000.0) + " sec");
  }

  /** Read one document from the chunk buffer and return a Lucene Document. */
  private static Document readOneDoc(ByteBuffer buffer) {
    Document doc = new Document();

    // Fixed numeric fields
    int geoNameID = buffer.getInt();
    double latitude = buffer.getDouble();
    double longitude = buffer.getDouble();
    long population = buffer.getLong();
    long elevation = buffer.getLong();
    int dem = buffer.getInt();
    long modifiedMsec = buffer.getLong();

    // TextField fields need String (analyzer requires char sequences)
    String name = readString(buffer);
    String asciiName = readString(buffer);
    String alternateNames = readString(buffer);

    // StringField/KeywordField can take BytesRef directly — avoids UTF-8 -> UTF-16 -> UTF-8 round-trip
    BytesRef featureClass = readBytesRef(buffer);
    BytesRef featureCode = readBytesRef(buffer);
    BytesRef countryCode = readBytesRef(buffer);
    BytesRef cc2 = readBytesRef(buffer);
    BytesRef admin1 = readBytesRef(buffer);
    BytesRef admin2 = readBytesRef(buffer);
    BytesRef admin3 = readBytesRef(buffer);
    BytesRef admin4 = readBytesRef(buffer);
    BytesRef timezone = readBytesRef(buffer);

    // Build document
    doc.add(new IntPoint("geoNameID", geoNameID));
    doc.add(new TextField("name", name, STORE));
    doc.add(new TextField("asciiName", asciiName, STORE));
    doc.add(new TextField("alternateNames", alternateNames, STORE));

    if (!Double.isNaN(latitude)) {
      doc.add(new DoubleField("latitude", latitude, STORE));
    }
    if (!Double.isNaN(longitude)) {
      doc.add(new DoubleField("longitude", longitude, STORE));
    }

    doc.add(new StringField("featureClass", featureClass, STORE));
    doc.add(new StringField("featureCode", featureCode, STORE));
    doc.add(new KeywordField("countryCode", countryCode, STORE));
    doc.add(new StringField("cc2", cc2, STORE));
    doc.add(new StringField("admin1Code", admin1, STORE));
    doc.add(new StringField("admin2Code", admin2, STORE));
    doc.add(new StringField("admin3Code", admin3, STORE));
    doc.add(new StringField("admin4Code", admin4, STORE));

    if (population >= 0) {
      doc.add(new LongField("population", population, STORE));
    }
    if (elevation != Long.MIN_VALUE) {
      doc.add(new LongField("elevation", elevation, STORE));
    }
    if (dem != Integer.MIN_VALUE) {
      doc.add(new IntField("dem", dem, STORE));
    }

    doc.add(new KeywordField("timezone", timezone, STORE));

    if (modifiedMsec >= 0) {
      doc.add(new LongField("modified", modifiedMsec, STORE));
    }

    return doc;
  }

  /** Read a length-prefixed UTF-8 string as String (needed for TextField which requires tokenization). */
  private static String readString(ByteBuffer buffer) {
    int len = Short.toUnsignedInt(buffer.getShort());
    if (len == 0) {
      return "";
    }
    byte[] bytes = new byte[len];
    buffer.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  /** Read a length-prefixed UTF-8 field as BytesRef — no charset conversion needed for StringField/KeywordField. */
  private static BytesRef readBytesRef(ByteBuffer buffer) {
    int len = Short.toUnsignedInt(buffer.getShort());
    if (len == 0) {
      return new BytesRef();
    }
    byte[] bytes = new byte[len];
    buffer.get(bytes);
    return new BytesRef(bytes, 0, len);
  }
}
