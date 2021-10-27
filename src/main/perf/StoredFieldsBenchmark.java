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

package perf;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;

import org.apache.lucene.codecs.lucene90.Lucene90Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

// javac -cp /l/trunk/lucene/core/build/libs/lucene-core-9.0.0-SNAPSHOT.jar src/main/perf/StoredFieldsBenchmark.java; java -cp /l/trunk/lucene/core/build/libs/lucene-core-9.0.0-SNAPSHOT.jar:. src/main/perf/StoredFieldsBenchmark.java /lucenedata/geonames/geonames.20160818.csv /l/indices/geonames BEST_SPEED

/** Benchmark indexing stored fields on 1M lines of Geonames. */
public class StoredFieldsBenchmark {

  public static void main(String args[]) throws Exception {
    if (args.length != 4) {
      System.err.println("Usage: StoredFieldsBenchmark /path/to/geonames.txt /path/to/index/dir (BEST_SPEED|BEST_COMPRESSION) doc_limit(or -1 means index all lines)");
      System.exit(1);
    }

    String geonamesDataPath = args[0];
    String indexPath = args[1];
    Lucene90Codec.Mode mode;
    switch (args[2]) {
      case "BEST_SPEED":
        mode = Lucene90Codec.Mode.BEST_SPEED;
        break;
      case "BEST_COMPRESSION":
        mode = Lucene90Codec.Mode.BEST_COMPRESSION;
        break;
      default:
        throw new AssertionError();
    }
    int docLimit = Integer.parseInt(args[3]);
    
    IOUtils.rm(Paths.get(indexPath));
    try (FSDirectory dir = FSDirectory.open(Paths.get(indexPath))) {

      System.err.println("Warm up indexing");
      try (IndexWriter iw = new IndexWriter(dir, getConfig(mode));
          LineNumberReader reader = new LineNumberReader(new InputStreamReader(Files.newInputStream(Paths.get(geonamesDataPath))))) {
        indexDocs(iw, reader, docLimit);
      }

      System.err.println("Now run indexing");
      try (IndexWriter iw = new IndexWriter(dir, getConfig(mode));
          LineNumberReader reader = new LineNumberReader(new InputStreamReader(Files.newInputStream(Paths.get(geonamesDataPath))))) {
        long t0 = System.nanoTime();
        indexDocs(iw, reader, docLimit);
        System.out.println(String.format(Locale.ROOT, "Indexing time: %d msec", (System.nanoTime() - t0) / 1_000_000));
      }

      long storeSizeBytes = 0;
      for (String f : dir.listAll()) {
        storeSizeBytes += dir.fileLength(f);
      }
      System.out.println(String.format(Locale.ROOT, "Stored fields size: %.3f MB", storeSizeBytes / 1024. / 1024.));

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        System.err.println("Warm up searching");
        getDocs(reader);

        System.err.println("Now run searching");
        // Take the min across multiple runs to decrease noise
        long minDurationNS = Long.MAX_VALUE;
        for (int i = 0; i < 10; ++i) {
          long t0 = System.nanoTime();
          getDocs(reader);
          minDurationNS = Math.min(minDurationNS, System.nanoTime() - t0);
        }
        System.out.println(String.format(Locale.ROOT, "Retrieved time: %.5f msec", minDurationNS / 1_000_000.));
      }
    }
  }

  private static IndexWriterConfig getConfig(Lucene90Codec.Mode mode) {
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setOpenMode(OpenMode.CREATE);
    iwc.setCodec(new Lucene90Codec(mode));
    iwc.setMergeScheduler(new SerialMergeScheduler());
    // provoke much segments, lots of compress/deompress/bulk copy:
    iwc.setMaxBufferedDocs(100);
    iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    return iwc;
  }

  static void indexDocs(IndexWriter iw, LineNumberReader reader, int docLimit) throws Exception {
    Document doc = new Document();
    Field fields[] = new Field[19];
    for (int i = 0; i < fields.length; i++) {
      fields[i] = new StoredField("field " + i, "");
      doc.add(fields[i]);
    }

    String line = null;
    while ((line = reader.readLine()) != null) {
      if (reader.getLineNumber() % 10000 == 0) {
        System.err.println("doc: " + reader.getLineNumber());
      }
      if (docLimit != -1 && reader.getLineNumber() == docLimit) {
        break;
      }
      String values[] = line.split("\t");
      if (values.length != fields.length) {
        throw new RuntimeException("bogus: " + values);
      }
      for (int i = 0; i < values.length; i++) {
        fields[i].setStringValue(values[i]);
      }
      iw.addDocument(doc);
    }
    iw.flush();
  }

  static int DUMMY;

  static void getDocs(IndexReader reader) throws IOException {
    int docId = 42;
    for (int i = 0; i < 10_000; ++i) {
      Document doc = reader.document(docId);
      DUMMY += doc.getFields().size(); // Prevent the JVM from optimizing away the read of the stored document
      docId = (docId + 65535) % reader.maxDoc();
    }
  }
}
