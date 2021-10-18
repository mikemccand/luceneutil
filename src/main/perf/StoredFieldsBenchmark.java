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

import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.lucene.codecs.lucene90.Lucene90Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

/** Benchmark indexing stored fields on 1M lines of Geonames. */
public class StoredFieldsBenchmark {

  public static void main(String args[]) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: StoredFieldsBenchmark /path/to/geonames.txt /path/to/index/dir (BEST_SPEED|BEST_COMPRESSION)");
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
    IOUtils.rm(Paths.get(indexPath));
    FSDirectory dir = FSDirectory.open(Paths.get(indexPath));

    System.out.println("Warm up");
    try (IndexWriter iw = new IndexWriter(dir, getConfig(mode));
        LineNumberReader reader = new LineNumberReader(new InputStreamReader(Files.newInputStream(Paths.get(geonamesDataPath))))) {
      indexDocs(iw, reader);
    }

    System.out.println("Now run");
    try (IndexWriter iw = new IndexWriter(dir, getConfig(mode));
        LineNumberReader reader = new LineNumberReader(new InputStreamReader(Files.newInputStream(Paths.get(geonamesDataPath))))) {
      long t0 = System.nanoTime();
      indexDocs(iw, reader);
      System.out.println("Millis: " + (System.nanoTime() - t0) / 1_000_000);
    }

    dir.close();
  }

  private static IndexWriterConfig getConfig(Lucene90Codec.Mode mode) {
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setOpenMode(OpenMode.CREATE);
    iwc.setCodec(new Lucene90Codec(mode));
    iwc.setMergeScheduler(new SerialMergeScheduler());
    iwc.setMaxBufferedDocs(100);
    iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    return iwc;
  }

  static void indexDocs(IndexWriter iw, LineNumberReader reader) throws Exception {
    Document doc = new Document();
    Field fields[] = new Field[19];
    for (int i = 0; i < fields.length; i++) {
      fields[i] = new StoredField("field " + i, "");
      doc.add(fields[i]);
    }

    String line = null;
    while ((line = reader.readLine()) != null) {
      if (reader.getLineNumber() % 10000 == 0) {
        System.out.println("doc: " + reader.getLineNumber());
      }
      if (reader.getLineNumber() == 1000000) {
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

}
