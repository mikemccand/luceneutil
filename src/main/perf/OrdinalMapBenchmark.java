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

import org.apache.lucene.codecs.lucene91.Lucene91Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

// javac -cp /l/trunk/lucene/core/build/libs/lucene-core-9.0.0-SNAPSHOT.jar src/main/perf/OrdinalMapBenchmark.java; java -cp /l/trunk/lucene/core/build/libs/lucene-core-9.0.0-SNAPSHOT.jar:. src/main/perf/OrdinalMapBenchmark.java /lucenedata/geonames/geonames.20160818.csv /l/indices/geonames -1

/** Benchmark indexing stored fields on 1M lines of Geonames. */
public class OrdinalMapBenchmark {

  public static void main(String args[]) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: OrdinalMapBenchmark /path/to/geonames.txt /path/to/index/dir doc_limit(or -1 means index all lines)");
      System.exit(1);
    }

    String geonamesDataPath = args[0];
    String indexPath = args[1];
    int docLimit = Integer.parseInt(args[2]);
    
    IOUtils.rm(Paths.get(indexPath));
    try (FSDirectory dir = FSDirectory.open(Paths.get(indexPath))) {

      System.err.println("Start indexing");
      try (IndexWriter iw = new IndexWriter(dir, getConfig());
          LineNumberReader reader = new LineNumberReader(new InputStreamReader(Files.newInputStream(Paths.get(geonamesDataPath))))) {
        indexDocs(iw, reader, docLimit);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        System.err.println("Warm up");
        for (String field : new String[] { "id", "name", "country_code", "time_zone" }) {
          loadOrdinalMap(reader, field);
        }

        System.err.println("Now run benchmark");
        for (String field : new String[] { "id", "name", "country_code", "time_zone" }) {
          // Take the min across multiple runs to decrease noise
          long minDurationNS = Long.MAX_VALUE;
          for (int i = 0; i < 10; ++i) {
            long t0 = System.nanoTime();
            loadOrdinalMap(reader, field);
            minDurationNS = Math.min(minDurationNS, System.nanoTime() - t0);
          }
          System.out.println(String.format(Locale.ROOT, "%s: %.5f msec", field, minDurationNS / 1_000_000.));
        }
      }
    }
  }

  private static IndexWriterConfig getConfig() {
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setOpenMode(OpenMode.CREATE);
    iwc.setCodec(new Lucene91Codec());
    iwc.setMergeScheduler(new SerialMergeScheduler());
    // TieredMergePolicy's 2MB floor segment size would create an index that has few segments compared to
    // real-world usage where documents have more fields including stored fields
    iwc.setMergePolicy(new LogDocMergePolicy());
    iwc.setMaxBufferedDocs(10_000);
    iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    return iwc;
  }

  static void indexDocs(IndexWriter iw, LineNumberReader reader, int docLimit) throws Exception {
    Document doc = new Document();
    Field id = new SortedDocValuesField("id", new BytesRef());
    doc.add(id);
    Field name = new SortedDocValuesField("name", new BytesRef());
    doc.add(name);
    Field countryCode = new SortedDocValuesField("country_code", new BytesRef());
    doc.add(countryCode);
    Field timeZone = new SortedDocValuesField("time_zone", new BytesRef());
    doc.add(timeZone);

    String line = null;
    while ((line = reader.readLine()) != null) {
      if (reader.getLineNumber() % 100000 == 0) {
        System.err.println("doc: " + reader.getLineNumber());
      }
      if (docLimit != -1 && reader.getLineNumber() == docLimit) {
        break;
      }
      String values[] = line.split("\t");
      if (values.length != 19) {
        throw new RuntimeException("bogus: " + values);
      }
      for (int i = 0; i < values.length; i++) {
        id.setBytesValue(new BytesRef(values[0]));
        name.setBytesValue(new BytesRef(values[1]));
        countryCode.setBytesValue(new BytesRef(values[8]));
        timeZone.setBytesValue(new BytesRef(values[17]));
      }
      iw.addDocument(doc);
    }
    iw.flush();
  }

  static Object DUMMY;

  static void loadOrdinalMap(IndexReader reader, String field) throws IOException {
    SortedDocValues[] values = new SortedDocValues[reader.leaves().size()];
    for (LeafReaderContext context : reader.leaves()) {
      values[context.ord] = DocValues.getSorted(context.reader(), field);
    }
    for (int i = 0; i < 10; ++i) {
      OrdinalMap map = OrdinalMap.build(null, values, 0f);
      if (map.getValueCount() == 0) {
        throw new Error("missing field: " + field);
      }
      DUMMY = map; // prevent the JVM from optimizing away the OrdinalMap construction
    }
  }
}
