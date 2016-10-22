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
package perf;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.PrintStreamInfoStream;

// See README.nyctaxis for the chunked docs source

// First: cd to /foo/bar/baz/lucene-solr-clone/lucene

// Then: ant clean jar"

// Then: javac -cp build/core/classes/java /l/util/src/main/perf/IndexTaxis.java ; java -cp build/core/classes/java:/l/util/src/main/perf IndexTaxis /c/taxisjava 1 /lucenedata/nyc-taxi-data/alltaxis.25M.csv.blocks 

public class IndexTaxis {

  private static final int NEWLINE = (byte) '\n';
  private static final int COMMA = (byte) ',';
  private static final byte[] header = new byte[128];

  static long startNS;

  private static class Chunk {
    public final byte[] bytes;
    public final int docCount;

    public Chunk(byte[] bytes, int docCount) {
      this.bytes = bytes;
      this.docCount = docCount;
    }
  }

  private synchronized static Chunk readChunk(BufferedInputStream docs) throws IOException {
    int count = docs.read(header, 0, header.length);
    if (count == -1) {
      // end
      return null;
    }

    int upto = 0;
    while (upto < header.length) {
      if (header[upto] == NEWLINE) {
        break;
      }
      upto++;
    }
    if (upto == header.length) {
      throw new AssertionError();
    }
    String[] parts = new String(header, 0, upto, StandardCharsets.UTF_8).split(" ");
    if (parts.length != 2) {
      throw new AssertionError();
    }
    int byteCount = Integer.parseInt(parts[0]);
    int docCount = Integer.parseInt(parts[1]);
    byte[] chunk = new byte[byteCount];
    int fragment = header.length-upto-1;
    System.arraycopy(header, upto+1, chunk, 0, fragment);
    count = docs.read(chunk, fragment, chunk.length - fragment);
    if (count != chunk.length - fragment) {
      throw new AssertionError();
    }
    return new Chunk(chunk, docCount);
  }

  static void addOneField(Document doc, String fieldName, String rawValue) {
    // nocommit
    /*
    if (fieldName.equals("pick_up_lat")) {
      double value = Double.parseDouble(rawValue);
      doc.add(new DoublePoint(fieldName, value));
      doc.add(new SortedNumericDocValuesField(fieldName, NumericUtils.doubleToSortableLong(value)));
    }
    */
    switch(fieldName) {
    case "vendor_id":
    case "cab_color":
    case "payment_type":
    case "trip_type":
    case "rate_code":
    case "store_and_fwd_flag":
      doc.add(new StringField(fieldName, rawValue, Field.Store.NO));
      doc.add(new SortedSetDocValuesField(fieldName, new BytesRef(rawValue)));
      break;
    case "vendor_name":
      doc.add(new TextField(fieldName, rawValue, Field.Store.NO));
      break;
    case "pick_up_date_time":
    case "drop_off_date_time":
      {
        long value = Long.parseLong(rawValue);
        doc.add(new LongPoint(fieldName, value));
        doc.add(new SortedNumericDocValuesField(fieldName, value));
      }
      break;
    case "passenger_count":
      {
        int value = Integer.parseInt(rawValue);
        doc.add(new IntPoint(fieldName, value));
        doc.add(new SortedNumericDocValuesField(fieldName, value));
      }
      break;
    case "trip_distance":
    case "pick_up_lat":
    case "pick_up_lon":
    case "drop_off_lat":
    case "drop_off_lon":
    case "fare_amount":
    case "surcharge":
    case "mta_tax":
    case "extra":
    case "ehail_fee":
    case "improvement_surcharge":
    case "tip_amount":
    case "tolls_amount":
    case "total_amount":
      {
        double value;
        try {
          value = Double.parseDouble(rawValue);
        } catch (NumberFormatException nfe) {
          System.out.println("WARNING: failed to parse \"" + rawValue + "\" as double for field \"" + fieldName + "\"");
          return;
        }
        doc.add(new DoublePoint(fieldName, value));
        doc.add(new SortedNumericDocValuesField(fieldName, NumericUtils.doubleToSortableLong(value)));
      }
      break;
    default:
      throw new AssertionError("failed to handle field \"" + fieldName + "\"");
    }
  }

  /** Index all documents contained in one chunk */
  static void indexOneChunk(String[] fields, Chunk chunk, IndexWriter w, AtomicInteger docCounter, AtomicLong bytesCounter) throws IOException {

    Document doc = new Document();
    byte[] bytes = chunk.bytes;
    if (bytes[bytes.length-1] != NEWLINE) {
      throw new AssertionError();
    }
    w.addDocuments(new Iterable<Document>() {
        @Override
        public Iterator<Document> iterator() {
          return new Iterator<Document>() {
            private int i;
            private Document nextDoc;
            private boolean nextSet;
            private int lastLineStart;
            private int chunkDocCount;

            @Override
            public boolean hasNext() {
              if (nextSet == false) {
                setNextDoc();
                nextSet = true;
              }

              return nextDoc != null;
            }

            @Override
            public Document next() {
              assert nextSet;
              nextSet = false;
              Document result = nextDoc;
              nextDoc = null;
              return result;
            }

            private void setNextDoc() {
              Document doc = new Document();
              int fieldUpto = 0;
              int lastFieldStart = i;
              for(;i<bytes.length;i++) {
                byte b = bytes[i];
                if (b == NEWLINE || b == COMMA) {
                  if (i > lastFieldStart) {
                    String s = new String(bytes, lastFieldStart, i-lastFieldStart, StandardCharsets.UTF_8);
                    addOneField(doc, fields[fieldUpto], s);
                  }
                  if (b == NEWLINE) {
                    if (fieldUpto != fields.length-1) {
                      throw new AssertionError("fieldUpto=" + fieldUpto + " vs fields.length-1=" + (fields.length-1));
                    }
                    chunkDocCount++;
                    this.nextDoc = doc;
                    int x = docCounter.incrementAndGet();
                    long y = bytesCounter.addAndGet((i+1) - lastLineStart);
                    if (x % 100000 == 0) {
                      double sec = (System.nanoTime() - startNS)/1000000000.0;
                      System.out.println(String.format(Locale.ROOT, "%.1f sec: %d docs; %.1f docs/sec; %.1f MB/sec", sec, x, x/sec, (y/1024./1024.)/sec));
                    }
                    fieldUpto = 0;
                    i++;
                    lastLineStart = i;
                    return;
                  } else {
                    fieldUpto++;
                  }
                  lastFieldStart = i+1;
                }
              }
              // System.out.println("chunk doc count: " + chunkDocCount);
            }
          };
        }
      });
  }

  public static void main(String[] args) throws Exception {
    Path indexPath = Paths.get(args[0]);
    Directory dir = FSDirectory.open(indexPath);
    int threadCount = Integer.parseInt(args[1]);
    Path docsPath = Paths.get(args[2]);

    IndexWriterConfig iwc = new IndexWriterConfig();
    //System.out.println("NOW SET INFO STREAM");
    iwc.setRAMBufferSizeMB(1024.);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    iwc.setInfoStream(new PrintStreamInfoStream(System.out));
    //((ConcurrentMergeScheduler) iwc.getMergeScheduler()).disableAutoIOThrottle();

    final IndexWriter w = new IndexWriter(dir, iwc);

    BufferedInputStream docs = new BufferedInputStream(Files.newInputStream(docsPath, StandardOpenOption.READ));

    // parse the header fields
    List<String> fieldsList = new ArrayList<>();
    StringBuilder builder = new StringBuilder();
    while (true) {
      int x = docs.read();
      if (x == -1) {
        throw new IllegalArgumentException("hit EOF while trying to read CSV header; are you sure you have the right CSV file!");
      }
      byte b = (byte) x;
      if (b == NEWLINE) {
        fieldsList.add(builder.toString());
        break;
      } else if (b == COMMA) {
        fieldsList.add(builder.toString());
        builder.setLength(0);
      } else {
        // this is OK because headers are all ascii:
        builder.append((char) b);
      }
    }

    final String[] fields = fieldsList.toArray(new String[fieldsList.size()]);

    Thread[] threads = new Thread[threadCount];

    final AtomicInteger docCounter = new AtomicInteger();
    final AtomicLong bytesCounter = new AtomicLong();

    startNS = System.nanoTime();    

    for(int i=0;i<threadCount;i++) {
      final int threadID = i;
      threads[i] = new Thread() {
          @Override
          public void run() {
            try {
              _run();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          private void _run() throws IOException {
            while (true) {
              Chunk chunk = readChunk(docs);
              if (chunk == null) {
                break;
              }
              indexOneChunk(fields, chunk, w, docCounter, bytesCounter);
            }
          }
        };
      threads[i].start();
    }

    for(int i=0;i<threadCount;i++) {
      threads[i].join();
    }
    System.out.println("Indexing done; now close");

    w.close();
    docs.close();
  }
}
