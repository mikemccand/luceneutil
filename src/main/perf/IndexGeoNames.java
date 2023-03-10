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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
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
//import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.PrintStreamInfoStream;

// rm -rf /l/scratch/indices/geonames; pushd core; ant jar; popd; javac -d /l/util/build -cp build/core/classes/java:build/analysis/common/classes/java /l/util/src/main/perf/IndexGeoNames.java; java -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.IndexGeoNames /lucenedata/geonames/allCountries.txt /l/scratch/indices/geonames 4 8

// javac -d /l/util/build -cp build/core/classes/java:build/analysis/common/classes/java /l/util/src/main/perf/IndexGeoNames.java

// java -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.IndexGeoNames /lucenedata/geonames/allCountries.txt /l/scratch/indices/geonames 4

public class IndexGeoNames {

  private static final int BATCH_SIZE = 128;

  final static boolean normal = false;

  public static void main(String[] args) throws Exception {
    String geoNamesFile = args[0];
    Path indexPath = Paths.get(args[1]);
    int numThreads = Integer.parseInt(args[2]);

    Directory dir = FSDirectory.open(indexPath);
    //IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_48, new StandardAnalyzer(Version.LUCENE_48));
    IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
    iwc.setOpenMode(OpenMode.CREATE);
    //iwc.setRAMBufferSizeMB(350);
    iwc.setInfoStream(new PrintStreamInfoStream(System.out));
    if (normal == false) {
      iwc.setRAMBufferSizeMB(128);
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      //iwc.setMergePolicy(NoMergePolicy.NO_COMPOUND_FILES);
    } else {
      // 5/5 segments:
      iwc.setMaxBufferedDocs(157234);
      iwc.setRAMBufferSizeMB(-1);
    }
    //((ConcurrentMergeScheduler) iwc.getMergeScheduler()).setMaxMergesAndThreads(3, 1);
    final IndexWriter w = new IndexWriter(dir, iwc);

    final Field.Store store = Field.Store.NO;

    // 64K buffer:
    InputStream is = new FileInputStream(geoNamesFile);
    final BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"), 1 << 16);
    final AtomicInteger docsIndexed = new AtomicInteger();

    final long startMS = System.currentTimeMillis();
    Thread[] threads = new Thread[numThreads];

    // With reuse it's ~ 38% faster (41.8 sec vs 67.0 sec):
    final boolean reuseDocAndFields = false;
    // With 20 threads, it's ~35% faster with batching:
    final boolean batchAddDocuments = false;

    final AtomicBoolean done = new AtomicBoolean();
    final ArrayBlockingQueue<Deque<String>> workQueue = new ArrayBlockingQueue<>(1000);

    for(int i=0;i<numThreads;i++) {
      threads[i] = new Thread() {
          @Override
          public void run() {
            ParsePosition datePos = new ParsePosition(0);
            SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd", Locale.US);

            if (reuseDocAndFields) {
              List<IndexableField> baseDoc = new ArrayList<>();
              IntPoint geoNameID = new IntPoint("geoNameID", 0);
              baseDoc.add(geoNameID);
              TextField nameField = new TextField("name", "", store);
              baseDoc.add(nameField);
              TextField asciiNameField = new TextField("asciiName", "", store);
              baseDoc.add(asciiNameField);
              TextField alternateNameField = new TextField("alternateNames", "", store);
              baseDoc.add(alternateNameField);
              DoubleField latitude = new DoubleField("latitude", 0, store);
              DoubleField longitude = new DoubleField("longitude", 0, store);
              StringField featureClassField = new StringField("featureClass", "", store);
              baseDoc.add(featureClassField);
              StringField featureCodeField = new StringField("featureCode", "", store);
              baseDoc.add(featureCodeField);
              KeywordField countryCodeField = new KeywordField("countryCode", "", store);
              baseDoc.add(countryCodeField);
              StringField cc2Field = new StringField("cc2", "", store);
              baseDoc.add(cc2Field);
              StringField admin1Field = new StringField("admin1", "", store);
              baseDoc.add(admin1Field);
              StringField admin2Field = new StringField("admin2", "", store);
              baseDoc.add(admin2Field);
              StringField admin3Field = new StringField("admin3", "", store);
              baseDoc.add(admin3Field);
              StringField admin4Field = new StringField("admin4", "", store);
              baseDoc.add(admin4Field);
              LongField population = new LongField("population", 0, store);
              LongField elevation = new LongField("elevation", 0, store);
              IntField dem = new IntField("dem", 0, store);
              KeywordField tzField = new KeywordField("timezone", "", store);
              baseDoc.add(tzField);
              LongField modified = new LongField("modified", 0, store);

              Deque<String> batch = null;
              while (true) {
                try {

                  if (batch == null || batch.isEmpty()) {
                    batch = workQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (batch == null) {
                      if (done.get()) {
                        break;
                      } else {
                        continue;
                      }
                    }
                  }

                  String line = batch.poll();
                  if (line == null) {
                    break;
                  }
                  String[] values = line.split("\t");

                  List<IndexableField> doc = new ArrayList<>(baseDoc);

                  geoNameID.setIntValue(Integer.parseInt(values[0]));
                  nameField.setStringValue(values[1]);
                  asciiNameField.setStringValue(values[2]);
                  alternateNameField.setStringValue(values[3]);

                  if (values[4].isEmpty() == false) {
                    double v = Double.parseDouble(values[4]);
                    latitude.setDoubleValue(v);
                    doc.add(latitude);
                  }
                  if (values[5].isEmpty() == false) {
                    double v = Double.parseDouble(values[5]);
                    longitude.setDoubleValue(v);
                    doc.add(longitude);
                  }

                  featureClassField.setStringValue(values[6]);
                  featureCodeField.setStringValue(values[7]);
                  countryCodeField.setStringValue(values[8]);
                  cc2Field.setStringValue(values[9]);
                  admin1Field.setStringValue(values[10]);
                  admin2Field.setStringValue(values[11]);
                  admin3Field.setStringValue(values[12]);
                  admin4Field.setStringValue(values[13]);

                  if (values[14].isEmpty() == false) {
                    long v = Long.parseLong(values[14]);
                    population.setLongValue(v);
                    doc.add(population);
                  }
                  if (values[15].isEmpty() == false) {
                    long v = Long.parseLong(values[15]);
                    elevation.setLongValue(v);
                    doc.add(elevation);
                  }
                  if (values[16].isEmpty() == false) {
                    int v = Integer.parseInt(values[16]);
                    dem.setIntValue(v);
                    doc.add(dem);
                  }

                  tzField.setStringValue(values[17]);
                  if (values[18].isEmpty() == false) {
                    datePos.setIndex(0);
                    Date date = dateParser.parse(values[18], datePos);
                    modified.setLongValue(date.getTime());
                    doc.add(modified);
                  }
                  w.addDocument(doc);
                  int count = docsIndexed.incrementAndGet();
                  if (count % 200000 == 0) {
                    long ms = System.currentTimeMillis();
                    System.out.println(count + ": " + ((ms - startMS)/1000.0) + " sec");
                  }
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            } else {
              Deque<String> batch = null;
              List<Document> documentBatch = new ArrayList<>();
              try {
                while (true) {

                  if (batch == null || batch.isEmpty()) {
                    batch = workQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (batch == null) {
                      if (done.get()) {
                        break;
                      } else {
                        continue;
                      }
                    }
                  }

                  String line = batch.poll();
                  if (line == null) {
                    break;
                  }
                  String[] values = line.split("\t");

                  Document doc = new Document();

                  doc.add(new IntPoint("geoNameID", Integer.parseInt(values[0])));
                  doc.add(new TextField("name", values[1], store));
                  doc.add(new TextField("asciiName", values[2], store));
                  doc.add(new TextField("alternateNames", values[3], store));

                  if (values[4].isEmpty() == false) {
                    double v = Double.parseDouble(values[4]);
                    doc.add(new DoubleField("latitude", v, store));
                  }
                  if (values[5].isEmpty() == false) {
                    double v = Double.parseDouble(values[5]);
                    doc.add(new DoubleField("longitude", v, store));
                  }

                  doc.add(new StringField("featureClass", values[6], store));
                  doc.add(new StringField("featureCode", values[7], store));
                  doc.add(new KeywordField("countryCode", values[8], store));
                  doc.add(new StringField("cc2", values[9], store));
                  doc.add(new StringField("admin1Code", values[10], store));
                  doc.add(new StringField("admin2Code", values[11], store));
                  doc.add(new StringField("admin3Code", values[12], store));
                  doc.add(new StringField("admin4Code", values[13], store));

                  if (values[14].isEmpty() == false) {
                    long v = Long.parseLong(values[14]);
                    doc.add(new LongField("population", v, store));
                  }
                  if (values[15].isEmpty() == false) {
                    long v = Long.parseLong(values[15]);
                    doc.add(new LongField("elevation", v, store));
                  }
                  if (values[16].isEmpty() == false) {
                    doc.add(new IntField("dem", Integer.parseInt(values[16]), store));
                  }

                  doc.add(new KeywordField("timezone", values[17], store));

                  if (values[18].isEmpty() == false) {
                    datePos.setIndex(0);
                    Date date = dateParser.parse(values[18], datePos);
                    doc.add(new LongField("modified", date.getTime(), store));
                  }

                  if (batchAddDocuments) {
                    documentBatch.add(doc);
                    if (documentBatch.size() == BATCH_SIZE) {
                      w.addDocuments(documentBatch);
                      documentBatch.clear();
                    }
                  } else {
                    w.addDocument(doc);
                  }

                  int count = docsIndexed.incrementAndGet();
                  if (count % 200000 == 0) {
                    long ms = System.currentTimeMillis();
                    System.out.println(count + ": " + ((ms - startMS)/1000.0) + " sec");
                  }
                }
                if (documentBatch.isEmpty() == false) {
                  w.addDocuments(documentBatch);
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          }
        };
    }
    for (Thread thread : threads) {
      thread.start();
    }
    Deque<String> batch = new ArrayDeque<>();
    for (String line = reader.readLine(); ; line = reader.readLine()) {
      if (line == null) {
        if (batch.isEmpty() == false) {
          workQueue.put(batch);
        }
        break;
      }
      batch.add(line);
      if (batch.size() == BATCH_SIZE) {
        workQueue.put(batch);
        batch = new ArrayDeque<>();
      }
    }
    done.set(true);
    for (Thread thread : threads) {
      thread.join();
    }
    w.close();
    long ms = System.currentTimeMillis();
    System.out.println(docsIndexed + ": " + ((ms - startMS)/1000.0) + " sec");
    //System.out.println("tot conflicts: " + BytesRefHash.totConflict);
    //w.shutdown(normal);
    dir.close();
  }
}
