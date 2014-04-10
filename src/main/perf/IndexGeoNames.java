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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.Version;

// rm -rf /l/scratch/indices/geonames; pushd core; ant jar; popd; javac -d /l/util/build -cp build/core/classes/java:build/analysis/common/classes/java /l/util/src/main/perf/IndexGeoNames.java; java -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.IndexGeoNames /lucenedata/geonames/allCountries.txt /l/scratch/indices/geonames 3

// javac -d /l/util/build -cp build/core/classes/java:build/analysis/common/classes/java /l/util/src/main/perf/IndexGeoNames.java

// java -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.IndexGeoNames /lucenedata/geonames/allCountries.txt /l/scratch/indices/geonames 3

public class IndexGeoNames {

  public static void main(String[] args) throws Exception {
    String geoNamesFile = args[0];
    File indexPath = new File(args[1]);
    int numThreads = Integer.parseInt(args[2]);
    if (indexPath.exists()) {
      throw new IllegalArgumentException("please remove indexPath \"" + indexPath + "\" before running");
    }

    Directory dir = FSDirectory.open(indexPath);
    IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_50, new StandardAnalyzer(Version.LUCENE_50));
    //iwc.setRAMBufferSizeMB(350);
    iwc.setRAMBufferSizeMB(64);
    //iwc.setInfoStream(new PrintStreamInfoStream(System.out));
    iwc.setMergePolicy(NoMergePolicy.NO_COMPOUND_FILES);
    //((ConcurrentMergeScheduler) iwc.getMergeScheduler()).setMaxMergesAndThreads(3, 1);
    final IndexWriter w = new IndexWriter(dir, iwc);

    final FieldType doubleFieldType = new FieldType(DoubleField.TYPE_NOT_STORED);
    //doubleFieldType.setNumericPrecisionStep(8);
    doubleFieldType.freeze();

    // 64K buffer:
    InputStream is = new FileInputStream(geoNamesFile);
    final BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"), 1 << 16);
    final AtomicInteger docsIndexed = new AtomicInteger();

    final long startMS = System.currentTimeMillis();
    final Field.Store store = Field.Store.YES;

    Thread[] threads = new Thread[numThreads];
    for(int i=0;i<numThreads;i++) {
      threads[i] = new Thread() {
          @Override
          public void run() {
            while (true) {
              try {
                // Curiously BufferedReader.readLine seems to be thread-safe...
                String line = reader.readLine();
                if (line == null) {
                  break;
                }
                String[] values = line.split("\t");

                Document doc = new Document();

                doc.add(new StringField("geoNameID", values[0], store));
                doc.add(new TextField("name", values[1], store));
                doc.add(new TextField("asciiName", values[2], store));
                doc.add(new TextField("alternateNames", values[3], store));

                if (values[4].isEmpty() == false) {
                  double v = Double.parseDouble(values[4]);
                  doc.add(new DoubleField("latitude", v, store));
                  doc.add(new DoubleDocValuesField("latitude", v));
                }
                if (values[5].isEmpty() == false) {
                  double v = Double.parseDouble(values[5]);
                  doc.add(new DoubleField("longitude", v, store));
                  doc.add(new DoubleDocValuesField("longitude", v));
                }

                doc.add(new StringField("featureClass", values[6], store));
                doc.add(new StringField("featureCode", values[7], store));
                doc.add(new StringField("countryCode", values[8], store));
                doc.add(new StringField("cc2", values[9], store));
                doc.add(new StringField("admin1Code", values[10], store));
                doc.add(new StringField("admin2Code", values[11], store));
                doc.add(new StringField("admin3Code", values[12], store));
                doc.add(new StringField("admin4Code", values[13], store));

                if (values[14].isEmpty() == false) {
                  long v = Long.parseLong(values[14]);
                  doc.add(new LongField("population", v, store));
                  doc.add(new NumericDocValuesField("population", v));
                }
                if (values[15].isEmpty() == false) {
                  long v = Long.parseLong(values[15]);
                  doc.add(new LongField("elevation", v, store));
                  doc.add(new NumericDocValuesField("elevation", v));
                }
                doc.add(new StringField("dem", values[16], store));
                doc.add(new StringField("timezone", values[17], store));
                // TODO: modification date
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
          }
        };
      threads[i].start();
    }
    for(int i=0;i<numThreads;i++) {
      threads[i].join();
    }
    long ms = System.currentTimeMillis();
    System.out.println(docsIndexed + ": " + ((ms - startMS)/1000.0) + " sec");
    w.commit();
    w.close();
    dir.close();
  }
}
