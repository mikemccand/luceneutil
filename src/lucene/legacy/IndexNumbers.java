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
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
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
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.Version;

// rm -rf /l/scratch/indices/numbers; pushd core; ant jar; popd; javac -d /l/util/build -cp build/core/classes/java:build/analysis/common/classes/java /l/util/src/main/perf/IndexNumbers.java; java -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.IndexNumbers /lucenedata/elasticsearch/twitter/timestamps.txt /l/scratch/indices/numbers 4 8

public class IndexNumbers {

  final static boolean normal = false;

  public static void main(String[] args) throws Exception {
    String numbersFile = args[0];
    File indexPath = new File(args[1]);
    int numThreads = Integer.parseInt(args[2]);
    int precStep = Integer.parseInt(args[3]);
    if (indexPath.exists()) {
      throw new IllegalArgumentException("please remove indexPath \"" + indexPath + "\" before running");
    }

    Directory dir = FSDirectory.open(indexPath);
    IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
    //iwc.setRAMBufferSizeMB(350);
    //iwc.setInfoStream(new PrintStreamInfoStream(System.out));
    if (normal == false) {
      iwc.setRAMBufferSizeMB(64);
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
    } else {
      // 5/5 segments:
      // 3273681 docs in twitter timestamps
      iwc.setMaxBufferedDocs(59522);
      iwc.setRAMBufferSizeMB(-1);
    }
    //((ConcurrentMergeScheduler) iwc.getMergeScheduler()).setMaxMergesAndThreads(3, 1);
    final IndexWriter w = new IndexWriter(dir, iwc);

    final Field.Store store = Field.Store.NO;

    final FieldType doubleFieldType = new FieldType(store == Field.Store.NO ? DoubleField.TYPE_NOT_STORED : DoubleField.TYPE_STORED);
    doubleFieldType.setNumericPrecisionStep(precStep);
    doubleFieldType.freeze();

    final FieldType longFieldType = new FieldType(store == Field.Store.NO ? LongField.TYPE_NOT_STORED : LongField.TYPE_STORED);
    longFieldType.setNumericPrecisionStep(precStep);
    longFieldType.freeze();

    final FieldType intFieldType = new FieldType(store == Field.Store.NO ? IntField.TYPE_NOT_STORED : IntField.TYPE_STORED);
    intFieldType.setNumericPrecisionStep(precStep);
    intFieldType.freeze();

    // 64K buffer:
    InputStream is = new FileInputStream(numbersFile);
    final BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"), 1 << 16);
    final AtomicInteger docsIndexed = new AtomicInteger();

    final long startMS = System.currentTimeMillis();
    Thread[] threads = new Thread[numThreads];
    for(int i=0;i<numThreads;i++) {
      threads[i] = new Thread() {
          @Override
          public void run() {
            ParsePosition datePos = new ParsePosition(0);
            SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd", Locale.US);

            Document doc = new Document();
            Field field = new LongField("number", 0L, longFieldType);
            doc.add(field);

            while (true) {
              try {
                // Curiously BufferedReader.readLine seems to be thread-safe...
                String line = reader.readLine();
                if (line == null) {
                  break;
                }
                field.setLongValue(Long.parseLong(line.trim()));
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
    //System.out.println("tot conflicts: " + BytesRefHash.totConflict);
    if (normal == false) {
      w.abortMerges();
    }
    w.close();
    dir.close();
  }
}
