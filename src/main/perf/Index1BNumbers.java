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

import java.io.File;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

// rm -rf /p/indices/1bnumbers; pushd core; ant jar; popd; javac -d /lucene/util/build -cp build/core/classes/java:build/analysis/common/classes/java /lucene/util/src/main/perf/Index1BNumbers.java; java -cp /lucene/util/build:build/core/classes/java:build/analysis/common/classes/java perf.Index1BNumbers /p/indices/1bnumbers 4 8

public class Index1BNumbers {
  private static class MakeNumbers {

    final Random random;
    final long end;
    long curValue;
    int countLeft;

    public MakeNumbers() {
      random = new Random(17);
      curValue = 1397724815596L;
      end = curValue + 2 + 3600*24*1000;
      nextValue();
    }

    private void nextValue() {
      // 11.574074074074074 per msec
      while (countLeft <= 0) {
        curValue++;
        countLeft = (int) Math.round(3 * random.nextGaussian() + 11.6);
      }
    }

    public synchronized long next() {
      long value = curValue;
      countLeft--;
      nextValue();
      //System.out.println(value);
      return value;
    }
  }

  public static void main(String[] args) throws Exception {
    File indexPath = new File(args[0]);
    int numThreads = Integer.parseInt(args[1]);
    int precStep = Integer.parseInt(args[2]);
    if (indexPath.exists()) {
      throw new IllegalArgumentException("please remove indexPath \"" + indexPath + "\" before running");
    }

    Directory dir = FSDirectory.open(indexPath);
    IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
    iwc.setRAMBufferSizeMB(512);
    final IndexWriter w = new IndexWriter(dir, iwc);

    final Field.Store store = Field.Store.NO;

    final FieldType longFieldType = new FieldType(store == Field.Store.NO ? LongField.TYPE_NOT_STORED : LongField.TYPE_STORED);
    longFieldType.setNumericPrecisionStep(precStep);
    longFieldType.freeze();

    final MakeNumbers numbers = new MakeNumbers();

    final long startMS = System.currentTimeMillis();
    final AtomicInteger docsIndexed = new AtomicInteger();
    Thread[] threads = new Thread[numThreads];
    for(int i=0;i<numThreads;i++) {
      threads[i] = new Thread() {
          @Override
          public void run() {

            Document doc = new Document();
            Field field = new LongField("number", 0L, longFieldType);
            doc.add(field);

            while (true) {
              try {
                long v = numbers.next();
                if (v >= numbers.end) {
                  break;
                }
                field.setLongValue(v);
                w.addDocument(doc);
                int count = docsIndexed.incrementAndGet();
                if (count % 200000 == 0) {
                  long ms = System.currentTimeMillis();
                  System.out.println(count + ": " + ((ms - startMS)/1000.0) + " sec; " + v + " vs " + numbers.end + " (" + (numbers.end - v) + " left)");
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
    w.close();
    dir.close();
  }
}
