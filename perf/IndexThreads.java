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

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.index.IndexDocument;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.util._TestUtil;

class IndexThreads {

  final IngestRatePrinter printer;
  final CountDownLatch startLatch = new CountDownLatch(1);
  final AtomicBoolean stop;
  final AtomicBoolean failed;
  final LineFileDocs docs;
  final Thread[] threads;

  public IndexThreads(Random random, IndexWriter w, TaxonomyWriter facetWriter, String lineFile, boolean storeBody, boolean tvsBody,
                      boolean bodyPostingsOffsets,
                      int numThreads, int docCountLimit, boolean addGroupingFields, boolean printDPS,
                      boolean doUpdate, float docsPerSecPerThread, boolean cloneDocs) throws IOException, InterruptedException {
    final AtomicInteger groupBlockIndex;

    docs = new LineFileDocs(lineFile, false, storeBody, tvsBody, bodyPostingsOffsets, cloneDocs, facetWriter);
    if (addGroupingFields) {
      IndexThread.group100 = randomStrings(100, random);
      IndexThread.group10K = randomStrings(10000, random);
      IndexThread.group100K = randomStrings(100000, random);
      IndexThread.group1M = randomStrings(1000000, random);
      groupBlockIndex = new AtomicInteger();
    } else {
      groupBlockIndex = null;
    }

    threads = new Thread[numThreads];
    
    final CountDownLatch stopLatch = new CountDownLatch(numThreads);
    final AtomicInteger count = new AtomicInteger();
    stop = new AtomicBoolean(false);
    failed = new AtomicBoolean(false);

    for(int thread=0;thread<numThreads;thread++) {
      threads[thread] = new IndexThread(random, startLatch, stopLatch, w, docs, docCountLimit, count, doUpdate, groupBlockIndex, stop, docsPerSecPerThread, failed);
      threads[thread].start();
    }

    Thread.sleep(10);

    if (printDPS) {
      printer = new IngestRatePrinter(count, stop);
      printer.start();
    } else {
      printer = null;
    }
  }

  public void start() {
    startLatch.countDown();
  }

  public long getBytesIndexed() {
    return docs.getBytesIndexed();
  }

  public void stop() throws InterruptedException, IOException {
    stop.getAndSet(true);
    for(Thread t : threads) {
      t.join();
    }
    if (printer != null) {
      printer.join();
    }
    docs.close();
  }

  public boolean done() {
    for(Thread t: threads) {
      if (t.isAlive()) {
        return false;
      }
    }

    return true;
  }

  private static class IndexThread extends Thread {
    public static String[] group100;
    public static String[] group100K;
    public static String[] group10K;
    public static String[] group1M;
    private final LineFileDocs docs;
    private final int numTotalDocs;
    private final IndexWriter w;
    private final AtomicBoolean stop;
    private final AtomicInteger count;
    private final AtomicInteger groupBlockIndex;
    private final boolean doUpdate;
    private final CountDownLatch startLatch;
    private final CountDownLatch stopLatch;
    private final float docsPerSec;
    private final Random random;
    private final AtomicBoolean failed;

    public IndexThread(Random random, CountDownLatch startLatch, CountDownLatch stopLatch, IndexWriter w,
                       LineFileDocs docs,
                       int numTotalDocs, AtomicInteger count, boolean doUpdate, AtomicInteger groupBlockIndex,
                       AtomicBoolean stop, float docsPerSec, AtomicBoolean failed) {
      this.startLatch = startLatch;
      this.stopLatch = stopLatch;
      this.w = w;
      this.docs = docs;
      this.numTotalDocs = numTotalDocs;
      this.count = count;
      this.doUpdate = doUpdate;
      this.groupBlockIndex = groupBlockIndex;
      this.stop = stop;
      this.docsPerSec = docsPerSec;
      this.random = random;
      this.failed = failed;
    }

    @Override
    public void run() {
      final int maxDoc = w.maxDoc();
      try {
        final LineFileDocs.DocState docState = docs.newDocState();
        final Field idField = docState.id;
        final long tStart = System.currentTimeMillis();
        final Field group100Field;
        final Field group100KField;
        final Field group10KField;
        final Field group1MField;
        final Field groupBlockField;
        final Field groupEndField;
        if (group100 != null) {
          group100Field = new StringField("group100", "", Field.Store.NO);
          docState.doc.add(group100Field);
          group10KField = new StringField("group10K", "", Field.Store.NO);
          docState.doc.add(group10KField);
          group100KField = new StringField("group100K", "", Field.Store.NO);
          docState.doc.add(group100KField);
          group1MField = new StringField("group1M", "", Field.Store.NO);
          docState.doc.add(group1MField);
          groupBlockField = new StringField("groupblock", "", Field.Store.NO);
          docState.doc.add(groupBlockField);
          // Binary marker field:
          groupEndField = new StringField("groupend", "x", Field.Store.NO);
        } else {
          group100Field = null;
          group100KField = null;
          group10KField = null;
          group1MField = null;
          groupBlockField = null;
          groupEndField = null;
        }

        try {
          startLatch.await();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }

        if (group100 != null) {

          if (numTotalDocs == -1) {
            throw new IllegalStateException("must specify numTotalDocs when indexing doc blocks for grouping");
          }

          // Add docs in blocks:
          
          final String[] groupBlocks;
          if (numTotalDocs >= 5000000) {
            groupBlocks = group1M;
          } else if (numTotalDocs >= 500000) {
            groupBlocks = group100K;
          } else {
            groupBlocks = group10K;
          }
          final double docsPerGroupBlock = numTotalDocs / (double) groupBlocks.length;

          while (!stop.get()) {
            final int groupCounter = groupBlockIndex.getAndIncrement();
            if (groupCounter >= groupBlocks.length) {
              break;
            }
            final int numDocs;
            // This will toggle between X and X+1 docs,
            // converging over time on average to the
            // floating point docsPerGroupBlock:
            if (groupCounter == groupBlocks.length-1) {
              numDocs = numTotalDocs - ((int) (groupCounter*docsPerGroupBlock));
            } else {
              numDocs = ((int) ((1+groupCounter)*docsPerGroupBlock)) - ((int) (groupCounter*docsPerGroupBlock));
            }
            groupBlockField.setStringValue(groupBlocks[groupCounter]);

            w.addDocuments(new Iterable<IndexDocument>() {
                @Override
                public Iterator<IndexDocument> iterator() {
                  return new Iterator<IndexDocument>() {
                    int upto;
                    Document doc;

                    @Override
                    public boolean hasNext() {
                      if (upto < numDocs) {
                        try {
                          doc = docs.nextDoc(docState);
                        } catch (IOException ioe) {
                          throw new RuntimeException(ioe);
                        }
                        if (doc == null) {
                          return false;
                        }

                        final int id = LineFileDocs.idToInt(idField.stringValue());
                        if (id >= numTotalDocs) {
                          throw new IllegalStateException();
                        }
                        if (((1+id) % 100000) == 0) {
                          System.out.println("Indexer: " + (1+id) + " docs... (" + (System.currentTimeMillis() - tStart) + " msec)");
                        }
                        group100Field.setStringValue(group100[id%100]);
                        group10KField.setStringValue(group10K[id%10000]);
                        group100KField.setStringValue(group100K[id%100000]);
                        group1MField.setStringValue(group1M[id%1000000]);
                        upto++;
                        if (upto == numDocs) {
                          doc.add(groupEndField);
                        }
                        count.incrementAndGet();
                        return true;
                      } else {
                        doc = null;
                        return false;
                      }
                    }

                    @Override
                    public IndexDocument next() {
                      return doc;
                    }

                    @Override
                    public void remove() {
                      throw new UnsupportedOperationException();
                    }
                  };
                }
              });

            docState.doc.removeField("groupend");
          }
        } else if (docsPerSec > 0 || doUpdate) {

          final long startNS = System.nanoTime();
          int threadCount = 0;
          while (!stop.get()) {
            final Document doc = docs.nextDoc(docState);
            if (doc == null) {
              break;
            }
            final int id = LineFileDocs.idToInt(idField.stringValue());
            if (numTotalDocs != -1 && id >= numTotalDocs) {
              break;
            }

            if (((1+id) % 100000) == 0) {
              System.out.println("Indexer: " + (1+id) + " docs... (" + (System.currentTimeMillis() - tStart) + " msec)");
            }
            // nocommit have a 'sometimesAdd' mode where 25%
            // of the time we add a new doc
            if (doUpdate) {
              final String updateID = LineFileDocs.intToID(random.nextInt(maxDoc));
              // NOTE: can't use docState.id in case doClone
              // was true
              doc.getField("id").setStringValue(updateID);
              w.updateDocument(new Term("id", updateID), doc);
            } else {
              w.addDocument(doc);
            }
            count.incrementAndGet();
            threadCount++;

            final long sleepNS = startNS + (long) (1000000000*(threadCount/docsPerSec)) - System.nanoTime();
            if (sleepNS > 0) {
              final long sleepMS = sleepNS/1000000;
              final int sleepNS2 = (int) (sleepNS - sleepMS*1000000);
              Thread.sleep(sleepMS, sleepNS2);
            }
          }
        } else {
          while (true) {
            final Document doc = docs.nextDoc(docState);
            if (doc == null) {
              break;
            }
            int docCount = count.incrementAndGet();
            if (numTotalDocs != -1 && docCount > numTotalDocs) {
              break;
            }
            if ((docCount % 100000) == 0) {
              System.out.println("Indexer: " + docCount + " docs... (" + (System.currentTimeMillis() - tStart) + " msec)");
            }
            w.addDocument(doc);
          }
        }
      } catch (Exception e) {
        failed.set(true);
        throw new RuntimeException(e);
      } finally {
        stopLatch.countDown();
      }
    }
  }

  private static class IngestRatePrinter extends Thread {

    private final AtomicInteger count;
    private final AtomicBoolean stop;
    public IngestRatePrinter(AtomicInteger count, AtomicBoolean stop){
      this.count = count;
      this.stop = stop;
    }
    
    @Override
		public void run() {
       long time = System.currentTimeMillis();
       System.out.println("startIngest: " + time);
       final long start = time;
       int lastCount = count.get();
       while(!stop.get()) {
         try {
           Thread.sleep(200);
         } catch(Exception ex) {
         }
         int numDocs = count.get();

         double current = numDocs - lastCount;
         long now = System.currentTimeMillis();
         double seconds = (now-time) / 1000.0d;
         System.out.println("ingest: " + (current / seconds) + " " + (now - start));
         time = now;
         lastCount = numDocs;
       }
    }
  }

  // NOTE: returned array might have dups
  private static String[] randomStrings(int count, Random random) {
    final String[] strings = new String[count];
    int i = 0;
    while(i < count) {
      final String s = _TestUtil.randomRealisticUnicodeString(random);
      if (s.length() >= 7) {
        strings[i++] = s;
      }
    }

    return strings;
  }
}
