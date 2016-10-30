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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.IOUtils;

public class SearchTaxis {

  private static class SearchThread extends Thread {
    final int threadID;
    final boolean sparse;
    final IndexSearcher searcher;
    final int iters;
    final Object printLock;
    final Random random;
    
    public SearchThread(int threadID, boolean sparse, IndexSearcher searcher, int iters, Object printLock, Random random) {
      this.threadID = threadID;
      this.sparse = sparse;
      this.searcher = searcher;
      this.iters = iters;
      this.printLock = printLock;
      this.random = random;
    }
    
    @Override
    public void run() {
      try {
        _run();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    private void _run() throws IOException {
      for(int i=0;i<iters;i++) {
        String color;
        String sortField;
        if (random.nextBoolean()) {
          color = "y";
          if (sparse) {
            sortField = "yellow_pickup_longitude";
          } else {
            sortField = "pickup_longitude";
          }
        } else {
          color = "g";
          if (sparse) {
            sortField = "green_pickup_longitude";
          } else {
            sortField = "pickup_longitude";
          }
        }
              
        Query query = new TermQuery(new Term("cab_color", color));
        Sort sort;
        if (random.nextBoolean()) {
          sort = new Sort(new SortField(sortField, SortField.Type.DOUBLE));
        } else {
          sort = null;
        }

        long t0 = System.nanoTime();
        TopDocs hits;
        if (sort == null) {
          hits = searcher.search(query, 10);
        } else {
          hits = searcher.search(query, 10, sort);
        }
        long t1 = System.nanoTime();

        synchronized(printLock) {
          System.out.println("T" + threadID + " " + color + " sort=" + sort + ": " + hits.totalHits + " hits in " + ((t1-t0)/1000000.) + " msec");
        }
      }
    }
  }
  
  public static void main(String[] args) throws IOException, InterruptedException {
    Path indexPath = Paths.get(args[0]);

    String sparseOrNot = args[1];
    boolean sparse;
    if (sparseOrNot.equals("sparse")) {
      sparse = true;
    } else if (sparseOrNot.equals("nonsparse")) {
      sparse = false;
    } else {
      throw new IllegalArgumentException("expected sparse or nonsparse but got: " + sparseOrNot);
    }
    
    Directory dir = FSDirectory.open(indexPath);

    IndexReader reader = DirectoryReader.open(dir);
    System.out.println("READER: " + reader);
    long bytes = 0;
    for(LeafReaderContext ctx : reader.leaves()) {
      CodecReader cr = (CodecReader) ctx.reader();
      /*
      for(Accountable acc : cr.getChildResources()) {
        System.out.println("  " + Accountables.toString(acc));
      }
      */
      bytes += cr.ramBytesUsed();
    }
    System.out.println("HEAP: " + bytes);

    IndexSearcher searcher = new IndexSearcher(reader);

    Random random = new Random(17);

    Object printLock = new Object();

    Thread[] threads = new Thread[2];
    for(int i=0;i<threads.length;i++) {
      threads[i] = new SearchThread(i, sparse, searcher, 100, printLock, random);
      threads[i].start();
    }

    for(Thread thread : threads) {
      thread.join();
    }

    IOUtils.close(reader, dir);
  }
}
