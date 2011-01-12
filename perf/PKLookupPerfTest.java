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

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.search.*;


// javac -cp build/classes/java perf/PKLookupPerfTest.java; java -cp .:build/classes/java perf.PKLookupPerfTest MMapDirectory /lucene/indices/clean.svn.Standard.nd10M/index multi 1000 17
// NO deletions allowed
// Requires you use an index w/ docid primary key field
public class PKLookupPerfTest {

  // TODO: share w/ SearchPerfTest
  private static IndexCommit findCommitPoint(String commit, Directory dir) throws IOException {
    Collection<IndexCommit> commits = IndexReader.listCommits(dir);
    for (final IndexCommit ic : commits) {
      Map<String,String> map = ic.getUserData();
      String ud = null;
      if (map != null) {
        ud = map.get("userData");
        System.out.println("found commit=" + ud);
        if (ud != null && ud.equals(commit)) {
          return ic;
        }
      }
    }
    throw new RuntimeException("could not find commit '" + commit + "'");
  }

  public static void main(String[] args) throws IOException {

    final Directory dir;
    final String dirImpl = args[0];
    final String dirPath = args[1];
    final String commit = args[2];
    final int numLookups = Integer.parseInt(args[3]);
    final long seed = Long.parseLong(args[4]);

    if (dirImpl.equals("MMapDirectory")) {
      dir = new MMapDirectory(new File(dirPath));
    } else if (dirImpl.equals("NIOFSDirectory")) {
      dir = new NIOFSDirectory(new File(dirPath));
    } else if (dirImpl.equals("SimpleFSDirectory")) {
      dir = new SimpleFSDirectory(new File(dirPath));
    } else {
      throw new RuntimeException("unknown directory impl \"" + dirImpl + "\"");
    }

    System.out.println("Commit=" + commit);
    final IndexReader r = IndexReader.open(findCommitPoint(commit, dir), true);
    System.out.println("Reader=" + r);
    if (r.hasDeletions()) {
      throw new RuntimeException("can't handle deletions");
    }

    final IndexSearcher s = new IndexSearcher(r);
    final int maxDoc = r.maxDoc();
    final Random rand = new Random(seed);

    for(int cycle=0;cycle<2;cycle++) {
      System.out.println("Cycle: " + (cycle==0 ? "warm" : "test"));
      System.out.println("  Lookup...");
      final String[] lookup = new String[numLookups];
      final int[] docIDs = new int[numLookups];
      for(int iter=0;iter<numLookups;iter++) {
        lookup[iter] = Integer.toString(rand.nextInt(maxDoc));
      }
      Arrays.fill(docIDs, -1);

      final long tStart = System.currentTimeMillis();
      final AtomicBoolean failed = new AtomicBoolean(false);
      for(int iter=0;iter<numLookups;iter++) {
        final Term t = new Term("docid", "");
        final int iterFinal = iter;
        s.search(new TermQuery(t.createTerm(lookup[iter])),
                 new Collector() {
            int base;

            @Override
              public void collect(int docID) {
              if (docIDs[iterFinal] != -1) {
                failed.set(true);
              }
              docIDs[iterFinal] = base + docID;
            }

            @Override
            public void setNextReader(AtomicReaderContext context) {
              base = context.docBase;
            }

            @Override
              public boolean acceptsDocsOutOfOrder() {
              return true;
            }

            @Override
              public void setScorer(Scorer s) {
            }
          });
      }

      if (failed.get()) {
        throw new RuntimeException("at least one lookup produced more than one result");
      }

      System.out.println("  Verify...");
      for(int iter=0;iter<numLookups;iter++) {
        final String found = r.document(docIDs[iter]).get("docid");
        if (!found.equals(lookup[iter])) {
          throw new RuntimeException("lookup of docid=" + lookup[iter] + " hit wrong docid=" + found);
        }
      }
      
      // cycle 0 is for warming
      if (cycle == 1) {
        final long t = (System.currentTimeMillis() - tStart);
        System.out.println(t + " msec for " + numLookups + " lookups (" + (1000*t/numLookups) + " us per lookup)");
      }
    }

    s.close();
    r.close();
    dir.close();
  }
}
