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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;

public class SearchTask extends Thread {

  protected final IndexSearcher s;
  private final QueryAndSort[] queries;
  private final int numIter;
  public final List<Result> results = new ArrayList<Result>();

  public SearchTask(Random r, IndexSearcher s, List<QueryAndSort> queriesList,
      int numIter, boolean shuffle) {
    this.s = s;
    List<QueryAndSort> queries = new ArrayList<QueryAndSort>(queriesList);
    if(shuffle)
      Collections.shuffle(queries, r);
    this.queries = queries.toArray(new QueryAndSort[queries.size()]);
    this.numIter = numIter;
  }

  public void run() {
    try {
      final IndexSearcher s = this.s;
      final QueryAndSort[] queries = this.queries;
      long totSum = 0;
      for (int iter = 0; iter < numIter; iter++) {
        for (int q = 0; q < queries.length; q++) {
          final QueryAndSort qs = queries[q];

          long t0 = System.nanoTime();
          final TopDocs hits;
          if (qs.s == null && qs.f == null) {
            hits = s.search(qs.q, 10);
          } else if (qs.s == null && qs.f != null) {
            hits = s.search(qs.q, qs.f, 10);
          } else {
            hits = s.search(qs.q, qs.f, 10, qs.s);
          }
          processHits(hits);
          final long delay = System.nanoTime() - t0;
          totSum += hits.totalHits;
          long check = 0;
          for (int i = 0; i < hits.scoreDocs.length; i++) {
            totSum += hits.scoreDocs[i].doc;
            check += hits.scoreDocs[i].doc;
          }
          Result r = new Result();
          r.t = delay;
          r.qs = qs;
          r.totHits = hits.totalHits;
          r.check = check;
          results.add(r);
        }
      }
      System.out.println("checksum=" + totSum);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
  
  protected void processHits(TopDocs hits) throws IOException {
    //
  }
}
