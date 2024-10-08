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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

// javac -d /l/util/build -cp build/core/classes/java:build/analysis/common/classes/java /l/util/src/main/perf/SearchGeoNames.java

// java -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.SearchGeoNames /l/scratch/indices/geonames

// pushd core; ant compile; popd; javac -d /l/util/build -cp build/core/classes/java:build/analysis/common/classes/java /l/util/src/main/perf/SearchGeoNames.java; java -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.SearchGeoNames /l/scratch/indices/geonames.8precstep 8

public class SearchGeoNames {
  public static void main(String[] args) throws Exception {
    Path indexPath = Paths.get(args[0]);
    Directory dir = FSDirectory.open(indexPath);
    IndexReader r = DirectoryReader.open(dir);
    System.out.println("r=" + r);
    IndexSearcher s = new IndexSearcher(r);
    s.setQueryCache(null); // don't bench the cache

    SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd", Locale.US);
    System.out.println("t=" + dateParser.parse("2014-12-01", new ParsePosition(0)).getTime());

    searchOneField(s, getQueries(s, "geoNameID", 0, 10000000));
    searchOneField(s, getQueries(s, "latitude", -50.0, 50.0));
    searchOneField(s, getQueries(s, "longitude", -180.0, 180.0));

    // 1993-12-01 to 2014-12-01:
    searchOneField(s, getQueries(s, "modified", 754722000000L, 1417410000000L));

    r.close();
    dir.close();
  }

  static final int QUERY_COUNT = 100;
  static final int ITERS = 20;
  
  private static List<Query> getQueries(IndexSearcher s, String field, double min, double max) throws IOException {
    // Fixed seed so we test same queries:
    Random r = new Random(19);
    System.out.println("\nfield=" + field);

    List<Query> queries = new ArrayList<>();
    for(int j=0;j<QUERY_COUNT;j++) {
      double v1 = min + r.nextDouble() * (max-min);
      double v2 = min + r.nextDouble() * (max-min);
      double minV = Math.min(v1, v2);
      double maxV = Math.max(v1, v2);
      Query query = DoublePoint.newRangeQuery(field, minV, maxV);
      queries.add(query);
    }
    return queries;
  }

  private static List<Query> getQueries(IndexSearcher s, String field, long min, long max) throws IOException {
    // Fixed seed so we test same queries:
    Random r = new Random(19);
    System.out.println("\nfield=" + field);

    List<Query> queries = new ArrayList<>();
    for(int j=0;j<QUERY_COUNT;j++) {
      // NOTE: fails if max-min > Long.MAX_VALUE, but we don't do that:
      long v1 = min + ((r.nextLong()<<1)>>>1) % (max-min);
      long v2 = min + ((r.nextLong()<<1)>>>1) % (max-min);
      long minV = Math.min(v1, v2);
      long maxV = Math.max(v1, v2);
      Query query = LongPoint.newRangeQuery(field, minV, maxV);
      queries.add(query);
    }
    return queries;
  }

  private static List<Query> getQueries(IndexSearcher s, String field, int min, int max) throws IOException {
    // Fixed seed so we test same queries:
    Random r = new Random(19);
    System.out.println("\nfield=" + field);

    List<Query> queries = new ArrayList<>();
    for(int j=0;j<QUERY_COUNT;j++) {
      // NOTE: fails if max-min > Long.MAX_VALUE, but we don't do that:
      int v1 = min + r.nextInt(max-min);
      int v2 = min + r.nextInt(max-min);
      int minV = Math.min(v1, v2);
      int maxV = Math.max(v1, v2);
      Query query = IntPoint.newRangeQuery(field, minV, maxV);
      queries.add(query);
    }
    return queries;
  }

  private static void searchOneField(IndexSearcher s, List<Query> queries) throws IOException {
    // warmup
    long tot = 0;
    for(int i=0;i<ITERS;i++) {
      for(Query query : queries) {
        TopDocs hits = s.search(query, 10);
        tot += hits.totalHits.value;
      }
    }

    // test
    long t0 = System.nanoTime();
    for(int i=0;i<ITERS;i++) {
      for(Query query : queries) {
        TopDocs hits = s.search(query, 10);
        tot += hits.totalHits.value;
      }
    }
    long t1 = System.nanoTime();

    System.out.println(String.format(Locale.ROOT,
                                     "  msec=%.1f",
                                     ((double) t1-t0)/ITERS/1000000.0));

    // paranoia:
    System.out.println("  tot=" + tot);
  }
}

// NOTE: need this patch
// 
// Index: lucene/core/src/java/org/apache/lucene/search/MultiTermQueryWrapperFilter.java
// ===================================================================
// --- lucene/core/src/java/org/apache/lucene/search/MultiTermQueryWrapperFilter.java	(revision 1588017)
// +++ lucene/core/src/java/org/apache/lucene/search/MultiTermQueryWrapperFilter.java	(working copy)
// @@ -77,6 +77,8 @@
//  
//    /** Returns the field name for this query */
//    public final String getField() { return query.getField(); }
// +
// +  public static int rewriteTermCount;
//    
//    /**
//     * Returns a DocIdSet with documents that should be permitted in search
// @@ -104,6 +106,7 @@
//        final FixedBitSet bitSet = new FixedBitSet(context.reader().maxDoc());
//        DocsEnum docsEnum = null;
//        do {
// +        rewriteTermCount++;
//          // System.out.println("  iter termCount=" + termCount + " term=" +
//          // enumerator.term().toBytesString());
//          docsEnum = termsEnum.docs(acceptDocs, docsEnum, DocsEnum.FLAG_NONE);
// 
