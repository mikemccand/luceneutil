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

// TODO
//   - mixin some sorting, eg sort by date

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

//import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.*;;
import org.apache.lucene.index.IndexCommit;
//import org.apache.lucene.index.codecs.CodecProvider;
//import org.apache.lucene.index.codecs.mocksep.MockSepCodec;
import org.apache.lucene.index.IndexReader;
//import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.*;
import org.apache.lucene.store.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.ReaderUtil;

// commits: single, multi, delsingle, delmulti

// trunk:
//   javac -cp build/classes/java:../modules/analysis/build/common/classes/java  SearchPerfTest.java; java -cp .:build/classes/java:../modules/analysis/build/common/classes/java SearchPerfTest Default /x/lucene/trunkwiki/index 2 10000 >& out.x

// 3x:
//   javac -cp build/classes/java  SearchPerfTest.java; java -cp .:build/classes/java SearchPerfTest Default /x/lucene/3xwiki/index 2 10000 >& out.x

public class SearchPerfTest {
  
  private static String[] queryStrings = {
    //"*:*",
    "states",
    "unit*",
    "uni*",
    "u*d",
    "un*d",
    "united~0.75",
    "united~0.6",
    "unit~0.7",
    "unit~0.5",
    "united OR states",
    "united AND states",
    "nebraska AND states",
    "\"united states\"",
    "\"united states\"~3",
  };

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

  private static void printOne(IndexSearcher s, QueryAndSort qs) throws IOException {
    final TopDocs hits;
    s.setDefaultFieldSortScoring(true, false);
    System.out.println("\nRUN: " + qs.q + " s=" + qs.s);
    if (qs.s == null) {
      // disambiguate by our stable id:
      Sort sort = new Sort(new SortField[] {
          new SortField(null, SortField.SCORE),
          new SortField("docid", SortField.INT)});
      if (qs.f == null) {
        hits = s.search(qs.q, null, 50, sort);
      } else {
        hits = s.search(qs.q, qs.f, 50, sort);
      }
    } else {
      // disambiguate by our stable id:
      SortField[] sortFields = new SortField[qs.s.getSort().length+1];
      System.arraycopy(qs.s.getSort(), 0, sortFields, 0, qs.s.getSort().length);
      sortFields[sortFields.length-1] = new SortField("docid", SortField.INT);
      hits = s.search(qs.q, qs.f, 50, new Sort(sortFields));
    }
    System.out.println("\nHITS q=" + qs.q + " s=" + qs.s + " tot=" + hits.totalHits);
    //System.out.println("  rewrite q=" + s.rewrite(qs.q));
    for(int i=0;i<hits.scoreDocs.length;i++) {
      System.out.println("  " + i + " doc=" + s.doc(hits.scoreDocs[i].doc).get("docid") + " score=" + hits.scoreDocs[i].score);
    }
    if (qs.q instanceof MultiTermQuery) {
      System.out.println("  " + ((MultiTermQuery) qs.q).getTotalNumberOfTerms() + " expanded terms");
    }
    s.setDefaultFieldSortScoring(false, false);
  }

  private static void addQuery(IndexSearcher s, List<QueryAndSort> queries, Query q, Sort sort, Filter f) throws IOException {
    QueryAndSort qs = new QueryAndSort(q, sort, f);

    /*
    if (q instanceof WildcardQuery || q instanceof PrefixQuery) {
      //((MultiTermQuery) q).setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
      ((MultiTermQuery) q).setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
      BooleanQuery.setMaxClauseCount(100000);
    }
    */
    queries.add(qs);
    printOne(s, qs);
  }
  
  private static final boolean shuffleQueries = true;

  public static void main(String[] args) throws Exception {

    // args: dirImpl indexPath numThread numIterPerThread
    //CodecProvider.getDefault().register(new MockSepCodec());
    // eg java SearchPerfTest /path/to/index 4 100
    final Directory dir;
    final String dirImpl = args[0];
    final String dirPath = args[1];
    final String analyzer = args[2];
    if (dirImpl.equals("MMapDirectory")) {
      dir = new MMapDirectory(new File(dirPath));
    } else if (dirImpl.equals("NIOFSDirectory")) {
      dir = new NIOFSDirectory(new File(dirPath));
    } else if (dirImpl.equals("SimpleFSDirectory")) {
      dir = new SimpleFSDirectory(new File(dirPath));
    } else {
      throw new RuntimeException("unknown directory impl \"" + dirImpl + "\"");
    }

    String taskType = System.getProperty("task.type", SearchTask.class.getName());
    System.out.println("Using dir impl " + dir.getClass().getName());
    System.out.println("Analyzer " + analyzer);
    System.out.println("Using TaskType: " + taskType);
    System.out.println("JVM " + (Constants.JRE_IS_64BIT ? "is" : "is not") + " 64bit");

    final long t0 = System.currentTimeMillis();
    final IndexSearcher s;
    Filter f = null;
    boolean doOldFilter = false;
    boolean doNewFilter = false;
    if (args.length == 8) {
      final String commit = args[5];
      System.out.println("open commit=" + commit);
      IndexReader reader = IndexReader.open(findCommitPoint(commit, dir), true);
      Filter filt = new RandomFilter(Double.parseDouble(args[7])/100.0);
      if (args[6].equals("FilterOld")) {
        f = new CachingWrapperFilter(filt);
        /*
        AtomicReaderContext[] leaves = ReaderUtil.leaves(reader.getTopReaderContext());
        for(int subID=0;subID<leaves.length;subID++) {
          f.getDocIdSet(leaves[subID]);
        }
        */
      } else {
        throw new RuntimeException("4th arg should be FilterOld or FilterNew");
      }
      s = new IndexSearcher(reader);
    } else if (args.length == 6) {
      final String commit = args[5];
      System.out.println("open commit=" + commit);
      s = new IndexSearcher(IndexReader.open(findCommitPoint(commit, dir), true));
    } else {
      // open last commit
      s = new IndexSearcher(dir);
    }

    System.out.println("reader=" + s.getIndexReader());

    //s.search(new TermQuery(new Term("body", "bar")), null, 10, new Sort(new SortField("unique1000000", SortField.STRING)));
    //final long t1 = System.currentTimeMillis();
    //System.out.println("warm time = " + (t1-t0)/1000.0);

    //System.gc();
    //System.out.println("RAM: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));

    final int threadCount = Integer.parseInt(args[3]);
    final int numIterPerThread = Integer.parseInt(args[4]);

    final List<QueryAndSort> queries = new ArrayList<QueryAndSort>();
    final Analyzer a;
    if (analyzer.equals("StandardAnalyzer")) {
      a = new StandardAnalyzer(Version.LUCENE_30);
    } else {
      throw new RuntimeException("unknown analyzer " + analyzer);
    }
    QueryParser p = new QueryParser(Version.LUCENE_30, "body", a);
    p.setLowercaseExpandedTerms(false);

    final Sort dateTimeSort = new Sort(new SortField("docdatenum", SortField.LONG));
    for(int i=0;i<queryStrings.length;i++) {
      Query q = p.parse(queryStrings[i]);

      // sort by score:
      addQuery(s, queries, q, null, f);

      // sort by date:
      //addQuery(s, queries, (Query) q.clone(), dateTimeSort, f);

      /*
      for(int j=0;j<7;j++) {
        String sortField;
        switch(j) {
        case 0:
          sortField = "country";
          break;
        case 1:
          sortField = "unique10";
          break;
        case 2:
          sortField = "unique100";
          break;
        case 3:
          sortField = "unique1000";
          break;
        case 4:
          sortField = "unique10000";
          break;
        case 5:
          sortField = "unique100000";
          break;
        case 6:
          sortField = "unique1000000";
          break;
        // not necessary, but compiler disagrees:
        default:
          sortField = null;
          break;
        }
        qs = new QueryAndSort(q, new Sort(new
                                          SortField(sortField,
                                          SortField.STRING)),
                                          f);
        printOne(s, qs);
        queries.add(qs);
      }
      */
    }

    {
      //addQuery(s, queries, new FuzzyQuery(new Term("body", "united"), 0.6f, 0, 50), null, f);
      //addQuery(s, queries, new FuzzyQuery(new Term("body", "united"), 0.7f, 0, 50), null, f);
    }

    Query q = new SpanFirstQuery(new SpanTermQuery(new Term("body", "unit")), 5);
    addQuery(s, queries, q, null, f);
    //addQuery(s, queries, q, dateTimeSort, f);

    q = new SpanNearQuery(
                          new SpanQuery[] {new SpanTermQuery(new Term("body", "unit")),
                                           new SpanTermQuery(new Term("body", "state"))},
                          10,
                          true);
    addQuery(s, queries, q, null, f);
    //addQuery(s, queries, q, dateTimeSort, f);

    // Seconds in the day 0..86400
    q = NumericRangeQuery.newIntRange("doctimesecnum", 10000, 60000, true, true);
    addQuery(s, queries, q, null, f);
    //addQuery(s, queries, q, dateTimeSort, f);

    final Random rand = new Random(17);

    final SearchTask[] threads = new SearchTask[threadCount];
    for(int i=0;i<threadCount-1;i++) {
      threads[i] = task(taskType, rand, s, queries, numIterPerThread, shuffleQueries);
      threads[i].start();
    }

    // I run one thread:
    threads[threadCount-1] = task(taskType, rand, s, queries, numIterPerThread, shuffleQueries);
    threads[threadCount-1].run();

    for(int i=0;i<threadCount-1;i++) {
      threads[i].join();
    }

    System.out.println("ns by query/coll:");
    for(QueryAndSort qs : queries) {
      int totHits = -1;
      for(int t=0;t<threadCount&&totHits==-1;t++) {
        for(Result r : threads[t].results) {
          if (r.qs == qs) {
            totHits = r.totHits;
            break;
          }
        }
      }

      System.out.println("  q=" + qs.q + " s=" + qs.s + " h=" + totHits);

      for(int t=0;t<threadCount;t++) {
        System.out.println("    t=" + t);
        long best = 0;
        for(Result r : threads[t].results) {
          if (r.qs == qs && (best == 0 || r.t < best)) {
            best = r.t;
          }
        }
        for(Result r : threads[t].results) {
          if (r.qs == qs) {
            if (best == r.t) {
              System.out.println("      " + r.t + " c=" + r.check + " **");
            } else {
              System.out.println("      " + r.t + " c=" + r.check);
            }
            if (r.totHits != totHits) {
              throw new RuntimeException("failed");
            }
          }
        }
      }
    }
  }
  private static final SearchTask task(String task,Random r, IndexSearcher s,
      List<QueryAndSort> queriesList, int numIter, boolean shuffle) {
    if(SearchTask.class.getName().equals(task)) {
      return new SearchTask(r, s, queriesList, numIter, shuffle);
    } else {
      try {
        Constructor<?> constructor = Class.forName(task).getConstructor(Random.class, IndexSearcher.class, List.class, int.class, boolean.class);
        return (SearchTask) constructor.newInstance(r, s, queriesList, numIter, shuffle);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
      
  }
}



/*
  %  gain
  0.0  -94.4
  0.1  -69.5
  0.25 -51.7
  0.5  -30.1
  0.75  -17.9
  1  -6.0
  1.25 4.9
  1.5 14.0
  2  30.8


0.0 0.1 0.25 0.5 0.75 1.0 1.25 1.5 2.0

-94.4 -69.5 -51.7 -30.1 -17.9 -6.0 4.9 14.0 30.8

 */
