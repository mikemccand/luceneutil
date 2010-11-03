import org.apache.lucene.util.*;
import org.apache.lucene.store.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.codecs.mocksep.MockSepCodec;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.search.*;
//import org.apache.lucene.analysis.core.*;
import org.apache.lucene.analysis.en.*;
import org.apache.lucene.queryParser.*;
import java.util.*;
import java.io.*;

// commits: single, multi, delsingle, delmulti

// trunk:
//   javac -cp build/classes/java:../modules/analysis/build/common/classes/java  SearchPerfTest.java; java -cp .:build/classes/java:../modules/analysis/build/common/classes/java SearchPerfTest /x/lucene/trunkwiki/index 2 10000 >& out.x

// 3x:
//   javac -cp build/classes/java  SearchPerfTest.java; java -cp .:build/classes/java SearchPerfTest /x/lucene/3xwiki/index 2 10000 >& out.x

public class SearchPerfTest {

  private static class QueryAndSort {
    final Query q;
    final Sort s;
    final Filter f;

    public QueryAndSort(Query q, Sort s, Filter f) {
      this.q = q;
      this.s = s;
      this.f = f;
    }
  }

  private static class Result {
    QueryAndSort qs;
    int colType;
    long t;
    int totHits;
    long check;
  }

  private static class SearchThread extends Thread {

    private final IndexSearcher s;
    private final QueryAndSort[] queries;
    private final int numIter;
    public final List<Result> results = new ArrayList<Result>();

    public SearchThread(Random r, IndexSearcher s, List<QueryAndSort> queriesList, int numIter) {
      this.s = s;
      List<QueryAndSort> queries = new ArrayList<QueryAndSort>(queriesList);
      //Collections.shuffle(queries, r);
      this.queries = queries.toArray(new QueryAndSort[queries.size()]);
      this.numIter = numIter;
    }

    public void run() {
      try {

        final IndexSearcher s = this.s;
        final QueryAndSort[] queries = this.queries;

        int queryUpto = 0;
        long totSum = 0;

        for(int iter=0;iter<numIter;iter++) {
          for(int q=0;q<queries.length;q++) {
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
            final long delay = System.nanoTime()-t0;
            totSum += hits.totalHits;
            long check = 0;
            for(int i=0;i<hits.scoreDocs.length;i++) {
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
  }

  private static String[] queryStrings = {
    //"*:*",
    "states",
    "unit*",
    "uni*",
    "u*d",
    "un*d",
    "united~1",
    "united~2",
    "unit~1",
    "unit~2",
    "united OR states",
    "united AND states",
    "\"united states\"",
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
    System.out.println("\nRUN: " + qs.q);
    if (qs.s == null && qs.f == null) {
      hits = s.search(qs.q, 10);
    } else if (qs.s == null && qs.f != null) {
      hits = s.search(qs.q, qs.f, 10);
    } else {
      hits = s.search(qs.q, qs.f, 10, qs.s);
    }

    System.out.println("\nHITS q=" + qs.q + " s=" + qs.s + " tot=" + hits.totalHits);
    //System.out.println("  rewrite q=" + s.rewrite(qs.q));
    for(int i=0;i<hits.scoreDocs.length;i++) {
      System.out.println("  " + i + " doc=" + hits.scoreDocs[i].doc + " score=" + hits.scoreDocs[i].score);
    }
    if (qs.q instanceof MultiTermQuery) {
      System.out.println("  " + ((MultiTermQuery) qs.q).getTotalNumberOfTerms() + " expanded terms");
    }
  }

  private static class RandomFilter extends Filter {
    final double pctKeep;

    public RandomFilter(double pctKeep) {
      this.pctKeep = pctKeep;
    }

    @Override
    public DocIdSet getDocIdSet(IndexReader r) {
      final Random rand = new Random(42);
      final int maxDoc = r.maxDoc();
      OpenBitSet bits = new OpenBitSet(maxDoc);
      for(int docID = 0;docID<maxDoc;docID++) {
        if (rand.nextDouble() <= pctKeep) {        
          bits.fastSet(docID);
        }
      }

      System.out.println("rfilt " + bits.cardinality());
      return bits;
    }
  }

  private static void addQuery(IndexSearcher s, List<QueryAndSort> queries, Query q, Sort sort, Filter f) throws IOException {
    QueryAndSort qs = new QueryAndSort(q, sort, f);
    /*
    if (q instanceof WildcardQuery || q instanceof PrefixQuery) {
      //((MultiTermQuery) q).setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
      ((MultiTermQuery) q).setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
    }
    */
    queries.add(qs);
    printOne(s, qs);
  }

  public static void main(String[] args) throws Exception {

    // args: indexPath numThread numIterPerThread

    // eg java SearchPerfTest /path/to/index 4 100

    CodecProvider.getDefault().register(new MockSepCodec());

    //final Directory dir = new MMapDirectory(new File(args[0]));
    final Directory dir = FSDirectory.open(new File(args[0]));
    //final Directory dir = new SimpleFSDirectory(new File(args[0]));
    final long t0 = System.currentTimeMillis();
    final IndexSearcher s;
    Filter f = null;
    boolean doOldFilter = false;
    boolean doNewFilter = false;
    if (args.length == 6) {
      final String commit = args[3];
      System.out.println("open commit=" + commit);
      IndexReader reader = IndexReader.open(findCommitPoint(commit, dir), true);
      Filter filt = new RandomFilter(Double.parseDouble(args[5])/100.0);
      if (args[4].equals("FilterOld")) {
        f = new CachingWrapperFilter(filt);
        IndexReader[] subReaders = reader.getSequentialSubReaders();
        for(int subID=0;subID<subReaders.length;subID++) {
          f.getDocIdSet(subReaders[subID]);
        }
      } else if (args[4].equals("FilterNew")) {
        reader = CachedFilterIndexReader.create(reader, filt);
      } else {
        throw new RuntimeException("4th arg should be FilterOld or FilterNew");
      }
      s = new IndexSearcher(reader);
    } else if (args.length == 4) {
      final String commit = args[3];
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

    final int threadCount = Integer.parseInt(args[1]);
    final int numIterPerThread = Integer.parseInt(args[2]);

    final List<QueryAndSort> queries = new ArrayList<QueryAndSort>();
    QueryParser p = new QueryParser(Version.LUCENE_31, "body", new EnglishAnalyzer(Version.LUCENE_31));

    for(int i=0;i<queryStrings.length;i++) {
      Query q = p.parse(queryStrings[i]);

      // sort by score:
      addQuery(s, queries, q, null, f);

      /*
      addQuery(s, queries, (Query) q.clone(),
               new Sort(new
                        SortField("doctitle",
                                  SortField.STRING)), f);
      */

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

    final Random rand = new Random(17);

    final SearchThread[] threads = new SearchThread[threadCount];
    for(int i=0;i<threadCount-1;i++) {
      threads[i] = new SearchThread(rand, s, queries, numIterPerThread);
      threads[i].start();
    }

    // I run one thread:
    threads[threadCount-1] = new SearchThread(rand, s, queries, numIterPerThread);
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