package perf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.QueryTimeoutImpl;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

/**
 * This helper tries to pollute a bit the types that are typically seen by queries at call sites to help better simulate production systems that may
 * <ul>
 *   <li>have a mix of Directory impls, e.g. because of NRTCachingDirectory</li>
 *   <li>have a mix of segments with deletions and no deletions,</li>
 *   <li>use multiple similarities.</li>
 * </ul>
 * <p>This matters because polymorphic call sites are much more expensive than bimorphic call sites, and bimorphic call sites may be noticeably more expensive than monomorphic call sites.
 */
public class TypePolluter {

  public static void pollute() throws IOException {
    // Use ByteBuffersDirectory instead of MMapDirectory to have multiple IndexInput sub-classes used by queries
    try (Directory dir = new ByteBuffersDirectory()) {

      // TODO: configure a non-default codec?
      IndexWriterConfig config = new IndexWriterConfig(null);;

      try (IndexWriter w = new IndexWriter(dir, config)) {
        // Add enough documents for the inverted index to have full blocks (128 postings)
        int docCount = 1024;
        for (int i = 0; i < docCount; ++i) {
          Document doc = new Document();
          doc.add(new StringField("id", Integer.toString(i), Store.NO));
          if (i % 3 != 0) {
            doc.add(new StringField("body", "a", Store.NO));
            doc.add(new NumericDocValuesField("int1", i % 3));
          }
          if (i % 7 != 0) {
            doc.add(new StringField("body", "b", Store.NO));
            doc.add(new NumericDocValuesField("int2", i % 7));
          }
          if (i % 11 != 0) {
            doc.add(new StringField("body", "c", Store.NO));
            doc.add(new NumericDocValuesField("int3", i % 11));
          }
          if (i % 13 != 0) {
            doc.add(new KnnFloatVectorField("vector", new float[] { i % 7 }));
          }
          w.addDocument(doc);
        }
        w.forceMerge(1);

        try (DirectoryReader reader = DirectoryReader.open(w)) {
          // Run queries with no deletions
          runQueries(reader);
        }
        // Add deleted docs to make sure that branches that exercise deleted docs are used even
        // though the benchmark may be running with no deleted docs
        for (int i = 0; i < docCount; i += 23) {
          w.deleteDocuments(new Term("id", Integer.toString(i)));
        }
        try (DirectoryReader reader = DirectoryReader.open(w)) {
          // Now run queries with deletions
          runQueries(reader);
          // ExitableDirectoryReader adds lots of wrappers everywhere
          runQueries(new ExitableDirectoryReader(reader, new QueryTimeoutImpl(Long.MAX_VALUE)));
        }
      }
    }
  }

  private static void runQueries(DirectoryReader reader) throws IOException {
    IndexSearcher searcher = new IndexSearcher(reader);
    // Exercise multiple similarities
    IndexSearcher booleanSearcher = new IndexSearcher(reader);
    booleanSearcher.setSimilarity(new BooleanSimilarity());
    IndexSearcher classicSearcher = new IndexSearcher(reader);
    classicSearcher.setSimilarity(new ClassicSimilarity());

    Query query1 = new TermQuery(new Term("body", "a"));
    Query query2 = new TermQuery(new Term("body", "b"));
    Query query3 = new FieldExistsQuery("int1");
    Query query4 = new FieldExistsQuery("int2");
    Query query5 = new BooleanQuery.Builder()
        .add(query1, Occur.SHOULD)
        .add(query2, Occur.SHOULD)
        .build();
    Query query6 = new BooleanQuery.Builder()
        .add(query1, Occur.MUST)
        .add(query2, Occur.MUST)
        .build();
    Query query7 = new BooleanQuery.Builder()
        .add(query3, Occur.SHOULD)
        .add(query4, Occur.SHOULD)
        .build();
    Query query8 = new BooleanQuery.Builder()
        .add(query3, Occur.MUST)
        .add(query4, Occur.MUST)
        .build();

    Query[] baseQueries = new Query[] { query1, query2, query3, query4, query5, query6, query7, query8 };

    // dense filter
    Query filter1 = new TermQuery(new Term("body", "c"));
    // sparse filter (especially useful to make sure that the vector search query exercises exact search)
    Query filter2 = new TermQuery(new Term("id", "1"));
    // filter not based on postings
    Query filter3 = new FieldExistsQuery("int3");

    List<Query> queries = new ArrayList<>();

    for (Query query : baseQueries) {
      queries.add(query);
      for (Query filter : new Query[] { filter1, filter2, filter3 }) {
         Query filteredQuery = new BooleanQuery.Builder()
             .add(query, Occur.MUST)
             .add(filter, Occur.FILTER)
             .build();
         queries.add(filteredQuery);
      }
    }

    // Handle vector search separately since filters need to be applied differently
    {
      Query vectorQuery = new KnnFloatVectorQuery("vector", new float[] { 1.5f }, 10);
      queries.add(vectorQuery);
      for (Query filter : new Query[] { filter1, filter2, filter3 }) {
        Query filteredQuery = new KnnFloatVectorQuery("vector", new float[] { 1.5f }, 10, filter);
        queries.add(filteredQuery);
      }
    }

    for (Query query : queries) {
      // Exhaustive evaluation, no scoring
      int count = searcher.count(query);
      // top-k evaluation, by score
      TopDocs hits1 = searcher.search(query, 10);
      TopDocs hits2 = booleanSearcher.search(query, 10);
      TopDocs hits3 = classicSearcher.search(query, 10);
      // top-k evaluation, by field
      TopDocs hits4 = searcher.search(query, 10, new Sort(new SortField("int", SortField.Type.INT)));

      if (count == 0
          || hits1.totalHits.value() == 0
          || hits2.totalHits.value() == 0
          || hits3.totalHits.value() == 0
          || hits4.totalHits.value() == 0) {
        // This helps catch errors if queries are malformed, and also prevents the JVM from skipping
        // the query if we don't use the result
        throw new Error("" + query);
      }
    }
  }
}
