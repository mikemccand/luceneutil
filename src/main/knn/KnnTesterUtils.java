package knn;

import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class KnnTesterUtils {

  /** Fetches values for the "id" field from search results
   */
  public static int[] getResultIds(TopDocs topDocs, StoredFields storedFields) throws IOException {
    int[] resultIds = new int[topDocs.scoreDocs.length];
    int i = 0;
    for (ScoreDoc doc : topDocs.scoreDocs) {
      if (doc.doc != NO_MORE_DOCS) {
        // there is a bug somewhere that can result in doc=NO_MORE_DOCS!  I think it happens
        // in some degenerate case (like input query has NaN in it?) that causes no results to
        // be returned from HNSW search?
        resultIds[i++] = Integer.parseInt(storedFields.document(doc.doc).get(KnnGraphTester.ID_FIELD));
      } else {
        System.out.println("NO_MORE_DOCS!");
      }
    }
    return resultIds;
  }
}
