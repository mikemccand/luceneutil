package perf;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldCache.DocTerms;
import org.apache.lucene.search.FieldCache.IntParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.cache.DocTermsIndexCreator.DocTermsIndexImpl;
import org.apache.lucene.util.BytesRef;

public class LoadFieldCacheSearchTask extends SearchTask {

  private DocTerms terms;

  public LoadFieldCacheSearchTask(Random r, IndexSearcher s,
      List<QueryAndSort> queriesList, int numIter, boolean shuffle) throws IOException {
    super(r, s, queriesList, numIter, shuffle);
    long t0 = System.nanoTime();
    terms = FieldCache.DEFAULT.getTerms(s.getIndexReader(), "docdate");
    final long delay = System.nanoTime() - t0;
    System.err.println("load took: "  + TimeUnit.MILLISECONDS.convert(delay, TimeUnit.NANOSECONDS) + " Milliseconds");
  }

  @Override
  protected void processHits(TopDocs hits) throws IOException {
    ScoreDoc[] scoreDocs = hits.scoreDocs;
    BytesRef ref = new BytesRef();
    for (int i = 0; i < scoreDocs.length; i++) {
      terms.getTerm(i, ref).utf8ToString();
    }
  }
}
