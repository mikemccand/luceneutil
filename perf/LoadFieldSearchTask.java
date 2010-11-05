package perf;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.SetBasedFieldSelector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

public class LoadFieldSearchTask extends SearchTask {

  private FieldSelector fieldSelector;

  public LoadFieldSearchTask(Random r, IndexSearcher s,
      List<QueryAndSort> queriesList, int numIter, boolean shuffle) throws IOException {
    super(r, s, queriesList, numIter, shuffle);
    Set<String> loadfields = new HashSet<String>();
    loadfields.add("docdate");
    this.fieldSelector = new SetBasedFieldSelector(loadfields,
        new HashSet<String>());
  }

  @Override
  protected void processHits(TopDocs hits) throws IOException {
    ScoreDoc[] scoreDocs = hits.scoreDocs;
    for (int i = 0; i < scoreDocs.length; i++) {
      this.s.doc(i, fieldSelector); // load all fields
    }
  }
}
