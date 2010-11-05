package perf.values;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.values.Reader.Source;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import perf.QueryAndSort;
import perf.SearchTask;

public class DocValuesSearchTask extends SearchTask {

  private Source bytes;

  public DocValuesSearchTask(Random r, IndexSearcher s,
      List<QueryAndSort> queriesList, int numIter, boolean shuffle) throws IOException {
    super(r, s, queriesList, numIter, shuffle);
    IndexReader indexReader = s.getIndexReader();
    long t0 = System.nanoTime();
    bytes = indexReader.getIndexValuesCache().getBytes("docdate");
    final long delay = System.nanoTime() - t0;
    System.err.println("RAM usage: "  +bytes.ramBytesUsed() + " bytes");
    System.err.println("load took: "  + TimeUnit.MILLISECONDS.convert(delay, TimeUnit.NANOSECONDS) + " Milliseconds");    
  }

  @Override
  protected void processHits(TopDocs hits) {
    ScoreDoc[] scoreDocs = hits.scoreDocs;
    for (int i = 0; i < scoreDocs.length; i++) {
      bytes.bytes(i).utf8ToString();
    }
  }
  
  
  

}
