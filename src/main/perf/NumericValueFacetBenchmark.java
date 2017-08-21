import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.LongValueFacetCounts;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;


// javac -cp lucene/build/core/classes/java:lucene/build/facet/classes/java /l/util/src/main/perf/NumericValueFacetBenchmark.java; java -cp .:lucene/build/core/classes/java:lucene/build/facet/classes/java:/l/util/src/main/perf NumericValueFacetBenchmark

public class NumericValueFacetBenchmark {
  public static void main(String[] args) throws IOException {
    Path indexPath = Paths.get("/l/tmp/facetbench");
    Directory dir = FSDirectory.open(indexPath);
    if (DirectoryReader.indexExists(dir) == false) {
      IndexWriter w = new IndexWriter(dir, new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE));
      for(int i=0;i<50000000;i++) {
        Document doc = new Document();
        doc.add(new NumericDocValuesField("field", i % 10));
        doc.add(new IntPoint("field", i % 10));
        doc.add(new IntPoint("i", i));
        w.addDocument(doc);
      }
      w.close();
    }
    
    IndexReader r = DirectoryReader.open(dir);
    IndexSearcher s = new IndexSearcher(r);

    long bestNS = Long.MAX_VALUE;

    for(int iter=0;iter<100;iter++) {
      long t0 = System.nanoTime();
      int hitCount;
      if (true) {
        //LongValueFacetCounts counts = new LongValueFacetCounts("field", r, false);
        LongValueFacetCounts counts = new LongValueFacetCounts("field", LongValuesSource.fromLongField("field"), r);
        FacetResult facets = counts.getAllChildrenSortByValue();        
        hitCount = ((Number) facets.value).intValue();
      } else {
        FacetsCollector fc = new FacetsCollector();
        //s.search(new MatchAllDocsQuery(), fc);
        s.search(IntPoint.newRangeQuery("i", 0, 24999999), fc);
        //LongValueFacetCounts counts = new LongValueFacetCounts("field", LongValuesSource.fromLongField("field"), fc);
        LongValueFacetCounts counts = new LongValueFacetCounts("field", fc, false);
        FacetResult facets = counts.getAllChildrenSortByValue();
        hitCount = ((Number) facets.value).intValue();
      }
      long t1 = System.nanoTime();
      long elapsedNS = t1 - t0;
      System.out.println(String.format(Locale.ROOT, "%d: %d hits, %.1f msec", iter, hitCount, (elapsedNS / 1000000.0)));
      if (elapsedNS < bestNS) {
        System.out.println("  ***");
        bestNS = elapsedNS;
      }
    }
  }
}
