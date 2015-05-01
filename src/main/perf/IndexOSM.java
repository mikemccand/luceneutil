import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.PackedQuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import com.spatial4j.core.context.SpatialContext;

// javac -cp /l/trunk/lucene/build/queries/lucene-queries-6.0.0-SNAPSHOT.jar:/l/trunk/lucene/spatial/lib/spatial4j-0.4.1.jar:/l/trunk/lucene/build/spatial/lucene-spatial-6.0.0-SNAPSHOT.jar:/l/trunk/lucene/build/core/lucene-core-6.0.0-SNAPSHOT.jar:/l/trunk/lucene/build/analysis/common/lucene-analyzers-common-6.0.0-SNAPSHOT.jar IndexOSM.java

// java -cp .:/l/trunk/lucene/build/queries/lucene-queries-6.0.0-SNAPSHOT.jar:/l/trunk/lucene/spatial/lib/spatial4j-0.4.1.jar:/l/trunk/lucene/build/spatial/lucene-spatial-6.0.0-SNAPSHOT.jar:/l/trunk/lucene/build/core/lucene-core-6.0.0-SNAPSHOT.jar:/l/trunk/lucene/build/analysis/common/lucene-analyzers-common-6.0.0-SNAPSHOT.jar IndexOSM javaindex

public class IndexOSM {

  public static void main(String[] args) throws IOException {
    Directory dir = FSDirectory.open(Paths.get(args[0]));
    IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    IndexWriter w = new IndexWriter(dir, iwc);

    SpatialContext ctx = SpatialContext.GEO;

    int maxLevels = 11;
    //SpatialPrefixTree grid = new GeohashPrefixTree(ctx, maxLevels);
    // SpatialPrefixTree grid = new PackedQuadPrefixTree(ctx, 25);
    SpatialPrefixTree grid = new QuadPrefixTree(ctx, 20);

    RecursivePrefixTreeStrategy strategy = new RecursivePrefixTreeStrategy(grid, "myGeoField");
    //strategy.setPruneLeafyBranches(false);

    CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT);

    int BUFFER_SIZE = 1 << 16;     // 64K
    InputStream is = new FileInputStream(new File("/lucenedata/open-street-maps/latlon.subsetPlusAllLondon.txt"));
    BufferedReader reader = new BufferedReader(new InputStreamReader(is, decoder), BUFFER_SIZE);
    int count = 0;
    long t0 = System.currentTimeMillis();
    while (true) {
      String line = reader.readLine();
      if (line == null) {
        break;
      }
      String[] parts = line.split(",");
      long id = Long.parseLong(parts[0]);
      double lat = Double.parseDouble(parts[1]);
      double lng = Double.parseDouble(parts[2]);
      Document doc = new Document();
      doc.add(new StoredField("id", id));
      doc.add(new NumericDocValuesField("id", id));
      for(Field f : strategy.createIndexableFields(ctx.makePoint(lng, lat))) {
        doc.add(f);
      }
      w.addDocument(doc);
      count++;
      if (count % 1000000 == 0) {
        System.out.println(count + "...");
      }
    }
    long t1 = System.currentTimeMillis();
    System.out.println(((t1-t0)/1000.) + " sec to index");

    System.out.println("Force merge...");
    w.forceMerge(1);
    long t2 = System.currentTimeMillis();
    System.out.println(((t2-t1)/1000.) + " sec to forceMerge");
    w.close();
    dir.close();
  }
}
