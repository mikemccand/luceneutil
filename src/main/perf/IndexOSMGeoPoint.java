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
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.document.GeoPointField;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.PrintStreamInfoStream;

// javac -cp build/sandbox/lucene-sandbox-6.0.0-SNAPSHOT.jar:build/queries/lucene-queries-6.0.0-SNAPSHOT.jar:spatial/lib/spatial4j-0.4.1.jar:build/spatial/lucene-spatial-6.0.0-SNAPSHOT.jar:build/core/lucene-core-6.0.0-SNAPSHOT.jar:build/analysis/common/lucene-analyzers-common-6.0.0-SNAPSHOT.jar /l/util/src/main/perf/IndexOSMGeoPoint.java

// rm -rf geopointindex; java -cp /l/util/src/main/perf:build/sandbox/lucene-sandbox-6.0.0-SNAPSHOT.jar:build/queries/lucene-queries-6.0.0-SNAPSHOT.jar:spatial/lib/spatial4j-0.4.1.jar:build/spatial/lucene-spatial-6.0.0-SNAPSHOT.jar:build/core/lucene-core-6.0.0-SNAPSHOT.jar:build/analysis/common/lucene-analyzers-common-6.0.0-SNAPSHOT.jar IndexOSMGeoPoint geopointindex

public class IndexOSMGeoPoint {

  public static void main(String[] args) throws IOException {
    Directory dir = FSDirectory.open(Paths.get(args[0]));
    IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    iwc.setMaxBufferedDocs(109630);
    iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    iwc.setMergePolicy(new LogDocMergePolicy());
    iwc.setMergeScheduler(new SerialMergeScheduler());
    iwc.setInfoStream(new PrintStreamInfoStream(System.out));
    IndexWriter w = new IndexWriter(dir, iwc);

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
      //doc.add(new StoredField("id", id));
      //doc.add(new NumericDocValuesField("id", id));
      doc.add(new GeoPointField("geo", lng, lat, Field.Store.NO));
      w.addDocument(doc);
      count++;
      if (count % 1000000 == 0) {
        System.out.println(count + "...");
      }
    }
    long t1 = System.currentTimeMillis();
    System.out.println(((t1-t0)/1000.) + " sec to index");

    //System.out.println("Force merge...");
    //w.forceMerge(1);
    long t2 = System.currentTimeMillis();
    //System.out.println(((t2-t1)/1000.) + " sec to forceMerge");
    w.close();
    long t3 = System.currentTimeMillis();
    System.out.println(((t3-t2)/1000.) + " sec to close");
    dir.close();
  }
}
