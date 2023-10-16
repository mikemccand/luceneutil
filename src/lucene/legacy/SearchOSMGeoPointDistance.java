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
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.GeoPointInBBoxQuery;
import org.apache.lucene.search.GeoPointDistanceQuery;
import org.apache.lucene.search.GeoPointInPolygonQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import com.spatial4j.core.context.SpatialContext;

// javac -cp /l/geopointradius/lucene/build/sandbox/lucene-sandbox-6.0.0-SNAPSHOT.jar:/l/geopointradius/lucene/build/queries/lucene-queries-6.0.0-SNAPSHOT.jar:/l/geopointradius/lucene/spatial/lib/spatial4j-0.4.1.jar:/l/geopointradius/lucene/build/spatial/lucene-spatial-6.0.0-SNAPSHOT.jar:/l/geopointradius/lucene/build/core/lucene-core-6.0.0-SNAPSHOT.jar:/l/geopointradius/lucene/build/analysis/common/lucene-analyzers-common-6.0.0-SNAPSHOT.jar SearchOSMGeoPointDistance.java

// java -cp .:/l/geopointradius/lucene/build/sandbox/lucene-sandbox-6.0.0-SNAPSHOT.jar:/l/geopointradius/lucene/build/queries/lucene-queries-6.0.0-SNAPSHOT.jar:/l/geopointradius/lucene/spatial/lib/spatial4j-0.4.1.jar:/l/geopointradius/lucene/build/spatial/lucene-spatial-6.0.0-SNAPSHOT.jar:/l/geopointradius/lucene/build/core/lucene-core-6.0.0-SNAPSHOT.jar:/l/geopointradius/lucene/build/analysis/common/lucene-analyzers-common-6.0.0-SNAPSHOT.jar SearchOSMGeoPointDistance geopointindex

public class SearchOSMGeoPointDistance {

  public static void main(String[] args) throws IOException {
    Directory dir = FSDirectory.open(Paths.get(args[0]));
    IndexReader r = DirectoryReader.open(dir);
    IndexSearcher indexSearcher = new IndexSearcher(r);

    /*
    for(float lon=-180; lon<175; lon += 10) {
      for(float lat=-90; lat<85; lat += 10) {
        for(float lonEnd=lon+10; lonEnd<=180; lonEnd += 10) {
          for(float latEnd=lat+10; latEnd<=90; latEnd += 10) {
    */

    // London, UK:
    int STEPS = 5;
    double MIN_LAT = 51.0919106;
    double MAX_LAT = 51.6542719;
    double MIN_LON = -0.3867282;
    double MAX_LON = 0.8492337;
    for(int iter=0;iter<100;iter++) {
      long tStart = System.nanoTime();
      long totHits = 0;
      int queryCount = 0;
      for(int latStep=0;latStep<STEPS;latStep++) {
        double lat = MIN_LAT + latStep * (MAX_LAT - MIN_LAT) / STEPS;
        for(int lonStep=0;lonStep<STEPS;lonStep++) {
          double lon = MIN_LON + lonStep * (MAX_LON - MIN_LON) / STEPS;

          for(int radiusKM=1;radiusKM<20;radiusKM+=1) {
            Query query = new GeoPointDistanceQuery("geo", lon, lat, (double) radiusKM);
            TotalHitCountCollector c = new TotalHitCountCollector();
            //long t0 = System.nanoTime();
            indexSearcher.search(query, c);
            //long t1 = System.nanoTime();
            //System.out.println("  " + c.getTotalHits() + " total hits");
            //System.out.println("  " + ((t1-t0)/1000000.0) + " msec");
            totHits += c.getTotalHits();
            queryCount++;
          }
        }
      }
      long tEnd = System.nanoTime();
      System.out.println("ITER: " + iter + " " + ((tEnd-tStart)/1000000000.0) + " sec; totHits=" + totHits + "; " + queryCount + " queries");
    }
    r.close();
    dir.close();
  }
}


