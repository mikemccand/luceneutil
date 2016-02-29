package perf;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.geopoint.search.GeoPointDistanceQuery;
import org.apache.lucene.spatial.geopoint.search.GeoPointInBBoxQuery;
import org.apache.lucene.spatial.geopoint.search.GeoPointInPolygonQuery;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.util.GeoDistanceUtils;
import org.apache.lucene.spatial.util.GeoEncodingUtils;
import org.apache.lucene.spatial.util.GeoRelationUtils;
import org.apache.lucene.spatial.util.GeoUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;

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

// javac -cp build/sandbox/lucene-sandbox-6.0.0-SNAPSHOT.jar:build/queries/lucene-queries-6.0.0-SNAPSHOT.jar:spatial/lib/spatial4j-0.4.1.jar:build/spatial/lucene-spatial-6.0.0-SNAPSHOT.jar:build/core/lucene-core-6.0.0-SNAPSHOT.jar:build/analysis/common/lucene-analyzers-common-6.0.0-SNAPSHOT.jar /l/util/src/main/perf/SearchOSMGeoPoint.java

// java -cp /l/util/src/main:build/sandbox/lucene-sandbox-6.0.0-SNAPSHOT.jar:build/queries/lucene-queries-6.0.0-SNAPSHOT.jar:spatial/lib/spatial4j-0.4.1.jar:build/spatial/lucene-spatial-6.0.0-SNAPSHOT.jar:build/core/lucene-core-6.0.0-SNAPSHOT.jar:build/analysis/common/lucene-analyzers-common-6.0.0-SNAPSHOT.jar perf.SearchOSMGeoPoint geopointindex

public class SearchOSMGeoPoint {

  public static void main(String[] args) throws IOException {
    Directory dir = FSDirectory.open(Paths.get(args[0]));
    IndexReader r = DirectoryReader.open(dir);
    long bytes = 0;
    for(LeafReaderContext ctx : r.leaves()) {
      CodecReader cr = (CodecReader) ctx.reader();
      for(Accountable acc : cr.getChildResources()) {
        System.out.println("  " + Accountables.toString(acc));
      }
      bytes += cr.ramBytesUsed();
    }
    System.out.println("READER MB: " + (bytes/1024./1024.));
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
      //long totStupidHits = 0;
      for(int latStep=0;latStep<STEPS;latStep++) {
        double lat = MIN_LAT + latStep * (MAX_LAT - MIN_LAT) / STEPS;
        for(int lonStep=0;lonStep<STEPS;lonStep++) {
          double lon = MIN_LON + lonStep * (MAX_LON - MIN_LON) / STEPS;
          for(int latStepEnd=latStep+1;latStepEnd<=STEPS;latStepEnd++) {
            double latEnd = MIN_LAT + latStepEnd * (MAX_LAT - MIN_LAT) / STEPS;
            for(int lonStepEnd=lonStep+1;lonStepEnd<=STEPS;lonStepEnd++) {
              double lonEnd = MIN_LON + lonStepEnd * (MAX_LON - MIN_LON) / STEPS;

              //System.out.println("LON: " + lon + " to " + lonEnd + " LAT: " + lat + " to " + latEnd);
              double[] lats = new double[5];
              double[] lons = new double[5];
              lats[0] = lat;
              lons[0] = lon;
              lats[1] = latEnd;
              lons[1] = lon;
              lats[2] = latEnd;
              lons[2] = lonEnd;
              lats[3] = lat;
              lons[3] = lonEnd;
              lats[4] = lat;
              lons[4] = lon;
              Query query;
              if (true) {
                //query = new GeoPointInBBoxQuery("geo", lon, lat, lonEnd, latEnd);
                double distance = GeoDistanceUtils.haversin(lat, lon, latEnd, lonEnd)/2.0;
                query = new GeoPointDistanceQuery("geo", (lon+lonEnd)/2.0, (lat+latEnd)/2.0, distance);
              } else {
                query = new GeoPointInPolygonQuery("geo", lons, lats);
              }
              TotalHitCountCollector c = new TotalHitCountCollector();
              //long t0 = System.nanoTime();
              indexSearcher.search(query, c);
              //long t1 = System.nanoTime();
              //System.out.println("  " + c.getTotalHits() + " total hits");
              //System.out.println("  " + ((t1-t0)/1000000.0) + " msec");
              totHits += c.getTotalHits();
              queryCount++;

              /*
              if (iter == 0) {
                totStupidHits += stupidBBoxQuery(indexSearcher, lon, lat, lonEnd, latEnd);
              }
              */

            }
          }
        }
      }
      long tEnd = System.nanoTime();
      //System.out.println("ITER: " + iter + " " + ((tEnd-tStart)/1000000000.0) + " sec; totHits=" + totHits + "; totStupidHits=" + totStupidHits + "; " + queryCount + " queries");
      System.out.println("ITER: " + iter + " " + ((tEnd-tStart)/1000000000.0) + " sec; totHits=" + totHits + "; " + queryCount + " queries");
    }
    r.close();
    dir.close();
  }

  private static int stupidBBoxQuery(IndexSearcher s, double minLon, double minLat, double maxLon, double maxLat) throws IOException {
    int totalHits = 0;
    for(LeafReaderContext ctx : s.getIndexReader().leaves()) {
      SortedNumericDocValues sdv = ctx.reader().getSortedNumericDocValues("geo");
      for(int docID=0;docID<ctx.reader().maxDoc();docID++) {
        sdv.setDocument(docID);
        for (int i=0; i<sdv.count(); ++i) {
          long hash = sdv.valueAt(i);
          double lon = GeoEncodingUtils.mortonUnhashLon(hash);
          double lat = GeoEncodingUtils.mortonUnhashLat(hash);
          if (GeoRelationUtils.pointInRectPrecise(lon, lat, minLon, minLat, maxLon, maxLat)) {
            totalHits++;
            break;
          }
        }
      }
    }

    return totalHits;
  }
}
