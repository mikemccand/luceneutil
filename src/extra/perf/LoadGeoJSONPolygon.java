import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.lucene.geo.Polygon;

// ant jar; javac -cp build/core/classes/java /l/util/src/main/perf/LoadGeoJSONPolygon.java

// java -cp build/core/classes/java:/l/util/src/main/perf LoadGeoJSONPolygon.java /l/BIGPolygon.mikemccand/cleveland.geojson

public class LoadGeoJSONPolygon {
  public static void main(String[] args) throws Exception {
    byte[] encoded = Files.readAllBytes(Paths.get(args[0]));
    String s = new String(encoded, StandardCharsets.UTF_8);
    System.out.println("Polygon string is " + s.length() + " characters");
    Polygon[] result = Polygon.fromGeoJSON(s);
    System.out.println(result.length + " polygons:");
    int vertexCount = 0;
    for(Polygon polygon : result) {
      vertexCount += polygon.getPolyLats().length;
      for(Polygon hole : polygon.getHoles()) {
        vertexCount += hole.getPolyLats().length;
      }
      System.out.println("  " + polygon.getPolyLats().length + " vertices; " + polygon.getHoles().length + " holes; " + vertexCount + " total vertices");
    }
  }
}
