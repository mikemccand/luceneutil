package perf;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.bkdtree3d.Geo3DDocValuesFormat;
import org.apache.lucene.bkdtree3d.Geo3DPointField;
import org.apache.lucene.bkdtree3d.PointInGeo3DShapeQuery;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene53.Lucene53Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.geo3d.GeoBBoxFactory;
import org.apache.lucene.geo3d.PlanetModel;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

// javac -cp build/core/classes/java:build/spatial3d/classes/java /l/util/src/main/perf/IndexAndSearchOpenStreetMapsGeo3D.java; java -cp /l/util/src/main:build/core/classes/java:build/spatial3d/classes/java perf.IndexAndSearchOpenStreetMapsGeo3D

public class IndexAndSearchOpenStreetMapsGeo3D {

  private static void createIndex() throws IOException {

    long t0 = System.nanoTime();

    CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT);

    int BUFFER_SIZE = 1 << 16;     // 64K
    InputStream is = Files.newInputStream(Paths.get("/lucenedata/open-street-maps/latlon.subsetPlusAllLondon.txt"));
    BufferedReader reader = new BufferedReader(new InputStreamReader(is, decoder), BUFFER_SIZE);

    DocValuesFormat dvFormat = new Geo3DDocValuesFormat();
    Directory dir = FSDirectory.open(Paths.get("bkdtestgeo3d"));
    Codec codec = new Lucene53Codec() {
      @Override
      public DocValuesFormat getDocValuesFormatForField(String field) {
        return dvFormat;
      }
      };
    IndexWriterConfig iwc = new IndexWriterConfig(null);
    //iwc.setRAMBufferSizeMB(256);
    iwc.setMaxBufferedDocs(109630);
    iwc.setCodec(codec);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    IndexWriter w = new IndexWriter(dir, iwc);
    
    while (true) {
      String line = reader.readLine();
      if (line == null) {
        break;
      }

      String[] parts = line.split(",");
      //long id = Long.parseLong(parts[0]);
      double lat = toRadians(Double.parseDouble(parts[1]));
      double lon = toRadians(Double.parseDouble(parts[2]));
      Document doc = new Document();
      doc.add(new Geo3DPointField("point", PlanetModel.WGS84, lat, lon));
      w.addDocument(doc);
    }
    long t1 = System.nanoTime();
    System.out.println(((t1-t0)/1000000000.0) + " sec to build index");
    System.out.println(w.maxDoc() + " total docs");
    //w.forceMerge(1);

    w.close();
    long t2 = System.nanoTime();
    System.out.println(((t2-t1)/1000000000.0) + " sec to close");
  }

  private static double toRadians(double degrees) {
    return Math.PI*(degrees/360.0);
  }

  private static void queryIndex() throws IOException {
    Directory dir = FSDirectory.open(Paths.get("bkdtestgeo3d"));
    System.out.println("DIR: " + dir);
    IndexReader r = DirectoryReader.open(dir);
    System.out.println("maxDoc=" + r.maxDoc());
    IndexSearcher s = new IndexSearcher(r);
    //SegmentReader sr = (SegmentReader) r.leaves().get(0).reader();
    //BKDTreeReader reader = ((BKDTreeSortedNumericDocValues) sr.getSortedNumericDocValues("point")).getBKDTreeReader();

    //System.out.println("reader MB heap=" + (reader.ramBytesUsed()/1024/1024.));

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
          for(int latStepEnd=latStep+1;latStepEnd<=STEPS;latStepEnd++) {
            double latEnd = MIN_LAT + latStepEnd * (MAX_LAT - MIN_LAT) / STEPS;
            for(int lonStepEnd=lonStep+1;lonStepEnd<=STEPS;lonStepEnd++) {
              double lonEnd = MIN_LON + lonStepEnd * (MAX_LON - MIN_LON) / STEPS;

              Query q = new PointInGeo3DShapeQuery(PlanetModel.WGS84, "point", GeoBBoxFactory.makeGeoBBox(PlanetModel.WGS84, toRadians(latEnd), toRadians(lat), toRadians(lon), toRadians(lonEnd)));
              TotalHitCountCollector c = new TotalHitCountCollector();
              //long t0 = System.nanoTime();
              s.search(q, c);

              //System.out.println("\nITER: now query lat=" + lat + " latEnd=" + latEnd + " lon=" + lon + " lonEnd=" + lonEnd);
              //Bits hits = reader.intersect(lat, latEnd, lon, lonEnd);
              //System.out.println("  total hits: " + hitCount);
              //totHits += ((FixedBitSet) hits).cardinality();
              //System.out.println("  add tot " + c.getTotalHits());
              totHits += c.getTotalHits();
              queryCount++;
            }
          }
        }
      }

      long tEnd = System.nanoTime();
      System.out.println("ITER: " + iter + " " + ((tEnd-tStart)/1000000000.0) + " sec; totHits=" + totHits + "; " + queryCount + " queries");
    }

    IOUtils.close(r, dir);
  }

  public static void main(String[] args) throws IOException {
    //createIndex();
    queryIndex();
  }
}
