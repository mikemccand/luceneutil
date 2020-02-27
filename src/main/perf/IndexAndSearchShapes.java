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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Circle;
import org.apache.lucene.geo.SimpleWKTShapeParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

import org.apache.lucene.index.PointValues;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

//  // STEPS:
////
////   * Download index data from http://home.apache.org/~ivera/osmdata.wkt.gz. this is a selection of polygon data  from OpenStreetMaps corpus and place in a local directory;.
////
////   * Download polygon  files from http://home.apache.org/~mikemccand/geobench and place in a local directory.
////
////   * Uncompress files
////
////   * cd /path/to/lucene, and go into lucene subdir and run "ant jar"
////
////   * tweak the commands below to match your classpath, then run with options like "-intersects -point -file osmdata.wkt -reindex"
//
// Example how to compile it:
// javac -cp path-to-lucene-solr/lucene/build/test-framework/classes/java:path-to-lucene-solr/lucene/build/codecs/classes/java:path-to-lucene-solr/lucene/build/core/classes/java:path-to-lucene-solr/lucene/build/sandbox/classes/java path-to-luceneutil/src/main/perf/IndexAndSearchShapes.java
//
// Example how to run it:
// java -Xmx10g -cp path-to-luceneutil/src/main:path-to-lucene-solr/lucene/build/test-framework/classes/java:path-to-lucene-solr/lucene/build/codecs/classes/java:path-to-lucene-solr/lucene/build/core/classes/java:path-to-lucene-solr/lucene/build/sandbox/classes/java perf.IndexAndSearchShapes -intersects -point -file osmdata.wkt

public class IndexAndSearchShapes {

    private static final int ITERS = 10;

    /** prefix for indexes that will be built */
    static final String INDEX_LOCATION;
    /** prefix for data files such as osmdata.wkt */
    static final String DATA_LOCATION;
    static {
        switch (System.getProperty("user.name")) {
            case "ivera":
                INDEX_LOCATION = "/data/shapeTest";
                DATA_LOCATION = "/data/";
                break;
            default:
                throw new UnsupportedOperationException("the benchmark does not know you, "+System.getProperty("user.name")+". please introduce yourself to the code and push");
        }
    }

    static int NUM_PARTS = 1;
    static final String FIELD = "shape";


    private static String getName(int part, String fileName, boolean isDev) {
        String name = INDEX_LOCATION + fileName + part + ".bkd";
        if (isDev) {
            name += ".dev";
        }
        return name;
    }

    /** Only used to compute bbox for all polygons loaded from the -polyFile */
    private static double minLat = Double.POSITIVE_INFINITY;
    private static double maxLat = Double.NEGATIVE_INFINITY;
    private static double minLon = Double.POSITIVE_INFINITY;
    private static double maxLon = Double.NEGATIVE_INFINITY;

    // NOTE: use geoJSONToJava.py to convert the geojson file to simple text file:
    private static List<Polygon[]> readPolygons(String fileName) throws IOException {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        int BUFFER_SIZE = 1 << 16;     // 64K
        InputStream is = Files.newInputStream(Paths.get(fileName));
        if (fileName.endsWith(".gz")) {
            is = new GZIPInputStream(is);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, decoder), BUFFER_SIZE);
        List<Polygon[]> result = new ArrayList<>();
        int totalVertexCount = 0;

        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            if (line.startsWith("count=") == false) {
                System.out.println("1 " + line);
                throw new AssertionError();
            }
            // Count is the number of polygons the query has:
            int count = Integer.parseInt(line.substring(6, line.indexOf(' ')));
            List<Polygon> polys = new ArrayList<>();
            for(int i=0;i<count;i++) {
                line = reader.readLine();
                // How many polygons (if this is > 1, the first poly is the real one, and
                // all others are hole-polys that are subtracted):
                if (line.startsWith("  poly count=") == false) {
                    throw new AssertionError();
                }
                int polyCount = Integer.parseInt(line.substring(13));
                List<Polygon> polyPlusHoles = new ArrayList<>();
                double sumLat = 0.0;
                double sumLon = 0.0;
                for(int j=0;j<polyCount;j++) {
                    line = reader.readLine();
                    if (line.startsWith("    vertex count=") == false) {
                        System.out.println("1w " + line);
                        throw new AssertionError();
                    }

                    int vertexCount = Integer.parseInt(line.substring(17));
                    double[] lats = new double[vertexCount];
                    double[] lons = new double[vertexCount];
                    line = reader.readLine();
                    if (line.startsWith("      lats ") == false) {
                        throw new AssertionError();
                    }
                    String[] parts = line.substring(11).split(" ");
                    if (parts.length != vertexCount) {
                        System.out.println(line);
                        throw new AssertionError();
                    }
                    for(int k=0;k<vertexCount;k++) {
                        lats[k] = Double.parseDouble(parts[k]);
                        sumLat += lats[k];
                        minLat = Math.min(minLat, lats[k]);
                        maxLat = Math.max(maxLat, lats[k]);
                    }

                    line = reader.readLine();
                    if (line.startsWith("      lons ") == false) {
                        System.out.println(line);
                        throw new AssertionError();
                    }
                    parts = line.substring(11).split(" ");
                    if (parts.length != vertexCount) {
                        System.out.println(line);
                        throw new AssertionError();
                    }
                    for(int k=0;k<vertexCount;k++) {
                        lons[k] = Double.parseDouble(parts[k]);
                        sumLon += lons[k];
                        minLon = Math.min(minLon, lons[k]);
                        maxLon = Math.max(maxLon, lons[k]);
                    }
                    polyPlusHoles.add(new Polygon(lats, lons));
                    totalVertexCount += vertexCount;
                }

                Polygon firstPoly = polyPlusHoles.get(0);
                Polygon[] holes = polyPlusHoles.subList(1, polyPlusHoles.size()).toArray(new Polygon[polyPlusHoles.size()-1]);

                polys.add(new Polygon(firstPoly.getPolyLats(), firstPoly.getPolyLons(), holes));
            }

            result.add(polys.toArray(new Polygon[polys.size()]));
        }
        System.out.println("Total vertex count: " + totalVertexCount);

        return result;
    }


    private static void createIndex(boolean fast, boolean doForceMerge, String fileName, boolean isDev) throws IOException, InterruptedException {

        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);

        int BUFFER_SIZE = 1 << 16;     // 64K
        InputStream is = Files.newInputStream(Paths.get(DATA_LOCATION, fileName));
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, decoder), BUFFER_SIZE);

        int NUM_THREADS;
        if (fast) {
            NUM_THREADS = Runtime.getRuntime().availableProcessors() / 2;
        } else {
            NUM_THREADS = 1;
        }

        int CHUNK = 10000;

        long t0 = System.nanoTime();
        AtomicLong totalCount = new AtomicLong();
        for(int part=0;part<NUM_PARTS;part++) {
            org.apache.lucene.store.Directory dir = org.apache.lucene.store.FSDirectory.open(Paths.get(getName(part, fileName, isDev)));

            org.apache.lucene.index.IndexWriterConfig iwc = new org.apache.lucene.index.IndexWriterConfig(null);
            iwc.setOpenMode(org.apache.lucene.index.IndexWriterConfig.OpenMode.CREATE);
            if (fast) {
                ((org.apache.lucene.index.TieredMergePolicy) iwc.getMergePolicy()).setMaxMergedSegmentMB(Double.POSITIVE_INFINITY);
                iwc.setRAMBufferSizeMB(1024);
            } else {
                iwc.setMaxBufferedDocs(109630);
                iwc.setMergePolicy(new org.apache.lucene.index.LogDocMergePolicy());
                iwc.setMergeScheduler(new org.apache.lucene.index.SerialMergeScheduler());
            }
            //iwc.setInfoStream(new PrintStreamInfoStream(System.out));
            org.apache.lucene.index.IndexWriter w = new org.apache.lucene.index.IndexWriter(dir, iwc);

            Thread[] threads = new Thread[NUM_THREADS];
            AtomicBoolean finished = new AtomicBoolean();
            Object lock = new Object();

            final int finalPart = part;

            for(int t=0;t<NUM_THREADS;t++) {
                threads[t] = new Thread() {
                    @Override
                    public void run() {
                        String[] lines = new String[CHUNK];
                        int chunkCount = 0;
                        while (finished.get() == false) {
                            try {
                                int count = CHUNK;
                                synchronized(lock) {
                                    for(int i=0;i<CHUNK;i++) {
                                        String line = reader.readLine();
                                        if (line == null) {
                                            count = i;
                                            finished.set(true);
                                            break;
                                        }
                                        lines[i] = line;
                                    }
                                    if (finalPart == 0 && totalCount.get()+count >= 2000000000) {
                                        finished.set(true);
                                    }
                                }

                                for(int i=0;i<count;i++) {
                                    try {
                                        Document doc = new Document();
                                        Object shape = SimpleWKTShapeParser.parse(lines[i]);
                                        //System.out.println(lines[i]);
                                        Field[] fields = createIndexableFields(FIELD, shape);
                                        for (Field field : fields) {
                                            doc.add(field);
                                        }
                                        w.addDocument(doc);
                                        long x = totalCount.incrementAndGet();
                                        if (x % 500000 == 0) {
                                            System.out.println(x + "...");
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        System.out.println(lines[i]);
                                        //throw new IOException("error indexing shape :" + lines[i], e);
                                    }
                                }
                                chunkCount++;

                            } catch (IOException ioe) {
                                throw new RuntimeException(ioe);
                            }
                        }
                    }
                };
                threads[t].start();
            }

            for(Thread thread : threads) {
                thread.join();
            }

            System.out.println("Part " + part + " is done: w.maxDoc()=" + w.getDocStats().maxDoc);
            w.commit();
            w.flush();
            System.out.println("done commit");
            long t1 = System.nanoTime();
            System.out.println(((t1-t0)/1000000000.0) + " sec to index part " + part);
            if (doForceMerge) {
                w.forceMerge(1);
                long t2 = System.nanoTime();
                System.out.println(((t2-t1)/1000000000.0) + " sec to force merge part " + part);
            }
            w.close();
        }
    }

    private static Field[]createIndexableFields(String field, Object shape) {
        if (shape instanceof Polygon) {
            return LatLonShape.createIndexableFields(field, (Polygon) shape);
        } else if (shape instanceof Line) {
            return LatLonShape.createIndexableFields(field, (Line) shape);
        } else if (shape instanceof Object[]) {
            Object[] shapes = (Object[]) shape;
            List<Field> fields = new ArrayList();
            for (Object aShape : shapes) {
                fields.addAll(Arrays.asList(createIndexableFields(field, aShape)));
            }
            return fields.toArray(new Field[fields.size()]);
        } else if (shape instanceof double[]) {
            double[] point = (double[]) shape;
            return LatLonShape.createIndexableFields(field, point[1], point[0]);
        }else {
            throw new IllegalArgumentException(shape.toString());
        }
    }

    /** One normally need not clone a Polygon (it's a read only holder class), but we do this here just to keep the benchmark honest, by
     *  including Polygon construction cost in measuring search run time. */
    private static Polygon[] clonePolygon(Polygon[] polies) {
        Polygon[] newPolies = new Polygon[polies.length];
        for(int i=0;i<polies.length;i++) {
            Polygon poly = polies[i];
            Polygon[] holes = poly.getHoles();
            if (holes.length > 0) {
                holes = clonePolygon(holes);
            }
            newPolies[i] = new Polygon(poly.getPolyLats(), poly.getPolyLons(), holes);
        }
        return newPolies;
    }

    private static void queryIndex(String queryClass, int gons, String polyFile, String fileName, boolean isDev, ShapeField.QueryRelation op) throws IOException {
        IndexSearcher[] searchers = new IndexSearcher[NUM_PARTS];
        org.apache.lucene.store.Directory[] dirs = new org.apache.lucene.store.Directory[NUM_PARTS];
        long sizeOnDisk = 0;
        for(int part=0;part<NUM_PARTS;part++) {
            dirs[part] = org.apache.lucene.store.FSDirectory.open(Paths.get(getName(part, fileName, isDev)));
            searchers[part] = new IndexSearcher(org.apache.lucene.index.DirectoryReader.open(dirs[part]));
            searchers[part].setQueryCache(null);
            for(String name : dirs[part].listAll()) {
                sizeOnDisk += dirs[part].fileLength(name);
            }
        }
        System.out.println("INDEX SIZE: " + (sizeOnDisk/1024./1024./1024.) + " GB");
        long bytes = 0;
        long maxDoc = 0;
        long numPoints = 0;
        for(IndexSearcher s : searchers) {
            org.apache.lucene.index.IndexReader r = s.getIndexReader();
            maxDoc += r.maxDoc();
            for(org.apache.lucene.index.LeafReaderContext ctx : r.leaves()) {
                org.apache.lucene.index.CodecReader cr = (org.apache.lucene.index.CodecReader) ctx.reader();
                PointValues values = cr.getPointValues(FIELD);
                if (values != null) {
                    numPoints += values.size();
                }
                bytes += cr.ramBytesUsed();
            }
        }
        System.out.println("READER MB: " + (bytes/1024./1024.));
        System.out.println("maxDoc=" + maxDoc);
        System.out.println("numPoints=" + numPoints);

        double bestQPS = Double.NEGATIVE_INFINITY;

        // million hits per second:
        double bestMHPS = Double.NEGATIVE_INFINITY;

        if (queryClass.equals("polyFile")) {

            // TODO: only load the double[][] here, so that we includ the cost of making Polygon and Query in each iteration!!
            List<Polygon[]> polygons = readPolygons(polyFile);


            System.out.println("\nUsing on-the-fly polygon queries, loaded from file " + polyFile);

            for (int iter = 0; iter < ITERS; iter++) {
                long tStart = System.nanoTime();
                long totHits = 0;
                int queryCount = 0;
                for (Polygon[] multiPolygon : polygons) {
                    // We do this to keep the benchmark honest, so any construction cost of a polygon is included in our run time measure:
                    multiPolygon = clonePolygon(multiPolygon);
                    Query q = LatLonShape.newPolygonQuery(FIELD, op, multiPolygon);
                    for (IndexSearcher s : searchers) {
                        int hitCount = s.count(q);
                        totHits += hitCount;
                    }
                    queryCount++;
                }

                long tEnd = System.nanoTime();
                double elapsedSec = (tEnd - tStart) / 1000000000.0;
                double qps = queryCount / elapsedSec;
                double mhps = (totHits / 1000000.0) / elapsedSec;
                System.out.println(String.format(Locale.ROOT,
                        "ITER %d: %.2f M hits/sec, %.2f QPS (%.2f sec for %d queries), totHits=%d",
                        iter, mhps, qps, elapsedSec, queryCount, totHits));
                if (qps > bestQPS) {
                    System.out.println("  ***");
                    bestQPS = qps;
                    bestMHPS = mhps;
                }

            }
        } else {
            System.out.println("\nUsing on-the-fly queries");
            int STEPS = 5;
            double MIN_LAT = 51.0919106;
            double MAX_LAT = 51.6542719;
            double MIN_LON = -0.3867282;
            double MAX_LON = 0.8492337;

            // makeRegularPoly has insanely slow math, so make the double[]'s here.
            // we still form the query inside the benchmark loop (e.g. to account for preprocessing)
            ArrayList<double[][]> polys = new ArrayList<>(225);
            if ("poly".equals(queryClass)) {

                for(int latStep=0;latStep<STEPS;latStep++) {
                    double lat = MIN_LAT + latStep * (MAX_LAT - MIN_LAT) / STEPS;
                    for(int lonStep=0;lonStep<STEPS;lonStep++) {
                        double lon = MIN_LON + lonStep * (MAX_LON - MIN_LON) / STEPS;
                        for(int latStepEnd=latStep+1;latStepEnd<=STEPS;latStepEnd++) {
                            double latEnd = MIN_LAT + latStepEnd * (MAX_LAT - MIN_LAT) / STEPS;
                            for(int lonStepEnd=lonStep+1;lonStepEnd<=STEPS;lonStepEnd++) {
                                double lonEnd = MIN_LON + lonStepEnd * (MAX_LON - MIN_LON) / STEPS;
                                double distanceMeters = org.apache.lucene.util.SloppyMath.haversinMeters(lat, lon, latEnd, lonEnd)/2.0;
                                double centerLat = (lat+latEnd)/2.0;
                                double centerLon = (lon+lonEnd)/2.0;
                                polys.add(makeRegularPoly(centerLat, centerLon, distanceMeters, gons));
                            }
                        }
                    }
                }
            }

            for(int iter=0;iter<ITERS;iter++) {
                long tStart = System.nanoTime();
                long totHits = 0;
                double totNearestDistance = 0.0;
                int queryCount = 0;

                for(int latStep=0;latStep<STEPS;latStep++) {
                    double lat = MIN_LAT + latStep * (MAX_LAT - MIN_LAT) / STEPS;
                    for(int lonStep=0;lonStep<STEPS;lonStep++) {
                        double lon = MIN_LON + lonStep * (MAX_LON - MIN_LON) / STEPS;
                        for(int latStepEnd=latStep+1;latStepEnd<=STEPS;latStepEnd++) {
                            double latEnd = MIN_LAT + latStepEnd * (MAX_LAT - MIN_LAT) / STEPS;
                            for(int lonStepEnd=lonStep+1;lonStepEnd<=STEPS;lonStepEnd++) {

                                double lonEnd = MIN_LON + lonStepEnd * (MAX_LON - MIN_LON) / STEPS;
                                double distanceMeters = org.apache.lucene.util.SloppyMath.haversinMeters(lat, lon, latEnd, lonEnd)/2.0;
                                double centerLat = (lat+latEnd)/2.0;
                                double centerLon = (lon+lonEnd)/2.0;


                                Query q = null;

                                switch(queryClass) {
                                    case "poly":
                                        double[][] poly = polys.get(queryCount);
                                        q = LatLonShape.newPolygonQuery(FIELD, op, new Polygon(poly[0], poly[1]));
                                        break;
                                    case "box":
                                        q = LatLonShape.newBoxQuery(FIELD, op, lat, latEnd, lon, lonEnd);
                                        break;
                                    case "point":
                                        q = LatLonShape.newPointQuery(FIELD, op, new double[] {lat, lon});
                                        break;
                                    case "distance":
                                        Circle circle = new Circle(centerLat, centerLon, distanceMeters);
                                        q = LatLonShape.newDistanceQuery(FIELD, ShapeField.QueryRelation.INTERSECTS, circle);
                                        break;
                                    default:
                                        throw new AssertionError("unknown queryClass " + queryClass);
                                }

                                for(IndexSearcher s : searchers) {
                                    int hitCount = s.count(q);
                                    totHits += hitCount;
                                }
                                queryCount++;
                            }
                        }
                    }
                }

                long tEnd = System.nanoTime();
                double elapsedSec = (tEnd-tStart)/1000000000.0;
                double qps = queryCount / elapsedSec;
                double mhps = (totHits/1000000.0) / elapsedSec;
                if (queryClass.equals("nearest")) {
                    System.out.println(String.format(Locale.ROOT,
                            "ITER %d: %.2f QPS (%.2f sec for %d queries), totNearestDistance=%.10f, totHits=%d",
                            iter, qps, elapsedSec, queryCount, totNearestDistance, maxDoc));
                } else {
                    System.out.println(String.format(Locale.ROOT,
                            "ITER %d: %.2f M hits/sec, %.2f QPS (%.2f sec for %d queries), totHits=%d",
                            iter, mhps, qps, elapsedSec, queryCount, totHits));
                }
                if (qps > bestQPS) {
                    System.out.println("  ***");
                    bestQPS = qps;
                    bestMHPS = mhps;
                }
            }
        }
        System.out.println("BEST M hits/sec: " + bestMHPS);
        System.out.println("BEST QPS: " + bestQPS);

        for(IndexSearcher s : searchers) {
            s.getIndexReader().close();
        }
        org.apache.lucene.util.IOUtils.close(dirs);
    }

    /** Makes an n-gon, centered at the provided lat/lon, and each vertex approximately
     *  distanceMeters away from the center.
     *
     * Do not invoke me across the dateline or a pole!! */
    private static double[][] makeRegularPoly(double centerLat, double centerLon, double radiusMeters, int gons) {

        // System.out.println("MAKE POLY: centerLat=" + centerLat + " centerLon=" + centerLon + " radiusMeters=" + radiusMeters + " gons=" + gons);

        double[][] result = new double[2][];
        result[0] = new double[gons+1];
        result[1] = new double[gons+1];
        //System.out.println("make gon=" + gons);
        for(int i=0;i<gons;i++) {
            double angle = 360.0-i*(360.0/gons);
            //System.out.println("  angle " + angle);
            double x = Math.cos(Math.toRadians(angle));
            double y = Math.sin(Math.toRadians(angle));
            double factor = 2.0;
            double step = 1.0;
            int last = 0;

            //System.out.println("angle " + angle + " slope=" + slope);
            // Iterate out along one spoke until we hone in on the point that's nearly exactly radiusMeters from the center:
            while (true) {
                double lat = centerLat + y * factor;
                GeoUtils.checkLatitude(lat);
                double lon = centerLon + x * factor;
                GeoUtils.checkLongitude(lon);
                double distanceMeters = org.apache.lucene.util.SloppyMath.haversinMeters(centerLat, centerLon, lat, lon);

                //System.out.println("  iter lat=" + lat + " lon=" + lon + " distance=" + distanceMeters + " vs " + radiusMeters);
                if (Math.abs(distanceMeters - radiusMeters) < 0.1) {
                    // Within 10 cm: close enough!
                    result[0][i] = lat;
                    result[1][i] = lon;
                    break;
                }

                if (distanceMeters > radiusMeters) {
                    // too big
                    //System.out.println("    smaller");
                    factor -= step;
                    if (last == 1) {
                        //System.out.println("      half-step");
                        step /= 2.0;
                    }
                    last = -1;
                } else if (distanceMeters < radiusMeters) {
                    // too small
                    //System.out.println("    bigger");
                    factor += step;
                    if (last == -1) {
                        //System.out.println("      half-step");
                        step /= 2.0;
                    }
                    last = 1;
                }
            }
        }

        // close poly
        result[0][gons] = result[0][0];
        result[1][gons] = result[1][0];

        //System.out.println("  polyLats=" + Arrays.toString(result[0]));
        //System.out.println("  polyLons=" + Arrays.toString(result[1]));

        return result;
    }

    private static String setQueryClass(String currentValue, String newValue) {
        if (currentValue != null) {
            throw new IllegalArgumentException("specify only one of -poly, -polyFile, -distance, -box, -point");
        }
        return newValue;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        boolean reindex = false;
        boolean fastReindex = false;
        String queryClass = null;
        String polyFile = null;
        ShapeField.QueryRelation op = ShapeField.QueryRelation.INTERSECTS;
        int gons = 0;
        boolean isDev = false;
        boolean forceMerge = false;
        String fileName = null;
        for(int i=0;i<args.length;i++) {
            String arg = args[i];
            if (arg.equals("-reindex")) {
                reindex = true;
            } else if (arg.equals("-reindexFast")) {
                reindex = true;
                fastReindex = true;
            } else if (arg.equals("-dev")) {
                isDev = true;
            } else if (arg.equals("-file")) {
                if (i + 1 < args.length) {
                    fileName = args[i+1];
                    i++;
                } else {
                    throw new IllegalArgumentException("missing file name argument to -file");
                }
            } else if (arg.equals("-intersects")) {
                op = ShapeField.QueryRelation.INTERSECTS;
            } else if (arg.equals("-within")) {
                op = ShapeField.QueryRelation.WITHIN;
            } else if (arg.equals("-disjoint")) {
                op = ShapeField.QueryRelation.DISJOINT;
            } else if (arg.equals("-contains")) {
                op = ShapeField.QueryRelation.CONTAINS;
            } else if (arg.equals("-polyMedium")) {
                // London boroughs:
                queryClass = setQueryClass(queryClass, "polyFile");
                polyFile = DATA_LOCATION + "/london.boroughs.poly.txt.gz";
            } else if (arg.equals("-polyRussia")) {
                // Just the one Russia Federation polygon from geonames shapes_simplified_low.txt
                queryClass = setQueryClass(queryClass, "polyFile");
                polyFile = DATA_LOCATION + "/russia.poly.txt.gz";
            } else if (arg.equals("-polyFile")) {
                queryClass = setQueryClass(queryClass, "polyFile");
                if (i + 1 < args.length) {
                    polyFile = args[i+1];
                    i++;
                } else {
                    throw new IllegalArgumentException("missing file argument to -polyFile");
                }
            } else if (arg.equals("-poly")) {
                queryClass = setQueryClass(queryClass, "poly");
                if (i + 1 < args.length) {
                    gons = Integer.parseInt(args[i+1]);
                    if (gons < 3) {
                        throw new IllegalArgumentException("gons must be >= 3; got " + gons);
                    }
                    i++;
                } else {
                    throw new IllegalArgumentException("missing gons argument to -poly");
                }
            }  else if (arg.equals("-box")) {
                queryClass = setQueryClass(queryClass, "box");
            } else if (arg.equals("-point")) {
                queryClass = setQueryClass(queryClass, "point");
            } else if (arg.equals("-distance")) {
                queryClass = setQueryClass(queryClass, "distance");
            } else if (arg.equals("-forceMerge")) {
                forceMerge = true;
            } else {
                throw new IllegalArgumentException("unknown command line option \"" + arg + "\"");
            }
        }
        if (queryClass == null) {
            throw new IllegalArgumentException("must specify exactly one of -box, -polyFile or -poly gons; got none");
        }
        if (fileName == null) {
            fileName = "osmdata.wkt";
            //throw new IllegalArgumentException("file name must must be provided using parameter -file <fileName>");
        }


        System.out.println("Index path: " + getName(0, fileName, isDev));

        if (reindex) {
            createIndex(fastReindex, forceMerge, fileName, isDev);
        }
        queryIndex(queryClass, gons, polyFile, fileName, isDev, op);
    }
}
