package org.apache.lucene.search;

import org.apache.lucene.document.GeoPointField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.GeoDistanceUtils;
import org.apache.lucene.util.GeoProjectionUtils;
import org.apache.lucene.util.GeoRect;
import org.apache.lucene.util.GeoUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.SloppyMath;
import org.json.JSONObject;

import java.util.List;
import java.util.Scanner;

/**
 *
 */
public class GeoProcessor extends GeoCalculator {
    private static boolean doPostToMap(Scanner in) {
        System.out.print(" Post to map (y/n)? ");
        String s = in.next();
        return (s.equalsIgnoreCase("Y"));
    }

    private static void postTermsEnumToMap(GeoRangeComputer[] grc) {
        for (GeoRangeComputer c : grc) {
            mapPoster.post(mapPoster.toJSON(c.mbr()));
            for (GeoRangeComputer.Range rng : c.rangeBounds) {
                try {
                    Thread.sleep(150);
                    mapPoster.post(new JSONObject(rng.toGeoJson()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * OPTION 1 - Compute Distance Between 2 Points
     */
    public static void computeDistance(Scanner in) {
        double[][] pts = getPoints(in, 2);
        // compute distance with haversine
        System.out.println("\n HAVERSINE: " + SloppyMath.haversin(pts[0][LAT_INDEX], pts[0][LON_INDEX], pts[1][LAT_INDEX], pts[1][LON_INDEX])*1000.0 + " meters");
        System.out.println(" VINCENTY : " + GeoDistanceUtils.vincentyDistance(pts[0][LON_INDEX], pts[0][LAT_INDEX], pts[1][LON_INDEX], pts[1][LAT_INDEX]) + " meters");
        System.out.println(" KARNEY   : **** IN WORK ****");
    }

    /**
     * OPTION 2 - Compute a new Point from existing Location and Bearing (Heading)
     */
    public static void computePointFromLocBearing(Scanner in) {
        double[][] pts = getPoints(in, 1);
        System.out.print(" Enter Bearing : ");
        double bearing = in.nextDouble();
        System.out.print(" Enter Distance: ");
        double distance = in.nextDouble();

        double[] location = GeoProjectionUtils.pointFromLonLatBearing(pts[0][LON_INDEX], pts[0][LAT_INDEX], bearing, distance, null);
        System.out.println("\n Vincenty: " + location[LON_INDEX] + ", " + location[LAT_INDEX]);
        System.out.println(" Karney  :  *** IN WORK ***");
    }

    /**
     * OPTION 3 - Compute a Bounding Box from an existing Location and Raidus
     */
    public static void boundingBoxFromPointRadius(Scanner in) {
        boolean postToMap = doPostToMap(in);
        double[][] point = getPoints(in, 1);
        double radius = getRadius(in);
        GeoRect rect = GeoUtils.circleToBBox(point[0][LON_INDEX], point[0][LAT_INDEX], radius);
        GeoRect[] rects;
        if (rect.maxLon < rect.minLon) {
            rects = new GeoRect[] {
                    new GeoRect(GeoUtils.MIN_LON_INCL, rect.maxLon, rect.minLat, rect.maxLat),
                    new GeoRect(rect.minLon, GeoUtils.MAX_LON_INCL, rect.minLat, rect.maxLat)
            };
        } else {
            rects = new GeoRect[] {rect};
        }

        for (GeoRect r : rects) {
            System.out.println("\n" + r);
            if (postToMap == true) {
                mapPoster.post(GeoMapPoster.toJSON(r));
                try {
                    Thread.sleep(500);
                } catch (Exception e) { e.printStackTrace(); }
            }
        }
    }

    /**
     *
     */
    public static void pointRadiusToPolygon(Scanner in) {
        boolean postToMap = doPostToMap(in);
        double[][] point = getPoints(in, 1);
        double radius = getRadius(in);

        List<double[]> polyPoints = GeoUtils.circleToPoly(point[0][LON_INDEX], point[0][LAT_INDEX], radius);

        if (postToMap == true) {
            mapPoster.post(GeoMapPoster.toJSON(polyPoints));
        }
    }

    public static void mapPoints(Scanner in) {
        double[][] point = getPoints(in, 1);
        mapPoster.post(GeoMapPoster.toJSON(point));
    }

    public static void mapLine(Scanner in) {
        System.out.print(" Number of points: ");
        double[][] points = getPoints(in, in.nextInt());
        mapPoster.post(GeoMapPoster.toJSON(true, points));
    }

    public static void reproject(Scanner in, CRS crs) {
        double[][] point = getPoints(in, crs, 1, 3);
        System.out.print(" Include ENU (y/n): ");
        boolean toENU =  in.next().equalsIgnoreCase("y");
        if (crs == CRS.LLA) {
            double[] result = GeoProjectionUtils.llaToECF(point[0][LON_INDEX], point[0][LAT_INDEX], 0, null);
            System.out.println(" Projected to ECEF: " + result[LON_INDEX] + ", " + result[LAT_INDEX] + ", " + result[ALT_INDEX]);
            if (toENU) {
                double[][] center = getPoints(in, CRS.LLA, 1, 3);
                GeoProjectionUtils.llaToENU(point[0][LON_INDEX], point[0][LAT_INDEX], point[0][ALT_INDEX],
                        center[0][LON_INDEX], center[0][LAT_INDEX], center[0][ALT_INDEX], result);
                System.out.println(" Projected to ENU : " + +result[LON_INDEX] + ", " + result[LAT_INDEX] + ", " + result[ALT_INDEX]);
            }
        } else if (crs == CRS.ECEF) {
            double[] result = GeoProjectionUtils.ecfToLLA(point[0][LON_INDEX], point[0][LAT_INDEX], point[0][ALT_INDEX], null);
            System.out.println(" Projected to LLA: " + result[LON_INDEX] + ", " + result[LAT_INDEX] + ", " + result[ALT_INDEX]);
            if (toENU) {
                double[][] center = getPoints(in, CRS.LLA, 1, 3);
                GeoProjectionUtils.ecfToENU(point[0][LON_INDEX], point[0][LAT_INDEX], point[0][ALT_INDEX], center[0][LON_INDEX],
                        center[0][LAT_INDEX], center[0][ALT_INDEX], result);
                System.out.println(" Projected to ENU : " + +result[LON_INDEX] + ", " + result[LAT_INDEX] + ", " + result[ALT_INDEX]);
            }
        } else if (crs == CRS.ENU) {
            double[][] center = getPoints(in, CRS.LLA, 1, 3);
            double[] result = GeoProjectionUtils.enuToLLA(point[0][LON_INDEX], point[0][LAT_INDEX], point[0][ALT_INDEX],
                    center[0][LON_INDEX], center[0][LAT_INDEX], center[0][ALT_INDEX], null);
            System.out.println(" Projected to LLA : " + result[LON_INDEX] + ", " + result[LAT_INDEX] + ", " + result[ALT_INDEX]);
            GeoProjectionUtils.enuToECF(point[0][LON_INDEX], point[0][LAT_INDEX], point[0][ALT_INDEX], center[0][LON_INDEX],
                    center[0][LAT_INDEX], center[0][ALT_INDEX], result);
            System.out.println(" Projected to ECEF : " + result[LON_INDEX] + ", " + result[LAT_INDEX] + ", " + result[ALT_INDEX]);
        }
    }

    public static void pointDistanceTermsEnum(Scanner in) {
        boolean postToMap = doPostToMap(in);
        double[][] center = getPoints(in, 1);
        double r = getRadius(in);
        GeoRangeComputer[] grc = GeoRangeComputer.pointRadius(center[0][LON_INDEX], center[0][LAT_INDEX], r);
        System.out.print( "Check target point (y/n): ");
        if (in.next().equalsIgnoreCase("y")) {
            double[][] qPoint = getPoints(in, 1);
            for (GeoRangeComputer c : grc) {
                c.contains(qPoint[0][LON_INDEX], qPoint[0][LAT_INDEX]);
            }
        }

        if (postToMap) {
            postTermsEnumToMap(grc);
        }
    }

    public static void bboxTermsEnum(Scanner in) {
        boolean postToMap = doPostToMap(in);
        GeoRect rect = getRect(in);
        GeoRangeComputer[] grc = GeoRangeComputer.bbox(rect.minLon, rect.minLat, rect.maxLon, rect.maxLat);
        System.out.print( "Check target point (y/n): ");
        if (in.next().equalsIgnoreCase("y")) {
            double[][] qPoint = getPoints(in, 1);
            for (GeoRangeComputer c : grc) {
                c.contains(qPoint[0][LON_INDEX], qPoint[0][LAT_INDEX]);
            }
        }

        if (postToMap) {
            postTermsEnumToMap(grc);
        }
    }

    public static void polygonTermsEnum(Scanner in) throws Exception {
        boolean postToMap = doPostToMap(in);
        System.out.print(" Enter number of points for polygon: ");
        int numPts = in.nextInt();
        double[][] pts = getPoints(in, numPts);
        double[][] poly = transposePoly(pts);

//        double[] lons = new double[pts.length];
//        double[] lats = new double[pts.length];
//        for (int i=0; i<pts.length; ++i) {
//            lons[i] = pts[i][LON_INDEX];
//            lats[i] = pts[i][LAT_INDEX];
//        }
        GeoRangeComputer[] grc = GeoRangeComputer.polygon(poly[LON_INDEX], poly[LAT_INDEX]);
        System.out.print( "Check target point (y/n): ");
        if (in.next().equalsIgnoreCase("y")) {
            double[][] qPoint = getPoints(in, 1);
            for (GeoRangeComputer c : grc) {
                c.contains(qPoint[0][LON_INDEX], qPoint[0][LAT_INDEX]);
            }
        }

        if (postToMap) {
            // draw polygon
            mapPoster.post(GeoMapPoster.toJSON(true, pts));
            Thread.sleep(500);
            // draw terms enum
            postTermsEnumToMap(grc);
        }
    }

    public static void displayPointRanges(Scanner in) {
        double[][] point = getPoints(in, 1);
        long hash, hashUpper;
        double lon, lat, lonUpper, latUpper;

        for (int i=63; i>=45; i-= GeoPointField.PRECISION_STEP) {
            BytesRefBuilder brb = new BytesRefBuilder();
            NumericUtils.longToPrefixCoded(GeoUtils.mortonHash(point[0][LON_INDEX], point[0][LAT_INDEX]), i, brb);
            BytesRef br = brb.get();
            hash = NumericUtils.prefixCodedToLong(br);
            hashUpper = hash | ((1L<<i)-1);
            lon = GeoUtils.mortonUnhashLon(hash);
            lat = GeoUtils.mortonUnhashLat(hash);
            lonUpper = GeoUtils.mortonUnhashLon(hashUpper);
            latUpper = GeoUtils.mortonUnhashLat(hashUpper);
            System.out.println(i + ": " + br + " " + hash + " (" + lon + "," + lat + ")" + " : " + "(" + lonUpper + "," + latUpper + ")");
        }
    }

    public static void closestPtOnCircle(Scanner in) {
        boolean postToMap = doPostToMap(in);
        System.out.println("Enter start point:");
        double[][] cntr = getPoints(in, 1);
//    GeoRect rect = getRect(in);
        System.out.println("Enter lower left and upper right of rectangle:");
        double[][] pt = getPoints(in, 2);

        double[] closestPt = new double[2];
        GeoDistanceUtils.closestPointOnBBox(pt[0][LON_INDEX], pt[0][LAT_INDEX], pt[1][LON_INDEX], pt[1][LAT_INDEX],
                cntr[0][LON_INDEX], cntr[0][LAT_INDEX], closestPt);

        System.out.println(" Closest Point: " + closestPt[LON_INDEX] + " " + closestPt[LAT_INDEX]);

        if (postToMap) {
            double[][] pts = new double[][] {
                    {cntr[0][LON_INDEX], cntr[0][LAT_INDEX]}, closestPt};
            // draw both points
            mapPoster.post(GeoMapPoster.toJSON(pts));
            // draw a line
            mapPoster.post(GeoMapPoster.toJSON(true, pts));
        }
    }

    public static void rectCrossesCircle(Scanner in) {
        boolean postToMap = doPostToMap(in);
        GeoRect rect = getRect(in);
        System.out.println("Enter Center Point: ");
        double[][] cntr = getPoints(in, 1);
        double r = getRadius(in);

        System.out.println(" Rectangle " + (GeoUtils.rectCrossesCircle(rect.minLon, rect.minLat, rect.maxLon, rect.maxLat,
                cntr[0][LON_INDEX], cntr[0][LAT_INDEX], r) ? " Crosses Circle" : " does not cross circle"));

        if (postToMap) {
            // draw rectangle
            mapPoster.post(GeoMapPoster.toJSON(rect));

            // convert point radius to poly
            List<double[]> polyPoints = GeoUtils.circleToPoly(cntr[0][LON_INDEX], cntr[0][LAT_INDEX], r);
            mapPoster.post(GeoMapPoster.toJSON(polyPoints));
        }
    }

    public static void rectPolyRelation(Scanner in) throws Exception {
        boolean postToMap = doPostToMap(in);
        GeoRect rect = getRect(in);
        double[][] poly = getPoly(in);
        double[][] polyTrans = transposePoly(poly);
        GeoRect polyBBox = GeoUtils.polyToBBox(polyTrans[LON_INDEX], polyTrans[LAT_INDEX]);

        boolean within = GeoUtils.rectWithinPoly(rect.minLon, rect.minLat, rect.maxLon, rect.maxLat, polyTrans[LON_INDEX],
                polyTrans[LAT_INDEX], polyBBox.minLon, polyBBox.minLat, polyBBox.maxLon, polyBBox.maxLat);
        boolean crosses = GeoUtils.rectCrossesPoly(rect.minLon, rect.minLat, rect.maxLon, rect.maxLat,
                polyTrans[LON_INDEX], polyTrans[LAT_INDEX], polyBBox.minLon, polyBBox.minLat, polyBBox.maxLon, polyBBox.maxLat);
        boolean contains = GeoUtils.rectContains(rect.minLon, rect.minLat, rect.maxLon, rect.maxLat, polyBBox.minLon, polyBBox.minLat,
                polyBBox.maxLon, polyBBox.maxLat);

        System.out.print(" Rectangle ");
        if (within) {
            System.out.println("Within Poly");
        } else if (crosses) {
            System.out.println("Crosses Poly");
        } else if (contains) {
            System.out.println("Contains Poly");
        } else {
            System.out.println("Disjoint from Poly");
        }

        if (postToMap) {
            // draw bbox
            mapPoster.post(GeoMapPoster.toJSON(polyBBox));
            Thread.sleep(500);
            // draw poly
            mapPoster.post(GeoMapPoster.toJSON(true, poly));
            Thread.sleep(500);
            // draw rect
            mapPoster.post(GeoMapPoster.toJSON(rect));
        }
    }

    public static void computeMaxRadius(Scanner in) {
        double[][] pt = getPoints(in, 1);
        double[][] ptOpposite = new double[][] { {(180.0 + pt[0][LON_INDEX]) % 360, pt[0][LAT_INDEX]} };
        System.out.println(" Point opposite: " + ptOpposite[0][LON_INDEX] + ", " + ptOpposite[0][LAT_INDEX]);
        System.out.println("   Haversine: " + SloppyMath.haversin(pt[0][LAT_INDEX], pt[0][LON_INDEX], ptOpposite[0][LAT_INDEX], ptOpposite[0][LON_INDEX])*1000.0);
        System.out.println("   Vincenty : " + GeoDistanceUtils.vincentyDistance(pt[0][LON_INDEX], pt[0][LAT_INDEX], ptOpposite[0][LON_INDEX], ptOpposite[0][LAT_INDEX]));
    }
}
