package org.apache.lucene.search;

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

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.lucene.util.GeoDistanceUtils;
import org.apache.lucene.util.GeoProjectionUtils;
import org.apache.lucene.util.GeoRect;
import org.apache.lucene.util.GeoUtils;
import org.apache.lucene.util.SloppyMath;
import org.json.JSONObject;

/**
 * A simple command line utility for testing {@code org.apache.lucene.util.}
 */
public class GeoCalculator {
  private static GeoMapPoster mapPoster = new GeoMapPoster("localhost", "3000");

  private static int ALT_INDEX = 2;
  private static int LAT_INDEX = 1;
  private static int LON_INDEX = 0;

  enum DistanceMethod {
    HAVERSINE, VINCENTY, KARNEY
  }

  enum CRS {
    LLA, ECEF, ENU
  }

  private static double[][] getPoints(Scanner in, int numPts) {
    return getPoints(in, CRS.LLA, numPts, 2);
  }

  public static double[][] getPoints(Scanner in, CRS crs, int numPts, int dim) {
    double[][] points = new double[numPts][];
    for (int p=0; p<numPts; ++p) {
      System.out.print(" Enter point " + p + " (lon, lat) | (lon, lat, alt): ");
      String pt = in.next();
      String[] c = pt.replaceAll("\\s","").split(",");
      points[p] = new double[c.length];
      for (int i=0; i<c.length; ++i) {
        points[p][i] = Double.parseDouble(c[i]);
      }
    }
    return points;
  }

//  public static double[][] getPoints(Scanner in, CRS crs, int numPts, int dim) {
//    String[] labels;
//    if (dim > 3) dim = 3;
//    else if (dim < 2) dim = 2;
//    if (crs == CRS.ECEF || crs == CRS.ENU) labels = new String[] {"x", "y"};
//    else labels = new String[] {"lon", "lat"};
//    final int len = numPts * dim;
//    double[][] points = new double[numPts][dim];
//    for (int i=0,p=0; i<len; ++i, p=i/dim) {
//      switch(i%dim) {
//        case 0:
//          System.out.print(" Enter " + labels[0] + " " + p + ": ");
//          points[p][LON_INDEX] = (crs == CRS.LLA) ? GeoUtils.normalizeLon(in.nextDouble()) : in.nextDouble();
//          break;
//        case 1:
//          System.out.print(" Enter " + labels[1] + " " + p + ": ");
//          points[p][LAT_INDEX] = (crs == CRS.LLA) ? GeoUtils.normalizeLat(in.nextDouble()) : in.nextDouble();
//          break;
//        case 2:
//          System.out.print(" Enter alt " + p + ": ");
//          points[p][ALT_INDEX] = in.nextDouble();
//          break;
//      }
//    }
//    return points;
//  }

  public static void getMapPosterURL(Scanner in) {

  }

  public static double getRadius(Scanner in) {
    System.out.print(" Enter Radius: ");
    return in.nextDouble();
  }

  public static GeoRect getRect(Scanner in) {
    System.out.println("Enter lower left:");
    double[][] ll = getPoints(in, 1);
    System.out.println("Enter upper right:");
    double[][] ur = getPoints(in, 1);
    return new GeoRect(ll[0][LON_INDEX], ur[0][LON_INDEX], ll[0][LAT_INDEX], ur[0][LAT_INDEX]); //double[][] {ll[0], ur[1]};
  }

  private static DistanceMethod getDistanceMethod(Scanner in, DistanceMethod... omit) {
    boolean error = true;
    String distMeth;
    final List<DistanceMethod> omission = Arrays.asList(omit);
    while (error == true) {
      System.out.println("\n\t------------------------\n\t Choose Distance method\n\t------------------------ ");
      if (!omission.contains(DistanceMethod.HAVERSINE)) System.out.println("\t a. Haversine");
      if (!omission.contains(DistanceMethod.VINCENTY)) System.out.println("\t b. Vincenty");
      if (!omission.contains(DistanceMethod.KARNEY)) System.out.println("\t c. Karney");
      System.out.println("\t------------------------");
      System.out.print("\t  Distance Method: ");
      distMeth = in.next();
      switch (distMeth) {
        case "a": return DistanceMethod.HAVERSINE;
        case "b": return DistanceMethod.VINCENTY;
        case "c": return DistanceMethod.KARNEY;
        case "q": return null;
        default:
          System.out.println("\n  ERROR!! INVALID DISTANCE OPTION SELECTED!");
          break;
      }
    }
    return null;
  }

  private static boolean doPostToMap(Scanner in) {
    System.out.print(" Post to map (y/n)? ");
    String s = in.next();
    return (s.equalsIgnoreCase("Y"));
  }

  private static void postTermsEnumToMap(GeoRangeComputer[] grc) {
    for (GeoRangeComputer c : grc) {
      mapPoster.post(mapPoster.toJSON(new GeoRect(c.minLon, c.maxLon, c.minLat, c.maxLat)));
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

  public static void sillyAsciiArt() {
    System.out.println("_________________________              ");
    System.out.println("    ||   ||     ||   ||                ");
    System.out.println("    ||   ||, , ,||   ||                ");
    System.out.println("    ||  (||/|/(\\||/  ||    L           ");
    System.out.println("    ||  ||| _'_`|||  ||    I           ");
    System.out.println("    ||   || o o ||   ||    L                ");
    System.out.println("    ||  (||  - `||)  ||    INCARCERATED ");
    System.out.println("    ||   ||  =  ||   ||                 ");
    System.out.println("NWK ||   ||\\___/||   || ");
    System.out.println("    ||___||) , (||___||   ");
    System.out.println("   /||---||-\\_/-||---||\\     ");
    System.out.println("  / ||--_||_____||_--|| \\   ");
    System.out.println(" (_(||)-| S123-45 |-(||)_)  ");
  }

  /**
   * Main Menu
   */
  public static String getOption(Scanner in) {
    System.out.println("\n/------------------------------------- GEO CALCULATOR -------------------------------/");
    System.out.println("/ 1. Distance Between 2 points                    2. Point from lon, lat, bearing    /");
    System.out.println("/ 3. Bounding box from point radius               4. Point Radius to Polygon         /");
    System.out.println("/ 5. Map Point(s)                                 6. Map Line                        /");
    System.out.println("/ 7. Re-project from LLA                          8. Re-project from ECEF            /");
    System.out.println("/ 9. Re-project from ENU                          a. Compute DistanceQuery TermsEnum /");
    System.out.println("/ b. Compute BBoxQuery TermsEnum                  c. Closest point on rectangle      /");
    System.out.println("/ d. Does Rect Cross Circle                       e. Compute max radius              /");
    System.out.println("/ 0. Options                                                                         /");
    System.out.println("/ q. QUIT                                                                            /");
    System.out.println("/------------------------------------------------------------------------------------/");
    System.out.print(  "Enter Option: ");
    return in.next();
  }

  /**
   * Main Method
   */
  public static void main(String[] args) {
    try {
      sillyAsciiArt();
      Scanner in = new Scanner(System.in);
      for (String option = getOption(in); ; option = getOption(in)) {
        System.out.println();
        if (option.equalsIgnoreCase("1")) computeDistance(in);
        else if (option.equalsIgnoreCase("2")) computePointFromLocBearing(in);
        else if (option.equalsIgnoreCase("3")) boundingBoxFromPointRadius(in);
        else if (option.equalsIgnoreCase("4")) pointRadiusToPolygon(in);
        else if (option.equalsIgnoreCase("5")) mapPoints(in);
        else if (option.equalsIgnoreCase("6")) mapLine(in);
        else if (option.equalsIgnoreCase("7")) reproject(in, CRS.LLA);
        else if (option.equalsIgnoreCase("8")) reproject(in, CRS.ECEF);
        else if (option.equalsIgnoreCase("9")) reproject(in, CRS.ENU);
        else if (option.equalsIgnoreCase("a")) pointDistanceTermsEnum(in);
        else if (option.equalsIgnoreCase("b")) bboxTermsEnum(in);
        else if (option.equalsIgnoreCase("c")) closestPtOnCircle(in);
        else if (option.equalsIgnoreCase("d")) rectCrossesCircle(in);
        else if (option.equalsIgnoreCase("e")) computeMaxRadius(in);
        else if (option.equalsIgnoreCase("0")) configureOptions(in);
        else if (option.equalsIgnoreCase("q")) {
          System.out.println("Exiting!\n");
          System.exit(0);
        } else System.out.println("\n\n!!!!!!ERROR: Invalid Option!!!!!!!!!\n");
      }
    }
    catch (Exception e) {}
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

  public static void computeMaxRadius(Scanner in) {
    double[][] pt = getPoints(in, 1);
    double[][] ptOpposite = new double[][] { {(180.0 + pt[0][LON_INDEX]) % 360, pt[0][LAT_INDEX]} };
    System.out.println(" Point opposite: " + ptOpposite[0][LON_INDEX] + ", " + ptOpposite[0][LAT_INDEX]);
    System.out.println("   Haversine: " + SloppyMath.haversin(pt[0][LAT_INDEX], pt[0][LON_INDEX], ptOpposite[0][LAT_INDEX], ptOpposite[0][LON_INDEX]));
    System.out.println("   Vincenty : " + GeoDistanceUtils.vincentyDistance(pt[0][LON_INDEX], pt[0][LAT_INDEX], ptOpposite[0][LON_INDEX], ptOpposite[0][LAT_INDEX]));
  }

  public static void configureOptions(Scanner in) {
    String option;
    while (true) {
      System.out.println("********************");
      System.out.println("*      Settings    *");
      System.out.println("********************");
      System.out.println("* a. Set Map Host  *");
      System.out.println("* b. Set Map Port  *");
      System.out.println("* q. EXIT SETTINGS *");
      System.out.println("********************");
      System.out.print("Enter option: ");
      option = in.next();

      if (option.equalsIgnoreCase("a")) {
        System.out.print(" Enter Map Host: ");
        mapPoster.setHost(in.next());
      } else if (option.equalsIgnoreCase("b")) {
        System.out.print(" Enter Map Port: ");
        mapPoster.setPort(in.next());
      } else if (option.equalsIgnoreCase("q")) {
        return;
      } else {
        System.out.println("\n\n!!!!!!ERROR: Invalid Option!!!!!!!!!\n");
      }
    }
  }
}
