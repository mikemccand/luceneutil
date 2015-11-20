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

import org.apache.lucene.util.GeoRect;

/**
 * A simple command line utility for testing {@code org.apache.lucene.util.}
 */
public class GeoCalculator {
  protected static GeoMapPoster mapPoster = new GeoMapPoster("localhost", "3000");

  protected static int ALT_INDEX = 2;
  protected static int LAT_INDEX = 1;
  protected static int LON_INDEX = 0;

  enum DistanceMethod {
    HAVERSINE, VINCENTY, KARNEY
  }

  enum CRS {
    LLA, ECEF, ENU
  }

  protected static double[][] getPoints(Scanner in, int numPts) {
    return getPoints(in, CRS.LLA, numPts, 2);
  }

  protected static double[][] getPoints(Scanner in, CRS crs, int numPts, int dim) {
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

  protected static double getRadius(Scanner in) {
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

  public static double[][] getPoly(Scanner in) {
    System.out.print("Enter number of points: ");
    return getPoints(in, in.nextInt());
  }

  /**
   * transposes from a series of points represented as [numPts][dimension] to a lon/lat double array
   */
  protected static double[][] transposePoly(double[][] poly) {
    final int numPts = poly.length;
    if (numPts == 0) return null;
    final int dim = poly[0].length;
    double[][] polyTrans = new double[dim][numPts];
    for (int i=0; i<numPts; ++i) {
      polyTrans[LON_INDEX][i] = poly[i][LON_INDEX];
      polyTrans[LAT_INDEX][i] = poly[i][LAT_INDEX];
      if (dim > 2) {
        for (int j=2; j<dim; ++j) {
          polyTrans[j][i] = poly[i][j];
        }
      }
    }
    return polyTrans;
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
    System.out.println("/ b. Compute BBoxQuery TermsEnum                  c. Compute PolygonQuery TermsEnum  /");
    System.out.println("/ d. Closest point on rectangle                   e. Does Rect Cross Circle          /");
    System.out.println("/ f. Is rect within circle                        g. Does Rect Cross Poly            /");
    System.out.println("/ h. Compute max radius                           i. Display Ranges for Point        /");
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
      Scanner in = new Scanner(System.in).useDelimiter("\n");
      for (String option = getOption(in); ; option = getOption(in)) {
        System.out.println();
        if (option.equalsIgnoreCase("1")) GeoProcessor.computeDistance(in);
        else if (option.equalsIgnoreCase("2")) GeoProcessor.computePointFromLocBearing(in);
        else if (option.equalsIgnoreCase("3")) GeoProcessor.boundingBoxFromPointRadius(in);
        else if (option.equalsIgnoreCase("4")) GeoProcessor.pointRadiusToPolygon(in);
        else if (option.equalsIgnoreCase("5")) GeoProcessor.mapPoints(in);
        else if (option.equalsIgnoreCase("6")) GeoProcessor.mapLine(in);
        else if (option.equalsIgnoreCase("7")) GeoProcessor.reproject(in, CRS.LLA);
        else if (option.equalsIgnoreCase("8")) GeoProcessor.reproject(in, CRS.ECEF);
        else if (option.equalsIgnoreCase("9")) GeoProcessor.reproject(in, CRS.ENU);
        else if (option.equalsIgnoreCase("a")) GeoProcessor.pointDistanceTermsEnum(in);
        else if (option.equalsIgnoreCase("b")) GeoProcessor.bboxTermsEnum(in);
        else if (option.equalsIgnoreCase("c")) GeoProcessor.polygonTermsEnum(in);
        else if (option.equalsIgnoreCase("d")) GeoProcessor.closestPtOnCircle(in);
        else if (option.equalsIgnoreCase("e")) GeoProcessor.rectCrossesCircle(in);
        else if (option.equalsIgnoreCase("f")) GeoProcessor.rectWithinCircle(in);
        else if (option.equalsIgnoreCase("g")) GeoProcessor.rectPolyRelation(in);
        else if (option.equalsIgnoreCase("h")) GeoProcessor.computeMaxRadius(in);
        else if (option.equalsIgnoreCase("i")) GeoProcessor.displayPointRanges(in);
        else if (option.equalsIgnoreCase("0")) configureOptions(in);
        else if (option.equalsIgnoreCase("q")) {
          System.out.println("Exiting!\n");
          System.exit(0);
        } else System.out.println("\n\n!!!!!!ERROR: Invalid Option!!!!!!!!!\n");
      }
    }
    catch (Exception e) {}
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
