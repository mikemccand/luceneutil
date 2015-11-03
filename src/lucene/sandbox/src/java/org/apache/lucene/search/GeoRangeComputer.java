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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.document.GeoPointField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.GeoDistanceUtils;
import org.apache.lucene.util.GeoRect;
import org.apache.lucene.util.GeoUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.SloppyMath;

/**
 * A validation utility for GeoTermsEnum, computes the ranges and provides utilities to check if a reduced resolution
 * point falls within the computed ranges.
 */
public class GeoRangeComputer {
//  // distance
//  protected double cntrLon;
//  protected double cntrLat;
//  protected double radius;

//  // bbox
//  public double minLon;
//  public double minLat;
//  public double maxLon;
//  public double maxLat;

//  // polygon
//  public double[] x;
//  public double[] y;

  private GeoRangeQueryEnum termEnum;

  private static short DETAIL_LEVEL;
  private static short MAX_SHIFT;
  private boolean containSearch = false;

  public RangeSet rset = new RangeSet();
  public TreeSet hset = new TreeSet<Long>();

  private final TreeMap<Integer, AtomicInteger> indexMap = new TreeMap<>();
  public final LinkedList<Range> rangeBounds = new LinkedList<>();

//  private RangeFile rangeFile;

  private static double LOG2 = StrictMath.log(2);

  GeoRangeComputer(final GeoRangeQueryEnum queryEnum) {
    try {
      this.termEnum = queryEnum;
      this.MAX_SHIFT = queryEnum.computeMaxShift();
      this.DETAIL_LEVEL = (short)(((GeoUtils.BITS<<1)-this.MAX_SHIFT)/2);

//      this.rangeFile = new RangeFile("./ranges.geojson", minLon, minLat, maxLon, maxLat);
      computeRange(0L, (short) (((GeoUtils.BITS) << 1) - 1));
      assert rangeBounds.isEmpty() == false;
//      rangeFile.finish();
      Collections.sort(rangeBounds);
//      rangeFile.close();
      System.out.println("Created " + rangeBounds.size() + " ranges");
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * entry point for recursively computing ranges
   */
  private final void computeRange(long term, final short shift) {
    final long split = term | (0x1L<<shift);
    assert shift < 64;
    final long upperMax;
    if (shift < 63) {
      upperMax = term | ((1L << (shift+1))-1);
    } else {
      upperMax = 0xffffffffffffffffL;
    }
    final long lowerMax = split-1;

    relateAndRecurse(term, lowerMax, shift);
    relateAndRecurse(split, upperMax, shift);
  }

  /**
   * recurse to higher level precision cells to find ranges along the space-filling curve that fall within the
   * query box
   *
   * @param start starting value on the space-filling curve for a cell at a given res
   * @param end ending value on the space-filling curve for a cell at a given res
   * @param res spatial res represented as a bit shift (MSB is lower res)
   * @return
   */
  private void relateAndRecurse(final long start, final long end, final short res) {
    final double minLon = GeoUtils.mortonUnhashLon(start);
    final double minLat = GeoUtils.mortonUnhashLat(start);
    final double maxLon = GeoUtils.mortonUnhashLon(end);
    final double maxLat = GeoUtils.mortonUnhashLat(end);

    final short level = (short)((GeoUtils.BITS<<1)-res>>>1);

    final boolean within = res % GeoPointField.PRECISION_STEP == 0 && termEnum.cellWithin(minLon, minLat, maxLon, maxLat);
//    final boolean crosses = cellCrosses(minLon, minLat, maxLon, maxLat);
    final boolean crosses = termEnum.cellIntersectsShape(minLon, minLat, maxLon, maxLat);
    if (within || (level == DETAIL_LEVEL && crosses)) {
      final short nextRes = (short)(res-1);
      if (nextRes % GeoPointField.PRECISION_STEP == 0) {
        long nextStart = start|(1L<<nextRes);
        addRange(start, nextStart-1, level, nextRes, !within);
        addRange(nextStart, nextStart|((1L<<nextRes)-1), level, nextRes, !within);
      } else {
        addRange(start, end, level, res, !within);
      }
    } else if (level < DETAIL_LEVEL &&  termEnum.cellIntersectsMBR(minLon, minLat, maxLon, maxLat)) {
        computeRange(start, (short) (res - 1));
    }
  }

  private void addRange(long start, long end, short level, short res, boolean boundary) {
    Range r;
    rset.add(r = new Range(start, end, res, level, boundary));
    hset.add(start);
    rangeBounds.add(r);
    // add the range to the range file
//    rangeFile.append(r);
    if (indexMap.containsKey((int)level)) {
      (indexMap.get((int) level)).incrementAndGet();
    } else {
      indexMap.put((int)(level), new AtomicInteger(1));
    }
  }

  public static GeoRangeComputer[] pointRadius(final double centerLon, final double centerLat, final double radius) {
    GeoRect bbox = GeoUtils.circleToBBox(centerLon, centerLat, radius);
    if (bbox.maxLon < bbox.minLon) {
      return new GeoRangeComputer[] {
        new GeoRangeComputer(new GeoRangeQueryEnum.Distance(new GeoRect(GeoUtils.MIN_LON_INCL, bbox.maxLon, bbox.minLat, bbox.maxLat), centerLon, centerLat, radius)),
        new GeoRangeComputer(new GeoRangeQueryEnum.Distance(new GeoRect(bbox.minLon, GeoUtils.MAX_LON_INCL, bbox.minLat, bbox.maxLat), centerLon, centerLat, radius))
      };
    }

    return new GeoRangeComputer[] { new GeoRangeComputer(new GeoRangeQueryEnum.Distance(bbox, centerLon, centerLat, radius)) };
  }

  public static GeoRangeComputer[] bbox(final double minLon, final double minLat, final double maxLon, final double maxLat) {
    if (maxLon < minLon) {
      return new GeoRangeComputer[] {
          new GeoRangeComputer(new GeoRangeQueryEnum.BBox(-180.0, minLat, maxLon, maxLat)),
          new GeoRangeComputer(new GeoRangeQueryEnum.BBox(minLon, minLat, 180.0, maxLat))
      };
    }
    return new GeoRangeComputer[] { new GeoRangeComputer(new GeoRangeQueryEnum.BBox(minLon, minLat, maxLon, maxLat)) };
  }

  public static GeoRangeComputer[] polygon(final double[] x, final double[] y) {
    GeoRect bbox = GeoUtils.polyToBBox(x, y);
//    if (bbox.maxLon < bbox.minLon) {
//      return new GeoRangeComputer[] {
//          new GeoRangeComputer(new GeoRangeQueryEnum.Polygon(new GeoRect(GeoUtils.MIN_LON_INCL, bbox.maxLon, bbox.minLat, bbox.maxLat), x, y)),
//          new GeoRangeComputer(new GeoRangeQueryEnum.Polygon(new GeoRect(bbox.minLon, GeoUtils.MAX_LON_INCL, bbox.minLat, bbox.maxLat), x, y))
//      };

    return new GeoRangeComputer[] { new GeoRangeComputer(new GeoRangeQueryEnum.Polygon(bbox, x, y)) };
  }

  public GeoRect mbr() {
    return termEnum.mbr;
  }

  public void contains(final double lon, final double lat) {
    // loop through all resolutions of lon/lat
    boolean contained = false;
    for (int i=63; i>=MAX_SHIFT; i-=GeoPointField.PRECISION_STEP) {
      BytesRefBuilder brb = new BytesRefBuilder();
      NumericUtils.longToPrefixCoded(GeoUtils.mortonHash(lon, lat), i, brb);
      BytesRef br = brb.get();
      System.out.println(br);
      final long reducedHash = NumericUtils.prefixCodedToLong(br);
      if (hset.contains(reducedHash)) {
        contained = true;
        System.out.println("CONTAINED at resolution " + i + " with hash " + reducedHash);
      }
//      System.out.println(i + ": " + GeoUtils.geoTermToString(reducedHash) + " " + contains(br));
    }
    if (contained == false) {
      System.out.println("NOT CONTAINED");
    }
  }

  public static void printReducedResAndDistance(final double lon, final double lat, final double centerLon, final double centerLat) {
    double[] closestPoint = new double[2];
    for (int i=63; i>=GeoPointField.PRECISION_STEP*MAX_SHIFT; i-=9) {
      BytesRefBuilder brb = new BytesRefBuilder();
      NumericUtils.longToPrefixCoded(GeoUtils.mortonHash(lon, lat), i, brb);
      final long hash = NumericUtils.prefixCodedToLong(brb.get());
      final double minX = GeoUtils.mortonUnhashLon(hash);
      final double minY = GeoUtils.mortonUnhashLat(hash);
      final long hashUpper = hash | ((1L<<i)-1);
      final double maxX = GeoUtils.mortonUnhashLon(hashUpper);
      final double maxY = GeoUtils.mortonUnhashLat(hashUpper);
      GeoDistanceUtils.closestPointOnBBox(minX, minY, maxX, maxY, centerLon, centerLat, closestPoint);
      final double distance = SloppyMath.haversin(closestPoint[1], closestPoint[0], centerLat, centerLon)*1000.0;
      System.out.println(i + ": " + closestPoint[0] + ", " + closestPoint[1] + " [" + distance + "]");
    }
  }

  public Range contains(BytesRef b) {
    containSearch = true;
    return rset.contains(b);
  }

  public void printRangesForLevel(short level) {
    if (!indexMap.containsKey((int)(level)))
      return;

    final int count = indexMap.get((int)(level)).get();
    int offset = 0;
    for (Integer i : indexMap.descendingKeySet()) {
      if (i == (int)(level)) break;
      offset += indexMap.get(i).get();
    }

    final ListIterator<Range> iter = rangeBounds.listIterator(offset);
    for (int i=0; i<count; ++i)
      System.out.println(iter.next());
  }

  private final class RangeSet extends TreeSet<Range> {
    public RangeSet() {
      super();
    }

    public Range ceiling(BytesRef term) {
      if (term == null)
        return first();
      final Range termRange = new Range(term);
      return super.ceiling(termRange);
    }

    public Range floor(BytesRef term) {
      if (term == null)
        return first();
      final Range termRange = new Range(term);
      return super.floor(termRange);
    }

    public Range contains(BytesRef b) {
      Range r = new Range(b);
      return (super.contains(r)) ? super.floor(r) : null;
    }
  }

  /**
   * Range Class
   */
  public final class Range implements Comparable<Range> {
    final BytesRef lower;
    final long lowerVal;
    final long upperVal;
    BytesRef cell;
    BytesRef upper;
    final short level;
    final boolean boundary;


    Range(final BytesRef term) {
      this.lower = term;
      this.boundary = false;
      this.lowerVal = NumericUtils.prefixCodedToLong(term);
      this.upperVal = -1;
      this.level = -1;
    }

    Range(final long lower, final long upper, final short res, final short level, final boolean boundary) {
      this.level = level;
      this.boundary = boundary;

      BytesRefBuilder brb = new BytesRefBuilder();
      NumericUtils.longToPrefixCodedBytes((this.lowerVal = lower), boundary ? 0 : res, brb);
      this.lower = brb.get();
      NumericUtils.longToPrefixCodedBytes((this.upperVal = upper), boundary ? 0 : res, (brb = new BytesRefBuilder()));
      this.upper = brb.get();
      NumericUtils.longToPrefixCoded(this.lowerVal, res, (brb = new BytesRefBuilder()));
      this.cell = brb.get();

    }

    public void merge(final Range r) {
      this.upper = r.upper;
    }

    @Override
    public int compareTo(Range other) {
      if (containSearch && other.level == DETAIL_LEVEL && other.boundary == true) {
        if (this.lowerVal > other.upperVal)
          return 1;
        else if (this.lowerVal < other.lowerVal)
          return -1;
        else return 0;
      }
      return this.lower.compareTo(other.lower);
    }

    @Override
    public boolean equals(Object other) {
      return this.lower.compareTo(((Range)(other)).lower) == 0;
    }
    @Override
    public final int hashCode() {
      return this.lower.hashCode();
    }

    @Override
    public String toString() {
      return GeoUtils.geoTermToString(lowerVal) + " " + lowerVal + " " +  cell.toString() + (((this.boundary) ? " cellCrosses " : " within ") + level);
    }

    public String toGeoJson() {

      final double llLat = GeoUtils.mortonUnhashLat(lowerVal);
      final double llLon = GeoUtils.mortonUnhashLon(lowerVal);
      final double urLat = GeoUtils.mortonUnhashLat(upperVal);
      final double urLon = GeoUtils.mortonUnhashLon(upperVal);
      final String coords = "\"coordinates\":[[[" + llLon + "," + llLat + "],[" + urLon + "," + llLat + "],[" + urLon + "," + urLat
          + "],[" + llLon + "," + urLat + "],[" + llLon + "," + llLat + "]]]";

      return "{\"type\":\"Feature\", \"properties\":{\"color\": " + ((boundary) ? "\"#ff0000\"" : "\"#0000ff\"")
          + ", \"fillColor\":" + ((boundary) ? "\"#ff0000\"" : "\"#0000ff\"") + "}, \"geometry\":{\"type\":\"Polygon\","
          + coords + "}},";
    }
  }

  /**
   * RangeFile class
   */
  public final class RangeFile extends FileOutputStream {
    private PrintStream stream;

    public RangeFile(String filename, final double minLon, final double minLat, final double maxLon, final double maxLat) throws FileNotFoundException {
      super(filename, true);
      stream = new PrintStream(this);
      stream.println("{\"type\":\"FeatureCollection\",\"features\":[");

      String coords = "\"coordinates\":[[[" + minLon + "," + minLat + "],[" + maxLon + "," + minLat + "],[" + maxLon
          + "," + maxLat + "],[" + minLon + "," + maxLat + "],[" + minLon + "," + minLat + "]]]";

      stream.println("{\"type\":\"Feature\", \"properties\":{}, \"geometry\":{\"type\":\"Polygon\","
          + coords + "}},");
    }

    public void append(Range range) {
      stream.println(range.toGeoJson());
    }

    public void finish() throws IOException {
      stream.println("]}");
      this.close();
    }
  }

  /**
   * Computes the range detail level as a function of the bounding box size. This is currently computed as
   */
  private static short computeDetailLevel(final double minLon, final double minLat, final double maxLon, final double maxLat) {
    short level = (short)(StrictMath.log(180.0 / (StrictMath.min(maxLon - minLon, maxLat - minLat) * 0.05)) / LOG2);
    return (short)StrictMath.max(9, StrictMath.min((level - (level % GeoPointField.PRECISION_STEP)), 18));
  }

  public static void main(String[] args) {
    long hash = GeoUtils.mortonHash(-95.95683785493188, 35.30495325717168);
    System.out.println(GeoUtils.mortonUnhashLon(hash) + ", " + GeoUtils.mortonUnhashLat(hash));

    System.exit(1);

    // bbox search
    GeoRangeComputer.bbox(-96.30716303968933,35.304955180599926, -95.78797371671509,35.55537284046483);

    // point distance
// world    GeoRangeComputer.pointRadius(-96.5795, 38.4282, 1800000);
    //GeoRangeComputer.pointRadius(-26.941900485389525, 10.247015102383324, 3586.9578460566954);

    double lon1 = -150.05159354245998; //30.491451638654798;
    double lat1 = -5.053155934333239; // -78.0656278460313;
    double lon2 = -150.8746243143247; //30.46092393997241;
    double lat2 = -4.606658343297533; // -79.27953520869464;
    double radius = 102888.67145438067;

//    GeoRangeComputer.pointRadius(lon1, lat1, radius);

//    GeoRangeComputer.pointRadius(-179.9538113027811, 32.94823588839368, 120000);
//    GeoRangeComputer grc = GeoRangeComputer.pointRadius(-36.16229612801503, -64.0072525758825, 2706142.9486930687);
//    grc.contains(-7.49075050731588, -87.61617663442154);

    double[] center = {177.40601424065187, -3.4950223628202366};  //{-13.322542850253924,-33.757485737285414};
    double distance = 7943468.307706215;//5481987.999944289;
    double[] contains = {135.2448693494081, 30.367465639421866}; //{-74.74051110705088, -71.76887356867866};

    GeoRangeComputer[] grc = GeoRangeComputer.pointRadius(center[0], center[1], distance);
    for (GeoRangeComputer c : grc) {
      c.contains(contains[0], contains[1]);
    }

//    BoundingBox bbox = GeoRangeComputer.computeBBox(center[0], center[1], distance);
//    System.out.println(bbox);
//
//    GeoRangeComputer.printReducedResAndDistance(contains[0], contains[1], center[0], center[1]);


//    GeoRangeComputer.pointRadius(139.73093327050339, -75.78400372037868,  931000);

//    GeoRangeComputer comp = GeoRangeComputer.pointRadius(-73.998776, 40.720611,1);
//    comp.printRangesForLevel((short)16);


//    double level = StrictMath.log((180.0d/(lon2 - lon1)))/StrictMath.log(2);
//
////    System.out.println(computeDetailLevel(lon1, lat2, lon2, lat1));
//    System.out.println(GeoDistanceUtils.vincentyDistance(lon1, lat1, lon2, lat2));
//    System.out.println(SloppyMath.haversin(center[1], center[0], contains[1], contains[0])*1000);
//
//    System.out.println(SloppyMath.haversin(17.706278258666824, -130.45021034445304, 17.228488997673622, -130.45021034445304)*1000);
  }
}