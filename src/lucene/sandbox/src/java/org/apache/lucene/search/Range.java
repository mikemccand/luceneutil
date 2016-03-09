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
import org.apache.lucene.spatial.util.GeoEncodingUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LegacyNumericUtils;

/**
 * Internal class to represent a range along the space filling curve
 */
public final class Range implements Comparable<Range> {
  final long start;
  short shift;

  public Range(final long start, final short res) {
    this.start = start;
    this.shift = res;
  }

  public BytesRef asTerm() {
    BytesRefBuilder brb = new BytesRefBuilder();
    LegacyNumericUtils.longToPrefixCoded(this.start, this.shift, brb);
    return brb.get();
  }

  @Override
  public int compareTo(Range other) {
    return Long.compare(this.start, other.start);
  }

  public boolean merge(Range other) {
    if (this.shift == other.shift) {
      final long prefixMask = ~((1L<<(this.shift+1))-1);
      if ((this.start & prefixMask) == (other.start & prefixMask)) {
        this.shift-=1;
        return true;
      }
    }
    return false;
  }

  public static String toGeoJson(final long lowerVal, final long upperVal) {
    final double llLat = GeoEncodingUtils.mortonUnhashLat(lowerVal);
    final double llLon = GeoEncodingUtils.mortonUnhashLon(lowerVal);
    final double urLat = GeoEncodingUtils.mortonUnhashLat(upperVal);
    final double urLon = GeoEncodingUtils.mortonUnhashLon(upperVal);
    final String coords = "\"coordinates\":[[[" + llLon + "," + llLat + "],[" + urLon + "," + llLat + "],[" + urLon + "," + urLat
        + "],[" + llLon + "," + urLat + "],[" + llLon + "," + llLat + "]]]";

    return "{\"type\":\"Feature\", \"properties\":{}, \"geometry\":{\"type\":\"Polygon\","
        + coords + "}},";
  }
}
