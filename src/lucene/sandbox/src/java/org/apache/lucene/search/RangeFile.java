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

/**
 *
 */
public class RangeFile extends FileOutputStream {
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

  public void append(final long lowerVal, final long upperVal) {
    stream.println(Range.toGeoJson(lowerVal, upperVal));
  }

  public void finish() throws IOException {
    stream.println("]}");
    this.close();
  }
}
