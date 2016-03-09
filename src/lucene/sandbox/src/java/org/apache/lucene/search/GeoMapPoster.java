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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;

import org.apache.lucene.spatial.util.GeoRect;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Requires NodeJS
 *
 *  See installation instructions here: https://nodejs.org/en/download/package-manager/
 *
 *  Once installed:
 *    1. navigate to the src/javascript/{2d | 3d} directory and type 'npm install'
 *    2. npm start
 *    3. be sure to set host and port from the calculator menu (option 0) default is localhost:8080
 */
public class GeoMapPoster {
  enum GeometryType {
    Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon
  }

  // http strings
  private static final String METHOD = "send_feed_item";
  private static final String POST = "POST";
  private static final String CONTENT_TYPE = "Content-Type";
  private static final String CONTENT_LENGTH = "Content-Length";
  private static final String APPLICATION_TYPE = "application/json";

  // json static
  private static final String JSON_TYPE = "type";
  private static final String JSON_FEATURES = "features";
  private static final String JSON_FEATURE_COLLECTION = "FeatureCollection";
  private static final String JSON_FEATURE = "Feature";
  private static final String JSON_GEOMETRY = "geometry";
  private static final String JSON_COORDINATES = "coordinates";
  private static final String JSON_PROPERTIES = "properties";

  private String host;
  private String port;

  private HttpURLConnection connection;
  private BufferedWriter out;

  public GeoMapPoster(String host, String port) {
    this.host = host;
    this.port = port;
  }

  private void connect() throws IOException {
    connection = (HttpURLConnection) ((new URL("http://" + host + ":" + port + "/" + METHOD)).openConnection());
    connection.setDoOutput(true);
    connection.setRequestMethod(POST);
    connection.setRequestProperty(CONTENT_TYPE, APPLICATION_TYPE);
  }

  public void post(JSONObject json) {
    try {
      this.connect();
      connection.setRequestProperty(CONTENT_LENGTH, String.valueOf(json.toString().length()));
      this.out = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream()));
      out.write(json.toString());
      out.close();
      String resp = connection.getResponseMessage();
      System.out.println("Mapping Result: " + resp);
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println("Check that the node server is running!");
    }
  }

  public final void close() {
    try {
      this.out.close();
    } catch(Exception e) {
      System.err.println(e);
    }
  }

  public static JSONObject toJSON(Object... objects) {
    return toJSON(false, objects);
  }

  /**
   * create the geojson
   */
  public static JSONObject toJSON(boolean connect, Object... objects) {
    if (objects == null || objects.length == 0) {
      throw new IllegalArgumentException("called GeoMapPoster.toJSON with empty object");
    }
    JSONObject featureCollection = new JSONObject().put(JSON_TYPE, JSON_FEATURE_COLLECTION);

    // add objects to the feature array
    JSONArray featureArray = new JSONArray();
    if (objects instanceof double[][]) {
      double[][] points = (double[][])objects;
      boolean multi = points.length > 1;
      if (multi == true) {
        featureArray.put(newGeometry((connect) ? GeometryType.LineString : GeometryType.MultiPoint, points));
      } else {
        featureArray.put(newGeometry(GeometryType.Point, points));
      }
    } else {
      for(Object o : objects) {
        if (o instanceof GeoRect) {
          GeoRect rect = (GeoRect)o;
          final double[][] points = new double[][]{ {rect.minLon, rect.minLat}, {rect.maxLon, rect.minLat},
              {rect.maxLon, rect.maxLat}, {rect.minLon, rect.maxLat}, {rect.minLon, rect.minLat}};
          featureArray.put(newGeometry(GeometryType.Polygon, points));
        } else if (o instanceof ArrayList) {
          ArrayList<double[]> pointList = (ArrayList<double[]>) o;
          double[][] points = new double[pointList.get(0).length][2];
          for (int i = 0; i < points.length; ++i) {
            points[i][0] = pointList.get(0)[i];
            points[i][1] = pointList.get(1)[i];
          }
          featureArray.put(newGeometry(GeometryType.Polygon, points));
        }
      }
    }
    featureCollection.put(JSON_FEATURES, featureArray);

    return featureCollection;
  }

  /**
   * create a new geometry from the type and array of points
   */
  private static JSONObject newGeometry(GeometryType type, final double[][] points) {
    JSONObject feature = new JSONObject().put(JSON_TYPE, JSON_FEATURE);
    JSONObject geom = new JSONObject().put(JSON_TYPE, type).put(JSON_COORDINATES, pointsToJSONArray(points, type));

    return feature.put(JSON_PROPERTIES, new JSONObject()).put(JSON_GEOMETRY, geom);
  }

  /**
   * convert points to a json array
   */
  private static JSONArray pointsToJSONArray(final double[][] points, GeometryType type) {
    if (points.length == 1) {
      return new JSONArray().put(points[0][0]).put(points[0][1]);
    }

    JSONArray pArray = new JSONArray();
    for (int i = 0; i < points.length; ++i) {
      pArray.put(new JSONArray().put(points[i][0]).put(points[i][1]));
    }

    // polygons
    if (type != GeometryType.MultiPoint && type != GeometryType.LineString) {
      JSONArray ringArray = new JSONArray();
      return ringArray.put(pArray);
    }
    return pArray;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public void setPort(String port) {
    this.port = port;
  }
}
