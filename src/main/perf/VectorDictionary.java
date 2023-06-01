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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import org.apache.lucene.index.VectorEncoding;

public class VectorDictionary {

  private final Map<String, float[]> dict = new HashMap<>();

  public final float scale;
  public final VectorEncoding vectorEncoding;

  public final int dimension;

  /**
   * Reads a vector dictionary in the GloVe format, a text file where each line has a
   * token followed by dimension floating point numbers in text, all space-separated.
   * @param filename the dictionary file
   */
  public static VectorDictionary create(String filename, float scale, VectorEncoding encoding) throws IOException {
    return new VectorDictionary(filename, scale, encoding);
  }

  /**
   * Reads a vector dictionary stored in two files, one containing words and another binary file
   * containing floating point vectors.
   * @param wordFile a file containing one word token per line
   * @param vectorFile a file containing vectors in LE 32 bit floating point
   */
  public static VectorDictionary create(String wordFile, String vectorFile, int dimension, float scale, VectorEncoding encoding) throws IOException {
    return new VectorDictionary(wordFile, vectorFile, dimension, scale, encoding);
  }

  public int size() {
    return dict.size();
  }

  public float[] get(String key) {
    return dict.get(key);
  }

  private VectorDictionary(String filename, float scale, VectorEncoding vectorEncoding) throws IOException {
    // read a dictionary file where each line has a token and its n-dimensional vector as text:
    // <word> <f1> <f2> ... <fn>
    this.scale = scale;
    this.vectorEncoding = vectorEncoding;
    int dim = 0;
    try (BufferedReader reader = Files.newBufferedReader(Paths.get(filename), StandardCharsets.UTF_8)) {
      String line = reader.readLine();
      dim = parseLine(line);
      while ((line = reader.readLine()) != null) {
        int lineDim = parseLine(line);
        if (dim != lineDim) {
          String err = String.format("vector dimension %s is not the initial dimension: %s for line: %s", lineDim, dim, line);
          throw new IllegalStateException(err);
        }
        /*
        if (dict.size() % 10000 == 0) {
          System.out.print("loaded " + dict.size() + "\n");
        }
        */
      }
    } catch (Exception e) {
      System.err.println("An error occurred after reading " + dict.size() + " entries from " + filename);
      throw e;
    }
    dimension = dim;
    System.out.println("loaded " + dict.size());
  }

  private int parseLine(String line) {
    String[] parts = line.split(" ");
    String token = parts[0];
    if (dict.containsKey(token)) {
      throw new IllegalStateException("token " + token + " seen twice");
    }
    float[] vector = new float[parts.length - 1];
    double sum2 = 0;
    for (int i = 1; i < parts.length; i++) {
      vector[i - 1] = Float.parseFloat(parts[i]);
    }
    add(token, vector);
    return vector.length;
  }

  private void add(String token, float[] vector) {
    double norm = vectorNorm(vector);
    if (norm > 0) {
      // We want unit vectors
      vectorDiv(vector, norm);
      dict.put(token, vector);
    } else {
      System.err.println("WARN: skipping token in dictionary with zero vector: " + token);
    }
  }

  private VectorDictionary(String wordFile, String vectorFile, int dimension, float scale, VectorEncoding vectorEncoding) throws IOException {
    // read a dictionary from two files, one with a token per line and the other with little-endian fp32 vectors
    this.scale = scale;
    this.vectorEncoding = vectorEncoding;
    this.dimension = dimension;
    try (BufferedReader words = Files.newBufferedReader(Paths.get(wordFile), StandardCharsets.UTF_8);
         InputStream vectors = Files.newInputStream(Paths.get(vectorFile))) {
      String token;
      byte[] buf = new byte[dimension * Float.BYTES];
      ByteBuffer bytes = ByteBuffer.wrap(buf)
        .order(ByteOrder.LITTLE_ENDIAN);
      while ((token = words.readLine()) != null) {
        if (dict.containsKey(token)) {
          // nocommit - hack to patch up some bad data I created
          continue;
          // throw new IllegalStateException("token " + token + " seen twice");
        }
        int nread = vectors.read(buf);
        if (nread < buf.length) {
          throw new IllegalStateException("EOF while reading vectors after " + dict.size() + " entries");
        }
        float[] vec = new float[dimension];
        bytes.asFloatBuffer().get(vec);
        add(token, vec);
      }
    } catch (Exception e) {
      System.err.println("An error occurred after reading " + dict.size() + " entries");
      throw e;
    }
    System.out.println("loaded " + dict.size());
  }

  public float[] computeTextVector(String text) {
    float[] dvec = new float[dimension];
    int count = 0;
    for (String token : tokenize(text)) {
      float[] tvec = dict.get(token);
      if (tvec != null) {
        if (Math.abs(vectorNorm(tvec) - 1) > 1e-5) {
          throw new IllegalStateException("Vector is not unitary for token '" + token + "'" +
                                          " norm=" + vectorNorm(tvec));
        }
        vectorAdd(dvec, tvec);
        count++;
      }
    }
    switch (vectorEncoding) {
      case BYTE -> {
        vectorDiv(dvec, vectorNorm(dvec) / scale);
        vectorClip(dvec, -128, 127);
      }
      case FLOAT32 -> {
        vectorDiv(dvec, vectorNorm(dvec));
      }
    }
    return dvec;
  }

  public static double vectorNorm(float[] x) {
    double sum2 = 0;
    for (float f : x) {
      sum2 += f * f;
    }
    return Math.sqrt(sum2);
  }

  static void vectorAdd(float[] x, float[] y) {
    assert x.length == y.length;
    for (int i = 0; i < x.length; i++) {
      x[i] += y[i];
    }
  }

  static void vectorDiv(float[] v, double x) {
    if (x == 0) {
      return;
    }
    for (int i = 0; i < v.length; i++) {
      v[i] /= x;
    }
  }

  static void vectorClip(float[] v, float min, float max) {
    for (int i = 0; i < v.length; i++) {
      v[i] = (float) Math.min(max, Math.max(min, v[i]));
    }
  }

  // tokenize on white space, most ascii punctuation, preserving -_,., then lower case.  Not very
  // sophisticated, but enough for performance testing on English text. Should we use StandardTokenizer?
  static Iterable<String> tokenize(String document) {
    List<String> tokens = new ArrayList<>();
    for (String t : document.split("[\\]\\[\\\\:\"'?/<> \t~`!@#$%^&*\\(\\)+={}]+")) {
      if (t.length() > 0) {
        t = t.toLowerCase();
        tokens.add(t);
      }
    }
    return tokens;
  }

  public Set<Map.Entry<String, float[]>> entrySet() {
    return dict.entrySet();
  }

}
