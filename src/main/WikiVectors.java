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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import perf.VectorDictionary;

/**
 * Precompute per-document semantic vectors as the average of their word vectors.  This tool is used
 * to generate document vectors from wiki line documents and a downloaded word embedding dictionary,
 * as a precursor for indexing vectors in benchmark runs. It's provided for "offline" (manual) use,
 * and doesn't factor into benchmark execution.
 */
public class WikiVectors<T> {

  private final VectorDictionary<?> dict;

  float scale;
  int dimension;

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      usage();
    }
    float scale = 0;
    List<String> argList = List.of(args);
    if (argList.get(0).equals("--test")) {
      test();
      return;
    }
    if (args.length < 3) {
      usage();
    }
    if (args[0].equals("-scale")) {
      scale = Float.parseFloat(args[1]);
      argList = argList.subList(2, argList.size());
    }
    if (argList.size() != 3) {
      usage();
    }
    WikiVectors wikiVectors = new WikiVectors(argList.get(0), scale);
    try (OutputStream out = Files.newOutputStream(Paths.get(argList.get(2)))) {
      wikiVectors.computeVectors(argList.get(1), out);
    }
  }

  static void usage() {
      System.err.println("usage: WikiVectors --test | [-scale X] <vectorDictionary> <lineDocs> <docVectorOutput>");
      System.exit(-1);
  }

  WikiVectors(String dictFileName, float scale) throws IOException {
    this.scale = scale;
    if (scale == 0) {
      dict = VectorDictionary.create(dictFileName);
    } else {
      dict = VectorDictionary.create(dictFileName, scale);
    }
  }

  void computeVectors(String lineDocFile, OutputStream out) throws IOException {
    if (scale == 0) {
      computeFloatVectors(lineDocFile, out);
    } else {
      computeByteVectors(lineDocFile, out);
    }
  }

  void computeFloatVectors(String lineDocFile, OutputStream out) throws IOException {
    int count = 0;
    CharsetDecoder dec=StandardCharsets.UTF_8.newDecoder()
      .onMalformedInput(CodingErrorAction.REPLACE); // replace invalid input with the UTF8 replacement character
    try (Reader r = Channels.newReader(FileChannel.open(Paths.get(lineDocFile)), dec, -1);
         BufferedReader in = new BufferedReader(r)) {
      String lineDoc;
      byte[] buffer = new byte[dict.dimension * Float.BYTES];
      ByteBuffer bbuf = ByteBuffer.wrap(buffer)
        .order(ByteOrder.LITTLE_ENDIAN);
      FloatBuffer fbuf = bbuf.asFloatBuffer();
      while ((lineDoc = in.readLine()) != null) {
        float[] dvec = (float[]) dict.computeTextVector(lineDoc);
        fbuf.position(0);
        fbuf.put(dvec);
        out.write(buffer);
        if (++count % 10000 == 0) {
          System.out.print("wrote " + count + "\n");
        }
      }
      System.out.println("wrote " + count);
    } catch (IOException e) {
      System.err.println("An error occurred on line " + (count + 1));
      throw e;
    }
  }

  void computeByteVectors(String lineDocFile, OutputStream out) throws IOException {
    int count = 0;
    CharsetDecoder dec=StandardCharsets.UTF_8.newDecoder()
      .onMalformedInput(CodingErrorAction.REPLACE); // replace invalid input with the UTF8 replacement character
    try (Reader r = Channels.newReader(FileChannel.open(Paths.get(lineDocFile)), dec, -1);
         BufferedReader in = new BufferedReader(r)) {
      String lineDoc;
      byte[] bvec = new byte[dict.dimension];
      while ((lineDoc = in.readLine()) != null) {
        float[] vec = dict.computeTextVector(lineDoc);
        for (int i = 0; i < vec.length; i++) {
          bvec[i] = (byte) vec[i];
        }
        out.write(bvec);
        if (++count % 10000 == 0) {
          System.out.print("wrote " + count + "\n");
        }
      }
      System.out.println("wrote " + count);
    } catch (IOException e) {
      System.err.println("An error occurred on line " + (count + 1));
      throw e;
    }
  }

  //-------------------------------------------------------------------------------//
  //                                                                               //
  //                               Test Methods                                    //
  //                                                                               //
  //-------------------------------------------------------------------------------//

  static void test() throws IOException {
    testUnscaled();
    System.out.println("testUnscaled: ok");
    testScaled();
    System.out.println("testScaled: ok");
  }

  static void testUnscaled() throws IOException {
    WikiVectors wikiVectors = new WikiVectors("resources/test-dict.txt", 0);
    assertEquals(100, wikiVectors.dict.dimension);
    assertEquals(100, wikiVectors.dict.get("many").length);
    assertEquals(0f, wikiVectors.dict.scale);
    assertEquals(4, wikiVectors.dict.size());
    // vectors were normalized
    assertClose(1f, (float) VectorDictionary.vectorNorm(wikiVectors.dict.get("publisher")));
    assertClose(1f, (float) VectorDictionary.vectorNorm(wikiVectors.dict.get("backstory")));
    assertClose(1f, (float) VectorDictionary.vectorNorm(wikiVectors.dict.get("many")));
    // compare ratios since these are invariant under scaling, and we normalized the input
    assertClose(-0.056504f / 0.16064f, wikiVectors.dict.get("publisher")[0] / wikiVectors.dict.get("publisher")[99]);
    assertClose(-0.32914f / 0.59499f, wikiVectors.dict.get("many")[0] / wikiVectors.dict.get("many")[99]);
    assertThat(wikiVectors.dict.get("geografia") == null);
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      wikiVectors.computeVectors("resources/test-tasks.txt", out);
      byte[] buf = out.toByteArray();
      FloatBuffer floats = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
      float[] vec = new float[100];
      // vector for "publisher backstory"
      floats.get(vec);
      assertClose(vec[0] / vec[99],
                  (wikiVectors.dict.get("publisher")[0] + wikiVectors.dict.get("backstory")[0])
                  /
                  (wikiVectors.dict.get("publisher")[99] + wikiVectors.dict.get("backstory")[99]));
      // vector for "many geografia" - geografia is not there
      floats.get(vec);
      assertClose(vec[0], wikiVectors.dict.get("many")[0]);
      assertClose(vec[99], wikiVectors.dict.get("many")[99]);
    }
  }

  static void testScaled() throws IOException {
    float scale = 128f;
    WikiVectors wikiVectors = new WikiVectors("resources/test-dict.txt", scale);
    assertEquals(100, wikiVectors.dict.dimension);
    assertEquals(100, wikiVectors.dict.get("many").length);
    assertEquals(scale, wikiVectors.dict.scale);
    assertEquals(4, wikiVectors.dict.size());
    // vectors were normalized
    assertClose(1f, (float) VectorDictionary.vectorNorm(wikiVectors.dict.get("publisher")));
    assertClose(1f, (float) VectorDictionary.vectorNorm(wikiVectors.dict.get("backstory")));
    assertClose(1f, (float) VectorDictionary.vectorNorm(wikiVectors.dict.get("many")));
    // compare ratios since these are invariant under scaling, and we normalized the input
    assertClose(-0.056504f / 0.16064f, wikiVectors.dict.get("publisher")[0] / wikiVectors.dict.get("publisher")[99]);
    assertClose(-0.32914f / 0.59499f, wikiVectors.dict.get("many")[0] / wikiVectors.dict.get("many")[99]);
    assertThat(wikiVectors.dict.get("geografia") == null);
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      wikiVectors.computeVectors("resources/test-tasks.txt", out);
      byte[] buf = out.toByteArray();
      // we wrote two 100-dimensional vectors
      assertEquals(200, buf.length);
      // vector for "publisher backstory"
      assertClose(buf[0] / (float) buf[99],
                  (wikiVectors.dict.get("publisher")[0] + wikiVectors.dict.get("backstory")[0])
                  /
                  (float) (wikiVectors.dict.get("publisher")[99] + wikiVectors.dict.get("backstory")[99]),
                  1/128f);
      // vector for "many geografia" - geografia is not there
      assertEquals(buf[100], scaleToByte(wikiVectors.dict.get("many")[0], scale));
      assertEquals(buf[199], scaleToByte(wikiVectors.dict.get("many")[99], scale));
    }
  }

  private static byte scaleToByte(float f, float scale) {
    return (byte) Math.min(Math.max(f * scale, -128), 127);
  }

  private static void assertClose(float a, float b) {
    assertClose(a, b, 1e-5f);
  }

  private static void assertClose(float a, float b, float tolerance) {
    if (Math.abs(a - b) > tolerance) {
      fail(a + " is not close to " + b);
    }
  }

  private static void assertEquals(Object a, Object b) {
    if (!a.equals(b)) {
      fail(a + " is not equal to " + b);
    }
  }

  private static void assertThat(boolean condition) {
    if (!condition) {
      fail("condition was not true");
    }
  }

  private static void fail(String message) {
    throw new AssertionError(message);
  }
}
