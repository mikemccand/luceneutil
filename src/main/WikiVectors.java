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
import java.util.List;

import perf.VectorDictionary;

/**
 * Precompute per-document semantic vectors as the average of their word vectors.  This tool is used
 * to generate document vectors from wiki line documents and a downloaded word embedding dictionary,
 * as a precursor for indexing vectors in benchmark runs. It's provided for "offline" (manual) use,
 * and doesn't factor into benchmark execution.
 */
public class WikiVectors {

  private final VectorDictionary dict;

  int dimension;

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      usage();
    }
    float scale = 1;
    List<String> argList = List.of(args);
    if (args[0].equals("-scale")) {
      scale = Float.parseFloat(args[1]);
      argList = argList.subList(2, argList.size());
    }
    if (argList.size() != 3) {
      usage();
    }
    WikiVectors wv = new WikiVectors(new VectorDictionary(argList.get(0), scale));
    wv.computeVectors(argList.get(1), argList.get(2));
  }

  static void usage() {
      System.err.println("usage: WikiVectors [-scale X] <vectorDictionary> <lineDocs> <docVectorOutput>");
      System.exit(-1);
  }

  WikiVectors(VectorDictionary dict) {
    this.dict = dict;
  }

  void computeVectors(String lineDocFile, String outputFile) throws IOException {
    int count = 0;
    CharsetDecoder dec=StandardCharsets.UTF_8.newDecoder()
      .onMalformedInput(CodingErrorAction.REPLACE); // replace invalid input with the UTF8 replacement character
    try (OutputStream out = Files.newOutputStream(Paths.get(outputFile));
         Reader r = Channels.newReader(FileChannel.open(Paths.get(lineDocFile)), dec, -1);
         BufferedReader in = new BufferedReader(r)) {
      String lineDoc;
      byte[] buffer = new byte[dict.dimension * Float.BYTES];
      ByteBuffer bbuf = ByteBuffer.wrap(buffer)
        .order(ByteOrder.LITTLE_ENDIAN);
      FloatBuffer fbuf = bbuf.asFloatBuffer();
      while ((lineDoc = in.readLine()) != null) {
        float[] dvec = dict.computeTextVector(lineDoc);
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

}
