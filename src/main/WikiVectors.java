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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Precompute per-document semantic vectors as the sum (average) of their word vectors.
 */
public class WikiVectors {

    int dimension;
    long tokenCount;
    long missingTokenCount;
    Map<String, float[]> vectors = new HashMap<>();

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("usage: WikiVectors <vectorDictionary> <lineDocs> <docVectorOutput>");
            System.exit(-1);
        }
        WikiVectors wv = new WikiVectors();
        wv.loadDict(args[0]);
        wv.computeVectors(args[1], args[2]);
    }

    int parseLine(String line) {
        String[] parts = line.split(" ");
        String token = parts[0];
        float[] vector = new float[parts.length - 1];
        for (int i = 1; i < parts.length; i++) {
            vector[i - 1] = Float.parseFloat(parts[i]);
        }
        if (vectors.containsKey(token)) {
            throw new IllegalStateException("token " + token + " seen twice");
        }
        vectors.put(token, vector);
        return vector.length;
    }

    void loadDict(String filename) throws IOException {
        // read a dictionary file where each line has a token and its n-dimensional vector as text:
        // <word> <f1> <f2> ... <fn>
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filename), StandardCharsets.UTF_8)) {
            String line = reader.readLine();
            dimension = parseLine(line);
            while ((line = reader.readLine()) != null) {
                int dim = parseLine(line);
                if (dim != dimension) {
                    String err = String.format("vector dimension %s is not the initial dimension: %s for line: %s", dim, dimension, line);
                    throw new IllegalStateException(err);
                }
                if (vectors.size() % 10000 == 0) {
                    System.out.print("loaded " + vectors.size() + "\r");
                    System.out.flush();
                }
            }
        }
        System.out.println("loaded " + vectors.size());
    }

    void computeVectors(String lineDocFile, String outputFile) throws IOException {
        int count = 0;
        CharsetDecoder dec=StandardCharsets.UTF_8.newDecoder()
            .onMalformedInput(CodingErrorAction.REPLACE); // replace invalid input with the UTF8 replacement character
        try (OutputStream out = Files.newOutputStream(Paths.get(outputFile));
             Reader r = Channels.newReader(FileChannel.open(Paths.get(lineDocFile)), dec, -1);
             BufferedReader in = new BufferedReader(r)) {
            String lineDoc;
            byte[] buffer = new byte[dimension * Float.BYTES];
            ByteBuffer bbuf = ByteBuffer.wrap(buffer)
                .order(ByteOrder.LITTLE_ENDIAN);
            FloatBuffer fbuf = bbuf.asFloatBuffer();
            while ((lineDoc = in.readLine()) != null) {
                float[] dvec = computeDocVector(lineDoc);
                fbuf.position(0);
                fbuf.put(dvec);
                out.write(buffer);
                if (++count % 10000 == 0) {
                    System.out.print("wrote " + count + "\r");
                    System.out.flush();
                }
            }
            System.out.println("wrote " + count);
            System.out.println("Token Coverage = " + (100 * tokenCount / (tokenCount + missingTokenCount)) + "%");
        } catch (IOException e) {
            System.err.println("An error occurred on line " + (count + 1));
            throw e;
        }
    }

    void vectorAdd(float[] x, float[] y) {
        assert x.length == y.length;
        for (int i = 0; i < x.length; i++) {
            x[i] += y[i];
        }
    }

    void vectorDiv(float[] v, float x) {
        for (int i = 0; i < v.length; i++) {
            v[i] /= x;
        }
    }

    float[] computeDocVector(String doc) {
        float[] dvec = new float[dimension];
        int count = 0;
        for (String token : tokenize(doc)) {
            float[] tvec = vectors.get(token);
            if (tvec != null) {
                vectorAdd(dvec, tvec);
                count++;
            } else {
                missingTokenCount++;
            }
        }
        tokenCount += count;
        vectorDiv(dvec, count);
        return dvec;
    }

    // tokenize on white space, most ascii punctuation, preserving -_,., then lower case.  Not very
    // sophisticated, but enough for performance testing on English text. Should we use StandardTokenizer?
    Iterable<String> tokenize(String document) {
        List<String> tokens = new ArrayList<>();
        for (String t : document.split("[\\]\\[\\\\:\"'?/<> \t~`!@#$%^&*\\(\\)+={}]+")) {
            if (t.length() > 0) {
                t = t.toLowerCase();
                tokens.add(t);
            }
        }
        return tokens;
    }

}
