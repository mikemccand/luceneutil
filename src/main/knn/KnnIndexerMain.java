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

package knn;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.NamedThreadFactory;

public class KnnIndexerMain {
  public Path docVectorsPath;
  public Path indexPath;
  public int maxConn = 16;
  public int beamWidth = 100;
  public VectorEncoding vectorEncoding = VectorEncoding.FLOAT32;
  public int dimension;
  public VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.COSINE;
  public int numDocs;

  public int docStartIndex = 0;
  boolean quiet = false;
  boolean parentJoin = false;
  Path parentJoinMetaFile = null;
  boolean useBp = false;

  @Override
  public String toString() {
    return "KnnIndexerMain {" +
        "docVectorsPath='" + docVectorsPath + '\'' +
        ", indexPath='" + indexPath + '\'' +
        ", maxConn=" + maxConn +
        ", beamWidth=" + beamWidth +
        ", vectorEncoding=" + vectorEncoding +
        ", dimension=" + dimension +
        ", similarityFunction=" + similarityFunction +
        ", numDocs=" + numDocs +
        ", docStartIndex=" + docStartIndex +
        ", quiet=" + quiet +
        '}';
  }

  public static void main(String[] args) throws Exception {
    KnnIndexerMain inputs = new KnnIndexerMain();

    try {
      int i = 0;
      while (i < args.length) {
        switch (args[i].toLowerCase()) {
          case "-docvectorspath" -> inputs.docVectorsPath = Path.of(args[++i]);
          case "-indexpath" -> inputs.indexPath = Path.of(args[++i]);
          case "-maxconn" -> inputs.maxConn = Integer.parseInt(args[++i]);
          case "-beamwidth" -> inputs.beamWidth = Integer.parseInt(args[++i]);
          case "-vectorencoding" -> inputs.vectorEncoding = VectorEncoding.valueOf(args[++i]);
          case "-similarityfunction" ->
              inputs.similarityFunction = VectorSimilarityFunction.valueOf(args[++i].toUpperCase());
          case "-numdocs" -> inputs.numDocs = Integer.parseInt(args[++i]);
          case "-bp" -> inputs.useBp = true;
          case "-docstartindex" -> inputs.docStartIndex = Integer.parseInt(args[++i]);
          case "-dimension" -> inputs.dimension = Integer.parseInt(args[++i]);
          case "-quiet" -> inputs.quiet = true;
          case "-parentjoin" -> {
            inputs.parentJoin = true;
            inputs.parentJoinMetaFile = Paths.get(args[++i]);
          }
          default -> throw new IllegalArgumentException("Cannot recognize the option " + args[i]);
        }
        i++;
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Please follow correct usage guidelines:" + inputs.usage());
    }

    if (!inputs.quiet) {
      System.out.println("Creating index with following configurations : " + inputs);
    }

    boolean quantize = true;
    int quantizeBits = 8;
    boolean quantizeCompress = true;
    int numMergeWorker = 16;
    int numMergeThread = 8;
    ExecutorService exec = Executors.newFixedThreadPool(numMergeThread, new NamedThreadFactory("hnsw-merge"));

    new KnnIndexer(inputs.docVectorsPath, inputs.indexPath,
                   KnnGraphTester.getCodec(inputs.maxConn, inputs.beamWidth, exec, numMergeWorker, quantize, quantizeBits, KnnGraphTester.IndexType.HNSW, quantizeCompress),
                   numMergeThread, inputs.vectorEncoding,
                   inputs.dimension, inputs.similarityFunction, inputs.numDocs, inputs.docStartIndex, inputs.quiet,
                   inputs.parentJoin, inputs.parentJoinMetaFile, inputs.useBp).createIndex();

    if (!inputs.quiet) {
      System.out.println("Successfully created index.");
    }
  }

  public String usage() {
    return "KnnIndexerMain \n" +
        "\t -docVectorsPath : path of the file containing vectors for document. <TODO: what format?>\n" +
        "\t -indexPath : path of the folder/dir where the index has to be created.\n" +
        "\t -maxConn : maximum connections per node for HNSW graph\n" +
        "\t -beamWidth : beam-width at graph creation time. Same as efConstruction in the HNSW paper.\n" +
        "\t -vectorEncoding: vector encoding. one of constant 'BYTE' or 'FLOAT32'\n" +
        "\t -dimension : dimension / size of the vectors \n" +
        "\t -similarityFunction : similarity function for vector comparison. One of ( EUCLIDEAN, DOT_PRODUCT, COSINE, MAXIMUM_INNER_PRODUCT )\n" +
        "\t -numDocs : number of document vectors to be used from the file\n" +
        "\t -docStartIndex : Start index of first document vector. This can be helpful when we want to run different with set of documents from within the same file.\n" +
        "\t -quiet : don't print anything on console if mentioned.\n" +
        "\t -parentJoin : create parentJoin index. Requires '*-metadata.csv'\n" +
        "\t -bp : use binary partitioning merged policy to reorder index\n";
  }
}
