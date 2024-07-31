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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.NeighborQueue;
//TODO Lucene may make these unavailable, we should pull in this from hppc directly
import org.apache.lucene.internal.hppc.IntIntHashMap;

// e.g. to compile with zero build tooling!:
//
//   cd /l/util; javac -d build -cp /l/trunk/lucene/core/build/classes/java/main src/main/knn/*.java 

/**
 * For testing indexing and search performance of a knn-graph
 *
 * <p>java -cp .../lib/*.jar knn.KnnGraphTester -ndoc 1000000 -search
 * .../vectors.bin
 */
public class KnnGraphTester {

  public static final String KNN_FIELD = "knn";
  public static final String ID_FIELD = "id";
  private static final String INDEX_DIR = "knnIndices";

  private int numDocs;
  private int dim;
  private int topK;
  private int numIters;
  private int fanout; // nocommit is this used anywhere :)  seems to be query time parameter...?
  private Path indexPath;
  private boolean quiet;
  private boolean reindex;
  private boolean forceMerge;
  private int reindexTimeMsec;
  private int beamWidth;
  private int maxConn;
  private boolean quantize;
  private int quantizeBits;
  private boolean quantizeCompress;
  private int numMergeThread;
  private int numMergeWorker;
  private ExecutorService exec;
  private VectorSimilarityFunction similarityFunction;
  private VectorEncoding vectorEncoding;
  private FixedBitSet matchDocs;
  private float selectivity;
  private boolean prefilter;
  private boolean randomCommits;

  private KnnGraphTester() {
    // set defaults
    numDocs = 1000;
    numIters = 1000;
    dim = 256;
    topK = 100;
    numMergeThread = 1;
    numMergeWorker = 1;
    fanout = topK;
    similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
    vectorEncoding = VectorEncoding.FLOAT32;
    selectivity = 1f;
    prefilter = false;
    randomCommits = false;
    quantizeBits = 7;
    quantizeCompress = false;
  }

  public static void main(String... args) throws Exception {
    new KnnGraphTester().runWithCleanUp(args);
  }

  private void runWithCleanUp(String... args) throws Exception {
    try {
      run(args);
    } finally {
      cleanUp();
    }
  }

  private void cleanUp() {
    if (exec != null) {
      exec.shutdownNow();
    }
  }

  private void run(String... args) throws Exception {
    String operation = null;
    Path docVectorsPath = null, queryPath = null, outputPath = null;
    for (int iarg = 0; iarg < args.length; iarg++) {
      String arg = args[iarg];
      switch (arg) {
        case "-search":
        case "-check":
        case "-stats":
        case "-dump":
          if (operation != null) {
            throw new IllegalArgumentException(
                "Specify only one operation, not both " + arg + " and " + operation);
          }
          operation = arg;
          if (operation.equals("-search")) {
            if (iarg == args.length - 1) {
              throw new IllegalArgumentException(
                  "Operation " + arg + " requires a following pathname");
            }
            queryPath = Paths.get(args[++iarg]);
          }
          break;
        case "-fanout":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-fanout requires a following number");
          }
          fanout = Integer.parseInt(args[++iarg]);
          break;
        case "-beamWidthIndex":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-beamWidthIndex requires a following number");
          }
          beamWidth = Integer.parseInt(args[++iarg]);
          break;
        case "-maxConn":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-maxConn requires a following number");
          }
          maxConn = Integer.parseInt(args[++iarg]);
          break;
        case "-dim":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-dim requires a following number");
          }
          dim = Integer.parseInt(args[++iarg]);
          break;
        case "-ndoc":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-ndoc requires a following number");
          }
          numDocs = Integer.parseInt(args[++iarg]);
          break;
        case "-niter":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-niter requires a following number");
          }
          numIters = Integer.parseInt(args[++iarg]);
          break;
        case "-reindex":
          reindex = true;
          break;
        case "-quantize":
          quantize = true;
          break;
        case "-quantizeBits":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-quantizeBits requires a following number");
          }
          quantizeBits = Integer.parseInt(args[++iarg]);
          break;
        case "-quantizeCompress":
          quantizeCompress = true;
          break;
        case "-topK":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-topK requires a following number");
          }
          topK = Integer.parseInt(args[++iarg]);
          break;
        case "-out":
          outputPath = Paths.get(args[++iarg]);
          break;
        case "-docs":
          docVectorsPath = Paths.get(args[++iarg]);
          break;
        case "-indexPath":
          indexPath = Paths.get(args[++iarg]);
          break;
        case "-encoding":
          String encoding = args[++iarg];
          switch (encoding) {
            case "byte":
              vectorEncoding = VectorEncoding.BYTE;
              break;
            case "float32":
              vectorEncoding = VectorEncoding.FLOAT32;
              break;
            default:
              throw new IllegalArgumentException("-encoding can be 'byte' or 'float32' only");
          }
          break;
        case "-metric":
          String metric = args[++iarg];
          switch (metric) {
            case "mip":
              similarityFunction = VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
              break;
            case "cosine":
              similarityFunction = VectorSimilarityFunction.COSINE;
              break;
            case "euclidean":
              similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
              break;
            case "angular":
              similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
              break;
            default:
              throw new IllegalArgumentException("-metric can be 'angular', 'euclidean', 'cosine', or 'mip' only");
          }
          break;
        case "-forceMerge":
          forceMerge = true;
          break;
        case "-prefilter":
          prefilter = true;
          break;
        case "-randomCommits":
          randomCommits = true;
          break;
        case "-filterSelectivity":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-filterSelectivity requires a following float");
          }
          selectivity = Float.parseFloat(args[++iarg]);
          if (selectivity <= 0 || selectivity >= 1) {
            throw new IllegalArgumentException("-filterSelectivity must be between 0 and 1");
          }
          break;
        case "-quiet":
          quiet = true;
          break;
        case "-numMergeWorker":
          numMergeWorker = Integer.parseInt(args[++iarg]);
          if (numMergeWorker <= 0) {
            throw new IllegalArgumentException("-numMergeWorker should be >= 1");
          }
          break;
        case "-numMergeThread":
          numMergeThread = Integer.parseInt(args[++iarg]);
          exec = Executors.newFixedThreadPool(numMergeThread, new NamedThreadFactory("hnsw-merge"));
          if (numMergeThread <= 0) {
            throw new IllegalArgumentException("-numMergeThread should be >= 1");
          }
          break;
        default:
          throw new IllegalArgumentException("unknown argument " + arg);
          // usage();
      }
    }
    if (operation == null && reindex == false) {
      usage();
    }
    if (prefilter && selectivity == 1f) {
      throw new IllegalArgumentException("-prefilter requires filterSelectivity between 0 and 1");
    }
    if (indexPath == null) {
      indexPath = Paths.get(formatIndexPath(docVectorsPath)); // derive index path
    }
    if (reindex) {
      if (docVectorsPath == null) {
        throw new IllegalArgumentException("-docs argument is required when indexing");
      }
      reindexTimeMsec = new KnnIndexer(
        docVectorsPath,
        indexPath,
        getCodec(maxConn, beamWidth, exec, numMergeWorker, quantize, quantizeBits, quantizeCompress),
        vectorEncoding,
        dim,
        similarityFunction,
        numDocs,
        0,
        quiet
      ).createIndex();
      System.out.println("reindex takes " + reindexTimeMsec + " ms");
    }
    if (forceMerge) {
      forceMerge();
    }
    if (operation != null) {
      switch (operation) {
        case "-search":
          if (docVectorsPath == null) {
            throw new IllegalArgumentException("missing -docs arg");
          }
          if (selectivity < 1) {
            matchDocs = generateRandomBitSet(numDocs, selectivity);
          }
          if (outputPath != null) {
            testSearch(indexPath, queryPath, outputPath, null);
          } else {
            testSearch(indexPath, queryPath, null, getNN(docVectorsPath, queryPath));
          }
          break;
        case "-stats":
          printFanoutHist(indexPath);
          break;
      }
    }
  }

  private String formatIndexPath(Path docsPath) {
    if (quantize) {
      return INDEX_DIR + "/" + docsPath.getFileName() + "-" + maxConn + "-" + beamWidth + "-"
              + quantizeBits + (quantizeCompress ? "-compressed" : "" ) + ".index";
    }
    return INDEX_DIR + "/" + docsPath.getFileName() + "-" + maxConn + "-" + beamWidth + ".index";
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void printFanoutHist(Path indexPath) throws IOException {
    try (Directory dir = FSDirectory.open(indexPath);
         DirectoryReader reader = DirectoryReader.open(dir)) {
      for (LeafReaderContext context : reader.leaves()) {
        LeafReader leafReader = context.reader();
        KnnVectorsReader vectorsReader =
            ((PerFieldKnnVectorsFormat.FieldsReader) ((CodecReader) leafReader).getVectorReader())
                .getFieldReader(KNN_FIELD);
        HnswGraph knnValues = ((Lucene99HnswVectorsReader) vectorsReader).getGraph(KNN_FIELD);
        System.out.printf("Leaf %d has %d layers\n", context.ord, knnValues.numLevels());
        System.out.printf("Leaf %d has %d documents\n", context.ord, leafReader.maxDoc());
        printGraphFanout(knnValues, leafReader.maxDoc());
        printGraphConnectedNess(knnValues);
      }
    }
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void forceMerge() throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.APPEND);
    iwc.setCodec(getCodec(maxConn, beamWidth, exec, numMergeWorker, quantize, quantizeBits, quantizeCompress));
    if (quiet == false) {
      // not a quiet place!
      iwc.setInfoStream(new PrintStreamInfoStream(System.out));
    }
    System.out.println("Force merge index in " + indexPath);
    long start = System.currentTimeMillis();
    try (IndexWriter iw = new IndexWriter(FSDirectory.open(indexPath), iwc)) {
      iw.forceMerge(1);
    }
    System.out.println("Force merge done in: " + (System.currentTimeMillis() - start) + " ms");
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void printGraphConnectedNess(HnswGraph knnValues) throws IOException {
    int numLevels = knnValues.numLevels();
    for (int level = numLevels - 1; level >= 0; level--) {
      HnswGraph.NodesIterator nodesIterator = knnValues.getNodesOnLevel(level);
      int numNodesOnLayer = nodesIterator.size();
      IntIntHashMap connectedNodes = new IntIntHashMap(numNodesOnLayer);
      // Start at entry point and search all nodes on this level
      int entryPoint = knnValues.entryNode();
      Deque<Integer> stack = new ArrayDeque<>();
      stack.push(entryPoint);
      while (!stack.isEmpty()) {
        int node = stack.pop();
        if (connectedNodes.containsKey(node)) {
          continue;
        }
        connectedNodes.put(node, 1);
        knnValues.seek(level, node);
        int friendOrd;
        while ((friendOrd = knnValues.nextNeighbor()) != NO_MORE_DOCS) {
          stack.push(friendOrd);
        }
      }
      System.out.printf(
        "Graph level=%d size=%d, connectedness=%.2f\n",
        level, numNodesOnLayer, connectedNodes.size() / (float) numNodesOnLayer);
    }
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void printGraphFanout(HnswGraph knnValues, int numDocs) throws IOException {
    int numLevels = knnValues.numLevels();
    for (int level = numLevels - 1; level >= 0; level--) {
      int count = 0;
      int min = Integer.MAX_VALUE, max = 0, total = 0;
      HnswGraph.NodesIterator nodesIterator = knnValues.getNodesOnLevel(level);
      int[] leafHist = new int[nodesIterator.size()];
      while (nodesIterator.hasNext()) {
        int node = nodesIterator.nextInt();
        int n = 0;
        knnValues.seek(level, node);
        while (knnValues.nextNeighbor() != NO_MORE_DOCS) {
          ++n;
        }
        ++leafHist[n];
        max = Math.max(max, n);
        min = Math.min(min, n);
        if (n > 0) {
          ++count;
          total += n;
        }
      }
      System.out.printf(
        "Graph level=%d size=%d, Fanout min=%d, mean=%.2f, max=%d\n",
        level, count, min, total / (float) count, max);
      printHist(leafHist, max, count, 10);
    }
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void printHist(int[] hist, int max, int count, int nbuckets) {
    System.out.print("%");
    for (int i = 0; i <= nbuckets; i++) {
      System.out.printf("%4d", i * 100 / nbuckets);
    }
    System.out.printf("\n %4d", hist[0]);
    int total = 0, ibucket = 1;
    for (int i = 1; i <= max && ibucket <= nbuckets; i++) {
      total += hist[i];
      while (total >= count * ibucket / nbuckets) {
        System.out.printf("%4d", i);
        ++ibucket;
      }
    }
    System.out.println();
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void testSearch(Path indexPath, Path queryPath, Path outputPath, int[][] nn)
      throws IOException {
    TopDocs[] results = new TopDocs[numIters];
    long elapsed, totalCpuTime, totalVisited = 0;
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    try (FileChannel input = FileChannel.open(queryPath)) {
      VectorReader targetReader = VectorReader.create(input, dim, vectorEncoding);
      VectorReaderByte targetReaderByte = null;
      if (targetReader instanceof VectorReaderByte b) {
        targetReaderByte = b;
      }
      if (quiet == false) {
        System.out.println("running " + numIters + " targets; topK=" + topK + ", fanout=" + fanout);
      }
      long start;
      ThreadMXBean bean = ManagementFactory.getThreadMXBean();
      long cpuTimeStartNs;
      try (Directory dir = FSDirectory.open(indexPath);
           DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        numDocs = reader.maxDoc();
        Query bitSetQuery = prefilter ? new BitSetQuery(matchDocs) : null;
        for (int i = 0; i < numIters; i++) {
          // warm up
          if (vectorEncoding.equals(VectorEncoding.BYTE)) {
            byte[] target = targetReaderByte.nextBytes();
            if (prefilter) {
              doKnnByteVectorQuery(searcher, KNN_FIELD, target, topK, fanout, bitSetQuery);
            } else {
              doKnnByteVectorQuery(searcher, KNN_FIELD, target, (int) (topK / selectivity), fanout, null);
            }
          } else {
            float[] target = targetReader.next();
            if (prefilter) {
              doKnnVectorQuery(searcher, KNN_FIELD, target, topK, fanout, bitSetQuery);
            } else {
              doKnnVectorQuery(searcher, KNN_FIELD, target, (int) (topK / selectivity), fanout, null);
            }
          }
        }
        targetReader.reset();
        start = System.nanoTime();
        cpuTimeStartNs = bean.getCurrentThreadCpuTime();
        for (int i = 0; i < numIters; i++) {
          if (vectorEncoding.equals(VectorEncoding.BYTE)) {
            byte[] target = targetReaderByte.nextBytes();
            if (prefilter) {
              results[i] = doKnnByteVectorQuery(searcher, KNN_FIELD, target, topK, fanout, bitSetQuery);
            } else {
              results[i] = doKnnByteVectorQuery(searcher, KNN_FIELD, target, (int) (topK / selectivity), fanout, null);
            }
          } else {
            float[] target = targetReader.next();
            if (prefilter) {
              results[i] = doKnnVectorQuery(searcher, KNN_FIELD, target, topK, fanout, bitSetQuery);
            } else {
              results[i] =
                doKnnVectorQuery(
                  searcher, KNN_FIELD, target, (int) (topK / selectivity), fanout, null);
            }
          }
          if (prefilter == false && matchDocs != null) {
            results[i].scoreDocs =
                Arrays.stream(results[i].scoreDocs)
                    .filter(scoreDoc -> matchDocs.get(scoreDoc.doc))
                    .toArray(ScoreDoc[]::new);
          }
        }
        totalCpuTime =
            TimeUnit.NANOSECONDS.toMillis(bean.getCurrentThreadCpuTime() - cpuTimeStartNs);
        elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start); // ns -> ms
        StoredFields storedFields = reader.storedFields();
        for (int i = 0; i < numIters; i++) {
          totalVisited += results[i].totalHits.value;
          for (ScoreDoc doc : results[i].scoreDocs) {
            if (doc.doc != NO_MORE_DOCS) {
              // there is a bug somewhere that can result in doc=NO_MORE_DOCS!  I think it happens
              // in some degenerate case (like input query has NaN in it?) that causes no results to
              // be returned from HNSW search?
              doc.doc = Integer.parseInt(storedFields.document(doc.doc).get("id"));
            } else {
              System.out.println("NO_MORE_DOCS!");
            }
          }
        }
      }
      if (quiet == false) {
        System.out.println(
            "completed "
                + numIters
                + " searches in "
                + elapsed
                + " ms: "
                + ((1000 * numIters) / elapsed)
                + " QPS "
                + "CPU time="
                + totalCpuTime
                + "ms");
      }
    } finally {
      executorService.shutdown();
    }
    if (outputPath != null) {
      ByteBuffer buf = ByteBuffer.allocate(4);
      IntBuffer ibuf = buf.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
      try (OutputStream out = Files.newOutputStream(outputPath)) {
        for (int i = 0; i < numIters; i++) {
          for (ScoreDoc doc : results[i].scoreDocs) {
            ibuf.position(0);
            ibuf.put(doc.doc);
            out.write(buf.array());
          }
        }
      }
    } else {
      if (quiet == false) {
        System.out.println("checking results");
      }
      float recall = checkResults(results, nn);
      totalVisited /= numIters;
      System.out.printf(
          Locale.ROOT,
          "%5.3f\t%5.2f\t%d\t%d\t%d\t%d\t%d\t%d\t%.2f\t%s\n",
          recall,
          totalCpuTime / (float) numIters,
          numDocs,
          fanout,
          maxConn,
          beamWidth,
          totalVisited,
          reindexTimeMsec,
          selectivity,
          prefilter ? "pre-filter" : "post-filter");
    }
  }

  private static TopDocs doKnnByteVectorQuery(
    IndexSearcher searcher, String field, byte[] vector, int k, int fanout, Query filter)
    throws IOException {
    ProfiledKnnByteVectorQuery profiledQuery = new ProfiledKnnByteVectorQuery(field, vector, k, fanout, filter);
    TopDocs docs = searcher.search(profiledQuery, k);
    return new TopDocs(new TotalHits(profiledQuery.totalVectorCount(), docs.totalHits.relation), docs.scoreDocs);
  }

  private static TopDocs doKnnVectorQuery(
      IndexSearcher searcher, String field, float[] vector, int k, int fanout, Query filter)
      throws IOException {
    ProfiledKnnFloatVectorQuery profiledQuery = new ProfiledKnnFloatVectorQuery(field, vector, k, fanout, filter);
    TopDocs docs = searcher.search(profiledQuery, k);
    return new TopDocs(new TotalHits(profiledQuery.totalVectorCount(), docs.totalHits.relation), docs.scoreDocs);
  }

  private float checkResults(TopDocs[] results, int[][] nn) {
    int totalMatches = 0;
    int totalResults = results.length * topK;
    for (int i = 0; i < results.length; i++) {
      // System.out.println(Arrays.toString(nn[i]));
      // System.out.println(Arrays.toString(results[i].scoreDocs));
      totalMatches += compareNN(nn[i], results[i]);
    }
    return totalMatches / (float) totalResults;
  }

  private int compareNN(int[] expected, TopDocs results) {
    int matched = 0;
    Set<Integer> expectedSet = new HashSet<>();
    for (int i = 0; i < topK; i++) {
      expectedSet.add(expected[i]);
    }
    for (ScoreDoc scoreDoc : results.scoreDocs) {
      if (expectedSet.contains(scoreDoc.doc)) {
        ++matched;
      }
    }
    return matched;
  }

  private int[][] getNN(Path docPath, Path queryPath) throws IOException {
    // look in working directory for cached nn file
    String hash = Integer.toString(Objects.hash(docPath, queryPath, numDocs, numIters, topK, similarityFunction.ordinal()), 36);
    String nnFileName = "nn-" + hash + ".bin";
    Path nnPath = Paths.get(nnFileName);
    if (Files.exists(nnPath) && isNewer(nnPath, docPath, queryPath) && selectivity == 1f) {
      return readNN(nnPath);
    } else {
      // TODO: enable computing NN from high precision vectors when
      // checking low-precision recall
      int[][] nn;
      if (vectorEncoding.equals(VectorEncoding.BYTE)) {
        nn = computeNNBytes(docPath, queryPath);
      } else {
        nn = computeNN(docPath, queryPath);
      }
      if (selectivity == 1f) {
        writeNN(nn, nnPath);
      }
      return nn;
    }
  }

  private boolean isNewer(Path path, Path... others) throws IOException {
    FileTime modified = Files.getLastModifiedTime(path);
    for (Path other : others) {
      if (Files.getLastModifiedTime(other).compareTo(modified) >= 0) {
        return false;
      }
    }
    return true;
  }

  private int[][] readNN(Path nnPath) throws IOException {
    int[][] result = new int[numIters][];
    try (FileChannel in = FileChannel.open(nnPath)) {
      IntBuffer intBuffer =
          in.map(FileChannel.MapMode.READ_ONLY, 0, numIters * topK * Integer.BYTES)
              .order(ByteOrder.LITTLE_ENDIAN)
              .asIntBuffer();
      for (int i = 0; i < numIters; i++) {
        result[i] = new int[topK];
        intBuffer.get(result[i]);
      }
    }
    return result;
  }

  private void writeNN(int[][] nn, Path nnPath) throws IOException {
    if (quiet == false) {
      System.out.println("writing true nearest neighbors to " + nnPath);
    }
    ByteBuffer tmp =
        ByteBuffer.allocate(nn[0].length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    try (OutputStream out = Files.newOutputStream(nnPath)) {
      for (int i = 0; i < numIters; i++) {
        tmp.asIntBuffer().put(nn[i]);
        out.write(tmp.array());
      }
    }
  }

  @SuppressForbidden(reason = "Uses random()")
  private static FixedBitSet generateRandomBitSet(int size, float selectivity) {
    FixedBitSet bitSet = new FixedBitSet(size);
    for (int i = 0; i < size; i++) {
      if (Math.random() < selectivity) {
        bitSet.set(i);
      } else {
        bitSet.clear(i);
      }
    }
    return bitSet;
  }

  private int[][] computeNNBytes(Path docPath, Path queryPath) throws IOException {
    int[][] result = new int[numIters][];
    if (quiet == false) {
      System.out.println("computing true nearest neighbors of " + numIters + " target vectors");
    }
    try (FileChannel in = FileChannel.open(docPath);
         FileChannel qIn = FileChannel.open(queryPath)) {
      VectorReaderByte docReader = (VectorReaderByte)VectorReader.create(in, dim, VectorEncoding.BYTE);
      VectorReaderByte queryReader = (VectorReaderByte)VectorReader.create(qIn, dim, VectorEncoding.BYTE);
      for (int i = 0; i < numIters; i++) {
        byte[] query = queryReader.nextBytes();
        NeighborQueue queue = new NeighborQueue(topK, false);
        for (int j = 0; j < numDocs; j++) {
          byte[] doc = docReader.nextBytes();
          float d = similarityFunction.compare(query, doc);
          if (matchDocs == null || matchDocs.get(j)) {
            queue.insertWithOverflow(j, d);
          }
        }
        docReader.reset();
        result[i] = new int[topK];
        for (int k = topK - 1; k >= 0; k--) {
          result[i][k] = queue.topNode();
          queue.pop();
        }
        if (quiet == false && (i + 1) % 10 == 0) {
          System.out.print(" " + (i + 1));
          System.out.flush();
        }
      }
    }
    return result;
  }

  /** Brute force computation of "true" nearest neighhbors. */
  private int[][] computeNN(Path docPath, Path queryPath)
      throws IOException {
    int[][] result = new int[numIters][];
    if (quiet == false) {
      System.out.println("computing true nearest neighbors of " + numIters + " target vectors");
    }
    try (FileChannel in = FileChannel.open(docPath);
         FileChannel qIn = FileChannel.open(queryPath)) {
      VectorReader docReader = VectorReader.create(in, dim, VectorEncoding.FLOAT32);
      VectorReader queryReader = VectorReader.create(qIn, dim, VectorEncoding.FLOAT32);
      for (int i = 0; i < numIters; i++) {
        float[] query = queryReader.next();
        NeighborQueue queue = new NeighborQueue(topK, false);
        for (int j = 0; j < numDocs; j++) {
          float[] doc = docReader.next();
          float d = similarityFunction.compare(query, doc);
          if (matchDocs == null || matchDocs.get(j)) {
            queue.insertWithOverflow(j, d);
          }
        }
        docReader.reset();
        result[i] = new int[topK];
        for (int k = topK - 1; k >= 0; k--) {
          result[i][k] = queue.topNode();
          queue.pop();
        }
        if (quiet == false && (i + 1) % 10 == 0) {
          System.out.print(" " + (i + 1));
          System.out.flush();
        }
      }
    }
    return result;
  }

  static Codec getCodec(int maxConn, int beamWidth, ExecutorService exec, int numMergeWorker, boolean quantize, int quantizeBits, boolean quantizeCompress) {
    if (exec == null) {
      return new Lucene912Codec() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
          return quantize ?
              new Lucene99HnswScalarQuantizedVectorsFormat(maxConn, beamWidth, numMergeWorker, quantizeBits, quantizeCompress, null, null) :
              new Lucene99HnswVectorsFormat(maxConn, beamWidth, numMergeWorker, null);
        }
      };
    } else {
      return new Lucene912Codec() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
          return quantize ?
              new Lucene99HnswScalarQuantizedVectorsFormat(maxConn, beamWidth, numMergeWorker, quantizeBits, quantizeCompress, null, exec) :
              new Lucene99HnswVectorsFormat(maxConn, beamWidth, numMergeWorker, exec);
        }
      };
    }
  }

  private static void usage() {
    String error =
        "Usage: TestKnnGraph [-reindex] [-search {queryfile}|-stats|-check] [-docs {datafile}] [-niter N] [-fanout N] [-maxConn N] [-beamWidth N] [-filterSelectivity N] [-prefilter]";
    System.err.println(error);
    System.exit(1);
  }

  private static class ProfiledKnnByteVectorQuery extends KnnByteVectorQuery {
    private final Query filter;
    private final int k;
    private final int fanout;
    private final String field;
    private final byte[] target;
    private long totalVectorCount;

    ProfiledKnnByteVectorQuery(String field, byte[] target, int k, int fanout, Query filter) {
      super(field, target, k + fanout, filter);
      this.field = field;
      this.target = target;
      this.k = k;
      this.fanout = fanout;
      this.filter = filter;
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
      TopDocs td = TopDocs.merge(k, perLeafResults);
      totalVectorCount = td.totalHits.value;
      return td;
    }

    long totalVectorCount() {
      return totalVectorCount;
    }
  }

  private static class ProfiledKnnFloatVectorQuery extends KnnFloatVectorQuery {
    private final Query filter;
    private final int k;
    private final int fanout;
    private final String field;
    private final float[] target;
    private long totalVectorCount;

    ProfiledKnnFloatVectorQuery(String field, float[] target, int k, int fanout, Query filter) {
      super(field, target, k + fanout, filter);
      this.field = field;
      this.target = target;
      this.k = k;
      this.fanout = fanout;
      this.filter = filter;
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
      TopDocs td = TopDocs.merge(k, perLeafResults);
      totalVectorCount = td.totalHits.value;
      return td;
    }

    long totalVectorCount() {
      return totalVectorCount;
    }

  }

  private static class BitSetQuery extends Query {
    private final FixedBitSet docs;
    private final int cardinality;

    BitSetQuery(FixedBitSet docs) {
      this.docs = docs;
      this.cardinality = docs.cardinality();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
        throws IOException {
      return new ConstantScoreWeight(this, boost) {
        Scorer scorer = new ConstantScoreScorer(score(), scoreMode, new BitSetIterator(docs, cardinality));
        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
          return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
              return scorer;
            }

            @Override
            public long cost() {
              return cardinality;
            }
          };
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return false;
        }
      };
    }

    @Override
    public void visit(QueryVisitor visitor) {
    }

    @Override
    public String toString(String field) {
      return "BitSetQuery";
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) && docs.equals(((BitSetQuery) other).docs);
    }

    @Override
    public int hashCode() {
      return 31 * classHash() + docs.hashCode();
    }
  }
}
