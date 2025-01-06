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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.bitvectors.HnswBitVectorsFormat;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.IntIntHashMap;
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
import org.apache.lucene.search.join.CheckJoinIndex;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.NeighborQueue;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
//TODO Lucene may make these unavailable, we should pull in this from hppc directly

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
  public static final String DOCTYPE_FIELD = "docType";
  public static final String DOCTYPE_PARENT = "_parent";
  public static final String DOCTYPE_CHILD = "_child";
  public static final String WIKI_ID_FIELD = "wikiID";
  public static final String WIKI_PARA_ID_FIELD = "wikiParaID";

  private int numDocs;
  private int dim;
  private int topK;
  private int numQueryVectors;
  private int fanout;  // this increases the internal HNSW search queue (search only) from topK to topK + fanout
  private Path indexPath;
  private boolean quiet;
  private boolean reindex;
  private boolean forceMerge;
  private int reindexTimeMsec;
  private double forceMergeTimeSec;
  private int indexNumSegments;
  private double indexSizeOnDiskMB;
  private long vectorDiskSizeBytes;
  // how much RAM searching needs to keep HNSW fully "hot":
  private long vectorRAMSizeBytes;
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
  private boolean parentJoin;
  private Path parentJoinMetaFile;
  private int numIndexThreads;
  // which query vector to seek to at the start
  private int queryStartIndex;
  // whether to reorder the index using binary partitioning
  private boolean useBp;

  private KnnGraphTester() {
    // set defaults
    numDocs = 1000;
    numQueryVectors = 1000;
    dim = 256;
    topK = 100;
    numMergeThread = 1;
    numMergeWorker = 1;
    fanout = topK;
    similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
    vectorEncoding = VectorEncoding.FLOAT32;
    selectivity = 1f;
    prefilter = false;
    quantize = false;
    randomCommits = false;
    quantizeBits = 7;
    quantizeCompress = false;
    numIndexThreads = 8;
    queryStartIndex = 0;
  }

  private static FileChannel getVectorFileChannel(Path path, int dim, VectorEncoding vectorEncoding) throws IOException {
    FileChannel in = FileChannel.open(path);
    System.out.println("path=" + path + " dim=" + dim + " vectorEncoding.byteSize=" + vectorEncoding.byteSize);
    if (in.size() % (dim * vectorEncoding.byteSize) != 0) {
      throw new IllegalArgumentException("vectors file \"" + path + "\" does not contain a whole number of vectors?  size=" + in.size());
    }
    return in;
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
        case "-search-and-stats":
        case "-check":
        case "-stats":
        case "-dump":
          if (operation != null) {
            throw new IllegalArgumentException(
                "Specify only one operation, not both " + arg + " and " + operation);
          }
          operation = arg;
          if (operation.equals("-search") || operation.equals("-search-and-stats")) {
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
          log("beamWidth = %d", beamWidth);
          break;
        case "-queryStartIndex":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-queryStartIndex requires a following number");
          }
          queryStartIndex = Integer.parseInt(args[++iarg]);
          log("queryStartIndex = %d", queryStartIndex);
          break;
        case "-maxConn":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-maxConn requires a following number");
          }
          maxConn = Integer.parseInt(args[++iarg]);
          log("maxConn = %d", maxConn);
          break;
        case "-dim":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-dim requires a following number");
          }
          dim = Integer.parseInt(args[++iarg]);
          log("Vector Dimensions: %d", dim);
          break;
        case "-ndoc":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-ndoc requires a following number");
          }
          numDocs = Integer.parseInt(args[++iarg]);
          log("numDocs = %d", numDocs);
          break;
        case "-niter":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-niter requires a following number");
          }
          numQueryVectors = Integer.parseInt(args[++iarg]);
          log("numQueryVectors = %d", numQueryVectors);
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
          if (quantizeBits != 1 && quantizeBits != 4 && quantizeBits != 7) {
            throw new IllegalArgumentException("-quantizeBits must be 1, 4 or 7");
          }
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
            case "angular": // TODO: why is angular a synonym for DOT_PRODUCT?  this only holds true if vectors are normalized to unit
                            // sphere?  but also, low values for angular mean the vectors are similar, but high values of dot_product mean
                            // the vectors are similar
            case "dot_product":
              similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
              break;
            default:
              throw new IllegalArgumentException("-metric can be 'mip', 'cosine', 'euclidean', 'angular' (or 'dot_product' -- same as 'angular') only; got: " + metric);
          }
          log("similarity = %s", similarityFunction);
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
          if (numMergeThread > 1) {
            exec = Executors.newFixedThreadPool(numMergeThread, new NamedThreadFactory("hnsw-merge"));
          }
          if (numMergeThread <= 0) {
            throw new IllegalArgumentException("-numMergeThread should be >= 1");
          }
          break;
        case "-parentJoin":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-parentJoin requires a following Path for parentJoinMetaFile");
          }
          parentJoinMetaFile = Paths.get(args[++iarg]);
          parentJoin = true;
          break;
        case "-numIndexThreads":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-numIndexThreads requires a following int");
          }
          numIndexThreads = Integer.parseInt(args[++iarg]);
          break;
        case "-bp":
          useBp = Boolean.parseBoolean(args[++iarg]);
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
      log("Index Path = %s", indexPath);
    }
    if (parentJoin && reindex == false && isParentJoinIndex(indexPath) == false) {
      throw new IllegalArgumentException("Provided index: [" + indexPath + "] does not have parent-child " +
          "document relationships. Rerun with -reindex or without -parentJoin argument");
    }

    if (reindex) {
      if (docVectorsPath == null) {
        throw new IllegalArgumentException("-docs argument is required when indexing");
      }
      reindexTimeMsec = new KnnIndexer(
        docVectorsPath,
        indexPath,
        getCodec(maxConn, beamWidth, exec, numMergeWorker, quantize, quantizeBits, quantizeCompress),
        numIndexThreads,
        vectorEncoding,
        dim,
        similarityFunction,
        numDocs,
        0,
        quiet,
        parentJoin,
        parentJoinMetaFile,
        useBp
      ).createIndex();
      System.out.println(String.format("reindex takes %.2f sec", msToSec(reindexTimeMsec)));
    }
    if (forceMerge) {
      forceMergeTimeSec = forceMerge();
    }
    try (Directory dir = FSDirectory.open(indexPath); IndexReader reader = DirectoryReader.open(dir)) {
      indexNumSegments = reader.leaves().size();
      System.out.println("index has " + indexNumSegments + " segments: " + ((StandardDirectoryReader) reader).getSegmentInfos());
      long indexSizeOnDiskBytes = 0;
      SegmentInfos infos = ((StandardDirectoryReader) reader).getSegmentInfos();
      for (SegmentCommitInfo info : infos) {
        long segSizeOnDiskBytes = 0;
        for (String fileName : info.files()) {
          segSizeOnDiskBytes += dir.fileLength(fileName);
        }
        indexSizeOnDiskBytes += segSizeOnDiskBytes;
        System.out.println(String.format(Locale.ROOT, "  %s: %.1f MB", info.info.name, segSizeOnDiskBytes / 1024. / 1024.));
      }
      indexSizeOnDiskBytes += dir.fileLength(infos.getSegmentsFileName());

      // long because maybe we at some point we support multi-valued vector fields:
      long totalVectorCount = 0;

      int encodingByteSize = -1;
      for (LeafReaderContext ctx : reader.leaves()) {
        KnnVectorsReader knnReader = ((SegmentReader) ctx.reader()).getVectorReader();

        int segEncodingByteSize;
        switch (vectorEncoding) {
        case BYTE:
          // TODO: does Lucene prevent int4/int7 quantization when input is byte per dimension?
          {
            ByteVectorValues vectors = knnReader.getByteVectorValues(KNN_FIELD);
            segEncodingByteSize = vectors.getEncoding().byteSize;
            totalVectorCount += vectors.size();
            break;
          }
        case FLOAT32:
          {
            FloatVectorValues vectors = knnReader.getFloatVectorValues(KNN_FIELD);
            segEncodingByteSize = vectors.getEncoding().byteSize;
            totalVectorCount += vectors.size();
            break;
          }
        default:
          throw new IllegalStateException("only FLOAT32 and BYTE input vectors are supported; got: " + vectorEncoding);
        }

        // TODO: why is encodingByteSize 4 for int4/int7 cases?
        if (encodingByteSize == -1) {
          encodingByteSize = segEncodingByteSize;
        } else if (encodingByteSize != segEncodingByteSize) {
          throw new IllegalStateException("encodingByteSize should not have changed across segments; got " +
                                          encodingByteSize + " and " + segEncodingByteSize);
        }
      }

      int origByteSize;
      switch (vectorEncoding) {
      case BYTE:
        // TODO: does Lucene prevent int4/int7 quantization when input is byte per dimension?
        origByteSize = Byte.BYTES;
        break;
      case FLOAT32:
        origByteSize = Float.BYTES;
        break;
      default:
        throw new IllegalStateException("only FLOAT32 and BYTE input vectors are supported; got: " + vectorEncoding);
      }
      System.out.println("encodingByteSize=" + encodingByteSize + " origByteSize=" + origByteSize);

      // TODO: why is encodingByteSize 4 even for int4/int7 cases?
      double realEncodingByteSize;
      if (quantize) {
        if (quantizeBits == 4) {
          if (quantizeCompress) {
            realEncodingByteSize = 0.5;
          } else {
            realEncodingByteSize = 1;
          }
        } else if (quantizeBits == 7) {
          realEncodingByteSize = 1;
        } else {
          throw new IllegalStateException("can only handle int4 and int7 quantized");
        }
      } else {
        realEncodingByteSize = Float.BYTES;
      }
      System.out.println("realEncodingByteSize=" + realEncodingByteSize);

      double diskBytesPerDim;
      if (origByteSize != (int) realEncodingByteSize) {
        // int4, int7 add one byte per dimension over the original
        diskBytesPerDim = origByteSize + realEncodingByteSize;
      } else {
        // unquantized
        diskBytesPerDim = origByteSize;
      }

      vectorDiskSizeBytes = (long) ((double) totalVectorCount * diskBytesPerDim * dim);
      vectorRAMSizeBytes = (long) ((double) totalVectorCount * realEncodingByteSize * dim);
      
      indexSizeOnDiskMB = indexSizeOnDiskBytes / 1024. / 1024.;
      System.out.println(String.format(Locale.ROOT, "index disk usage is %.2f MB", indexSizeOnDiskMB));
      System.out.println(String.format(Locale.ROOT, "vector disk usage is %.2f MB", vectorDiskSizeBytes/1024./1024.));
      System.out.println(String.format(Locale.ROOT, "vector RAM usage is %.2f MB", vectorRAMSizeBytes/1024./1024.));
    }
    
    if (operation != null) {
      switch (operation) {
        case "-search":
        case "-search-and-stats":
          if (docVectorsPath == null) {
            throw new IllegalArgumentException("missing -docs arg");
          }
          if (selectivity < 1) {
            matchDocs = generateRandomBitSet(numDocs, selectivity);
          }
          if (outputPath != null) {
            testSearch(indexPath, queryPath, queryStartIndex, outputPath, null);
          } else {
            testSearch(indexPath, queryPath, queryStartIndex, null, getExactNN(docVectorsPath, queryPath, queryStartIndex));
          }
          if (operation.equals("-search-and-stats")) {
            // also print stats, after searching
            printFanoutHist(indexPath);
          }
          break;
        case "-stats":
          printFanoutHist(indexPath);
          break;
      }
    }
  }

  private String formatIndexPath(Path docsPath) {
    List<String> suffix = new ArrayList<>();
    suffix.add(Integer.toString(maxConn));
    suffix.add(Integer.toString(beamWidth));
    if (useBp) {
      suffix.add("bp");
    }
    if (quantize) {
      suffix.add(Integer.toString(quantizeBits));
      if (quantizeCompress == true) {
        suffix.add("-compressed");
      }
    }
    if (parentJoin) {
      suffix.add("parentJoin");
    }
    return INDEX_DIR + "/" + docsPath.getFileName() + "-" + String.join("-", suffix) + ".index";
  }

  private boolean isParentJoinIndex(Path indexPath) {
    return indexPath.toString().contains("parentJoin");
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
  private double forceMerge() throws IOException {
    IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.APPEND);
    iwc.setCodec(getCodec(maxConn, beamWidth, exec, numMergeWorker, quantize, quantizeBits, quantizeCompress));
    System.out.println("Force merge index in " + indexPath);
    long startNS = System.nanoTime();
    try (IndexWriter iw = new IndexWriter(FSDirectory.open(indexPath), iwc)) {
      iw.forceMerge(1);
    }
    long endNS = System.nanoTime();
    double elapsedSec = nsToSec(endNS - startNS);
    System.out.println(String.format(Locale.ROOT, "Force merge done in %.2f sec", elapsedSec));
    return elapsedSec;
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
      long sumDelta = 0;
      HnswGraph.NodesIterator nodesIterator = knnValues.getNodesOnLevel(level);
      int[] leafHist = new int[nodesIterator.size()];
      while (nodesIterator.hasNext()) {
        int node = nodesIterator.nextInt();
        int n = 0;
        knnValues.seek(level, node);
        int nbr, lastNeighbor = -1, firstNeighbor = -1;
        while ((nbr = knnValues.nextNeighbor()) != NO_MORE_DOCS) {
          if (firstNeighbor == -1) {
            firstNeighbor = nbr;
          }
          // we see repeated neighbor nodes?!
          assert nbr >= lastNeighbor : "neighbors out of order for node " + node + ": " + nbr + "<" + lastNeighbor + " 1st=" + firstNeighbor;
          lastNeighbor = nbr;
          ++n;
        }
        ++leafHist[n];
        max = Math.max(max, n);
        min = Math.min(min, n);
        if (n > 0) {
          ++count;
          total += n;
          sumDelta += lastNeighbor - firstNeighbor;
        }
      }
      System.out.printf(
        "Graph level=%d size=%d, Fanout min=%d, mean=%.2f, max=%d, meandelta=%.2f\n",
        level, count, min, total / (float) count, max, sumDelta / (float) total);
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
      while (total >= count * ibucket / nbuckets  && ibucket <= nbuckets) {
        System.out.printf("%4d", i);
        ++ibucket;
      }
    }
    System.out.println();
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void testSearch(Path indexPath, Path queryPath, int queryStartIndex, Path outputPath, int[][] nn)
      throws IOException {
    TopDocs[] results = new TopDocs[numQueryVectors];
    int[][] resultIds = new int[numQueryVectors][];
    long elapsed, totalCpuTimeMS, totalVisited = 0;
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    try (FileChannel input = getVectorFileChannel(queryPath, dim, vectorEncoding)) {
      long queryPathSizeInBytes = input.size();
      System.out.println((int) (queryPathSizeInBytes / (dim * vectorEncoding.byteSize)) + " query vectors in queryPath \"" + queryPath + "\"");
      VectorReader targetReader = VectorReader.create(input, dim, vectorEncoding, queryStartIndex);
      VectorReaderByte targetReaderByte = null;
      if (targetReader instanceof VectorReaderByte b) {
        targetReaderByte = b;
      }
      log("searching " + numQueryVectors + " query vectors; topK=" + topK + ", fanout=" + fanout);
      long startNS;
      ThreadMXBean bean = ManagementFactory.getThreadMXBean();
      long cpuTimeStartNs;
      try (MMapDirectory dir = new MMapDirectory(indexPath)) {
        dir.setPreload((x, ctx) -> x.endsWith(".vec") || x.endsWith(".veq"));
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          IndexSearcher searcher = new IndexSearcher(reader);
          numDocs = reader.maxDoc();
          Query bitSetQuery = prefilter ? new BitSetQuery(matchDocs) : null;
          // warm up
          for (int i = 0; i < numQueryVectors; i++) {
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
                doKnnVectorQuery(searcher, KNN_FIELD, target, topK, fanout, bitSetQuery, parentJoin);
              } else {
                doKnnVectorQuery(searcher, KNN_FIELD, target, (int) (topK / selectivity), fanout, null, parentJoin);
              }
            }
          }
          targetReader.reset();
          startNS = System.nanoTime();
          cpuTimeStartNs = bean.getCurrentThreadCpuTime();
          for (int i = 0; i < numQueryVectors; i++) {
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
                results[i] = doKnnVectorQuery(searcher, KNN_FIELD, target, topK, fanout, bitSetQuery, parentJoin);
              } else {
                results[i] =
                  doKnnVectorQuery(
                    searcher, KNN_FIELD, target, (int) (topK / selectivity), fanout, null, parentJoin);
              }
              if (prefilter == false && matchDocs != null) {
                results[i].scoreDocs =
                  Arrays.stream(results[i].scoreDocs)
                    .filter(scoreDoc -> matchDocs.get(scoreDoc.doc))
                    .toArray(ScoreDoc[]::new);
              }
            }
          }
          totalCpuTimeMS =
            TimeUnit.NANOSECONDS.toMillis(bean.getCurrentThreadCpuTime() - cpuTimeStartNs);
          elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNS); // ns -> ms

          // Fetch, validate and write result document ids.
          StoredFields storedFields = reader.storedFields();
          for (int i = 0; i < numQueryVectors; i++) {
            totalVisited += results[i].totalHits.value();
            resultIds[i] = KnnTesterUtils.getResultIds(results[i], storedFields);
          }
          if (quiet == false) {
            System.out.println(
              "completed "
                + numQueryVectors
                + " searches in "
                + elapsed
                + " ms: "
                + ((1000 * numQueryVectors) / elapsed)
                + " QPS "
                + "CPU time="
                + totalCpuTimeMS
                + "ms");
          }
        }
      }
    } finally {
      executorService.shutdown();
    }
    if (outputPath != null) {
      ByteBuffer tmp =
        ByteBuffer.allocate(resultIds[0].length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      try (OutputStream out = Files.newOutputStream(outputPath)) {
        for (int i = 0; i < numQueryVectors; i++) {
          tmp.asIntBuffer().put(nn[i]);
          out.write(tmp.array());
        }
      }
    } else {
      log("checking results");
      float recall = checkResults(resultIds, nn);
      totalVisited /= numQueryVectors;
      String quantizeDesc;
      if (quantize) {
        quantizeDesc = Integer.toString(quantizeBits) + " bits";
      } else {
        quantizeDesc = "no";
      }
      double reindexSec = reindexTimeMsec / 1000.0;
      System.out.printf(
          Locale.ROOT,
          "SUMMARY: %5.3f\t%5.3f\t%d\t%d\t%d\t%d\t%d\t%s\t%d\t%.2f\t%.2f\t%.2f\t%d\t%.2f\t%.2f\t%s\t%5.3f\t%5.3f\n",
          recall,
          totalCpuTimeMS / (float) numQueryVectors,
          numDocs,
          topK,
          fanout,
          maxConn,
          beamWidth,
          quantizeDesc,
          totalVisited,
          reindexSec,
          numDocs / reindexSec,
          forceMergeTimeSec,
          indexNumSegments,
          indexSizeOnDiskMB,
          selectivity,
          prefilter ? "pre-filter" : "post-filter",
          vectorDiskSizeBytes / 1024. / 1024.,
          vectorRAMSizeBytes / 1024. / 1024.);
    }
  }

  private static double nsToSec(long ns) {
    return ns / (double) 1_000_000_000;
  }

  private static double msToSec(long ms) {
    return ms / (double) 1_000;
  }

  private static TopDocs doKnnByteVectorQuery(
    IndexSearcher searcher, String field, byte[] vector, int k, int fanout, Query filter)
    throws IOException {
    ProfiledKnnByteVectorQuery profiledQuery = new ProfiledKnnByteVectorQuery(field, vector, k, fanout, filter);
    TopDocs docs = searcher.search(profiledQuery, k);
    return new TopDocs(new TotalHits(profiledQuery.totalVectorCount(), docs.totalHits.relation()), docs.scoreDocs);
  }

  private static TopDocs doKnnVectorQuery(
      IndexSearcher searcher, String field, float[] vector, int k, int fanout, Query filter, boolean isParentJoinQuery)
      throws IOException {
    if (isParentJoinQuery) {
      ParentJoinBenchmarkQuery parentJoinQuery = new ParentJoinBenchmarkQuery(vector, null, k);
      return searcher.search(parentJoinQuery, k);
    }
    ProfiledKnnFloatVectorQuery profiledQuery = new ProfiledKnnFloatVectorQuery(field, vector, k, fanout, filter);
    TopDocs docs = searcher.search(profiledQuery, k);
    return new TopDocs(new TotalHits(profiledQuery.totalVectorCount(), docs.totalHits.relation()), docs.scoreDocs);
  }

  private float checkResults(int[][] results, int[][] nn) {
    int totalMatches = 0;
    int totalResults = results.length * topK;
    for (int i = 0; i < results.length; i++) {
      // System.out.println(Arrays.toString(nn[i]));
      // System.out.println(Arrays.toString(results[i].scoreDocs));
      totalMatches += compareNN(nn[i], results[i]);
    }
    return totalMatches / (float) totalResults;
  }

  private int compareNN(int[] expected, int[] results) {
    int matched = 0;
    Set<Integer> expectedSet = new HashSet<>();
    for (int i = 0; i < topK; i++) {
      expectedSet.add(expected[i]);
    }
    for (int docId : results) {
      if (expectedSet.contains(docId)) {
        ++matched;
      }
    }
    return matched;
  }

  /** Returns the topK nearest neighbors for each target query.
   *
   * The method runs "numQueryVectors" target queries and returns "topK" nearest neighbors
   * for each of them. Nearest Neighbors are computed using exact match.
   */
  private int[][] getExactNN(Path docPath, Path queryPath, int queryStartIndex) throws IOException, InterruptedException {
    // look in working directory for cached nn file
    String hash = Integer.toString(Objects.hash(docPath, queryPath, numDocs, numQueryVectors, topK, similarityFunction.ordinal(), parentJoin, queryStartIndex), 36);
    String nnFileName = "nn-" + hash + ".bin";
    Path nnPath = Paths.get(nnFileName);
    if (Files.exists(nnPath) && isNewer(nnPath, docPath, queryPath) && selectivity == 1f) {
      System.out.println("  read pre-cached exact match vectors from cache file \"" + nnPath + "\"");
      return readExactNN(nnPath);
    } else {
      System.out.println("  now compute brute-force exact KNN matches");
      long startNS = System.nanoTime();
      // TODO: enable computing NN from high precision vectors when
      // checking low-precision recall
      int[][] nn;
      if (vectorEncoding.equals(VectorEncoding.BYTE)) {
        nn = computeExactNNByte(docPath, queryPath, queryStartIndex);
      } else {
        nn = computeExactNN(docPath, queryPath, queryStartIndex);
      }
      if (selectivity == 1f) {
        writeExactNN(nn, nnPath);
      }
      long elapsedMS = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNS); // ns -> ms
      System.out.printf("took %.3f sec to compute brute-force exact matches\n", elapsedMS / 1000.);
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

  private int[][] readExactNN(Path nnPath) throws IOException {
    int[][] result = new int[numQueryVectors][];
    try (FileChannel in = FileChannel.open(nnPath)) {
      IntBuffer intBuffer =
          in.map(FileChannel.MapMode.READ_ONLY, 0, numQueryVectors * topK * Integer.BYTES)
              .order(ByteOrder.LITTLE_ENDIAN)
              .asIntBuffer();
      for (int i = 0; i < numQueryVectors; i++) {
        result[i] = new int[topK];
        intBuffer.get(result[i]);
      }
    }
    return result;
  }

  private void writeExactNN(int[][] nn, Path nnPath) throws IOException {
    if (quiet == false) {
      System.out.println("writing true nearest neighbors to cache file \"" + nnPath + "\"");
    }
    ByteBuffer tmp =
        ByteBuffer.allocate(nn[0].length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    try (OutputStream out = Files.newOutputStream(nnPath)) {
      for (int i = 0; i < numQueryVectors; i++) {
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

  private int[][] computeExactNNByte(Path docPath, Path queryPath, int queryStartIndex) throws IOException, InterruptedException {
    int[][] result = new int[numQueryVectors][];
    log("computing true nearest neighbors of " + numQueryVectors + " target vectors");
    List<ComputeNNByteTask> tasks = new ArrayList<>();
    try (FileChannel qIn = getVectorFileChannel(queryPath, dim, vectorEncoding)) {
      VectorReaderByte queryReader = (VectorReaderByte) VectorReader.create(qIn, dim, VectorEncoding.BYTE, queryStartIndex);
      for (int i = 0; i < numQueryVectors; i++) {
        byte[] query = queryReader.nextBytes().clone();
        tasks.add(new ComputeNNByteTask(i, query, docPath, result));
      }
    }
    ForkJoinPool.commonPool().invokeAll(tasks);
    return result;
  }

  class ComputeNNByteTask implements Callable<Void> {

    private final int queryOrd;
    private final byte[] query;
    private final Path docPath;
    private final int[][] result;

    ComputeNNByteTask(int queryOrd, byte[] query, Path docPath, int[][] result) {
      this.queryOrd = queryOrd;
      this.query = query;
      this.docPath = docPath;
      this.result = result;
    }

    @Override
    public Void call() {
        NeighborQueue queue = new NeighborQueue(topK, false);
        try (FileChannel in = FileChannel.open(docPath)) {
          // TODO: support docStartIndex here too
          VectorReaderByte docReader = (VectorReaderByte)VectorReader.create(in, dim, VectorEncoding.BYTE, 0);
          for (int j = 0; j < numDocs; j++) {
            byte[] doc = docReader.nextBytes();
            float d = similarityFunction.compare(query, doc);
            if (d == 0f) {
              System.out.println("WARNING: identical doc and query vector (distance=0)");
            }
            if (matchDocs == null || matchDocs.get(j)) {
              queue.insertWithOverflow(j, d);
            }
          }
          docReader.reset();
          result[queryOrd] = new int[topK];
          for (int k = topK - 1; k >= 0; k--) {
            result[queryOrd][k] = queue.topNode();
            queue.pop();
          }
          if (quiet == false && (queryOrd + 1) % 10 == 0) {
            log(" " + (queryOrd + 1));
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return null;
    }
  }

  /** Brute force computation of "true" nearest neighhbors. */
  private int[][] computeExactNN(Path docPath, Path queryPath, int queryStartIndex)
      throws IOException, InterruptedException {
    int[][] result = new int[numQueryVectors][];
    log("computing true nearest neighbors of " + numQueryVectors + " target vectors");
    log("parentJoin = %s", parentJoin);
    if (parentJoin) {
      try (Directory dir = FSDirectory.open(indexPath);
           DirectoryReader reader = DirectoryReader.open(dir)) {
        CheckJoinIndex.check(reader, ParentJoinBenchmarkQuery.parentsFilter);
        List<ComputeExactSearchNNFloatTask> tasks = new ArrayList<>();
        try (FileChannel qIn = getVectorFileChannel(queryPath, dim, vectorEncoding)) {
          VectorReader queryReader = (VectorReader) VectorReader.create(qIn, dim, VectorEncoding.FLOAT32, queryStartIndex);
          for (int i = 0; i < numQueryVectors; i++) {
            float[] query = queryReader.next().clone();
            tasks.add(new ComputeExactSearchNNFloatTask(i, query, docPath, result, reader));
          }
        }
        ForkJoinPool.commonPool().invokeAll(tasks);
      }
    } else {
      // TODO: Use exactSearch here?
      // System.out.println("common pool uses " + ForkJoinPool.getCommonPoolParallelism() + " threads");
      System.out.println("now compute brute-force KNN hits for " + numQueryVectors + " query vectors from \"" + queryPath + "\" starting at query index " + queryStartIndex);
      List<ComputeNNFloatTask> tasks = new ArrayList<>();
      try (FileChannel qIn = getVectorFileChannel(queryPath, dim, vectorEncoding)) {
        VectorReader queryReader = (VectorReader) VectorReader.create(qIn, dim, VectorEncoding.FLOAT32, queryStartIndex);
        for (int i = 0; i < numQueryVectors; i++) {
          float[] query = queryReader.next().clone();
          tasks.add(new ComputeNNFloatTask(i, query, docPath, result));
        }
      }
      ForkJoinPool.commonPool().invokeAll(tasks);
    }
    return result;
  }

  // TODO: would it be faster to swap the stride?  for each indexed vector, run through all query vectors updating their
  // separate PQs?  better locality since we load each indexed vector just once and share it?  we would make thread work units
  // from chunks of indexed vectors?
  class ComputeNNFloatTask implements Callable<Void> {

    private final int queryOrd;
    private final float[] query;
    private final Path docPath;
    private final int[][] result;

    ComputeNNFloatTask(int queryOrd, float[] query, Path docPath, int[][] result) {
      this.queryOrd = queryOrd;
      this.query = query;
      this.docPath = docPath;
      this.result = result;
    }

    @Override
    public Void call() {
      NeighborQueue queue = new NeighborQueue(topK, false);
      // TODO: support docStartIndex here too
      try (FileChannel in = FileChannel.open(docPath)) {
        VectorReader docReader = (VectorReader) VectorReader.create(in, dim, VectorEncoding.FLOAT32, 0);
        for (int j = 0; j < numDocs; j++) {
          float[] doc = docReader.next();
          float d = similarityFunction.compare(query, doc);
          if (matchDocs == null || matchDocs.get(j)) {
            if (d == 0f) {
              System.out.println("WARNING: identical doc and query vector (distance=0)");
            }
            queue.insertWithOverflow(j, d);
          }
        }
        docReader.reset();
        result[queryOrd] = new int[topK];
        for (int k = topK - 1; k >= 0; k--) {
          result[queryOrd][k] = queue.topNode();
          queue.pop();
        }
        if ((queryOrd + 1) % 10 == 0) {
          log(" " + (queryOrd + 1));
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    }
  }

  /** Uses ExactSearch from Lucene queries to compute nearest neighbors.
   */
  class ComputeExactSearchNNFloatTask implements Callable<Void> {

    private final int queryOrd;
    private final float[] query;
    private final Path docPath;
    private final int[][] result;
    private final IndexReader reader;

    ComputeExactSearchNNFloatTask(int queryOrd, float[] query, Path docPath, int[][] result, IndexReader reader) {
      this.queryOrd = queryOrd;
      this.query = query;
      this.docPath = docPath;
      this.result = result;
      this.reader = reader;
    }

    @Override
    public Void call() {
      // we only use this for ParentJoin benchmarks right now, TODO: extend for all computeExactNN needs.
      try {
        ParentJoinBenchmarkQuery parentJoinQuery = new ParentJoinBenchmarkQuery(query, null, topK);
        TopDocs topHits = ParentJoinBenchmarkQuery.runExactSearch(reader, parentJoinQuery);
        StoredFields storedFields = reader.storedFields();
        result[queryOrd] = KnnTesterUtils.getResultIds(topHits, storedFields);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    }
  }

  private void log(String msg, Object... args) {
    if (quiet == false) {
      System.out.printf((msg) + "\n", args);
      System.out.flush();
    }
  }

  static Codec getCodec(int maxConn, int beamWidth, ExecutorService exec, int numMergeWorker, boolean quantize, int quantizeBits, boolean quantizeCompress) {
    if (exec == null) {
      return new Lucene101Codec() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
          if (quantize) {
            if (quantizeBits == 1) {
              return new HnswBitVectorsFormat(maxConn, beamWidth, numMergeWorker, null);
            } else {
              return new Lucene99HnswScalarQuantizedVectorsFormat(maxConn, beamWidth, numMergeWorker, quantizeBits, quantizeCompress, null, null);
            }
          } else {
            return new Lucene99HnswVectorsFormat(maxConn, beamWidth, numMergeWorker, null);
          }
        }
      };
    } else {
      return new Lucene101Codec() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
          if (quantize) {
            if (quantizeBits == 1) {
              return new HnswBitVectorsFormat(maxConn, beamWidth, numMergeWorker, exec);
            } else {
              return new Lucene99HnswScalarQuantizedVectorsFormat(maxConn, beamWidth, numMergeWorker, quantizeBits, quantizeCompress, null, exec);
            }
          } else {
            return new Lucene99HnswVectorsFormat(maxConn, beamWidth, numMergeWorker, exec);
          }
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
      totalVectorCount = td.totalHits.value();
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
      totalVectorCount = td.totalHits.value();
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
