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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
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
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.IntIntHashMap;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.valuesource.ByteKnnVectorFieldSource;
import org.apache.lucene.queries.function.valuesource.ByteVectorSimilarityFunction;
import org.apache.lucene.queries.function.valuesource.ConstKnnByteVectorValueSource;
import org.apache.lucene.queries.function.valuesource.ConstKnnFloatValueSource;
import org.apache.lucene.queries.function.valuesource.FloatKnnVectorFieldSource;
import org.apache.lucene.queries.function.valuesource.FloatVectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.CheckJoinIndex;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.hnsw.HnswGraph;

import perf.SearchPerfTest.ThreadDetails;
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
public class KnnGraphTester implements FormatterLogger {

  enum IndexType {
    HNSW,
    FLAT
  }

  enum FilterStrategy {
    QUERY_TIME_PRE_FILTER,
    QUERY_TIME_POST_FILTER,
    INDEX_TIME_FILTER,
    INDEX_TIME_SPARSE
  }

  private static final Set<Integer> ALLOWED_BITS = Set.of(1, 2, 4, 7, 8);

  public static final String KNN_FIELD = "knn";
  public static final String KNN_FIELD_FILTERED = "knn-filtered";
  public static final String ID_FIELD = "id";
  private static final String INDEX_DIR = "knnIndices";
  public static final String DOCTYPE_FIELD = "docType";
  public static final String DOCTYPE_PARENT = "_parent";
  public static final String DOCTYPE_CHILD = "_child";
  public static final String WIKI_ID_FIELD = "wikiID";
  public static final String WIKI_PARA_ID_FIELD = "wikiParaID";

  public static final BitSetProducer parentsFilter =
    new QueryBitSetProducer(new TermQuery(new Term(DOCTYPE_FIELD, DOCTYPE_PARENT)));
  private Long randomSeed;
  private Random random;
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
  private int numQueryThread;
  private int numSearchThread;
  private ExecutorService exec;
  private VectorSimilarityFunction similarityFunction;
  private VectorEncoding vectorEncoding;
  private Query filterQuery;
  private FilterStrategy filterStrategy;
  private Float filterSelectivity;
  private boolean randomCommits;
  private boolean parentJoin;
  private Path parentJoinMetaFile;
  private int numIndexThreads;
  // which query vector to seek to at the start
  private int queryStartIndex;
  // whether to reorder the index using binary partitioning
  private boolean useBp;
  // index type, e.g. flat, hnsw
  private IndexType indexType;
  // oversampling, e.g. the multiple * k to gather before checking recall
  private float overSample;

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
    filterStrategy = null;
    filterSelectivity = null;
    quantize = false;
    randomCommits = false;
    quantizeBits = 7;
    quantizeCompress = false;
    numIndexThreads = 8;
    numQueryThread = 0;
    numSearchThread = 0;
    queryStartIndex = 0;
    indexType = IndexType.HNSW;
    overSample = 1f;
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

    // matching default similarityFunction from ctor:
    assert similarityFunction == VectorSimilarityFunction.DOT_PRODUCT;
    String metric = "dot_product";

    boolean explicitIndexPath = false;
    
    Path docVectorsPath = null, queryPath = null, outputPath = null;
    quiet = Arrays.asList(args).contains("-quiet");
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
        case "-indexType":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-indexType requires a following pathname");
          }
          String indexKind = args[++iarg].toLowerCase().trim();
          switch (indexKind) {
            case "hnsw":
              indexType = IndexType.HNSW;
              break;
            case "flat":
              indexType = IndexType.FLAT;
              break;
            default:
              throw new IllegalArgumentException("-indexType can be 'hnsw' or 'flat' only");
          }
          break;
        case "-overSample":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-overSample requires a following float");
          }
          overSample = Float.parseFloat(args[++iarg]);
          if (overSample < 1) {
            throw new IllegalArgumentException("-overSample must be >= 1");
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
          log("beamWidth = %d\n", beamWidth);
          break;
        case "-queryStartIndex":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-queryStartIndex requires a following number");
          }
          queryStartIndex = Integer.parseInt(args[++iarg]);
          log("queryStartIndex = %d\n", queryStartIndex);
          break;
        case "-maxConn":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-maxConn requires a following number");
          }
          maxConn = Integer.parseInt(args[++iarg]);
          log("maxConn = %d\n", maxConn);
          break;
        case "-dim":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-dim requires a following number");
          }
          dim = Integer.parseInt(args[++iarg]);
          log("Vector Dimensions: %d\n", dim);
          break;
        case "-ndoc":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-ndoc requires a following number");
          }
          numDocs = Integer.parseInt(args[++iarg]);
          log("numDocs = %d\n", numDocs);
          break;
        case "-niter":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-niter requires a following number");
          }
          numQueryVectors = Integer.parseInt(args[++iarg]);
          log("numQueryVectors = %d\n", numQueryVectors);
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
          if (ALLOWED_BITS.contains(quantizeBits) == false) {
            throw new IllegalArgumentException("-quantizeBits must be 1, 2, 4, 7 or 8");
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
          explicitIndexPath = true;
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
          metric = args[++iarg];
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
          log("similarity = %s\n", similarityFunction);
          break;
        case "-forceMerge":
          forceMerge = true;
          break;
        case "-randomCommits":
          randomCommits = true;
          break;
        case "-filterStrategy":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-filterStrategy requires a following string, one of (case-insensitive) {'query-time-pre-filter', 'query-time-post-filter', 'index-time-filter'}");
          }
          String filterStrategyVal = args[++iarg].toLowerCase().trim();
          filterStrategy = switch (filterStrategyVal) {
            case "query-time-pre-filter" -> FilterStrategy.QUERY_TIME_PRE_FILTER;
            case "query-time-post-filter" -> FilterStrategy.QUERY_TIME_POST_FILTER;
            case "index-time-filter" -> FilterStrategy.INDEX_TIME_FILTER;
            case "index-time-sparse" -> FilterStrategy.INDEX_TIME_SPARSE;
            default -> throw new IllegalArgumentException("-filterStrategy must be one of (case-insensitive) {'query-time-pre-filter', 'query-time-post-filter', 'index-time-filter'}, found: " + filterStrategyVal);
          };
          break;
        case "-filterSelectivity":
          if (iarg == args.length - 1) {
            throw new IllegalArgumentException("-filterSelectivity requires a following float");
          }
          filterSelectivity = Float.parseFloat(args[++iarg]);
          if (filterSelectivity <= 0 || filterSelectivity >= 1) {
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
        case "-numQueryThread":
          // 0: single thread mode (not passing a executorService)
          // -1: use number of threads equal to the number available processors
          // Otherwise: create a fixed pool with determined number of threads.
          numQueryThread = Integer.parseInt(args[++iarg]);
          if (numQueryThread == -1) {
            numQueryThread = Runtime.getRuntime().availableProcessors();
          } else if (numQueryThread < 0) {
            throw new IllegalArgumentException("-numQueryThread must be >= 0");
          }
          log("numQueryThread = %d\n", numQueryThread);
          break;
        case "-numSearchThread":
          // 0: single thread mode (not passing a executorService) 
          // -1: use number of threads equal to the number available processors
          // Otherwise: create a fixed pool with determined number of threads.
          numSearchThread = Integer.parseInt(args[++iarg]);
          if (numSearchThread == -1) {
            numSearchThread = Runtime.getRuntime().availableProcessors();
          } else if (numSearchThread < 0) {
            throw new IllegalArgumentException("-numSearchThread must be >= 0");
          }
          log("numSearchThread = %d\n", numSearchThread);
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
        case "-seed":
          randomSeed = Long.parseLong(args[++iarg]);
          break;
        default:
          throw new IllegalArgumentException("unknown argument " + arg);
          // usage();
      }
    }
    if (operation == null && reindex == false) {
      usage();
    }

    Bits filtered = null;
    if (filterStrategy != null && filterSelectivity == null || filterStrategy == null && filterSelectivity != null) {
      throw new IllegalArgumentException("Either both or none of -filterStrategy or -filterSelectivity should be specified");
    }
    if (parentJoin && reindex == false && isParentJoinIndex(indexPath) == false) {
      throw new IllegalArgumentException("Provided index: [" + indexPath + "] does not have parent-child " +
          "document relationships. Rerun with -reindex or without -parentJoin argument");
    }
    if (randomSeed == null) {
      randomSeed = System.nanoTime();
    }
    log("Seed = %d\n", randomSeed);
    random = new Random(randomSeed);

    if (filterSelectivity != null) {
      filtered = selectRandomDocs(random, numDocs, filterSelectivity);
    }

    String indexKey = formatIndexKey(indexType, maxConn, beamWidth, useBp,
                                     quantize, quantizeBits, quantizeCompress,
                                     parentJoin, filterStrategy, filterSelectivity, randomSeed,
                                     docVectorsPath, numDocs, metric, forceMerge);
    log("index key = %s\n", indexKey);
    
    if (indexPath == null) {
      // use indexKey for path so that we reindex if anything volatile to indexing changes:
      indexPath = Paths.get("knn-reuse/indices/" + indexKey);
    }
    log("Index Path = %s\n", indexPath);
    
    Path indexKeyPath = indexPath.resolve("index-key.txt");

    // if user passed their own indexPath, we validate that the key is correct:
    String prevIndexKey;
    if (Files.exists(indexKeyPath)) {
      prevIndexKey = Files.readString(indexKeyPath);
    } else {
      prevIndexKey = null;
    }

    String reindexReason;
    if (reindex) {
      reindexReason = "explicitly requested";
    } else if (Files.exists(indexPath) == false) {
      reindexReason = "index does not exist";
    } else if (prevIndexKey == null) {
      reindexReason = "index key file does not exist";
    } else if (prevIndexKey.equals(indexKey) == false) {
      reindexReason = String.format(Locale.ROOT, "index key changed:\n  old: %s\n  new: %s", prevIndexKey, indexKey);
    } else {
      // we will not reindex
      reindexReason = null;
    }

    int segmentCount;
    
    if (reindexReason != null) {

      if (explicitIndexPath && reindex == false) {
        // paranoia: do not suddenly up and delete the provided -indexPath -- ask user to do so manually
        throw new IllegalStateException("expliict index path (" + indexPath + ") provided, but we need to reindex: " + reindexReason + "; please pre-remove this index and re-run");
      }
      
      log("will now reindex: " + reindexReason + "\n");
      
      if (docVectorsPath == null) {
        throw new IllegalArgumentException("-docs argument is required when indexing");
      }

      KnnIndexer.FilterScheme indexTimeFilter;

      if (filterStrategy == null) {
        indexTimeFilter = null;
      } else if (filterStrategy == FilterStrategy.INDEX_TIME_FILTER) {
        indexTimeFilter = new KnnIndexer.FilterScheme(filtered, true);
      } else if (filterStrategy == FilterStrategy.INDEX_TIME_SPARSE) {
        indexTimeFilter = new KnnIndexer.FilterScheme(filtered, false);
      } else {
        indexTimeFilter = null;
      }

      reindexTimeMsec = new KnnIndexer(
        docVectorsPath,
        indexPath,
        getCodec(maxConn, beamWidth, exec, numMergeWorker, quantize, quantizeBits, indexType),
        numIndexThreads,
        vectorEncoding,
        dim,
        similarityFunction,
        numDocs,
        0,
        quiet,
        parentJoin,
        parentJoinMetaFile,
        useBp,
        indexTimeFilter
      ).createIndex();
      Files.writeString(indexKeyPath, indexKey);
      log("reindex takes %.2f sec\n", msToSec(reindexTimeMsec));
      // save indexing time so future runs that re-use this index remember:
      Files.writeString(indexTimePath(indexPath), String.valueOf(reindexTimeMsec));
      segmentCount = -1;
    } else {
      // paranoia: let's sanity check:
      try (Directory dir = FSDirectory.open(indexPath);
           IndexReader reader = DirectoryReader.open(dir)) {
        if (reader.numDocs() != numDocs) {
          throw new IllegalStateException("previously created index \"" + indexPath + "\" has wrong numDocs=" + reader.numDocs() + "; expected " + numDocs);
        }
        segmentCount = reader.leaves().size();
      }

      String s;
      Path path = indexTimePath(indexPath);
      try {
        s = Files.readString(path);
      } catch (NoSuchFileException nsfe) {
        // ok -- back compat: old index that didn't write indexing time
        log("WARNING: index did not record index time msec in %s\n", path);
        s = null;
      }
      if (s != null) {
        log("retrieving previously saved indexing time in %s\n", path);
        reindexTimeMsec = Integer.parseInt(s);
        log("read previously saved indexing time: %d msec\n", reindexTimeMsec);
      } else {
        reindexTimeMsec = -1;
      }
    }
    
    if (forceMerge) {
      if (segmentCount == 1) {
        log("skip force-merge: index already has 1 segment\n");
        String s;
        Path path = forceMergeTimePath(indexPath);
        try {
          s = Files.readString(path);
        } catch (NoSuchFileException nsfe) {
          // ok -- back compat: old index that didn't write force merge time
          log("WARNING: index did not record force-merge-time seconds in %s\n", path);
          s = null;
        }
        if (s != null) {
          log("retrieving previously saved force merge time in %s\n", path);
          forceMergeTimeSec = Double.parseDouble(s);
          log("previously saved force merge time: %g sec\n", forceMergeTimeSec);
        } else {
          forceMergeTimeSec = -1;
        }
      } else {
        forceMergeTimeSec = forceMerge();
        // save force merge time so future runs that re-use this index remember:
        Files.writeString(forceMergeTimePath(indexPath), String.valueOf(forceMergeTimeSec));
      }
    }
    if (filterStrategy == FilterStrategy.INDEX_TIME_SPARSE) {
      printIndexStatistics(indexPath, KNN_FIELD_FILTERED);
    } else {
      printIndexStatistics(indexPath, KNN_FIELD);
    }
    if (operation != null) {
      switch (operation) {
        case "-search":
        case "-search-and-stats":
          if (docVectorsPath == null) {
            throw new IllegalArgumentException("missing -docs arg");
          }
          if (filterSelectivity != null) {
            filterQuery = createFilterQuery(indexPath, filtered);
          }
          if (outputPath != null) {
            testSearch(indexPath, queryPath, queryStartIndex, outputPath, null);
          } else {
            testSearch(indexPath, queryPath, queryStartIndex, null, getExactNN(docVectorsPath, indexPath, queryPath, queryStartIndex, metric));
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

  // we save forcemerge elapsed time sec to this file inside the index
  private static Path forceMergeTimePath(Path indexPath) {
    return indexPath.resolve("force-merge-time-sec.txt");
  }

  // we save indexing elapsed time sec to this file inside the index
  private static Path indexTimePath(Path indexPath) {
    return indexPath.resolve("index-time-msec.txt");
  }

  private void printIndexStatistics(Path indexPath, String field) throws IOException {
    try (Directory dir = FSDirectory.open(indexPath);
         IndexReader reader = DirectoryReader.open(dir)) {
      indexNumSegments = reader.leaves().size();
      log("index has %d segments: %s\n", indexNumSegments, ((StandardDirectoryReader) reader).getSegmentInfos());

      if (indexNumSegments == 1 && numSearchThread > 1) {
        log("WARNING: intra-query concurrency requested (-numSearchThread=%d) but index has only one segment!\n", numSearchThread);
      }
          
      long indexSizeOnDiskBytes = 0;
      SegmentInfos infos = ((StandardDirectoryReader) reader).getSegmentInfos();
      for (SegmentCommitInfo info : infos) {
        long segSizeOnDiskBytes = 0;
        for (String fileName : info.files()) {
          segSizeOnDiskBytes += dir.fileLength(fileName);
        }
        indexSizeOnDiskBytes += segSizeOnDiskBytes;
        log("  %s: %.1f MB\n", info.info.name, segSizeOnDiskBytes / 1024. / 1024.);
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
              ByteVectorValues vectors = knnReader.getByteVectorValues(field);
              segEncodingByteSize = vectors.getEncoding().byteSize;
              totalVectorCount += vectors.size();
              break;
            }
          case FLOAT32:
            {
              FloatVectorValues vectors = knnReader.getFloatVectorValues(field);
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
      log("encodingByteSize=" + encodingByteSize + " origByteSize=" + origByteSize + "\n");

      // TODO: why is encodingByteSize 4 even for int4/int7 cases?
      double realEncodingByteSize;
      double overHead = 0;
      if (quantize) {
        if (quantizeBits == 1) {
          realEncodingByteSize = 1.0 / 8.0;
          overHead = Float.BYTES * 3 + Short.BYTES; // 3 floats & 1 short
        } else if (quantizeBits == 2) {
          realEncodingByteSize = 2.0 / 8.0;
          overHead = Float.BYTES * 3 + Short.BYTES; // 3 floats & 1 short
        } else if (quantizeBits == 4 || quantizeBits == 7 || quantizeBits == 8) {
          // upper, lower, additional correction (similarity dependent), component sum.
          overHead = Float.BYTES * 3 + Integer.BYTES;
          if (quantizeBits == 4) {
            realEncodingByteSize = 0.5;
          } else {
            realEncodingByteSize = 1;
          }
        } else {
          throw new IllegalStateException("can only handle int1, int4, int7, and int8 quantized");
        }
      } else {
        realEncodingByteSize = Float.BYTES;
      }
      log("realEncodingByteSize=" + realEncodingByteSize + "\n");

      vectorRAMSizeBytes = (long) ((double) totalVectorCount * (realEncodingByteSize * dim + overHead));
      if (quantize) {
        vectorDiskSizeBytes = (long) ((double) totalVectorCount * ((long)origByteSize * dim)) + vectorRAMSizeBytes;
      } else {
        vectorDiskSizeBytes = (long) ((double) totalVectorCount * (realEncodingByteSize * dim));
      }
      indexSizeOnDiskMB = indexSizeOnDiskBytes / 1024. / 1024.;
      log("index disk usage is %.2f MB\n", indexSizeOnDiskMB);
      log("vector disk usage is %.2f MB\n", vectorDiskSizeBytes/1024./1024.);
      log("vector RAM usage is %.2f MB\n", vectorRAMSizeBytes/1024./1024.);
    }
  }

  private static Bits selectRandomDocs(Random random, int size, float selectivity) {
    FixedBitSet bitSet = new FixedBitSet(size);
    for (int i = 0; i < size; i++) {
      if (random.nextFloat() < selectivity) {
        bitSet.set(i);
      } else {
        bitSet.clear(i);
      }
    }
    return bitSet;
  }

  private static Query createFilterQuery(Path indexPath, Bits bitSet) throws IOException {
    try (Directory dir = FSDirectory.open(indexPath);
         DirectoryReader reader = DirectoryReader.open(dir)) {
      BitSet[] segmentDocs = new BitSet[reader.leaves().size()];
      for (var leafContext : reader.leaves()) {
        var storedFields = leafContext.reader().storedFields();
        FixedBitSet segmentBitSet = new FixedBitSet(reader.maxDoc());
        for (int d = 0; d < leafContext.reader().maxDoc(); d++) {
          int docID = Integer.parseInt(storedFields.document(d, Set.of(KnnGraphTester.ID_FIELD)).get(KnnGraphTester.ID_FIELD));
          if (bitSet.get(docID)) {
            segmentBitSet.set(d);
          }
        }
        segmentDocs[leafContext.ord] = segmentBitSet;
      }
      return new BitSetQuery(segmentDocs);
    }
  }

  // static so we are forced to pass in all things that are volatile wrt indexing (if they change, it requires reindexing)
  private static String formatIndexKey(IndexType indexType, int maxConn, int beamWidth,
                                       boolean useBp,
                                       boolean quantize, int quantizeBits, boolean quantizeCompress,
                                       boolean parentJoin, FilterStrategy filterStrategy,
                                       Float filterSelectivity, Long randomSeed,
                                       Path docPath, int numDocs, String metric, boolean forceMerge)
    throws IOException {

    List<String> suffix = new ArrayList<>();

    // if vector similarity distance (dot-product, mip, cosine) changes, reindex:
    suffix.add(metric);

    // if docs source or number of docs changes, reindex:
    suffix.add("i" + docPath.getFileName());

    // include mod time of docs source so if it's rewritten we know to reindex:
    suffix.add(Long.toString(Files.getLastModifiedTime(docPath).toMillis()));

    suffix.add(Integer.toString(numDocs));

    // with index-time filters we compute and store filter-dependent HNSW graph, so we must reindex if that filter changes.  if
    // it is a query-time filter we don't reindex:
    if (filterStrategy == FilterStrategy.INDEX_TIME_FILTER) {
      suffix.add(filterStrategy.toString());
      suffix.add(filterSelectivity.toString());
      // random seed is used to pick which exact vectors are accepted by the filter -- we could in theory instead encode the full
      // bitset but that's (usually?) too long:
      suffix.add(String.valueOf(randomSeed));
    }
    
    if (indexType == IndexType.FLAT) {
      suffix.add("flat");
    } else {
      // if HNSW hyperparams change, or bg (vector reordering) is enabled, reindex:
      suffix.add(Integer.toString(maxConn));
      suffix.add(Integer.toString(beamWidth));
      if (useBp) {
        suffix.add("bp");
      }
    }
    if (quantize) {
      suffix.add(Integer.toString(quantizeBits));
      // TODO: is this still allowed -- I thought we've wired quantizeCompress=true when quantize=true?
      if (quantizeCompress) {
        suffix.add("compressed");
      }
    }

    if (parentJoin) {
      suffix.add("parentJoin");
    }

    if (forceMerge) {
      suffix.add("fm");
    } else {
      suffix.add("notfm");
    }

    return String.join("-", suffix);
  }

  // static so we are forced to pass in all things that are volatile wrt indexing (if they change, it requires reindexing)
  private static String formatExactNNKey(boolean parentJoin, FilterStrategy filterStrategy, Float filterSelectivity,
                                         Long randomSeed, Path docPath, Path queryVectorsPath, int numDocs, String metric,
                                         int numQueryVectors, int queryStartIndex, int topK) {
    List<String> suffix = new ArrayList<>();
    suffix.add(metric);

    suffix.add("i" + docPath.getFileName());
    suffix.add(Integer.toString(numDocs));

    suffix.add("q" + queryVectorsPath.getFileName());
    suffix.add(Integer.toString(numQueryVectors));

    if (queryStartIndex != 0) {
      suffix.add("qs" + queryStartIndex);
    }

    suffix.add(metric);

    if (parentJoin) {
      suffix.add("parentJoin");
    }

    if (filterStrategy == FilterStrategy.INDEX_TIME_FILTER) {
      suffix.add(filterStrategy.toString());
      suffix.add(filterSelectivity.toString());
      suffix.add(String.valueOf(randomSeed));
    }
    
    return docPath.getFileName() + "-" + String.join("-", suffix);
  }

  private boolean isParentJoinIndex(Path indexPath) {
    return indexPath.toString().contains("parentJoin");
  }

  private void printFanoutHist(Path indexPath) throws IOException {
    if (filterStrategy == FilterStrategy.INDEX_TIME_SPARSE) {
      printFanoutHist(indexPath, KNN_FIELD_FILTERED);
    } else {
      printFanoutHist(indexPath, KNN_FIELD);
    }
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void printFanoutHist(Path indexPath, String field) throws IOException {
    if (indexType == IndexType.FLAT) {
      log("flat has no graphs\n");
      return;
    }
    try (Directory dir = FSDirectory.open(indexPath);
         DirectoryReader reader = DirectoryReader.open(dir)) {
      for (LeafReaderContext context : reader.leaves()) {
        LeafReader leafReader = context.reader();
        KnnVectorsReader vectorsReader = ((CodecReader) leafReader).getVectorReader().unwrapReaderForField(field);
        HnswGraph knnValues;
        if (vectorsReader instanceof Lucene99HnswVectorsReader hnswVectorsReader) {
          knnValues = hnswVectorsReader.getGraph(field);
        } else {
          throw new IllegalStateException("unsupported vectors reader: " + vectorsReader.getClass().getName());
        }

        log("Leaf %d has %d layers\n", context.ord, knnValues.numLevels());
        log("Leaf %d has %d documents\n", context.ord, leafReader.maxDoc());
        printGraphFanout(knnValues, leafReader.maxDoc());
        printGraphConnectedNess(knnValues);
      }
    }
  }

  @SuppressForbidden(reason = "Prints stuff")
  private double forceMerge() throws IOException, InterruptedException {
    IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.APPEND);
    iwc.setCodec(getCodec(maxConn, beamWidth, exec, numMergeWorker, quantize, quantizeBits, indexType));
    KnnIndexer.TrackingConcurrentMergeScheduler tcms = new KnnIndexer.TrackingConcurrentMergeScheduler();
    iwc.setMergeScheduler(tcms);
    KnnIndexer.TrackingTieredMergePolicy ttmp = new KnnIndexer.TrackingTieredMergePolicy();
    iwc.setMergePolicy(ttmp);
    log("Force merge index in " + indexPath + "\n");
    long startNS = System.nanoTime();
    try (IndexWriter iw = new IndexWriter(FSDirectory.open(indexPath), iwc)) {
      iw.forceMerge(1, false);
      KnnIndexer.waitForMergesWithStatus(ttmp, tcms, this);
    }
    long endNS = System.nanoTime();
    double elapsedSec = nsToSec(endNS - startNS);
    log("Force merge done in %.2f sec\n", elapsedSec);
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
      log("Graph level=%d size=%d, connectedness=%.2f\n",
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
      log("Graph level=%d size=%d, Fanout min=%d, mean=%.2f, max=%d, meandelta=%.2f\n",
          level, count, min, total / (float) count, max, sumDelta / (float) total);
      if (!quiet) {
        printHist(leafHist, max, count, 10);
      }
    }
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void printHist(int[] hist, int max, int count, int nbuckets) {
    System.out.print("%");
    for (int i = 0; i <= nbuckets; i++) {
      log("%4d", i * 100 / nbuckets);
    }
    log("\n %4d", hist[0]);
    int total = 0, ibucket = 1;
    for (int i = 1; i <= max && ibucket <= nbuckets; i++) {
      total += hist[i];
      while (total >= count * ibucket / nbuckets  && ibucket <= nbuckets) {
        log("%4d", i);
        ++ibucket;
      }
    }
    log("\n");
  }

  @SuppressForbidden(reason = "Prints stuff")
  private void testSearch(Path indexPath, Path queryPath, int queryStartIndex, Path outputPath, int[][] nn)
      throws IOException {
    int[][] resultIds = new int[numQueryVectors][];
    long elapsedMS, totalCpuTimeMS, totalVisited = 0;
    ExecutorService queryExecutor = null;
    if (numQueryThread > 0) {
      queryExecutor = Executors.newFixedThreadPool(numQueryThread, new NamedThreadFactory("query-dispatch"));
    }

    ExecutorService searchExecutor = null;
    if (numSearchThread > 0) {
      searchExecutor = Executors.newFixedThreadPool(numSearchThread, new NamedThreadFactory("search-worker"));
    }
    try (VectorReader<?> targetReader = VectorReader.create(queryPath, dim, vectorEncoding)) {
      log(targetReader.getVectorCount() + " query vectors in queryPath \"" + queryPath + "\"\n");
      log("searching %d query vectors; topK=%d, fanout=%d\n", numQueryVectors, topK, fanout);
      long startNS;
      try (MMapDirectory dir = new MMapDirectory(indexPath)) {
        // TODO: hmm dangerous since index isn't necessarily going to fit in RAM?
        dir.setPreload((x, ctx) -> x.endsWith(".vec") || x.endsWith(".veq"));
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          IndexSearcher searcher = new IndexSearcher(reader, searchExecutor);
          int indexNumDocs = reader.maxDoc();
          if (numDocs != indexNumDocs && !parentJoin) {
            throw new IllegalStateException("index size mismatch, expected " + numDocs + " but index has " + indexNumDocs);
          }

          List<IOSupplier<TopDocs>> queries =
              IntStream.range(queryStartIndex, queryStartIndex + numQueryVectors)
              .<IOSupplier<TopDocs>>mapToObj(i -> () -> doKnnVectorQuery(i, targetReader, searcher))
              .toList();

          // warm up
          invokeAllAsync(queries, queryExecutor).join();
          log("done warmup\n");
          startNS = System.nanoTime();
          ThreadDetails startThreadDetails = new ThreadDetails();
          List<TopDocs> results = invokeAllAsync(queries, queryExecutor).join();
          ThreadDetails endThreadDetails = new ThreadDetails();
          perf.SearchPerfTest.ElapsedMSAndCoreCount elapsed = endThreadDetails.subtract(startThreadDetails);
          elapsedMS = TimeUnit.NANOSECONDS.toMillis(endThreadDetails.ns - startThreadDetails.ns);
          if (elapsed != null) {
            totalCpuTimeMS = (long) (elapsed.avgCPUCount() * elapsedMS);
          } else {
            totalCpuTimeMS = -1;
          }
          
          // Fetch, validate and write result document ids.
          StoredFields storedFields = reader.storedFields();
          for (int i = 0; i < numQueryVectors; i++) {
            totalVisited += results.get(i).totalHits.value();
            resultIds[i] = KnnTesterUtils.getResultIds(results.get(i), storedFields);
          }
          log(
              "completed "
              + numQueryVectors
              + " searches in "
              + elapsedMS
              + " ms: "
              + ((1000 * numQueryVectors) / elapsedMS)
              + " QPS "
              + "CPU time="
              + totalCpuTimeMS
              + " ms\n");
        }
      }
    } finally {
      if (queryExecutor != null) {
        queryExecutor.shutdown();
      }
      if (searchExecutor != null) {
        searchExecutor.shutdown();
      }
    }

    // TODO: why is this either/or?  couldn't i save output and also check recall?
    if (outputPath != null) {
      ByteBuffer tmp =
        ByteBuffer.allocate(resultIds[0].length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      try (OutputStream out = Files.newOutputStream(outputPath)) {
        for (int i = 0; i < numQueryVectors; i++) {
          // TODO: hmm isn't nn null in this case (from up above how we call tesSearch...)?  isn't
          // the intention to write the topK returned (via index's approximate KNN impl) vectors
          // for each query vector?  should nn be resultIds below?  confused...
          tmp.asIntBuffer().put(nn[i]);
          out.write(tmp.array());
        }
      }
    } else {
      log("checking results\n");
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
          "SUMMARY: %5.3f\t%5.3f\t%5.3f\t%5.3f\t%d\t%d\t%d\t%d\t%d\t%s\t%d\t%.2f\t%.2f\t%.2f\t%d\t%.2f\t%s\t%s\t%5.3f\t%5.3f\t%5.3f\t%s\t%s\n",
          recall,
          elapsedMS / (float) numQueryVectors,
          totalCpuTimeMS / (float) numQueryVectors,
          totalCpuTimeMS / (float) elapsedMS,
          numDocs,
          this.topK,
          this.fanout,
          maxConn,
          beamWidth,
          quantizeDesc,
          totalVisited,
          reindexSec,
          numDocs / reindexSec,
          forceMergeTimeSec,
          indexNumSegments,
          indexSizeOnDiskMB,
          filterStrategy == null ? "null" : filterStrategy.toString().toLowerCase().replace('_', '-'),
          filterSelectivity == null ? "N/A" : String.format(Locale.ROOT, "%.2f", filterSelectivity),
          overSample,
          vectorDiskSizeBytes / 1024. / 1024.,
          vectorRAMSizeBytes / 1024. / 1024.,
          Boolean.valueOf(useBp).toString(),
          indexType.toString()
        );
    }
  }

  private static double nsToSec(long ns) {
    return ns / (double) 1_000_000_000;
  }

  private static double msToSec(long ms) {
    return ms / (double) 1_000;
  }

  private static String getKnnField(FilterStrategy strategy) {
    if (strategy == null) {
      return KNN_FIELD;
    } else if (strategy == FilterStrategy.INDEX_TIME_FILTER || strategy == FilterStrategy.INDEX_TIME_SPARSE) {
      return KNN_FIELD_FILTERED;
    } else {
      return KNN_FIELD;
    }
  }

  private TopDocs doKnnVectorQuery(int index, VectorReader<?> targetReader, IndexSearcher searcher) throws IOException {
    return switch (targetReader) {
      case VectorReader.Byte byteVectors -> doKnnVectorQuery(byteVectors.read(index), searcher);
      case VectorReader.Float32 floatVectors -> doKnnVectorQuery(floatVectors.read(index), searcher);
    };
  }

  private TopDocs doKnnVectorQuery(byte[] vector, IndexSearcher searcher) throws IOException {
    Query queryTimeFilter = null;
    if (filterStrategy == FilterStrategy.QUERY_TIME_PRE_FILTER) {
      queryTimeFilter = filterQuery;
    }

    String knnField = getKnnField(filterStrategy);

    int k = (overSample > 1) ? Math.round((topK + fanout) * overSample) : topK + fanout;
    var profiledQuery = new ProfiledKnnByteVectorQuery(knnField, vector, k, queryTimeFilter);
    Query query = profiledQuery;
    if (filterStrategy == FilterStrategy.QUERY_TIME_POST_FILTER) {
      query = new BooleanQuery.Builder()
              .add(profiledQuery, BooleanClause.Occur.MUST)
              .add(filterQuery, BooleanClause.Occur.FILTER)
              .build();
    }

    TopDocs topDocs = searcher.search(query, topK);
    // Use the profiled visitedCount
    return new TopDocs(new TotalHits(profiledQuery.totalVectorCount(), topDocs.totalHits.relation()), topDocs.scoreDocs);
  }

  private TopDocs doKnnVectorQuery(float[] vector, IndexSearcher searcher) throws IOException {
    Query queryTimeFilter = null;
    if (filterStrategy == FilterStrategy.QUERY_TIME_PRE_FILTER) {
      queryTimeFilter = filterQuery;
    }

    String knnField = getKnnField(filterStrategy);
    if (filterStrategy == FilterStrategy.INDEX_TIME_FILTER) {
      knnField = KNN_FIELD_FILTERED;
    }

    if (parentJoin) {
      var topChildVectors = new DiversifyingChildrenFloatKnnVectorQuery(knnField, vector, null, topK + fanout, parentsFilter);
      var query = new ToParentBlockJoinQuery(topChildVectors, parentsFilter, org.apache.lucene.search.join.ScoreMode.Max);
      return searcher.search(query, topK);
    }

    int k = (overSample > 1) ? Math.round((topK + fanout) * overSample) : topK + fanout;
    var profiledQuery = new ProfiledKnnFloatVectorQuery(knnField, vector, k, queryTimeFilter);
    Query query = profiledQuery;
    if (filterStrategy == FilterStrategy.QUERY_TIME_POST_FILTER) {
      query = new BooleanQuery.Builder()
          .add(query, BooleanClause.Occur.MUST)
          .add(filterQuery, BooleanClause.Occur.FILTER)
          .build();
    }

    TopDocs topDocs = searcher.search(query, topK);
    // Use the profiled visitedCount
    return new TopDocs(new TotalHits(profiledQuery.totalVectorCount(), topDocs.totalHits.relation()), topDocs.scoreDocs);
  }

  private static <T> CompletableFuture<List<T>> invokeAllAsync(Collection<IOSupplier<T>> suppliers, Executor executor) {
    ArrayList<T> results = new ArrayList<>(suppliers.size());
    if (executor == null) {
      // Execute on current thread
      try {
        for (IOSupplier<T> supplier : suppliers) {
          results.add(supplier.get());
        }
        return CompletableFuture.completedFuture(results);
      } catch (IOException e) {
        return CompletableFuture.failedFuture(e);
      }
    }

    CompletableFuture<List<T>> result = CompletableFuture.completedFuture(results);
    for (IOSupplier<T> supplier : suppliers) {
      final Supplier<T> uncheckedSupplier = () -> {
        try {
          return supplier.get();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      };
      result = result.thenCombine(
          CompletableFuture.supplyAsync(uncheckedSupplier, executor != null ? executor : Runnable::run),
          (list, next) -> {
            list.add(next);
            return list;
          });
    }
    return result;
  }

  private float checkResults(int[][] results, int[][] nn) {
    int totalMatches = 0;
    int totalResults = results.length * topK;
    for (int i = 0; i < results.length; i++) {
      // System.out.println("compare " + Arrays.toString(nn[i]) + " to ");
      // System.out.println(Arrays.toString(results[i]));
      totalMatches += compareNN(nn[i], results[i]);
    }
    return totalMatches / (float) totalResults;
  }

  private int compareNN(int[] expected, int[] results) {
    int matched = 0;
    Set<Integer> expectedSet = new HashSet<>();
    Set<Integer> alreadySeen = new HashSet<>();
    for (int i = 0; i < topK; i++) {
      expectedSet.add(expected[i]);
    }
    for (int docId : results) {
      if (alreadySeen.add(docId) == false) {
        throw new IllegalStateException("duplicate docId=" + docId);
      }
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
  private int[][] getExactNN(Path docPath, Path indexPath,
                             Path queryPath, int queryStartIndex,
                             String metric) throws IOException, InterruptedException {

    String exactNNKey = formatExactNNKey(parentJoin,
                                         filterStrategy, filterSelectivity, randomSeed, docPath, queryPath, numDocs, metric,
                                         numQueryVectors, queryStartIndex, topK);
    
    log("exact nn key = %s\n", exactNNKey);

    // look in knn-exact-nn sub-directory for cached nn file
    Path nnPath = Paths.get("knn-reuse", "exact-nn", exactNNKey + ".bin");

    // make sure the docs/queries source vectors were not changed since the cached .nn file was created:
    boolean exists = Files.exists(nnPath);
    boolean nnIsNewerThanVectors;

    if (exists) {
      nnIsNewerThanVectors = isNewer(nnPath, docPath, queryPath);
    } else {
      nnIsNewerThanVectors = false;
    }

    if (exists && nnIsNewerThanVectors) {
      log("  read pre-cached exact match vectors from cache file \"" + nnPath + "\"\n");
      return readExactNN(nnPath);
    } else {
      String reason;
      if (exists) {
        assert nnIsNewerThanVectors == false;
        reason = "doc or quqery vectors files is newer tha cached exact-nn results";
      } else {
        reason = "exact-nn results don't exist";
      }
      log("  now compute brute-force exact KNN matches: %s\n", reason);
      long startNS = System.nanoTime();

      Path subdir = Paths.get("knn-reuse", "exact-nn");
      if (Files.exists(subdir) == false) {
        Files.createDirectories(subdir);
      }
      
      // TODO: enable computing NN from high precision vectors when
      // checking low-precision recall
      int[][] nn;
      if (vectorEncoding.equals(VectorEncoding.BYTE)) {
        nn = computeExactNNByte(queryPath, queryStartIndex);
      } else {
        nn = computeExactNN(queryPath, queryStartIndex);
      }
      log("\n");
      writeExactNN(nn, nnPath);
      long elapsedMS = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNS); // ns -> ms
      log("took %.3f sec to compute brute-force exact matches\n", elapsedMS / 1000.);
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
      long expectedSize = numQueryVectors * topK * Integer.BYTES;
      if (in.size() != expectedSize) {
        throw new IllegalStateException("exact-nn file \"" + nnPath + "\" should be size()=" + expectedSize + " but is actually " + in.size());
      }
      IntBuffer intBuffer =
        in.map(FileChannel.MapMode.READ_ONLY, 0, expectedSize)
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
    log("\nwriting true nearest neighbors to cache file \"" + nnPath + "\"\n");
    ByteBuffer tmp =
        ByteBuffer.allocate(nn[0].length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    try (OutputStream out = Files.newOutputStream(nnPath)) {
      for (int i = 0; i < numQueryVectors; i++) {
        tmp.asIntBuffer().put(nn[i]);
        out.write(tmp.array());
      }
    }
  }

  private static void runTasksWithProgress(List<Callable<Void>> tasks, AtomicInteger completedCount, FormatterLogger log) throws IOException, InterruptedException {
    // don't use the common pool else it messes up accounting total CPU spend across threads in ThreadDetails.subtract!
    // ForkJoinPool.commonPool().invokeAll(tasks);    

    int coreCount = Runtime.getRuntime().availableProcessors();

    // oddly, at least on beast3 (128 cores), exact NN is much slower with all (including hyperthread'd) cores:
    int poolThreadCount = Math.max(1, coreCount/2);
    log.log("using %d threads to compute exact NN\n", poolThreadCount);
    ForkJoinPool pool = new ForkJoinPool(poolThreadCount);

    int taskCount = tasks.size();

    // submit all tasks
    List<Future<Void>> futures = new ArrayList<>(taskCount);
    for (Callable<Void> task : tasks) {
      futures.add(pool.submit(task));
    }

    // report progress every 5 seconds
    long startNS = System.nanoTime();
    long nextReportTimeNS = System.nanoTime();
    while (true) {
      boolean isDone = completedCount.get() == taskCount;

      long nowNS = System.nanoTime();
      if (isDone || nowNS > nextReportTimeNS) {
        // isDone above ensures we see the final 100%
        int completed = completedCount.get();
        double pct = 100.0 * completed / taskCount;
        log.log("  %6.1f s: %5.1f %% (%5d / %d) vectors\n",
                (nowNS - startNS) / 1000000000.,
                pct, completed, taskCount);
        nextReportTimeNS += TimeUnit.SECONDS.toNanos(5);
      }

      if (isDone) {
        break;
      }

      // TODO: we could use an object monitor to wait/notify to avoid this final up to 10 msec delay on completion...
      Thread.sleep(10); // check every 100ms
    }

    pool.shutdown();
  }

  private int[][] computeExactNNByte(Path queryPath, int queryStartIndex) throws IOException, InterruptedException {
    int[][] result = new int[numQueryVectors][];
    log("computing true nearest neighbors of " + numQueryVectors + " target vectors\n");
    AtomicInteger completedCount = new AtomicInteger(0);
    List<Callable<Void>> tasks = new ArrayList<>();
    try (MMapDirectory dir = new MMapDirectory(indexPath)) {
      dir.setPreload((x, ctx) -> x.endsWith(".vec") || x.endsWith(".veq"));
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        if (reader.maxDoc() != numDocs && !parentJoin) {
          throw new IllegalStateException("index size mismatch, expected " + numDocs + " but index has " + reader.maxDoc());
        }
        IndexSearcher searcher = new IndexSearcher(reader);
        try (var queryReader = (VectorReader.Byte) VectorReader.create(queryPath, dim, VectorEncoding.BYTE)) {
          for (int i = 0; i < numQueryVectors; i++) {
            byte[] query = queryReader.read(queryStartIndex + i);
            tasks.add(new ComputeNNByteTask(searcher, i + queryStartIndex, query, result, completedCount));
          }
        }
        runTasksWithProgress(tasks, completedCount, this);
      }
      log("\n");
      return result;
    }
  }

  class ComputeNNByteTask implements Callable<Void> {

    private final int queryOrd;
    private final byte[] query;
    private final int[][] result;
    private final IndexSearcher searcher;
    private final AtomicInteger completedCount;
    private final String field;

    ComputeNNByteTask(IndexSearcher searcher, int queryOrd, byte[] query, int[][] result, AtomicInteger completedCount) {
      this.searcher = searcher;
      this.queryOrd = queryOrd;
      this.query = query;
      this.result = result;
      this.completedCount = completedCount;
      if (filterStrategy == FilterStrategy.INDEX_TIME_SPARSE) {
        field = KNN_FIELD_FILTERED;
      } else {
        field = KNN_FIELD;
      }
    }

    @Override
    public Void call() {
      try {
        var queryVector = new ConstKnnByteVectorValueSource(query);
        var docVectors = new ByteKnnVectorFieldSource(field);
        Query query = new FunctionQuery(new ByteVectorSimilarityFunction(similarityFunction, queryVector, docVectors));
        if (filterQuery != null) {
          query = new BooleanQuery.Builder()
                  .add(query, BooleanClause.Occur.SHOULD)
                  .add(filterQuery, BooleanClause.Occur.FILTER)
                  .build();
        }
        var topDocs = searcher.search(query, topK);
        result[queryOrd] = knn.KnnTesterUtils.getResultIds(topDocs, searcher.storedFields());
        completedCount.incrementAndGet();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    }
  }

  /** Brute force computation of "true" nearest neighbors. */
  private int[][] computeExactNN(Path queryPath, int queryStartIndex)
      throws IOException, InterruptedException {
    int[][] result = new int[numQueryVectors][];
    log("computing true nearest neighbors of " + numQueryVectors + " target vectors\n");
    log("parentJoin = %s\n", parentJoin);
    AtomicInteger completedCount = new AtomicInteger(0);
    try (MMapDirectory dir = new MMapDirectory(indexPath)) {
      dir.setPreload((x, ctx) -> x.endsWith(".vec") || x.endsWith(".veq"));
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        log("now compute brute-force KNN hits for " + numQueryVectors + " query vectors from \"" + queryPath + "\" starting at query index " + queryStartIndex + "\n");
        if (reader.maxDoc() != numDocs && parentJoin == false) {
          throw new IllegalStateException("index size mismatch, expected " + numDocs + " but index has " + reader.maxDoc());
        }
        IndexSearcher searcher = new IndexSearcher(reader);
        if (parentJoin) {
          CheckJoinIndex.check(reader, parentsFilter);
        }
        List<Callable<Void>> tasks = new ArrayList<>();
        try (var queryReader = (VectorReader.Float32) VectorReader.create(queryPath, dim, VectorEncoding.FLOAT32)) {
          for (int i = 0; i < numQueryVectors; i++) {
            float[] query = queryReader.read(queryStartIndex + i);
            if (parentJoin) {
              tasks.add(new ComputeExactSearchNNFloatTask(searcher, queryStartIndex + i, query, result, completedCount));
            } else {
              tasks.add(new ComputeNNFloatTask(searcher, queryStartIndex + i, query, result, completedCount));
            }
          }
          runTasksWithProgress(tasks, completedCount, this);
        }
        log("\n");
        return result;
      }
    }
  }

  // TODO: would it be faster to swap the stride?  for each indexed vector, run through all query vectors updating their
  // separate PQs?  better locality since we load each indexed vector just once and share it?  we would make thread work units
  // from chunks of indexed vectors?
  class ComputeNNFloatTask implements Callable<Void> {

    private final IndexSearcher searcher;
    private final int queryOrd;
    private final float[] query;
    private final int[][] result;
    private final AtomicInteger completedCount;
    private final String field;

    ComputeNNFloatTask(IndexSearcher searcher, int queryOrd, float[] query, int[][] result, AtomicInteger completedCount) {
      this.searcher = searcher;
      this.queryOrd = queryOrd;
      this.query = query;
      this.result = result;
      this.completedCount = completedCount;
      if (filterStrategy == FilterStrategy.INDEX_TIME_SPARSE) {
        field = KNN_FIELD_FILTERED;
      } else {
        field = KNN_FIELD;
      }
    }

    @Override
    public Void call() {
      // TODO: support docStartIndex here too
      try {
        var queryVector = new ConstKnnFloatValueSource(query);
        var docVectors = new FloatKnnVectorFieldSource(field);
        Query query = new FunctionQuery(new FloatVectorSimilarityFunction(similarityFunction, queryVector, docVectors));
        if (filterQuery != null) {
          query = new BooleanQuery.Builder()
                  .add(query, BooleanClause.Occur.SHOULD)
                  .add(filterQuery, BooleanClause.Occur.FILTER)
                  .build();
        }
        var topDocs = searcher.search(query, topK);
        result[queryOrd] = knn.KnnTesterUtils.getResultIds(topDocs, searcher.storedFields());
        completedCount.incrementAndGet();
      } catch (IOException e) {
        log("Exception " + e + "\n");
        throw new RuntimeException(e);
      } catch (Throwable t) {
        log("Throwable " + t + "\n");
        throw t;
      }
      return null;
    }
  }

  /** Uses ExactSearch from Lucene queries to compute nearest neighbors.
   */
  class ComputeExactSearchNNFloatTask implements Callable<Void> {

    private final IndexSearcher searcher;
    private final int queryOrd;
    private final float[] query;
    private final int[][] result;
    private final AtomicInteger completedCount;
    private final String field;

    ComputeExactSearchNNFloatTask(IndexSearcher searcher, int queryOrd, float[] query, int[][] result, AtomicInteger completedCount) {
      this.searcher = searcher;
      this.queryOrd = queryOrd;
      this.query = query;
      this.result = result;
      this.completedCount = completedCount;
      if (filterStrategy == FilterStrategy.INDEX_TIME_SPARSE) {
        field = KNN_FIELD_FILTERED;
      } else {
        field = KNN_FIELD;
      }
    }

    @Override
    public Void call() {
      // we only use this for ParentJoin benchmarks right now, TODO: extend for all computeExactNN needs.
      try {
        var queryVector = new ConstKnnFloatValueSource(query);
        var docVectors = new FloatKnnVectorFieldSource(field);
        var childQuery = new BooleanQuery.Builder()
          .add(new FunctionQuery(new FloatVectorSimilarityFunction(similarityFunction, queryVector, docVectors)), BooleanClause.Occur.SHOULD)
          .add(new TermQuery(new Term(DOCTYPE_FIELD, DOCTYPE_CHILD)), BooleanClause.Occur.FILTER)
          .build();
        var query = new ToParentBlockJoinQuery(childQuery, parentsFilter, org.apache.lucene.search.join.ScoreMode.Max);
        var topDocs = searcher.search(query, topK);
        result[queryOrd] = knn.KnnTesterUtils.getResultIds(topDocs, searcher.storedFields());
        completedCount.incrementAndGet();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    }
  }

  public void log(String msg, Object... args) {
    if (quiet == false) {
      System.out.printf(Locale.ROOT, msg, args);
      System.out.flush();
    }
  }

  static Codec getCodec(int maxConn, int beamWidth, ExecutorService exec, int numMergeWorker, boolean quantize, int quantizeBits, IndexType indexType) {
    return new Lucene104Codec() {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        if (quantize) {
          return switch (quantizeBits) {
            case 1 -> switch (indexType) {
              case FLAT -> new Lucene104ScalarQuantizedVectorsFormat(ScalarEncoding.SINGLE_BIT_QUERY_NIBBLE);
              case HNSW -> new Lucene104HnswScalarQuantizedVectorsFormat(ScalarEncoding.SINGLE_BIT_QUERY_NIBBLE, maxConn, beamWidth, numMergeWorker, exec);
            };
            case 2 -> switch (indexType) {
              case FLAT -> new Lucene104ScalarQuantizedVectorsFormat(ScalarEncoding.DIBIT_QUERY_NIBBLE);
              case HNSW -> new Lucene104HnswScalarQuantizedVectorsFormat(ScalarEncoding.DIBIT_QUERY_NIBBLE, maxConn, beamWidth, numMergeWorker, exec);
            };
            case 4 -> switch (indexType) {
              case FLAT -> new Lucene104ScalarQuantizedVectorsFormat(ScalarEncoding.PACKED_NIBBLE);
              case HNSW -> new Lucene104HnswScalarQuantizedVectorsFormat(ScalarEncoding.PACKED_NIBBLE, maxConn, beamWidth, numMergeWorker, exec);
            };
            case 7 -> switch (indexType) {
              case FLAT -> new Lucene104ScalarQuantizedVectorsFormat(ScalarEncoding.SEVEN_BIT);
              case HNSW -> new Lucene104HnswScalarQuantizedVectorsFormat(ScalarEncoding.SEVEN_BIT, maxConn, beamWidth, numMergeWorker, exec);
            };
            case 8 -> switch (indexType) {
              case FLAT -> new Lucene104ScalarQuantizedVectorsFormat(ScalarEncoding.UNSIGNED_BYTE);
              case HNSW -> new Lucene104HnswScalarQuantizedVectorsFormat(ScalarEncoding.UNSIGNED_BYTE, maxConn, beamWidth, numMergeWorker, exec);
            };
            default -> throw new IllegalArgumentException("Unsupported quantizeBits: " + quantizeBits);
          };
        } else {
          return new Lucene99HnswVectorsFormat(maxConn, beamWidth, numMergeWorker, exec);
        }
      }
    };
  }

  private static void usage() {
    String error =
        "Usage: TestKnnGraph [-reindex] [-search {queryfile}|-stats|-check] [-docs {datafile}] [-niter N] [-fanout N] [-maxConn N] [-beamWidth N] [-filterSelectivity N] [-prefilter]";
    System.err.println(error);
    System.exit(1);
  }

  private static class ProfiledKnnByteVectorQuery extends KnnByteVectorQuery {
    private long totalVectorCount;

    ProfiledKnnByteVectorQuery(String field, byte[] target, int k, Query filter) {
      super(field, target, k, filter);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
      totalVectorCount = 0;
      return super.rewrite(indexSearcher);
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
      TopDocs td = super.mergeLeafResults(perLeafResults);
      // merge leaf can happen any number of times during a rewrite
      totalVectorCount += td.totalHits.value();
      return td;
    }

    long totalVectorCount() {
      return totalVectorCount;
    }
  }

  private static class ProfiledKnnFloatVectorQuery extends KnnFloatVectorQuery {
    private long totalVectorCount;

    ProfiledKnnFloatVectorQuery(String field, float[] target, int k, Query filter) {
      super(field, target, k, filter);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
      totalVectorCount = 0;
      return super.rewrite(indexSearcher);
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
      TopDocs td = super.mergeLeafResults(perLeafResults);
      // merge leaf can happen any number of times during a rewrite
      totalVectorCount += td.totalHits.value();
      return td;
    }

    long totalVectorCount() {
      return totalVectorCount;
    }
  }

  private static class BitSetQuery extends Query {
    private final BitSet[] segmentDocs;

    BitSetQuery(BitSet[] segmentDocs) {
      this.segmentDocs = segmentDocs;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
      return new ConstantScoreWeight(this, boost) {
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
          var bitSet = segmentDocs[context.ord];
          var cardinality = bitSet.cardinality();
          var scorer = new ConstantScoreScorer(score(), scoreMode, new BitSetIterator(bitSet, cardinality));
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
      return sameClassAs(other) && Arrays.equals(segmentDocs, ((BitSetQuery) other).segmentDocs);
    }

    @Override
    public int hashCode() {
      return 31 * classHash() + Arrays.hashCode(segmentDocs);
    }
  }
}
