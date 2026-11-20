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

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.HnswGraphProvider;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.hnsw.HnswGraph;
import org.apache.lucene.util.hnsw.InitializedHnswGraphBuilder;
import org.apache.lucene.util.hnsw.NeighborArray;
import org.apache.lucene.util.hnsw.OnHeapHnswGraph;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * A utility for computing graph statistics. Given an HnswGraph, HnswStat will compute the
 * following, for each graph level:
 *
 * histogram of out-degree
 * histogram of inter-node score (ie "distance")
 * histogram of hop-distances; (foreach entry node, the minimum number of hops to every node in the graph)
 * the graph radius (max of hop-distances; may be infinite if the graph is not fully reachable from every entry node).
 *
 * TODO, maybe:
 * percentage of nodes that are connected to their closest neighbor (hard to compute - requires "brute force" NxN check).
 * percentage of links that are reciprocated
 *
 * analyses to perform:
 *  sweep across (different vectors), (maxconn), (beam-width), (quantization), (diversity)
 *  compare with "brute force" ie very high beam-width and low diversity
 *  compare with pippin
 *  also compute recall and look for correlations
 *
 * Q: can we train a better graph if we use some query sample?
 */
public final class HnswStat {
  private static final int MAX_HOP_SAMPLES = 50;
  private static final int HIST_SIZE = 50;
  private final HnswGraph hnsw;
  private final VectorSimilarityFunction similarity;
  private float minScore;
  private float maxScore;
  final DegreeHist degreeHist;
  final long[][] levelGraphScoreHist;
  final long[] randomScoreHist;
  final HopCounter[] levelHopCounters;
  private final PrintFormat printFormat;

  record DegreeHist(long[][] levelInDegreeHist, long[][] levelOutDegreeHist) {}

  HnswStat(HnswGraph hnsw,
           VectorSimilarityFunction similarity,
           PrintFormat printFormat,
           UpdateableRandomVectorScorer scorer)
    throws IOException {
    this.hnsw = hnsw;
    this.similarity = similarity;
    if (hnsw instanceof OnHeapHnswGraph ohhg) {
      computeMinMaxScores(ohhg);
      // must go first since it can generate new scores and change min/max score
      this.randomScoreHist = computeRandomScoreHist(scorer);
      this.levelGraphScoreHist = computeLevelScoreHist(ohhg);
    } else {
      this.levelGraphScoreHist = null;
      this.randomScoreHist = null;
    }
    this.degreeHist = computeDegreeHist();
    this.levelHopCounters = countHops();
    this.printFormat = printFormat;
  }

  public static void main(String... args) throws Exception {
    if (args.length == 0 || args.length > 2) {
      error();
    }
    PrintFormat format = PrintFormat.Text;
    String indexPath = null;
    for (String arg : args) {
      if (arg.equals("--tsv")) {
        format = PrintFormat.Tsv;
      } else if (arg.equals("--text")) {
        format = PrintFormat.Text;
      } else {
        if (indexPath != null) {
          error();
        }
        indexPath = arg;
      }
    }
    printIndexStats(System.out, indexPath, format);
  }

  private static void error() {
    System.err.println("usage: HnswStat <index path> [--tsv]");
    System.exit(1);
  }

  static void printIndexStats(PrintStream printer, String indexPath, PrintFormat printFormat) throws IOException {
    Method neighborSortMethod;
    try {
      neighborSortMethod = NeighborArray.class.getDeclaredMethod("sort", RandomVectorScorer.class);
      neighborSortMethod.setAccessible(true);
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    DefaultFlatVectorScorer flatVectorScorer = DefaultFlatVectorScorer.INSTANCE;
    if (printFormat == PrintFormat.Tsv) {
      printTsvHeader(printer);
    }
    try (Directory dir = FSDirectory.open(Path.of(indexPath))) {
      try (IndexReader reader = DirectoryReader.open(dir)) {
        Set<String> hnswFields = new HashSet<>();
        for (LeafReaderContext ctx : reader.leaves()) {
          LeafReader leafReader = ctx.reader();
          FieldInfos finfos = leafReader.getFieldInfos();
          for (FieldInfo finfo : finfos) {
            if (finfo.getVectorDimension() > 0) {
              String field = finfo.name;
              KnnVectorsReader vectorReader = ((SegmentReader) leafReader).getVectorReader()
                .unwrapReaderForField(field);
              if (vectorReader instanceof HnswGraphProvider hgp) {
                hnswFields.add(field);
              }
            }
          }
        }
        for (String field : hnswFields) {
          for (LeafReaderContext ctx : reader.leaves()) {
            LeafReader leafReader = ctx.reader();
            // make a single identity map big enough for any vector field in this segment
            int[] identityOrdMap = new int[leafReader.maxDoc()];
            for (int i = 0; i < identityOrdMap.length; i++) {
              identityOrdMap[i] = i;
            }
            KnnVectorsReader vectorReader = ((SegmentReader) leafReader).getVectorReader().unwrapReaderForField(field);
            /*
              long asinCount = leafReader.totalTermFreq(new Term("asin_or_proffer", "asin"));
              System.out.println(" vector field " + field + " segment " + ctx.ord + " asins=" + asinCount
              + " maxdoc=" + leafReader.maxDoc());
            */
            if (vectorReader instanceof HnswGraphProvider hgp) {
              FloatVectorValues vectorValues = vectorReader.getFloatVectorValues(field);
              HnswGraph hnsw = hgp.getGraph(field);
              if (hnsw.size() == 0) {
                continue;
              }
              VectorSimilarityFunction similarityFunction =
                leafReader.getFieldInfos().fieldInfo(field).getVectorSimilarityFunction();
              RandomVectorScorerSupplier scorerSupplier =
                flatVectorScorer.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
              OnHeapHnswGraph graph = InitializedHnswGraphBuilder.initGraph(
                                                                            hnsw, identityOrdMap, hnsw.size(), 500, scorerSupplier);

              resolveScores(graph, scorerSupplier.scorer(), neighborSortMethod);
              HnswStat stats =
                new HnswStat(graph, similarityFunction, printFormat, scorerSupplier.scorer());
              stats.print(printer, field, ctx.ord);
            }
          }
        }
      }
    }
  }

  // Populate the scores in an on-heap graph that was initialized from an index and has NaN placeholder scores.
  private static void resolveScores(OnHeapHnswGraph graph, UpdateableRandomVectorScorer scorer, Method method) throws IOException {
    for (int level = 0; level < graph.numLevels(); level++) {
      HnswGraph.NodesIterator it = graph.getNodesOnLevel(level);
      while (it.hasNext()) {
        int node = it.nextInt();
        scorer.setScoringOrdinal(node); // scores must be relative to the array's owner
        try {
          method.invoke(graph.getNeighbors(level, node), scorer);
        } catch (Exception ex) {
          throw new IOException(ex);
        }
      }
    }
  }

  public void print(PrintStream out, String field, int segmentNum) throws IOException {
    switch (printFormat) {
      case Text -> printText(out, field, segmentNum, similarity, hnsw);
      case Tsv -> printTsv(out, field, segmentNum, similarity, hnsw);
      default -> throw new IllegalStateException("?");
    }
  }

  static void printTsvHeader(PrintStream printer) {
    // print header; each row represents a (field,segment,layer) tuple
    // possibly simplify to layer 0 only?
    printer.print("field\tsegment\tlevel\tcount\tsimilarity\tmaxConn");
    printer.print(
                  "\tout_degree_count\tout_degree_avg\tout_degree_min\tout_degree_max\tout_degree_p90\tout_degree_p99");
    printer.print("\tin_degree_count\tin_degree_avg\tin_degree_min\tin_degree_max\tin_degree_p90\tin_degree_p99");
    printer.print("\tscore_count\tscore_avg\tscore_min\tscore_max\tscore_p90\tscore_p99");
    printer.print("\trscore_count\trscore_avg\trscore_min\trscore_max\trscore_p90\trscore_p99");
    printer.print("\thop_count_count\thop_count_avg\thop_count_min\thop_count_max\thop_count_p90\thop_count_p99");
    printer.print("\n");
  }

  void printTsv(
                PrintStream printer, String field, int segmentNum, VectorSimilarityFunction similarity, HnswGraph hnsw)
    throws IOException {
    // write data in TSV format.
    // for each histogram, summarize by (avg, p0, p50, p90, p99, p100)
    int numLevels = hnsw.numLevels();
    float scoreRange = maxScore - minScore;
    for (int level = 0; level < numLevels; level++) {
      int size = hnsw.getNodesOnLevel(level).size();
      printer.printf(
                     Locale.ROOT, "%s\t%d\t%d\t%d\t%s\t%d", field, segmentNum, level, size, similarity, hnsw.maxConn());
      // degree out
      printHistTsv(printer, degreeHist.levelOutDegreeHist()[level], (n) -> (float) n);
      // degree in
      printHistTsv(printer, degreeHist.levelInDegreeHist()[level], (n) -> (float) n);
      // score
      float scale = scoreRange / levelGraphScoreHist[level].length;
      printHistTsv(printer, levelGraphScoreHist[level], (n) -> (n + 0.5f) * scale);
      // random score
      printHistTsv(printer, randomScoreHist, (n) -> (n + 0.5f) * scoreRange / randomScoreHist.length);
      // hop count
      printHistTsv(printer, levelHopCounters[level].hist, (n) -> n + 1f);
      printer.println();
    }
  }

  void printText(PrintStream out, String field, int segmentNum, VectorSimilarityFunction similarity, HnswGraph hnsw)
    throws IOException {
    printHeader(out, field, segmentNum, similarity, hnsw);
    printLevelInfo(out);
    printDegreeStats(out);
    printScoreStats(out);
    printHopStats(out);
  }

  private void printHeader(
                           PrintStream out, String field, int segmentNum, VectorSimilarityFunction similarity, HnswGraph hnsw)
    throws IOException {
    out.printf(
               Locale.ROOT,
               "field %s segment %d %d vectors in %d levels, similarity=%s, maxConn=%d\n",
               field,
               segmentNum,
               hnsw.size(),
               hnsw.numLevels(),
               similarity,
               hnsw.maxConn());
  }

  private void printLevelInfo(PrintStream out) throws IOException {
    for (int level = 0; level < hnsw.numLevels(); level++) {
      int levelSize = hnsw.getNodesOnLevel(level).size();
      out.printf(Locale.ROOT, "Level %d size=%d\n", level, levelSize);
    }
  }

  public void printDegreeStats(PrintStream out) throws IOException {
    out.printf(Locale.ROOT, "\nOUT DEGREE\n");
    for (int level = 0; level < hnsw.numLevels(); level++) {
      printHist(out, "L" + level, degreeHist.levelOutDegreeHist()[level], (n) -> (float) n);
    }
    out.printf(Locale.ROOT, "\nIN DEGREE\n");
    for (int level = 0; level < hnsw.numLevels(); level++) {
      printHist(out, "L" + level, degreeHist.levelInDegreeHist()[level], (n) -> (float) n);
    }
  }

  public void printScoreStats(PrintStream out) throws IOException {
    out.printf(Locale.ROOT, "\nSCORES IN GRAPH\n");
    float range = maxScore - minScore;
    for (int level = 0; level < hnsw.numLevels(); level++) {
      float bucketSize = range / levelGraphScoreHist[level].length;
      printHist(out, "L" + level, levelGraphScoreHist[level], (n) -> (n + 0.5f) * bucketSize);
    }
    out.printf(Locale.ROOT, "\nRANDOM SCORES\n");
    float bucketSize = range / randomScoreHist.length;
    printHist(out, "ALL", randomScoreHist, (n) -> (n + 0.5f) * bucketSize);
  }

  public void printHopStats(PrintStream out) throws IOException {
    out.printf(Locale.ROOT, "\nHOPS\n");
    for (int level = 0; level < hnsw.numLevels(); level++) {
      HopCounter counter = levelHopCounters[level];
      if (hnsw.getNodesOnLevel(level).size() <= 1) {
        continue;
      }
      printHist(out, "L" + level, counter.hist, (n) -> n + 1f);
      float ope = counter.orphansPerEntry;
      if (level < hnsw.numLevels() - 1) {
        ope /= (float) Math.min(hnsw.getNodesOnLevel(level + 1).size(), MAX_HOP_SAMPLES);
      }
      if (counter.maxHops > counter.hist.length) {
        out.printf(Locale.ROOT, " max hops = %d", counter.maxHops);
      }
      out.printf(Locale.ROOT, " orphans (over all entries)=%d, avg orphans per entry=%f\n", counter.orphans, ope);
    }
  }

  private static final char[] BLOCK = {'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'};

  record Stats(int count, float avg, float min, float max, int imax, float p90, float p99) {}

  private Stats statsFromHist(long[] hist, Function<Integer, Float> valueMap) {
    float min = Float.MAX_VALUE, max = 0, total = 0;
    int totalCount = 0;
    int imin = 0, imax = hist.length - 1;
    long maxCount = 0;
    for (int ival = 0; ival < hist.length; ival++) {
      long count = hist[ival];
      if (count != 0) {
        float value = valueMap.apply(ival);
        if (value < min) {
          min = value;
          imin = ival;
        }
        if (value > max) {
          max = value;
          imax = ival;
        }
        if (count > maxCount) {
          maxCount = count;
        }
        total += value * count;
        totalCount += count;
      }
    }
    int cum = 0;
    Float p90 = null, p99 = null;
    for (int ival = 0; ival < hist.length; ival++) {
      cum += hist[ival];
      if (p90 == null && cum >= 0.9 * totalCount) {
        p90 = valueMap.apply(ival);
      }
      if (p99 == null && cum >= 0.99 * totalCount) {
        p99 = valueMap.apply(ival);
      }
    }
    return new Stats(totalCount, total / totalCount, min, max, imax, p90, p99);
  }

  private void printHistTsv(PrintStream out, long[] hist, Function<Integer, Float> valueMap) {
    Stats stats = statsFromHist(hist, valueMap);
    if (stats.count() > 0) {
      out.printf(
                 Locale.ROOT,
                 "\t%d\t%f\t%f\t%f\t%f\t%f",
                 stats.count(),
                 stats.avg(),
                 stats.min(),
                 stats.max(),
                 stats.p90(),
                 stats.p99());
    } else {
      out.printf("\t0\t0\t0\t0\t0\t0");
    }
  }

  private void printHist(PrintStream out, String label, long[] hist, Function<Integer, Float> valueMap) {
    Stats stats = statsFromHist(hist, valueMap);
    if (stats.count() == 0) {
      out.printf(Locale.ROOT, "%s no values\n", label);
      return;
    }
    out.printf(Locale.ROOT, "%s min=%.2f, mean=%.2f, max=%.2f\n", label, stats.min(), stats.avg(), stats.max());
    for (int ival = 0; ival <= stats.imax(); ival++) {
      // hist buckets counts range: [0, maxCount]
      if (hist[ival] == 0) {
        out.print(" ");
      } else {
        double ratio = hist[ival] / (stats.count() + 0.00001);
        int block = (int) (ratio * BLOCK.length);
        assert block >= 0 : "block < 0: " + block + " hist=" + hist[ival] + " maxCount=" + stats.count();
        out.print(BLOCK[block]);
      }
    }
    out.print("\n");
  }

  private void computeMinMaxScores(OnHeapHnswGraph hwwg) throws IOException {
    if (similarity == VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT) {
      // costly but what else can we do that is reliable?
      // possibly extend the histogram dynamically ...
      minScore = Float.POSITIVE_INFINITY;
      maxScore = Float.NEGATIVE_INFINITY;
      for (int level = 0; level < hnsw.numLevels(); level++) {
        HnswGraph.NodesIterator nodesIterator = hnsw.getNodesOnLevel(level);
        while (nodesIterator.hasNext()) {
          int node = nodesIterator.nextInt();
          NeighborArray neighbors = hwwg.getNeighbors(level, node);
          for (int i = 0; i < neighbors.size(); i++) {
            float score = neighbors.getScores(i);
            minScore = Math.min(score, minScore);
            maxScore = Math.max(score, maxScore);
          }
        }
      }
    } else {
      minScore = 0;
      maxScore = 1;
    }
  }

  /**
   * score histograms need to know the possible range of values up front. This is controlled by the
   * VectorSimilarityFunction:
   *
   * EUCLIDEAN: (0, 1]
   * DOT_PRODUCT: [0, 1]
   * COSINE: [0, 1]
   * MAXIMUM_INNER_PRODUCT: [-INF, +INF]
   *
   * For M.I.P., we require an additional pass to compute min, max scores over the entire corpus.
   */
  private long[][] computeLevelScoreHist(OnHeapHnswGraph ohhg) throws IOException {
    int numLevels = ohhg.numLevels();
    long[][] levelGraphScoreHist = new long[numLevels][HIST_SIZE];
    float multiplier = HIST_SIZE / (maxScore - minScore);
    for (int level = numLevels - 1; level >= 0; level--) {
      HnswGraph.NodesIterator nodesIterator = ohhg.getNodesOnLevel(level);
      if (nodesIterator.size() == 1) {
        continue;
      }
      long[] scoreHist = levelGraphScoreHist[level];
      while (nodesIterator.hasNext()) {
        int node = nodesIterator.nextInt();
        NeighborArray neighbors = ohhg.getNeighbors(level, node);
        for (int i = 0; i < neighbors.size(); i++) {
          float score = neighbors.getScores(i);
          int bucket = Math.min((int) Math.floor((score - minScore) * multiplier), HIST_SIZE - 1);
          scoreHist[bucket]++;
        }
      }
    }
    return levelGraphScoreHist;
  }

  private long[] computeRandomScoreHist(UpdateableRandomVectorScorer scorer) throws IOException {
    Random random = new Random();
    outer:
    while (true) {
      long[] scoreHist = new long[HIST_SIZE];
      float multiplier = HIST_SIZE / (maxScore - minScore);
      int size = hnsw.size();
      for (int i = 0; i < size; i++) {
        scorer.setScoringOrdinal(random.nextInt(size));
        float score = scorer.score(random.nextInt(size));
        if (score < minScore || score > maxScore) {
          // this can happen since we take minScore from a "representative" sample
          // so if we do see a score out of range, reset and start over
          minScore = Math.min(score, minScore);
          maxScore = Math.max(score, maxScore);
          continue outer;
        }
        int bucket = Math.min((int) Math.floor((score - minScore) * multiplier), HIST_SIZE - 1);
        scoreHist[bucket]++;
      }
      return scoreHist;
    }
  }

  private DegreeHist computeDegreeHist() throws IOException {
    int numLevels = hnsw.numLevels();
    long[][] levelOutDegreeHist = new long[numLevels][];
    long[][] levelInDegreeHist = new long[numLevels][];
    for (int level = numLevels - 1; level >= 0; level--) {
      HnswGraph.NodesIterator nodesIterator = hnsw.getNodesOnLevel(level);
      long[] outDegreeHist;
      if (level == 0) {
        outDegreeHist = new long[2 * hnsw.maxConn() + 1];
      } else {
        outDegreeHist = new long[hnsw.maxConn() + 1];
      }
      levelOutDegreeHist[level] = outDegreeHist;
      int[] inDegree = new int[hnsw.size()];
      int maxInDegree = 0;
      while (nodesIterator.hasNext()) {
        int node = nodesIterator.nextInt();
        int n = 0;
        hnsw.seek(level, node);
        int nbr;
        while ((nbr = hnsw.nextNeighbor()) != NO_MORE_DOCS) {
          ++n;
          inDegree[nbr]++;
          maxInDegree = Math.max(maxInDegree, inDegree[nbr]);
        }
        ++outDegreeHist[n];
      }
      // System.out.println("max in-degree = " + maxInDegree);
      long[] inDegreeHist = new long[maxInDegree + 1];
      // We could iterate over NodesIterator to get only the non-zero entries, but this seems fast enough
      for (int degree : inDegree) {
        if (degree > 0) {
          inDegreeHist[degree]++;
        }
      }
      levelInDegreeHist[level] = inDegreeHist;
    }
    return new DegreeHist(levelInDegreeHist, levelOutDegreeHist);
  }

  static class HopCounter {
    int orphans;
    int orphansPerEntry;
    int maxHops;
    long[] hist = new long[HIST_SIZE];
  }

  // for each node on level N, compute the distance, in graph hops, to every node on level N+1
  // and histogram those distances.  We presume HIST_SIZE to be > max hop distance, but it could be
  // more. We also record the maxHops, in case it should be > HIST_SIZE, but it never seems to be.
  private HopCounter[] countHops() throws IOException {
    int numLevels = hnsw.numLevels();
    // 0-99 represent histogram (counts for hop distances 1-100+). We don't count the entry nodes,
    // which have a 0 hop distance from themselves.
    FixedBitSet visitedAll = new FixedBitSet(hnsw.size());
    HopCounter[] hopCounter = new HopCounter[numLevels];
    int topEntryNode = hnsw.entryNode();
    // level N - 1 has a single entryNode:
    hopCounter[numLevels - 1] = new HopCounter();
    computeHopHist(topEntryNode, numLevels - 1, hopCounter[numLevels - 1], visitedAll);
    // only one entry point, so these metrics are the same for top level
    hopCounter[numLevels - 1].orphans = hopCounter[numLevels - 1].orphansPerEntry;
    for (int level = numLevels - 2; level >= 0; level--) {
      visitedAll.clear();
      hopCounter[level] = new HopCounter();
      // other levels' entry nodes are the nodes in the next highest level
      HnswGraph.NodesIterator nodesIterator = hnsw.getNodesOnLevel(level + 1);
      int sampleRate = nodesIterator.size() / MAX_HOP_SAMPLES;
      OUTER:
      while (nodesIterator.hasNext()) {
        for (int sample = 0; sample < sampleRate; sample++) {
          // sample the entry nodes; otherwise this takes forever
          nodesIterator.nextInt();
          if (!nodesIterator.hasNext()) {
            break OUTER;
          }
        }
        int entryNode = nodesIterator.nextInt();
        FixedBitSet visited = new FixedBitSet(hnsw.size());
        computeHopHist(entryNode, level, hopCounter[level], visited);
        visitedAll.or(visited);
      }
      hopCounter[level].orphans = hnsw.getNodesOnLevel(level).size() - visitedAll.cardinality();
    }
    return hopCounter;
  }

  // Q: should we say that a node is unreachable (count its distance as INF) if it is not
  // reachable from a single entry node? Or only if it is unreachable from all entry nodes?
  // I guess we want both stats: missed by a given entry node, and missed by all
  private void computeHopHist(int entryNode, int level, HopCounter hopCounter, FixedBitSet visited)
    throws IOException {
    IntsRefBuilder queue = new IntsRefBuilder();
    IntsRefBuilder nextQueue = new IntsRefBuilder();
    long[] hopHist = hopCounter.hist;
    int hopDist = 1;
    int maxHopDist = 1;
    queue.append(entryNode);
    while (queue.length() > 0) {
      do {
        int node = queue.intAt(queue.length() - 1);
        visited.set(node);
        queue.setLength(queue.length() - 1);
        hnsw.seek(level, node);
        int neighbor;
        while ((neighbor = hnsw.nextNeighbor()) != NO_MORE_DOCS) {
          if (visited.get(neighbor)) {
            continue;
          }
          visited.set(neighbor);
          hopHist[hopDist - 1]++;
          if (maxHopDist > hopCounter.maxHops) {
            hopCounter.maxHops = maxHopDist;
          }
          nextQueue.append(neighbor);
        }
      } while (queue.length() > 0);
      if (nextQueue.length() > 0) {
        maxHopDist += 1;
        // in case the histogram is too small to contain maxHopDist, count all the greater
        // values in the last bucket?
        hopDist = Math.min(maxHopDist, hopHist.length - 1);
        queue = nextQueue;
        nextQueue = new IntsRefBuilder();
      }
    }
    int orphanCount = hnsw.getNodesOnLevel(level).size() - visited.cardinality();
    hopCounter.orphansPerEntry += orphanCount;
  }

  enum PrintFormat {
    Text,
    Tsv
  }
}

