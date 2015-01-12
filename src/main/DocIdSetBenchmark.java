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

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageTester;
import org.apache.lucene.util.RoaringDocIdSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.packed.EliasFanoDocIdSet;

/**
 * Benchmark for {@link DocIdSet} implementations.
 */
public class DocIdSetBenchmark {

  /** Tracks scores for each {@link DocIdSet} impl. */
  private static class ScoresRegister {

    private final Map<Object, Map<Float, Map<Class<?>, Long>>> scores;

    ScoresRegister() {
      scores = new LinkedHashMap<>();
    }

    public void registerScore(String key, Float loadFactor, Class<?> cls, long score) {
      Map<Float, Map<Class<?>, Long>> m1 = scores.get(key);
      if (m1 == null) {
        m1 = new LinkedHashMap<>();
        scores.put(key, m1);
      }

      Map<Class<?>, Long> m2 = m1.get(loadFactor);
      if (m2 == null) {
        m2 = new LinkedHashMap<>();
        m1.put(loadFactor, m2);
      }

      Long s = m2.get(cls);
      if (s != null) {
        score = Math.max(s, score);
      }
      m2.put(cls, score);
    }

    // print the tables that are used for the visualizations
    public void printChartsTables() {
      for (Map.Entry<Object, Map<Float, Map<Class<?>, Long>>> entry : scores.entrySet()) {
        System.out.println("#### " + entry.getKey());
        Map<Float, Map<Class<?>, Long>> m2 = entry.getValue();
        System.out.print("['log10(loadFactor)', '" + FixedBitSet.class.getSimpleName() + "'");
        for (Class<?> cls : m2.values().iterator().next().keySet()) {
          if (cls != FixedBitSet.class) {
            System.out.print(", '" + cls.getSimpleName() + "'");
          }
        }
        System.out.println("],");
        for (Map.Entry<Float, Map<Class<?>, Long>> entry2 : m2.entrySet()) {
          final float loadFactor = entry2.getKey();
          final Map<Class<?>, Long> scores = entry2.getValue();
          final double fbsScore = scores.get(FixedBitSet.class);
          System.out.print("[" + Math.log10(loadFactor) + ",0");
          for (Map.Entry<Class<?>, Long> score : scores.entrySet()) {
            if (score.getKey() != FixedBitSet.class) {
              System.out.print("," + Math.log(score.getValue() / fbsScore) / Math.log(2));
            }
          }
          System.out.println("],");
        }
      }
    }

  }

  private static int DUMMY; // to prevent JVM optimizations
  private static final long SECOND = 1000L * 1000L * 1000L; // in ns
  private static final Random RANDOM = new Random();
  private static DocIdSetFactory[] FACTORIES = new DocIdSetFactory[] {
    new DocIdSetFactory() {
      public Class<?> getKey() {
        return RoaringDocIdSet.class;
      }
      @Override
      public DocIdSet copyOf(DocIdSet set, int numBits) throws IOException {
        RoaringDocIdSet.Builder copy = new RoaringDocIdSet.Builder(numBits);
        final DocIdSetIterator disi = set.iterator();
        for (int doc = disi.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = disi.nextDoc()) {
          copy.add(doc);
        }
        return copy.build();
      }
    },
    new DocIdSetFactory() {
      public Class<?> getKey() {
        return EliasFanoDocIdSet.class;
      }
      @Override
      public DocIdSet copyOf(DocIdSet set, int numBits) throws IOException {
        int cardinality = 0, maxDoc = -1;
        final DocIdSetIterator disi = set.iterator();
        for (int doc = disi.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = disi.nextDoc()) {
          ++cardinality;
          maxDoc = doc;
        }
        final EliasFanoDocIdSet copy = new EliasFanoDocIdSet(cardinality, maxDoc);
        copy.encodeFromDisi(set.iterator());
        return copy;
      }
    },
    new DocIdSetFactory() {
      public Class<?> getKey() {
        return SparseFixedBitSet.class;
      }
      @Override
      public DocIdSet copyOf(DocIdSet set, int numBits) throws IOException {
        SparseFixedBitSet copy = new SparseFixedBitSet(numBits);
        copy.or(set.iterator());
        return new BitDocIdSet(copy, copy.approximateCardinality());
      }
    }
  };
  private static final int MAX_DOC = 1 << 24;
  private static float[] LOAD_FACTORS = new float[] {0.00001f, 0.0001f, 0.001f, 0.01f, 0.1f, 0.5f, 0.9f, 0.99f, 1f};

  private static abstract class DocIdSetFactory {
    public abstract Class<?> getKey();
    public abstract DocIdSet copyOf(DocIdSet set, int numBits) throws IOException;
  }

  protected static FixedBitSet randomSet(int numBits, int numBitsSet) {
    assert numBitsSet <= numBits;
    final FixedBitSet set = new FixedBitSet(numBits);
    if (numBitsSet == numBits) {
      set.set(0, numBits);
    } else {
      for (int i = 0; i < numBitsSet; ++i) {
        while (true) {
          final int o = RANDOM.nextInt(numBits);
          if (!set.get(o)) {
            set.set(o);
            break;
          }
        }
      }
    }
    return set;
  }

  protected static FixedBitSet randomSet(int numBits, float percentSet) {
    return randomSet(numBits, (int) (percentSet * numBits));
  }

  private static int exhaustIterator(DocIdSet set) throws IOException {
    int dummy = 0;
    final DocIdSetIterator it = set.iterator();
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      dummy += doc;
    }
    return dummy;
  }

  private static int exhaustIterator(DocIdSet set, int increment) throws IOException {
    int dummy = 0;
    final DocIdSetIterator it = set.iterator();
    for (int doc = -1; doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.advance(doc + increment)) {
      dummy += doc;
    }
    return dummy;
  }

  private static Collection<Map.Entry<Class<?>, DocIdSet>> sets(int numBits, float load) throws IOException {
    final BitDocIdSet fixedSet = new BitDocIdSet(randomSet(numBits, load));
    final List<Map.Entry<Class<?>, DocIdSet>> sets = new ArrayList<>();
    sets.add(new AbstractMap.SimpleImmutableEntry<Class<?>, DocIdSet>(FixedBitSet.class, fixedSet));
    for (DocIdSetFactory factory : FACTORIES) {
      sets.add(new AbstractMap.SimpleImmutableEntry<Class<?>, DocIdSet>(factory.getKey(), factory.copyOf(fixedSet, numBits)));
    }
    return sets;
  }

  // Compute how many FixedBitSets can be built in one second
  public static long scoreBuildFixedBitSet(DocIdSet set, int maxDoc) throws IOException {
    final long start = System.nanoTime();
    int dummy = 0;
    long score = 0;
    while (System.nanoTime() - start < SECOND) {
      final FixedBitSet copy = new FixedBitSet(maxDoc);
      DocIdSetIterator iterator = set.iterator();
      if (iterator != null) {
        copy.or(iterator);
      }
      dummy += copy.hashCode();
      ++score;
    }
    DUMMY += dummy;
    return score;
  }

  //Compute how many DocIdSets can be built in one second
  public static long scoreBuild(DocIdSetFactory factory, DocIdSet set, int numBits) throws IOException {
    final long start = System.nanoTime();
    int dummy = 0;
    long score = 0;
    while (System.nanoTime() - start < SECOND) {
      final DocIdSet copy = factory.copyOf(set, numBits);
      dummy += copy.hashCode();
      ++score;
    }
    DUMMY += dummy;
    return score;
  }

  // Compute how many times a DocIdSet can be iterated in one second
  public static long score(DocIdSet set) throws IOException {
    final long start = System.nanoTime();
    int dummy = 0;
    long score = 0;
    while (System.nanoTime() - start < SECOND) {
      dummy += exhaustIterator(set);
      ++score;
    }
    DUMMY += dummy;
    return score;
  }

  //Compute how many times a DocIdSet can be advanced in one second
  public static long score(DocIdSet set, int inc) throws IOException {
    final long start = System.nanoTime();
    int dummy = 0;
    long score = 0;
    while (System.nanoTime() - start < SECOND) {
      dummy += exhaustIterator(set, inc);
      ++score;
    }
    DUMMY += dummy;
    return score;
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    ScoresRegister reg = new ScoresRegister();
    for (float loadFactor : LOAD_FACTORS) {
      for (Map.Entry<Class<?>, DocIdSet> set : sets(MAX_DOC, loadFactor)) {
        final long memSize = RamUsageTester.sizeOf(set.getValue());
        reg.registerScore("memory", loadFactor, set.getKey(), memSize);
        System.out.println(set.getKey().getSimpleName() + "\t" + loadFactor + "\t" + memSize);
      }
    }
    System.out.println();
    System.out.println("JVM warm-up");
    long start = System.nanoTime();
    while (System.nanoTime() - start < 30 * SECOND) {
      final int numBits = 1 << 18;
      for (float loadFactor : LOAD_FACTORS) {
        final BitDocIdSet fixedSet = new BitDocIdSet(randomSet(numBits, loadFactor), 0);
        scoreBuildFixedBitSet(fixedSet, numBits);
        for (DocIdSetFactory factory : FACTORIES) {
          scoreBuild(factory, fixedSet, numBits);
        }
        for (Map.Entry<Class<?>, DocIdSet> set : sets(numBits, loadFactor)) {
          score(set.getValue());
          score(set.getValue(), 313);
        }
      }
    }

    // Start the test
    System.out.println("LoadFactor\tBenchmark\tImplementation\tScore");
    for (int i = 0; i < 3; ++i) {
      for (float load : LOAD_FACTORS) {
        final Collection<Map.Entry<Class<?>, DocIdSet>> sets = sets(MAX_DOC, load);
        // Free memory so that GC doesn't kick in while the benchmark is running
        System.gc();
        Thread.sleep(5 * 1000);
        DocIdSet fastestSet = null;
        for (Map.Entry<Class<?>, DocIdSet> set : sets) {
          fastestSet = set.getValue();
          if (set.getValue() instanceof RoaringDocIdSet) { // fastest at nextDoc
            break;
          }
        }
        long score = scoreBuildFixedBitSet(fastestSet, MAX_DOC);
        reg.registerScore("build", load, FixedBitSet.class, score);
        System.out.println(load + "\tbuild\t" + FixedBitSet.class.getSimpleName() + "\t" + score);
        for (DocIdSetFactory factory : FACTORIES) {
          score = scoreBuild(factory, fastestSet, MAX_DOC);
          reg.registerScore("build", load, factory.getKey(), score);
          System.out.println(load + "\tbuild\t" + factory.getKey().getSimpleName() + "\t" + score);
        }
        System.gc();
        Thread.sleep(5 * 1000);
        for (Map.Entry<Class<?>, DocIdSet> set : sets) {
          score = score(set.getValue());
          reg.registerScore("nextDoc", load, set.getKey(), score);
          System.out.println(load + "\tnextDoc()\t" + set.getKey().getSimpleName() + "\t" + score(set.getValue()));
        }
        for (int inc : new int[] {1, 31, 313, 3571, 33533, 319993}) { // primes
          final String key = "advance(" + inc + ")";
          for (Map.Entry<Class<?>, DocIdSet> set : sets) {
            score = score(set.getValue(), inc);
            reg.registerScore(key, load, set.getKey(), score);
            System.out.println(load + "\t" + key + "\t" + set.getKey().getSimpleName() + "\t" + score);
          }
        }
      }
    }
    System.out.println("DONE " + DUMMY);
    System.out.println("Tables for Google charts:");
    reg.printChartsTables();
  }
}
