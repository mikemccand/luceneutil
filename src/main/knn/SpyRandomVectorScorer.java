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
import java.util.Arrays;

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

/**
 * Wraps a {@link RandomVectorScorer} and records every score computed during HNSW traversal.
 */
public class SpyRandomVectorScorer implements RandomVectorScorer {

  private final RandomVectorScorer delegate;
  private float[] recordedScores;
  private int count;

  public SpyRandomVectorScorer(RandomVectorScorer delegate) {
    this.delegate = delegate;
    this.recordedScores = new float[1024];
    this.count = 0;
  }

  @Override
  public float score(int node) throws IOException {
    float s = delegate.score(node);
    record(s);
    return s;
  }

  @Override
  public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
    float maxScore = delegate.bulkScore(nodes, scores, numNodes);
    for (int i = 0; i < numNodes; i++) {
      record(scores[i]);
    }
    return maxScore;
  }

  @Override
  public int maxOrd() {
    return delegate.maxOrd();
  }

  @Override
  public int ordToDoc(int ord) {
    return delegate.ordToDoc(ord);
  }

  @Override
  public Bits getAcceptOrds(Bits acceptDocs) {
    return delegate.getAcceptOrds(acceptDocs);
  }

  private void record(float score) {
    if (count == recordedScores.length) {
      recordedScores = Arrays.copyOf(recordedScores, recordedScores.length * 2);
    }
    recordedScores[count++] = score;
  }

  /** Returns the number of scores recorded so far. */
  public int scoreCount() {
    return count;
  }

  /** Returns a copy of the recorded scores array, trimmed to the actual count. */
  public float[] getScores() {
    return Arrays.copyOf(recordedScores, count);
  }

  /** Resets the recorded scores for the next query. */
  public void reset() {
    count = 0;
  }
}
