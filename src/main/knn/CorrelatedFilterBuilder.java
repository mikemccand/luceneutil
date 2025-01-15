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

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.FixedBitSet;

import java.util.Random;

public class CorrelatedFilterBuilder {

    final private float selectivity;
    final private float targetCorrelation;
    final private Random random;

    public CorrelatedFilterBuilder(float selectivity, float targetCorrelation, Random random) {
        if (selectivity <= 0 || selectivity >= 1) {
            throw new IllegalArgumentException("selectivity must be between 0 and 1");
        }
        if (targetCorrelation < -1 || targetCorrelation == 0 || targetCorrelation > 1) {
            throw new IllegalArgumentException("targetCorrelation must be in the range: [-1, 0) V (0, 1]");
        }

        this.selectivity = selectivity;
        this.targetCorrelation = targetCorrelation;
        this.random = random;
    }

    /**
     * Creates a FixedBitSet filter over the input docs to achieve:
     *  1. A filter cardinality = selectivity %
     *  2. A normalized correlation ≈ targetCorrelation
     * We calculate correlation using the "point biserial" formula over the scores & filter.
     * "Normalized" means the target correlation (in the range [-1 ,1]) will be adjusted
     * proportionally to the range of possible correlations.
     * e.g. If the filter is set for all the highest scores, and point biserial correlation = 0.5,
     * then a target correlation of 0.8 will be interpreted as a point biserial correlation of 0.4.
     */
    public FixedBitSet getCorrelatedFilter(TopDocs docs) {
        // TODO: experiment deriving these based on inputs
        final float CORRELATION_TOLERANCE = 0.01f;
        final int MAX_ITER = 1000;
        final int FLIP_BATCH_SIZE = 10;

        if (docs.scoreDocs.length <= 1) {
            throw new IllegalArgumentException("topDocs must contain > 1 scoreDocs");
        }
        int n = docs.scoreDocs.length;
        FixedBitSet filter = new FixedBitSet(n);
        // Compute stdDev once and reuse as it never changes
        double stdDev = scoresStdDev(docs);
        final int filterCardinality = (int) (selectivity * n);

        // Start with largest/smallest possible correlation by
        // setting the highest (for corr > 0) / lowest (for corr < 0) scored vectors
        if (targetCorrelation > 0) {
            for (int i = 0; i < filterCardinality; i++) {
                filter.set(docs.scoreDocs[i].doc);
            }
        } else {
            for (int i = n - 1; i > n - 1 - filterCardinality; i--) {
                filter.set(docs.scoreDocs[i].doc);
            }
        }

        double currCorr = pointBiserialCorrelation(docs, filter, stdDev); // This will be the min/max correlation possible

        final double weightedTargetCorr = (targetCorrelation * currCorr) * (targetCorrelation < 0 ? -1 : 1);
        double currErr = Math.abs(currCorr - weightedTargetCorr);

        if (currErr < CORRELATION_TOLERANCE) {
            return filter;
        }

        // Attempt up to MAX_ITER flipping rounds
        for (int i = 0; i < MAX_ITER; i++) {
            FixedBitSet newFilter = filter.clone();
            for (int j = 0; j < FLIP_BATCH_SIZE; j++) {
                if (currCorr < weightedTargetCorr) {
                    // Flip a random 1 from the 10 worst scores
                    int onesToSkip = random.nextInt(10);
                    int skippedOnes = 0;
                    int k = n - 1;
                    while (k > 0 && skippedOnes <= onesToSkip) {
                        if (newFilter.get(docs.scoreDocs[k--].doc)) {
                            skippedOnes++;
                        }
                    }
                    newFilter.clear(docs.scoreDocs[k + 1].doc);

                    // Flip a random 0 from the 10 best scores
                    int zerosToSkip = random.nextInt(10);
                    int skippedZeros = 0;
                    k = 0;
                    while (k < n - 1 && skippedZeros <= zerosToSkip) {
                        if (!newFilter.get(docs.scoreDocs[k++].doc)) {
                            skippedZeros++;
                        }
                    }
                    newFilter.set(docs.scoreDocs[k - 1].doc);
                } else {
                    // Flip a random 1 from the 10 best scores
                    int onesToSkip = random.nextInt(10);
                    int skippedOnes = 0;
                    int k = 0;
                    while (k < n - 1 && skippedOnes <= onesToSkip) {
                        if (newFilter.get(docs.scoreDocs[k++].doc)) {
                            skippedOnes++;
                        }
                    }
                    newFilter.clear(docs.scoreDocs[k - 1].doc);

                    // Flip a random 0 from the 10 worst scores
                    int zerosToSkip = random.nextInt(10);
                    int skippedZeros = 0;
                    k = n - 1;
                    while (k > 0 && skippedZeros <= zerosToSkip) {
                        if (!newFilter.get(docs.scoreDocs[k--].doc)) {
                            skippedZeros++;
                        }
                    }
                    newFilter.set(docs.scoreDocs[k + 1].doc);
                }
            }

            double newCorr = pointBiserialCorrelation(docs, newFilter, stdDev);
            double newErr = Math.abs(newCorr - weightedTargetCorr);
            if (newErr < currErr) {
                filter = newFilter;
                currCorr = newCorr;
                currErr = newErr;
            }

            if (currErr < CORRELATION_TOLERANCE) {
                break;
            }
        }

        return filter;
    }

    private double pointBiserialCorrelation(TopDocs docs, FixedBitSet filter, double stdDev) {
        if (stdDev == 0) { // All scores are identical
            return 0.0;
        }

        int numScore1 = 0;
        double sumScore1 = 0.0;
        int numScore0 = 0;
        double sumScore0 = 0.0;
        for (ScoreDoc scoreDoc : docs.scoreDocs) {
            if (filter.get(scoreDoc.doc)) {
                sumScore1 += scoreDoc.score;
                numScore1++;
            } else {
                sumScore0 += scoreDoc.score;
                numScore0++;
            }
        }
        double meanScore1 = sumScore1 / numScore1;
        double meanScore0 = sumScore0 / numScore0;

        // Point-biserial correlation formula
        return ((meanScore1 - meanScore0) / stdDev) * Math.sqrt(selectivity * (1 - selectivity));
    }

    private double scoresStdDev(TopDocs docs) {
        int n = docs.scoreDocs.length;

        double sum = 0.0;
        for (ScoreDoc scoreDoc : docs.scoreDocs) {
            sum += scoreDoc.score;
        }
        double mean = sum / n;

        double sumSqDiffs = 0.0;
        for (ScoreDoc scoreDoc : docs.scoreDocs) {
            double diff = scoreDoc.score - mean;
            sumSqDiffs += diff * diff;
        }

        return Math.sqrt(sumSqDiffs / (n - 1));
    }
}
