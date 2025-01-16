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

import knn.CorrelatedFilterBuilder;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.FixedBitSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

// This may take >2min to run
public class CorrelatedFilterBuilderTest {

    // We test over every combination of the following (numDocs, selectivities, correlations)
    static Stream<Arguments> provideTestParameters() {
        int[] numDocs = {10_000, 100_000, 1_000_000};
        double[] selectivities = {0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95};
        double[] correlations = {-1.0, -0.75, -0.5, -0.25, 0.25, 0.5, 0.75, 1.0};

        return Arrays.stream(numDocs)
                .boxed()
                .flatMap(numDoc ->
                        Arrays.stream(selectivities)
                                .boxed()
                                .flatMap(selectivity ->
                                        Arrays.stream(correlations)
                                                .boxed()
                                                .map(correlation -> Arguments.of(numDoc, selectivity, correlation))
                                )
                );
    }

    @ParameterizedTest
    @MethodSource("provideTestParameters")
    public void testSelectivityCorrelationCombinations(int numDocs, double targetSelectivity, double targetCorr) {
        Random random = new Random(42);

        CorrelatedFilterBuilder correlatedFilterBuilder = new CorrelatedFilterBuilder((float) targetSelectivity,
                (float) targetCorr, random);
        TopDocs docs = generateTopDocs(numDocs, random);
        FixedBitSet filter = correlatedFilterBuilder.getCorrelatedFilter(docs);
        double stdDev = correlatedFilterBuilder.scoresStdDev(docs);
        double weightedTargetCorr = calculateWeightedCorrTarget(correlatedFilterBuilder, docs,
                (int) (numDocs * targetSelectivity), (float) targetCorr, stdDev);

        float actualSelectivity = (float) filter.cardinality() / numDocs;
        double actualCorr = correlatedFilterBuilder.pointBiserialCorrelation(docs, filter, stdDev);

        assertEquals((float) targetSelectivity, actualSelectivity, "Selectivity should be exactly equal to the target");
        assertTrue(
                Math.abs(weightedTargetCorr - actualCorr) <= 0.02,
                String.format("Correlation should be within 0.02 of target. Expected: %f, Actual: %f",
                        weightedTargetCorr, actualCorr)
        );
    }

    private TopDocs generateTopDocs(int n, Random random) {
        ScoreDoc[] scoreDocs = new ScoreDoc[n];
        for (int i = 0; i < n; i++) {
            scoreDocs[i] = new ScoreDoc(i, random.nextFloat(100));
        }
        Arrays.sort(scoreDocs, (a, b) -> Float.compare(b.score, a.score));
        return new TopDocs(new TotalHits(n, TotalHits.Relation.EQUAL_TO), scoreDocs);
    }

    private double calculateWeightedCorrTarget(CorrelatedFilterBuilder correlatedFilterBuilder, TopDocs docs,
                                               int filterCardinality, float targetCorr, double stdDev) {
        FixedBitSet filter = new FixedBitSet(docs.scoreDocs.length);
        for (int i = 0; i < filterCardinality; i++) {
            filter.set(docs.scoreDocs[i].doc);
        }
        double pbCorr = correlatedFilterBuilder.pointBiserialCorrelation(docs, filter, stdDev);
        return (targetCorr * pbCorr);
    }
}