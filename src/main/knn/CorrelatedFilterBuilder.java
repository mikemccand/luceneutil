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

import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.FixedBitSet;

import java.util.Random;

/**
 * Builds a FixedBitSet filter over the input docs to achieve:
 *  1. A filter cardinality = (selectivity)%
 *  2. A normalized correlation ≈ correlation
 */
public class CorrelatedFilterBuilder {

    final private float selectivity;
    final private float correlation;
    final private int n;

    public CorrelatedFilterBuilder(int n, float selectivity, float correlation) {
        if (selectivity <= 0 || selectivity >= 1) {
            throw new IllegalArgumentException("selectivity must be in the range (0, 1)");
        }
        if (correlation < -1 || correlation > 1) {
            throw new IllegalArgumentException("correlation must be in the range: [-1, 1]");
        }
        this.selectivity = selectivity;
        this.correlation = correlation;
        this.n = n;
    }


    /**
     * The filter is built such that:
     *  - correlation = -1 means the lowest scoring (selectivity) of docs are set
     *  - correlation = 1 means the highest scoring (selectivity) of docs are set
     * Correlation between -1 and 0 starts like -1, but a random (1 - |correlation|) of the set docs are cleared
     * and set randomly over the entire range.
     * Similarly, correlation between 0 and 1 starts like 1, and follows the same process.
     */
    public FixedBitSet getCorrelatedFilter(TopDocs docs, Random random) {
        FixedBitSet filter = new FixedBitSet(n);
        final int filterCardinality = (int) (selectivity * n);

        // Start with largest/smallest possible correlation by
        // setting the highest (for corr > 0) / lowest (for corr < 0) scored vectors
        if (correlation > 0) {
            for (int i = 0; i < filterCardinality; i++) {
                filter.set(docs.scoreDocs[i].doc);
            }
        } else {
            for (int i = n - 1; i > n - 1 - filterCardinality; i--) {
                filter.set(docs.scoreDocs[i].doc);
            }
        }

        // Randomly flip (1 - |correlation|) of the set bits
        final int amountToFlip = (int) ((1 - Math.abs(correlation)) * filterCardinality);
        int flipped = 0;
        while (flipped < amountToFlip) {
            int i;
            if (correlation > 0) {
                i = random.nextInt(filterCardinality);
            } else {
                i = random.nextInt(n - filterCardinality, n);
            }
            if (filter.getAndClear(docs.scoreDocs[i].doc)) {
                setRandomClearBit(filter, random);
                flipped++;
            }
        }

        return filter;
    }

    private void setRandomClearBit(FixedBitSet bitSet, Random random) {
        int randomBit;
        do {
            randomBit = random.nextInt(bitSet.length());
        } while (bitSet.get(randomBit));
        bitSet.set(randomBit);
    }

}
