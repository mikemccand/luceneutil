package perf;
/**
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
import java.util.Random;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

class RandomFilter extends Filter {
  final double fractionKeep;
  final long randomSeed;

  public RandomFilter(double pctKeep, long randomSeed) {
    this.fractionKeep = pctKeep/100.0;
    this.randomSeed = randomSeed;
  }

  @Override
  public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) {
    final Random rand = new Random(randomSeed);
    final int maxDoc = context.reader().maxDoc();
    FixedBitSet bits = new FixedBitSet(maxDoc);
    for(int docID = 0;docID<maxDoc;docID++) {
      if (rand.nextDouble() <= fractionKeep) {        
        bits.set(docID);
      }
    }

    return BitsFilteredDocIdSet.wrap(bits, acceptDocs);
  }

  @Override
  public int hashCode() {
    return new Double(fractionKeep).hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return this == other;
  }

  @Override
  public String toString() {
    return "RandomFilter(seed=" + randomSeed + " pctKeep=" + (100.0*fractionKeep) + "%)";
  }
}
