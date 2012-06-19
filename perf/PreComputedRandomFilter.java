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

import java.util.List;
import java.util.Random;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

// TODO: can't we use RandomFilter here...?
final class PreComputedRandomFilter extends Filter {

  private final FixedBitSet[] segmentBits;
  private final double pct;

  public PreComputedRandomFilter(DirectoryReader reader, Random random, double pctAcceptDocs) {
    //System.out.println("FILT: pct=" + pctAcceptDocs);
    this.pct = pctAcceptDocs;
    pctAcceptDocs /= 100.0;
    final List<AtomicReaderContext> subReaders = reader.getTopReaderContext().leaves();
    segmentBits = new FixedBitSet[subReaders.size()];
    for(int segID=0;segID<subReaders.size();segID++) {
      final int maxDoc = subReaders.get(segID).reader().maxDoc();
      final FixedBitSet bits = segmentBits[segID] = new FixedBitSet(maxDoc);
      int count = 0;
      for(int docID=0;docID<maxDoc;docID++) {
        if (random.nextDouble() <= pctAcceptDocs) {
          bits.set(docID);
          count++;
        }
      }
      //System.out.println("  r=" + maxDoc + " ct=" + count);
    }
  }

  @Override
  public int hashCode() {
    return new Double(pct).hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return this == other;
  }

  @Override
  public String toString() {
    return "PreComputedRandomFilter(pctAccept=" + pct + ")";
  }

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) {
    final FixedBitSet bits = segmentBits[context.ord];
    assert context.reader().maxDoc() == bits.length();
    return BitsFilteredDocIdSet.wrap(bits, acceptDocs);
  }
}

