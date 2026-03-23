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

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;

final class PKLookupTask extends Task {
  private final BytesRef[] ids;
  private final int[] answers;
  private final int ord;
  private static final int NON_EXISTENT_IDS_PERCENT = 10;
  private final Set<BytesRef> syntheticMissIds;

  @Override
  public String getCategory() {
    return "PKLookup";
  }

  private PKLookupTask(PKLookupTask other) {
    ids = other.ids;
    ord = other.ord;
    answers = new int[ids.length];
    syntheticMissIds = other.syntheticMissIds;
    Arrays.fill(answers, -1);
  }

  public PKLookupTask(int maxDoc, Random random, int count, Set<BytesRef> seen, int ord) {
    this.ord = ord;
    ids = new BytesRef[count];
    answers = new int[count];
    Arrays.fill(answers, -1);
    syntheticMissIds = new HashSet<>();

    int missCount = (count * NON_EXISTENT_IDS_PERCENT) / 100;
    if (missCount == 0 && NON_EXISTENT_IDS_PERCENT > 0) {
      throw new IllegalArgumentException("count=" + count + " is too small to " +
              "produce any non-existent IDs with NON_EXISTENT_IDS_PERCENT=" + NON_EXISTENT_IDS_PERCENT);
    }
    int hitCount = count - missCount;

    int idx = 0;
    while (idx < count) {
      BytesRef id;
      if (idx < hitCount) {
        id = new BytesRef(LineFileDocs.intToID(random.nextInt(maxDoc)));
      } else {
        // Corrupt a real-looking ID by replacing a random digit with an uppercase letter.
        // Since intToID produces base-36 [0-9a-z], an uppercase char guarantees a non-existent ID
        // while keeping it naturally distributed among real IDs in sorted order.
        char[] chars = LineFileDocs.intToID(random.nextInt(maxDoc)).toCharArray();
        int pos = random.nextInt(chars.length);
        chars[pos] = (char) ('A' + random.nextInt(26));
        id = new BytesRef(new String(chars));
      }
      if (seen.contains(id) == false) {
        seen.add(id);
        if (idx >= hitCount) {
          syntheticMissIds.add(id);
        }
        ids[idx++] = id;
      }
    }

    // Sort so lookups are in term-order, optimizing for Lucene's fastest batch PK lookup use case
    Arrays.sort(ids);
  }

  @Override
  public Task clone() {
    return new PKLookupTask(this);
  }

  @Override
  public void go(IndexState state, TaskParser taskParser) throws IOException {
    final IndexSearcher searcher = state.mgr.acquire();
    try {
        final List<LeafReaderContext> subReaders = searcher.getIndexReader().leaves();

        IndexState.PKLookupState[] pkStates = new IndexState.PKLookupState[subReaders.size()];
        for (int subIDX = 0; subIDX < subReaders.size(); subIDX++) {
            LeafReaderContext ctx = subReaders.get(subIDX);
            ThreadLocal<IndexState.PKLookupState> states = state.pkLookupStates.get(ctx.reader().getCoreCacheHelper().getKey());
            // NPE here means you are trying to use this task on a newly refreshed NRT reader!
            IndexState.PKLookupState pkState = states.get();
          if (pkState == null) {
              pkState = new IndexState.PKLookupState(ctx.reader(), "id");
              states.set(pkState);
            }
            pkStates[subIDX] = pkState;
        }

        for (int idx = 0; idx < ids.length; idx++) {
          final BytesRef id = ids[idx];
          boolean found = false;
          for (int subIDX : state.leafOrder) {
            IndexState.PKLookupState pkState = pkStates[subIDX];
            //System.out.println("\nTASK: sub=" + sub);
            //System.out.println("TEST: lookup " + ids[idx].utf8ToString());
            if (pkState.termsEnum.seekExact(id)) {
              //System.out.println("  found!");
              PostingsEnum docs = pkState.termsEnum.postings(pkState.postingsEnum, 0);
              assert docs != null;
              for (int d = docs.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = docs.nextDoc()) {
                if (pkState.liveDocs == null || pkState.liveDocs.get(d)) {
                  answers[idx] = subReaders.get(subIDX).docBase + d;
                  found = true;
                  break;
                }
              }
              if (found) {
                break;
              }
            }
          }
        }
    } finally {
        state.mgr.release(searcher);
    }
  }

  @Override
  public String toString() {
    return "PK" + ord + "[" + ids.length + "]";
  }

  @Override
  public long checksum() {
    // TODO, but, not sure it makes sense since we will
    // run a different PK lookup each time...?
    return 0;
  }

  @Override
  public void printResults(PrintStream out, IndexState state) throws IOException {
    for(int idx=0;idx<ids.length;idx++) {
      if (answers[idx] == -1) {
        if (syntheticMissIds.contains(ids[idx])) {
          // Expected miss -- this is a synthetic non-existent ID
          continue;
        } else if (state.hasDeletions) {
          // Could be a deleted doc
          // TODO: we should verify that these are in fact the deleted docs...
          continue;
        } else {
          throw new RuntimeException("PKLookup: id=" + ids[idx].utf8ToString() + " failed to find a matching document");
        }
      }

      /*
      final int id = LineFileDocs.idToInt(ids[idx]);
      //System.out.println("  " + id + " -> " + answers[idx]);
      final int actual = state.docIDToID[answers[idx]];
      if (actual != id) {
        throw new RuntimeException("PKLookup: id=" + LineFileDocs.intToID(id) + " returned doc with id=" + LineFileDocs.intToID(actual) + " docID=" + answers[idx]);
      }
      */
    }
  }
}

