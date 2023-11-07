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
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;

final class PKLookupWithTermStateTask extends Task {
  private final BytesRef[] ids;
  private final int[] answers;
  private final int ord;

  @Override
  public String getCategory() {
    return "PKLookupWithTermState";
  }

  private PKLookupWithTermStateTask(PKLookupWithTermStateTask other) {
    ids = other.ids;
    ord = other.ord;
    answers = new int[ids.length];
    Arrays.fill(answers, -1);
  }

  public PKLookupWithTermStateTask(int maxDoc, Random random, int count, Set<BytesRef> seen, int ord) {
    this.ord = ord;
    ids = new BytesRef[count];
    answers = new int[count];
    Arrays.fill(answers, -1);
    int idx = 0;
    while(idx < count) {
      final BytesRef id = new BytesRef(LineFileDocs.intToID(random.nextInt(maxDoc)));
      if (seen.contains(id) == false) {
        seen.add(id);
        ids[idx++] = id;
      }
    }
  }

  @Override
  public Task clone() {
    return new PKLookupWithTermStateTask(this);
  }

  @Override
  public void go(IndexState state, TaskParser taskParser) throws IOException {

    final IndexSearcher searcher = state.mgr.acquire();
    try {
      final List<LeafReaderContext> subReaders = searcher.getIndexReader().leaves();
      IndexState.PKLookupWithTermStateState[] pkStates = new IndexState.PKLookupWithTermStateState[subReaders.size()];
      for(int subIDX=0;subIDX<subReaders.size();subIDX++) {
        LeafReaderContext ctx = subReaders.get(subIDX);
        ThreadLocal<IndexState.PKLookupWithTermStateState> states = state.pkLookupWithTermStateStates.get(ctx.reader().getCoreCacheHelper().getKey());
        // NPE here means you are trying to use this task on a newly refreshed NRT reader!
        IndexState.PKLookupWithTermStateState pkState = states.get();
        if (pkState == null) {
          pkState = new IndexState.PKLookupWithTermStateState(ctx.reader(), "id");
          states.set(pkState);
        }
        pkStates[subIDX] = pkState;
      }

      // first pass: seek by term and record TermState when a segment has the term
      for(int idx=0;idx<ids.length;idx++) {
        final BytesRef id = ids[idx];
        for(int subIDX=0;subIDX<subReaders.size();subIDX++) {
          IndexState.PKLookupWithTermStateState pkState = pkStates[subIDX];
          //System.out.println("\nTASK: sub=" + sub);
          //System.out.println("TEST: lookup " + ids[idx].utf8ToString());
          if (pkState.termsEnum.seekExact(id)) { 
            //System.out.println("  found!");
            pkState.termStates.put(id, pkState.termsEnum.termState());
          }
        }
      }

      // second pass: actually resolve each term to docid
      for(int idx=0;idx<ids.length;idx++) {
        final BytesRef id = ids[idx];
        int base = 0;
        for(int subIDX=0;subIDX<subReaders.size();subIDX++) {
          IndexState.PKLookupWithTermStateState pkState = pkStates[subIDX];
          TermState termState = pkState.termStates.get(id);
          if (termState != null) {
            // this segment has this id
            pkState.termsEnum.seekExact(id, termState);
            PostingsEnum docs = pkState.termsEnum.postings(pkState.postingsEnum, 0);
            assert docs != null;
            int docID = DocIdSetIterator.NO_MORE_DOCS;
            for (int d = docs.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = docs.nextDoc()) {
              if (pkState.liveDocs == null || pkState.liveDocs.get(d)) {
                // stop iterating for additional docs since we know this is a unique id
                docID = d;
                break;
              }
            }
            if (docID != DocIdSetIterator.NO_MORE_DOCS) {
              answers[idx] = base + docID;
              // break out of subIDX loop since we know this is a unique id, no other segment will have it
              break;
            }
          }
          base += subReaders.get(subIDX).reader().maxDoc();
        }
      }
    } finally {
      state.mgr.release(searcher);
    }
  }

  @Override
  public String toString() {
    return "PKTS" + ord + "[" + ids.length + "]";
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
        if (!state.hasDeletions) {
          throw new RuntimeException("PKLookupWithTermState: id=" + LineFileDocs.idToInt(ids[idx].utf8ToString()) + " failed to find a matching document");
        } else {
          // TODO: we should verify that these are in fact
          // the deleted docs...
          continue;
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

