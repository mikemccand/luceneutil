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
import java.util.Arrays;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;

final class PKLookupTask extends Task {
  private final BytesRef[] ids;
  private final int[] answers;
  private final int ord;

  @Override
  public String getCategory() {
    return "PKLookup";
  }

  private PKLookupTask(PKLookupTask other) {
    ids = other.ids;
    ord = other.ord;
    answers = new int[ids.length];
  }

  public PKLookupTask(int maxDoc, Random random, int count, Set<BytesRef> seen, int ord) {
    this.ord = ord;
    ids = new BytesRef[count];
    answers = new int[count];
    int idx = 0;
    while(idx < count) {
      final BytesRef id = new BytesRef(LineFileDocs.intToID(random.nextInt(maxDoc)));
      /*
        if (idx == 0) {
        id = new BytesRef("000013688");
        } else {
        id = new BytesRef(LineFileDocs.intToID(random.nextInt(maxDoc)));
        }
      */
      if (!seen.contains(id)) {
        seen.add(id);
        ids[idx++] = id;
      }
    }
    Arrays.sort(ids);
  }

  @Override
  public Task clone() {
    return new PKLookupTask(this);
  }

  @Override
  public void go(IndexState state) throws IOException {

    final IndexSearcher searcher = state.mgr.acquire();
    try {
      final boolean DO_DOC_LOOKUP = true;
      int base = 0;
      for (IndexReader sub : ((DirectoryReader) searcher.getIndexReader()).getSequentialSubReaders()) {
        final TermsEnum termsEnum = ((AtomicReader) sub).fields().terms("id").iterator(null);

        DocsEnum docs = null;
        //System.out.println("\nTASK: sub=" + sub);
        for(int idx=0;idx<ids.length;idx++) {
          //System.out.println("TEST: lookup " + ids[idx].utf8ToString());
          if (termsEnum.seekExact(ids[idx], false)) { 
            //System.out.println("  found!");
            docs = termsEnum.docs(null, docs, false);
            assert docs != null;
            final int docID = docs.nextDoc();
            if (docID == DocsEnum.NO_MORE_DOCS) {
              answers[idx] = -1;
            } else {
              answers[idx] = base + docID;
              //System.out.println("  docID=" + docID);
            }
          }
        }
        base += sub.maxDoc();
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
  public void printResults(IndexState state) throws IOException {
    for(int idx=0;idx<ids.length;idx++) {

      if (answers[idx] == DocsEnum.NO_MORE_DOCS) {
        throw new RuntimeException("PKLookup: id=" + ids[idx].utf8ToString() + " failed to find a matching document");
      }

      final int id = LineFileDocs.idToInt(ids[idx]);
      //System.out.println("  " + id + " -> " + answers[idx]);
      final int actual = state.docIDToID[answers[idx]];
      if (actual != id) {
        throw new RuntimeException("PKLookup: id=" + LineFileDocs.intToID(id) + " returned doc with id=" + LineFileDocs.intToID(actual) + " docID=" + answers[idx]);
      }
    }
  }
}

