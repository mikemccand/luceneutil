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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
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
    Arrays.fill(answers, -1);
  }

  public PKLookupTask(int maxDoc, Random random, int count, Set<BytesRef> seen, int ord) {
    this.ord = ord;
    ids = new BytesRef[count];
    answers = new int[count];
    Arrays.fill(answers, -1);
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
      final List<AtomicReaderContext> subReaders = searcher.getIndexReader().leaves();
      final TermsEnum[] termsEnums = new TermsEnum[subReaders.size()];
      final DocsEnum[] docsEnums = new DocsEnum[subReaders.size()];
      for(int subIDX=0;subIDX<subReaders.size();subIDX++) {
        termsEnums[subIDX] = subReaders.get(subIDX).reader().fields().terms("id").iterator(null);
      }

      for(int idx=0;idx<ids.length;idx++) {
        int base = 0;
        final BytesRef id = ids[idx];
        for(int subIDX=0;subIDX<subReaders.size();subIDX++) {
          final AtomicReader sub = subReaders.get(subIDX).reader();
          final TermsEnum termsEnum = termsEnums[subIDX];
          //System.out.println("\nTASK: sub=" + sub);
          //System.out.println("TEST: lookup " + ids[idx].utf8ToString());
          if (termsEnum.seekExact(id, false)) { 
            //System.out.println("  found!");
            final DocsEnum docs = docsEnums[subIDX] = termsEnum.docs(sub.getLiveDocs(), docsEnums[subIDX], 0);
            assert docs != null;
            final int docID = docs.nextDoc();
            if (docID != DocsEnum.NO_MORE_DOCS) {
              answers[idx] = base + docID;
              break;
            }
          }
          base += sub.maxDoc();
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

