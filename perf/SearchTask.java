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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;

public class SearchTask extends Thread {

  public final static int NUM_PK_LOOKUP = 2000;

  protected final IndexSearcher s;
  private final QueryAndSort[] queries;
  private final int numIter;
  public final List<Result> results = new ArrayList<Result>();
  private final IndexReader[] subReaders;
  private final int[] pkAnswers;
  private final DirectSpellChecker spellChecker = new DirectSpellChecker();

  public SearchTask(Random r, IndexSearcher s, List<QueryAndSort> queriesList,
                    int numIter, boolean shuffle) {
    this.s = s;
    if (queriesList != null) {
      List<QueryAndSort> queries = new ArrayList<QueryAndSort>(queriesList);
      if (shuffle) {
        Collections.shuffle(queries, r);
      }
      this.queries = queries.toArray(new QueryAndSort[queries.size()]);
    } else {
      this.queries = null;
    }
    this.numIter = numIter;
    this.subReaders = s.getIndexReader().getSequentialSubReaders();
    pkAnswers = new int[NUM_PK_LOOKUP];
    spellChecker.setThresholdFrequency(1.0f);
  }

  public int[] getPKAnswers() {
    return pkAnswers;
  }

  // TODO: refactor to its own "task"
  // TODO: also test lookup of non-existent IDs
  // TODO: respect del docs?
  // TODO: need to test on index that had some number of
  //       updated docs (so that PK will match N-1 del docs
  //       and 1 non-del doc)

  // Bulk resolve array of PKs into their docIDs
  public final long pkLookup(BytesRef[] ids, int[] answers) throws IOException {
    assert ids.length == answers.length;
    long sum = 0;
    final boolean DO_DOC_LOOKUP = true;
    int base = 0;
    for(IndexReader sub : subReaders) {
      DocsEnum docs = null;
      final TermsEnum termsEnum = sub.fields().terms("id").getThreadTermsEnum();
      for(int idx=0;idx<ids.length;idx++) {
        //if (TermsEnum.SeekStatus.FOUND == termsEnum.seek(ids[idx], false, true)) { 
        if (TermsEnum.SeekStatus.FOUND == termsEnum.seek(ids[idx], false)) { 
          docs = termsEnum.docs(null, docs);
          assert docs != null;
          final int docID = docs.nextDoc();
          if (docID == DocsEnum.NO_MORE_DOCS) {
            answers[idx] = -1;
          } else {
            answers[idx] = base + docID;
            sum += docID;
          }
        }
      }
      base += sub.maxDoc();
    }

    return sum;
  }

  public final SuggestWord[] doRespell(String term) throws IOException {
    return spellChecker.suggestSimilar(new Term("body", term),
                                       10, s.getIndexReader(),
                                       true);
  }

  public void run() {
    try {
      final IndexSearcher s = this.s;
      final QueryAndSort[] queries = this.queries;
      long totSum = 0;
      for (int iter = 0; iter < numIter; iter++) {
        for (int q = 0; q < queries.length; q++) {
          final QueryAndSort qs = queries[q];

          long check = 0;
          Result r = new Result();
          final long delay;
          long t0 = System.nanoTime();
          if (qs.respell != null) {
            final SuggestWord[] corrections = doRespell(qs.respell);
            delay = System.nanoTime() - t0;
            for(SuggestWord suggest: corrections) {
              check += suggest.string.hashCode() + suggest.freq;
            }
            r.totHits = corrections.length;
            totSum += check;
          } else if (qs.pkIDs != null) {
            check = pkLookup(qs.pkIDs, pkAnswers);
            delay = System.nanoTime() - t0;
            totSum += check;
            r.totHits = qs.pkIDs.length;
          } else {
            final TopDocs hits;
            if (qs.s == null && qs.f == null) {
              hits = s.search(qs.q, 10);
            } else if (qs.s == null && qs.f != null) {
              hits = s.search(qs.q, qs.f, 10);
            } else {
              hits = s.search(qs.q, qs.f, 10, qs.s);
            }
            delay = System.nanoTime() - t0;
            r.totHits = hits.totalHits;
            for (int i = 0; i < hits.scoreDocs.length; i++) {
              totSum += hits.scoreDocs[i].doc;
              check += hits.scoreDocs[i].doc;
            }
            processHits(hits);
            totSum += hits.totalHits;
          }
          r.t = delay;
          r.qs = qs;
          r.check = check;
          results.add(r);
        }
      }
      System.out.println("checksum=" + totSum);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
  
  protected void processHits(TopDocs hits) throws IOException {
    //
  }
}
