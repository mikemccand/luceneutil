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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.CheckJoinIndex;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.List;

public class ParentJoinBenchmarkQuery extends DiversifyingChildrenFloatKnnVectorQuery {

  IndexReader reader;
  int topK;

  static ParentJoinBenchmarkQuery create(IndexReader reader, String knnField, String parentField, float[] queryVector, int topK) throws IOException {
    BitSetProducer parentsFilter =
        new QueryBitSetProducer(new TermQuery(new Term(parentField, "_parent")));
    CheckJoinIndex.check(reader, parentsFilter);
    System.out.println("Index is parentJoin eligible...");
    System.out.println("Creating ParentJoinQuery. Reader statistics:");
    System.out.flush();
    return new ParentJoinBenchmarkQuery(reader, knnField, queryVector, null, topK, parentsFilter);
  }

  ParentJoinBenchmarkQuery(IndexReader reader, String field, float[] query, Query childFilter, int k, BitSetProducer parentsFilter) throws IOException {
    super(field, query, childFilter, k, parentsFilter);
    this.reader = reader;
    this.topK = k;
  }

  // expose for benchmarking
  @Override
  public TopDocs exactSearch(LeafReaderContext context, DocIdSetIterator acceptIterator, QueryTimeout queryTimeout) throws IOException {
    return super.exactSearch(context, acceptIterator, queryTimeout);
  }

  public TopDocs runExactSearch() throws IOException {
    IndexSearcher searcher = new IndexSearcher(reader);
    List<LeafReaderContext> leafReaderContexts = reader.leaves();
    TopDocs[] perLeafResults = new TopDocs[leafReaderContexts.size()];
    int leaf = 0;
    System.out.println("Num leaves in index: " + leafReaderContexts.size());
    for (LeafReaderContext ctx : leafReaderContexts) {
      final LeafReader r = ctx.reader();
      TermQuery children = new TermQuery(new Term("docType", "_child"));
      Weight childrenWeight = children.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
      DocIdSetIterator acceptDocs = childrenWeight.scorer(ctx).iterator();
//      Bits liveDocs = r.getLiveDocs();
//      FilteredDocIdSetIterator acceptDocs =
//          new FilteredDocIdSetIterator(DocIdSetIterator.all(r.maxDoc())) {
//            @Override
//            protected boolean match(int doc) {
//              return liveDocs == null || liveDocs.get(doc);
//            }
//          };
      System.out.println("Running exactSearch for leaf: " + leaf);
      System.out.println("Leaf maxdoc: " + r.maxDoc());
      System.out.println("Leaf numDocs: " + r.numDocs());
      System.out.println("Accept Docs with childrenWeight DISI cost: " + acceptDocs.cost());
      System.out.flush();
      perLeafResults[leaf] = exactSearch(ctx, acceptDocs, null);
      if (ctx.docBase > 0) {
        for (ScoreDoc scoreDoc : perLeafResults[leaf].scoreDocs) {
          scoreDoc.doc += ctx.docBase;
        }
      }
      leaf++;
    }
    return super.mergeLeafResults(perLeafResults);
  }

  private static void log(String msg, Object... args) {
    System.out.printf((msg) + "%n", args);
  }
}
