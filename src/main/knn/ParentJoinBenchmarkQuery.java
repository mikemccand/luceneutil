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

//package knn;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;

import java.io.IOException;
import java.util.List;

import static KnnGraphTester.*;

/** Exposes functions to directly invoke {@link DiversifyingChildrenFloatKnnVectorQuery#exactSearch}
 */
public class ParentJoinBenchmarkQuery extends DiversifyingChildrenFloatKnnVectorQuery {

  public static final BitSetProducer parentsFilter =
    new QueryBitSetProducer(new TermQuery(new Term(DOCTYPE_FIELD, DOCTYPE_PARENT)));

  private static final TermQuery childDocQuery = new TermQuery(new Term(DOCTYPE_FIELD, DOCTYPE_CHILD));

  ParentJoinBenchmarkQuery(float[] queryVector, Query childFilter, int k) throws IOException {
    super(KNN_FIELD, queryVector, childFilter, k, parentsFilter);
  }

  // expose for benchmarking
  @Override
  public TopDocs exactSearch(LeafReaderContext context, DocIdSetIterator acceptIterator, QueryTimeout queryTimeout) throws IOException {
    return super.exactSearch(context, acceptIterator, queryTimeout);
  }

  public static TopDocs runExactSearch(IndexReader reader, ParentJoinBenchmarkQuery query) throws IOException {
    IndexSearcher searcher = new IndexSearcher(reader);
    List<LeafReaderContext> leafReaderContexts = reader.leaves();
    TopDocs[] perLeafResults = new TopDocs[leafReaderContexts.size()];
    int leaf = 0;
    for (LeafReaderContext ctx : leafReaderContexts) {
      Weight childrenWeight = childDocQuery.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
      DocIdSetIterator acceptDocs = childrenWeight.scorer(ctx).iterator();
      perLeafResults[leaf] = query.exactSearch(ctx, acceptDocs, null);
      if (ctx.docBase > 0) {
        for (ScoreDoc scoreDoc : perLeafResults[leaf].scoreDocs) {
          scoreDoc.doc += ctx.docBase;
        }
      }
      leaf++;
    }
    return query.mergeLeafResults(perLeafResults);
  }
}
