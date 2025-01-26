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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatMultiVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.List;

public class MultiVectorBenchmarkKnnFloatQuery extends KnnFloatMultiVectorQuery {

  public MultiVectorBenchmarkKnnFloatQuery(String field, float[] target, int k) {
    super(field, target, k);
  }

  public MultiVectorBenchmarkKnnFloatQuery(String field, float[] target, int k, Query filter) {
    super(field, target, k, filter);
  }

  @Override
  public TopDocs exactSearch(LeafReaderContext context, DocIdSetIterator acceptIterator, QueryTimeout queryTimeout) throws IOException {
    return super.exactSearch(context, acceptIterator, queryTimeout);
  }

  public static TopDocs runExactSearch(IndexReader reader, MultiVectorBenchmarkKnnFloatQuery query) throws IOException {
    IndexSearcher searcher = new IndexSearcher(reader);
    BooleanQuery booleanQuery =
        new BooleanQuery.Builder()
            .add(new FieldExistsQuery(query.getField()), BooleanClause.Occur.FILTER)
            .build();
    Query rewritten = searcher.rewrite(booleanQuery);
    final Weight filterWeight = searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1f);

    List<LeafReaderContext> leafReaderContexts = reader.leaves();
    TopDocs[] perLeafResults = new TopDocs[leafReaderContexts.size()];
    int leaf = 0;
    for (LeafReaderContext ctx : leafReaderContexts) {
      DocIdSetIterator filterIterator = filterWeight.scorer(ctx).iterator();
      LeafReader ctxReader = ctx.reader();
      final Bits liveDocs = ctxReader.getLiveDocs();
      BitSet acceptDocs = createBitSet(filterIterator, liveDocs, ctxReader.maxDoc());
      final int cost = acceptDocs.cardinality();
      perLeafResults[leaf] = query.exactSearch(ctx, new BitSetIterator(acceptDocs, cost), null);
      if (ctx.docBase > 0) {
        for (ScoreDoc scoreDoc : perLeafResults[leaf].scoreDocs) {
          scoreDoc.doc += ctx.docBase;
        }
      }
      leaf++;
    }
    return query.mergeLeafResults(perLeafResults);
  }

  private static BitSet createBitSet(DocIdSetIterator iterator, Bits liveDocs, int maxDoc)
      throws IOException {
    if (liveDocs == null && iterator instanceof BitSetIterator bitSetIterator) {
      // If we already have a BitSet and no deletions, reuse the BitSet
      return bitSetIterator.getBitSet();
    } else {
      // Create a new BitSet from matching and live docs
      FilteredDocIdSetIterator filterIterator =
          new FilteredDocIdSetIterator(iterator) {
            @Override
            protected boolean match(int doc) {
              return liveDocs == null || liveDocs.get(doc);
            }
          };
      return BitSet.of(filterIterator, maxDoc);
    }
  }
}
