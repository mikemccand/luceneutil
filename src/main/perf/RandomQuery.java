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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.AbstractDocIdSetIterator;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

import java.io.IOException;

public class RandomQuery extends Query {

  final double fractionKeep;

  public RandomQuery(double pctKeep) {
    this.fractionKeep = pctKeep / 100.0;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    return new ConstantScoreWeight(this, boost) {
      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final int maxDoc = context.reader().maxDoc();
        final int interval = (int) (1 / fractionKeep);
        final DocIdSetIterator iterator = new AbstractDocIdSetIterator() {

          @Override
          public int nextDoc() throws IOException {
            int intervalId = (doc + interval) / interval;
            int addend = (31 * intervalId) % interval;
            int newDoc = intervalId * interval + addend;
            assert newDoc > doc;
            if (newDoc >= maxDoc) {
              return doc = NO_MORE_DOCS;
            } else {
              return doc = newDoc;
            }
          }

          @Override
          public int advance(int target) throws IOException {
            if (target >= maxDoc) {
              return doc = NO_MORE_DOCS;
            }
            int intervalId = target / interval;
            int addend = (31 * intervalId) % interval;
            doc = intervalId * interval + addend;
            if (doc < target) {
              intervalId++;
              addend = (31 * intervalId) % interval;
              doc = intervalId * interval + addend;
            }
            assert doc >= target;
            if (doc >= maxDoc) {
              return doc = NO_MORE_DOCS;
            }
            return doc;
          }

          @Override
          public long cost() {
            return maxDoc / interval;
          }

        };
        return new DefaultScorerSupplier(new ConstantScoreScorer(score(), ScoreMode.COMPLETE, iterator));
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return false;
      }
    };
  }

  public void visit(QueryVisitor visitor) {
  }

  @Override
  public String toString(String field) {
    return "RandomQuery(fractionKeep=" + fractionKeep + ")";
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + Double.valueOf(fractionKeep).hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return sameClassAs(obj) && fractionKeep == ((RandomQuery) obj).fractionKeep;
  }
}
