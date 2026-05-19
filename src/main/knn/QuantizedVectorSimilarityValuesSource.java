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

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.VectorScorer;

/**
 * A {@link DoubleValuesSource} that computes vector similarity against the field's quantized
 * vectors by calling {@link FloatVectorValues#scorer(float[])}.
 *
 * <p>This is a workaround for the missing quantized counterpart to Lucene's
 * {@code FullPrecisionFloatVectorSimilarityValuesSource}, which reranks using full-precision
 * float32 vectors regardless of how the field was indexed. See {@link KnnGraphTester} where this
 * is wired in for the full story.
 */
public class QuantizedVectorSimilarityValuesSource extends DoubleValuesSource {

  private final float[] queryVector;
  private final String fieldName;

  public QuantizedVectorSimilarityValuesSource(float[] queryVector, String fieldName) {
    this.queryVector = queryVector;
    this.fieldName = fieldName;
  }

  @Override
  public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
    FloatVectorValues vectorValues = ctx.reader().getFloatVectorValues(fieldName);
    if (vectorValues == null) {
      FloatVectorValues.checkField(ctx.reader(), fieldName);
      return DoubleValues.EMPTY;
    }
    FieldInfo fi = ctx.reader().getFieldInfos().fieldInfo(fieldName);
    if (fi.getVectorDimension() != queryVector.length) {
      throw new IllegalArgumentException(
        "query vector dimension does not match field dimension: " + queryVector.length
          + " != " + fi.getVectorDimension() + " (field=" + fieldName + ")");
    }
    // unlike FullPrecisionFloatVectorSimilarityValuesSource which calls .rescorer() (i.e. raw
    // float32), we call .scorer() so the rerank pass reads the actual quantized vectors of the
    // rerank field.
    final VectorScorer scorer = vectorValues.scorer(queryVector);
    if (scorer == null) {
      return DoubleValues.EMPTY;
    }
    final DocIdSetIterator iterator = scorer.iterator();
    return new DoubleValues() {
      @Override
      public double doubleValue() throws IOException {
        return scorer.score();
      }

      @Override
      public boolean advanceExact(int doc) throws IOException {
        return doc >= iterator.docID() && (iterator.docID() == doc || iterator.advance(doc) == doc);
      }
    };
  }

  @Override
  public boolean needsScores() {
    return false;
  }

  @Override
  public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
    return this;
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, Arrays.hashCode(queryVector));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    QuantizedVectorSimilarityValuesSource other = (QuantizedVectorSimilarityValuesSource) obj;
    return Objects.equals(fieldName, other.fieldName)
      && Arrays.equals(queryVector, other.queryVector);
  }

  @Override
  public String toString() {
    return "QuantizedVectorSimilarityValuesSource(fieldName=" + fieldName + " queryVector="
      + Arrays.toString(queryVector) + ")";
  }
}
