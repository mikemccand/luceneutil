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

package org.apache.lucene.util;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.VectorUtil;

/**
 * Check adjacent documents to see if they have the same vector??
 */
public class CheckVectors {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("usage: CheckVectors <index>");
      System.exit(-1);
    }
    check(args[0]);
  }

  static void check (String indexDir) throws IOException {
    try (FSDirectory dir = FSDirectory.open(Paths.get(indexDir));
         DirectoryReader reader = DirectoryReader.open(dir)) {
      for (LeafReaderContext ctx : reader.leaves()) {
        LeafReader leaf = ctx.reader();
        for (int i = 0; i < leaf.maxDoc(); i++) {
          VectorValues vectors = leaf.getVectorValues("vector");
          float[] last = new float[vectors.dimension()];
          int lastDoc = -1;
          for (int doc = vectors.nextDoc(); doc != NO_MORE_DOCS; doc = vectors.nextDoc()) {
            float value = vectors.vectorValue();
            if (VectorUtil.squareDistance(last, value) < 1e-4) {
              throw new IllegalStateException("Same vector for docs " + lastDoc " and " + doc);
            }
            System.arraycopy(value, 0, last, 0, last.length);
            lastDoc = doc;
          }
        }
      }
    }
  }

}
