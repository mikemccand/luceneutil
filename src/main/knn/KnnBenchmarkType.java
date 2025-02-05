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

public enum KnnBenchmarkType {

  /** Default single valued knn vector search benchmark */
  DEFAULT(""),

  /** MultiVector Benchmark: Uses parent-child block join index and query for multivalued vectors */
  PARENT_JOIN("parentJoin"),

  /** MultiVector Benchmark: Uses multiVector query for multivalued vectors */
  MULTI_VECTOR("multiVector");

  public final String indexTag;

  KnnBenchmarkType(String tag) {
    indexTag = tag;
  }
}
