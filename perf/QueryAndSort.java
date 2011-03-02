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
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.BytesRef;

// TODO: make this more abstract "task" like class
public class QueryAndSort {
  final Query q;
  final Sort s;
  final Filter f;
  final BytesRef[] pkIDs;
  final String respell;

  public QueryAndSort(Query q, Sort s, Filter f) {
    this.q = q;
    this.s = s;
    this.f = f;
    pkIDs = null;
    respell = null;
  }

  public QueryAndSort(BytesRef[] pkIDs) {
    this.pkIDs = pkIDs;
    respell = null;
    q = null;
    s = null;
    f = null;
  }

  public QueryAndSort(String respell) {
    this.respell = respell;
    pkIDs = null;
    q = null;
    s = null;
    f = null;
  }
}
