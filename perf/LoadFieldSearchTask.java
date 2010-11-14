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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.SetBasedFieldSelector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

public class LoadFieldSearchTask extends SearchTask {

  private FieldSelector fieldSelector;

  public LoadFieldSearchTask(Random r, IndexSearcher s,
      List<QueryAndSort> queriesList, int numIter, boolean shuffle) throws IOException {
    super(r, s, queriesList, numIter, shuffle);
    Set<String> loadfields = new HashSet<String>();
    loadfields.add("docdate");
    this.fieldSelector = new SetBasedFieldSelector(loadfields,
        new HashSet<String>());
  }

  @Override
  protected void processHits(TopDocs hits) throws IOException {
    ScoreDoc[] scoreDocs = hits.scoreDocs;
    for (int i = 0; i < scoreDocs.length; i++) {
      this.s.doc(i, fieldSelector); // load all fields
    }
  }
}
