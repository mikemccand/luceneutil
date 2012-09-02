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

import java.io.File;

import org.apache.lucene.store.*;
import org.apache.lucene.search.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;

// javac -Xlint:deprecation -cp build/core/classes/java perf/RunFuzzyQuery.java

// java -cp .:build/core/classes/java perf.RunFuzzyQuery /q/lucene/indices/wikimedium.3x.nightly.nd27.625M/index unit 1

public final class RunFuzzyQuery {
  public static void main(String[] args) throws Exception {
    final String dirPath = args[0];
    final String text = args[1];
    final int ed = Integer.parseInt(args[2]);

    final Directory dir = new MMapDirectory(new File(dirPath));
    final DirectoryReader r = DirectoryReader.open(dir);
    final IndexSearcher s = new IndexSearcher(r);

    // nocommit
    //final float minSim = ((float) ed) / text.length();
    final float minSim = (float) ed;
    //final float minSim = 1 - ((float)ed+1) / text.length();
    System.out.println("minSim=" + minSim);

    for(int iter=0;iter<10;iter++) {
      final Query q = new FuzzyQuery(new Term("body", text), ed, 0, 50, true);
      final long t0 = System.currentTimeMillis();
      final TopDocs hits = s.search(q, 10);
      final long t1 = System.currentTimeMillis();
      System.out.println("iter " + iter + ": " + (t1-t0) + " msec");
      if (iter == 0) {
        System.out.println("  " + hits.totalHits + " total hits");
        for(ScoreDoc hit : hits.scoreDocs) {
          final StoredDocument doc = s.doc(hit.doc);
          System.out.println("    doc=" + doc.get("titleTokenized") + " score=" + hit.score);
        }
      }
    }

    r.close();
    dir.close();
  }
}
