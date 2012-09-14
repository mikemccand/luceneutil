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
import java.util.List;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
//import org.apache.lucene.sandbox.postingshighlight.PostingsHighlighter;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.vectorhighlight.FastVectorHighlighter;
import org.apache.lucene.util.BytesRef;

class IndexState {
  public final ReferenceManager<IndexSearcher> mgr;
  public final DirectSpellChecker spellChecker;
  public final Filter groupEndFilter;
  public final FastVectorHighlighter highlighter;
  //public final PostingsHighlighter postingsHighlighter;
  public final String textFieldName;
  public int[] docIDToID;

  public IndexState(ReferenceManager<IndexSearcher> mgr, String textFieldName, DirectSpellChecker spellChecker, String hiliteImpl) throws IOException {
    this.mgr = mgr;
    this.spellChecker = spellChecker;
    this.textFieldName = textFieldName;
    groupEndFilter = new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("groupend", "x"))));
    if (hiliteImpl.equals("FastVectorHighlighter")) {
      highlighter = new FastVectorHighlighter(true, true);
      //postingsHighlighter = null;
    } else if (hiliteImpl.equals("PostingsHighlighter")) {
      highlighter = null;
      //postingsHighlighter = new PostingsHighlighter(textFieldName);
    } else {
      throw new IllegalArgumentException("unrecognized -hiliteImpl \"" + hiliteImpl + "\"");
    }
  }

  public void setDocIDToID() throws IOException {
    IndexSearcher searcher = mgr.acquire();
    try {
      docIDToID = new int[searcher.getIndexReader().maxDoc()];
      int base = 0;
      for(AtomicReaderContext sub : searcher.getIndexReader().leaves()) {
        final int[] ids = FieldCache.DEFAULT.getInts(sub.reader(), "id", new FieldCache.IntParser() {
            @Override
            public int parseInt(BytesRef term) {
              return LineFileDocs.idToInt(term);
            }
          }, false);
        System.arraycopy(ids, 0, docIDToID, base, ids.length);
        base += ids.length;
      }
    } finally {
      mgr.release(searcher);
    }
  }
}
