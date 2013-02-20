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
import java.util.Map;

import org.apache.lucene.facet.params.CategoryListParams.OrdinalPolicy;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.postingshighlight.PostingsHighlighter;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.vectorhighlight.FastVectorHighlighter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

class IndexState {
  public final ReferenceManager<IndexSearcher> mgr;
  public final DirectSpellChecker spellChecker;
  public final Filter groupEndFilter;
  public final FastVectorHighlighter fastHighlighter;
  public final boolean useHighlighter;
  public final PostingsHighlighter postingsHighlighter;
  public final String textFieldName;
  public int[] docIDToID;
  public final boolean hasDeletions;
  public final Map<String,TaxonomyReader> taxoReaders;
  public final List<FacetGroup> facetGroups;

  public IndexState(ReferenceManager<IndexSearcher> mgr, Map<String,TaxonomyReader> taxoReaders, String textFieldName, DirectSpellChecker spellChecker,
                    String hiliteImpl, List<FacetGroup> facetGroups) throws IOException {
    this.mgr = mgr;
    this.spellChecker = spellChecker;
    this.textFieldName = textFieldName;
    this.taxoReaders = taxoReaders;
    this.facetGroups = facetGroups;
    
    groupEndFilter = new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("groupend", "x"))));
    if (hiliteImpl.equals("FastVectorHighlighter")) {
      fastHighlighter = new FastVectorHighlighter(true, true);
      useHighlighter = false;
      postingsHighlighter = null;
    } else if (hiliteImpl.equals("PostingsHighlighter")) {
      fastHighlighter = null;
      useHighlighter = false;
      postingsHighlighter = new PostingsHighlighter();
    } else if (hiliteImpl.equals("Highlighter")) {
      fastHighlighter = null;
      useHighlighter = true;
      postingsHighlighter = null;
    } else {
      throw new IllegalArgumentException("unrecognized -hiliteImpl \"" + hiliteImpl + "\"");
    }
    IndexSearcher searcher = mgr.acquire();
    try {
      hasDeletions = searcher.getIndexReader().hasDeletions();
    } finally {
      mgr.release(searcher);
    }
  }

  public void setDocIDToID() throws IOException {
    IndexSearcher searcher = mgr.acquire();
    try {
      docIDToID = new int[searcher.getIndexReader().maxDoc()];
      int base = 0;
      for(AtomicReaderContext sub : searcher.getIndexReader().leaves()) {
        final FieldCache.Ints ids = FieldCache.DEFAULT.getInts(sub.reader(), "id", new FieldCache.IntParser() {
            @Override
            public int parseInt(BytesRef term) {
              return LineFileDocs.idToInt(term);
            }

            @Override
            public TermsEnum termsEnum(Terms terms) throws IOException {
              return terms.iterator(null);
            }

          }, false);
        int maxDoc = sub.reader().maxDoc();
        for(int i=0;i<maxDoc;i++) {
          docIDToID[base+i] = ids.get(i);
        }
        base += maxDoc;
      }
    } finally {
      mgr.release(searcher);
    }
  }
}
