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
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.search.CountingFacetsCollector;
import org.apache.lucene.facet.search.FacetArrays;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.search.StandardFacetsCollector;
import org.apache.lucene.facet.search.aggregator.Aggregator;
import org.apache.lucene.facet.search.params.CountFacetRequest;
import org.apache.lucene.facet.search.params.FacetRequest;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.search.CachingCollector;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.grouping.BlockGroupingCollector;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.search.grouping.term.TermAllGroupsCollector;
import org.apache.lucene.search.grouping.term.TermFirstPassGroupingCollector;
import org.apache.lucene.search.grouping.term.TermSecondPassGroupingCollector;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.highlight.TokenSources;
import org.apache.lucene.search.vectorhighlight.FieldQuery;
import org.apache.lucene.util.BytesRef;

final class SearchTask extends Task {
  private final String category;
  private final Query q;
  private final Sort s;
  private final Filter f;
  private final String group;
  private final int topN;
  private final boolean singlePassGroup;
  private final boolean doCountGroups;
  private final boolean doHilite;
  private final boolean doDateFacets;
  private final boolean doAllFacets;
  private final boolean doStoredLoads;

  private TopDocs hits;
  private TopGroups<?> groupsResultBlock;
  private TopGroups<BytesRef> groupsResultTerms;
  private FieldQuery fieldQuery;
  private Highlighter highlighter;
  private List<FacetResult> facets;
  private double hiliteMsec;
  private double getFacetResultsMsec;

  public SearchTask(String category, Query q, Sort s, String group, Filter f, int topN,
                    boolean doHilite, boolean doDateFacets, boolean doAllFacets, boolean doStoredLoads) {
    this.category = category;
    this.q = q;
    this.s = s;
    this.f = f;
    this.doDateFacets = doDateFacets;
    this.doAllFacets = doAllFacets;
    if (group != null && group.startsWith("groupblock")) {
      this.group = "groupblock";
      this.singlePassGroup = group.equals("groupblock1pass");
      doCountGroups = true;
    } else {
      this.group = group;
      this.singlePassGroup = false;
      doCountGroups = false;
    }
    this.topN = topN;
    this.doHilite = doHilite;
    this.doStoredLoads = doStoredLoads;
  }

  @Override
  public Task clone() {
    Query q2 = q.clone();
    if (q2 == null) {
      throw new RuntimeException("q=" + q + " failed to clone");
    }
    if (singlePassGroup) {
      return new SearchTask(category, q2, s, "groupblock1pass", f, topN, doHilite, doDateFacets, doAllFacets, doStoredLoads);
    } else {
      return new SearchTask(category, q2, s, group, f, topN, doHilite, doDateFacets, doAllFacets, doStoredLoads);
    }
  }

  @Override
  public String getCategory() {
    return category;
  }

  @Override
  public void go(IndexState state) throws IOException {
    //System.out.println("go group=" + this.group + " single=" + singlePassGroup + " xxx=" + xxx + " this=" + this);
    final IndexSearcher searcher = state.mgr.acquire();

    //System.out.println("GO query=" + q);

    try {
      if (doHilite) {
        if (state.fastHighlighter != null) {
          fieldQuery = state.fastHighlighter.getFieldQuery(q, searcher.getIndexReader());
        } else if (state.useHighlighter) {
          highlighter = new Highlighter(new SimpleHTMLFormatter(), new QueryScorer(q));
        } else {
          // no setup for postingshighlighter
        }
      }

      if (group != null) {
        if (singlePassGroup) {
          final BlockGroupingCollector c = new BlockGroupingCollector(Sort.RELEVANCE, 10, true, state.groupEndFilter);
          searcher.search(q, c);
          groupsResultBlock = c.getTopGroups(null, 0, 0, 10, true);

          if (doHilite) {
            hilite(groupsResultBlock, state, searcher);
          }

        } else {
          //System.out.println("GB: " + group);
          final TermFirstPassGroupingCollector c1 = new TermFirstPassGroupingCollector(group, Sort.RELEVANCE, 10);
          final CachingCollector cCache = CachingCollector.create(c1, true, 32.0);

          final Collector c;
          final TermAllGroupsCollector allGroupsCollector;
          // Turn off AllGroupsCollector for now -- it's very slow:
          if (false && doCountGroups) {
            allGroupsCollector = new TermAllGroupsCollector(group);
            c = MultiCollector.wrap(allGroupsCollector, cCache);
          } else {
            allGroupsCollector = null;
            c = cCache;
          }
          
          searcher.search(q, c);

          final Collection<SearchGroup<BytesRef>> topGroups = c1.getTopGroups(0, true);
          if (topGroups != null) {
            final TermSecondPassGroupingCollector c2 = new TermSecondPassGroupingCollector(group, topGroups, Sort.RELEVANCE, null, 10, true, true, true);
            if (cCache.isCached()) {
              cCache.replay(c2);
            } else {
              searcher.search(q, c2);
            }
            groupsResultTerms = c2.getTopGroups(0);
            if (allGroupsCollector != null) {
              groupsResultTerms = new TopGroups<BytesRef>(groupsResultTerms,
                                                          allGroupsCollector.getGroupCount());
            }
            if (doHilite) {
              hilite(groupsResultTerms, state, searcher);
            }
          }
        }
      } else if ((doDateFacets || doAllFacets) && state.taxoReader != null) {
        // TODO: support sort, filter too!!
        /*
        FacetSearchParams fsp = new FacetSearchParams(state.iParams);
        fsp.setClCache(state.clCache);
        */
        /*
        FacetSearchParams fsp = new FacetSearchParams();
        if (false && state.fastHighlighter != null) {
          fsp.addFacetRequest(new CountFacetRequest(new CategoryPath("Date"), 10) {
              @Override
              public Aggregator createAggregator(boolean useComplements,
                                                 FacetArrays arrays, IndexReader reader,
                                                 TaxonomyReader taxonomy) {
                //System.out.println("NO PARENTS AGG");
                try {
                  return new NoParentsCountingAggregator(taxonomy, arrays.getIntArray());
                } catch (IOException ioe) {
                  throw new RuntimeException(ioe);
                }
              }
            });
        } else {
          fsp.addFacetRequest(new CountFacetRequest(new CategoryPath("Date"), 10));
        }
        Collector facetsCollector;
        if (state.fastHighlighter == null) {
          facetsCollector = new FacetsCollector(fsp, searcher.getIndexReader(), state.taxoReader);
        } else {
          facetsCollector = new DocValuesFacetsCollector(fsp, state.taxoReader);
        }
        */

        List<FacetRequest> facetRequests = new ArrayList<FacetRequest>();
        facetRequests.add(new CountFacetRequest(new CategoryPath("Date"), 10));
        // TODO: parse these fields names from the task instead...
        if (doAllFacets) {
          facetRequests.add(new CountFacetRequest(new CategoryPath("categories"), 10));
          facetRequests.add(new CountFacetRequest(new CategoryPath("username"), 10));
          facetRequests.add(new CountFacetRequest(new CategoryPath("characterCount"), 10));
          facetRequests.add(new CountFacetRequest(new CategoryPath("imageCount"), 10));
          facetRequests.add(new CountFacetRequest(new CategoryPath("sectionCount"), 10));
          facetRequests.add(new CountFacetRequest(new CategoryPath("subSectionCount"), 10));
          facetRequests.add(new CountFacetRequest(new CategoryPath("subSubSectionCount"), 10));
          facetRequests.add(new CountFacetRequest(new CategoryPath("refCount"), 10));
        }

        FacetSearchParams fsp = new FacetSearchParams(facetRequests, state.iParams);
        FacetsCollector facetsCollector;
        if (true || state.fastHighlighter != null) {
          //facetsCollector = new DecoderCountingFacetsCollector(fsp, state.taxoReader);
          //facetsCollector = new PostCollectionCountingFacetsCollector(fsp, state.taxoReader);
          facetsCollector = new CountingFacetsCollector(fsp, state.taxoReader);
        } else {
          //facetsCollector = new CountingFacetsCollector(fsp, state.taxoReader);
          facetsCollector = new StandardFacetsCollector(fsp, searcher.getIndexReader(), state.taxoReader);
        }
        // TODO: determine in order by the query...?
        TopScoreDocCollector hitsCollector = TopScoreDocCollector.create(10, false);
        searcher.search(q, MultiCollector.wrap(hitsCollector, facetsCollector));
        hits = hitsCollector.topDocs();
        long t0 = System.nanoTime();
        /*
        if (state.fastHighlighter != null) {
          facets = ((DecoderCountingFacetsCollector) facetsCollector).getFacetResults();
        } else {
          facets = ((FacetsCollector) facetsCollector).getFacetResults();
        }
        */
        facets = facetsCollector.getFacetResults();
        getFacetResultsMsec = (System.nanoTime() - t0)/1000000.0;
      } else if (s == null && f == null) {
        hits = searcher.search(q, topN);
        if (doHilite) {
          hilite(hits, state, searcher, q);
        }
      } else if (s == null && f != null) {
        hits = searcher.search(q, f, topN);
        if (doHilite) {
          hilite(hits, state, searcher, q);
        }
      } else {
        hits = searcher.search(q, f, topN, s);
        if (doHilite) {
          hilite(hits, state, searcher, q);
        }
        /*
          final boolean fillFields = true;
          final boolean fieldSortDoTrackScores = true;
          final boolean fieldSortDoMaxScore = true;
          final TopFieldCollector c = TopFieldCollector.create(s, topN,
          fillFields,
          fieldSortDoTrackScores,
          fieldSortDoMaxScore,
          false);
          searcher.search(q, c);
          hits = c.topDocs();
        */
      }
      if (hits != null) {
        totalHitCount = hits.totalHits;

        if (doStoredLoads) {
          for (int i = 0; i < hits.scoreDocs.length; i++) {
            ScoreDoc scoreDoc = hits.scoreDocs[i];
            StoredDocument doc = searcher.doc(scoreDoc.doc);
            for (StorableField field : doc) {
              field.stringValue();
            }
          }
        }

      } else if (groupsResultBlock != null) {
        totalHitCount = groupsResultBlock.totalHitCount;
      }
    } catch (Throwable t) {
      System.out.println("EXC: " + q);
      throw new RuntimeException(t);
      //System.out.println("TE: " + TermsEnum.getStats());
    } finally {
      state.mgr.release(searcher);
      fieldQuery = null;
      highlighter = null;
    }
  }

  private void hilite(TopGroups<?> groups, IndexState indexState, IndexSearcher searcher) throws IOException {
    for(GroupDocs<?> group : groups.groups) {
      for(ScoreDoc sd : group.scoreDocs) {
        hilite(sd.doc, indexState, searcher);
      }
    }
  }

  private void hilite(TopDocs hits, IndexState indexState, IndexSearcher searcher, Query query) throws IOException {
    long t0 = System.nanoTime();
    if (indexState.fastHighlighter != null || indexState.useHighlighter) {
      for(ScoreDoc sd : hits.scoreDocs) {
        hilite(sd.doc, indexState, searcher);
      }
      //System.out.println("  q=" + query + ": hilite time: " + ((t1-t0)/1000000.0));
    } else {
      // TODO: why is this one finding 2 frags when the others find 1?
      String[] frags = indexState.postingsHighlighter.highlight(indexState.textFieldName, query, searcher, hits, 2);
      //System.out.println("  q=" + query + ": hilite time: " + ((t1-t0)/1000000.0));
      for(int hit=0;hit<frags.length;hit++) {
        String frag = frags[hit];
        //System.out.println("\nhilite title=" + searcher.doc(hits.scoreDocs[hit].doc).get("titleTokenized") + " query=" + q);
        //System.out.println("  frag: " + frag);
        if (frag != null) {
          // It's fine for frag to be null: it's a
          // placeholder, meaning this hit had no hilite
          totHiliteHash += frag.hashCode();
        }
      }
    }
    long t1 = System.nanoTime();
    hiliteMsec = (t1-t0)/1000000.0;
  }

  public int totHiliteHash;

  private void hilite(int docID, IndexState indexState, IndexSearcher searcher) throws IOException {
    //System.out.println("\nhilite title=" + searcher.doc(docID).get("titleTokenized") + " query=" + q);
    if (indexState.fastHighlighter != null) {
      for(String h : indexState.fastHighlighter.getBestFragments(fieldQuery,
                                                                 searcher.getIndexReader(), docID,
                                                                 indexState.textFieldName,
                                                                 100, 2)) {
        totHiliteHash += h.hashCode();
        //System.out.println("  frag: " + h);
      }
    } else {
      StoredDocument doc = searcher.doc(docID);
      String text = doc.get(indexState.textFieldName);
      // NOTE: passing null for analyzer: TermVectors must
      // be indexed!
      TokenStream tokenStream = TokenSources.getAnyTokenStream(searcher.getIndexReader(), docID, indexState.textFieldName, null);
      TextFragment[] frags;
      try {
        frags = highlighter.getBestTextFragments(tokenStream, text, false, 2);
      } catch (InvalidTokenOffsetsException ioe) {
        throw new RuntimeException(ioe);
      }

      //int fragCount = 0;
      for (int j = 0; j < frags.length; j++) {
        if (frags[j] != null && frags[j].getScore() > 0) {
          //System.out.println("  frag " + j + ": " + frags[j].toString());
          totHiliteHash += frags[j].toString().hashCode();
          //fragCount++;
        }
      }
      //System.out.println("  " + docID + ": " + fragCount + " frags");
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof SearchTask) {
      final SearchTask otherSearchTask = (SearchTask) other;
      if (!q.equals(otherSearchTask.q)) {
        return false;
      }
      if (s != null) {
        if (otherSearchTask.s != null) {
          if (!s.equals(otherSearchTask.s)) {
            return false;
          }
        } else {
          if (otherSearchTask.s != null) {
            return false;
          }
        }
      }
      if (topN != otherSearchTask.topN) {
        return false;
      }

      if (group != null && !group.equals(otherSearchTask.group)) {
        return false;
      } else if (otherSearchTask.group != null) {
        return false;
      }

      if (f != null) {
        if (otherSearchTask.f == null) {
          return false;
        } else if (!f.equals(otherSearchTask.f)) {
          return false;
        }
      } else if (otherSearchTask.f != null) {
        return false;
      }

      return true;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    int hashCode = q.hashCode();
    if (s != null) {
      hashCode ^= s.hashCode();
    }
    if (group != null) {
      hashCode ^= group.hashCode();
    }
    if (f != null) {
      hashCode ^= f.hashCode();
    }
    hashCode *= topN;
    return hashCode;
  }

  @Override
  public long checksum() {
    final long PRIME = 641;
    long sum = 0;
    //System.out.println("checksum q=" + q + " f=" + f);
    if (group != null) {
      if (singlePassGroup) {
        for(GroupDocs<?> groupDocs : groupsResultBlock.groups) {
          sum += groupDocs.totalHits;
          for(ScoreDoc hit : groupDocs.scoreDocs) {
            sum = sum * PRIME + hit.doc;
          }
        }
      } else {
        for(GroupDocs<BytesRef> groupDocs : groupsResultTerms.groups) {
          sum += groupDocs.totalHits;
          for(ScoreDoc hit : groupDocs.scoreDocs) {
            sum = sum * PRIME + hit.doc;
            if (hit instanceof FieldDoc) {
              final FieldDoc fd = (FieldDoc) hit;
              if (fd.fields != null) {
                for(Object o : fd.fields) {
                  sum = sum * PRIME + o.hashCode();
                }
              }
            }
          }
        }
      }
    } else {
      sum = hits.totalHits;
      for(ScoreDoc hit : hits.scoreDocs) {
        //System.out.println("  " + hit.doc);
        sum = sum * PRIME + hit.doc;
        if (hit instanceof FieldDoc) {
          final FieldDoc fd = (FieldDoc) hit;
          if (fd.fields != null) {
            for(Object o : fd.fields) {
              sum = sum * PRIME + o.hashCode();
            }
          }
        }
      }
      //System.out.println("  final=" + sum);
    }

    return sum;
  }

  @Override
  public String toString() {
    return "cat=" + category + " q=" + q + " s=" + s + " f=" + f + " group=" + (group == null ?  null : group.replace("\n", "\\n")) +
      (group == null ? " hits=" + hits.totalHits :
       " groups=" + (singlePassGroup ?
                     (groupsResultBlock.groups.length + " hits=" + groupsResultBlock.totalHitCount + " groupTotHits=" + groupsResultBlock.totalGroupedHitCount + " totGroupCount=" + groupsResultBlock.totalGroupCount) :
                     (groupsResultTerms.groups.length + " hits=" + groupsResultTerms.totalHitCount + " groupTotHits=" + groupsResultTerms.totalGroupedHitCount + " totGroupCount=" + groupsResultTerms.totalGroupCount)));
  }

  @Override
  public void printResults(PrintStream out, IndexState state) throws IOException {
    if (group != null) {
      if (singlePassGroup) {
        for(GroupDocs<?> groupDocs : groupsResultBlock.groups) {
          out.println("  group=null" + " totalHits=" + groupDocs.totalHits + " groupRelevance=" + groupDocs.groupSortValues[0]);
          for(ScoreDoc hit : groupDocs.scoreDocs) {
            out.println("    doc=" + hit.doc + " score=" + hit.score);
          }
        }
      } else {
        for(GroupDocs<BytesRef> groupDocs : groupsResultTerms.groups) {
          out.println("  group=" + (groupDocs.groupValue == null ? "null" : groupDocs.groupValue.utf8ToString().replace("\n", "\\n")) + " totalHits=" + groupDocs.totalHits + " groupRelevance=" + groupDocs.groupSortValues[0]);
          for(ScoreDoc hit : groupDocs.scoreDocs) {
            out.println("    doc=" + hit.doc + " score=" + hit.score);
          }
        }
      }
    } else if (hits instanceof TopFieldDocs) {
      final TopFieldDocs fieldHits = (TopFieldDocs) hits;
      for(int idx=0;idx<hits.scoreDocs.length;idx++) {
        FieldDoc hit = (FieldDoc) hits.scoreDocs[idx];
        final Object v = hit.fields[0];
        final String vs;
        if (v instanceof Long) {
          vs = v.toString();
        } else {
          vs = ((BytesRef) v).utf8ToString();
        }
        out.println("  doc=" + state.docIDToID[hit.doc] + " field=" + vs);
      }
    } else {
      for(ScoreDoc hit : hits.scoreDocs) {
        out.println("  doc=" + state.docIDToID[hit.doc] + " score=" + hit.score);
      }
    }

    if (hiliteMsec > 0) {
      out.println(String.format("  hilite time %.4f msec", hiliteMsec));
    }
    if (getFacetResultsMsec > 0) {
      out.println(String.format("  getFacetResults time %.4f msec", getFacetResultsMsec));
    }

    if (facets != null) {
      out.println("  facets:");
      for(FacetResult fr : facets) {
        printFacets(0, out, fr.getFacetResultNode(), "    ");
      }
    }
  }

  private void printFacets(int depth, PrintStream out, FacetResultNode node, String indent) {
    //out.println(indent + " " + node.getLabel().getComponent(depth) + " (" + (int) node.getValue() + ")");
    out.println(indent + " " + node.getLabel() + " (" + (int) node.getValue() + ")");
    for(FacetResultNode childNode : node.getSubResults()) {
      printFacets(depth+1, out, childNode, indent + "  ");
    }
  }
}

