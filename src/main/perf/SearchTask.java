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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.range.LongRangeFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.sandbox.facet.utils.FacetBuilder;
import org.apache.lucene.sandbox.facet.utils.FacetOrchestrator;
import org.apache.lucene.sandbox.facet.utils.TaxonomyFacetBuilder;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.AllGroupsCollector;
import org.apache.lucene.search.grouping.BlockGroupingCollector;
import org.apache.lucene.search.grouping.FirstPassGroupingCollector;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TermGroupSelector;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.search.grouping.TopGroupsCollector;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.highlight.TokenSources;
import org.apache.lucene.search.vectorhighlight.FieldQuery;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

final class SearchTask extends Task {
  private final String category;
  private final Query q;
  private final Sort s;
  private final String group;
  private final int topN;
  private final boolean singlePassGroup;
  private final boolean doCountGroups;
  private final boolean doHilite;
  private final boolean doStoredLoads;
  private final boolean doDrillSideways;
  private final boolean isCountOnly;

  private TopDocs hits;
  private int count = -1;
  private TopGroups<?> groupsResultBlock;
  private TopGroups<BytesRef> groupsResultTerms;
  private FieldQuery fieldQuery;
  private Highlighter highlighter;
  private List<FacetResult> facetResults;
  private double hiliteMsec;
  private double getFacetResultsMsec;
  private List<TaskParser.TaskBuilder.FacetTask> facetRequests;
  private String vectorField;

  public SearchTask(String category, boolean isCountOnly, Query q, Sort s, String group, int topN,
                    boolean doHilite, boolean doStoredLoads, List<TaskParser.TaskBuilder.FacetTask> facetRequests,
                    String vectorField, boolean doDrillSideways) {
    this.category = category;
    this.isCountOnly = isCountOnly;
    if (isCountOnly) {
      // some attempt at catching mistakes!
      if (s != null) {
        throw new IllegalArgumentException("sorting with count-only queries makes no sense?");
      }
      if (facetRequests != null && facetRequests.isEmpty() == false) {
        throw new IllegalArgumentException("cannot do faceting and count-only at once");
      }
      if (doHilite) {
        throw new IllegalArgumentException("cannot do hiliting and count-only at once");
      }
      if (doStoredLoads) {
        throw new IllegalArgumentException("cannot load stored fields and do count-only at once");
      }
      if (group != null) {
        throw new IllegalArgumentException("cannot do grouping and count-only at once");
      }
    }
    this.q = q;
    this.s = s;
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
    this.facetRequests = facetRequests;
    this.vectorField = vectorField;
    this.doDrillSideways = doDrillSideways;
  }

  @Override
  public Task clone() {
    if (singlePassGroup) {
      return new SearchTask(category, isCountOnly, q, s, "groupblock1pass", topN, doHilite, doStoredLoads, facetRequests, vectorField, doDrillSideways);
    } else {
      return new SearchTask(category, isCountOnly, q, s, group, topN, doHilite, doStoredLoads, facetRequests, vectorField, doDrillSideways);
    }
  }

  @Override
  public String getCategory() {
    return category;
  }

  @Override
  public void go(IndexState state, TaskParser taskParser) throws IOException {
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
          final BlockGroupingCollector c = new BlockGroupingCollector(Sort.RELEVANCE, 10, true, searcher.createWeight(searcher.rewrite(state.groupEndQuery), ScoreMode.COMPLETE_NO_SCORES, 1));
          searcher.search(q, c);
          groupsResultBlock = c.getTopGroups(Sort.RELEVANCE, 0, 0, 10);

          if (doHilite) {
            hilite(groupsResultBlock, state, searcher);
          }

        } else {
          //System.out.println("GB: " + group);
          final FirstPassGroupingCollector<BytesRef> c1 = new FirstPassGroupingCollector(new TermGroupSelector(group), Sort.RELEVANCE, 10);

          final Collector c;
          final AllGroupsCollector<BytesRef> allGroupsCollector;
          // Turn off AllGroupsCollector for now -- it's very slow:
          if (false && doCountGroups) {
            allGroupsCollector = new AllGroupsCollector(new TermGroupSelector(group));
            //c = MultiCollector.wrap(allGroupsCollector, c1);
            c = c1;
          } else {
            allGroupsCollector = null;
            c = c1;
          }
          
          searcher.search(q, c);

          final Collection<SearchGroup<BytesRef>> topGroups = c1.getTopGroups(0);
          if (topGroups != null) {
            final TopGroupsCollector<BytesRef> c2 = new TopGroupsCollector<>(new TermGroupSelector(group), topGroups, Sort.RELEVANCE, Sort.RELEVANCE, 10, true);
            searcher.search(q, c2);
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
      } else if (facetRequests.isEmpty() == false) {
        if (state.facetsConfig == null) {
          throw new IllegalStateException("Missing facet config, cannot run facet requests");
        }
        if (doDrillSideways) {
          // nocommit todo
          hits = null;
          facetResults = null;
        } else {
          facetResults = new ArrayList<FacetResult>();
          // TODO: support sort, filter too!!
          // TODO: support other facet methods
          List<TaskParser.TaskBuilder.FacetTask> postCollectionFacetTasks = new ArrayList<>();
          List<TaskParser.TaskBuilder.FacetTask> duringCollectionFacetTasks = new ArrayList<>();
          for (TaskParser.TaskBuilder.FacetTask request : facetRequests) {
            switch (request.facetMode()) {
              case UNDEFINED:
              case POST_COLLECTION:
                postCollectionFacetTasks.add(request);
                break;
              case DURING_COLLECTION:
                duringCollectionFacetTasks.add(request);
            }
          }
          long t0 = System.nanoTime();
          if (duringCollectionFacetTasks.isEmpty() == false) {
            // TODO: sandbox facet module doesn't currently have methods to aggregate for all docs in the index.
            //       if we see regression because of that, there might be something we can optimize in searcher/scorer
            //       for MatchAllDocsQuery to make collection for all docs in the index faster?
            FacetOrchestrator facetOrchestrator = new FacetOrchestrator();
            List<FacetBuilder> facetBuilders = new ArrayList<>();
            for (TaskParser.TaskBuilder.FacetTask request : duringCollectionFacetTasks) {
              // TODO: handle other types, not just taxonomy
              FacetBuilder facetBuilder;
              if (request.dimension().endsWith(".taxonomy")) {
                facetBuilder = new TaxonomyFacetBuilder(state.facetsConfig, state.taxoReader, request.dimension()).withSortByCount().withTopN(10);
              } else {
                // should have been prevented higher up:
                throw new AssertionError("unknown facet method \"" + state.facetFields.get(request.dimension()) + "\"");
              }
              facetBuilders.add(facetBuilder);
              facetOrchestrator.addBuilder(facetBuilder);
            }
            // The first collector manager in the list is responsible for collecting hits,
            // except when handling MatchAllDocsQuery, where hits are not collected due to historical reasons.
            TopScoreDocCollectorManager mainCollector = null;
            if (q instanceof MatchAllDocsQuery == false) {
              mainCollector = new TopScoreDocCollectorManager(10, null, Integer.MAX_VALUE);
            }
            hits = facetOrchestrator.collect(q, searcher, mainCollector);
            for (FacetBuilder facetBuilder : facetBuilders) {
              FacetResult result = facetBuilder.getResult();
              //  To replicate current TaxonomyFacets behaviour
              if (result.childCount == 0) {
                result = null;
              }
              facetResults.add(result);
            }
          }
          if (postCollectionFacetTasks.isEmpty() == false) {
            if (q instanceof MatchAllDocsQuery) {
              for (TaskParser.TaskBuilder.FacetTask request : postCollectionFacetTasks) {
                if (request.dimension().startsWith("range:")) {
                  throw new AssertionError("fix me!");
                } else if (request.dimension().endsWith(".taxonomy")) {
                  // TODO: fixme to handle N facets in one indexed field!  Need to make the facet counts once per indexed field...
                  Facets facets = new FastTaxonomyFacetCounts(state.facetsConfig.getDimConfig(request.dimension()).indexFieldName, searcher.getIndexReader(), state.taxoReader, state.facetsConfig);
                  FacetResult res = facets.getTopChildren(10, request.dimension());
                  facetResults.add(res);
                } else if (request.dimension().endsWith(".sortedset")) {
                  // TODO: fixme to handle N facets in one SSDV field!  Need to make the facet counts once per indexed field...
                  SortedSetDocValuesReaderState ssdvFacetsState = state.getSortedSetReaderState(state.facetsConfig.getDimConfig(request.dimension()).indexFieldName);
                  SortedSetDocValuesFacetCounts facets = new SortedSetDocValuesFacetCounts(ssdvFacetsState);
                  facetResults.add(facets.getTopChildren(10, request.dimension()));
                } else {
                  // should have been prevented higher up:
                  throw new AssertionError("unknown facet method \"" + state.facetFields.get(request.dimension()) + "\"");
                }
              }
              getFacetResultsMsec = (System.nanoTime() - t0) / 1000000.0;
            } else {
              FacetsCollectorManager.FacetsResult fr = FacetsCollectorManager.search(searcher, q, 10, new FacetsCollectorManager());
              hits = fr.topDocs();
              FacetsCollector fc = fr.facetsCollector();
              for (TaskParser.TaskBuilder.FacetTask facetRequest : postCollectionFacetTasks) {
                String request = facetRequest.dimension();
                if (request.startsWith("range:")) {
                  int i = request.indexOf(':', 6);
                  if (i == -1) {
                    throw new IllegalArgumentException("range facets request \"" + request + "\" is missing field; should be range:field:0-10,10-20");
                  }
                  String field = request.substring(6, i);
                  String[] rangeStrings = request.substring(i + 1, request.length()).split(",");
                  LongRange[] ranges = new LongRange[rangeStrings.length];
                  for (int rangeIDX = 0; rangeIDX < ranges.length; rangeIDX++) {
                    String rangeString = rangeStrings[rangeIDX];
                    int j = rangeString.indexOf('-');
                    if (j == -1) {
                      throw new IllegalArgumentException("range facets request should be X-Y; got: " + rangeString);
                    }
                    long start = Long.parseLong(rangeString.substring(0, j));
                    long end = Long.parseLong(rangeString.substring(j + 1));
                    ranges[rangeIDX] = new LongRange(rangeString, start, true, end, true);
                  }
                  LongRangeFacetCounts facets = new LongRangeFacetCounts(field, fc, ranges);
                  facetResults.add(facets.getTopChildren(ranges.length, field));
                } else if (request.endsWith(".taxonomy")) {
                  // TODO: fixme to handle N facets in one indexed field!  Need to make the facet counts once per indexed field...
                  Facets facets = new FastTaxonomyFacetCounts(state.facetsConfig.getDimConfig(request).indexFieldName, state.taxoReader, state.facetsConfig, fc);
                  facetResults.add(facets.getTopChildren(10, request));
                } else if (request.endsWith(".sortedset")) {
                  // TODO: fixme to handle N facets in one SSDV field!  Need to make the facet counts once per indexed field...
                  SortedSetDocValuesReaderState ssdvFacetsState = state.getSortedSetReaderState(state.facetsConfig.getDimConfig(request).indexFieldName);
                  SortedSetDocValuesFacetCounts facets = new SortedSetDocValuesFacetCounts(ssdvFacetsState, fc);
                  facetResults.add(facets.getTopChildren(10, request));
                } else {
                  // should have been prevented higher up:
                  throw new AssertionError("unknown facet method \"" + state.facetFields.get(request) + "\"");
                }
              }
            }
          }
          getFacetResultsMsec = (System.nanoTime() - t0) / 1000000.0;
        }
      } else if (s == null) {
        // no sort
        if (isCountOnly) {
          countOnlyCount = searcher.count(q);
        } else {
          hits = searcher.search(q, topN);
        }
        if (doHilite) {
          hilite(hits, state, searcher, q);
        }
      } else {
        // yes sort
        hits = searcher.search(q, topN, s);
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
          StoredFields storedFields = searcher.storedFields();
          for (int i = 0; i < hits.scoreDocs.length; i++) {
            ScoreDoc scoreDoc = hits.scoreDocs[i];
            storedFields.document(scoreDoc.doc);
          }
        }
        if (vectorField != null) {
          IndexReader reader = searcher.getIndexReader();
          List<LeafReaderContext> leaves = reader.leaves();
          List<FloatVectorValues> perLeafFloatVectors = new ArrayList<>();
          for (LeafReaderContext ctx : leaves) {
            FloatVectorValues floatVectorValues = ctx.reader().getFloatVectorValues(vectorField);	
            perLeafFloatVectors.add(floatVectorValues);
          }
          for (int i = 0; i < hits.scoreDocs.length; i++) {
            ScoreDoc scoreDoc = hits.scoreDocs[i];
            int doc = scoreDoc.doc;
            int segment = ReaderUtil.subIndex(doc, leaves);
            FloatVectorValues floatVectors = perLeafFloatVectors.get(segment);
            if (floatVectors != null) {
              floatVectors.vectorValue(doc - leaves.get(segment).docBase);
            }
          }
        }

      } else if (groupsResultBlock != null) {
        totalHitCount = new TotalHits(groupsResultBlock.totalHitCount, TotalHits.Relation.EQUAL_TO);
      }
    } catch (Throwable t) {
      System.out.println("EXC: " + this);
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
      for(ScoreDoc sd : group.scoreDocs()) {
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
      /*
      String[] frags = indexState.postingsHighlighter.highlight(indexState.textFieldName, query, searcher, hits, 2);
      //System.out.println("  q=" + query + ": hilite time: " + ((t1-t0)/1000000.0));
      for(int hit=0;hit<frags.length;hit++) {
        String frag = frags[hit];
        //System.out.println("  title=" + searcher.doc(hits.scoreDocs[hit].doc).get("titleTokenized"));
        //System.out.println("    frags: " + frag);
        if (frag != null) {
          // It's fine for frag to be null: it's a
          // placeholder, meaning this hit had no hilite
          totHiliteHash += frag.hashCode();
        }
      }
      */
      // TODO: switch to UnifiedHighlighter
    }
    long t1 = System.nanoTime();
    hiliteMsec = (t1-t0)/1000000.0;
  }

  public int totHiliteHash;

  private void hilite(int docID, IndexState indexState, IndexSearcher searcher) throws IOException {
    //System.out.println("  title=" + searcher.doc(docID).get("titleTokenized"));
    if (indexState.fastHighlighter != null) {
      for(String h : indexState.fastHighlighter.getBestFragments(fieldQuery,
                                                                 searcher.getIndexReader(), docID,
                                                                 indexState.textFieldName,
                                                                 100, 2)) {
        totHiliteHash += h.hashCode();
        //System.out.println("    frag: " + h);
      }
    } else {
      Document doc = searcher.storedFields().document(docID);
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

      for (int j = 0; j < frags.length; j++) {
        if (frags[j] != null && frags[j].getScore() > 0) {
          //System.out.println("    frag " + j + ": " + frags[j].toString());
          totHiliteHash += frags[j].toString().hashCode();
        }
      }
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof SearchTask) {
      final SearchTask otherSearchTask = (SearchTask) other;
      if (q.equals(otherSearchTask.q) == false) {
        return false;
      }
      if (s != null) {
        if (otherSearchTask.s != null) {
          if (s.equals(otherSearchTask.s) == false) {
            return false;
          }
        } else {
          if (otherSearchTask.s != null) {
            return false;
          }
        }
      }
      if (isCountOnly != otherSearchTask.isCountOnly) {
        return false;
      }
      if (topN != otherSearchTask.topN) {
        return false;
      }

      if (group != null && !group.equals(otherSearchTask.group)) {
        return false;
      } else if (otherSearchTask.group != null) {
        return false;
      }

      //System.out.println("COMPARE: this=" + this + " other=" + other);

      if (facetRequests != null && facetRequests.equals(otherSearchTask.facetRequests) == false) {
        return false;
      } else if (otherSearchTask.facetRequests != null) {
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
    if (facetRequests != null) {
      hashCode ^= facetRequests.hashCode();
    }
    if (isCountOnly) {
      hashCode ^= 59236321;
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
          sum += groupDocs.totalHits().value();
          for(ScoreDoc hit : groupDocs.scoreDocs()) {
            sum = sum * PRIME + hit.doc;
          }
        }
      } else {
        for(GroupDocs<BytesRef> groupDocs : groupsResultTerms.groups) {
          sum += groupDocs.totalHits().value();
          for(ScoreDoc hit : groupDocs.scoreDocs()) {
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
    } else if (hits == null) {
      if (countOnlyCount == -1) {
        throw new AssertionError("either hits should be non-null or countOnlyCount should not be -1");
      }
      sum = countOnlyCount;
    } else {
      sum = hits.totalHits.value();
      for(ScoreDoc hit : hits.scoreDocs) {
        //System.out.println("  " + hit.doc);
        sum = sum * PRIME + hit.doc;
        if (hit instanceof FieldDoc) {
          final FieldDoc fd = (FieldDoc) hit;
          if (fd.fields != null) {
            for(Object o : fd.fields) {
              if (o != null) {
                sum = sum * PRIME + o.hashCode();
              }
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
    StringBuilder b = new StringBuilder();
    b.append("cat=");
    b.append(category);
    b.append(" q=");
    b.append(q);
    if (countOnlyCount != -1) {
      b.append(" countOnlyCount=" + countOnlyCount);
    } else {
      b.append(" s=");
      b.append(s);
      b.append(" group=");
      if (group == null) {
        b.append("null");
        b.append(" hits=");
        if (hits == null) {
          b.append("null");
        } else {
          b.append(hits.totalHits.value());
          if (hits.totalHits.relation() == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO) {
            b.append('+');
          }
        }
      } else {
        b.append(group.replace("\n", "\\n"));
        b.append(" groups=");
        if (singlePassGroup) {
          b.append(groupsResultBlock.groups.length);
          b.append(" hits=");
          b.append(groupsResultBlock.totalHitCount);
          b.append(" groupTotHits=");
          b.append(groupsResultBlock.totalGroupedHitCount);
          b.append(" totGroupCount=");
          b.append(groupsResultBlock.totalGroupCount);
        } else {
          b.append(groupsResultTerms.groups.length);
          b.append(" hits=");
          b.append(groupsResultTerms.totalHitCount);
          b.append(" groupTotHits=");
          b.append(groupsResultTerms.totalGroupedHitCount);
          b.append(" totGroupCount=");
          b.append(groupsResultTerms.totalGroupCount);
        }
      }
      b.append(" facets=" + facetRequests);
    }

    return b.toString();
  }

  @Override
  public void printResults(PrintStream out, IndexState state) throws IOException {
    IndexSearcher searcher = state.mgr.acquire();
    try {
      if (group != null) {
        if (singlePassGroup) {
          for(GroupDocs<?> groupDocs : groupsResultBlock.groups) {
            out.println("  group=null" + " totalHits=" + groupDocs.totalHits() + " groupRelevance=" + groupDocs.groupSortValues()[0]);
            for(ScoreDoc hit : groupDocs.scoreDocs()) {
              out.println("    doc=" + hit.doc + " score=" + hit.score);
            }
          }
        } else {
          for(GroupDocs<BytesRef> groupDocs : groupsResultTerms.groups) {
            out.println("  group=" + (groupDocs.groupValue() == null ? "null" : groupDocs.groupValue().utf8ToString().replace("\n", "\\n"))
                        + " totalHits=" + groupDocs.totalHits() + " groupRelevance=" + groupDocs.groupSortValues()[0]);
            for(ScoreDoc hit : groupDocs.scoreDocs()) {
              out.println("    doc=" + hit.doc + " score=" + hit.score);
            }
          }
        }
      } else if (hits instanceof TopFieldDocs) {
        StoredFields storedFields = searcher.storedFields();
        for(int idx=0;idx<hits.scoreDocs.length;idx++) {
          FieldDoc hit = (FieldDoc) hits.scoreDocs[idx];
          final Object v = hit.fields[0];
          final String vs;
          if (v instanceof Number) {
            vs = v.toString();
          } else if (v == null) {
            vs = "null";
          } else {
            vs = ((BytesRef) v).utf8ToString();
          }
          out.println("  doc=" + LineFileDocs.idToInt(storedFields.document(hit.doc).get("id")) + " " + s.getSort()[0].getField() + "=" + vs);
        }
      } else if (hits != null) {
        StoredFields storedFields = searcher.storedFields();
        for(ScoreDoc hit : hits.scoreDocs) {
          out.println("  doc=" + LineFileDocs.idToInt(storedFields.document(hit.doc).get("id")) + " score=" + hit.score);
          // print explanation
          //Explanation explain = searcher.explain(q, hit.doc);
          //out.println("    explain: " + explain);
        }
      }

      if (hiliteMsec > 0) {
        out.println(String.format(Locale.ROOT, "  hilite time %.4f msec", hiliteMsec));
      }
      if (getFacetResultsMsec > 0) {
        out.println(String.format(Locale.ROOT, "  getFacetResults time %.4f msec", getFacetResultsMsec));
      }

      if (facetResults != null) {
        out.println("  facets:");
        for(FacetResult fr : facetResults) {
          out.println("    " + fr);
        }
      }
    } finally {
      state.mgr.release(searcher);
    }
  }
}
