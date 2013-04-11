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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;

class TaskParser {
  private final QueryParser queryParser;
  private final String fieldName;
  private final Map<Double,Filter> filters;
  private final Sort dateTimeSort;
  private final Sort titleSort;
  private final Sort titleDVSort;
  private final int topN;
  private final Random random;
  private final boolean doStoredLoads;

    public TaskParser(QueryParser queryParser,
                    String fieldName,
                    Map<Double,Filter> filters,
                    int topN,
                    Random random,
                    boolean doStoredLoads) {
    this.queryParser = queryParser;
    this.fieldName = fieldName;
    this.filters = filters;
    this.topN = topN;
    this.random = random;
    this.doStoredLoads = doStoredLoads;
    dateTimeSort = new Sort(new SortField("datenum", SortField.Type.LONG));
    titleSort = new Sort(new SortField("title", SortField.Type.STRING));
    titleDVSort = new Sort(new SortField("titleDV", SortField.Type.STRING));
  }

  private final static Pattern filterPattern = Pattern.compile(" \\+filter=([0-9\\.]+)%");
  private final static Pattern minShouldMatchPattern = Pattern.compile(" \\+minShouldMatch=(\\d+)($| )");

  public Task parseOneTask(String line) throws ParseException {

    final int spot = line.indexOf(':');
    if (spot == -1) {
      throw new RuntimeException("task line is malformed: " + line);
    }
    final String category = line.substring(0, spot);
      
    int spot2 = line.indexOf(" #");
    if (spot2 == -1) {
      spot2 = line.length();
    }
      
    String text = line.substring(spot+1, spot2).trim();
    String origText = text;

    final Task task;
    if (category.equals("Respell")) {
      task = new RespellTask(new Term(fieldName, text));
    } else {
      if (text.length() == 0) {
        throw new RuntimeException("null query line");
      }

      // Check for filter (eg: " +filter=0.5%")
      final Matcher m = filterPattern.matcher(text);
      Filter filter;
      if (m.find()) {
        final double filterPct = Double.parseDouble(m.group(1));
        // Splice out the filter string:
        text = (text.substring(0, m.start(0)) + text.substring(m.end(0), text.length())).trim();
        filter = filters.get(filterPct);
        if (filter == null) {
	  filter = new CachingWrapperFilter(new RandomFilter(filterPct, random.nextLong()));
          filters.put(filterPct, filter);
        }
      } else {
        filter = null;
      }

      final Matcher m2 = minShouldMatchPattern.matcher(text);
      final int minShouldMatch;
      if (m2.find()) {
        minShouldMatch = Integer.parseInt(m2.group(1));
        // Splice out the minShouldMatch string:
        text = (text.substring(0, m2.start(0)) + text.substring(m2.end(0), text.length())).trim();
      } else {
        minShouldMatch = 0;
      }

      final List<FacetGroup> facetGroups = new ArrayList<FacetGroup>();
      while (true) {
        int i = text.indexOf("+facets:");
        if (i == -1) {
          break;
        }
        int j = text.indexOf(" ", i);
        if (j == -1) {
          j = text.length();
        }
        facetGroups.add(new FacetGroup(text.substring(i+8, j)));
        text = text.substring(0, i) + text.substring(j);
      }

      if (facetGroups.size() > 1) {
        throw new IllegalArgumentException("can only run one facet CLP per query for now");
      }

      final Sort sort;
      final Query query;
      final String group;
      final boolean doHilite;

      boolean doStoredLoads = this.doStoredLoads;

      if (text.startsWith("hilite//")) {
        doHilite = true;
        text = text.substring(8);

        // Highlighting does its own loading
        doStoredLoads = false;
      } else {
        doHilite = false;
      }

      if (text.startsWith("near//")) {
        final int spot3 = text.indexOf(' ');
        if (spot3 == -1) {
          throw new RuntimeException("failed to parse query=" + text);
        }
        query = new SpanNearQuery(
                                  new SpanQuery[] {new SpanTermQuery(new Term(fieldName, text.substring(6, spot3))),
                                                   new SpanTermQuery(new Term(fieldName, text.substring(spot3+1).trim()))},
                                  10,
                                  true);
        sort = null;
        group = null;
      } else if (text.startsWith("disjunctionMax//")) {
        final int spot3 = text.indexOf(' ');
        if (spot3 == -1) {
          throw new RuntimeException("failed to parse query=" + text);
        }
        DisjunctionMaxQuery dismax = new DisjunctionMaxQuery(1f);
        dismax.add(new TermQuery(new Term(fieldName, text.substring(16, spot3))));
        dismax.add(new TermQuery(new Term(fieldName, text.substring(spot3+1).trim())));
        query = dismax;
        sort = null;
        group = null;
      } else if (text.startsWith("nrq//")) {
        // field start end
        final int spot3 = text.indexOf(' ');
        if (spot3 == -1) {
          throw new RuntimeException("failed to parse query=" + text);
        }
        final int spot4 = text.indexOf(' ', spot3+1);
        if (spot4 == -1) {
          throw new RuntimeException("failed to parse query=" + text);
        }
        final String nrqFieldName = text.substring(5, spot3);
        final int start = Integer.parseInt(text.substring(1+spot3, spot4));
        final int end = Integer.parseInt(text.substring(1+spot4));
        query = NumericRangeQuery.newIntRange(nrqFieldName, start, end, true, true);
        sort = null;
        group = null;
      } else if (text.startsWith("datetimesort//")) {
        sort = dateTimeSort;
        query = queryParser.parse(text.substring(14, text.length()));
        group = null;
      } else if (text.startsWith("titlesort//")) {
        sort = titleSort;
        query = queryParser.parse(text.substring(11, text.length()));
        group = null;
      } else if (text.startsWith("titledvsort//")) {
        sort = titleDVSort;
        query = queryParser.parse(text.substring(13, text.length()));
        group = null;
      } else if (text.startsWith("group100//")) {
        group = "group100";
        query = queryParser.parse(text.substring(10, text.length()));
        sort = null;
      } else if (text.startsWith("group10K//")) {
        group = "group10K";
        query = queryParser.parse(text.substring(10, text.length()));
        sort = null;
      } else if (text.startsWith("group100K//")) {
        group = "group100K";
        query = queryParser.parse(text.substring(11, text.length()));
        sort = null;
      } else if (text.startsWith("group1M//")) {
        group = "group1M";
        query = queryParser.parse(text.substring(9, text.length()));
        sort = null;
      } else if (text.startsWith("groupblock1pass//")) {
        group = "groupblock1pass";
        query = queryParser.parse(text.substring(17, text.length()));
        sort = null;
      } else if (text.startsWith("groupblock//")) {
        group = "groupblock";
        query = queryParser.parse(text.substring(12, text.length()));
        sort = null;
      } else {
        group = null;
        query = queryParser.parse(text);
        sort = null;
      }

      if (query.toString().equals("")) {
        throw new RuntimeException("query text \"" + text + "\" parsed to empty query");
      }

      /*
      if (category.startsWith("Or")) {
        for(BooleanClause clause : ((BooleanQuery) query).clauses()) {
          ((TermQuery) clause.getQuery()).setNoSkip();
        }
      }
      */

      if (minShouldMatch != 0) {
        if (!(query instanceof BooleanQuery)) {
          throw new RuntimeException("minShouldMatch can only be used with BooleanQuery: query=" + origText);
        }
        ((BooleanQuery) query).setMinimumNumberShouldMatch(minShouldMatch);
      }

      task = new SearchTask(category, query, sort, group, filter, topN, doHilite, doStoredLoads, facetGroups);
    }

    return task;
  }
}
