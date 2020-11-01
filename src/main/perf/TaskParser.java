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

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class TaskParser {

  private final QueryParser queryParser;
  private final String fieldName;
  private final Sort titleDVSort;
  private final Sort titleBDVSort;
  private final Sort monthDVSort; // Month of the "last modified timestamp", SORTED doc values
  private final Sort dayOfYearDVSort; // Day of the year of the "last modified timestamp", NUMERIC doc values
  private final Sort lastModNDVSort;
  private final int topN;
  private final Random random;
  private final String vectorField;
  private final boolean doStoredLoads;
  private final IndexState state;

  public TaskParser(IndexState state,
                    QueryParser queryParser,
                    String fieldName,
                    int topN,
                    Random random,
                    String vectorField,
                    boolean doStoredLoads) {
    this.queryParser = queryParser;
    this.fieldName = fieldName;
    this.topN = topN;
    this.random = random;
    this.vectorField = vectorField;
    this.doStoredLoads = doStoredLoads;
    this.state = state;
    titleDVSort = new Sort(new SortField("titleDV", SortField.Type.STRING));
    titleBDVSort = new Sort(new SortField("titleBDV", SortField.Type.STRING_VAL));
    monthDVSort = new Sort(new SortField("monthSortedDV", SortField.Type.STRING));
    dayOfYearDVSort = new Sort(new SortField("dayOfYearNumericDV", SortField.Type.INT));
    lastModNDVSort = new Sort(new SortField("lastModNDV", SortField.Type.LONG));
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
      Query filter;
      if (m.find()) {
        final double filterPct = Double.parseDouble(m.group(1));
        // Splice out the filter string:
        text = (text.substring(0, m.start(0)) + text.substring(m.end(0), text.length())).trim();
        filter = new RandomQuery(filterPct);
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

      final List<String> facets = new ArrayList<String>();
      while (true) {
        int i = text.indexOf(" +facets:");
        if (i == -1) {
          break;
        }
        int j = text.indexOf(" ", i+1);
        if (j == -1) {
          j = text.length();
        }
        String facetDim = text.substring(i+9, j);
        int k = facetDim.indexOf(".");
        if (k == -1) {
          throw new IllegalArgumentException("+facet:x should have format Dim.(taxonomy|sortedset); got: " + facetDim);
        }
        String s = facetDim.substring(0, k);
        if (state.facetFields.containsKey(s) == false) {
          throw new IllegalArgumentException("facetDim " + s + " was not indexed");
        }
        facets.add(facetDim);
        text = text.substring(0, i) + text.substring(j);
      }

      final List<String> drillDowns = new ArrayList<String>();

      // Eg: +drillDown:Date=2001,2004
      while (true) {
        int i = text.indexOf("+drillDown:");
        if (i == -1) {
          break;
        }
        int j = text.indexOf(" ", i);
        if (j == -1) {
          j = text.length();
        }

        String s = text.substring(i+11, j);
        text = text.substring(0, i) + text.substring(j);

        drillDowns.add(s);
      }

      boolean doDrillSideways;
      if (text.indexOf("+drillSideways") != -1) {
        text = text.replace("+drillSideways", "");
        doDrillSideways = true;
        if (drillDowns.size() == 0) {
          throw new RuntimeException("cannot +drillSideways unless at least one +drillDown is defined");
        }
      } else {
        doDrillSideways = false;
      }

      final Sort sort;
      Query query;
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


      if (text.startsWith("ordered//")) {
        final int spot3 = text.indexOf(' ');
        if (spot3 == -1) {
          throw new RuntimeException("failed to parse query=" + text);
        }
        query = new IntervalQuery(fieldName,
          Intervals.maxwidth(10,
            Intervals.ordered(
              Intervals.term(text.substring(9, spot3)),
              Intervals.term(text.substring(spot3+1).trim())
            )
        ));
        sort = null;
        group = null;
      } else if (text.startsWith("near//")) {
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
      } else if (text.startsWith("multiPhrase//(")) {
        int colon = text.indexOf(':');
        if (colon == -1) {
          throw new RuntimeException("failed to parse query=" + text);
        }
        String field = text.substring("//multiPhrase(".length(), colon);
        MultiPhraseQuery.Builder b = new MultiPhraseQuery.Builder();
        int endParen = text.indexOf(')');
        if (endParen == -1) {
          throw new RuntimeException("failed to parse query=" + text);
        }
        String queryText = text.substring(colon+1, endParen);
        String elements[] = queryText.split("\\s+");
        for (int i = 0; i < elements.length; i++) {
          String words[] = elements[i].split("\\|");
          Term terms[] = new Term[words.length];
          for (int j = 0; j < words.length; j++) {
            terms[j] = new Term(field, words[j]);
          }
          b.add(terms);
        }
        query = b.build();
        sort = null;
        group = null;
      } else if (text.startsWith("disjunctionMax//")) {
        final int spot3 = text.indexOf(' ');
        if (spot3 == -1) {
          throw new RuntimeException("failed to parse query=" + text);
        }
        List<Query> clauses = new ArrayList<Query>();
        clauses.add(new TermQuery(new Term(fieldName, text.substring(16, spot3))));
        clauses.add(new TermQuery(new Term(fieldName, text.substring(spot3+1).trim())));
        DisjunctionMaxQuery dismax = new DisjunctionMaxQuery(clauses, 0.1f);
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
        query = IntPoint.newRangeQuery(nrqFieldName, start, end);
        sort = null;
        group = null;
      } else if (text.startsWith("datetimesort//")) {
        throw new IllegalArgumentException("use lastmodndvsort instead");
      } else if (text.startsWith("titlesort//")) {
        throw new IllegalArgumentException("use titledvsort instead");
      } else if (text.startsWith("titledvsort//")) {
        sort = titleDVSort;
        query = queryParser.parse(text.substring(13, text.length()));
        group = null;
      } else if (text.startsWith("titlebdvsort//")) {
        sort = titleBDVSort;
        query = queryParser.parse(text.substring(14, text.length()));
        group = null;
      } else if (text.startsWith("monthdvsort//")) {
        sort = monthDVSort;
        query = queryParser.parse(text.substring(13, text.length()));
        group = null;
      } else if (text.startsWith("dayofyeardvsort//")) {
        sort = dayOfYearDVSort;
        query = queryParser.parse(text.substring(17, text.length()));
        group = null;
      } else if (text.startsWith("lastmodndvsort//")) {
        sort = lastModNDVSort;
        query = queryParser.parse(text.substring(16, text.length()));
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

      if (minShouldMatch != 0) {
        if (!(query instanceof BooleanQuery)) {
          throw new RuntimeException("minShouldMatch can only be used with BooleanQuery: query=" + origText);
        }
        Builder b = new BooleanQuery.Builder();
        b.setMinimumNumberShouldMatch(minShouldMatch);
        for (BooleanClause clause : ((BooleanQuery) query)) {
          b.add(clause);
        }
        query = b.build();
      }

      Query query2;

      if (!drillDowns.isEmpty()) {
        DrillDownQuery q = new DrillDownQuery(state.facetsConfig, query);
        for(String s : drillDowns) {
          int i = s.indexOf('=');
          if (i == -1) {
            throw new IllegalArgumentException("drilldown is missing =");
          }
          String dim = s.substring(0, i);
          String values = s.substring(i+1);

          while (true) {
            i = values.indexOf(',');
            if (i == -1) {
              q.add(dim, values);
              break;
            }
            q.add(dim, values.substring(0, i));
            values = values.substring(i+1);
          }
        }
        query2 = q;
      } else {
        query2 = query;
      }

      if (filter != null) {
        query2 = new BooleanQuery.Builder()
            .add(query2, Occur.MUST)
            .add(filter, Occur.FILTER)
            .build();
      }

      /*
        if (category.startsWith("Or")) {
        for(BooleanClause clause : ((BooleanQuery) query).clauses()) {
        ((TermQuery) clause.getQuery()).setNoSkip();
        }
        }
      */

      task = new SearchTask(category, query2, sort, group, topN, doHilite, doStoredLoads, facets, vectorField, doDrillSideways);
    }

    return task;
  }
}
