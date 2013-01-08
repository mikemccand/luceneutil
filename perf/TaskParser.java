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
    titleDVSort.getSort()[0].setUseIndexValues(true);
  }

  private final static Pattern filterPattern = Pattern.compile(" \\+filter=([0-9\\.]+)%");

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

      final boolean doDateFacets;

      if (text.indexOf("+dateFacets") != -1) {
        doDateFacets = true;
        text = text.replace("+dateFacets", "");
      } else {
        doDateFacets = false;
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
                                                   new SpanTermQuery(new Term(fieldName, text.substring(1+spot3)))},
                                  10,
                                  true);
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

      task = new SearchTask(category, query, sort, group, filter, topN, doHilite, doDateFacets, doStoredLoads);
    }

    return task;
  }
}
