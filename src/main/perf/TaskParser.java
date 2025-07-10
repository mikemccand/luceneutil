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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.document.IntField;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.IntervalQuery;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanOrQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CombinedFieldQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.TermQuery;

class TaskParser implements Closeable {

  private final QueryParser queryParser;
  private final String fieldName;
  private final Sort titleDVSort;
  private final Sort titleBDVSort;
  private final Sort monthDVSort; // Month of the "last modified timestamp", SORTED doc values
  private final Sort dayOfYearSort; // Day of the year of the "last modified timestamp", NUMERIC doc values
  private final Sort lastModSort;
  private final int topN;
  private final Random random;
  private final boolean doStoredLoads;
  private final IndexState state;
  final TestContext testContext;

  // non-null if we have vectors to use for KNN search:
  private final String vectorField;

  // these is used when we run search-time inference by tokenizing query text and
  // summing (averaging? and normalizing) the per-token vectors stored in
  // vectorDictionary:
  private final VectorDictionary vectorDictionary;
  private final int vectorDimension;

  // this is used when we simply use pre-computed (in random order, unassociated
  // with the specific query text) query vectors.  see src/python/infer_token_vectors_cohere.py:
  private final SeekableByteChannel vectorChannel;

  public TaskParser(IndexState state,
                    QueryParser queryParser,
                    String fieldName,
                    int topN,
                    Random random,
                    VectorDictionary vectorDictionary,
                    Path vectorFilePath,
                    int vectorDimension,
                    boolean doStoredLoads, TestContext testContext) throws IOException {
    this.queryParser = queryParser;
    this.fieldName = fieldName;
    this.topN = topN;
    this.random = random;
    this.doStoredLoads = doStoredLoads;
    this.state = state;
    this.vectorDictionary = vectorDictionary;
    this.testContext = testContext;
    if (vectorDictionary != null) {
      vectorField = "vector";
      vectorChannel = null;
      if (vectorDimension != -1) {
        throw new RuntimeException("vectorDimension must be -1 when vectorDictionary is used; got: " + vectorDimension);
      }
    } else if (vectorFilePath != null) {
      vectorField = "vector";
      vectorChannel = Files.newByteChannel(vectorFilePath, StandardOpenOption.READ);
      if (vectorDimension < 1) {
        throw new RuntimeException("vectorDimension must be > 0 when vectorFilePath is non-null; got: " + vectorDimension);
      }
    } else {
      vectorChannel = null;
      vectorField = null;
      if (vectorDimension != -1) {
        throw new RuntimeException("vectorDimension must be -1 when neither vectorDictionary nor vectorFilePath is used; got: " + vectorDimension);
      }
    }

    this.vectorDimension = vectorDimension;
    
    titleDVSort = new Sort(KeywordField.newSortField("title", false, SortedSetSelector.Type.MIN));
    titleBDVSort = new Sort(new SortField("titleBDV", SortField.Type.STRING_VAL));
    monthDVSort = new Sort(KeywordField.newSortField("month", false, SortedSetSelector.Type.MIN));
    dayOfYearSort = new Sort(IntField.newSortField("dayOfYear", false, SortedNumericSelector.Type.MIN));
    lastModSort = new Sort(LongField.newSortField("lastMod", false, SortedNumericSelector.Type.MIN));
  }

  @Override
  public void close() throws IOException {
    if (vectorChannel != null) {
      vectorChannel.close();
    }
  }

  private final static Pattern filterPattern = Pattern.compile(" \\+filter=([0-9\\.]+)%");
  private final static Pattern preFilterPattern = Pattern.compile(" \\+preFilter=([0-9\\.]+)%");
  private final static Pattern countOnlyPattern = Pattern.compile("count\\((.*?)\\)");
  private final static Pattern minShouldMatchPattern = Pattern.compile(" \\+minShouldMatch=(\\d+)($| )");
  // pattern: taskName term1 term2 term3 term4 +combinedFields=field1^1.0,field2,field3^2.0
  // this pattern doesn't handle all variations of floating numbers, such as .9 , but should be good enough for perf test query parsing purpose
  private final static Pattern combinedFieldsPattern = Pattern.compile(" \\+combinedFields=((\\p{Alnum}+(\\^\\d+.\\d)?,)+\\p{Alnum}+(\\^\\d+.\\d)?)");

  /**
   * First pass, parsing from String to some task, may/may not be an UnparsedTask
   * Called within TaskSource while creating tasks
   */
  public Task firstPassParse(String line) throws ParseException, IOException {

    String[] catTask = parseCategory(line);

    float[] vector;
    
    // special case when pulling from pre-computed embeddings -- this is to ensure the same vector (copied
    // during clone()) is searched N times when -taskRepeatCount=N.  this works for the vectorDicionary case
    // since the same lexical tokens are inferred to embeddings on each query execution.
    if (catTask[1].startsWith("vector//") && vectorChannel != null) {
      vector = new float[vectorDimension];
      ByteBuffer buffer = ByteBuffer.allocate(vectorDimension * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      int n = vectorChannel.read(buffer);
      if (n != vectorDimension * Float.BYTES) {
        throw new RuntimeException("expected " + (vectorDimension * Float.BYTES) + " vector bytes but read " + n);
      }
      buffer.position(0);
      buffer.asFloatBuffer().get(vector);
    } else {
      vector = null;
    }
    
    return new UnparsedTask(catTask, vector);
  }

  /**
   * Second pass, parsing from UnparsedTask to some concrete tasks, like SearchTask
   * May not be called from the same parser that did the first pass
   */
  public Task secondPassParse(UnparsedTask unparsedSearchTask) throws ParseException, IOException {
    return new TaskBuilder(unparsedSearchTask, this.testContext).build();
  }

  /**
   * @param line task line
   * @return array of [category, remainingText]
   */
  public static String[] parseCategory(String line) {
    final int spot = line.indexOf(':');
    if (spot == -1) {
      throw new RuntimeException("task line is malformed: " + line);
    }
    String category = line.substring(0, spot);

    int spot2 = line.indexOf(" #");
    if (spot2 == -1) {
      spot2 = line.length();
    }

    String remaining = line.substring(spot+1, spot2).trim();
    return new String[] {category, remaining};
  }

  class TaskBuilder {
    final String category;
    final String origText;
    final TestContext testContext;

    List<FacetTask> facets;
    List<FieldAndWeight> combinedFields;
    List<String> dismaxFields;
    String text;
    boolean doDrillSideways, doHilite, doStoredLoadsTask;
    Sort sort;
    String group;

    // this is only set when pulling pre-computed embeddings from file:
    private float[] vector;

    TaskBuilder(String line, TestContext testContext) {
      String[] categoryAndText = parseCategory(line);
      category = categoryAndText[0];
      origText = categoryAndText[1];
      this.testContext = testContext;
    }

    TaskBuilder(UnparsedTask unparsedSearchTask, TestContext testContext) {
      category = unparsedSearchTask.getCategory();
      origText = unparsedSearchTask.getOrigText();
      vector = unparsedSearchTask.vector;
      this.testContext = testContext;
    }

    Task build() throws ParseException, IOException {
      if (category.equals("Respell")) {
        return new RespellTask(new Term(fieldName, origText));
      } else {
        if (origText.length() == 0) {
          throw new RuntimeException("null query line");
        }
        return buildSearchTask(origText);
      }
    }

    SearchTask buildSearchTask(String input) throws ParseException, IOException {
      text = input;
      Query filter = parseFilter();
      boolean isCountOnly = parseIsCountOnly();
      facets = parseFacets();
      List<String> drillDowns = parseDrillDowns();
      doStoredLoadsTask = TaskParser.this.doStoredLoads;
      parseHilite();
      String[] taskAndType = parseTaskType(text);
      String taskType = taskAndType[0];
      text = taskAndType[1];
      int msm = parseMinShouldMatch();
      combinedFields = parseCombinedFields();
      dismaxFields = parseDismaxFields();
      Query query = buildQuery(taskType, text, msm);
      Query query2 = applyDrillDowns(query, drillDowns);
      Query query3 = applyFilter(query2, filter);
      return new SearchTask(category, isCountOnly, query3, sort, group, topN, doHilite, doStoredLoadsTask, facets, null, doDrillSideways);
    }

    String[] parseTaskType(String line) {
      int spot = line.indexOf("//");
      if (spot == -1) {
        return new String[]{"", line};
      } else {
        return new String[]{
          line.substring(0, spot),
          line.substring(spot + 2)
        };
      }
    }

    Query applyFilter(Query query, Query filter) {
      if (filter == null) {
        return query;
      } else {
        return new BooleanQuery.Builder()
          .add(query, Occur.MUST)
          .add(filter, Occur.FILTER)
          .build();
      }
    }

    Query applyDrillDowns(Query query, List<String> drillDowns) {
      if (drillDowns.isEmpty()) {
        return query;
      }
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
      return q;
    }

    Query parseFilter() {
      // Check for filter (eg: " +filter=0.5%")
      final Matcher m = filterPattern.matcher(text);
      if (m.find()) {
        final double filterPct = Double.parseDouble(m.group(1));
        // Splice out the filter string:
        text = (text.substring(0, m.start(0)) + text.substring(m.end(0), text.length())).trim();
        return new RandomQuery(filterPct);
      }
      return null;
    }

    Query parsePreFilter() {
      // Check for pre-filter (eg: " +preFilter=0.5%"), only relevant to vector search
      final Matcher m = preFilterPattern.matcher(text);
      if (m.find()) {
        final double filterPct = Double.parseDouble(m.group(1));
        // Splice out the filter string:
        text = (text.substring(0, m.start(0)) + text.substring(m.end(0), text.length())).trim();
        return new RandomQuery(filterPct);
      }
      return null;
    }

    boolean parseIsCountOnly() {
      // Check for count: "count(...)"
      final Matcher m = countOnlyPattern.matcher(text);
      if (m.find()) {
        // Splice out the count string:
        text = (text.substring(0, m.start(0)) + m.group(1) + text.substring(m.end(0), text.length())).trim();
        return true;
      }
      return false;
    }

    int parseMinShouldMatch() {
      final Matcher m2 = minShouldMatchPattern.matcher(text);
      int minShouldMatch = 0;
      if (m2.find()) {
        minShouldMatch = Integer.parseInt(m2.group(1));
        // Splice out the minShouldMatch string:
        text = (text.substring(0, m2.start(0)) + text.substring(m2.end(0), text.length())).trim();
      }
      return minShouldMatch;
    }

    class FieldAndWeight {
      final String field;
      final float weight;

      FieldAndWeight(String field, float weight) {
        this.field = field;
        this.weight = weight;
      }
    }

    List<FieldAndWeight> parseCombinedFields() {
      final Matcher m = combinedFieldsPattern.matcher(text);
      List<FieldAndWeight> result = new ArrayList<>();
      if (m.find()) {
        for (String fieldAndWeight : m.group(1).split(",")) {
          if (fieldAndWeight.contains("^")) { // boosted field
            String[] pair = fieldAndWeight.split("\\^");
            result.add(new FieldAndWeight(pair[0], Float.valueOf(pair[1])));
          } else {
            result.add(new FieldAndWeight(fieldAndWeight, 1L));
          }
        }
        // Splice out the combinedFields string:
        text = (text.substring(0, m.start(0)) + text.substring(m.end(0), text.length())).trim();
        return result;
      } else {
        return null;
      }
    }

    List<String> parseDismaxFields() {
      String marker = "+dismaxFields=";
      int i = text.indexOf(marker);
      if (i >= 0) {
        String[] fields = text.substring(i + marker.length()).split(",");
        text = text.substring(0, i);
        return Arrays.asList(fields);
      }
      return null;
    }

    List<FacetTask> parseFacets() {
      List<FacetTask> facets = new ArrayList<>();
      Map<String, TestContext.FacetMode> prefixTypes = Map.of(" +facets:", TestContext.FacetMode.UNDEFINED,
              " +post_collection_facets:", TestContext.FacetMode.POST_COLLECTION,
              " +during_collection_facets:", TestContext.FacetMode.DURING_COLLECTION
      );
      while (true) {
        boolean found = false;
        for (Map.Entry<String, TestContext.FacetMode> ent : prefixTypes.entrySet()) {
          String prefix = ent.getKey();
          TestContext.FacetMode facetMode = ent.getValue();
          int prefixLength = prefix.length();
          int i = text.indexOf(prefix);
          if (i == -1) {
            continue;
          }
          found = true;
          if (testContext.facetMode != TestContext.FacetMode.UNDEFINED) {
                  if (facetMode != TestContext.FacetMode.UNDEFINED &&
                  facetMode != testContext.facetMode) {
                    throw new IllegalArgumentException("Task and test context define different facet modes. Task: "
                            + facetMode + ", test context: " + testContext.facetMode);
                  }
                  facetMode = testContext.facetMode;
          }
          int j = text.indexOf(" ", i + 1);
          if (j == -1) {
            j = text.length();
          }
          String facetDim = text.substring(i + prefixLength, j);
          int k = facetDim.indexOf(".");
          if (k == -1) {
            throw new IllegalArgumentException("+facet:x should have format Dim.(taxonomy|sortedset); got: " + facetDim);
          }
          String s = facetDim.substring(0, k);
          if (state.facetFields.containsKey(s) == false) {
            throw new IllegalArgumentException("facetDim " + s + " was not indexed");
          }
          facets.add(new FacetTask(facetDim, facetMode));
          text = text.substring(0, i) + text.substring(j);
        }
        if (!found) {
          break;
        }
      }
      return facets;
    }

    record FacetTask(String dimension, TestContext.FacetMode facetMode){
      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FacetTask facetTask)) return false;
        return Objects.equals(facetMode, facetTask.facetMode) && Objects.equals(dimension, facetTask.dimension);
      }

      @Override
      public int hashCode() {
        return Objects.hash(dimension, facetMode);
      }

      @Override
      public String toString() {
        // facetMode is ignored so that when results are parsed to python's
        // SearchTask facets_request param was the same for all modes
        // and results for different modes were compared in benchUtil.compareHits
        return "FacetTask{" +
                "dimension='" + dimension + '\'' +
                '}';
      }
    }

    List<String> parseDrillDowns() {
      List<String> drillDowns = new ArrayList<>();
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

      if (text.indexOf("+drillSideways") != -1) {
        text = text.replace("+drillSideways", "");
        doDrillSideways = true;
        if (drillDowns.size() == 0) {
          throw new RuntimeException("cannot +drillSideways unless at least one +drillDown is defined");
        }
      } else {
        doDrillSideways = false;
      }
      return drillDowns;
    }

    void parseHilite() {
      if (text.startsWith("hilite//")) {
        doHilite = true;
        text = text.substring(8);

        // Highlighting does its own loading
        doStoredLoadsTask = false;
      } else {
        doHilite = false;
      }
    }

    Query buildQuery(String type, String text, int minShouldMatch) throws ParseException, IOException {
      Query query;
      switch(type) {
        case "ordered":
          return parseOrderedQuery();
        case "spanDis":
          return parseSpanDisjunctions();
        case "intervalDis":
          return parseIntervalDisjunctions(true);
        case "intervalDisMin":
          return parseIntervalDisjunctions(false);
        case "near":
          return parseNearQuery();
        case "multiPhrase":
          return parseMultiPhrase();
        case "disjunctionMax":
          return parseDisjunctionMax();
        case "nrq":
          return parseNRQ();
        case "intSet":
          return parseIntSet();
        case "datetimesort":
          throw new IllegalArgumentException("use lastmodndvsort instead");
        case "titlesort":
          throw new IllegalArgumentException("use titledvsort instead");
        case "vector":
          return parseVectorQuery();
        default:
          setSortAndGroup(type);
          query = queryParser.parse(text);
      }
      if (query.toString().equals("")) {
        throw new RuntimeException("query text \"" + text + "\" parsed to empty query");
      }
      if (group != null && group.equals("groupblock1pass")) {
        // confirm the index really indexed doc blocks for grouping to avoid scary confusing NPEs
        if (state.groupBlocksExist == false) {
          throw new IllegalStateException("cannot execute 'groupblock1pass': index was not built with grouping doc blocks");
        }
      }

      if (combinedFields != null) {
        return rewriteToCombinedFieldQuery(query);
      }

      if (dismaxFields != null) {
        List<Query> dismaxClauses = new ArrayList<>();
        for (String field : dismaxFields) {
          if (query instanceof TermQuery tq) {
            dismaxClauses.add(new TermQuery(new Term(field, tq.getTerm().bytes())));
          } else if (query instanceof BooleanQuery bq) {
            BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
            for (BooleanClause clause : bq.clauses()) {
              if (clause.query() instanceof TermQuery tq) {
                bqBuilder.add(new TermQuery(new Term(field, tq.getTerm().bytes())), clause.occur());
              } else {
                throw new IllegalStateException("Cannot change field of query: " + clause.query());
              }
            }
            dismaxClauses.add(bqBuilder.build());
          } else {
            throw new IllegalStateException("Cannot change field of query: " + query);
          }
        }
        return new DisjunctionMaxQuery(dismaxClauses, 0f);
      }

      if (minShouldMatch == 0) {
        return query;
      } else {
        if (!(query instanceof BooleanQuery)) {
          throw new RuntimeException("minShouldMatch can only be used with BooleanQuery: query=" + origText);
        }
        Builder b = new BooleanQuery.Builder();
        b.setMinimumNumberShouldMatch(minShouldMatch);
        for (BooleanClause clause : ((BooleanQuery) query)) {
          b.add(clause);
        }
        return b.build();
      }
    }

    private Query rewriteToCombinedFieldQuery(Query query) {
      if (query instanceof TermQuery tq) {
        CombinedFieldQuery.Builder cfqBuilder = new CombinedFieldQuery.Builder(tq.getTerm().bytes());
        for (FieldAndWeight fieldAndWeight : combinedFields) {
          cfqBuilder.addField(fieldAndWeight.field, fieldAndWeight.weight);
        }
        return cfqBuilder.build();
      } else if (query instanceof BooleanQuery bq) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.setMinimumNumberShouldMatch(bq.getMinimumNumberShouldMatch());
        for (BooleanClause clause : bq.clauses()) {
          builder.add(rewriteToCombinedFieldQuery(clause.query()), clause.occur());
        }
        return builder.build();
      } else {
        throw new IllegalArgumentException("Cannot rewrite query " + query + " to a CombinedFieldQuery");
      }
    }

    void setSortAndGroup(String taskType) {
      switch(taskType) {
        case "titledvsort":
          sort = titleDVSort;
          break;
        case "titlebdvsort":
          sort = titleBDVSort;
          break;
        case "monthdvsort":
          sort = monthDVSort;
          break;
        case "dayofyeardvsort":
          sort = dayOfYearSort;
          break;
        case "lastmodndvsort":
          sort = lastModSort;
          break;
        case "group100":
          group = "group100";
          break;
        case "group10K":
          group = "group10K";
          break;
        case "group100K":
          group = "group100K";
          break;
        case "group1M":
          group = "group1M";
          break;
        case "groupblock1pass":
          group = "groupblock1pass";
          break;
        case "groupblock":
          group = "groupblock";
          break;
      }
    }

    Query parseNRQ() {
      // field start end
      final int spot3 = text.indexOf(' ');
      if (spot3 == -1) {
        throw new RuntimeException("failed to parse query=" + text);
      }
      final int spot4 = text.indexOf(' ', spot3+1);
      if (spot4 == -1) {
        throw new RuntimeException("failed to parse query=" + text);
      }
      final String nrqFieldName = text.substring(0, spot3);
      final int start = Integer.parseInt(text.substring(1+spot3, spot4));
      final int end = Integer.parseInt(text.substring(1+spot4));
      return IntPoint.newRangeQuery(nrqFieldName, start, end);
    }

    Query parseIntSet() {
      // field num1 num2 num3 ...
      final String[] splits = text.trim().split("\\s+");
      return IntPoint.newSetQuery(splits[0],
          Arrays.stream(splits).skip(1).mapToInt(Integer::parseInt).toArray());
    }

    Query parseOrderedQuery() {
      final int spot3 = text.indexOf(' ');
      if (spot3 == -1) {
        throw new RuntimeException("failed to parse query=" + text);
      }
      return new IntervalQuery(fieldName,
          Intervals.maxwidth(10,
              Intervals.ordered(
                  Intervals.term(text.substring(0, spot3)),
                  Intervals.term(text.substring(spot3+1).trim()))));
    }

    Query parseNearQuery() {
      final int spot3 = text.indexOf(' ');
      if (spot3 == -1) {
        throw new RuntimeException("failed to parse query=" + text);
      }
      return new SpanNearQuery(
          new SpanQuery[] {new SpanTermQuery(new Term(fieldName, text.substring(0, spot3))),
            new SpanTermQuery(new Term(fieldName, text.substring(spot3+1).trim()))},
          10,
          true);
    }

    Query parseSpanDisjunctions() {
      String[] fieldHolder = new String[1];
      int[] slopHolder = new int[] {10}; // default to slop of 10
      String[][][] clauses = parseDisjunctionSpec(fieldHolder, slopHolder);
      String field = fieldHolder[0];
      int slop = slopHolder[0];
      SpanQuery[] spanClauses = Arrays.stream(clauses).map((component) -> {
        SpanQuery[] disjunct = Arrays.stream(component).map((words) -> {
          if (words.length == 1) {
            return new SpanTermQuery(new Term(field, words[0]));
          } else {
            return new SpanNearQuery(Arrays.stream(words).map((word) -> {
              return new SpanTermQuery(new Term(field, word));
            }).toArray((size) -> new SpanQuery[size]), 0, true);
          }
        }).toArray((size) -> new SpanQuery[size]);
        return disjunct.length == 1 ? disjunct[0] : new SpanOrQuery(disjunct);
      }).toArray((size) -> new SpanQuery[size]);
      // NOTE: in contrast to intervals (below), with spans there is no special
      // case for slop==0; we have only SpanNearQuery
      return spanClauses.length == 1 ? spanClauses[0] : new SpanNearQuery(spanClauses, slop, true);
    }

    Query parseIntervalDisjunctions(boolean rewrite) {
      String[] fieldHolder = new String[1];
      int[] slopHolder = new int[] {10}; // default to slop of 10
      String[][][] clauses = parseDisjunctionSpec(fieldHolder, slopHolder);
      String field = fieldHolder[0];
      int slop = slopHolder[0];
      IntervalsSource[] intervalClauses = Arrays.stream(clauses).map((component) -> {
        IntervalsSource[] disjunct = Arrays.stream(component).map((words) -> {
          if (words.length == 1) {
            return Intervals.term(words[0]);
          } else {
            IntervalsSource[] intervalWords = Arrays.stream(words).map((word) -> {
              return Intervals.term(word);
            }).toArray((size) -> new IntervalsSource[size]);
            return Intervals.phrase(intervalWords);
          }
        }).toArray((size) -> new IntervalsSource[size]);
        return disjunct.length == 1 ? disjunct[0] : Intervals.or(rewrite, disjunct);
      }).toArray((size) -> new IntervalsSource[size]);
      IntervalsSource positional;
      if (intervalClauses.length == 1) {
        // NOTE: apparently maxgaps/ordered/phrase do not rewrite for the single-clause
        // case? ... or in any event not in a way that's transparent immediately after
        // query construction. So we do it manually here, in order be sure that Intervals
        // can "put their best foot forward" on the plain-disjunction case.
        positional = intervalClauses[0];
      } else if (slop == 0) {
        // assumption: "phrase" is equivalent to "maxgaps(0, ordered)"?
        positional = Intervals.phrase(intervalClauses);
      } else {
        // the usual case
        positional = Intervals.maxgaps(slop, Intervals.ordered(intervalClauses));
      }
      return new IntervalQuery(field, positional);
    }

    Query parseMultiPhrase() {
      String[] fieldHolder = new String[1];
      int[] slopHolder = new int[1]; // implicit default to slop=0
      String[][][] clauses = parseDisjunctionSpec(fieldHolder, slopHolder);
      if (slopHolder[0] != 0) {
        throw new IllegalArgumentException("multiPhrase only supports slop==0; found:" + slopHolder[0]);
      }
      String field = fieldHolder[0];
      MultiPhraseQuery.Builder b = new MultiPhraseQuery.Builder();
      for (int i = 0; i < clauses.length; i++) {
        String words[][] = clauses[i];
        Term terms[] = new Term[words.length];
        for (int j = 0; j < words.length; j++) {
          terms[j] = new Term(field, words[j][0]);
        }
        b.add(terms);
      }
      return b.build();
    }

    private String[][][] parseDisjunctionSpec(String[] fieldHolder, int[] slopHolder) {
      int colon = text.indexOf(':');
      if (colon == -1) {
        throw new RuntimeException("failed to parse query=" + text);
      }
      fieldHolder[0] = text.substring("(".length(), colon);
      int endParen = text.indexOf(')');
      if (endParen == -1) {
        throw new RuntimeException("failed to parse query=" + text);
      }
      int checkExplicitSlop = endParen + 1;
      if (text.length() > checkExplicitSlop && text.charAt(checkExplicitSlop) == '~') {
        slopHolder[0] = Integer.parseInt(text.substring(checkExplicitSlop + 1).split("[^0-9]", 2)[0]);
      }
      String queryText = text.substring(colon+1, endParen);
      return Arrays.stream(queryText.split("\\s+")).map((clause) -> {
        return Arrays.stream(clause.split("\\|")).map((component) -> {
          return component.split("-");
        }).toArray((size) -> new String[size][]);
      }).toArray((size) -> new String[size][][]);
    }

    Query parseDisjunctionMax() {
      final int spot3 = text.indexOf(' ');
      if (spot3 == -1) {
        throw new RuntimeException("failed to parse query=" + text);
      }
      List<Query> clauses = new ArrayList<Query>();
      clauses.add(new TermQuery(new Term(fieldName, text.substring(0, spot3))));
      clauses.add(new TermQuery(new Term(fieldName, text.substring(spot3+1).trim())));
      return new DisjunctionMaxQuery(clauses, 0.1f);
    }

    Query parseVectorQuery() throws IOException {
      Query preFilter = parsePreFilter();

      float[] queryVector;
      if (vectorChannel != null) {
        if (this.vector == null) {
          throw new IllegalStateException("reading from pre-computed embeddings but pre-loaded vector is null");
        }
        queryVector = this.vector;
      } else {
        // search time inference from query text
        queryVector = vectorDictionary.computeTextVector(text);
      }
      
      if (preFilter != null) {
        return new KnnFloatVectorQuery(vectorField, queryVector, topN, preFilter);
      } else {
        return new KnnFloatVectorQuery(vectorField, queryVector, topN);
      }
    }
  }
}
