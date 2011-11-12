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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
//import org.apache.lucene.index.codecs.mocksep.MockSepCodec;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.similarities.BasicSimilarityProvider;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.SimilarityProvider;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.*;
import org.apache.lucene.search.grouping.*;
import org.apache.lucene.search.grouping.term.*;
import org.apache.lucene.search.spans.*;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.store.*;
import org.apache.lucene.util.*;

// TODO
//   - post queries on pao
//   - fix pk lookup to tolerate deletes
//   - get regexp title queries
//   - test shingle at search time
//   - push doSort into here...

// commits: single, multi, delsingle, delmulti

// trunk:
//   javac -Xlint -Xlint:deprecation -cp build/classes/java:../modules/suggest/build/classes/java:../modules/analysis/build/common/classes/java:../modules/grouping/build/classes/java perf/SearchPerfTest.java perf/RandomFilter.java
//   java -cp .:build/contrib/spellchecker/classes/java:build/classes/java:../modules/analysis/build/common/classes/java perf.SearchPerfTest MMapDirectory /p/lucene/indices/wikimedium.clean.svn.Standard.nd10M/index StandardAnalyzer /p/lucene/data/tasks.txt 6 1 body -1 no multi

public class SearchPerfTest {
  
  private static class IndexState {
    public final IndexSearcher searcher;
    public final IndexReader[] subReaders;
    public final DirectSpellChecker spellChecker;
    public final Filter groupEndFilter;
    public int[] docIDToID;

    public IndexState(IndexSearcher searcher, DirectSpellChecker spellChecker) {
      this.searcher = searcher;
      this.spellChecker = spellChecker;
      subReaders = searcher.getIndexReader().getSequentialSubReaders();
      groupEndFilter = new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("groupend", "x"))));
    }

    private void setDocIDToID() throws IOException {
      docIDToID = new int[searcher.getIndexReader().maxDoc()];
      int base = 0;
      for(IndexReader sub : searcher.getIndexReader().getSequentialSubReaders()) {
        final int[] ids = FieldCache.DEFAULT.getInts(sub, "id", new FieldCache.IntParser() {
            @Override
            public int parseInt(BytesRef term) {
              return LineFileDocs.idToInt(term);
            }
          }, false);
        System.arraycopy(ids, 0, docIDToID, base, ids.length);
        base += ids.length;
      }
    }
  }

  // Abstract class representing a single task (one query,
  // one batch of PK lookups, on respell).  Each Task
  // instance is executed and results are recorded in it and
  // then later verified/summarized:
  private static abstract class Task {
    //public String origString;

    public abstract void go(IndexState state) throws IOException;

    public abstract String getCategory();

    @Override
    public abstract Task clone();

    // these are set once the task is executed
    public long runTimeNanos;
    public int threadID;

    // Called after go, to return "summary" of the results.
    // This may use volatile docIDs -- the checksum is just
    // used to verify the same task run multiple times got
    // the same result, ie that things are in fact thread
    // safe:
    public abstract long checksum();

    // Called after go to print details of the task & result
    // to stdout:
    public abstract void printResults(IndexState state) throws IOException;
  }

  private final static class SearchTask extends Task {
    private final String category;
    private final Query q;
    private final Sort s;
    private final Filter f;
    private final String group;
    private final int topN;
    private final boolean singlePassGroup;
    private final boolean doCountGroups;
    private TopDocs hits;
    private TopGroups<Object> groupsResultBlock;
    private TopGroups<BytesRef> groupsResultTerms;

    public SearchTask(String category, Query q, Sort s, String group, Filter f, int topN) {
      this.category = category;
      this.q = q;
      this.s = s;
      this.f = f;
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
    }

    @Override
    public Task clone() {
      Query q2 = (Query) q.clone();
      if (q2 == null) {
        throw new RuntimeException("q=" + q + " failed to clone");
      }
      if (singlePassGroup) {
        return new SearchTask(category, q2, s, "groupblock1pass", f, topN);
      } else {
        return new SearchTask(category, q2, s, group, f, topN);
      }
    }

    @Override
    public String getCategory() {
      return category;
    }

    @Override
    public void go(IndexState state) throws IOException {
      //System.out.println("go group=" + this.group + " single=" + singlePassGroup + " xxx=" + xxx + " this=" + this);
      if (group != null) {
        if (singlePassGroup) {
          final BlockGroupingCollector c = new BlockGroupingCollector(Sort.RELEVANCE, 10, true, state.groupEndFilter);
          state.searcher.search(q, c);
          groupsResultBlock = c.getTopGroups(null, 0, 0, 10, true);
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
          
          state.searcher.search(q, c);

          final Collection<SearchGroup<BytesRef>> topGroups = c1.getTopGroups(0, true);
          if (topGroups != null) {
            final TermSecondPassGroupingCollector c2 = new TermSecondPassGroupingCollector(group, topGroups, Sort.RELEVANCE, null, 10, true, true, true);
            if (cCache.isCached()) {
              cCache.replay(c2);
            } else {
              state.searcher.search(q, c2);
            }
            groupsResultTerms = c2.getTopGroups(0);
            if (allGroupsCollector != null) {
              groupsResultTerms = new TopGroups<BytesRef>(groupsResultTerms,
                                                          allGroupsCollector.getGroupCount());
            }
          }
        }
      } else if (s == null && f == null) {
        hits = state.searcher.search(q, topN);
      } else if (s == null && f != null) {
        hits = state.searcher.search(q, f, topN);
      } else {
        hits = state.searcher.search(q, f, topN, s);
      }

      //System.out.println("TE: " + TermsEnum.getStats());
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
      long sum = 0;
      //System.out.println("checksum q=" + q + " f=" + f);
      if (group != null) {
        if (singlePassGroup) {
          for(GroupDocs<Object> groupDocs : groupsResultBlock.groups) {
            sum += groupDocs.totalHits;
            for(ScoreDoc hit : groupDocs.scoreDocs) {
              sum += hit.doc;
            }
          }
        } else {
          for(GroupDocs<BytesRef> groupDocs : groupsResultTerms.groups) {
            sum += groupDocs.totalHits;
            for(ScoreDoc hit : groupDocs.scoreDocs) {
              sum += hit.doc;
            }
          }
        }
      } else {
        sum = hits.totalHits;
        for(ScoreDoc hit : hits.scoreDocs) {
          //System.out.println("  " + hit.doc);
          sum += hit.doc;
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
    public void printResults(IndexState state) throws IOException {
      if (group != null) {
        if (singlePassGroup) {
          for(GroupDocs<Object> groupDocs : groupsResultBlock.groups) {
            System.out.println("  group=null" + " totalHits=" + groupDocs.totalHits + " groupRelevance=" + groupDocs.groupSortValues[0]);
            for(ScoreDoc hit : groupDocs.scoreDocs) {
              System.out.println("    doc=" + hit.doc + " score=" + hit.score);
            }
          }
        } else {
          for(GroupDocs<BytesRef> groupDocs : groupsResultTerms.groups) {
            System.out.println("  group=" + (groupDocs.groupValue == null ? "null" : groupDocs.groupValue.utf8ToString().replace("\n", "\\n")) + " totalHits=" + groupDocs.totalHits + " groupRelevance=" + groupDocs.groupSortValues[0]);
            for(ScoreDoc hit : groupDocs.scoreDocs) {
              System.out.println("    doc=" + hit.doc + " score=" + hit.score);
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
          System.out.println("  doc=" + state.docIDToID[hit.doc] + " field=" + vs);
        }
      } else {
        for(ScoreDoc hit : hits.scoreDocs) {
          System.out.println("  doc=" + state.docIDToID[hit.doc] + " score=" + hit.score);
        }
      }
    }
  }

  private final static class PKLookupTask extends Task {
    private final BytesRef[] ids;
    private final int[] answers;
    private final int ord;

    @Override
    public String getCategory() {
      return "PKLookup";
    }

    private PKLookupTask(PKLookupTask other) {
      ids = other.ids;
      ord = other.ord;
      answers = new int[ids.length];
    }

    public PKLookupTask(int maxDoc, Random random, int count, Set<BytesRef> seen, int ord) {
      this.ord = ord;
      ids = new BytesRef[count];
      answers = new int[count];
      int idx = 0;
      while(idx < count) {
        final BytesRef id = new BytesRef(LineFileDocs.intToID(random.nextInt(maxDoc)));
        /*
        if (idx == 0) {
          id = new BytesRef("000013688");
        } else {
          id = new BytesRef(LineFileDocs.intToID(random.nextInt(maxDoc)));
        }
        */
        if (!seen.contains(id)) {
          seen.add(id);
          ids[idx++] = id;
        }
      }
      Arrays.sort(ids);
    }

    @Override
    public Task clone() {
      return new PKLookupTask(this);
    }

    @Override
    public void go(IndexState state) throws IOException {
      final boolean DO_DOC_LOOKUP = true;
      int base = 0;
      for (IndexReader sub : state.subReaders) {
        DocsEnum docs = null;
        final TermsEnum termsEnum = sub.fields().terms("id").getThreadTermsEnum();
        //System.out.println("\nTASK: sub=" + sub);
        for(int idx=0;idx<ids.length;idx++) {
          //System.out.println("TEST: lookup " + ids[idx].utf8ToString());
          //if (TermsEnum.SeekStatus.FOUND == termsEnum.seek(ids[idx], false, true)) { 
          if (termsEnum.seekExact(ids[idx], false)) { 
            //System.out.println("  found!");
            docs = termsEnum.docs(null, docs);
            assert docs != null;
            final int docID = docs.nextDoc();
            if (docID == DocsEnum.NO_MORE_DOCS) {
              answers[idx] = -1;
            } else {
              answers[idx] = base + docID;
              //System.out.println("  docID=" + docID);
            }
          }
        }
        base += sub.maxDoc();
      }
    }

    @Override
    public String toString() {
      return "PK" + ord + "[" + ids.length + "]";
    }

    @Override
    public long checksum() {
      // TODO, but, not sure it makes sense since we will
      // run a different PK lookup each time...?
      return 0;
    }

    @Override
    public void printResults(IndexState state) throws IOException {
      for(int idx=0;idx<ids.length;idx++) {

        if (answers[idx] == DocsEnum.NO_MORE_DOCS) {
          throw new RuntimeException("PKLookup: id=" + ids[idx].utf8ToString() + " failed to find a matching document");
        }

        final int id = LineFileDocs.idToInt(ids[idx]);
        //System.out.println("  " + id + " -> " + answers[idx]);
        final int actual = state.docIDToID[answers[idx]];
        if (actual != id) {
          throw new RuntimeException("PKLookup: id=" + LineFileDocs.intToID(id) + " returned doc with id=" + LineFileDocs.intToID(actual) + " docID=" + answers[idx]);
        }
      }
    }
  }

  private final static class RespellTask extends Task {
    private final Term term;
    private SuggestWord[] answers;

    public RespellTask(Term term) {
      this.term = term;
    }

    @Override
    public Task clone() {
      return new RespellTask(term);
    }

    @Override
    public void go(IndexState state) throws IOException {
      answers = state.spellChecker.suggestSimilar(term, 10, state.searcher.getIndexReader(), SuggestMode.SUGGEST_MORE_POPULAR);
    }

    @Override
    public String toString() {
      return "respell " + term.text();
    }
    
    @Override
    public String getCategory() {
      return "Respell";
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof RespellTask) {
        return term.equals(((RespellTask) other).term);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return term.hashCode();
    }

    @Override
    public long checksum() {
      long sum = 0;
      for(SuggestWord suggest : answers) {
        sum += suggest.string.hashCode() + Integer.valueOf(suggest.freq).hashCode();
      }
      return sum;
    }

    @Override
    public void printResults(IndexState state) {
      for(SuggestWord suggest : answers) {
        System.out.println("  " + suggest.string + " freq=" + suggest.freq + " score=" + suggest.score);
      }
    }
  }

  private static IndexCommit findCommitPoint(String commit, Directory dir) throws IOException {
    Collection<IndexCommit> commits = IndexReader.listCommits(dir);
    for (final IndexCommit ic : commits) {
      Map<String,String> map = ic.getUserData();
      String ud = null;
      if (map != null) {
        ud = map.get("userData");
        System.out.println("found commit=" + ud);
        if (ud != null && ud.equals(commit)) {
          return ic;
        }
      }
    }
    throw new RuntimeException("could not find commit '" + commit + "'");
  }

  private final static Pattern filterPattern = Pattern.compile(" \\+filter=([0-9\\.]+)%");

  // TODO: can't we use RandomFilter here...?
  private static final class PreComputedRandomFilter extends Filter {

    private final FixedBitSet[] segmentBits;
    private final double pct;

    public PreComputedRandomFilter(List<Integer> maxDocPerSeg, Random random, double pctAcceptDocs) {
      //System.out.println("FILT: pct=" + pctAcceptDocs);
      this.pct = pctAcceptDocs;
      pctAcceptDocs /= 100.0;
      segmentBits = new FixedBitSet[maxDocPerSeg.size()];
      for(int segID=0;segID<maxDocPerSeg.size();segID++) {
        final int maxDoc = maxDocPerSeg.get(segID);
        final FixedBitSet bits = segmentBits[segID] = new FixedBitSet(maxDoc);
        int count = 0;
        for(int docID=0;docID<maxDoc;docID++) {
          if (random.nextDouble() <= pctAcceptDocs) {
            bits.set(docID);
            count++;
          }
        }
        //System.out.println("  r=" + maxDoc + " ct=" + count);
      }
    }

    @Override
    public int hashCode() {
      return new Double(pct).hashCode();
    }

    @Override
    public boolean equals(Object other) {
      return this == other;
    }

    @Override
    public String toString() {
      return "PreComputedRandomFilter(pctAccept=" + pct + ")";
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) {
      final FixedBitSet bits = segmentBits[context.ord];
      assert context.reader.maxDoc() == bits.length();
      return BitsFilteredDocIdSet.wrap(bits, acceptDocs);
    }
  }

  private static List<Task> loadTasks(IndexReader reader, QueryParser p, String fieldName, String filePath, boolean doSort, Random random) throws IOException, ParseException {

    final List<Task> tasks = new ArrayList<Task>();

    final List<Integer> maxDocPerSegment = new ArrayList<Integer>();
    for(IndexReader subReader : reader.getSequentialSubReaders()) {
      maxDocPerSegment.add(subReader.maxDoc());
    }

    final BufferedReader taskFile = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"), 16384);
    int count = 0;

    final Sort dateTimeSort = new Sort(new SortField("datenum", SortField.Type.LONG));
    final Sort titleSort = new Sort(new SortField("title", SortField.Type.STRING));

    while(true) {
      String line = taskFile.readLine();
      if (line == null) {
        break;
      }
      line = line.trim();
      if (line.indexOf("#") == 0) {
        continue;
      }

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
        final Filter filter;
        if (m.find()) {
          final double filterPct = Double.parseDouble(m.group(1));
          // Splice out the filter string:
          text = (text.substring(0, m.start(0)) + text.substring(m.end(0), text.length())).trim();
          filter = new CachingWrapperFilter(new PreComputedRandomFilter(maxDocPerSegment, random, filterPct));
        } else {
          filter = null;
        }

        final Sort sort;
        final Query query;
        final String group;
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
          query = p.parse(text.substring(14, text.length()));
          group = null;
        } else if (text.startsWith("titlesort//")) {
          sort = titleSort;
          query = p.parse(text.substring(11, text.length()));
          group = null;
        } else if (text.startsWith("group100//")) {
          group = "group100";
          query = p.parse(text.substring(10, text.length()));
          sort = null;
        } else if (text.startsWith("group10K//")) {
          group = "group10K";
          query = p.parse(text.substring(10, text.length()));
          sort = null;
        } else if (text.startsWith("group100K//")) {
          group = "group100K";
          query = p.parse(text.substring(10, text.length()));
          sort = null;
        } else if (text.startsWith("group1M//")) {
          group = "group1M";
          query = p.parse(text.substring(9, text.length()));
          sort = null;
        } else if (text.startsWith("groupblock1pass//")) {
          group = "groupblock1pass";
          query = p.parse(text.substring(17, text.length()));
          sort = null;
        } else if (text.startsWith("groupblock//")) {
          group = "groupblock";
          query = p.parse(text.substring(12, text.length()));
          sort = null;
        } else {
          group = null;
          query = p.parse(text);
          sort = null;
        }

        if (query.toString().equals("")) {
          throw new RuntimeException("query text \"" + text + "\" parsed to empty query");
        }

        /*
        final int mod = (count++) % 3;
        if (!doSort || mod == 0) {
          sort = null;
        } else if (mod == 1) {
          sort = dateTimeSort;
        } else {
          sort = titleSort;
        }
        */
        task = new SearchTask(category, query, sort, group, filter, 10);
      }

      //task.origString = line;
      tasks.add(task);
    }

    return tasks;
  }

  public static void main(String[] args) throws Exception {

    // args: dirImpl indexPath numThread numIterPerThread
    // eg java SearchPerfTest /path/to/index 4 100
    final Directory dir;
    final String dirImpl = args[0];
    final String dirPath = args[1];
    final String analyzer = args[2];
    final String tasksFile = args[3];
    final int threadCount = Integer.parseInt(args[4]);
    final int taskRepeatCount = Integer.parseInt(args[5]);
    final String fieldName = args[6];
    int numTaskPerCat = Integer.parseInt(args[7]);
    final boolean doSort = args[8].equals("yes");

    // Used to choose which random subset of tasks we will
    // run, to generate the PKLookup tasks, and to generate
    // any random pct filters:
    final long staticRandomSeed = Long.parseLong(args[9]);

    // Used to shuffle the random subset of tasks:
    final long randomSeed = Long.parseLong(args[10]);

    if (dirImpl.equals("MMapDirectory")) {
      dir = new MMapDirectory(new File(dirPath));
    } else if (dirImpl.equals("NIOFSDirectory")) {
      dir = new NIOFSDirectory(new File(dirPath));
    } else if (dirImpl.equals("SimpleFSDirectory")) {
      dir = new SimpleFSDirectory(new File(dirPath));
    } else {
      throw new RuntimeException("unknown directory impl \"" + dirImpl + "\"");
    }
    
    // TODO: this could be way better.
    final String similarity = args[11];
    // now reflect
    final Class<? extends Similarity> simClazz = 
      Class.forName("org.apache.lucene.search.similarities." + similarity).asSubclass(Similarity.class);
    final SimilarityProvider provider = new BasicSimilarityProvider(simClazz.newInstance());

    System.out.println("Using dir impl " + dir.getClass().getName());
    System.out.println("Analyzer " + analyzer);
    System.out.println("Similarity " + similarity);
    System.out.println("Thread count " + threadCount);
    System.out.println("Task repeat count " + taskRepeatCount);
    System.out.println("Tasks file " + tasksFile);
    System.out.println("Num task per cat " + numTaskPerCat);
    System.out.println("JVM " + (Constants.JRE_IS_64BIT ? "is" : "is not") + " 64bit");

    //final long t0 = System.currentTimeMillis();
    final IndexSearcher searcher;
    Filter f = null;
    boolean doOldFilter = false;
    boolean doNewFilter = false;
    if (args.length == 15) {
      final String commit = args[12];
      System.out.println("open commit=" + commit);
      IndexReader reader = IndexReader.open(findCommitPoint(commit, dir), true);
      Filter filt = new RandomFilter(Double.parseDouble(args[14])/100.0);
      if (args[13].equals("FilterOld")) {
        f = new CachingWrapperFilter(filt);
        /*
        AtomicReaderContext[] leaves = ReaderUtil.leaves(reader.getTopReaderContext());
        for(int subID=0;subID<leaves.length;subID++) {
          f.getDocIdSet(leaves[subID]);
        }
        */
      } else {
        throw new RuntimeException("4th arg should be FilterOld or FilterNew");
      }
      searcher = new IndexSearcher(reader);
      searcher.setSimilarityProvider(provider);
    } else if (args.length == 13) {
      final String commit = args[12];
      System.out.println("open commit=" + commit);
      searcher = new IndexSearcher(IndexReader.open(findCommitPoint(commit, dir), true));
      searcher.setSimilarityProvider(provider);
    } else {
      // open last commit
      System.out.println("open latest commit");
      searcher = new IndexSearcher(dir);
      searcher.setSimilarityProvider(provider);
    }

    System.out.println("searcher=" + searcher);

    //s.search(new TermQuery(new Term("body", "bar")), null, 10, new Sort(new SortField("unique1000000", SortField.STRING)));
    //final long t1 = System.currentTimeMillis();
    //System.out.println("warm time = " + (t1-t0)/1000.0);

    //System.gc();
    //System.out.println("RAM: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
    final Analyzer a;
    if (analyzer.equals("EnglishAnalyzer")) {
      a = new EnglishAnalyzer(Version.LUCENE_31);
    } else if (analyzer.equals("ClassicAnalyzer")) {
      a = new ClassicAnalyzer(Version.LUCENE_30);
    } else if (analyzer.equals("StandardAnalyzer")) {
      a = new StandardAnalyzer(Version.LUCENE_40, Collections.emptySet());
    } else if (analyzer.equals("ShingleStandardAnalyzer")) {
      a = new ShingleAnalyzerWrapper(new StandardAnalyzer(Version.LUCENE_40, Collections.emptySet()),
                                     2, 2, ShingleFilter.TOKEN_SEPARATOR, true, true);
    } else {
      throw new RuntimeException("unknown analyzer " + analyzer);
    } 

    final Set<BytesRef> pkSeenIDs = new HashSet<BytesRef>();
    final List<PKLookupTask> pkLookupTasks = new ArrayList<PKLookupTask>();

    final QueryParser p = new QueryParser(Version.LUCENE_31, "body", a);
    p.setLowercaseExpandedTerms(false);

    // Pick a random top N tasks per category:
    final Random staticRandom = new Random(staticRandomSeed);
    final List<Task> tasks = loadTasks(searcher.getIndexReader(), p, fieldName, tasksFile, doSort, staticRandom);
    Collections.shuffle(tasks, staticRandom);
    final List<Task> prunedTasks = pruneTasks(tasks, numTaskPerCat);

    //for(Task task : prunedTasks) {
    //System.out.println("TASK: " + task.origString);
    //}

    // Add PK tasks
    final int numPKTasks = (int) Math.min(searcher.getIndexReader().maxDoc()/6000., numTaskPerCat);
    for(int idx=0;idx<numPKTasks;idx++) {
      prunedTasks.add(new PKLookupTask(searcher.getIndexReader().maxDoc(), staticRandom, 4000, pkSeenIDs, idx));
    }

    final List<Task> allTasks = new ArrayList<Task>();

    final Random random = new Random(randomSeed);

    // Copy the pruned tasks multiple times, shuffling the order each time:
    for(int iter=0;iter<taskRepeatCount;iter++) {
      Collections.shuffle(prunedTasks, random);
      for(Task task : prunedTasks) {
        allTasks.add(task.clone());
      }
    }
    System.out.println("TASK LEN=" + allTasks.size());

    final AtomicInteger nextTask = new AtomicInteger();
    final Map<Task,Task> tasksSeen = new HashMap<Task,Task>();

    final Thread[] threads = new Thread[threadCount];
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch stopLatch = new CountDownLatch(threadCount);
    final DirectSpellChecker spellChecker = new DirectSpellChecker();
    // Evil respeller:
    //spellChecker.setMinPrefix(0);
    //spellChecker.setMaxInspections(1024);
    final IndexState indexState = new IndexState(searcher, spellChecker);
    for(int threadIDX=0;threadIDX<threadCount;threadIDX++) {
      threads[threadIDX] = new TaskThread(startLatch, stopLatch, allTasks, nextTask, indexState, threadIDX);
      threads[threadIDX].start();
    }
    Thread.sleep(10);

    final long startNanos = System.nanoTime();
    startLatch.countDown();
    stopLatch.await();
    final long endNanos = System.nanoTime();

    System.out.println("\n" + ((endNanos - startNanos)/1000000.0) + " msec total");

    indexState.setDocIDToID();

    System.out.println("\nResults for " + allTasks.size() + " tasks:");
    for(final Task task : allTasks) {
      final Task other = tasksSeen.get(task);
      if (other != null) {
        if (task.checksum() != other.checksum()) {
          throw new RuntimeException("task " + task + " hit different checksums: " + task.checksum() + " vs " + other.checksum());
        }
      } else {
        tasksSeen.put(task, task);
      }
      System.out.println("\nTASK: " + task);
      System.out.println("  " + (task.runTimeNanos/1000000.0) + " msec");
      System.out.println("  thread " + task.threadID);
      task.printResults(indexState);
    }

    allTasks.clear();

    // Try to get RAM usage -- some ideas poached from http://www.javaworld.com/javaworld/javatips/jw-javatip130.html
    final Runtime runtime = Runtime.getRuntime();
    long usedMem1 = usedMemory(runtime);
    long usedMem2 = Long.MAX_VALUE;
    for(int iter=0;iter<10;iter++) {
      runtime.runFinalization();
      runtime.gc();
      Thread.currentThread().yield();
      Thread.sleep(100);
      usedMem2 = usedMem1;
      usedMem1 = usedMemory(runtime);
    }
    System.out.println("\nHEAP: " + usedMemory(runtime));
  }

  private static List<Task> pruneTasks(List<Task> tasks, int numTaskPerCat) {
    final Map<String,Integer> catCounts = new HashMap<String,Integer>();
    final List<Task> newTasks = new ArrayList<Task>();
    for(Task task : tasks) {
      final String cat = task.getCategory();
      Integer v = catCounts.get(cat);
      int catCount;
      if (v == null) {
        catCount = 0;
      } else {
        catCount = v.intValue();
      }

      if (catCount >= numTaskPerCat) {
        // System.out.println("skip task cat=" + cat);
        continue;
      }
      catCount++;
      catCounts.put(cat, catCount);
      newTasks.add(task);
    }

    return newTasks;
  }

  private static long usedMemory(Runtime runtime) {
    return runtime.totalMemory() - runtime.freeMemory();
  }
  
  private static class TaskThread extends Thread {
    private final CountDownLatch startLatch;
    private final CountDownLatch stopLatch;
    private final List<Task> tasks;
    private final AtomicInteger nextTask;
    private final IndexState indexState;
    private final int threadID;

    public TaskThread(CountDownLatch startLatch, CountDownLatch stopLatch, List<Task> tasks, AtomicInteger nextTask, IndexState indexState, int threadID) {
      this.startLatch = startLatch;
      this.stopLatch = stopLatch;
      this.tasks = tasks;
      this.nextTask = nextTask;
      this.indexState = indexState;
      this.threadID = threadID;
    }

    @Override
    public void run() {
      try {
        startLatch.await();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return;
      }

      try {
        while(true) {
          final int next = nextTask.getAndIncrement();
          if (next >= tasks.size()) {
            break;
          }
          final Task task = tasks.get(next);
          final long t0 = System.nanoTime();
          try {
            task.go(indexState);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
          task.runTimeNanos = System.nanoTime()-t0;
          task.threadID = threadID;
        }
      } finally {
        stopLatch.countDown();
      }
    }
  }
}
