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

import org.apache.lucene.store.*;
import org.apache.lucene.index.*;
import org.apache.lucene.util.*;
import org.apache.lucene.search.spell.*;

import java.io.IOException;
import java.io.File;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.io.FileOutputStream;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;

// TODO
//   - maybe run the query and if it produces too few results, nuke it? (eg AndHighMed)
//   - must dedup -- make sure no query is repeated
//   - would be nice to do 3-word phrase queries too?

/*
  Reads an existing index and derives "hard" queries from it.

  Steps:
    * Make a shingles index (use Index):
      javac -Xlint:deprecation -cp ../modules/analysis/build/common/classes/java:build/classes/java:build/classes/test perf/Indexer.java perf/LineFileDocs.java
      java -cp .:../modules/analysis/build/common/classes/java:build/classes/java:build/classes/test perf.Indexer NIOFSDirectory /p/lucene/indices/shingles ShingleStandardAnalyzer /p/lucene/data/enwiki-20110115-lines.txt 1000000 6 no yes 256.0 -1 Standard no >& /dev/shm/index.x

    * Run this:
      javac -cp build/classes/java:build/contrib/spellchecker/classes/java perf/CreateQueries.java
      java -cp build/classes/java:build/contrib/spellchecker/classes/java:. perf.CreateQueries /p/lucene/indices/shingles body queries.txt >& /dev/shm/terms.x
*/

// javac -cp build/classes/java:build/contrib/spellchecker/classes/java perf/CreateQueries.java

// java -cp build/classes/java:build/contrib/spellchecker/classes/java:. perf.CreateQueries /p/lucene/indices/wikimedium.clean.svn.Standard.nd10M/index body queries.txt
public class CreateQueries {

  private static class TermFreq {
    BytesRef term;
    long df;

    public TermFreq(BytesRef term, long df) {
      this.term = new BytesRef(term);
      this.df = df;
    }
  }

  private static class MostFrequentTerms extends PriorityQueue<TermFreq> {
    public MostFrequentTerms(int maxSize) {
      initialize(maxSize);
    }

    @Override
    protected boolean lessThan(TermFreq tf1, TermFreq tf2) {
      return tf1.df < tf2.df;
    }
  }

  // Number of queries for each type (ie 500 BooleanOrQuery, 500 PhraseQuery, etc.):
  private final static int NUM_QUERIES = 500;
  private final static int TOP_N = 50000;

  public static void main(String[] args) throws IOException {
    if (args.length != 3) {
      System.out.println();
      System.out.println("Usage: java perf.CreateQueries /path/to/shingled/index fieldName queriesFileOut");
      System.exit(1);
    }

    final String indexPath = args[0];
    final String field = args[1];
    final String queriesFileOut = args[2];

    final BufferedWriter queriesOut = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(queriesFileOut),"UTF8"));

    final Directory dir = FSDirectory.open(new File(indexPath));
    final IndexReader r = IndexReader.open(dir);

    System.out.println("\nFind top df terms...");
    
    // First pass: get high/medium freq terms:
    final TermFreq[] topTerms = getTopTermsByDocFreq(r, field, TOP_N, false);

    final long maxDF = topTerms[0].df;
    int upto = 1;
    while(topTerms[upto].df >= maxDF/10) {
      upto++;
    }
    
    final TermFreq[] highFreqTerms = new TermFreq[upto];
    System.arraycopy(topTerms, 0, highFreqTerms, 0, highFreqTerms.length);

    for(int idx=0;idx<NUM_QUERIES;idx++) {
      final TermFreq tf = topTerms[idx];
      queriesOut.write("Term: " + tf.term.utf8ToString() + " # freq=" + tf.df + "\n");
    }

    while(topTerms[upto].df >= maxDF/100) {
      upto++;
    }
    final TermFreq[] mediumFreqTerms = new TermFreq[upto - highFreqTerms.length];
    System.arraycopy(topTerms, highFreqTerms.length, mediumFreqTerms, 0, mediumFreqTerms.length);

    final Random random = new Random(1742);

    System.out.println("  " + highFreqTerms.length + " high freq terms");
    System.out.println("  " + mediumFreqTerms.length + " medium freq terms");

    makePrefixQueries(mediumFreqTerms, queriesOut);
    makeNRQs(random, queriesOut);

    makeAndOrQueries(random, highFreqTerms, mediumFreqTerms, queriesOut);

    makeWildcardQueries(topTerms, queriesOut);

    processShingles(r, field, queriesOut);

    makeFuzzyAndRespellQueries(r, field, topTerms, queriesOut);

    queriesOut.close();

    r.close();
    dir.close();
  }

  private static void makeNRQs(Random random, Writer queriesOut) throws IOException {
    // Add in some numeric range queries:
    for(int idx=0;idx<NUM_QUERIES;idx++) {
      // Seconds in the day 0..86400
      final int gap = 30000 + random.nextInt(56400);
      final int start = random.nextInt(86400-gap);
      queriesOut.write("IntNRQ: nrq//timesecnum " + start + " " + (start+gap) + "\n");
    }
    queriesOut.flush();
  }

  private static void makePrefixQueries(TermFreq[] terms, Writer queriesOut) throws IOException {

    final Set<String> seen = new HashSet<String>();

    int idx = 0;
    int lastCount = 0;
    while(seen.size() < NUM_QUERIES) {
      if (idx == terms.length) {
        throw new RuntimeException("not enough unique prefixes");
      }
      final String term = terms[idx++].term.utf8ToString();
      if (term.length() >= 3) {
        String pref = term.substring(0, 3);
        if (!seen.contains(pref)) {
          seen.add(pref);
          queriesOut.write("Prefix3: " + pref + "*\n");
        }
      }
    }
    queriesOut.flush();
  }

  private static void makeWildcardQueries(TermFreq[] terms, Writer queriesOut) throws IOException {

    final Set<String> seen = new HashSet<String>();

    int idx = 0;
    int lastCount = 0;
    while(seen.size() < NUM_QUERIES) {
      if (idx == terms.length) {
        throw new RuntimeException("not enough unique prefixes");
      }
      final String term = terms[idx++].term.utf8ToString();
      if (term.length() >= 3) {
        String wc = term.substring(0, 2) + "*" + term.substring(term.length()-1);
        if (!seen.contains(wc)) {
          seen.add(wc);
          queriesOut.write("Wildcard: " + wc + "\n");
        }
      }
    }
    queriesOut.flush();
  }

  private static void makeFuzzyAndRespellQueries(IndexReader r, String field, TermFreq[] topTerms, Writer queriesOut) throws IOException {

    System.out.println("\nFind top fuzzy/respell terms...");
    final DirectSpellChecker spellChecker = new DirectSpellChecker();    
    spellChecker.setThresholdFrequency(1.0f);

    final MostFrequentTerms pq = new MostFrequentTerms(3*NUM_QUERIES);

    // TODO: use threads...?
    int count = 0;
    for(TermFreq tdf : topTerms) {
      if ((++count) % 1000 == 0) {
        System.out.println("  "  + count + " of " + topTerms.length + "...");
      }
      if (tdf.term.length < 5) {
        continue;
      }
      // TODO: make my own fuzzy enum?
      long sumDF = 0;
      SuggestWord[] suggested = spellChecker.suggestSimilar(new Term(field, tdf.term), 50, r, true);
      if (suggested.length < 5) {
        continue;
      }
      for(SuggestWord suggest : suggested) {
        sumDF += suggest.freq;
      }

      // Strongly favor higher number of suggestions and gently favor higher sumDF:
      final long score = (long) (Math.log(sumDF) * suggested.length);

      final TermFreq newTF = new TermFreq(tdf.term, score); 
      final TermFreq bumpedTF = pq.insertWithOverflow(newTF);

      if (bumpedTF != newTF) {
        System.out.println("  " + newTF.term.utf8ToString() + " score=" + score + " suggestCount=" + suggested.length);
      }
    }

    if (pq.size() < 3*NUM_QUERIES) {
      throw new RuntimeException("index is too small: only " + pq.size() + " top fuzzy terms");
    }

    int downTo = 3*NUM_QUERIES;
    while (pq.size()>0) {
      TermFreq tdf = pq.pop();
      System.out.println("  " + tdf.term.utf8ToString() + " freq=" + tdf.df);
      final int mod = pq.size() % 3;
      if (mod == 0) {
        queriesOut.write("Fuzzy1: " + tdf.term.utf8ToString() + "~1\n");
      } else if (mod == 1) {
        queriesOut.write("Fuzzy2: " + tdf.term.utf8ToString() + "~2\n");
      } else {
        queriesOut.write("Respell: " + tdf.term.utf8ToString() + "\n");
      }
    }
    queriesOut.flush();
  }


  private static void makeAndOrQueries(Random random, TermFreq[] highFreqTerms, TermFreq[] mediumFreqTerms, Writer queriesOut) throws IOException {

    final Set<String> seen = new HashSet<String>();

    // +high +high
    int count = 0;
    while(count < NUM_QUERIES) {
      int idx1 = random.nextInt(highFreqTerms.length);
      int idx2 = idx1;
      while(idx2 == idx1) {
        idx2 = random.nextInt(highFreqTerms.length);
      }
      if (idx1 > idx2) {
        final int sav = idx1;
        idx1 = idx2;
        idx2 = sav;
      }
      final TermFreq high1 = highFreqTerms[idx1];
      final TermFreq high2 = highFreqTerms[idx2];
      final String query = "+" + high1.term.utf8ToString() + " +" + high2.term.utf8ToString();
      if (!seen.contains(query)) {
        seen.add(query);
        count++;
        queriesOut.write("AndHighHigh: " + query + " # freq=" + high1.df + " freq=" + high2.df + "\n");
      }
    }

    // +high +med
    count = 0;
    while(count < NUM_QUERIES) {
      final int idx1 = random.nextInt(highFreqTerms.length);
      final int idx2 = random.nextInt(mediumFreqTerms.length);
      final TermFreq high = highFreqTerms[idx1];
      final TermFreq medium = mediumFreqTerms[idx2];
      final String query = "+" + high.term.utf8ToString() + " +" + medium.term.utf8ToString();
      if (!seen.contains(query)) {
        seen.add(query);
        count++;
        queriesOut.write("AndHighMed: " + query + " # freq=" + high.df + " freq=" + medium.df + "\n");
      }
    }
    
    // high high
    count = 0;
    while(count < NUM_QUERIES) {
      int idx1 = random.nextInt(highFreqTerms.length);
      int idx2 = idx1;
      while(idx2 == idx1) {
        idx2 = random.nextInt(highFreqTerms.length);
      }
      if (idx1 > idx2) {
        final int sav = idx1;
        idx1 = idx2;
        idx2 = sav;
      }
      final TermFreq high1 = highFreqTerms[idx1];
      final TermFreq high2 = highFreqTerms[idx2];
      final String query = high1.term.utf8ToString() + " " + high2.term.utf8ToString();
      if (!seen.contains(query)) {
        seen.add(query);
        count++;
        queriesOut.write("OrHighHigh: " + query + " # freq=" + high1.df + " freq=" + high2.df + "\n");
      }
    }

    // high med
    count = 0;
    while(count < NUM_QUERIES) {
      final int idx1 = random.nextInt(highFreqTerms.length);
      final int idx2 = random.nextInt(mediumFreqTerms.length);
      final TermFreq high = highFreqTerms[idx1];
      final TermFreq medium = mediumFreqTerms[idx2];
      final String query = high.term.utf8ToString() + " " + medium.term.utf8ToString();
      if (!seen.contains(query)) {
        seen.add(query);
        count++;
        queriesOut.write("OrHighMed: " + query + " # freq=" + high.df + " freq=" + medium.df + "\n");
      }
    }
    queriesOut.flush();
  }

  private static TermFreq[] getTopTermsByDocFreq(IndexReader r, String field, int topN, boolean doShingles) throws IOException {
    final MostFrequentTerms pq = new MostFrequentTerms(topN);
    Terms terms = MultiFields.getTerms(r, field);
    if (terms != null) {
      TermsEnum termsEnum = terms.iterator();
      while (termsEnum.next() != null) {
        final boolean isShingle = termsEnum.term().utf8ToString().indexOf(' ') != -1;
        if (isShingle == doShingles) {
          pq.insertWithOverflow(new TermFreq(termsEnum.term(), termsEnum.docFreq()));
        }
      }
    } else {
      throw new RuntimeException("field '" + field + "' does not exist");
    }

    if (pq.size() < topN) {
      throw new RuntimeException("index is too small: only " + pq.size() + " unique terms");
    }

    final TermFreq[] topTerms = new TermFreq[topN];
    int downTo = topN-1;
    while (pq.size()>0) {
      topTerms[downTo--] = pq.pop();
    }

    return topTerms;
  }

  private static void processShingles(IndexReader r, String field, Writer queriesOut) throws IOException {
    System.out.println("\nFind phrase queries...");
    // First pass: get high/medium freq shingles:
    final TermFreq[] topShingles = getTopTermsByDocFreq(r, field, 3*NUM_QUERIES, true);
    
    for(int idx=0;idx<3*NUM_QUERIES;idx++) {
      final TermFreq tf = topShingles[idx];
      final int mod = idx % 3;
      if (mod == 0) {
        queriesOut.write("Phrase: \"" + tf.term.utf8ToString() + "\" # freq=" + tf.df + "\n");
      } else if (mod == 1) {
        queriesOut.write("SloppyPhrase: \"" + tf.term.utf8ToString() + "\"~4 # freq=" + tf.df + "\n");
      } else {
        queriesOut.write("SpanNear: near//" + tf.term.utf8ToString() + " # freq=" + tf.df + "\n");
      }
    }
    queriesOut.flush();
  }
}
