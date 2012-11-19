import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.icu.ICUFoldingFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.search.spell.TermFreqIterator;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingSuggester;
import org.apache.lucene.search.suggest.analyzing.FuzzySuggester;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

// TODO
//   - char filter to remove ', -, /

// javac -cp /l/lucene.trunk2/lucene/build/core/classes/java:/l/lucene.trunk2/lucene/build/suggest/classes/java:/l/lucene.trunk2/lucene/build/analysis/common/classes/java:/l/lucene.trunk2/lucene/build/analysis/icu/classes/java FreeDBSuggest.java
// java -Xmx14g -cp .:/l/lucene.trunk2/lucene/build/core/classes/java:/l/lucene.trunk2/lucene/build/suggest/classes/java:/l/lucene.trunk2/lucene/build/analysis/common/classes/java:/l/lucene.trunk2/lucene/build/analysis/icu/classes/java:/l/util.trunk2/../lucene.trunk2/lucene/analysis/icu/lib/icu4j-49.1.jar FreeDBSuggest -create

public class FreeDBSuggest {
  public static void main(String[] args) throws Exception {

    // StandardAnalyzer plus ICUFoldingFilter
    Analyzer a = new Analyzer() {
        @Override 
        protected TokenStreamComponents createComponents(final String fieldName,
                                                         final Reader reader) {

          /*
          Tokenizer t = new WhitespaceTokenizer(Version.LUCENE_50, reader);
          TokenStream tf = t;
          //TokenStream tf = new LowerCaseFilter(Version.LUCENE_50, tf);
          return new TokenStreamComponents(t, tf);
          */

          // StandardAnalyzer + ICUFoldingFilter:
          Version matchVersion = Version.LUCENE_50;
          final int maxTokenLength = 255;

          final StandardTokenizer src = new StandardTokenizer(matchVersion, reader);
          src.setMaxTokenLength(maxTokenLength);
          TokenStream tok = src;
          //TokenStream tok = new StandardFilter(matchVersion, tok);
          tok = new LowerCaseFilter(matchVersion, tok);
          //tok = new StopFilter(matchVersion, tok, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
          tok = new ICUFoldingFilter(tok);
          return new TokenStreamComponents(src, tok) {
            @Override
            protected void setReader(final Reader reader) throws IOException {
              src.setMaxTokenLength(maxTokenLength);
              super.setReader(reader);
            }
          };
        }
      };

    Lookup suggester = new AnalyzingSuggester(a, a, AnalyzingSuggester.PRESERVE_SEP, 256, -1);
    //Lookup suggester = new FuzzySuggester(a, a, AnalyzingSuggester.PRESERVE_SEP, 256, -1, 1, true, 1, 3);

    boolean doCreate = false;
    boolean doServer = false;
    String suggestFileName = "freedb.suggest";
    for(String arg : args) {
      if (arg.equals("-create")) {
        doCreate = true;
      } else if (arg.equals("-server")) {
        doServer = true;
      } else {
        suggestFileName = arg;
      }
    }

    if (doCreate) {
      CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT);
      InputStream is = new FileInputStream("/lucenedata/freedb/all.txt");
      final BufferedReader reader = new BufferedReader(new InputStreamReader(is, decoder), 1<<16);
      //final Set<String> seen = new HashSet<String>();
      final AtomicInteger songCount = new AtomicInteger();
      final AtomicInteger albumCount = new AtomicInteger();
      final Random random = new Random(17);

      TermFreqIterator terms = new TermFreqIterator() {

          String[] current;
          String title;
          int currentUpto;
          int count;
          long weight;

          @Override
          public BytesRef next() throws IOException {

            if (current == null || currentUpto == current.length) {
              String line = reader.readLine();
              //System.out.println("got line: " + line);
              if (line == null) {
                System.out.println("Done reading!");
                return null;
              }
              albumCount.incrementAndGet();
              current = line.trim().split("\t");
              if (current.length == 2) {
                System.out.println("FAIL: " + line);
              }
              String diskID = current[0];
              title = current[1];
              currentUpto = 2;
              count++;
              if (count % 100000 == 0) {
                System.out.println(count + "...");
              }
            }
            songCount.incrementAndGet();
            weight = 1+random.nextInt(1000000);
            // OOME @ 12G heap:
            //return new BytesRef(current[currentUpto++] + " [" + title + "]");
            return new BytesRef(current[currentUpto++]);
          }

          @Override
          public long weight() {
            return weight;
          }

          @Override
          public Comparator<BytesRef> getComparator() {
            return null;
          }
        };

      long t0 = System.nanoTime();
      suggester.build(terms);
      long t1 = System.nanoTime();
      reader.close();
      System.out.println("Done building: " + ((t1-t0)/1000000000.) + " sec; " + songCount + " songs; " + albumCount + " albums");

      FileOutputStream os = new FileOutputStream(new File(suggestFileName));
      suggester.store(os);
      os.close();
      System.out.println("Saved to " + suggestFileName + ": " + ((AnalyzingSuggester) suggester).sizeInBytes() + " bytes");
    } else {
      long t0 = System.nanoTime();
      FileInputStream is = new FileInputStream(new File(suggestFileName));
      suggester.load(is);
      is.close();
      long t1 = System.nanoTime();
      System.out.println(((t1-t0)/1000000.0) + " msec to load");
    }

    /*
    for(String arg : args) {
      if (!arg.equals("-create")) {
        long t0 = System.nanoTime();
        List<LookupResult> results = suggester.lookup(arg, false, 10);
        long t1 = System.nanoTime();
        System.out.println("Suggestions for " + arg + " (" + ((t1-t0)/1000000.0) + " msec):");
        for(LookupResult result : results) {
          System.out.println("  " + result);
        }
      }
    }
    */

    if (doServer) {
      byte[] buffer = new byte[100];
      while(true) {
        if (System.in.read(buffer, 0, 2) != 2) {
          break;
        }
        int len = Integer.parseInt(new String(buffer, 0, 2, "UTF-8").trim());
        if (System.in.read(buffer, 0, len) != len) {
          break;
        }
        List<LookupResult> results = suggester.lookup(new String(buffer, 0, len, "UTF-8"), false, 10);
        StringBuilder sb = new StringBuilder();
        for(LookupResult result : results) {
          sb.append(result.toString());
          sb.append('\n');
        }
        byte[] response = sb.toString().getBytes("UTF-8");
        byte[] lenBytes = String.format("%5d", response.length).getBytes("UTF-8");
        System.out.write(lenBytes, 0, lenBytes.length);
        System.out.write(response, 0, response.length);
      }
    } else {

      int TOP_N = 7;
      int ITERS = 10;

      for(int prefixLen=2;prefixLen<12;prefixLen+=2) {
        CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT);
        InputStream is = new FileInputStream("/lucenedata/freedb/subset.txt");
        final BufferedReader reader = new BufferedReader(new InputStreamReader(is, decoder), 1<<16);
        List<String> queries = new ArrayList<String>();
        while (true) {
          String line = reader.readLine();
          if (line == null) {
            break;
          }
          BytesRef b = new BytesRef(line.trim());
          if (b.length >= prefixLen) {
            b.length = prefixLen;
            String s = null;
            try {
              s = b.utf8ToString();
            } catch (ArrayIndexOutOfBoundsException aioobe) {
            }
            if (s != null) {
              List<LookupResult> results = suggester.lookup(s, false, TOP_N);
              if (results.size() != 0) {
                queries.add(s);
              }
            }
          }
        }
        reader.close();

        System.out.println("\nprefixLen=" + prefixLen);
        double bestSec = 0;
        for(int cycle=0;cycle<1;cycle++) {
          long hash = 0;
          long t0 = System.nanoTime();
          for(int iter=0;iter<ITERS;iter++) {
            //System.out.println("ITER=" + iter);
            for(String q : queries) {
              if (iter == 0 && cycle == 0) {
                System.out.println("  q=" + q);
              }
              for(LookupResult r : suggester.lookup(q, false, TOP_N)) {
                if (iter == 0 && cycle == 0) {
                  System.out.println("    " + r.key + " " + r.value);
                }
                hash += r.key.toString().hashCode();
              }
            }
          }
          long t1 = System.nanoTime();
          double sec = (t1-t0)/1000000000.;
          System.out.println(String.format("  cycle %d: %.2f sec for %d lookups = %.1f lookups/sec; hash=%d",
                                           cycle, sec, (ITERS*queries.size()), (ITERS*queries.size()/sec), hash));
          if (cycle == 0 || sec < bestSec) {
            bestSec = sec;
          }
        }
        System.out.println(String.format("  best: %.1f lookups/sec", (ITERS*queries.size()/bestSec)));
      }
    }
  }
}
