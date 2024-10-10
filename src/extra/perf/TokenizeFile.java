import java.io.BufferedReader;
import java.io.CharArrayReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
import org.apache.lucene.analysis.pattern.SimplePatternTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

// javac -cp ../../build/analysis/common/lucene-analyzers-common-7.0.0-SNAPSHOT.jar:../../build/core/lucene-core-7.0.0-SNAPSHOT.jar TokenizeFile.java

// java -cp .:../../build/analysis/common/lucene-analyzers-common-7.0.0-SNAPSHOT.jar:../../build/core/lucene-core-7.0.0-SNAPSHOT.jar TokenizeFile /l/data/geonames-documents.json.ls.blocks

// pushd ../../core; ant jar; popd; ant jar; javac -cp ../../build/analysis/common/lucene-analyzers-common-7.0.0-SNAPSHOT.jar:../../build/core/lucene-core-7.0.0-SNAPSHOT.jar /l/util/src/main/perf/TokenizeFile.java; java -cp /l/util/src/main/perf:../../build/analysis/common/lucene-analyzers-common-7.0.0-SNAPSHOT.jar:../../build/core/lucene-core-7.0.0-SNAPSHOT.jar TokenizeFile /lucenedata/enwiki/enwiki-20130102-lines.txt
  
public class TokenizeFile {

  public static void main(String[] args) throws IOException {
    double minSec = Double.POSITIVE_INFINITY;
    for(int iter=0;iter<10;iter++) {
      double sec = run(iter, args[0]);
      System.out.println(String.format(Locale.ROOT, "iter %d: %.3f sec", iter, sec));
      if (sec < minSec) {
        System.out.println("  **");
        minSec = sec;
      }
    }
  }

  private static double run(int iter, String fileName) throws IOException {
    File wikiLinesFile = new File(fileName);
    // 64 KB buffer
    InputStream is = new FileInputStream(wikiLinesFile);
    BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"), 1 << 16);
    Tokenizer ts = new SimplePatternTokenizer("[^ \t\r\n]+");
    //Tokenizer ts = new PatternTokenizer(Pattern.compile("[^ \t\r\n]+"), 0);
    CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
    long hashSum = 0;
    int tokenCount = 0;
    long t0 = System.nanoTime();
    char[] buffer = new char[1024*1024];
    while (tokenCount < 30000000) {
      /*
      String s = reader.readLine();
      if (s == null) {
        break;
      }
      ts.setReader(new StringReader(s));
      */
      int count = reader.read(buffer, 0, buffer.length);
      if (count == -1) {
        break;
      }
      ts.setReader(new CharArrayReader(buffer, 0, count));
      ts.reset();
      while (ts.incrementToken()) {
        hashSum += termAtt.hashCode();
        tokenCount++;
      }
      ts.close();
    }
    long t1 = System.nanoTime();
    double sec = (t1-t0)/1000000000.0;
    if (iter == 0) {
      System.out.println(String.format(Locale.ROOT, "%d tokens, hashSum=%d", tokenCount, hashSum));
    }
    reader.close();
    return sec;
  }
}
