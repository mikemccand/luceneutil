import java.util.*;
import java.io.*;
import org.apache.tika.language.LanguageIdentifier;

// javac -cp /lucene/tika.clean/tika-app/target/tika-app-1.0-SNAPSHOT.jar /lucene/util/langdetect/TikaDetectLanguagePerfTest.java 

// java -cp /lucene/tika.clean/tika-app/target/tika-app-1.0-SNAPSHOT.jar:/lucene/util/langdetect TikaDetectLanguagePerfTest /lucene/util/langdetect/europarl.17.test 

public class TikaDetectLanguagePerfTest {
    public static void main(String[] args) throws Exception {
        List<String> testData = new ArrayList<String>();
        BufferedReader is = new BufferedReader(new InputStreamReader(new FileInputStream(args[0]), "utf-8"));
        int totBytes = 0;
        while (true) {
            String line = is.readLine();
            if (line == null) {
                break;
            }
            line = line.trim();
            int idx = line.indexOf('\t');
            if (idx <= 0) continue;
            String text = line.substring(idx + 1);
            testData.add(text);
            totBytes += text.length();
        }
        is.close();

        long best = -1;
        for(int iter=0;iter<10;iter++) {
            System.out.println("iter " + iter);
            List<String> answers = new ArrayList<String>();
            long t0 = System.currentTimeMillis();
            for(String test : testData) {
                final LanguageIdentifier lid = new LanguageIdentifier(test);
                answers.add(lid.getLanguage());
            }
            long t = System.currentTimeMillis() - t0;
            System.out.println("  " + t + " msec");
            if (best == -1 || t < best) {
                best = t;
                System.out.println("  **");
            }
        }
        System.out.println("Best " + best + " msec; totBytes=" + totBytes + " MB/sec=" + (totBytes/1024./1024./(best/1000.)));
    }
}