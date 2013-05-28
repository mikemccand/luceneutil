import java.io.*;
import java.util.*;
import com.carrotsearch.labs.langid.LangIdV3;

// javac -cp dawid LangIDJavaPerfTest.java

// java -cp .:dawid LangIDJavaPerfTest europarl.21.test 

public class LangIDJavaPerfTest {
    private static final double DEFAULT_ALPHA = 0.5;

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

        LangIdV3 langid = new LangIdV3();

        long best = -1;
        for(int iter=0;iter<10;iter++) {
            System.out.println("iter " + iter);
            List<String> answers = new ArrayList<String>();
            long t0 = System.currentTimeMillis();
            for(String test : testData) {
              answers.add(langid.classify(test, true).getLangCode());
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
