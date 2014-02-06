import java.io.*;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.Language;
import java.util.*;

// javac -cp /usr/local/src/langdetect-09-13-2011/lib/langdetect.jar LDDetectLanguagePerfTest.java 

// java -cp /usr/local/src/langdetect-09-13-2011/lib/langdetect.jar:/usr/local/src/langdetect-09-13-2011/lib/jsonic-1.2.0.jar:. LDDetectLanguagePerfTest europarl.21.test 

public class LDDetectLanguagePerfTest {
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

        DetectorFactory.loadProfile("/usr/local/src/langdetect-09-13-2011/profiles");
        DetectorFactory.setSeed(0);

        long best = -1;
        for(int iter=0;iter<10;iter++) {
            System.out.println("iter " + iter);
            List<String> answers = new ArrayList<String>();
            long t0 = System.currentTimeMillis();
            for(String test : testData) {
                final Detector d = DetectorFactory.create(DEFAULT_ALPHA);
                d.append(test);
                String answer = d.detect();
                answers.add(d.detect());
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