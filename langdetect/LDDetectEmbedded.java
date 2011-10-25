import java.io.*;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.Language;

// javac -cp /home/mike/src/langdetect/lib/langdetect.jar /lucene/util/langdetect/LDDetectEmbedded.java 

public class LDDetectEmbedded {
    private static final double DEFAULT_ALPHA = 0.5;

    public static void main(String[] args) throws Exception {

        DetectorFactory.loadProfile("/home/mike/src/langdetect/profiles");
        DetectorFactory.setSeed(0);

        final InputStream is = new BufferedInputStream(System.in);
        final OutputStream os = new BufferedOutputStream(System.out);
        final byte[] num = new byte[7];
        while(true) {
            if (is.read(num) != 7) {
                break;
            }
            final int byteCount = Integer.parseInt(new String(num, "utf8").trim());
            byte[] bytes = new byte[byteCount];
            if (is.read(bytes) != byteCount) {
                break;
            }
            final Detector d = DetectorFactory.create(DEFAULT_ALPHA);
            d.append(new String(bytes, "UTF-8"));
            String answer = d.detect();
            final byte[] result = answer.getBytes("UTF-8");
            os.write(String.format("%7d", result.length).getBytes("UTF-8"));
            os.write(result);
            os.flush();
        }
        is.close();
        os.close();
    }
}