import java.io.*;
import org.apache.tika.language.LanguageIdentifier;

// javac -cp /lucene/tika.clean/tika-app/target/tika-app-1.0-SNAPSHOT.jar /lucene/util/langdetect/TikaDetectLanguageEmbedded.java 

// evalEuroparl.py runs this and sends tests / gets results
// over pipes:
public class TikaDetectLanguageEmbedded {
    public static void main(String[] args) throws Exception {

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
            final LanguageIdentifier lid = new LanguageIdentifier(new String(bytes, "UTF-8"));
            final byte[] result = (lid.getLanguage() + " " + lid.isReasonablyCertain()).getBytes("UTF-8");
            os.write(String.format("%7d", result.length).getBytes("UTF-8"));
            os.write(result);
            os.flush();
        }
        is.close();
        os.close();
    }
}