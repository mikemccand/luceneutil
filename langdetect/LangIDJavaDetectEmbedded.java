import java.io.*;
import com.carrotsearch.labs.langid.LangIdV3;

// javac -cp dawid LangIDJavaDetectEmbedded.java

public class LangIDJavaDetectEmbedded {

  public static void main(String[] args) throws Exception {

    LangIdV3 langid = new LangIdV3();


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
      byte[] result = langid.classify(new String(bytes, "UTF-8"), true).getLangCode().getBytes("UTF-8");
      os.write(String.format("%7d", result.length).getBytes("UTF-8"));
      os.write(result);
      os.flush();
    }
    is.close();
    os.close();
  }
}