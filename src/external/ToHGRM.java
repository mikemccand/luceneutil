import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.HdrHistogram.Histogram;

// javac -cp lib/HdrHistogram.jar ToHGRM.java

public class ToHGRM {

  public static void main(String[] args) throws Exception {
    double maxSec = Double.parseDouble(args[0]);
    
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    Histogram h = new Histogram((long) (maxSec * 1000), 3);
    while(true) {
      String line = br.readLine();
      if (line == null) {
        break;
      }
      h.recordValue((long) (1000*Double.parseDouble(line.trim())));
    }

    h.getHistogramData().outputPercentileDistribution(System.out, 50, 1.0);
  }
}
