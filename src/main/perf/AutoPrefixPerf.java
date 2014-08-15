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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsFormat;
import org.apache.lucene.codecs.lucene49.Lucene49Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.Version;

// rm -rf /l/indices/numbers; pushd core; ant jar; popd; javac -d /l/util/build -cp build/core/classes/java:build/analysis/common/classes/java /l/util/src/main/perf/AutoPrefixPerf.java

// java -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.AutoPrefixPerf /lucenedata/numbers/randlongs.10m.txt /lucenedata/numbers/randlongs.queries.txt /l/indices/numbers 8 0

public class AutoPrefixPerf {

  final static boolean normal = false;

  public static void main(String[] args) throws Exception {
    String numbersFile = args[0];
    String queriesFile = args[1];
    File indexPath = new File(args[2]);

    int precStep = Integer.parseInt(args[3]);
    boolean useNumericField = (precStep != 0);

    BytesRefBuilder binaryToken = new BytesRefBuilder();

    int minTermsInPrefix = Integer.parseInt(args[4]);
    Directory dir = FSDirectory.open(indexPath);
    if (indexPath.exists() == false) {
      IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
      iwc.setMaxBufferedDocs(30000);
      iwc.setRAMBufferSizeMB(-1);

      final PostingsFormat pf;

      if (useNumericField) {
        // Disable auto-prefix when testing NumericField!
        if (minTermsInPrefix != 0) {
          throw new IllegalArgumentException("only precStep or minTermsInPrefix should be non-zero");
        }
        pf = new Lucene41PostingsFormat(25, 48, 0, 0);
      } else {
        if (minTermsInPrefix == 0) {
          throw new IllegalArgumentException("one of precStep or minTermsInPrefix must be non-zero");
        }
        pf = new Lucene41PostingsFormat(25, 48, minTermsInPrefix, (minTermsInPrefix-1)*2);
      }

      iwc.setCodec(new Lucene49Codec() {
          @Override
          public PostingsFormat getPostingsFormatForField(String field) {
            return pf;
          }
        });

      iwc.setInfoStream(new PrintStreamInfoStream(System.out));
      iwc.setMergeScheduler(new SerialMergeScheduler());

      TieredMergePolicy tmp = (TieredMergePolicy) iwc.getMergePolicy();
      tmp.setFloorSegmentMB(.1);
      //ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler) iwc.getMergeScheduler();
      // More concurrency (for SSD)
      //cms.setMaxMergesAndThreads(5, 3);
      final IndexWriter w = new IndexWriter(dir, iwc);

      Document doc = new Document();
      Field field;
      if (useNumericField) {
        FieldType longFieldType = new FieldType(LongField.TYPE_NOT_STORED);
        longFieldType.setNumericPrecisionStep(precStep);
        longFieldType.freeze();
        field = new LongField("number", 0L, longFieldType);
        doc.add(field);
      } else {
        FieldType longFieldType = new FieldType(TextField.TYPE_NOT_STORED);
        longFieldType.setIndexOptions(IndexOptions.DOCS_ONLY);
        longFieldType.setOmitNorms(true);
        longFieldType.freeze();
        field = new Field("number", new BinaryTokenStream(binaryToken.get()), longFieldType);
        doc.add(field);
      }

      long startMS = System.currentTimeMillis();

      // 64K buffer:
      InputStream is = new FileInputStream(numbersFile);
      BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"), 1 << 16);

      int count = 0;
      while (true) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        Long v = Long.parseLong(line.trim());
        if (useNumericField) {
          field.setLongValue(v);
        } else {
          NumericUtils.longToPrefixCoded(v, 0, binaryToken);
        }
        w.addDocument(doc);
        count++;
        if (count % 200000 == 0) {
          long ms = System.currentTimeMillis();
          System.out.println("Indexed " + count + ": " + ((ms - startMS)/1000.0) + " sec");
        }
      }
      reader.close();

      System.out.println("Final Indexed " + count + ": " + ((System.currentTimeMillis() - startMS)/1000.0) + " sec");
      System.out.println("Close...");
      w.close();
      System.out.println("After close: " + ((System.currentTimeMillis() - startMS)/1000.0) + " sec");

      // Print CheckIndex:
      ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
      CheckIndex checker = new CheckIndex(dir);
      checker.setInfoStream(new PrintStream(bos, false, IOUtils.UTF_8), true);
      CheckIndex.Status status = checker.checkIndex();
      System.out.println("Done CheckIndex:");
      System.out.println(bos.toString(IOUtils.UTF_8));
      if (status.clean == false) {
        throw new IllegalStateException("CheckIndex failed");
      }
    } else {
      System.out.println("Skip indexing: index already exists");
    }

    List<Query> queries = new ArrayList<>();
    InputStream is = new FileInputStream(queriesFile);
    BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"), 1 << 16);
    while (true) {
      String line = reader.readLine();
      if (line == null) {
        break;
      }
      String[] numbers = line.trim().split(" ");
      if (numbers.length != 2) {
        throw new IllegalArgumentException("could not parse query line: " + line);
      }
      long minValue = Long.parseLong(numbers[0]);
      long maxValue = Long.parseLong(numbers[1]);
      if (useNumericField) {
        queries.add(NumericRangeQuery.newLongRange("number", precStep,
                                                   minValue, maxValue,
                                                   true, true));
      } else {
        NumericUtils.longToPrefixCoded(minValue, 0, binaryToken);
        BytesRef minTerm = binaryToken.toBytesRef();
        NumericUtils.longToPrefixCoded(maxValue, 0, binaryToken);
        BytesRef maxTerm = binaryToken.toBytesRef();
        queries.add(new TermRangeQuery("number", minTerm, maxTerm, true, true));
      }

      if (queries.size() == 200) {
        break;
      }
    }

    DirectoryReader r = DirectoryReader.open(dir);
    IndexSearcher s = new IndexSearcher(r);
    long bestMS = Long.MAX_VALUE;
    for(int iter=0;iter<10;iter++) {
      long startMS = System.currentTimeMillis();
      long totalHits = 0;
      for(Query query : queries) {
        totalHits += s.search(query, 10).totalHits;
      }
      long ms = System.currentTimeMillis() - startMS;
      System.out.println("iter " + iter + ": " + ms + " msec; totalHits=" + totalHits);
      if (ms < bestMS) {
        System.out.println("  **");
        bestMS = ms;
      }
    }
    /*
    long t0 = System.currentTimeMillis();
    long bytesUsed = 0;
    for(int i=0;i<1000;i++) {
      for(AtomicReaderContext ctx : r.leaves()) {
        bytesUsed += ((SegmentReader) ctx.reader()).ramBytesUsed();
      }
    }
    System.out.println((System.currentTimeMillis() - t0) + " msec for 1000 ramBytesUsed: " + (bytesUsed / 1000));
    */

    r.close();
    dir.close();
  }

  // TODO: do we need a BinaryField?  This is too hard...:
  static final class BinaryTokenStream extends TokenStream {
    private final ByteTermAttribute bytesAtt = addAttribute(ByteTermAttribute.class);
    private boolean available = true;
  
    public BinaryTokenStream(BytesRef bytes) {
      bytesAtt.setBytesRef(bytes);
    }
  
    @Override
    public boolean incrementToken() {
      if (available) {
        clearAttributes();
        available = false;
        return true;
      }
      return false;
    }
  
    @Override
    public void reset() {
      available = true;
    }
  
    public interface ByteTermAttribute extends TermToBytesRefAttribute {
      public void setBytesRef(BytesRef bytes);
    }
  
    public static class ByteTermAttributeImpl extends AttributeImpl implements ByteTermAttribute,TermToBytesRefAttribute {
      private BytesRef bytes;
    
      @Override
      public void fillBytesRef() {
        // no-op: the bytes was already filled by our owner's incrementToken
      }
    
      @Override
      public BytesRef getBytesRef() {
        return bytes;
      }

      @Override
      public void setBytesRef(BytesRef bytes) {
        this.bytes = bytes;
      }
    
      @Override
      public void clear() {}
    
      @Override
      public void copyTo(AttributeImpl target) {
        ByteTermAttributeImpl other = (ByteTermAttributeImpl) target;
        other.bytes = bytes;
      }
    }
  }
}


