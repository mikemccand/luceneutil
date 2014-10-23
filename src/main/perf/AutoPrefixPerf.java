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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.apache.lucene.codecs.lucene410.Lucene410Codec;
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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
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
    Path indexPath = Paths.get(args[2]);

    int precStep = Integer.parseInt(args[3]);
    boolean useNumericField = (precStep != 0);
    int maxTermsInPrefix;
    int minTermsInPrefix;
    if (useNumericField == false) {
      minTermsInPrefix = Integer.parseInt(args[4]);
      maxTermsInPrefix = Integer.parseInt(args[5]);
    } else {
      minTermsInPrefix = 0;
      maxTermsInPrefix = 0;
    }

    BytesRefBuilder binaryToken = new BytesRefBuilder();
    binaryToken.grow(8);
    binaryToken.setLength(8);

    Directory dir = FSDirectory.open(indexPath);
    if (Files.notExists(indexPath) == false) {
      IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
      iwc.setMaxBufferedDocs(30000);
      iwc.setRAMBufferSizeMB(-1);
      iwc.setMergePolicy(new LogDocMergePolicy());

      final PostingsFormat pf;

      if (useNumericField) {
        // Disable auto-prefix when testing NumericField!
        if (minTermsInPrefix != 0) {
          throw new IllegalArgumentException("only precStep or minTermsInPrefix should be non-zero");
        }
        pf = new Lucene41PostingsFormat(25, 48, 0, 0);
      } else {
        /*
        if (minTermsInPrefix == 0) {
          throw new IllegalArgumentException("one of precStep or minTermsInPrefix must be non-zero");
        }
        */
        pf = new Lucene41PostingsFormat(25, 48, minTermsInPrefix, maxTermsInPrefix);
        //pf = new Lucene41PostingsFormat(25, 48, minTermsInPrefix, Integer.MAX_VALUE);
      }

      iwc.setCodec(new Lucene410Codec() {
          @Override
          public PostingsFormat getPostingsFormatForField(String field) {
            return pf;
          }
        });

      iwc.setInfoStream(new PrintStreamInfoStream(System.out));
      iwc.setMergeScheduler(new SerialMergeScheduler());

      //TieredMergePolicy tmp = (TieredMergePolicy) iwc.getMergePolicy();
      //tmp.setFloorSegmentMB(.1);
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
        longFieldType.setIndexRanges(true);
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
          //NumericUtils.longToPrefixCoded(v, 0, binaryToken);
          longToBytes(v, binaryToken);
          //if (bytesToLong(binaryToken.get()) != v) {
          //  throw new RuntimeException("wrong long: v=" + v + " vs " + bytesToLong(binaryToken.get()));
          //}
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

      // nocommit just to make debugging easier:
      //System.out.println("Optimize...");
      //w.forceMerge(1);

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

      SegmentInfos infos = new SegmentInfos();
      infos.read(dir);

      long totBytes = 0;
      for(SegmentCommitInfo info : infos) {
        totBytes += info.sizeInBytes();
      }
      System.out.println("\nTotal index size: " + totBytes + " bytes");
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
        longToBytes(minValue, binaryToken);
        BytesRef minTerm = binaryToken.toBytesRef();
        longToBytes(maxValue, binaryToken);
        BytesRef maxTerm = binaryToken.toBytesRef();
        queries.add(new TermRangeQuery("number", minTerm, maxTerm, true, true));
      }

      if (queries.size() == 200) {
        break;
      }
    }

    DirectoryReader r = DirectoryReader.open(dir);
    IndexSearcher s = new IndexSearcher(r);

    printQueryTerms((MultiTermQuery) queries.get(0), s);

    long bestMS = Long.MAX_VALUE;
    for(int iter=0;iter<10;iter++) {
      long startMS = System.currentTimeMillis();
      long totalHits = 0;
      long hash = 0;
      for(Query query : queries) {
        TopDocs hits = s.search(query, 10);
        totalHits += hits.totalHits;
        hash = hash * 31 + hits.totalHits; 
      }
      long ms = System.currentTimeMillis() - startMS;
      System.out.println("iter " + iter + ": " + ms + " msec; totalHits=" + totalHits + " hash=" + hash);
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

  private static void longToBytes(long v, BytesRefBuilder token) {
    // Like NumericUtils.longToPrefixCodedBytes, but without the
    // prefix byte, and fully binary encoding (not 7 bit clean):
    long sortableBits = v ^ 0x8000000000000000L;
    int index = 7;
    while (index >= 0) {
      token.setByteAt(index, (byte) (sortableBits & 0xffL));
      index--;
      sortableBits >>>= 8;
    }
  }

  private static long bytesToLong(BytesRef term) {
    long v = 0;
    for(int i=0;i<8;i++) {
      v <<= 8;
      v |= (term.bytes[term.offset+i] & 0xffL);
    }
    return v ^ 0x8000000000000000L;
  }

  private static void printQueryTerms(final MultiTermQuery mtq, final IndexSearcher searcher) throws IOException {
    final AtomicInteger termCount = new AtomicInteger();
    final AtomicInteger docCount = new AtomicInteger();
    // TODO: is there an easier way to see terms an MTQ matches?  this is awkward
    MultiTermQuery.RewriteMethod rewriter = mtq.getRewriteMethod();
    if (mtq instanceof TermRangeQuery) {
      TermRangeQuery trq = (TermRangeQuery) mtq;
      BytesRef lowerTerm = trq.getLowerTerm();
      BytesRef upperTerm = trq.getUpperTerm();
      System.out.println("query: " +
                         bytesToLong(lowerTerm) + " " + lowerTerm +
                         " - " +
                         bytesToLong(upperTerm) + " " + upperTerm);
    } else {
      System.out.println("query: " + mtq);
    }
    System.out.println("  query matches " + searcher.search(mtq, 1).totalHits + " docs");
    mtq.setRewriteMethod(new MultiTermQuery.RewriteMethod() {
        @Override
        public Query rewrite(IndexReader reader, MultiTermQuery query) throws IOException {
          for(AtomicReaderContext ctx : searcher.getIndexReader().leaves()) {
            TermsEnum termsEnum = getTermsEnum(mtq, ctx.reader().fields().terms(mtq.getField()), null);
            System.out.println("  reader=" + ctx.reader());
            BytesRef term;
            while ((term = termsEnum.next()) != null) {
              System.out.println("    term: len=" + term.length + " " + term + " dF=" + termsEnum.docFreq());
              termCount.incrementAndGet();
              docCount.addAndGet(termsEnum.docFreq());
            }
          }

          return null;
        }
      });
    mtq.rewrite(searcher.getIndexReader());
    System.out.println("  total terms: " + termCount);
    System.out.println("  total docs: " + docCount);
    mtq.setRewriteMethod(rewriter);
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
