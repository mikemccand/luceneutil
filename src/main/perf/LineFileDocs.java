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

// FIELDS_HEADER_INDICATOR###   title   timestamp   text    username    characterCount  categories  imageCount  sectionCount    subSectionCount subSubSectionCount  refCount

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DateFormatSymbols;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;

public class LineFileDocs implements Closeable {

  public final static String VECTOR_FIELD_NAME = "vector";

  // sentinel:
  private final static LineFileDoc END = new LineFileDoc.TextBased("END", null, -1);

  private BufferedReader reader;
  private SeekableByteChannel channel;
  private final static int BUFFER_SIZE = 1 << 16;     // 64K
  private final boolean doRepeat;
  private final String path;
  private final boolean storeBody;
  private final boolean tvsBody;
  private final boolean bodyPostingsOffsets;
  private final AtomicLong bytesIndexed = new AtomicLong();
  private final boolean doClone;
  private final TaxonomyWriter taxoWriter;
  // maps field name to 1 (taxonomy) | 2 (sorted set)
  private final Map<String,Integer> facetFields;
  private final FacetsConfig facetsConfig;
  private String[] extraFacetFields;
  private final boolean addDVFields;
  private final BlockingQueue<LineFileDoc> queue = new ArrayBlockingQueue<>(1024);
  private final BlockingQueue<LineFileDoc> recycleBin = new ArrayBlockingQueue<>(1024);
  private final Thread readerThread;
  final boolean isBinary;
  private final ThreadLocal<LineFileDoc> nextDocs = new ThreadLocal<>();
  private final String[] months = DateFormatSymbols.getInstance(Locale.ROOT).getMonths();
  private final String vectorFile;
  private final int vectorDimension;
  private final VectorEncoding vectorEncoding;
  private SeekableByteChannel vectorChannel;

  public LineFileDocs(String path, boolean doRepeat, boolean storeBody, boolean tvsBody, boolean bodyPostingsOffsets,
                      boolean doClone, TaxonomyWriter taxoWriter, Map<String,Integer> facetFields,
                      FacetsConfig facetsConfig, boolean addDVFields, String vectorFile, int vectorDimension,
                      VectorEncoding vectorEncoding)
    throws IOException {
    this.path = path;
    this.isBinary = path.endsWith(".bin");
    this.storeBody = storeBody;
    this.tvsBody = tvsBody;
    this.bodyPostingsOffsets = bodyPostingsOffsets;
    this.doClone = doClone;
    this.doRepeat = doRepeat;
    this.taxoWriter = taxoWriter;
    this.facetFields = facetFields;
    this.facetsConfig = facetsConfig;
    this.addDVFields = addDVFields;
    this.vectorFile = vectorFile;
    this.vectorDimension = vectorDimension;
    this.vectorEncoding = vectorEncoding;

    open();
    readerThread = new Thread() {
        @Override
        public void run() {
          try {
            readDocs();
          } catch (Throwable t) {
            throw new RuntimeException(t);
          }
        }
      };
    readerThread.setName("LineFileDocs reader");
    readerThread.setDaemon(true);
    readerThread.start();
  }

  private void readDocs() throws Exception {
    if (isBinary) {
      byte[] headerBytes = new byte[8];
      ByteBuffer header = ByteBuffer.wrap(headerBytes);
      header.order(ByteOrder.LITTLE_ENDIAN);
      int totalDocCount = 0;
      while (true) {
        header.position(0);
        int x = channel.read(header);
        if (x == -1) {
          if (doRepeat) {
            close();
            open();
            x = channel.read(header);
          } else {
            break;
          }
        }

        if (x != 8) {
          throw new RuntimeException("expected 8 header bytes but read " + x);
        }
        int docCountInBlock = header.getInt(0);
        int length = header.getInt(4);
        //System.out.println("count= " + count + " len=" + length);
        ByteBuffer buffer = ByteBuffer.wrap(new byte[length]);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        x = channel.read(buffer);
        if (x != length) {
          throw new RuntimeException("expected " + length + " document bytes but read " + x);
        }
        buffer.position(0);
        queue.put(new LineFileDoc.BinaryBased(buffer, readVector(docCountInBlock), totalDocCount, docCountInBlock));
        totalDocCount += docCountInBlock;
      }
    } else {
      // This is a txt based line file doc
      int id = 0;
      while (true) {
        String line = reader.readLine();
        if (line == null) {
          if (doRepeat) {
            close();
            open();
            line = reader.readLine();
          } else {
            break;
          }
        }
        queue.put(new LineFileDoc.TextBased(line, readVector(1), id++));
      }
    }
    for(int i=0;i<128;i++) {
      queue.put(END);
    }
  }

  private float[] readVector(int count) throws IOException {
    if (vectorChannel == null) {
      return null;
    }
    float[] vector = new float[count * vectorDimension];
    ByteBuffer buffer = ByteBuffer.allocate(count * vectorDimension * Float.BYTES)
      .order(ByteOrder.LITTLE_ENDIAN);
    int n = vectorChannel.read(buffer);
    if (n != count * vectorDimension * Float.BYTES) {
      throw new RuntimeException("expected " + count * vectorDimension * Float.BYTES + " vector bytes (count=" + count + " vectorDimension=" + vectorDimension + ") but read " + n);
    }
    buffer.position(0);
    buffer.asFloatBuffer().get(vector);
    return vector;
  }

  public long getBytesIndexed() {
    return bytesIndexed.get();
  }

  private void open() throws IOException {
    if (isBinary) {
      channel = Files.newByteChannel(Paths.get(path), StandardOpenOption.READ);
    } else {
      InputStream is = new FileInputStream(path);
      reader = new BufferedReader(new InputStreamReader(is, "UTF-8"), BUFFER_SIZE);
      String firstLine = reader.readLine();
      if (firstLine.startsWith("FIELDS_HEADER_INDICATOR")) {
        int defaultFieldLength = 4;
        if (firstLine.startsWith("FIELDS_HEADER_INDICATOR###\tdoctitle\tdocdate\tbody") == false &&
            firstLine.startsWith("FIELDS_HEADER_INDICATOR###\ttitle\ttimestamp\ttext") == false &&
            firstLine.startsWith("FIELD_HEADER_INDICATOR###\tdoctitle\tdocdate\tbody\tRandomLabel") == false) {
          throw new IllegalArgumentException("unrecognized header in line docs file: " + firstLine.trim());
        }
        if (firstLine.startsWith("FIELDS_HEADER_INDICATOR###\tdoctitle\tdocdate\tbody\tRandomLabel")) {
          defaultFieldLength = 5;
        }
        if (facetFields.isEmpty() == false) {
          String[] fields = firstLine.split("\t");
          if (fields.length > defaultFieldLength) {
            extraFacetFields = Arrays.copyOfRange(fields, defaultFieldLength, fields.length);
            System.out.println("Additional facet fields: " + Arrays.toString(extraFacetFields));

            List<String> extraFacetFieldsList = Arrays.asList(extraFacetFields);

            // Verify facet fields now:
            for(String field : facetFields.keySet()) {
              if (field.equals("Date") == false && field.equals("Month") == false && field.equals("DayOfYear") == false
                      && field.equals("RandomLabel") == false && extraFacetFieldsList.contains(field) == false) {
                throw new IllegalArgumentException("facet field \"" + field + "\" is not recognized");
              }
            }
          } else {
            // Verify facet fields now:
            for(String field : facetFields.keySet()) {
              if (field.equals("Date") == false && field.equals("Month") == false && field.equals("DayOfYear") == false
                      && field.equals("RandomLabel") == false) {
                throw new IllegalArgumentException("facet field \"" + field + "\" is not recognized");
              }
            }
          }
        }
        // Skip header
      } else {
        // Old format: no header
        reader.close();
        is = new FileInputStream(path);
        reader = new BufferedReader(new InputStreamReader(is, "UTF-8"), BUFFER_SIZE);
      }
    }
    if (vectorFile != null) {
      vectorChannel = Files.newByteChannel(Paths.get(vectorFile), StandardOpenOption.READ);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
    if (vectorChannel != null) {
      vectorChannel.close();
      vectorChannel = null;
    }
  }

  public static String intToID(int id) {
    // Base 36, prefixed with 0s to be length 6 (= 2.2 B)
    final String s = String.format("%6s", Integer.toString(id, Character.MAX_RADIX)).replace(' ', '0');
    //System.out.println("fromint: " + id + " -> " + s);
    return s;
  }

  public static int idToInt(BytesRef id) {
    // Decode base 36
    int accum = 0;
    int downTo = id.length + id.offset - 1;
    int multiplier = 1;
    while(downTo >= id.offset) {
      final char ch = (char) (id.bytes[downTo--]&0xff);
      final int digit;
      if (ch >= '0' && ch <= '9') {
        digit = ch - '0';
      } else if (ch >= 'a' && ch <= 'z') {
        digit = 10 + (ch-'a');
      } else {
        assert false;
        digit = -1;
      }
      accum += multiplier * digit;
      multiplier *= 36;
    }

    //System.out.println("toint: " + id.utf8ToString() + " -> " + accum);
    return accum;
  }

  public static int idToInt(String id) {
    // Decode base 36
    int accum = 0;
    int downTo = id.length() - 1;
    int multiplier = 1;
    while(downTo >= 0) {
      final char ch = id.charAt(downTo--);
      final int digit;
      if (ch >= '0' && ch <= '9') {
        digit = ch - '0';
      } else if (ch >= 'a' && ch <= 'z') {
        digit = 10 + (ch-'a');
      } else {
        assert false;
        digit = -1;
      }
      accum += multiplier * digit;
      multiplier *= 36;
    }

    //System.out.println("toint: " + id + " -> " + accum);
    return accum;
  }

  private final static char SEP = '\t';

  public static final class DocState {
    final Document doc;
    final Field titleTokenized;
    final Field title;
    final Field month;
    final IntField dayOfYear;
    final BinaryDocValuesField titleBDV;
    final LongField lastMod;
    final Field body;
    final Field id;
    final Field idPoint;
    final Field idDV;
    final Field date;
    final Field randomLabel;

    //final NumericDocValuesField dateMSec;
    //final LongField rand;
    final Field timeSec;
    // Necessary for "old style" wiki line files:
    final SimpleDateFormat dateParser = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss", Locale.US);
    final KnnFloatVectorField floatVectorField;
    final KnnByteVectorField byteVectorField;

    // For just y/m/day:
    //final SimpleDateFormat dateParser = new SimpleDateFormat("y/M/d", Locale.US);

    //final SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US);
    final Calendar dateCal = Calendar.getInstance();
    final ParsePosition datePos = new ParsePosition(0);

    DocState(boolean storeBody, boolean tvsBody, boolean bodyPostingsOffsets, boolean addDVFields, int vectorDimension, VectorEncoding vectorEncoding) {
      doc = new Document();

      if (addDVFields == false) {
        title = new StringField("title", "", Field.Store.NO);
      } else {
        title = new KeywordField("title", "", Field.Store.NO);
      }
      doc.add(title);

      if (addDVFields) {
        titleBDV = new BinaryDocValuesField("titleBDV", new BytesRef());
        doc.add(titleBDV);

        lastMod = new LongField("lastMod", -1, Field.Store.NO);
        doc.add(lastMod);

        month = new KeywordField("month", "", Store.NO);
        doc.add(month);

        dayOfYear = new IntField("dayOfYear", 0, Field.Store.NO);
        doc.add(dayOfYear);

        idDV = new NumericDocValuesField("id", 0);
        doc.add(idDV);
      } else {
        titleBDV = null;
        lastMod = null;
        month = null;
        dayOfYear = null;
        idDV = null;
      }

      titleTokenized = new TextField("titleTokenized", "", Field.Store.YES);
      doc.add(titleTokenized);

      FieldType bodyFieldType = new FieldType(TextField.TYPE_NOT_STORED);
      if (storeBody) {
        bodyFieldType.setStored(true);
      }

      if (tvsBody) {
        bodyFieldType.setStoreTermVectors(true);
        bodyFieldType.setStoreTermVectorOffsets(true);
        bodyFieldType.setStoreTermVectorPositions(true);
      }

      if (bodyPostingsOffsets) {
        bodyFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
      }

      body = new Field("body", "", bodyFieldType);
      doc.add(body);

      randomLabel = new StringField("randomLabel", "", Field.Store.NO);
      doc.add(body);

      id = new StringField("id", "", Field.Store.YES);
      doc.add(id);

      idPoint = new IntPoint("id", 0);
      doc.add(idPoint);

      date = new StringField("date", "", Field.Store.YES);
      doc.add(date);

      //dateMSec = new NumericDocValuesField("datenum", 0L);
      //doc.add(dateMSec);

      //rand = new LongField("rand", 0L, Field.Store.NO);
      //doc.add(rand);

      timeSec = new IntPoint("timesecnum", 0);
      doc.add(timeSec);

      if (vectorDimension > 0) {
        if (vectorEncoding == VectorEncoding.FLOAT32) {
          floatVectorField = new KnnFloatVectorField(VECTOR_FIELD_NAME, new float[vectorDimension], VectorSimilarityFunction.DOT_PRODUCT);
          doc.add(floatVectorField);
          byteVectorField = null;
        } else {
          byteVectorField = new KnnByteVectorField(VECTOR_FIELD_NAME, new byte[vectorDimension], VectorSimilarityFunction.DOT_PRODUCT);
          doc.add(byteVectorField);
          floatVectorField = null;
        }
      } else {
        floatVectorField = null;
        byteVectorField = null;
      }
    }
  }

  public DocState newDocState() {
    return new DocState(storeBody, tvsBody, bodyPostingsOffsets, addDVFields, vectorDimension, vectorEncoding);
  }

  // TODO: is there a pre-existing way to do this!!!
  static Document cloneDoc(Document doc1) {
    final Document doc2 = new Document();

    for(IndexableField f0 : doc1.getFields()) {
      Field f = (Field) f0;
      if (f instanceof StringField) {
        doc2.add(new StringField(f.name(), f.stringValue(), f.fieldType().stored() ? Field.Store.YES : Field.Store.NO));
      } else if (f instanceof KeywordField) {
        doc2.add(new KeywordField(f.name(), f.stringValue(), f.fieldType().stored() ? Field.Store.YES : Field.Store.NO));
      } else if (f instanceof TextField) {
        doc2.add(new TextField(f.name(), f.stringValue(), f.fieldType().stored() ? Field.Store.YES : Field.Store.NO));
      } else if (f instanceof IntField) {
        doc2.add(new IntField(f.name(), ((IntField) f).numericValue().intValue(), Field.Store.NO));
      } else if (f instanceof LongField) {
        doc2.add(new LongField(f.name(), ((LongField) f).numericValue().longValue(), Field.Store.NO));
      } else if (f instanceof LongPoint) {
        doc2.add(new LongPoint(f.name(), ((LongPoint) f).numericValue().longValue()));
      } else if (f instanceof IntPoint) {
        doc2.add(new IntPoint(f.name(), ((IntPoint) f).numericValue().intValue()));
      } else if (f instanceof SortedDocValuesField) {
        doc2.add(new SortedDocValuesField(f.name(), f.binaryValue()));
      } else if (f instanceof NumericDocValuesField) {
        doc2.add(new NumericDocValuesField(f.name(), f.numericValue().longValue()));
      } else if (f instanceof BinaryDocValuesField) {
        doc2.add(new BinaryDocValuesField(f.name(), f.binaryValue()));
      } else if (f instanceof KnnFloatVectorField) {
        KnnFloatVectorField knnf = ((KnnFloatVectorField) f);
        doc2.add(new KnnFloatVectorField(f.name(), knnf.vectorValue(), f.fieldType().vectorSimilarityFunction()));
      } else {
        Field field2 = new Field(f.name(),
                                 f.stringValue(),
                                 f.fieldType());
        doc2.add(field2);
      }
    }

    return doc2;
  }

  /* Call this function to put the remaining doc block into recycle queue */
  public void recycle() {
    if (isBinary && nextDocs.get() != null && nextDocs.get().getBlockByteText().hasRemaining()) {
      try {
        recycleBin.put(nextDocs.get());
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      }
      nextDocs.set(null);
    }
  }

  /* Call this function to make sure the calling thread will have something to index */
  public boolean reserve() {
    if (isBinary == false) {
      return true; // don't need to reserve anything with text based LFD
    }
    LineFileDoc lfd = nextDocs.get();
    if (lfd != null && lfd.getBlockByteText().hasRemaining()) {
      return true; // we have next document
    }
    try {
      lfd = queue.take();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    }
    if (lfd == END) {
      return false;
    }
    nextDocs.set(lfd);
    return true;
  }

  public Document nextDoc(DocState doc) throws IOException {
    return nextDoc(doc, false);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public Document nextDoc(DocState doc, boolean expected) throws IOException {

    long msecSinceEpoch;
    int timeSec;
    int spot4;
    String line;
    String title;
    String body;
    String randomLabel;
    int myID = -1;

    if (isBinary) {

      float[] vector = new float[vectorDimension];

      LineFileDoc lfd;

      // reserve() is okay to be called multiple times
      if (reserve() == false) {
        if (expected == false) {
          return null;
        } else {
          // the caller expects there are more documents, we will be blocking on recycleBin for 10 seconds
          try {
            lfd = recycleBin.poll(10, TimeUnit.SECONDS);
            if (lfd == null) {
              throw new IllegalStateException("Expected docs in recycleBin but not found anything");
            }
            nextDocs.set(lfd);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
          }
        }
      } else {
        lfd = nextDocs.get();
      }
      assert lfd != null && lfd != END && lfd.getBlockByteText().hasRemaining();
      // buffer format described in buildBinaryLineDocs.py
      ByteBuffer buffer = lfd.getBlockByteText();
      int titleLenBytes = buffer.getInt();
      int bodyLenBytes = buffer.getInt();
      int randomLabelLenBytes = buffer.getInt();
      timeSec  = buffer.getInt();
      myID = lfd.getNextId();
      msecSinceEpoch  = buffer.getLong();
//      System.out.println("    titleLen=" + titleLenBytes + " bodyLenBytes=" + bodyLenBytes +
//              " randomLabelLenBytes=" + randomLabelLenBytes + " msecSinceEpoch=" + msecSinceEpoch + " timeSec=" + timeSec);
      byte[] bytes = buffer.array();

      char[] titleChars = new char[titleLenBytes];
      int titleLenChars = UnicodeUtil.UTF8toUTF16(bytes, buffer.position(), titleLenBytes, titleChars);
      title = new String(titleChars, 0, titleLenChars);
//      System.out.println("title: " + title);

      char[] bodyChars = new char[bodyLenBytes];
      int bodyLenChars = UnicodeUtil.UTF8toUTF16(bytes, buffer.position()+titleLenBytes, bodyLenBytes, bodyChars);
      body = new String(bodyChars, 0, bodyLenChars);
//      System.out.println("body: " + body);

      char[] randomLabelChars = new char[randomLabelLenBytes];
      int randomLabelLenChars = UnicodeUtil.UTF8toUTF16(bytes, buffer.position()+titleLenBytes+bodyLenBytes, randomLabelLenBytes, randomLabelChars);
      randomLabel = new String(randomLabelChars, 0, randomLabelLenChars);
//      System.out.println("randomLabel: " + randomLabel);

      buffer.position(buffer.position() + titleLenBytes + bodyLenBytes + randomLabelLenBytes);

      doc.dateCal.setTimeInMillis(msecSinceEpoch);

      spot4 = 0;
      line = null;

      if (lfd.vector != null) {
        if (doc.floatVectorField != null) {
          lfd.getVector(doc.floatVectorField.vectorValue());
        } else {
          lfd.getVector(doc.byteVectorField.vectorValue());
        }
      }

    } else {
      LineFileDoc lfd;
      try {
        lfd = queue.take();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      }
      if (lfd == END) {
        return null;
      }
      line = lfd.getStringText();
      myID = lfd.getNextId();

      int spot = line.indexOf(SEP);
      if (spot == -1) {
        throw new RuntimeException("line: [" + line + "] is in an invalid format !");
      }
      int spot2 = line.indexOf(SEP, 1 + spot);
      if (spot2 == -1) {
        throw new RuntimeException("line: [" + line + "] is in an invalid format !");
      }
      int spot3 = line.indexOf(SEP, 1 + spot2);
      if (spot3 == -1) {
        throw new RuntimeException("line: [" + line + "] is in an invalid format !" +
                "Your source file (enwiki-20120502-lines-1k.txt) might be out of date." +
                "Please download an updated version from home.apache.org/~mikemccand");
      }
      spot4 = line.indexOf(SEP, 1 + spot3);
      if (spot4 == -1) {
        spot4 = line.length();
      }

      body = line.substring(1+spot2, spot3);

      randomLabel = line.substring(1+spot3, spot4).strip();

      title = line.substring(0, spot);

      final String dateString = line.substring(1+spot, spot2);
      doc.date.setStringValue(dateString);
      doc.datePos.setIndex(0);
      final Date date = doc.dateParser.parse(dateString, doc.datePos);
      if (date == null) {
        System.out.println("FAILED: " + dateString);
      }
      //doc.dateMSec.setLongValue(date.getTime());

      //doc.rand.setLongValue(rand.nextInt(10000));
      //System.out.println("DATE: " + date);
      doc.dateCal.setTime(date);
      msecSinceEpoch = doc.dateCal.getTimeInMillis();
      timeSec = doc.dateCal.get(Calendar.HOUR_OF_DAY)*3600 + doc.dateCal.get(Calendar.MINUTE)*60 + doc.dateCal.get(Calendar.SECOND);
      if (doc.floatVectorField != null) {
        doc.floatVectorField.setVectorValue((float[]) lfd.vector.array());
      } else if (doc.byteVectorField != null) {
        doc.byteVectorField.setVectorValue((byte[]) lfd.vector.array());
      }
    }

    if (myID == -1) {
      throw new RuntimeException("ID not set correctly");
    }

    bytesIndexed.addAndGet(body.length() + title.length() + randomLabel.length());
    doc.body.setStringValue(body);
    doc.title.setStringValue(title);
    doc.randomLabel.setStringValue(randomLabel);
    if (addDVFields) {
      doc.titleBDV.setBytesValue(new BytesRef(title));
      final String month = months[doc.dateCal.get(Calendar.MONTH)];
      doc.month.setStringValue(month);
      doc.dayOfYear.setIntValue(doc.dateCal.get(Calendar.DAY_OF_YEAR));
      doc.idDV.setLongValue(myID);
    }
    doc.titleTokenized.setStringValue(title);
    doc.id.setStringValue(intToID(myID));
    doc.idPoint.setIntValue(myID);

    if (addDVFields) {
      doc.lastMod.setLongValue(msecSinceEpoch);
    }

    doc.timeSec.setIntValue(timeSec);

    if (facetFields.isEmpty() == false) {
      Document doc2 = cloneDoc(doc.doc);

      if (facetFields.containsKey("Date")) {
        int flag = facetFields.get("Date");
        if ((flag & 1) != 0) {
          doc2.add(new FacetField("Date.taxonomy",
                                  ""+doc.dateCal.get(Calendar.YEAR),
                                  ""+doc.dateCal.get(Calendar.MONTH),
                                  ""+doc.dateCal.get(Calendar.DAY_OF_MONTH)));
        }
        if ((flag & 2) != 0) {
          doc2.add(new SortedSetDocValuesFacetField("Date.sortedset",
                                                    ""+doc.dateCal.get(Calendar.YEAR),
                                                    ""+doc.dateCal.get(Calendar.MONTH),
                                                    ""+doc.dateCal.get(Calendar.DAY_OF_MONTH)));
        }
      }

      if (facetFields.containsKey("Month")) {
        int flag = facetFields.get("Month");
        if ((flag & 1) != 0) {
          doc2.add(new FacetField("Month.taxonomy", months[doc.dateCal.get(Calendar.MONTH)]));
        }
        if ((flag & 2) != 0) {
          doc2.add(new SortedSetDocValuesFacetField("Month.sortedset", months[doc.dateCal.get(Calendar.MONTH)]));
        }
      }

      if (facetFields.containsKey("DayOfYear")) {
        int flag = facetFields.get("DayOfYear");
        if ((flag & 1) != 0) {
          doc2.add(new FacetField("DayOfYear.taxonomy", Integer.toString(doc.dateCal.get(Calendar.DAY_OF_YEAR))));
        }
        if ((flag & 2) != 0) {
          doc2.add(new SortedSetDocValuesFacetField("DayOfYear.sortedset", Integer.toString(doc.dateCal.get(Calendar.DAY_OF_YEAR))));
        }
      }

      if (facetFields.containsKey("RandomLabel")) {
        int flag = facetFields.get("RandomLabel");
        if ((flag & 1) != 0) {
          doc2.add(new FacetField("RandomLabel.taxonomy", randomLabel));
        }
        if ((flag & 2) != 0) {
          doc2.add(new SortedSetDocValuesFacetField("RandomLabel.sortedset", randomLabel));
        }
      }

      if (extraFacetFields != null) {
        String[] extraValues = line.substring(spot4+1).split("\t");

        for(int i=0;i<extraFacetFields.length;i++) {
          String extraFieldName = extraFacetFields[i];
          if (facetFields.containsKey(extraFieldName)) {
            if (extraFieldName.equals("categories")) {
              for (String cat : extraValues[i].split("\\|")) {
                // TODO: scary how taxo writer writes a
                // second /categories ord for this case ...
                if (cat.length() == 0) {
                  continue;
                }
                doc2.add(new FacetField("categories", cat));
              }
            } else if (extraFieldName.equals("characterCount")) {

              // Make number drilldown hierarchy, so eg 1877
              // characters is under
              // 0-1M/0-100K/0-10K/1-2K/1800-1900:
              List<String> nodes = new ArrayList<String>();
              int value = Integer.parseInt(extraValues[i]);
              int accum = 0;
              int base = 1000000;
              while(base > 100) {
                int factor = (value-accum) / base;
                nodes.add(String.format("%d - %d", accum+factor*base, accum+(factor+1)*base));
                accum += factor * base;
                base /= 10;
              }
              doc2.add(new FacetField(extraFieldName, nodes.toArray(new String[nodes.size()])));
            } else {
              doc2.add(new FacetField(extraFieldName, extraValues[i]));
            }
          }
        }

        /*
        String dvFieldName = "$facets_sorted_doc_values";
        doc.doc.removeFields(dvFieldName);
        for(CategoryPath path : paths) {
          //System.out.println("ADD: " + path.toString());
          doc.doc.add(new SortedSetDocValuesField(dvFieldName, new BytesRef(path.toString(FacetIndexingParams.DEFAULT_FACET_DELIM_CHAR))));
        }
        */
      }
      return facetsConfig.build(taxoWriter, doc2);
    } else if (doClone) {
      return cloneDoc(doc.doc);
    } else {
      return doc.doc;
    }
  }

  private static abstract class LineFileDoc {

    // This vector can be vector value for one or more documents
    // more specifically, for text based LFD the vector is single valued
    // but for binary based LFD the vector contains value for all the documents
    // in the block
    private final Buffer vector;

    LineFileDoc(float[] vector) {
      if (vector == null) {
        this.vector = null;
      } else {
        this.vector = FloatBuffer.wrap(vector);
      }
    }

    /**
     * Get the id for the next document, can only be called n times, where n
     * is the number of documents carried by this LFD. Usually 1 doc if it is
     * text based, or multiple if it is binary based
     */
    abstract int getNextId();

    /**
     * This method is only for txt based LFD, should only return value for 1 document
     */
    String getStringText() {
      throw new UnsupportedOperationException();
    }

    /**
     * This method is only for binary based LFD, it returns unconsumed buffer for all the documents encoded
     * in the same block, the returned binary buffer should be consumed and positioned to the start of next
     * document before next call
     */
    ByteBuffer getBlockByteText() {
      throw new UnsupportedOperationException();
    }

    private static final class TextBased extends LineFileDoc {
      final String stringText;

      final int id; // This is the exact id since it is txt based LFD

      TextBased(String text, float[] vector, int id) {
        super(vector);
        stringText = text;
        this.id = id;
      }

      @Override
      int getNextId() {
        return id;
      }

      @Override
      String getStringText() {
        return stringText;
      }
    }

    private static final class BinaryBased extends LineFileDoc {

      final ByteBuffer blockByteText;
      private int nextId; // will have multiple doc in a same LFD so we'll determine id using a base
      private int docCount; // and a count

      BinaryBased(ByteBuffer bytes, float[] vector, int idBase, int docCount) {
        super(vector);
        blockByteText = bytes;
        this.nextId = idBase;
        this.docCount = docCount;
      }

      @Override
      int getNextId() {
        if (docCount-- == 0) {
          throw new IllegalStateException("Calling getId more than docCount");
        }
        return nextId++;
      }

      @Override
      ByteBuffer getBlockByteText() {
        return blockByteText;
      }
    }

    LineFileDoc(String text, byte[] vector) {
      if (vector == null) {
        this.vector = null;
      } else {
        this.vector = ByteBuffer.wrap(vector);
      }
    }

    LineFileDoc(ByteBuffer bytes, byte[] vector) {
      if (vector == null) {
        this.vector = null;
      } else {
        this.vector = ByteBuffer.wrap(vector);
      }
    }

    void getVector(float[] in) {
      ((FloatBuffer) vector).get(in);
    }

    void getVector(byte[] in) {
      ((ByteBuffer) vector).get(in);
    }
  }
}
