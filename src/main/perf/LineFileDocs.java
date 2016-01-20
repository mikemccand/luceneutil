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

// FIELDS_HEADER_INDICATOR###	title	timestamp	text	username	characterCount	categories	imageCount	sectionCount	subSectionCount	subSubSectionCount	refCount

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;

public class LineFileDocs implements Closeable {

  private BufferedReader reader;
  private final static int BUFFER_SIZE = 1 << 16;     // 64K
  private final boolean doRepeat;
  private final String path;
  private final boolean storeBody;
  private final boolean tvsBody;
  private final boolean bodyPostingsOffsets;
  private final AtomicLong bytesIndexed = new AtomicLong();
  private final boolean doClone;
  private final TaxonomyWriter taxoWriter;
  private final Set<String> facetFields;
  private final FacetsConfig facetsConfig;
  private String[] extraFacetFields;
  private final boolean addDVFields;

  public LineFileDocs(String path, boolean doRepeat, boolean storeBody, boolean tvsBody, boolean bodyPostingsOffsets,
                      boolean doClone, TaxonomyWriter taxoWriter, Set<String> facetFields, FacetsConfig facetsConfig,
                      boolean addDVFields) throws IOException {
    this.path = path;
    this.storeBody = storeBody;
    this.tvsBody = tvsBody;
    this.bodyPostingsOffsets = bodyPostingsOffsets;
    this.doClone = doClone;
    this.doRepeat = doRepeat;
    this.taxoWriter = taxoWriter;
    this.facetFields = facetFields;
    this.facetsConfig = facetsConfig;
    this.addDVFields = addDVFields;
    open();
  }

  public long getBytesIndexed() {
    return bytesIndexed.get();
  }

  private void open() throws IOException {
    InputStream is = new FileInputStream(path);
    reader = new BufferedReader(new InputStreamReader(is, "UTF-8"), BUFFER_SIZE);
    String firstLine = reader.readLine();
    if (firstLine.startsWith("FIELDS_HEADER_INDICATOR")) {
      if (!firstLine.startsWith("FIELDS_HEADER_INDICATOR###	doctitle	docdate	body") &&
          !firstLine.startsWith("FIELDS_HEADER_INDICATOR###	title	timestamp	text")) {
        throw new IllegalArgumentException("unrecognized header in line docs file: " + firstLine.trim());
      }
      if (facetFields.isEmpty() == false) {
        String[] fields = firstLine.split("\t");
        if (fields.length > 4) {
          extraFacetFields = Arrays.copyOfRange(fields, 4, fields.length);
          System.out.println("Additional facet fields: " + Arrays.toString(extraFacetFields));

          List<String> extraFacetFieldsList = Arrays.asList(extraFacetFields);

          // Verify facet fields now:
          for(String field : facetFields) {
            if (!field.equals("Date") && !extraFacetFieldsList.contains(field)) {
              throw new IllegalArgumentException("facet field \"" + field + "\" is not recognized");
            }
          }
        } else {
          // Verify facet fields now:
          for(String field : facetFields) {
            if (!field.equals("Date")) {
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

  @Override
	public synchronized void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
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
    final Field titleDV;
    //final BinaryDocValuesField titleBDV;
    final NumericDocValuesField lastModNDV; 
    final Field body;
    final Field id;
    final Field date;
    //final NumericDocValuesField dateMSec;
    //final LongField rand;
    final Field timeSec;
    // Necessary for "old style" wiki line files:
    final SimpleDateFormat dateParser = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss", Locale.US);

    // For just y/m/day:
    //final SimpleDateFormat dateParser = new SimpleDateFormat("y/M/d", Locale.US);

    //final SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US);
    final Calendar dateCal = Calendar.getInstance();
    final ParsePosition datePos = new ParsePosition(0);

    DocState(boolean storeBody, boolean tvsBody, boolean bodyPostingsOffsets, boolean addDVFields) {
      doc = new Document();
      
      title = new StringField("title", "", Field.Store.NO);
      doc.add(title);

      if (addDVFields) {
      	titleDV = new SortedDocValuesField("titleDV", new BytesRef(""));
      	doc.add(titleDV);
      	
      	//titleBDV = new BinaryDocValuesField("titleBDV", new BytesRef(""));
      	//doc.add(titleBDV);
      	
      	lastModNDV = new NumericDocValuesField("lastModNDV", -1);
      	doc.add(lastModNDV);
      } else {
      	titleDV = null;
      	//titleBDV = null;
      	lastModNDV = null;
      }
      
      titleTokenized = new Field("titleTokenized", "", TextField.TYPE_STORED);
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

      id = new Field("id", "", StringField.TYPE_STORED);
      doc.add(id);

      date = new Field("date", "", StringField.TYPE_STORED);
      doc.add(date);

      //dateMSec = new NumericDocValuesField("datenum", 0L);
      //doc.add(dateMSec);

      //rand = new LongField("rand", 0L, Field.Store.NO);
      //doc.add(rand);

      timeSec = new IntField("timesecnum", 0, Field.Store.NO);
      doc.add(timeSec);
    }
  }

  public DocState newDocState() {
    return new DocState(storeBody, tvsBody, bodyPostingsOffsets, addDVFields);
  }

  // TODO: is there a pre-existing way to do this!!!
  static Document cloneDoc(Document doc1) {
    final Document doc2 = new Document();
    for(IndexableField f0 : doc1.getFields()) {
      Field f = (Field) f0;
      if (f instanceof LongField) {
        doc2.add(new LongField(f.name(), ((LongField) f).numericValue().longValue(), Field.Store.NO));
      } else if (f instanceof IntField) {
        doc2.add(new IntField(f.name(), ((IntField) f).numericValue().intValue(), Field.Store.NO));
      } else if (f instanceof SortedDocValuesField) {
        doc2.add(new SortedDocValuesField(f.name(), f.binaryValue()));
      } else if (f instanceof NumericDocValuesField) {
        doc2.add(new NumericDocValuesField(f.name(), f.numericValue().longValue()));
      } else if (f instanceof BinaryDocValuesField) {
        doc2.add(new BinaryDocValuesField(f.name(), f.binaryValue()));
      } else {
        Field field2 = new Field(f.name(),
                                 f.stringValue(),
                                 f.fieldType());
        doc2.add(field2);
      }
    }

    return doc2;
  }

  private int readCount;

  //private final Random rand = new Random(17);

  @SuppressWarnings({"rawtypes", "unchecked"})
  public Document nextDoc(DocState doc) throws IOException {
    String line;
    final int myID;
    synchronized(this) {
      myID = readCount++;
      line = reader.readLine();
      if (line == null) {
        if (doRepeat) {
          close();
          open();
          line = reader.readLine();
        } else {
          return null;
        }
      }
    }

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
      spot3 = line.length();
    }
    bytesIndexed.addAndGet(spot3);

    doc.body.setStringValue(line.substring(1+spot2, spot3));
    final String title = line.substring(0, spot);
    doc.title.setStringValue(title);
    if (addDVFields) {
      //doc.titleBDV.setBytesValue(new BytesRef(title));
      doc.titleDV.setBytesValue(new BytesRef(title));
      doc.titleTokenized.setStringValue(title);
    }
    final String dateString = line.substring(1+spot, spot2);
    doc.date.setStringValue(dateString);
    doc.id.setStringValue(intToID(myID));

    doc.datePos.setIndex(0);
    final Date date = doc.dateParser.parse(dateString, doc.datePos);
    if (date == null) {
      System.out.println("FAILED: " + dateString);
    }
    //doc.dateMSec.setLongValue(date.getTime());

    //doc.rand.setLongValue(rand.nextInt(10000));

    doc.dateCal.setTime(date);
    if (addDVFields) {
      doc.lastModNDV.setLongValue(doc.dateCal.getTimeInMillis());
    }

    final int sec = doc.dateCal.get(Calendar.HOUR_OF_DAY)*3600 + doc.dateCal.get(Calendar.MINUTE)*60 + doc.dateCal.get(Calendar.SECOND);
    doc.timeSec.setIntValue(sec);

    if (facetFields.isEmpty() == false) {
      FacetField dateFacetField = new FacetField("Date",
                                                 ""+doc.dateCal.get(Calendar.YEAR),
                                                 ""+doc.dateCal.get(Calendar.MONTH),
                                                 ""+doc.dateCal.get(Calendar.DAY_OF_MONTH));

      Document doc2 = cloneDoc(doc.doc);

      if (facetFields.contains("Date")) {
        doc2.add(dateFacetField);
      }

      if (extraFacetFields != null) {
        String[] extraValues = line.substring(spot3+1, line.length()).split("\t");
        
        for(int i=0;i<extraFacetFields.length;i++) {
          String extraFieldName = extraFacetFields[i];
          if (facetFields.contains(extraFieldName)) {
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
}

