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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.document.*;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.util.BytesRef;

public class LineFileDocs implements Closeable {

  private BufferedReader reader;
  private final static int BUFFER_SIZE = 1 << 16;     // 64K
  private final boolean doRepeat;
  private final String path;
  private final AtomicLong bytesIndexed = new AtomicLong();

  public LineFileDocs(String path, boolean doRepeat) throws IOException {
    this.path = path;
    open();
    this.doRepeat = doRepeat;
  }

  public long getBytesIndexed() {
    return bytesIndexed.get();
  }

  private void open() throws IOException {
    final InputStream is = new FileInputStream(path);
    reader = new BufferedReader(new InputStreamReader(is, "UTF-8"), BUFFER_SIZE);
  }

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
      final char ch = (char) id.charAt(downTo--);
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
    final Field body;
    final Field id;
    final Field date;
    final NumericField dateMSec;
    final NumericField timeSec;
    final SimpleDateFormat dateParser = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss", Locale.US);
    final Calendar dateCal = Calendar.getInstance();
    final ParsePosition datePos = new ParsePosition(0);

    public DocState() {
      doc = new Document();
      
      title = new StringField("title", "");
      doc.add(title);

      titleTokenized = new Field("titleTokenized", "", TextField.TYPE_STORED);
      doc.add(titleTokenized);

      body = new TextField("body", "");
      doc.add(body);

      id = new Field("id", "", StringField.TYPE_STORED);
      doc.add(id);

      date = new Field("date", "", StringField.TYPE_STORED);
      doc.add(date);

      dateMSec = new NumericField("datenum");
      doc.add(dateMSec);

      timeSec = new NumericField("timesecnum");
      doc.add(timeSec);
    }
  }

  public DocState newDocState() {
    return new DocState();
  }

  private final ThreadLocal<DocState> threadDocs = new ThreadLocal<DocState>();

  private int readCount;

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

    bytesIndexed.addAndGet(line.length());

    int spot = line.indexOf(SEP);
    if (spot == -1) {
      throw new RuntimeException("line: [" + line + "] is in an invalid format !");
    }
    int spot2 = line.indexOf(SEP, 1 + spot);
    if (spot2 == -1) {
      throw new RuntimeException("line: [" + line + "] is in an invalid format !");
    }

    doc.body.setValue(line.substring(1+spot2, line.length()));
    final String title = line.substring(0, spot);
    doc.title.setValue(title);
    doc.titleTokenized.setValue(title);
    final String dateString = line.substring(1+spot, spot2);
    doc.date.setValue(dateString);
    doc.id.setValue(intToID(myID));

    doc.datePos.setIndex(0);
    final Date date = doc.dateParser.parse(dateString, doc.datePos);
    doc.dateMSec.setLongValue(date.getTime());

    doc.dateCal.setTime(date);
    final int sec = doc.dateCal.get(Calendar.HOUR_OF_DAY)*3600 + doc.dateCal.get(Calendar.MINUTE)*60 + doc.dateCal.get(Calendar.SECOND);
    doc.timeSec.setIntValue(sec);

    return doc.doc;
  }
}

