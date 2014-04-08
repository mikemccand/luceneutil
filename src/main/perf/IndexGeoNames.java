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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

// javac -d /l/util/build -cp build/core/classes/java:build/analysis/common/classes/java /l/util/src/main/perf/IndexGeoNames.java

// java -cp /l/util/build:build/core/classes/java:build/analysis/common/classes/java perf.IndexGeoNames /lucenedata/geonames/allCountries.txt /l/scratch/indices/geonames 

public class IndexGeoNames {

  public static void main(String[] args) throws IOException {
    String geoNamesFile = args[0];
    File indexPath = new File(args[1]);
    if (indexPath.exists()) {
      throw new IllegalArgumentException("please remove indexPath \"" + indexPath + "\" before running");
    }

    Directory dir = FSDirectory.open(indexPath);
    IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_50, new StandardAnalyzer(Version.LUCENE_50));
    iwc.setRAMBufferSizeMB(250);
    IndexWriter w = new IndexWriter(dir, iwc);

    // 64K buffer:
    InputStream is = new FileInputStream(geoNamesFile);
    BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"), 1 << 16);
    int count = 0;
    long startMS = System.currentTimeMillis();
    while (true) {
      String line = reader.readLine();
      if (line == null) {
        break;
      }
      String[] values = line.split("\t");
      //System.out.println("LINE: " + Arrays.toString(values));

      Document doc = new Document();
      doc.add(new StringField("geoNameID", values[0], Field.Store.NO));
      doc.add(new TextField("name", values[1], Field.Store.NO));
      doc.add(new TextField("asciiName", values[2], Field.Store.NO));
      doc.add(new TextField("alternateNames", values[3], Field.Store.NO));
      /*
      doc.add(new DoubleField("latitude", Double.parseDouble(values[4]), Field.Store.NO));
      doc.add(new DoubleField("longitude", Double.parseDouble(values[5]), Field.Store.NO));
      doc.add(new StringField("featureClass", values[6], Field.Store.NO));
      doc.add(new StringField("featureCode", values[7], Field.Store.NO));
      doc.add(new StringField("countryCode", values[8], Field.Store.NO));
      doc.add(new StringField("cc2", values[9], Field.Store.NO));
      doc.add(new StringField("admin1Code", values[10], Field.Store.NO));
      doc.add(new StringField("admin2Code", values[11], Field.Store.NO));
      doc.add(new StringField("admin3Code", values[12], Field.Store.NO));
      doc.add(new StringField("admin4Code", values[13], Field.Store.NO));
      doc.add(new LongField("population", Long.parseLong(values[14]), Field.Store.NO));
      doc.add(new LongField("elevation", Long.parseLong(values[15]), Field.Store.NO));
      doc.add(new StringField("dem", values[16], Field.Store.NO));
      doc.add(new StringField("timezone", values[17], Field.Store.NO));
      */
      // TODO: modification date
      w.addDocument(doc);
      count++;
      if (count % 200000 == 0) {
        long ms = System.currentTimeMillis();
        System.out.println(count + ": " + ((ms - startMS)/1000.0) + " sec");
      }
    }
    w.close(false);
    long ms = System.currentTimeMillis();
    System.out.println("end: " + count + ": " + ((ms - startMS)/1000.0) + " sec");
    dir.close();
  }
}
