/*
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

package perf;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.lucene99.Lucene99Codec.Mode;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

/** 
 * does ugly hacks to print out disk usage analysis of a lucene index 
 * <p>
 * You need a lucene 6.x core jar, then do this:
 * javac -cp /path/to/lucene-core.jar DiskUsage.java
 * java -cp /path/to/lucene-core.jar:. DiskUsage /elasticserach/data/elasticsearch/nodes/0/indices/whatever/0/index
 */
public class DiskUsage {
  
  public static void main(String args[]) throws Exception {
    if (args.length != 1) {
      System.err.println("java [-Djava.io.tmpdir=/scratch] [-Dmode=BEST_COMPRESSION] -cp lucene-core.jar:./build DiskUsage <path to lucene index>");
      System.exit(1);
    }

    IndexWriterConfig conf = new IndexWriterConfig(null);
    conf.setOpenMode(OpenMode.CREATE);
    // force codec to write per-field filenames.
    conf.setCodec(new Lucene99Codec(Mode.valueOf(System.getProperty("mode", "BEST_SPEED"))) {
      @Override
      public PostingsFormat getPostingsFormatForField(String field) {
        return new Lucene90PostingsFormat();
      }

      @Override
      public DocValuesFormat getDocValuesFormatForField(String field) {
        return new Lucene90DocValuesFormat();
      }
    });
    
    Path tmp = Files.createTempDirectory(null);
    System.err.println("analyzing... (using " + tmp + " for temporary storage)");
    
    try (Directory dir = FSDirectory.open(Paths.get(args[0]));
         DirectoryReader reader = DirectoryReader.open(dir);
         Directory scratch = FSDirectory.open(tmp);
         IndexWriter writer = new IndexWriter(scratch, conf)) {
      
      CodecReader inputs[] = new CodecReader[reader.leaves().size()];
      for (int i = 0; i < inputs.length; i++) {
        inputs[i] = (CodecReader) reader.leaves().get(i).reader();
      }
      writer.addIndexes(inputs);
      
      try (DirectoryReader newReader = DirectoryReader.open(writer)) {
        assert newReader.leaves().size() == 1;
        SegmentReader sr = (SegmentReader) newReader.leaves().get(0).reader();
        report(sr, analyzeFields(sr));
      }
    } finally {
      IOUtils.rm(tmp);
    }
  }
  
  /** Returns the codec suffix from this file name, or null if there is no suffix. */
  public static String parseSuffix(String filename) {
    if (!filename.startsWith("_")) {
      return null;
    }
    String parts[] = IndexFileNames.stripExtension(filename).substring(1).split("_");
    // 4 cases: 
    // segment.ext
    // segment_gen.ext
    // segment_codec_suffix.ext
    // segment_gen_codec_suffix.ext
    if (parts.length == 3) {
      return parts[2];
    } else if (parts.length == 4) {
      return parts[3];
    } else {
      return null;
    }
  }
  
  static class FieldStats implements Comparable<FieldStats> {
    final String name;
    long termsBytes;
    long postingsBytes;
    long proxBytes;
    long dvBytes;
    long pointsBytes;
    int docCountWithField;
    
    FieldStats(String name) {
      this.name = name;
    }
    
    long totalBytes() {
      return termsBytes + postingsBytes + proxBytes + dvBytes + pointsBytes;
    }

    @Override
    public int compareTo(FieldStats o) {
      // reverse order
      int cmp = Long.compare(o.totalBytes(), totalBytes());
      if (cmp == 0) {
        cmp = name.compareTo(o.name);
      }
      return cmp;
    }
  }
  
  static Set<FieldStats> analyzeFields(SegmentReader reader) throws Exception {
    Map<String,FieldStats> stats = new HashMap<>();
    Map<String,String> dvSuffixes = new HashMap<>();
    Map<String,String> postingsSuffixes = new HashMap<>();
    for (FieldInfo field : reader.getFieldInfos()) {
      FieldStats fieldStats = new FieldStats(field.name);
      stats.put(field.name, fieldStats);
      Map<String,String> attributes = field.attributes();
      if (attributes != null) {
        String postingsSuffix = attributes.get(PerFieldPostingsFormat.PER_FIELD_SUFFIX_KEY);
        if (postingsSuffix != null) {
          postingsSuffixes.put(postingsSuffix, field.name);
        }
        String dvSuffix = attributes.get(PerFieldDocValuesFormat.PER_FIELD_SUFFIX_KEY);
        if (dvSuffix != null) {
          dvSuffixes.put(dvSuffix, field.name);
        }
      }

      DocIdSetIterator docsWithField;
      switch(field.getDocValuesType()) {
      case NUMERIC:
        docsWithField = reader.getNumericDocValues(field.name);
        break;
      case BINARY:
        docsWithField = reader.getBinaryDocValues(field.name);
        break;
      case SORTED:
        docsWithField = reader.getSortedDocValues(field.name);
        break;
      case SORTED_NUMERIC:
        docsWithField = reader.getSortedNumericDocValues(field.name);
        break;
      case SORTED_SET:
        docsWithField = reader.getSortedSetDocValues(field.name);
        break;
      case NONE:
        docsWithField = null;
        break;
      default:
        docsWithField = null;
        break;
      }

      if (docsWithField != null) {
        int count = 0;
        while(docsWithField.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          count++;
        }
        fieldStats.docCountWithField = count;
      }
    }
    
    Directory directory = reader.directory();
    for (String file : directory.listAll()) {
      String suffix = parseSuffix(file);
      long bytes = directory.fileLength(file);
      if (suffix != null) {
        switch (IndexFileNames.getExtension(file)) {
          case "dvd":
          case "dvm":
            stats.get(dvSuffixes.get(suffix)).dvBytes += bytes;
            break;
          case "tim":
          case "tip":
          case "tmd":
            stats.get(postingsSuffixes.get(suffix)).termsBytes += bytes;
            break;
          case "doc":
            stats.get(postingsSuffixes.get(suffix)).postingsBytes += bytes;
            break;
          case "pos":
          case "pay":
            stats.get(postingsSuffixes.get(suffix)).proxBytes += bytes;
            break;
          default: 
            throw new AssertionError("unexpected suffixed file: " + file);
        }
      }
    }

    return new TreeSet<FieldStats>(stats.values());
  }
  
  static void report(SegmentReader reader, Set<FieldStats> stats) throws Exception {
    long totalSize = 0;
    long storeSize = 0;
    long vectorSize = 0;
    long normsSize = 0;
    long dvsSize = 0;
    long postingsSize = 0;
    long pointsSize = 0;
    long termsSize = 0;
    long proxSize = 0;
    for (String file : reader.directory().listAll()) {
      long size = reader.directory().fileLength(file);
      totalSize += size;
      String extension = IndexFileNames.getExtension(file);
      if (extension != null) {
        switch (extension) {
          case "fdm":
          case "fdt":
          case "fdx":
            storeSize += size;
            break;
          case "tvm":
          case "tvx":
          case "tvd":
            vectorSize += size;
            break;
          case "nvd":
          case "nvm":
            normsSize += size;
            break;
          case "dvd":
          case "dvm":
            dvsSize += size;
            break;
          case "tim":
          case "tip":
            termsSize += size;
            break;
          case "pos":
          case "pay":
            proxSize += size;
            break;
          case "doc":
            postingsSize += size;
            break;
          case "kdm":
          case "kdi":
          case "kdd":
            pointsSize += size;
            break;
        }
      }
    }
    
    DecimalFormat df = new DecimalFormat("#,##0");
    System.out.printf("total disk:    %15s\n", df.format(totalSize));
    System.out.printf("num docs:      %15s\n", df.format(reader.numDocs()));
    System.out.printf("stored fields: %15s\n", df.format(storeSize));
    System.out.printf("term vectors:  %15s\n", df.format(vectorSize));
    System.out.printf("norms:         %15s\n", df.format(normsSize));
    System.out.printf("docvalues:     %15s\n", df.format(dvsSize));
    System.out.printf("postings:      %15s\n", df.format(postingsSize));
    System.out.printf("prox:          %15s\n", df.format(proxSize));
    System.out.printf("points:        %15s\n", df.format(pointsSize));
    System.out.printf("terms:         %15s\n", df.format(termsSize));
    System.out.println();

    int maxFieldNameLength = 0;
    for (FieldStats field : stats) {
        maxFieldNameLength = Math.max(maxFieldNameLength, field.name.length());
    }

    // Make sure we format to enough room for the max field length:
    String fieldNameFormat = "%" + maxFieldNameLength + "s";
    
    System.out.printf(fieldNameFormat + " %15s %15s %15s %15s %15s %15s %15s %20s\n", "field", "total", "terms dict", "postings", "proximity", "points", "docvalues", "% with dv", "features");
    System.out.printf(fieldNameFormat + " %15s %15s %15s %15s %15s %15s %15s %20s\n", "=====", "=====", "==========", "========", "=========", "=========", "=========", "========", "========");

    for (FieldStats field : stats) {
      System.out.printf(fieldNameFormat + " %15s %15s %15s %15s %15s %15s %14.1f%% %20s\n", 
                       field.name,
                       df.format(field.totalBytes()),
                       df.format(field.termsBytes),
                       df.format(field.postingsBytes),
                       df.format(field.proxBytes),
                       df.format(field.pointsBytes),
                       df.format(field.dvBytes),
                       (100.0*field.docCountWithField)/reader.maxDoc(),
                       features(reader.getFieldInfos().fieldInfo(field.name)));
    }
  }

  static String features(FieldInfo fi) {
    StringBuilder sb = new StringBuilder();
    IndexOptions options = fi.getIndexOptions();
    if (options != IndexOptions.NONE) {
      String words[] = options.toString().split("_");
      sb.append(words[words.length-1].toLowerCase());
      sb.append(" ");
    }
    if (fi.hasPayloads()) {
      sb.append("payloads ");
    }
    if (fi.hasNorms()) {
      sb.append("norms ");
    }
    if (fi.hasVectors()) {
      sb.append("vectors ");
    }
    if (fi.getPointDimensionCount() != 0) {
      sb.append(fi.getPointNumBytes());
      sb.append("bytes/");
      sb.append(fi.getPointDimensionCount());
      sb.append("D ");
    }
    DocValuesType dvType = fi.getDocValuesType();
    if (dvType != DocValuesType.NONE) {
      sb.append(dvType.toString().toLowerCase());
    }
    return sb.toString().trim();
  }
}
