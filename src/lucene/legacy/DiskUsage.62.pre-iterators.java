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
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat.Mode;
import org.apache.lucene.codecs.lucene62.Lucene62Codec;
import org.apache.lucene.codecs.lucene54.Lucene54DocValuesFormat;
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
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
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
    conf.setCodec(new Lucene62Codec(Mode.valueOf(System.getProperty("mode", "BEST_SPEED"))) {
      @Override
      public PostingsFormat getPostingsFormatForField(String field) {
        return new Lucene50PostingsFormat();
      }

      @Override
      public DocValuesFormat getDocValuesFormatForField(String field) {
        return new Lucene54DocValuesFormat();
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

      Bits docsWithField = reader.getDocsWithField(field.name);
      if (docsWithField != null) {
          int count = 0;
          for(int docID=0;docID<reader.maxDoc();docID++) {
              if (docsWithField.get(docID)) {
                  count++;
              }
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
      } else {
        // not a per-field file, but we can hackishly do this for the points case.
        if ("dii".equals(IndexFileNames.getExtension(file))) {
          System.err.println("retrieving per-field point usage, if you see a scary corruption error, its probably just this tool!!!!");
          try (ChecksumIndexInput in = directory.openChecksumInput(file, IOContext.READONCE)) {
            // fail hard if its not exactly the version we do this hack for.
            CodecUtil.checkIndexHeader(in, "Lucene60PointsFormatMeta", 0, 0, reader.getSegmentInfo().info.getId(), "");
            int fieldCount = in.readVInt();
            // strangely, bkd offsets are not in any guaranteed order
            TreeMap<Long,String> offsetToField = new TreeMap<>();
            for (int i = 0; i < fieldCount; i++) {
              int field = in.readVInt();
              long offset = in.readVLong();
              offsetToField.put(offset, reader.getFieldInfos().fieldInfo(field).name);
            }
            // now we can traverse in order
            long previousOffset = 0;
            for (Map.Entry<Long,String> entry : offsetToField.entrySet()) {
              long offset = entry.getKey();
              String field = entry.getValue();
              stats.get(field).pointsBytes += (offset - previousOffset);
              previousOffset = offset;
            }
            CodecUtil.checkFooter(in);
          }
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
          case "fdt":
          case "fdx":
            storeSize += size;
            break;
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
          case "dii":
          case "dim":
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
