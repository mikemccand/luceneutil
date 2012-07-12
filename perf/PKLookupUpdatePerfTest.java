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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.core.*;
import org.apache.lucene.codecs.*;
import org.apache.lucene.codecs.bloom.BloomFilteringPostingsFormat;
import org.apache.lucene.codecs.bloom.DefaultBloomFilterFactory;
import org.apache.lucene.codecs.lucene40.Lucene40Codec;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.NRTManager.TrackingIndexWriter;
import org.apache.lucene.store.*;
import org.apache.lucene.util.*;
import org.apache.lucene.util.hash.MurmurHash2;

// javac -cp ../build/core/classes/java:../build/analysis/common/classes/java perf/PKLookupUpdatePerfTest.java
// java -Xbatch -server -Xmx2g -Xms2g -cp .:../build/core/classes/java:../build/analysis/common/classes/java perf.PKLookupUpdatePerfTest MMapDirectory /p/lucene/indices/pk 1000000 17 [bloom|pulsing|memory|lucene40]

/**
 * Performs lookups on random primary keys and if they don't exist adds them to the index.
 * If they do exist, the previous values are first read then updated (here a simple count field is incremented).
 * 
 * This is a toy example (Lucene's docFreq could be used if we wanted to simply count terms) but
 * is representative of a worst-case scenarios where Lucene is used to service heavy updates.
 * 
 * An important business requirement in this worst-case scenario is that the state of an update must be
 * immediately visible to the next data item in the queue (i.e. we may see the same 2 keys in a row...).
 * 
 * This requirement does present a considerable challenge and I have experimented with 3 options:
 * 
 *  1) Call NRTManager.maybeRefreshBlocking() after every update/insert. This is expensive but the cost can 
 *     be alleviated to some extent through the use of NRTCachingDirectory to cache the small (1 doc!) segments
 *  2) Maintain a searchable RAMDirectory for unflushed content, periodically flushing via addIndexes(...). This was slow.
 *  3) Maintain a simple HashMap with key=primary key and value=an object with unflushed state  and periodically write
 *     this buffered batch of changes.
 * 
 * Option 3 is the fastest approach and the one implemented here. 
 * 
 * The use of BloomFilterPostingsFormat in this context is especially useful (~2x performance improvement) 
 *    
 * 
 * @author MAHarwood
 *
 */
public class PKLookupUpdatePerfTest {
  private static final boolean doWorstCaseFlushing = true;
  private static final boolean shareEnums = true;
  private static final boolean doSortKeys = true;
  private static final boolean useDocValues = true;
  private static final boolean doDirectDelete = true;
  private static final boolean lookupByReader = false;
  private static final boolean useBase36 = true;

  
  private static final String KEY_FIELD_NAME = "id";
  private static final String COUNT_FIELD_NAME = "count";  
  private static IndexWriter writer;
  static final Codec pulsingCodec = new Lucene40Codec() {      
    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
      return PostingsFormat.forName("Pulsing40");
    }
  };
  static final Codec bloomCodec = new Lucene40Codec() {      
    PostingsFormat bloomPf = new BloomFilteringPostingsFormat(
        PostingsFormat.forName("Pulsing40"), new DefaultBloomFilterFactory()
        {
          @Override
          public FuzzySet getSetForField(FieldInfo info) {
            return FuzzySet.createSetBasedOnMaxMemory(100 * 1024 * 1024,
                new MurmurHash2());
          }          
        });
    
    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
       return bloomPf;
    }
  };

  static final Codec memoryCodec = new Lucene40Codec() {      
    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
      return PostingsFormat.forName("Memory");
    }
    };

  
  private static IndexWriter createEmptyIndex(final Directory dir,
      final int docCount, Codec codec) throws IOException {
    final IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_40,
        new WhitespaceAnalyzer(Version.LUCENE_40));
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    
    iwc.setCodec(codec);
    if (doWorstCaseFlushing) {
      // 5 segs per level in 3 levels:
      int mbd = docCount / (5 * 111);
      iwc.setMaxBufferedDocs(mbd);
      iwc.setRAMBufferSizeMB(-1.0);
    } else {
      iwc.setRAMBufferSizeMB(128);
    }
    final IndexWriter w = new IndexWriter(dir, iwc);
    return w;
  }
  
  public static void main(String[] args) throws IOException,
      InterruptedException {
    
    final String dirImpl = args[0];
    final String dirPath = args[1];
    final int numDocs = Integer.parseInt(args[2]);
    final int keySpaceSize = Integer.parseInt(args[3]);
    final long seed = Long.parseLong(args[4]);
    final String codecChoice = args[5];
    
    final Codec codec;
    if("bloom".equalsIgnoreCase(codecChoice))
    {
      codec=bloomCodec;
    } else if ("lucene40".equalsIgnoreCase(codecChoice)) {
      codec=new Lucene40Codec();
    } else if ("memory".equalsIgnoreCase(codecChoice)) {
      codec=memoryCodec;
    } else if ("pulsing".equalsIgnoreCase(codecChoice)) {
      codec = pulsingCodec;
    } else  {
      throw new RuntimeException("Bad choice of codec :"+codecChoice);
    }

    //InfoStream.setDefault(new PrintStreamInfoStream(System.out));

    long minTime = Long.MAX_VALUE;

    for(int iter=0;iter<10;iter++) {
      System.out.println("\nITER: " + (1+iter));
      final Directory dir;
      if (dirImpl.equals("MMapDirectory")) {
        dir = new MMapDirectory(new File(dirPath));
      } else if (dirImpl.equals("NIOFSDirectory")) {
        dir = new NIOFSDirectory(new File(dirPath));
      } else if (dirImpl.equals("SimpleFSDirectory")) {
        dir = new SimpleFSDirectory(new File(dirPath));
      } else {
        throw new RuntimeException("unknown directory impl \"" + dirImpl + "\"");
      }
      writer = createEmptyIndex(dir, numDocs, codec);
      TrackingIndexWriter tw = new TrackingIndexWriter(writer);
      NRTManager nrtManager = new NRTManager(tw, new SearcherFactory(), true);
    
      int insertsThisBatch=0;
      int updatesThisBatch=0;
      final Random rand = new Random(seed);
      final Document doc = new Document();
      final Field keyField = new Field(KEY_FIELD_NAME, "", StringField.TYPE_NOT_STORED);
      doc.add(keyField);
      final IntDocValuesField countDVField;
      final Field countField;
      if (useDocValues) {
        countDVField = new IntDocValuesField(COUNT_FIELD_NAME, 0);
        doc.add(countDVField);
        countField = null;
      } else {
        countField = new StoredField(COUNT_FIELD_NAME, 0);
        doc.add(countField);
        countDVField = null;
      }

      final Comparator<Entry<BytesRef,Integer>> sortByKey = new Comparator<Entry<BytesRef,Integer>>() {
        @Override
        public int compare(Entry<BytesRef,Integer> a, Entry<BytesRef,Integer> b) {
          return a.getKey().compareTo(b.getKey());
        }
      };

      final long PRIME = 641;

      long checkSum = 0;

      int dirDelCount = 0;
      int nonDirDelCount = 0;
      System.out.println("codec="+codecChoice+" dir="+dirImpl);
    
      long start = System.currentTimeMillis();
      HashMap<BytesRef,Integer> dedupedBatch = new HashMap<BytesRef,Integer>();
      for (int i = 0; i < numDocs; i++) {
        final String s;
        final int id = rand.nextInt(keySpaceSize);
        if (useBase36) {
          s = String.format("%6s", Integer.toString(id, Character.MAX_RADIX)).replace(' ', '0');
        } else {
          s = String.format("%09d", id);
        }
        BytesRef pk = new BytesRef(s);
        //Check if this key already in this batch
        Integer previousTotalThisBatch = dedupedBatch.get(pk);
        int totalThisBatch;
        if (previousTotalThisBatch != null) {
          totalThisBatch = previousTotalThisBatch + 1;
        } else {
          totalThisBatch = 1;
        }
        dedupedBatch.put(pk, totalThisBatch);
      
        //Once we have a batch of changes big enough, write them to disk, incrementing any previous counts 
        if (dedupedBatch.size() > 100000) {
          long batchStart = System.currentTimeMillis();
          IndexSearcher searcher = nrtManager.acquire();
          //System.out.println("nd=" + searcher.getIndexReader().numDocs());

          try {

            final List<AtomicReaderContext> subReaders = searcher.getTopReaderContext().leaves();

            final DocsEnum[] docsEnums = new DocsEnum[subReaders.size()];
            final TermsEnum[] termsEnums = new TermsEnum[subReaders.size()];
            final Bits[] subLiveDocs = new Bits[subReaders.size()];
            final DocValues.Source[] subCounts;
            if (useDocValues) {
              subCounts = new DocValues.Source[subReaders.size()];
            } else {
              subCounts = null;
            }

            if (shareEnums) {
              for(int subIdx=0;subIdx<subReaders.size();subIdx++) {
                termsEnums[subIdx] = subReaders.get(subIdx).reader().fields().terms(KEY_FIELD_NAME).iterator(null);
              }
            }
            for(int subIdx=0;subIdx<subReaders.size();subIdx++) {
              if (useDocValues) {
                // Direct source is faster:
                //subCounts[subIdx] = subReaders.get(subIdx).reader().docValues(COUNT_FIELD_NAME).getSource();
                subCounts[subIdx] = subReaders.get(subIdx).reader().docValues(COUNT_FIELD_NAME).getDirectSource();
              }
              subLiveDocs[subIdx] = subReaders.get(subIdx).reader().getLiveDocs();
            }

            final List<Entry<BytesRef,Integer>> ents = new ArrayList<Entry<BytesRef,Integer>>(dedupedBatch.entrySet());
            if (doSortKeys) {
              Collections.sort(ents, sortByKey);
            }
            
            if (lookupByReader) {
              
              final int[] todo = new int[ents.size()];
              int left = ents.size();

              // First pass: find old count, delete:
              final int[] newCounts = new int[ents.size()];
              for(int subIDX=0;subIDX<subReaders.size();subIDX++) {
                AtomicReaderContext atomicReaderContext = subReaders.get(subIDX);
                AtomicReader subReader = atomicReaderContext.reader();
                DocValues.Source counts;
                if (useDocValues) {
                  counts = subCounts[subIDX];
                } else {
                  counts = null;
                }

                Bits liveDocs = subLiveDocs[subIDX];

                int upto = 0;
                for(int todoIDX=0;todoIDX<left;todoIDX++) {
                  final int entIDX;
                  if (subIDX == 0) {
                    entIDX = todoIDX;
                  } else {
                    entIDX = todo[todoIDX];
                  }

                  final Entry<BytesRef,Integer> keyAndCount = ents.get(entIDX);
                  final BytesRef pkBytes = keyAndCount.getKey();
                  int countThisBatch = keyAndCount.getValue();

                  DocsEnum de;
                  if (shareEnums) {
                    if (termsEnums[subIDX].seekExact(pkBytes, false)) {
                      de = docsEnums[subIDX] = termsEnums[subIDX].docs(liveDocs, docsEnums[subIDX], false);
                    } else {
                      todo[upto++] = entIDX;
                      continue;
                    }
                  } else {
                    de = subReader.termDocsEnum(liveDocs, KEY_FIELD_NAME,
                                                pkBytes, false);
                  }
                  if (de != null) {
                    int nextDoc=de.nextDoc();
                    if (nextDoc!=DocsEnum.NO_MORE_DOCS) {
                      final int oldCount;
                      if (useDocValues) {
                        oldCount = (int) counts.getInt(nextDoc);
                      } else {
                        oldCount = subReader.document(nextDoc).getField(COUNT_FIELD_NAME).numericValue().intValue();
                      }
                      if (oldCount != 0) {
                        int newCount = oldCount + countThisBatch;
                        if (doDirectDelete && tw.tryDeleteDocument(((SegmentReader) subReader).getSegmentInfo(), nextDoc) != -1) {
                          newCount = -newCount;
                          dirDelCount++;
                        } else {
                          nonDirDelCount++;
                        }
                        newCounts[entIDX] = newCount;
                      } else {
                        newCounts[entIDX] = -countThisBatch;
                      }
                    } else {
                      todo[upto++] = entIDX;
                    }
                  } else {
                    todo[upto++] = entIDX;
                  }
                }
                left = upto;
              }

              // Second pass: update
              for(int entIDX=0;entIDX<ents.size();entIDX++) {
                final Entry<BytesRef,Integer> keyAndCount = ents.get(entIDX);
                final BytesRef pkBytes = keyAndCount.getKey();
                int newCount = newCounts[entIDX];
                final boolean doUpdate;
                if (newCount == 0) {
                  doUpdate = false;
                  newCount = keyAndCount.getValue();
                  insertsThisBatch++;
                } else if (newCount < 0) {
                  doUpdate = false;
                  newCount = -newCount;
                  updatesThisBatch++;
                } else {
                  doUpdate = true;
                  updatesThisBatch++;
                }
                
                keyField.setStringValue(pkBytes.utf8ToString());
                if (useDocValues) {
                  countDVField.setIntValue(newCount);
                } else {
                  countField.setIntValue(newCount);
                }
                checkSum = checkSum * PRIME + newCount;

                //Did we find an already-stored document?
                if (!doUpdate) {
                  tw.addDocument(doc);
                } else {
                  tw.updateDocument(new Term(KEY_FIELD_NAME, pkBytes), doc);
                }
              }
              
            } else {

              for (Entry<BytesRef,Integer> keyAndCount : ents) {
            
                BytesRef pkBytes = keyAndCount.getKey();
                int countThisBatch = keyAndCount.getValue();
            
                // search all subreaders for a previously
                // written document
                int oldCount = 0;
                boolean doUpdate = true;
                for (int subIDX=0;subIDX<subReaders.size();subIDX++) {

                  AtomicReaderContext atomicReaderContext = subReaders.get(subIDX);
                  AtomicReader subReader = atomicReaderContext.reader();
                  DocValues.Source counts;
                  if (useDocValues) {
                    counts = subCounts[subIDX];
                  } else {
                    counts = null;
                  }

                  Bits liveDocs = subLiveDocs[subIDX];
                  DocsEnum de;
                  if (shareEnums) {
                    if (termsEnums[subIDX].seekExact(pkBytes, false)) {
                      de = docsEnums[subIDX] = termsEnums[subIDX].docs(liveDocs, docsEnums[subIDX], false);
                    } else {
                      continue;
                    }
                  } else {
                    de = subReader.termDocsEnum(liveDocs, KEY_FIELD_NAME,
                                                pkBytes, false);
                  }
                  if (de != null) {
                    int nextDoc=de.nextDoc();
                    if (nextDoc!=DocsEnum.NO_MORE_DOCS) {
                      if (useDocValues) {
                        oldCount = (int) counts.getInt(nextDoc);
                      } else {
                        oldCount = subReader.document(nextDoc).getField(COUNT_FIELD_NAME).numericValue().intValue();
                      }
                      if (doDirectDelete && tw.tryDeleteDocument(((SegmentReader) subReader).getSegmentInfo(), nextDoc) != -1) {
                        doUpdate = false;
                        dirDelCount++;
                      } else {
                        nonDirDelCount++;
                      }
                      break;
                    }
                  }
                }

                keyField.setStringValue(keyAndCount.getKey().utf8ToString());
                final int newCount = oldCount + countThisBatch;
                if (useDocValues) {
                  countDVField.setIntValue(newCount);
                } else {
                  countField.setIntValue(newCount);
                }
                checkSum = checkSum * PRIME + newCount;

                if (oldCount == 0) {
                  insertsThisBatch++;
                } else {
                  updatesThisBatch++;
                }

                //Did we find an already-stored document?
                if (oldCount == 0 || !doUpdate) {
                  //No - a new document - store using addDocument()
                  tw.addDocument(doc);
                } else {
                  tw.updateDocument(new Term(KEY_FIELD_NAME, keyAndCount.getKey()), doc);
                }
              }
            }
          } finally {
            nrtManager.release(searcher);
          }
          //Need to make newly written batch of changes
          //visible
          long beforeRefresh = System.currentTimeMillis();
          nrtManager.maybeRefreshBlocking();
          long endTime = System.currentTimeMillis();
          long batchDiff = endTime - batchStart;
          System.out.println("Writing batch took " + batchDiff + " ms; refresh reader took " + (endTime - beforeRefresh) + " ms inserts="+insertsThisBatch+" updates="+updatesThisBatch);
          insertsThisBatch=0;
          updatesThisBatch=0;          
          dedupedBatch.clear();
        }
      }
      long tCloseStart = System.currentTimeMillis();
      nrtManager.close();
      writer.close();
      long tEnd = System.currentTimeMillis();
      long diff = tEnd - start;
      System.out.println("Took " + diff + " ms; close took " + (tEnd - tCloseStart) + " (checksum=" + checkSum + " delCount=" + dirDelCount + " vs " + nonDirDelCount);
      dir.close();

      if (diff < minTime) {
        System.out.println("  **");
        minTime = diff;
      }
    }
    System.out.println("Best time: " + minTime + " ms");
  }
}
