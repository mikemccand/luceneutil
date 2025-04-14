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

// TODO
//  - be able to quickly run a/b tests again
//  - absorb nrt, pklokup, search, indexing into one tool?
//  - switch to named cmd line args
//  - get pk lookup working w/ remote tasks

import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.classic.ClassicAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.QueryTimeoutImpl;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.RamUsageEstimator;

import com.sun.management.ThreadMXBean;

import perf.IndexThreads.Mode;

// TODO
//   - post queries on pao
//   - fix pk lookup to tolerate deletes
//   - get regexp title queries
//   - test shingle at search time

// commits: single, multi, delsingle, delmulti

// trunk:
//   javac -Xlint -Xlint:deprecation -cp .:$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java perf/SearchPerfTest.java perf/LineFileDocs.java perf/RandomQuery.java
//   java -cp .:$LUCENE_HOME/build/highlighter/classes/java:$LUCENE_HOME/build/codecs/classes/java:$LUCENE_HOME/build/core/classes/java:$LUCENE_HOME/build/test-framework/classes/java:$LUCENE_HOME/build/queryparser/classes/java:$LUCENE_HOME/build/suggest/classes/java:$LUCENE_HOME/build/analysis/common/classes/java:$LUCENE_HOME/build/grouping/classes/java perf.SearchPerfTest -dirImpl MMapDirectory -indexPath /l/scratch/indices/wikimedium10m.lucene.trunk2.Lucene41.nd10M/index -analyzer StandardAnalyzerNoStopWords -taskSource term.tasks -numConcurrentQueries 2 -field body -topN 10 -staticSeed 0 -seed 0 -similarity DefaultSimilarity -commit multi -hiliteImpl FastVectorHighlighter -log search.log -nrt -indexThreadCount 1 -docsPerSecPerThread 10 -reopenEverySec 5 -postingsFormat Lucene41 -idFieldPostingsFormat Lucene41 -taskRepeatCount 1000 -tasksPerCat 5 -lineDocsFile /lucenedata/enwiki/enwiki-20120502-lines-1k.txt

@SuppressWarnings("deprecation")
public class SearchPerfTest {

  private static final int WARMUP_MSEC = 5000;

  // ReferenceManager that never changes its searcher:
  private static class SingleIndexSearcher extends ReferenceManager<IndexSearcher> {

    public SingleIndexSearcher(IndexSearcher s) {
      this.current = s;
    }

    @Override
    public void decRef(IndexSearcher ref) throws IOException {
      ref.getIndexReader().decRef();
    }

    @Override
    protected IndexSearcher refreshIfNeeded(IndexSearcher ref) {
      return null;
    }

    @Override
    protected boolean tryIncRef(IndexSearcher ref) {
      return ref.getIndexReader().tryIncRef();
    }
    
    @Override
    protected int getRefCount(IndexSearcher ref) {
    	return ref.getIndexReader().getRefCount();
    }
  }
  
  public static void main(String[] clArgs) throws Exception {

    StatisticsHelper stats = new StatisticsHelper();
    stats.startStatistics();
    try {
      _main(clArgs);
    } finally {
      stats.stopStatistics();
    }
  }

  private static final ThreadMXBean threadBean = (ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();

  private static double nsToMS(long ns) {
    return ns / 1_000_000.0;
  }

  private static double msToNS(double ms) {
    return ms * 1_000_000.0;
  }

  /** Snapshots all running threads and their CPU counters */
  static final class ThreadDetails {
    public final long[] threadIDs;
    public final long[] cpuTimesNS;
    public final ThreadInfo[] threadInfos;
    public final long ns;

    public ThreadDetails() {
      ns = System.nanoTime();
      threadIDs = threadBean.getAllThreadIds();
      cpuTimesNS = threadBean.getThreadCpuTime(threadIDs);
      threadInfos = threadBean.getThreadInfo(threadIDs);
    }
  }

  private static void _main(String[] clArgs) throws Exception {

    // args: dirImpl indexPath numThread numIterPerThread
    // eg java SearchPerfTest /path/to/index 4 100
    final Args args = new Args(clArgs);

    Directory dir0;
    final String dirPath = args.getString("-indexPath") + "/index";
    final String dirImpl = args.getString("-dirImpl");

    OpenDirectory od = OpenDirectory.get(dirImpl);

    /*
    } else if (dirImpl.equals("NativePosixMMapDirectory")) {
      dir0 = new NativePosixMMapDirectory(new File(dirPath));
      ramDir = null;
      if (doFacets) {
        facetsDir = new NativePosixMMapDirectory(new File(facetsDirPath));
      }
    } else if (dirImpl.equals("CachingDirWrapper")) {
      dir0 = new CachingRAMDirectory(new MMapDirectory(new File(dirPath)));
      ramDir = null;
    } else if (dirImpl.equals("RAMExceptDirectPostingsDirectory")) {
      // Load only non-postings files into RAMDir (assumes
      // Lucene40PF is the wrapped PF):
      Set<String> postingsExtensions = new HashSet<String>();
      postingsExtensions.add("frq");
      postingsExtensions.add("prx");
      postingsExtensions.add("tip");
      postingsExtensions.add("tim");
      
      ramDir =  new RAMDirectory();
      Directory fsDir = new MMapDirectory(new File(dirPath));
      for (String file : fsDir.listAll()) {
        int idx = file.indexOf('.');
        if (idx != -1 && postingsExtensions.contains(file.substring(idx+1, file.length()))) {
          continue;
        }

        fsDir.copy(ramDir, file, file, IOContext.READ);
      }
      dir0 = new FileSwitchDirectory(postingsExtensions,
                                     fsDir,
                                     ramDir,
                                     true);
      if (doFacets) {
        facetsDir = new RAMDirectory(new SimpleFSDirectory(new File(facetsDirPath)), IOContext.READ);
      }
      */

    dir0 = od.open(Paths.get(dirPath));

    // TODO: NativeUnixDir?

    final String analyzer = args.getString("-analyzer");
    final String tasksFile = args.getString("-taskSource");
    final int numConcurrentQueries = args.getInt("-numConcurrentQueries");
    final String fieldName = args.getString("-field");
    final boolean printHeap = args.getFlag("-printHeap");
    final boolean doPKLookup = args.getFlag("-pk");
    // search concurrency: 0 means "disabled", -1 means "auto"
    int searchConcurrency = args.getInt("-searchConcurrency", 0);
    final int topN = args.getInt("-topN");
    final boolean doStoredLoads = args.getFlag("-loadStoredFields");
    final boolean exitable = args.getFlag("-exitable");
    final TestContext testContext = TestContext.parse(args.getString("-context", ""));

    if (searchConcurrency == -1) {
      searchConcurrency = Runtime.getRuntime().availableProcessors();
    }

    final ExecutorService executorService;
    if (searchConcurrency != 0) {
      executorService = new ThreadPoolExecutor(searchConcurrency, searchConcurrency, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
                                               new NamedThreadFactory("ConcurrentSearches"));
    } else {
      executorService = null;
    }

    // Used to choose which random subset of tasks we will
    // run, to generate the PKLookup tasks, and to generate
    // any random pct filters:
    final long staticRandomSeed = args.getLong("-staticSeed");

    // Used to shuffle the random subset of tasks:
    final long randomSeed = args.getLong("-seed");

    // TODO: this could be way better.
    final String similarity = args.getString("-similarity");
    // now reflect
    final Class<? extends Similarity> simClazz = 
      Class.forName("org.apache.lucene.search.similarities." + similarity).asSubclass(Similarity.class);
    final Similarity sim = simClazz.newInstance();

    System.out.println("Using dir impl " + dir0.getClass().getName());
    System.out.println("Analyzer " + analyzer);
    System.out.println("Similarity " + similarity);
    System.out.println("Number of concurrent queries " + numConcurrentQueries);
    System.out.println("topN " + topN);
    System.out.println("JVM " + (Constants.JRE_IS_64BIT ? "is" : "is not") + " 64bit");
    System.out.println("Pointer is " + RamUsageEstimator.NUM_BYTES_OBJECT_REF + " bytes");
    System.out.println("Concurrent segment reads is " + searchConcurrency);
 
    final Analyzer a;
    if (analyzer.equals("EnglishAnalyzer")) {
      a = new EnglishAnalyzer();
    } else if (analyzer.equals("ClassicAnalyzer")) {
      a = new ClassicAnalyzer();
    } else if (analyzer.equals("StandardAnalyzer")) {
      a = new StandardAnalyzer();
    } else if (analyzer.equals("StandardAnalyzerNoStopWords")) {
      a = new StandardAnalyzer(CharArraySet.EMPTY_SET);
    } else if (analyzer.equals("ShingleStandardAnalyzer")) {
      a = new ShingleAnalyzerWrapper(new StandardAnalyzer(CharArraySet.EMPTY_SET),
                                     2, 2, ShingleFilter.DEFAULT_TOKEN_SEPARATOR, true, true, ShingleFilter.DEFAULT_FILLER_TOKEN);
    } else {
      throw new RuntimeException("unknown analyzer " + analyzer);
    } 

    final ReferenceManager<IndexSearcher> mgr;
    final IndexWriter writer;
    final Directory dir;

    final String commit = args.getString("-commit");
    final String hiliteImpl = args.getString("-hiliteImpl");

    final String logFile = args.getString("-log");

    final long tSearcherStart = System.currentTimeMillis();

    final boolean verifyCheckSum = !args.getFlag("-skipVerifyChecksum");
    final boolean recacheFilterDeletes = args.getFlag("-recacheFilterDeletes");
    final String vectorDict;
    final Path vectorFilePath;
    final int vectorDimension;
    
    if (args.hasArg("-vectorDict")) {
      vectorDict = args.getString("-vectorDict");
      vectorFilePath = null;
      vectorDimension = -1;
    } else if (args.hasArg("-vectorFile")) {
      vectorFilePath = Paths.get(args.getString("-vectorFile"));
      if (args.hasArg("-vectorDimension")) {
        vectorDimension = args.getInt("-vectorDimension");
        if (vectorDimension < 1) {
          throw new RuntimeException("-vectorDimension must be > 0; got: " + vectorDimension);
        }
      } else {
        throw new RuntimeException("with -vectorFile you must also provide -vectorDimension");
      }
      vectorDict = null;
    } else {
      if (args.hasArg("-vectorDimension")) {
        throw new RuntimeException("with -vectorDimension you must also provide -vectorFile");
      }
      vectorDict = null;
      vectorFilePath = null;
      vectorDimension = -1;
    }

    if (recacheFilterDeletes) {
      throw new UnsupportedOperationException("recacheFilterDeletes was deprecated");
    }

    if (args.getFlag("-nrt")) {
      // TODO: get taxoReader working here too
      // TODO: factor out & share this CL processing w/ Indexer
      final int indexThreadCount = args.getInt("-indexThreadCount");
      final String lineDocsFile = args.getString("-lineDocsFile");
      final float docsPerSecPerThread = args.getFloat("-docsPerSecPerThread");
      final float reopenEverySec = args.getFloat("-reopenEverySec");
      final boolean storeBody = args.getFlag("-store");
      final boolean tvsBody = args.getFlag("-tvs");
      final boolean useCFS = args.getFlag("-cfs");
      final String defaultPostingsFormat = args.getString("-postingsFormat");
      final String idFieldPostingsFormat = args.getString("-idFieldPostingsFormat");
      final boolean verbose = args.getFlag("-verbose");
      final boolean cloneDocs = args.getFlag("-cloneDocs");
      final Mode mode = Mode.valueOf(args.getString("-mode", "update").toUpperCase(Locale.ROOT));

      final long reopenEveryMS = (long) (1000 * reopenEverySec);

      if (verbose) {
        InfoStream.setDefault(new PrintStreamInfoStream(System.out));
      }
      
      if (!dirImpl.equals("RAMExceptDirectPostingsDirectory")) {
        System.out.println("Wrap NRTCachingDirectory");
        dir0 = new NRTCachingDirectory(dir0, 20, 400.0);
      }

      dir = dir0;

      final IndexWriterConfig iwc = new IndexWriterConfig(a);
      iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
      iwc.setRAMBufferSizeMB(256.0);
      iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);

      // TODO: also RAMDirExceptDirect...?  need to
      // ... block deletes against wrapped FSDir?
      
      if (commit != null && commit.length() > 0) {
        System.out.println("Opening writer on commit=" + commit);
        iwc.setIndexCommit(PerfUtils.findCommitPoint(commit, dir));
      }

      ((TieredMergePolicy) iwc.getMergePolicy()).setNoCFSRatio(useCFS ? 1.0 : 0.0);
      //((TieredMergePolicy) iwc.getMergePolicy()).setMaxMergedSegmentMB(1024);
      //((TieredMergePolicy) iwc.getMergePolicy()).setReclaimDeletesWeight(3.0);
      //((TieredMergePolicy) iwc.getMergePolicy()).setMaxMergeAtOnce(4);

      final Codec codec = new Lucene103Codec() {
          @Override
          public PostingsFormat getPostingsFormatForField(String field) {
            return PostingsFormat.forName(field.equals("id") ?
                                          idFieldPostingsFormat : defaultPostingsFormat);
          }
        };
      iwc.setCodec(codec);

      final ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler) iwc.getMergeScheduler();
      // Only let one merge run at a time...
      // ... but queue up up to 4, before index thread is stalled:
      cms.setMaxMergesAndThreads(4, 1);

      iwc.setMergedSegmentWarmer(new IndexWriter.IndexReaderWarmer() {
          @Override
          public void warm(LeafReader reader) throws IOException {
            final long t0 = System.currentTimeMillis();
            //System.out.println("DO WARM: " + reader);
            IndexSearcher s = createIndexSearcher(reader, executorService);
            s.setQueryCache(null); // don't bench the cache
            s.search(new TermQuery(new Term(fieldName, "united")), 10);
            final long t1 = System.currentTimeMillis();
            System.out.println("warm segment=" + reader + " numDocs=" + reader.numDocs() + ": took " + (t1-t0) + " msec");
          }
        });
      
      writer = new IndexWriter(dir, iwc);
      System.out.println("Initial writer.maxDoc()=" + writer.getDocStats().maxDoc);

      // TODO: add -nrtBodyPostingsOffsets instead of
      // hardwired false:
      boolean addDVFields = mode == Mode.BDV_UPDATE || mode == Mode.NDV_UPDATE;
      LineFileDocs lineFileDocs = new LineFileDocs(lineDocsFile, false, storeBody, tvsBody, false, cloneDocs, null, null, null, addDVFields, null, 0, null);
      IndexThreads threads = new IndexThreads(new Random(17), writer, new AtomicBoolean(false), lineFileDocs, indexThreadCount, -1, false, false, mode, docsPerSecPerThread, null, -1.0, -1);
      threads.start();

      mgr = new SearcherManager(writer, new SearcherFactory() {
          @Override
          public IndexSearcher newSearcher(IndexReader reader, IndexReader previous) {
            IndexSearcher s = createIndexSearcher(reader, executorService);
            s.setQueryCache(null); // don't bench the cache
            s.setSimilarity(sim);
            return s;
          }
        });

      System.out.println("reopen every " + reopenEverySec);

      Thread reopenThread = new Thread() {
          @Override
          public void run() {
            try {
              final long startMS = System.currentTimeMillis();

              int reopenCount = 1;
              while (true) {
                final long sleepMS = startMS + (reopenCount * reopenEveryMS) - System.currentTimeMillis();
                if (sleepMS < 0) {
                	System.out.println("WARNING: reopen fell behind by " + Math.abs(sleepMS) + " ms");
                } else {
                	Thread.sleep(sleepMS);
                }

                Thread.sleep(sleepMS);
                mgr.maybeRefresh();
                reopenCount++;
                IndexSearcher s = mgr.acquire();
                try {
                  System.out.println(String.format(Locale.ENGLISH, "%.1fs: done reopen; writer.maxDoc()=%d; searcher.maxDoc()=%d; searcher.numDocs()=%d",
                                                   (System.currentTimeMillis() - startMS)/1000.0,
                                                   writer.getDocStats().maxDoc, s.getIndexReader().maxDoc(),
                                                   s.getIndexReader().numDocs()));
                } finally {
                  mgr.release(s);
                }
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };
      reopenThread.setName("ReopenThread");
      reopenThread.setPriority(4+Thread.currentThread().getPriority());
      reopenThread.start();

    } else {
      dir = dir0;
      writer = null;
      final DirectoryReader _reader;
      if (commit != null && commit.length() > 0) {
        System.out.println("Opening searcher on commit=" + commit);
        _reader = DirectoryReader.open(PerfUtils.findCommitPoint(commit, dir));
      } else {
        // open last commit
        _reader = DirectoryReader.open(dir);
      }
      // if exitable == true, wrap the directory readery by ExitableDirectoryReader with (almost) infinite timeout budget.
      final DirectoryReader reader = exitable ? ExitableDirectoryReader.wrap(_reader, new QueryTimeoutImpl(-1L)) : _reader;

      IndexSearcher s = createIndexSearcher(reader, executorService);
      s.setQueryCache(null); // don't bench the cache
      s.setSimilarity(sim);
      System.out.println("maxDoc=" + reader.maxDoc() + " numDocs=" + reader.numDocs() + " %tg live docs=" + (100.*reader.maxDoc()/reader.numDocs()));

      mgr = new SingleIndexSearcher(s);
    }

    System.out.println((System.currentTimeMillis() - tSearcherStart) + " msec to init searcher/NRT");

    {
      IndexSearcher s = mgr.acquire();
      try {
        System.out.println("Searcher: numDocs=" + s.getIndexReader().numDocs() + " maxDoc=" + s.getIndexReader().maxDoc());
        IndexSearcher.LeafSlice[] slices = IndexSearcher.slices(s.getIndexReader().leaves(), 250_000, 5, false);
        System.out.println("Reader has " + slices.length + " slices, from " + s.getIndexReader().leaves().size() + " segments:");
        // TODO: sort by descending segment size -- it makes it easier to eyeball the segment -> slice mapping.  OR, maybe just
        // print the slices not the segments?
        for (LeafReaderContext leaf : s.getIndexReader().leaves()) {
          System.out.println("  " + ((SegmentReader) leaf.reader()).getSegmentName() + " has maxDoc=" + leaf.reader().maxDoc());
        }
      } finally {
        mgr.release(s);
      }
    }

    //System.out.println("searcher=" + searcher);

    FacetsConfig facetsConfig = new FacetsConfig();
    facetsConfig.setHierarchical("Date.taxonomy", true);
    facetsConfig.setHierarchical("Date.sortedset", true);

    // all unique facet group fields ($facet alone, by default):
    final Set<String> facetFields = new HashSet<>();

    // facet dim name -> facet method
    final Map<String,Integer> facetDimMethods = new HashMap<>();
    if (args.hasArg("-facets")) {
      for(String arg : args.getStrings("-facets")) {
        String[] dims = arg.split(";");
        String facetGroupField;
        String facetMethod;
        if (dims[0].equals("taxonomy") || dims[0].equals("sortedset")) {
          // method --> use the default facet field for this group
          facetGroupField = FacetsConfig.DEFAULT_INDEX_FIELD_NAME;
          facetMethod = dims[0];
        } else {
          // method:indexFieldName --> use a custom facet field for this group
          int i = dims[0].indexOf(":");
          if (i == -1) {
            throw new IllegalArgumentException("-facets: expected (taxonomy|sortedset):fieldName but got " + dims[0]);
          }
          facetMethod = dims[0].substring(0, i);
          if (facetMethod.equals("taxonomy") == false && facetMethod.equals("sortedset") == false) {
            throw new IllegalArgumentException("-facets: expected (taxonomy|sortedset):fieldName but got " + dims[0]);
          }
          facetGroupField = dims[0].substring(i+1);
        }
        facetFields.add(facetGroupField);
        for(int i=1;i<dims.length;i++) {
          int flag;
          if (facetDimMethods.containsKey(dims[i])) {
            flag = facetDimMethods.get(dims[i]);
          } else {
            flag = 0;
          }
          if (facetMethod.equals("taxonomy")) {
            flag |= 1;
            facetsConfig.setIndexFieldName(dims[i]+".taxonomy", facetGroupField + ".taxonomy");
          } else {
            flag |= 2;
            facetsConfig.setIndexFieldName(dims[i]+".sortedset", facetGroupField + ".sortedset");
          }
          facetDimMethods.put(dims[i], flag);
        }
      }
    }

    TaxonomyReader taxoReader;
    Path taxoPath = Paths.get(args.getString("-indexPath"), "facets");
    Directory taxoDir = od.open(taxoPath);
    if (DirectoryReader.indexExists(taxoDir)) {
      taxoReader = new DirectoryTaxonomyReader(taxoDir);
      System.out.println("Taxonomy has " + taxoReader.getSize() + " ords");
    } else {
      taxoReader = null;
    }

    final Random staticRandom = new Random(staticRandomSeed);
    final Random random = new Random(randomSeed);

    final DirectSpellChecker spellChecker = new DirectSpellChecker();
    final IndexState indexState = new IndexState(executorService, mgr, taxoReader, fieldName, spellChecker, hiliteImpl, facetsConfig, facetDimMethods);

    VectorDictionary vectorDictionary;
    if (vectorDict != null) {
      Float scale = args.getFloat("-vectorScale", null);
      long start = System.nanoTime();
      if (vectorDict.charAt(0) == '(') {
        // python sends as a tuple
        String[] parts = vectorDict.substring(1, vectorDict.length() - 1).split(", ");
        // strip off quotes
        String tokenFile = parts[0].substring(1, parts[0].length() - 1);
        String vectorFile = parts[1].substring(1, parts[1].length() - 1);
        int dim = Integer.parseInt(parts[2]);
        if (scale != null) {
          vectorDictionary = VectorDictionary.create(tokenFile, vectorFile, dim, scale, VectorEncoding.BYTE);
        } else {
          vectorDictionary = VectorDictionary.create(tokenFile, vectorFile, dim, 0, VectorEncoding.FLOAT32);
        }
      } else {
        if (scale != null) {
          vectorDictionary = VectorDictionary.create(vectorDict, scale, VectorEncoding.BYTE);
        } else {
          vectorDictionary = VectorDictionary.create(vectorDict, 0, VectorEncoding.FLOAT32);
        }
      }
      System.out.println("vector dictionary loaded from " + vectorDict + " in " + nsToMS(System.nanoTime() - start) + "ms");
    } else {
      vectorDictionary = null;
    }
    TaskParserFactory taskParserFactory =
      new TaskParserFactory(indexState, fieldName, a, "body", topN, random, vectorDictionary, vectorFilePath, vectorDimension, doStoredLoads, testContext);

    final TaskSource tasks;

    try (TaskParser taskParser = taskParserFactory.getTaskParser()) {
      if (tasksFile.startsWith("server:")) {
        // TODO: what is this "server:" tasks source!?  does it still work?
        int idx = tasksFile.indexOf(':', 8);
        if (idx == -1) {
          throw new RuntimeException("server is missing the port; should be server:interface:port (got: " + tasksFile + ")");
        }
        String iface = tasksFile.substring(7, idx);
        int port = Integer.valueOf(tasksFile.substring(1+idx));
        RemoteTaskSource remoteTasks = new RemoteTaskSource(iface, port, numConcurrentQueries, taskParser);

        // nocommit must stop thread?
        tasks = remoteTasks;
      } else {
        // Load the tasks from a file:
        final int taskRepeatCount = args.getInt("-taskRepeatCount");
        final int numTaskPerCat = args.getInt("-tasksPerCat");
        final boolean groupByCat = args.getFlag("-groupByCat");
        tasks = new LocalTaskSource(indexState, tasksFile, taskParser, staticRandom, random,
                                    numTaskPerCat, taskRepeatCount, doPKLookup, groupByCat);
        System.out.println("Task repeat count " + taskRepeatCount);
        System.out.println("Tasks file " + tasksFile);
        System.out.println("Num task per cat " + numTaskPerCat);
      }
    }

    args.check();

    // Evil respeller:
    //spellChecker.setMinPrefix(0);
    //spellChecker.setMaxInspections(1024);

    // set by the first coordinator thread that sees end of tasks:
    AtomicReference<ThreadDetails> endThreadDetailsRef = new AtomicReference<>();
    
    final TaskThreads taskThreads = new TaskThreads(tasks, indexState, numConcurrentQueries, taskParserFactory, endThreadDetailsRef);
    Thread.sleep(10);

    final long startNanos = System.nanoTime();
    taskThreads.start();

    // TODO: pull this into thread so that if tasks finish before warmup, we break out of this sleep and exit with TestWasTooShortException!!
    Thread.sleep(WARMUP_MSEC);
    final long postWarmupNanos = System.nanoTime();

    // capture CPU of all running threads, after warmup:
    ThreadDetails startThreadDetails = new ThreadDetails();
    taskThreads.finish();
    final long finishNanos = System.nanoTime();

    ThreadDetails endThreadDetails = endThreadDetailsRef.get();

    double avgCPUCount = -1d;
    double elapsedMS = nsToMS(endThreadDetails.ns - startThreadDetails.ns);

    if (Arrays.equals(startThreadDetails.threadIDs, endThreadDetails.threadIDs)) {
      // only report CPU stats if post-warmup runtime is at least as long as warmup:
      if ((finishNanos - postWarmupNanos) > msToNS(WARMUP_MSEC)) {
        long sumCPUTimeNS = 0;
        for(int i=0;i<startThreadDetails.threadIDs.length;i++) {
          sumCPUTimeNS += endThreadDetails.cpuTimesNS[i] - startThreadDetails.cpuTimesNS[i];
          System.out.println("thread " + startThreadDetails.threadIDs[i] + " name=" + startThreadDetails.threadInfos[i].getThreadName() + " cpu@start=" + startThreadDetails.cpuTimesNS[i] + " cpu@end=" + endThreadDetails.cpuTimesNS[i] +
                             " deltaMS=" + nsToMS(endThreadDetails.cpuTimesNS[i] - startThreadDetails.cpuTimesNS[i]));
        }
        avgCPUCount = nsToMS(sumCPUTimeNS) / elapsedMS;
                                               
        System.out.println("\nAverage CPU cores used: " + avgCPUCount);
      } else {
        System.out.println("\nAverage CPU cores used: -1\n  (test run was too short)");
      }
    } else {
      System.out.println("NOTE: start/end threads changed; cannot compute average CPU");
      System.out.println("\nStart threads:");
      for(int i=0;i<startThreadDetails.threadIDs.length;i++) {
        System.out.println(i + ": " + startThreadDetails.threadIDs[i] + " -> " + startThreadDetails.threadInfos[i].getThreadName() + " CPU=" + startThreadDetails.cpuTimesNS[i]);
      }
      System.out.println("\nEnd threads:");
      for(int i=0;i<endThreadDetails.threadIDs.length;i++) {
        System.out.println(i + ": " + endThreadDetails.threadIDs[i] + " -> " + endThreadDetails.threadInfos[i].getThreadName());
      }
      // throw new IllegalStateException("thread IDs changed: " + startThreadDetails.threadIDs.length + " vs " endThreadDetails.threadIDs.length);
      System.out.println("\nAverage CPU cores used: -1\n  (thread IDs changed during run; maybe due to attached jstack during run?)");
    }

    final List<Task> allTasks = tasks.getAllTasks();

    PrintStream out = new PrintStream(logFile);

    if (allTasks != null) {
      // Tasks were local: verify checksums:

      // indexState.setDocIDToID();

      final Map<Task,Task> tasksSeen = new HashMap<Task,Task>();

      out.println("\nStart of tasks winddown: " + nsToMS(endThreadDetails.ns - startNanos) + " msec");
      out.println("\nElapsed MS (excluding warmup and winddown): " + elapsedMS);
      out.println("\nAverage CPU cores used: " + avgCPUCount);
      out.println("\nResults for " + allTasks.size() + " tasks:");

      boolean fail = false;
      for(final Task task : allTasks) {
        if (verifyCheckSum) {
          final Task other = tasksSeen.get(task);
          if (other != null) {
            if (task.checksum() != other.checksum()) {
              System.out.println("\nTASK:");
              task.printResults(System.out, indexState);
              System.out.println("\nOTHER TASK:");
              other.printResults(System.out, indexState);
              fail = true;
              //throw new RuntimeException("task " + task + " hit different checksums: " + task.checksum() + " vs " + other.checksum() + " other=" + other);
            }
          } else {
            tasksSeen.put(task, task);
          }
        }
        out.println("\nTASK: " + task);
        out.println("  " + nsToMS(task.runTimeNanos) + " msec @ " + ((task.startTimeNanos - startNanos)/1000000.0) + " msec");
        out.println("  thread " + task.threadID);
        task.printResults(out, indexState);
      }
      if (fail) {
        throw new RuntimeException("some tasks got different results across different threads");
      }

      allTasks.clear();
    }

    if (executorService != null) {
      executorService.shutdownNow();
    }

    mgr.close();

    if (taxoReader != null) {
      taxoReader.close();
    }

    if (writer != null) {
      // Don't actually commit any index changes:
      writer.rollback();
    }

    dir.close();

    if (printHeap) {

      // Try to get RAM usage -- some ideas poached from http://www.javaworld.com/javaworld/javatips/jw-javatip130.html
      final Runtime runtime = Runtime.getRuntime();
      long usedMem1 = PerfUtils.usedMemory(runtime);
      long usedMem2 = Long.MAX_VALUE;
      for(int iter = 0; iter < 10; iter++) {
        // runtime.runFinalization();
        runtime.gc();
        Thread.yield();
        Thread.sleep(100);
        usedMem2 = usedMem1;
        usedMem1 = PerfUtils.usedMemory(runtime);
      }
      out.println("\nHEAP: " + PerfUtils.usedMemory(runtime));
    }
    out.close();
  }

  private static IndexSearcher createIndexSearcher(IndexReader reader, ExecutorService executorService) {
      return new IndexSearcher(reader, executorService);
  }
}
