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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50Codec;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spell.DirectSpellChecker;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

import perf.IndexThreads.Mode;

// cd /a/lucene/trunk/checkout
// ln -s /path/to/lucene/util/perf .
// ant compile; javac -cp ../modules/analysis/build/common/classes/java:build/classes/java:build/classes/test perf/NRTPerfTest.java perf/LineFileDocs.java
// java -Xmx2g -Xms2g -server -Xbatch -cp .:lib/junit-4.7.jar:../modules/analysis/build/common/classes/java:build/classes/java:build/classes/test perf.NRTPerfTest MMapDirectory /p/lucene/indices/wikimedium.clean.svn.Standard.nd10M/index multi /lucene/data/enwiki-20110115-lines-1k-fixed.txt 17 1000 1000 2 1 1 update 1 no

// TODO
//   - maybe target certain MB/sec update rate...?
//   - hmm: we really should have a separate line file, shuffled, that holds the IDs for each line; this way we can update doc w/ same original doc and then we can assert hit counts match
//   - share *Task code from SearchPerfTest

public class NRTPerfTest {

	static final class MergedReaderWarmer extends IndexWriter.IndexReaderWarmer {

		private final String field;

		MergedReaderWarmer(String field) {
			this.field = field;
		}

		@Override
		public void warm(LeafReader reader) throws IOException {
			final long t0 = System.currentTimeMillis();
			//System.out.println("DO WARM: " + reader);
			IndexSearcher s = new IndexSearcher(reader);
			s.search(new TermQuery(new Term(field, "10")), 10);

			// Warm terms dict & index:
			/*
		  final TermsEnum te = reader.fields().terms("body").iterator();
		  long sumDocs = 0;
		  DocsEnum docs = null;
		  int counter = 0;
		  final List<BytesRef> terms = new ArrayList<BytesRef>();
		  while(te.next() != null) {
		    docs = te.docs(null, docs);
		    if (counter++ % 50 == 0) {
		      terms.add(new BytesRef(te.term()));
		    }
		    int docID;
		    while((docID = docs.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
		      sumDocs += docID;
		    }
		  }
		  Collections.reverse(terms);

		  System.out.println("warm: " + terms.size() + " terms");
		  for(BytesRef term : terms) {
		    sumDocs += reader.docFreq("body", term);
		  }
			 */
			final long t1 = System.currentTimeMillis();
			System.out.println("warm took " + (t1-t0) + " msec");

			//NativePosixUtil.mlockTermsDict(reader, "id");
		}
	}

	static final class ReopenThread extends Thread {

		private final double reopenPerSec;

		private final SearcherManager manager;

		private final AtomicInteger[] reopensByTime;

		private final double runTimeSec;

		ReopenThread(double reopenPerSec, SearcherManager manager, AtomicInteger[] reopensByTime, double runTimeSec) {
			this.reopenPerSec = reopenPerSec;
			this.manager = manager;
			this.reopensByTime = reopensByTime;
			this.runTimeSec = runTimeSec;
		}

		@Override
		public void run() {
			try {
				final long startMS = System.currentTimeMillis();
				final long stopMS = startMS + (long) (runTimeSec * 1000);

				int reopenCount = 1;
				while (true) {
					final long t = System.currentTimeMillis();
					if (t >= stopMS) {
						break;
					}

					final long sleepMS = startMS + (long) (1000*(reopenCount/reopenPerSec)) - System.currentTimeMillis();
					if (sleepMS < 0) {
						System.out.println("WARNING: reopen fell behind by " + Math.abs(sleepMS) + " ms");
					} else {
						Thread.sleep(sleepMS);
					}

					IndexSearcher curS = manager.acquire();
					try {
						final long tStart = System.nanoTime();
						manager.maybeRefresh();
						++reopenCount;
						IndexSearcher newS = manager.acquire();
						try {
							if (curS != newS) {
								System.out.println("Reopen: " + String.format("%9.4f", (System.nanoTime() - tStart)/1000000.0) + " msec");
								reopensByTime[currentQT.get()].incrementAndGet();
							} else {
								System.out.println("WARNING: no changes on reopen");
							}
						} finally {
							manager.release(newS);
						}
					} finally {
						manager.release(curS);
					}
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	static class RandomTaskSource implements TaskSource {

		private final List<Task> tasks;
		private final AtomicInteger nextTask = new AtomicInteger();
		private final int numTasks;

		public RandomTaskSource(TaskParser taskParser, String tasksFile, Random random) throws IOException, ParseException {
			tasks = LocalTaskSource.loadTasks(taskParser, tasksFile);
			numTasks = tasks.size();
			Collections.shuffle(tasks, random);
			System.out.println("TASK LEN=" + tasks.size());
		}

		@Override
		public List<Task> getAllTasks() {
			return tasks;
		}

		@Override
		public Task nextTask() {
			final int next = nextTask.getAndIncrement() % numTasks;
			return tasks.get(next);
		}

		@Override
		public void taskDone(Task task, long queueTimeNS, int toalHitCount) {}
	}

	static final AtomicInteger currentQT = new AtomicInteger();
	static AtomicInteger[] docsIndexedByTime;
	static AtomicInteger[] searchesByTime;
	static AtomicLong[] totalUpdateTimeByTime; 
	static int statsEverySec;

	public static void main(String[] args) throws Exception {

		final String dirImpl = args[0];
		final String dirPath = args[1];
		final String commit = args[2];
		final String lineDocFile = args[3];
		final long seed = Long.parseLong(args[4]);
		final double docsPerSec = Double.parseDouble(args[5]);
		final double runTimeSec = Double.parseDouble(args[6]);
		final int numSearchThreads = Integer.parseInt(args[7]);
		int numIndexThreads = Integer.parseInt(args[8]);
		if (numIndexThreads > docsPerSec) {
			System.out.println("INFO: numIndexThreads higher than docsPerSec, adjusting numIndexThreads");
			numIndexThreads = (int) Math.max(1, docsPerSec);
		}
		final double reopenPerSec = Double.parseDouble(args[9]);
		final Mode mode = Mode.valueOf(args[10].toUpperCase(Locale.ROOT));
		statsEverySec = Integer.parseInt(args[11]);
		final boolean doCommit = args[12].equals("yes");
		final double mergeMaxWriteMBPerSec = Double.parseDouble(args[13]);
		if (mergeMaxWriteMBPerSec != 0.0) {
			throw new IllegalArgumentException("mergeMaxWriteMBPerSec must be 0.0 until LUCENE-3202 is done");
		}
		final String tasksFile = args[14];
		if (Files.notExists(Paths.get(tasksFile))) {
			throw new FileNotFoundException("tasks file not found " + tasksFile);
		}

		final boolean hasProcMemInfo = Files.exists(Paths.get("/proc/meminfo"));

		System.out.println("DIR=" + dirImpl);
		System.out.println("Index=" + dirPath);
		System.out.println("Commit=" + commit);
		System.out.println("LineDocs=" + lineDocFile);
		System.out.println("Docs/sec=" + docsPerSec);
		System.out.println("Run time sec=" + runTimeSec);
		System.out.println("NumSearchThreads=" + numSearchThreads);
		System.out.println("NumIndexThreads=" + numIndexThreads);
		System.out.println("Reopen/sec=" + reopenPerSec);
		System.out.println("Mode=" + mode);
		System.out.println("tasksFile=" + tasksFile);

		System.out.println("Record stats every " + statsEverySec + " seconds");
		final int count = (int) ((runTimeSec / statsEverySec) + 2);
		docsIndexedByTime = new AtomicInteger[count];
		searchesByTime = new AtomicInteger[count];
		totalUpdateTimeByTime = new AtomicLong[count];
		final AtomicInteger reopensByTime[] = new AtomicInteger[count];
		for (int i = 0; i < count; i++) {
			docsIndexedByTime[i] = new AtomicInteger();
			searchesByTime[i] = new AtomicInteger();
			totalUpdateTimeByTime[i] = new AtomicLong();
			reopensByTime[i] = new AtomicInteger();
		}

		System.out.println("Max merge MB/sec = " + (mergeMaxWriteMBPerSec <= 0.0 ? "unlimited" : mergeMaxWriteMBPerSec));
		final Random random = new Random(seed);

		final LineFileDocs docs = new LineFileDocs(lineDocFile, true, false, false, false, false, null, new HashSet<String>(), null, true);

		final Directory dir0;
		if (dirImpl.equals("MMapDirectory")) {
			dir0 = new MMapDirectory(Paths.get(dirPath));
		} else if (dirImpl.equals("NIOFSDirectory")) {
			dir0 = new NIOFSDirectory(Paths.get(dirPath));
		} else if (dirImpl.equals("SimpleFSDirectory")) {
			dir0 = new SimpleFSDirectory(Paths.get(dirPath));
		} else {
			docs.close();
			throw new RuntimeException("unknown directory impl \"" + dirImpl + "\"");
		}
		//final NRTCachingDirectory dir = new NRTCachingDirectory(dir0, 10, 200.0, mergeMaxWriteMBPerSec);
		final NRTCachingDirectory dir = new NRTCachingDirectory(dir0, 20, 400.0);
		//final MergeScheduler ms = dir.getMergeScheduler();
		//final Directory dir = dir0;
		//final MergeScheduler ms = new ConcurrentMergeScheduler();

		final String field = "body";

		// Open an IW on the requested commit point, but, don't
		// delete other (past or future) commit points:
		// TODO take Analyzer as parameter
		StandardAnalyzer analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET);
		final IndexWriterConfig conf = new IndexWriterConfig(analyzer);
		conf.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
		conf.setRAMBufferSizeMB(256.0);
		conf.setMaxThreadStates(numIndexThreads);
		//iwc.setMergeScheduler(ms);

		final Codec codec = new Lucene50Codec() {
			@Override
			public PostingsFormat getPostingsFormatForField(String field) {
				if (field.equals("id")) {
					return PostingsFormat.forName("Memory");
				} else {
					return PostingsFormat.forName("Lucene50");
				}
			}

			private final DocValuesFormat direct = DocValuesFormat.forName("Direct");
			@Override
			public DocValuesFormat getDocValuesFormatForField(String field) {
				return direct;
			}
		};

		conf.setCodec(codec);

		/*
    iwc.setMergePolicy(new LogByteSizeMergePolicy());
    ((LogMergePolicy) iwc.getMergePolicy()).setUseCompoundFile(false);
    ((LogMergePolicy) iwc.getMergePolicy()).setMergeFactor(30);
    ((LogByteSizeMergePolicy) iwc.getMergePolicy()).setMaxMergeMB(10000.0);
    System.out.println("USING LOG BS MP");
		 */

		TieredMergePolicy tmp = new TieredMergePolicy();
		tmp.setNoCFSRatio(0.0);
		tmp.setMaxMergedSegmentMB(1000000.0);
		//tmp.setReclaimDeletesWeight(3.0);
		//tmp.setMaxMergedSegmentMB(7000.0);
		conf.setMergePolicy(tmp);

		if (!commit.equals("none")) {
			conf.setIndexCommit(PerfUtils.findCommitPoint(commit, dir));
		}

		// Make sure merges run @ higher prio than indexing:
		final ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler) conf.getMergeScheduler();
		cms.setMaxMergesAndThreads(4, 1);

		conf.setMergedSegmentWarmer(new MergedReaderWarmer(field));

		final IndexWriter w = new IndexWriter(dir, conf);
		// w.setInfoStream(System.out);

		IndexThreads.UpdatesListener updatesListener = new IndexThreads.UpdatesListener() {
			long startTimeNS;
			@Override
			public void beforeUpdate() {
				startTimeNS = System.nanoTime();
			}
			@Override
			public void afterUpdate() {
				int idx = currentQT.get();
				totalUpdateTimeByTime[idx].addAndGet(System.nanoTime() - startTimeNS);
				docsIndexedByTime[idx].incrementAndGet();
			}
		};
		IndexThreads indexThreads = new IndexThreads(random, w, docs, numIndexThreads, -1, false, false, mode,
                                                             (float) (docsPerSec / numIndexThreads), updatesListener, -1.0, -1);

		// NativePosixUtil.mlockTermsDict(startR, "id");
		final SearcherManager manager = new SearcherManager(w, true, null);
		IndexSearcher s = manager.acquire();
		try {
			System.out.println("Reader=" + s.getIndexReader());
		} finally {
			manager.release(s);
		}

		final DirectSpellChecker spellChecker = new DirectSpellChecker();
		final IndexState indexState = new IndexState(manager, null, field, spellChecker, "PostingsHighlighter", null);
		final Map<Double,Filter> filters = new HashMap<Double,Filter>();
		final QueryParser qp = new QueryParser(field, analyzer);
		qp.setLowercaseExpandedTerms(false);
		TaskParser taskParser = new TaskParser(indexState, qp, field, filters, 10, random, true);
		final TaskSource tasks = new RandomTaskSource(taskParser, tasksFile, random) {
			@Override
			public void taskDone(Task task, long queueTimeNS, int toalHitCount) {
				searchesByTime[currentQT.get()].incrementAndGet();
			}
		};
		System.out.println("Task repeat count 1");
		System.out.println("Tasks file " + tasksFile);
		System.out.println("Num task per cat 20");
		final TaskThreads taskThreads = new TaskThreads(tasks, indexState, numSearchThreads);

		final ReopenThread reopenThread = new ReopenThread(reopenPerSec, manager, reopensByTime, runTimeSec);
		reopenThread.setName("ReopenThread");
		reopenThread.setPriority(4+Thread.currentThread().getPriority());
		System.out.println("REOPEN PRI " + reopenThread.getPriority());

		indexThreads.start();
		reopenThread.start();
		taskThreads.start();

		Thread.currentThread().setPriority(5+Thread.currentThread().getPriority());
		System.out.println("TIMER PRI " + Thread.currentThread().getPriority());

		//System.out.println("Start: " + new Date());

		final long startMS = System.currentTimeMillis();
		final long stopMS = startMS + (long) (runTimeSec * 1000);
		int lastQT = -1;
		while (true) {
			final long t = System.currentTimeMillis();
			if (t >= stopMS) {
				break;
			}
			final int qt = (int) ((t-startMS)/statsEverySec/1000);
			currentQT.set(qt);
			if (qt != lastQT) {
				final int prevQT = lastQT;
				lastQT = qt;
				if (prevQT > 0) {
					final String other;
					if (hasProcMemInfo) {
						other = " D=" + getLinuxDirtyBytes();
					} else {
						other = "";
					}
					int prev = prevQT - 1;
					System.out.println(String.format("QT %d searches=%d docs=%d reopens=%s totUpdateTime=%d", 
							prev, 
							searchesByTime[prev].get(),
							docsIndexedByTime[prev].get(),
							reopensByTime[prev].get() + other,
							TimeUnit.NANOSECONDS.toMillis(totalUpdateTimeByTime[prev].get())));
				}
			}
			Thread.sleep(25);
		}

		taskThreads.stop();
		reopenThread.join();
		indexThreads.stop();

		System.out.println("By time:");
		for (int i = 0; i < searchesByTime.length - 2; i++) {
			System.out.println(String.format("  %d searches=%d docs=%d reopens=%d totUpdateTime=%d", 
					i*statsEverySec,
					searchesByTime[i].get(),
					docsIndexedByTime[i].get(),
					reopensByTime[i].get(),
					TimeUnit.NANOSECONDS.toMillis(totalUpdateTimeByTime[i].get())));
		}

		manager.close();
		if (doCommit) {
                  w.close();
		} else {
                  w.rollback();
		}
	}

	private static long getLinuxDirtyBytes() throws Exception {
		final BufferedReader br = new BufferedReader(new FileReader("/proc/meminfo"), 4096);
		int dirtyKB = -1;
		try {
			while(true) {
				String line = br.readLine();
				if (line == null) {
					break;
				} else if (line.startsWith("Dirty:")) {
					final String trimmed = line.trim();
					dirtyKB = Integer.parseInt(trimmed.substring(7, trimmed.length()-3).trim());
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace(System.out);
		} finally {
			br.close();
		}

		return dirtyKB;
	}
}
