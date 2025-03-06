# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

try:
  from pygnuplot import gnuplot as Gnuplot
except ImportError:
  Gnuplot = None


class IndexChart(object):
  def __init__(self, log_file, competitor, out_base=".", start_sec=50, end_sec=200):
    self.log_file = log_file
    self.start_ms = start_sec * 1000
    self.start_sec = start_sec
    self.end_ms = end_sec * 1000
    self.end_sec = end_sec
    self.competitor = competitor
    self.out_base = out_base
    self.terminal = "png"

  def buildRaw(self, meta=None):
    if not meta:
      meta = {}
    lines = open(self.log_file, "r").readlines()
    raw_data = []
    raw_flush = []
    for line in lines:
      if line.startswith("ingest: "):
        _, docs, millis = line.split(" ")
        raw_data.append((float(docs), int(millis)))
      elif line.startswith("flush:"):
        _, byte, begin, end = line.split(" ")
        begin = int(begin)
        duration = int(end) - begin
        bps = int(byte) / (duration / 1000.0)
        raw_flush.append((bps, duration, begin))
      elif line.startswith("Threads"):
        meta["threads"] = line.split(" ")[1].strip()
      elif line.startswith("RAM Buffer MB"):
        meta["ram"] = line.split(":")[1].strip()
      elif line.startswith("Dir"):
        meta["dir"] = line.split(" ")[1].strip()
      elif line.startswith("Indexer: indexing done"):
        meta["total"] = int(line.split(" ")[3][1:]) / 1000.0
        meta["docs"] = int(line.split(" ")[6])
      elif line.startswith("Indexer: waitForMerges done"):
        meta["merge_total"] = int(line.split(" ")[3][1:]) / 1000.0
      elif line.startswith("Indexer: commit"):
        meta["commit_total"] = int(line.split(" ")[4]) / 1000.0
      elif line.startswith("startIngest:"):
        meta["start_millis"] = int(line.split(" ")[1])

    return raw_data, raw_flush

  def buildStats(self, meta=None):
    raw_data, raw_flush = self.buildRaw(meta)
    start = 0
    last_window_start = 0
    docs_per_second = []
    flush_stats = []
    for docs, millis in raw_data:
      if start == 0:
        start = millis
      if (millis - start) > (self.end_ms):
        break
      if (millis - start) > (self.start_ms):
        if last_window_start == 0:
          last_window_start = millis
        else:
          docs_per_second.append((docs, self.start_sec + ((millis - last_window_start) / 1000.0)))
    if "start_millis" in meta:
      start_millis = meta["start_millis"]
      for bps, duration, begin in raw_flush:
        start_ms = begin - start_millis
        end_ms = start_ms + duration
        mbs = bps / 1024.0 / 1024.0
        if self.end_ms < end_ms:
          break
        one_set = [(0, start_ms / 1000.0), (mbs, start_ms / 1000.0), (mbs, end_ms / 1000.0), (0, end_ms / 1000.0)]
        flush_stats.append(one_set)
    return docs_per_second, flush_stats

  def plot(self):
    meta = {"competitor": self.competitor}
    docs_per_second, flush = self.buildStats(meta)
    gp = Gnuplot.Gnuplot(persist=1)
    title = (
      "%(competitor)s No. Threads: %(threads)s RAM Buffer: %(ram)s MB\\n Directory: %(dir)s numDocs: %(docs)d \\n indexing: %(total)d sec \\n merges: %(merge_total)d sec. \\n commit: %(commit_total)d sec."
      % meta
    )
    x = [sec for _, sec in docs_per_second]
    y = [docs for docs, sec in docs_per_second]
    if not x:
      print("not enough data collected for indexing stats - skipping")
      return
    kw = {"title": "ingest rate", "with": "lines"}
    data = Gnuplot.Data(x, y, **kw)
    gp('set title "%s"' % (title))
    gp('set style line 1 lc rgb "green"')
    gp.set_range("yrange", "[0 : 60000]")
    gp.set_range("xrange", "[%d : %d]" % (self.start_sec, self.end_sec))
    gp.xlabel("seconds")
    gp.ylabel("documents per second")
    gp.plot(data)
    gp.save("/tmp/foo.txt")
    gp.hardcopy(filename="%s/%s_dps.png" % (self.out_base, self.competitor), terminal=self.terminal)
    gp.close()
    if flush:
      gp = Gnuplot.Gnuplot(persist=1)
      title = (
        "%(competitor)s No. Threads: %(threads)s RAM Buffer: %(ram)s MB\\n Directory: %(dir)s numDocs: %(docs)d \\n indexing: %(total)d sec \\n merges: %(merge_total)d sec. \\n commit: %(commit_total)d sec."
        % meta
      )
      gp('set title "%s"' % (title))
      gp("set style data filledcurves")
      gp('set xlabel "seconds"')
      gp('set ylabel "flushing MB/sec"')
      gp("set style fill solid 0.25 border")
      for one_set in flush:
        data = Gnuplot.Data(x, y, title="")
        x = [sec for _, sec in one_set]
        y = [bps for bps, _ in one_set]
        # gp('set style fill pattern 2')
        gp("set xrange [%d : %d]" % (self.start_sec, self.end_sec))
        gp("set yrange [0 : 50]")
        gp.replot(data)

      gp.hardcopy(filename="%s/%s_flush.png" % (self.out_base, self.competitor), terminal=self.terminal)
      gp.close()


# if you want to get flushing rate for RT apply this patch
"""

Index: lucene/src/java/org/apache/lucene/index/DocumentsWriterFlushControl.java
===================================================================
--- lucene/src/java/org/apache/lucene/index/DocumentsWriterFlushControl.java	(revision 1086947)
+++ lucene/src/java/org/apache/lucene/index/DocumentsWriterFlushControl.java	(working copy)
@@ -50,7 +50,7 @@
   private final DocumentsWriterPerThreadPool perThreadPool;
   private final FlushPolicy flushPolicy;
   private boolean closed = false;
-  private final HashMap<DocumentsWriterPerThread, Long> flushingWriters = new HashMap<DocumentsWriterPerThread, Long>();
+  private final HashMap<DocumentsWriterPerThread, FlushStats> flushingWriters = new HashMap<DocumentsWriterPerThread, FlushStats>();
   private final BufferedDeletes pendingDeletes;
 
   DocumentsWriterFlushControl(FlushPolicy flushPolicy,
@@ -123,8 +123,9 @@
   synchronized void doAfterFlush(DocumentsWriterPerThread dwpt) {
     assert flushingWriters.containsKey(dwpt);
     numFlushing--;
-    Long bytes = flushingWriters.remove(dwpt);
-    flushBytes -= bytes.longValue();
+    FlushStats stats = flushingWriters.remove(dwpt);
+    flushBytes -= stats.bytes;
+    System.out.println(stats);
     perThreadPool.recycle(dwpt);
     healthiness.updateStalled(this);
   }
@@ -172,7 +173,7 @@
             dwpt = perThreadPool.replaceForFlush(perThread, closed);
             assert !flushingWriters.containsKey(dwpt) : "DWPT is already flushing";
             // record the flushing DWPT to reduce flushBytes in doAfterFlush
-            flushingWriters.put(dwpt, Long.valueOf(bytes));
+            flushingWriters.put(dwpt, new FlushStats(bytes, System.currentTimeMillis()));
             numPending--; // write access synced
             numFlushing++;
             return dwpt;
@@ -260,4 +261,20 @@
   int numActiveDWPT() {
     return this.perThreadPool.getMaxThreadStates();
   }
+  
+  private static class FlushStats {
+    final long bytes;
+    final long timeInMillis;
+    
+    public FlushStats(long bytes, long timeInMillis) {
+      this.bytes = bytes;
+      this.timeInMillis = timeInMillis;
+    }
+    @Override
+    public String toString() {
+      return "flush: " + bytes + " " + timeInMillis + " " + System.currentTimeMillis();
+    }
+    
+    
+  }
 }
\ No newline at end of file



"""
