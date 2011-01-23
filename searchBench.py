#!/usr/bin/env python


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

import time
import sys
import os
import benchUtil
import common
import constants

# TODO
#  - add sort-by-title

if '-ea' in sys.argv:
  JAVA_COMMAND += ' -ea:org.apache.lucene...'

INDEX_NUM_THREADS = constants.INDEX_NUM_THREADS
if '-debug' in sys.argv:
  INDEX_NUM_DOCS = 100000
else:
  INDEX_NUM_DOCS = 10000000

# This is #docs in /lucene/data/enwiki-20100302-pages-articles-lines-1k.txt:
# INDEX_NUM_DOCS = 24900504

# Must be evenly divisible by number of threads:
INDEX_NUM_DOCS -= INDEX_NUM_DOCS % INDEX_NUM_THREADS

WIKI_LINE_FILE = constants.WIKI_LINE_FILE

osName = common.osName

def run(*competitors):  
  r = benchUtil.RunAlgs(constants.JAVA_COMMAND)
  if '-noc' not in sys.argv:
    for c in competitors:
      r.compile(c)
  search = '-search' in sys.argv
  index  = '-index' in sys.argv

  if index:
    seen = set()
    for c in competitors:
      if c.index not in seen:
        seen.add(c.index)

    if len(seen) == 1:
      # if all jobs are going to share single index, use many threads
      numThreads = INDEX_NUM_THREADS
    else:
      # else we must use 1 thread so indices are identical
      numThreads = 1

    seen = set()
    for c in competitors:
      if c.index not in seen:
        seen.add(c.index)
        r.makeIndex(c.index)

  logUpto = 0
  if not search:
    return
  if '-debugs' in sys.argv or '-debug' in sys.argv:
    iters = 1
    itersPerJVM = 15
    threads = 1
  else:
    iters = 2
    itersPerJVM = 40
    threads = 4

  results = {}
  for c in competitors:
    print 'Search on %s...' % c.checkout
    if osName == 'linux':
      benchUtil.run("sudo %s/dropCaches.sh" % constants.BENCH_BASE_DIR)
    t0 = time.time()
    results[c] = r.runSimpleSearchBench(c, iters, itersPerJVM, threads, filter=None)
    print '  %.2f sec' % (time.time() - t0)

  r.simpleReport(results[competitors[0]],
                 results[competitors[1]],
                 '-jira' in sys.argv,
                 '-html' in sys.argv,
                 cmpDesc=competitors[1].name,
                 baseDesc=competitors[0].name)


class Competitor(object):
  TASKS = { "search" : "perf.SearchTask",
          "loadIdFC" : "perf.LoadFieldCacheSearchTask",
          "loadIdDV" : "perf.values.DocValuesSearchTask"}

  def __init__(self, name, checkout, index, dirImpl, analyzer, commitPoint, task='search'):
    self.name = name
    self.index = index
    self.checkout = checkout
    self.searchTask = self.TASKS[task];
    self.commitPoint = commitPoint
    self.dirImpl = dirImpl
    self.analyzer = analyzer

  def compile(self, cp):
    benchUtil.run('javac -cp %s perf/*.java >> compile.log 2>&1' % cp,  'compile.log')

  def setTask(self, task):
    self.searchTask = self.TASKS[task];
    return self

  def taskRunProperties(self):
    return '-Dtask.type=%s' % self.searchTask

class DocValueCompetitor(Competitor):
  def __init__(self, sourceDir, index, task='loadIdDV'):
    Competitor.__init__(self, sourceDir, index, task)
  
  def compile(self, cp):
    command = 'javac -cp %s perf/*.java >> compile.log 2>&1' % cp
    benchUtil.run(command,  'compile.log')
    benchUtil.run('javac -cp %s:./perf perf/values/*.java >> compile.log 2>&1' % cp,  'compile.log')

def standardVsBulkVIntVarGap():
  index1 = benchUtil.Index('bulkbranch', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=True)
  index2 = benchUtil.Index('bulkbranch', 'wiki', 'BulkVInt', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=True)
  run(
    Competitor('base', 'bulkbranch', index1, 'MMapDirectory', 'multi'),
    Competitor('bulk', 'bulkbranch', index2, 'MMapDirectory', 'multi'),
    )

def standardVsFOR():
  index1 = benchUtil.Index('clean.svn', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=True)
  index2 = benchUtil.Index('bulkbranch', 'wiki', 'FrameOfRef', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=True)
  run(
    Competitor('base', 'clean.svn', index1, 'MMapDirectory', 'multi'),
    Competitor('for', 'bulkbranch', index2, 'MMapDirectory', 'multi'),
    )

def bigIndex():
  index = benchUtil.Index('clean.svn', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=True)
  run(
    Competitor('base', 'clean.svn', index, 'MMapDirectory', 'multi'),
    Competitor('base2', 'clean.svn', index, 'MMapDirectory', 'multi'),
    )

def standardVsStandard():
  index1 = benchUtil.Index('clean.svn', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=True)
  index2 = benchUtil.Index('blockterms', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=True)
  run(
    Competitor('clean', 'clean.svn', index1, 'NIOFSDirectory', 'multi'),
    Competitor('robspec', 'robspec', index1, 'NIOFSDirectory', 'multi'),
    #Competitor('blockterms', 'blockterms.old', index2, 'MMapDirectory', 'multi'),
    )

def testRT():
  index1 = benchUtil.Index('clean.svn', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  index2 = benchUtil.Index('realtime', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  run(
    Competitor('clean', 'clean.svn', index1, 'MMapDirectory', 'multi'),
    Competitor('realtime', 'realtime', index2, 'MMapDirectory', 'multi'),
    )

def testNIOWrite():
  index1 = benchUtil.Index('clean.svn', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  index2 = benchUtil.Index('niowrite', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  run(
    Competitor('clean', 'clean.svn', index1, 'NIOFSDirectory', 'multi'),
    Competitor('niowrite', 'niowrite', index2, 'NIOFSDirectory', 'multi'),
    )

def sepVsSep():
  index1 = benchUtil.Index('clean.svn', 'wiki', 'MockSep', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  index2 = benchUtil.Index('blockterms', 'wiki', 'MockSep', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  run(
    Competitor('clean', 'clean.svn', index1, 'MMapDirectory', 'multi'),
    Competitor('block', 'blockterms', index2, 'MMapDirectory', 'multi'),
    )

def bulkFixedVsVarGap():
  index1 = benchUtil.Index('bulkbranch.clean', 'wiki', 'BulkVInt', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=True)
  index2 = benchUtil.Index('bulkbranch', 'wiki', 'BulkVInt', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=True)
  run(
    Competitor('bulkfixedgap', 'bulkbranch.clean', index1, 'MMapDirectory', 'multi'),
    Competitor('bulkvargap', 'bulkbranch', index2, 'MMapDirectory', 'multi'),
    )
  
def test30vsTrunk():
  index1 = benchUtil.Index('30x', 'wiki', 'StandardAnalyzer', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  index2 = benchUtil.Index('clean.svn', 'wiki', 'ClassicAnalyzer', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  run(
    Competitor('3.0', '30x', index1, 'NIOFSDirectory', 'StandardAnalyzer', 'multi'),
    Competitor('4.0', 'clean.svn', index2, 'NIOFSDirectory', 'ClassicAnalyzer', 'multi'),
    )

def main():
  index1 = benchUtil.Index('solrcene-clean', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  index2 = benchUtil.Index('solrcene', 'wiki', 'Standard', INDEX_NUM_DOCS, INDEX_NUM_THREADS, lineDocSource=WIKI_LINE_FILE, doOptimize=False)
  run(
    Competitor('trunk', 'solrcene-clean', index1, 'NIOFSDirectory', 'multi'),
    Competitor('patch', 'solrcene', index2, 'NIOFSDirectory', 'multi'),
    )

if __name__ == '__main__':
  # nocommit
  #standardVsBulkVIntVarGap()
  #standardVsFOR()
  #bulkFixedVsVarGap()
  #standardVsStandard()
  #testRT()
  #testNIOWrite()
  #sepVsSep()
  test30vsTrunk()
  #bigIndex()



# NOTE: when running on 3.0, apply this patch:
"""
Index: src/java/org/apache/lucene/search/FuzzyQuery.java
===================================================================
--- src/java/org/apache/lucene/search/FuzzyQuery.java	(revision 1062278)
+++ src/java/org/apache/lucene/search/FuzzyQuery.java	(working copy)
@@ -133,6 +133,8 @@
     }
 
     int maxSize = BooleanQuery.getMaxClauseCount();
+    // nocommit
+    maxSize = 50;
     PriorityQueue<ScoreTerm> stQueue = new PriorityQueue<ScoreTerm>();
     FilteredTermEnum enumerator = getEnum(reader);
     try {
Index: contrib/benchmark/src/java/org/apache/lucene/benchmark/byTask/feeds/DocData.java
===================================================================
--- contrib/benchmark/src/java/org/apache/lucene/benchmark/byTask/feeds/DocData.java	(revision 1062278)
+++ contrib/benchmark/src/java/org/apache/lucene/benchmark/byTask/feeds/DocData.java	(working copy)
@@ -29,6 +29,7 @@
   private String body;
   private String title;
   private String date;
+  private int id;
   private Properties props;
   
   public void clear() {
@@ -37,6 +38,7 @@
     title = null;
     date = null;
     props = null;
+    id = -1;
   }
   
   public String getBody() {
@@ -57,6 +59,10 @@
     return name;
   }
 
+  public int getID() {
+    return id;
+  }
+
   public Properties getProps() {
     return props;
   }
@@ -85,6 +91,10 @@
     this.name = name;
   }
 
+  public void setID(int id) {
+    this.id = id;
+  }
+
   public void setProps(Properties props) {
     this.props = props;
   }
Index: contrib/benchmark/src/java/org/apache/lucene/benchmark/byTask/feeds/DocMaker.java
===================================================================
--- contrib/benchmark/src/java/org/apache/lucene/benchmark/byTask/feeds/DocMaker.java	(revision 1062278)
+++ contrib/benchmark/src/java/org/apache/lucene/benchmark/byTask/feeds/DocMaker.java	(working copy)
@@ -20,14 +20,21 @@
 import java.io.IOException;
 import java.io.UnsupportedEncodingException;
 import java.util.HashMap;
+import java.util.Calendar;
 import java.util.Map;
 import java.util.Properties;
+import java.util.Locale;
 import java.util.Random;
+import java.util.Date;
+import java.util.concurrent.atomic.AtomicInteger;
+import java.text.SimpleDateFormat;
+import java.text.ParsePosition;
 
 import org.apache.lucene.benchmark.byTask.utils.Config;
 import org.apache.lucene.benchmark.byTask.utils.Format;
 import org.apache.lucene.document.Document;
 import org.apache.lucene.document.Field;
+import org.apache.lucene.document.NumericField;
 import org.apache.lucene.document.Field.Index;
 import org.apache.lucene.document.Field.Store;
 import org.apache.lucene.document.Field.TermVector;
@@ -82,6 +89,7 @@
   static class DocState {
     
     private final Map<String,Field> fields;
+    private final Map<String,NumericField> numericFields;
     private final boolean reuseFields;
     final Document doc;
     DocData docData = new DocData();
@@ -92,6 +100,7 @@
       
       if (reuseFields) {
         fields =  new HashMap<String,Field>();
+        numericFields = new HashMap<String,NumericField>();
         
         // Initialize the map with the default fields.
         fields.put(BODY_FIELD, new Field(BODY_FIELD, "", bodyStore, bodyIndex, termVector));
@@ -99,9 +108,13 @@
         fields.put(DATE_FIELD, new Field(DATE_FIELD, "", store, index, termVector));
         fields.put(ID_FIELD, new Field(ID_FIELD, "", Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
         fields.put(NAME_FIELD, new Field(NAME_FIELD, "", store, index, termVector));
+
+        numericFields.put(DATE_MSEC_FIELD, new NumericField(DATE_MSEC_FIELD));
+        numericFields.put(TIME_SEC_FIELD, new NumericField(TIME_SEC_FIELD));
         
         doc = new Document();
       } else {
+        numericFields = null;
         fields = null;
         doc = null;
       }
@@ -124,18 +137,42 @@
       }
       return f;
     }
+
+    NumericField getNumericField(String name) {
+      if (!reuseFields) {
+        return new NumericField(name);
+      }
+
+      NumericField f = numericFields.get(name);
+      if (f == null) {
+        f = new NumericField(name);
+        numericFields.put(name, f);
+      }
+      return f;
+    }
   }
   
-  private int numDocsCreated = 0;
   private boolean storeBytes = false;
 
+  private static class DateUtil {
+    public SimpleDateFormat parser = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss", Locale.US);
+    public Calendar cal = Calendar.getInstance();
+    public ParsePosition pos = new ParsePosition(0);
+    public DateUtil() {
+      parser.setLenient(true);
+    }
+  }
+
   // leftovers are thread local, because it is unsafe to share residues between threads
   private ThreadLocal<LeftOver> leftovr = new ThreadLocal<LeftOver>();
   private ThreadLocal<DocState> docState = new ThreadLocal<DocState>();
+  private ThreadLocal<DateUtil> dateParsers = new ThreadLocal<DateUtil>();
 
   public static final String BODY_FIELD = "body";
   public static final String TITLE_FIELD = "doctitle";
   public static final String DATE_FIELD = "docdate";
+  public static final String DATE_MSEC_FIELD = "docdatenum";
+  public static final String TIME_SEC_FIELD = "doctimesecnum";
   public static final String ID_FIELD = "docid";
   public static final String BYTES_FIELD = "bytes";
   public static final String NAME_FIELD = "docname";
@@ -155,6 +192,7 @@
   private int lastPrintedNumUniqueTexts = 0;
 
   private long lastPrintedNumUniqueBytes = 0;
+  private final AtomicInteger numDocsCreated = new AtomicInteger();
 
   private int printNum = 0;
 
@@ -169,7 +207,16 @@
     
     // Set ID_FIELD
     Field idField = ds.getField(ID_FIELD, storeVal, Index.NOT_ANALYZED_NO_NORMS, termVecVal);
-    idField.setValue("doc" + (r != null ? r.nextInt(updateDocIDLimit) : incrNumDocsCreated()));
+    int id;
+    if (r != null) {
+      id = r.nextInt(updateDocIDLimit);
+    } else {
+      id = docData.getID();
+      if (id == -1) {
+        id = numDocsCreated.getAndIncrement();
+      }
+    }
+    idField.setValue(Integer.toString(id));
     doc.add(idField);
     
     // Set NAME_FIELD
@@ -181,13 +228,39 @@
     doc.add(nameField);
     
     // Set DATE_FIELD
-    String date = docData.getDate();
+    DateUtil util = dateParsers.get();
+    if (util == null) {
+      util = new DateUtil();
+      dateParsers.set(util);
+    }
+    Date date = null;
+    String dateString = docData.getDate();
+    if (dateString != null) {
+      util.pos.setIndex(0);
+      date = util.parser.parse(dateString, util.pos);
+      //System.out.println(dateString + " parsed to " + date);
+    } else {
+      dateString = "";
+    }
+    Field dateStringField = ds.getField(DATE_FIELD, storeVal, indexVal, termVecVal);
+    dateStringField.setValue(dateString);
+    doc.add(dateStringField);
+
     if (date == null) {
-      date = "";
+      // just set to right now
+      date = new Date();
     }
-    Field dateField = ds.getField(DATE_FIELD, storeVal, indexVal, termVecVal);
-    dateField.setValue(date);
+
+    NumericField dateField = ds.getNumericField(DATE_MSEC_FIELD);
+    dateField.setLongValue(date.getTime());
     doc.add(dateField);
+
+    util.cal.setTime(date);
+    final int sec = util.cal.get(Calendar.HOUR_OF_DAY)*3600 + util.cal.get(Calendar.MINUTE)*60 + util.cal.get(Calendar.SECOND);
+
+    NumericField timeSecField = ds.getNumericField(TIME_SEC_FIELD);
+    timeSecField.setIntValue(sec);
+    doc.add(timeSecField);
     
     // Set TITLE_FIELD
     String title = docData.getTitle();
@@ -252,10 +325,6 @@
     return ds;
   }
 
-  protected synchronized int incrNumDocsCreated() {
-    return numDocsCreated++;
-  }
-
   /**
    * Closes the {@link DocMaker}. The base implementation closes the
    * {@link ContentSource}, and it can be overridden to do more work (but make
@@ -331,9 +400,9 @@
   public void printDocStatistics() {
     boolean print = false;
     String col = "                  ";
-    StringBuffer sb = new StringBuffer();
+    StringBuilder sb = new StringBuilder();
     String newline = System.getProperty("line.separator");
-    sb.append("------------> ").append(Format.simpleName(getClass())).append(" statistics (").append(printNum).append("): ").append(newline);
+    sb.append("------------> ").append(getClass().getSimpleName()).append(" statistics (").append(printNum).append("): ").append(newline);
     int nut = source.getTotalDocsCount();
     if (nut > lastPrintedNumUniqueTexts) {
       print = true;
@@ -363,7 +432,7 @@
     // re-initiate since properties by round may have changed.
     setConfig(config);
     source.resetInputs();
-    numDocsCreated = 0;
+    numDocsCreated.set(0);
     resetLeftovers();
   }
   
Index: contrib/benchmark/src/java/org/apache/lucene/benchmark/byTask/feeds/LineDocSource.java
===================================================================
--- contrib/benchmark/src/java/org/apache/lucene/benchmark/byTask/feeds/LineDocSource.java	(revision 1062278)
+++ contrib/benchmark/src/java/org/apache/lucene/benchmark/byTask/feeds/LineDocSource.java	(working copy)
@@ -48,6 +48,7 @@
 
   private File file;
   private BufferedReader reader;
+  private int readCount;
 
   private synchronized void openFile() {
     try {
@@ -71,9 +72,12 @@
   
   @Override
   public DocData getNextDocData(DocData docData) throws NoMoreDataException, IOException {
-    String line;
+    final String line;
+    final int myID;
+    
     synchronized(this) {
       line = reader.readLine();
+      myID = readCount++;
       if (line == null) {
         if (!forever) {
           throw new NoMoreDataException();
@@ -96,6 +100,7 @@
     }
     // The date String was written in the format of DateTools.dateToString.
     docData.clear();
+    docData.setID(myID);
     docData.setBody(line.substring(1 + spot2, line.length()));
     docData.setTitle(line.substring(0, spot));
     docData.setDate(line.substring(1 + spot, spot2));
"""
