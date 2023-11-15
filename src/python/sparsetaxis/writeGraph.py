import datetime
import pickle
import os
import re
import pysftp

CHANGES = [
  ('2016-07-04 07:13:41', 'LUCENE-7351: Doc id compression for dimensional points'),
  ('2016-07-07 08:02:29', 'LUCENE-7369: Similarity.coord and BooleanQuery.disableCoord are removed'),
  ('2016-07-12 15:57:56', 'LUCENE-7371: Better compression of dimensional points values'),
  ('2016-07-29 08:23:54', 'LUCENE-7396: speed up flush of points'),
  ('2016-08-03 12:34:06', 'LUCENE-7403: Use blocks of exactly maxPointsInLeafNode in the 1D points case'),
  ('2016-08-03 12:35:48', 'LUCENE-7399: Speed up flush of points, v2'),
  ('2016-08-12 17:54:33', 'LUCENE-7409: improve MMapDirectory\'s IndexInput to detect if a clone is being used after its parent was closed'),
  ('2016-09-21 13:41:41', 'LUCENE-7407: Switch doc values to iterator API'),
  ('2016-10-04 17:00:53', 'LUCENE-7474: Doc values writers should use sparse encoding'),
  ('2016-10-17 07:28:20', 'LUCENE-7489: Better sparsity support for Lucene70DocValuesFormat'),
  ('2016-10-18 13:05:50', 'LUCENE-7501: Save one heap byte per index node in the dimensional points index for the 1D case'),
  ('2016-10-18 14:08:29', 'LUCENE-7489: Wrap only once in case GCD compression is used'),
  ('2016-10-24 08:51:23', 'LUCENE-7462: Give doc values an advanceExact method'),
  ('2016-10-31 00:04:37', 'LUCENE-7135: This issue accidentally caused FSDirectory.open to use NIOFSDirectory instead of MMapDirectory'),
  ('2016-11-02 10:48:29', 'LUCENE-7135: Fixed this issue so we use MMapDirectory again'),
  ('2016-11-10 13:04:15', 'LUCENE-7545: Dense norms/doc-values should not consume memory for the IW buffer'),
  ('2016-11-23', 'Take best of 5 JVM runs for each search benchmark to reduce noise'),
  ('2016-12-04', 'LUCENE-7563: Compress the in-memory BKD points index'),
  ('2016-12-07', 'LUCENE-7583: buffer small leaf-block writes in BKDWriter'),
  ('2016-12-11', 'Re-enable JVM\'s background and tiered compilation'),
  ('2016-12-15', 'LUCENE-7589: Prevent outliers from raising number of doc-values bits for all documents'),
  ('2016-12-20', 'LUCENE-7579: Sort segments on flush, not merge'),
  ('2019-04-23', 'Switched to OpenJDK 11'),
  ('2019-04-30', 'Switched GC back to ParallelGC (away from default G1GC)'),
  ('2020-11-06', 'Move to new beast 3 Ryzen Threadripper 3990X hardware for all nightly benchmarks'),
  ('2021-06-18', 'LUCENE-9996: Reduced RAM usage per DWPT'),
  ('2021-06-24', 'LUCENE-9613: Encode ordinals like numerics'),
  ('2021-07-29', 'LUCENE-10031: Faster merging with index sorting enabled'),
  ('2021-08-16', 'LUCENE-10014: Fixed GCD compression'),
  ('2021-09-28', 'LUCENE-10125: Optimize primitive writes in OutputStreamIndexOutput'),
  ('2021-10-07', 'LUCENE-10153: Speed up BKDWriter using VarHandles'),
  ('2021-10-21', 'LUCENE-10165: Implement Lucene90DocValuesProducer#getMergeInstance'),
  ('2021-11-01', 'LUCENE-10196: Improve IntroSorter with 3-ways partitioning'),
  ('2022-12-23', 'Cut over numeric fields to LongField / DoubleField'),
  ('2022-12-28', 'GITHUB#12037: Optimize flush of SORTED_NUMERIC fields in conjunction with index sorting'),
  ('2023-02-13', 'Cut over keyword fields to KeywordField'),
  ('2023-02-21', 'GITHUB#12139: Skip the TokenStream overhead for simple keywords'),
  ]

reMergeTime = re.compile(r': (\d+) (?:ms|msec) to merge ([a-z ]+) \[(\d+) docs\]')
reTotMergeTime = re.compile(r': merge time (\d+) ms for (\d+) docs')
reFlushTime = re.compile(r': flush time ([.0-9]+) ms')
reFlushPostings = re.compile(r'flush postings as segment .*? numDocs=(\d+)$')
reDocsPerMB = re.compile('ramUsed=([.,0-9]+) MB newFlushedSize.*? docs/MB=([.,0-9]+)$')
reIndexingRate = re.compile('([.0-9]+) sec: (\d+) docs; ([.0-9]+) docs/sec; ([.0-9]+) MB/sec')

def extractIndexStats(indexLog):
  mergeTimesSec = {}
  flushTimeSec = 0
  docsPerMBRAM = 0
  totMergeTimeMS = 0
  totMergeDocs = 0
  docsPerMBDisk = 0
  flushCount = 0
  lastDPSMatch = None
  with open(indexLog, 'r', encoding='utf-8') as f:
    while True:
      line = f.readline()
      if line == '':
        break
      line = line.strip()
      m = reMergeTime.search(line)
      if m is not None:
        msec, part, docCount = m.groups()
        msec = int(msec)
        docCount = int(docCount)
        if part not in mergeTimesSec:
          mergeTimesSec[part] = [0, 0]
        l = mergeTimesSec[part]
        l[0] += msec/1000.0
        l[1] += docCount
      m = reTotMergeTime.search(line)
      if m is not None:
        totMergeTimeMS += float(m.group(1))
        totMergeDocs += int(m.group(2))
      m = reFlushTime.search(line)
      if m is not None:
        flushTimeSec += float(m.group(1))/1000.
      m = reFlushPostings.search(line)
      if m is not None:
        flushDocCount = int(m.group(1))
      m = reDocsPerMB.search(line)
      if m is not None:
        ramUsed = float(m.group(1).replace(',', ''))
        docsPerMBRAM += flushDocCount/ramUsed
        docsPerMBDisk += float(m.group(2).replace(',', ''))
        flushCount += 1
      m = reIndexingRate.search(line)
      if m is not None:
        lastDPSMatch = m

  mergeTimesSec['total'] = (totMergeTimeMS/1000.0, totMergeDocs)
  return float(lastDPSMatch.group(3)), mergeTimesSec, flushTimeSec, docsPerMBRAM/flushCount, docsPerMBDisk/flushCount

def msecToQPS(x):
  return 1000./x

reHits = re.compile('T(.) (.*?) sort=(.*?): ([0-9]+\\+?)(?: hits)? hits in ([.0-9]+) msec')
reHeapUsagePart = re.compile(r'^  ([a-z ]+) \[.*?\]: ([0-9.]+) (.B|bytes)$')
def extractSearchStats(searchLog):
  
  heapBytes = None
  heapBytesByPart = {}
  byThread = {}
  with open(searchLog, 'r', encoding='utf-8') as f:
    while True:
      line = f.readline()
      if line == '':
        break
      line = line.rstrip()
      if line.startswith('HEAP: '):
        heapBytes = int(line[6:])
      else:
        m = reHits.match(line)
        if m is not None:
          threadID, queryDesc, sortDesc, hitCount, msec = m.groups()
          if threadID not in byThread:
            byThread[threadID] = []
          if sortDesc == 'null':
            sortDesc = None
          else:
            sortDesc = 'longitude'
          if hitCount.endswith('+'):
            hitCount = hitCount[:-1]
          byThread[threadID].append((queryDesc, sortDesc, int(hitCount), float(msec)))
        else:
          m = reHeapUsagePart.match(line)
          if m is not None:
            part, size, unit = m.groups()
            size = float(size)
            if unit == 'GB':
              size *= 1024*1024*1024
            elif unit == 'MB':
              size *= 1024*1024
            elif unit == 'KB':
              size *= 1024
            elif unit == 'bytes':
              pass
            else:
              raise RuntimeError('unhandled unit %s' % unit)
            heapBytesByPart[part] = heapBytesByPart.get(part, 0.0) + size

  byQuerySort = {}
  for threadID, results in byThread.items():
    # discard warmup
    results = results[10:]
    for queryDesc, sortDesc, hitCount, msec in results:
      tup = (queryDesc, sortDesc)
      if tup not in byQuerySort:
        byQuerySort[tup] = []
      byQuerySort[tup].append(msec)

  allResults = [heapBytes, heapBytesByPart]
  # TODO: also "both colors" (all docs) and "latitude point range"
  for key in (('cab_color:g', None),
              ('cab_color:g', 'longitude'),
              ('cab_color:y', None),
              ('cab_color:y', 'longitude'),
              ('cab_color:y cab_color:g', None)):
    l = byQuerySort[key]
    l.sort()
    # median result:
    allResults.append(l[len(l)//2])

  key1 = ('IndexOrDocValuesQuery(indexQuery=green_pickup_latitude:[40.75 TO 40.9], dvQuery=green_pickup_latitude:[4630931869959979008 TO 4630952980583232307]) IndexOrDocValuesQuery(indexQuery=yellow_pickup_latitude:[40.75 TO 40.9], dvQuery=yellow_pickup_latitude:[4630931869959979008 TO 4630952980583232307])', None)
  key2 = ('IndexOrDocValuesQuery(indexQuery=pickup_latitude:[40.75 TO 40.9], dvQuery=pickup_latitude:[4630931869959979008 TO 4630952980583232307])', None)
  if key1 in byQuerySort:
    l = byQuerySort[key1]
  else:
    l = byQuerySort[key2]

  l.sort()
  # median result:
  allResults.append(l[len(l)//2])

  # heap, green-no-sort, green-longitude-sort, yellow-no-sort, yellow-longitude-sort
  return tuple(allResults)

def extractDiskUsageStats(logFileName):

  with open(logFileName, 'r') as f:
    while True:
      line = f.readline()
      if line == '':
        raise RuntimeError('unexpected EOF while parsing "%s"' % logFileName)
      elif line.startswith('num docs:'):
        break

    mbByPart = {}

    while True:
      line = f.readline()
      line = line.strip()
      if line == '':
        break
      what, size = line.split(':')
      mb = int(size.strip().replace(',', ''))/1024./1024.
      mbByPart[what] = mb

    while True:
      line = f.readline()
      if '====' in line:
        break

    mbByField = {}

    while True:
      line = f.readline()
      if line == '':
        break
      line = line.strip()
      tup = line.split()
      fieldName = tup[0]
      totMB = int(tup[1].replace(',', ''))/1024./1024.
      mbByField[fieldName] = totMB

    return mbByPart, mbByField
      

def toGB(x):
  return x/1024./1024./1024.

def toMB(x):
  return x/1024./1024.

def toDateTime(parts):
  parts = (int(x) for x in parts)
  return datetime.datetime(*parts)

reDateTime = re.compile(r'(\d\d\d\d)\.(\d\d)\.(\d\d)\.(\d\d)\.(\d\d)\.(\d\d)')

def toMSEpoch(dt):
  epoch = datetime.datetime.utcfromtimestamp(0)
  return 1000. * (dt - epoch).total_seconds()

def getFastest(searchStats, index):
  minMS = None
  for stats in searchStats:
    msec = stats[index]
    if minMS is None or msec < minMS:
      minMS = msec
  return minMS
  
def main():

  global dateWindow
  
  allTimes = []

  l = os.listdir('/l/logs.nightly/taxis')
  l.sort()

  indexSizeData = []
  indexSizePartsData = []
  indexSizePerFieldData = []
  indexDPSData = []
  checkIndexTimeData = []
  flushTimesData = []
  searcherHeapMBData = []
  searcherHeapMBPartData = []
  searchSortQPSData = []
  searchQPSData = []
  searchBQQPSData = []
  searchRangeQPSData = []
  docsPerMBRAMData = []
  docsPerMBDiskData = []
  dvMergeTimesData = []
  totMergeTimesData = []
  gitHashes = []
  
  for fileName in l:
    if os.path.exists('/l/logs.nightly/taxis/%s/results.pk' % fileName):
      results = pickle.loads(open('/l/logs.nightly/taxis/%s/results.pk' % fileName, 'rb').read())

      # load results written directly by the benchmarker:
      luceneRev, nonSparseDiskBytes, nonSparseCheckIndexTimeSec, nonSparseDiskUsageTimeSec, \
                 sparseDiskBytes, sparseCheckIndexTimeSec, sparseDiskUsageTimeSec, \
                 sparseSortedDiskBytes, sparseSortedCheckIndexTimeSec, sparseSortedDiskUsageTimeSec, \
                 = results

      gitHashes.append(luceneRev)

      # parse logs for more details results:
      logResultsFileName = '/l/logs.nightly/taxis/%s/logResults.pk' % fileName
      if os.path.exists(logResultsFileName):
        sparseIndexStats, nonSparseIndexStats, sparseSortedIndexStats, \
                          sparseSearchStats, nonSparseSearchStats, sparseSortedSearchStats, \
                          sparseDiskUsageStats, nonSparseDiskUsageStats, sparseSortedDiskUsageStats, \
                          = pickle.loads(open(logResultsFileName, 'rb').read())
      else:
        sparseIndexStats = extractIndexStats('/l/logs.nightly/taxis/%s/index.1threads.sparse.log' % fileName)
        nonSparseIndexStats = extractIndexStats('/l/logs.nightly/taxis/%s/index.1threads.nonsparse.log' % fileName)
        sparseSortedIndexStats = extractIndexStats('/l/logs.nightly/taxis/%s/index.1threads.sparse.sorted.log' % fileName)

        searchLog = '/l/logs.nightly/taxis/%s/searchsparse.log' % fileName
        if os.path.exists(searchLog):
          sparseSearchStats = [extractSearchStats(searchLog)]
          nonSparseSearchStats = [extractSearchStats('/l/logs.nightly/taxis/%s/searchnonsparse.log' % fileName)]
          sparseSortedSearchStats = [extractSearchStats('/l/logs.nightly/taxis/%s/searchsparse-sorted.log' % fileName)]
        else:
          upto = 0
          sparseSearchStats = []
          nonSparseSearchStats = []
          sparseSortedSearchStats = []
          while True:
            searchLog = '/l/logs.nightly/taxis/%s/searchsparse.%d.log' % (fileName, upto)
            if not os.path.exists(searchLog):
              break
            sparseSearchStats.append(extractSearchStats(searchLog))
            nonSparseSearchStats.append(extractSearchStats('/l/logs.nightly/taxis/%s/searchnonsparse.%s.log' % (fileName, upto)))
            sparseSortedSearchStats.append(extractSearchStats('/l/logs.nightly/taxis/%s/searchsparse-sorted.%s.log' % (fileName, upto)))
            upto += 1

        sparseDiskUsageStats = extractDiskUsageStats('/l/logs.nightly/taxis/%s/diskUsagesparse.log' % fileName)
        nonSparseDiskUsageStats = extractDiskUsageStats('/l/logs.nightly/taxis/%s/diskUsagenonsparse.log' % fileName)
        sparseSortedDiskUsageStats = extractDiskUsageStats('/l/logs.nightly/taxis/%s/diskUsagesparse-sorted.log' % fileName)

        open(logResultsFileName, 'wb').write(pickle.dumps((sparseIndexStats, nonSparseIndexStats, sparseSortedIndexStats,
                                                           sparseSearchStats, nonSparseSearchStats, sparseSortedSearchStats,
                                                           sparseDiskUsageStats, nonSparseDiskUsageStats, sparseSortedDiskUsageStats)))
                                             
      m = reDateTime.match(fileName)

      x = [m.groups()]
      #for part in ('stored fields', 'term vectors', 'norms', 'docvalues', 'postings', 'prox', 'points', 'terms'):
      for part in ('docvalues', 'points'):
        x.append(nonSparseDiskUsageStats[0][part])
        x.append(sparseDiskUsageStats[0][part])
        x.append(sparseSortedDiskUsageStats[0][part])
      indexSizePartsData.append(tuple(x))

      x = [m.groups()]
      for fieldName in ('dropoff_latitude', 'fare_amount', 'dropoff_datetime'):
        for stats in (nonSparseDiskUsageStats, sparseDiskUsageStats, sparseSortedDiskUsageStats):
          if fieldName in stats[1]:
            mb = stats[1][fieldName]
          else:
            mb = stats[1]['green_' + fieldName] + stats[1]['yellow_' + fieldName]
          x.append(mb)
      indexSizePerFieldData.append(tuple(x))

      indexSizeData.append((m.groups(), toGB(nonSparseDiskBytes), toGB(sparseDiskBytes), toGB(sparseSortedDiskBytes)))
      indexDPSData.append((m.groups(), nonSparseIndexStats[0]/1000., sparseIndexStats[0]/1000., sparseSortedIndexStats[0]/1000.))
      checkIndexTimeData.append((m.groups(), nonSparseCheckIndexTimeSec, sparseCheckIndexTimeSec, sparseSortedCheckIndexTimeSec))
      flushTimesData.append((m.groups(), nonSparseIndexStats[2], sparseIndexStats[2], sparseSortedIndexStats[2]))
      docsPerMBRAMData.append((m.groups(), nonSparseIndexStats[3]/1000., sparseIndexStats[3]/1000., sparseSortedIndexStats[3]/1000.))
      docsPerMBDiskData.append((m.groups(), nonSparseIndexStats[4]/1000., sparseIndexStats[4]/1000., sparseSortedIndexStats[4]/1000.))
      try:
        dvMergeTimesData.append((m.groups(), nonSparseIndexStats[1]['doc values'][0], sparseIndexStats[1]['doc values'][0], sparseSortedIndexStats[1]['doc values'][0]))
      except:
        print('FAILED for %s' % fileName)
        raise
      try:
        totMergeTimesData.append((m.groups(), nonSparseIndexStats[1]['total'][0], sparseIndexStats[1]['total'][0], sparseSortedIndexStats[1]['total'][0]))
      except:
        print('FAILED for %s' % fileName)
        raise

      searcherHeapMBData.append((m.groups(),
                                 toMB(nonSparseSearchStats[0][0]),
                                 toMB(sparseSearchStats[0][0]),
                                 toMB(sparseSortedSearchStats[0][0])))
      x = [m.groups()]
      for part in 'postings', 'docvalues', 'stored fields', 'points':
        for stats in nonSparseSearchStats, sparseSearchStats, sparseSortedSearchStats:
          x.append(toMB(stats[0][1].get(part, 0)))
      searcherHeapMBPartData.append(tuple(x))
      searchSortQPSData.append((m.groups(),
                                msecToQPS(getFastest(nonSparseSearchStats, 3)),
                                msecToQPS(getFastest(sparseSearchStats, 3)),
                                msecToQPS(getFastest(sparseSortedSearchStats, 3)),
                                msecToQPS(getFastest(nonSparseSearchStats, 5)),
                                msecToQPS(getFastest(sparseSearchStats, 5)),
                                msecToQPS(getFastest(sparseSortedSearchStats, 5))))
      searchQPSData.append((m.groups(),
                            msecToQPS(getFastest(nonSparseSearchStats, 2)),
                            msecToQPS(getFastest(sparseSearchStats, 2)),
                            msecToQPS(getFastest(sparseSortedSearchStats, 2)),
                            msecToQPS(getFastest(nonSparseSearchStats, 4)),
                            msecToQPS(getFastest(sparseSearchStats, 4)),
                            msecToQPS(getFastest(sparseSortedSearchStats, 4))));
      searchBQQPSData.append((m.groups(),
                              msecToQPS(getFastest(nonSparseSearchStats, 6)),
                              msecToQPS(getFastest(sparseSearchStats, 6)),
                              msecToQPS(getFastest(sparseSortedSearchStats, 6))))
      searchRangeQPSData.append((m.groups(),
                                 msecToQPS(getFastest(nonSparseSearchStats, 7)),
                                 msecToQPS(getFastest(sparseSearchStats, 7)),
                                 msecToQPS(getFastest(sparseSortedSearchStats, 7))))

      allTimes.append(m.groups())

  # attach each known change to the next datapoint after that change's timestamp:
  lastDateTime = None
  for i in range(len(CHANGES)):
    x = CHANGES[i][0].split()
    x1 = tuple(int(y) for y in x[0].split('-'))
    if len(x) == 1:
      x1 += (0, 0, 0)
    else:
      x1 += tuple(int(y) for y in x[1].split(':'))

    changeDateTime = datetime.datetime(*x1)
      
    for tup in allTimes:
      pointDateTime = datetime.datetime(*(int(x) for x in tup))
      if lastDateTime is not None and pointDateTime >= changeDateTime:
        CHANGES[i] += ('%s-%s-%s %s:%s:%s' % tup,)
        #print('%s -> %s' % (CHANGES[i][0], CHANGES[i][2]))
        break
      lastDateTime = pointDateTime
      
  startDateTime = toDateTime(indexSizeData[0][0])
  endDateTime = toDateTime(indexSizeData[-1][0])
  sixHours = datetime.timedelta(hours=6)

  # This way it's clear we are seeing the whole date range:
  dateWindow = (toMSEpoch(startDateTime - sixHours), toMSEpoch(endDateTime + sixHours))

  with open('/x/tmp/sparseResults.html', 'w') as f:
    f.write('''
<html>
<head>
<title>Sparse Lucene benchmarks</title>
<script type="text/javascript" src="dygraph-combined-dev.js"></script>
<script type="text/javascript">
''')
    f.write('gitHashes = %s;\n' % repr(gitHashes))
    f.write('''
function onPointClick(e, p) {
  if (p.idx > 0) {
    top.location = "https://github.com/apache/lucene/compare/" + gitHashes[p.idx-1] + "..." + gitHashes[p.idx];
  } else {
    top.location = "https://github.com/apache/lucene/commit/" + gitHashes[p.idx];
  }
}
</script>
<style type="text/css">
a:hover * {
  text-decoration: underline;
}

html * {
  #font-size: 1em !important;
  text-decoration: none;
  #color: #000 !important;
  font-family: Verdana !important;
}

.dygraph-legend {
  font-size: 70% !important;
}

.dygraph-legend > span.highlight { border: 1px solid grey}

.dygraph-legend > span.highlight { display: inline; }
</style>
</head>
<body>
''')

    f.write('''
<style type="text/css">
#summary {
  position: absolute;
  left: 10px;
  top: %3%%;
}
</style>
<div id="summary" style="height:17%%; width:95%%">
This benchmark indexes and searches a 20 M document subset of the <a href="http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml">New York City taxi ride corpus</a>, in both a sparse and dense way.  Green taxi rides make up ~11.5% of the 20 M documents, and yellow are ~88.5%.  See <a href="https://www.elastic.co/blog/sparse-versus-dense-document-values-with-apache-lucene">this blog post</a> for details.<p>Click and drag to zoom; shift + click and drag to scroll after zooming; hover over an annotation to see details; click on a data point to see its source code changes.
</div>
''')

    writeOneGraph(f, indexSizeData, 'index_size', 'Index size (GB)')
    writeOneGraph(f, indexSizePartsData, 'index_size_parts', 'Index size by part (MB)',
                  ('Date', 'Doc values (dense)', 'Doc values (sparse)', 'Doc values (sparse-sorted)',
                   'Points (dense)', 'Points (sparse)', 'Points (sparse-sorted)'))
    writeOneGraph(f, indexSizePerFieldData, 'index_size_by_field', 'Index size by field (MB)',
                  ('Date', 'Dropoff latitude (dense)', 'Dropoff latitude (sparse)', 'Dropoff latitude (sparse-sorted)',
                   'Fare amount (dense)', 'Fare amount (sparse)', 'Fare amount (sparse-sorted)',
                   'Dropoff datetime (dense)', 'Dropoff datetime (sparse)', 'Dropoff datetime (sparse-sorted)'))
    writeOneGraph(f, indexDPSData, 'index_throughput', 'Indexing rate 1 thread (K docs/sec)')
    writeOneGraph(f, docsPerMBRAMData, 'index_docs_per_mb_ram', 'Docs per MB RAM at flush (K docs)')
    writeOneGraph(f, docsPerMBDiskData, 'index_docs_per_mb_disk', 'Docs per MB Disk at flush (K docs)')
    writeOneGraph(f, checkIndexTimeData, 'check_index_time', 'CheckIndex time (Seconds)')
    writeOneGraph(f, flushTimesData, 'flush_times', 'New segment flush time (Seconds)')
    writeOneGraph(f, dvMergeTimesData, 'dv_merge_times', 'Doc values merge time (Seconds)')
    writeOneGraph(f, totMergeTimesData, 'tot_merge_times', 'Merge time (Seconds)')
    writeOneGraph(f, searcherHeapMBData, 'searcher_heap', 'Searcher heap used (MB)')
    writeOneGraph(f, searcherHeapMBPartData, 'searcher_heap_parts', 'Searcher heap used by part (MB)',
                  ('Date',
                   'Postings (dense)', 'Postings (sparse)', 'Postings (sparse-sorted)',
                   'Doc values (dense)', 'Doc values (sparse)', 'Doc values (sparse-sorted)',
                   'Stored fields (dense)', 'Stored fields (sparse)', 'Stored fields (sparse-sorted)',
                   'Points (dense)', 'Points (sparse)', 'Points (sparse-sorted)'))
    writeOneGraph(f, searchSortQPSData, 'search_sort_qps', 'TermQuery, sort by longitude (QPS)',
                  ('Date', 'Green cab (dense)', 'Green cab (sparse)', 'Green cab (sparse-sorted)', 'Yellow cab (dense)', 'Yellow cab (sparse)', 'Yellow cab (sparse-sorted)'))
    writeOneGraph(f, searchQPSData, 'search_qps', 'TermQuery (QPS)',
                  ('Date', 'Green cab (dense)', 'Green cab (sparse)', 'Green cab (sparse-sorted)', 'Yellow cab (dense)', 'Yellow cab (sparse)', 'Yellow cab (sparse-sorted)'))
    writeOneGraph(f, searchBQQPSData, 'search_bq_qps', 'BooleanQuery SHOULD green + SHOULD yellow (QPS)')
    writeOneGraph(f, searchRangeQPSData, 'search_range_qps', 'Pickup latitude range (QPS)')

    f.write('</body>\n</html>\n')

  if True:
    print('Copy charts up...')
    with pysftp.Connection('home.apache.org', username='mikemccand') as c:
      with c.cd('public_html/lucenebench'):
        c.put('/x/tmp/sparseResults.html', 'sparseResults.html')

topPct = 20

def getLabel(label):
  if label < 26:
    s = chr(65+label)
  else:
    s = '%s%s' % (chr(65+(label//26 - 1)), chr(65 + (label%26)))
  return s

reTitleAndUnits = re.compile(r'(.*?) \((.*?)\)')

def writeOneGraph(f, data, id, title, headers=None):
  global topPct

  m = reTitleAndUnits.match(title)
  title, units = m.groups()
  
  f.write('''
<style type="text/css">
#%s {
  position: absolute;
  left: 10px;
  top: %d%%;
}
</style>
''' % (id, topPct))

  topPct += 55

  if headers is None:
    headers = ('Date', 'Dense', 'Sparse', 'Sparse (sorted)')

  f.write('''
<div id="%s" style="height:50%%; width:95%%"></div>
<script type="text/javascript">
  g = new Dygraph(

  // containing div
  document.getElementById("%s"),
  "%s\\n"
''' % (id, id, ','.join(headers)))

  maxValue = None

  for timestamp, *values in data:
    f.write('  + "%s-%s-%s %s:%s:%s' % timestamp)
    f.write(',%s\\n"\n' % ','.join([str(x) for x in values]))
    for x in values:
      if type(x) is float:
        if maxValue is None or x > maxValue:
          maxValue = x

  if True or id in ('search_qps', 'search_sort_qps'):
    # fix the value axis so the legend doesn't obscure the series:
    otherOptions = '    "valueRange": [0.0, %s],' % int(1.30*maxValue)
  else:
    otherOptions = ''
  

  f.write('''
  , { "title": "<a href=\'#%s\'><font size=+2>%s</font></a>",
    // "colors": ["#DD1E2F", "#EBB035", "#06A2CB", "#218559", "#B0A691", "#192823"],
    "colors": ["#00BFB3", "#FED10A", "#0078A0", "#DF4998", "#93C90E", "#00A9E5", "#222", "#AAA", "#777"],
    "drawGapEdgePoints": true,
    "xlabel": "Date",
    "ylabel": "%s",
    "pointClickCallback": onPointClick,
    //"labelsDivWidth": 500,
    "labelsSeparateLines": true,
    "pointSize": 2,
    "gridLineColor": "#BBB",
    "colorSaturation": 0.5,
    "highlightCircleSize": 4,
    //"strokeWidth": 2.0,
    "connectSeparatedPoints": true,
    "drawPoints": true,
    "includeZero": true,
    "axisLabelColor": "#555",
    "axisLineColor": "#555",
    "dateWindow": [%s, %s],
    highlightSeriesOpts: {
      strokeWidth: 3,
      strokeBorderWidth: 1,
      highlightCircleSize: 5
    },
    %s
  });
  ''' % (id, title, units, dateWindow[0], dateWindow[1], otherOptions))

  f.write('g.ready(function() {g.setAnnotations([')
  for i in range(len(CHANGES)):
    change = CHANGES[i]
    if len(change) == 3:
      timeStamp = change[2]
      f.write('{series: "%s", x: "%s", shortText: "%s", text: "%s"},\n' % \
              (headers[2], timeStamp, getLabel(i), change[1].replace('"', '\\"')))
  f.write(']);});\n')

  f.write('</script>\n')
  
if __name__ == '__main__':
  main()
  #print(extractIndexStats('/l/logs.nightly/taxis/2016.11.02.21.06.27/index.1threads.sparse.sorted.log'))
