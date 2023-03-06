KNOWN_CHANGES = [
    ('2011-04-25',
     'Switched to 240 GB OCZ Vertex III',
     """
   Switched from a traditional spinning-magnets hard drive (Western Digital Caviar Green, 1TB) to a 240 GB <a href="http://www.ocztechnology.com/ocz-vertex-3-sata-iii-2-5-ssd.html">OCZ Vertex III SSD</a>; this change gave a small increase in indexing rate, drastically reduced variance on the NRT reopen time (NRT is IO intensive), and didn't affect query performance (which is expected since the postings are small enough to fit into the OS's IO cache.
   """),

    ('2011-05-02',
     'LUCENE-3023: concurrent flushing (DocWriterPerThread)',
     """
   Concurrent flushing, a major improvement to Lucene, was committed.  Before this change, flushing a segment in IndexWriter was single-threaded and blocked all other indexing threads; after this change, each indexing thread flushes its own segment without blocking indexing of other threads.  On highly concurrent hardware (the machine running these tests has 24 cores) this can result in a tremendous increase in Lucene\'s indexing throughput.  See <a href="http://blog.mikemccandless.com/2011/05/265-indexing-speedup-with-lucenes.html">this post</a> for details.

   <p> Some queries did get slower, because the index now has more segments.  Unfortunately, the index produced by concurrent flushing will vary, night to night, in how many segments it contains, so this is a further source of noise in the search results."""),

    ('2011-05-06',
     'Make search index consistent',
     """
   Changed how I build the index used for searching, to only use one thread.  This results in exactly the same index structure (same segments, same docs per segment) from night to night, to avoid the added noise from change B.
   """),

    ('2011-05-07',
     'Change to 20 indexing threads (from 6), 350 MB RAM buffer (from 512)',
     """
   Increased number of indexing threads from 6 to 20 and dropped the IndexWriter RAM buffer from 512 MB to 350 MB.  See <a href="http://blog.mikemccandless.com/2011/05/265-indexing-speedup-with-lucenes.html">this post</a> for details.
   """),

    ('2011-05-11',
     'Add TermQuery with sorting',
     """
   Added TermQuery, sorting by date/time and title fields.
   """),

    ('2011-05-14',
     'Add TermQuery with grouping',
     """
   Added TermQuery, grouping by fields with 100, 10K, 1M unique values.
   """),

    ('2011-06-03',
     'Add single-pass grouping',
     """
   Added Term (bgroup) and Term (bgroup, 1pass) using the BlockGroupingCollector for grouping into 1M unique groups.
   """),

    ('2011-06-26',
     'Use MemoryCodec for id field; switched to NRTCachingDirectory for NRT test',
     '''
   Switched to MemoryCodec for the primary-key 'id' field so that lookups (either for PKLookup test or for deletions during reopen in the NRT test) are fast, with no IO.  Also switched to NRTCachingDirectory for the NRT test, so that small new segments are written only in RAM.
   '''),

    ('2011-07-04',
     'Switched from Java 1.6.0_21 to 1.6.0_26',
     '''
   Switched from Java 1.6.0_21 to 1.6.0_26
   '''),

    ('2011-07-11',
     'LUCENE-3233: arc array optimizations to FST',
     '''
   <a href="https://issues.apache.org/jira/browse/LUCENE-3233">LUCENE-3233</a>: fast SynonymFilter using an FST, including an optimization to the FST representation allowing array arcs even when some arcs have large outputs; this resulted in a good speedup for MemoryCodec, which also speeds up the primary key lookup performance.
   '''),

    ('2011-07-22',
     'LUCENE-3328: specialize code for AND of TermQuery',
     '''
   <a href="https://issues.apache.org/jira/browse/LUCENE-3328">LUCENE-3328</a>: If all clauses of a BooleanQuery are MUST and are TermQuery then create a specialized scorer for scoring this common case.
   '''),

    ('2011-07-30',
     'Switched back to Java 1.6.0_21 from 1.6.0_26',
     '''
   Switched back to Java 1.6.0_21 from 1.6.0_26 because _26 would sometimes deadlock threads.
   '''),

    ('2011-08-20',
     'LUCENE-3030: cutover to more efficient BlockTree terms dict',
     '''
   <a href="https://issues.apache.org/jira/browse/LUCENE-3030">LUCENE-3030</a>: cutover to more efficient BlockTree terms dict.
   '''),

    ('2011-09-22',
     'LUCENE-3215: sloppy PhraseQuery speedups',
     '''
   <a href="https://issues.apache.org/jira/browse/LUCENE-3215">LUCENE-3215</a>: more efficient scoring for sloppy PhraseQuery.
   '''),

    ('2011-11-30',
     'LUCENE-3584: make postings bulk API codec-private',
     '''
   <a href="https://issues.apache.org/jira/browse/LUCENE-3584">LUCENE-3584</a>: make postings bulk API codec-private
   '''),

    ('2011-12-07',
     'Switched to Java 1.7.0_01',
     'Switched to Java 1.7.0_01'),

    ('2011-12-16',
     'LUCENE-3648: JIT optimizations to Lucene40 DocsEnum',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-3648">LUCENE-3648</a>: JIT optimizations to Lucene40 DocsEnum'),

    ('2012-01-30',
     'LUCENE-2858: Split IndexReader in AtomicReader and CompositeReader',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-2858">LUCENE-2858</a>: Split IndexReader in AtomicReader and CompositeReader'),

    ('2012-03-18',
     'LUCENE-3738: Be consistent about negative vInt/vLong',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-3738">LUCENE-3738</a>: Be consistent about negative vInt/vLong'),

    ('2012-05-25',
     'LUCENE-4062: new aligned packed-bits implementations',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-4062">LUCENE-4062</a>: new aligned packed-bits implementations'),

    ('2012-05-28',
     'Disable Java\'s compressed OOPS, and LUCENE-4055: refactor SegmentInfos/FieldInfos',
     'Disable Java\'s compressed OOPS (-XX:-UseCompressedOops), and <a href="https://issues.apache.org/jira/browse/LUCENE-4055">LUCENE-4055</a>: refactor SegmentInfos/FieldInfos'),

    ('2012-05-06',
     'LUCENE-4024: FuzzyQuery never does edit distance > 2',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-4024">LUCENE-4024</a>: FuzzyQuery never does edit distance > 2'),

    ('2012-05-15',
     'LUCENE-4024: (rev 1338668) fixed ob1 bug causing FuzzyQ(1) to be TermQuery',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-4024">LUCENE-4024</a>: (rev <a href="http://svn.apache.org/viewvc?view=revision&revision=1338668">1338668</a>) fixed ob1 bug causing FuzzyQ(1) to be TermQuery'),

    ('2012-06-02',
     'Re-enable Java\'s compressed OOPS',
     'Re-enable Java\'s compressed OOPS'),

    ('2012-06-06',
     'Switched to Java 1.7.0_04',
     'Switched to Java 1.7.0_04'),

    ('2012-06-26',
     'Fixed silly performance bug in PKLookupTask.java',
     'Fixed silly performance bug in PKLookupTask.java'),

    ('2012-10-06',
     'Stopped overclocking the computer running benchmarks.',
     'Stopped overclocking the computer running benchmarks.'),

    ('2012-10-15',
     'LUCENE-4446: switch to BlockPostingsFormat',
     'LUCENE-4446: switch to BlockPostingsFormat'),

    ('2012-12-10',
     'LUCENE-4598: small optimizations to facet aggregation',
     'LUCENE-4598: small optimizations to facet aggregation'),

    ('2013-01-11',
     'LUCENE-4620: IntEncoder/Decoder bulk API',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-4620">LUCENE-4620</a>: IntEncoder/Decoder bulk API'),

    ('2013-01-17',
     'Facet performance improvements',
     'Facet performance improvements: LUCENE-4686, LUCENE-4620, LUCENE-4602'),

    ('2013-01-21',
     'Facet performance improvements',
     'Facet performance improvements: LUCENE-4600'),

    ('2013-01-24',
     'Switched to NO_PARENTS faceting',
     'Switched to NO_PARENTS faceting'),

    ('2013-02-07',
     'DocValues improvements (LUCENE-4547) and facets API improvements (LUCENE-4757)',
     'DocValues improvements (<a href="https://issues.apache.org/jira/browse/LUCENE-4547">LUCENE-4547</a>) and facets API improvements (<a href="https://issues.apache.org/jira/browse/LUCENE-4757">LUCENE-4757</a>)'),

    ('2013-02-12',
     'LUCENE-4764: new Facet42DocValuesFormat for faster but more RAM-consuming DocValues',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-4764">LUCENE-4764</a>: new Facet42DocValuesFormat for faster but more RAM-consuming DocValues'),

    ('2013-02-22',
     'LUCENE-4791: optimize ConjunctionTermScorer to use skipping on first term',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-4791">LUCENE-4791</a>: optimize ConjunctionTermScorer to use skipping on first term'),

    ('2013-03-14',
     'LUCENE-4607: add DISI/Spans.cost',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-4607">LUCENE-4607</a>: add DISI/Spans.cost'),

    ('2013-05-03',
     'LUCENE-4946: SorterTemplate',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-4946">LUCENE-4946</a>: SorterTemplate'),

    ('2013-06-20',
     'LUCENE-5063: compress int and long FieldCache entries',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-5063">LUCENE-5063</a>: compress int and long FieldCache entries'),

    ('2013-07-31',
     'LUCENE-5140: recover slowdown in span queries and exact phrase query',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-5140">LUCENE-5140</a>: recover slowdown in span queries and exact phrase query'),

    ('2013-09-10',
     'Switched to Java 1.7.0_40',
     'Switched to Java 1.7.0_40'),

    ('2013-11-09',
     'Switched to DirectDocValuesFormat for the Date facets field.',
     'Switched to DirectDocValuesFormat for the Date facets field.'),

    ('2014-02-06',
     'LUCENE-5425: performance improvement for FixedBitSet.iterator',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-5425">LUCENE-5425: performance improvement for FixedBitSet.iterator</a>',),

    ('2014-04-05',
     'LUCENE-5527: LeafCollector (made CachingCollector slower)',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-5527">LUCENE-527: LeafCollector (made CachingCollector slower)</a>',),

    ('2014-04-25',
     'Upgraded to Ubuntu 14.04 LTS (kernel 3.13.0-32-generic #57)',
     'Upgraded to Ubuntu 14.04 LTS (kernel 3.13.0-32-generic #57)'),

    ('2014-06-10',
     'Switched from DirectDVFormat to Lucene\'s default for Date facet field',
     'Switched from DirectDVFormat to Lucene\'s default for Date facet field'),

    ('2014-07-25',
     'Disabled transparent huge pages',
     'Disabled transparent huge pages'),

    ('2014-08-30',
     'Re-enabled transparent huge pages',
     'Re-enabled transparent huge pages'),

    ('2014-03-11',
     'LUCENE-5487: add BulkScorer',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-5487">LUCENE-5487: add BulkScorer</a>'),

    ('2014-11-01',
     'LUCENE-6030: norms compression',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-6030">LUCENE-6030: norms compression</a>'),

    ('2014-11-22',
     'Upgrade from java 1.7.0_55-b13 to java 1.8.0_20-ea-b05',
     'Upgrade from java 1.7.0_55-b13 to java 1.8.0_20-ea-b05'),

    ('2015-01-15',
     'LUCENE-6179: remove out-of-order scoring',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-6179">LUCENE-6179: remove out-of-order scoring</a>'),

    ('2015-01-19',
     'LUCENE-6184: BooleanScorer better deals with sparse clauses',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-6184">LUCENE-6184: BooleanScorer better deals with sparse clauses</a>'),

    ('2015-02-13',
     'LUCENE-6198: Two phase intersection',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-6198">LUCENE-6198: Two phase intersection</a> (approximations are not needed by any query in this benchmark, but the change refactored ConjunctionScorer a bit)'),

    ('2015-02-23',
     'LUCENE-6275: SloppyPhraseScorer reuses ConjunctionDISI',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-6275">LUCENE-6275: SloppyPhraseScorer reuses ConjunctionDISI</a>'),

    ('2015-03-02',
     'LUCENE-6320: Speed up CheckIndex',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-6320">LUCENE-6320: Speed up CheckIndex</a>'),

    ('2015-03-06',
     'Upgrade JDK from 1.8.0_25-b17 to 1.8.0_40-b25',
     'Upgrade JDK from 1.8.0_25-b17 to 1.8.0_40-b25'),

    ('2015-04-02',
     'LUCENE-6308: span queries support two-phased iteration',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-6308">LUCENE-6308: span queries support two-phased iteration</a>'),

    ('2015-04-04',
     'LUCENE-5879: add auto-prefix terms',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-5879">LUCENE-5879: add auto-prefix terms</a>'),

    ('2015-06-24',
     'LUCENE-6548: some optimizations to block tree intersect',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-6548">LUCENE-6548: some optimizations to block tree intersect</a>'),

    ('2015-09-15',
     'LUCENE-6789: switch to BM25 scoring by default',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-6789">LUCENE-6789 switch to BM25 scoring by default</a>'),

    ('2015-10-05',
     'Randomize what time of day benchmark runs',
     'Randomize what time of day benchmark runs'),

    ('2015-12-02',
     'Upgrade to beast2 (72 cores, 256 GB RAM)',
     'Upgrade to beast2 (72 cores, 256 GB RAM)'),

    ('2015-12-10',
     'LUCENE-6919: Change the Scorer API to expose an iterator instead of extending DocIdSetIterator',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-6919">LUCENE-6919: Change the Scorer API to expose an iterator instead of extending DocIdSetIterator</a>'),

    ('2015-12-14',
     'LUCENE-6917: Change from LegacyNumericRangeQuery to DimensionalRangeQuery',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-6917">LUCENE-6917: Change from LegacyNumericRangeQuery to DimensionalRangeQuery</a>'),

    ('2016-05-23',
     'Fix silly benchmark bottlenecks; increase indexing heap to 8 GB; increase indexing buffer to 2 GB; increase indexing threads to 20; use default postings format for all fields; do not wait for commit in the end (just rollback); turn off merge IO throttling',
     '<a href="https://github.com/mikemccand/luceneutil/commit/b24e28dd1bf9a9fcacd693c4162d5ebb03d4afe1">Fix silly benchmark bottlenecks and re-tune for high indexing throughput</a>'),

    ('2016-05-25',
     'Fix another benchmark bottleneck for 1 KB docs (but this added a bug in TermDateFacets, fixed on 10/18)',
     'Fix another benchmark bottleneck for 1 KB docs (but this added a bug in TermDateFacets, fixed on 10/18)'),

    ('2016-06-13',
     'LUCENE-7330: Speed up conjunctions',
     '<a href="https://issues.apache.org/jira/browse/LUCENE-7330">LUCENE-7330: Speed up conjunctions</a>'),

    ('2016-07-11',
     'Upgrade beast2 OS from Ubuntu 15.04 to 16.04',
     'Upgrade beast2 OS from Ubuntu 15.04 to 16.04'),

    ('2016-07-16',
     'Upgrade beast2 kernel from 4.4.x to 4.6.x',
     'Upgrade beast2 kernel from 4.4.x to 4.6.x'),

    ('2016-08-17',
     'Upgrade beast2 kernel from 4.6.x to 4.7.0',
     'Upgrade beast2 kernel from 4.6.x to 4.7.0'),

    ('2016-09-04',
     'Upgrade beast2 kernel from 4.7.0 to 4.7.2',
     'Upgrade beast2 kernel from 4.7.0 to 4.7.2'),

    ('2016-09-21',
     'LUCENE-7407: Change doc values from random access to iterator API',
     'LUCENE-7407: Change doc values from random access to iterator API',
     ),

    ('2016-10-18',
     'Fix silly TermDateFacets bug causing single date facet to be indexed for all docs, added on 5/25',
     'Fix silly TermDateFacets bug causing single date facet to be indexed for all docs, added on 5/25',
     ),

    ('2016-10-24',
     'LUCENE-7462: give doc values an advanceExact API',
     'LUCENE-7462: give doc values an advanceExact API',
     ),

    ('2016-10-26',
     'LUCENE-7519: optimize computing browse-only facets, LUCENE-7489: Remove one layer of abstraction in binary doc values and single-valued numerics',
     'LUCENE-7519: optimize computing browse-only facets, LUCENE-7489: Remove one layer of abstraction in binary doc values and single-valued numerics',
     ),

    ('2016-10-27',
     'Re-enable transparent huge pages in Linux',
     'Re-enable transparent huge pages in Linux',
     ),

    ('2016-10-31',
     'LUCENE-7135: This issue accidentally caused FSDirectory.open to use NIOFSDirectory instead of MMapDirectory for e.g. CheckIndex',
     'LUCENE-7135: This issue accidentally caused FSDirectory.open to use NIOFSDirectory instead of MMapDirectory for e.g. CheckIndex'),

    ('2016-11-02',
     'LUCENE-7135: Fixed this issue so we use MMapDirectory again',
     'LUCENE-7135: Fixed this issue so we use MMapDirectory again'),

    ('2017-01-18',
     'LUCENE-7641: Speed up point ranges that match most documents',
     'LUCENE-7641: Speed up point ranges that match most documents'),

    ('2017-10-27',
     'LUCENE-7997: BM25 to use doubles instead of floats',
     'LUCENE-7997: BM25 to use doubles instead of floats'),

    ('2018-01-31',
     'LUCENE-4198: Allow codecs to index term impacts',
     'LUCENE-4198: Allow codecs to index term impacts'),

    ('2018-02-20',
     'LUCENE-8153: CheckIndex spends less time checking impacts',
     'LUCENE-8153: CheckIndex spends less time checking impacts'),

    ('2018-05-02',
     'LUCENE-8279: CheckIndex now cross-checks terms with norms',
     'LUCENE-8279: CheckIndex now cross-checks terms with norms'),

    ('2018-05-11',
     'Primary key now indexed with the default codec instead of the specialized Memory postings format',
     'Primary key now indexed with the default codec instead of the specialized Memory postings format'),

    ('2018-05-25',
     'LUCENE-8312: Leverage impacts for SynonymQuery (introduced regression for non-scoring term queries)',
     'LUCENE-8312: Leverage impacts for SynonymQuery (introduced regression for non-scoring term queries)'),

    ('2018-08-07',
     'LUCENE-8312: Fixed regression with non-scoring term queries',
     'LUCENE-8312: Fixed regression with non-scoring term queries'),

    ('2018-08-07',
     'LUCENE-8060: Stop counting total hits by default',
     'LUCENE-8060: Stop counting total hits by default'),

    ('2018-08-18',
     'LUCENE-8448: Propagate min competitive score to sub clauses',
     'LUCENE-8448: Propagate min competitive score to sub clauses'),

    ('2018-11-19',
     'LUCENE-8464: ConstantScoreScorer now implements setMinCompetitiveScore',
     'LUCENE-8464: ConstantScoreScorer now implements setMinCompetitiveScore'),

    ('2019-04-23',
     'Switched to OpenJDK 11',
     'Switched to OpenJDK 11'),

    ('2019-04-30',
     'Switched GC back to ParallelGC (away from default G1GC)',
     'Switched GC back to ParallelGC (away from default G1GC)'),

    ('2019-05-06',
     'LUCENE-8781: FST lookup performance has been improved in many cases by encoding Arcs using full-sized arrays with gaps',
     'LUCENE-8781: FST lookup performance has been improved in many cases by encoding Arcs using full-sized arrays with gaps'),

    ('2019-05-24',
     'LUCENE-8770: Two-phase support in conjunctions',
     'LUCENE-8770: Two-phase support in conjunctions'),

    ('2019-07-02',
     'LUCENE-8901: Load freq blocks lazily',
     'LUCENE-8901: Load freq blocks lazily'),

    ('2019-07-09',
     'LUCENE-8311: Compute impacts for phrase queries',
     'LUCENE-8311: Compute impacts for phrase queries'),

    ('2019-09-26',
     'LUCENE-8980: Blocktree seekExact now checks min-max range of the segment',
     'LUCENE-8980: Blocktree seekExact now checks min-max range of the segment'),

    ('2019-10-14',
     'LUCENE-8920: Disable direct addressing of arcs.',
     'LUCENE-8920: Disable direct addressing of arcs.'),

    ('2019-11-14',
     'LUCENE-8920: Re-enable direct addressing of arcs.',
     'LUCENE-8920: Re-enable direct addressing of arcs.'),

    ('2019-11-19',
     'LUCENE-9027: SIMD decompression of postings.',
     'LUCENE-9027: SIMD decompression of postings.'),

    ('2019-11-21',
     'LUCENE-9056: Fewer conditionals in #nextDoc/#advance',
     'LUCENE-9056: Fewer conditionals in #nextDoc/#advance'),

    ('2020-01-13',
     'Switch to OpenJDK 13',
     'Switch to OpenJDK 13'),

    ('2020-01-14',
     'Switch to OpenJDK 12',
     'Switch to OpenJDK 12'),

    ('2020-01-17',
     'Move invariant checks of CompetitiveImpactAccumulator under an assert',
     'Move invariant checks of CompetitiveImpactAccumulator under an assert'),

    ('2020-01-24',
     'LUCENE-4702: compress suffix bytes in terms dictionary',
     'LUCENE-4702: compress suffix bytes in terms dictionary'),

    ('2020-02-18',
     'LUCENE-9211: Adding compression to BinaryDocValues storage',
     'LUCENE-9211: Adding compression to BinaryDocValues storage'),

    ('2020-09-09',
     'LUCENE-9511: Include StoredFieldsWriter in DWPT accounting',
     'LUCENE-9511: Include StoredFieldsWriter in DWPT accounting'),

    ('2020-10-27',
     'LUCENE-9280: enable optimization to skip non-competitive documents when sorting by field by indexing benchmark datetime field as both points and doc values',
     'LUCENE-9280: optimization to skip non-competitive documents when sorting by field by indexing benchmark datetime field as both points and doc values'),

    ('2020-11-06',
     'Move to new beast 3 Ryzen Threadripper 3990X hardware for all nightly benchmarks',
     'Move to new beast 3 Ryzen Threadripper 3990X hardware for all nightly benchmarks: 64 cores (128 with hyperthreading), 256 GB RAM, 960 GB Intel 905P Optane, Linux 5.9.2-arch1-1, still OpenJDK 12.0.2+10'),

    ('2020-11-14',
     'LUCENE-9378: Configurable compression for binary doc values',
     'LUCENE-9378: Configurable compression for binary doc values'),

    ('2020-12-04',
     'Switch to JDK 15 (from JDK 12)',
     'Switch to JDK 15 (from JDK 12)'),

    ('2020-12-09 10:00:13',
     'Add KNN vectors to indexing metrics',
     'Add KNN vectors to indexing metrics',),

    ('2020-12-10',
     'LUCENE-9626: switch to native arrays for HNSW ANN vector search',
     'LUCENE-9626: switch to native arrays for HNSW ANN vector search'),

    ('2020-12-28',
     'LUCENE-9644: add diversity to HNSW (ANN search) neighbor selection, apparently yielding performance gains to VectorSearch task',
     'LUCENE-9644: add diversity to HNSW (ANN search) neighbor selection, apparently yielding performance gains to VectorSearch task'),

    ('2021-01-09 13:35:50',
     'increase max number of concurrent merges from 3 to 12 for indexing tasks; increase Indexer heap from 8 to 32 GB',
     'increase max number of concurrent merges from 3 to 12 for indexing tasks; increase Indexer heap from 8 to 32 GB'),

    ('2021-01-07',
     'LUCENE-9652: add dedicated method, DataInput.readLEFloats, to read float[] from DataInput, optimizing HNSW KNN',
     'LUCENE-9652: add dedicated method, DataInput.readLEFloats, to read float[] from DataInput, optimizing HNSW KNN'),

    ('2021-01-24 17:25:07',
     'enable BinaryDocValues compression in taxonomy index',
     'enable BinaryDocValues compression in taxonomy index'),

    ('2021-01-28',
     'LUCENE-9695: WTF somehow this bug fix hurt vector indexing throughput?',
     'LUCENE-9695: WTF somehow this bug fix hurt vector indexing throughput?'),

    ('2021-02-25',
     'Upgrade beast3 to Arch Linux 5.11.1',
     'Upgrade beast3 to Arch Linux 5.11.1'),

    ('2021-03-14 08:23:12',
     'Move vectors indexing to dedicated (separate) indexing task',
     'Move vectors indexing to dedicated (separate) indexing task'),

    ('2021-06-24 00:03:16',
     'LUCENE-9613: Create blocks for ords when it helps Lucene80DocValuesFormat',
     'LUCENE-9613: Create blocks for ords when it helps Lucene80DocValuesFormat'),

    ('2021-08-24 00:03:22',
     'LUCENE-5309: specialize single-valued SortedSetDocValues faceting',
     'LUCENE-5309: specialize single-valued SortedSetDocValues faceting'),

    ('2021-08-26 07:26:00',
     'LUCENE-10067: specialize SSDV ordinal decode',
     'LUCENE-10067: specialize SSDV ordinal decode'),

    ('2021-08-27 10:46:59',
     'Upgrade to JDK 16.0.2+7',
     'Upgrade to JDK 16.0.2+7'),

    ('2021-09-01 00:03:16',
     'LUCENE-9662: CheckIndex should be concurrent',
     'LUCENE-9662: CheckIndex should be concurrent',),

    ('2021-09-03 00:03:25',
     'Use 16 concurrent threads for CheckIndex',
     'Use 16 concurrent threads for CheckIndex'),

    ('2021-09-25 00:03:25',
     'LUCENE-10109: Increase default beam width from 16 to 100',
     'LUCENE-10109: Increase default beam width from 16 to 100'),

    ('2021-10-05 12:21:08',
     'Upgrade Linux kernel from 5.13.12 to 5.14.8',
     'Upgrade Linux kernel from 5.13.12 to 5.14.8'),

    ('2021-10-19 08:14:33',
     'Upgrade to JDK17+35, and pass -release to ecj linting',
     'Upgrade to JDK17+35, and pass -release to ecj linting'),

    ('2021-11-29 ',
     'Stop passing -release to ecj since it makes it quite a bit slower',
     'Stop passing -release to ecj since it makes it quite a bit slower'),

    ('2021-11-24 18:04:23',
     'LUCENE-10062: switch to storing taxonomy Facet ordinals from custom encoding in BINARY DV field, to SSDV field',
     'LUCENE-10062: switch to storing taxonomy Facet ordinals from custom encoding in BINARY DV field, to SSDV field'),

    ('2021-12-21',
     'Upgrade Arch Linux kernel from 5.14.x to 5.15.10',
     'Upgrade Arch Linux kernel from 5.14.x to 5.15.10'),

    ('2022-01-03 18:03:13',
     'LUCENE-10346: specialize single-valued doc values during taxonomy facet counting',
     'LUCENE-10346: specialize single-valued doc values during taxonomy facet counting'),

    ('2022-01-19 10:17:11',
     'Upgrade arch linux 5.15.10 -> 5.16.1; LUCENE-10375: Speed up HNSW merge by writing combined vector data',
     'Upgrade arch linux 5.15.10 -> 5.16.1; LUCENE-10375: Speed up HNSW merge by writing combined vector data'),

    ('2022-01-26 18:03:08',
     'LUCENE-10054: Make HnswGraph hierarchical',
     'LUCENE-10054: Make HnswGraph hierarchical'),

    ('2022-02-18 07:54:59',
     'LUCENE-10391: Reuse data structures across HnswGraph#searchLevel calls',
     'LUCENE-10391: Reuse data structures across HnswGraph#searchLevel calls'),

    ('2022-02-18 07:54:59',
     'LUCENE-10408 Better encoding of doc Ids in vectors',
     'LUCENE-10408 Better encoding of doc Ids in vectors'),

    ('2022-02-25 18:03:10',
     'LUCENE-10421: Use fixed seed for HNSW search',
     'LUCENE-10421: Use fixed seed for HNSW search'),

    ('2022-03-23 18:03:07',
     'LUCENE-10481: FacetsCollector sets ScoreMode.COMPLETE_NO_SCORES when scores are not needed',
     'LUCENE-10481: FacetsCollector sets ScoreMode.COMPLETE_NO_SCORES when scores are not needed'),

    ('2022-04-21 18:03:04',
     'LUCENE-10517: specialize SSDV pure-browse facets for some cases',
     'LUCENE-10517: specialize SSDV pure-browse facets for some cases'),

    ('2022-05-12 18:02:51',
     'LUCENE-10527: Use 2*maxConn for last layer in HNSW',
     'LUCENE-10527: Use 2*maxConn for last layer in HNSW'),

    ('2022-05-19',
     'GITHUB#11610: Prevent pathological O(N^2) merges',
     'GITHUB#11610: Prevent pathological O(N^2) merges'),

    ('2022-06-07 18:02:50',
     'LUCENE-10078: Enable merge-on-refresh by default',
     'LUCENE-10078: Enable merge-on-refresh by default'),

    ('2022-07-04 18:02:46',
     'LUCENE-10480: Use BMM scorer for 2 clauses disjunction',
     'LUCENE-10480: Use BMM scorer for 2 clauses disjunction'),

    ('2022-07-08 18:02:52',
     'LUCENE-10480: Move scoring from advance to TwoPhaseIterator#matches',
     'LUCENE-10480: Move scoring from advance to TwoPhaseIterator#matches'),

    ('2022-07-13 18:02:33',
     '#1010: Specialize ordinal encoding for SortedSetDocValues',
     '#1010: Specialize ordinal encoding for SortedSetDocValues'),

    ('2022-07-23 06:00:31',
     'LUCENE-10592: Build HNSW graph during indexing',
     'LUCENE-10592: Build HNSW graph during indexing'),

    ('2022-07-30 18:02:50',
     'LUCENE-10633: Dynamic pruning for queries sorted by SORTED(_SET) field',
     'LUCENE-10633: Dynamic pruning for queries sorted by SORTED(_SET) field'),

    ('2022-08-10',
     'GITHUB#1017: Add new ShapeDocValuesField for LatLonShape and XYShape',
     'GITHUB#1017: Add new ShapeDocValuesField for LatLonShape and XYShape'),

    ('2022-09-28',
     'GITHUB#11824: recover performance regression of LatLonPoint#newPolygonQuery',
     'GITHUB#11824: recover performance regression of LatLonPoint#newPolygonQuery'),

    ('2022-09-29',
     'Update all Arch Linux packages, including kernel from 5.17.5 -> 5.19.12',
     'Update all Arch Linux packages, including kernel from 5.17.5 -> 5.19.12'),

    ('2022-09-30',
     'Switch from OpenJDK 17.0.1+12 -> standard Arch Linux OpenJDK 17.0.4.1+1',
     'Switch from OpenJDK 17.0.1+12 -> standard Arch Linux OpenJDK 17.0.4.1+1'),

    ('2022-10-06',
     'luceneutil#192: increase the default topN from 10 to 100',
     'luceneutil#192: increase the default topN from 10 to 100'),

    ('2022-10-19',
     'Fix CombinedFieldsQuery tasks: https://github.com/mikemccand/luceneutil/commit/56729cf341a443fb81148dd25d3d49cb88bc72e8',
     'Fix CombinedFieldsQuery tasks: https://github.com/mikemccand/luceneutil/commit/56729cf341a443fb81148dd25d3d49cb88bc72e8'),

    ('2022-10-26',
     'Upgrade to OpenJDK 19.0.1+10',
     'Upgrade to OpenJDK 19.0.1+10'),

    ('2022-11-03',
     'Add --enable-preview command-line JVM flag to test new Panama-based MMapDirectory implementation',
     'Add --enable-preview command-line JVM flag to test new Panama-based MMapDirectory implementation'),

    ('2022-11-16',
     'GITHUB#11939: fix bug of incorrect cost after upgradeToBitSet in DocIdSetBuilder class',
     'GITHUB#11939: fix bug of incorrect cost after upgradeToBitSet in DocIdSetBuilder class'),

    ('2023-02-28',
     'GITHUB#12055: Better skipping for multi-term queries with a FILTER rewrite',
     'GITHUB#12055: Better skipping for multi-term queries with a FILTER rewrite'),

]
