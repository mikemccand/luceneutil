import pickle
import sys
import re
import os
import datetime

def parse_timestamp(s):
  return datetime.datetime.fromisoformat(s)

# TODO
#   - cutover to https://github.com/apache/lucene/issues/14182 once it's done
#   - support addIndexes source too
#   - what to do with the RAM MB -> flushed size MB ratio?  it'd be nice to measure "ram efficiency" somehow of newly flushed segments..
#   - get an infostream w/ concurrent deletes ... maybe from the NRT indexing test?
#   - record segment id?
#   - also part commitMergeDeletes line
#   - fix IW/infostream/tracing
#     - note why a flush was triggered -- NRT reader, commit, RAM full, flush-by-doc-count, etc.
#   - open issue for this WTF:
'''
IFD 0 [2025-01-31T12:35:58.900206053Z; Lucene Merge Thread #0]: now delete 0 files: []
IFD 0 [2025-01-31T12:35:58.900827035Z; Lucene Merge Thread #0]: now delete 0 files: []
IFD 0 [2025-01-31T12:35:58.901284266Z; Lucene Merge Thread #0]: now delete 0 files: []
IFD 0 [2025-01-31T12:35:58.901879608Z; Lucene Merge Thread #0]: now delete 0 files: []
IFD 0 [2025-01-31T12:35:58.902341316Z; Lucene Merge Thread #0]: now delete 0 files: []
IFD 0 [2025-01-31T12:35:58.902886391Z; Lucene Merge Thread #0]: now delete 0 files: []
IFD 0 [2025-01-31T12:35:58.903504215Z; Lucene Merge Thread #0]: now delete 0 files: []
IFD 0 [2025-01-31T12:35:58.904037509Z; Lucene Merge Thread #0]: now delete 0 files: []
IFD 0 [2025-01-31T12:35:58.904657761Z; Lucene Merge Thread #0]: now delete 0 files: []
IFD 0 [2025-01-31T12:35:58.905104934Z; Lucene Merge Thread #0]: now delete 0 files: []
'''

class Segment:

  all_segments = []

  # how many docs are deleted when this segment is first lit.  for a
  # flushed segment, these are deletions of newly indexed documents in
  # the segment, before the segment was flushed.  for a merged
  # segment, these are deletions of documents in the merging segments
  # that arrived after the merge started and before it
  # finished/committed/was lit:
  born_del_count = None

  # when this segment was lit (enrolled into in-memory segment infos)
  publish_timestamp = None

  # which segment this segment was merged into:
  merged_into = None
  
  def __init__(self, name, source, max_doc, size_mb, start_time, end_time, start_infostream_line_number):
    # 'flush' or 'merge'
    Segment.all_segments.append(self)
    self.source = source
    self.name = name
    assert (type(source) is str and source == 'flush') or (type(source) is tuple and source[0] == 'merge' and len(source) > 1)
    self.max_doc = max_doc
    self.size_mb = size_mb
    self.start_time = start_time
    self.start_infostream_line_number = start_infostream_line_number
    self.end_time = end_time
    # e.g. 'write_doc_values', 'write_knn', 'merging_started', 'add_deletes'...
    self.events = []

  def add_event(self, timestamp, event, infostream_line_number):
    if len(self.events) > 0 and timestamp < self.events[-1][0]:
      raise RuntimeError(f'events must append forwards in time: segment {self.name} has {self.events=} but new {event=} moves backwards?')
    self.events.append((timestamp, event, infostream_line_number))

  def to_verbose_string(self, global_start_time):
    l = []
    l.append(f'segment {self.name} ({self.source})')
    l.append(f'  max_doc={self.max_doc}')
    l.append(f'  {self.size_mb} MB')
    l.append(f'  created at {(self.start_time - global_start_time).total_seconds():.1f} sec')
    if self.end_time is not None:
      l.append(f'  {(self.end_time - self.start_time).total_seconds():.1f} sec lifetime (merged into {self.merged_into.name})')
    l.append(f'  events:')
    for ts, event, line_number in self.events:
      l.append(f'    {(ts - self.start_time).total_seconds():.3f} sec: {event}')
    return '\n'.join(l)

def to_float(s):
  '''
  Converts float number strings that might have comma, e.g. '1,464.435', to float.
  '''
  return float(s.replace(',', ''))

'''
Parses a full IndexWriter InfoStream to per-segment details for rendering with segments_to_html.py
'''

def main():
  if len(sys.argv) != 3:
    raise RuntimeError('Usage: python3 -u infostream_to_segments.py <path-to-infostream-log> <path-to-segments-file-out>')

  infostream_log = sys.argv[1]
  segments_out = sys.argv[2]
  if os.path.exists(segments_out):
    raise RuntimeError(f'please first remove output file {segments_out}')

  by_segment_name = {}
  by_thread_name = {}

  # e.g. MS 0 [2025-01-31T12:35:34.054288816Z; main]: initDynamicDefaults maxThreadCount=16 maxMergeCount=21
  re_timestamp_thread = re.compile(r'^.*? \[([^;]+); (.*?)\]: ')

  # e.g.: DWPT 0 [2025-01-31T12:35:49.296482953Z; Index #1]: flush postings as segment _m numDocs=49800
  re_flush_start = re.compile(r'^DWPT \d+ \[([^;]+); (.*?)\]: flush postings as segment _(.*?) numDocs=(\d+)$')

  # e.g.: IW 0 [2025-01-31T12:35:49.642983664Z; Index #10]: 330 ms to write points
  re_msec_to_write = re.compile(r'^IW \d+ \[([^;]+); (.*?)\]: (\d+) ms to write (.*?)$')

  # e.g.: DWPT 0 [2025-01-31T12:35:51.198819989Z; Index #34]: new segment has 0 deleted docs
  re_new_segment_deleted_docs = re.compile(r'^DWPT \d+ \[([^;]+); (.*?)\]: new segment has (\d+) deleted docs$')

  # e.g.: DWPT 0 [2025-01-31T12:35:51.200492790Z; Index #34]: flushed: segment=_0 ramUsed=74.271 MB newFlushedSize=33.995 MB docs/MB=1,464.435
  re_new_flushed_size = re.compile(r'^DWPT \d+ \[([^;]+); (.*?)\]: flushed: segment=_(.*?) ramUsed=(.*) MB newFlushedSize=(.*?) MB docs/MB=(.*?)$')

  # e.g.: IW 0 [2025-01-31T12:36:04.992415479Z; Index #2]: publish sets newSegment delGen=38 seg=_11(11.0.0):C49789:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326964992, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpprjm
  re_publish_delgen = re.compile(r'^IW \d+ \[([^;]+); (.*?)\]: publish sets newSegment delGen=\d+ seg=_([^\(]+)')

  # e.g.: DWPT 0 [2025-01-31T12:35:51.801527396Z; Index #5]: flush time 3097.274264 ms
  re_flush_time = re.compile(r'^DWPT \d+ \[([^;]+); (.*?)\]: flush time (.*?) ms$')
  
  # maybe?: DWPT 0 [2025-01-31T12:35:51.200730335Z; Index #34]: flush time 2588.276633 ms

  # e.g.: IW 0 [2025-01-31T12:35:51.817783927Z; Lucene Merge Thread #0]: merge seg=_1y _f(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951546, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvw _l(11.0.0):C49798:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951756, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvy _u(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951689, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwj _w(11.0.0):C49797:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951801, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvq _a(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951625, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw3 _r(11.0.0):C49783:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951688, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvs _e(11.0.0):C49779:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951736, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvr _p(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951761, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwa _5(11.0.0):C49778:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951706, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwq _h(11.0.0):C49793:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951625, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw7
  re_merge_start = re.compile(r'^IW \d+ \[([^;]+); (.*?)\]: merge seg=_(.*?) (.*?)$')

  re_merging_segment = re.compile(r'_(.*?)\(.*?\):([cC])(\d+):\[diagnostics=\{.*?\}\]:\[attributes=\{.*?\}\] :id=[0-9a-z]+?')

  # e.g.: SM 0 [2025-01-31T12:35:51.981685491Z; Lucene Merge Thread #2]: 13 ms to merge stored fields [497911 docs]
  re_msec_to_merge = re.compile(r'^SM \d+ \[([^;]+); (.*?)\]: (\d+) ms to merge (.*?) \[(\d+) docs\]$')

  # e.g.: IW 0 [2025-01-31T12:35:58.899009723Z; Lucene Merge Thread #0]: merge codec=Lucene101 maxDoc=497858; merged segment has no vectors; norms; docValues; prox; freqs; points; 7.1 sec to merge segment [303.82 MB, 42.94 MB/sec]
  re_merge_finish = re.compile(r'^IW \d+ \[([^;]+); (.*?)\]: merge codec=(.*?) maxDoc=(\d+);.*? \[([0-9.]+) MB, ([0-9.]+) MB/sec\]$')

  # e.g.: IW 0 [2025-01-31T12:35:58.899418368Z; Lucene Merge Thread #0]: commitMerge: _f(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951546, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvw _l(11.0.0):C49798:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951756, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvy _u(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951689, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwj _w(11.0.0):C49797:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951801, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvq _a(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951625, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw3 _r(11.0.0):C49783:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951688, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvs _e(11.0.0):C49779:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951736, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvr _p(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951761, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwa _5(11.0.0):C49778:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951706, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwq _h(11.0.0):C49793:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951625, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw7 index=_0(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951200, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppst _w(11.0.0):C49797:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951801, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvq _e(11.0.0):C49779:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951736, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvr _r(11.0.0):C49783:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951688, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvs _y(11.0.0):C49795:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951485, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvt _g(11.0.0):C49795:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951687, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvu _v(11.0.0):C49790:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951685, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvv _f(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951546, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvw _1(11.0.0):C49789:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951627, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvx _l(11.0.0):C49798:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951756, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvy _6(11.0.0):C49778:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951640, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvz _n(11.0.0):C49793:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951685, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw0 _7(11.0.0):C49800:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951687, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw1 _d(11.0.0):C49776:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951729, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw2 _a(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951625, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw3 _t(11.0.0):C49780:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951790, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw4 _s(11.0.0):C49786:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951536, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw5 _9(11.0.0):C49775:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951756, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw6 _h(11.0.0):C49793:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951625, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw7 _8(11.0.0):C49791:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951619, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw8 _2(11.0.0):C49792:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951688, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw9 _p(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951761, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwa _4(11.0.0):C49796:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951687, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwb _b(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951709, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwd _3(11.0.0):C49780:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951702, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwf _o(11.0.0):C49799:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951688, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwg _j(11.0.0):C49786:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951636, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwi _u(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951689, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwj _x(11.0.0):C49785:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951629, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwk _z(11.0.0):C49779:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951568, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwl _k(11.0.0):C49785:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951685, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwm _c(11.0.0):C49801:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951558, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwn _q(11.0.0):C49795:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951690, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwo _i(11.0.0):C49798:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951641, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwp _5(11.0.0):C49778:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951706, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwq _m(11.0.0):C49800:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951966, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppx2
  re_merge_commit = re.compile(r'^IW \d+ \[([^;]+); (.*?)\]: commitMerge: ')
  
  line_number = -1

  global_start_time = None
  global_end_time = None
  
  with open(infostream_log, 'r') as f:
    while True:
      line = f.readline()
      if line == '':
        break
      line = line.strip()
      # print(f'{line}')
      line_number += 1

      try:

        m = re_timestamp_thread.search(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          if global_start_time is None:
            global_start_time = timestamp
          global_end_time = timestamp
          
        m = re_flush_start.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
            
          thread_name = m.group(2)
          segment_name = '_' + m.group(3)
          max_doc = int(m.group(4))

          segment = Segment(segment_name, 'flush', max_doc, None, timestamp, None, line_number)
          by_segment_name[segment_name] = segment
          assert thread_name not in by_thread_name, f'thread {thread_name} was already/still in by_thread_name?'

          by_thread_name[thread_name] = segment

          # print(f'flush start at {timestamp}: {segment_name=} {max_doc=} {thread_name=}')
          continue

        m = re_msec_to_write.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          msec = int(m.group(3))
          index_part = m.group(4)

          segment = by_thread_name[thread_name]

          # TODO: hmm this is "off by one"?  it's when we finished writing XYZ?  i could subtract the msec from the timestamp maybe?
          segment.add_event(timestamp, f'done write {index_part}', line_number)
          continue

        m = re_new_segment_deleted_docs.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          del_count = int(m.group(3))
          segment = by_thread_name[thread_name]
          segment.born_del_count = del_count
          continue

        m = re_new_flushed_size.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          segment_name = '_' + m.group(3)
          ram_used_mb = float(m.group(4))
          new_flushed_size_mb = float(m.group(5))
          docs_per_mb = to_float(m.group(6))

          segment = by_segment_name[segment_name]
          assert segment == by_thread_name[thread_name]
          segment.size_mb = new_flushed_size_mb
          continue

        m = re_flush_time.search(line)
        if m is not None:
          # this is the last per-segment message printed for the thread doing the flushing:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          segment = by_thread_name[thread_name]
          segment.publish_timestamp = timestamp
          # no?
          # segment.end_time = timestamp
          del by_thread_name[thread_name]
          continue
        
        m = re_publish_delgen.search(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          segment_name = '_' + m.group(3)
          by_segment_name[segment_name].add_event(timestamp, 'light', line_number)
          continue
        
        m = re_merge_start.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          segment_name = '_' + m.group(3)
          # parse the very verbose merging segments description
          merging_segments = re_merging_segment.findall(m.group(4))
          merging_segment_names = [f'_{x[0]}' for x in merging_segments]
          
          print(f'{len(merging_segments)} merging segments in {m.group(4)}: {merging_segments}')

          segment = Segment(segment_name, ('merge', merging_segment_names), None, None, timestamp, None, line_number)
          by_segment_name[segment_name] = segment
          by_thread_name[thread_name] = segment
          continue

        m = re_msec_to_merge.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          msec = int(m.group(3))
          part = m.group(4)
          max_doc = int(m.group(5))
          segment = by_thread_name[thread_name]
          segment.add_event(timestamp, f'done merge {part}', line_number)
          if segment.max_doc is None:
            segment.max_doc = max_doc
          elif max_doc != segment.max_doc:
            raise RuntimeError(f'merging segment {segment.name} sees different max_doc={segment.max_doc} vs {max_doc}')

        m = re_merge_finish.search(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          segment = by_thread_name[thread_name]
          # codec
          max_doc = int(m.group(4))
          size_mb = float(m.group(5))
          # mb/sec
          if segment.max_doc is None:
            segment.max_doc = max_doc
          elif max_doc != segment.max_doc:
            raise RuntimeError(f'merging segment {segment.name} sees different max_doc={segment.max_doc} vs {max_doc}')
          segment.size_mb = size_mb
          segment.add_event(timestamp, f'done merge', line_number)
          
        m = re_merge_commit.search(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          segment = by_thread_name[thread_name]
          # TODO: also parse/verify the "index=" part of this line?  that we know all the segments in the index now?
          segment.add_event(timestamp, 'light', line_number)

          for segment_name in segment.source[1]:
            old_segment = by_segment_name[segment_name]
            old_segment.end_time = timestamp
            old_segment.merged_into = segment
          
      except KeyboardInterrupt:
        raise
      except:
        print(f'unhandled exception on line {line_number} of {infostream_log}: {line}')
        raise

  for segment in Segment.all_segments:
    print(f'\n{segment.name}:\n{segment.to_verbose_string(global_start_time)}')

  open(segments_out, 'wb').write(pickle.dumps((global_start_time, global_end_time, Segment.all_segments)))
  print(f'wrote {len(Segment.all_segments)} segments to "{segments_out}": {os.path.getsize(segments_out)/1024/1024:.1f} MB')

if __name__ == '__main__':
  main()
