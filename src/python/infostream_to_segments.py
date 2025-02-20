#
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
#

import pickle
import bisect
import sys
import re
import os
import math
import datetime

def parse_timestamp(s):
  return datetime.datetime.fromisoformat(s)

# TODO
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

# e.g. typically many on one line like this: _f(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951546, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvw
#   or (with static index sort): IFD 211 [2025-02-06T18:26:20.518261400Z; GCR-Writer-1-thread-6]: now checkpoint "_a(9.11.2):C14171/34:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={timestamp=1738866380509, java.runtime.version=23.0.2+7, java.vendor=Amazon.com Inc., os=Linux, os.arch=aarch64, os.version=4.14.355-275.582.amzn2.aarch64, lucene.version=9.11.2, source=flush}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=dz4cethjje165l9ysayg0m9f1" [1 segments ; isCommit = false]
re_segment_desc = re.compile(r'_(.*?)\(.*?\):([cC])(\d+)(/\d+)?:\[(indexSort=.*?)?(diagnostics=\{.*?\})\]:\[attributes=(\{.*?\})\](:delGen=\d+)? :id=[0-9a-z]+?')


def sec_to_time_delta(td_sec):
  hr = int(td_sec/3600.)
  mn = int((td_sec - hr*3600)/60.)
  sc = td_sec - hr*3600 - mn*60
  return f'{hr:02}:{mn:02}:{sc:04.1f}'

class Segment:

  all_segments = []

  # how many docs are deleted when this segment is first lit.  for a
  # flushed segment, these are deletions of newly indexed documents in
  # the segment, before the segment was flushed.  for a merged
  # segment, these are deletions of documents in the merging segments
  # that arrived after the merge started and before it
  # finished/committed/was lit:
  born_del_count = None

  # if this segment was produced by merge, the number of deleted docs that
  # merge compacted away in total from the merging segments
  del_count_reclaimed = None

  # how many of my own deletes were collapsed when I got merged
  del_count_merged_away = None

  # current deleted doc count
  del_count = None

  # when this segment was lit (enrolled into in-memory segment infos)
  publish_timestamp = None

  # which segment this segment was merged into:
  merged_into = None

  # None if this merge was not a merge-on-commit.  if it is, then this is a tuple ('success' | 'timeout', timedelta-in-seconds)
  merge_during_commit = None

  # how much RAM this (flushed) segment was using in IW's accounting
  # when it was flushed (None for merged segments)
  ram_used_mb = None

  # int ordinal counting which full flush this segment was part of
  full_flush_ord = None

  # currently only set for flushed segments. sometimes 34 files!!
  file_count = None

  # for merged segments, how large IW estimated it would be (based on simple pro-rata discount from %deletes + sum of input segment sizes)
  estimate_size_mb = None

  is_cfs = False
  
  def __init__(self, name, source, max_doc, size_mb, start_time, end_time, start_infostream_line_number):
    # 'flush' or 'merge'
    Segment.all_segments.append(self)
    self.source = source
    self.name = name
    assert (type(source) is str and source in ('flush', 'flush-commit', 'flush-by-RAM')) or (type(source) is tuple and source[0] == 'merge' and len(source) > 1)
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

  def merge_event(self, timestamp, event, infostream_line_number):
    '''
    Like add_event, except the timestamp might be out of order, so we do insertion sort.
    '''
    insert_at = bisect.bisect_right(self.events, timestamp, key=lambda x: x[0])
    self.events.insert(insert_at, (timestamp, event, infostream_line_number))

  def to_verbose_string(self, global_start_time, global_end_time, with_line_numbers):
    l = []
    s = f'segment {self.name}'
    if self.source in ('flush', 'flush-commit', 'flush-by-RAM'):
      s += f' ({self.source})'
    elif self.merge_during_commit is not None:
      s += ' (merge-on-commit: ' + ' '.join(self.source[1]) + ')'
    else:
      s += ' (merge: ' + ' '.join(self.source[1]) + ')'
    l.append(s)
    if self.merge_during_commit is not None:
      if type(self.merge_during_commit) is tuple:
        state, seconds = self.merge_during_commit
        l.append(f'  merge-during-commit: {state} {seconds:.2f} sec')
      else:
        # could be a single int, if merge was still running at end of InfoStream
        pass
    if self.is_cfs:
      l.append('  cfs')
    else:
      l.append('  not cfs')
      
    if self.del_count_reclaimed is not None:
      l.append(f'  del_count_reclaimed={self.del_count_reclaimed:,} ({100.*self.del_count_reclaimed/(self.max_doc + self.del_count_reclaimed):.1f}%)')
    md = f'  max_doc={self.max_doc:,}'
    if self.size_mb is not None:
      md += f' docs_per_mb={self.max_doc/self.size_mb:,.1f}'
    l.append(md)
    if self.born_del_count is not None:
      l.append(f'  born_del_count={self.born_del_count:,} ({100.*self.born_del_count/self.max_doc:.1f}%)')
    if self.del_count is not None:
      if self.end_time is None:
        end_time = global_end_time
      else:
        end_time = self.end_time
      dps = self.del_count / (end_time - self.start_time).total_seconds()
      l.append(f'  final del_count={self.del_count:,} ({100.*self.del_count/self.max_doc:.1f}%, {dps:,.1f} dd/s)')
    if type(self.source) is str and self.source.startswith('flush'):
      if self.ram_used_mb is None:
        # this can happen if InfoStream ends as a segment is flushing
        # raise RuntimeError(f'flushed segment {self.name} is missing ramUsedMB')
        if self.size_mb is None:
          l.append(f'  ?? MB (?? efficiency)')
        else:
          l.append(f'  {self.size_mb:,.1f} MB (?? efficiency)')
      else:
        l.append(f'  {self.size_mb:,.1f} MB ({100*self.size_mb/self.ram_used_mb:.1f}% efficiency, ram_used_mb={self.ram_used_mb:,.1f} MB)')
    else:
      if self.size_mb is None:
        l.append(f'  ?? MB')
      elif self.estimate_size_mb is not None:
        pct = 100.*self.size_mb / self.estimate_size_mb
        l.append(f'  {self.size_mb:,.1f} MB vs estimate {self.estimate_size_mb:,.1f} MB {pct:.1f}%')
      else:
        l.append(f'  {self.size_mb:,.1f} MB vs estimate ?? MB')
    l.append(f'  created at {(self.start_time - global_start_time).total_seconds():,.1f} sec')
    if self.publish_timestamp is not None:
      btt = (self.publish_timestamp - self.start_time).total_seconds()
      bt = sec_to_time_delta(btt)
    else:
      btt = 0
      bt = '??'
    if self.end_time is not None:
      if self.merged_into is not None:
        dtt = (self.end_time - self.merged_into.start_time).total_seconds()
        dt = sec_to_time_delta(dtt)
      else:
        dt = '??'
        dtt = 0
      ltt = (self.end_time - self.start_time).total_seconds()
    else:
      dt = '0.0'
      dtt = 0
      ltt = (global_end_time - self.start_time).total_seconds()

    l.append(f'  times: {bt} / {sec_to_time_delta(ltt-btt-dtt)} / {dt}')
    if self.merged_into is not None:
      l.append(f'  merged into {self.merged_into.name}')
    l.append(f'  events:')
    last_ts = self.start_time

    # we subsample all delete flushes if there are too many...:
    del_count_count = len([x for x in self.events if x[1].startswith('del ')])

    if del_count_count > 0:
      print_per_del = 9./del_count_count
    else:
      print_per_del = None

    last_del_print = -1
    del_inc = 0

    # if we have too many del_count events, subsample which ones to
    # print: always print first and last, but subsample in between to
    # get to ~9 ish
    do_del_print = {}
    last_del_index = None
    for i, (ts, event, line_number) in enumerate(self.events):
      if event.startswith('del '):
        last_del_index = i
        del_inc += print_per_del
        if int(del_inc) == last_del_print:
          # record that we skipped some del prints AFTER the prior one, so we
          # can add ellipsis below:
          do_del_print[last_del_print_index] = True
          continue
        do_del_print[i] = False
        last_del_print = int(del_inc)
        last_del_print_index = i

    do_del_print[last_del_index] = False

    sum_del_count = 0
    for i, (ts, event, line_number) in enumerate(self.events):
      is_del = event.startswith('del ')
      if is_del:
        # print(f'got {event}')
        del_inc = int(event[4:event.find(' ', 4)].replace(',', ''))
        sum_del_count += del_inc
        event += f' ({100.*sum_del_count/self.max_doc:.1f}%)'
      s = f'+{(ts - last_ts).total_seconds():.2f}'

      if not is_del or i in do_del_print:
        if with_line_numbers:
          s = f'    {s:>7s} s: {event} [line {line_number:,}]'
        else:
          s = f'    {s:>7s} s: {event}'
        if is_del and do_del_print.get(i):
          s += '...'
        l.append(s)
      last_ts = ts
        
    return '\n'.join(l)

def to_float(s):
  '''
  Converts float number strings that might have comma, e.g. '1,464.435', to float.
  '''
  return float(s.replace(',', ''))

'''
Parses a full IndexWriter InfoStream to per-segment details for rendering with segments_to_html.py
'''

def parse_seg_details(s):
  seg_details = []
  for tup in re_segment_desc.findall(s):
    # TODO: we could parse timestamp from diagnostics...
    segment_name = '_' + tup[0]
    cfs_letter = tup[1]
    if cfs_letter == 'C':
      is_cfs = False
    elif cfs_letter == 'c':
      is_cfs = True
    else:
      raise RuntimeError(f'expected c or C for CFS boolean but got {cfs_letter}')
    max_doc = int(tup[2])
    if tup[3] != '':
      del_count = int(tup[3][1:])
    else:
      del_count = 0

    diagnostics = tup[5]
    if 'source=merge' in diagnostics:
      source = ('merge', [])
    elif 'source=flush' in diagnostics:
      source = 'flush'
    else:
      raise RuntimeError(f'seg {segment_name}: a new source in {diagnostics}?')

    attributes = tup[6]
    del_gen = tup[7]
    if del_gen != '':
      del_gen = int(del_gen[8:])
    else:
      del_gen = None

    seg_details.append((segment_name, source, is_cfs, max_doc, del_count, del_gen, diagnostics, attributes))

  return seg_details
  
def main():
  if len(sys.argv) != 3:
    raise RuntimeError('Usage: python3 -u infostream_to_segments.py <path-to-infostream-log> <path-to-segments-file-out>')

  infostream_log = sys.argv[1]
  segments_out = sys.argv[2]
  if os.path.exists(segments_out):
    raise RuntimeError(f'please first remove output file {segments_out}')

  by_segment_name = {}
  by_thread_name = {}

  full_flush_count = -1

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
  
  # e.g.: IW 0 [2025-02-02T20:48:42.699743134Z; ReopenThread]: create compound file
  re_create_compound_file = re.compile(r'^IW \d+ \[([^;]+); (.*?)\]: create compound file$')
  
  # maybe?: DWPT 0 [2025-01-31T12:35:51.200730335Z; Index #34]: flush time 2588.276633 ms

  # e.g.: IW 0 [2025-01-31T12:35:51.817783927Z; Lucene Merge Thread #0]: merge seg=_1y _f(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951546, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvw _l(11.0.0):C49798:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951756, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvy _u(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951689, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwj _w(11.0.0):C49797:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951801, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvq _a(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951625, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw3 _r(11.0.0):C49783:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951688, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvs _e(11.0.0):C49779:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951736, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvr _p(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951761, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwa _5(11.0.0):C49778:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951706, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwq _h(11.0.0):C49793:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951625, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw7
  re_merge_start = re.compile(r'^IW \d+ \[([^;]+); (.*?)\]: merge seg=_(.*?) (.*?)$')

  # e.g.: SM 0 [2025-01-31T12:35:51.981685491Z; Lucene Merge Thread #2]: 13 ms to merge stored fields [497911 docs]
  re_msec_to_merge = re.compile(r'^SM \d+ \[([^;]+); (.*?)\]: (\d+) ms to merge (.*?) \[(\d+) docs\]$')
  re_msec_to_build_docmaps = re.compile(r'^SM \d+ \[([^;]+); (.*?)\]: ([\d.]+) msec to build merge sorted DocMaps$')

  # e.g.: IW 0 [2025-01-31T12:35:58.899009723Z; Lucene Merge Thread #0]: merge codec=Lucene101 maxDoc=497858; merged segment has no vectors; norms; docValues; prox; freqs; points; 7.1 sec to merge segment [303.82 MB, 42.94 MB/sec]
  re_merge_finish = re.compile(r'^IW \d+ \[([^;]+); (.*?)\]: merge codec=(.*?) maxDoc=(\d+);.*? \[([0-9.]+) MB, ([0-9.]+) MB/sec\]$')

  # e.g.: IW 0 [2025-01-31T12:35:58.899418368Z; Lucene Merge Thread #0]: commitMerge: _f(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951546, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvw _l(11.0.0):C49798:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951756, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvy _u(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951689, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwj _w(11.0.0):C49797:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951801, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvq _a(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951625, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw3 _r(11.0.0):C49783:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951688, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvs _e(11.0.0):C49779:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951736, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvr _p(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951761, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwa _5(11.0.0):C49778:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951706, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwq _h(11.0.0):C49793:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951625, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw7 index=_0(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951200, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppst _w(11.0.0):C49797:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951801, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvq _e(11.0.0):C49779:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951736, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvr _r(11.0.0):C49783:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951688, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvs _y(11.0.0):C49795:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951485, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvt _g(11.0.0):C49795:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951687, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvu _v(11.0.0):C49790:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951685, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvv _f(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951546, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvw _1(11.0.0):C49789:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951627, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvx _l(11.0.0):C49798:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951756, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvy _6(11.0.0):C49778:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951640, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvz _n(11.0.0):C49793:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951685, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw0 _7(11.0.0):C49800:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951687, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw1 _d(11.0.0):C49776:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951729, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw2 _a(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951625, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw3 _t(11.0.0):C49780:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951790, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw4 _s(11.0.0):C49786:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951536, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw5 _9(11.0.0):C49775:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951756, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw6 _h(11.0.0):C49793:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951625, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw7 _8(11.0.0):C49791:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951619, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw8 _2(11.0.0):C49792:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951688, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw9 _p(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951761, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwa _4(11.0.0):C49796:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951687, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwb _b(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951709, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwd _3(11.0.0):C49780:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951702, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwf _o(11.0.0):C49799:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951688, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwg _j(11.0.0):C49786:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951636, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwi _u(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951689, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwj _x(11.0.0):C49785:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951629, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwk _z(11.0.0):C49779:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951568, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwl _k(11.0.0):C49785:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951685, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwm _c(11.0.0):C49801:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951558, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwn _q(11.0.0):C49795:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951690, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwo _i(11.0.0):C49798:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951641, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwp _5(11.0.0):C49778:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951706, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwq _m(11.0.0):C49800:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951966, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppx2
  re_merge_commit = re.compile(r'^IW \d+ \[([^;]+); (.*?)\]: commitMerge: ')

  # e.g. IFD 0 [2025-02-02T20:48:41.430973144Z; main]: now checkpoint "_89(11.0.0):C10061266:[diagnostics={timestamp=1738471577758, os.version=6.12.4-arch1-1, os=Linux, java.vendor=Arch Linux, mergeFactor=10, java.runtime.version=23, os.arch=amd64, source=merge, lucene.version=11.0.0, mergeMaxNumSegments=-1}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3ses _1c(11.0.0):C621189:[diagnostics={timestamp=1738471469509, os.version=6.12.4-arch1-1, os=Linux, java.vendor=Arch Linux, mergeFactor=10, java.runtime.version=23, os.arch=amd64, source=merge, lucene.version=11.0.0, mergeMaxNumSegments=-1}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3run _1o(11.0.0):C858490:[diagnostics={timestamp=1738471473331, os.version=6.12.4-arch1-1, os=Linux, java.vendor=Arch Linux, mergeFactor=10, java.runtime.version=23, os.arch=amd64, source=merge, lucene.version=11.0.0, mergeMaxNumSegments=-1}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3rw2 _6s(11.0.0):C9775889:[diagnostics={timestamp=1738471534474, os.version=6.12.4-arch1-1, os=Linux, java.vendor=Arch Linux, mergeFactor=10, java.runtime.version=23, os.arch=amd64, source=merge, lucene.version=11.0.0, mergeMaxNumSegments=-1}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3ser _24(11.0.0):C1173700:[diagnostics={timestamp=1738471479224, os.version=6.12.4-arch1-1, os=Linux, java.vendor=Arch Linux, mergeFactor=10, java.runtime.version=23, os.arch=amd64, source=merge, lucene.version=11.0.0, mergeMaxNumSegments=-1}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3ryj _3n(11.0.0):C1116447:[diagnostics={timestamp=1738471497653, os.version=6.12.4-arch1-1, os=Linux, java.vendor=Arch Linux, mergeFactor=10, java.runtime.version=23, os.arch=amd64, source=merge, lucene.version=11.0.0, mergeMaxNumSegments=-1}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3s2x _4i(11.0.0):C1133465:[diagnostics={timestamp=1738471507644, os.version=6.12.4-arch1-1, os=Linux, java.vendor=Arch Linux, mergeFactor=10, java.runtime.version=23, os.arch=amd64, source=merge, lucene.version=11.0.0, mergeMaxNumSegments=-1}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3s5c _7v(11.0.0):C1164338:[diagnostics={timestamp=1738471546722, os.version=6.12.4-arch1-1, os=Linux, java.vendor=Arch Linux, mergeFactor=10, java.runtime.version=23, os.arch=amd64, source=merge, lucene.version=11.0.0, mergeMaxNumSegments=-1}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sd3 _7d(11.0.0):C108162:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471553445, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sdu _7x(11.0.0):C34764:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471553979, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sdv _7n(11.0.0):C74924:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471555045, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sdw _7h(11.0.0):C80743:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471556260, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sdx _7u(11.0.0):C44341:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471556917, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sdy _7z(11.0.0):C29383:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471557330, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sdz _7l(11.0.0):C77113:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471558412, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se0 _7k(11.0.0):C79984:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471559541, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se1 _7q(11.0.0):C56634:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471560348, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se2 _7e(11.0.0):C100837:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471561742, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se3 _7i(11.0.0):C89761:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471562974, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se4 _81(11.0.0):C19348:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471563252, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se5 _7r(11.0.0):C53974:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471564015, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se6 _83(11.0.0):C13311:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471564202, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se7 _7o(11.0.0):C64703:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471565126, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se8 _7f(11.0.0):C97021:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471566465, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se9 _7t(11.0.0):C47426:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471567136, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sea _80(11.0.0):C24675:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471567500, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3seb _84(11.0.0):C9909:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471567636, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sec _7w(11.0.0):C40123:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471568203, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sed _82(11.0.0):C18105:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471568453, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3see _87(11.0.0):C2803:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471568504, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sef _7s(11.0.0):C52959:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471569278, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3seg _7b(11.0.0):C109635:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471570785, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3seh _7g(11.0.0):C90152:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471572034, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sei _7j(11.0.0):C80909:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471573145, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sej _7p(11.0.0):C64576:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471574043, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sek _7y(11.0.0):C32492:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471576085, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sem _7a(11.0.0):C114056:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471577618, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sen _86(11.0.0):C7431:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471577731, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3seo" [38 segments ; isCommit = false]
  re_now_checkpoint = re.compile(r'^IFD \d+ \[([^;]+); (.*?)\]: now checkpoint (.*?) \[(\d+) segments ; isCommit = (false|true)\]$')

  re_seg_size = re.compile(r'^MP \d+ \[([^;]+); (.*?)\]:\s+seg=_(.*?)\(.*? size=([0-9.]+) MB( \[floored])?$')

  re_new_merge_deletes = re.compile(r'^IW \d+ \[([^;]+); (.*?)\]: (\d+) new deletes since merge started')

  # e.g.: IW 21247 [2025-02-07T18:17:42.621678574Z; GCR-Writer-1-thread-6]: now run merges during commit: MergeSpec:
  re_merge_during_commit_start = re.compile(r'^IW \d+ \[([^;]+); (.*?)\]: now run merges during commit')

  # e.g.: IW 21550 [2025-02-07T18:18:27.651937537Z; GCR-Writer-1-thread-6]: done waiting for merges during commit
  re_merge_during_commit_end = re.compile(r'^IW \d+ \[([^;]+); (.*?)\]: done waiting for merges during commit')

  # e.g.: MS 2366 [2025-02-07T17:47:09.669633693Z; GCR-Writer-1-thread-11]:     launch new thread [Lucene Merge Thread #12]
  re_launch_new_merge_thread = re.compile(r'^MS \d+ \[([^;]+); (.*?)\]:\s+launch new thread \[(.*?)\]$')

  # e.g.: DW 2593 [2025-02-07T17:48:03.090578058Z; GCR-Writer-1-thread-4]: startFullFlush
  re_start_full_flush = re.compile(r'^DW \d+ \[([^;]+); (.*?)\]: startFullFlush$')

  # e.g.: DW 2891 [2025-02-07T17:48:10.332403301Z; GCR-Writer-1-thread-4]: GCR-Writer-1-thread-4 finishFullFlush success=true
  re_end_full_flush = re.compile(r'^DW \d+ \[([^;]+); (.*?)\]: .*?finishFullFlush success=true$')

  # e.g.: IW 153984 [2025-02-07T21:14:02.215553235Z; GCR-Writer-1-thread-8]: commit: start
  re_start_commit = re.compile(r'^IW \d+ \[([^;]+); (.*?)\]: commit: start$')

  # e.g.: IW 2575 [2025-02-07T17:47:54.694279252Z; GCR-Writer-1-thread-11]: startCommit index=_1b(9.11.2):C247182/1309:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os.version=4.14.355-275.582.amzn2.aarch64, java.vendor=Amazon.com Inc., os=Linux, mergeFactor=5, java.runtime.version=23.0.2+7, os.arch=aarch64, source=merge, lucene.version=9.11.2, mergeMaxNumSegments=-1, timestamp=1738950305614}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=589sfyynireleyyu9fa3c21e4 _1n(9.11.2):C124912/477:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os.version=4.14.355-275.582.amzn2.aarch64, java.vendor=Amazon.com Inc., os=Linux, mergeFactor=5, java.runtime.version=23.0.2+7, os.arch=aarch64, source=merge, lucene.version=9.11.2, mergeMaxNumSegments=-1, timestamp=1738950366061}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=589sfyynireleyyu9fa3c21e5 _23(9.11.2):C123523:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os.version=4.14.355-275.582.amzn2.aarch64, java.vendor=Amazon.com Inc., os=Linux, mergeFactor=5, java.runtime.version=23.0.2+7, os.arch=aarch64, source=merge, lucene.version=9.11.2, mergeMaxNumSegments=-1, timestamp=1738950429672}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=589sfyynireleyyu9fa3c21eb _24(9.11.2):C53964:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os.version=4.14.355-275.582.amzn2.aarch64, java.vendor=Amazon.com Inc., os=Linux, mergeFactor=5, java.runtime.version=23.0.2+7, os.arch=aarch64, source=merge, lucene.version=9.11.2, mergeMaxNumSegments=-1, timestamp=1738950429684}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=589sfyynireleyyu9fa3c21ed _25(9.11.2):C13577:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os.version=4.14.355-275.582.amzn2.aarch64, java.vendor=Amazon.com Inc., os=Linux, mergeFactor=3, java.runtime.version=23.0.2+7, os.arch=aarch64, source=merge, lucene.version=9.11.2, mergeMaxNumSegments=-1, timestamp=1738950429703}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=589sfyynireleyyu9fa3c21ef changeCount=168
  re_start_commit_index = re.compile(r'^IW \d+ \[([^;]+); (.*?)\]: startCommit index=(.*?) changeCount=(\d+)$')

  # e.g.: DWPT 219 [2025-02-07T17:43:00.953692447Z; GCR-Writer-1-thread-8]: flushedFiles=[_7.fdm, _7_BloomFilter_0.pos, _7.kdi, _7_BloomFilter_0.tim, _7_Lucene99HnswVectorsFormat_1.vec, _7.kdm, _7_BloomFilter_0.tmd, _7_Lucene99HnswVectorsFormat_1.vemf, _7_Lucene99_0.pay, _7.kdd, _7_Lucene99HnswVectorsFormat_2.vem, _7_BloomFilter_0.blm, _7_Lucene99HnswVectorsFormat_2.vec, _7_BloomFilter_0.pay, _7.fnm, _7_Lucene99HnswVectorsFormat_0.vemf, _7_Lucene99_0.tip, _7_BloomFilter_0.tip, _7_Lucene99_0.tim, _7.dvm, _7_Lucene99HnswVectorsFormat_0.vex, _7_Lucene99HnswVectorsFormat_1.vem, _7_Lucene99_0.pos, _7_BloomFilter_0.doc, _7.dvd, _7_Lucene99HnswVectorsFormat_0.vem, _7_Lucene99_0.tmd, _7_Lucene99HnswVectorsFormat_1.vex, _7_Lucene99HnswVectorsFormat_0.vec, _7.fdt, _7_Lucene99HnswVectorsFormat_2.vex, _7_Lucene99_0.doc, _7.fdx, _7_Lucene99HnswVectorsFormat_2.vemf]
  re_flushed_files = re.compile(r'^DWPT \d+ \[([^;]+); (.*?)\]: flushedFiles=\[(.*?)\]$')

  # e.g.: IW 512 [2025-02-07T17:43:24.119106602Z; GCR-Writer-1-thread-8]: done all syncs: [_m_Lucene99HnswVectorsFormat_1.vem, _l.fnm, _m.kdi, _k_Lucene99_0.tmd, _l_BloomFilter_0.pay, _k.dvd, _m.kdm, _m_Lucene99HnswVectorsFormat_1.vec, _k.dvm, _l_BloomFilter_0.tim, _l_BloomFilter_0.tip, _m.kdd, _m_Lucene99HnswVectorsFormat_1.vex, _k_Lucene99HnswVectorsFormat_2.vem, _m.dvd, _l_Lucene99_0.tip, _k_BloomFilter_0.tip, _k_BloomFilter_0.tim, _k.kdm, _m.dvm, _k_Lucene99HnswVectorsFormat_2.vec, _m_Lucene99_0.tim, _m_BloomFilter_0.blm, _l_Lucene99_0.pay, _k_BloomFilter_0.pay, _m_Lucene99_0.tip, _k.kdi, _m_Lucene99_0.pay, _m.si, _k_Lucene99HnswVectorsFormat_2.vex, _m_BloomFilter_0.pos, _l_Lucene99_0.tim, _l_Lucene99HnswVectorsFormat_2.vemf, _k.kdd, _l_Lucene99HnswVectorsFormat_2.vec, _m.fdt, _k_Lucene99HnswVectorsFormat_0.vem, _m.fdx, _l_Lucene99HnswVectorsFormat_2.vem, _l_Lucene99HnswVectorsFormat_1.vemf, _l_Lucene99HnswVectorsFormat_2.vex, _m_Lucene99HnswVectorsFormat_1.vemf, _k_Lucene99HnswVectorsFormat_0.vex, _m.fdm, _k_Lucene99_0.pay, _k.fdt, _l_BloomFilter_0.tmd, _l_Lucene99HnswVectorsFormat_0.vec, _k_BloomFilter_0.blm, _m_BloomFilter_0.pay, _l_Lucene99HnswVectorsFormat_0.vem, _k.fdx, _k_Lucene99HnswVectorsFormat_0.vec, _l_Lucene99HnswVectorsFormat_0.vex, _m_BloomFilter_0.tip, _m_BloomFilter_0.tim, _l_Lucene99_0.pos, _k.fdm, _k_Lucene99HnswVectorsFormat_2.vemf, _m_Lucene99HnswVectorsFormat_2.vex, _k_BloomFilter_0.doc, _m_BloomFilter_0.tmd, _l.kdi, _l.kdd, _k_Lucene99_0.tim, _k_Lucene99_0.tip, _m_Lucene99HnswVectorsFormat_2.vem, _m.fnm, _l.kdm, _k_BloomFilter_0.pos, _m_Lucene99HnswVectorsFormat_2.vec, _l.si, _l_Lucene99_0.doc, _m_Lucene99HnswVectorsFormat_0.vemf, _m_Lucene99_0.tmd, _k_Lucene99HnswVectorsFormat_0.vemf, _m_Lucene99HnswVectorsFormat_0.vex, _l_Lucene99_0.tmd, _k.fnm, _m_Lucene99HnswVectorsFormat_0.vem, _l_BloomFilter_0.doc, _l.dvd, _l.dvm, _m_Lucene99HnswVectorsFormat_0.vec, _k_Lucene99HnswVectorsFormat_1.vex, _m_Lucene99_0.doc, _l.fdm, _k_Lucene99_0.pos, _k_BloomFilter_0.tmd, _l.fdt, _l_BloomFilter_0.pos, _k_Lucene99HnswVectorsFormat_1.vemf, _k_Lucene99HnswVectorsFormat_1.vem, _m_BloomFilter_0.doc, _k_Lucene99HnswVectorsFormat_1.vec, _l.fdx, _l_BloomFilter_0.blm, _l_Lucene99HnswVectorsFormat_1.vex, _l_Lucene99HnswVectorsFormat_0.vemf, _m_Lucene99_0.pos, _k_Lucene99_0.doc, _k.si, _l_Lucene99HnswVectorsFormat_1.vec, _m_Lucene99HnswVectorsFormat_2.vemf, _l_Lucene99HnswVectorsFormat_1.vem]
  re_done_all_sync = re.compile(r'^IW \d+ \[([^;]+); (.*?)\]: done all syncs: \[(.*?)\]')

  # e.g.: IW 152529 [2025-02-07T21:10:59.329824625Z; Lucene Merge Thread #672]: merged segment size=1597.826 MB vs estimate=1684.011 MB
  re_merge_size_vs_estimate = re.compile(r'^IW \d+ \[([^;]+); (.*?)\]: merged segment size=([0-9.]+) MB vs estimate=([0-9.]+) MB$')

  # FP 1 [2025-02-12T03:12:32.803827868Z; GCR-Writer-1-thread-9]: trigger flush: activeBytes=4295836570 deleteBytes=56720 vs ramBufferMB=4096.0
  re_trigger_flush = re.compile(r'^FP \d+ \[([^;]+); (.*?)\]: trigger flush: activeBytes=(\d+) deleteBytes=(\d+) vs ramBufferMB=([0-9.]+)$')

  line_number = 0

  global_start_time = None
  global_end_time = None

  # the first time we see checkpoint we gather all pre-existing
  # segments and carefully initialize them:
  first_checkpoint = True

  # each entry is [start_time, end_time]
  merge_during_commit_events = []

  # counter (generation) of which merge-during-commit event we are in, or None if
  # we are not currently commit-merging
  merge_on_commit_thread_name = None
  merge_commit_threads = None

  commit_thread_name = None

  checkpoints = []
  commits = []
  trigger_flush_thread_name = None
  
  # slightly different from commit, e.g. nrtReader also does full flush by not fsync
  full_flush_events = []

  with open(infostream_log, 'r') as f:
    while True:
      line = f.readline()
      if line == '':
        break
      line = line.strip()
      # print(f'{line}')
      line_number += 1

      try:

        m = re_now_checkpoint.match(line)
        if m is not None:
          # print(f'parse checkpoint')
          # parse initial index segments (build on prior IW
          # sessions) -- we don't know lifetime of these segments,
          # so we just stack initially w/ canned lifetimes:
          s = m.group(3)
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          num_segments_expected = int(m.group(4))
          seg_details = parse_seg_details(m.group(3))

          if len(seg_details) != num_segments_expected:
            print(f'failed to parse the expected {num_segments_expected} number of sogments: found {len(seg_details)}: {seg_details}\n{line}')
            raise RuntimeError(f'failed to parse the expected {num_segments_expected} number of sogments: found {len(seg_details)} {seg_details}')
          
          if first_checkpoint:
            # seed the initial segments in the index
            for segment_name, source, is_cfs, max_doc, del_count, del_gen, diagnostics, attributes in seg_details:
              segment = Segment(segment_name, source, max_doc, None, timestamp - datetime.timedelta(seconds=30), None, line_number)
              by_segment_name[segment_name] = segment
            global_start_time = timestamp
            # print(f'add initial segment {segment_name} {max_doc=} {del_count=}')
          else:
            for segment_name, source, is_cfs, max_doc, del_count, del_gen, diagnostics, attributes in seg_details:
              segment = by_segment_name[segment_name]
              if del_count != segment.del_count:
                assert segment.del_count is None or del_count > segment.del_count
                # print(f'update {segment_name} del_count from {segment.del_count} to {del_count}')
                if segment.del_count is None:
                  del_inc = del_count
                else:
                  del_inc = del_count - segment.del_count
                segment.add_event(timestamp, f'del +{del_inc:,} gen {del_gen}', line_number)
                segment.del_count = del_count

          checkpoints.append((timestamp, thread_name) + tuple(seg_details))
          # print(f'{seg_checkpoint=}')

          first_checkpoint = False
          continue

        m = re_seg_size.match(line)
        if m is not None:
          segment_name = '_' + m.group(3)
          size_mb = float(m.group(4))
          segment = by_segment_name[segment_name]
          if segment.size_mb is None:
            print(f'now set initial segment size for {segment_name} to {size_mb:.3f}')
            segment.size_mb = size_mb
          if global_start_time is None:
            global_start_time = timestamp
          continue

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

          print(f'compare {thread_name=} to {commit_thread_name=} {segment_name=}')

          if commit_thread_name is not None:
            # logic may not be quite correct?  can we flush segment due to RAM usage even while another thread already kicked off commit?  unlikely...
            source = 'flush-commit'
          elif trigger_flush_thread_name == thread_name:
            source = 'flush-by-RAM'
          else:
            source = 'flush'
          print(f'  {source=}')
            
          segment = Segment(segment_name, source, max_doc, None, timestamp, None, line_number)

          if in_full_flush:
            segment.full_flush_ord = full_flush_count
          else:
            # print(f'WARNING: non-full-flush case not handled?  segment={segment_name}')
            pass
            
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
          segment.del_count = del_count
          continue

        m = re_new_flushed_size.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          segment_name = '_' + m.group(3)
          ram_used_mb = to_float(m.group(4))
          new_flushed_size_mb = to_float(m.group(5))
          docs_per_mb = to_float(m.group(6))

          segment = by_segment_name[segment_name]
          assert segment == by_thread_name[thread_name]
          segment.size_mb = new_flushed_size_mb
          segment.ram_used_mb = ram_used_mb
          continue

        m = re_flush_time.search(line)
        if m is not None:
          # this is the last per-segment message printed for the thread doing the flushing:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          segment = by_thread_name[thread_name]
          # TODO: this regexp is doing nothing?
          # segment.publish_timestamp = timestamp
          # no?
          # segment.end_time = timestamp
          del by_thread_name[thread_name]
          continue

        m = re_create_compound_file.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          segment = by_thread_name[thread_name]
          segment.add_event(timestamp, 'create compound file', line_number)
          segment.is_cfs = True
          continue
        
        m = re_publish_delgen.search(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          segment_name = '_' + m.group(3)
          segment.publish_timestamp = timestamp
          by_segment_name[segment_name].add_event(timestamp, 'light', line_number)
          if thread_name == trigger_flush_thread_name:
            trigger_flush_thread_name = None
          continue

        m = re_new_merge_deletes.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          del_count = int(m.group(3))
          if del_count > 0:
            by_thread_name[thread_name].born_del_count = del_count
            by_thread_name[thread_name].del_count = del_count
            by_thread_name[thread_name].add_event(timestamp, f'del {del_count:,} born', line_number)
            print(f'merged born del count {by_thread_name[thread_name].name} --> {by_thread_name[thread_name].born_del_count}')
          continue
        
        m = re_merge_start.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          segment_name = '_' + m.group(3)
          # parse the very verbose merging segments description
          # TODO: pull out other stuff, like compound or not, number of deletions, etc.
          merging_segments = re_segment_desc.findall(m.group(4))
          merging_segment_names = [f'_{x[0]}' for x in merging_segments]

          # how many deletes this merge reclaims:
          del_count_reclaimed = 0
          sum_max_doc = 0
          for tup in merging_segments:
            merging_seg_name = '_'+tup[0]
            max_doc = int(tup[2])
            sum_max_doc += max_doc
            del_count = tup[3]
            if len(del_count) > 0:
              del_count_inc = int(del_count[1:])
            else:
              del_count_inc = 0
            del_count_reclaimed += del_count_inc
            seg = by_segment_name[merging_seg_name]
            seg.del_count_merged_away = del_count_inc
          
          # print(f'{len(merging_segments)} merging segments in {m.group(4)}: {merging_segments}')

          segment = Segment(segment_name, ('merge', merging_segment_names), None, None, timestamp, None, line_number)
          segment.sum_max_doc = sum_max_doc
          segment.del_count_reclaimed = del_count_reclaimed

          if len(merge_during_commit_events) > 0 and merge_during_commit_events[-1][1] is None:
            # ensure this merge kickoff was really due to a merge-on-commit merge request:
            print(f'check {thread_name} vs {merge_commit_threads}')
            if thread_name in merge_commit_threads:
              print(f'MOC: YES {line_number}')
              segment.merge_during_commit = len(merge_during_commit_events)
            else:
              print(f'MOC: NO {line_number}')
              segment.merge_during_commit = None
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
            if max_doc != segment.sum_max_doc - segment.del_count_reclaimed:
              # likely IW is not logging the actual maxDoc/delCount for segments being merged?
              new_del_count_reclaimed = segment.sum_max_doc - max_doc
              print(f'WARNING: segment {segment.name} has wrong count sums?  {segment.max_doc=} {segment.sum_max_doc=} {segment.del_count_reclaimed=} {line_number=}; fix del_count_reclaimed from {segment.del_count_reclaimed} to {new_del_count_reclaimed}')
              segment.del_count_reclaimed = new_del_count_reclaimed
              # raise RuntimeError(f'segment {segment.name} has wrong count sums?  {segment.max_doc=} {segment.sum_max_doc=} {segment.del_count_reclaimed=} {line_number=}')
          elif max_doc != segment.max_doc:
            raise RuntimeError(f'merging segment {segment.name} sees different max_doc={segment.max_doc} vs {max_doc} {line_number=}')
          continue

        m = re_msec_to_build_docmaps.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          msec = float(m.group(3))
          segment = by_thread_name[thread_name]
          segment.add_event(timestamp, 'build merge sorted DocMaps', line_number)
          continue
        
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
          continue
          
        m = re_merge_commit.search(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          segment = by_thread_name[thread_name]
          # TODO: also parse/verify the "index=" part of this line?  that we know all the segments in the index now?
          segment.add_event(timestamp, 'light', line_number)
          segment.publish_timestamp = timestamp
          if segment.merge_during_commit is not None:
            if segment.merge_during_commit == len(merge_during_commit_events) and merge_during_commit_events[-1][1] is None:
              # merge commit finished in time!
              segment.merge_during_commit = ('success', (timestamp - merge_during_commit_events[-1][0]).total_seconds())
            else:
              segment.merge_during_commit = ('timeout', (timestamp - merge_during_commit_events[segment.merge_during_commit-1][0]).total_seconds())

          for segment_name in segment.source[1]:
            old_segment = by_segment_name.get(segment_name)
            if old_segment is not None:
              old_segment.end_time = timestamp
              old_segment.merged_into = segment
              old_segment.merge_event(segment.start_time, f'start merge into {segment.name}', line_number)
              old_segment.add_event(timestamp, f'merged into {segment.name}', line_number)
            else:
              raise RuntimeError(f'cannot find segment {segment_name} for merge?')
          continue

        m = re_merge_during_commit_start.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          print(f'MOC: start {thread_name} {line_number}')
          merge_during_commit_events.append([timestamp, None])
          merge_on_commit_thread_name = thread_name
          merge_commit_threads = set()
          continue
        
        m = re_launch_new_merge_thread.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          new_merge_thread_name = m.group(3)
          # print(f'got launch new thread {thread_name} and {new_merge_thread_name}')
          if thread_name == merge_on_commit_thread_name and (len(merge_during_commit_events) > 0 and merge_during_commit_events[-1][1] is None):
            # so we know, for certain, that the merged kicked off during a merge-on-commit window
            # really came from the merge-on-commit thread
            merge_commit_threads.add(new_merge_thread_name)
          continue

        m = re_merge_during_commit_end.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          print(f'MOC: end {thread_name} {line_number}')

          merge_on_commit_thread_name = None
          merge_commit_threads = None
          
          merge_during_commit_events[-1][1] = timestamp
          continue

        m = re_start_full_flush.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          full_flush_events.append([timestamp, None])
          in_full_flush = True
          full_flush_count += 1
          full_flush_start_time = timestamp
          print(f'start full flush {line_number}')
          continue

        m = re_end_full_flush.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          print(f'clear commit thread')
          commit_thread_name = None
          assert full_flush_events[-1][1] is None
          full_flush_events[-1][1] = timestamp
          in_full_flush = False
          continue
          
        m = re_start_commit.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          commit_thread_name = thread_name
          print(f'set commit thread {commit_thread_name}')
          continue
          
        m = re_start_commit_index.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          seg_details = parse_seg_details(m.group(3))
          commits.append((timestamp, thread_name) + tuple(seg_details))
          print(f'startCommit: {seg_details}')
          continue

        m = re_flushed_files.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          s = m.group(3).split(', ')
          by_thread_name[thread_name].file_count = len(s)
          continue
          
        m = re_done_all_sync.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          s = m.group(3).split(', ')
          # insert number of files in the commit:
          commits[-1] = commits[-1][:2] + (len(s),) + commits[-1][2:]
          continue

        m = re_merge_size_vs_estimate.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          actual_mb = float(m.group(3))
          estimate_mb = float(m.group(4))
          # print(f'actual mb: {by_thread_name[thread_name].size_mb} vs {actual_mb}')
          by_thread_name[thread_name].estimate_size_mb = estimate_mb
          by_thread_name[thread_name].size_mb = actual_mb
          continue

        m = re_trigger_flush.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          activeBytes = int(m.group(3))
          deleteBytes = int(m.group(3))
          ramBufferMB = float(m.group(3))
          trigger_flush_thread_name = thread_name
          
      except KeyboardInterrupt:
        raise
      except:
        print(f'unhandled exception on line {line_number} of {infostream_log}: {line}')
        raise

  for segment in Segment.all_segments:
    print(f'\n{segment.name}:\n{segment.to_verbose_string(global_start_time, global_end_time, True)}')
    if segment.end_time is None:
      print(f'set end_time for segment {segment.name} to global end time')
      segment.end_time = global_end_time

  open(segments_out, 'wb').write(pickle.dumps((global_start_time, global_end_time, Segment.all_segments, full_flush_events, merge_during_commit_events, checkpoints, commits)))
  
  print(f'wrote {len(Segment.all_segments)} segments to "{segments_out}": {os.path.getsize(segments_out)/1024/1024:.1f} MB')

if __name__ == '__main__':
  main()
