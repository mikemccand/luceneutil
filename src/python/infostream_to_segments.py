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

import bisect
import datetime
import os
import pickle
import re
import sys
from typing import Optional

# thank you Claude:
def parse_timestamp(timestamp_str: str) -> Optional[datetime.datetime]:
    """
    Parse ISO 8601 timestamp with cross-Python version compatibility.
    
    Fixes the issue where datetime.datetime.fromisoformat() fails on Python < 3.11 with:
    - Nanosecond precision timestamps (e.g., '.744930252')
    - 'Z' timezone designator 
    
    Args:
        timestamp_str: ISO 8601 timestamp string
                      (e.g., '2025-06-22T17:29:31.744930252Z')
    
    Returns:
        datetime object, or exception if parsing fails
        
    Example usage:
        # Replace this:
        # dt = datetime.datetime.fromisoformat(timestamp_str)
        
        # With this:
        dt = parse_timestamp_compatible(timestamp_str)
        if dt is None:
            # Handle parsing error
            continue
    """

    # For Python 3.11+, try native fromisoformat first
    if sys.version_info >= (3, 11):
        try:
            return datetime.datetime.fromisoformat(timestamp_str)
        except ValueError:
            # Fall through to compatibility handling
            pass

    # Compatibility processing for older Python versions
    processed = timestamp_str.strip()

    # Step 1: Handle 'Z' timezone (replace with '+00:00')
    if processed.endswith('Z'):
        processed = processed[:-1] + '+00:00'

    # Step 2: Handle nanosecond precision
    if '.' in processed:
        # Find the fractional seconds part
        if '+' in processed:
            main_part, tz_part = processed.rsplit('+', 1)
            tz_part = '+' + tz_part
        elif processed.count('-') >= 3:  # Date has 2 dashes, timezone has 1
            main_part, tz_part = processed.rsplit('-', 1)
            tz_part = '-' + tz_part
        else:
            main_part = processed
            tz_part = ''

        # Split main part to get fractional seconds
        if '.' in main_part:
            datetime_part, fractional_part = main_part.split('.', 1)

            # Truncate nanoseconds to microseconds (max 6 digits)
            if len(fractional_part) > 6:
                fractional_part = fractional_part[:6]

            # Reconstruct timestamp
            processed = f"{datetime_part}.{fractional_part}{tz_part}"

    # Parse the processed timestamp
    return datetime.datetime.fromisoformat(processed)

# TODO
#   - CMS logs helpful progress indicators for each merge thread -- can we parse this and render somehow?:
#      merge thread Lucene Merge Thread #154 estSize=4674.5 MB (written=3931.3 MB) runTime=17016.6s (stopped=0.0s, paused=0.0s) rate=unlimited
#        leave running at Infinity MB/sec
#      merge thread Lucene Merge Thread #401 estSize=3353.0 MB (written=738.2 MB) runTime=3437.3s (stopped=0.0s, paused=0.0s) rate=unlimited
#        leave running at Infinity MB/sec
#      merge thread Lucene Merge Thread #463 estSize=3128.8 MB (written=726.3 MB) runTime=145.2s (stopped=0.0s, paused=0.0s) rate=unlimited
#        leave running at Infinity MB/sec"
#   - also parse "skip apply merge during commit" message to validate timeout of MOC merge
#   - why do del counts sometimes go backwards?  "now checkpoint" is not guaranteed to be monotonic?
#   - we could validate MOC parsing by checking segments actually committed (IW logs this) vs our re-parsing / re-construction?
#   - support addIndexes source too
#   - also parse commitMergeDeletes line
#   - fix IW/infostream/tracing
#     - note why a flush was triggered -- NRT reader, commit, RAM full, flush-by-doc-count, etc.
#   - open issue for this WTF:
"""
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
"""

# e.g. typically many on one line like this: _f(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951546, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvw
#   or (with static index sort): IFD 211 [2025-02-06T18:26:20.518261400Z; GCR-Writer-1-thread-6]: now checkpoint "_a(9.11.2):C14171/34:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={timestamp=1738866380509, java.runtime.version=23.0.2+7, java.vendor=Amazon.com Inc., os=Linux, os.arch=aarch64, os.version=4.14.355-275.582.amzn2.aarch64, lucene.version=9.11.2, source=flush}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=dz4cethjje165l9ysayg0m9f1" [1 segments ; isCommit = false]
re_segment_desc = re.compile(r"_(.*?)\(.*?\):([cC])(\d+)(/\d+)?:\[(indexSort=.*?)?(diagnostics=\{.*?\})\]:\[attributes=(\{.*?\})\](:delGen=\d+)? :id=[0-9a-z]+?")


def sec_to_time_delta(td_sec):
  hr = int(td_sec / 3600.0)
  mn = int((td_sec - hr * 3600) / 60.0)
  sc = td_sec - hr * 3600 - mn * 60
  return f"{hr:02}:{mn:02}:{sc:04.1f}"


class Segment:
  # the rate at which deletes are reclaimed (merged away) -- this is only set for
  # merged segments:
  del_reclaims_per_sec = None

  # the rate at which the application is deleting documents in this segment:
  del_creates_per_sec = None
  del_create_count = None

  # net average write amplification from entire geneology of this
  # segment.  flushed segments start at 1 since that is a 1X write
  # amplification.  merged segments take a weighted average of their
  # incoming (merged) segments, pro-rated by % deletes of each.
  net_write_amplification = None

  max_ancestor_depth = None

  # true if this merge was selected by IW.forceMergeDeletes
  is_force_merge_deletes = False

  all_segments = []

  # how many docs are deleted when this segment is first lit.  for a
  # flushed segment, these are deletions of newly indexed documents in
  # the segment, before the segment was flushed.  for a merged
  # segment, these are deletions of documents in the merging segments
  # that arrived after the merge started and before it
  # finished/committed/was lit:
  born_del_count = None

  # if this segment was produced by merge, the number of deleted docs that
  # merge compacted away in total from the merging segments.
  del_count_reclaimed = None

  # how many of my own deletes were collapsed when I got merged
  del_count_merged_away = None

  # current (while parsing) and final (at end of parsing) deleted doc count
  del_count = None

  # when this segment was lit (enrolled into in-memory segment infos)
  publish_timestamp = None

  # which segment this segment was merged into:
  merged_into = None

  # None if this merge was not a merge-on-commit.  if it is, then this is a tuple ('success' | 'timeout', timedelta-in-seconds)
  merge_during_commit = None
  merge_during_commit_ord = None

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
    Segment.all_segments.append(self)
    self.source = source
    self.name = name
    assert (type(source) is str and source in ("flush", "flush-commit", "flush-by-RAM")) or (type(source) is tuple and source[0] == "merge" and len(source) > 1)
    self.max_doc = max_doc
    self.size_mb = size_mb
    # this is when segment first started being written -- birth!
    self.start_time = start_time
    self.start_infostream_line_number = start_infostream_line_number
    self.end_time = end_time
    # e.g. 'write_doc_values', 'write_knn', 'merging_started', 'add_deletes'...
    self.events = []

  def add_event(self, timestamp, event, infostream_line_number):
    if len(self.events) > 0 and timestamp < self.events[-1][0]:
      raise RuntimeError(f"events must append forwards in time: segment {self.name} has {self.events=} but new {event=} moves backwards?")
    self.events.append((timestamp, event, infostream_line_number))

  def merge_event(self, timestamp, event, infostream_line_number):
    """Like add_event, except the timestamp might be out of order, so we do insertion sort."""
    insert_at = bisect.bisect_right(self.events, timestamp, key=lambda x: x[0])
    self.events.insert(insert_at, (timestamp, event, infostream_line_number))

  def to_verbose_string(self, global_start_time, global_end_time, with_line_numbers, with_all_deletes=False):
    l = []
    s = f"segment {self.name}"
    if self.source in ("flush", "flush-commit"):
      s += f" ({self.source})"
    elif self.source == "flush-by-RAM":
      s += f" ({self.source} @ {self.ram_buffer_mb:.1f} MB buffer)"
    elif self.merge_during_commit is not None:
      s += " (merge-on-commit: " + " ".join(self.source[1]) + ")"
    elif self.is_force_merge_deletes:
      s += " (merge-force-deletes: " + " ".join(self.source[1]) + ")"
    else:
      s += " (merge: " + " ".join(self.source[1]) + ")"
    l.append(s)
    md = f"  {self.max_doc:,} max_doc"
    if self.size_mb is not None:
      md += f" ({self.max_doc / self.size_mb:,.1f} docs/MB)"
    l.append(md)
    l.append(f"  write-ampl: {self.net_write_amplification:.1f} X (ancestors: {self.max_ancestor_depth})")
    if type(self.source) is str and self.source.startswith("flush"):
      if self.ram_used_mb is None:
        # this can happen if InfoStream ends as a segment is flushing
        # raise RuntimeError(f'flushed segment {self.name} is missing ramUsedMB')
        if self.size_mb is None:
          l.append("  ?? MB (?? efficiency)")
        else:
          l.append(f"  {self.size_mb:,.1f} MB (?? efficiency)")
      else:
        l.append(f"  {self.size_mb:,.1f} MB ({100 * self.size_mb / self.ram_used_mb:.1f}% efficiency, ram_used_mb={self.ram_used_mb:,.1f} MB)")
    elif self.size_mb is None:
      l.append("  ?? MB")
    elif self.estimate_size_mb is not None:
      pct = 100.0 * self.size_mb / self.estimate_size_mb
      l.append(f"  {self.size_mb:,.1f} MB vs estimate {self.estimate_size_mb:,.1f} MB {pct:.1f}%")
    else:
      l.append(f"  {self.size_mb:,.1f} MB vs estimate ?? MB")

    if self.merge_during_commit is not None:
      if type(self.merge_during_commit) is tuple:
        state, seconds = self.merge_during_commit
        l.append(f"  merge-during-commit: {state} {seconds:.2f} sec")
      else:
        # could be a single int, if merge was still running at end of InfoStream
        pass
    if self.is_cfs:
      l.append("  cfs")

    if self.del_creates_per_sec is not None:
      l.append(f"  dels creates {self.del_create_count:,} ({self.del_creates_per_sec:.1f} dels/sec) ({100.0 * self.del_create_count / self.max_doc:.1f}%)")
    if self.del_reclaims_per_sec is not None:
      l.append(f"  dels reclaimed {self.del_count_reclaimed:,} ({self.del_reclaims_per_sec:.1f} dels/sec) ({100.0 * self.del_count_reclaimed / (self.max_doc + self.del_count_reclaimed):.1f}%)")
    if self.born_del_count is not None:
      l.append(f"  born dels {self.born_del_count:,} ({100.0 * self.born_del_count / self.max_doc:.1f}%)")
    if self.del_count is not None:
      if self.end_time is None:
        end_time = global_end_time
      else:
        end_time = self.end_time
      dps = self.del_count / (end_time - self.start_time).total_seconds()
      l.append(f"  final dels {self.del_count:,} ({100.0 * self.del_count / self.max_doc:.1f}%, {dps:,.1f} dels/s)")
    # l.append(f'  created at {(self.start_time - global_start_time).total_seconds():,.1f} sec')
    # if self.end_time is not None:
    #  l.append(f'  end_time: {self.end_time}')
    if self.publish_timestamp is not None:
      btt = (self.publish_timestamp - self.start_time).total_seconds()
      bt = sec_to_time_delta(btt)
    else:
      btt = 0
      bt = "??"
    if self.end_time is not None:
      if self.merged_into is not None:
        dtt = (self.end_time - self.merged_into.start_time).total_seconds()
        dt = sec_to_time_delta(dtt)
      else:
        dt = "??"
        dtt = 0
      ltt = (self.end_time - self.start_time).total_seconds()
    else:
      dt = "0.0"
      dtt = 0
      ltt = (global_end_time - self.start_time).total_seconds()

    # how long the segment was alive just for searching, divided by its total (including dawn + dusk)
    # lifetime.  if a segment, immediately on being written, is selected for merging, it has day_pct 0.
    # this can never be 100% since it must take time to write and merge a segment...
    day_pct = 100.0 * (ltt - btt - dtt) / ltt

    l.append(f"  life-efficiency {day_pct:.1f}%")
    l.append(f"  times {bt} / {sec_to_time_delta(ltt - btt - dtt)} / {dt}")
    if self.merged_into is not None:
      l.append(f"  merged into {self.merged_into.name}")
    l.append("  events")
    last_ts = self.start_time

    # we subsample all delete flushes if there are too many...:
    del_count_count = len([x for x in self.events if x[1].startswith("del ")])

    if del_count_count > 0:
      print_per_del = 9.0 / del_count_count
    else:
      print_per_del = None

    last_del_print = -1
    del_inc = 0

    # if we have too many del_count events, subsample which ones to
    # print: always print first and last, but subsample in between to
    # get to ~9 ish
    do_del_print = {}
    last_del_index = None
    last_del_print = None
    last_del_print_index = None
    for i, (ts, event, line_number) in enumerate(self.events):
      if event.startswith("del "):
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
      is_del = event.startswith("del ")
      if is_del:
        # print(f'got {event}')
        del_inc = int(event[4 : event.find(" ", 4)].replace(",", ""))
        sum_del_count += del_inc
        event += f" ({100.0 * sum_del_count / self.max_doc:.1f}%)"
      s = f"+{(ts - last_ts).total_seconds():.2f}"

      if not is_del or i in do_del_print or with_all_deletes:
        if with_line_numbers:
          s = f"    {s:>7s} s: {event} [line {line_number:,}]"
        else:
          s = f"    {s:>7s} s: {event}"
        if not with_all_deletes and (is_del and do_del_print.get(i)):
          s += "..."
        l.append(s)
      last_ts = ts

    return "\n".join(l)


def to_float(s):
  """Converts float number strings that might have comma, e.g. '1,464.435', to float."""
  return float(s.replace(",", ""))


"""
Parses a full IndexWriter InfoStream to per-segment details for rendering with segments_to_html.py
"""


def parse_seg_details(s):
  seg_details = []
  for tup in re_segment_desc.findall(s):
    # TODO: we could parse timestamp from diagnostics...
    segment_name = "_" + tup[0]
    cfs_letter = tup[1]
    if cfs_letter == "C":
      is_cfs = False
    elif cfs_letter == "c":
      is_cfs = True
    else:
      raise RuntimeError(f"expected c or C for CFS boolean but got {cfs_letter}")
    max_doc = int(tup[2])
    if tup[3] != "":
      del_count = int(tup[3][1:])
    else:
      del_count = 0

    diagnostics = tup[5]
    if "source=merge" in diagnostics:
      source = ("merge", [])
    elif "source=flush" in diagnostics:
      source = "flush"
    else:
      raise RuntimeError(f"seg {segment_name}: a new source in {diagnostics}?")

    attributes = tup[6]
    del_gen = tup[7]
    if del_gen != "":
      del_gen = int(del_gen[8:])
    else:
      del_gen = None

    seg_details.append((segment_name, source, is_cfs, max_doc, del_count, del_gen, diagnostics, attributes))

  return seg_details


def main():
  if len(sys.argv) != 3:
    raise RuntimeError("Usage: python3 -u infostream_to_segments.py <path-to-infostream-log> <path-to-segments-file-out>")

  infostream_log = sys.argv[1]
  segments_out = sys.argv[2]
  if os.path.exists(segments_out):
    raise RuntimeError(f"please first remove output file {segments_out}")

  by_segment_name = {}
  by_thread_name = {}

  full_flush_count = -1

  # e.g. MS 0 [2025-01-31T12:35:34.054288816Z; main]: initDynamicDefaults maxThreadCount=16 maxMergeCount=21
  re_timestamp_thread = re.compile(r"^.*? \[([^;]+); (.*?)\]: ")

  # e.g.: DWPT 0 [2025-01-31T12:35:49.296482953Z; Index #1]: flush postings as segment _m numDocs=49800
  re_flush_start = re.compile(r"^DWPT \d+ \[([^;]+); (.*?)\]: flush postings as segment _(.*?) numDocs=(\d+)$")

  # e.g.: IW 0 [2025-01-31T12:35:49.642983664Z; Index #10]: 330 ms to write points
  re_msec_to_write = re.compile(r"^IW \d+ \[([^;]+); (.*?)\]: (\d+) ms to write (.*?)$")

  # e.g.: DWPT 0 [2025-01-31T12:35:51.198819989Z; Index #34]: new segment has 0 deleted docs
  re_new_segment_deleted_docs = re.compile(r"^DWPT \d+ \[([^;]+); (.*?)\]: new segment has (\d+) deleted docs$")

  # e.g.: DWPT 0 [2025-01-31T12:35:51.200492790Z; Index #34]: flushed: segment=_0 ramUsed=74.271 MB newFlushedSize=33.995 MB docs/MB=1,464.435
  re_new_flushed_size = re.compile(r"^DWPT \d+ \[([^;]+); (.*?)\]: flushed: segment=_(.*?) ramUsed=(.*) MB newFlushedSize=(.*?) MB docs/MB=(.*?)$")

  # e.g.: IW 0 [2025-01-31T12:36:04.992415479Z; Index #2]: publish sets newSegment delGen=38 seg=_11(11.0.0):C49789:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326964992, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpprjm
  re_publish_delgen = re.compile(r"^IW \d+ \[([^;]+); (.*?)\]: publish sets newSegment delGen=\d+ seg=_([^\(]+)")

  # e.g.: DWPT 0 [2025-01-31T12:35:51.801527396Z; Index #5]: flush time 3097.274264 ms
  re_flush_time = re.compile(r"^DWPT \d+ \[([^;]+); (.*?)\]: flush time (.*?) ms$")

  # e.g.: IW 0 [2025-02-02T20:48:42.699743134Z; ReopenThread]: create compound file
  re_create_compound_file = re.compile(r"^IW \d+ \[([^;]+); (.*?)\]: create compound file$")

  # maybe?: DWPT 0 [2025-01-31T12:35:51.200730335Z; Index #34]: flush time 2588.276633 ms

  # e.g.: IW 0 [2025-01-31T12:35:51.817783927Z; Lucene Merge Thread #0]: merge seg=_1y _f(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951546, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvw _l(11.0.0):C49798:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951756, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvy _u(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951689, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwj _w(11.0.0):C49797:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951801, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvq _a(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951625, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw3 _r(11.0.0):C49783:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951688, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvs _e(11.0.0):C49779:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951736, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvr _p(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951761, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwa _5(11.0.0):C49778:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951706, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwq _h(11.0.0):C49793:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951625, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw7
  re_merge_start = re.compile(r"^IW \d+ \[([^;]+); (.*?)\]: merge seg=_(.*?) (.*?)$")

  # e.g.: SM 0 [2025-01-31T12:35:51.981685491Z; Lucene Merge Thread #2]: 13 ms to merge stored fields [497911 docs]
  re_msec_to_merge = re.compile(r"^SM \d+ \[([^;]+); (.*?)\]: (\d+) ms to merge (.*?) \[(\d+) docs\]$")
  re_msec_to_build_docmaps = re.compile(r"^SM \d+ \[([^;]+); (.*?)\]: ([\d.]+) msec to build merge sorted DocMaps$")

  # e.g.: IW 0 [2025-01-31T12:35:58.899009723Z; Lucene Merge Thread #0]: merge codec=Lucene103 maxDoc=497858; merged segment has no vectors; norms; docValues; prox; freqs; points; 7.1 sec to merge segment [303.82 MB, 42.94 MB/sec]
  re_merge_finish = re.compile(r"^IW \d+ \[([^;]+); (.*?)\]: merge codec=(.*?) maxDoc=(\d+);.*? \[([0-9.]+) MB, ([0-9.]+) MB/sec\]$")

  # e.g.: IW 0 [2025-01-31T12:35:58.899418368Z; Lucene Merge Thread #0]: commitMerge: _f(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951546, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvw _l(11.0.0):C49798:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951756, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvy _u(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951689, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwj _w(11.0.0):C49797:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951801, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvq _a(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951625, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw3 _r(11.0.0):C49783:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951688, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvs _e(11.0.0):C49779:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951736, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvr _p(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951761, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwa _5(11.0.0):C49778:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951706, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwq _h(11.0.0):C49793:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951625, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw7 index=_0(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951200, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppst _w(11.0.0):C49797:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951801, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvq _e(11.0.0):C49779:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951736, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvr _r(11.0.0):C49783:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951688, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvs _y(11.0.0):C49795:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951485, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvt _g(11.0.0):C49795:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951687, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvu _v(11.0.0):C49790:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951685, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvv _f(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951546, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvw _1(11.0.0):C49789:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951627, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvx _l(11.0.0):C49798:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951756, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvy _6(11.0.0):C49778:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951640, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppvz _n(11.0.0):C49793:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951685, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw0 _7(11.0.0):C49800:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951687, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw1 _d(11.0.0):C49776:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951729, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw2 _a(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951625, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw3 _t(11.0.0):C49780:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951790, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw4 _s(11.0.0):C49786:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951536, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw5 _9(11.0.0):C49775:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951756, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw6 _h(11.0.0):C49793:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951625, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw7 _8(11.0.0):C49791:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951619, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw8 _2(11.0.0):C49792:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951688, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppw9 _p(11.0.0):C49784:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951761, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwa _4(11.0.0):C49796:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951687, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwb _b(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951709, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwd _3(11.0.0):C49780:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951702, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwf _o(11.0.0):C49799:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951688, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwg _j(11.0.0):C49786:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951636, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwi _u(11.0.0):C49781:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951689, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwj _x(11.0.0):C49785:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951629, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwk _z(11.0.0):C49779:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951568, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwl _k(11.0.0):C49785:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951685, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwm _c(11.0.0):C49801:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951558, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwn _q(11.0.0):C49795:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951690, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwo _i(11.0.0):C49798:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951641, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwp _5(11.0.0):C49778:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951706, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppwq _m(11.0.0):C49800:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738326951966, java.runtime.version=23, java.vendor=Arch Linux, os=Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=o6ynrzazp1d8kt6wycnpppx2
  re_merge_commit = re.compile(r"^IW \d+ \[([^;]+); (.*?)\]: commitMerge: ")

  # e.g. IFD 0 [2025-02-02T20:48:41.430973144Z; main]: now checkpoint "_89(11.0.0):C10061266:[diagnostics={timestamp=1738471577758, os.version=6.12.4-arch1-1, os=Linux, java.vendor=Arch Linux, mergeFactor=10, java.runtime.version=23, os.arch=amd64, source=merge, lucene.version=11.0.0, mergeMaxNumSegments=-1}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3ses _1c(11.0.0):C621189:[diagnostics={timestamp=1738471469509, os.version=6.12.4-arch1-1, os=Linux, java.vendor=Arch Linux, mergeFactor=10, java.runtime.version=23, os.arch=amd64, source=merge, lucene.version=11.0.0, mergeMaxNumSegments=-1}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3run _1o(11.0.0):C858490:[diagnostics={timestamp=1738471473331, os.version=6.12.4-arch1-1, os=Linux, java.vendor=Arch Linux, mergeFactor=10, java.runtime.version=23, os.arch=amd64, source=merge, lucene.version=11.0.0, mergeMaxNumSegments=-1}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3rw2 _6s(11.0.0):C9775889:[diagnostics={timestamp=1738471534474, os.version=6.12.4-arch1-1, os=Linux, java.vendor=Arch Linux, mergeFactor=10, java.runtime.version=23, os.arch=amd64, source=merge, lucene.version=11.0.0, mergeMaxNumSegments=-1}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3ser _24(11.0.0):C1173700:[diagnostics={timestamp=1738471479224, os.version=6.12.4-arch1-1, os=Linux, java.vendor=Arch Linux, mergeFactor=10, java.runtime.version=23, os.arch=amd64, source=merge, lucene.version=11.0.0, mergeMaxNumSegments=-1}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3ryj _3n(11.0.0):C1116447:[diagnostics={timestamp=1738471497653, os.version=6.12.4-arch1-1, os=Linux, java.vendor=Arch Linux, mergeFactor=10, java.runtime.version=23, os.arch=amd64, source=merge, lucene.version=11.0.0, mergeMaxNumSegments=-1}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3s2x _4i(11.0.0):C1133465:[diagnostics={timestamp=1738471507644, os.version=6.12.4-arch1-1, os=Linux, java.vendor=Arch Linux, mergeFactor=10, java.runtime.version=23, os.arch=amd64, source=merge, lucene.version=11.0.0, mergeMaxNumSegments=-1}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3s5c _7v(11.0.0):C1164338:[diagnostics={timestamp=1738471546722, os.version=6.12.4-arch1-1, os=Linux, java.vendor=Arch Linux, mergeFactor=10, java.runtime.version=23, os.arch=amd64, source=merge, lucene.version=11.0.0, mergeMaxNumSegments=-1}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sd3 _7d(11.0.0):C108162:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471553445, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sdu _7x(11.0.0):C34764:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471553979, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sdv _7n(11.0.0):C74924:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471555045, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sdw _7h(11.0.0):C80743:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471556260, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sdx _7u(11.0.0):C44341:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471556917, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sdy _7z(11.0.0):C29383:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471557330, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sdz _7l(11.0.0):C77113:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471558412, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se0 _7k(11.0.0):C79984:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471559541, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se1 _7q(11.0.0):C56634:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471560348, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se2 _7e(11.0.0):C100837:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471561742, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se3 _7i(11.0.0):C89761:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471562974, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se4 _81(11.0.0):C19348:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471563252, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se5 _7r(11.0.0):C53974:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471564015, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se6 _83(11.0.0):C13311:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471564202, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se7 _7o(11.0.0):C64703:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471565126, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se8 _7f(11.0.0):C97021:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471566465, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3se9 _7t(11.0.0):C47426:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471567136, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sea _80(11.0.0):C24675:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471567500, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3seb _84(11.0.0):C9909:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471567636, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sec _7w(11.0.0):C40123:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471568203, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sed _82(11.0.0):C18105:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471568453, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3see _87(11.0.0):C2803:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471568504, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sef _7s(11.0.0):C52959:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471569278, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3seg _7b(11.0.0):C109635:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471570785, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3seh _7g(11.0.0):C90152:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471572034, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sei _7j(11.0.0):C80909:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471573145, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sej _7p(11.0.0):C64576:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471574043, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sek _7y(11.0.0):C32492:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471576085, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sem _7a(11.0.0):C114056:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471577618, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3sen _86(11.0.0):C7431:[diagnostics={os.arch=amd64, os.version=6.12.4-arch1-1, lucene.version=11.0.0, source=flush, timestamp=1738471577731, java.runtime.version=23, os=Linux, java.vendor=Arch Linux}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=9xrhn5oqlwx65m3bmjtje3seo" [38 segments ; isCommit = false]
  re_now_checkpoint = re.compile(r"^IFD \d+ \[([^;]+); (.*?)\]: now checkpoint (.*?) \[(\d+) segments ; isCommit = (false|true)\]$")

  re_seg_size = re.compile(r"^MP \d+ \[([^;]+); (.*?)\]:\s+seg=_(.*?)\(.*? size=([0-9.]+) MB( \[floored])?$")

  re_new_merge_deletes = re.compile(r"^IW \d+ \[([^;]+); (.*?)\]: (\d+) new deletes since merge started")

  # e.g.: IW 21247 [2025-02-07T18:17:42.621678574Z; GCR-Writer-1-thread-6]: now run merges during commit: MergeSpec:
  #  followed by successive lines "1: ...", "2: ...", etc.
  re_merge_during_commit_start = re.compile(r"^IW \d+ \[([^;]+); (.*?)\]: now run merges during commit: MergeSpec:$")

  # e.g.: IW 21550 [2025-02-07T18:18:27.651937537Z; GCR-Writer-1-thread-6]: done waiting for merges during commit
  re_merge_during_commit_end = re.compile(r"^IW \d+ \[([^;]+); (.*?)\]: done waiting for merges during commit")

  # e.g.: MS 2366 [2025-02-07T17:47:09.669633693Z; GCR-Writer-1-thread-11]:     launch new thread [Lucene Merge Thread #12]
  re_launch_new_merge_thread = re.compile(r"^MS \d+ \[([^;]+); (.*?)\]:\s+launch new thread \[(.*?)\]$")

  # e.g.: DW 2593 [2025-02-07T17:48:03.090578058Z; GCR-Writer-1-thread-4]: startFullFlush
  re_start_full_flush = re.compile(r"^DW \d+ \[([^;]+); (.*?)\]: startFullFlush$")

  # e.g.: DW 2891 [2025-02-07T17:48:10.332403301Z; GCR-Writer-1-thread-4]: GCR-Writer-1-thread-4 finishFullFlush success=true
  re_end_full_flush = re.compile(r"^DW \d+ \[([^;]+); (.*?)\]: .*?finishFullFlush success=true$")

  # e.g.: IW 153984 [2025-02-07T21:14:02.215553235Z; GCR-Writer-1-thread-8]: commit: start
  re_start_commit = re.compile(r"^IW \d+ \[([^;]+); (.*?)\]: commit: start$")

  # e.g.: IW 2575 [2025-02-07T17:47:54.694279252Z; GCR-Writer-1-thread-11]: startCommit index=_1b(9.11.2):C247182/1309:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os.version=4.14.355-275.582.amzn2.aarch64, java.vendor=Amazon.com Inc., os=Linux, mergeFactor=5, java.runtime.version=23.0.2+7, os.arch=aarch64, source=merge, lucene.version=9.11.2, mergeMaxNumSegments=-1, timestamp=1738950305614}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=589sfyynireleyyu9fa3c21e4 _1n(9.11.2):C124912/477:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os.version=4.14.355-275.582.amzn2.aarch64, java.vendor=Amazon.com Inc., os=Linux, mergeFactor=5, java.runtime.version=23.0.2+7, os.arch=aarch64, source=merge, lucene.version=9.11.2, mergeMaxNumSegments=-1, timestamp=1738950366061}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=589sfyynireleyyu9fa3c21e5 _23(9.11.2):C123523:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os.version=4.14.355-275.582.amzn2.aarch64, java.vendor=Amazon.com Inc., os=Linux, mergeFactor=5, java.runtime.version=23.0.2+7, os.arch=aarch64, source=merge, lucene.version=9.11.2, mergeMaxNumSegments=-1, timestamp=1738950429672}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=589sfyynireleyyu9fa3c21eb _24(9.11.2):C53964:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os.version=4.14.355-275.582.amzn2.aarch64, java.vendor=Amazon.com Inc., os=Linux, mergeFactor=5, java.runtime.version=23.0.2+7, os.arch=aarch64, source=merge, lucene.version=9.11.2, mergeMaxNumSegments=-1, timestamp=1738950429684}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=589sfyynireleyyu9fa3c21ed _25(9.11.2):C13577:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os.version=4.14.355-275.582.amzn2.aarch64, java.vendor=Amazon.com Inc., os=Linux, mergeFactor=3, java.runtime.version=23.0.2+7, os.arch=aarch64, source=merge, lucene.version=9.11.2, mergeMaxNumSegments=-1, timestamp=1738950429703}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=589sfyynireleyyu9fa3c21ef changeCount=168
  re_start_commit_index = re.compile(r"^IW \d+ \[([^;]+); (.*?)\]: startCommit index=(.*?) changeCount=(\d+)$")

  # e.g.: DWPT 219 [2025-02-07T17:43:00.953692447Z; GCR-Writer-1-thread-8]: flushedFiles=[_7.fdm, _7_BloomFilter_0.pos, _7.kdi, _7_BloomFilter_0.tim, _7_Lucene99HnswVectorsFormat_1.vec, _7.kdm, _7_BloomFilter_0.tmd, _7_Lucene99HnswVectorsFormat_1.vemf, _7_Lucene99_0.pay, _7.kdd, _7_Lucene99HnswVectorsFormat_2.vem, _7_BloomFilter_0.blm, _7_Lucene99HnswVectorsFormat_2.vec, _7_BloomFilter_0.pay, _7.fnm, _7_Lucene99HnswVectorsFormat_0.vemf, _7_Lucene99_0.tip, _7_BloomFilter_0.tip, _7_Lucene99_0.tim, _7.dvm, _7_Lucene99HnswVectorsFormat_0.vex, _7_Lucene99HnswVectorsFormat_1.vem, _7_Lucene99_0.pos, _7_BloomFilter_0.doc, _7.dvd, _7_Lucene99HnswVectorsFormat_0.vem, _7_Lucene99_0.tmd, _7_Lucene99HnswVectorsFormat_1.vex, _7_Lucene99HnswVectorsFormat_0.vec, _7.fdt, _7_Lucene99HnswVectorsFormat_2.vex, _7_Lucene99_0.doc, _7.fdx, _7_Lucene99HnswVectorsFormat_2.vemf]
  re_flushed_files = re.compile(r"^DWPT \d+ \[([^;]+); (.*?)\]: flushedFiles=\[(.*?)\]$")

  # e.g.: IW 512 [2025-02-07T17:43:24.119106602Z; GCR-Writer-1-thread-8]: done all syncs: [_m_Lucene99HnswVectorsFormat_1.vem, _l.fnm, _m.kdi, _k_Lucene99_0.tmd, _l_BloomFilter_0.pay, _k.dvd, _m.kdm, _m_Lucene99HnswVectorsFormat_1.vec, _k.dvm, _l_BloomFilter_0.tim, _l_BloomFilter_0.tip, _m.kdd, _m_Lucene99HnswVectorsFormat_1.vex, _k_Lucene99HnswVectorsFormat_2.vem, _m.dvd, _l_Lucene99_0.tip, _k_BloomFilter_0.tip, _k_BloomFilter_0.tim, _k.kdm, _m.dvm, _k_Lucene99HnswVectorsFormat_2.vec, _m_Lucene99_0.tim, _m_BloomFilter_0.blm, _l_Lucene99_0.pay, _k_BloomFilter_0.pay, _m_Lucene99_0.tip, _k.kdi, _m_Lucene99_0.pay, _m.si, _k_Lucene99HnswVectorsFormat_2.vex, _m_BloomFilter_0.pos, _l_Lucene99_0.tim, _l_Lucene99HnswVectorsFormat_2.vemf, _k.kdd, _l_Lucene99HnswVectorsFormat_2.vec, _m.fdt, _k_Lucene99HnswVectorsFormat_0.vem, _m.fdx, _l_Lucene99HnswVectorsFormat_2.vem, _l_Lucene99HnswVectorsFormat_1.vemf, _l_Lucene99HnswVectorsFormat_2.vex, _m_Lucene99HnswVectorsFormat_1.vemf, _k_Lucene99HnswVectorsFormat_0.vex, _m.fdm, _k_Lucene99_0.pay, _k.fdt, _l_BloomFilter_0.tmd, _l_Lucene99HnswVectorsFormat_0.vec, _k_BloomFilter_0.blm, _m_BloomFilter_0.pay, _l_Lucene99HnswVectorsFormat_0.vem, _k.fdx, _k_Lucene99HnswVectorsFormat_0.vec, _l_Lucene99HnswVectorsFormat_0.vex, _m_BloomFilter_0.tip, _m_BloomFilter_0.tim, _l_Lucene99_0.pos, _k.fdm, _k_Lucene99HnswVectorsFormat_2.vemf, _m_Lucene99HnswVectorsFormat_2.vex, _k_BloomFilter_0.doc, _m_BloomFilter_0.tmd, _l.kdi, _l.kdd, _k_Lucene99_0.tim, _k_Lucene99_0.tip, _m_Lucene99HnswVectorsFormat_2.vem, _m.fnm, _l.kdm, _k_BloomFilter_0.pos, _m_Lucene99HnswVectorsFormat_2.vec, _l.si, _l_Lucene99_0.doc, _m_Lucene99HnswVectorsFormat_0.vemf, _m_Lucene99_0.tmd, _k_Lucene99HnswVectorsFormat_0.vemf, _m_Lucene99HnswVectorsFormat_0.vex, _l_Lucene99_0.tmd, _k.fnm, _m_Lucene99HnswVectorsFormat_0.vem, _l_BloomFilter_0.doc, _l.dvd, _l.dvm, _m_Lucene99HnswVectorsFormat_0.vec, _k_Lucene99HnswVectorsFormat_1.vex, _m_Lucene99_0.doc, _l.fdm, _k_Lucene99_0.pos, _k_BloomFilter_0.tmd, _l.fdt, _l_BloomFilter_0.pos, _k_Lucene99HnswVectorsFormat_1.vemf, _k_Lucene99HnswVectorsFormat_1.vem, _m_BloomFilter_0.doc, _k_Lucene99HnswVectorsFormat_1.vec, _l.fdx, _l_BloomFilter_0.blm, _l_Lucene99HnswVectorsFormat_1.vex, _l_Lucene99HnswVectorsFormat_0.vemf, _m_Lucene99_0.pos, _k_Lucene99_0.doc, _k.si, _l_Lucene99HnswVectorsFormat_1.vec, _m_Lucene99HnswVectorsFormat_2.vemf, _l_Lucene99HnswVectorsFormat_1.vem]
  re_done_all_sync = re.compile(r"^IW \d+ \[([^;]+); (.*?)\]: done all syncs: \[(.*?)\]")

  # e.g.: IW 152529 [2025-02-07T21:10:59.329824625Z; Lucene Merge Thread #672]: merged segment size=1597.826 MB vs estimate=1684.011 MB
  re_merge_size_vs_estimate = re.compile(r"^IW \d+ \[([^;]+); (.*?)\]: merged segment size=([0-9.]+) MB vs estimate=([0-9.]+) MB$")

  # e.g.: FP 1 [2025-02-12T03:12:32.803827868Z; GCR-Writer-1-thread-9]: trigger flush: activeBytes=4295836570 deleteBytes=56720 vs ramBufferMB=4096.0
  re_trigger_flush = re.compile(r"^FP \d+ \[([^;]+); (.*?)\]: trigger flush: activeBytes=(\d+) deleteBytes=(\d+) vs ramBufferMB=([0-9.]+)$")

  # e.g. FP 1 [2025-02-20T06:32:01.061172105Z; GCR-Writer-1-thread-2]: 10 in-use non-flushing threads states
  re_in_use_non_flushing = re.compile(r"^FP \d+ \[([^;]+); (.*?)\]: (\d+) in-use non-flushing threads states$")

  # BD 1 [2025-02-20T06:32:01.561945031Z; GCR-Writer-1-thread-8]: compressed 34384 to 1040 bytes (3.02%) for deletes/updates; private segment null
  re_bd_compressed = re.compile(r"^BD \d+ \[([^;]+); (.*?)\]: compressed .*?; private segment null$")

  # e.g. IW 1 [2025-03-11T02:30:48.683898017Z; GCR-Writer-1-thread-20]: forceMergeDeletes: index now _7s(9.12.1):C331348/80627:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=2, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741659638391}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=4 :id=f0n9dlan30ntuukei465a3xsu _a4(9.12.1):C223780/37011:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=1, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741659871610}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=2 :id=f0n9dlan30ntuukei465a3xsv _8n(9.12.1):C270142/61221:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=2, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741659721395}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=4 :id=f0n9dlan30ntuukei465a3xsw _am(9.12.1):C381258/43045:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=2, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741659890146}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xsx _an(9.12.1):C274693/35956:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=4, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741659890147}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xsy _9k(9.12.1):C173088/27503:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=5, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741659805880}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=3 :id=f0n9dlan30ntuukei465a3xsz _bn(9.12.1):C216932/21056:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=5, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741659983564}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xt0 _cq(9.12.1):C27133/814:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=5, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741660070266}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=2 :id=f0n9dlan30ntuukei465a3xt1 _cm(9.12.1):C109775/5754:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=5, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741660070249}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xt2 _bp(9.12.1):C50036/4069:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=5, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741659983593}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=3 :id=f0n9dlan30ntuukei465a3xt3 _bo(9.12.1):C62456/5584:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=5, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741659983585}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=3 :id=f0n9dlan30ntuukei465a3xt4 _cp(9.12.1):C43268/931:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=5, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741660070254}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=2 :id=f0n9dlan30ntuukei465a3xt5 _co(9.12.1):C49110/1357:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=5, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741660070252}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=2 :id=f0n9dlan30ntuukei465a3xt6 _cn(9.12.1):C58416/1839:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=5, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741660070250}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=2 :id=f0n9dlan30ntuukei465a3xt7 _dl(9.12.1):C39382/757:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=5, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741660145963}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xt8 _dk(9.12.1):C46809/471:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=5, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741660145963}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xt9 _dj(9.12.1):C52043/554:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=5, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741660145944}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xta _dm(9.12.1):C17614/198:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=5, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741660145967}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xtb _dn(9.12.1):C2368/28:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={mergeMaxNumSegments=-1, lucene.version=9.12.1, source=merge, os.arch=aarch64, java.runtime.version=23.0.2+7, mergeFactor=3, os=Linux, java.vendor=Amazon.com Inc., os.version=5.10.234-225.895.amzn2.aarch64, timestamp=1741660145989}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xtc _ce(9.12.1):C13477/1047:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660145234, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=2 :id=f0n9dlan30ntuukei465a3xtd _ch(9.12.1):C12442/164:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660144776, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=2 :id=f0n9dlan30ntuukei465a3xte _d3(9.12.1):C8104/84:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660181795, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=2 :id=f0n9dlan30ntuukei465a3xtf _d0(9.12.1):C8097/66:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660185508, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=2 :id=f0n9dlan30ntuukei465a3xtg _dg(9.12.1):C2366/5:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660212321, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xs2 _d6(9.12.1):C7908/18:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660223692, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xs3 _db(9.12.1):C3810:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660218189, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=f0n9dlan30ntuukei465a3xs4 _d5(9.12.1):C8523/12:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660227230, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xs5 _da(9.12.1):C5032/15:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660227856, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xs6 _d9(9.12.1):C6689/29:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660232027, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xs7 _d8(9.12.1):C7459/68:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660235610, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xs8 _d2(9.12.1):C8290/13:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660235299, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xs9 _de(9.12.1):C3205/49:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660230400, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xsa _d1(9.12.1):C9060/70:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660236983, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xsb _dc(9.12.1):C2980:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660235435, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=f0n9dlan30ntuukei465a3xsc _d7(9.12.1):C10002/17:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660241235, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xsn _dd(9.12.1):C3380/2:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660237021, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xso _df(9.12.1):C4973/2:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660239894, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xsp _do(9.12.1):C1636:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660237709, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}] :id=f0n9dlan30ntuukei465a3xsq _d4(9.12.1):C9825/33:[indexSort=<int: "marketplaceid"> missingValue=-1,<long: "asin_ve_sort_value">! missingValue=0,<string: "ve-family-id"> missingValue=SortField.STRING_FIRST,<string: "docid"> missingValue=SortField.STRING_FIRST]:[diagnostics={os=Linux, java.vendor=Amazon.com Inc., java.runtime.version=23.0.2+7, timestamp=1741660248673, source=flush, lucene.version=9.12.1, os.version=5.10.234-225.895.amzn2.aarch64, os.arch=aarch64}]:[attributes={Lucene90StoredFieldsFormat.mode=BEST_SPEED}]:delGen=1 :id=f0n9dlan30ntuukei465a3xst
  re_force_merge_deletes_start = re.compile(r"^IW \d+ \[([^;]+); (.*?)\]: forceMergeDeletes: index now")

  # e.g. MS 1 [2025-03-11T02:30:48.686201756Z; GCR-Writer-1-thread-20]:   no more merges pending; now return
  re_no_more_merges = re.compile(r"^MS \d+ \[([^;]+); (.*?)\]:\s+no more merges pending; now return")

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

  next_line_is_flush_by_ram = False
  ram_buffer_mb = None
  push_back_line = None

  in_full_flush = False
  merge_commit_merges = None
  do_strip_log_prefix = None
  force_merge_deletes_thread_name = None
  force_merge_deletes_threads = set()

  with open(infostream_log) as f:
    while True:
      if push_back_line is not None:
        line = push_back_line
        push_back_line = None
      else:
        line = f.readline()
        line_number += 1
      if line == "":
        break
      line = line.strip()
      # print(f'{line}')

      if do_strip_log_prefix is None:
        do_strip_log_prefix = line.find("LoggingInfoStream:") != -1

      orig_line = line

      if do_strip_log_prefix:
        spot = line.find("LoggingInfoStream:")
        line = line[spot + 19 :]
        print(f"now: {line}")

      try:
        if next_line_is_flush_by_ram:
          # curiously, thread 1 can trigger flush-by-ram, but thread 2 can sometimes be the one that swoops
          # in and picks the biggest segment and flushes it (by RAM trigger).  we try to detect such a swoop
          # here:
          m = re_bd_compressed.match(line)
          if m is not None:
            timestamp = parse_timestamp(m.group(1))
            thread_name = m.group(2)
            next_line_is_flush_by_ram = False
            assert trigger_flush_thread_name is not None
            print(f"now set trigger_flush_thread_name from {trigger_flush_thread_name} to {thread_name} line {line_number}")
            trigger_flush_thread_name = thread_name
            continue

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
            print(f"failed to parse the expected {num_segments_expected} number of sogments: found {len(seg_details)}: {seg_details}\n{line}")
            raise RuntimeError(f"failed to parse the expected {num_segments_expected} number of sogments: found {len(seg_details)} {seg_details}")

          if first_checkpoint:
            # seed the initial segments in the index
            for segment_name, source, is_cfs, max_doc, del_count, del_gen, diagnostics, attributes in seg_details:
              segment = Segment(segment_name, source, max_doc, None, timestamp - datetime.timedelta(seconds=30), None, line_number)
              # this is most likely not correct (underestimate), so if index was pre-existing, all write amplification will
              # be undercounted:
              segment.net_write_amplification = 1

              # also most likely wrong:
              if type(source) is str and source.startswith("flush"):
                segment.max_ancestor_depth = 0
              else:
                segment.max_ancestor_depth = 1

              print(f"do first {segment_name}")
              by_segment_name[segment_name] = segment
              segment.add_event(timestamp, "light", line_number)
              segment.del_count_reclaimed = 0
            global_start_time = timestamp
            # print(f'add initial segment {segment_name} {max_doc=} {del_count=}')
            first_checkpoint = False
          else:
            for segment_name, source, is_cfs, max_doc, del_count, del_gen, diagnostics, attributes in seg_details:
              segment = by_segment_name[segment_name]
              if del_count != segment.del_count:
                # assert segment.del_count is None or del_count > segment.del_count, f'{segment_name=} {segment.del_count=} {del_count=} {line_number=}\n{line=}'
                if segment.del_count is not None and segment.del_count > del_count:
                  print(f"WARNING: del_count went backwards? {segment_name=} {segment.del_count=} {del_count=} {line_number=}\n{line=}")
                else:
                  # print(f'update {segment_name} del_count from {segment.del_count} to {del_count}')
                  if segment.del_count is None:
                    del_inc = del_count
                  else:
                    del_inc = del_count - segment.del_count
                  if del_inc > 0:
                    # TODO: is it normal/ok dels didn't change?  i think so?
                    segment.add_event(timestamp, f"del +{del_inc:,} gen {del_gen}", line_number)
                    segment.del_count = del_count

          checkpoints.append((timestamp, thread_name) + tuple(seg_details))
          # print(f'{seg_checkpoint=}')

          continue

        m = re_seg_size.match(line)
        if m is not None:
          segment_name = "_" + m.group(3)
          size_mb = float(m.group(4))
          segment = by_segment_name[segment_name]
          if segment.size_mb is None:
            # necessary when IW started up on an already populated index -- we see which segments it
            # initially loaded, but it's only later (now, on this line) where we see how large each
            # of those initial segment is:
            print(f"now set initial segment size for {segment_name} to {size_mb:.3f}")
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
          segment_name = "_" + m.group(3)
          max_doc = int(m.group(4))

          print(f"compare {thread_name=} to {commit_thread_name=} and {trigger_flush_thread_name=} {segment_name=}")

          if trigger_flush_thread_name == thread_name:
            source = "flush-by-RAM"
            trigger_flush_thread_name = None
          elif commit_thread_name is not None:
            # logic may not be quite correct?  can we flush segment due to RAM usage even while another thread already kicked off commit?  unlikely...
            source = "flush-commit"
          else:
            source = "flush"
          print(f"  {source=}")

          segment = Segment(segment_name, source, max_doc, None, timestamp, None, line_number)
          segment.net_write_amplification = 1
          # flushed segments are brand new, no ancestors:
          segment.max_ancestor_depth = 0

          if source == "flush-by-RAM":
            assert ram_buffer_mb is not None
            segment.ram_buffer_mb = ram_buffer_mb
            ram_buffer_mb = None

          if in_full_flush:
            segment.full_flush_ord = full_flush_count
          else:
            # print(f'WARNING: non-full-flush case not handled?  segment={segment_name}')
            pass

          by_segment_name[segment_name] = segment
          assert thread_name not in by_thread_name, f"thread {thread_name} was already/still in by_thread_name?"

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
          segment.add_event(timestamp, f"done write {index_part}", line_number)
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
          segment_name = "_" + m.group(3)
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
          segment.add_event(timestamp, "create compound file", line_number)
          segment.is_cfs = True
          continue

        m = re_publish_delgen.search(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          segment_name = "_" + m.group(3)
          segment.publish_timestamp = timestamp
          by_segment_name[segment_name].add_event(timestamp, "light", line_number)
          continue

        m = re_new_merge_deletes.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          del_count = int(m.group(3))
          if del_count > 0:
            by_thread_name[thread_name].born_del_count = del_count
            by_thread_name[thread_name].del_count = del_count
            by_thread_name[thread_name].add_event(timestamp, f"del {del_count:,} born", line_number)
            print(f"merged born del count {by_thread_name[thread_name].name} --> {by_thread_name[thread_name].born_del_count}")
          continue

        m = re_merge_start.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          segment_name = "_" + m.group(3)
          # parse the very verbose merging segments description
          # TODO: pull out other stuff, like compound or not, number of deletions, etc.
          merging_segments = re_segment_desc.findall(m.group(4))
          merging_segment_names = [f"_{x[0]}" for x in merging_segments]

          # how many deletes this merge reclaims:
          del_count_reclaimed = 0
          sum_max_doc = 0

          sum_live_size_mb = 0
          sum_net_write_amplification = 0
          max_ancestor_depth = 0

          for tup in merging_segments:
            merging_seg_name = "_" + tup[0]
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

            live_ratio = 1.0 - (del_count_inc / max_doc)
            live_size_mb = live_ratio * seg.size_mb

            sum_live_size_mb += live_size_mb

            # 1+ to account for the merge, which copies in the bytes already written and writes
            # the live portion of the bytes again:
            sum_net_write_amplification += (1 + seg.net_write_amplification) * live_size_mb

            max_ancestor_depth = max(max_ancestor_depth, seg.max_ancestor_depth)

          # print(f'{len(merging_segments)} merging segments in {m.group(4)}: {merging_segments}')

          segment = Segment(segment_name, ("merge", merging_segment_names), None, None, timestamp, None, line_number)
          segment.sum_max_doc = sum_max_doc
          segment.del_count_reclaimed = del_count_reclaimed
          segment.max_ancestor_depth = max_ancestor_depth + 1

          if thread_name in force_merge_deletes_threads:
            # TODO: would be less fragile to match by segments being merged?
            # first merge done by this thread is force-merge-deletes:
            force_merge_deletes_threads.remove(thread_name)
            segment.is_force_merge_deletes = True

          if sum_live_size_mb == 0:
            segment.net_write_amplification = 0
          else:
            segment.net_write_amplification = sum_net_write_amplification / sum_live_size_mb

          if len(merge_during_commit_events) > 0 and merge_during_commit_events[-1][1] is None:
            # ensure this merge kickoff was really due to a merge-on-commit merge request:
            # print(f'MOC check {thread_name} vs {merge_commit_threads} and {merging_segment_names} vs {merge_commit_merges}')
            if thread_name in merge_commit_threads:
              # print(f'MOC: YES {line_number}')
              segment.merge_during_commit = len(merge_during_commit_events)
            elif tuple(merging_segment_names) in merge_commit_merges:
              # print(f'MOC: YES2 {line_number}')
              segment.merge_during_commit = len(merge_during_commit_events)
            else:
              # print(f'MOC: NO {line_number}')
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
          segment.add_event(timestamp, f"done merge {part}", line_number)
          if segment.max_doc is None:
            segment.max_doc = max_doc
            if max_doc != segment.sum_max_doc - segment.del_count_reclaimed:
              # likely IW is not logging the actual maxDoc/delCount for segments being merged?
              new_del_count_reclaimed = segment.sum_max_doc - max_doc
              print(
                f"WARNING: segment {segment.name} has wrong count sums?  {segment.max_doc=} {segment.sum_max_doc=} {segment.del_count_reclaimed=} {line_number=}; fix del_count_reclaimed from {segment.del_count_reclaimed} to {new_del_count_reclaimed}"
              )
              segment.del_count_reclaimed = new_del_count_reclaimed
              # raise RuntimeError(f'segment {segment.name} has wrong count sums?  {segment.max_doc=} {segment.sum_max_doc=} {segment.del_count_reclaimed=} {line_number=}')
          elif max_doc != segment.max_doc:
            raise RuntimeError(f"merging segment {segment.name} sees different max_doc={segment.max_doc} vs {max_doc} {line_number=}")
          continue

        m = re_msec_to_build_docmaps.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          msec = float(m.group(3))
          segment = by_thread_name[thread_name]
          segment.add_event(timestamp, "build merge sorted DocMaps", line_number)
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
            raise RuntimeError(f"merging segment {segment.name} sees different max_doc={segment.max_doc} vs {max_doc}")
          segment.size_mb = size_mb
          segment.add_event(timestamp, "done merge", line_number)
          continue

        m = re_merge_commit.search(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          segment = by_thread_name[thread_name]
          # TODO: also parse/verify the "index=" part of this line?  that we know all the segments in the index now?
          segment.add_event(timestamp, "light", line_number)
          segment.publish_timestamp = timestamp
          if segment.merge_during_commit is not None:
            segment.merge_during_commit_ord = segment.merge_during_commit - 1
            if segment.merge_during_commit == len(merge_during_commit_events) and merge_during_commit_events[-1][1] is None:
              # merge commit finished in time!
              segment.merge_during_commit = ("success", (timestamp - merge_during_commit_events[-1][0]).total_seconds())
            else:
              segment.merge_during_commit = ("timeout", (timestamp - merge_during_commit_events[segment.merge_during_commit - 1][0]).total_seconds())

          for segment_name in segment.source[1]:
            old_segment = by_segment_name.get(segment_name)
            if old_segment is not None:
              old_segment.end_time = timestamp
              old_segment.merged_into = segment
              old_segment.merge_event(segment.start_time, f"start merge into {segment.name}", line_number)
              old_segment.add_event(timestamp, f"merged into {segment.name}", line_number)
            else:
              raise RuntimeError(f"cannot find segment {segment_name} for merge?")
          continue

        m = re_merge_during_commit_start.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          # print(f"MOC: start {thread_name} {line_number} {timestamp}")
          merge_during_commit_events.append([timestamp, None])
          merge_on_commit_thread_name = thread_name
          merge_commit_threads = set()

          merge_commit_merges = set()

          # moc thread sneakinss -- one thread might register some merges, while another pulls them and launches CMS threads
          # peek ahead to read all the merges that will be merge-on-commit
          counter = 1
          while True:
            line = f.readline()
            line_number += 1
            if line == "":
              push_back_line = line
              break
            line = line.strip()
            if line.startswith(f"{counter}: "):
              seg_details = parse_seg_details(line[line.find(":") + 1 :].strip())
              tup = tuple(x[0] for x in seg_details)
              # print(f'now add moc tup: {tup}')
              merge_commit_merges.add(tup)
              counter += 1
            else:
              push_back_line = line
              break

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
          # print(f"got launch new merge thread {thread_name=} {new_merge_thread_name} {force_merge_deletes_thread_name=}")
          if force_merge_deletes_thread_name is not None:
            force_merge_deletes_threads.add(new_merge_thread_name)
          continue

        m = re_merge_during_commit_end.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          # print(f'MOC: end {thread_name} {line_number} {timestamp}')

          merge_on_commit_thread_name = None
          merge_commit_threads = None
          merge_commit_merges = None

          merge_during_commit_events[-1][1] = timestamp
          continue

        m = re_start_full_flush.match(line)
        if m is not None:
          # we should NOT be in the middle of a flush-by-RAM?  hmm but what if commit
          # is called when we are ...?
          assert trigger_flush_thread_name is None, f"{trigger_flush_thread_name} on line {line_number}"
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          full_flush_events.append([timestamp, None])
          in_full_flush = True
          full_flush_count += 1
          full_flush_start_time = timestamp
          # print(f"start full flush {line_number}")
          continue

        m = re_end_full_flush.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          # print(f"clear commit thread")
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
          # print(f"set commit thread {commit_thread_name} {timestamp=}")
          continue

        m = re_start_commit_index.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          seg_details = parse_seg_details(m.group(3))
          commits.append((timestamp, thread_name) + tuple(seg_details))
          # print(f"startCommit: {seg_details}")
          continue

        m = re_flushed_files.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          s = m.group(3).split(", ")
          by_thread_name[thread_name].file_count = len(s)
          continue

        m = re_done_all_sync.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          s = m.group(3).split(", ")
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
          deleteBytes = int(m.group(4))
          ram_buffer_mb = float(m.group(5))
          trigger_flush_thread_name = thread_name

        m = re_in_use_non_flushing.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          next_line_is_flush_by_ram = True
          print(f"now set next_line_flush_by_ram thread={thread_name}")
          # e.g.:
          # FP 1 [2025-02-20T06:32:01.061172105Z; GCR-Writer-1-thread-2]: 10 in-use non-flushing threads states
          # BD 1 [2025-02-20T06:32:01.561945031Z; GCR-Writer-1-thread-8]: compressed 34384 to 1040 bytes (3.02%) for deletes/updates; private segment null
          # DWPT 1 [2025-02-20T06:32:01.562510185Z; GCR-Writer-1-thread-8]: flush postings as segment _1h numDocs=17052

        m = re_force_merge_deletes_start.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          force_merge_deletes_thread_name = thread_name

        m = re_no_more_merges.match(line)
        if m is not None:
          timestamp = parse_timestamp(m.group(1))
          thread_name = m.group(2)
          if force_merge_deletes_thread_name is not None:
            if force_merge_deletes_thread_name == thread_name:
              force_merge_deletes_thread_name = None
            else:
              # this can legit happen if one thread is doing merge-on-commit, and another thread calls forceMergeDeletes.  in this case,
              # the FMD thread can register a bunch of merges, but the MOC thread pulls (some of) those merges and launches them
              # raise RuntimeError(f"WTF saw merges done from different {thread_name=} than {force_merge_deletes_thread_name=}?")
              force_merge_deletes_thread_name = None

      except KeyboardInterrupt:
        raise
      except:
        print(f"unhandled exception on line {line_number} of {infostream_log}: {orig_line}")
        raise

  for segment in Segment.all_segments:
    print(f"\n{segment.name}:\n{segment.to_verbose_string(global_start_time, global_end_time, True, True)}")
    if segment.end_time is None:
      print(f"set end_time for segment {segment.name} to global end time")
      segment.end_time = global_end_time

    # carefully accumulate delete count to avoid double-counting deletes.  it's tricky
    # because deletes that arrive as a segment is being merged away are first applied (lit)
    # in the merging segments.  but when the merge finishes, those new deletes (that arrived
    # during merge) are carried over to the newly merged segment ("born delete count"), so
    # we don't want to double count those concurrent deletes while merging.  deletes that were
    # already applied to the merging segments before the merge started are compacted away.
    del_count_so_far = 0

    for timestamp, event, line_number in segment.events:
      if event.startswith("del "):
        del_count_so_far += int(event[4 : event.find(" ", 4)].replace(",", ""))
      elif event == "light":
        if segment.del_count_reclaimed is not None:
          segment.del_reclaims_per_sec = segment.del_count_reclaimed / (timestamp - segment.start_time).total_seconds()
        elif type(segment.source) is tuple:
          print(f"WARNING: merged segment {segment.name} has no del_count_reclaimed")
      elif event.startswith("start merge into"):
        # this is dusk start for this segment -- carefully compute incoming deletes rate
        segment.del_create_count = del_count_so_far
        segment.del_creates_per_sec = del_count_so_far / (timestamp - segment.start_time).total_seconds()

  if len(Segment.all_segments) == 0:
    raise RuntimeError(f"found no segments in {infostream_log}")

  open(segments_out, "wb").write(pickle.dumps((global_start_time, global_end_time, Segment.all_segments, full_flush_events, merge_during_commit_events, checkpoints, commits)))

  print(f'wrote {len(Segment.all_segments)} segments to "{segments_out}": {os.path.getsize(segments_out) / 1024 / 1024:.1f} MB')


if __name__ == "__main__":
  main()
