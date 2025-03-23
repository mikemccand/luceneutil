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

import argparse
import bisect
import collections
import random
import sys

#
# each line of index events source is this syntax:
#
#   <timestamp> <op> <docid> [<size-in-bytes>]
#
# <timestamp> is a float in elapsed seconds, must monotonically increase, and must
# begin at 0.0.  <op> is add, update, del, refresh.  <docid> should be an int.
# <size-in-bytes> is only present for add and update ops.
#

# unlike Lucene, this sim requires a primary key field ("docid"), and requires that the
# indexing operations are correct (never add same docid twice w/o first deleting the prior
# one, or using update_document)


# TODO
#   - more accurately model spiky search traffic, in synchrony with incoming indexing events
#   - better model merged segment size -- not just sum of non-deleted docs, but some synergy/overlap e.g. terms dict
#   - more accurately model doc size -> segment size (and then RAM multiplier over that)
#   - pluggable flush policy, instead of always flushing largest segment
#   - allow targetting certain docs to certain segments (in-RAM segment routing)
#   - allow new session appending to an already created index
#   - hmm could this write a good enough infostream to generate segtrace?  or, better, write the segments.pk directly?
#   - add optimistic concurrency version C/V
#   - allow passing seed by cl
#   - allow passing num_indexing_threads by cl
#   - also experiment with intentional assigment of docs to segments
#   - simulate merge-on-commit
#   - maybe a flush policy?
#   - input params
#     - number of indexing threads
#     - pluggle MP
#   - output
#     - net search cost
#     - net bytes replicated
#     - write amplification
#     - search concurrency somehow?

timestamp_sec = None


class Segment:
  next_name = 0
  segments_by_name = {}

  def __init__(self):
    self.name = f"_{Segment.next_name}"
    Segment.next_name += 1
    self.segments_by_name[self.name] = self
    self.start_time_sec = timestamp_sec

    # will be set to flush or merge with details:
    self.source = None

    self.deletes = set()

    # maps each doc to size-in-bytes
    self.docs = {}

    # true sum (no multiplier) of all docs in this segment; this does NOT take dels into account
    self.size_in_bytes = 0

    # False once we are written to disk
    self.in_ram = True

  def __repr__(self):
    if self.in_ram:
      mult = seg_ram_multiplier * seg_disk_multiplier
    else:
      mult = 1.0
    return f"<segment {self.name} in_ram={self.in_ram} size={mult * self.size_in_bytes / 1024 / 1024:.1f} MB>"

  def add_document(self, docid, size_bytes):
    """
    Only used for in-memory segments.
    """

    assert docid not in self.docs
    assert docid not in self.deletes

    self.docs[docid] = size_bytes
    self.size_in_bytes += size_bytes

  def delete_document(self, docid):
    assert docid in self.docs
    assert docid not in self.deletes
    self.deletes.add(docid)


class Index:
  def __init__(self, rand, num_index_threads=6, ram_multiplier=4.0, max_ram_buffer_mb=4096, disk_doc_multiplier=1.0, merge_mb_per_sec=1024 / 60):
    global seg_ram_multiplier
    global seg_disk_multiplier

    seg_ram_multiplier = ram_multiplier
    seg_disk_multiplier = disk_doc_multiplier

    # how many MB/sec one merge produces -- we use this to estimate merge run time
    # based on its final (after compacting deletes) merged size
    self.merge_mb_per_sec = merge_mb_per_sec

    # multiplier over doc's size that it will consume in RAM (in-memory
    # segments take more RAM than on-disk segments to store documents):
    self.ram_multiplier = ram_multiplier

    # multiplier on disk usage vs reported doc size -- very app dependent, and overly simplistic
    # to just use a single multiplier, but hey..
    self.disk_doc_multiplier = disk_doc_multiplier

    self.both_multiplier = ram_multiplier * disk_doc_multiplier
    print(f"both multiplier {self.both_multiplier}")

    self.ram_bytes_used = 0
    self.merging_segments = set()

    self.max_ram_buffer_bytes = max_ram_buffer_mb * 1024 * 1024

    self.num_index_threads = num_index_threads
    self.docid_to_segment = {}
    self.deletes_by_segment = {}
    self.on_disk_name_to_segment = {}
    self.index_thread_to_segment = collections.defaultdict(Segment)
    self.rand = rand

    # "on-disk" segments
    self.segments = []

    # pending merges to complete/commit, ordered by future timestamp
    self.pending = []

  def get_doc_counts(self):
    max_doc = 0
    del_count = 0
    for seg in self.segments:
      max_doc += len(seg.docs)
      del_count += len(seg.deletes)
    for seg in self.index_thread_to_segment.values():
      max_doc += len(seg.docs)
      del_count += len(seg.deletes)
    return max_doc, del_count

  def refresh(self):
    """
    Returns frozen segments for searching.
    """

    for index_thread, seg in list(self.index_thread_to_segment.items()):
      self.flush(index_thread, seg, "refresh")
    print(f"after refresh {self.ram_bytes_used} {len(self.index_thread_to_segment)}")

    # all segments are now on-disk

    l = []
    for seg in self.segments:
      l.append((seg, seg.deletes.copy()))

    return l

  def flush(self, index_thread, seg, reason):
    print(f"{timestamp_sec:8.2f} s: now flush {seg=} {reason=} {index_thread=}")
    self.segments.append(seg)
    seg.in_ram = False
    seg.source = "flush:" + reason
    self.ram_bytes_used -= seg.size_in_bytes * self.both_multiplier
    # print(f'  after flush ram={self.ram_bytes_used/1024/1024:.1f}')
    assert self.index_thread_to_segment[index_thread] == seg
    del self.index_thread_to_segment[index_thread]
    self.maybe_merge("flush")

  def finish_merge(self, merged_seg, to_merge_segments, del_docs_to_reclaim, final_size_bytes, reason):
    for seg in to_merge_segments:
      assert seg in self.merging_segments
      self.merging_segments.remove(seg)

    merge_desc = ",".join(seg.name for seg in to_merge_segments)
    print(f"{timestamp_sec:8.2f} s: now finish merge seg={merged_seg.name} reason={reason} {merge_desc}")
    to_reclaim = set(to_merge_segments)

    del_reclaim_count = 0
    merged_size_bytes = 0

    for i in range(len(to_merge_segments)):
      seg = to_merge_segments[i]

      # these were the deletes as they existed when the merge kicked off:
      seg_dels = del_docs_to_reclaim[i]

      for docid, size_bytes in seg.docs.items():
        if docid not in seg_dels:
          merged_seg.docs[docid] = size_bytes
          merged_size_bytes += size_bytes
        else:
          del_reclaim_count += 1

      # carryover any new deletes:
      for docid in seg.deletes:
        # this doc is currently deleted, but wasn't deleted when merge kicked off,
        # so it was a concurrent delete that arrived while the merge was running:
        if docid not in seg_dels:
          assert docid in merged_seg.docs
          assert docid not in merged_seg.deletes
          merged_seg.deletes.add(docid)

    print(f"    {len(merged_seg.deletes)} carryover deletes")
    print(f"    {del_reclaim_count} reclaimed deletes")
    print(f"    {merged_size_bytes / 1024 / 1024:.1f} MB")
    assert merged_size_bytes == final_size_bytes, f"{merged_size_bytes=} {final_size_bytes=}"

    merged_seg.size_in_bytes = merged_size_bytes
    merged_seg.in_ram = False
    merged_seg.source = ("merge", to_merge_segments, del_docs_to_reclaim)

    loc = 0
    first_loc = None
    del_seg_count = 0
    while loc < len(self.segments):
      seg = self.segments[loc]
      if seg in to_reclaim:
        if first_loc is None:
          # this is where we will placed the newly merged segment
          first_loc = loc
        del self.segments[loc]
        del_seg_count += 1
      else:
        loc += 1

    # make sure we saw (and deleted) all segments we just merged
    assert del_seg_count == len(to_merge_segments)

    self.segments.insert(first_loc, merged_seg)
    self.maybe_merge("finish-merge")

  def launch_merge(self, to_merge_segments, reason):
    """
    Simulates a merge running, scheduling the end of the merge to commit / reclaim deletes.
    The merge runs in the background ... once the clock advances to the merge finish time,
    we commit the merge.

    We model merge run-time as simple linear multiplier on size of merged segment.
    """

    for seg in to_merge_segments:
      assert seg not in self.merging_segments
      self.merging_segments.add(seg)

    merged_seg = Segment()

    final_size_bytes = 0
    del_reclaim_bytes = 0
    del_reclaim_count = 0
    del_docs_to_reclaim = []
    for seg in to_merge_segments:
      final_size_bytes += seg.size_in_bytes

      # account for delete reclaiming -- we must make a copy of del docs at the start
      # because these are the actual deletes we will compact away.  while the merge is running,
      # new deletes will concurrently arrive against these merging segments, and we must
      # carryover those deletes at the end
      seg_del_docs = seg.deletes.copy()
      del_reclaim_count += len(seg_del_docs)
      for delete_docid in seg_del_docs:
        del_reclaim_bytes += seg.docs[delete_docid]
      del_docs_to_reclaim.append(seg_del_docs)

    merge_seg_names = [seg.name for seg in to_merge_segments]

    merge_run_time_sec = (final_size_bytes / 1024 / 1024) / self.merge_mb_per_sec

    print(
      f"{timestamp_sec:8.2f} s: start merge seg={merged_seg.name} {','.join(merge_seg_names)} del_reclaim={del_reclaim_bytes / 1024 / 1024:.1f} MB ({del_reclaim_count} docs) final_size={final_size_bytes / 1024 / 1024:.1f} MB - {del_reclaim_bytes / 1024 / 1024:.1f} MB; merge will take {merge_run_time_sec:.1f} s (= {timestamp_sec + merge_run_time_sec:.1f} s abs)"
    )
    final_size_bytes -= del_reclaim_bytes

    finish_time_sec = timestamp_sec + merge_run_time_sec
    spot = bisect.bisect_left(self.pending, finish_time_sec, key=lambda x: x[0])

    self.pending.insert(spot, (finish_time_sec, "finish-merge", merged_seg, to_merge_segments, del_docs_to_reclaim, final_size_bytes, reason))

  def maybe_merge(self, reason):
    # we can only look at segments that are not already being merged:
    eligible_segments = list(set(self.segments) - self.merging_segments)

    # so we make deterministic choices under same random seed:
    eligible_segments.sort(key=lambda x: -x.size_in_bytes)

    if len(eligible_segments) > 10:
      # random merging!
      if self.rand.random() <= 0.2:
        merge_num_segments = self.rand.randint(2, 10)
        segs = self.rand.sample(eligible_segments, merge_num_segments)
        self.launch_merge(segs, reason)

  def add_document(self, docid, size_bytes):
    self.ram_bytes_used += self.both_multiplier * size_bytes

    index_thread = self.rand.randint(0, self.num_index_threads - 1)
    seg = self.index_thread_to_segment[index_thread]

    if docid in seg.docs and docid in seg.deletes:
      # weird/annoying case that Lucene supports but we do not, where same docid is indexed more than
      # once into the same in-RAM segment.  for now we hack around this by pretending we didn't
      # see the prior add+delete, but this isn't accurate to Lucene.  hopefully minor...
      seg.size_in_bytes -= seg.docs[docid]
      self.ram_bytes_used -= self.both_multiplier * seg.docs[docid]
      del seg.docs[docid]
      seg.deletes.remove(docid)
      # print(f"WARNING: {timestamp_sec:.2f} s: handling the annoying double-add to RAM segment case for {docid=}")

    seg.add_document(docid, size_bytes)

  def update_document(self, docid, size_bytes):
    # in Lucene these two events are atomic:
    self.delete_document(docid)
    self.add_document(docid, size_bytes)

  def run_events(self, next_timestamp_sec):
    global timestamp_sec
    while len(self.pending) > 0 and self.pending[0][0] < next_timestamp_sec:
      timestamp_sec = self.pending[0][0]
      if self.pending[0][1] == "finish-merge":
        ts, _, merged_seg, to_merge_segments, del_docs_to_reclaim, final_size_bytes, reason = self.pending[0]
        self.finish_merge(merged_seg, to_merge_segments, del_docs_to_reclaim, final_size_bytes, reason)
        del self.pending[0]

  def find_docid(self, docid):
    # check in-RAM segments
    for seg in self.index_thread_to_segment.values():
      if docid in seg.docs and docid not in seg.deletes:
        return seg

    # check on-disk segments
    for seg in self.segments:
      if docid in seg.docs and docid not in seg.deletes:
        return seg

    return None

  def delete_document(self, docid):
    seg = self.find_docid(docid)
    if seg is not None:
      seg.delete_document(docid)
      # print(f'delete {docid=} found {seg=}')

  def maybe_flush_by_ram(self):
    while self.ram_bytes_used > self.max_ram_buffer_bytes:
      # print(f'{timestamp_sec:8.2f} s: now flush-by-ram: {self.ram_bytes_used/1024./1024.:.1f} MB')
      # TODO: pluggable flush policy
      max_seg = None
      sum_size_in_bytes = 0
      for index_thread, seg in self.index_thread_to_segment.items():
        # print(f'seg {seg.name} ram {self.both_multiplier*seg.size_in_bytes/1024/1024}')
        sum_size_in_bytes += seg.size_in_bytes
        if max_seg is None or seg.size_in_bytes > max_seg.size_in_bytes:
          max_seg = seg
          max_index_thread = index_thread
      # print(f'{self.both_multiplier*sum_size_in_bytes} vs {self.ram_bytes_used}')
      assert int(self.both_multiplier * sum_size_in_bytes) == self.ram_bytes_used
      self.flush(max_index_thread, max_seg, "by_ram")


def main():
  global timestamp_sec

  parser = argparse.ArgumentParser("Simulates an indexing run from a captured log of actual indexing events, to test different merge policies")
  parser.add_argument("events_file", type=str)
  parser.add_argument("--seed", type=int, default=17, help="random seed")
  args = parser.parse_args()

  index_events_source = args.events_file

  print(f"seed is {args.seed=}")
  rand_seed = args.seed
  rand = random.Random(rand_seed)
  refresh_every_sec = 60
  print_every_sec = 5

  index = Index(rand)

  next_refresh_sec = refresh_every_sec
  next_print_sec = print_every_sec

  # each doc's size is the incoming serialized size; deflate this to the "true" content size (ish):
  incoming_size_mult = 0.33

  searching_segs = []
  replica_segs = set()

  search_net_docs = 0
  search_net_deletes = 0
  net_replicate_bytes = 0

  with open(index_events_source, "r") as f:
    while True:
      line = f.readline()
      if line == "":
        break

      tup = line.split()
      next_timestamp_sec = float(tup[0])

      # finish any merges:
      index.run_events(next_timestamp_sec)

      timestamp_sec = next_timestamp_sec

      match tup[1]:
        case "add":
          docid = tup[2]
          size_bytes = int(incoming_size_mult * int(tup[3]))
          index.add_document(docid, size_bytes)
        case "update":
          docid = tup[2]
          size_bytes = int(incoming_size_mult * int(tup[3]))
          index.update_document(docid, size_bytes)
        case "del":
          docid = tup[2]
          index.delete_document(docid)
        case _:
          raise RuntimeError(f'operation must be add, update, or del; got "{tup[0]}"')

      index.maybe_flush_by_ram()

      if timestamp_sec > next_refresh_sec:
        print(f"\n{timestamp_sec:8.2f} s: now timed-refresh @ {index.ram_bytes_used / 1024 / 1024.0:.1f} MB")
        searching_segs = index.refresh()
        print(f"{len(index.segments)} on-disk segments")
        next_refresh_sec += refresh_every_sec

        # add up search cost -- since we only get a new searchable image with every refresh, we just sum
        # search cost, now, on the frozen deletes.  more accurate would be to record actual incoming query
        # timestamps and replay against matched refreshed searchers

        for seg, frozen_deletes in searching_segs:
          search_net_docs += len(seg.docs)
          search_net_deletes += len(frozen_deletes)
          if seg not in replica_segs:
            replica_segs.add(seg)
            net_replicate_bytes += seg.size_in_bytes

      if timestamp_sec > next_print_sec:
        max_doc, del_count = index.get_doc_counts()
        print(
          f"\n{timestamp_sec:8.2f} s: seg_count={len(index.segments)} num_doc={max_doc - del_count:,} {max_doc=:,} {del_count=:,} ({100 * del_count / max_doc:.1f} %) merges_running={len(index.pending)}"
        )
        next_print_sec += print_every_sec

    # summary stats
    print(f"\nDONE!")
    print(f"  {search_net_docs=:,} {search_net_deletes=:,} ({100.0 * search_net_deletes / search_net_docs:.1f} %)")
    print(f"  replicated {net_replicate_bytes / 1024 / 1024 / 1024.0:,.1f} GB")


if __name__ == "__main__":
  main()
