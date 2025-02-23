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

import math
import pickle
import sys
import os
import infostream_to_segments
import datetime

infostream_to_segments.sec_to_time_delta

# pip3 install intervaltree
import intervaltree

# pip3 install graphviz
import graphviz

# TODO
#   - IW should differentiate "done waithing for merge during commit" timeout vs all requested merges finished
#   - fix TMP to log deletes target and changes to the target
#   - fix time-cursor text to say "in full flush", "in merge on commit", etc.
#   - fix zoom slider to only zoom in x
#   - why does delete pct suddenly drop too much (screen shot 1)
#   - flush-by-ram detection is buggy
#   - make flush-by-ram a different color
#   - make scatterplot of merged segment size vs merge time
#   - get #t=... anchor link working!
#   - plot mb * sec?
#   - plot scatterplot
#   - plot some histograms ... %deletes reclaimed on merge, seg size mb, seg age sec
#   - record forceMergeDeletes triggers too
#   - can we record cpu ticks somehow into infostream...
#   - get this to somehow download/install dygraph-min.js, dygraph.css, etc.?
#   - hmm this happens once: WARNING: segment _h4 has wrong count sums?  segment.max_doc=1087768 segment.sum_max_doc=1183259 segment.del_count_reclaimed=95341 line_number=31483; fix del_count_reclaimed from 95341 to 95491
#   - add delete rate too
#   - add the skew/mis-prediction of expected merge size vs actual
#   - tie together all segments flushed in a single full flush, e.g. see if app lacks enough flush-currency?
#   - count number of files in each segment
#   - detect indexing thread drift
#   - more accurately compute index dps, write amplification
#   - Plot reducer/wirter too
#   - display absolute time in time cursor agg details
#   - how to get merge times broken out by field
#   - sliding cursor showing segment count, which are alive, how many deletes, etc.
#   - how to reflect deletes
#   - get rid of redundant seg_name vs id in the segment rects
#   - track segment "depth" (how many ancesters)?
#   - how does CFS print during merge?  extract that too
#   - get flush source/reason in there -- _244 was bit delayed -- it was not in full flush, so why did it flush
#   - count/report CFS stats too
#   - test this on some more interesting infostreams
#   - hmm add click handler to jump back to merge input segments (_1gu -> _20r)
#   - handle multiple IW sessions
#   - capture nrtReader flushes/merges-on-nrt-reader too
#   - maybe produce an overall summary of indexing "behavior"?
#     - how many merge-on-commits finished in the window
#     - how many concurrent flushes/merges w/ time
#     - the bandwidth number
#     - deletes rate vs merge rate
#     - merge lopsidedness, range of number of segments merged, merge selection "efficiency"
#     - net write amplification
#     - update vs delete vs append rate
#   - pop out nice graphs showing stuff over time, linking out to lines in InfoStream
#     - full-flush and how long they took / how many segments / bytes
#     - write amplification
#     - ram -> segment size efficiency of flush
#   - am i pulling born deleted correctly for flushed segments?
#   - also report on how much HNSW concurrency is in use
#   - extract commit times too, nrt reader times
#     - and maybe attach flush reassons?  flush-on-ram, on-nrt, commit
#   - hmm how to view the "full" geneology of one segment -- it is always a "full" tree down to original roots (flushed segments)
#     - dot graph?
#   - enable simple x zoom
#   - can we indicate how unbalanced each merge is somehow?
#   - track "merge efficiency" (how unbalanced)
#   - can we track the usefulness of merging, how long it's "in service" before being merged away, vs its dawn (how long it took to produce)
#   - hmm maybe add an overlaid line chart showing badnwdith used from merging/flushingx
#   - get mouseover working for the gray merge lines too
#   - have deletes gradually make segment lighter and lighter?
#   - improved 2D bin packing?
#   - maybe make merge connection lines thicker the higher the "level" of the merge?
#   - include "reason" in segs -- forceMerge, nrtReopen, commit/flush
#   - get more interesting trace -- nrt?  deletions!
#   - make sure we can handle infoStream on an already built index, i.e. pre-existing segments
#     from before infoStream was turned on
#   - hmm can I be more cautious in assigning segments to levels?  like leave lowest levels
#     "intentionally" open for the big merged segments?
#     - or ... try to put segments close in level if they were created close in time?
#     - sort of a bin packing problem...
#   - derive y_pix_per_level from whole_height and max(level)
#   - zoom in/out, x and y separately
#   - NO
#     - maybe use D3?

# so pickle is happy
Segment = infostream_to_segments.Segment

def ts_to_js_date(timestamp):
  return f'new Date({timestamp.year}, {timestamp.month}, {timestamp.day}, {timestamp.hour}, {timestamp.minute}, {timestamp.second + timestamp.microsecond/1000000:.4f})'

'''
Opens a segments.pk previously written by parsing an IndexWriter InfoStream log using
infostream_to_segments.py, and writes a semi-interactive 2D rendering/timeline of the
segment flushing/merging/deletes using fabric.js / HTML5 canvas.
'''

USE_FABRIC = False
USE_SVG = True

def compute_time_metrics(checkpoints, commits, full_flush_events, segments, start_abs_time, end_abs_time):
  '''
  Computes aggregate metrics by time: MB/sec writing
  (flushing/merging), number of segments, number/%tg deletes, max_doc,
  concurrent flush count, concurrent merge count, total disk usage,
  indexing rate, write amplification, delete rate, add rate
  '''

  name_to_segment = {}

  # single list that mixes end and start times
  all_times = []
  for segment in segments:
    if segment.end_time is None or segment.size_mb is None:
      # InfoStream ends before merge finished
      continue
    all_times.append((segment.start_time, 'segstart', segment))

    for timestamp, event, line_number in segment.events:
      if event == 'light':
        all_times.append((timestamp, 'seglight', segment))
        if segment.del_count_reclaimed is not None:
          segment.del_reclaims_per_sec = segment.del_count_reclaimed / (timestamp - segment.start_time).total_seconds()
        break
    else:
      # 9:59:44
      print(f'WARNING: no seglight event for {segment.name}')

    if segment.end_time is None:
      end_time = end_abs_time
    else:
      end_time = segment.end_time
    all_times.append((end_time, 'segend', segment))
    name_to_segment[segment.name] = segment

  for checkpoint in checkpoints:
    all_times.append((checkpoint[0], 'checkpoint', checkpoint))

  for commit in commits:
    all_times.append((commit[0], 'commit', commit))

  for full_flush in full_flush_events:
    # can be None if InfoStream ended before last full flush finished:
    if full_flush[1] is not None:
      all_times.append((full_flush[1], 'fullflushend', full_flush))

  all_times.sort(key=lambda x: x[0])

  flush_mbs = 0
  merge_mbs = 0
  cur_max_doc = 0
  num_segments = 0
  tot_size_mb = 0
  flush_dps = 0
  merge_dps = 0
  cur_del_count = 0
  commit_size_mb = 0
  last_commit_size_mb = 0
  del_reclaims_per_sec = 0
  flush_thread_count = 0
  merge_thread_count = 0
  # segment names included in last commit
  last_commit_seg_names = set()
  l = []
  seg_name_to_size = {}

  last_full_flush_ord = -1

  cur_full_flush_time = all_times[0][0]
  cur_full_flush_ord = -1
  cur_full_flush_doc_count = 0

  time_aggs = []

  upto = 0
  index_file_count = 0
  last_full_flush_time = 0
  last_real_full_flush_time_sec = 0

  while upto < len(all_times):
  
    timestamp, what, item = all_times[upto]
    upto += 1

    if what == 'fullflushend':
      last_real_full_flush_time_sec = (item[1] - item[0]).total_seconds()

    if what in ('segstart', 'segend', 'seglight'):

      segment = item

      if segment.end_time is None:
        end_time = end_abs_time
      else:
        end_time = segment.end_time

      dur_sec = (end_time - segment.start_time).total_seconds()

      # MB/sec written by this segment
      mbs = segment.size_mb / dur_sec

      # docs/sec
      dps = segment.max_doc / dur_sec

      is_flush = type(segment.source) is str and segment.source.startswith('flush')

      print(f'seg {segment.name} source={segment.source} {is_flush=}')

      if is_flush:

        # aggregate doc count of all flushed segments within this single full flush
        if what == 'segstart':
          if segment.full_flush_ord != cur_full_flush_ord:
            print(f'ord now {segment.full_flush_ord}')
            if cur_full_flush_ord != -1:
              flush_dps = cur_full_flush_doc_count / (timestamp - last_full_flush_time).total_seconds()
              print(f'  dps now {flush_dps} gap={(timestamp - last_full_flush_time).total_seconds()}')
            last_full_flush_time = cur_full_flush_time
            cur_full_flush_doc_count = 0
            cur_full_flush_ord = segment.full_flush_ord
            cur_full_flush_time = timestamp
        
          # print(f'add max_doc={segment.max_doc} line_number={segment.start_infostream_line_number}')
          cur_full_flush_doc_count += segment.max_doc
          
        # newly written segment (from just indexed docs)
        if what == 'segstart':
          flush_mbs += mbs
          flush_thread_count += 1
          seg_name_to_size[segment.name] = segment.size_mb
        elif what == 'segend':
          tot_size_mb -= segment.size_mb
        elif what == 'seglight':
          flush_thread_count -= 1
          flush_mbs -= mbs
          tot_size_mb += segment.size_mb
      else:
        if what == 'segstart':
          merge_mbs += mbs
          merge_dps += dps
          del_reclaims_per_sec += segment.del_reclaims_per_sec
          merge_thread_count += 1
          seg_name_to_size[segment.name] = segment.size_mb
        elif what == 'segend':
          tot_size_mb -= segment.size_mb
        elif what == 'seglight':
          tot_size_mb += segment.size_mb
          merge_thread_count -= 1
          merge_mbs -= mbs
          merge_dps -= dps
          del_reclaims_per_sec -= segment.del_reclaims_per_sec

      if cur_max_doc == 0:
        # sidestep delete-by-zero ha
        del_pct = 0
      else:
        del_pct = 100.*cur_del_count/cur_max_doc
        
    elif what == 'checkpoint':
      # this is IW's internal checkpoint, much more frequent than external commit
      
      # a point-in-time checkpoint
      sum_max_doc = 0
      sum_del_count = 0
      
      for seg_tuple in item[2:]:
        # each seg_tuple is (segment_name, source, is_cfs, max_doc, del_count, del_gen, diagnostics, attributes))
        seg_name = seg_tuple[0]
        sum_max_doc += seg_tuple[3]
        sum_del_count += seg_tuple[4]

      cur_max_doc = sum_max_doc
      cur_del_count = sum_del_count
      num_segments = len(item) - 2

      if cur_max_doc == 0:
        # sidestep delete-by-zero ha
        del_pct = 0
      else:
        del_pct = 100.*cur_del_count/cur_max_doc
    elif what == 'commit':
      # this is external (user-called) commit
      index_file_count = item[2]
      commit_seg_names = set()
      commit_size_mb = 0
      for seg_details in item[3:]:
        seg_name = seg_details[0]
        commit_seg_names.add(seg_name)
        if seg_name not in last_commit_seg_names:
          commit_size_mb += seg_name_to_size[seg_name]
      last_commit_seg_names = commit_seg_names
      last_commit_size_mb = commit_size_mb

    # skip/coalesce multiple entries at precisely the same time, so merge commit (which appears as N segend
    # followed by seglight of the new merged segment at the same timestamp
    if upto >= len(all_times)-1 or timestamp != all_times[upto][0]:

      print(f'now do keep {infostream_to_segments.sec_to_time_delta((timestamp-start_abs_time).total_seconds())}: {flush_thread_count=} {segment.name=} {what}')

      # print(f'{upto=} {timestamp=} {what=} new Date({timestamp.year}, {timestamp.month}, {timestamp.day}, {timestamp.hour}, {timestamp.minute}, {timestamp.second + timestamp.microsecond/1000000:.4f})')

      l.append(f'[{ts_to_js_date(timestamp)}, {num_segments}, {tot_size_mb/1024:.3f}, {merge_mbs:.3f}, {flush_mbs:.3f}, {merge_mbs+flush_mbs:.3f}, {cur_max_doc/1000000.}, {del_pct:.3f}, {merge_thread_count}, {flush_thread_count}, {del_reclaims_per_sec/100.:.3f}, {flush_dps/100:.3f}, {merge_dps/100:.3f}, {commit_size_mb/100.:.3f}, {index_file_count/100:.2f}, {last_real_full_flush_time_sec:.2f}], // {what=}')

      time_aggs.append((timestamp, num_segments, tot_size_mb, merge_mbs, flush_mbs, merge_mbs+flush_mbs, cur_max_doc, del_pct, merge_thread_count, flush_thread_count, del_reclaims_per_sec, flush_dps, merge_dps, last_commit_size_mb))
    
      # so we only output one spiky point when the commit happened
      commit_size_mb = 0
    else:
      print(f'now do skip {infostream_to_segments.sec_to_time_delta((timestamp-start_abs_time).total_seconds())}: {flush_thread_count=} {segment.name=} {what}')

  # dygraph
  #   - https://dygraphs.com/2.2.1/dist/dygraph.min.js
  #   - https://dygraphs.com/2.2.1/dist/dygraph.css
  with open('segmetrics.html', 'w') as f:
    f.write(
    '''
<html><head>
<link rel="stylesheet" href="dygraph.css"></link>
<style>
#graphdiv .dygraph-legend > span.highlight {
  border: 2px solid black;
  font-weight: bold;
  font-size: 24;
  width: 500px;
}

#graphdiv .dygraph-legend > span {
  font-weight: bold;
  font-size: 24;
  width: 500px;
}
</style>
<script type="text/javascript" src="dygraph.min.js"></script>
</head>
</body>
  <div id="graphdiv" style="height: 90%; width: 90%"></div>
<script type="text/javascript">
    
Dygraph.onDOMready(function onDOMready() {
  g = new Dygraph(

    // containing div
    document.getElementById("graphdiv"),
    [
    ''')

    f.write('\n'.join(l))

    f.write('''
      ],

      {labels: ["Time", "Segment count", "Size (GB)", "Merge (MB/s)", "Flush (MB/s)", "Tot (MB/s)", "Max Doc (M)", "Del %", "Merging Threads", "Flushing Threads", "Del reclaim rate (C/sec)", "Indexing rate (C docs/sec)", "Merging rate (C docs/sec)", "Commit delta (CMB)", "File count (C)", "Full flush time (sec)"],
       highlightCircleSize: 2,
       strokeWidth: 1,
''')

    # zoom to first 10 minute window
    min_timestamp = all_times[0][0]
    f.write(f'dateWindow: [{ts_to_js_date(min_timestamp)}, {ts_to_js_date(min_timestamp + datetime.timedelta(seconds=600))}],')

    f.write('''
       strokeBorderWidth: 1,
       showRangeSelector: true,
       labelsSeparateLines: true,
       highlightSeriesOpts: {
            strokeWidth: 3,
            highlightCircleSize: 5,
            strokeBorderWidth: 2,
            highlightSeriesBackgroundAlpha: 0.5,
       }
    }
  );
});
</script>
</body>
</html>
''')

  return time_aggs
    
def main():

  if len(sys.argv) != 3:
    raise RuntimeError(f'usage: {sys.executable} -u segments_to_html.py segments.pk out.html')

  segments_file_in = sys.argv[1]
  html_file_out = sys.argv[2]
  if os.path.exists(html_file_out):
    raise RuntimeError(f'please remove {html_file_out} first')

  start_abs_time, end_abs_time, segments, full_flush_events, merge_during_commit_events, checkpoints, commits = pickle.load(open(segments_file_in, 'rb'))
  print(f'{len(segments)} segments spanning {(segments[-1].start_time-segments[0].start_time).total_seconds()} seconds')

  time_aggs = compute_time_metrics(checkpoints, commits, full_flush_events, segments, start_abs_time, end_abs_time)
    
  _l = []
  w = _l.append

  padding_x = 5
  padding_y = 2

  # x_pixels_per_sec = (whole_width - 2*padding_x) / (end_abs_time - start_abs_time).total_seconds()
  x_pixels_per_sec = 3.0
  print(f'{x_pixels_per_sec=}')

  # whole_width = 100 * 1024
  whole_width = int(x_pixels_per_sec * (end_abs_time - start_abs_time).total_seconds() + 2*padding_x)
  whole_height = 1800

  gv = graphviz.Digraph(comment=f'Segments from {segments_file_in}', graph_attr={'rankdir': 'LR'})

  w('<html style="height:100%">')
  w('<head>')
  # w('<script src="https://cdn.jsdelivr.net/npm/fabric@latest/dist/index.min.js"></script>')
  w('</head>')
  w('<body style="height:95%; padding:0; margin:5">')
  w('<div sytle="position: relative;">')
  w('<form>')
  w('<div style="position: absolute; y: 0; display: flex; justify-content: flex-end; z-index:11">')
  w('Zoom:&nbsp;<input type="range" id="zoomSlider" min="0.5" max="3" value="1" step=".01" style="width:300px;"/>')
  w('</div>')
  w('</form>')
  w('')
  w(f'<div id="details" style="position:absolute;left: 5;top: 5; z-index:10; background: rgba(255, 255, 255, 0.85); font-size: 18; font-weight: bold;"></div>')
  w('<div id="details2" style="position:absolute; right:0; display: flex; justify-content: flex-end; z-index:10; background: rgba(255, 255, 255, 0.85); font-size: 18; font-weight: bold;"></div>')
  
  w(f'<div id="divit" align=left style="position:absolute; left:5; top:5; overflow:scroll;height:100%;width:100%">')

  w(f'<svg preserveAspectRatio="none" id="it" viewBox="0 0 {whole_width+400} {whole_height+100}" width="{whole_width}" height="{whole_height}" xmlns="http://www.w3.org/2000/svg" style="height:100%">')
  # w(f'<svg id="it" viewBox="0 0 {whole_width+400} {whole_height + 100}" width={whole_width} height={whole_height} xmlns="http://www.w3.org/2000/svg" style="height:100%">')
  # w(f'<svg id="it" width={whole_width} height={whole_height} xmlns="http://www.w3.org/2000/svg" style="height:100%">')
  # w(f'<svg id="it" width={whole_width} height={whole_height} xmlns="http://www.w3.org/2000/svg" style="height:100%">')

  #w('var textbox = null;')
  #w('// so we can intersect mouse point with boxes:')
  #w(f'var ps = new PlanarSet();')

  print(f'{(end_abs_time - start_abs_time).total_seconds()} total seconds')
  y_pixels_per_log_mb = 4

  segment_name_to_level = {}

  segment_name_to_segment = {}

  segment_name_to_gv_node = {}

  max_level = 0

  min_start_abs_time = None

  # for making histograms/scatter plots
  ages_sizes = []

  for segment in segments:

    if segment.end_time is None:
      end_time = end_abs_time
    else:
      end_time = segment.end_time

    light_timestamp = None
    
    for timestamp, event, line_number in segment.events:
      if event == 'light':
        light_timestamp = timestamp
        break
    if segment.size_mb is not None and light_timestamp is not None:  
      ages_sizes.append((segment.source, (end_time - segment.start_time).total_seconds(), segment.size_mb, (light_timestamp - segment.start_time).total_seconds(), segment.max_doc))
    
    assert segment.name not in segment_name_to_segment, f'segment name {segment.name} appears twice?'
    segment_name_to_segment[segment.name] = segment
    label = f'{segment.name}: docs={segment.max_doc:,}'
    if segment.size_mb is not None:
      label += f' mb={segment.size_mb:,.1f}'
    segment_name_to_gv_node[segment.name] = gv.node(segment.name, label=label)

    if type(segment.source) is str and segment.source.startswith('flush'):
      pass
    else:
      for merged_seg_name in segment.source[1]:
        merged_seg = segment_name_to_segment[merged_seg_name]
        if merged_seg.del_count_merged_away is not None:
          label = f'{merged_seg.del_count_merged_away:,} dels'
          weight = merged_seg.del_count_merged_away
        else:
          label = ''
          weight = 0
        gv.edge(merged_seg_name, segment.name, label=label, weight=str(weight))
  with open('ages_sizes.txt', 'w') as f:
    for source, age_sec, size_mb, write_time, max_doc in ages_sizes:
      if size_mb is not None:
        f.write(f'{size_mb:.1f}\t{max_doc}\n')

  if False:
    print(f'gv:\n{gv.source}')
    gv_file_name_out = gv.render('segments', format='svg')
    print(f"now render gv: {gv_file_name_out}")
    print(f'size={os.path.getsize("segments.svg")}')

  # first pass to assign free level to each segment.  segments come in
  # order of their birth:

  # this is actually a fun 2D bin-packing problem (assigning segments to levels)! ... I've only tried
  # these two simple approaches so far:

  if True:
    # sort segments by lifetime, and assign to level bottom up, so long lived segments are always down low

    # so we have a bit of horizontal space b/w segments:
    pad_time_sec = 1.0

    half_pad_time = datetime.timedelta(seconds=pad_time_sec/2)

    tree = intervaltree.IntervalTree()

    seg_times = []
    for segment in segments:
      if segment.end_time is None:
        end_time = end_abs_time
      else:
        end_time = segment.end_time
      seg_times.append((end_time - segment.start_time, segment))
      
    for ignore, segment in sorted(seg_times, key=lambda x: -x[0]):

      if segment.end_time is None:
        end_time = end_abs_time
      else:
        end_time = segment.end_time

      # all levels that are in use overlapping this new segment:
      used_levels = [segment_name_to_level[interval.data.name] for interval in tree[segment.start_time:end_time]]

      new_level = 0
      while True:
        if new_level not in used_levels:
          break
        new_level += 1

      tree[segment.start_time-half_pad_time:end_time+half_pad_time] = segment

      segment_name_to_level[segment.name] = new_level
      # print(f'{segment.name} -> level {new_level}')

      max_level = max(new_level, max_level)

      if min_start_abs_time is None or segment.start_time < min_start_abs_time:
        min_start_abs_time = segment.start_time


  elif False:
    # sort segments by size, descending, and assign to level bottom up, so big segments are always down low

    tree = intervaltree.IntervalTree()

    for segment in sorted(segments, key=lambda segment: -segment.size_mb):

      assert segment.name not in segment_name_to_segment, f'segment name {segment.name} appears twice?'
      segment_name_to_segment[segment.name] = segment

      if segment.end_time is None:
        end_time = end_abs_time
      else:
        end_time = segment.end_time

      # all levels that are in use overlapping this new segment:
      used_levels = [segment_name_to_level[interval.data.name] for interval in tree[segment.start_time:end_time]]

      new_level = 0
      while True:
        if new_level not in used_levels:
          break
        new_level += 1

      tree[segment.start_time:end_time] = segment

      segment_name_to_level[segment.name] = new_level
      # print(f'{segment.name} -> level {new_level}')

      max_level = max(new_level, max_level)

      if min_start_abs_time is None or segment.start_time < min_start_abs_time:
        min_start_abs_time = segment.start_time

  else:

    # a simpler first-come first-serve level assignment

    # assign each new segment to a free level -- this is not how IndexWriter does it, but
    # it doesn't much matter what the order is.  e.g. TMP disregards segment order (treats
    # them all as a set of segments)
    level_to_live_segment = {}

    for segment in segments:

      assert segment.name not in segment_name_to_segment, f'segment name {segment.name} appears twice?'

      segment_name_to_segment[segment.name] = segment

      # prune expired segments -- must create list(...) so that we don't directly
      # iterate the items iterator of the dict while deleting from it:
      for level, segment2 in list(level_to_live_segment.items()):
        if segment2.end_time is not None and segment2.end_time < segment.start_time:
          del level_to_live_segment[level]

      # assign a level
      level = 0
      while True:
        if level in level_to_live_segment:
          level += 1
        else:
          break

      level_to_live_segment[level] = segment

      segment_name_to_level[segment.name] = level
      # print(f'{segment.name} -> level {level}')

      max_level = max(level, max_level)

      if min_start_abs_time is None or segment.start_time < min_start_abs_time:
        min_start_abs_time = segment.start_time

  # print(f'{max_level=}')
  y_pixels_per_level = whole_height / (1+max_level)

  for segment in segments:
    level = segment_name_to_level[segment.name]
    # print(f'{(segment.start_time - min_start_abs_time).total_seconds()}')

    light_timestamp = None
    
    for timestamp, event, line_number in segment.events:
      if event == 'light':
        light_timestamp = timestamp
        break

    x0 = padding_x + x_pixels_per_sec * (segment.start_time - min_start_abs_time).total_seconds()
    if segment.end_time is None:
      t1 = end_abs_time
    else:
      t1 = segment.end_time
    x1 = padding_x + x_pixels_per_sec * (t1 - min_start_abs_time).total_seconds()

    if segment.size_mb is None:
      # this likely means the merge was still in-flight when the InfoStream ended:
      size_mb = 100.0
      # raise RuntimeError(f'segment {segment.name} has size_mb=None')
    else:
      size_mb = segment.size_mb
    
    height = min(y_pixels_per_level - padding_y, y_pixels_per_log_mb * math.log(size_mb/2.))
    height = max(7, height)

    y1 = whole_height - int(y_pixels_per_level * level)
    y0 = y1 - height
    if type(segment.source) is str and segment.source.startswith('flush'):
      color = '#ff0000'
    else:
      color = '#0000ff'

    w(f'\n  <rect id="{segment.name}" x={x0:.2f} y={y0:.2f} width={x1-x0:.2f} height={height:.2f} rx=4 fill="{color}" seg_name="{segment.name}"/>')
    # w(f'\n  <rect id="{segment.name}" x={x0:.2f} y={y0:.2f} width={x1-x0:.2f} height={height:.2f} rx=4 fill="{color}"/>')

    # shade the time segment is being written, before it's lit:
    if light_timestamp is not None:
      x_light = padding_x + x_pixels_per_sec * (light_timestamp - min_start_abs_time).total_seconds()

      if color == '#ff0000':
        new_color = '#ff8888'
      else:
        new_color = '#8888ff'
      w(f'  <!--dawn:-->')
      w(f'  <rect x={x0:.2f} y={y0:.2f} width={x_light-x0:.2f} height={height:.2f} rx=4 fill="{new_color}" seg_name="{segment.name}"/>')

    if segment.merged_into is not None:
      # this segment was merged away eventually
      if color == '#ff0000':
        new_color = '#880000'
      else:
        new_color = '#000088'
      dusk_timestamp = segment.merged_into.start_time
      assert dusk_timestamp < t1
      x_dusk = padding_x + x_pixels_per_sec * (dusk_timestamp - min_start_abs_time).total_seconds()
      w(f'  <!--dusk:-->')
      w(f'  <rect x={x_dusk:.2f} y={y0:.2f} width={x1-x_dusk:.2f} height={height:.2f} rx=4 fill="{new_color}" seg_name="{segment.name}"/>')

      light_timestamp2 = None

      for timestamp2, event2, line_number2 in segment.merged_into.events:
        if event2 == 'light':
          light_timestamp2 = timestamp2
          break

      if light_timestamp2 is not None:
        merge_level = segment_name_to_level[segment.merged_into.name]

        merge_height = min(y_pixels_per_level - padding_y, y_pixels_per_log_mb * math.log(segment.merged_into.size_mb))
        merge_height = max(5, merge_height)

        y1a = whole_height - int(y_pixels_per_level * merge_level) - merge_height

        x_light_merge = padding_x + x_pixels_per_sec * (light_timestamp2 - min_start_abs_time).total_seconds()

        # draw line segment linking segment into its merged segment
        # w(f'  <line id="line{segment.name}{segment.merged_into.name}" x1="{x_dusk}" y1="{y0 + height/2}" x2="{x_light_merge}" y2="{y1a+merge_height/2}" stroke="darkgray" stroke-width="2" stroke-dasharray="20,20"/>')
        w(f'  <line id="line{segment.name}{segment.merged_into.name}" x1="{x_dusk}" y1="{y0 + height/2}" x2="{x_light_merge}" y2="{y1a+merge_height/2}" stroke="black" stroke-width="1"/>')

        if False:
          w('var l = document.createElementNS("http://www.w3.org/2000/svg", "line");')
          w(f'l.setAttribute("id", ");')
          w(f'l.setAttribute("x1", {x_dusk});')
          w(f'l.setAttribute("y1", {y0 + y_pixels_per_level/2});')
          w(f'l.setAttribute("x2", {x_light_merge});')
          w(f'l.setAttribute("y2", {y1a-y_pixels_per_level/2});')
          w(f'mysvg.appendChild(l);')

  w(f'\n\n  <!-- full flush events -->')
  for start_time, end_time in full_flush_events:
    if end_time is None:
      continue
    x0 = padding_x + x_pixels_per_sec * (start_time - min_start_abs_time).total_seconds()
    y0 = 0
    x1 = padding_x + x_pixels_per_sec * (end_time - min_start_abs_time).total_seconds()
    y1 = whole_height
    w(f'  <rect fill="cyan" fill-opacity=0.2 x={x0:.2f} y={y0:.2f} width={x1-x0:.2f} height="{y1-y0:.2f}"/>')

  w(f'\n\n  <!-- merge-on-commit events -->')
  for start_time, end_time in merge_during_commit_events:
    if end_time is None:
      continue
    x0 = padding_x + x_pixels_per_sec * (start_time - min_start_abs_time).total_seconds()
    y0 = 0
    x1 = padding_x + x_pixels_per_sec * (end_time - min_start_abs_time).total_seconds()
    y1 = whole_height
    w(f'  <rect fill="orange" fill-opacity=0.2 x={x0:.2f} y={y0:.2f} width={x1-x0:.2f} height="{y1-y0:.2f}"/>')

  #w('canvas.on("mouse:over", show_segment_details);')
  # w('canvas.on("mouse:out", function(e) {canvas.remove(textbox);  textbox = null;});')
  w('</svg>')
  w('</div>')
  w('<script>')
  w('var agg_metrics = [')
  for tup in time_aggs:
    timestamp, num_segments, tot_size_mb, merge_mbs, flush_mbs, tot_mbs, cur_max_doc, del_pct, merge_thread_count, flush_thread_count, del_reclaims_per_sec, flush_dps, merge_dps, last_commit_size_mb = tup

    sec = (timestamp - min_start_abs_time).total_seconds()
    ts = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
    w(f'  [{sec:.4f}, {num_segments}, {tot_size_mb:.3f}, {merge_mbs:.3f}, {flush_mbs:.3f}, {tot_mbs:.3f}, {cur_max_doc}, {del_pct:.3f}, {merge_thread_count}, {flush_thread_count}, {del_reclaims_per_sec:.3f}, {flush_dps:.3f}, {merge_dps:.3f}, {last_commit_size_mb:.3f}, "{ts}"],')
  w('];')
  w('const seg_merge_map = new Map();')
  # w('const seg_details_map = new Map();')
  w('const seg_details2_map = new Map();')
  w('\n// super verbose (includes InfoStream line numbers):')
  w('const seg_details3_map = new Map();')
  for segment in segments:
    if type(segment.source) is tuple and segment.source[0] == 'merge':
      w(f'seg_merge_map.set("{segment.name}", {segment.source[1]});')
    if segment.size_mb is None:
      smb = 'n/a'
    else:
      smb = f'{segment.size_mb:.1f} MB'
    details = f'{segment.name}:\n  {smb}\n  {segment.max_doc} max_doc'
    if segment.end_time is not None:
      end_time = segment.end_time
    else:
      end_time = end_abs_time
    details += f'\n  {(end_time - segment.start_time).total_seconds():.1f} sec'
    if segment.born_del_count is not None and segment.born_del_count > 0:
      details += f'\n  {100.*segment.born_del_count/segment.max_doc:.1f}% stillborn'
      
    # w(f'seg_details_map.set("{segment.name}", {repr(details)});')
    w(f'seg_details2_map.set("{segment.name}", {repr(segment.to_verbose_string(min_start_abs_time, end_abs_time, False))});')
    w(f'seg_details3_map.set("{segment.name}", {repr(segment.to_verbose_string(min_start_abs_time, end_abs_time, True))});')

  w('''
  const top_details = document.getElementById('details');
  const top_details2 = document.getElementById('details2');
  slider = document.getElementById('zoomSlider');
  svg = document.getElementById('it');

  slider.addEventListener('input', function() {
    zoom = slider.value;
  ''')
  w('    console.log("zoom=" + zoom);')
  #w(f'    var new_view_box = "0 0 " + ({whole_width+400}/zoom) + " " + ({whole_height+100}/zoom);')
  w(f'    var new_view_box = "0 0 " + ({whole_width+400}/zoom) + " " + ({whole_height+100}/zoom);')
  # svg.style.transform = `scale(${zoom})`;
  w('    svg.setAttribute("viewBox", new_view_box);')
  w(f'    svg.setAttribute("width", {whole_width}*zoom);')
  w(f'    svg.setAttribute("height", {whole_height}*zoom);')
  w('  });')
  w('''
  // parse possible scroll x/y point (bookmarked exact location)
  var hash = window.location.hash;

  if (hash != null && hash.startsWith("#pos=")) {
    var arr = hash.substring(5).split(',');
    var xpos = parseInt(arr[0]);
    var ypos = parseInt(arr[1]);
  
    // first, scroll so the requested pixel is center of window:
    mydiv.scrollLeft = Math.floor(xpos - mydiv.offsetWidth/2);

    // second, simulate mouse move over that position:
    var evt = {};
    evt.is_from_url_anchor = true;
    evt.clientX = xpos - mydiv.scrollLeft;
    evt.clientY = ypos;
    evt.ctrlKey = false;
    mysvg.onmousemove(evt);
   }
''')
  
  w('</script>')
  w('''
<script>

  function binarySearchWithInsertionPoint(arr, target) {
    let low = 0;
    let high = arr.length - 1;

    while (low <= high) {
      const mid = Math.floor((low + high) / 2);

      if (arr[mid][0] === target) {
        return mid;
      } else if (arr[mid][0] < target) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return low;
  }

  var highlighting = new Map();
  const svgns = "http://www.w3.org/2000/svg";
  function highlight(seg_name, color, do_seg_details, do_seg_super_verbose, transformed_point) {
    if (!highlighting.has(seg_name)) {

      // get the full segment (not dawn/dusk):
      var rect2 = document.getElementById(seg_name);

      var rect3 = document.createElementNS(svgns, "rect");

      // draw a new rect to "highlight"
      highlighting.set(seg_name, rect3);
      rect3.selectable = false;

      rect3.setAttribute("x", rect2.x.baseVal.value);
      rect3.setAttribute("y", rect2.y.baseVal.value);
      rect3.setAttribute("width", rect2.width.baseVal.value);
      rect3.setAttribute("height", rect2.height.baseVal.value);
      rect3.setAttribute("fill", color);
      mysvg.appendChild(rect3);

      if (do_seg_details) {
        /*
        var text = document.createElementNS(svgns, "text");

        text.setAttribute("font-family", "Verdana");
        text.setAttribute("font-size", "32");
        text.setAttribute("font-weight", "bold");
        text.setAttribute("font-color", "lime");

        highlighting.set(seg_name + ":t", text);
        text.selectable = false;
        text.style.fill = "lime";
        text.textContent = seg_details_map.get(seg_name);
        text.setAttribute("x", transformed_point.x);
        text.setAttribute("y", transformed_point.y);
        text.setAttribute("width", "200");
        text.setAttribute("height", "200");
        mysvg.appendChild(text);
        */
        if (do_seg_super_verbose) {
          top_details.innerHTML = "<pre>" + seg_details3_map.get(seg_name) + "</pre>";
        } else {
          top_details.innerHTML = "<pre>" + seg_details2_map.get(seg_name) + "</pre>";
        }
      }
    }
  }

  var x_cursor = null;

  mysvg = document.getElementById("it");
  mydiv = document.getElementById("divit");

  mysvg.onmousedown = function(evt) {
    var text = document.location.href.split('#')[0] + "#pos=" + (mydiv.scrollLeft + evt.clientX) + "," + evt.clientY;
    navigator.clipboard.writeText(text).then(function() {
      alert('Link copied! ' + text)
    }, function(err) {
      alert('Link copied failed :(')
    });
  }

  var first_mouse_move = true;
  
  mysvg.onmousemove = function(evt) {
    if (first_mouse_move && evt.is_from_url_anchor == null) {
      first_mouse_move = false;
      return;
    }
    // console.log("clientX+scrollLeft=" + (evt.clientX + mydiv.scrollLeft) + " clientY=" + evt.clientY);
    // r = mysvg.getBoundingClientRect();
    r = mysvg.getBBox();
    //console.log("bbox is " + r);
    point = mysvg.createSVGPoint();
    // screen point within mysvg:
    // console.log("sub " + r.left + " " + r.top);
    //point.x = evt.clientX - r.left;
    //point.y = evt.clientY - r.top;
    point.x = evt.clientX;
    point.y = evt.clientY;
    transformed_point = point.matrixTransform(mysvg.getScreenCTM().inverse());
  ''')

  w(f'      var t = (transformed_point.x - {padding_x}) / {x_pixels_per_sec};')
  w('''

    var spot = binarySearchWithInsertionPoint(agg_metrics, t);
    // console.log(agg_metrics[spot]);
    let tup = agg_metrics[spot];
    let timestamp = tup[0];
    let num_segments = tup[1];
    let tot_size_mb = tup[2];
    let merge_mbs = tup[3];
    let flush_mbs = tup[4];
    let tot_mbs = tup[5];
    let max_doc = tup[6];
    let del_pct = tup[7];
    let merge_thread_count = tup[8];
    let flush_thread_count = tup[9];
    let del_reclaims_per_sec = tup[10];
    let flush_dps = tup[11];
    let merge_dps = tup[12];
    let last_commit_size_mb = tup[13];
    let abs_timestamp = tup[14];
    let hr = Math.floor(t/3600.)
    let mn = Math.floor((t - hr*3600)/60.)
    let sc = t - hr*3600 - mn*60;
    var scs;
    if (sc < 10.0) {
      scs = "0" + sc.toFixed(2);
    } else {
      scs = sc.toFixed(2);
    }
    let text = "<pre>" + String(hr).padStart(2, '0') + ":" + String(mn).padStart(2, '0') + ":" + scs + "<br>" +
           abs_timestamp + "<br>" +
           num_segments + " segments<br>" +
           (tot_size_mb/1024.).toFixed(2) + " GB<br>" +
           (max_doc/1000000.).toFixed(2) + "M docs (" + del_pct.toFixed(2) + "% deletes)<br>" +
           "IO tot writes: " + tot_mbs.toFixed(2) + " MB/sec<br>" +
           "Last commit: " + (last_commit_size_mb / 1024.).toFixed(2) + " GB<br>" +
           "flushes: " + flush_thread_count + " threads<br>        " + flush_mbs.toFixed(1) + " MB/sec<br>        " + flush_dps.toFixed(1) + " docs/sec<br>" +
           "merges: " + merge_thread_count + " threads<br>        " + merge_mbs.toFixed(1) + " MB/sec<br>       " + merge_dps.toFixed(1) + " docs/sec<br>       " + del_reclaims_per_sec.toFixed(1) + " del-reclaim/sec<br>" +
           "</pre>";

    top_details2.innerHTML = text;
  
    // console.log("  SVG X:", transformed_point.x, "SVG Y:", transformed_point.y);
    var now_highlight = new Set();
    var now_sub_highlight = new Set();
    var merge_seg_name = null;
    if (x_cursor == null) {
      x_cursor = document.createElementNS(svgns, "line");
      x_cursor.setAttribute("stroke", "black");
      x_cursor.setAttribute("stroke-width", "1");
      mysvg.appendChild(x_cursor);
    }
    x_cursor.setAttribute("x1", transformed_point.x);
    x_cursor.setAttribute("y1", 0);
    x_cursor.setAttribute("x2", transformed_point.x);
  ''')
  w(f'      x_cursor.setAttribute("y2", {whole_height});')
  w('''

    document.elementsFromPoint(evt.clientX, evt.clientY).forEach(function(element, index, array) {
      if (element instanceof SVGRectElement) {
        var seg_name = element.getAttribute("seg_name");
        // var seg_name = element.id;
        // must null-check because we will also select our newly added highlight rects!  weird recursion...
        if (seg_name != null) {
          now_highlight.add(seg_name);
          if (seg_merge_map.has(seg_name)) {
            // the hilited segment is a merge segment
            merge_seg_name = seg_name;
            //console.log("  --> " + seg_merge_map.get(seg_name));
            // hrmph no .addAll for Set?
            // now_sub_highlight.add(...seg_merge_map.get(seg_name));
            for (let sub_seg_name of seg_merge_map.get(seg_name)) {
              now_sub_highlight.add(sub_seg_name);
            }
            // console.log("  sub: " + JSON.stringify(now_sub_highlight, null, 2));
          }
        }
      }
    });

    // it is safe to delete from map as long as I use this "let .. of" loop!?
    for (let [name, rect] of highlighting) {
      var seg_name;
      if (name.endsWith(":t")) {
        seg_name = name.substring(0, name.length - 2);
      } else {
        seg_name = name;
      }
      if (!now_highlight.has(seg_name) && !now_sub_highlight.has(seg_name)) {
        //console.log("unhighlight " + name);
        mysvg.removeChild(rect);
        highlighting.delete(name);
      }
    }
    for (let seg_name of now_highlight) {
      //console.log("highlight " + seg_name);
      var do_seg_details;
      if (merge_seg_name != null) {
        do_seg_details = seg_name == merge_seg_name;
      } else {
        do_seg_details = true;
      }

      highlight(seg_name, "cyan", do_seg_details, evt.ctrlKey, transformed_point);
    }
    // TODO: just use map w/ colors?
    for (let seg_name of now_sub_highlight) {
      // console.log("sub highlight " + seg_name);
      highlight(seg_name, "orange", false, false, transformed_point);
    }
  };
</script>''')

  w('</div>')
  w('</body>')
  w('</html>')

  with open(html_file_out, 'w') as f:
    f.write('\n'.join(_l))

if __name__ == '__main__':
  main()
