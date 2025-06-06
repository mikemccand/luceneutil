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

import datetime
import math
import os
import pickle
import sys

import infostream_to_segments

infostream_to_segments.sec_to_time_delta

# pip3 install intervaltree
# pip3 install graphviz
import graphviz
import intervaltree

# TODO
#   - can we somehow do better job conveying "new delete rate" vs "merge reclaim rate"?
#     - add "new delete rate" to dygraph / top right text box
#   - add max ancestor to aggs chart and RHS text box
#   - add "new deletes during merging" onto merging segments?  opposite of born-del-count "new dels during dawn" and "new dels during dusk"
#   - dot
#     - color red/blue also
#   - tell me how many deletes for a seg as I mouse over it
#   - draw merge lines in 2nd pass -- now some are occluded by some segments and some are not
#   - fix IW logging to say how many MOC merges kicked off / how many timed out / how many finished
#   - MOC: should be able to tell if it timed out on any merge vs all finished in time
#   - also coalesce mocs into their "ords", like full flushes
#   - tune to more harmonius color scheme
#   - from "last commit size" also break out how much was flushed vs merged segments
#   - IW should differentiate "done waithing for merge during commit" timeout vs all requested merges finished
#   - fix zoom slider to only zoom in x
#   - flush-by-ram detection is buggy
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

merge_on_commit_color = "rgb(253,235,208)"
merge_on_commit_highlight_color = "rgb(202,188,166)"
full_flush_color = "rgb(214,234,248)"
full_flush_highlight_color = "rgb(171,187,198)"

flush_by_ram_color = "#ff00ff"
flush_color = "#ff0000"
merge_color = "#0000ff"

flush_by_ram_dawn_color = "#ff88ff"
flush_dawn_color = "#ff8888"
merge_dawn_color = "#8888ff"

flush_by_ram_dusk_color = "#880088"
flush_dusk_color = "#880000"
merge_dusk_color = "#000088"

merging_segment_highlight_color = "orange"
segment_highlight_color = "cyan"

# so pickle is happy
Segment = infostream_to_segments.Segment


def ts_to_js_date(timestamp):
  return f"new Date({timestamp.year}, {timestamp.month}, {timestamp.day}, {timestamp.hour}, {timestamp.minute}, {timestamp.second + timestamp.microsecond / 1000000:.4f})"


"""
Opens a segments.pk previously written by parsing an IndexWriter InfoStream log using
infostream_to_segments.py, and writes a semi-interactive 2D rendering/timeline of the
segment flushing/merging/deletes using fabric.js / HTML5 canvas.
"""

USE_FABRIC = False
USE_SVG = True


def add_ts(by_time, timestamp, seg_name, del_count, max_doc):
  # print(f"add_ts: {timestamp_secs=} {seg_name=} {del_count=}")
  if timestamp not in by_time:
    by_time[timestamp] = {}
  by_time[timestamp][seg_name] = f"{del_count}/{max_doc}"


def get_seg_del_times(segments):
  by_time = {}
  by_name = {}
  for segment in segments:
    by_name[segment.name] = segment
    del_count = 0
    if segment.max_doc is None:
      continue
    for timestamp, event, line_number in segment.events:
      if False and event == "light" and segment.born_del_count is not None and segment.born_del_count > 0:
        del_count = segment.born_del_count
        add_ts(by_time, timestamp, segment.name, segment.born_del_count, segment.max_doc)

      if event.startswith("del "):
        del_inc = int(event[4 : event.find(" ", 4)].replace(",", ""))
        del_count += del_inc
        add_ts(by_time, timestamp, segment.name, del_count, segment.max_doc)
    if segment.end_time is not None:
      # eol marker
      add_ts(by_time, segment.end_time, segment.name, -1, -1)

  l = sorted(by_time.items())
  for i in range(1, len(l)):
    cur = l[i][1]
    for seg_name, dels in l[i - 1][1].items():
      # carry over prior time's deletions if this segment had no new ones now... this
      # way at GUI-time we can just lookup one spot in this array and know the del count
      # of all segments:
      end_time = by_name[seg_name].end_time
      if seg_name not in cur:
        cur[seg_name] = dels
    for seg_name, dels in list(cur.items()):
      if dels == "-1/-1":
        # eol marker
        del cur[seg_name]

  if False:
    for i in range(len(l)):
      k, v = l[i]
      l2 = sorted(v.items(), key=lambda x: -x[1])
      l[i] = (k, l2)

  # print(json.dumps(l, indent=2))
  return l


def compute_time_metrics(checkpoints, commits, full_flush_events, segments, start_abs_time, end_abs_time):
  """Computes aggregate metrics by time: MB/sec writing
  (flushing/merging), number of segments, number/%tg deletes, max_doc,
  concurrent flush count, concurrent merge count, total disk usage,
  indexing rate, write amplification, delete rate, add rate
  """
  name_to_segment = {}

  # single list that mixes end and start times
  all_times = []
  for segment in segments:
    if segment.end_time is None or segment.size_mb is None:
      # InfoStream ends before merge finished
      continue
    all_times.append((segment.start_time, "segstart", segment))

    for timestamp, event, line_number in segment.events:
      if event == "light":
        all_times.append((timestamp, "seglight", segment))
        break
    else:
      print(
        f"WARNING: no light event for {segment.source} segment {segment.name}; likely because they were still flushing when InfoStream ends: time from end is {(end_abs_time - segment.start_time).total_seconds():.1f} s"
      )

    if segment.end_time is None:
      end_time = end_abs_time
    else:
      end_time = segment.end_time
    all_times.append((end_time, "segend", segment))
    name_to_segment[segment.name] = segment

  for checkpoint in checkpoints:
    all_times.append((checkpoint[0], "checkpoint", checkpoint))

  for commit in commits:
    all_times.append((commit[0], "commit", commit))

  for full_flush in full_flush_events:
    # can be None if InfoStream ended before last full flush finished:
    if full_flush[1] is not None:
      all_times.append((full_flush[1], "fullflushend", full_flush))

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
  del_creates_per_sec = 0
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

  # incremented at end of writing of each segment
  tot_flush_write_mb = 0
  tot_merge_write_mb = 0

  # weighted (by size_mb) sum of each segment's write amplification
  tot_weighted_write_ampl = 0

  while upto < len(all_times):
    timestamp, what, item = all_times[upto]
    upto += 1

    if what == "fullflushend":
      last_real_full_flush_time_sec = (item[1] - item[0]).total_seconds()

    if what in ("segstart", "segend", "seglight"):
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

      is_flush = type(segment.source) is str and segment.source.startswith("flush")

      # print(f'seg {segment.name} source={segment.source} {is_flush=}')

      if is_flush:
        # aggregate doc count of all flushed segments within this single full flush
        if what == "segstart":
          if segment.full_flush_ord != cur_full_flush_ord:
            print(f"ord now {segment.full_flush_ord}")
            if cur_full_flush_ord != -1:
              flush_dps = cur_full_flush_doc_count / (timestamp - last_full_flush_time).total_seconds()
              print(f"  dps now {flush_dps} gap={(timestamp - last_full_flush_time).total_seconds()}")
            last_full_flush_time = cur_full_flush_time
            cur_full_flush_doc_count = 0
            cur_full_flush_ord = segment.full_flush_ord
            cur_full_flush_time = timestamp

          # print(f'add max_doc={segment.max_doc} line_number={segment.start_infostream_line_number}')
          cur_full_flush_doc_count += segment.max_doc

        # newly written segment (from just indexed docs)
        if what == "segstart":
          flush_mbs += mbs
          flush_thread_count += 1
          seg_name_to_size[segment.name] = segment.size_mb
        elif what == "segend":
          tot_size_mb -= segment.size_mb
          tot_weighted_write_ampl -= segment.size_mb * segment.net_write_amplification
        elif what == "seglight":
          flush_thread_count -= 1
          flush_mbs -= mbs
          tot_size_mb += segment.size_mb
          tot_flush_write_mb += segment.size_mb
          tot_weighted_write_ampl += segment.size_mb * segment.net_write_amplification
      elif what == "segstart":
        merge_mbs += mbs
        merge_dps += dps
        if segment.del_reclaims_per_sec is not None:
          del_reclaims_per_sec += segment.del_reclaims_per_sec
        if segment.del_creates_per_sec is not None:
          del_creates_per_sec += segment.del_creates_per_sec
        merge_thread_count += 1
        seg_name_to_size[segment.name] = segment.size_mb
      elif what == "segend":
        tot_size_mb -= segment.size_mb
        tot_weighted_write_ampl -= segment.size_mb * segment.net_write_amplification
        if segment.del_creates_per_sec is not None:
          del_creates_per_sec -= segment.del_creates_per_sec
      elif what == "seglight":
        tot_size_mb += segment.size_mb
        tot_merge_write_mb += segment.size_mb
        merge_thread_count -= 1
        merge_mbs -= mbs
        merge_dps -= dps
        if segment.del_reclaims_per_sec is not None:
          del_reclaims_per_sec -= segment.del_reclaims_per_sec
        tot_weighted_write_ampl += segment.size_mb * segment.net_write_amplification

      if cur_max_doc == 0:
        # sidestep delete-by-zero ha
        del_pct = 0
      else:
        del_pct = 100.0 * cur_del_count / cur_max_doc

    elif what == "checkpoint":
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
        del_pct = 100.0 * cur_del_count / cur_max_doc
    elif what == "commit":
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
    if upto >= len(all_times) - 1 or timestamp != all_times[upto][0]:
      # print(f'now do keep {infostream_to_segments.sec_to_time_delta((timestamp-start_abs_time).total_seconds())}: {flush_thread_count=} {segment.name=} {what}')

      # print(f'{upto=} {timestamp=} {what=} new Date({timestamp.year}, {timestamp.month}, {timestamp.day}, {timestamp.hour}, {timestamp.minute}, {timestamp.second + timestamp.microsecond/1000000:.4f})')

      l.append(
        f"[{ts_to_js_date(timestamp)}, {num_segments}, {tot_size_mb / 1024:.3f}, {merge_mbs:.3f}, {flush_mbs:.3f}, {merge_mbs + flush_mbs:.3f}, {cur_max_doc / 1000000.0}, {del_pct:.3f}, {merge_thread_count}, {flush_thread_count}, {del_creates_per_sec / 100.0:.3f}, {del_reclaims_per_sec / 100.0:.3f}, {flush_dps / 100:.3f}, {merge_dps / 100:.3f}, {commit_size_mb / 100.0:.3f}, {index_file_count / 100:.2f}, {last_real_full_flush_time_sec:.2f}], // {what=}"
      )

      if tot_size_mb == 0:
        write_ampl = 0
      else:
        write_ampl = tot_weighted_write_ampl / tot_size_mb

      time_aggs.append(
        (
          timestamp,
          num_segments,
          tot_size_mb,
          merge_mbs,
          flush_mbs,
          merge_mbs + flush_mbs,
          cur_max_doc,
          cur_del_count,
          del_pct,
          merge_thread_count,
          flush_thread_count,
          del_creates_per_sec,
          del_reclaims_per_sec,
          flush_dps,
          merge_dps,
          last_commit_size_mb,
          tot_merge_write_mb,
          tot_flush_write_mb,
          write_ampl,
        )
      )

      # so we only output one spiky point when the commit happened
      commit_size_mb = 0
    else:
      # print(f'now do skip {infostream_to_segments.sec_to_time_delta((timestamp-start_abs_time).total_seconds())}: {flush_thread_count=} {segment.name=} {what}')
      pass

  # dygraph
  #   - https://dygraphs.com/2.2.1/dist/dygraph.min.js
  #   - https://dygraphs.com/2.2.1/dist/dygraph.css
  with open("segmetrics.html", "w") as f:
    f.write(
      """
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
    """
    )

    f.write("\n".join(l))

    f.write("""
      ],

      {labels: ["Time", "Segment count", "Size (GB)", "Merge (MB/s)", "Flush (MB/s)", "Tot (MB/s)", "Maxdoc (M)", "Del %", "Merge threads", "Flush threads", "Del create (C/s)", "Del reclaim (C/s)", "Index rate (C/s)", "Merge rate (C/s)", "Commit delta (CMB)", "File count (C)", "Full flush (s)"],
       highlightCircleSize: 2,
       strokeWidth: 1,
""")

    # zoom to first 10 minute window
    min_timestamp = all_times[0][0]
    f.write(f"dateWindow: [{ts_to_js_date(min_timestamp)}, {ts_to_js_date(min_timestamp + datetime.timedelta(seconds=600))}],")

    f.write("""
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
""")

  return time_aggs


def main():
  if len(sys.argv) != 3:
    raise RuntimeError(f"usage: {sys.executable} -u segments_to_html.py segments.pk out.html")

  segments_file_in = sys.argv[1]
  html_file_out = sys.argv[2]
  if os.path.exists(html_file_out):
    raise RuntimeError(f"please remove {html_file_out} first")

  start_abs_time, end_abs_time, segments, full_flush_events, merge_during_commit_events, checkpoints, commits = pickle.load(open(segments_file_in, "rb"))
  print(f"{len(segments)} segments spanning {(segments[-1].start_time - segments[0].start_time).total_seconds()} seconds")

  time_aggs = compute_time_metrics(checkpoints, commits, full_flush_events, segments, start_abs_time, end_abs_time)

  all_seg_del_times = get_seg_del_times(segments)

  # get_seg_del_times(segments, start_abs_time, end_abs_time)

  _l = []
  w = _l.append

  padding_x = 5
  padding_y = 2

  # x_pixels_per_sec = (whole_width - 2*padding_x) / (end_abs_time - start_abs_time).total_seconds()
  x_pixels_per_sec = 3.0
  # print(f'{x_pixels_per_sec=}')

  # whole_width = 100 * 1024
  whole_width = int(x_pixels_per_sec * (end_abs_time - start_abs_time).total_seconds() + 2 * padding_x)
  whole_height = 1800

  gv = graphviz.Digraph(comment=f"Segments from {segments_file_in}", graph_attr={"rankdir": "LR"})

  w('<html style="height:100%">')
  w("<head>")
  # w('<script src="https://cdn.jsdelivr.net/npm/fabric@latest/dist/index.min.js"></script>')
  w("</head>")
  w('<body style="height:95%; padding:0; margin:5">')
  w('<div sytle="position: relative;">')
  w("<form>")
  w('<div style="position: absolute; y: 0; display: flex; justify-content: flex-end; z-index:11">')
  w('Zoom:&nbsp;<input type="range" id="zoomSlider" min="0.5" max="3" value="1" step=".01" style="width:300px;"/>')
  w("</div>")
  w("</form>")
  w("")
  w('<div id="details" style="position:absolute;left: 5;top: 5; z-index:10; background: rgba(255, 255, 255, 0.85); font-size: 18; font-weight: bold;"></div>')
  w('<div id="details2" style="position:absolute; right:0; display: flex; justify-content: flex-end; z-index:10; background: rgba(255, 255, 255, 0.85); font-size: 18; font-weight: bold;"></div>')

  w('<div id="divit" align=left style="position:absolute; left:5; top:5; overflow:scroll;height:100%;width:100%">')

  w(
    f'<svg preserveAspectRatio="none" id="it" viewBox="0 0 {whole_width + 400} {whole_height + 100}" width="{whole_width}" height="{whole_height}" xmlns="http://www.w3.org/2000/svg" style="height:100%">'
  )
  # w(f'<svg id="it" viewBox="0 0 {whole_width+400} {whole_height + 100}" width={whole_width} height={whole_height} xmlns="http://www.w3.org/2000/svg" style="height:100%">')
  # w(f'<svg id="it" width={whole_width} height={whole_height} xmlns="http://www.w3.org/2000/svg" style="height:100%">')
  # w(f'<svg id="it" width={whole_width} height={whole_height} xmlns="http://www.w3.org/2000/svg" style="height:100%">')

  min_start_abs_time = None
  # TODO: agg_metrics starts at negative seconds now ...
  # min_start_abs_time = start_abs_time

  by_flush_ord = {}
  by_moc_ord = {}

  # coalesce by flush/moc ord so we can present some details about each moc/ff
  for segment in segments:
    if min_start_abs_time is None or segment.start_time < min_start_abs_time:
      min_start_abs_time = segment.start_time

    flush_ord = segment.full_flush_ord
    if flush_ord is not None:
      if flush_ord not in by_flush_ord:
        by_flush_ord[flush_ord] = [0, 0, 0]

      l = by_flush_ord[flush_ord]
      if segment.size_mb is not None:
        l[0] += 1
        l[1] += segment.size_mb
        l[2] += segment.max_doc
    moc_ord = segment.merge_during_commit_ord
    if moc_ord is not None:
      # print(f'{moc_ord=}')
      if moc_ord not in by_moc_ord:
        by_moc_ord[moc_ord] = [0, 0]

      l = by_moc_ord[moc_ord]
      if segment.size_mb is not None:
        l[0] += 1
        l[1] += segment.size_mb

  w("\n\n  <!-- full flush events -->")
  for flush_ord, (start_time, end_time) in enumerate(full_flush_events):
    if end_time is None:
      continue

    x0 = padding_x + x_pixels_per_sec * (start_time - min_start_abs_time).total_seconds()
    y0 = 0
    x1 = padding_x + x_pixels_per_sec * (end_time - min_start_abs_time).total_seconds()
    y1 = whole_height
    # w(f'  <rect fill="cyan" fill-opacity=0.2 x={x0:.2f} y={y0:.2f} width={x1-x0:.2f} height="{y1-y0:.2f}"/>')
    flush_id = f"ff_{flush_ord}"
    w(f'  <rect fill="{full_flush_color}" x={x0:.2f} y={y0:.2f} width={x1 - x0:.2f} height={y1 - y0:.2f} seg_name="{flush_id}" id="{flush_id}"/>')

  w("\n\n  <!-- merge-on-commit events -->")
  for moc_ord, (start_time, end_time) in enumerate(merge_during_commit_events):
    if end_time is None:
      continue
    x0 = padding_x + x_pixels_per_sec * (start_time - min_start_abs_time).total_seconds()
    y0 = 0
    x1 = padding_x + x_pixels_per_sec * (end_time - min_start_abs_time).total_seconds()
    y1 = whole_height
    # w(f'  <rect fill="orange" fill-opacity=0.2 x={x0:.2f} y={y0:.2f} width={x1-x0:.2f} height="{y1-y0:.2f}"/>')
    moc_id = f"moc_{moc_ord}"
    w(f'  <rect fill="{merge_on_commit_color}" x={x0:.2f} y={y0:.2f} width={x1 - x0:.2f} height={y1 - y0:.2f} seg_name="{moc_id}" id="{moc_id}"/>')

  # w('var textbox = null;')
  # w('// so we can intersect mouse point with boxes:')
  # w(f'var ps = new PlanarSet();')

  print(f"{(end_abs_time - start_abs_time).total_seconds()} total seconds")
  y_pixels_per_log_mb = 4

  segment_name_to_level = {}

  segment_name_to_segment = {}

  segment_name_to_gv_node = {}

  max_level = 0

  # for making histograms/scatter plots
  ages_sizes = []

  for segment in segments:
    if segment.end_time is None:
      end_time = end_abs_time
    else:
      end_time = segment.end_time

    light_timestamp = None

    for timestamp, event, line_number in segment.events:
      if event == "light":
        light_timestamp = timestamp
        break
    if segment.size_mb is not None and light_timestamp is not None:
      ages_sizes.append((segment.source, (end_time - segment.start_time).total_seconds(), segment.size_mb, (light_timestamp - segment.start_time).total_seconds(), segment.max_doc))

    assert segment.name not in segment_name_to_segment, f"segment name {segment.name} appears twice?"
    segment_name_to_segment[segment.name] = segment
    label = f"{segment.name}: docs={segment.max_doc:,}"
    if segment.size_mb is not None:
      label += f" mb={segment.size_mb:,.1f}"

    is_flush = type(segment.source) is str and segment.source.startswith("flush")

    if is_flush:
      fill_color = flush_color
    else:
      fill_color = merge_color

    segment_name_to_gv_node[segment.name] = gv.node(segment.name, label=label, color=fill_color)

    if is_flush:
      pass
    else:
      for merged_seg_name in segment.source[1]:
        merged_seg = segment_name_to_segment[merged_seg_name]
        if merged_seg.del_count_merged_away is not None:
          label = f"{merged_seg.del_count_merged_away:,} dels"
          weight = merged_seg.del_count_merged_away
        else:
          label = ""
          weight = 0
        # gv.edge(merged_seg_name, segment.name, label=label, weight=str(weight))
        gv.edge(merged_seg_name, segment.name, label=label)
  with open("ages_sizes.txt", "w") as f:
    for source, age_sec, size_mb, write_time, max_doc in ages_sizes:
      if size_mb is not None:
        f.write(f"{size_mb:.1f}\t{max_doc}\n")

  if True:
    print(f"gv:\n{gv.source}")
    gv_file_name_out = gv.render("segments", format="svg")
    print(f"now render gv: {gv_file_name_out}")
    print(f"size={os.path.getsize('segments.svg')}")

  # first pass to assign free level to each segment.  segments come in
  # order of their birth:

  # this is actually a fun 2D bin-packing problem (assigning segments to levels)! ... I've only tried
  # these two simple approaches so far:

  if True:
    # sort segments by lifetime, and assign to level bottom up, so long lived segments are always down low

    # so we have a bit of horizontal space b/w segments:
    pad_time_sec = 1.0

    half_pad_time = datetime.timedelta(seconds=pad_time_sec / 2)

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
      used_levels = [segment_name_to_level[interval.data.name] for interval in tree[segment.start_time : end_time]]

      new_level = 0
      while True:
        if new_level not in used_levels:
          break
        new_level += 1

      tree[segment.start_time - half_pad_time : end_time + half_pad_time] = segment

      segment_name_to_level[segment.name] = new_level
      # print(f'{segment.name} -> level {new_level}')

      max_level = max(new_level, max_level)

  elif False:
    # sort segments by size, descending, and assign to level bottom up, so big segments are always down low

    tree = intervaltree.IntervalTree()

    for segment in sorted(segments, key=lambda segment: -segment.size_mb):
      assert segment.name not in segment_name_to_segment, f"segment name {segment.name} appears twice?"
      segment_name_to_segment[segment.name] = segment

      if segment.end_time is None:
        end_time = end_abs_time
      else:
        end_time = segment.end_time

      # all levels that are in use overlapping this new segment:
      used_levels = [segment_name_to_level[interval.data.name] for interval in tree[segment.start_time : end_time]]

      new_level = 0
      while True:
        if new_level not in used_levels:
          break
        new_level += 1

      tree[segment.start_time : end_time] = segment

      segment_name_to_level[segment.name] = new_level
      # print(f'{segment.name} -> level {new_level}')

      max_level = max(new_level, max_level)

  else:
    # a simpler first-come first-serve level assignment

    # assign each new segment to a free level -- this is not how IndexWriter does it, but
    # it doesn't much matter what the order is.  e.g. TMP disregards segment order (treats
    # them all as a set of segments)
    level_to_live_segment = {}

    for segment in segments:
      assert segment.name not in segment_name_to_segment, f"segment name {segment.name} appears twice?"

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

  # print(f'{max_level=}')
  y_pixels_per_level = whole_height / (1 + max_level)

  for segment in segments:
    level = segment_name_to_level[segment.name]
    # print(f'{(segment.start_time - min_start_abs_time).total_seconds()}')

    light_timestamp = None

    for timestamp, event, line_number in segment.events:
      if event == "light":
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

    height = min(y_pixels_per_level - padding_y, y_pixels_per_log_mb * math.log(size_mb / 2.0))
    height = max(7, height)

    y1 = whole_height - int(y_pixels_per_level * level)
    y0 = y1 - height
    if type(segment.source) is str and segment.source.startswith("flush"):
      if segment.source == "flush-by-RAM":
        color = flush_by_ram_color
      else:
        color = flush_color
    else:
      color = merge_color

    w(f'\n  <rect id="{segment.name}" x={x0:.2f} y={y0:.2f} width={x1 - x0:.2f} height={height:.2f} rx=4 fill="{color}" seg_name="{segment.name}"/>')
    # w(f'\n  <rect id="{segment.name}" x={x0:.2f} y={y0:.2f} width={x1-x0:.2f} height={height:.2f} rx=4 fill="{color}"/>')

    # dawn: shade the time segment is being written, before it's lit:
    if light_timestamp is not None:
      x_light = padding_x + x_pixels_per_sec * (light_timestamp - min_start_abs_time).total_seconds()

      if color == flush_by_ram_color:
        new_color = flush_by_ram_dawn_color
      elif color == flush_color:
        new_color = flush_dawn_color
      else:
        new_color = merge_dawn_color

      w("  <!--dawn:-->")
      w(f'  <rect x={x0:.2f} y={y0:.2f} width={x_light - x0:.2f} height={height:.2f} rx=4 fill="{new_color}" seg_name="{segment.name}"/>')

    if segment.merged_into is not None:
      # dusk: this segment was merged away eventually
      if color == flush_by_ram_color:
        new_color = flush_by_ram_dusk_color
      elif color == flush_color:
        new_color = flush_dusk_color
      else:
        new_color = merge_dusk_color
      dusk_timestamp = segment.merged_into.start_time
      assert dusk_timestamp < t1
      x_dusk = padding_x + x_pixels_per_sec * (dusk_timestamp - min_start_abs_time).total_seconds()
      w("  <!--dusk:-->")
      w(f'  <rect x={x_dusk:.2f} y={y0:.2f} width={x1 - x_dusk:.2f} height={height:.2f} rx=4 fill="{new_color}" seg_name="{segment.name}"/>')

      light_timestamp2 = None

      for timestamp2, event2, line_number2 in segment.merged_into.events:
        if event2 == "light":
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
        w(f'  <line id="line{segment.name}{segment.merged_into.name}" x1="{x_dusk}" y1="{y0 + height / 2}" x2="{x_light_merge}" y2="{y1a + merge_height / 2}" stroke="black" stroke-width="1"/>')

        if False:
          w('var l = document.createElementNS("http://www.w3.org/2000/svg", "line");')
          w('l.setAttribute("id", ");')
          w(f'l.setAttribute("x1", {x_dusk});')
          w(f'l.setAttribute("y1", {y0 + y_pixels_per_level / 2});')
          w(f'l.setAttribute("x2", {x_light_merge});')
          w(f'l.setAttribute("y2", {y1a - y_pixels_per_level / 2});')
          w("mysvg.appendChild(l);")

  # w('canvas.on("mouse:over", show_segment_details);')
  # w('canvas.on("mouse:out", function(e) {canvas.remove(textbox);  textbox = null;});')
  w("</svg>")
  w("</div>")
  w("<script>")
  w("const time_window_details_map = new Map();")

  for flush_ord, (start_time, end_time) in enumerate(full_flush_events):
    if end_time is None:
      continue
    # w(f'  <rect fill="cyan" fill-opacity=0.2 x={x0:.2f} y={y0:.2f} width={x1-x0:.2f} height="{y1-y0:.2f}"/>')
    flush_id = f"ff_{flush_ord}"
    l = by_flush_ord.get(flush_ord, [0, 0, 0])
    w(f'time_window_details_map.set("{flush_id}", "Flush {(end_time - start_time).total_seconds():.1f} s, {l[0]} segs, {l[1]:.1f} MB, {l[2] / 1000:.1f} K docs");')

  for moc_ord, (start_time, end_time) in enumerate(merge_during_commit_events):
    if end_time is None:
      continue
    moc_id = f"moc_{moc_ord}"
    l = by_moc_ord.get(moc_ord, [0, 0])
    w(f'time_window_details_map.set("{moc_id}", "Merge on commit {(end_time - start_time).total_seconds():.1f} sec, {l[0]} merges, {l[1]:.1f} MB");')

  w(f'merge_on_commit_color = "{merge_on_commit_color}";')
  w(f'merge_on_commit_highlight_color = "{merge_on_commit_highlight_color}";')
  w(f'full_flush_color = "{full_flush_color}";')
  w(f'full_flush_highlight_color = "{full_flush_highlight_color}";')
  w(f'merging_segment_highlight_color = "{merging_segment_highlight_color}";')
  w(f'segment_highlight_color = "{segment_highlight_color}";')
  w("var selected_seg_name;")
  w("var all_seg_del_times = [")
  for k, v in all_seg_del_times:
    w(f"  [{(k - min_start_abs_time).total_seconds():10.3f}, {v}],")
  w("]")
  w("var agg_metrics = [")
  for tup in time_aggs:
    (
      timestamp,
      num_segments,
      tot_size_mb,
      merge_mbs,
      flush_mbs,
      tot_mbs,
      cur_max_doc,
      del_count,
      del_pct,
      merge_thread_count,
      flush_thread_count,
      del_reclaims_per_sec,
      del_create_per_sec,
      flush_dps,
      merge_dps,
      last_commit_size_mb,
      tot_merge_write_mb,
      tot_flush_write_mb,
      write_ampl,
    ) = tup

    sec = (timestamp - min_start_abs_time).total_seconds()
    ts = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
    w(
      f'  [{sec:.4f}, {num_segments}, {tot_size_mb:.3f}, {merge_mbs:.3f}, {flush_mbs:.3f}, {tot_mbs:.3f}, {cur_max_doc}, {del_pct:.3f}, {merge_thread_count}, {flush_thread_count}, {del_reclaims_per_sec:.3f}, {flush_dps:.3f}, {merge_dps:.3f}, {last_commit_size_mb:.3f}, "{ts}", {tot_merge_write_mb:.3f}, {tot_flush_write_mb:.3f}, {write_ampl:.3f}, {del_create_per_sec:.3f}, {del_count}],'
    )

  w("];")
  w("const seg_merge_map = new Map();")
  # w('const seg_details_map = new Map();')
  w("const seg_details2_map = new Map();")
  w("\n// super verbose (includes InfoStream line numbers):")
  w("const seg_details3_map = new Map();")
  for segment in segments:
    if type(segment.source) is tuple and segment.source[0] == "merge":
      w(f'seg_merge_map.set("{segment.name}", {segment.source[1]});')
    if segment.size_mb is None:
      smb = "n/a"
    else:
      smb = f"{segment.size_mb:.1f} MB"
    details = f"{segment.name}:\n  {smb}\n  {segment.max_doc} max_doc"
    if segment.end_time is not None:
      end_time = segment.end_time
    else:
      end_time = end_abs_time
    details += f"\n  {(end_time - segment.start_time).total_seconds():.1f} sec"
    if segment.born_del_count is not None and segment.born_del_count > 0:
      details += f"\n  {100.0 * segment.born_del_count / segment.max_doc:.1f}% stillborn"

    # w(f'seg_details_map.set("{segment.name}", {repr(details)});')
    w(f'seg_details2_map.set("{segment.name}", {segment.to_verbose_string(min_start_abs_time, end_abs_time, False)!r});')
    w(f'seg_details3_map.set("{segment.name}", {segment.to_verbose_string(min_start_abs_time, end_abs_time, True)!r});')

  w("""
  const top_details = document.getElementById('details');
  const top_details2 = document.getElementById('details2');
  slider = document.getElementById('zoomSlider');
  svg = document.getElementById('it');

  slider.addEventListener('input', function() {
    zoom = slider.value;
  """)
  w('    console.log("zoom=" + zoom);')
  # w(f'    var new_view_box = "0 0 " + ({whole_width+400}/zoom) + " " + ({whole_height+100}/zoom);')
  w(f'    var new_view_box = "0 0 " + ({whole_width + 400}/zoom) + " " + ({whole_height + 100}/zoom);')
  # svg.style.transform = `scale(${zoom})`;
  w('    svg.setAttribute("viewBox", new_view_box);')
  w(f'    svg.setAttribute("width", {whole_width}*zoom);')
  w(f'    svg.setAttribute("height", {whole_height}*zoom);')
  w("  });")
  w("</script>")
  w("""
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

  var time_window_details;
  
  var highlighting = new Map();
  const svgns = "http://www.w3.org/2000/svg";
  function highlight(seg_name, color, do_seg_details, do_seg_super_verbose, transformed_point) {
    if (!highlighting.has(seg_name)) {

      if (seg_name.startsWith("ff_")) {
        // full flush
        var el = document.getElementById(seg_name);
        //console.log("highlight ff " + seg_name);
        el.setAttribute("fill", full_flush_highlight_color);
        highlighting.set(seg_name, el);
        time_window_details = time_window_details_map.get(seg_name);
      } else if (seg_name.startsWith("moc_")) {
        // merge-on-commit
        var el = document.getElementById(seg_name);
        //console.log("highlight moc " + seg_name);
        el.setAttribute("fill", merge_on_commit_highlight_color);
        highlighting.set(seg_name, el);
        time_window_details = time_window_details_map.get(seg_name);
      } else {
        // normal segment

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
          if (do_seg_super_verbose) {
            top_details.innerHTML = "<pre>" + seg_details3_map.get(seg_name) + "</pre>";
          } else {
            top_details.innerHTML = "<pre>" + seg_details2_map.get(seg_name) + "</pre>";
          }
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
  """)

  w(f"      var t = (transformed_point.x - {padding_x}) / {x_pixels_per_sec};")
  w("""

    // lookup segment del counts at this time:
    // console.log("selected: " + selected_seg_name);
    var cur_seg_del = null;
    if (selected_seg_name != null) {
      var spot0 = binarySearchWithInsertionPoint(all_seg_del_times, t) - 1;
      if (spot0 >= 0) {
        let del_map = all_seg_del_times[spot0];
        // console.log("del_map=" + JSON.stringify(del_map));
        if (selected_seg_name in del_map[1]) {
          var seg_del = del_map[1][selected_seg_name];
          // console.log("cur dels for " + selected_seg_name + ": " + seg_del);
          var dels = seg_del.split("/");
          var del_count = parseInt(dels[0]);
          var seg_max_doc = parseInt(dels[1]);
          cur_seg_del = selected_seg_name + " dels: " + (100.0*del_count/seg_max_doc).toFixed(1) + "% (" + del_count + "/" + seg_max_doc + ")";;
        }
      }
    }

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
    let tot_merge_write_mb = tup[15];
    let tot_flush_write_mb = tup[16];
    let write_ampl = tup[17];
    let del_create_per_sec = tup[18];
    let tot_del_count = tup[19];
    let hr = Math.floor(t/3600.)
    let mn = Math.floor((t - hr*3600)/60.)
    let sc = t - hr*3600 - mn*60;
    var scs;
    if (sc < 10.0) {
      scs = "0" + sc.toFixed(2);
    } else {
      scs = sc.toFixed(2);
    }
    let text = "<div>";

    if (time_window_details != null) {
      text += "<font color=red>" + time_window_details + "</font><br>";
    }

    if (cur_seg_del != null) {
      text += "<font color=red>" + cur_seg_del + "</font><br>";
    }

    var write_ampl_all_time;
    if (tot_flush_write_mb == 0) {
      write_ampl_all_time = 0;
    } else {
      write_ampl_all_time = (tot_flush_write_mb + tot_merge_write_mb) / tot_flush_write_mb;
    }
    
    text = text + "<pre>" +  String(hr).padStart(2, '0') + ":" + String(mn).padStart(2, '0') + ":" + scs + "<br>" +
           abs_timestamp + "<br>" +
           num_segments + " segments<br>" +
           (tot_size_mb/1024.).toFixed(2) + " GB<br>" +
           (max_doc/1000000.).toFixed(2) + "M docs (" + (tot_del_count/1000).toFixed(2) + "K, " + del_pct.toFixed(2) + "% deletes)<br>" +
           "IO now: " + tot_mbs.toFixed(2) + " MB/s<br>" +
           "Tot writes GB:<br>  " + (tot_merge_write_mb/1024.).toFixed(1) + " merge<br>  " + (tot_flush_write_mb/1024.).toFixed(1) + " flush<br>  " + write_ampl_all_time.toFixed(1) + "/" + write_ampl.toFixed(1) + " write-ampl<br>" +
           "Last commit: " + (last_commit_size_mb / 1024.).toFixed(2) + " GB<br>" +
           "Flushes: " + flush_thread_count + " threads<br>        " + flush_mbs.toFixed(1) + " MB/s<br>        " + flush_dps.toFixed(1) + " docs/s<br>" +
           "Merges: " + merge_thread_count + " threads<br>        " + merge_mbs.toFixed(1) + " MB/s<br>       " + merge_dps.toFixed(1) + " docs/s<br>       " + del_create_per_sec.toFixed(1) + "/" + del_reclaims_per_sec.toFixed(1) + " del create/reclaim/s<br>" +
           "</pre>";

    text += "</div>";

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
  """)
  w(f'      x_cursor.setAttribute("y2", {whole_height});')
  w("""

    document.elementsFromPoint(evt.clientX, evt.clientY).forEach(function(element, index, array) {
      if (element instanceof SVGRectElement) {
        var seg_name = element.getAttribute("seg_name");
        // var seg_name = element.id;
        // must null-check because we will also select our newly added highlight rects!  weird recursion...
        if (seg_name != null) {
          if (seg_name.startsWith("moc_")) {
            now_highlight.add(seg_name);
          } else if (seg_name.startsWith("ff_")) {
            now_highlight.add(seg_name);
          } else {
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
        if (seg_name.startsWith("ff_")) {
          // console.log("now unhighlight " + seg_name);
          var el = document.getElementById(seg_name);
          el.setAttribute("fill", full_flush_color);
          time_window_details = null;
        } else if (seg_name.startsWith("moc_")) {
          // console.log("now unhighlight " + seg_name);
          var el = document.getElementById(seg_name);
          el.setAttribute("fill", merge_on_commit_color);
          time_window_details = null;
        } else {
          mysvg.removeChild(rect);
        }
        highlighting.delete(name);
      }
    }
    selected_seg_name = null;
    for (let seg_name of now_highlight) {
      //console.log("highlight " + seg_name);
      var do_seg_details;
      if (merge_seg_name != null) {
        do_seg_details = seg_name == merge_seg_name;
      } else {
        do_seg_details = true;
      }

      highlight(seg_name, "cyan", do_seg_details, evt.ctrlKey, transformed_point);
      if (seg_name.startsWith("ff_") == false && seg_name.startsWith("moc_") == false) {
        selected_seg_name = seg_name;
      }
    }
    // TODO: just use map w/ colors?
    for (let seg_name of now_sub_highlight) {
      // console.log("sub highlight " + seg_name);
      highlight(seg_name, merging_segment_highlight_color, false, false, transformed_point);
    }
  };

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
  
</script>""")

  w("</div>")
  w("</body>")
  w("</html>")

  with open(html_file_out, "w") as f:
    f.write("\n".join(_l))


if __name__ == "__main__":
  main()
