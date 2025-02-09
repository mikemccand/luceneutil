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

# pip3 install intervaltree
import intervaltree

# TODO
#   - count/report CFS stats too
#   - hmm add click handler to jump back to merge input segments (_1gu -> _20r)
#   - handle multiple IW sessions
#   - add colored bands during full flush / merge commit wait / etc.
#   - add flush reason to segment source
#   - why IW/SM static counters so high?
#   - maybe produce an overall summary of indexing "behavior"?
#     - how many merge-on-commits finished in the window
#     - how many concurrent flushes/merges w/ time
#     - the bandwidth number
#     - deletes rate vs merge rate
#     - merge lopsidedness, range of number of segments merged, merge selection "efficiency"
#     - net write amplification
#     - update vs delete vs append rate
#   - pop out nice graphs showing stuff over time, linking out to lines in InfoStream
#     - merge del reclaim efficiency (% deletes reclaimed)
#     - size of index, % deletes
#     - full-flush and how long they took / how many segments / bytes
#     - indexing rate
#     - io write rate
#     - write amp;lification
#     - nujmber of thresds/segments flushing/merging
#     - ram -> segment size efficiency of flush
#   - am i pulling born deleted correctly for flushed segments?
#   - also report on how much HNSW concurrency is in use
#   - extract commit times too
#     - and maybe attach flush reassons?  flush-on-ram, on-nrt, commit
#   - hmm how to view the "full" geneology of one segment -- it is always a "full" tree down to original roots (flushed segments)
#   - enable simple x zoom
#   - can we indicate how unbalanced each merge is somehow?
#   - track "merge efficiency" (how unbalanced)
#   - add x-cursor showing current time
#   - can we track the usefulness of merging, how long it's "in service" before being merged away, vs its dawn (how long it took to produce)
#   - hmm maybe add an overlaid line chart showing badnwdith used from merging/flushingx
#   - get mouseover working for the gray merge lines too
#   - have deletes gradually make segment lighter and lighter?
#   - improved 2D bin packing?
#   - maybe make merge connection lines thicker the higher the "level" of the merge?
#   - include "reason" in segs -- forceMerge, nrtReopen, commit/flush
#   - maybe use D3?
#   - get more interesting trace -- nrt?  deletions!
#   - make sure we can handle infoStream on an already built index, i.e. pre-existing segments
#     from before infoStream was turned on
#   - hmm can I be more cautious in assigning segments to levels?  like leave lowest levels
#     "intentionally" open for the big merged segments?
#     - or ... try to put segments close in level if they were created close in time?
#     - sort of a bin packing problem...
#   - try to work on mobile?
#   - derive y_pix_per_level from whole_height and max(level)
#   - zoom in/out, x and y separately
#   - sliding cursor showing segment count, which are alive, how many deletes, etc.
#   - mouse over merge segment should highlight segments it's merging
#   - ooh link to the line number in infoStream.log so we can jump to the right place for debugging

# so pickle is happy
Segment = infostream_to_segments.Segment

'''
Opens a segments.pk previously written by parsing an IndexWriter InfoStream log using
infostream_to_segments.py, and writes a semi-interactive 2D rendering/timeline of the
segment flushing/merging/deletes using fabric.js / HTML5 canvas.
'''

USE_FABRIC = False
USE_SVG = True

def compute_bandwidth_by_time(segments, start_abs_time, end_abs_time):

  # single list that mixes end and start times
  all_times = []
  for segment in segments:
    if segment.end_time is None or segment.size_mb is None:
      # InfoStream ends before merge finished
      continue
    all_times.append((segment.start_time, segment))
    all_times.append((segment.end_time, segment))

  all_times.sort(key=lambda x: x[0])

  # TODO: how to plot this?
  bw = 0
  for time, segment in all_times:
    # MB/sec written by this merge
    mbs_for_merge = segment.size_mb / (segment.end_time - segment.start_time).total_seconds()
    if time == segment.start_time:
      bw += mbs_for_merge
    else:
      bw -= mbs_for_merge
    
    # print(f'{(time - start_abs_time).total_seconds():6.1f} sec: {bw:.1f} MB/sec')
    print(f'{(time - start_abs_time).total_seconds():6.1f}\t{bw:.1f}')

def main():

  if len(sys.argv) != 3:
    raise RuntimeError(f'usage: {sys.executable} -u segments_to_html.py segments.pk out.html')

  segments_file_in = sys.argv[1]
  html_file_out = sys.argv[2]
  if os.path.exists(html_file_out):
    raise RuntimeError(f'please remove {html_file_out} first')

  start_abs_time, end_abs_time, segments, full_flush_events, merge_during_commit_events = pickle.load(open(segments_file_in, 'rb'))
  print(f'{len(segments)} segments spanning {(segments[-1].start_time-segments[0].start_time).total_seconds()} seconds')

  compute_bandwidth_by_time(segments, start_abs_time, end_abs_time)
    
  _l = []
  w = _l.append

  whole_width = 100 * 1024
  whole_height = 1800

  w('<html style="height:100%">')
  w('<head>')
  # w('<script src="https://cdn.jsdelivr.net/npm/fabric@latest/dist/index.min.js"></script>')
  w('</head>')
  w('<body style="height:95%; padding:0; margin:5">')
  w('<form>')
  w('<div style="display: flex; justify-content: flex-end;">')
  w('Zoom:&nbsp;<input type="range" id="zoomSlider" min="0.5" max="3" value="1" step=".01" style="width:300px;"/>')
  w('</div>')
  w('</form>')
  w('')
  #w(f'<div id="details" style="position:absolute;left=5;top=5;z-index=0;background:rgba(0xff,0xff,0xff,.6)"></div>')
  w(f'<div id="details" style="position:absolute;left=5;top=5;z-index=0"></div>')
  w(f'<div align=left style="overflow:scroll;height:100%;width:100%">')
  w(f'<svg preserveAspectRatio="none" id="it" viewBox="0 0 {whole_width+400} {whole_height+100}" width="{whole_width}" height="{whole_height}" xmlns="http://www.w3.org/2000/svg" style="height:100%">')
  # w(f'<svg id="it" viewBox="0 0 {whole_width+400} {whole_height + 100}" width={whole_width} height={whole_height} xmlns="http://www.w3.org/2000/svg" style="height:100%">')
  # w(f'<svg id="it" width={whole_width} height={whole_height} xmlns="http://www.w3.org/2000/svg" style="height:100%">')
  # w(f'<svg id="it" width={whole_width} height={whole_height} xmlns="http://www.w3.org/2000/svg" style="height:100%">')

  #w('var textbox = null;')
  #w('// so we can intersect mouse point with boxes:')
  #w(f'var ps = new PlanarSet();')

  padding_x = 5
  padding_y = 2

  print(f'{(end_abs_time - start_abs_time).total_seconds()} total seconds')
  x_pixels_per_sec = (whole_width - 2*padding_x) / (end_abs_time - start_abs_time).total_seconds()
  y_pixels_per_log_mb = 4

  segment_name_to_level = {}

  segment_name_to_segment = {}

  max_level = 0

  min_start_abs_time = None

  # first pass to assign free level to each segment.  segments come in
  # order of their birth:

  # this is actually a fun 2D bin-packing problem (assigning segments to levels)! ... I've only tried
  # these two simple approaches so far:

  # TODO: maybe instead sort by how long segment is alive?  should "roughly" correlate to segment size...

  if True:
    # sort segments by size, descending, and assign to level bottom up, so big segments are always down low

    tree = intervaltree.IntervalTree()

    seg_times = []
    for segment in segments:
      if segment.end_time is None:
        end_time = end_abs_time
      else:
        end_time = segment.end_time
      seg_times.append((end_time - segment.start_time, segment))
      
    for ignore, segment in sorted(seg_times, key=lambda x: -x[0]):

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
      print(f'{segment.name} -> level {new_level}')

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
      print(f'{segment.name} -> level {new_level}')

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
      print(f'{segment.name} -> level {level}')

      max_level = max(level, max_level)

      if min_start_abs_time is None or segment.start_time < min_start_abs_time:
        min_start_abs_time = segment.start_time

  print(f'{max_level=}')
  y_pixels_per_level = whole_height / (1+max_level)

  for segment in segments:
    level = segment_name_to_level[segment.name]
    print(f'{(segment.start_time - min_start_abs_time).total_seconds()}')

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
    if segment.source == 'flush':
      color = '#ff0000'
    else:
      color = '#0000ff'
    if USE_SVG:
      w(f'\n  <rect id="{segment.name}" x={x0:.2f} y={y0:.2f} width={x1-x0:.2f} height={height:.2f} rx=4 fill="{color}" seg_name="{segment.name}"/>')

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

    elif USE_FABRIC:
      w(f'var rect = new fabric.Rect({{left: {x0}, top: {y0}, width: {x1-x0+1}, height: {height}, fill: "{color}"}});')
      w('rect.selectable = false;')
      w(f'rect.details = "{segment.name}: {segment.size_mb:.2f} MB";')
    #w(f'rect.on("mouse:over", function(e) {{alert(header); header.innerHTML = "{segment.name}";}});')
    #w('rect.on("mouse:over", function() {console.log("mouse over");});')
    #w('rect.on("mouse:out", function() {console.log("mouse out");});')
    #w('rect.on("mouse:out", mouse_out);')
      w('canvas.add(rect);')
    else:
      w(f'ctx.fillStyle = "{color}";')
      w(f'ctx.fillRect({x0}, {y0}, {x1-x0+1}, {height});')
      w(f'var b = new Box({x0}, {y0}, {x1}, {y0+height});')
      w(f'b.segment_name = {segment.name}')
      w(f'b.details = "{segment.name}: {segment.size_mb:.2f} MB";')
      w('// so we can intersect mouse point with boxes:')
      w('ps.add(b);')

      # shade the time segment is being written, before it's lit:
      if light_timestamp is not None:
        x_light = int(x_pixels_per_sec * (light_timestamp - min_start_abs_time).total_seconds())

        if color == '#ff0000':
          new_color = '#ff8888'
        else:
          new_color = '#8888ff'
        w(f'ctx.fillStyle = "{new_color}";')
        w(f'ctx.fillRect({x0}, {y0}, {x_light-x0+1}, {height});')

      if segment.merged_into is not None:
        # this segment was merged away eventually
        if color == '#ff0000':
          new_color = '#880000'
        else:
          new_color = '#000088'
        next_merge_timestamp = segment.merged_into.start_time
        assert next_merge_timestamp < t1
        x_next_merge = int(x_pixels_per_sec * (next_merge_timestamp - min_start_abs_time).total_seconds())
        w(f'ctx.fillStyle = "{new_color}";')
        w(f'ctx.fillRect({x_next_merge}, {y0}, {x1-x_next_merge+1}, {height});')
        
  #w('canvas.on("mouse:over", show_segment_details);')
  # w('canvas.on("mouse:out", function(e) {canvas.remove(textbox);  textbox = null;});')
  if USE_SVG:
    w('</svg>')
    w('</div>')
    w('''
<script>
    var highlighting = new Map();
    function highlight(seg_name, color, do_seg_details, transformed_point) {
      if (!highlighting.has(seg_name)) {

        // get the full segment (not dawn/dusk):
        var rect2 = document.getElementById(seg_name);

        var svgns = "http://www.w3.org/2000/svg";
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
          top_details.innerHTML = "<pre>" + seg_details2_map.get(seg_name) + "</pre>";
        }
      }
    }

    mysvg = document.getElementById("it");
    mysvg.onmousemove = function(evt) {
      // console.log("clientX=" + evt.clientX + " clientY=" + evt.clientY);
      // r = mysvg.getBoundingClientRect();
      r = mysvg.getBBox();
      //console.log("bbox is " + r);
      point = mysvg.createSVGPoint();
      // screen point within mysvg:
      // console.log("sub " + r.left + " " + r.top);
      //point.x = evt.clientX - r.left;
      //point.y = evt.clientY - r.top;
      point.x = Math.max(20, evt.clientX + 20);
      point.y = Math.max(20, evt.clientY - 20);
      transformed_point = point.matrixTransform(mysvg.getScreenCTM().inverse());
      // console.log("  SVG X:", transformed_point.x, "SVG Y:", transformed_point.y);
      var now_highlight = new Set();
      var now_sub_highlight = new Set();
      var merge_seg_name = null;
      document.elementsFromPoint(evt.clientX, evt.clientY).forEach(function(element, index, array) {
        if (element instanceof SVGRectElement) {
          var seg_name = element.getAttribute("seg_name");
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
        highlight(seg_name, "cyan", do_seg_details, transformed_point);
      }
      // TODO: just use map w/ colors?
      for (let seg_name of now_sub_highlight) {
        // console.log("sub highlight " + seg_name);
        highlight(seg_name, "orange", false, transformed_point);
      }
    };
</script>
''')
  else:
    w('</script>')

  w('<script>')
  w('const seg_merge_map = new Map();')
  w('const seg_details_map = new Map();')
  w('const seg_details2_map = new Map();')
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
      
    w(f'seg_details_map.set("{segment.name}", {repr(details)});')
    w(f'seg_details2_map.set("{segment.name}", {repr(segment.to_verbose_string(min_start_abs_time))});')

  w('''
  const top_details = document.getElementById('details');
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
  w('</script>')
  w('</body>')
  w('</html>')

  with open(html_file_out, 'w') as f:
    f.write('\n'.join(_l))

if __name__ == '__main__':
  main()
