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

# TODO
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

# Flatten.PlanarSet tutorial: https://observablehq.com/@alexbol99/flattenjs-tutorials-planar-set

# so pickle is happy
Segment = infostream_to_segments.Segment

'''
Opens a segments.pk previously written by parsing an IndexWriter InfoStream log using
infostream_to_segments.py, and writes a semi-interactive 2D rendering/timeline of the
segment flushing/merging/deletes using fabric.js / HTML5 canvas.
'''

USE_FABRIC = False
USE_SVG = True

def main():

  if len(sys.argv) != 3:
    raise RuntimeError(f'usage: {sys.executable} -u segments_to_html.py segments.pk out.html')

  segments_file_in = sys.argv[1]
  html_file_out = sys.argv[2]
  if os.path.exists(html_file_out):
    raise RuntimeError(f'please remove {html_file_out} first')

  start_abs_time, end_abs_time, segments = pickle.load(open(segments_file_in, 'rb'))
  print(f'{len(segments)} segments spanning {(segments[-1].start_time-segments[0].start_time).total_seconds()} seconds')
  
  _l = []
  w = _l.append

  whole_width = 25 * 1024
  whole_height = 1800

  w('<html style="height:100%">')
  w('<head>')
  # w('<script src="https://cdn.jsdelivr.net/npm/fabric@latest/dist/index.min.js"></script>')
  w('</head>')
  w('<body style="height:100%; padding:0; margin:0">')
  if not USE_SVG:
    w(f'<canvas id="it" style="border:1px solid #000000;" width={whole_width} height={whole_height}>')
    w('</canvas>')
    w('<script>')
    # w('// import { StaticCanvas, FabricText } from "fabric"')
    # w('import { Box, PlanarSet } from "@flatten-js/core";')
    w('function mouse_over(e) {')
    w('  alert("over: " + e.target);')
    w('}')
    w('function mouse_out(e) {')
    w('  alert("out: " + e);')
    w('}')
    w('')
    w('''
  function on_mouse_move(e) {
    //console.log("mouse move " + e.x + " " + e.y);
    var rect = canvas.getBoundingClientRect();
    var hits = ps.hit(Point(e.x + rect.left, e.y + rect.top));
    //console.log("  hits: " + hits);
  }

  function show_segment_details(e) {
    if (e != null && e.target != null) {
      pointer = canvas.getPointer(e.e);
      if (textbox != null) {
        textbox.set({text: e.target.details,
                     top: pointer.y,
                     left: pointer.x});
        canvas.renderAll();
      } else {
        textbox = new fabric.Textbox(e.target.details,
                                     {left: pointer.x,
                                      top: pointer.y,
                                      fontSize: 14,
                                      fontFamily: "Verdana"});
        textbox.selectable = false;
        textbox.editable = false;
        canvas.add(textbox);
      }
    }
  }
    ''')
  w('')
  if USE_SVG:
    w(f'<div align=left style="overflow:scroll;height:100%;width:100%">')
    w(f'<svg preserveAspectRatio="none" id="it" viewBox="0 0 {whole_width+400} {whole_height}" width={whole_width} height={whole_height} xmlns="http://www.w3.org/2000/svg" style="height:100%">')
    # w(f'<svg id="it" width={whole_width} height={whole_height} xmlns="http://www.w3.org/2000/svg" style="height:100%">')
  elif USE_FABRIC:
    w('var canvas = new fabric.Canvas(document.getElementById("it"));')
  else:
    w('var canvas = document.getElementById("it");')
    w('var ctx = canvas.getContext("2d");')
    w('canvas.addEventListener("mousemove", on_mouse_move, true);')

  #w('var textbox = null;')
  #w('// so we can intersect mouse point with boxes:')
  #w(f'var ps = new PlanarSet();')

  padding_x = 5
  padding_y = 2

  print(f'{(end_abs_time - start_abs_time).total_seconds()} total seconds')
  x_pixels_per_sec = (whole_width - 2*padding_x) / (end_abs_time - start_abs_time).total_seconds()
  y_pixels_per_log_mb = 2

  # assign each new segment to a free level -- this is not how IndexWriter does it, but
  # it doesn't much matter what the order is.  e.g. TMP disregards segment order (treats
  # them all as a set of segments)
  level_to_live_segment = {}

  segment_name_to_level = {}

  segment_name_to_segment = {}

  max_level = 0

  min_start_abs_time = None

  # first pass to assign free level to each segment.  segments come in
  # order of their birth:
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

    x0 = padding_x + x_pixels_per_sec * (segment.start_time - min_start_abs_time).total_seconds()
    if segment.end_time is None:
      t1 = end_abs_time
    else:
      t1 = segment.end_time
    x1 = padding_x + x_pixels_per_sec * (t1 - min_start_abs_time).total_seconds()

    if segment.size_mb is None:
      raise RuntimeError(f'segment {segment.name} has size_mb=None')
    
    height = min(y_pixels_per_level - padding_y, y_pixels_per_log_mb * math.log(segment.size_mb))
    height = max(5, height)

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
        next_merge_timestamp = segment.merged_into.start_time
        assert next_merge_timestamp < t1
        x_next_merge = padding_x + x_pixels_per_sec * (next_merge_timestamp - min_start_abs_time).total_seconds()
        w(f'  <!--dusk:-->')
        w(f'  <rect x={x_next_merge:.2f} y={y0:.2f} width={x1-x_next_merge:.2f} height={height:.2f} rx=4 fill="{new_color}" seg_name="{segment.name}"/>')
      
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
          text.setAttribute("font-size", "24");

          highlighting.set(seg_name + ":t", text);
          text.selectable = false;
          text.textContent = seg_details_map.get(seg_name);
          text.setAttribute("x", transformed_point.x);
          text.setAttribute("y", transformed_point.y);
          text.setAttribute("width", "200");
          text.setAttribute("height", "200");
          mysvg.appendChild(text);
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
  for segment in segments:
    if type(segment.source) is tuple and segment.source[0] == 'merge':
      w(f'seg_merge_map.set("{segment.name}", {segment.source[1]});')
    details = f'{segment.name}:\n  {segment.size_mb:.1f} MB\n  {segment.max_doc} max_doc'
    if segment.end_time is not None:
      end_time = segment.end_time
    else:
      end_time = end_abs_time
    details += f'\n  {(end_time - segment.start_time).total_seconds():.1f} sec'
    w(f'seg_details_map.set("{segment.name}", {repr(details)});')
    
  w('</script>')
  w('</body>')
  w('</html>')

  with open(html_file_out, 'w') as f:
    f.write('\n'.join(_l))

if __name__ == '__main__':
  main()
