import math
import pickle
import sys
import os
import infostream_to_segments

# TODO
#   - maybe use svg not canvas?  d3js visualisation?
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

  canvas_width = 10 * 1024
  canvas_height = 2048

  w('<html>')
  w('<head>')
  w('<script src="https://cdn.jsdelivr.net/npm/fabric@latest/dist/index.min.js"></script>')
  w('</head>')
  w('<body>')
  w(f'<canvas id="it" style="border:1px solid #000000;" width={canvas_width} height={canvas_height}>')
  w('</canvas>')
  w('<script>')
  w('// import { StaticCanvas, FabricText } from "fabric"')
  w('import { Box, PlanarSet } from "@flatten-js/core";')
  w('function mouse_over(e) {')
  w('  alert("over: " + e.target);')
  w('}')
  w('function mouse_out(e) {')
  w('  alert("out: " + e);')
  w('}')
  w('')
  w('''
function on_mouse_move(e) {
  console.log("mouse move " + e.x + " " + e.y);
  var rect = canvas.getBoundingClientRect();
  var hits = ps.hit(Point(e.x + rect.left, e.y + rect.top));
  console.log("  hits: " + hits);
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
                                    fontSize: 24,
                                    fontFamily: "Verdana"});
      textbox.selectable = false;
      textbox.editable = false;
      canvas.add(textbox);
    }
  }
}
  ''')
  w('')
  if USE_FABRIC:
    w('var canvas = new fabric.Canvas(document.getElementById("it"));')
  else:
    w('var canvas = document.getElementById("it");')
    w('var ctx = canvas.getContext("2d");')
    w('canvas.addEventListener("mousemove", on_mouse_move, true);')

  w('var textbox = null;')
  w('// so we can intersect mouse point with boxes:')
  w(f'var ps = new PlanarSet();')

  padding_x = 5
  padding_y = 2

  x_pixels_per_sec = (canvas_width - 2*padding_x) / (end_abs_time - start_abs_time).total_seconds()
  y_pixels_per_log_mb = 1.3

  y_pixels_per_level = 20

  # assign each new segment to a free level -- this is not how IndexWriter does it, but
  # it doesn't much matter what the order is.  e.g. TMP disregards segment order (treats
  # them all as a set of segments)
  level_to_live_segment = {}

  segment_name_to_level = {}

  segment_name_to_segment = {}

  # segments come in order of their birth:
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

    print(f'{(segment.start_time - start_abs_time).total_seconds()}')

    light_timestamp = None
    
    for timestamp, event in segment.events:
      if event == 'light':
        light_timestamp = timestamp

    x0 = padding_x + int(x_pixels_per_sec * (segment.start_time - start_abs_time).total_seconds())
    if segment.end_time is None:
      t1 = end_abs_time
    else:
      t1 = segment.end_time
    x1 = int(x_pixels_per_sec * (t1 - start_abs_time).total_seconds())

    height = min(y_pixels_per_level - padding_y, int(y_pixels_per_log_mb * math.log(segment.size_mb)))
    height = max(2, height)

    y1 = canvas_height - int(y_pixels_per_level * level)
    y0 = y1 - height
    if segment.source == 'flush':
      color = '#ff0000'
    else:
      color = '#0000ff'
    if USE_FABRIC:
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
        x_light = int(x_pixels_per_sec * (light_timestamp - start_abs_time).total_seconds())

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
        x_next_merge = int(x_pixels_per_sec * (next_merge_timestamp - start_abs_time).total_seconds())
        w(f'ctx.fillStyle = "{new_color}";')
        w(f'ctx.fillRect({x_next_merge}, {y0}, {x1-x_next_merge+1}, {height});')
        
  #w('canvas.on("mouse:over", show_segment_details);')
  # w('canvas.on("mouse:out", function(e) {canvas.remove(textbox);  textbox = null;});')
  w('</script>')  
  w('</body>')
  w('</html>')

  with open(html_file_out, 'w') as f:
    f.write('\n'.join(_l))

if __name__ == '__main__':
  main()
