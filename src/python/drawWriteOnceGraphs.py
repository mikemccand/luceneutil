import os
import random
import shutil
import sys

nonce = random.randint(0, sys.maxint)


def main():
  arcs = [(0, 1, "the"), (1, 2, "purple"), (2, 3, "write-once"), (2, 4, "write"), (4, 3, "once"), (4, 5, "again"), (5, 6, "foo"), (3, 7, "ran"), (6, 7, "bar")]

  # nocommit
  # arcs.reverse()

  OUT_DIR = "/x/tmp/graphs"
  if os.path.exists(OUT_DIR):
    shutil.rmtree(OUT_DIR)
  os.makedirs(OUT_DIR)

  for i in range(1, len(arcs) + 1):
    _l = []
    w = _l.append
    w("digraph x {")
    w("  rankdir = LR;")
    seen = set()
    for j in range(len(arcs)):
      if j == i - 1:
        color = "red"
      elif j < i:
        color = "black"
      else:
        color = "white"

      fromNode, toNode, label = arcs[j]
      for node in (fromNode, toNode):
        if node not in seen:
          seen.add(node)
          w('  %s [shape=circle,color="%s",label=""]' % (node, color))
      w('  %s -> %s [color=%s,label="%s",fontcolor=%s]' % (fromNode, toNode, color, label, color))
    w("}")
    open("%s/%d_%s.dot" % (OUT_DIR, i, nonce), "wb").write("\n".join(_l))
    if os.system("dot -Tpng %s/%d_%s.dot > %s/%d_%s.png" % (OUT_DIR, i, nonce, OUT_DIR, i, nonce)):
      raise RuntimeError("dot failed")

    _l = []
    w = _l.append
    if i == 1:
      w('<a href="" onClick="if (event.shiftKey) {} else {window.location=\'%s_%s.html\';} return false;"><img src=%s_%s.png></a>' % (i + 1, nonce, i, nonce))
    elif i == len(arcs):
      w('<a href="" onClick="if (event.shiftKey) {window.location=\'%s_%s.html\';} return false;"><img src=%s_%s.png></a>' % (i - 1, nonce, i, nonce))
    else:
      w(
        "<a href=\"\" onClick=\"if (event.shiftKey) {window.location='%s_%s.html';} else {window.location='%s_%s.html';} return false;\"><img src=%s_%s.png></a>"
        % (i - 1, nonce, i + 1, nonce, i, nonce)
      )

    open("%s/%s_%d.html" % (OUT_DIR, i, nonce), "wb").write("\n".join(_l))

  open("%s/start.html" % OUT_DIR, "wb").write('<a href="1_%s.html">start</a>' % nonce)


if __name__ == "__main__":
  main()
