import sys

lines = []

while True:
  line = sys.stdin.readline()
  if line.startswith('<!DOCTYPE HTML>'):
    lines.append(line)
  elif len(lines) > 0:
    lines.append(line)
    if line.startswith('</html>'):
      break

print(''.join(lines))
