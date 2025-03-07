import io
import struct
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f, open(sys.argv[2], "wb") as fOut:
  # write CSV header outside of blocks:
  header = f.readline()
  fOut.write(header.encode("utf-8"))

  pending = io.BytesIO()
  while True:
    line = f.readline()
    if len(line) == 0:
      break

    pending.write(line.encode("utf-8"))

    if pending.tell() > 128 * 1024:
      fOut.write(struct.pack("i", pending.tell()))
      fOut.write(pending.getbuffer())
      pending = io.BytesIO()

  if pending.tell() > 0:
    fOut.write(struct.pack("i", pending.tell()))
    fOut.write(pending.getbuffer())
