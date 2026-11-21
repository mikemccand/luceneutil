"""Converts GeoNames allCountries.txt (tab-separated) into a binary format
that can be read directly by Java without string splitting.

Usage::

  python3 -u src/python/buildBinaryGeoNames.py <input.txt> <output.bin>
  python3 -u src/python/buildBinaryGeoNames.py /lucenedata/geonames/allCountries.txt /lucenedata/geonames/allCountries.bin

Binary format (little-endian):
  Repeated chunks:
    [int32: docCount] [int32: chunkByteLength] [chunk bytes...]

  Each doc within a chunk:
    [int32: geoNameID]
    [double: latitude]    (NaN if missing)
    [double: longitude]   (NaN if missing)
    [int64: population]   (-1 if missing)
    [int64: elevation]    (Long.MIN_VALUE if missing)
    [int32: dem]          (Integer.MIN_VALUE if missing)
    [int64: modifiedMsec] (-1 if missing)
    [int16: nameLen] [nameBytes...]
    [int16: asciiNameLen] [asciiNameBytes...]
    [int16: alternateNamesLen] [alternateNamesBytes...]
    [int16: featureClassLen] [featureClassBytes...]
    [int16: featureCodeLen] [featureCodeBytes...]
    [int16: countryCodeLen] [countryCodeBytes...]
    [int16: cc2Len] [cc2Bytes...]
    [int16: admin1Len] [admin1Bytes...]
    [int16: admin2Len] [admin2Bytes...]
    [int16: admin3Len] [admin3Bytes...]
    [int16: admin4Len] [admin4Bytes...]
    [int16: timezoneLen] [timezoneBytes...]
"""

import datetime
import io
import math
import struct
import sys

CHUNK_SIZE_THRESHOLD = 64 * 1024  # flush chunk when pending bytes exceed 64KB

epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def parse_date(s):
  """Parse yyyy-MM-dd to milliseconds since epoch (UTC), or -1 if empty/invalid."""
  if not s.strip():
    return -1
  try:
    dt = datetime.datetime.strptime(s.strip(), "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
    return int((dt - epoch).total_seconds() * 1000)
  except ValueError:
    return -1


def encode_str(s):
  """Encode a string field as [int16 length][utf-8 bytes]."""
  b = s.encode("utf-8")
  return struct.pack("<H", len(b)) + b


def flush(pending, pending_doc_count, f_out):
  data = pending.getvalue()
  f_out.write(struct.pack("<ii", pending_doc_count, len(data)))
  f_out.write(data)


def main():
  if len(sys.argv) != 3:
    print(f"Usage: python3 {sys.argv[0]} <input.txt> <output.bin>")
    sys.exit(1)

  input_path = sys.argv[1]
  output_path = sys.argv[2]

  print(f"Converting GeoNames text file: {input_path}")
  print(f"Output binary file: {output_path}")

  doc_count = 0
  pending = io.BytesIO()
  pending_doc_count = 0

  with open(input_path, encoding="utf-8") as f_in, open(output_path, "wb") as f_out:
    for line in f_in:
      line = line.rstrip("\n")
      if not line:
        continue

      fields = line.split("\t")
      if len(fields) != 19:
        raise ValueError(f"Expected 19 tab-separated fields but got {len(fields)} at line {doc_count + 1}")

      # Parse numeric fields
      geo_name_id = int(fields[0]) if fields[0] else 0
      latitude = float(fields[4]) if fields[4] else math.nan
      longitude = float(fields[5]) if fields[5] else math.nan
      population = int(fields[14]) if fields[14] else -1
      elevation = int(fields[15]) if fields[15] else -9223372036854775808  # Long.MIN_VALUE
      dem = int(fields[16]) if fields[16] else -2147483648  # Integer.MIN_VALUE
      modified_msec = parse_date(fields[18])

      # Write fixed-size numeric fields
      pending.write(struct.pack("<i", geo_name_id))
      pending.write(struct.pack("<d", latitude))
      pending.write(struct.pack("<d", longitude))
      pending.write(struct.pack("<q", population))
      pending.write(struct.pack("<q", elevation))
      pending.write(struct.pack("<i", dem))
      pending.write(struct.pack("<q", modified_msec))

      # Write variable-length string fields
      pending.write(encode_str(fields[1]))  # name
      pending.write(encode_str(fields[2]))  # asciiName
      pending.write(encode_str(fields[3]))  # alternateNames
      pending.write(encode_str(fields[6]))  # featureClass
      pending.write(encode_str(fields[7]))  # featureCode
      pending.write(encode_str(fields[8]))  # countryCode
      pending.write(encode_str(fields[9]))  # cc2
      pending.write(encode_str(fields[10]))  # admin1
      pending.write(encode_str(fields[11]))  # admin2
      pending.write(encode_str(fields[12]))  # admin3
      pending.write(encode_str(fields[13]))  # admin4
      pending.write(encode_str(fields[17]))  # timezone

      pending_doc_count += 1
      doc_count += 1

      if pending.tell() > CHUNK_SIZE_THRESHOLD:
        flush(pending, pending_doc_count, f_out)
        pending = io.BytesIO()
        pending_doc_count = 0

      if doc_count % 1_000_000 == 0:
        print(f"  {doc_count} docs converted...")

    # Flush remaining
    if pending_doc_count > 0:
      flush(pending, pending_doc_count, f_out)

  print(f"Done. {doc_count} docs written to {output_path}")


if __name__ == "__main__":
  main()
