#!/usr/bin/env python3
"""
Shuffle Cohere v3 Wikipedia vectors while keeping all paragraphs of each wiki_id together.

This tool:
1. Builds an index mapping each wiki_id to its byte offsets in the CSV and vector files
2. Shuffles the wiki_id groups
3. Copies contiguous blocks from original files in shuffled order

Usage:
  python3 shuffle_wiki_ids.py <csv_file> <vec_file> <dimensions> <output_csv> <output_vec>
"""

import sys
import random
import time
import os
import subprocess

# STOP_AT = 1000000
# STOP_AT = 4_700_000
STOP_AT = None

ID_PREFIX = "20231101.en_"

def read_exact(f, n_bytes, file_type='file'):
  """read exactly n_bytes from file or raise an exception."""
  data = f.read(n_bytes)
  if len(data) != n_bytes:
    raise RuntimeError(f'failed to read {n_bytes} bytes from {file_type}')
  return data

def add_paragraph_count_column(input_csv, output_csv):
  """read csv file grouping by wiki_id, count paragraphs per wiki_id, and write with paragraph_count column."""
  # get total file size for progress estimation
  total_file_size_bytes = os.path.getsize(input_csv)

  start_time_sec = time.time()
  next_progress_time_sec = start_time_sec + 5

  with open(input_csv, 'rb') as f_in, open(output_csv, 'wb') as f_out:
    # read and write header (add paragraph_count column after id field)
    header_line = f_in.readline().decode('utf-8').rstrip('\r\n')
    header_parts = header_line.split(',', 1)  # split on first comma to get id field
    if len(header_parts) >= 2:
      new_header = header_parts[0] + ',paragraph_count,' + header_parts[1]
    else:
      new_header = header_line + ',paragraph_count'
    header_bytes = (new_header + '\n').encode('utf-8')
    f_out.write(header_bytes)
    header_bytes_len = len(header_bytes)

    # read lines and group by wiki_id
    row_count = 0
    wiki_page_count = 0
    current_wiki_id = None
    buffered_lines = []

    for line in f_in:
      line_str = line.decode('utf-8').rstrip('\r\n')
      parts = line_str.split(',', 1)

      if len(parts) < 2:
        raise RuntimeError(f'invalid csv line: {line_str}')

      full_id = parts[0]
      # extract wiki_id from full_id (format: prefix.language_wiki-id_paragraph-id)
      wiki_id, para_id = split_id(full_id, row_count+len(buffered_lines)+1)

      # if we hit a new wiki_id, write out the buffered lines with their paragraph count
      if current_wiki_id is not None and wiki_id != current_wiki_id:
        para_count = len(buffered_lines)
        for buffered_line in buffered_lines:
          buffered_parts = buffered_line.split(',', 1)
          modified_line = buffered_parts[0] + ',' + str(para_count) + ',' + buffered_parts[1]
          f_out.write((modified_line + '\n').encode('utf-8'))
          row_count += 1

        buffered_lines = []
        wiki_page_count += 1

        now_sec = time.time()
        if now_sec >= next_progress_time_sec:
          elapsed_sec = now_sec - start_time_sec
          pct = 100.0 * f_in.tell() / total_file_size_bytes
          print(f'  CSV: {wiki_page_count} wiki_ids, {row_count} rows ({pct:.1f}%) ({elapsed_sec:.1f} sec)...')
          next_progress_time_sec = now_sec + 5

      current_wiki_id = wiki_id
      buffered_lines.append(line_str)

    # write out the last buffered group
    if buffered_lines:
      para_count = len(buffered_lines)
      for buffered_line in buffered_lines:
        buffered_parts = buffered_line.split(',', 1)
        modified_line = buffered_parts[0] + ',' + str(para_count) + ',' + buffered_parts[1]
        f_out.write((modified_line + '\n').encode('utf-8'))
        row_count += 1
      wiki_page_count += 1

    elapsed_sec = time.time() - start_time_sec
    print(f'  CSV: {wiki_page_count} wiki_ids, {row_count} rows (100.0%) ({elapsed_sec:.1f} sec)')
    return header_bytes_len

def copy_using_write_plan(input_file, output_file, output_size, write_plan, file_type):
  """read input file sequentially and write to output file using random access (shuffled) write plan."""
  total_wiki_page_count = len(write_plan)
  start_time_sec = time.time()
  next_progress_time_sec = start_time_sec + 5

  with open(input_file, 'rb') as f_in, open(output_file, 'wb') as f_out:
    # pre-allocate output file
    os.posix_fallocate(f_out.fileno(), 0, output_size)

    for i, (read_len, write_pos) in enumerate(write_plan):
      # read sequential block from input
      block = read_exact(f_in, read_len, file_type)

      # write to shuffled output position
      f_out.seek(write_pos)
      f_out.write(block)

      now_sec = time.time()
      if now_sec >= next_progress_time_sec:
        pct = 100.0 * (i + 1) / total_wiki_page_count
        elapsed_sec = now_sec - start_time_sec
        print(f'  {file_type}: {i + 1}/{total_wiki_page_count} wiki_ids ({pct:.1f}%) ({elapsed_sec:.1f} sec)...')
        next_progress_time_sec = now_sec + 5

    elapsed_sec = time.time() - start_time_sec
    f_out.seek(0, os.SEEK_END)
    assert f_out.tell() == output_size, f'{f_out.tell()=} {output_size=}'
    print(f'  {file_type}: {total_wiki_page_count}/{total_wiki_page_count} wiki_ids (100.0%) ({elapsed_sec:.1f} sec)')

def split_id(id_str, line_num, id_prefix=ID_PREFIX):
  """Parse wiki_id and paragraph_id from the full ID."""
  if not id_str.startswith(id_prefix):
    raise RuntimeError(f'all wiki_id should start with {id_prefix} but saw {id_str} at row {line_num}')
  tup = id_str[len(id_prefix):].split('_')
  if len(tup) != 2:
    raise RuntimeError(f'all wiki_id should have form wiki-id_paragraph-id but saw {id_str[len(id_prefix):]} at row {line_num}')
  # TODO: should we further valdiate \d+ for each?  coalesced correctly ("see once" each wiki_id)
  return tup[0], tup[1]  # wiki_id, paragraph_id

def build_index(csv_file, vec_file, dimensions):
  """
  Build index mapping wiki_id -> (csv_start_byte, csv_end_byte, vec_start_byte, vec_end_byte, paragraph_count)

  Returns:
    - wiki_id_index: dict mapping wiki_id to file offsets
    - wiki_ids_in_order: list of wiki_ids in file order
  """
  wiki_id_index = {}
  wiki_ids_in_order = []

  vector_size_bytes = dimensions * 4
  row_count = 0
  current_wiki_id = None
  csv_start_byte = 0
  vec_start_byte = 0
  paragraph_count = 0

  print(f'Building index of wiki_ids...')
  start_time_sec = time.time()

  # so we print progress right at the start
  next_progress_time_sec = time.time()

  with open(csv_file, 'rb') as f:
    # skip header line
    header = f.readline()
    csv_start_byte = f.tell()
    vec_start_byte = 0

    while True:
      line_start_byte = f.tell()
      line = f.readline()

      if not line:
        # end of file - save last group
        if current_wiki_id is not None:
          vec_end_byte = vec_start_byte + (paragraph_count * vector_size_bytes)
          # line_start_byte is the end of the current wiki_id since it's the start of the next wiki_id
          wiki_id_index[current_wiki_id] = (csv_start_byte, line_start_byte, vec_start_byte, vec_end_byte, paragraph_count)
          wiki_ids_in_order.append(current_wiki_id)
        break

      # decode line and parse csv
      line_str = line.decode('utf-8').rstrip('\r\n')
      # simple csv split - split on first comma to get the id field
      fields = line_str.split(',', 1)
      if not fields:
        continue

      full_id = fields[0]
      wiki_id, paragraph_id = split_id(full_id, row_count + 1)

      if wiki_id != current_wiki_id:
        # new wiki_id group - save the previous one
        if current_wiki_id is not None:
          vec_end_byte = vec_start_byte + (paragraph_count * vector_size_bytes)
          # line_start_byte is the end of the current wiki_id since it's the start of the next wiki_id
          wiki_id_index[current_wiki_id] = (csv_start_byte, line_start_byte, vec_start_byte, vec_end_byte, paragraph_count)
          wiki_ids_in_order.append(current_wiki_id)

          # nocommit
          if STOP_AT is not None and len(wiki_ids_in_order) >= STOP_AT:
            print(f'now {STOP_AT=} {f.tell()=} {line_start_byte=}')
            break

        # start new group
        current_wiki_id = wiki_id
        csv_start_byte = line_start_byte
        vec_start_byte = vec_start_byte + (paragraph_count * vector_size_bytes) if current_wiki_id is not None else 0
        paragraph_count = 0

      paragraph_count += 1
      row_count += 1

      now_sec = time.time()
      if now_sec >= next_progress_time_sec:
        print(f'  {row_count} rows, {len(wiki_id_index)} wiki_ids...')
        next_progress_time_sec = now_sec + 5

  elapsed_sec = time.time() - start_time_sec
  print(f'Built index in {elapsed_sec:.1f} sec: {len(wiki_id_index)} wiki_ids, {row_count} rows (100.0%)')

  return wiki_id_index, wiki_ids_in_order

def shuffle_and_copy(header_bytes_len, csv_file, vec_file, output_csv, output_vec, wiki_id_index, wiki_ids_in_order, dimensions):
  """
  Shuffle wiki_ids using write-side random access.

  Strategy:
  1. Calculate total output file sizes
  2. Pre-allocate output files with fallocate
  3. Build a write plan (shuffled wiki_id order with output positions)
  4. Read input files sequentially, write to shuffled positions
  """
  vector_size_bytes = dimensions * 4

  # calculate total output sizes
  total_csv_size = sum(wiki_id_index[wid][1] - wiki_id_index[wid][0] for wid in wiki_ids_in_order)
  total_vec_size = sum(wiki_id_index[wid][3] - wiki_id_index[wid][2] for wid in wiki_ids_in_order)

  # since we are purely shuffling, the sizes should not change
  if STOP_AT is None:
    assert total_vec_size == os.path.getsize(vec_file), f'{total_vec_size=} {os.path.getsize(vec_file)=}'
    assert header_bytes_len + total_csv_size == os.path.getsize(csv_file), f'{total_csv_size=} {os.path.getsize(csv_file)=}'

  # shuffle wiki_ids with fixed seed for reproducibility
  print(f'Shuffling {len(wiki_ids_in_order)} wiki_ids...')
  r = random.Random(42 * 17)
  wiki_ids_in_shuffled_order = list(wiki_ids_in_order)
  r.shuffle(wiki_ids_in_shuffled_order)

  # build mapping from input position to output position
  csv_input_to_output = {}  # csv_start -> output_pos
  vec_input_to_output = {}  # vec_start -> output_pos
  output_csv_pos = 0
  output_vec_pos = 0

  print(f'{len(wiki_ids_in_shuffled_order)} and {len(wiki_ids_in_shuffled_order)} wiki ids')

  for wiki_id in wiki_ids_in_shuffled_order:
    csv_start, csv_end, vec_start, vec_end, para_count = wiki_id_index[wiki_id]
    csv_len = csv_end - csv_start
    vec_len = vec_end - vec_start

    csv_input_to_output[csv_start] = output_csv_pos
    vec_input_to_output[vec_start] = output_vec_pos

    output_csv_pos += csv_len
    output_vec_pos += vec_len

  # build write plans in input order: for each wiki_id in original order, record (len, write_pos)
  csv_write_plan = []  # list of (len, write_pos)
  vec_write_plan = []  # list of (len, write_pos)

  for wiki_id in wiki_ids_in_order:
    csv_start, csv_end, vec_start, vec_end, para_count = wiki_id_index[wiki_id]
    csv_len = csv_end - csv_start
    vec_len = vec_end - vec_start

    pos = csv_input_to_output[csv_start]
    assert pos + csv_len <= output_csv_pos
    csv_write_plan.append((csv_len, pos))

    pos = vec_input_to_output[vec_start]
    assert pos + vec_len <= output_vec_pos
    vec_write_plan.append((vec_len, pos))

  print(f'Reading input files sequentially and writing to shuffled positions...')
  overall_start_time_sec = time.time()

  copy_using_write_plan(csv_file, output_csv, total_csv_size, csv_write_plan, 'CSV')
  copy_using_write_plan(vec_file, output_vec, total_vec_size, vec_write_plan, 'VEC')

  elapsed_sec = time.time() - overall_start_time_sec
  print(f'Shuffled files written in {elapsed_sec:.1f} sec')

def main():
  if len(sys.argv) != 6:
    print(f'Usage: {sys.argv[0]} <csv_file> <vec_file> <dimensions> <output_csv> <output_vec>')
    sys.exit(1)

  csv_file = sys.argv[1]
  vec_file = sys.argv[2]
  dimensions = int(sys.argv[3])
  output_csv = sys.argv[4]
  output_vec = sys.argv[5]

  # first: add paragraph_count column to csv and save as temp file
  print(f'Adding paragraph_count column to CSV...')
  temp_csv = output_csv + '.para_count'
  header_bytes_len = add_paragraph_count_column(csv_file, temp_csv)

  # build index from the temp csv with paragraph count
  wiki_id_index, wiki_ids_in_order = build_index(temp_csv, vec_file, dimensions)

  # shuffle and copy
  shuffle_and_copy(header_bytes_len, temp_csv, vec_file, output_csv, output_vec, wiki_id_index, wiki_ids_in_order, dimensions)

  # clean up temporary file
  os.remove(temp_csv)

  print(f'Done!')
  print(f'  {output_csv} written')
  print(f'  {output_vec} written')

if __name__ == '__main__':
  main()
