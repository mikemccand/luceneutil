import array
import os
import random
import subprocess
import sys
import time


def main():
  filename_in = sys.argv[1]
  filename_out = sys.argv[2]

  # print('dropping all IO caches...')
  # subprocess.run('sudo sync', shell=True, check=True)
  # subprocess.run('sudo bash -c "echo 3 > /proc/sys/vm/drop_caches"', shell=True, check=True)
  # print('  done')

  record_size = 768 * 4
  size_in_bytes = os.path.getsize(filename_in)
  if size_in_bytes % record_size != 0:
    raise RuntimeError(f"input file size is {size_in_bytes} but that is not an even multiple of {record_size}")
  n_records = size_in_bytes // record_size

  print(f"{n_records=}")

  # shuffle during read is slower (855.4 sec vs 394.6 sec on fast SSD for
  # 33.3M vectors / 96 GB, because it's blocking IO, and we are single threaded,
  # whereas shuffle on write is not blocking (OS manages moving dirty bytes to disk
  # in the background, but the file fragmentation might be crazy?)
  shuffle_on_read = False

  # Generate shuffled positions
  r = random.Random(42 * 17)
  if False:
    positions = list(range(n_records))
    r.shuffle(positions)
  else:
    positions = array.array("i", range(n_records))
    r.shuffle(positions)
  print("  done shuffle block ids")

  # Sequential read, random write
  count = 0
  next_pct = 1
  with open(filename_in, "rb") as fin, open(filename_out, "wb") as fout:
    # Pre-allocate output file (Linux-specific, actually allocates space)
    os.posix_fallocate(fout.fileno(), 0, n_records * record_size)
    print("  done fallocate")

    t0 = time.time()
    for pos in positions:
      spot = pos * record_size
      if shuffle_on_read:
        # read random spot, append on write
        fin.seek(spot)
        fout.write(fin.read(record_size))
      else:
        # read sequentially, seek+write randomly
        record = fin.read(record_size)
        fout.seek(spot)
        fout.write(record)

      count += 1
      if count >= next_pct * (n_records / 100):
        print(f"{time.time() - t0:6.1f} sec: {next_pct}%...")
        next_pct += 1

    t1 = time.time()
    print(f"took {t1 - t0:.1f} sec")


if __name__ == "__main__":
  main()
