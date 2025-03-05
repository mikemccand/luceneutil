import sys

import numpy as np


def calculate_statistics(file):
  np_array = np.fromfile(file, dtype=np.float32)
  percentiles = [1, 10, 50, 90, 99, 100]
  for percentile in percentiles:
    print(percentile, "Percentile = ", np.percentile(np_array, percentile))
  print("average: " + str(np.average(np_array)))
  print("stddev: " + str(np.std(np_array)))
  print("min .. max: " + str(np.min(np_array)) + " .. " + str(np.max(np_array)))


if __name__ == "__main__":
  with open(sys.argv[1], "rb") as inp:
    calculate_statistics(inp)
