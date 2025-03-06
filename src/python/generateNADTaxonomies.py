#!/usr/bin/env python

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

# source data: https://www.transportation.gov/gis/national-address-database

import gzip
import os
from zipfile import ZipFile

STATE_INDEX = 1
COUNTY_INDEX = 2
STREETNAME_INDEX = 15
ADDNAME_INDEX = 20

with ZipFile("../../../data/NAD_r8_TXT.zip", "r") as zip:
  with zip.open("TXT/NAD_r8.txt", "r") as nad:
    with gzip.open("../../../data/NAD_taxonomy.txt.gz", "wb") as out:
      i = 0
      skipped_lines = 0
      for line in nad:
        if i % 100000 == 0:
          print("Processed ", i, " lines")
        str_line = line.decode("utf-8")
        line_arr = str_line.split(",")
        if len(line_arr) < 21:
          skipped_lines += 1
          continue
        hierarchy = (line_arr[STATE_INDEX], line_arr[COUNTY_INDEX], line_arr[STREETNAME_INDEX], line_arr[ADDNAME_INDEX])
        if hierarchy[2] == "":
          hierarchy = (hierarchy[0], hierarchy[1], "NONE", hierarchy[3])
        if hierarchy[3] == "":
          hierarchy = (hierarchy[0], hierarchy[1], hierarchy[2], "NONE")
        str_hierarchy = hierarchy[0] + "," + hierarchy[1] + "," + hierarchy[2] + "," + hierarchy[3] + "\n"
        out.write(str_hierarchy.encode("utf8"))
        i += 1
      print("Read", i, "lines")
      print("Skipped", skipped_lines, "lines")
      print("NAD_taxonomy.txt.gz contains", os.stat("../../../data/NAD_taxonomy.txt.gz").st_size, "bytes of compressed data")
