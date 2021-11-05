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

import sys
import os
import re

from random import randrange

USAGE= """
Usage: python createLineFileDocsWithRandomLabel.py

"""

ORIGINAL_LINEFILE = 'enwiki-20120502-lines-1k.txt'
TARGET_LINEFILE = 'enwiki-20120502-lines-1k-with-random-label.txt'

def createLineFileDocsWithRandomLabels(original_file, target_file):
    if original_file is None:
        cwd = os.getcwd()
        parent, base = os.path.split(cwd)
        data_dir = os.path.join(parent, 'data')

        if not os.path.exists(data_dir):
            print('download data before running this script')
            exit()

        original_file = os.path.join(data_dir, ORIGINAL_LINEFILE)
        target_file = os.path.join(data_dir, TARGET_LINEFILE)
    else:
        if not os.path.exist(original_file):
            print("Recieved invalid path to data")
            exit()

    with open(original_file, 'r', encoding='ISO-8859-1') as original, open(target_file, 'w', encoding='ISO-8859-1') as out:
        first_line = True
        skipped = 0
        i = 0
        for line in original:
            if i % 100000 == 0:
                print("Converted ", i, " line file docs")
            line_arr = line.strip().split('\t')
            if first_line:
                line_arr.append('RandomLabel')
                first_line = False
                line_to_write = '\t'.join(line_arr) + '\n'
                out.write(line_to_write)
                continue
            else:
                try:
                    if not isNotEmpty(line_arr[2]):
                        line_arr.append('EMPTY_LABEL')
                        random_label = "EMPTY_LABEL"
                    else:
                        random_label = chooseRandomLabel(line_arr[2])
                        if random_label == "EMPTY_BODY":
                            random_label = "EMPTY_LABEL"
                            line_arr[2] = "EMPTY_LABEL"
                except IndexError:
                    # found a few lines that looked like this: ['Biosensor', '05-APR-2012 04:12:36.000'] with no body
                    line_arr.append('EMPTY_LABEL')
                    random_label = 'EMPTY_LABEL'
                line_arr.append(random_label)
            first_line = False
            line_to_write = '\t'.join(line_arr) + '\n'
            out.write(line_to_write)
            i += 1
        print("Indexed ", i, " total documents")
        print("Skipped ", skipped, " documents")

def chooseRandomLabel(body):
    body_arr = list(filter(isNotEmpty, re.split(r'\W', body)))
    if (len(body_arr) == 0):
        return "EMPTY_BODY"
    label = None
    i = 0
    while not isNotEmpty(label) and i < 5:
        label = body_arr[randrange(len(body_arr))]
    if not isNotEmpty(label):
        return "EMPTY_LABEL"
    else:
        return label

def isNotEmpty(str):
    if str and str.strip():
        return True
    return False

if __name__ == '__main__':
    if '-help' in sys.argv or '--help' in sys.argv:
        print(USAGE)
    else:
        if len(sys.argv) == 3:
            createLineFileDocsWithRandomLabels(sys.argv[1], sys.argv[2])
        elif len(sys.argv) == 1:
            createLineFileDocsWithRandomLabels(None, None)
        else:
            print("Invalid arguments")
            exit()
