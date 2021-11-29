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
import random

def createLineFileDocsWithRandomLabels(original_file, target_file):
    if not os.path.exists(original_file):
        raise RuntimeError(f'original file {original_file} does not exist?')

    if os.path.exists(target_file):
        raise RuntimeError(f'target file {target_file} exists -- please delete first and retry')

    print(f'Reading from {original_file} and writing to {target_file}')

    with open(original_file, 'r', encoding='utf-8') as f:
        line = f.readline()
        original_has_header = line.startswith('FIELDS_HEADER_INDICATOR###')

    print(f'input has header?={original_has_header}')

    # alas, we have UTF-8 bugs in some of our line docs files, so I had to use errors='replace' below:
    with open(original_file, 'r', encoding='utf-8', errors='replace') as original, open(target_file, 'w', encoding='utf-8') as out:
        i = 0

        # always write header even if input does not have it:
        line_arr = ['FIELDS_HEADER_INDICATOR###', 'doctitle', 'docdate', 'body', 'RandomLabel']
        out.write('\t'.join(line_arr) + '\n')

        skip_header = original_has_header
        
        for line in original:

            # Lucene line docs files always terminate each line with newline, so we strip just that
            # one newline and no other interesting trailing whitespace that's part of the body of
            # each doc:
            line = line[:-1]
            
            line_arr = line.split('\t')

            if skip_header:
                if line_arr != ['FIELDS_HEADER_INDICATOR###', 'doctitle', 'docdate', 'body']:
                    # prolly only succeeds on Unix-like filesystems:
                    os.remove(target_file)
                    raise RuntimeError(f'unsuported header: {line_arr} -- expected [FIELDS_HEADER_INDICATOR###, doctitle, docdate, body]')
                skip_header = False
                # we already (optimistically) pre-wrote the header above
                continue

            if len(line_arr) != 3:
                raise RuntimeError(f'can only handle line file docs with three entries (title, date, body); saw {len(line_arr)} entries: {line_arr}')

            body = line_arr[2]

            if isNotEmpty(body):
                random_label = chooseRandomLabel(body)
            else:
                random_label = "EMPTY_LABEL"
            line_arr.append(random_label)
            out.write('\t'.join(line_arr) + '\n')
            i += 1
            if i % 100000 == 0:
                print("Converted ", i, " line file docs")
        print("Indexed ", i, " total documents")

def chooseRandomLabel(body):
    body_arr = list(filter(isNotEmpty, re.split(r'\W', body)))
    if len(body_arr) == 0:
        print(f'WARNING: empty body: {repr(body)}')
        return 'EMPTY_LABEL'
    label = None
    i = 0
    while not isNotEmpty(label) and i < 5:
        label = random.choice(body_arr)
    if not isNotEmpty(label):
        return 'EMPTY_LABEL'
    else:
        return label

def isNotEmpty(str):
    return str and str.strip()

if __name__ == '__main__':
    if '-help' in sys.argv or '--help' in sys.argv:
        print('Usage: python createLineFileDocsWithRandomLabel.py file-name-in file-name-out [--help]')
    else:
        if len(sys.argv) == 3:
            ORIGINAL_LINEFILE = sys.argv[1]
            TARGET_LINEFILE = sys.argv[2]
            createLineFileDocsWithRandomLabels(ORIGINAL_LINEFILE, TARGET_LINEFILE)
        else:
            print("Invalid arguments")
            print('Usage: python createLineFileDocsWithRandomLabel.py file-name-in file-name-out [--help]')
