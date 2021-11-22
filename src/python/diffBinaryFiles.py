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

import struct
import sys
import datetime
import io

def read_exact_byte_count(file_name, f, byte_count):
    b = f.read(byte_count)
    if len(b) != byte_count:
        raise RuntimeError(f'premature eof in {file_name}: expected to read {byte_count} bytes but only got {len(b)}')
    return b

def unpack(file_name, fmt, f):
    return struct.unpack(fmt, read_exact_byte_count(file_name, f, struct.calcsize(fmt)))

def read_docs(file_name, with_random_label):

    with open(file_name, 'rb') as f:
        docs_left = 0

        while True:
            if docs_left == 0:
                # read next chunk
                fmt = 'ii'
                size = struct.calcsize(fmt)
                b = f.read(size)
                if len(b) == 0:
                    # EOF -- ok
                    break
                elif len(b) != size:
                    raise RuntimeErrro(f'premature eof in {file_name}')
                else:
                    # super duper
                    docs_left, chunk_size = struct.unpack(fmt, b)
                    #print(f'chunk: {docs_left} docs {chunk_size} bytes')
                    pending = io.BytesIO(read_exact_byte_count(file_name, f, chunk_size))

            if docs_left == 0:
                break

            if with_random_label:
                title_len, body_len, random_label_len, time_sec, msec_since_epoch = unpack(file_name, 'iiiil', pending)
                #print(f'title_len {title_len}, body_len {body_len}, random_label_len {random_label_len}')
            else:
                title_len, body_len, msec_since_epoch, time_sec = unpack(file_name, 'iili', pending)
                #print(f'title_len {title_len}, body_len {body_len}')

            title = read_exact_byte_count(file_name, pending, title_len)
            #print(f'got title {title}')
            body = read_exact_byte_count(file_name, pending, body_len)
            if with_random_label:
                random_label = read_exact_byte_count(file_name, pending, random_label_len)
                #yield title.decode('utf-8'), body.decode('utf-8'), random_label.decode('utf-8'), time_sec, msec_since_epoch
                yield title, body, random_label, time_sec, msec_since_epoch
            else:
                #yield title.decode('utf-8'), body.decode('utf-8'), time_sec, msec_since_epoch
                yield title, body, time_sec, msec_since_epoch

            docs_left -= 1

def get(seq, i):
    return seq[i] if i < len(seq) else None
    

file_name1 = sys.argv[1]
file_name2 = sys.argv[2]

source1 = read_docs(file_name1, False)
source2 = read_docs(file_name2, True)

doc_count = 0

for title, body, time_sec, msec_since_epoch in source1:
    #print(f'got source1 title {title}')

    title2, body2, random_label2, time_sec2, msec_since_epoc2 = next(source2)

    if title != title2:
        raise RuntimeError(f'doc {doc_count}: titles differ:\n  {title}\n  {title2}')

    # the Random Facet Label PR lost trailing whitespace in the body text in createLineFileDocsWithRandomLabel.py:
    body = body.rstrip()
    body2 = body2.rstrip()

    if body != body2:
        print(f'doc {doc_count}: title {title}')
        for i in range(max(len(body), len(body2))):
            print(f'{i}: {get(body, i)}, {get(body2, i)}')
        raise RuntimeError(f'doc {doc_count}: bodies differ ({len(body)} bytes vs {len(body2)} bytes):\n  {body}\n  {body2}')

    doc_count += 1
