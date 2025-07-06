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

import base64
import json
import os
import time
import urllib.request

import constants

# Blunders gives us a nice interactive flame chart from JFRs, published to:
#
#   https://blunders.io/lucene-bench
#
# See also the blog post https://blunders.io/posts/lucene-bench-2021-01-10
#
# Thank you Anton Hägerstrand for making this possible!

BASE_BLUNDERS_URL = "https://api.blunders.io"


def upload(name, label, description, jfr_files):
  if constants.BLUNDER_ACCESS_KEY is None:
    raise RuntimeError("localconstants.BLUNDER_ACCESS_KEY not set; cannot upload")

  # We first initialize an upload, using a label (computer-readable, primary key), name (human readable) and longer description.
  upload_url = init_upload(label, name, description)

  if type(jfr_files) is str:
    jfr_files = [jfr_files]

  t0 = time.time()
  # Sorry about the curl, but couldn't figure out how to stream data in python
  files_string = " ".join(['"%s"' % x for x in jfr_files])
  print(f"Blunders: now uploading files {files_string} to blunders.io...")
  run_command(f"cat {files_string} | gzip -c | curl -H 'content-type: application/octet-stream' -v -XPUT --data-binary @- '{upload_url}'")
  print(f"  took: {time.time() - t0:.2f} seconds")


def delete(label):
  if constants.BLUNDER_ACCESS_KEY is None:
    raise RuntimeError("localconstants.BLUNDER_ACCESS_KEY not set; cannot delete")

  req = urllib.request.Request(f"{BASE_BLUNDERS_URL}/lucene-nightly-bench-jfr/{label}")
  req.method = "DELETE"
  req.add_header("Authorization", blunders_auth_header())
  urllib.request.urlopen(req).read()


def run_command(cmd):
  # print(f"Blunders: now run {cmd}")
  out = os.system(cmd)
  if out:
    raise RuntimeError("command failed: %s" % cmd)


def init_upload(label, name, description):
  data = json.dumps({"label": label, "name": name, "description": description}).encode("utf-8")
  req = urllib.request.Request(f"{BASE_BLUNDERS_URL}/lucene-nightly-bench-jfr/_upload", data)
  req.add_header("Authorization", blunders_auth_header())
  req.add_header("Content-type", "application/json")
  resp = json.loads(urllib.request.urlopen(req).read().decode("utf-8"))
  upload_url = resp["uploadUrl"]
  return upload_url


def blunders_auth_header():
  base64string = base64.b64encode(f"lucene-nightly-bench:{constants.BLUNDER_ACCESS_KEY}".encode()).decode("utf-8")
  authorization = "Basic %s" % base64string
  return authorization
