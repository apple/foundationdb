#!/usr/bin/env python3
#
# alloc_instrumentation_traces.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""
Example trace:
{  "Severity": "10", "Time": "194.878474", "DateTime": "2022-02-01T16:28:27Z", "Type": "MemSample", "Machine": "2.1.1.0:2", "ID": "0000000000000000", "Count": "943", "TotalSize": "540000000", "SampleCount": "54", "Hash": "980074757", "Bt": "addr2line -e fdbserver.debug -p -C -f -i 0x1919b72 0x3751d43 0x37518cc 0x19930f8 0x199dac3 0x1999e7c 0x21a1061 0x31e8fc5 0x31e784a 0x10ab3a8 0x36bf4c6 0x36bf304 0x36beea4 0x36bf352 0x36bfa1c 0x10ab3a8 0x37b22fe 0x37a16ee 0x368c754 0x19202d5 0x7fb3fe2d6555 0x1077029", "ThreadID": "10074331651862410074", "LogGroup": "default" }
"""


# This program analyzes MemSample trace events produced by setting ALLOC_INSTRUMENTATION in FastAlloc.h
# It outputs the top memory users by total size as well as number of allocations.

# Example usage: cat trace.* | ./alloc_instrumentation_traces.py

import sys
import json

byCnt = []
bySize = []
totalSize = 0

lastTimestamp = ""

for line in sys.stdin:
  ev = json.loads(line.rstrip())
  type = ev["Type"]
  
  if (type != 'MemSample'):
    continue
  bt = ev["Bt"]

  if (bt == "na"):
    continue

  timestamp = ev["Time"]
  cnt = int(ev["Count"])
  scnt = int(ev["SampleCount"])
  size = int(ev["TotalSize"])
  h = ev["Hash"]

  if (timestamp != lastTimestamp):
    byCnt = []
    bySize = []
    totalSize = 0
    lastTimestamp = timestamp


  # print(str(cnt) + " " + str(scnt) + " " + str(size) + " " + h)

  byCnt.append( (cnt, scnt, size, h, bt) )
  bySize.append( (size, cnt, size, h, bt) )
  totalSize += size

byCnt.sort(reverse=True)
bySize.sort(reverse=True)

btByHash = {}

byte_suffix = ["Bytes", "KB", "MB", "GB", "TB"]
def byte_str(bytes):
  suffix_idx = 0
  while (bytes >= 1024 * 10):
    suffix_idx += 1
    bytes //= 1024
  return str(bytes) + ' ' + byte_suffix[suffix_idx]

print("By Size")
print("-------\r\n")
for x in bySize[:10]:
  # print(str(x[0]) + ": " + x[3])
  print(str(x[1]) + " / " + byte_str(x[0]) + " (" + byte_str(x[0] // x[1]) + " per alloc):\r\n" + x[4] + "\r\n")
  btByHash[x[3]] = x[4]

print()
print("By Count")
print("--------\r\n")
for x in byCnt[:5]:
  # print(str(x[0]) + ": " + x[3])
  print(str(x[0]) + " / " + byte_str(x[2]) + " (" + byte_str(x[2] // x[0]) + " per alloc):\r\n" + x[4] + "\r\n")
  btByHash[x[3]] = x[4]

