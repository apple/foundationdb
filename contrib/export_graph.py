#!/usr/bin/env python3
#
# export_graph.py
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

import json
import matplotlib.pyplot as plt
import argparse
import ddsketch_calc as dd

# setup cmdline args
parser = argparse.ArgumentParser(description="Graphs DDSketch distribution")
parser.add_argument(
    "-t", "--txn", help="Transaction type (ex: g8ui)", required=True, type=str
)
parser.add_argument("--file", help="Path to ddsketch json", required=True, type=str)
parser.add_argument("--title", help="Title for the graph", required=False, type=str)
parser.add_argument("--savefig", help="Will save the plot to a file if set", type=str)
parser.add_argument("--op", help="Which OP to plot (casing matters)", type=str)
args = parser.parse_args()


# Opening JSON file
f = open(args.file)
data = json.load(f)

# parse json and init sketch
buckets = data[args.t][args.op]["buckets"]
error = data[args.t][args.op]["errorGuarantee"]
sketch = dd.DDSketch(error)

# trim the tails of the distribution
ls = [i for i, e in enumerate(buckets) if e != 0]
actual_data = buckets[ls[0] : ls[-1] + 1]
indices = range(ls[0], ls[-1] + 1)
actual_indices = [sketch.getValue(i) for i in indices]

# configure the x-axis to make more sense
fig, ax = plt.subplots()
ax.ticklabel_format(useOffset=False, style="plain")
plt.plot(actual_indices, actual_data)
plt.xlabel("Latency (in us)")
plt.ylabel("Frequency count")

plt_title = "Title"
if args.title is not None:
    plt_title = args.title
plt.title(plt_title)
plt.xlim([actual_indices[0], actual_indices[-1]])
if args.savefig is not None:
    plt.savefig(args.savefig, format="png")
else:
    plt.show()
