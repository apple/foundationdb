#!/usr/bin/env python3
#
# ddsketch_compare.py
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

import argparse
import json
import numpy as np


# kullback-leibler divergence (or relative entropy)
def relative_entropy(p, q):
    difference = 0.0
    for i in range(len(p)):
        if p[i] != 0.0 and q[i] != 0.0:
            difference += p[i] * np.log2(p[i] / q[i])
    return difference


# jensen-shannon divergence (or symmetric relative entropy)
def relative_entropy_symmetric(dd1, dd2):
    # normalize p, q into distribution
    sum1 = sum(dd1)
    sum2 = sum(dd2)

    p = [dd1[i] / sum1 for i in range(len(dd1))]
    q = [dd2[i] / sum2 for i in range(len(dd2))]
    m = [0.5 * (p[i] + q[i]) for i in range(len(p))]

    return 0.5 * relative_entropy(p, m) + 0.5 * relative_entropy(q, m)


# setup cmdline args
parser = argparse.ArgumentParser(description="Compares two DDSketch distributions")
parser.add_argument(
    "--txn1", help="Transaction type for first file", required=True, type=str
)
parser.add_argument(
    "--txn2", help="Transaction type for second file", required=True, type=str
)
parser.add_argument(
    "--file1", help="Path to first ddsketch json", required=True, type=str
)
parser.add_argument(
    "--file2", help="Path to second ddsketch json'", required=True, type=str
)
parser.add_argument("--op", help="Operation name", type=str)
args = parser.parse_args()

f1 = open(args.file1)
f2 = open(args.file2)
data1 = json.load(f1)
data2 = json.load(f2)

if (
    data1[args.txn1][args.op]["errorGuarantee"]
    != data2[args.txn2][args.op]["errorGuarantee"]
):
    print("ERROR: The sketches have different error guarantees and cannot be compared!")
    exit()

b1 = data1[args.txn1][args.op]["buckets"]
b2 = data2[args.txn2][args.op]["buckets"]

re = relative_entropy_symmetric(b1, b2)
print("The similarity is: ", round(re, 8))
print("1 means least alike, 0 means most alike")
