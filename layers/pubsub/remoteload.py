#!/usr/bin/python -u
#
# remoteload.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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


import os
import sys

sys.path[:0] = [os.path.join(os.path.dirname(__file__), '..', '..', 'bindings', 'python')]
import fdb
import argparse
from pubsub_bigdoc import PubSub

parser = argparse.ArgumentParser()
parser.add_argument("--zkAddr")
parser.add_argument("--database")
parser.add_argument("--userStart", type=int)
parser.add_argument("--userCount", type=int)
args = parser.parse_args()

# zkAddr = '10.0.3.1:2181/bbc'
# database = 'TwitDB'
# users = 1000
# followers = 10

db = fdb.open(args.zkAddr, args.database)
ps = PubSub(db)

print 'creating users',
for i in range(args.userStart, args.userCount):
    ps.create_inbox_and_feed('%09d' % i)
    if i > 0 and i % 100 == 0:
        print i,
print 'done'

# @fdb.transactional
# def done(tr):
#     tr['/done/%d' % args.userStart] = 'done'
#
# done(db)
