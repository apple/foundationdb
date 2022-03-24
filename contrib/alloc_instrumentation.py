#!/usr/bin/env python3
#
# alloc_instrumentation.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

import fileinput
import argparse
from collections import defaultdict

# Processes the stdout produced by defining ALLOC_INSTRUMENTATION_STDOUT in FastAlloc.h

allocs = {} 

class Allocation:
    def __init__(self, size, backtrace):
        self.size = size
        self.backtrace = backtrace

def print_stacks(stack_count, sort_by_count):
    counts = defaultdict(int)
    sizes = defaultdict(int) 
    for id, allocation in allocs.items():
        counts[allocation.backtrace] += 1
        sizes[allocation.backtrace] += allocation.size

    sort_dict = counts if sort_by_count else sizes
    ordered_list = [(val, backtrace) for (backtrace, val) in sort_dict.items()]
    ordered_list.sort()

    if stack_count:
        ordered_list = ordered_list[-stack_count:]

    for size, backtrace in ordered_list:
        print(str.format('bytes={0:<10} count={1:<8} {2}', sizes[backtrace], counts[backtrace], backtrace))
    
    print('-'*80)

def process_line(line, quiet):
    items = line.split('\t')
    if items[0] == 'Alloc':
        allocs[items[1]] = Allocation(size=int(items[2]), backtrace=items[3])
    elif items[0] == 'Dealloc': 
        allocs.pop(items[1], None)
    elif not quiet:
        print(line)

def non_negative_int(value_str):
    value = int(value_str)
    if value < 0:
        raise argparse.ArgumentTypeError("%s is negative" % value)
    return value

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Parses the output from enabling ALLOC_INSTRUMENTATION in FoundationDB and reports information about the top memory users.')
    parser.add_argument('input_file', type=str, help='Path to file(s) containing the output from a run of FoundationDB with ALLOC_INSTRUMENTATION enabled. If not specified, stdin will be used.', default='-', nargs='*')
    parser.add_argument('-f', '--logging-frequency', type=non_negative_int, help='How frequently the top stacks will be logged, measured in lines of output processed. A value of 0 disables periodic logging. Defaults to 1,000,000.', default=1000000)
    parser.add_argument('-p', '--periodic-stack-count', type=non_negative_int, help='How many stack traces to log when periodically logging output. A value of 0 results in all stacks being logged. Defaults to 15.', default=15)
    parser.add_argument('-s', '--final-stack-count', type=non_negative_int, help='How many stack traces to log when finished processing output. A value of 0 results in all stacks being logged. Defaults to 0.', default=0)
    parser.add_argument('-c', '--sort-by-count', action='store_true', default=False, help='If specified, stacks will be sorted by largest count rather than largest number of bytes.')
    parser.add_argument('-q', '--quiet', action='store_true', default=False, help='If specified, lines from the input file that are not parsable by this tool will not be printed.')

    args = parser.parse_args()

    # Process each line, periodically reporting the top stacks by size
    for line_num, line in enumerate(fileinput.input(args.input_file)):
        process_line(line.rstrip(), args.quiet)
        if args.logging_frequency and line_num and line_num % args.logging_frequency == 0:
            print_stacks(args.periodic_stack_count, args.sort_by_count)

    # Print all stacks 
    print_stacks(args.final_stack_count, args.sort_by_count)
