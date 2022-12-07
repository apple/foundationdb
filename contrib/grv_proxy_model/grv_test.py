#!/usr/bin/env python3

#
# grv_test.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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
import inspect
import sys

import rate_model
import workload_model
import proxy_model
import ratekeeper_model
from priority import Priority
from plot import Plotter

parser = argparse.ArgumentParser()
parser.add_argument('-w', '--workload', type=str, help='Name of workload to run')
parser.add_argument('-r', '--ratekeeper', type=str, help='Name of ratekeeper model')
parser.add_argument('-d', '--duration', type=int, default=240, help='Duration of simulated test, in seconds. Defaults to 240.')
parser.add_argument('-L', '--limiter', type=str, default='Original', help='Name of limiter implementation. Defaults to \'Original\'.')
parser.add_argument('-p', '--proxy', type=str, default='ProxyModel', help='Name of proxy implementation. Defaults to \'ProxyModel\'.')
parser.add_argument('--list', action='store_true', default=False, help='List options for all models.')
parser.add_argument('--no-graph', action='store_true', default=False, help='Disable graphical output.')

args = parser.parse_args()


def print_choices_list(context=None):
    if context == 'workload' or context is None:
        print('Workloads:')
        for w in workload_model.predefined_workloads.keys():
            print('  %s' % w)

    if context == 'ratekeeper' or context is None:
        print('\nRatekeeper models:')
        for r in ratekeeper_model.predefined_ratekeeper.keys():
            print('  %s' % r)

    proxy_model_classes = [c for c in [getattr(proxy_model, a) for a in dir(proxy_model)] if inspect.isclass(c)]

    if context == 'proxy' or context is None:
        print('\nProxy models:')
        for p in proxy_model_classes:
            if issubclass(p, proxy_model.ProxyModel):
                print('  %s' % p.__name__)

    if context == 'limiter' or context is None:
        print('\nProxy limiters:')
        for p in proxy_model_classes:
            if issubclass(p, proxy_model.Limiter) and p != proxy_model.Limiter:
                name = p.__name__
                if name.endswith('Limiter'):
                    name = name[0:-len('Limiter')]
                print('  %s' % name)


if args.workload is None or args.ratekeeper is None:
    print('ERROR: A workload (-w/--workload) and ratekeeper model (-r/--ratekeeper) must be specified.\n')
    print_choices_list()
    sys.exit(1)

if args.list:
    print_choices_list()
    sys.exit(0)


def validate_class_type(var, name, superclass):
    cls = getattr(var, name, None)
    return cls is not None and inspect.isclass(cls) and issubclass(cls, superclass)


if args.ratekeeper not in ratekeeper_model.predefined_ratekeeper:
    print('Invalid ratekeeper model `%s\'' % args.ratekeeper)
    print_choices_list('ratekeeper')
    sys.exit(1)

if args.workload not in workload_model.predefined_workloads:
    print('Invalid workload model `%s\'' % args.workload)
    print_choices_list('workload')
    sys.exit(1)

if not validate_class_type(proxy_model, args.proxy, proxy_model.ProxyModel):
    print('Invalid proxy model `%s\'' % args.proxy)
    print_choices_list('proxy')
    sys.exit(1)

limiter_name = args.limiter
if not validate_class_type(proxy_model, limiter_name, proxy_model.Limiter):
    limiter_name += 'Limiter'
    if not validate_class_type(proxy_model, limiter_name, proxy_model.Limiter):
        print('Invalid proxy limiter `%s\'' % args.limiter)
        print_choices_list('limiter')
        sys.exit(1)

ratekeeper = ratekeeper_model.predefined_ratekeeper[args.ratekeeper]
workload = workload_model.predefined_workloads[args.workload]

limiter = getattr(proxy_model, limiter_name)
proxy = getattr(proxy_model, args.proxy)(args.duration, ratekeeper, workload, limiter)

proxy.run()

for priority in workload.priorities():
    latencies = sorted([p for t in proxy.results.latencies[priority].values() for p in t])
    total_started = sum(proxy.results.started[priority].values())
    still_queued = sum([r.count for r in proxy.request_queue if r.priority == priority])

    if len(latencies) > 0:
        print('\n%s: %d requests in %d seconds (rate=%f). %d still queued.' % (priority, total_started, proxy.time, float(total_started) / proxy.time, still_queued))
        print('  Median latency: %f' % latencies[len(latencies) // 2])
        print('  90%% latency: %f' % latencies[int(0.9 * len(latencies))])
        print('  99%% latency: %f' % latencies[int(0.99 * len(latencies))])
        print('  99.9%% latency: %f' % latencies[int(0.999 * len(latencies))])
        print('  Max latency: %f' % latencies[-1])

print('')

if not args.no_graph:
    plotter = Plotter(proxy.results)
    plotter.display()
