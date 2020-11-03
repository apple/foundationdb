/*
 * Histogram.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <flow/Histogram.h>
#include <flow/Trace.h>
// TODO: remove dependency on fdbrpc.

// we need to be able to check if we're in simulation so that the histograms are properly
// scoped to the right "machine".
// either we pull g_simulator into flow, or flow (and the I/O path) will be unable to log performance
// metrics.
#include <fdbrpc/simulator.h>

// pull in some global pointers too:  These types are implemented in fdbrpc/sim2.actor.cpp, which is not available here.  Yuck.
// If you're not using the simulator, these will remain null, and all should be well.

// TODO: create a execution context abstraction that allows independent flow instances within a process.
// The simulator would be the main user of it, and histogram would be the only other user (for now).
ISimulator* g_pSimulator = nullptr;
thread_local ISimulator::ProcessInfo* ISimulator::currentProcess = nullptr;

// Fallback registry when we're not in simulation -- if we had execution contexts we wouldn't need to check if
// we have a simulated contex here; we'd just use the current context regardless.
static HistogramRegistry * globalHistograms = nullptr;

HistogramRegistry & GetHistogramRegistry() {
    ISimulator::ProcessInfo * info = g_simulator.getCurrentProcess();

    if (info) {
        // in simulator; scope histograms to simulated process
        return info->histograms;
    }
    // avoid link order issues where the registry hasn't been initialized, but we're
    // instantiating a histogram
    if (globalHistograms == nullptr) {
        globalHistograms = new HistogramRegistry();
    }
    return *globalHistograms;
}

void HistogramRegistry::registerHistogram(Histogram * h) {
    if (histograms.find(h->name()) != histograms.end()) {
        TraceEvent(SevError, "HistogramDoubleRegistered")
            .detail("group", h->group)
            .detail("op", h->op);
        ASSERT(false);
    }
    histograms.insert(std::pair<std::string, Histogram*>(h->name(), h));
}

void HistogramRegistry::unregisterHistogram(Histogram * h) {
    if (histograms.find(h->name()) == histograms.end()) {
        TraceEvent(SevError, "HistogramNotRegistered")
            .detail("group", h->group)
            .detail("op", h->op);
    }
    ASSERT(histograms.erase(h->name()) == 1);
}

void HistogramRegistry::logReport() {
    for (auto & i : histograms) {
        i.second->writeToLog();
        i.second->clear();
    }
}

void Histogram::writeToLog() {
    bool active = false;
    for (uint32_t i = 0; i < 32; i++) {
        if (buckets[i]) {
            active = true;
            break;
        }
    }
    if (!active) {
        return ;
    }

    TraceEvent e(SevInfo, "Histogram");
    e.detail("Group", group).detail("Op", op);
    for (uint32_t i = 0; i < 32; i++) {
        if (buckets[i]) {
            switch(unit) {
            case Unit::microseconds:
            {
                uint32_t usec = ((uint32_t)1)<<i;
                e.detail(format("%u.%03u", usec / 1000, usec % 1000), buckets[i]);
                break;
            }
            case Unit::bytes:
                e.detail(format("%u", ((uint32_t)1)<<i), buckets[i]);
                break;
            default:
                ASSERT(false);
            }
        }
    }
}
