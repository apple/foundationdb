/*
 * IDisk.cpp
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

#include "IDisk.h"
#include "fdbrpc/simulator.h"
#include "flow/FastRef.h"
#include "flow/Knobs.h"
//#include "flow/actorcompiler.h" // This must be the last #include.

struct SimSSD : public IDisk, public ReferenceCounted<SimSSD> {
	SimSSD(int64_t iops, int64_t bandwidth) :
        nextOperation(0),
        iops(iops),
        bandwidth(bandwidth) {}
	void addref() override { ReferenceCounted<SimSSD>::addref(); }
	void delref() override { ReferenceCounted<SimSSD>::delref(); }
    Future<Void> override waitUntilDiskReady(int64_t size, bool sync) {
        if (g_simulator.connectionFailuresDisableDuration > 1e4)
            return delay(0.0001);
        if (nextOperation < now()) nextOperation = now();
        nextOperation += (1.0 / iops) + (size / bandwidth);

        double randomLatency;
        if (sync) {
            randomLatency = .005 + g_random->random01() * (BUGGIFY ? 1.0 : 0.010);
        } else
            randomLatency = 10 * g_random->random01() / iops;

        return delayUntil(nextOperation + randomLatency);
    }
    Future<int> override read(int h, void* data, int length) {
        return ::read(h, data, static_cast<unsigned int>(length));
    }
    Future<int> override write(int h, StringRef data) {
        return ::write(h, static_cast<const void*>(data.begin()), data.size());
    }

private:
    double nextOperation;
    int64_t iops;
    int64_t bandwidth;
};

struct NeverDisk : public IDisk, public ReferenceCounted<NeverDisk> {
    void addref() override { ReferenceCounted<NeverDisk>::addref(); }
    void delref() override { ReferenceCounted<NeverDisk>::delref(); }
    Future<Void> override waitUntilDiskReady(int64_t size, bool sync) {
        return Never();
    }
    Future<int> override read(int h, void* data, int length) {
        return ::read(h, data, static_cast<unsigned int>(length));
    }
    Future<int> override write(int h, StringRef data) {
        return ::write(h, static_cast<const void*>(data.begin()), data.size());
    }
};

Reference<IDisk> createSimSSD() {
    return Reference<IDisk>(new SimSSD(FLOW_KNOBS->SIM_DISK_IOPS, FLOW_KNOBS->SIM_DISK_BANDWIDTH));
}
Reference<IDisk> createNeverDisk() {
    return Reference<IDisk>(new NeverDisk());
}
