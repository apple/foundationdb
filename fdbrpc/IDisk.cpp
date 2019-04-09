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

#if defined(_WIN32)
#include <io.h>
#elif defined(__unixish__)
#define _read ::read
#define _write ::write
#endif

struct NormalDisk : public IDisk, public ReferenceCounted<NormalDisk> {
	NormalDisk(int64_t iops, int64_t bandwidth) :
        nextOperation(0),
        iops(iops),
        bandwidth(bandwidth) {}
	void addref() override { ReferenceCounted<NormalDisk>::addref(); }
	void delref() override { ReferenceCounted<NormalDisk>::delref(); }
    Future<Void> override waitUntilDiskReady(int64_t size, bool sync) {
        return waitUntilDiskReady(size, sync, 1);
    }
    Future<int> override read(int h, void* data, int length) {
        return _read(h, data, static_cast<unsigned int>(length));
    }
    Future<int> override write(int h, StringRef data) {
        return _write(h, static_cast<const void*>(data.begin()), data.size());
    }

protected:
    double nextOperation;
    const int64_t iops;
    const int64_t bandwidth;

    Future<Void> override waitUntilDiskReady(int64_t size, bool sync, int numIops) {
        if (g_simulator.connectionFailuresDisableDuration > 1e4)
            return delay(0.0001);
        if (nextOperation < now()) nextOperation = now();
        nextOperation += (numIops / iops) + (size / bandwidth);

        double randomLatency;
        if (sync) {
            randomLatency = .005 + g_random->random01() * (BUGGIFY ? 1.0 : 0.010);
        } else
            randomLatency = 10 * g_random->random01() / iops;

        return delayUntil(nextOperation + randomLatency);
    }
};

struct NeverDisk : public IDisk, public ReferenceCounted<NeverDisk> {
    void addref() override { ReferenceCounted<NeverDisk>::addref(); }
    void delref() override { ReferenceCounted<NeverDisk>::delref(); }
    Future<Void> override waitUntilDiskReady(int64_t size, bool sync) {
        return Never();
    }
    Future<int> override read(int h, void* data, int length) {
        return Never();
    }
    Future<int> override write(int h, StringRef data) {
        return Never();
    }
};

struct EmptyDisk : public NormalDisk, public ReferenceCounted<EmptyDisk> {
    void addref() override { ReferenceCounted<EmptyDisk>::addref(); };
    void delref() override { ReferenceCounted<EmptyDisk>::delref(); }
    Future<int> override read(int h, void* data, int length) {
        memset(data, 0, length);
        return length;
    }
};

struct EBSDisk : public NormalDisk, public ReferenceCounted<EBSDisk> {
    static const int constexpr blockSize = 4096;
    static const double constexpr epochLength = 1.0;

    EBSDisk(int64_t iops, int64_t bandwidth) : NormalDisk(iops, bandwidth) {}

    void addref() override { ReferenceCounted<EBSDisk>::addref(); }
    void delref() override { ReferenceCounted<EBSDisk>::delref(); }

    Future<Void> waitUntilDiskReady(int64_t size, bool sync) {
        int numIops = (size % blockSize) ? (size / blockSize + 1) : (size / blockSize);
        return NormalDisk::waitUntilDiskReady(size, sync, numIops);
    }
};

Reference<IDisk> createSimulatedDisk(DiskType diskType) {
    switch(diskType) {
        case DiskType::Normal:
            return Reference<IDisk>(new NormalDisk(FLOW_KNOBS->SIM_DISK_IOPS, FLOW_KNOBS->SIM_DISK_BANDWIDTH));
        case DiskType::Slow:
            return Reference<IDisk>(new NormalDisk(FLOW_KNOBS->SIM_DISK_IOPS / 1e3, FLOW_KNOBS->SIM_DISK_BANDWIDTH / 1e3));
        case DiskType::Dead:
            return Reference<IDisk>(new NeverDisk());
        case DiskType::EBS:
            return Reference<IDisk>(new EBSDisk(FLOW_KNOBS->SIM_DISK_IOPS, FLOW_KNOBS->SIM_DISK_BANDWIDTH));
        default:
            return Reference<IDisk>();
    }
}
