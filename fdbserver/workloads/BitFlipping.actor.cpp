/*
 * BitFlipping.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/QuietDatabase.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct BitFlippingWorkload : TestWorkload {
    bool enabled;
    double testDuration;
    double percentBitFlips;
    BitFlippingWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
        enabled = !clientId; // only do this on the "first" client
        testDuration = getOption(options, LiteralStringRef("testDuration"), 10.0);
        percentBitFlips = getOption(options, LiteralStringRef("percentBitFlips"), 1.0);
        TraceEvent("BitFlippingWorkload")
            .detail("TestDuration", testDuration).detail("Percentage", percentBitFlips);
    }

    std::string description() const override {
        if (&g_simulator == g_network)
            return "BitFlipping";
        else
            return "NoSimBitFlipping";
    }

    Future<Void> setup(Database const& cx) override { return Void(); }

    Future<Void> start(Database const& cx) override {
        if (enabled) {
            return timeout(reportErrors(flipBitsClient<WorkerInterface>(cx, this), "BitFlippingError"),
                           testDuration,
                           Void());
        } else
            return Void();
    }

    Future<bool> check(Database const& cx) override { return true; }

    void getMetrics(vector<PerfMetric>& m) override {}

    static void checkBitFlipResult(Future<Void> res, WorkerInterface worker) {
        if (res.isError()) {
            auto err = res.getError();
            if (err.code() == error_code_client_invalid_operation) {
                TraceEvent(SevError, "ChaosDisabled")
                    .detail("OnEndpoint", worker.waitFailure.getEndpoint().addresses.address.toString());
            } else {
                TraceEvent(SevError, "BitFlippingFailed")
                    .detail("OnEndpoint", worker.waitFailure.getEndpoint().addresses.address.toString())
                    .error(err);
            }
        }
    }

    ACTOR void doBitFlips(WorkerInterface worker, double percentage, double startDelay = 0.0) {
        state Future<Void> res;
        wait(::delay(startDelay));
        SetFailureInjection::FlipBitsCommand flipBits;
        flipBits.percentBitFlips = percentage;
        SetFailureInjection req;
        req.flipBits = flipBits;
        res = worker.clientInterface.setFailureInjection.getReply(req);
        wait(ready(res));
        checkBitFlipResult(res, worker);
    }

    ACTOR static Future<Void> getAllStorageWorkers(Database cx, BitFlippingWorkload* self, std::vector<WorkerInterface>* result) {
        result->clear();
        state std::vector<WorkerInterface> res = wait(getStorageWorkers(cx, self->dbInfo, false));
        for (auto& worker : res) {
            result->emplace_back(worker);
        }
        return Void();
    }

    ACTOR template <class W>
    Future<Void> flipBitsClient(Database cx, BitFlippingWorkload* self) {
        state double lastTime = now();
        state double workloadEnd = now() + self->testDuration;
        state std::vector<W> machines;
        loop {
            wait(poisson(&lastTime, 1));
            wait(BitFlippingWorkload::getAllStorageWorkers(cx, self, &machines));
            auto machine = deterministicRandom()->randomChoice(machines);
            self->doBitFlips(machine, self->percentBitFlips);
        }
    }
};
WorkloadFactory<BitFlippingWorkload> BitFlippingWorkloadFactory("BitFlipping");
