/*
 * ClogSingleConnection.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class ClogSingleConnectionWorkload : public TestWorkload {
	double delaySeconds;
	Optional<double> clogDuration; // If empty, clog forever

public:
	static constexpr auto NAME = "ClogSingleConnection";
	ClogSingleConnectionWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		auto minDelay = getOption(options, "minDelay"_sr, 0.0);
		auto maxDelay = getOption(options, "maxDelay"_sr, 10.0);
		ASSERT_LE(minDelay, maxDelay);
		delaySeconds = minDelay + deterministicRandom()->random01() * (maxDelay - minDelay);
		if (hasOption(options, "clogDuration"_sr)) {
			clogDuration = getOption(options, "clogDuration"_sr, "");
		}
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (g_network->isSimulated() && clientId == 0) {
			return map(delay(delaySeconds), [this](Void _) {
				clogRandomPair();
				return Void();
			});
		} else {
			return Void();
		}
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	void clogRandomPair() {
		auto m1 = deterministicRandom()->randomChoice(g_simulator->getAllProcesses());
		auto m2 = deterministicRandom()->randomChoice(g_simulator->getAllProcesses());
		if (m1->address.ip != m2->address.ip) {
			g_simulator->clogPair(m1->address.ip, m2->address.ip, clogDuration.orDefault(10000));
		}
	}
};

WorkloadFactory<ClogSingleConnectionWorkload> ClogSingleConnectionWorkloadFactory;
