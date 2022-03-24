/*
 * ProtocolVersion.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last include

struct ProtocolVersionWorkload : TestWorkload {
	ProtocolVersionWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	std::string description() const override { return "ProtocolVersionWorkload"; }

	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	ACTOR Future<Void> _start(ProtocolVersionWorkload* self, Database cx) {
		state std::vector<ISimulator::ProcessInfo*> allProcesses = g_pSimulator->getAllProcesses();
		state std::vector<ISimulator::ProcessInfo*>::iterator diffVersionProcess =
		    find_if(allProcesses.begin(), allProcesses.end(), [](const ISimulator::ProcessInfo* p) {
			    return p->protocolVersion != currentProtocolVersion;
		    });

		ASSERT(diffVersionProcess != allProcesses.end());

		RequestStream<ProtocolInfoRequest> requestStream{ Endpoint::wellKnown({ (*diffVersionProcess)->addresses },
			                                                                  WLTOKEN_PROTOCOL_INFO) };
		ProtocolInfoReply reply = wait(retryBrokenPromise(requestStream, ProtocolInfoRequest{}));

		ASSERT(reply.version != g_network->protocolVersion());
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ProtocolVersionWorkload> ProtocolVersionWorkloadFactory("ProtocolVersion");
