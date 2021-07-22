/*
 * DropConnections.actor.cpp
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

#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/WorkerInterface.actor.h"

#include <regex>
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

struct DropConnections : TestWorkload {
	static const std::string DESCRIPTION;
	double enableAfter = -1.0;
	double disableAfter = -1.0;
	Value processClass = LiteralStringRef("");
	Value processRegex = LiteralStringRef(".*");

	DropConnections(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enableAfter = getOption(options, LiteralStringRef("enableAfter"), enableAfter);
		disableAfter = getOption(options, LiteralStringRef("enableAfter"), disableAfter);
		processClass = getOption(options, LiteralStringRef("processClass"), processClass);
		processRegex = getOption(options, LiteralStringRef("processRegex"), processRegex);
	}

	std::string description() override { return DESCRIPTION; }
	Future<Void> setup(Database const&) {
		ASSERT(!g_network->isSimulated()); // currently doesn't work in simulation as this is a basic simulation feature
		return Void();
	}

	ACTOR static Future<Void> doToggle(DropConnections* self, Database cx, double after, bool setTo) {
		state std::vector<Future<Void>> replies;
		state std::vector<WorkerInterface> workers;
		if (after < 0.0) {
			return Void();
		}
		wait(delay(after));
		std::vector<WorkerDetails> allWorkers =
		    wait(self->dbInfo->get().clusterInterface.getWorkers.getReply(GetWorkersRequest{}));
		bool filterByRole = self->processClass != LiteralStringRef("");
		ProcessClass filterRole;
		if (filterByRole) {
			filterRole = ProcessClass(self->processClass.toString(), ProcessClass::CommandLineSource);
		}
		std::regex ipFilter{ self->processRegex.toString() };
		for (const auto& worker : allWorkers) {
			if (worker.processClass == ProcessClass::TesterClass) {
				continue;
			}
			if (filterByRole && filterRole.classType() == worker.processClass.classType()) {
				workers.emplace_back(worker.interf);
				continue;
			}
			std::smatch smatch;
			std::string address = worker.interf.waitFailure.getEndpoint().addresses.address.toString();
			if (std::regex_match(address, smatch, ipFilter)) {
				workers.emplace_back(worker.interf);
			}
		}
		for (auto& worker : workers) {
			SetFailureInjection req;
			req.injectNetworkFailures = setTo;
			replies.emplace_back(worker.clientInterface.setFailureInjection.getReply(req));
		}
		wait(waitForAllReady(replies));
		for (int i = 0; i < replies.size(); ++i) {
			auto& reply = replies[i];
			if (reply.isError()) {
				Error err = reply.getError();
				if (err.code() == error_code_client_invalid_operation) {
					TraceEvent(SevError, "ChaosDisabled")
					    .detail("OnEndpoint", workers[i].waitFailure.getEndpoint().addresses.address);
				} else {
					TraceEvent(SevError, "NetworkFailureError").error(err, true);
				}
			}
		}
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		std::vector<Future<Void>> res;
		res.emplace_back(doToggle(this, cx, enableAfter, true));
		res.emplace_back(doToggle(this, cx, disableAfter, false));
		return waitForAll(res);
	}
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(vector<PerfMetric>& m) override {}
};

const std::string DropConnections::DESCRIPTION = "DropConnections";

} // namespace

WorkloadFactory<DropConnections> RandomCloggingWorkloadFactory(DropConnections::DESCRIPTION.c_str());