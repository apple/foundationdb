/*
 * HealthMonitorClient.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/HealthMonitorClient.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbclient/ClusterInterface.h"
#include "flow/actorcompiler.h" // has to be last include
#include <unordered_set>

struct HealthMonitorClientState : ReferenceCounted<HealthMonitorClientState> {
	HealthMonitorClientState() { }
};

ACTOR Future<Void> healthMonitorClientLoop(ClusterInterface controller, Reference<HealthMonitorClientState> hmState) {
	state Version version = 0;
	state Future<HealthMonitoringReply> request = Never();
	state Future<Void> nextRequest = delay(0, TaskPriority::FailureMonitor);
	state Future<Void> requestTimeout = Never();
	state double before = now();
	state double waitfor = 0;

	state int CLIENT_REQUEST_FAILED_TIMEOUT_SECS = 2; /* seconds */
	try {
		loop {
			choose {
				when(HealthMonitoringReply reply = wait(request)) {
					g_network->setCurrentTask(TaskPriority::DefaultDelay);
					request = Never();
					requestTimeout = Never();
					version = reply.healthInformationVersion;

					before = now();
					waitfor = FLOW_KNOBS->HEALTH_MONITOR_CLIENT_REQUEST_INTERVAL_SECS;
					nextRequest = delayJittered(waitfor, TaskPriority::FailureMonitor);
				}
				when(wait(requestTimeout)) {
					g_network->setCurrentTask(TaskPriority::DefaultDelay);
					requestTimeout = Never();
					TraceEvent(SevWarn, "HealthMonitoringServerDown").detail("OldServerID", controller.id());
				}
				when(wait(nextRequest)) {
					g_network->setCurrentTask(TaskPriority::DefaultDelay);
					nextRequest = Never();

					double elapsed = now() - before;
					double slowThreshold = .200 + waitfor + FLOW_KNOBS->MAX_BUGGIFIED_DELAY;
					double warnAlwaysThreshold = CLIENT_KNOBS->FAILURE_MIN_DELAY / 2;

					if (elapsed > slowThreshold && deterministicRandom()->random01() < elapsed / warnAlwaysThreshold) {
						TraceEvent(elapsed > warnAlwaysThreshold ? SevWarnAlways : SevWarn, "HealthMonitorClientSlow")
						    .detail("Elapsed", elapsed)
						    .detail("Expected", waitfor);
					}

					std::map<NetworkAddress, int> closedPeers;
					for (const auto& entry : FlowTransport::transport().healthMonitor()->getPeerClosedHistory()) {
						closedPeers[entry.second] += 1;
					}

					HealthMonitoringRequest req;
					req.healthInformationVersion = version;
					req.closedPeers = closedPeers;
					req.peerStatus = FlowTransport::transport().healthMonitor()->getPeerStatus();
					request = controller.healthMonitoring.getReply(req, TaskPriority::FailureMonitor);
					if (!controller.healthMonitoring.getEndpoint().isLocal())
						requestTimeout = delay(CLIENT_REQUEST_FAILED_TIMEOUT_SECS, TaskPriority::FailureMonitor);
				}
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_broken_promise) // broken promise from clustercontroller means it has died (and
		                                           // hopefully will be replaced)
			return Void();
		TraceEvent(SevError, "HealthMonitorClientError").error(e);
		throw; // goes nowhere
	}
}

ACTOR Future<Void> healthMonitorClient(Reference<AsyncVar<Optional<struct ClusterInterface>>> ci) {
	TraceEvent("HealthMonitorStart").detail("IsClient", FlowTransport::transport().isClient());
	if (FlowTransport::transport().isClient()) {
		wait(Never());
	}

	state Reference<HealthMonitorClientState> hmState = Reference<HealthMonitorClientState>(new HealthMonitorClientState());
	loop {
		state Future<Void> client =
		    ci->get().present() ? healthMonitorClientLoop(ci->get().get(), hmState) : Void();
		wait(ci->onChange());
	}
}
