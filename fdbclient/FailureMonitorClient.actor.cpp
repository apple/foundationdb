/*
 * FailureMonitorClient.actor.cpp
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

#include "fdbclient/FailureMonitorClient.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbclient/ClusterInterface.h"
#include "flow/actorcompiler.h" // has to be last include

struct FailureMonitorClientState : ReferenceCounted<FailureMonitorClientState> {
	std::set<NetworkAddress> knownAddrs;
	double serverFailedTimeout;

	FailureMonitorClientState() {
		serverFailedTimeout = CLIENT_KNOBS->FAILURE_TIMEOUT_DELAY;
	}
};

ACTOR Future<Void> failureMonitorClientLoop(
	SimpleFailureMonitor* monitor,
	ClusterInterface controller,
	Reference<FailureMonitorClientState> fmState,
	bool trackMyStatus)
{
	state Version version = 0;
	state Future<FailureMonitoringReply> request = Never();
	state Future<Void> nextRequest = delay(0, TaskFailureMonitor);
	state Future<Void> requestTimeout = Never();
	state double before = now();
	state double waitfor = 0;

	for(auto& addr : controller.failureMonitoring.getEndpoint().addresses) {
		monitor->setStatus(addr, FailureStatus(false));
		fmState->knownAddrs.insert(addr);
	}

	//The cluster controller's addresses (controller.failureMonitoring.getEndpoint().addresses) are treated specially because we can declare that it is down independently
	//of the response from the cluster controller. It still needs to be in knownAddrs in case the cluster controller changes, so the next cluster controller resets its state

	try {
		loop {
			choose {
				when( FailureMonitoringReply reply = wait( request ) ) {
					g_network->setCurrentTask(TaskDefaultDelay);
					request = Never();
					requestTimeout = Never();
					if (reply.allOthersFailed) {
						// Reset all systems *not* mentioned in the reply to the default (failed) state
						for(auto& addr : controller.failureMonitoring.getEndpoint().addresses) {
							fmState->knownAddrs.erase( addr );
						}

						std::set<NetworkAddress> changedAddresses;
						for(int c=0; c<reply.changes.size(); c++) {
							for(auto& addr : reply.changes[c].addresses) {
								changedAddresses.insert( addr );
							}
						}
						for(auto it : fmState->knownAddrs)
							if (!changedAddresses.count( it ))
								monitor->setStatus( it, FailureStatus() );
						fmState->knownAddrs.clear();
					} else {
						ASSERT( version != 0 );
					}

					if( monitor->getState( controller.failureMonitoring.getEndpoint() ).isFailed() )
						TraceEvent("FailureMonitoringServerUp").detail("OldServer",controller.id());

					for(auto& addr : controller.failureMonitoring.getEndpoint().addresses) {
						monitor->setStatus( addr, FailureStatus(false) );
						fmState->knownAddrs.insert( addr );
					}

					//if (version != reply.failureInformationVersion)
					//	printf("Client '%s': update from %lld to %lld (%d changes, aof=%d)\n", g_network->getLocalAddress().toString().c_str(), version, reply.failureInformationVersion, reply.changes.size(), reply.allOthersFailed);

					version = reply.failureInformationVersion;
					fmState->serverFailedTimeout = reply.considerServerFailedTimeoutMS * .001;
					for(int c=0; c<reply.changes.size(); c++) {
						//printf("Client '%s': status of '%s' is now '%s'\n", g_network->getLocalAddress().toString().c_str(), reply.changes[c].address.toString().c_str(), reply.changes[c].status.failed ? "Failed" : "OK");
						for(auto& addr : reply.changes[c].addresses) {
							monitor->setStatus( addr, reply.changes[c].status );
							if (reply.changes[c].status != FailureStatus())
								fmState->knownAddrs.insert( addr );
							else
								fmState->knownAddrs.erase( addr );
							ASSERT( addr != controller.failureMonitoring.getEndpoint().getPrimaryAddress() || !reply.changes[c].status.failed );
						}
					}
					before = now();
					waitfor = reply.clientRequestIntervalMS * .001;
					nextRequest = delayJittered( waitfor, TaskFailureMonitor );
				}
				when( wait( requestTimeout ) ) {
					g_network->setCurrentTask(TaskDefaultDelay);
					requestTimeout = Never();
					TraceEvent(SevWarn, "FailureMonitoringServerDown").detail("OldServerID",controller.id());
					for(auto& addr : controller.failureMonitoring.getEndpoint().addresses) {
						monitor->setStatus( addr, FailureStatus(true) );
						fmState->knownAddrs.erase( addr );
					}
				}
				when( wait( nextRequest ) ) {
					g_network->setCurrentTask(TaskDefaultDelay);
					nextRequest = Never();

					double elapsed = now() - before;
					double slowThreshold = .200 + waitfor + FLOW_KNOBS->MAX_BUGGIFIED_DELAY;
					double warnAlwaysThreshold = CLIENT_KNOBS->FAILURE_MIN_DELAY/2;

					if (elapsed > slowThreshold && g_random->random01() < elapsed / warnAlwaysThreshold) {
						TraceEvent(elapsed > warnAlwaysThreshold ? SevWarnAlways : SevWarn, "FailureMonitorClientSlow").detail("Elapsed", elapsed).detail("Expected", waitfor);
					}

					FailureMonitoringRequest req;
					req.failureInformationVersion = version;
					req.addresses = g_network->getLocalAddresses();
					if (trackMyStatus)
						req.senderStatus = FailureStatus(false);
					request = controller.failureMonitoring.getReply( req, TaskFailureMonitor );
					if(!controller.failureMonitoring.getEndpoint().isLocal())
						requestTimeout = delay( fmState->serverFailedTimeout, TaskFailureMonitor );
				}
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_broken_promise)  // broken promise from clustercontroller means it has died (and hopefully will be replaced)
			return Void();
		TraceEvent(SevError, "FailureMonitorClientError").error(e);
		throw;  // goes nowhere
	}
}

ACTOR Future<Void> failureMonitorClient( Reference<AsyncVar<Optional<struct ClusterInterface>>> ci, bool trackMyStatus ) {
	state SimpleFailureMonitor* monitor = static_cast<SimpleFailureMonitor*>( &IFailureMonitor::failureMonitor() );
	state Reference<FailureMonitorClientState> fmState = Reference<FailureMonitorClientState>(new FailureMonitorClientState());

	loop {
		state Future<Void> client = ci->get().present() ? failureMonitorClientLoop(monitor, ci->get().get(), fmState, trackMyStatus) : Void();
		wait( ci->onChange() );
	}
}
