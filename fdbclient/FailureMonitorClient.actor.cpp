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
#include <unordered_set>

struct FailureMonitorClientState : ReferenceCounted<FailureMonitorClientState> {
	std::unordered_set<NetworkAddress> knownAddrs;
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
	state Future<Void> nextRequest = delay(0, TaskPriority::FailureMonitor);
	state Future<Void> requestTimeout = Never();
	state double before = now();
	state double waitfor = 0;

	state NetworkAddressList controlAddr = controller.failureMonitoring.getEndpoint().addresses;
	monitor->setStatus(controlAddr.address, FailureStatus(false));
	fmState->knownAddrs.insert(controlAddr.address);
	if(controlAddr.secondaryAddress.present()) {
		monitor->setStatus(controlAddr.secondaryAddress.get(), FailureStatus(false));
		fmState->knownAddrs.insert(controlAddr.secondaryAddress.get());
	}

	//The cluster controller's addresses (controller.failureMonitoring.getEndpoint().addresses) are treated specially because we can declare that it is down independently
	//of the response from the cluster controller. It still needs to be in knownAddrs in case the cluster controller changes, so the next cluster controller resets its state

	try {
		loop {
			choose {
				when( FailureMonitoringReply reply = wait( request ) ) {
					g_network->setCurrentTask(TaskPriority::DefaultDelay);
					request = Never();
					requestTimeout = Never();
					if (reply.allOthersFailed) {
						// Reset all systems *not* mentioned in the reply to the default (failed) state
						fmState->knownAddrs.erase( controller.failureMonitoring.getEndpoint().addresses.address );
						if(controller.failureMonitoring.getEndpoint().addresses.secondaryAddress.present()) {
							fmState->knownAddrs.erase( controller.failureMonitoring.getEndpoint().addresses.secondaryAddress.get() );
						}

						std::set<NetworkAddress> changedAddresses;
						for(int c=0; c<reply.changes.size(); c++) {
							changedAddresses.insert( reply.changes[c].addresses.address );
							if(reply.changes[c].addresses.secondaryAddress.present()) {
								changedAddresses.insert( reply.changes[c].addresses.secondaryAddress.get() );
							}
						}
						for(auto& it : fmState->knownAddrs)
							if (!changedAddresses.count( it ))
								monitor->setStatus( it, FailureStatus() );
						fmState->knownAddrs.clear();
					} else {
						ASSERT( version != 0 );
					}

					if( monitor->getState( controller.failureMonitoring.getEndpoint() ).isFailed() )
						TraceEvent("FailureMonitoringServerUp").detail("OldServer",controller.id());

					monitor->setStatus(controlAddr.address, FailureStatus(false));
					fmState->knownAddrs.insert(controlAddr.address);
					if(controlAddr.secondaryAddress.present()) {
						monitor->setStatus(controlAddr.secondaryAddress.get(), FailureStatus(false));
						fmState->knownAddrs.insert(controlAddr.secondaryAddress.get());
					}

					//if (version != reply.failureInformationVersion)
					//	printf("Client '%s': update from %lld to %lld (%d changes, aof=%d)\n", g_network->getLocalAddress().toString().c_str(), version, reply.failureInformationVersion, reply.changes.size(), reply.allOthersFailed);

					version = reply.failureInformationVersion;
					fmState->serverFailedTimeout = reply.considerServerFailedTimeoutMS * .001;
					for(int c=0; c<reply.changes.size(); c++) {
						//printf("Client '%s': status of '%s' is now '%s'\n", g_network->getLocalAddress().toString().c_str(), reply.changes[c].address.toString().c_str(), reply.changes[c].status.failed ? "Failed" : "OK");
						auto& addrList = reply.changes[c].addresses;
						monitor->setStatus( addrList.address, reply.changes[c].status );
						if(addrList.secondaryAddress.present()) {
							monitor->setStatus( addrList.secondaryAddress.get(), reply.changes[c].status );
						}
						if (reply.changes[c].status != FailureStatus()) {
							fmState->knownAddrs.insert( addrList.address );
							if(addrList.secondaryAddress.present()) {
								fmState->knownAddrs.insert( addrList.secondaryAddress.get() );
							}
						} else {
							fmState->knownAddrs.erase( addrList.address );
							if(addrList.secondaryAddress.present()) {
								fmState->knownAddrs.erase( addrList.secondaryAddress.get() );
							}
						}
					}
					before = now();
					waitfor = reply.clientRequestIntervalMS * .001;
					nextRequest = delayJittered( waitfor, TaskPriority::FailureMonitor );
				}
				when( wait( requestTimeout ) ) {
					g_network->setCurrentTask(TaskPriority::DefaultDelay);
					requestTimeout = Never();
					TraceEvent(SevWarn, "FailureMonitoringServerDown").detail("OldServerID",controller.id());
					monitor->setStatus(controlAddr.address, FailureStatus(true));
					fmState->knownAddrs.erase(controlAddr.address);
					if(controlAddr.secondaryAddress.present()) {
						monitor->setStatus(controlAddr.secondaryAddress.get(), FailureStatus(true));
						fmState->knownAddrs.erase(controlAddr.secondaryAddress.get());
					}
				}
				when( wait( nextRequest ) ) {
					g_network->setCurrentTask(TaskPriority::DefaultDelay);
					nextRequest = Never();

					double elapsed = now() - before;
					double slowThreshold = .200 + waitfor + FLOW_KNOBS->MAX_BUGGIFIED_DELAY;
					double warnAlwaysThreshold = CLIENT_KNOBS->FAILURE_MIN_DELAY/2;

					if (elapsed > slowThreshold && deterministicRandom()->random01() < elapsed / warnAlwaysThreshold) {
						TraceEvent(elapsed > warnAlwaysThreshold ? SevWarnAlways : SevWarn, "FailureMonitorClientSlow").detail("Elapsed", elapsed).detail("Expected", waitfor);
					}

					FailureMonitoringRequest req;
					req.failureInformationVersion = version;
					req.addresses = g_network->getLocalAddresses();
					if (trackMyStatus)
						req.senderStatus = FailureStatus(false);
					request = controller.failureMonitoring.getReply( req, TaskPriority::FailureMonitor );
					if(!controller.failureMonitoring.getEndpoint().isLocal())
						requestTimeout = delay( fmState->serverFailedTimeout, TaskPriority::FailureMonitor );
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
	TraceEvent("FailureMonitorStart").detail("IsClient", FlowTransport::isClient());
	if (FlowTransport::isClient()) {
		wait(Never());
	}

	state SimpleFailureMonitor* monitor = static_cast<SimpleFailureMonitor*>( &IFailureMonitor::failureMonitor() );
	state Reference<FailureMonitorClientState> fmState = Reference<FailureMonitorClientState>(new FailureMonitorClientState());
	auto localAddr = g_network->getLocalAddresses();
	monitor->setStatus(localAddr.address, FailureStatus(false));
	if(localAddr.secondaryAddress.present()) {
		monitor->setStatus(localAddr.secondaryAddress.get(), FailureStatus(false));
	}
	loop {
		state Future<Void> client = ci->get().present() ? failureMonitorClientLoop(monitor, ci->get().get(), fmState, trackMyStatus) : Void();
		wait( ci->onChange() );
	}
}
