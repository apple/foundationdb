/*
 * FailureMonitor.h
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

#ifndef FLOW_FAILUREMONITOR_H
#define FLOW_FAILUREMONITOR_H
#pragma once

#include "flow/flow.h"
#include "fdbrpc/FlowTransport.h" // Endpoint
#include <unordered_map>
#include <unordered_set>

/*

IFailureMonitor is used by load balancing, data distribution and other components
to report on which other machines are unresponsive or experiencing other failures.
This is vital both to reconfigure the system in response to failures and to prevent
actors from waiting forever for replies from remote machines that are no longer
available.  When waiting for a reply, clients should generally stop waiting and
try an alternative server when a failure is reported, rather than relying on timeouts.

The information tracked for each machine is a FailureStatus, which
for the moment is just a boolean but might be richer in the future.

Get an IFailureMonitor by calling g_network->failureMonitor(); the simulator keeps
one for each simulated machine and ASIONetwork keeps one for each process.

The system attempts to ensure that failures are reported quickly, but may occasionally
report a working system as failed temporarily.  Clients that intend to take very costly
actions as a result of a failure should probably wait a while to see if a machine becomes
unfailed first.  If possible use onFailedFor() which in the future may react to 'permanent'
failures immediately.

In older FDB, information reported through this interface was actually actively supplied by
failureMonitorClient, which exchanges FailureMonitoringRequest/Reply pairs with the
failureDetectionServer actor on the ClusterController.

Now it is done locally by each process with help of of FlowTransport. Whenever a network
connection is establish/failed, the address is marked as available or failed accordingly. We
do however take an optimistic approach of assuming every newly discovered address
(when deserializing an endpoint) is healthy by default.

In the future it may be augmented with locally available information about failures (e.g.
TCP connection loss in ASIONetwork or unexpectedly long response times for application requests).

Communications failures are tracked at NetworkAddress granularity.  When a request is made to
a missing endpoint on a non-failed machine, this information is reported back to the requesting
machine and tracked at the endpoint level.

*/

struct FailureStatus {
	bool failed;

	FailureStatus() : failed(true) {}
	explicit FailureStatus(bool failed) : failed(failed) {}
	bool isFailed() const { return failed; }
	bool isAvailable() const { return !failed; }

	bool operator==(FailureStatus const& r) const { return failed == r.failed; }
	bool operator!=(FailureStatus const& r) const { return failed != r.failed; }
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, failed);
	}
};

class IFailureMonitor {
public:
	// Returns the currently known status for the endpoint
	virtual FailureStatus getState(Endpoint const& endpoint) const = 0;

	// Returns the currently known status for the address
	virtual FailureStatus getState(NetworkAddress const& address) const = 0;

	// Only use this function when the endpoint is known to be failed
	virtual void endpointNotFound(Endpoint const&) = 0;

	// The next time the known status for the endpoint changes, returns the new status.
	virtual Future<Void> onStateChanged(Endpoint const& endpoint) = 0;

	// Returns when onFailed(endpoint) || transport().onDisconnect( endpoint.getPrimaryAddress() )
	virtual Future<Void> onDisconnectOrFailure(Endpoint const& endpoint) = 0;

	// Returns when transport().onDisconnect( address )
	virtual Future<Void> onDisconnect(NetworkAddress const& address) = 0;

	// Returns true if the endpoint is failed but the address of the endpoint is not failed.
	virtual bool onlyEndpointFailed(Endpoint const& endpoint) const = 0;

	// Returns true if the endpoint will never become available.
	virtual bool permanentlyFailed(Endpoint const& endpoint) const = 0;

	// Called by FlowTransport when a connection closes and a prior request or reply might be lost
	virtual void notifyDisconnect(NetworkAddress const&) = 0;

	// Called to update the failure status of network address directly when running client.
	virtual void setStatus(NetworkAddress const& address, FailureStatus const& status) = 0;

	// Returns when the known status of endpoint is next equal to status.  Returns immediately
	//   if appropriate.
	Future<Void> onStateEqual(Endpoint const& endpoint, FailureStatus status);

	// Returns when the status of the given endpoint is next considered "failed"
	Future<Void> onFailed(Endpoint const& endpoint) { return onStateEqual(endpoint, FailureStatus()); }

	// Returns when the status of the given endpoint has continuously been "failed" for sustainedFailureDuration +
	// (elapsedTime*sustainedFailureSlope)
	Future<Void> onFailedFor(Endpoint const& endpoint,
	                         double sustainedFailureDuration,
	                         double sustainedFailureSlope = 0.0);

	// Returns the failure monitor that the calling machine should use
	static IFailureMonitor& failureMonitor() {
		return *static_cast<IFailureMonitor*>((void*)g_network->global(INetwork::enFailureMonitor));
	}
};

// SimpleFailureMonitor is the sole implementation of IFailureMonitor.  It has no
//   failure detection logic; it just implements the interface and reacts to setStatus() etc.
// Initially all addresses are considered failed, but all endpoints of a non-failed address are considered OK.

class SimpleFailureMonitor : public IFailureMonitor {
public:
	SimpleFailureMonitor();
	void setStatus(NetworkAddress const& address, FailureStatus const& status) override;
	void endpointNotFound(Endpoint const&) override;
	void notifyDisconnect(NetworkAddress const&) override;

	Future<Void> onStateChanged(Endpoint const& endpoint) override;
	FailureStatus getState(Endpoint const& endpoint) const override;
	FailureStatus getState(NetworkAddress const& address) const override;
	Future<Void> onDisconnectOrFailure(Endpoint const& endpoint) override;
	Future<Void> onDisconnect(NetworkAddress const& address) override;
	bool onlyEndpointFailed(Endpoint const& endpoint) const override;
	bool permanentlyFailed(Endpoint const& endpoint) const override;

	void reset();

private:
	std::unordered_map<NetworkAddress, FailureStatus> addressStatus;
	YieldedAsyncMap<Endpoint, bool> endpointKnownFailed;
	AsyncMap<NetworkAddress, bool> disconnectTriggers;
	std::unordered_set<Endpoint> failedEndpoints;

	friend class OnStateChangedActorActor;
};

#endif
