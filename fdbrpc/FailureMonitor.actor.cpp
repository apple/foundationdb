/*
 * FailureMonitor.actor.cpp
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

#include "fdbrpc/FailureMonitor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<Void> waitForStateEqual(IFailureMonitor* monitor, Endpoint endpoint, FailureStatus status) {
	loop {
		Future<Void> change = monitor->onStateChanged(endpoint);
		if (monitor->getState(endpoint) == status)
			return Void();
		wait(change);
	}
}

ACTOR Future<Void> waitForContinuousFailure(IFailureMonitor* monitor,
                                            Endpoint endpoint,
                                            double sustainedFailureDuration,
                                            double slope) {
	state double startT = now();

	loop {
		wait(monitor->onFailed(endpoint));
		if (monitor->permanentlyFailed(endpoint))
			return Void();

		// X == sustainedFailureDuration + slope * (now()-startT+X)
		double waitDelay = (sustainedFailureDuration + slope * (now() - startT)) / (1 - slope);

		// SOMEDAY: if we know that this process is a server or client we can tune this optimization better
		if (waitDelay <
		    std::min(FLOW_KNOBS->CLIENT_REQUEST_INTERVAL,
		             FLOW_KNOBS->SERVER_REQUEST_INTERVAL)) // We will not get a failure monitoring update in this amount
		                                                   // of time, so there is no point in waiting for changes
			waitDelay = 0;
		choose {
			when(wait(monitor->onStateEqual(endpoint, FailureStatus(false)))) {
			} // SOMEDAY: Use onStateChanged() for efficiency
			when(wait(delay(waitDelay))) { return Void(); }
		}
	}
}

Future<Void> IFailureMonitor::onStateEqual(Endpoint const& endpoint, FailureStatus status) {
	if (status == getState(endpoint))
		return Void();
	return waitForStateEqual(this, endpoint, status);
}

Future<Void> IFailureMonitor::onFailedFor(Endpoint const& endpoint, double sustainedFailureDuration, double slope) {
	ASSERT(slope < 1.0);
	return waitForContinuousFailure(this, endpoint, sustainedFailureDuration, slope);
}

SimpleFailureMonitor::SimpleFailureMonitor() {
	// Mark ourselves as available in FailureMonitor
	const auto& localAddresses = FlowTransport::transport().getLocalAddresses();
	addressStatus[localAddresses.address] = FailureStatus(false);
	if (localAddresses.secondaryAddress.present()) {
		addressStatus[localAddresses.secondaryAddress.get()] = FailureStatus(false);
	}
}

void SimpleFailureMonitor::setStatus(NetworkAddress const& address, FailureStatus const& status) {

	// if (status.failed)
	//	printf("On machine '%s': Machine '%s' is failed\n", g_network->getLocalAddress().toString().c_str(),
	//         address.toString().c_str()); printf("%s.setState(%s, %s) %p\n", g_network->getLocalAddress().toString(),
	//         address.toString(), status.failed ? "FAILED" : "OK", this); addressStatus.set( address, status );

	// onStateChanged() will be waiting on endpointKnownFailed only where it is false, so if the address status
	// for an endpoint that is waited on changes, the waiter sees its failure status change
	auto it = addressStatus.find(address);

	if (it == addressStatus.end()) {
		if (status != FailureStatus()) {
			TraceEvent("NotifyAddressHealthy").suppressFor(1.0).detail("Address", address);
			addressStatus[address] = status;
			endpointKnownFailed.triggerRange(Endpoint({ address }, UID()), Endpoint({ address }, UID(-1, -1)));
		}
	} else {
		bool triggerEndpoint = status != it->second;
		if (status != FailureStatus())
			it->second = status;
		else
			addressStatus.erase(it);
		if (triggerEndpoint) {
			if (status.failed) {
				TraceEvent("NotifyAddressFailed").suppressFor(1.0).detail("Address", address);
			} else {
				TraceEvent("NotifyAddressHealthyPresent").suppressFor(1.0).detail("Address", address);
			}
			endpointKnownFailed.triggerRange(Endpoint({ address }, UID()), Endpoint({ address }, UID(-1, -1)));
		}
	}
}

void SimpleFailureMonitor::endpointNotFound(Endpoint const& endpoint) {
	// SOMEDAY: Expiration (this "leaks" memory)
	if (endpoint.token.first() == -1) {
		TraceEvent("WellKnownEndpointNotFound")
		    .suppressFor(1.0)
		    .detail("Address", endpoint.getPrimaryAddress())
		    .detail("TokenFirst", endpoint.token.first())
		    .detail("TokenSecond", endpoint.token.second());
		return;
	}
	TraceEvent("EndpointNotFound")
	    .suppressFor(1.0)
	    .detail("Address", endpoint.getPrimaryAddress())
	    .detail("Token", endpoint.token);
	if (endpoint.getPrimaryAddress().isPublic()) {
		if (failedEndpoints.size() > 100000) {
			TraceEvent(SevWarnAlways, "TooManyFailedEndpoints").suppressFor(1.0);
			failedEndpoints.clear();
		}
		failedEndpoints.insert(endpoint);
	}
	endpointKnownFailed.trigger(endpoint);
}

void SimpleFailureMonitor::notifyDisconnect(NetworkAddress const& address) {
	//TraceEvent("NotifyDisconnect").detail("Address", address);
	endpointKnownFailed.triggerRange(Endpoint({ address }, UID()), Endpoint({ address }, UID(-1, -1)));
	disconnectTriggers.trigger(address);
}

Future<Void> SimpleFailureMonitor::onDisconnectOrFailure(Endpoint const& endpoint) {
	// If the endpoint or address is already failed, return right away
	auto i = addressStatus.find(endpoint.getPrimaryAddress());
	if (i == addressStatus.end() || i->second.isFailed() || failedEndpoints.count(endpoint)) {
		TraceEvent("AlreadyDisconnected").detail("Addr", endpoint.getPrimaryAddress()).detail("Tok", endpoint.token);
		return Void();
	}

	// Return when the endpoint is triggered, which means that either the endpoint has become known failed, or the
	//   address has changed state (and since it was previously not failed, it must now be failed), or
	//   notifyDisconnect() has been called.
	return endpointKnownFailed.onChange(endpoint);
}

Future<Void> SimpleFailureMonitor::onDisconnect(NetworkAddress const& address) {
	return disconnectTriggers.onChange(address);
}

Future<Void> SimpleFailureMonitor::onStateChanged(Endpoint const& endpoint) {
	// Wait on endpointKnownFailed if it is false, to pick up both endpointNotFound errors (which set it to true)
	//   and changes to addressStatus (which trigger a range).  Don't wait on endpointKnownFailed if it is true, because
	//   failure status for that endpoint can never change (and we could be spuriously triggered by setStatus)
	// Also returns spuriously when notifyDisconnect is called (which doesn't actually change the state), but callers
	//   check the state so it's OK
	if (failedEndpoints.count(endpoint))
		return Never();
	else
		return endpointKnownFailed.onChange(endpoint);
}

FailureStatus SimpleFailureMonitor::getState(Endpoint const& endpoint) const {
	if (failedEndpoints.count(endpoint))
		return FailureStatus(true);
	else {
		auto a = addressStatus.find(endpoint.getPrimaryAddress());
		if (a == addressStatus.end())
			return FailureStatus();
		else
			return a->second;
		// printf("%s.getState(%s) = %s %p\n", g_network->getLocalAddress().toString(), endpoint.address.toString(),
		//        a.failed ? "FAILED" : "OK", this);
	}
}

FailureStatus SimpleFailureMonitor::getState(NetworkAddress const& address) const {
	auto a = addressStatus.find(address);
	if (a == addressStatus.end())
		return FailureStatus();
	else
		return a->second;
}

bool SimpleFailureMonitor::onlyEndpointFailed(Endpoint const& endpoint) const {
	if (!failedEndpoints.count(endpoint))
		return false;
	auto a = addressStatus.find(endpoint.getPrimaryAddress());
	if (a == addressStatus.end())
		return true;
	else
		return !a->second.failed;
}

bool SimpleFailureMonitor::permanentlyFailed(Endpoint const& endpoint) const {
	return failedEndpoints.count(endpoint);
}

void SimpleFailureMonitor::reset() {
	addressStatus = std::unordered_map<NetworkAddress, FailureStatus>();
	failedEndpoints = std::unordered_set<Endpoint>();
	endpointKnownFailed.resetNoWaiting();
}
