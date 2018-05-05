/*
 * FailureMonitor.actor.cpp
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

#include "flow/actorcompiler.h"
#include "FailureMonitor.h"

ACTOR Future<Void> waitForStateEqual( IFailureMonitor* monitor, Endpoint endpoint, FailureStatus status ) {
	loop {
		Future<Void> change = monitor->onStateChanged(endpoint);
		if (monitor->getState(endpoint) == status)
			return Void();
		Void _ = wait( change );
	}
}

ACTOR Future<Void> waitForContinuousFailure( IFailureMonitor* monitor, Endpoint endpoint, double sustainedFailureDuration, double slope ) {
	state double startT = now();
	loop {
		Void _ = wait( monitor->onFailed( endpoint ) );
		if(monitor->permanentlyFailed(endpoint))
			return Void();

		// X == sustainedFailureDuration + slope * (now()-startT+X)
		double waitDelay = (sustainedFailureDuration + slope * (now()-startT)) / (1-slope);

		//SOMEDAY: if we know that this process is a server or client we can tune this optimization better
		if(waitDelay < std::min(FLOW_KNOBS->CLIENT_REQUEST_INTERVAL, FLOW_KNOBS->SERVER_REQUEST_INTERVAL)) //We will not get a failure monitoring update in this amount of time, so there is no point in waiting for changes
			waitDelay = 0;
		choose {
			when (Void _ = wait( monitor->onStateEqual( endpoint, FailureStatus(false) ) )) {}  // SOMEDAY: Use onStateChanged() for efficiency
			when (Void _ = wait( delay(waitDelay) )) {
				return Void();
			}
		}
	}
}

Future<Void> IFailureMonitor::onStateEqual( Endpoint const& endpoint, FailureStatus status ) {
	if ( status == getState(endpoint) ) return Void();
	return waitForStateEqual(this, endpoint, status);
}

Future<Void> IFailureMonitor::onFailedFor( Endpoint const& endpoint, double sustainedFailureDuration, double slope ) {
	ASSERT( slope < 1.0 );
	return waitForContinuousFailure( this, endpoint, sustainedFailureDuration, slope );
}

void SimpleFailureMonitor::setStatus( NetworkAddress const& address, FailureStatus const& status ) {

	//if (status.failed)
	//	printf("On machine '%s': Machine '%s' is failed\n", g_network->getLocalAddress().toString().c_str(), address.toString().c_str());
	//printf("%s.setState(%s, %s) %p\n", g_network->getLocalAddress().toString(), address.toString(), status.failed ? "FAILED" : "OK", this);
	//addressStatus.set( address, status );

	// onStateChanged() will be waiting on endpointKnownFailed only where it is false, so if the address status
	// for an endpoint that is waited on changes, the waiter sees its failure status change
	auto it = addressStatus.find(address);

	//TraceEvent("NotifyFailureStatus").detail("Address", address).detail("Status", status.failed ? "Failed" : "OK").detail("Present", it == addressStatus.end());
	if (it == addressStatus.end()) {
		if (status != FailureStatus()) {
			addressStatus[address]=status;
			endpointKnownFailed.triggerRange( Endpoint(address, UID()), Endpoint(address, UID(-1,-1)) );
		}
	} else {
		bool triggerEndpoint = status != it->value;
		if (status != FailureStatus())
			it->value = status;
		else
			addressStatus.erase(it);
		if(triggerEndpoint)
			endpointKnownFailed.triggerRange( Endpoint(address, UID()), Endpoint(address, UID(-1,-1)) );
	}
}

void SimpleFailureMonitor::endpointNotFound( Endpoint const& endpoint ) {
	// SOMEDAY: Expiration (this "leaks" memory)
	TraceEvent("EndpointNotFound").detail("Address", endpoint.address).detail("Token", endpoint.token).suppressFor(1.0);
	endpointKnownFailed.set( endpoint, true );
}

void SimpleFailureMonitor::notifyDisconnect( NetworkAddress const& address ) {
	//TraceEvent("NotifyDisconnect").detail("Address", address);
	endpointKnownFailed.triggerRange( Endpoint(address, UID()), Endpoint(address, UID(-1,-1)) );
}

Future<Void> SimpleFailureMonitor::onDisconnectOrFailure( Endpoint const& endpoint ) {
	// If the endpoint or address is already failed, return right away
	auto i = addressStatus.find(endpoint.address);
	if (i == addressStatus.end() || i->value.isFailed() || endpointKnownFailed.get(endpoint)) {
		TraceEvent("AlreadyDisconnected").detail("Addr", endpoint.address).detail("Tok", endpoint.token);
		return Void();
	}

	// Return when the endpoint is triggered, which means that either the endpoint has become known failed, or the
	//   address has changed state (and since it was previously not failed, it must now be failed), or notifyDisconnect()
	//   has been called.
	return endpointKnownFailed.onChange(endpoint);
}

Future<Void> SimpleFailureMonitor::onStateChanged( Endpoint const& endpoint ) {
	// Wait on endpointKnownFailed if it is false, to pick up both endpointNotFound errors (which set it to true)
	//   and changes to addressStatus (which trigger a range).  Don't wait on endpointKnownFailed if it is true, because
	//   failure status for that endpoint can never change (and we could be spuriously triggered by setStatus)
	// Also returns spuriously when notifyDisconnect is called (which doesn't actually change the state), but callers
	//   check the state so it's OK
	if (endpointKnownFailed.get(endpoint))
		return Never();
	else
		return endpointKnownFailed.onChange(endpoint);
}

FailureStatus SimpleFailureMonitor::getState( Endpoint const& endpoint ) {
	if (endpointKnownFailed.get(endpoint))
		return FailureStatus(true);
	else {
		auto a = addressStatus.find(endpoint.address);
		if (a == addressStatus.end()) return FailureStatus();
		else return a->value;
		//printf("%s.getState(%s) = %s %p\n", g_network->getLocalAddress().toString(), endpoint.address.toString(), a.failed ? "FAILED" : "OK", this);
	}
}

bool SimpleFailureMonitor::onlyEndpointFailed( Endpoint const& endpoint ) {
	if(!endpointKnownFailed.get(endpoint))
		return false;
	auto a = addressStatus.find(endpoint.address);
	if (a == addressStatus.end()) return true;
	else return !a->value.failed;
}

bool SimpleFailureMonitor::permanentlyFailed( Endpoint const& endpoint ) {
	return endpointKnownFailed.get(endpoint);
}

void SimpleFailureMonitor::reset() {
	addressStatus = Map< NetworkAddress, FailureStatus >();
	endpointKnownFailed.resetNoWaiting();
}
