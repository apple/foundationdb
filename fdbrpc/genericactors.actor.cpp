/*
 * genericactors.actor.cpp
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

#include "genericactors.actor.h"	// Gets genericactors.actor.g.h indirectly
#include "flow/network.h"
#include "simulator.h"

ACTOR void simDeliverDuplicate( Standalone<StringRef> data, Endpoint destination ) {
	Void _ = wait( delay( g_random->random01() * FLOW_KNOBS->MAX_DELIVER_DUPLICATE_DELAY ) );
	FlowTransport::transport().sendUnreliable( SerializeSourceRaw(data), destination );
}

ACTOR Future<Void> disableConnectionFailuresAfter( double time, std::string context ) {
	Void _ = wait( delay(time) );

	if(g_network->isSimulated()) {
		g_simulator.connectionFailuresDisableDuration = 1e6;
		g_simulator.speedUpSimulation = true;
		TraceEvent(SevWarnAlways, ("DisableConnectionFailures_" + context).c_str());
	}
	return Void();
}
