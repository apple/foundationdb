/*
 * LoadBalance.actor.cpp
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

#include "fdbrpc/LoadBalance.actor.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

FDB_DEFINE_BOOLEAN_PARAM(AtMostOnce);
FDB_DEFINE_BOOLEAN_PARAM(TriedAllOptions);

// Throwing all_alternatives_failed will cause the client to issue a GetKeyLocationRequest to the proxy, so this actor
// attempts to limit the number of these errors thrown by a single client to prevent it from saturating the proxies with
// these requests
ACTOR Future<Void> allAlternativesFailedDelay(Future<Void> okFuture) {
	if (now() - g_network->networkInfo.newestAlternativesFailure > FLOW_KNOBS->ALTERNATIVES_FAILURE_RESET_TIME) {
		g_network->networkInfo.oldestAlternativesFailure = now();
	}

	double delay = FLOW_KNOBS->ALTERNATIVES_FAILURE_MIN_DELAY;
	if (now() - g_network->networkInfo.lastAlternativesFailureSkipDelay > FLOW_KNOBS->ALTERNATIVES_FAILURE_SKIP_DELAY) {
		g_network->networkInfo.lastAlternativesFailureSkipDelay = now();
	} else {
		double elapsed = now() - g_network->networkInfo.oldestAlternativesFailure;
		delay = std::max(delay,
		                 std::min(elapsed * FLOW_KNOBS->ALTERNATIVES_FAILURE_DELAY_RATIO,
		                          FLOW_KNOBS->ALTERNATIVES_FAILURE_MAX_DELAY));
		delay = std::max(delay,
		                 std::min(elapsed * FLOW_KNOBS->ALTERNATIVES_FAILURE_SLOW_DELAY_RATIO,
		                          FLOW_KNOBS->ALTERNATIVES_FAILURE_SLOW_MAX_DELAY));
	}

	g_network->networkInfo.newestAlternativesFailure = now();

	choose {
		when(wait(okFuture)) {}
		when(wait(::delayJittered(delay))) { throw all_alternatives_failed(); }
	}
	return Void();
}
