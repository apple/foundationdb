/*
 * CoordinationInterface.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/core/CoordinationInterface.h"

GenerationRegInterface::GenerationRegInterface(NetworkAddress const& remote)
  : read(Endpoint::wellKnown({ remote }, WLTOKEN_GENERATIONREG_READ)),
    write(Endpoint::wellKnown({ remote }, WLTOKEN_GENERATIONREG_WRITE)) {}

GenerationRegInterface::GenerationRegInterface(INetwork* local) {
	read.makeWellKnownEndpoint(WLTOKEN_GENERATIONREG_READ, TaskPriority::Coordination);
	write.makeWellKnownEndpoint(WLTOKEN_GENERATIONREG_WRITE, TaskPriority::Coordination);
}

LeaderElectionRegInterface::LeaderElectionRegInterface(NetworkAddress const& remote)
  : ClientLeaderRegInterface(remote), candidacy(Endpoint::wellKnown({ remote }, WLTOKEN_LEADERELECTIONREG_CANDIDACY)),
    electionResult(Endpoint::wellKnown({ remote }, WLTOKEN_LEADERELECTIONREG_ELECTIONRESULT)),
    leaderHeartbeat(Endpoint::wellKnown({ remote }, WLTOKEN_LEADERELECTIONREG_LEADERHEARTBEAT)),
    forward(Endpoint::wellKnown({ remote }, WLTOKEN_LEADERELECTIONREG_FORWARD)) {}

LeaderElectionRegInterface::LeaderElectionRegInterface(INetwork* local) : ClientLeaderRegInterface(local) {
	candidacy.makeWellKnownEndpoint(WLTOKEN_LEADERELECTIONREG_CANDIDACY, TaskPriority::Coordination);
	electionResult.makeWellKnownEndpoint(WLTOKEN_LEADERELECTIONREG_ELECTIONRESULT, TaskPriority::Coordination);
	leaderHeartbeat.makeWellKnownEndpoint(WLTOKEN_LEADERELECTIONREG_LEADERHEARTBEAT, TaskPriority::Coordination);
	forward.makeWellKnownEndpoint(WLTOKEN_LEADERELECTIONREG_FORWARD, TaskPriority::Coordination);
}

ServerCoordinators::ServerCoordinators(Reference<IClusterConnectionRecord> ccr) : ClientCoordinators(ccr) {
	ClusterConnectionString cs = ccr->getConnectionString();
	for (auto h : cs.hostnames) {
		leaderElectionServers.emplace_back(h);
		stateServers.emplace_back(h);
	}
	for (auto s : cs.coords) {
		leaderElectionServers.emplace_back(s);
		stateServers.emplace_back(s);
	}
}
