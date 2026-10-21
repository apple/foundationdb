/*
 * SimEventEngine.h
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

// A gRPC EventEngine that runs all of gRPC's I/O, timers and closures on the Flow run loop, over
// INetworkConnections. Under Sim2 this puts real gRPC on simulated sockets and simulated time.

#ifdef FLOW_GRPC_ENABLED
#ifndef FDBRPC_GRPC_SIM_EVENT_ENGINE_H
#define FDBRPC_GRPC_SIM_EVENT_ENGINE_H

#include <cstdint>

namespace sim_grpc {

struct SimEventEngineStats {
	uint64_t connects = 0; // outgoing connection attempts
	uint64_t clockReads = 0; // gpr_now() calls served from simulated time
	uint64_t offThreadCalls = 0; // engine or clock entries from a thread other than the run loop's
	uint64_t droppedClosures = 0; // scheduled closures still pending when stats were read
	uint64_t strandedClosures = 0; // pending closures scheduled from a simulated process that has since failed
};

// Pointer channel argument naming the ISimulator::ProcessInfo that owns a channel's connections. Without it a
// connection belongs to whichever simulated process happened to trigger the connect.
inline constexpr char kSimProcessChannelArg[] = "fdb.sim_process";

// Must be called on the run loop thread before gRPC is first used. Later calls do nothing.
void installSimEventEngine();
SimEventEngineStats simEventEngineStats();

} // namespace sim_grpc

#endif
#endif
