/*
 * MetricClient.h
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
#include "fdbserver/MetricClient.h"
#include "flow/FastRef.h"
#include "flow/Knobs.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h"

StatsdClient::StatsdClient() : socket_fd(-1) {
	std::string destAddr = FLOW_KNOBS->METRICS_UDP_EMISSION_ADDR;
	NetworkAddress destAddress =
	    NetworkAddress::parse(destAddr + ":" + std::to_string(FLOW_KNOBS->METRICS_UDP_EMISSION_PORT));
	socket = INetworkConnections::net()->createUDPSocket(destAddress);
}

void StatsdClient::send(const MetricBatch& batch) {
	if (!socket.isReady()) {
		return;
	}
	socket_fd = socket.get()->native_handle();
	if (socket_fd != -1) {
		::send(socket_fd, batch.statsd_message.data(), batch.statsd_message.size(), MSG_DONTWAIT);
	}
}