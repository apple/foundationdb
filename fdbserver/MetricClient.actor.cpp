/*
 * MetricClient.actor.cpp
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
#include "flow/OTELMetrics.h"
#include "flow/TDMetric.actor.h"
#include "fdbrpc/Msgpack.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h"

UDPMetricClient::UDPMetricClient()
  : socket_fd(-1), model(knobToMetricModel(FLOW_KNOBS->METRICS_DATA_MODEL)),
    buf{ MsgpackBuffer{ .buffer = std::make_unique<uint8_t[]>(1024), .data_size = 0, .buffer_size = 1024 } },
    address((model == STATSD) ? FLOW_KNOBS->STATSD_UDP_EMISSION_ADDR : FLOW_KNOBS->OTEL_UDP_EMISSION_ADDR),
    port((model == STATSD) ? FLOW_KNOBS->STATSD_UDP_EMISSION_PORT : FLOW_KNOBS->OTEL_UDP_EMISSION_PORT) {
	NetworkAddress destAddress = NetworkAddress::parse(address + ":" + std::to_string(port));
	socket = INetworkConnections::net()->createUDPSocket(destAddress);
	model = knobToMetricModel(FLOW_KNOBS->METRICS_DATA_MODEL);
}

void UDPMetricClient::send(const MetricBatch& batch) {
	if (!socket.isReady()) {
		return;
	}
	socket_fd = socket.get()->native_handle();
	if (socket_fd != -1) {
		switch (model) {
		case STATSD: {
			::send(socket_fd, batch.statsd_message.data(), batch.statsd_message.size(), MSG_DONTWAIT);
			break;
		}
		case OTEL: {
			// Send the Counters, gauges and histograms as seperate packets
			// Use ext32 msgpack to encode the different types
			if (!batch.counters.empty()) {
				auto f_Sum = [](const std::vector<OTELSum>& vec, MsgpackBuffer& buf) {
					serialize_vector(vec, buf, serialize_otelsum);
				};
				serialize_ext(batch.counters, buf, OTELMetricType::Sum, f_Sum);
				::send(socket_fd, buf.buffer.get(), buf.data_size, MSG_DONTWAIT);
				buf.reset();
			}
			if (!batch.gauges.empty()) {
				auto f_Gauge = [](const std::vector<OTELGauge>& vec, MsgpackBuffer& buf) {
					serialize_vector(vec, buf, serialize_otelgauge);
				};
				serialize_ext(batch.gauges, buf, OTELMetricType::Gauge, f_Gauge);
				::send(socket_fd, buf.buffer.get(), buf.data_size, MSG_DONTWAIT);
				buf.reset();
			}
			if (!batch.hists.empty()) {
				auto f_Hist = [](const std::vector<OTELHistogram>& vec, MsgpackBuffer& buf) {
					serialize_vector(vec, buf, serialize_otelhist);
				};
				serialize_ext(batch.hists, buf, OTELMetricType::Hist, f_Hist);
				::send(socket_fd, buf.buffer.get(), buf.data_size, MSG_DONTWAIT);
				buf.reset();
			}
			break;
		}
		default:
			break;
		}
	}
}