/*
 * MetricClient.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
#include "fdbrpc/Stats.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/Knobs.h"
#include "flow/OTELMetrics.h"
#include "flow/TDMetric.actor.h"
#include "flow/Msgpack.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include <cstddef>
#ifndef WIN32
#include <sys/socket.h>
#endif
#include "flow/network.h"
#include "flow/IUDPSocket.h"
#include "flow/IConnection.h"
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

// Since MSG_DONTWAIT isn't defined for Windows, we need to add a
// ifndef guard here to avoid any compilation issues
void UDPMetricClient::send_packet(int fd, const void* data, size_t len) {
#ifndef WIN32
	::send(fd, data, len, MSG_DONTWAIT);
#endif
}

void UDPMetricClient::send(MetricCollection* metrics) {
	if (!socket.isReady()) {
		return;
	}
	socket_fd = socket.get()->native_handle();
	if (socket_fd == -1)
		return;
	if (model == OTLP) {
		std::vector<std::vector<OTEL::OTELSum>> sums;
		std::vector<OTEL::OTELGauge> gauges;

		// Define custom serialize functions
		auto f_sums = [](const std::vector<OTEL::OTELSum>& vec, MsgpackBuffer& buf) {
			typedef void (*func_ptr)(const OTEL::OTELSum&, MsgpackBuffer&);
			func_ptr f = OTEL::serialize;
			serialize_vector(vec, buf, f);
		};

		auto f_hists = [](const std::vector<OTEL::OTELHistogram>& vec, MsgpackBuffer& buf) {
			typedef void (*func_ptr)(const OTEL::OTELHistogram&, MsgpackBuffer&);
			func_ptr f = OTEL::serialize;
			serialize_vector(vec, buf, f);
		};

		auto f_gauge = [](const std::vector<OTEL::OTELGauge>& vec, MsgpackBuffer& buf) {
			typedef void (*func_ptr)(const OTEL::OTELGauge&, MsgpackBuffer&);
			func_ptr f = OTEL::serialize;
			serialize_vector(vec, buf, f);
		};

		std::vector<OTEL::OTELSum> currentSums;
		size_t current_msgpack = 0;
		for (const auto& [_, s] : metrics->sumMap) {
			if (current_msgpack < MAX_OTELSUM_PACKET_SIZE) {
				currentSums.push_back(std::move(s));
				current_msgpack += s.getMsgpackBytes();
			} else {
				sums.push_back(std::move(currentSums));
				currentSums.clear();
				current_msgpack = 0;
			}
		}
		if (!sums.empty()) {
			for (const auto& currSums : sums) {
				serialize_ext(currSums, buf, OTEL::OTELMetricType::Sum, f_sums);
				send_packet(socket_fd, buf.buffer.get(), buf.data_size);
				int error = errno;
				TraceEvent("MetricsSumUdpErrno", UID()).detail("Errno", error);
				buf.reset();
			}
			metrics->sumMap.clear();
		}

		// Each histogram should be in a separate because of their large sizes
		// Expected DDSketch size is ~4200 entries * 9 bytes = 37800
		for (const auto& [_, h] : metrics->histMap) {
			const std::vector<OTEL::OTELHistogram> singleHist{ std::move(h) };
			serialize_ext(singleHist, buf, OTEL::OTELMetricType::Hist, f_hists);
			send_packet(socket_fd, buf.buffer.get(), buf.data_size);
			int error = errno;
			TraceEvent("MetricsHistUdpErrno", UID()).detail("Errno", error);
			buf.reset();
		}

		metrics->histMap.clear();

		for (const auto& [_, g] : metrics->gaugeMap) {
			gauges.push_back(std::move(g));
		}
		if (!gauges.empty()) {
			serialize_ext(gauges, buf, OTEL::OTELMetricType::Gauge, f_gauge);
			send_packet(socket_fd, buf.buffer.get(), buf.data_size);
			int error = errno;
			TraceEvent("MetricsGaugeUdpErrno", UID()).detail("Errno", error);
			metrics->gaugeMap.clear();
			buf.reset();
		}
	} else if (model == MetricsDataModel::STATSD) {
		std::string messages;
		for (const auto& msg : metrics->statsd_message) {
			// Account for max udp packet size (+1 since we add '\n')
			if (messages.size() + msg.size() + 1 < IUDPSocket::MAX_PACKET_SIZE) {
				messages += (std::move(msg) + '\n');
			} else {
				send_packet(socket_fd, buf.buffer.get(), buf.data_size);
			}
		}
		if (!messages.empty()) {
			send_packet(socket_fd, messages.data(), messages.size());
		}
		metrics->statsd_message.clear();
	}
}