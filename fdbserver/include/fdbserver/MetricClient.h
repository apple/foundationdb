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
#include "flow/TDMetric.actor.h"
#include "flow/Msgpack.h"
#include "flow/network.h"
#ifndef METRIC_CLIENT_H
#define METRIC_CLIENT_H
class IMetricClient {
protected:
	MetricsDataModel model;

public:
	virtual void send(MetricCollection*) = 0;
	virtual ~IMetricClient() {}
};

class UDPMetricClient : public IMetricClient {
private:
	// Since we can't quickly determine the exact packet size for OTELSum in msgpack
	// we play on the side of caution and make our maximum 3/4 of the official one
	static constexpr uint32_t MAX_OTELSUM_PACKET_SIZE = 0.75 * IUDPSocket::MAX_PACKET_SIZE;
	MetricsDataModel model;
	Future<Reference<IUDPSocket>> socket;
	int socket_fd;
	MsgpackBuffer buf;
	std::string address;
	int port;
	void send_packet(int fd, const void* data, size_t len);

public:
	UDPMetricClient();
	void send(MetricCollection*) override;
};
#endif