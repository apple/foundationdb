/*
 * FluentDSampleIngestor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/ActorLineageProfiler.h"
#include <boost/asio.hpp>

namespace {
struct FluentDSocket {
	virtual ~FluentDSocket() {}
	virtual void connect(NetworkAddress& endpoint) = 0;
	// virtual void send() = 0;
};

struct TCPFluentDSocket : FluentDSocket {
	boost::asio::io_context& io_context;
	boost::asio::ip::tcp::socket socket;
	TCPFluentDSocket(boost::asio::io_context& context) : io_context(context), socket(context) {}
	void connect(NetworkAddress& endpoint) override { boost::asio::ip::tcp::resolver resolver(io_context); }
};

struct UDPFluentDSocket : FluentDSocket {
	boost::asio::io_context& io_context;
	boost::asio::ip::tcp::socket socket;
	UDPFluentDSocket(boost::asio::io_context& context) : io_context(context), socket(context) {}
	void connect(NetworkAddress& endpoint) override {}
};
} // namespace

struct FluentDIngestorImpl {
	using Protocol = FluentDIngestor::Protocol;
	boost::asio::io_context io_context;
	std::unique_ptr<FluentDSocket> socket;
	FluentDIngestorImpl(Protocol protocol, NetworkAddress& endpoint) {
		switch (protocol) {
		case Protocol::TCP:
			socket.reset(new TCPFluentDSocket(io_context));
			break;
		case Protocol::UDP:
			socket.reset(new UDPFluentDSocket(io_context));
			break;
		}
		socket->connect(endpoint);
	}
};

FluentDIngestor::~FluentDIngestor() {}

FluentDIngestor::FluentDIngestor(Protocol protocol, NetworkAddress& endpoint) {}