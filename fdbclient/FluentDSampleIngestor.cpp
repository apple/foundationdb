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
#include <boost/asio/co_spawn.hpp>

namespace {

boost::asio::ip::address ipAddress(IPAddress const& n) {
	if (n.isV6()) {
		return boost::asio::ip::address_v6(n.toV6());
	} else {
		return boost::asio::ip::address_v4(n.toV4());
	}
}

template <class Protocol>
boost::asio::ip::basic_endpoint<Protocol> toEndpoint(NetworkAddress const n) {
	return boost::asio::ip::basic_endpoint<Protocol>(ipAddress(n.ip), n.port);
}

struct FluentDSocket {
	virtual ~FluentDSocket() {}
	virtual void connect(NetworkAddress const& endpoint) = 0;
	virtual void send(std::shared_ptr<Sample> const& sample) = 0;
	virtual const boost::system::error_code& failed() const = 0;
};

template <class Protocol>
struct FluentDSocketImpl : FluentDSocket, std::enable_shared_from_this<FluentDSocketImpl<Protocol>> {
	static constexpr unsigned MAX_QUEUE_SIZE = 100;
	boost::asio::io_context& context;
	typename Protocol::socket socket;
	FluentDSocketImpl(boost::asio::io_context& context) : context(context), socket(context) {}
	bool ready = false;
	std::deque<std::shared_ptr<Sample>> queue;
	boost::system::error_code _failed;

	const boost::system::error_code& failed() const override { return _failed; }

	void sendCompletionHandler(boost::system::error_code const& ec) {
		if (ec) {
			// TODO: trace error
			_failed = ec;
			return;
		}
		if (queue.empty()) {
			ready = true;
		} else {
			auto sample = queue.front();
			queue.pop_front();
			sendImpl<Protocol>(sample);
		}
	}

	template <class P>
	std::enable_if_t<std::is_same_v<boost::asio::ip::tcp, P>> sendImpl(std::shared_ptr<Sample> const& sample) {
		boost::asio::async_write(
		    socket,
		    boost::asio::const_buffer(sample->data, sample->size),
		    [sample, self = this->shared_from_this()](auto const& ec, size_t) { self->sendCompletionHandler(ec); });
	}

	template <class P>
	std::enable_if_t<std::is_same_v<boost::asio::ip::udp, P>> sendImpl(std::shared_ptr<Sample> const& sample) {
		socket.async_send(
		    boost::asio::const_buffer(sample->data, sample->size),
		    [sample, self = this->shared_from_this()](auto const& ec, size_t) { self->sendCompletionHandler(ec); });
	}

	void send(std::shared_ptr<Sample> const& sample) override {
		if (_failed) {
			return;
		}
		if (ready) {
			ready = false;
			sendImpl<Protocol>(sample);
		} else {
			if (queue.size() < MAX_QUEUE_SIZE) {
				queue.push_back(sample);
			} // TODO: else trace a warning
		}
	}

	void connect(NetworkAddress const& endpoint) override {
		auto to = toEndpoint<Protocol>(endpoint);
		socket.async_connect(to, [self = this->shared_from_this()](boost::system::error_code const& ec) {
			if (ec) {
				// TODO: error handling
				self->_failed = ec;
				return;
			}
			self->ready = true;
		});
	}
};

} // namespace

struct FluentDIngestorImpl {
	using Protocol = FluentDIngestor::Protocol;
	Protocol protocol;
	NetworkAddress endpoint;
	boost::asio::io_context& io_context;
	std::unique_ptr<FluentDSocket> socket;
	boost::asio::steady_timer retryTimer;
	FluentDIngestorImpl(Protocol protocol, NetworkAddress const& endpoint)
	  : protocol(protocol), endpoint(endpoint), io_context(ActorLineageProfiler::instance().context()),
	    retryTimer(io_context) {
		connect();
	}

	~FluentDIngestorImpl() { retryTimer.cancel(); }

	void connect() {
		switch (protocol) {
		case Protocol::TCP:
			socket.reset(new FluentDSocketImpl<boost::asio::ip::tcp>(io_context));
			break;
		case Protocol::UDP:
			socket.reset(new FluentDSocketImpl<boost::asio::ip::udp>(io_context));
			break;
		}
		socket->connect(endpoint);
	}

	void retry() {
		retryTimer = boost::asio::steady_timer(io_context, std::chrono::seconds(1));
		retryTimer.async_wait([this](auto const& ec) {
			if (ec) {
				return;
			}
			connect();
		});
		socket.reset();
	}
};

FluentDIngestor::~FluentDIngestor() {
	delete impl;
}

FluentDIngestor::FluentDIngestor(Protocol protocol, NetworkAddress& endpoint)
  : impl(new FluentDIngestorImpl(protocol, endpoint)) {}

void FluentDIngestor::ingest(const std::shared_ptr<Sample>& sample) {
	if (!impl->socket) {
		// the connection failed in the past and we wait for a timeout before we retry
		return;
	} else if (impl->socket->failed()) {
		impl->retry();
		return;
	} else {
		impl->socket->send(sample);
	}
}

void FluentDIngestor::getConfig(std::map<std::string, std::string>& res) const {
	res["ingestor"] = "fluentd";
	res["collector_endpoint"] = impl->endpoint.toString();
	res["collector_protocol"] = impl->protocol == Protocol::TCP ? "tcp" : "udp";
}
