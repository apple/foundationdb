/*
 * FluentDSampleIngestor.cpp
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

#include "fdbclient/ActorLineageProfiler.h"
#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <msgpack.hpp>

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

template <class Protocol, class Callback>
class SampleSender : public std::enable_shared_from_this<SampleSender<Protocol, Callback>> {
	using Socket = typename Protocol::socket;
	using Iter = typename decltype(Sample::data)::iterator;
	Socket& socket;
	Callback callback;
	Iter iter, end;
	std::shared_ptr<Sample> sample_; // to keep from being deallocated

	struct Buf {
		const char* data;
		const unsigned size;
		Buf(const char* data, unsigned size) : data(data), size(size) {}
		Buf(Buf const&) = delete;
		Buf& operator=(Buf const&) = delete;
		~Buf() { delete[] data; }
	};

	void sendCompletionHandler(boost::system::error_code const& ec) {
		if (ec) {
			callback(ec);
		} else {
			++iter;
			sendNext();
		}
	}

	void send(boost::asio::ip::tcp::socket& socket, std::shared_ptr<Buf> const& buf) {
		boost::system::error_code ec;
		socket.send(boost::asio::const_buffer(buf->data, buf->size), 0, ec);
		this->sendCompletionHandler(ec);
	}
	void send(boost::asio::ip::udp::socket& socket, std::shared_ptr<Buf> const& buf) {
		boost::system::error_code ec;
		socket.send(boost::asio::const_buffer(buf->data, buf->size), 0, ec);
		this->sendCompletionHandler(ec);
	}

	void sendNext() {
		if (iter == end) {
			callback(boost::system::error_code());
			return;
		}
		// 1. calculate size of buffer
		unsigned size = 1; // 1 for fixmap identifier byte
		auto waitState = to_string(iter->first);
		if (waitState.size() < 32) {
			size += waitState.size() + 1;
		} else {
			size += waitState.size() + 2;
		}
		size += iter->second.second;
		// 2. allocate the buffer
		std::unique_ptr<char[]> buf(new char[size]);
		unsigned off = 0;
		// 3. serialize fixmap
		buf[off++] = 0x81; // map of size 1
		// 3.1 serialize key
		if (waitState.size() < 32) {
			buf[off++] = 0xa0 + waitState.size(); // fixstr
		} else {
			buf[off++] = 0xd9;
			buf[off++] = char(waitState.size());
		}
		memcpy(buf.get() + off, waitState.data(), waitState.size());
		off += waitState.size();
		// 3.2 append serialized value
		memcpy(buf.get() + off, iter->second.first, iter->second.second);
		// 4. send the result to fluentd
		send(socket, std::make_shared<Buf>(buf.release(), size));
	}

public:
	SampleSender(Socket& socket, Callback const& callback, std::shared_ptr<Sample> const& sample)
	  : socket(socket), callback(callback), iter(sample->data.begin()), end(sample->data.end()), sample_(sample) {
		sendNext();
	}
};

// Sample function to make instanciation of SampleSender easier
template <class Protocol, class Callback>
std::shared_ptr<SampleSender<Protocol, Callback>> makeSampleSender(typename Protocol::socket& socket,
                                                                   Callback const& callback,
                                                                   std::shared_ptr<Sample> const& sample) {
	return std::make_shared<SampleSender<Protocol, Callback>>(socket, callback, sample);
}

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
			sendImpl(sample);
		}
	}

	void sendImpl(std::shared_ptr<Sample> const& sample) {
		makeSampleSender<Protocol>(
		    socket,
		    [self = this->shared_from_this()](boost::system::error_code const& ec) { self->sendCompletionHandler(ec); },
		    sample);
	}

	void send(std::shared_ptr<Sample> const& sample) override {
		if (_failed) {
			return;
		}
		if (ready) {
			ready = false;
			sendImpl(sample);
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
	std::shared_ptr<FluentDSocket> socket;
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
