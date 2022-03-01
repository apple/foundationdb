/*
 * Tracing.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#include "flow/Tracing.h"
#include "fdbserver/LocalConfiguration.h"
#include "flow/IRandom.h"
#include "flow/UnitTest.h"

#include "fdbclient/FDBTypes.h"
#include "flow/Knobs.h"
#include "flow/network.h"

#include <functional>
#include <iomanip>
#include <memory>

#include "flow/actorcompiler.h" // has to be last include

#ifdef NO_INTELLISENSE
namespace {
#endif

// Initial size of buffer used to store serialized traces. Buffer will be
// resized when necessary.
constexpr int kTraceBufferSize = 1024;

// The time interval between each report of the tracer queue size (seconds).
constexpr float kQueueSizeLogInterval = 5.0;

struct NoopTracer : ITracer {
	TracerType type() const override { return TracerType::DISABLED; }
	void trace(Span const& span) override {}
	void trace(OTELSpan const& span) override {}
};

struct LogfileTracer : ITracer {
	TracerType type() const override { return TracerType::LOG_FILE; }
	void trace(Span const& span) override {
		TraceEvent te(SevInfo, "TracingSpan", span.context);
		te.detail("Location", span.location.name)
		    .detail("Begin", format("%.6f", span.begin))
		    .detail("End", format("%.6f", span.end));
		if (span.parents.size() == 1) {
			te.detail("Parent", *span.parents.begin());
		} else {
			for (auto parent : span.parents) {
				TraceEvent(SevInfo, "TracingSpanAddParent", span.context).detail("AddParent", parent);
			}
		}
		for (const auto& [key, value] : span.tags) {
			TraceEvent(SevInfo, "TracingSpanTag", span.context).detail("Key", key).detail("Value", value);
		}
	}
	void trace(OTELSpan const& span) override {
		TraceEvent te(SevInfo, "TracingSpan", span.context.traceID);
		te.detail("SpanID", span.context.spanID)
		    .detail("Location", span.location.name)
		    .detail("Begin", format("%.6f", span.begin))
		    .detail("End", format("%.6f", span.end))
		    .detail("Kind", span.kind)
		    .detail("Status", span.status)
		    .detail("Parent Span ID", span.parentContext.spanID);

		for (const auto& [key, value] : span.attributes) {
			TraceEvent(SevInfo, "TracingSpanTag", span.context.traceID).detail("Key", key).detail("Value", value);
		}
		// TODO do we want links and events or is that too much noise?
	}
};

struct TraceRequest {
	std::unique_ptr<uint8_t[]> buffer;
	// Amount of data in buffer (bytes).
	std::size_t data_size;
	// Size of buffer (bytes).
	std::size_t buffer_size;

	void write_byte(uint8_t byte) { write_bytes(&byte, 1); }

	void write_bytes(const uint8_t* buf, std::size_t n) {
		resize(n);
		std::copy(buf, buf + n, buffer.get() + data_size);
		data_size += n;
	}

	void resize(std::size_t n) {
		if (data_size + n <= buffer_size) {
			return;
		}

		std::size_t size = buffer_size;
		while (size < data_size + n) {
			size *= 2;
		}

		TraceEvent(SevInfo, "TracingSpanResizedBuffer").detail("OldSize", buffer_size).detail("NewSize", size);
		auto new_buffer = std::make_unique<uint8_t[]>(size);
		std::copy(buffer.get(), buffer.get() + data_size, new_buffer.get());
		buffer = std::move(new_buffer);
		buffer_size = size;
	}

	void reset() { data_size = 0; }
};

// A server listening for UDP trace messages, run only in simulation.
ACTOR Future<Void> simulationStartServer() {
	// We're going to force the address to be loopback regardless of FLOW_KNOBS->TRACING_UDP_LISTENER_ADDR
	// because we're in simulation testing mode.
	TraceEvent(SevInfo, "UDPServerStarted")
	    .detail("Address", "127.0.0.1")
	    .detail("Port", FLOW_KNOBS->TRACING_UDP_LISTENER_PORT);
	state NetworkAddress localAddress =
	    NetworkAddress::parse("127.0.0.1:" + std::to_string(FLOW_KNOBS->TRACING_UDP_LISTENER_PORT));
	state Reference<IUDPSocket> serverSocket = wait(INetworkConnections::net()->createUDPSocket(localAddress));
	serverSocket->bind(localAddress);

	state Standalone<StringRef> packetString = makeString(IUDPSocket::MAX_PACKET_SIZE);
	state uint8_t* packet = mutateString(packetString);

	loop {
		int size = wait(serverSocket->receive(packet, packet + IUDPSocket::MAX_PACKET_SIZE));
		auto message = packetString.substr(0, size);

		// For now, just check the first byte in the message matches. Data is
		// currently written as an array, so first byte should match msgpack
		// array notation. In the future, the entire message should be
		// deserialized to make sure all data is written correctly.
		ASSERT(message[0] == (4 | 0b10010000) || (5 | 0b10010000));
	}
}

/*
// Runs on an interval, printing debug information and performing other
// connection tasks.
ACTOR Future<Void> traceLog(int* pendingMessages, bool* sendError) {
    state bool sendErrorReset = false;

    loop {
        TraceEvent("TracingSpanQueueSize").detail("PendingMessages", *pendingMessages);

        // Wait at least one full loop before attempting to send messages
        // again.
        if (sendErrorReset) {
            sendErrorReset = false;
            *sendError = false;
        } else if (*sendError) {
            sendErrorReset = true;
        }

        wait(delay(kQueueSizeLogInterval));
    }
}
*/

struct UDPTracer : public ITracer {
protected:
	// Serializes span fields as an array into the supplied TraceRequest
	// buffer.
	void serialize_span(const Span& span, TraceRequest& request) {
		// If you change the serialization format here, make sure to update the
		// fluentd filter to be able to correctly parse the updated format! See
		// the msgpack specification for more info on the bit patterns used
		// here.
		uint8_t size = 8;
		if (span.parents.size() == 0)
			--size;
		request.write_byte(size | 0b10010000); // write as array

		serialize_string(g_network->getLocalAddress().toString(), request); // ip:port

		serialize_value(span.context.first(), request, 0xcf); // trace id
		serialize_value(span.context.second(), request, 0xcf); // token (span id)

		serialize_value(span.begin, request, 0xcb); // start time
		serialize_value(span.end - span.begin, request, 0xcb); // duration

		serialize_string(span.location.name.toString(), request);

		serialize_map(span.tags, request);

		serialize_vector(span.parents, request);
	}

	void serialize_span(const OTELSpan& span, TraceRequest& request) {
		uint8_t size = 11;
		size = size + span.links.size() + span.events.size() + span.attributes.size();
		request.write_byte(size | 0b10010000); // write as array
		serialize_value(span.context.traceID.first(), request, 0xcf); // trace id
		serialize_value(span.context.traceID.second(), request, 0xcf); // trace id
		serialize_value(span.context.spanID, request, 0xcf); // spanid
		// parent value
		serialize_value(span.parentContext.traceID.first(), request, 0xcf); // trace id
		serialize_value(span.parentContext.traceID.second(), request, 0xcf); // trace id
		serialize_value(span.parentContext.spanID, request, 0xcf); // spanId
		// Payload
		serialize_string(span.location.name.toString(), request);
		serialize_value(span.begin, request, 0xcb); // start time
		serialize_value(span.end, request, 0xcb); // end
		// Kind
		serialize_value(span.kind, request, 0xcc); // end
		// Status
		serialize_value(span.status, request, 0xcc); // end
		// Links
		serialize_vector(span.links, request);
		// Events
		serialize_vector(span.events, request);
		// Attributes
		serialize_map(span.attributes, request);
	}

private:
	// Writes the given value in big-endian format to the request. Sets the
	// first byte to msgpack_type.
	template <typename T>
	inline void serialize_value(const T& val, TraceRequest& request, uint8_t msgpack_type) {
		request.write_byte(msgpack_type);

		const uint8_t* p = reinterpret_cast<const uint8_t*>(std::addressof(val));
		for (size_t i = 0; i < sizeof(T); ++i) {
			request.write_byte(p[sizeof(T) - i - 1]);
		}
	}

	// Writes the given string to the request as a sequence of bytes. Inserts a
	// format byte at the beginning of the string according to the its length,
	// as specified by the msgpack specification.
	inline void serialize_string(const uint8_t* c, int length, TraceRequest& request) {
		if (length <= 31) {
			// A size 0 string is ok. We still need to write a byte
			// identifiying the item as a string, but can set the size to 0.
			request.write_byte(static_cast<uint8_t>(length) | 0b10100000);
		} else if (length <= 255) {
			request.write_byte(0xd9);
			request.write_byte(static_cast<uint8_t>(length));
		} else if (length <= 65535) {
			request.write_byte(0xda);
			request.write_byte(static_cast<uint16_t>(length));
		} else {
			// TODO: Add support for longer strings if necessary.
			ASSERT(false);
		}

		request.write_bytes(c, length);
	}

	inline void serialize_string(const std::string& str, TraceRequest& request) {
		serialize_string(reinterpret_cast<const uint8_t*>(str.data()), str.size(), request);
	}

	// Writes the given vector of SpanIDs to the request. If the vector is
	// empty, the request is not modified.
	inline void serialize_vector(const SmallVectorRef<SpanID>& vec, TraceRequest& request) {
		int size = vec.size();
		if (size == 0) {
			return;
		}

		if (size <= 15) {
			request.write_byte(static_cast<uint8_t>(size) | 0b10010000);
		} else if (size <= 65535) {
			request.write_byte(0xdc);
			request.write_byte(reinterpret_cast<const uint8_t*>(&size)[1]);
			request.write_byte(reinterpret_cast<const uint8_t*>(&size)[0]);
		} else {
			// TODO: Add support for longer vectors if necessary.
			ASSERT(false);
		}

		for (const auto& parentContext : vec) {
			serialize_value(parentContext.second(), request, 0xcf);
		}
	}

	// Writes the given vector of linked SpanContext's to the request. If the vector is
	// empty, the request is not modified.
	inline void serialize_vector(const std::vector<SpanContext>& vec, TraceRequest& request) {
		int size = vec.size();
		if (size == 0) {
			return;
		}

		if (size <= 15) {
			request.write_byte(static_cast<uint8_t>(size) | 0b10010000);
		} else if (size <= 65535) {
			request.write_byte(0xdc);
			request.write_byte(reinterpret_cast<const uint8_t*>(&size)[1]);
			request.write_byte(reinterpret_cast<const uint8_t*>(&size)[0]);
		} else {
			// TODO: Add support for longer vectors if necessary.
			ASSERT(false);
		}

		for (const auto& link : vec) {
			serialize_value(link.traceID.first(), request, 0xcf); // trace id
			serialize_value(link.traceID.second(), request, 0xcf); // trace id
			serialize_value(link.spanID, request, 0xcf); // spanid
		}
	}

	// Writes the given vector of linked SpanContext's to the request. If the vector is
	// empty, the request is not modified.
	inline void serialize_vector(const std::vector<OTELEvent>& vec, TraceRequest& request) {
		int size = vec.size();
		if (size == 0) {
			return;
		}

		if (size <= 15) {
			request.write_byte(static_cast<uint8_t>(size) | 0b10010000);
		} else if (size <= 65535) {
			request.write_byte(0xdc);
			request.write_byte(reinterpret_cast<const uint8_t*>(&size)[1]);
			request.write_byte(reinterpret_cast<const uint8_t*>(&size)[0]);
		} else {
			// TODO: Add support for longer vectors if necessary.
			ASSERT(false);
		}

		for (const auto& event : vec) {
			serialize_string(event.name.toString(), request); // event name
			serialize_value(event.time, request, 0xcb); // event time
			serialize_map(event.attributes, request);
		}
	}

	inline void serialize_map(const std::unordered_map<StringRef, StringRef>& map, TraceRequest& request) {
		int size = map.size();

		if (size <= 15) {
			request.write_byte(static_cast<uint8_t>(size) | 0b10000000);
		} else {
			// TODO: Add support for longer maps if necessary.
			ASSERT(false);
		}

		for (const auto& [key, value] : map) {
			serialize_string(key.begin(), key.size(), request);
			serialize_string(value.begin(), value.size(), request);
		}
	}
};

#ifndef WIN32
ACTOR Future<Void> fastTraceLogger(int* unreadyMessages, int* failedMessages, int* totalMessages, bool* sendError) {
	state bool sendErrorReset = false;

	loop {
		TraceEvent("TracingSpanStats")
		    .detail("UnreadyMessages", *unreadyMessages)
		    .detail("FailedMessages", *failedMessages)
		    .detail("TotalMessages", *totalMessages)
		    .detail("SendError", *sendError);

		if (sendErrorReset) {
			sendErrorReset = false;
			*sendError = false;
		} else if (*sendError) {
			sendErrorReset = true;
		}

		wait(delay(kQueueSizeLogInterval));
	}
}

struct FastUDPTracer : public UDPTracer {
	FastUDPTracer()
	  : unready_socket_messages_(0), failed_messages_(0), total_messages_(0), socket_fd_(-1), send_error_(false) {
		request_ = TraceRequest{ .buffer = std::make_unique<uint8_t[]>(kTraceBufferSize),
			                     .data_size = 0,
			                     .buffer_size = kTraceBufferSize };
	}

	TracerType type() const override { return TracerType::NETWORK_LOSSY; }

	// TODO - DRY this up
	void trace(OTELSpan const& span) override {
		static std::once_flag once;
		std::call_once(once, [&]() {
			log_actor_ = fastTraceLogger(&unready_socket_messages_, &failed_messages_, &total_messages_, &send_error_);
			std::string destAddr = FLOW_KNOBS->TRACING_UDP_LISTENER_ADDR;
			if (g_network->isSimulated()) {
				udp_server_actor_ = simulationStartServer();
				// Force loopback when in simulation mode
				destAddr = "127.0.0.1";
			}
			NetworkAddress destAddress =
			    NetworkAddress::parse(destAddr + ":" + std::to_string(FLOW_KNOBS->TRACING_UDP_LISTENER_PORT));

			socket_ = INetworkConnections::net()->createUDPSocket(destAddress);
		});

		if (span.location.name.size() == 0) {
			return;
		}

		++total_messages_;
		if (!socket_.isReady()) {
			++unready_socket_messages_;
			return;
		} else if (socket_fd_ == -1) {
			socket_fd_ = socket_.get()->native_handle();
		}

		if (send_error_) {
			return;
		}

		serialize_span(span, request_);

		int bytesSent = send(socket_fd_, request_.buffer.get(), request_.data_size, MSG_DONTWAIT);
		if (bytesSent == -1) {
			// Will forgo checking errno here, and assume all error messages
			// should be treated the same.
			++failed_messages_;
			send_error_ = true;
		}
		request_.reset();
	}

	// TODO - DRY this up
	void trace(Span const& span) override {
		static std::once_flag once;
		std::call_once(once, [&]() {
			log_actor_ = fastTraceLogger(&unready_socket_messages_, &failed_messages_, &total_messages_, &send_error_);
			std::string destAddr = FLOW_KNOBS->TRACING_UDP_LISTENER_ADDR;
			if (g_network->isSimulated()) {
				udp_server_actor_ = simulationStartServer();
				// Force loopback when in simulation mode
				destAddr = "127.0.0.1";
			}
			NetworkAddress destAddress =
			    NetworkAddress::parse(destAddr + ":" + std::to_string(FLOW_KNOBS->TRACING_UDP_LISTENER_PORT));

			socket_ = INetworkConnections::net()->createUDPSocket(destAddress);
		});

		if (span.location.name.size() == 0) {
			return;
		}

		++total_messages_;
		if (!socket_.isReady()) {
			++unready_socket_messages_;
			return;
		} else if (socket_fd_ == -1) {
			socket_fd_ = socket_.get()->native_handle();
		}

		if (send_error_) {
			return;
		}

		serialize_span(span, request_);

		int bytesSent = send(socket_fd_, request_.buffer.get(), request_.data_size, MSG_DONTWAIT);
		if (bytesSent == -1) {
			// Will forgo checking errno here, and assume all error messages
			// should be treated the same.
			++failed_messages_;
			send_error_ = true;
		}
		request_.reset();
	}

private:
	TraceRequest request_;

	int unready_socket_messages_;
	int failed_messages_;
	int total_messages_;

	int socket_fd_;
	bool send_error_;

	Future<Reference<IUDPSocket>> socket_;
	Future<Void> log_actor_;
	Future<Void> udp_server_actor_;
};
#endif

ITracer* g_tracer = new NoopTracer();

#ifdef NO_INTELLISENSE
} // namespace
#endif

void openTracer(TracerType type) {
	if (g_tracer->type() == type) {
		return;
	}
	delete g_tracer;
	switch (type) {
	case TracerType::DISABLED:
		g_tracer = new NoopTracer{};
		break;
	case TracerType::LOG_FILE:
		g_tracer = new LogfileTracer{};
		break;
	case TracerType::NETWORK_LOSSY:
#ifndef WIN32
		g_tracer = new FastUDPTracer{};
#endif
		break;
	case TracerType::SIM_END:
		ASSERT(false);
		break;
	}
}

ITracer::~ITracer() {}

Span& Span::operator=(Span&& o) {
	if (begin > 0.0 && context.second() > 0) {
		end = g_network->now();
		g_tracer->trace(*this);
	}
	arena = std::move(o.arena);
	context = o.context;
	begin = o.begin;
	end = o.end;
	location = o.location;
	parents = std::move(o.parents);
	o.begin = 0;
	// TODO: Why no tags in assignment copy overload?
	return *this;
}

Span::~Span() {
	if (begin > 0.0 && context.second() > 0) {
		end = g_network->now();
		g_tracer->trace(*this);
	}
}

OTELSpan& OTELSpan::operator=(OTELSpan&& o) {
	if (begin > 0.0 && o.context.isSampled() > 0) {
		end = g_network->now();
		g_tracer->trace(*this);
	}
	context = o.context;
	parentContext = o.parentContext;
	begin = o.begin;
	end = o.end;
	location = o.location;
	links = std::move(o.links);
	events = std::move(o.events);
	// TODO do we need tags?
	o.begin = 0;
	return *this;
}

OTELSpan::~OTELSpan() {
	if (begin > 0.0 && context.isSampled()) {
		end = g_network->now();
		g_tracer->trace(*this);
	}
}

TEST_CASE("/flow/Tracing/CreateOTELSpan") {
	// Sampling disabled, no parent.
	OTELSpan notSampled("foo"_loc);
	ASSERT(!notSampled.context.isSampled());

	// FORCE SAMPLING
	OTELSpan sampled("foo"_loc, []() { return 1.0; });
	ASSERT(sampled.context.isSampled());

	// Ensure child traceID matches parent, when parent is sampled.
	OTELSpan childTraceIDMatchesParent(
	    "foo"_loc, []() { return 1.0; }, SpanContext(UID(100, 101), 200, TraceFlags::sampled));
	ASSERT(childTraceIDMatchesParent.context.traceID.first() ==
	       childTraceIDMatchesParent.parentContext.traceID.first());
	ASSERT(childTraceIDMatchesParent.context.traceID.second() ==
	       childTraceIDMatchesParent.parentContext.traceID.second());

	// When the parent isn't sampled AND it has legitimate values we should not sample a child,
	// even if the child was randomly selected for sampling.
	OTELSpan parentNotSampled(
	    "foo"_loc, []() { return 1.0; }, SpanContext(UID(1, 1), 1, TraceFlags::unsampled));
	ASSERT(!parentNotSampled.context.isSampled());

	// When the parent isn't sampled AND it has zero values for traceID and spanID this means
	// we should defer to the child as the new root of the trace as there was no actual parent.
	// If the child was sampled we should send the child trace with a null parent.
	OTELSpan noParent(
	    "foo"_loc, []() { return 1.0; }, SpanContext(UID(0, 0), 0, TraceFlags::unsampled));
	ASSERT(noParent.context.isSampled());

	return Void();
};
