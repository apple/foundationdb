/*
 * Tracing.actor.cpp
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

#include "fdbrpc/Msgpack.h"
#include "fdbclient/Tracing.h"
#include "flow/IRandom.h"
#include "flow/UnitTest.h"
#include "flow/Knobs.h"
#include "fdbclient/IKnobCollection.h"
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
};

struct LogfileTracer : ITracer {
	TracerType type() const override { return TracerType::LOG_FILE; }
	void trace(Span const& span) override {
		TraceEvent te(SevInfo, "TracingSpan", span.context.traceID);
		te.detail("SpanID", span.context.spanID)
		    .detail("Location", span.location.name)
		    .detail("Begin", format("%.6f", span.begin))
		    .detail("End", format("%.6f", span.end))
		    .detail("Kind", span.kind)
		    .detail("Status", span.status)
		    .detail("ParentSpanID", span.parentContext.spanID);

		for (const auto& link : span.links) {
			TraceEvent(SevInfo, "TracingSpanLink", span.context.traceID)
			    .detail("TraceID", link.traceID)
			    .detail("SpanID", link.spanID);
		}
		for (const auto& [key, value] : span.attributes) {
			TraceEvent(SevInfo, "TracingSpanTag", span.context.traceID).detail("Key", key).detail("Value", value);
		}
		for (const auto& event : span.events) {
			TraceEvent(SevInfo, "TracingSpanEvent", span.context.traceID)
			    .detail("Name", event.name)
			    .detail("Time", event.time);
			for (const auto& [key, value] : event.attributes) {
				TraceEvent(SevInfo, "TracingSpanEventAttribute", span.context.traceID)
				    .detail("Key", key)
				    .detail("Value", value);
			}
		}
	}
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
	// Serializes span fields as an array into the supplied TraceRequest
	// buffer.
	void serialize_span(const Span& span, MsgpackBuffer& buf) {
		uint16_t size = 12;
		buf.write_byte(size | 0b10010000); // write as array
		serialize_value(span.context.traceID.first(), buf, 0xcf); // trace id
		serialize_value(span.context.traceID.second(), buf, 0xcf); // trace id
		serialize_value(span.context.spanID, buf, 0xcf); // spanid
		// parent span id
		serialize_value(span.parentContext.spanID, buf, 0xcf); // spanId
		// Payload
		serialize_string(span.location.name.toString(), buf);
		serialize_value(span.begin, buf, 0xcb); // start time
		serialize_value(span.end, buf, 0xcb); // end
		// Kind
		serialize_value(span.kind, buf, 0xcc);
		// Status
		serialize_value(span.status, buf, 0xcc);
		// Links
		serialize_vector(span.links, buf);
		// Events
		serialize_vector(span.events, buf);
		// Attributes
		serialize_map(span.attributes, buf);
	}

private:
	// Writes the given vector of linked SpanContext's to the request. If the vector is
	// empty, the request is not modified.
	inline void serialize_vector(const SmallVectorRef<SpanContext>& vec, MsgpackBuffer& buf) {
		int size = vec.size();
		if (size <= 15) {
			buf.write_byte(static_cast<uint8_t>(size) | 0b10010000);
		} else if (size <= 65535) {
			buf.write_byte(0xdc);
			buf.write_byte(reinterpret_cast<const uint8_t*>(&size)[1]);
			buf.write_byte(reinterpret_cast<const uint8_t*>(&size)[0]);
		} else {
			TraceEvent(SevWarn, "TracingSpanSerializeVector").detail("Failed to MessagePack encode large vector", size);
			ASSERT_WE_THINK(false);
		}

		for (const auto& link : vec) {
			serialize_value(link.traceID.first(), buf, 0xcf); // trace id
			serialize_value(link.traceID.second(), buf, 0xcf); // trace id
			serialize_value(link.spanID, buf, 0xcf); // spanid
		}
	}

	// Writes the given vector of linked SpanEventRef's to the request. If the vector is
	// empty, the request is not modified.
	inline void serialize_vector(const SmallVectorRef<SpanEventRef>& vec, MsgpackBuffer& buf) {
		int size = vec.size();
		if (size <= 15) {
			buf.write_byte(static_cast<uint8_t>(size) | 0b10010000);
		} else if (size <= 65535) {
			buf.write_byte(0xdc);
			buf.write_byte(reinterpret_cast<const uint8_t*>(&size)[1]);
			buf.write_byte(reinterpret_cast<const uint8_t*>(&size)[0]);
		} else {
			TraceEvent(SevWarn, "TracingSpanSerializeVector").detail("Failed to MessagePack encode large vector", size);
			ASSERT_WE_THINK(false);
		}

		for (const auto& event : vec) {
			serialize_string(event.name.toString(), buf); // event name
			serialize_value(event.time, buf, 0xcb); // event time
			serialize_vector(event.attributes, buf);
		}
	}

	inline void serialize_vector(const SmallVectorRef<KeyValueRef>& vals, MsgpackBuffer& buf) {
		int size = vals.size();
		if (size <= 15) {
			// N.B. We're actually writing this out as a fixmap here in messagepack format!
			// fixmap	1000xxxx	0x80 - 0x8f
			buf.write_byte(static_cast<uint8_t>(size) | 0b10000000);
		} else {
			TraceEvent(SevWarn, "TracingSpanSerializeVector").detail("Failed to MessagePack encode large vector", size);
			ASSERT_WE_THINK(false);
		}

		for (const auto& kv : vals) {
			serialize_string(kv.key.toString(), buf);
			serialize_string(kv.value.toString(), buf);
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
		request_ = MsgpackBuffer{ .buffer = std::make_unique<uint8_t[]>(kTraceBufferSize),
			                      .data_size = 0,
			                      .buffer_size = kTraceBufferSize };
	}

	TracerType type() const override { return TracerType::NETWORK_LOSSY; }

	void prepare(int size) {
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

		if (size == 0) {
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
	}

	void write() {
		int bytesSent = send(socket_fd_, request_.buffer.get(), request_.data_size, MSG_DONTWAIT);
		if (bytesSent == -1) {
			// Will forgo checking errno here, and assume all error messages
			// should be treated the same.
			++failed_messages_;
			send_error_ = true;
		}
		request_.reset();
	}

	void trace(Span const& span) override {
		prepare(span.location.name.size());
		serialize_span(span, request_);
		write();
	}

private:
	MsgpackBuffer request_;

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
	if (begin > 0.0 && context.isSampled()) {
		end = g_network->now();
		g_tracer->trace(*this);
	}
	arena = std::move(o.arena);
	// All memory referenced in *Ref fields of Span is now (potentially)
	// invalid, and o no longer has ownership of any memory referenced by *Ref
	// fields of o. We must ensure that o no longer references any memory it no
	// longer owns, and that *this no longer references any memory it no longer
	// owns. Not every field references arena memory, but this std::exchange
	// pattern provides a nice template for getting this right in a concise way
	// should we add more fields to Span.

	attributes = std::exchange(o.attributes, decltype(o.attributes)());
	begin = std::exchange(o.begin, decltype(o.begin)());
	context = std::exchange(o.context, decltype(o.context)());
	end = std::exchange(o.end, decltype(o.end)());
	events = std::exchange(o.events, decltype(o.events)());
	kind = std::exchange(o.kind, decltype(o.kind)());
	links = std::exchange(o.links, decltype(o.links)());
	location = std::exchange(o.location, decltype(o.location)());
	parentContext = std::exchange(o.parentContext, decltype(o.parentContext)());
	status = std::exchange(o.status, decltype(o.status)());

	return *this;
}

Span::~Span() {
	if (begin > 0.0 && context.isSampled()) {
		end = g_network->now();
		if (!links.empty()) {
			addAttribute("batchID"_sr, context.traceID.toString());
			for (const auto& link : links) {
				auto linkedLoc = location.name.withSuffix("-linked"_sr);
				Span linked(Location{ linkedLoc }, link);
				linked.begin = begin;
				linked.addAttribute("batchID"_sr, context.traceID.toString());
			}
		}
		g_tracer->trace(*this);
	}
}

TEST_CASE("/flow/Tracing/CreateOTELSpan") {
	// Sampling disabled, no parent.
	Span notSampled("foo"_loc);
	ASSERT(!notSampled.context.isSampled());

	// Ensure child traceID matches parent, when parent is sampled.
	Span childTraceIDMatchesParent("foo"_loc, SpanContext(UID(100, 101), 200, TraceFlags::sampled));
	ASSERT(childTraceIDMatchesParent.context.traceID.first() ==
	       childTraceIDMatchesParent.parentContext.traceID.first());
	ASSERT(childTraceIDMatchesParent.context.traceID.second() ==
	       childTraceIDMatchesParent.parentContext.traceID.second());

	// When the parent isn't sampled AND it has legitimate values we should not sample a child,
	// even if the child was randomly selected for sampling.
	Span parentNotSampled("foo"_loc, SpanContext(UID(1, 1), 1, TraceFlags::unsampled));
	ASSERT(!parentNotSampled.context.isSampled());

	return Void();
};

TEST_CASE("/flow/Tracing/AddEvents") {
	// Use helper method to add an OTELEventRef to an OTELSpan.
	Span span1("span_with_event"_loc);
	auto arena = span1.arena;
	SmallVectorRef<KeyValueRef> attrs;
	attrs.push_back(arena, KeyValueRef("foo"_sr, "bar"_sr));
	span1.addEvent("read_version"_sr, 1.0, attrs);
	ASSERT(span1.events[0].name.toString() == "read_version");
	ASSERT(span1.events[0].time == 1.0);
	ASSERT(span1.events[0].attributes.begin()->key.toString() == "foo");
	ASSERT(span1.events[0].attributes.begin()->value.toString() == "bar");

	// Use helper method to add an OTELEventRef with no attributes to an OTELSpan
	Span span2("span_with_event"_loc);
	span2.addEvent(StringRef(span2.arena, "commit_succeed"_sr), 1234567.100);
	ASSERT(span2.events[0].name.toString() == "commit_succeed");
	ASSERT(span2.events[0].time == 1234567.100);
	ASSERT(span2.events[0].attributes.size() == 0);

	// Add fully constructed OTELEventRef to OTELSpan passed by value.
	Span span3("span_with_event"_loc);
	auto s3Arena = span3.arena;
	SmallVectorRef<KeyValueRef> s3Attrs;
	s3Attrs.push_back(s3Arena, KeyValueRef("xyz"_sr, "123"_sr));
	span3.addEvent("commit_fail"_sr, 1234567.100, s3Attrs).addEvent("commit_succeed"_sr, 1111.001, s3Attrs);
	ASSERT(span3.events[0].name.toString() == "commit_fail");
	ASSERT(span3.events[0].time == 1234567.100);
	ASSERT(span3.events[0].attributes.size() == 1);
	ASSERT(span3.events[0].attributes.begin()->key.toString() == "xyz");
	ASSERT(span3.events[0].attributes.begin()->value.toString() == "123");
	ASSERT(span3.events[1].name.toString() == "commit_succeed");
	ASSERT(span3.events[1].time == 1111.001);
	ASSERT(span3.events[1].attributes.size() == 1);
	ASSERT(span3.events[1].attributes.begin()->key.toString() == "xyz");
	ASSERT(span3.events[1].attributes.begin()->value.toString() == "123");
	return Void();
};

TEST_CASE("/flow/Tracing/AddAttributes") {
	Span span1("span_with_attrs"_loc,
	           SpanContext(deterministicRandom()->randomUniqueID(),
	                       deterministicRandom()->randomUInt64(),
	                       TraceFlags::sampled));
	IKnobCollection::getMutableGlobalKnobCollection().setKnob("tracing_span_attributes_enabled",
	                                                          KnobValueRef::create(bool{ true }));
	auto arena = span1.arena;
	span1.addAttribute(StringRef(arena, "foo"_sr), StringRef(arena, "bar"_sr));
	span1.addAttribute(StringRef(arena, "operation"_sr), StringRef(arena, "grv"_sr));
	ASSERT_EQ(span1.attributes.size(), 3); // Includes default attribute of "address"
	ASSERT(span1.attributes[1] == KeyValueRef("foo"_sr, "bar"_sr));
	ASSERT(span1.attributes[2] == KeyValueRef("operation"_sr, "grv"_sr));

	Span span2("span_with_attrs"_loc,
	           SpanContext(deterministicRandom()->randomUniqueID(),
	                       deterministicRandom()->randomUInt64(),
	                       TraceFlags::sampled));
	auto s2Arena = span2.arena;
	span2.addAttribute(StringRef(s2Arena, "a"_sr), StringRef(s2Arena, "1"_sr))
	    .addAttribute(StringRef(s2Arena, "b"_sr), "2"_sr)
	    .addAttribute(StringRef(s2Arena, "c"_sr), "3"_sr);

	ASSERT_EQ(span2.attributes.size(), 4); // Includes default attribute of "address"
	ASSERT(span2.attributes[1] == KeyValueRef("a"_sr, "1"_sr));
	ASSERT(span2.attributes[2] == KeyValueRef("b"_sr, "2"_sr));
	ASSERT(span2.attributes[3] == KeyValueRef("c"_sr, "3"_sr));
	return Void();
};

TEST_CASE("/flow/Tracing/AddLinks") {
	Span span1("span_with_links"_loc);
	ASSERT(!span1.context.isSampled());
	ASSERT(!span1.context.isValid());
	span1.addLink(SpanContext(UID(100, 101), 200, TraceFlags::sampled));
	span1.addLink(SpanContext(UID(200, 201), 300, TraceFlags::unsampled))
	    .addLink(SpanContext(UID(300, 301), 400, TraceFlags::sampled));

	// Ensure the root span is now sampled and traceID and spanIDs are set.
	ASSERT(span1.context.isSampled());
	ASSERT(span1.context.isValid());
	// Ensure links are present.
	ASSERT(span1.links[0].traceID == UID(100, 101));
	ASSERT(span1.links[0].spanID == 200);
	ASSERT(span1.links[0].m_Flags == TraceFlags::sampled);
	ASSERT(span1.links[1].traceID == UID(200, 201));
	ASSERT(span1.links[1].spanID == 300);
	ASSERT(span1.links[1].m_Flags == TraceFlags::unsampled);
	ASSERT(span1.links[2].traceID == UID(300, 301));
	ASSERT(span1.links[2].spanID == 400);
	ASSERT(span1.links[2].m_Flags == TraceFlags::sampled);

	Span span2("span_with_links"_loc);
	ASSERT(!span2.context.isSampled());
	ASSERT(!span2.context.isValid());
	auto link1 = SpanContext(UID(1, 1), 1, TraceFlags::sampled);
	auto link2 = SpanContext(UID(2, 2), 2, TraceFlags::sampled);
	auto link3 = SpanContext(UID(3, 3), 3, TraceFlags::sampled);
	span2.addLinks({ link1, link2 }).addLinks({ link3 });
	// Ensure the root span is now sampled and traceID and spanIDs are set.
	ASSERT(span2.context.isSampled());
	ASSERT(span2.context.isValid());
	ASSERT(span2.links[0].traceID == UID(1, 1));
	ASSERT(span2.links[0].spanID == 1);
	ASSERT(span2.links[0].m_Flags == TraceFlags::sampled);
	ASSERT(span2.links[1].traceID == UID(2, 2));
	ASSERT(span2.links[1].spanID == 2);
	ASSERT(span2.links[1].m_Flags == TraceFlags::sampled);
	ASSERT(span2.links[2].traceID == UID(3, 3));
	ASSERT(span2.links[2].spanID == 3);
	ASSERT(span2.links[2].m_Flags == TraceFlags::sampled);
	return Void();
};

uint16_t swapUint16BE(uint8_t* index) {
	uint16_t value;
	memcpy(&value, index, sizeof(value));
	return fromBigEndian16(value);
}

uint64_t swapUint64BE(uint8_t* index) {
	uint64_t value;
	memcpy(&value, index, sizeof(value));
	return fromBigEndian64(value);
}

double swapDoubleBE(uint8_t* index) {
	double value;
	memcpy(&value, index, sizeof(value));
	char* const p = reinterpret_cast<char*>(&value);
	for (size_t i = 0; i < sizeof(double) / 2; ++i)
		std::swap(p[i], p[sizeof(double) - i - 1]);
	return value;
}

std::string readMPString(uint8_t* index, int len) {
	uint8_t data[len + 1];
	std::copy(index, index + len, data);
	data[len] = '\0';
	return reinterpret_cast<char*>(data);
}

std::string readMPString(uint8_t* index) {
	auto len = 0;
	switch (*index) {
	case 0xda:
		index++; // read the size in the next 2 bytes
		len = swapUint16BE(index);
		index += 2; // move index past the size bytes
		break;
	default:
		// We & out the bits here that contain the length the initial 3 higher order bits are
		// to signify this is a string of len <= 31 chars.
		len = static_cast<uint8_t>(*index & 0b00011111);
		index++;
	}
	uint8_t data[len + 1];
	std::copy(index, index + len, data);
	data[len] = '\0';
	return reinterpret_cast<char*>(data);
}

// Windows doesn't like lack of header and declaration of constructor for FastUDPTracer
#ifndef WIN32
TEST_CASE("/flow/Tracing/FastUDPMessagePackEncoding") {
	IKnobCollection::getMutableGlobalKnobCollection().setKnob("tracing_span_attributes_enabled",
	                                                          KnobValueRef::create(bool{ true }));
	Span span1("encoded_span"_loc);
	auto request = MsgpackBuffer{ .buffer = std::make_unique<uint8_t[]>(kTraceBufferSize),
		                          .data_size = 0,
		                          .buffer_size = kTraceBufferSize };
	auto tracer = FastUDPTracer();
	tracer.serialize_span(span1, request);
	auto data = request.buffer.get();
	ASSERT(data[0] == 0b10011100); // Default array size.
	request.reset();

	// Test - constructor OTELSpan(const Location& location, const SpanContext parent, const SpanContext& link)
	// Will delegate to other constructors.
	Span span2("encoded_span"_loc,
	           SpanContext(UID(100, 101), 1, TraceFlags::sampled),
	           { SpanContext(UID(200, 201), 2, TraceFlags::sampled) });
	tracer.serialize_span(span2, request);
	data = request.buffer.get();
	ASSERT(data[0] == 0b10011100); // 12 element array.
	// Verify the Parent Trace ID overwrites this spans Trace ID
	ASSERT(data[1] == 0xcf);
	ASSERT(swapUint64BE(&data[2]) == 100);
	ASSERT(data[10] == 0xcf);
	ASSERT(swapUint64BE(&data[11]) == 101);
	ASSERT(data[19] == 0xcf);
	// We don't care about the next 8 bytes, they are the ID for the span itself and will be random.
	// Parent SpanID.
	ASSERT(data[28] == 0xcf);
	ASSERT(swapUint64BE(&data[29]) == 1);
	// Read and verify span name
	ASSERT(readMPString(&data[37]) == "encoded_span");
	// Verify begin/end is encoded, we don't care about the values
	ASSERT(data[50] == 0xcb);
	ASSERT(data[59] == 0xcb);
	// SpanKind
	ASSERT(data[68] == 0xcc);
	ASSERT(data[69] == static_cast<uint8_t>(SpanKind::SERVER));
	// Status
	ASSERT(data[70] == 0xcc);
	ASSERT(data[71] == static_cast<uint8_t>(SpanStatus::OK));
	// Linked SpanContext
	ASSERT(data[72] == 0b10010001);
	ASSERT(data[73] == 0xcf);
	ASSERT(swapUint64BE(&data[74]) == 200);
	ASSERT(data[82] == 0xcf);
	ASSERT(swapUint64BE(&data[83]) == 201);
	ASSERT(data[91] == 0xcf);
	ASSERT(swapUint64BE(&data[92]) == 2);
	// Events
	ASSERT(data[100] == 0b10010000); // empty
	// Attributes
	ASSERT(data[101] == 0b10000001); // single k/v pair
	ASSERT(data[102] == 0b10100111); // length of key string "address" == 7

	request.reset();

	// Exercise all fluent interfaces, include links, events, and attributes.
	Span span3("encoded_span_3"_loc, SpanContext());
	auto s3Arena = span3.arena;
	SmallVectorRef<KeyValueRef> attrs;
	attrs.push_back(s3Arena, KeyValueRef("foo"_sr, "bar"_sr));
	span3.addAttribute("operation"_sr, "grv"_sr)
	    .addLink(SpanContext(UID(300, 301), 400, TraceFlags::sampled))
	    .addEvent(StringRef(s3Arena, "event1"_sr), 100.101, attrs);
	tracer.serialize_span(span3, request);
	data = request.buffer.get();
	ASSERT(data[0] == 0b10011100); // 12 element array.
	// We don't care about the next 36 bytes as there is no parent and a randomly assigned Trace and SpanID
	// Read and verify span name
	ASSERT(readMPString(&data[37]) == "encoded_span_3");
	// Verify begin/end is encoded, we don't care about the values
	ASSERT(data[52] == 0xcb);
	ASSERT(data[61] == 0xcb);
	// SpanKind
	ASSERT(data[70] == 0xcc);
	ASSERT(data[71] == static_cast<uint8_t>(SpanKind::SERVER));
	// Status
	ASSERT(data[72] == 0xcc);
	ASSERT(data[73] == static_cast<uint8_t>(SpanStatus::OK));
	// Linked SpanContext
	ASSERT(data[74] == 0b10010001);
	ASSERT(data[75] == 0xcf);
	ASSERT(swapUint64BE(&data[76]) == 300);
	ASSERT(data[84] == 0xcf);
	ASSERT(swapUint64BE(&data[85]) == 301);
	ASSERT(data[93] == 0xcf);
	ASSERT(swapUint64BE(&data[94]) == 400);
	// Events
	ASSERT(data[102] == 0b10010001); // empty
	ASSERT(readMPString(&data[103]) == "event1");
	ASSERT(data[110] == 0xcb);
	ASSERT(swapDoubleBE(&data[111]) == 100.101);
	// Events Attributes
	ASSERT(data[119] == 0b10000001); // single k/v pair
	ASSERT(readMPString(&data[120]) == "foo");
	ASSERT(readMPString(&data[124]) == "bar");
	// Attributes
	ASSERT(data[128] == 0b10000010); // two k/v pair
	// Reconstruct map from MessagePack wire format data and verify.
	std::unordered_map<std::string, std::string> attributes;
	auto index = 129;

	auto firstKey = readMPString(&data[index]);
	index += firstKey.length() + 1; // +1 for control byte
	auto firstValue = readMPString(&data[index]);
	index += firstValue.length() + 1; // +1 for control byte
	attributes[firstKey] = firstValue;

	auto secondKey = readMPString(&data[index]);
	index += secondKey.length() + 1; // +1 for control byte
	auto secondValue = readMPString(&data[index]);
	attributes[secondKey] = secondValue;
	// We don't know what the value for address will be, so just verify it is in the map.
	ASSERT(attributes.find("address") != attributes.end());
	ASSERT(attributes["operation"] == "grv");

	request.reset();

	// Test message pack encoding for string >= 256 && <= 65535 chars
	const char* longString = "yGUtj42gSKfdqib3f0Ri4OVhD7eWyTbKsH/g9+x4UWyXry7NIBFIapPV9f1qdTRl"
	                         "2jXcZI8Ua/Gp8k9EBn7peaEN1uj4w9kf4FQ2Lalu0VrA4oquQoaKYr+wPsLBak9i"
	                         "uyZDF9sX/HW4pVvQhPQdXQWME5E7m58XFMpZ3H8HNXuytWInEuh97SRLlI0RhrvG"
	                         "ixNpYtYlvghsLCrEdZMMGnS2gXgGufIdg1xKJd30fUbZLHcYIC4DTnL5RBpkbQCR"
	                         "SGKKUrpIb/7zePhBDi+gzUzyAcbQ2zUbFWI1KNi3zQk58uUG6wWJZkw+GCs7Cc3V"
	                         "OUxOljwCJkC4QTgdsbbFhxUC+rtoHV5xAqoTQwR0FXnWigUjP7NtdL6huJUr3qRv"
	                         "40c4yUI1a4+P5vJa";
	Span span4;
	auto location = Location();
	location.name = StringRef(span4.arena, longString);
	span4.location = location;
	tracer.serialize_span(span4, request);
	data = request.buffer.get();
	ASSERT(data[0] == 0b10011100); // 12 element array.
	// We don't care about the next 36 bytes as there is no parent and a randomly assigned Trace and SpanID
	// Read and verify span name
	ASSERT(data[37] == 0xda);
	ASSERT(readMPString(&data[37]) == longString);
	return Void();
};
#endif
