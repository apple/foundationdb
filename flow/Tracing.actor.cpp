/*
 * Tracing.cpp
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

#include "flow/network.h"
#include "flow/actorcompiler.h" // has to be last include

#include <functional>

namespace {

// Serialized packets will be sent to this port via UDP. In simulation, a UDP
// server also listens on this port.
constexpr uint16_t kUdpPort = 8889;

// Initial size of buffer used to store serialized traces. Buffer will be
// resized when necessary.
constexpr int kTraceBufferSize = 1024;

// The time interval between each report of the tracer queue size (seconds).
constexpr float kQueueSizeLogInterval = 5.0;

struct NoopTracer : ITracer {
	TracerType type() const { return TracerType::DISABLED; }
	void trace(Span const& span) override {}
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
	}
};

struct TraceRequest {
	uint8_t* buffer;
	// Amount of data in buffer (bytes).
	std::size_t data_size;
	// Size of buffer (bytes).
	std::size_t buffer_size;

	void write_byte(uint8_t byte) {
		write_bytes(&byte, 1);
	}

	void write_bytes(uint8_t* buf, std::size_t n) {
		resize(n);
		memcpy(buffer + data_size, buf, n);
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
		uint8_t* new_buffer = new uint8_t[size];
		memcpy(new_buffer, buffer, data_size);
		free(buffer);
		buffer = new_buffer;
		buffer_size = size;
	}

	void reset() {
		data_size = 0;
	}
};

// A server listening for UDP trace messages, run only in simulation.
ACTOR Future<Void> simulationStartServer() {
	TraceEvent(SevInfo, "UDPServerStarted").detail("Port", kUdpPort);
	state NetworkAddress localAddress = NetworkAddress::parse("127.0.0.1:" + std::to_string(kUdpPort));
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
		ASSERT(message[0] == (6 | 0b10010000));
	}
}

ACTOR Future<Void> traceSend(FutureStream<TraceRequest> inputStream, std::queue<TraceRequest>* buffers, int* pendingMessages, bool* sendError) {
	state NetworkAddress localAddress = NetworkAddress::parse("127.0.0.1:" + std::to_string(kUdpPort));
	state Reference<IUDPSocket> socket = wait(INetworkConnections::net()->createUDPSocket(localAddress));

	loop choose {
		when(state TraceRequest request = waitNext(inputStream)) {
			try {
				if (!(*sendError)) {
					int bytesSent = wait(socket->send(request.buffer, request.buffer + request.data_size));
				}
				 --(*pendingMessages);
				request.reset();
				buffers->push(request);
			} catch (Error& e) {
				TraceEvent("TracingSpanSendError").detail("Error", e.what());
				*sendError = true;
			}
		}
	}
}

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

struct FluentDTracer : ITracer {
public:
	~FluentDTracer() override {
		while (!buffers_.empty()) {
			auto& request = buffers_.front();
			buffers_.pop();
			free(request.buffer);
		}
	}

	TracerType type() const override { return TracerType::FLUENTD; }

	// Serializes the given span to msgpack format and sends the data via UDP.
	void trace(Span const& span) override {
		static std::once_flag once;
		std::call_once(once, [&]() {
			send_actor_ = traceSend(stream_.getFuture(), &buffers_, &pending_messages_, &send_error_);
			log_actor_ = traceLog(&pending_messages_, &send_error_);
			if (g_network->isSimulated()) {
				udp_server_actor_ = simulationStartServer();
			}
		});

		if (span.location.name.size() == 0) {
			return;
		}

		// ASSERT(!send_actor_.isReady());
		// ASSERT(!log_actor_.isReady());

		if (buffers_.empty()) {
			buffers_.push(TraceRequest{
				.buffer = new uint8_t[kTraceBufferSize],
				.data_size = 0,
				.buffer_size = kTraceBufferSize
			});
		}

		auto request = buffers_.front();
		buffers_.pop();

		// Serialize span fields as an array. If you change the serialization
		// format here, make sure to update the fluentd filter to be able to
		// correctly parse the updated format!
		uint8_t size = 5;
		if (span.parents.size() == 0) --size;
		request.write_byte(size | 0b10010000); // write as array

		serialize_string(span.context.toString(), request);

		serialize_value(span.begin, request, 0xcb);
		serialize_value(span.end, request, 0xcb);

		serialize_string(span.location.name.toString(), request);

		serialize_vector(span.parents, request);

		++pending_messages_;
		stream_.send(request);
	}

private:
	// Writes the given value in big-endian order to the request. Sets the
	// first byte to msgpack_type if it is supplied.
	template <typename T>
	inline void serialize_value(const T& val, TraceRequest& request, uint8_t msgpack_type = 0) {
		if (msgpack_type) {
			request.write_byte(msgpack_type);
		}

		const uint8_t* p = reinterpret_cast<const uint8_t*>(std::addressof(val));
		for (size_t i = 0; i < sizeof(T); ++i) {
			request.write_byte(p[sizeof(T) - i - 1]);
		}
	}

	// Writes the given string to the request as a sequence of bytes. Inserts a
	// format byte at the beginning of the string according to the its length,
	// specified by the msgpack specification.
	inline void serialize_string(const std::string& str, TraceRequest& request) {
		int size = str.size();
		ASSERT(size > 0);

		if (size <= 31) {
			request.write_byte((uint8_t) size | 0b10100000);
		} else if (size <= 255) {
			request.write_byte(0xd9);
			request.write_byte((uint8_t) size);
		} else {
			// TODO: Add support for longer strings if necessary.
			ASSERT(false);
		}

		request.write_bytes((uint8_t*) str.data(), size);
	}

	// Writes the given vector to the request. Assumes each element in the
	// vector is a SpanID, and serializes as two big-endian 64-bit integers.
	inline void serialize_vector(const SmallVectorRef<SpanID>& vec, TraceRequest& request) {
		int size = vec.size();
		if (size == 0) {
			return;
		}

		if (size <= 15) {
			request.write_byte((uint8_t) size | 0b10010000);
		} else if (size <= 65535) {
			request.write_byte(0xdc);
			request.write_byte(((uint8_t*) &size)[1]);
			request.write_byte(((uint8_t*) &size)[0]);
		} else {
			// TODO: Add support for longer vectors if necessary.
			ASSERT(false);
		}

		for (const auto& parentContext : vec) {
			serialize_string(parentContext.toString(), request);
		}
	}

	// Sending data is asynchronous, so it is necessary to keep the buffer
	// around until the send completes. Therefore, multiple buffers may be
	// needed at any one time to handle multiple trace calls.
	std::queue<TraceRequest> buffers_;
	int pending_messages_;
	bool send_error_;

	PromiseStream<TraceRequest> stream_;
	Future<Void> send_actor_;
	Future<Void> log_actor_;
	Future<Void> udp_server_actor_;
};

ITracer* g_tracer = new NoopTracer();

} // namespace

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
	case TracerType::FLUENTD:
		g_tracer = new FluentDTracer{};
		break;
	}
}

ITracer::~ITracer() {}

Span& Span::operator=(Span&& o) {
	if (begin > 0.0) {
		end = g_network->now();
		g_tracer->trace(*this);
	}
	arena = std::move(o.arena);
	context = o.context;
	begin = o.begin;
	end = o.end;
	location = o.location;
	parents = std::move(o.parents);
	return *this;
}

Span::~Span() {
	if (begin > 0.0) {
		end = g_network->now();
		g_tracer->trace(*this);
	}
}
