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

ACTOR Future<Void> traceSend(FutureStream<TraceRequest> inputStream, std::queue<TraceRequest>* buffers, int* pending_messages) {
	state NetworkAddress localAddress = NetworkAddress::parse("127.0.0.1:8889");
	state Reference<IUDPSocket> socket = wait(INetworkConnections::net()->createUDPSocket(localAddress));

	loop choose {
		when(state TraceRequest request = waitNext(inputStream)) {
			int bytesSent = wait(socket->send(request.buffer, request.buffer + request.data_size));
			--(*pending_messages);
			request.reset();
			buffers->push(request);
		}
	}
}

ACTOR Future<Void> traceLog(int* pending_messages) {
	loop {
		TraceEvent("TracingSpanQueueSize").detail("PendingMessages", *pending_messages);
		wait(delay(5.0));
	}
}

struct FluentDTracer : ITracer {
public:
	~FluentDTracer() override {
		// TODO: Handle case where socket->send returns after FluentDTracer instance is destructed?
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
			send_actor_ = traceSend(stream_.getFuture(), &buffers_, &pending_messages_);
			log_actor_ = traceLog(&pending_messages_);
		});

		// ASSERT(!actor_.isReady());

		if (buffers_.empty()) {
			buffers_.push(TraceRequest{
				.buffer = new uint8_t[256],
				.data_size = 0,
				.buffer_size = 256
			});
		}

		auto request = buffers_.front();
		buffers_.pop();

		// request.write_byte(6 | 0b10010000); // write as array
		request.write_byte(6 | 0b10000000); // write as map

		serialize_string("first", request);
		serialize_value(span.context.first(), request, 0xcf);
		serialize_string("second", request);
		serialize_value(span.context.second(), request, 0xcf);

		serialize_string("begin", request);
		serialize_value(span.begin, request, 0xcb);
		serialize_string("end", request);
		serialize_value(span.end, request, 0xcb);

		serialize_string("location", request);
		serialize_string(span.location.name.toString(), request);

		serialize_string("parents", request);
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
		int size = vec.size() * 2;
		if (size <= 15) {
			request.write_byte((uint8_t) size | 0b10010000);
		} else if (size <= 65535) {
			request.write_byte(0xdc);
			request.write_bytes((uint8_t*) &size, 2);
		} else {
			// TODO: Add support for longer vectors if necessary.
			ASSERT(false);
		}

		for (const auto& parentContext : vec) {
			serialize_value(parentContext.first(), request, 0xcf);
			serialize_value(parentContext.second(), request, 0xcf);
		}
	}

	// Sending data is asynchronous, so it is necessary to keep the buffer
	// around until the send completes. Therefore, multiple buffers may be
	// needed at any one time to handle multiple trace calls.
	std::queue<TraceRequest> buffers_;
	int pending_messages_;

	PromiseStream<TraceRequest> stream_;
	Future<Void> send_actor_;
	Future<Void> log_actor_;
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
