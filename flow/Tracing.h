/*
 * Tracing.h
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

#pragma once

#include "fdbclient/FDBTypes.h"
#include "flow/IRandom.h"
#include <unordered_set>
#include <atomic>

struct Location {
	StringRef name;
};

inline Location operator"" _loc(const char* str, size_t size) {
	return Location{ StringRef(reinterpret_cast<const uint8_t*>(str), size) };
}

struct Span {
	Span(SpanID context, Location location, std::initializer_list<SpanID> const& parents = {})
	  : context(context), begin(g_network->now()), location(location), parents(arena, parents.begin(), parents.end()) {
		if (parents.size() > 0) {
			// If the parents' token is 0 (meaning the trace should not be
			// recorded), set the child token to 0 as well. Otherwise, generate
			// a new, random token.
			uint64_t traceId = 0;
			if ((*parents.begin()).second() > 0) {
				traceId = deterministicRandom()->randomUInt64();
			}
			this->context = SpanID((*parents.begin()).first(), traceId);
		}
	}
	Span(Location location, std::initializer_list<SpanID> const& parents = {})
	  : Span(UID(deterministicRandom()->randomUInt64(),
	             deterministicRandom()->random01() < FLOW_KNOBS->TRACING_SAMPLE_RATE
	                 ? deterministicRandom()->randomUInt64()
	                 : 0),
	         location,
	         parents) {}
	Span(Location location, SpanID context) : Span(location, { context }) {}
	Span(const Span&) = delete;
	Span(Span&& o) {
		arena = std::move(o.arena);
		context = o.context;
		begin = o.begin;
		end = o.end;
		location = o.location;
		parents = std::move(o.parents);
		o.context = UID();
		o.begin = 0.0;
		o.end = 0.0;
	}
	Span() {}
	~Span();
	Span& operator=(Span&& o);
	Span& operator=(const Span&) = delete;
	void swap(Span& other) {
		std::swap(arena, other.arena);
		std::swap(context, other.context);
		std::swap(begin, other.begin);
		std::swap(end, other.end);
		std::swap(location, other.location);
		std::swap(parents, other.parents);
	}

	void addParent(SpanID span) {
		if (parents.size() == 0) {
			uint64_t traceId = 0;
			if (span.second() > 0) {
				traceId = context.second() == 0 ? deterministicRandom()->randomUInt64() : context.second();
			}
			// Use first parent to set trace ID. This is non-ideal for spans
			// with multiple parents, because the trace ID will associate the
			// span with only one trace. A workaround is to look at the parent
			// relationships instead of the trace ID. Another option in the
			// future is to keep a list of trace IDs.
			context = SpanID(span.first(), traceId);
		}
		parents.push_back(arena, span);
	}

	void addTag(const StringRef& key, const StringRef& value) { tags[key] = value; }

	Arena arena;
	UID context = UID();
	double begin = 0.0, end = 0.0;
	Location location;
	SmallVectorRef<SpanID> parents;
	std::unordered_map<StringRef, StringRef> tags;
};

// The user selects a tracer using a string passed to fdbserver on boot.
// Clients should not refer to TracerType directly, and mappings of names to
// values in this enum can change without notice.
enum class TracerType {
	DISABLED = 0,
	NETWORK_LOSSY = 1,
	SIM_END = 2, // Any tracers that come after SIM_END will not be tested in simulation
	LOG_FILE = 3
};

struct ITracer {
	virtual ~ITracer();
	virtual TracerType type() const = 0;
	// passed ownership to the tracer
	virtual void trace(Span const& span) = 0;
};

void openTracer(TracerType type);

template <class T>
struct SpannedDeque : Deque<T> {
	Span span;
	explicit SpannedDeque(Location loc) : span(loc) {}
	SpannedDeque(SpannedDeque&& other) : Deque<T>(std::move(other)), span(std::move(other.span)) {}
	SpannedDeque(SpannedDeque const&) = delete;
	SpannedDeque& operator=(SpannedDeque const&) = delete;
	SpannedDeque& operator=(SpannedDeque&& other) {
		*static_cast<Deque<T>*>(this) = std::move(other);
		span = std::move(other.span);
	}
};
