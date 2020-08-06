/*
 * Tracing.h
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
	  : context(context), begin(g_network->now()), location(location), parents(arena, parents.begin(), parents.end()) {}
	Span(Location location, std::initializer_list<SpanID> const& parents = {})
	  : Span(deterministicRandom()->randomUniqueID(), location, parents) {}
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

	void addParent(SpanID span) { parents.push_back(arena, span); }

	Arena arena;
	UID context = UID();
	double begin = 0.0, end = 0.0;
	Location location;
	SmallVectorRef<SpanID> parents;
};

enum class TracerType { DISABLED, LOG_FILE };

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
	explicit SpannedDeque(Location loc) : span(deterministicRandom()->randomUniqueID(), loc) {}
	SpannedDeque(SpannedDeque&& other) : Deque<T>(std::move(other)), span(std::move(other.span)) {}
	SpannedDeque(SpannedDeque const&) = delete;
	SpannedDeque& operator=(SpannedDeque const&) = delete;
	SpannedDeque& operator=(SpannedDeque&& other) {
		*static_cast<Deque<T>*>(this) = std::move(other);
		span = std::move(other.span);
	}
};
