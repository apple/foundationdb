/*
 * FBTrace.h
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

struct SpanImpl {
	explicit SpanImpl(UID contex, Location location,
	                  std::unordered_set<UID> const& parents = std::unordered_set<UID>());
	SpanImpl(const SpanImpl&) = delete;
	SpanImpl(SpanImpl&&) = delete;
	SpanImpl& operator=(const SpanImpl&) = delete;
	SpanImpl& operator=(SpanImpl&&) = delete;
	void addref();
	void delref();

	~SpanImpl();
	UID context;
	double begin, end;
	Location location;
	std::unordered_set<UID> parents;

private:
	std::atomic<unsigned> refCount = 1;
};

class Span {
	Reference<SpanImpl> impl;

public:
	Span(UID context, Location location, std::unordered_set<UID> const& parents = std::unordered_set<UID>())
	  : impl(new SpanImpl(context, location, parents)) {}
	Span(Location location, std::unordered_set<UID> const& parents = std::unordered_set<UID>())
	  : impl(new SpanImpl(deterministicRandom()->randomUniqueID(), location, parents)) {}
	Span(Location location, Span const& parent)
	  : impl(new SpanImpl(deterministicRandom()->randomUniqueID(), location, { parent->context })) {}
	Span(Location location, std::initializer_list<Span> const& parents)
	  : impl(new SpanImpl(deterministicRandom()->randomUniqueID(), location)) {
		for (const auto& parent : parents) {
			impl->parents.insert(parent->context);
		}
	}
	Span(const Span&) = default;
	Span(Span&&) = default;
	Span() {}
	Span& operator=(Span&&) = default;
	Span& operator=(const Span&) = default;
	SpanImpl* operator->() const { return impl.getPtr(); }
	SpanImpl& operator*() const { return *impl; }
	void reset() { impl.clear(); }
};

enum class TracerType { DISABLED, LOG_FILE };

struct ITracer {
	virtual ~ITracer();
	virtual TracerType type() const = 0;
	// passed ownership to the tracer
	virtual void trace(SpanImpl* span) = 0;
};

void openTracer(TracerType type);

template <class T>
struct SpannedDeque : Deque<T> {
	Span span;
	explicit SpannedDeque(Location loc) : span(deterministicRandom()->randomUniqueID(), loc) {}
	explicit SpannedDeque(Span span) : span(span) {}
	SpannedDeque(SpannedDeque&& other) : Deque<T>(std::move(other)), span(std::move(other.span)) {}
	SpannedDeque(SpannedDeque const& other) : Deque<T>(other), span(other.span) {}
	SpannedDeque& operator=(SpannedDeque const& other) {
		*static_cast<Deque<T>*>(this) = other;
		span = other.span;
		return *this;
	}
	SpannedDeque& operator=(SpannedDeque&& other) {
		*static_cast<Deque<T>*>(this) = std::move(other);
		span = std::move(other.span);
	}
	Span resetSpan() {
		auto res = span;
		span = Span(span->location);
		return res;
	}
};
