/*
 * FBTrace.cpp
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

namespace {

struct NoopTracer : ITracer {
	TracerType type() const { return TracerType::DISABLED; }
	void trace(SpanImpl* span) override { delete span; }
};

struct LogfileTracer : ITracer {
	TracerType type() const { return TracerType::LOG_FILE; }
	void trace(SpanImpl* span) override {
		TraceEvent te(SevInfo, "TracingSpan", span->context);
		te.detail("Location", span->location.name).detail("Begin", span->begin).detail("End", span->end);
		if (span->parents.size() == 1) {
			te.detail("Parent", *span->parents.begin());
		} else {
			for (auto parent : span->parents) {
				TraceEvent(SevInfo, "TracingSpanAddParent", span->context).detail("AddParent", parent);
			}
		}
	}
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
	}
}

ITracer::~ITracer() {}

SpanImpl::SpanImpl(UID context, Location location, std::unordered_set<UID> const& parents)
  : context(context), location(location), parents(parents) {
	begin = g_network->now();
}

SpanImpl::~SpanImpl() {}

void SpanImpl::addref() {
	++refCount;
}

void SpanImpl::delref() {
	if (--refCount == 0) {
		end = g_network->now();
		g_tracer->trace(this);
	}
}
