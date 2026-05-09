/*
 * TxnCounters.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#ifndef FLOW_TXNCOUNTERS_H
#define FLOW_TXNCOUNTERS_H
#pragma once

#include "flow/SimpleCounter.h"

struct TxnCounters {
	SimpleCounter<int64_t>* started;
	SimpleCounter<int64_t>* committed;
	SimpleCounter<int64_t>* aborted;
};

inline TxnCounters* makeCounters(const char* prefix) {
	std::string p(prefix);
	auto* c = new TxnCounters();
	c->started = SimpleCounter<int64_t>::makeCounter(p + "/started");
	c->committed = SimpleCounter<int64_t>::makeCounter(p + "/committed");
	c->aborted = SimpleCounter<int64_t>::makeCounter(p + "/aborted");
	return c;
}

#endif
