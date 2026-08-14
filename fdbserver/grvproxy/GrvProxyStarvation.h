/*
 * GrvProxyStarvation.h
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

#ifndef FDBSERVER_GRVPROXY_GRVPROXYSTARVATION_H
#define FDBSERVER_GRVPROXY_GRVPROXYSTARVATION_H
#pragma once

#include "flow/FastRef.h"
#include "flow/flow.h"

#include <cstdint>

// Tracks whether a lower-priority GRV canary has made progress between checks made at ReadSocket priority.
// A whole-event-loop stall delays both sides and therefore does not accumulate consecutive misses after it clears.
class GrvProxyStarvationDetector : public ReferenceCounted<GrvProxyStarvationDetector> {
public:
	explicit GrvProxyStarvationDetector(int maxConsecutiveMisses)
	  : maxConsecutiveMisses(maxConsecutiveMisses), progressGeneration(0), lastObservedGeneration(0),
	    consecutiveMisses(0) {
		ASSERT_GT(maxConsecutiveMisses, 1);
	}

	void recordProgress() { ++progressGeneration; }

	bool checkForStarvation() {
		if (progressGeneration != lastObservedGeneration) {
			lastObservedGeneration = progressGeneration;
			consecutiveMisses = 0;
			return false;
		}

		++consecutiveMisses;
		return consecutiveMisses >= maxConsecutiveMisses;
	}

	int getConsecutiveMisses() const { return consecutiveMisses; }

private:
	int maxConsecutiveMisses;
	uint64_t progressGeneration;
	uint64_t lastObservedGeneration;
	int consecutiveMisses;
};

#endif // FDBSERVER_GRVPROXY_GRVPROXYSTARVATION_H
