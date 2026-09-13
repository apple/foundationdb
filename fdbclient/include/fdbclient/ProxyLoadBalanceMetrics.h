/*
 * ProxyLoadBalanceMetrics.h
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

#ifndef FDBCLIENT_PROXYLOADBALANCEMETRICS_H
#define FDBCLIENT_PROXYLOADBALANCEMETRICS_H
#pragma once

#include "flow/Knobs.h"

#include <cstddef>

// Packed proxy feedback uses the quotient for recent transaction rate and the remainder for CPU busyness.
// Preserve signed values and carries at the precision boundary.
class ProxyCpuMetric {
public:
	static int decode(int processBusyTime) {
		return processBusyTime % FLOW_KNOBS->BASIC_LOAD_BALANCE_COMPUTE_PRECISION;
	}

	static double minimumTotal(std::size_t alternativeCount) {
		return FLOW_KNOBS->BASIC_LOAD_BALANCE_COMPUTE_PRECISION * FLOW_KNOBS->BASIC_LOAD_BALANCE_MIN_CPU *
		       alternativeCount;
	}
};

class ProxyGrvMetric {
public:
	static int decode(int processBusyTime) {
		return processBusyTime / FLOW_KNOBS->BASIC_LOAD_BALANCE_COMPUTE_PRECISION;
	}

	static double minimumTotal(std::size_t alternativeCount) {
		return FLOW_KNOBS->BASIC_LOAD_BALANCE_MIN_REQUESTS * alternativeCount;
	}
};

#endif
