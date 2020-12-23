/*
 * IRateControl.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "flow/flow.h"

class IRateControl {
public:
	virtual ~IRateControl() {}
	// Future is Ready once you can use n units;
	virtual Future<Void> getAllowance(unsigned int n) = 0;
	// If all of the allowance is not used the unused units can be given back.
	// For convenience, n can safely be negative.
	virtual void returnUnused(int n) = 0;
	virtual void addref() = 0;
	virtual void delref() = 0;
};

// An IRateControl implemenation that allows at most hands out at most windowLimit units of 'credit' in windowSeconds seconds
class SpeedLimit final : public IRateControl, ReferenceCounted<SpeedLimit> {
public:
	SpeedLimit(int windowLimit, int windowSeconds) : m_limit(windowLimit), m_seconds(windowSeconds), m_last_update(0), m_budget(0) {
		m_budget_max = m_limit * m_seconds;
		m_last_update = timer();
	}
	~SpeedLimit() = default;

	void addref() override { ReferenceCounted<SpeedLimit>::addref(); }
	void delref() override { ReferenceCounted<SpeedLimit>::delref(); }

	Future<Void> getAllowance(unsigned int n) override {
		// Replenish budget based on time since last update
		double ts = timer();
		// returnUnused happens to do exactly what we want here
		returnUnused((ts - m_last_update) / m_seconds * m_limit);
		m_last_update = ts;
		m_budget -= n;
		// If budget is still >= 0 then it's safe to use the allowance right now.
		if(m_budget >= 0)
			return Void();
		// Otherise return the amount of time it will take for the budget to rise to 0.
		return delay(m_seconds * -m_budget / m_limit);
	}

	void returnUnused(int n) override {
		if(n < 0)
			return;
		m_budget = std::min<int64_t>(m_budget + n, m_budget_max);
	}

private:
	int m_limit;
	double m_seconds;
	double m_last_update;
	int64_t m_budget;
	int64_t m_budget_max;
};

// An IRateControl implemenation that enforces no limit
class Unlimited final : public IRateControl, ReferenceCounted<Unlimited> {
public:
	Unlimited() {}
	~Unlimited() = default;
	void addref() override { ReferenceCounted<Unlimited>::addref(); }
	void delref() override { ReferenceCounted<Unlimited>::delref(); }

	Future<Void> getAllowance(unsigned int n) override { return Void(); }
	void returnUnused(int n) override {}
};
