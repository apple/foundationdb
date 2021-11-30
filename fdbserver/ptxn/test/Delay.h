/*
 * Delay.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_PTXN_TEST_DELAY_H
#define FDBSERVER_PTXN_TEST_DELAY_H

#pragma once

#include "flow/flow.h"
#include "flow/IRandom.h"

namespace ptxn::test {

class Delay {
	bool m_enabled = false;

protected:
	virtual double getDelayTime() = 0;

public:
	bool enabled() const { return m_enabled; }
	void setEnable(const bool flag) { m_enabled = flag; }
	void enable() { setEnable(true); }
	void disable() { setEnable(false); }

	Future<Void> operator()() {
		if (enabled()) {
			return delay(getDelayTime());
		}
		return Void();
	}
};

class FixedTimeDelay : public Delay {
	double m_delayTime;

protected:
	virtual double getDelayTime() { return m_delayTime; }

public:
	FixedTimeDelay(const double delayTime_ = 1.0) : m_delayTime(delayTime_) {}

	double delayTime() const { return const_cast<FixedTimeDelay*>(this)->getDelayTime(); }
	void setDelayTime(const double delayTime) { m_delayTime = delayTime; }
};

class RandomDelay : public Delay {
	double m_delayLower;
	double m_delayUpper;

protected:
	virtual double getDelayTime() {
		return m_delayLower + (m_delayUpper - m_delayLower) * deterministicRandom()->random01();
	}

public:
	RandomDelay(const double delayLower_ = 0.0005, const double delayUpper_ = 0.001)
	  : m_delayLower(delayLower_), m_delayUpper(delayUpper_) {}

	double delayLower() const { return m_delayLower; }
	double delayUpper() const { return m_delayUpper; }
	void setDelayLower(const double lower) { m_delayLower = lower; }
	void setDelayUpper(const double upper) { m_delayUpper = upper; }
};

class ExponentalBackoffDelay : public Delay {
	double m_initialBackoff;
	bool m_useJitter = true;
	double m_jitterLower;
	double m_jitterUpper;
	double m_backoff;
	int m_numBackoffs = 0;

	double getJitterTime() const {
		static const int RANGE = 10000;
		return (m_jitterUpper - m_jitterLower) * deterministicRandom()->randomInt(0, RANGE) / RANGE + m_jitterLower;
	}

	virtual double getDelayTime() {
		double delayTime = m_backoff + (m_useJitter ? getJitterTime() : 0);
		m_backoff *= 2;
		++m_numBackoffs;
		return delayTime;
	}

public:
	ExponentalBackoffDelay(const double initialBackoff_ = 0.01,
	                       const bool useJitter_ = true,
	                       const double jitterLower_ = 0.0001,
	                       const double jitterUpper_ = 0.0002)
	  : m_initialBackoff(initialBackoff_), m_useJitter(useJitter_), m_jitterLower(jitterLower_),
	    m_jitterUpper(jitterUpper_), m_backoff(initialBackoff_), m_numBackoffs(0) {}

	double initialBackoff() const { return m_initialBackoff; }
	void setInitialBackoff(const double initialBackoff) { m_initialBackoff = initialBackoff; }

	bool useJitter() const { return m_useJitter; }
	void setUseJitter(const bool useJitter) { m_useJitter = useJitter; }

	double jitterLower() const { return m_jitterLower; }
	void setJitterLower(const double jitterLower) { m_jitterLower = jitterLower; }

	double jitterUpper() const { return m_jitterUpper; }
	void setJitterUpper(const double jitterUpper) { m_jitterUpper = jitterUpper; }

	int numBackoffs() const { return m_numBackoffs; }
	// Resets the backoffs to 0
	void resetBackoffs() {
		m_numBackoffs = 0;
		m_backoff = m_initialBackoff;
	}
};

} // namespace ptxn::test

#endif // FDBSERVER_PTXN_TEST_DELAY_H