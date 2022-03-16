/*
 * Smoother.h
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

#include "flow/flow.h"
#include <cmath>

template <class T>
class SmootherImpl {
	// Times (t) are expected to be nondecreasing

	double eFoldingTime;
	double total;
	mutable double time, estimate;

	void update(double t) const {
		double elapsed = t - time;
		if (elapsed) {
			time = t;
			estimate += (total - estimate) * (1 - exp(-elapsed / eFoldingTime));
		}
	}

protected:
	explicit SmootherImpl(double eFoldingTime) : eFoldingTime(eFoldingTime) { reset(0); }

public:
	void reset(double value) {
		time = 0;
		total = value;
		estimate = value;
	}
	void setTotal(double total, double t = T::now()) { addDelta(total - this->total, t); }
	void addDelta(double delta, double t = T::now()) {
		update(t);
		total += delta;
	}
	// smoothTotal() is a continuous (under)estimate of the sum of all addDeltas()
	double smoothTotal(double t = T::now()) const {
		update(t);
		return estimate;
	}
	// smoothRate() is d/dt[smoothTotal], and is NOT continuous
	double smoothRate(double t = T::now()) const {
		update(t);
		return (total - estimate) / eFoldingTime;
	}

	double getTotal() const { return total; }
};

class Smoother : public SmootherImpl<Smoother> {
public:
	static double now() { return ::now(); }
	explicit Smoother(double eFoldingTime) : SmootherImpl<Smoother>(eFoldingTime) {}
};
class TimerSmoother : public SmootherImpl<TimerSmoother> {
public:
	static double now() { return timer(); }
	explicit TimerSmoother(double eFoldingTime) : SmootherImpl<TimerSmoother>(eFoldingTime) {}
};
