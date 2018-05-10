/*
 * Smoother.h
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

#ifndef FLOW_SMOOTHER_H
#define FLOW_SMOOTHER_H
#pragma once

#include "flow/flow.h"
#include <cmath>

struct Smoother {
	// Times (t) are expected to be nondecreasing

	explicit Smoother( double eFoldingTime ) : eFoldingTime(eFoldingTime) { reset(0); }
	void reset(double value) { time = 0; total = value; estimate = value; }

	void setTotal( double total, double t = now() ) { addDelta( total - this->total, t); }
	void addDelta( double delta, double t = now() ) {
		update(t);
		total += delta;
	}
	// smoothTotal() is a continuous (under)estimate of the sum of all addDeltas()
	double smoothTotal( double t = now() ) {
		update(t);
		return estimate;
	}
	// smoothRate() is d/dt[smoothTotal], and is NOT continuous
	double smoothRate( double t = now() ) {
		update(t);
		return (total-estimate) / eFoldingTime;
	}

	void update(double t) {
		double elapsed = t - time;
		if(elapsed) {
			time = t;
			estimate += (total-estimate) * (1-exp( -elapsed/eFoldingTime ));
		}
	}

	double eFoldingTime;
	double time, total, estimate;
};

struct TimerSmoother {
	// Times (t) are expected to be nondecreasing

	explicit TimerSmoother( double eFoldingTime ) : eFoldingTime(eFoldingTime) { reset(0); }
	void reset(double value) { time = 0; total = value; estimate = value; }

	void setTotal( double total, double t = timer() ) { addDelta( total - this->total, t); }
	void addDelta( double delta, double t = timer() ) {
		update(t);
		total += delta;
	}
	// smoothTotal() is a continuous (under)estimate of the sum of all addDeltas()
	double smoothTotal( double t = timer() ) {
		update(t);
		return estimate;
	}
	// smoothRate() is d/dt[smoothTotal], and is NOT continuous
	double smoothRate( double t = timer() ) {
		update(t);
		return (total-estimate) / eFoldingTime;
	}

	void update(double t) {
		double elapsed = t - time;
		time = t;
		estimate += (total-estimate) * (1-exp( -elapsed/eFoldingTime ));
	}

	double eFoldingTime;
	double time, total, estimate;
};

#endif
