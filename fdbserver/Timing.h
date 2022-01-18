/*
 * Timing.h
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

#ifndef FDBSERVER_TIMING_H
#define FDBSERVER_TIMING_H

#pragma once

#include <vector>

#include "flow/flow.h"

class Timing {
	double m_start;

public:
	Timing() : m_start(now()) {}

	// Gets the time the Timing object created
	double getStart() const { return m_start; }

	// Gets the duration of the timing object
	double duration() const { return now() - m_start; }
};

class Stopwatch {
	std::vector<double> m_laps;

public:
	Stopwatch() : m_laps{ now() } {}

	// Laps the watch, stores the current time and return the duration between this lap and the previous one
	double lap() {
		m_laps.push_back(now());
		return *(m_laps.rbegin()) - *std::next(m_laps.rbegin());
	}
};

#endif // FDBSERVER_TIMING_H