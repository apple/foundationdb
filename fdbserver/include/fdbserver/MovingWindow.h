/*
 * MovingWindow.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#ifndef FOUNDATIONDB_MOVINGWINDOW_H
#define FOUNDATIONDB_MOVINGWINDOW_H

#include <limits.h>
#include "flow/Deque.h"
#include "fdbserver/Knobs.h"

// Smoother will try to modify the sum of all samples and return an estimtaed value, while
// MovingWindow will just sum up all the samples, and getAverage() will return the average
// change rate in the past <timeWindow>.
// The motivation of MovingWindow comes from "MovingData" trace, where we want to get the
// average bytes moved by DD in the past DD_TRACE_MOVE_BYTES_AVERAGE_INTERVAL. This data could be
// precise and clear to show how DD works recently.
// As for Smoother, however, it doesn't have a method to calculate average change rate, and it will
// take all of the samples into account, while we don't really need it for "MovingData". Also, saying there
// is no DD workload recently, we could just return 0 with MovingWindow and Smoother will return
// a value closed to 0, where the former method is more obvious and clear to understand DD workloads.
template <class T>
class MovingWindow {
private:
	T previous;
	T total;
	// To avoid having a super large Deque which may lead OOM, we set a maxSize for it.
	// Actually, Deque has its own Deque::max_size = 1 << 30, We may narrow it down here.
	int maxDequeSize;
	Deque<std::pair<double, T>> updates; // pair{time, numeric}
	double interval;
	// Updated when initialization Or pop() due to full Deque
	double previousPopTime;

	void pop() {
		previous += updates.front().second;
		updates.pop_front();
	}

public:
	MovingWindow() = default;
	explicit MovingWindow(double timeWindow)
	  : previous(0), total(0), maxDequeSize(SERVER_KNOBS->MOVING_WINDOW_SAMPLE_SIZE / sizeof(std::pair<double, T>)),
	    interval(timeWindow), previousPopTime(now()) {}

	T getTotal() const { return total; }

	double getAverage() {
		if (now() - interval <= previousPopTime) { // struct is just initialized Or pop() due to full
			return (total - previous) / (now() - previousPopTime);
		} else {
			while (!updates.empty() && updates.front().first < now() - interval) {
				pop();
			}
			return (total - previous) / interval;
		}
	}

	void addSample(T sample) {
		total += sample;
		updates.push_back(std::make_pair(now(), sample));
		// If so, we would pop the front element from the Deque.
		while (updates.size() > maxDequeSize) {
			previousPopTime = updates.front().first;
			pop();
		}
	}
};

#endif // FOUNDATIONDB_MOVINGWINDOW_H