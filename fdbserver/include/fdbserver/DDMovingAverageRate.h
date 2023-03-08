/*
 * DDMovingAverageRate.h
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

#ifndef FOUNDATIONDB_DDMOVINGAVERAGERATE_H
#define FOUNDATIONDB_DDMOVINGAVERAGERATE_H

#include <limits.h>
#include "flow/Deque.h"
#include "fdbserver/Knobs.h"

// Perfomed as the rolling window to calculate average moving bytes rate by DD.
template <class T>
struct MovingAverageRate {
private:
	T previous;
	T total;
	// To avoid having a super large Deque which may lead OOM, we set a maxSize for it.
	// Actually, Deque has its own Deque::max_size = 1 << 30, We may narrow it down here.
	// = 100MB / sizeof(pair<double, int64_t>) = 100MB / 16B ~ 2^16 ~ SHRT_MAX
	int maxSize;
    // True if the length of updates > maxSize
	bool isFull;
	Deque<std::pair<double, T>> updates; // pair{time, bytes}
    double interval;
    double startTS;         // timestamp of creation

    void pop() {
        previous += updates.front().second;
        updates.pop_front();
    }

public:
    MovingAverageRate()
	  : previous(0), total(0), 
      maxSize(SHRT_MAX), 
      isFull(false),
	    interval(SERVER_KNOBS->DD_TRACE_MOVE_BYTES_AVERAGE_INTERVAL),
        startTS(now()) {}

	T getTotal() const { return total; }

	double getAverage() {
		while (!updates.empty() && updates.front().first < now() - interval) {
			pop();
			isFull = false;
		}

        if (isFull) {
            ASSERT(updates.back().first != updates.front().first);
            return (total - previous) / (updates.back().first - updates.front().first);
        } else if (now() - startTS < interval) {        // DD has just initialized
            return (total - previous) / (now() - startTS);
        } else {
            return (total - previous) / interval;
        }
	}

	void addSample(T sample) {
		total += sample;
		updates.push_back(std::make_pair(now(), sample));
		// If so, we would pop the front element from the Deque.
		while (updates.size() > maxSize) {
            pop();
			isFull = true;
		}
	}

};

#endif // FOUNDATIONDB_DDMOVINGAVERAGERATE_H