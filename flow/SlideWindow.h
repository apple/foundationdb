/**
 * SlideWindow.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#ifndef FLOW_SLIDEWINDOW_H
#define FLOW_SLIDEWINDOW_H

#pragma once

#include <list>
#include <memory>
#include <unordered_map>

#include "flow/flow.h"

template <typename V, typename accumulate_type=V, class queue_type=std::list<std::pair<double, V>>>
class SlideWindow {
public:
	using value_t = V;
	using accumulate_t = accumulate_type;
	using queue_t = queue_type;

	const double SLIDE_WINDOW_LENGTH;
	SlideWindow(const double& slideWindowLength, const value_t initialValue = value_t()) : SLIDE_WINDOW_LENGTH(slideWindowLength), slideWindowValue(initialValue) {}

	void add(const value_t& v) {
		sweep();
		timestampedValue.push_back({now(), v});
		slideWindowValue += v;
	}

	const value_t& get() {
		sweep();
		return slideWindowValue;
	}

protected:
	void sweep() {
		const auto CRITERIA = now() - SLIDE_WINDOW_LENGTH;
		while (!timestampedValue.empty() && timestampedValue.front().first < CRITERIA) {
			slideWindowValue -= timestampedValue.front().second;
			timestampedValue.pop_front();
		}
	}

private:
	queue_t timestampedValue;

	accumulate_t slideWindowValue;
};

#endif  // FLOW_SLIDEWINDOW_H
