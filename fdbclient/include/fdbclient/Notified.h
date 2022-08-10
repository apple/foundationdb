/*
 * Notified.h
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

#ifndef FDBCLIENT_NOTIFIED_H
#define FDBCLIENT_NOTIFIED_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "flow/TDMetric.actor.h"

template <class T>
struct IsMetricHandle : std::false_type {};
template <class T>
struct IsMetricHandle<MetricHandle<T>> : std::true_type {};

template <class T, class ValueType = T>
struct Notified {
	explicit Notified(ValueType v = 0) { val = v; }

	[[nodiscard]] Future<Void> whenAtLeast(const ValueType& limit) {
		if (val >= limit)
			return Void();
		Promise<Void> p;
		waiting.emplace(limit, p);
		return p.getFuture();
	}

	[[nodiscard]] ValueType get() const { return val; }

	void initMetric(const StringRef& name, const StringRef& id) {
		if constexpr (IsMetricHandle<T>::value) {
			ValueType v = val;
			val.init(name, id);
			val = v;
		} else {
			TraceEvent(SevError, "InvalidNotifiedOperation")
			    .detail("Reason", "Notified<T> where T is not a metric: Can't use initMetric");
		}
	}

	void set(const ValueType& v) {
		ASSERT(v >= val);
		if (v != val) {
			val = v;

			std::vector<Promise<Void>> toSend;
			while (waiting.size() && v >= waiting.top().first) {
				Promise<Void> p = std::move(waiting.top().second);
				waiting.pop();
				toSend.push_back(p);
			}
			for (auto& p : toSend) {
				p.send(Void());
			}
		}
	}

	void operator=(const ValueType& v) { set(v); }

	Notified(Notified&& r) noexcept : waiting(std::move(r.waiting)), val(std::move(r.val)) {}
	void operator=(Notified&& r) noexcept {
		waiting = std::move(r.waiting);
		val = std::move(r.val);
	}

	int numWaiting() { return waiting.size(); }

private:
	using Item = std::pair<ValueType, Promise<Void>>;
	struct ItemCompare {
		bool operator()(const Item& a, const Item& b) { return a.first > b.first; }
	};
	std::priority_queue<Item, std::vector<Item>, ItemCompare> waiting;
	T val;
};

using NotifiedVersion = Notified<VersionMetricHandle, VersionMetricHandle::ValueType>;
using NotifiedDouble = Notified<double>;

#endif
