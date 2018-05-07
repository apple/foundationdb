/*
 * Stats.h
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

#ifndef FLOW_STATS_H
#define FLOW_STATS_H
#pragma once

// Yet another performance statistics interface
/*

struct MyCounters {
CounterCollection cc;
Counter foo, bar, baz;
MyCounters() : foo("foo", cc), bar("bar", cc), baz("baz", cc) {}
};

*/

#include "flow.h"
#include "TDMetric.actor.h"

struct ICounter {
	// All counters have a name and value
	virtual std::string const& getName() const = 0;
	virtual int64_t getValue() const = 0;

	// Counters may also have rate and roughness
	virtual bool hasRate() const = 0;
	virtual double getRate() const = 0;
	virtual bool hasRoughness() const = 0;
	virtual double getRoughness() const = 0;

	virtual void resetInterval() = 0;

	virtual void remove() {}
};

struct CounterCollection {
	CounterCollection(std::string name, std::string id = std::string()) : name(name), id(id) {}
	std::vector<struct ICounter*> counters, counters_to_remove;
	~CounterCollection() { for (auto c : counters_to_remove) c->remove(); }
	std::string name;
	std::string id;
};

struct Counter : ICounter {
public:
	typedef int64_t Value;

	Counter(std::string const& name, CounterCollection& collection);

	void operator += (Value delta);
	void operator ++ () { *this += 1; }
	void clear();
	void resetInterval();

	std::string const& getName() const { return name; }

	Value getIntervalDelta() const { return interval_delta; }
	Value getValue() const { return interval_start_value + interval_delta; }
	double getRate() const;				// dValue / dt
	double getRoughness() const;			// value deltas come in "clumps" of this many ( 1 = periodic, 2 = poisson, 10 = periodic clumps of 10 (nearly) simultaneous delta )
	bool hasRate() const { return true; }
	bool hasRoughness() const { return true; }

private:
	std::string name;
	double interval_start, last_event, interval_sq_time;
	Value interval_delta, interval_start_value;
	Int64MetricHandle metric;
};

template <class F>
struct SpecialCounter : ICounter, FastAllocated<SpecialCounter<F>> {
	SpecialCounter(CounterCollection& collection, std::string const& name, F && f) : name(name), f(f) { collection.counters.push_back(this); collection.counters_to_remove.push_back(this); }
	virtual void remove() { delete this; }

	virtual std::string const& getName() const { return name; }
	virtual int64_t getValue() const { return f(); }

	virtual void resetInterval() {}

	virtual bool hasRate() const { return false; }
	virtual double getRate() const { throw internal_error(); }
	virtual bool hasRoughness() const { return false; }
	virtual double getRoughness() const { throw internal_error(); }

	std::string name;
	F f;
};
template <class F>
static void specialCounter(CounterCollection& collection, std::string const& name, F && f) { new SpecialCounter<F>(collection, name, std::move(f)); }

Future<Void> traceCounters(std::string const& traceEventName, UID const& traceEventID, double const& interval, CounterCollection* const& counters, std::string const& trackLatestName = std::string());

#endif