/*
 * Stats.actor.cpp
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

#include "Stats.h"

Counter::Counter(std::string const& name, CounterCollection& collection)
: name(name), interval_start(0), last_event(0), interval_sq_time(0), interval_start_value(0), interval_delta(0)
{
	metric.init(collection.name + "." + (char)toupper(name.at(0)) + name.substr(1), collection.id);
	collection.counters.push_back(this);
}

void Counter::operator += (Value delta) {
	if (!delta) return;  //< Otherwise last_event will be reset
	interval_delta += delta;
	auto t = now();
	auto elapsed = t - last_event;
	interval_sq_time += elapsed * elapsed;
	last_event = t;

	metric += delta;
}

double Counter::getRate() const {
	double elapsed = now() - interval_start;
	return elapsed > 0 ? interval_delta / elapsed : 0;
}

double Counter::getRoughness() const {
	double elapsed = now() - interval_start;
	if(elapsed == 0) {
		return 0;
	}

	double delay = interval_sq_time / elapsed;
	return delay * getRate() * 2;
}

void Counter::resetInterval() {
	interval_start_value += interval_delta;
	interval_delta = 0;
	interval_sq_time = 0;
	interval_start = now();
	last_event = interval_start;  // <FIXME: Is this right?
}

void Counter::clear() {
	resetInterval();
	interval_start_value = 0;

	metric = 0;
}

ACTOR Future<Void> traceCounters(std::string traceEventName, UID traceEventID, double interval, CounterCollection* counters, std::string trackLatestName) {
	Void _ = wait(delay(0)); // Give an opportunity for all members used in special counters to be initialized

	for (ICounter* c : counters->counters)
		c->resetInterval();

	state double last_interval = now();

	loop{
		TraceEvent te(traceEventName.c_str(), traceEventID);
		te.detail("Elapsed", now() - last_interval);
		for (ICounter* c : counters->counters) {
			if (c->hasRate() && c->hasRoughness())
				te.detailf(c->getName().c_str(), "%g %g %lld", c->getRate(), c->getRoughness(), (long long)c->getValue());
			else
				te.detail(c->getName().c_str(), c->getValue());
			c->resetInterval();
		}
		if (!trackLatestName.empty())
			te.trackLatest(trackLatestName.c_str());

		last_interval = now();
		Void _ = wait(delay(interval));
	}
}