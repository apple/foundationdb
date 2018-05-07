/*
 * Notified.h
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

#ifndef FDBCLIENT_NOTIFIED_H
#define FDBCLIENT_NOTIFIED_H
#pragma once

#include "FDBTypes.h"
#include "flow/TDMetric.actor.h"

struct NotifiedVersion {
	NotifiedVersion( StringRef& name, StringRef const &id, Version version = 0 ) : val(name, id, version) { val = version; }
	NotifiedVersion( Version version = 0 ) : val(StringRef(), StringRef(), version) {}

	void initMetric(const StringRef& name, const StringRef &id) { 
		Version version = val;
		val.init(name, id); 
		val = version;
	}

	Future<Void> whenAtLeast( Version limit ) {
		if (val >= limit) 
			return Void();
		Promise<Void> p;
		waiting.push( std::make_pair(limit,p) );
		return p.getFuture();
	}

	Version get() const { return val; }

	void set( Version v ) {
		ASSERT( v >= val );
		if (v != val) {
			val = v;

			std::vector<Promise<Void>> toSend;
			while ( waiting.size() && v >= waiting.top().first ) {
				Promise<Void> p = std::move(waiting.top().second);
				waiting.pop();
				toSend.push_back(p);
			}
			for(auto& p : toSend) {
				p.send(Void());
			}
		}
	}

	void operator=( Version v ) {
		set( v );
	}

	NotifiedVersion(NotifiedVersion&& r) noexcept(true) : waiting(std::move(r.waiting)), val(std::move(r.val)) {}
	void operator=(NotifiedVersion&& r) noexcept(true) { waiting = std::move(r.waiting); val = std::move(r.val); }

private:
	typedef std::pair<Version,Promise<Void>> Item;
	struct ItemCompare {
		bool operator()(const Item& a, const Item& b) { return a.first > b.first; }
	};
	std::priority_queue<Item, std::vector<Item>, ItemCompare> waiting;
	VersionMetricHandle val;
};

#endif
