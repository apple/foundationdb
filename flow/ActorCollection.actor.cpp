/*
 * ActorCollection.actor.cpp
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

#include "flow/ActorCollection.h"
#include "flow/IndexedSet.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

ACTOR Future<Void> actorCollection( FutureStream<Future<Void>> addActor, int* pCount, double *lastChangeTime, double *idleTime, double *allTime, bool returnWhenEmptied )
{
	state int64_t nextTag = 0;
	state Map<int64_t, Future<Void>> tag_streamHelper;
	state PromiseStream<int64_t> complete;
	state PromiseStream<Error> errors;
	state int count = 0;
	if (!pCount) pCount = &count;

	loop choose {
		when (Future<Void> f = waitNext(addActor)) {
			int64_t t = nextTag++;
			tag_streamHelper[t] = streamHelper( complete, errors, tag(f, t) );
			++*pCount;
			if( *pCount == 1 && lastChangeTime && idleTime && allTime) {
				double currentTime = now();
				*idleTime += currentTime - *lastChangeTime;
				*allTime += currentTime - *lastChangeTime;
				*lastChangeTime = currentTime;
			}
		}
		when (int64_t t = waitNext(complete.getFuture())) {
			if (!--*pCount) {
				if( lastChangeTime && idleTime && allTime) {
					double currentTime = now();
					*allTime += currentTime - *lastChangeTime;
					*lastChangeTime = currentTime;
				}
				if (returnWhenEmptied)
					return Void();
			}
			tag_streamHelper.erase(t);
		}
		when (Error e = waitNext(errors.getFuture())) { throw e; }
	}
}

template<>
struct Traceable<std::string> : std::true_type {
	template<class Str>
	static std::string toString(Str&& s) {
		return std::forward<Str>(s);
	}
};

template<class T, class U>
struct Traceable<std::pair<T, U>> {
	static constexpr bool value = Traceable<T>::value && Traceable<U>::value;
	static std::string toString(const std::pair<T, U>& p) {
		auto tStr = Traceable<T>::toString(p.first);
		auto uStr = Traceable<U>::toString(p.second);
		std::string result(tStr.size() + uStr.size() + 3, 'x');
		std::copy(tStr.begin(), tStr.end(), result.begin());
		auto iter = result.begin() + tStr.size();
		*(iter++) = ' ';
		*(iter++) = '-';
		*(iter++) = ' ';
		std::copy(uStr.begin(), uStr.end(), iter);
		return result;
	}
};


TEST_CASE("/flow/TraceEvent") {
	state unsigned i;
	state double startTime;
	state std::vector<std::string> strings;
	strings.reserve(10000);
	for (i = 0; i < 100; ++i) {
		for (int j = 0; j < 100; ++j) {
			strings.emplace_back(g_random->randomAlphaNumeric(g_random->randomInt(1, 30)));
		}
		wait(delay(0));
	}
	TraceEvent("pairsfilled")
		.detail("MemoryUsage", getMemoryUsage());
	startTime = g_network->now();
	for (i = 0; i < 100000; ++i) {
		for (unsigned j = 0; j < 100; ++j) {
			StringRef key(strings[g_random->randomInt(0, strings.size())]);
			auto p = std::make_pair(key, g_random->randomInt(-1000, 1000));
			TraceEvent("TestTraceLineNoDebug")
				.detail("Num", g_random->randomInt(0, 1000))
				.detail("Double", g_random->random01())
				.detail("str", strings[g_random->randomInt(0, strings.size())])
				.detail("pair", p);
		}
		wait(delay(0));
	}
	TraceEvent("TraceDuration")
		.detail("Time", g_network->now() - startTime);
	startTime = g_network->now();
	for (i = 0; i < 1000000; ++i) {
		for (unsigned j = 0; j < 100; ++j) {
			StringRef key(strings[g_random->randomInt(0, strings.size())]);
			auto p = std::make_pair(key, g_random->randomInt(-1000, 1000));
			TraceEvent(SevDebug, "TestTraceLineDebug")
				.detail("Num", g_random->randomInt(0, 1000))
				.detail("Double", g_random->random01())
				.detail("str", strings[g_random->randomInt(0, strings.size())])
				.detail("pair", p);
		}
		wait(delay(0));
	}
	TraceEvent("TraceDuration")
		.detail("Time", g_network->now() - startTime);
	return Void();
}
