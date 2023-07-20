/*
 * LatencyBandsMap.actor.cpp
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

#include "fdbserver/LatencyBandsMap.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

class LatencyBandsMapImpl {
public:
	ACTOR static Future<Void> expireOldTagsActor(LatencyBandsMap* self) {
		loop {
			wait(delay(5.0));
			for (auto it = self->map.begin(); it != self->map.end();) {
				const auto& [tag, expirableBands] = *it;
				if (now() - expirableBands.lastUpdated > SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TAG_EXPIRE_AFTER) {
					CODE_PROBE(true, "LatencyBandsMap erasing expired tag");
					it = self->map.erase(it);
				} else {
					++it;
				}
			}
		}
	}
};

LatencyBandsMap::ExpirableBands::ExpirableBands(LatencyBands&& bands)
  : latencyBands(std::move(bands)), lastUpdated(now()) {}

Optional<LatencyBands*> LatencyBandsMap::getLatencyBands(TransactionTag tag) {
	if (map.size() == maxSize && !map.count(tag)) {
		CODE_PROBE(true, "LatencyBandsMap reached maxSize");
		return {};
	}
	auto const [it, inserted] = map.try_emplace(
	    tag, LatencyBands(name, id, loggingInterval, [tag](auto& te) { te.detail("Tag", printable(tag)); }));
	auto& expirableBands = it->second;
	if (inserted) {
		for (const auto& threshold : thresholds) {
			expirableBands.latencyBands.addThreshold(threshold);
		}
	}
	expirableBands.lastUpdated = now();
	return &expirableBands.latencyBands;
}

LatencyBandsMap::LatencyBandsMap(std::string const& name, UID id, double loggingInterval, int maxSize)
  : name(name), id(id), loggingInterval(loggingInterval), maxSize(maxSize) {
	expireOldTags = LatencyBandsMapImpl::expireOldTagsActor(this);
}

void LatencyBandsMap::addMeasurement(TransactionTag tag, double value, int count) {
	auto bands = getLatencyBands(tag);
	if (bands.present()) {
		bands.get()->addMeasurement(value, count);
	}
}

void LatencyBandsMap::addThreshold(double value) {
	thresholds.push_back(value);
	for (auto& [tag, expirableBands] : map) {
		expirableBands.latencyBands.addThreshold(value);
	}
}

TEST_CASE("/fdbserver/LatencyBandsMap/Simple") {
	state LatencyBandsMap latencyBandsMap("TestLatencyBandsMap", deterministicRandom()->randomUniqueID(), 10.0, 100);
	state Standalone<VectorRef<TransactionTagRef>> tags;
	tags.push_back_deep(tags.arena(), "a"_sr);
	tags.push_back_deep(tags.arena(), "b"_sr);
	tags.push_back_deep(tags.arena(), "c"_sr);
	latencyBandsMap.addThreshold(0.1);
	latencyBandsMap.addThreshold(0.2);
	latencyBandsMap.addThreshold(0.4);
	state int measurements = 0;
	loop {
		wait(delayJittered(0.1));
		auto const tag = deterministicRandom()->randomChoice(tags);
		latencyBandsMap.addMeasurement(tag, deterministicRandom()->random01());
		if (++measurements == 1000) {
			return Void();
		}
	}
}

TEST_CASE("/fdbserver/LatencyBandsMap/MaxSize") {
	LatencyBandsMap latencyBandsMap("TestLatencyBandsMap", deterministicRandom()->randomUniqueID(), 10.0, 2);
	latencyBandsMap.addMeasurement("a"_sr, deterministicRandom()->random01());
	latencyBandsMap.addMeasurement("b"_sr, deterministicRandom()->random01());
	latencyBandsMap.addMeasurement("c"_sr, deterministicRandom()->random01());
	ASSERT_EQ(latencyBandsMap.size(), 2);
	return Void();
}

TEST_CASE("/fdbserver/LatencyBandsMap/Expire") {
	state LatencyBandsMap latencyBandsMap("TestLatencyBandsMap", deterministicRandom()->randomUniqueID(), 10.0, 100);
	latencyBandsMap.addMeasurement("a"_sr, deterministicRandom()->random01());
	latencyBandsMap.addMeasurement("b"_sr, deterministicRandom()->random01());
	latencyBandsMap.addMeasurement("c"_sr, deterministicRandom()->random01());
	latencyBandsMap.addThreshold(0.1);
	latencyBandsMap.addThreshold(0.2);
	latencyBandsMap.addThreshold(0.4);
	ASSERT_EQ(latencyBandsMap.size(), 3);
	state int waitIterations = 0;
	loop {
		wait(delay(1.0));
		latencyBandsMap.addMeasurement("a"_sr, deterministicRandom()->random01());
		latencyBandsMap.addMeasurement("b"_sr, deterministicRandom()->random01());
		if (++waitIterations == 2 * SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TAG_EXPIRE_AFTER) {
			break;
		}
	}
	ASSERT_EQ(latencyBandsMap.size(), 2);
	return Void();
}
