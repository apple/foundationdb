/*
 * TagThrottleApi.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/TagThrottle.actor.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.actor.h"
#include "fdbrpc/simulator.h"

struct TagThrottleApiWorkload : TestWorkload {
	bool autoThrottleEnabled;
	double testDuration;

	constexpr static auto NAME = "TagThrottleApi";

	TagThrottleApiWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		autoThrottleEnabled = SERVER_KNOBS->AUTO_TAG_THROTTLING_ENABLED;
	}

	Future<Void> setup(Database const& cx) override {
		DatabaseContext::debugUseTags = true;
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (SERVER_KNOBS->GLOBAL_TAG_THROTTLING || this->clientId != 0)
			return Void();
		return timeout(runThrottleApi(this, cx), testDuration, Void());
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	static Optional<TagThrottleType> randomTagThrottleType() {
		Optional<TagThrottleType> throttleType;
		switch (deterministicRandom()->randomInt(0, 3)) {
		case 0:
			throttleType = TagThrottleType::AUTO;
			break;
		case 1:
			throttleType = TagThrottleType::MANUAL;
			break;
		default:
			break;
		}

		return throttleType;
	}

	Future<Void> throttleTag(
	    Database cx,
	    std::map<std::pair<TransactionTag, TransactionPriority>, TagThrottleInfo>* manuallyThrottledTags) {
		TransactionTag tag =
		    TransactionTagRef(deterministicRandom()->randomChoice(DatabaseContext::debugTransactionTagChoices));
		TransactionPriority priority = deterministicRandom()->randomChoice(allTransactionPriorities);
		double rate = deterministicRandom()->random01() * 20;
		double duration = 1 + deterministicRandom()->random01() * 19;

		TagSet tagSet;
		tagSet.addTag(tag);

		try {
			co_await ThrottleApi::throttleTags(cx.getReference(),
			                                   tagSet,
			                                   rate,
			                                   duration,
			                                   TagThrottleType::MANUAL,
			                                   priority,
			                                   Optional<double>(),
			                                   TagThrottledReason::MANUAL);
		} catch (Error& e) {
			Error err = e;
			if (e.code() == error_code_too_many_tag_throttles) {
				ASSERT(manuallyThrottledTags->size() >= SERVER_KNOBS->MAX_MANUAL_THROTTLED_TRANSACTION_TAGS);
				co_return;
			}

			throw err;
		}

		manuallyThrottledTags->insert_or_assign(
		    std::make_pair(tag, priority),
		    TagThrottleInfo(
		        tag, TagThrottleType::MANUAL, priority, rate, now() + duration, duration, TagThrottledReason::MANUAL));
	}

	Future<Void> unthrottleTag(
	    Database cx,
	    std::map<std::pair<TransactionTag, TransactionPriority>, TagThrottleInfo>* manuallyThrottledTags) {
		TransactionTag tag =
		    TransactionTagRef(deterministicRandom()->randomChoice(DatabaseContext::debugTransactionTagChoices));
		TagSet tagSet;
		tagSet.addTag(tag);

		Optional<TagThrottleType> throttleType = TagThrottleApiWorkload::randomTagThrottleType();
		Optional<TransactionPriority> priority = deterministicRandom()->coinflip()
		                                             ? Optional<TransactionPriority>()
		                                             : deterministicRandom()->randomChoice(allTransactionPriorities);

		bool erased = false;
		double maxExpiration = 0;
		if (!throttleType.present() || throttleType.get() == TagThrottleType::MANUAL) {
			for (auto p : allTransactionPriorities) {
				if (!priority.present() || priority.get() == p) {
					auto itr = manuallyThrottledTags->find(std::make_pair(tag, p));
					if (itr != manuallyThrottledTags->end()) {
						maxExpiration = std::max(maxExpiration, itr->second.expirationTime);
						erased = true;
						manuallyThrottledTags->erase(itr);
					}
				}
			}
		}

		bool removed = co_await ThrottleApi::unthrottleTags(cx.getReference(), tagSet, throttleType, priority);
		if (removed) {
			ASSERT(erased || !throttleType.present() || throttleType.get() == TagThrottleType::AUTO);
		} else {
			ASSERT(maxExpiration < now());
		}
	}

	Future<Void> getTags(
	    TagThrottleApiWorkload* self,
	    Database cx,
	    std::map<std::pair<TransactionTag, TransactionPriority>, TagThrottleInfo> const* manuallyThrottledTags) {

		std::vector<TagThrottleInfo> tags =
		    co_await ThrottleApi::getThrottledTags(cx.getReference(), CLIENT_KNOBS->TOO_MANY);

		int manualThrottledTags = 0;
		int activeAutoThrottledTags = 0;
		for (auto& tag : tags) {
			if (tag.throttleType == TagThrottleType::AUTO) {
				ASSERT(self->autoThrottleEnabled);
			} else if (tag.throttleType == TagThrottleType::MANUAL) {
				ASSERT(manuallyThrottledTags->find(std::make_pair(tag.tag, tag.priority)) !=
				       manuallyThrottledTags->end());
				++manualThrottledTags;
			} else if (tag.expirationTime > now()) {
				++activeAutoThrottledTags;
			}
		}

		ASSERT(manualThrottledTags <= SERVER_KNOBS->MAX_MANUAL_THROTTLED_TRANSACTION_TAGS);
		ASSERT(activeAutoThrottledTags <= SERVER_KNOBS->MAX_AUTO_THROTTLED_TRANSACTION_TAGS);

		int minManualThrottledTags = 0;
		int maxManualThrottledTags = 0;
		for (auto& tag : *manuallyThrottledTags) {
			if (tag.second.expirationTime > now()) {
				++minManualThrottledTags;
			}
			++maxManualThrottledTags;
		}

		ASSERT(manualThrottledTags >= minManualThrottledTags && manualThrottledTags <= maxManualThrottledTags);
	}

	Future<Void> getRecommendedTags(TagThrottleApiWorkload* self, Database cx) {
		std::vector<TagThrottleInfo> tags =
		    co_await ThrottleApi::getRecommendedTags(cx.getReference(), CLIENT_KNOBS->TOO_MANY);

		for (auto& tag : tags) {
			ASSERT(tag.throttleType == TagThrottleType::AUTO);
		}
	}

	Future<Void> unthrottleTagGroup(
	    Database cx,
	    std::map<std::pair<TransactionTag, TransactionPriority>, TagThrottleInfo>* manuallyThrottledTags) {
		Optional<TagThrottleType> throttleType = TagThrottleApiWorkload::randomTagThrottleType();
		Optional<TransactionPriority> priority = deterministicRandom()->coinflip()
		                                             ? Optional<TransactionPriority>()
		                                             : deterministicRandom()->randomChoice(allTransactionPriorities);

		bool unthrottled = co_await ThrottleApi::unthrottleAll(cx.getReference(), throttleType, priority);
		if (!throttleType.present() || throttleType.get() == TagThrottleType::MANUAL) {
			bool unthrottleExpected = false;
			bool empty = manuallyThrottledTags->empty();
			for (auto itr = manuallyThrottledTags->begin(); itr != manuallyThrottledTags->end();) {
				if (!priority.present() || priority.get() == itr->first.second) {
					if (itr->second.expirationTime > now()) {
						unthrottleExpected = true;
					}

					itr = manuallyThrottledTags->erase(itr);
				} else {
					++itr;
				}
			}

			if (throttleType.present()) {
				ASSERT((unthrottled && !empty) || (!unthrottled && !unthrottleExpected));
			} else {
				ASSERT(unthrottled || !unthrottleExpected);
			}
		}
	}

	Future<Void> enableAutoThrottling(TagThrottleApiWorkload* self, Database cx) {
		Reference<DatabaseContext> db = cx.getReference();
		if (deterministicRandom()->coinflip()) {
			co_await ThrottleApi::enableAuto(db, true);
			self->autoThrottleEnabled = true;
			if (deterministicRandom()->coinflip()) {
				co_await ThrottleApi::unthrottleAll(db, TagThrottleType::AUTO, Optional<TransactionPriority>());
			}
		} else {
			co_await ThrottleApi::enableAuto(db, false);
			self->autoThrottleEnabled = false;
		}
	}

	Future<Void> runThrottleApi(TagThrottleApiWorkload* self, Database cx) {
		std::map<std::pair<TransactionTag, TransactionPriority>, TagThrottleInfo> manuallyThrottledTags;
		while (true) {
			double delayTime = deterministicRandom()->random01() * 5;
			co_await delay(delayTime);

			int action = deterministicRandom()->randomInt(0, 6);

			if (action == 0) {
				co_await self->throttleTag(cx, &manuallyThrottledTags);
			} else if (action == 1) {
				co_await self->unthrottleTag(cx, &manuallyThrottledTags);
			} else if (action == 2) {
				co_await self->getTags(self, cx, &manuallyThrottledTags);
			} else if (action == 3) {
				co_await self->unthrottleTagGroup(cx, &manuallyThrottledTags);
			} else if (action == 4) {
				co_await self->enableAutoThrottling(self, cx);
			} else if (action == 5) {
				co_await self->getRecommendedTags(self, cx);
			}
		}
	}
};

WorkloadFactory<TagThrottleApiWorkload> TagThrottleApiWorkloadFactory;
