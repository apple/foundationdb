/*
 * TagThrottler.h
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

#include "fdbserver/TagThrottler.h"

class RkTagThrottleCollection : NonCopyable {
	struct RkTagData {
		Smoother requestRate;
		RkTagData() : requestRate(CLIENT_KNOBS->TAG_THROTTLE_SMOOTHING_WINDOW) {}
	};

	struct RkTagThrottleData {
		ClientTagThrottleLimits limits;
		Smoother clientRate;

		// Only used by auto-throttles
		double created = now();
		double lastUpdated = 0;
		double lastReduced = now();
		bool rateSet = false;

		RkTagThrottleData() : clientRate(CLIENT_KNOBS->TAG_THROTTLE_SMOOTHING_WINDOW) {}

		double getTargetRate(Optional<double> requestRate) {
			if (limits.tpsRate == 0.0 || !requestRate.present() || requestRate.get() == 0.0 || !rateSet) {
				return limits.tpsRate;
			} else {
				return std::min(limits.tpsRate, (limits.tpsRate / requestRate.get()) * clientRate.smoothTotal());
			}
		}

		Optional<double> updateAndGetClientRate(Optional<double> requestRate) {
			if (limits.expiration > now()) {
				double targetRate = getTargetRate(requestRate);
				if (targetRate == std::numeric_limits<double>::max()) {
					rateSet = false;
					return targetRate;
				}
				if (!rateSet) {
					rateSet = true;
					clientRate.reset(targetRate);
				} else {
					clientRate.setTotal(targetRate);
				}

				double rate = clientRate.smoothTotal();
				ASSERT(rate >= 0);
				return rate;
			} else {
				TEST(true); // Get throttle rate for expired throttle
				rateSet = false;
				return Optional<double>();
			}
		}
	};

	void initializeTag(TransactionTag const& tag) { tagData.try_emplace(tag); }

public:
	RkTagThrottleCollection() {}

	RkTagThrottleCollection(RkTagThrottleCollection&& other) {
		autoThrottledTags = std::move(other.autoThrottledTags);
		manualThrottledTags = std::move(other.manualThrottledTags);
		tagData = std::move(other.tagData);
	}

	void operator=(RkTagThrottleCollection&& other) {
		autoThrottledTags = std::move(other.autoThrottledTags);
		manualThrottledTags = std::move(other.manualThrottledTags);
		tagData = std::move(other.tagData);
	}

	double computeTargetTpsRate(double currentBusyness, double targetBusyness, double requestRate) {
		ASSERT(currentBusyness > 0);

		if (targetBusyness < 1) {
			double targetFraction = targetBusyness * (1 - currentBusyness) / ((1 - targetBusyness) * currentBusyness);
			return requestRate * targetFraction;
		} else {
			return std::numeric_limits<double>::max();
		}
	}

	// Returns the TPS rate if the throttle is updated, otherwise returns an empty optional
	Optional<double> autoThrottleTag(UID id,
	                                 TransactionTag const& tag,
	                                 double fractionalBusyness,
	                                 Optional<double> tpsRate = Optional<double>(),
	                                 Optional<double> expiration = Optional<double>()) {
		ASSERT(!tpsRate.present() || tpsRate.get() >= 0);
		ASSERT(!expiration.present() || expiration.get() > now());

		auto itr = autoThrottledTags.find(tag);
		bool present = (itr != autoThrottledTags.end());
		if (!present) {
			if (autoThrottledTags.size() >= SERVER_KNOBS->MAX_AUTO_THROTTLED_TRANSACTION_TAGS) {
				TEST(true); // Reached auto-throttle limit
				return Optional<double>();
			}

			itr = autoThrottledTags.try_emplace(tag).first;
			initializeTag(tag);
		} else if (itr->second.limits.expiration <= now()) {
			TEST(true); // Re-throttling expired tag that hasn't been cleaned up
			present = false;
			itr->second = RkTagThrottleData();
		}

		auto& throttle = itr->second;

		if (!tpsRate.present()) {
			if (now() <= throttle.created + SERVER_KNOBS->AUTO_TAG_THROTTLE_START_AGGREGATION_TIME) {
				tpsRate = std::numeric_limits<double>::max();
				if (present) {
					return Optional<double>();
				}
			} else if (now() <= throttle.lastUpdated + SERVER_KNOBS->AUTO_TAG_THROTTLE_UPDATE_FREQUENCY) {
				TEST(true); // Tag auto-throttled too quickly
				return Optional<double>();
			} else {
				tpsRate = computeTargetTpsRate(fractionalBusyness,
				                               SERVER_KNOBS->AUTO_THROTTLE_TARGET_TAG_BUSYNESS,
				                               tagData[tag].requestRate.smoothRate());

				if (throttle.limits.expiration > now() && tpsRate.get() >= throttle.limits.tpsRate) {
					TEST(true); // Tag auto-throttle rate increase attempt while active
					return Optional<double>();
				}

				throttle.lastUpdated = now();
				if (tpsRate.get() < throttle.limits.tpsRate) {
					throttle.lastReduced = now();
				}
			}
		}
		if (!expiration.present()) {
			expiration = now() + SERVER_KNOBS->AUTO_TAG_THROTTLE_DURATION;
		}

		ASSERT(tpsRate.present() && tpsRate.get() >= 0);

		throttle.limits.tpsRate = tpsRate.get();
		throttle.limits.expiration = expiration.get();

		Optional<double> clientRate = throttle.updateAndGetClientRate(getRequestRate(tag));

		TraceEvent("RkSetAutoThrottle", id)
		    .detail("Tag", tag)
		    .detail("TargetRate", tpsRate.get())
		    .detail("Expiration", expiration.get() - now())
		    .detail("ClientRate", clientRate)
		    .detail("Created", now() - throttle.created)
		    .detail("LastUpdate", now() - throttle.lastUpdated)
		    .detail("LastReduced", now() - throttle.lastReduced);

		if (tpsRate.get() != std::numeric_limits<double>::max()) {
			return tpsRate.get();
		} else {
			return Optional<double>();
		}
	}

	void manualThrottleTag(UID id,
	                       TransactionTag const& tag,
	                       TransactionPriority priority,
	                       double tpsRate,
	                       double expiration,
	                       Optional<ClientTagThrottleLimits> const& oldLimits) {
		ASSERT(tpsRate >= 0);
		ASSERT(expiration > now());

		auto& priorityThrottleMap = manualThrottledTags[tag];
		auto result = priorityThrottleMap.try_emplace(priority);
		initializeTag(tag);
		ASSERT(result.second); // Updating to the map is done by copying the whole map

		result.first->second.limits.tpsRate = tpsRate;
		result.first->second.limits.expiration = expiration;

		if (!oldLimits.present()) {
			TEST(true); // Transaction tag manually throttled
			TraceEvent("RatekeeperAddingManualThrottle", id)
			    .detail("Tag", tag)
			    .detail("Rate", tpsRate)
			    .detail("Priority", transactionPriorityToString(priority))
			    .detail("SecondsToExpiration", expiration - now());
		} else if (oldLimits.get().tpsRate != tpsRate || oldLimits.get().expiration != expiration) {
			TEST(true); // Manual transaction tag throttle updated
			TraceEvent("RatekeeperUpdatingManualThrottle", id)
			    .detail("Tag", tag)
			    .detail("Rate", tpsRate)
			    .detail("Priority", transactionPriorityToString(priority))
			    .detail("SecondsToExpiration", expiration - now());
		}

		Optional<double> clientRate = result.first->second.updateAndGetClientRate(getRequestRate(tag));
		ASSERT(clientRate.present());
	}

	Optional<ClientTagThrottleLimits> getManualTagThrottleLimits(TransactionTag const& tag,
	                                                             TransactionPriority priority) {
		auto itr = manualThrottledTags.find(tag);
		if (itr != manualThrottledTags.end()) {
			auto priorityItr = itr->second.find(priority);
			if (priorityItr != itr->second.end()) {
				return priorityItr->second.limits;
			}
		}

		return Optional<ClientTagThrottleLimits>();
	}

	PrioritizedTransactionTagMap<ClientTagThrottleLimits> getClientRates(bool autoThrottlingEnabled) {
		PrioritizedTransactionTagMap<ClientTagThrottleLimits> clientRates;

		for (auto tagItr = tagData.begin(); tagItr != tagData.end();) {
			bool tagPresent = false;

			double requestRate = tagItr->second.requestRate.smoothRate();
			auto manualItr = manualThrottledTags.find(tagItr->first);
			if (manualItr != manualThrottledTags.end()) {
				Optional<ClientTagThrottleLimits> manualClientRate;
				for (auto priority = allTransactionPriorities.rbegin(); !(priority == allTransactionPriorities.rend());
				     ++priority) {
					auto priorityItr = manualItr->second.find(*priority);
					if (priorityItr != manualItr->second.end()) {
						Optional<double> priorityClientRate = priorityItr->second.updateAndGetClientRate(requestRate);
						if (!priorityClientRate.present()) {
							TEST(true); // Manual priority throttle expired
							priorityItr = manualItr->second.erase(priorityItr);
						} else {
							if (!manualClientRate.present() ||
							    manualClientRate.get().tpsRate > priorityClientRate.get()) {
								manualClientRate = ClientTagThrottleLimits(priorityClientRate.get(),
								                                           priorityItr->second.limits.expiration);
							} else {
								TEST(true); // Manual throttle overriden by higher priority
							}

							++priorityItr;
						}
					}

					if (manualClientRate.present()) {
						tagPresent = true;
						TEST(true); // Using manual throttle
						clientRates[*priority][tagItr->first] = manualClientRate.get();
					}
				}

				if (manualItr->second.empty()) {
					TEST(true); // All manual throttles expired
					manualThrottledTags.erase(manualItr);
					break;
				}
			}

			auto autoItr = autoThrottledTags.find(tagItr->first);
			if (autoItr != autoThrottledTags.end()) {
				Optional<double> autoClientRate = autoItr->second.updateAndGetClientRate(requestRate);
				if (autoClientRate.present()) {
					double adjustedRate = autoClientRate.get();
					double rampStartTime = autoItr->second.lastReduced + SERVER_KNOBS->AUTO_TAG_THROTTLE_DURATION -
					                       SERVER_KNOBS->AUTO_TAG_THROTTLE_RAMP_UP_TIME;
					if (now() >= rampStartTime && adjustedRate != std::numeric_limits<double>::max()) {
						TEST(true); // Tag auto-throttle ramping up

						double targetBusyness = SERVER_KNOBS->AUTO_THROTTLE_TARGET_TAG_BUSYNESS;
						if (targetBusyness == 0) {
							targetBusyness = 0.01;
						}

						double rampLocation = (now() - rampStartTime) / SERVER_KNOBS->AUTO_TAG_THROTTLE_RAMP_UP_TIME;
						adjustedRate =
						    computeTargetTpsRate(targetBusyness, pow(targetBusyness, 1 - rampLocation), adjustedRate);
					}

					tagPresent = true;
					if (autoThrottlingEnabled) {
						auto result = clientRates[TransactionPriority::DEFAULT].try_emplace(
						    tagItr->first, adjustedRate, autoItr->second.limits.expiration);
						if (!result.second && result.first->second.tpsRate > adjustedRate) {
							result.first->second =
							    ClientTagThrottleLimits(adjustedRate, autoItr->second.limits.expiration);
						} else {
							TEST(true); // Auto throttle overriden by manual throttle
						}
						clientRates[TransactionPriority::BATCH][tagItr->first] =
						    ClientTagThrottleLimits(0, autoItr->second.limits.expiration);
					}
				} else {
					ASSERT(autoItr->second.limits.expiration <= now());
					TEST(true); // Auto throttle expired
					if (BUGGIFY) { // Temporarily extend the window between expiration and cleanup
						tagPresent = true;
					} else {
						autoThrottledTags.erase(autoItr);
					}
				}
			}

			if (!tagPresent) {
				TEST(true); // All tag throttles expired
				tagItr = tagData.erase(tagItr);
			} else {
				++tagItr;
			}
		}

		return clientRates;
	}

	void addRequests(TransactionTag const& tag, int requests) {
		if (requests > 0) {
			TEST(true); // Requests reported for throttled tag

			auto tagItr = tagData.try_emplace(tag);
			tagItr.first->second.requestRate.addDelta(requests);

			double requestRate = tagItr.first->second.requestRate.smoothRate();

			auto autoItr = autoThrottledTags.find(tag);
			if (autoItr != autoThrottledTags.end()) {
				autoItr->second.updateAndGetClientRate(requestRate);
			}

			auto manualItr = manualThrottledTags.find(tag);
			if (manualItr != manualThrottledTags.end()) {
				for (auto priorityItr = manualItr->second.begin(); priorityItr != manualItr->second.end();
				     ++priorityItr) {
					priorityItr->second.updateAndGetClientRate(requestRate);
				}
			}
		}
	}

	Optional<double> getRequestRate(TransactionTag const& tag) {
		auto itr = tagData.find(tag);
		if (itr != tagData.end()) {
			return itr->second.requestRate.smoothRate();
		}
		return Optional<double>();
	}

	int64_t autoThrottleCount() const { return autoThrottledTags.size(); }

	int64_t manualThrottleCount() const {
		int64_t count = 0;
		for (auto itr = manualThrottledTags.begin(); itr != manualThrottledTags.end(); ++itr) {
			count += itr->second.size();
		}

		return count;
	}

	TransactionTagMap<RkTagThrottleData> autoThrottledTags;
	TransactionTagMap<std::map<TransactionPriority, RkTagThrottleData>> manualThrottledTags;
	TransactionTagMap<RkTagData> tagData;
	uint32_t busyReadTagCount = 0, busyWriteTagCount = 0;
};

class TagThrottlerImpl {
	Database db;
	UID id;
	RkTagThrottleCollection throttledTags;
	uint64_t throttledTagChangeId{ 0 };
	bool autoThrottlingEnabled{ false };

	ACTOR static Future<Void> monitorThrottlingChanges(TagThrottlerImpl* self) {
		state bool committed = false;
		loop {
			state ReadYourWritesTransaction tr(self->db);

			loop {
				try {
					tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

					state Future<RangeResult> throttledTagKeys = tr.getRange(tagThrottleKeys, CLIENT_KNOBS->TOO_MANY);
					state Future<Optional<Value>> autoThrottlingEnabled = tr.get(tagThrottleAutoEnabledKey);

					if (!committed) {
						BinaryWriter limitWriter(Unversioned());
						limitWriter << SERVER_KNOBS->MAX_MANUAL_THROTTLED_TRANSACTION_TAGS;
						tr.set(tagThrottleLimitKey, limitWriter.toValue());
					}

					wait(success(throttledTagKeys) && success(autoThrottlingEnabled));

					if (autoThrottlingEnabled.get().present() &&
					    autoThrottlingEnabled.get().get() == LiteralStringRef("0")) {
						TEST(true); // Auto-throttling disabled
						if (self->autoThrottlingEnabled) {
							TraceEvent("AutoTagThrottlingDisabled", self->id).log();
						}
						self->autoThrottlingEnabled = false;
					} else if (autoThrottlingEnabled.get().present() &&
					           autoThrottlingEnabled.get().get() == LiteralStringRef("1")) {
						TEST(true); // Auto-throttling enabled
						if (!self->autoThrottlingEnabled) {
							TraceEvent("AutoTagThrottlingEnabled", self->id).log();
						}
						self->autoThrottlingEnabled = true;
					} else {
						TEST(true); // Auto-throttling unspecified
						if (autoThrottlingEnabled.get().present()) {
							TraceEvent(SevWarnAlways, "InvalidAutoTagThrottlingValue", self->id)
							    .detail("Value", autoThrottlingEnabled.get().get());
						}
						self->autoThrottlingEnabled = SERVER_KNOBS->AUTO_TAG_THROTTLING_ENABLED;
						if (!committed)
							tr.set(tagThrottleAutoEnabledKey,
							       LiteralStringRef(self->autoThrottlingEnabled ? "1" : "0"));
					}

					RkTagThrottleCollection updatedTagThrottles;

					TraceEvent("RatekeeperReadThrottledTags", self->id)
					    .detail("NumThrottledTags", throttledTagKeys.get().size());
					for (auto entry : throttledTagKeys.get()) {
						TagThrottleKey tagKey = TagThrottleKey::fromKey(entry.key);
						TagThrottleValue tagValue = TagThrottleValue::fromValue(entry.value);

						ASSERT(tagKey.tags.size() == 1); // Currently, only 1 tag per throttle is supported

						if (tagValue.expirationTime == 0 ||
						    tagValue.expirationTime > now() + tagValue.initialDuration) {
							TEST(true); // Converting tag throttle duration to absolute time
							tagValue.expirationTime = now() + tagValue.initialDuration;
							BinaryWriter wr(IncludeVersion(ProtocolVersion::withTagThrottleValueReason()));
							wr << tagValue;
							state Value value = wr.toValue();

							tr.set(entry.key, value);
						}

						if (tagValue.expirationTime > now()) {
							TransactionTag tag = *tagKey.tags.begin();
							Optional<ClientTagThrottleLimits> oldLimits =
							    self->throttledTags.getManualTagThrottleLimits(tag, tagKey.priority);

							if (tagKey.throttleType == TagThrottleType::AUTO) {
								updatedTagThrottles.autoThrottleTag(
								    self->id, tag, 0, tagValue.tpsRate, tagValue.expirationTime);
								if (tagValue.reason == TagThrottledReason::BUSY_READ) {
									updatedTagThrottles.busyReadTagCount++;
								} else if (tagValue.reason == TagThrottledReason::BUSY_WRITE) {
									updatedTagThrottles.busyWriteTagCount++;
								}
							} else {
								updatedTagThrottles.manualThrottleTag(self->id,
								                                      tag,
								                                      tagKey.priority,
								                                      tagValue.tpsRate,
								                                      tagValue.expirationTime,
								                                      oldLimits);
							}
						}
					}

					self->throttledTags = std::move(updatedTagThrottles);
					++self->throttledTagChangeId;

					state Future<Void> watchFuture = tr.watch(tagThrottleSignalKey);
					wait(tr.commit());
					committed = true;

					wait(watchFuture);
					TraceEvent("RatekeeperThrottleSignaled", self->id).log();
					TEST(true); // Tag throttle changes detected
					break;
				} catch (Error& e) {
					TraceEvent("RatekeeperMonitorThrottlingChangesError", self->id).error(e);
					wait(tr.onError(e));
				}
			}
		}
	}

	Optional<double> autoThrottleTag(UID id, TransactionTag tag, double busyness) {
		return throttledTags.autoThrottleTag(id, tag, busyness);
	}

	Future<Void> tryAutoThrottleTag(TransactionTag tag, double rate, double busyness, TagThrottledReason reason) {
		// NOTE: before the comparison with MIN_TAG_COST, the busiest tag rate also compares with MIN_TAG_PAGES_RATE
		// currently MIN_TAG_PAGES_RATE > MIN_TAG_COST in our default knobs.
		if (busyness > SERVER_KNOBS->AUTO_THROTTLE_TARGET_TAG_BUSYNESS && rate > SERVER_KNOBS->MIN_TAG_COST) {
			TEST(true); // Transaction tag auto-throttled
			Optional<double> clientRate = autoThrottleTag(id, tag, busyness);
			if (clientRate.present()) {
				TagSet tags;
				tags.addTag(tag);

				Reference<DatabaseContext> dbRef = Reference<DatabaseContext>::addRef(db.getPtr());
				return ThrottleApi::throttleTags(dbRef,
				                                 tags,
				                                 clientRate.get(),
				                                 SERVER_KNOBS->AUTO_TAG_THROTTLE_DURATION,
				                                 TagThrottleType::AUTO,
				                                 TransactionPriority::DEFAULT,
				                                 now() + SERVER_KNOBS->AUTO_TAG_THROTTLE_DURATION,
				                                 reason);
			}
		}
		return Void();
	}

public:
	TagThrottlerImpl(Database db, UID id) : db(db), id(id) {}
	Future<Void> monitorThrottlingChanges() { return monitorThrottlingChanges(this); }

	void addRequests(TransactionTag tag, int count) { throttledTags.addRequests(tag, count); }
	uint64_t getThrottledTagChangeId() const { return throttledTagChangeId; }
	PrioritizedTransactionTagMap<ClientTagThrottleLimits> getClientRates() {
		return throttledTags.getClientRates(autoThrottlingEnabled);
	}
	int64_t autoThrottleCount() const { return throttledTags.autoThrottleCount(); }
	uint32_t busyReadTagCount() const { return throttledTags.busyReadTagCount; }
	uint32_t busyWriteTagCount() const { return throttledTags.busyWriteTagCount; }
	int64_t manualThrottleCount() const { return throttledTags.manualThrottleCount(); }
	bool isAutoThrottlingEnabled() const { return autoThrottlingEnabled; }

	Future<Void> tryAutoThrottleTag(StorageQueueInfo& ss, int64_t storageQueue, int64_t storageDurabilityLag) {
		// NOTE: we just keep it simple and don't differentiate write-saturation and read-saturation at the moment. In
		// most of situation, this works. More indicators besides queue size and durability lag could be investigated in
		// the future
		if (storageQueue > SERVER_KNOBS->AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES ||
		    storageDurabilityLag > SERVER_KNOBS->AUTO_TAG_THROTTLE_DURABILITY_LAG_VERSIONS) {
			if (ss.busiestWriteTag.present()) {
				return tryAutoThrottleTag(ss.busiestWriteTag.get(),
				                          ss.busiestWriteTagRate,
				                          ss.busiestWriteTagFractionalBusyness,
				                          TagThrottledReason::BUSY_WRITE);
			}
			if (ss.busiestReadTag.present()) {
				return tryAutoThrottleTag(ss.busiestReadTag.get(),
				                          ss.busiestReadTagRate,
				                          ss.busiestReadTagFractionalBusyness,
				                          TagThrottledReason::BUSY_READ);
			}
		}
		return Void();
	}

}; // class TagThrottlerImpl

TagThrottler::TagThrottler(Database db, UID id) : impl(PImpl<TagThrottlerImpl>::create(db, id)) {}
TagThrottler::~TagThrottler() = default;
Future<Void> TagThrottler::monitorThrottlingChanges() {
	return impl->monitorThrottlingChanges();
}
void TagThrottler::addRequests(TransactionTag tag, int count) {
	impl->addRequests(tag, count);
}
uint64_t TagThrottler::getThrottledTagChangeId() const {
	return impl->getThrottledTagChangeId();
}
PrioritizedTransactionTagMap<ClientTagThrottleLimits> TagThrottler::getClientRates() {
	return impl->getClientRates();
}
int64_t TagThrottler::autoThrottleCount() const {
	return impl->autoThrottleCount();
}
uint32_t TagThrottler::busyReadTagCount() const {
	return impl->busyReadTagCount();
}
uint32_t TagThrottler::busyWriteTagCount() const {
	return impl->busyWriteTagCount();
}
int64_t TagThrottler::manualThrottleCount() const {
	return impl->manualThrottleCount();
}
bool TagThrottler::isAutoThrottlingEnabled() const {
	return impl->isAutoThrottlingEnabled();
}
Future<Void> TagThrottler::tryAutoThrottleTag(StorageQueueInfo& ss,
                                              int64_t storageQueue,
                                              int64_t storageDurabilityLag) {
	return impl->tryAutoThrottleTag(ss, storageQueue, storageDurabilityLag);
}
