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
 *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbserver/TagThrottler.h"
#include "fdbserver/RkTagThrottleCollection.h"

class TagThrottlerImpl {
	Database db;
	UID id;
	RkTagThrottleCollection throttledTags;
	uint64_t throttledTagChangeId{ 0 };
	bool autoThrottlingEnabled{ false };
	Future<Void> expiredTagThrottleCleanup;

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
								updatedTagThrottles.updateBusyTagCount(tagValue.reason);
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

	Future<Void> tryUpdateAutoThrottling(TransactionTag tag, double rate, double busyness, TagThrottledReason reason) {
		// NOTE: before the comparison with MIN_TAG_COST, the busiest tag rate also compares with MIN_TAG_PAGES_RATE
		// currently MIN_TAG_PAGES_RATE > MIN_TAG_COST in our default knobs.
		if (busyness > SERVER_KNOBS->AUTO_THROTTLE_TARGET_TAG_BUSYNESS && rate > SERVER_KNOBS->MIN_TAG_COST) {
			TEST(true); // Transaction tag auto-throttled
			Optional<double> clientRate = throttledTags.autoThrottleTag(id, tag, busyness);
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
	TagThrottlerImpl(Database db, UID id) : db(db), id(id) {
		expiredTagThrottleCleanup = recurring([this]() { ThrottleApi::expire(this->db.getReference()); },
		                                      SERVER_KNOBS->TAG_THROTTLE_EXPIRED_CLEANUP_INTERVAL);
	}
	Future<Void> monitorThrottlingChanges() { return monitorThrottlingChanges(this); }

	void addRequests(TransactionTag tag, int count) { throttledTags.addRequests(tag, count); }
	uint64_t getThrottledTagChangeId() const { return throttledTagChangeId; }
	PrioritizedTransactionTagMap<ClientTagThrottleLimits> getClientRates() {
		return throttledTags.getClientRates(autoThrottlingEnabled);
	}
	int64_t autoThrottleCount() const { return throttledTags.autoThrottleCount(); }
	uint32_t busyReadTagCount() const { return throttledTags.getBusyReadTagCount(); }
	uint32_t busyWriteTagCount() const { return throttledTags.getBusyWriteTagCount(); }
	int64_t manualThrottleCount() const { return throttledTags.manualThrottleCount(); }
	bool isAutoThrottlingEnabled() const { return autoThrottlingEnabled; }

	Future<Void> tryUpdateAutoThrottling(StorageQueueInfo const& ss) {
		// NOTE: we just keep it simple and don't differentiate write-saturation and read-saturation at the moment. In
		// most of situation, this works. More indicators besides queue size and durability lag could be investigated in
		// the future
		auto storageQueue = ss.getStorageQueueBytes();
		auto storageDurabilityLag = ss.getDurabilityLag();
		if (storageQueue > SERVER_KNOBS->AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES ||
		    storageDurabilityLag > SERVER_KNOBS->AUTO_TAG_THROTTLE_DURABILITY_LAG_VERSIONS) {
			// TODO: Update once size is potentially > 1
			ASSERT_WE_THINK(ss.busiestWriteTags.size() <= 1);
			ASSERT_WE_THINK(ss.busiestReadTags.size() <= 1);
			for (const auto& busyWriteTag : ss.busiestWriteTags) {
				return tryUpdateAutoThrottling(busyWriteTag.tag,
				                               busyWriteTag.rate,
				                               busyWriteTag.fractionalBusyness,
				                               TagThrottledReason::BUSY_WRITE);
			}
			for (const auto& busyReadTag : ss.busiestReadTags) {
				return tryUpdateAutoThrottling(
				    busyReadTag.tag, busyReadTag.rate, busyReadTag.fractionalBusyness, TagThrottledReason::BUSY_READ);
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
Future<Void> TagThrottler::tryUpdateAutoThrottling(StorageQueueInfo const& ss) {
	return impl->tryUpdateAutoThrottling(ss);
}
