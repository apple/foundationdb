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

class TagThrottlerImpl {
public:
	ACTOR static Future<Void> monitorThrottlingChanges(TagThrottler* self) {
		state bool committed = false;
		loop {
			state ReadYourWritesTransaction tr(self->ratekeeper->db);

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
							TraceEvent("AutoTagThrottlingDisabled", self->ratekeeper->id).log();
						}
						self->autoThrottlingEnabled = false;
					} else if (autoThrottlingEnabled.get().present() &&
					           autoThrottlingEnabled.get().get() == LiteralStringRef("1")) {
						TEST(true); // Auto-throttling enabled
						if (!self->autoThrottlingEnabled) {
							TraceEvent("AutoTagThrottlingEnabled", self->ratekeeper->id).log();
						}
						self->autoThrottlingEnabled = true;
					} else {
						TEST(true); // Auto-throttling unspecified
						if (autoThrottlingEnabled.get().present()) {
							TraceEvent(SevWarnAlways, "InvalidAutoTagThrottlingValue", self->ratekeeper->id)
							    .detail("Value", autoThrottlingEnabled.get().get());
						}
						self->autoThrottlingEnabled = SERVER_KNOBS->AUTO_TAG_THROTTLING_ENABLED;
						if (!committed)
							tr.set(tagThrottleAutoEnabledKey,
							       LiteralStringRef(self->autoThrottlingEnabled ? "1" : "0"));
					}

					RkTagThrottleCollection updatedTagThrottles;

					TraceEvent("RatekeeperReadThrottledTags", self->ratekeeper->id)
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
								    self->ratekeeper->id, tag, 0, tagValue.tpsRate, tagValue.expirationTime);
								if (tagValue.reason == TagThrottledReason::BUSY_READ) {
									updatedTagThrottles.busyReadTagCount++;
								} else if (tagValue.reason == TagThrottledReason::BUSY_WRITE) {
									updatedTagThrottles.busyWriteTagCount++;
								}
							} else {
								updatedTagThrottles.manualThrottleTag(self->ratekeeper->id,
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
					TraceEvent("RatekeeperThrottleSignaled", self->ratekeeper->id).log();
					TEST(true); // Tag throttle changes detected
					break;
				} catch (Error& e) {
					TraceEvent("RatekeeperMonitorThrottlingChangesError", self->ratekeeper->id).error(e);
					wait(tr.onError(e));
				}
			}
		}
	}

}; // class TagThrottlerImpl

Future<Void> TagThrottler::monitorThrottlingChanges() {
	return TagThrottlerImpl::monitorThrottlingChanges(this);
}
