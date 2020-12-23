/*
 * TagThrottle.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/TagThrottle.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/DatabaseContext.h"

#include "flow/actorcompiler.h" // has to be last include

void TagSet::addTag(TransactionTagRef tag) {
	ASSERT(CLIENT_KNOBS->MAX_TRANSACTION_TAG_LENGTH < 256); // Tag length is encoded with a single byte
	ASSERT(CLIENT_KNOBS->MAX_TAGS_PER_TRANSACTION < 256); // Number of tags is encoded with a single byte

	if(tag.size() > CLIENT_KNOBS->MAX_TRANSACTION_TAG_LENGTH) {
		throw tag_too_long();
	}
	if(tags.size() >= CLIENT_KNOBS->MAX_TAGS_PER_TRANSACTION) {
		throw too_many_tags();
	}

	TransactionTagRef tagRef(arena, tag);
	auto it = find(tags.begin(), tags.end(), tagRef);
	if (it == tags.end()) {
		tags.push_back(std::move(tagRef));
		bytes += tag.size();
	}
}

size_t TagSet::size() const {
	return tags.size();
}

// Format for throttle key:
//
// tagThrottleKeysPrefix + [auto-throttled (1-byte 0/1)] + [priority (1-byte)] + [tag list]
// tag list consists of 1 or more consecutive tags, each encoded as:
// tag.size() (1 byte) + tag's bytes. For example, tag 'foo' is: \x03foo
// The tags are listed in sorted order
//
// Currently, the throttle API supports only 1 tag per throttle
Key TagThrottleKey::toKey() const {
	ASSERT(CLIENT_KNOBS->MAX_TRANSACTION_TAG_LENGTH < 256);
	ASSERT(tags.size() > 0);

	ASSERT(tags.size() == 1); // SOMEDAY: support multiple tags per throttle

	int size = tagThrottleKeysPrefix.size() + tags.size() + 2;
	for(auto tag : tags) {
		ASSERT(tag.size() <= CLIENT_KNOBS->MAX_TRANSACTION_TAG_LENGTH);
		size += tag.size();
	}

	Key result;

	uint8_t* str = new (result.arena()) uint8_t[size];
	result.contents() = StringRef(str, size);

	memcpy(str, tagThrottleKeysPrefix.begin(), tagThrottleKeysPrefix.size());
	str += tagThrottleKeysPrefix.size();

	*(str++) = (uint8_t)throttleType;
	*(str++) = (uint8_t)priority;

	for(auto tag : tags) {
		*(str++) = (uint8_t)tag.size();
		if(tag.size() > 0) {
			memcpy(str, tag.begin(), tag.size());
			str += tag.size();
		}
	}

	return result;
}

TagThrottleKey TagThrottleKey::fromKey(const KeyRef& key) {
	const uint8_t *str = key.substr(tagThrottleKeysPrefix.size()).begin();
	TagThrottleType throttleType = TagThrottleType(*(str++));
	TransactionPriority priority = TransactionPriority(*(str++));
	TagSet tags;

	while(str < key.end()) {
		uint8_t size = *(str++);
		tags.addTag(TransactionTagRef(str, size));
		str += size;
	}

	return TagThrottleKey(tags, throttleType, priority);
}

TagThrottleValue TagThrottleValue::fromValue(const ValueRef& value) {
	TagThrottleValue throttleValue;
	BinaryReader reader(value, IncludeVersion(ProtocolVersion::withTagThrottleValueReason()));
	reader >> throttleValue;
	return throttleValue;
}

namespace ThrottleApi {
	ACTOR Future<bool> getValidAutoEnabled(Transaction* tr, Database db) {
		state bool result;
		loop {
			Optional<Value> value = wait(tr->get(tagThrottleAutoEnabledKey));
			if(!value.present()) {
				tr->reset();
				wait(delay(CLIENT_KNOBS->DEFAULT_BACKOFF));
				continue;
			}
			else if(value.get() == LiteralStringRef("1")) {
				result = true;
			}
			else if(value.get() == LiteralStringRef("0")) {
				result = false;
			}
			else {
				TraceEvent(SevWarnAlways, "InvalidAutoTagThrottlingValue", db->dbId).detail("Value", value.get());
				tr->reset();
				wait(delay(CLIENT_KNOBS->DEFAULT_BACKOFF));
				continue;
			}
			return result;
		};
	}

	void signalThrottleChange(Transaction &tr) {
		tr.atomicOp(tagThrottleSignalKey, LiteralStringRef("XXXXXXXXXX\x00\x00\x00\x00"), MutationRef::SetVersionstampedValue);
	}

	ACTOR Future<Void> updateThrottleCount(Transaction *tr, int64_t delta) {
		state Future<Optional<Value>> countVal = tr->get(tagThrottleCountKey);
		state Future<Optional<Value>> limitVal = tr->get(tagThrottleLimitKey);

		wait(success(countVal) && success(limitVal));

		int64_t count = 0;
		int64_t limit = 0;

		if(countVal.get().present()) {
			BinaryReader reader(countVal.get().get(), Unversioned());
			reader >> count;
		}

		if(limitVal.get().present()) {
			BinaryReader reader(limitVal.get().get(), Unversioned());
			reader >> limit;
		}

		count += delta;

		if(count > limit) {
			throw too_many_tag_throttles();
		}

		BinaryWriter writer(Unversioned());
		writer << count;

		tr->set(tagThrottleCountKey, writer.toValue());
		return Void();
	}

	ACTOR Future<std::vector<TagThrottleInfo>> getThrottledTags(Database db, int limit, bool containsRecommend) {
		state Transaction tr(db);
		state bool reportAuto = containsRecommend;
		loop {
			try {
				if (!containsRecommend) {
					wait(store(reportAuto, getValidAutoEnabled(&tr, db)));
				}
				Standalone<RangeResultRef> throttles = wait(tr.getRange(
					reportAuto ? tagThrottleKeys : KeyRangeRef(tagThrottleKeysPrefix, tagThrottleAutoKeysPrefix), limit));
				std::vector<TagThrottleInfo> results;
				for(auto throttle : throttles) {
					results.push_back(TagThrottleInfo(TagThrottleKey::fromKey(throttle.key), TagThrottleValue::fromValue(throttle.value)));
				}
				return results;
			}
			catch(Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<std::vector<TagThrottleInfo>> getRecommendedTags(Database db, int limit) {
		state Transaction tr(db);
		loop {
			try {
				bool enableAuto = wait(getValidAutoEnabled(&tr, db));
				if(enableAuto) {
					return std::vector<TagThrottleInfo>();
				}

				Standalone<RangeResultRef> throttles = wait(tr.getRange(KeyRangeRef(tagThrottleAutoKeysPrefix, tagThrottleKeys.end), limit));
				std::vector<TagThrottleInfo> results;
				for(auto throttle : throttles) {
					results.push_back(TagThrottleInfo(TagThrottleKey::fromKey(throttle.key), TagThrottleValue::fromValue(throttle.value)));
				}
				return results;
			}
			catch(Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> throttleTags(Database db, TagSet tags, double tpsRate, double initialDuration,
                                    TagThrottleType throttleType, TransactionPriority priority, Optional<double> expirationTime,
                                    Optional<TagThrottledReason> reason) {
		state Transaction tr(db);
		state Key key = TagThrottleKey(tags, throttleType, priority).toKey();

		ASSERT(initialDuration > 0);

		if(throttleType == TagThrottleType::MANUAL) {
			reason = TagThrottledReason::MANUAL;
		}
		TagThrottleValue throttle(tpsRate, expirationTime.present() ? expirationTime.get() : 0, initialDuration,
	                              reason.present() ? reason.get() : TagThrottledReason::UNSET);
		BinaryWriter wr(IncludeVersion(ProtocolVersion::withTagThrottleValueReason()));
		wr << throttle;
		state Value value = wr.toValue();

		loop {
			try {
				if(throttleType == TagThrottleType::MANUAL) {
					Optional<Value> oldThrottle = wait(tr.get(key));
					if(!oldThrottle.present()) {
						wait(updateThrottleCount(&tr, 1));
					}
				}

				tr.set(key, value);

				if(throttleType == TagThrottleType::MANUAL) {
					signalThrottleChange(tr);
				}

				wait(tr.commit());
				return Void();
			}
			catch(Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<bool> unthrottleTags(Database db, TagSet tags, Optional<TagThrottleType> throttleType, Optional<TransactionPriority> priority) {
		state Transaction tr(db);

		state std::vector<Key> keys;
		for(auto p : allTransactionPriorities) {
			if(!priority.present() || priority.get() == p) {
				if(!throttleType.present() || throttleType.get() == TagThrottleType::AUTO) {
					keys.push_back(TagThrottleKey(tags, TagThrottleType::AUTO, p).toKey());
				}
				if(!throttleType.present() || throttleType.get() == TagThrottleType::MANUAL) {
					keys.push_back(TagThrottleKey(tags, TagThrottleType::MANUAL, p).toKey());
				}
			}
		}

		state bool removed = false;

		loop {
			try {
				state std::vector<Future<Optional<Value>>> values;
				for(auto key : keys) {
					values.push_back(tr.get(key));
				}

				wait(waitForAll(values));

				int delta = 0;
				for(int i = 0; i < values.size(); ++i) {
					if(values[i].get().present()) {
						if(TagThrottleKey::fromKey(keys[i]).throttleType == TagThrottleType::MANUAL) {
							delta -= 1;
						}

						tr.clear(keys[i]);

						// Report that we are removing this tag if we ever see it present.
						// This protects us from getting confused if the transaction is maybe committed.
						// It's ok if someone else actually ends up removing this tag at the same time
						// and we aren't the ones to actually do it.
						removed = true;
					}
				}

				if(delta != 0) {
					wait(updateThrottleCount(&tr, delta));
				}
				if(removed) {
					signalThrottleChange(tr);
					wait(tr.commit());
				}

				return removed;
			}
			catch(Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<bool> unthrottleMatchingThrottles(Database db, KeyRef beginKey, KeyRef endKey, Optional<TransactionPriority> priority, bool onlyExpiredThrottles) {
		state Transaction tr(db);

		state KeySelector begin = firstGreaterOrEqual(beginKey);
		state KeySelector end = firstGreaterOrEqual(endKey);

		state bool removed = false;

		loop {
			try {
				state Standalone<RangeResultRef> tags = wait(tr.getRange(begin, end, 1000));
				state uint64_t unthrottledTags = 0;
				uint64_t manualUnthrottledTags = 0;
				for(auto tag : tags) {
					if(onlyExpiredThrottles) {
						double expirationTime = TagThrottleValue::fromValue(tag.value).expirationTime;
						if(expirationTime == 0 || expirationTime > now()) {
							continue;
						}
					}

					TagThrottleKey key = TagThrottleKey::fromKey(tag.key);
					if(priority.present() && key.priority != priority.get()) {
						continue;
					}

					if(key.throttleType == TagThrottleType::MANUAL) {
						++manualUnthrottledTags;
					}

					removed = true;
					tr.clear(tag.key);
					unthrottledTags ++;
				}

				if(manualUnthrottledTags > 0) {
					wait(updateThrottleCount(&tr, -manualUnthrottledTags));
				}

				if(unthrottledTags > 0) {
					signalThrottleChange(tr);
				}

				wait(tr.commit());

				if(!tags.more) {
					return removed;
				}

				ASSERT(tags.size() > 0);
				begin = KeySelector(firstGreaterThan(tags[tags.size()-1].key), tags.arena());
			}
			catch(Error& e) {
				wait(tr.onError(e));
			}
		}
	}	

	Future<bool> unthrottleAll(Database db, Optional<TagThrottleType> tagThrottleType, Optional<TransactionPriority> priority) {
		KeyRef begin = tagThrottleKeys.begin;
		KeyRef end = tagThrottleKeys.end;

		if(tagThrottleType.present() && tagThrottleType == TagThrottleType::AUTO) {
			begin = tagThrottleAutoKeysPrefix;
		}
		else if(tagThrottleType.present() && tagThrottleType == TagThrottleType::MANUAL) {
			end = tagThrottleAutoKeysPrefix;
		}

		return unthrottleMatchingThrottles(db, begin, end, priority, false);	
	}

	Future<bool> expire(Database db) {
		return unthrottleMatchingThrottles(db, tagThrottleKeys.begin, tagThrottleKeys.end, Optional<TransactionPriority>(), true);
	}

	ACTOR Future<Void> enableAuto(Database db, bool enabled) {
		state Transaction tr(db);

		loop {
			try {
				Optional<Value> value = wait(tr.get(tagThrottleAutoEnabledKey));
				if (!value.present() || (enabled && value.get() != LiteralStringRef("1")) || (!enabled && value.get() != LiteralStringRef("0"))) {
					tr.set(tagThrottleAutoEnabledKey, LiteralStringRef(enabled ? "1" : "0"));
					signalThrottleChange(tr);

					wait(tr.commit());
				}
				return Void();
			}
			catch(Error& e) {
				wait(tr.onError(e));
			}
		}
	}
}
