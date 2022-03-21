/*
 * TagThrottle.actor.h
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

#include "flow/Arena.h"
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_TAG_THROTTLE_ACTOR_G_H)
#define FDBCLIENT_TAG_THROTTLE_ACTOR_G_H
#include "fdbclient/TagThrottle.actor.g.h"
#elif !defined(FDBCLIENT_TAG_THROTTLE_ACTOR_H)
#define FDBCLIENT_TAG_THROTTLE_ACTOR_H

#pragma once

#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "flow/Error.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/ThreadHelper.actor.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "flow/actorcompiler.h" // This must be the last #include.

typedef StringRef TransactionTagRef;
typedef Standalone<TransactionTagRef> TransactionTag;

FDB_DECLARE_BOOLEAN_PARAM(ContainsRecommended);
FDB_DECLARE_BOOLEAN_PARAM(Capitalize);

class TagSet {
public:
	typedef std::vector<TransactionTagRef>::const_iterator const_iterator;

	TagSet() : bytes(0) {}

	void addTag(TransactionTagRef tag);
	size_t size() const;

	const_iterator begin() const { return tags.begin(); }

	const_iterator end() const { return tags.end(); }

	void clear() {
		tags.clear();
		bytes = 0;
	}

	template <class Context>
	void save(uint8_t* out, Context&) const {
		uint8_t* start = out;
		for (const auto& tag : *this) {
			*(out++) = (uint8_t)tag.size();

			std::copy(tag.begin(), tag.end(), out);
			out += tag.size();
		}

		ASSERT((size_t)(out - start) == size() + bytes);
	}

	template <class Context>
	void load(const uint8_t* data, size_t size, Context& context) {
		// const uint8_t *start = data;
		const uint8_t* end = data + size;
		while (data < end) {
			uint8_t len = *(data++);
			// Tags are already deduplicated
			const auto& tag = tags.emplace_back(context.tryReadZeroCopy(data, len), len);
			data += len;
			bytes += tag.size();
		}

		ASSERT(data == end);

		// Deserialized tag sets share the arena with the request that contained them
		// For this reason, persisting a TagSet that shares memory with other request
		// members should be done with caution.
		arena = context.arena();
	}

	size_t getBytes() const { return bytes; }

	const Arena& getArena() const { return arena; }

	// Used by fdbcli commands
	std::string toString(Capitalize = Capitalize::False) const;

private:
	size_t bytes;
	Arena arena;
	// Currently there are never >= 256 tags, so
	// std::vector is faster than std::set. This may
	// change if we allow more tags in the future.
	std::vector<TransactionTagRef> tags;
};

template <>
struct dynamic_size_traits<TagSet> : std::true_type {
	// May be called multiple times during one serialization
	template <class Context>
	static size_t size(const TagSet& t, Context&) {
		return t.size() + t.getBytes();
	}

	// Guaranteed to be called only once during serialization
	template <class Context>
	static void save(uint8_t* out, const TagSet& t, Context& c) {
		t.save(out, c);
	}

	// Context is an arbitrary type that is plumbed by reference throughout the
	// load call tree.
	template <class Context>
	static void load(const uint8_t* data, size_t size, TagSet& t, Context& context) {
		t.load(data, size, context);
	}
};

enum class TagThrottleType : uint8_t { MANUAL, AUTO };

enum class TagThrottledReason : uint8_t { UNSET = 0, MANUAL, BUSY_READ, BUSY_WRITE };

struct TagThrottleKey {
	TagSet tags;
	TagThrottleType throttleType;
	TransactionPriority priority;

	TagThrottleKey() : throttleType(TagThrottleType::MANUAL), priority(TransactionPriority::DEFAULT) {}
	TagThrottleKey(TagSet tags, TagThrottleType throttleType, TransactionPriority priority)
	  : tags(tags), throttleType(throttleType), priority(priority) {}

	Key toKey() const;
	static TagThrottleKey fromKey(const KeyRef& key);
};

struct TagThrottleValue {
	double tpsRate;
	double expirationTime;
	double initialDuration;
	TagThrottledReason reason;

	TagThrottleValue() : tpsRate(0), expirationTime(0), initialDuration(0), reason(TagThrottledReason::UNSET) {}
	TagThrottleValue(double tpsRate, double expirationTime, double initialDuration, TagThrottledReason reason)
	  : tpsRate(tpsRate), expirationTime(expirationTime), initialDuration(initialDuration), reason(reason) {}

	static TagThrottleValue fromValue(const ValueRef& value);

	// To change this serialization, ProtocolVersion::TagThrottleValue must be updated, and downgrades need to be
	// considered
	template <class Ar>
	void serialize(Ar& ar) {
		if (ar.protocolVersion().hasTagThrottleValueReason()) {
			serializer(ar, tpsRate, expirationTime, initialDuration, reason);
		} else if (ar.protocolVersion().hasTagThrottleValue()) {
			serializer(ar, tpsRate, expirationTime, initialDuration);
			if (ar.isDeserializing) {
				reason = TagThrottledReason::UNSET;
			}
		}
	}
};

struct TagThrottleInfo {
	TransactionTag tag;
	TagThrottleType throttleType;
	TransactionPriority priority;
	double tpsRate;
	double expirationTime;
	double initialDuration;
	TagThrottledReason reason;

	TagThrottleInfo(TransactionTag tag,
	                TagThrottleType throttleType,
	                TransactionPriority priority,
	                double tpsRate,
	                double expirationTime,
	                double initialDuration,
	                TagThrottledReason reason = TagThrottledReason::UNSET)
	  : tag(tag), throttleType(throttleType), priority(priority), tpsRate(tpsRate), expirationTime(expirationTime),
	    initialDuration(initialDuration), reason(reason) {}

	TagThrottleInfo(TagThrottleKey key, TagThrottleValue value)
	  : throttleType(key.throttleType), priority(key.priority), tpsRate(value.tpsRate),
	    expirationTime(value.expirationTime), initialDuration(value.initialDuration), reason(value.reason) {
		ASSERT(key.tags.size() == 1); // Multiple tags per throttle is not currently supported
		tag = *key.tags.begin();
	}
};

struct ClientTagThrottleLimits {
	double tpsRate;
	double expiration;

	ClientTagThrottleLimits() : tpsRate(0), expiration(0) {}
	ClientTagThrottleLimits(double tpsRate, double expiration) : tpsRate(tpsRate), expiration(expiration) {}

	template <class Archive>
	void serialize(Archive& ar) {
		// Convert expiration time to a duration to avoid clock differences
		double duration = 0;
		if (!ar.isDeserializing) {
			duration = expiration - now();
		}

		serializer(ar, tpsRate, duration);

		if (ar.isDeserializing) {
			expiration = now() + duration;
		}
	}
};

struct ClientTrCommitCostEstimation {
	int opsCount = 0;
	uint64_t writeCosts = 0;
	std::deque<std::pair<int, uint64_t>> clearIdxCosts;
	uint32_t expensiveCostEstCount = 0;
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, opsCount, writeCosts, clearIdxCosts, expensiveCostEstCount);
	}
};

// Keys to view and control tag throttling
extern const KeyRangeRef tagThrottleKeys;
extern const KeyRef tagThrottleKeysPrefix;
extern const KeyRef tagThrottleAutoKeysPrefix;
extern const KeyRef tagThrottleSignalKey;
extern const KeyRef tagThrottleAutoEnabledKey;
extern const KeyRef tagThrottleLimitKey;
extern const KeyRef tagThrottleCountKey;

namespace ThrottleApi {

// The template functions can be called with Native API like DatabaseContext, Transaction/ReadYourWritesTransaction
// or using IClientAPI like IDatabase, ITransaction

ACTOR template <class Tr>
Future<bool> getValidAutoEnabled(Reference<Tr> tr) {
	state bool result;
	loop {
		// hold the returned standalone object's memory
		state typename Tr::template FutureT<Optional<Value>> valueF = tr->get(tagThrottleAutoEnabledKey);
		Optional<Value> value = wait(safeThreadFutureToFuture(valueF));
		if (!value.present()) {
			tr->reset();
			wait(delay(CLIENT_KNOBS->DEFAULT_BACKOFF));
			continue;
		} else if (value.get() == LiteralStringRef("1")) {
			result = true;
		} else if (value.get() == LiteralStringRef("0")) {
			result = false;
		} else {
			TraceEvent(SevWarnAlways, "InvalidAutoTagThrottlingValue").detail("Value", value.get());
			tr->reset();
			wait(delay(CLIENT_KNOBS->DEFAULT_BACKOFF));
			continue;
		}
		return result;
	};
}

ACTOR template <class DB>
Future<std::vector<TagThrottleInfo>> getRecommendedTags(Reference<DB> db, int limit) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	loop {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		try {
			bool enableAuto = wait(getValidAutoEnabled(tr));
			if (enableAuto) {
				return std::vector<TagThrottleInfo>();
			}
			state typename DB::TransactionT::template FutureT<RangeResult> f =
			    tr->getRange(KeyRangeRef(tagThrottleAutoKeysPrefix, tagThrottleKeys.end), limit);
			RangeResult throttles = wait(safeThreadFutureToFuture(f));
			std::vector<TagThrottleInfo> results;
			for (auto throttle : throttles) {
				results.push_back(TagThrottleInfo(TagThrottleKey::fromKey(throttle.key),
				                                  TagThrottleValue::fromValue(throttle.value)));
			}
			return results;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class DB>
Future<std::vector<TagThrottleInfo>>
getThrottledTags(Reference<DB> db, int limit, ContainsRecommended containsRecommended = ContainsRecommended::False) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state bool reportAuto = containsRecommended;
	loop {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		try {
			if (!containsRecommended) {
				wait(store(reportAuto, getValidAutoEnabled(tr)));
			}
			state typename DB::TransactionT::template FutureT<RangeResult> f = tr->getRange(
			    reportAuto ? tagThrottleKeys : KeyRangeRef(tagThrottleKeysPrefix, tagThrottleAutoKeysPrefix), limit);
			RangeResult throttles = wait(safeThreadFutureToFuture(f));
			std::vector<TagThrottleInfo> results;
			for (auto throttle : throttles) {
				results.push_back(TagThrottleInfo(TagThrottleKey::fromKey(throttle.key),
				                                  TagThrottleValue::fromValue(throttle.value)));
			}
			return results;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

template <class Tr>
void signalThrottleChange(Reference<Tr> tr) {
	tr->atomicOp(
	    tagThrottleSignalKey, LiteralStringRef("XXXXXXXXXX\x00\x00\x00\x00"), MutationRef::SetVersionstampedValue);
}

ACTOR template <class Tr>
Future<Void> updateThrottleCount(Reference<Tr> tr, int64_t delta) {
	state typename Tr::template FutureT<Optional<Value>> countVal = tr->get(tagThrottleCountKey);
	state typename Tr::template FutureT<Optional<Value>> limitVal = tr->get(tagThrottleLimitKey);

	wait(success(safeThreadFutureToFuture(countVal)) && success(safeThreadFutureToFuture(limitVal)));

	int64_t count = 0;
	int64_t limit = 0;

	if (countVal.get().present()) {
		BinaryReader reader(countVal.get().get(), Unversioned());
		reader >> count;
	}

	if (limitVal.get().present()) {
		BinaryReader reader(limitVal.get().get(), Unversioned());
		reader >> limit;
	}

	count += delta;

	if (count > limit) {
		throw too_many_tag_throttles();
	}

	BinaryWriter writer(Unversioned());
	writer << count;

	tr->set(tagThrottleCountKey, writer.toValue());
	return Void();
}

ACTOR template <class DB>
Future<bool> unthrottleMatchingThrottles(Reference<DB> db,
                                         KeyRef beginKey,
                                         KeyRef endKey,
                                         Optional<TransactionPriority> priority,
                                         bool onlyExpiredThrottles) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	state KeySelector begin = firstGreaterOrEqual(beginKey);
	state KeySelector end = firstGreaterOrEqual(endKey);

	state bool removed = false;

	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			// holds memory of the RangeResult
			state typename DB::TransactionT::template FutureT<RangeResult> f = tr->getRange(begin, end, 1000);
			state RangeResult tags = wait(safeThreadFutureToFuture(f));
			state uint64_t unthrottledTags = 0;
			uint64_t manualUnthrottledTags = 0;
			for (auto tag : tags) {
				if (onlyExpiredThrottles) {
					double expirationTime = TagThrottleValue::fromValue(tag.value).expirationTime;
					if (expirationTime == 0 || expirationTime > now()) {
						continue;
					}
				}

				TagThrottleKey key = TagThrottleKey::fromKey(tag.key);
				if (priority.present() && key.priority != priority.get()) {
					continue;
				}

				if (key.throttleType == TagThrottleType::MANUAL) {
					++manualUnthrottledTags;
				}

				removed = true;
				tr->clear(tag.key);
				unthrottledTags++;
			}

			if (manualUnthrottledTags > 0) {
				wait(updateThrottleCount(tr, -manualUnthrottledTags));
			}

			if (unthrottledTags > 0) {
				signalThrottleChange(tr);
			}

			wait(safeThreadFutureToFuture(tr->commit()));

			if (!tags.more) {
				return removed;
			}

			ASSERT(tags.size() > 0);
			begin = KeySelector(firstGreaterThan(tags[tags.size() - 1].key), tags.arena());
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

template <class DB>
Future<bool> expire(DB db) {
	return unthrottleMatchingThrottles(
	    db, tagThrottleKeys.begin, tagThrottleKeys.end, Optional<TransactionPriority>(), true);
}

template <class DB>
Future<bool> unthrottleAll(Reference<DB> db,
                           Optional<TagThrottleType> tagThrottleType,
                           Optional<TransactionPriority> priority) {
	KeyRef begin = tagThrottleKeys.begin;
	KeyRef end = tagThrottleKeys.end;

	if (tagThrottleType.present() && tagThrottleType == TagThrottleType::AUTO) {
		begin = tagThrottleAutoKeysPrefix;
	} else if (tagThrottleType.present() && tagThrottleType == TagThrottleType::MANUAL) {
		end = tagThrottleAutoKeysPrefix;
	}

	return unthrottleMatchingThrottles(db, begin, end, priority, false);
}

ACTOR template <class DB>
Future<bool> unthrottleTags(Reference<DB> db,
                            TagSet tags,
                            Optional<TagThrottleType> throttleType,
                            Optional<TransactionPriority> priority) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	state std::vector<Key> keys;
	for (auto p : allTransactionPriorities) {
		if (!priority.present() || priority.get() == p) {
			if (!throttleType.present() || throttleType.get() == TagThrottleType::AUTO) {
				keys.push_back(TagThrottleKey(tags, TagThrottleType::AUTO, p).toKey());
			}
			if (!throttleType.present() || throttleType.get() == TagThrottleType::MANUAL) {
				keys.push_back(TagThrottleKey(tags, TagThrottleType::MANUAL, p).toKey());
			}
		}
	}

	state bool removed = false;

	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			state std::vector<typename DB::TransactionT::template FutureT<Optional<Value>>> valueFutures;
			state std::vector<Future<Optional<Value>>> values;
			values.reserve(keys.size());
			for (auto key : keys) {
				valueFutures.push_back(tr->get(key));
				values.push_back(safeThreadFutureToFuture(valueFutures.back()));
			}

			wait(waitForAll(values));

			int delta = 0;
			for (int i = 0; i < values.size(); ++i) {
				if (values[i].get().present()) {
					if (TagThrottleKey::fromKey(keys[i]).throttleType == TagThrottleType::MANUAL) {
						delta -= 1;
					}

					tr->clear(keys[i]);

					// Report that we are removing this tag if we ever see it present.
					// This protects us from getting confused if the transaction is maybe committed.
					// It's ok if someone else actually ends up removing this tag at the same time
					// and we aren't the ones to actually do it.
					removed = true;
				}
			}

			if (delta != 0) {
				wait(updateThrottleCount(tr, delta));
			}
			if (removed) {
				signalThrottleChange(tr);
				wait(safeThreadFutureToFuture(tr->commit()));
			}

			return removed;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class DB>
Future<Void> throttleTags(Reference<DB> db,
                          TagSet tags,
                          double tpsRate,
                          double initialDuration,
                          TagThrottleType throttleType,
                          TransactionPriority priority,
                          Optional<double> expirationTime = Optional<double>(),
                          Optional<TagThrottledReason> reason = Optional<TagThrottledReason>()) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	state Key key = TagThrottleKey(tags, throttleType, priority).toKey();

	ASSERT(initialDuration > 0);

	if (throttleType == TagThrottleType::MANUAL) {
		reason = TagThrottledReason::MANUAL;
	}
	TagThrottleValue throttle(tpsRate,
	                          expirationTime.present() ? expirationTime.get() : 0,
	                          initialDuration,
	                          reason.present() ? reason.get() : TagThrottledReason::UNSET);
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withTagThrottleValueReason()));
	wr << throttle;
	state Value value = wr.toValue();

	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			if (throttleType == TagThrottleType::MANUAL) {
				// hold the returned standalone object's memory
				state typename DB::TransactionT::template FutureT<Optional<Value>> oldThrottleF = tr->get(key);
				Optional<Value> oldThrottle = wait(safeThreadFutureToFuture(oldThrottleF));
				if (!oldThrottle.present()) {
					wait(updateThrottleCount(tr, 1));
				}
			}

			tr->set(key, value);

			if (throttleType == TagThrottleType::MANUAL) {
				signalThrottleChange(tr);
			}

			wait(safeThreadFutureToFuture(tr->commit()));
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class DB>
Future<Void> enableAuto(Reference<DB> db, bool enabled) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			// hold the returned standalone object's memory
			state typename DB::TransactionT::template FutureT<Optional<Value>> valueF =
			    tr->get(tagThrottleAutoEnabledKey);
			Optional<Value> value = wait(safeThreadFutureToFuture(valueF));
			if (!value.present() || (enabled && value.get() != LiteralStringRef("1")) ||
			    (!enabled && value.get() != LiteralStringRef("0"))) {
				tr->set(tagThrottleAutoEnabledKey, LiteralStringRef(enabled ? "1" : "0"));
				signalThrottleChange<typename DB::TransactionT>(tr);

				wait(safeThreadFutureToFuture(tr->commit()));
			}
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

}; // namespace ThrottleApi

template <class Value>
using TransactionTagMap = std::unordered_map<TransactionTag, Value, std::hash<TransactionTagRef>>;

template <class Value>
using PrioritizedTransactionTagMap = std::map<TransactionPriority, TransactionTagMap<Value>>;

template <class Value>
using UIDTransactionTagMap = std::unordered_map<UID, TransactionTagMap<Value>>;

#include "flow/unactorcompiler.h"
#endif
