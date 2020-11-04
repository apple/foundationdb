/*
 * TagThrottle.h
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

#ifndef FDBCLIENT_TAG_THROTTLE_H
#define FDBCLIENT_TAG_THROTTLE_H

#pragma once

#include "flow/Error.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "fdbclient/FDBTypes.h"

#include <set>

class Database;

namespace ThrottleApi {
}

typedef StringRef TransactionTagRef;
typedef Standalone<TransactionTagRef> TransactionTag;

class TagSet {
public:
	typedef std::vector<TransactionTagRef>::const_iterator const_iterator;

	TagSet() : bytes(0) {}

	void addTag(TransactionTagRef tag); 
	size_t size() const;

	const_iterator begin() const {
		return tags.begin();
	}

	const_iterator end() const {
		return tags.end();
	}

	void clear() {
		tags.clear();
		bytes = 0;
	}

	template <class Context>
	void save(uint8_t* out, Context& c) const {
		uint8_t *start = out;
		for (const auto& tag : *this) {
			*(out++) = (uint8_t)tag.size();

			std::copy(tag.begin(), tag.end(), out);
			out += tag.size();
		}

		ASSERT((size_t)(out - start) == size() + bytes);
	}

	template <class Context>
	void load(const uint8_t* data, size_t size, Context& context) {
		//const uint8_t *start = data;
		const uint8_t *end = data + size;
		while(data < end) {
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

enum class TagThrottleType : uint8_t {
	MANUAL,
	AUTO
};

enum class TagThrottledReason: uint8_t {
	UNSET = 0,
	MANUAL,
	BUSY_READ,
	BUSY_WRITE
};

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

	//To change this serialization, ProtocolVersion::TagThrottleValue must be updated, and downgrades need to be considered
	template<class Ar>
	void serialize(Ar& ar) {
		if(ar.protocolVersion().hasTagThrottleValueReason()) {
			serializer(ar, tpsRate, expirationTime, initialDuration, reinterpret_cast<uint8_t&>(reason));
		}
		else if(ar.protocolVersion().hasTagThrottleValue()) {
			serializer(ar, tpsRate, expirationTime, initialDuration);
			if(ar.isDeserializing) {
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

	TagThrottleInfo(TransactionTag tag, TagThrottleType throttleType, TransactionPriority priority, double tpsRate, double expirationTime, double initialDuration, TagThrottledReason reason = TagThrottledReason::UNSET)
		: tag(tag), throttleType(throttleType), priority(priority), tpsRate(tpsRate), expirationTime(expirationTime), initialDuration(initialDuration), reason(reason) {}

	TagThrottleInfo(TagThrottleKey key, TagThrottleValue value)
		: throttleType(key.throttleType), priority(key.priority), tpsRate(value.tpsRate), expirationTime(value.expirationTime), initialDuration(value.initialDuration), reason(value.reason)
	{
		ASSERT(key.tags.size() == 1); // Multiple tags per throttle is not currently supported
		tag = *key.tags.begin();
	}
};

namespace ThrottleApi {
	Future<std::vector<TagThrottleInfo>> getThrottledTags(Database const& db, int const& limit, bool const& containsRecommend = false);
	Future<std::vector<TagThrottleInfo>> getRecommendedTags(Database const& db, int const& limit);

	Future<Void> throttleTags(Database const& db, TagSet const& tags, double const& tpsRate, double const& initialDuration, 
	                         TagThrottleType const& throttleType, TransactionPriority const& priority, Optional<double> const& expirationTime = Optional<double>(),
                              Optional<TagThrottledReason> const& reason = Optional<TagThrottledReason>());

	Future<bool> unthrottleTags(Database const& db, TagSet const& tags, Optional<TagThrottleType> const& throttleType, Optional<TransactionPriority> const& priority);

	Future<bool> unthrottleAll(Database db, Optional<TagThrottleType> throttleType, Optional<TransactionPriority> priority);
	Future<bool> expire(Database db);

	Future<Void> enableAuto(Database const& db, bool const& enabled);
};

BINARY_SERIALIZABLE(TransactionPriority);

template<class Value>
using TransactionTagMap = std::unordered_map<TransactionTag, Value, std::hash<TransactionTagRef>>;

template<class Value>
using PrioritizedTransactionTagMap = std::map<TransactionPriority, TransactionTagMap<Value>>;

template <class Value>
using UIDTransactionTagMap = std::unordered_map<UID, TransactionTagMap<Value>>;
#endif
