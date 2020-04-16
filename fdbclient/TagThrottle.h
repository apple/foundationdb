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

typedef StringRef TransactionTagRef;
typedef Standalone<TransactionTagRef> TransactionTag;

struct TagThrottleInfo {
	enum class Priority {
		BATCH,
		DEFAULT,
		IMMEDIATE
	};

	static const char* priorityToString(Priority priority, bool capitalize=true) {
		switch(priority) {
			case Priority::BATCH:
				return capitalize ? "Batch" : "batch";
			case Priority::DEFAULT:
				return capitalize ? "Default" : "default";
			case Priority::IMMEDIATE:
				return capitalize ? "Immediate" : "immediate";
		}

		ASSERT(false);
		throw internal_error();
	}

	static Priority priorityFromReadVersionFlags(int flags); 

	double tpsRate;
	double expiration;
	bool autoThrottled;
	Priority priority;

	bool serializeExpirationAsDuration;

	TagThrottleInfo() : tpsRate(0), expiration(0), autoThrottled(false), priority(Priority::DEFAULT), serializeExpirationAsDuration(true) {}
	TagThrottleInfo(double tpsRate, double expiration, bool autoThrottled, Priority priority, bool serializeExpirationAsDuration) 
		: tpsRate(tpsRate), expiration(expiration), autoThrottled(autoThrottled), priority(priority), 
		  serializeExpirationAsDuration(serializeExpirationAsDuration) {}

	template<class Ar>
	void serialize(Ar& ar) {
		if(ar.isDeserializing) {
			serializer(ar, tpsRate, expiration, autoThrottled, priority, serializeExpirationAsDuration);
			if(serializeExpirationAsDuration) {
				expiration += now();
			}
		}
		else {
			double serializedExpiration = expiration;
			if(serializeExpirationAsDuration) {
				serializedExpiration = std::max(expiration - now(), 0.0);
			}

			serializer(ar, tpsRate, serializedExpiration, autoThrottled, priority, serializeExpirationAsDuration);
		}
	}
};

BINARY_SERIALIZABLE(TagThrottleInfo::Priority);

class TagSet {
public:
	typedef std::set<TransactionTagRef>::const_iterator const_iterator;

	TagSet() : bytes(0) {}

	void addTag(TransactionTagRef tag); 
	size_t size();

	const_iterator begin() const {
		return tags.begin();
	}

	const_iterator end() const {
		return tags.end();
	}

//private:
	Arena arena; // TODO: where to hold this memory?
	std::set<TransactionTagRef> tags;
	size_t bytes;
};

template <>
struct dynamic_size_traits<TagSet> : std::true_type {
	// May be called multiple times during one serialization
	template <class Context>
	static size_t size(const TagSet& t, Context&) {
		return t.tags.size() + t.bytes;
	}

	// Guaranteed to be called only once during serialization
	template <class Context>
	static void save(uint8_t* out, const TagSet& t, Context&) {
		for (const auto& tag : t.tags) {
			*out = (uint8_t)tag.size();
			++out;

			std::copy(tag.begin(), tag.end(), out);
			out += tag.size();
		}
	}

	// Context is an arbitrary type that is plumbed by reference throughout the
	// load call tree.
	template <class Context>
	static void load(const uint8_t* data, size_t size, TagSet& t, Context& context) {
		const uint8_t *end = data + size;
		while(data < end) {
			uint8_t len = *data;
			++data;
			TransactionTagRef tag(context.tryReadZeroCopy(data, len), len);
			data += len;

			t.tags.insert(tag);
			t.bytes += tag.size();
		}

		t.arena = context.arena(); // TODO: this arena could be big
	}
};

template<class Value>
using TagThrottleMap = std::unordered_map<TransactionTag, Value, std::hash<TransactionTagRef>>;

template<class Value>
using PrioritizedTagThrottleMap = std::map<TagThrottleInfo::Priority, TagThrottleMap<Value>>;

namespace ThrottleApi {
	// Currently, only 1 tag in a key is supported
	Key throttleKeyForTags(std::set<TransactionTagRef> const& tags);
	TransactionTagRef tagFromThrottleKey(KeyRef key);

	Future<std::map<TransactionTag, TagThrottleInfo>> getTags(Database const& db, int const& limit);

	Future<Void> throttleTag(Database const& db, TransactionTagRef const& tag, double const& tpsRate, double const& expiration, 
	                         bool const& serializeExpirationAsDuration, bool const& autoThrottled); // TODO: priorities

	Future<bool> unthrottleTag(Database const& db, TransactionTagRef const& tag);

	Future<uint64_t> unthrottleManual(Database db);
	Future<uint64_t> unthrottleAuto(Database db);
	Future<uint64_t> unthrottleAll(Database db);

	Future<Void> enableAuto(Database const& db, bool const& enabled);

	TagThrottleInfo decodeTagThrottleValue(const ValueRef& value);
};

#endif