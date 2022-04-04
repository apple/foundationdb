/*
 * TagThrottle.actor.cpp
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

#include "fdbclient/TagThrottle.actor.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/DatabaseContext.h"

#include "flow/actorcompiler.h" // has to be last include

void TagSet::addTag(TransactionTagRef tag) {
	ASSERT(CLIENT_KNOBS->MAX_TRANSACTION_TAG_LENGTH < 256); // Tag length is encoded with a single byte
	ASSERT(CLIENT_KNOBS->MAX_TAGS_PER_TRANSACTION < 256); // Number of tags is encoded with a single byte

	if (tag.size() > CLIENT_KNOBS->MAX_TRANSACTION_TAG_LENGTH) {
		throw tag_too_long();
	}
	if (tags.size() >= CLIENT_KNOBS->MAX_TAGS_PER_TRANSACTION) {
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

std::string TagSet::toString(Capitalize capitalize) const {
	ASSERT(!tags.empty());
	if (tags.size() == 1) {
		std::string start = capitalize ? "Tag" : "tag";
		return format("%s `%s'", start.c_str(), tags[0].toString().c_str());
	}
	std::string result = capitalize ? "Tags (" : "tags (";
	for (int index = 0; index < tags.size() - 1; ++index) {
		result += format("`%s', ", tags[index].toString().c_str());
	}
	return result + format("`%s')", tags.back().toString().c_str());
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
	for (auto tag : tags) {
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

	for (auto tag : tags) {
		*(str++) = (uint8_t)tag.size();
		if (tag.size() > 0) {
			memcpy(str, tag.begin(), tag.size());
			str += tag.size();
		}
	}

	return result;
}

TagThrottleKey TagThrottleKey::fromKey(const KeyRef& key) {
	const uint8_t* str = key.substr(tagThrottleKeysPrefix.size()).begin();
	TagThrottleType throttleType = TagThrottleType(*(str++));
	TransactionPriority priority = TransactionPriority(*(str++));
	TagSet tags;

	while (str < key.end()) {
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

FDB_DEFINE_BOOLEAN_PARAM(ContainsRecommended);
FDB_DEFINE_BOOLEAN_PARAM(Capitalize);

TEST_CASE("TagSet/toString") {
	{
		TagSet tagSet;
		tagSet.addTag("a"_sr);
		ASSERT(tagSet.toString() == "tag `a'");
		ASSERT(tagSet.toString(Capitalize::True) == "Tag `a'");
	}
	{
		// Order is not guaranteed when multiple tags are present
		TagSet tagSet;
		tagSet.addTag("a"_sr);
		tagSet.addTag("b"_sr);
		auto tagString = tagSet.toString();
		ASSERT(tagString == "tags (`a', `b')" || tagString == "tags (`b', `a')");
		auto capitalizedTagString = tagSet.toString(Capitalize::True);
		ASSERT(capitalizedTagString == "Tags (`a', `b')" || capitalizedTagString == "Tags (`b', `a')");
	}
	return Void();
}
