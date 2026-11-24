/*
 * TagThrottle.cpp
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

#include "fdbclient/SystemData.h"
#include "fdbclient/TagThrottle.h"

double const ClientTagThrottleLimits::NO_EXPIRATION = std::numeric_limits<double>::max();

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
		tags.push_back(tagRef);
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
		if (!tag.empty()) {
			memcpy(str, tag.begin(), tag.size());
			str += tag.size();
		}
	}

	return result;
}

Optional<TagThrottleKey> TagThrottleKey::tryFromKey(const KeyRef& key) {
	// A throttle currently encodes one tag: type, priority, length, and the tag bytes.
	if (!key.startsWith(tagThrottleKeysPrefix)) {
		return {};
	}
	KeyRef suffix = key.removePrefix(tagThrottleKeysPrefix);
	if (suffix.size() < 3) {
		return {};
	}
	auto throttleType = TagThrottleType(suffix[0]);
	auto priority = TransactionPriority(suffix[1]);
	int tagLength = suffix[2];
	if (throttleType > TagThrottleType::AUTO || priority > TransactionPriority::MAX || tagLength != suffix.size() - 3 ||
	    tagLength > CLIENT_KNOBS->MAX_TRANSACTION_TAG_LENGTH) {
		return {};
	}
	TagSet tags;
	tags.addTag(suffix.substr(3));
	return TagThrottleKey(tags, throttleType, priority);
}

Optional<TagThrottleValue> TagThrottleValue::tryFromValue(const ValueRef& value) {
	constexpr int minSize = sizeof(ProtocolVersion) + 3 * sizeof(double) + sizeof(TagThrottledReason);
	if (value.size() < minSize) {
		return {};
	}
	BinaryReader reader(value, Unversioned());
	ProtocolVersion version;
	reader >> version;
	if (!version.hasTagThrottleValueReason() || version >= minInvalidProtocolVersion) {
		return {};
	}
	reader.setProtocolVersion(version);
	TagThrottleValue throttle;
	reader >> throttle;
	return throttle;
}

TEST_CASE("/fdbclient/TagThrottle/KeyDecoding") {
	TagSet tags;
	tags.addTag("abc"_sr);
	auto key = TagThrottleKey(tags, TagThrottleType::AUTO, TransactionPriority::BATCH).toKey();
	for (int size = 0; size < key.size(); ++size) {
		ASSERT(!TagThrottleKey::tryFromKey(key.substr(0, size)).present());
	}
	auto decoded = TagThrottleKey::tryFromKey(key);
	ASSERT(decoded.present());
	ASSERT(decoded.get().throttleType == TagThrottleType::AUTO);
	ASSERT(decoded.get().priority == TransactionPriority::BATCH);
	ASSERT(*decoded.get().tags.begin() == "abc"_sr);
	ASSERT(!TagThrottleKey::tryFromKey(key.withSuffix("\x00"_sr)).present());

	TagSet emptyTag;
	emptyTag.addTag(""_sr);
	auto emptyKey = TagThrottleKey(emptyTag, TagThrottleType::MANUAL, TransactionPriority::IMMEDIATE).toKey();
	auto decodedEmpty = TagThrottleKey::tryFromKey(emptyKey);
	ASSERT(decodedEmpty.present() && decodedEmpty.get().tags.size() == 1);
	return Void();
}

TEST_CASE("/fdbclient/TagThrottle/ValueDecoding") {
	BinaryWriter writer(IncludeVersion(ProtocolVersion::withTagThrottleValueReason()));
	TagThrottleValue throttle(1.0, 2.0, 3.0, TagThrottledReason::MANUAL);
	writer << throttle;
	auto value = writer.toValue();
	for (int size = 0; size < value.size(); ++size) {
		ASSERT(!TagThrottleValue::tryFromValue(value.substr(0, size)).present());
	}
	auto decoded = TagThrottleValue::tryFromValue(value);
	ASSERT(decoded.present());
	ASSERT(decoded.get().tpsRate == 1.0 && decoded.get().expirationTime == 2.0 &&
	       decoded.get().initialDuration == 3.0 && decoded.get().reason == TagThrottledReason::MANUAL);
	return Void();
}

TEST_CASE("/fdbclient/TagSet/WireLengths") {
	constexpr FileIdentifier id{ 12345 };
	for (StringRef payload : { "\x00\x03tag"_sr, "\x00\x04tag"_sr }) {
		ObjectWriter writer(Unversioned());
		writer.serialize(id, payload);
		TagSet tags;
		ArenaObjectReader reader(Arena(), writer.toStringRef(), Unversioned());
		if (payload[1] == 3) {
			reader.deserialize(id, tags);
			ASSERT(tags.size() == 2 && tags.begin()->empty() && tags.begin()[1] == "tag"_sr);
		} else if (!g_network->isSimulated()) {
			// The object reader asserts on malformed input in simulation.
			tags.addTag("original"_sr);
			try {
				reader.deserialize(id, tags);
				ASSERT(false);
			} catch (Error& e) {
				ASSERT(e.code() == error_code_serialization_failed);
				ASSERT(tags.size() == 1 && *tags.begin() == "original"_sr);
			}
		}
	}
	return Void();
}

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
