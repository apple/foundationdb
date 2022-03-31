/*
 * FDBTypes.h
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

#ifndef FDBCLIENT_FDBTYPES_H
#define FDBCLIENT_FDBTYPES_H

#include <algorithm>
#include <set>
#include <string>
#include <vector>
#include <unordered_set>
#include <boost/functional/hash.hpp>

#include "flow/Arena.h"
#include "flow/flow.h"

typedef int64_t Version;
typedef uint64_t LogEpoch;
typedef uint64_t Sequence;
typedef StringRef KeyRef;
typedef StringRef ValueRef;
typedef int64_t Generation;
typedef UID SpanID;

enum {
	tagLocalitySpecial = -1, // tag with this locality means it is invalidTag (id=0), txsTag (id=1), or cacheTag (id=2)
	tagLocalityLogRouter = -2,
	tagLocalityRemoteLog = -3, // tag created by log router for remote (aka. not in Primary DC) tLogs
	tagLocalityUpgraded = -4, // tlogs with old log format
	tagLocalitySatellite = -5,
	tagLocalityLogRouterMapped = -6, // The pseudo tag used by log routers to pop the real LogRouter tag (i.e., -2)
	tagLocalityTxs = -7,
	tagLocalityBackup = -8, // used by backup role to pop from TLogs
	tagLocalityInvalid = -99
}; // The TLog and LogRouter require these number to be as compact as possible

inline bool isPseudoLocality(int8_t locality) {
	return locality == tagLocalityLogRouterMapped || locality == tagLocalityBackup;
}

#pragma pack(push, 1)
struct Tag {
	// if locality > 0,
	//    locality decides which DC id the tLog is in;
	//    id decides which SS owns the tag; id <-> SS mapping is in the system keyspace: serverTagKeys.
	// if locality < 0, locality decides the type of tLog set: satellite, LR, or remote tLog, etc.
	//    id decides which tLog in the tLog type will be used.
	int8_t locality;
	uint16_t id;

	Tag() : locality(tagLocalitySpecial), id(0) {}
	Tag(int8_t locality, uint16_t id) : locality(locality), id(id) {}

	bool operator==(const Tag& r) const { return locality == r.locality && id == r.id; }
	bool operator!=(const Tag& r) const { return locality != r.locality || id != r.id; }
	bool operator<(const Tag& r) const { return locality < r.locality || (locality == r.locality && id < r.id); }

	int toTagDataIndex() const { return locality >= 0 ? 2 * locality : 1 - (2 * locality); }

	bool isNonPrimaryTLogType() const { return locality < 0; }

	std::string toString() const { return format("%d:%d", locality, id); }

	template <class Ar>
	force_inline void serialize_unversioned(Ar& ar) {
		serializer(ar, locality, id);
	}
};

template <>
struct flow_ref<Tag> : std::integral_constant<bool, false> {};

#pragma pack(pop)

template <class Ar>
void load(Ar& ar, Tag& tag) {
	tag.serialize_unversioned(ar);
}
template <class Ar>
void save(Ar& ar, Tag const& tag) {
	const_cast<Tag&>(tag).serialize_unversioned(ar);
}

template <>
struct struct_like_traits<Tag> : std::true_type {
	using Member = Tag;
	using types = pack<uint16_t, int8_t>;

	template <int i, class Context>
	static const index_t<i, types>& get(const Member& m, Context&) {
		if constexpr (i == 0) {
			return m.id;
		} else {
			static_assert(i == 1);
			return m.locality;
		}
	}

	template <int i, class Type, class Context>
	static void assign(Member& m, const Type& t, Context&) {
		if constexpr (i == 0) {
			m.id = t;
		} else {
			static_assert(i == 1);
			m.locality = t;
		}
	}
};

template <>
struct Traceable<Tag> : std::true_type {
	static std::string toString(const Tag& value) { return value.toString(); }
};

namespace std {
template <>
struct hash<Tag> {
	std::size_t operator()(const Tag& tag) const {
		std::size_t seed = 0;
		boost::hash_combine(seed, std::hash<int8_t>{}(tag.locality));
		boost::hash_combine(seed, std::hash<uint16_t>{}(tag.id));
		return seed;
	}
};
} // namespace std

static const Tag invalidTag{ tagLocalitySpecial, 0 };
static const Tag txsTag{ tagLocalitySpecial, 1 };
static const Tag cacheTag{ tagLocalitySpecial, 2 };

enum { txsTagOld = -1, invalidTagOld = -100 };

struct TagsAndMessage {
	StringRef message;
	VectorRef<Tag> tags;

	TagsAndMessage() {}
	TagsAndMessage(StringRef message, VectorRef<Tag> tags) : message(message), tags(tags) {}

	// Loads tags and message from a serialized buffer. "rd" is checkpointed at
	// its begining position to allow the caller to rewind if needed.
	// T can be ArenaReader or BinaryReader.
	template <class T>
	void loadFromArena(T* rd, uint32_t* messageVersionSub) {
		int32_t messageLength;
		uint16_t tagCount;
		uint32_t sub;

		rd->checkpoint();
		*rd >> messageLength >> sub >> tagCount;
		if (messageVersionSub)
			*messageVersionSub = sub;
		tags = VectorRef<Tag>((Tag*)rd->readBytes(tagCount * sizeof(Tag)), tagCount);
		const int32_t rawLength = messageLength + sizeof(messageLength);
		rd->rewind();
		rd->checkpoint();
		message = StringRef((const uint8_t*)rd->readBytes(rawLength), rawLength);
	}

	// Returns the size of the header, including: msg_length, version.sub, tag_count, tags.
	int32_t getHeaderSize() const {
		return sizeof(int32_t) + sizeof(uint32_t) + sizeof(uint16_t) + tags.size() * sizeof(Tag);
	}

	StringRef getMessageWithoutTags() const { return message.substr(getHeaderSize()); }

	// Returns the message with the header.
	StringRef getRawMessage() const { return message; }
};

struct KeyRangeRef;
struct KeyValueRef;

template <class Collection>
void uniquify(Collection& c) {
	std::sort(c.begin(), c.end());
	c.resize(std::unique(c.begin(), c.end()) - c.begin());
}

inline std::string describe(const Tag item) {
	return format("%d:%d", item.locality, item.id);
}

inline std::string describe(const int item) {
	return format("%d", item);
}

// Allows describeList to work on a vector of std::string
std::string describe(const std::string& s);

template <class T>
std::string describe(Reference<T> const& item) {
	return item->toString();
}

std::string describe(UID const& item);

template <class T>
std::string describe(T const& item) {
	return item.toString();
}

template <class K, class V>
std::string describe(std::map<K, V> const& items, int max_items = -1) {
	if (!items.size())
		return "[no items]";

	std::string s;
	int count = 0;
	for (auto it = items.begin(); it != items.end(); it++) {
		if (++count > max_items && max_items >= 0)
			break;
		if (count > 1)
			s += ",";
		s += describe(it->first) + "=>" + describe(it->second);
	}
	return s;
}

template <class T>
std::string describeList(T const& items, int max_items) {
	if (!items.size())
		return "[no items]";

	std::string s;
	int count = 0;
	for (auto const& item : items) {
		if (++count > max_items && max_items >= 0)
			break;
		if (count > 1)
			s += ",";
		s += describe(item);
	}
	return s;
}

template <class T>
std::string describe(std::vector<T> const& items, int max_items = -1) {
	return describeList(items, max_items);
}

template <class T>
std::string describe(std::unordered_set<T> const& items, int max_items = -1) {
	return describeList(items, max_items);
}

template <typename T>
struct Traceable<std::vector<T>> : std::true_type {
	static std::string toString(const std::vector<T>& value) { return describe(value); }
};

template <class T>
std::string describe(std::set<T> const& items, int max_items = -1) {
	return describeList(items, max_items);
}

template <typename T>
struct Traceable<std::set<T>> : std::true_type {
	static std::string toString(const std::set<T>& value) { return describe(value); }
};

std::string printable(const StringRef& val);
std::string printable(const std::string& val);
std::string printable(const KeyRangeRef& range);
std::string printable(const VectorRef<KeyRangeRef>& val);
std::string printable(const VectorRef<StringRef>& val);
std::string printable(const VectorRef<KeyValueRef>& val);
std::string printable(const KeyValueRef& val);

template <class T>
std::string printable(const Optional<T>& val) {
	if (val.present())
		return printable(val.get());
	return "[not set]";
}

inline bool equalsKeyAfter(const KeyRef& key, const KeyRef& compareKey) {
	if (key.size() + 1 != compareKey.size() || compareKey[compareKey.size() - 1] != 0)
		return false;
	return compareKey.startsWith(key);
}

struct KeyRangeRef {
	const KeyRef begin, end;
	KeyRangeRef() {}
	KeyRangeRef(const KeyRef& begin, const KeyRef& end) : begin(begin), end(end) {
		if (begin > end) {
			TraceEvent("InvertedRange").detail("Begin", begin).detail("End", end);
			throw inverted_range();
		}
	}
	KeyRangeRef(Arena& a, const KeyRangeRef& copyFrom) : begin(a, copyFrom.begin), end(a, copyFrom.end) {}
	bool operator==(const KeyRangeRef& r) const { return begin == r.begin && end == r.end; }
	bool operator!=(const KeyRangeRef& r) const { return begin != r.begin || end != r.end; }
	bool contains(const KeyRef& key) const { return begin <= key && key < end; }
	bool contains(const KeyRangeRef& keys) const { return begin <= keys.begin && keys.end <= end; }
	bool intersects(const KeyRangeRef& keys) const { return begin < keys.end && keys.begin < end; }
	bool intersects(const VectorRef<KeyRangeRef>& keysVec) const {
		for (const auto& keys : keysVec) {
			if (intersects(keys)) {
				return true;
			}
		}
		return false;
	}
	bool empty() const { return begin == end; }
	bool singleKeyRange() const { return equalsKeyAfter(begin, end); }

	Standalone<KeyRangeRef> withPrefix(const StringRef& prefix) const {
		return KeyRangeRef(begin.withPrefix(prefix), end.withPrefix(prefix));
	}

	KeyRangeRef withPrefix(const StringRef& prefix, Arena& arena) const {
		return KeyRangeRef(begin.withPrefix(prefix, arena), end.withPrefix(prefix, arena));
	}

	KeyRangeRef removePrefix(const StringRef& prefix) const {
		return KeyRangeRef(begin.removePrefix(prefix), end.removePrefix(prefix));
	}

	const KeyRangeRef& operator=(const KeyRangeRef& rhs) {
		const_cast<KeyRef&>(begin) = rhs.begin;
		const_cast<KeyRef&>(end) = rhs.end;
		return *this;
	}

	int expectedSize() const { return begin.expectedSize() + end.expectedSize(); }

	template <class Ar>
	force_inline void serialize(Ar& ar) {
		if (!ar.isDeserializing && equalsKeyAfter(begin, end)) {
			StringRef empty;
			serializer(ar, const_cast<KeyRef&>(end), empty);
		} else {
			serializer(ar, const_cast<KeyRef&>(begin), const_cast<KeyRef&>(end));
		}
		if (ar.isDeserializing && end == StringRef() && begin != StringRef()) {
			ASSERT(begin[begin.size() - 1] == '\x00');
			const_cast<KeyRef&>(end) = begin;
			const_cast<KeyRef&>(begin) = end.substr(0, end.size() - 1);
		}

		if (begin > end) {
			TraceEvent("InvertedRange").detail("Begin", begin).detail("End", end);
			throw inverted_range();
		};
	}

	struct ArbitraryOrder {
		bool operator()(KeyRangeRef const& a, KeyRangeRef const& b) const {
			if (a.begin < b.begin)
				return true;
			if (a.begin > b.begin)
				return false;
			return a.end < b.end;
		}
	};

	std::string toString() const { return "Begin:" + begin.printable() + "End:" + end.printable(); }
};

template <>
struct Traceable<KeyRangeRef> : std::true_type {
	static std::string toString(const KeyRangeRef& value) {
		auto begin = Traceable<StringRef>::toString(value.begin);
		auto end = Traceable<StringRef>::toString(value.end);
		std::string result;
		result.reserve(begin.size() + end.size() + 3);
		std::copy(begin.begin(), begin.end(), std::back_inserter(result));
		result.push_back(' ');
		result.push_back('-');
		result.push_back(' ');
		std::copy(end.begin(), end.end(), std::back_inserter(result));
		return result;
	}
};

inline KeyRangeRef operator&(const KeyRangeRef& lhs, const KeyRangeRef& rhs) {
	KeyRef b = std::max(lhs.begin, rhs.begin), e = std::min(lhs.end, rhs.end);
	if (e < b)
		return KeyRangeRef();
	return KeyRangeRef(b, e);
}

struct KeyValueRef {
	KeyRef key;
	ValueRef value;
	KeyValueRef() {}
	KeyValueRef(const KeyRef& key, const ValueRef& value) : key(key), value(value) {}
	KeyValueRef(Arena& a, const KeyValueRef& copyFrom) : key(a, copyFrom.key), value(a, copyFrom.value) {}
	bool operator==(const KeyValueRef& r) const { return key == r.key && value == r.value; }
	bool operator!=(const KeyValueRef& r) const { return key != r.key || value != r.value; }

	int expectedSize() const { return key.expectedSize() + value.expectedSize(); }

	template <class Ar>
	force_inline void serialize(Ar& ar) {
		serializer(ar, key, value);
	}

	struct OrderByKey {
		bool operator()(KeyValueRef const& a, KeyValueRef const& b) const { return a.key < b.key; }
		template <class T>
		bool operator()(T const& a, KeyValueRef const& b) const {
			return a < b.key;
		}
		template <class T>
		bool operator()(KeyValueRef const& a, T const& b) const {
			return a.key < b;
		}
	};

	struct OrderByKeyBack {
		bool operator()(KeyValueRef const& a, KeyValueRef const& b) const { return a.key > b.key; }
		template <class T>
		bool operator()(T const& a, KeyValueRef const& b) const {
			return a > b.key;
		}
		template <class T>
		bool operator()(KeyValueRef const& a, T const& b) const {
			return a.key > b;
		}
	};
};

template <>
struct string_serialized_traits<KeyValueRef> : std::true_type {
	int32_t getSize(const KeyValueRef& item) const {
		return 2 * sizeof(uint32_t) + item.key.size() + item.value.size();
	}

	uint32_t save(uint8_t* out, const KeyValueRef& item) const {
		auto begin = out;
		uint32_t sz = item.key.size();
		*reinterpret_cast<decltype(sz)*>(out) = sz;
		out += sizeof(sz);
		memcpy(out, item.key.begin(), sz);
		out += sz;
		sz = item.value.size();
		*reinterpret_cast<decltype(sz)*>(out) = sz;
		out += sizeof(sz);
		memcpy(out, item.value.begin(), sz);
		out += sz;
		return out - begin;
	}

	template <class Context>
	uint32_t load(const uint8_t* data, KeyValueRef& t, Context& context) {
		auto begin = data;
		uint32_t sz;
		memcpy(&sz, data, sizeof(sz));
		data += sizeof(sz);
		t.key = StringRef(context.tryReadZeroCopy(data, sz), sz);
		data += sz;
		memcpy(&sz, data, sizeof(sz));
		data += sizeof(sz);
		t.value = StringRef(context.tryReadZeroCopy(data, sz), sz);
		data += sz;
		return data - begin;
	}
};

template <>
struct Traceable<KeyValueRef> : std::true_type {
	static std::string toString(const KeyValueRef& value) {
		return Traceable<KeyRef>::toString(value.key) + format(":%d", value.value.size());
	}
};

using Key = Standalone<KeyRef>;
using Value = Standalone<ValueRef>;
using KeyRange = Standalone<KeyRangeRef>;
using KeyValue = Standalone<KeyValueRef>;
using KeySelector = Standalone<struct KeySelectorRef>;
using RangeResult = Standalone<struct RangeResultRef>;
using MappedRangeResult = Standalone<struct MappedRangeResultRef>;

enum { invalidVersion = -1, latestVersion = -2, MAX_VERSION = std::numeric_limits<int64_t>::max() };

inline Key keyAfter(const KeyRef& key) {
	if (key == LiteralStringRef("\xff\xff"))
		return key;

	Standalone<StringRef> r;
	uint8_t* s = new (r.arena()) uint8_t[key.size() + 1];
	if (key.size() > 0) {
		memcpy(s, key.begin(), key.size());
	}
	s[key.size()] = 0;
	((StringRef&)r) = StringRef(s, key.size() + 1);
	return r;
}
inline KeyRef keyAfter(const KeyRef& key, Arena& arena) {
	if (key == LiteralStringRef("\xff\xff"))
		return key;
	uint8_t* t = new (arena) uint8_t[key.size() + 1];
	memcpy(t, key.begin(), key.size());
	t[key.size()] = 0;
	return KeyRef(t, key.size() + 1);
}
inline KeyRange singleKeyRange(const KeyRef& a) {
	return KeyRangeRef(a, keyAfter(a));
}
inline KeyRangeRef singleKeyRange(KeyRef const& key, Arena& arena) {
	uint8_t* t = new (arena) uint8_t[key.size() + 1];
	memcpy(t, key.begin(), key.size());
	t[key.size()] = 0;
	return KeyRangeRef(KeyRef(t, key.size()), KeyRef(t, key.size() + 1));
}
inline KeyRange prefixRange(KeyRef prefix) {
	Standalone<KeyRangeRef> range;
	KeyRef start = KeyRef(range.arena(), prefix);
	KeyRef end = strinc(prefix, range.arena());
	range.contents() = KeyRangeRef(start, end);
	return range;
}

// Returns (one of) the shortest key(s) either contained in keys or equal to keys.end,
// assuming its length is no more than CLIENT_KNOBS->SPLIT_KEY_SIZE_LIMIT. If the length of
// the shortest key exceeds that limit, then the end key is returned.
// The returned reference is valid as long as keys is valid.
KeyRef keyBetween(const KeyRangeRef& keys);

struct KeySelectorRef {
private:
	KeyRef key; // Find the last item less than key

public:
	bool orEqual; // (or equal to key, if this is true)
	int offset; // and then move forward this many items (or backward if negative)
	KeySelectorRef() : orEqual(false), offset(0) {}
	KeySelectorRef(const KeyRef& key, bool orEqual, int offset) : orEqual(orEqual), offset(offset) { setKey(key); }

	KeySelectorRef(Arena& arena, const KeySelectorRef& copyFrom)
	  : key(arena, copyFrom.key), orEqual(copyFrom.orEqual), offset(copyFrom.offset) {}
	int expectedSize() const { return key.expectedSize(); }

	void removeOrEqual(Arena& arena) {
		if (orEqual) {
			setKey(keyAfter(key, arena));
			orEqual = false;
		}
	}

	KeyRef getKey() const { return key; }

	void setKey(KeyRef const& key);
	void setKeyUnlimited(KeyRef const& key);

	std::string toString() const;

	bool isBackward() const {
		return !orEqual && offset <= 0;
	} // True if the resolution of the KeySelector depends only on keys less than key
	bool isFirstGreaterOrEqual() const { return !orEqual && offset == 1; }
	bool isFirstGreaterThan() const { return orEqual && offset == 1; }
	bool isLastLessOrEqual() const { return orEqual && offset == 0; }

	// True iff, regardless of the contents of the database, lhs must resolve to a key > rhs
	bool isDefinitelyGreater(KeyRef const& k) { return offset >= 1 && (isFirstGreaterOrEqual() ? key > k : key >= k); }
	// True iff, regardless of the contents of the database, lhs must resolve to a key < rhs
	bool isDefinitelyLess(KeyRef const& k) { return offset <= 0 && (isLastLessOrEqual() ? key < k : key <= k); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, orEqual, offset);
	}
};

inline bool operator==(const KeySelectorRef& lhs, const KeySelectorRef& rhs) {
	return lhs.getKey() == rhs.getKey() && lhs.orEqual == rhs.orEqual && lhs.offset == rhs.offset;
}
inline KeySelectorRef lastLessThan(const KeyRef& k) {
	return KeySelectorRef(k, false, 0);
}
inline KeySelectorRef lastLessOrEqual(const KeyRef& k) {
	return KeySelectorRef(k, true, 0);
}
inline KeySelectorRef firstGreaterThan(const KeyRef& k) {
	return KeySelectorRef(k, true, +1);
}
inline KeySelectorRef firstGreaterOrEqual(const KeyRef& k) {
	return KeySelectorRef(k, false, +1);
}
inline KeySelectorRef operator+(const KeySelectorRef& s, int off) {
	return KeySelectorRef(s.getKey(), s.orEqual, s.offset + off);
}
inline KeySelectorRef operator-(const KeySelectorRef& s, int off) {
	return KeySelectorRef(s.getKey(), s.orEqual, s.offset - off);
}
inline bool selectorInRange(KeySelectorRef const& sel, KeyRangeRef const& range) {
	// Returns true if the given range suffices to at least begin to resolve the given KeySelectorRef
	return sel.getKey() >= range.begin && (sel.isBackward() ? sel.getKey() <= range.end : sel.getKey() < range.end);
}

template <>
struct Traceable<KeySelectorRef> : std::true_type {
	static std::string toString(const KeySelectorRef& value) { return value.toString(); }
};

template <class Val>
struct KeyRangeWith : KeyRange {
	Val value;
	KeyRangeWith() {}
	KeyRangeWith(const KeyRangeRef& range, const Val& value) : KeyRange(range), value(value) {}
	bool operator==(const KeyRangeWith& r) const { return KeyRangeRef::operator==(r) && value == r.value; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ((KeyRange&)*this), value);
	}
};
template <class Val>
KeyRangeWith<Val> keyRangeWith(const KeyRangeRef& range, const Val& value) {
	return KeyRangeWith<Val>(range, value);
}

struct MappedKeyValueRef;

struct GetRangeLimits {
	enum { ROW_LIMIT_UNLIMITED = -1, BYTE_LIMIT_UNLIMITED = -1 };

	int rows;
	int minRows;
	int bytes;

	GetRangeLimits() : rows(ROW_LIMIT_UNLIMITED), minRows(1), bytes(BYTE_LIMIT_UNLIMITED) {}
	explicit GetRangeLimits(int rowLimit) : rows(rowLimit), minRows(1), bytes(BYTE_LIMIT_UNLIMITED) {}
	GetRangeLimits(int rowLimit, int byteLimit) : rows(rowLimit), minRows(1), bytes(byteLimit) {}

	void decrement(VectorRef<KeyValueRef> const& data);
	void decrement(KeyValueRef const& data);
	void decrement(VectorRef<MappedKeyValueRef> const& data);
	void decrement(MappedKeyValueRef const& data);

	// True if either the row or byte limit has been reached
	bool isReached();

	// True if data would cause the row or byte limit to be reached
	bool reachedBy(VectorRef<KeyValueRef> const& data);

	bool hasByteLimit();
	bool hasRowLimit();

	bool hasSatisfiedMinRows();
	bool isValid() const {
		return (rows >= 0 || rows == ROW_LIMIT_UNLIMITED) && (bytes >= 0 || bytes == BYTE_LIMIT_UNLIMITED) &&
		       minRows >= 0 && (minRows <= rows || rows == ROW_LIMIT_UNLIMITED);
	}
};

struct RangeResultRef : VectorRef<KeyValueRef> {
	bool more; // True if (but not necessarily only if) values remain in the *key* range requested (possibly beyond the
	           // limits requested) False implies that no such values remain
	Optional<KeyRef> readThrough; // Only present when 'more' is true. When present, this value represent the end (or
	                              // beginning if reverse) of the range which was read to produce these results. This is
	                              // guaranteed to be less than the requested range.
	bool readToBegin;
	bool readThroughEnd;

	RangeResultRef() : more(false), readToBegin(false), readThroughEnd(false) {}
	RangeResultRef(Arena& p, const RangeResultRef& toCopy)
	  : VectorRef<KeyValueRef>(p, toCopy), more(toCopy.more),
	    readThrough(toCopy.readThrough.present() ? KeyRef(p, toCopy.readThrough.get()) : Optional<KeyRef>()),
	    readToBegin(toCopy.readToBegin), readThroughEnd(toCopy.readThroughEnd) {}
	RangeResultRef(const VectorRef<KeyValueRef>& value, bool more, Optional<KeyRef> readThrough = Optional<KeyRef>())
	  : VectorRef<KeyValueRef>(value), more(more), readThrough(readThrough), readToBegin(false), readThroughEnd(false) {
	}
	RangeResultRef(bool readToBegin, bool readThroughEnd)
	  : more(false), readToBegin(readToBegin), readThroughEnd(readThroughEnd) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ((VectorRef<KeyValueRef>&)*this), more, readThrough, readToBegin, readThroughEnd);
	}

	int logicalSize() const {
		return VectorRef<KeyValueRef>::expectedSize() - VectorRef<KeyValueRef>::size() * sizeof(KeyValueRef);
	}

	std::string toString() const {
		return "more:" + std::to_string(more) +
		       " readThrough:" + (readThrough.present() ? readThrough.get().toString() : "[unset]") +
		       " readToBegin:" + std::to_string(readToBegin) + " readThroughEnd:" + std::to_string(readThroughEnd);
	}
};

template <>
struct Traceable<RangeResultRef> : std::true_type {
	static std::string toString(const RangeResultRef& value) {
		return Traceable<VectorRef<KeyValueRef>>::toString(value);
	}
};

// Similar to KeyValueRef, but result can be empty.
struct GetValueReqAndResultRef {
	KeyRef key;
	Optional<ValueRef> result;

	GetValueReqAndResultRef() {}
	GetValueReqAndResultRef(Arena& a, const GetValueReqAndResultRef& copyFrom)
	  : key(a, copyFrom.key), result(a, copyFrom.result) {}

	bool operator==(const GetValueReqAndResultRef& rhs) const { return key == rhs.key && result == rhs.result; }
	bool operator!=(const GetValueReqAndResultRef& rhs) const { return !(rhs == *this); }
	int expectedSize() const { return key.expectedSize() + result.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, result);
	}
};

struct GetRangeReqAndResultRef {
	KeySelectorRef begin, end;
	RangeResultRef result;

	GetRangeReqAndResultRef() {}
	//	KeyValueRef(const KeyRef& key, const ValueRef& value) : key(key), value(value) {}
	GetRangeReqAndResultRef(Arena& a, const GetRangeReqAndResultRef& copyFrom)
	  : begin(a, copyFrom.begin), end(a, copyFrom.end), result(a, copyFrom.result) {}

	bool operator==(const GetRangeReqAndResultRef& rhs) const {
		return begin == rhs.begin && end == rhs.end && result == rhs.result;
	}
	bool operator!=(const GetRangeReqAndResultRef& rhs) const { return !(rhs == *this); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, begin, end, result);
	}
};

using MappedReqAndResultRef = std::variant<GetValueReqAndResultRef, GetRangeReqAndResultRef>;

struct MappedKeyValueRef : KeyValueRef {
	// Save the original key value at the base (KeyValueRef).

	MappedReqAndResultRef reqAndResult;

	MappedKeyValueRef() = default;
	MappedKeyValueRef(Arena& a, const MappedKeyValueRef& copyFrom) : KeyValueRef(a, copyFrom) {
		const auto& reqAndResultCopyFrom = copyFrom.reqAndResult;
		if (std::holds_alternative<GetValueReqAndResultRef>(reqAndResultCopyFrom)) {
			auto getValue = std::get<GetValueReqAndResultRef>(reqAndResultCopyFrom);
			reqAndResult = GetValueReqAndResultRef(a, getValue);
		} else if (std::holds_alternative<GetRangeReqAndResultRef>(reqAndResultCopyFrom)) {
			auto getRange = std::get<GetRangeReqAndResultRef>(reqAndResultCopyFrom);
			reqAndResult = GetRangeReqAndResultRef(a, getRange);
		} else {
			throw internal_error();
		}
	}

	bool operator==(const MappedKeyValueRef& rhs) const {
		return static_cast<const KeyValueRef&>(*this) == static_cast<const KeyValueRef&>(rhs) &&
		       reqAndResult == rhs.reqAndResult;
	}
	bool operator!=(const MappedKeyValueRef& rhs) const { return !(rhs == *this); }

	// It relies on the base to provide the expectedSize. TODO: Consider add the underlying request and key values into
	// expected size?
	//	int expectedSize() const { return ((KeyValueRef*)this)->expectedSisze() + reqA }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ((KeyValueRef&)*this), reqAndResult);
	}
};

struct MappedRangeResultRef : VectorRef<MappedKeyValueRef> {
	// Additional information on range result. See comments on RangeResultRef.
	bool more;
	Optional<KeyRef> readThrough;
	bool readToBegin;
	bool readThroughEnd;

	MappedRangeResultRef() : more(false), readToBegin(false), readThroughEnd(false) {}
	MappedRangeResultRef(Arena& p, const MappedRangeResultRef& toCopy)
	  : VectorRef<MappedKeyValueRef>(p, toCopy), more(toCopy.more),
	    readThrough(toCopy.readThrough.present() ? KeyRef(p, toCopy.readThrough.get()) : Optional<KeyRef>()),
	    readToBegin(toCopy.readToBegin), readThroughEnd(toCopy.readThroughEnd) {}
	MappedRangeResultRef(const VectorRef<MappedKeyValueRef>& value,
	                     bool more,
	                     Optional<KeyRef> readThrough = Optional<KeyRef>())
	  : VectorRef<MappedKeyValueRef>(value), more(more), readThrough(readThrough), readToBegin(false),
	    readThroughEnd(false) {}
	MappedRangeResultRef(bool readToBegin, bool readThroughEnd)
	  : more(false), readToBegin(readToBegin), readThroughEnd(readThroughEnd) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ((VectorRef<MappedKeyValueRef>&)*this), more, readThrough, readToBegin, readThroughEnd);
	}

	std::string toString() const {
		return "more:" + std::to_string(more) +
		       " readThrough:" + (readThrough.present() ? readThrough.get().toString() : "[unset]") +
		       " readToBegin:" + std::to_string(readToBegin) + " readThroughEnd:" + std::to_string(readThroughEnd);
	}
};

struct KeyValueStoreType {
	constexpr static FileIdentifier file_identifier = 6560359;
	// These enumerated values are stored in the database configuration, so should NEVER be changed.
	// Only add new ones just before END.
	// SS storeType is END before the storageServerInterface is initialized.
	enum StoreType { SSD_BTREE_V1, MEMORY, SSD_BTREE_V2, SSD_REDWOOD_V1, MEMORY_RADIXTREE, SSD_ROCKSDB_V1, END };

	KeyValueStoreType() : type(END) {}
	KeyValueStoreType(StoreType type) : type(type) {
		if ((uint32_t)type > END)
			this->type = END;
	}
	operator StoreType() const { return StoreType(type); }
	StoreType storeType() const { return StoreType(type); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, type);
	}

	std::string toString() const {
		switch (type) {
		case SSD_BTREE_V1:
			return "ssd-1";
		case SSD_BTREE_V2:
			return "ssd-2";
		case SSD_REDWOOD_V1:
			return "ssd-redwood-1-experimental";
		case SSD_ROCKSDB_V1:
			return "ssd-rocksdb-v1";
		case MEMORY:
			return "memory";
		case MEMORY_RADIXTREE:
			return "memory-radixtree-beta";
		default:
			return "unknown";
		}
	}

private:
	uint32_t type;
};

template <>
struct Traceable<KeyValueStoreType> : std::true_type {
	static std::string toString(KeyValueStoreType const& value) { return value.toString(); }
};

struct TLogVersion {
	enum Version {
		UNSET = 0,
		// Everything between BEGIN and END should be densely packed, so that we
		// can iterate over them easily.
		// V3 was the introduction of spill by reference;
		// V4 changed how data gets written to satellite TLogs so that we can peek from them;
		// V5 merged reference and value spilling
		// V6 added span context to list of serialized mutations sent from proxy to tlogs
		// V7 use xxhash3 for TLog checksum
		// V1 = 1,  // 4.6 is dispatched to via 6.0
		V2 = 2, // 6.0
		V3 = 3, // 6.1
		V4 = 4, // 6.2
		V5 = 5, // 6.3
		V6 = 6, // 7.0
		V7 = 7, // 7.2
		MIN_SUPPORTED = V2,
		MAX_SUPPORTED = V7,
		MIN_RECRUITABLE = V6,
		DEFAULT = V6,
	} version;

	TLogVersion() : version(UNSET) {}
	TLogVersion(Version v) : version(v) {}

	operator Version() const { return version; }

	template <class Ar>
	void serialize(Ar& ar) {
		uint32_t v = (uint32_t)version;
		serializer(ar, v);
		version = (Version)v;
	}

	static ErrorOr<TLogVersion> FromStringRef(StringRef s) {
		if (s == LiteralStringRef("2"))
			return V2;
		if (s == LiteralStringRef("3"))
			return V3;
		if (s == LiteralStringRef("4"))
			return V4;
		if (s == LiteralStringRef("5"))
			return V5;
		if (s == LiteralStringRef("6"))
			return V6;
		if (s == LiteralStringRef("7"))
			return V7;
		return default_error_or();
	}
};

template <>
struct Traceable<TLogVersion> : std::true_type {
	static std::string toString(TLogVersion const& value) { return Traceable<Version>::toString(value.version); }
};

struct TLogSpillType {
	// These enumerated values are stored in the database configuration, so can NEVER be changed.  Only add new ones
	// just before END.
	enum SpillType {
		UNSET = 0,
		DEFAULT = 2,
		VALUE = 1,
		REFERENCE = 2,
		END = 3,
	};

	TLogSpillType() : type(DEFAULT) {}
	TLogSpillType(SpillType type) : type(type) {
		if ((uint32_t)type >= END) {
			this->type = UNSET;
		}
	}
	operator SpillType() const { return SpillType(type); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, type);
	}

	std::string toString() const {
		switch (type) {
		case VALUE:
			return "value";
		case REFERENCE:
			return "reference";
		case UNSET:
			return "unset";
		default:
			ASSERT(false);
		}
		return "";
	}

	static ErrorOr<TLogSpillType> FromStringRef(StringRef s) {
		if (s == LiteralStringRef("1"))
			return VALUE;
		if (s == LiteralStringRef("2"))
			return REFERENCE;
		return default_error_or();
	}

	uint32_t type;
};

// Contains the amount of free and total space for a storage server, in bytes
struct StorageBytes {
	// Free space on the filesystem
	int64_t free;
	// Total space on the filesystem
	int64_t total;
	// Used by *this* store, not total - free
	int64_t used;
	// Amount of space available for use by the store, which includes free space on the filesystem
	// and internal free space within the store data that is immediately reusable.
	int64_t available;
	// Amount of space that could eventually be available for use after garbage collection
	int64_t temp;

	StorageBytes() {}
	StorageBytes(int64_t free, int64_t total, int64_t used, int64_t available, int64_t temp = 0)
	  : free(free), total(total), used(used), available(available), temp(temp) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, free, total, used, available);
	}

	std::string toString() const {
		return format("{%.2f MB total, %.2f MB free, %.2f MB available, %.2f MB used, %.2f MB temp}",
		              total / 1e6,
		              free / 1e6,
		              available / 1e6,
		              used / 1e6,
		              temp / 1e6);
	}

	void toTraceEvent(TraceEvent& e) const {
		e.detail("StorageBytesUsed", used)
		    .detail("StorageBytesTemp", temp)
		    .detail("StorageBytesTotal", total)
		    .detail("StorageBytesFree", free)
		    .detail("StorageBytesAvailable", available);
	}
};
struct LogMessageVersion {
	// Each message pushed into the log system has a unique, totally ordered LogMessageVersion
	// See ILogSystem::push() for how these are assigned
	Version version;
	uint32_t sub;

	void reset(Version v) {
		version = v;
		sub = 0;
	}

	bool operator<(LogMessageVersion const& r) const {
		if (version < r.version)
			return true;
		if (r.version < version)
			return false;
		return sub < r.sub;
	}
	bool operator>(LogMessageVersion const& r) const { return r < *this; }
	bool operator<=(LogMessageVersion const& r) const { return !(*this > r); }
	bool operator>=(LogMessageVersion const& r) const { return !(*this < r); }

	bool operator==(LogMessageVersion const& r) const { return version == r.version && sub == r.sub; }

	std::string toString() const { return format("%lld.%d", version, sub); }

	LogMessageVersion(Version version, uint32_t sub) : version(version), sub(sub) {}
	explicit LogMessageVersion(Version version) : version(version), sub(0) {}
	LogMessageVersion() : version(0), sub(0) {}
	bool empty() const { return (version == 0) && (sub == 0); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, sub);
	}
};

struct AddressExclusion {
	IPAddress ip;
	int port;

	AddressExclusion() : ip(0), port(0) {}
	explicit AddressExclusion(const IPAddress& ip) : ip(ip), port(0) {}
	explicit AddressExclusion(const IPAddress& ip, int port) : ip(ip), port(port) {}

	bool operator<(AddressExclusion const& r) const {
		if (ip != r.ip)
			return ip < r.ip;
		return port < r.port;
	}
	bool operator==(AddressExclusion const& r) const { return ip == r.ip && port == r.port; }

	bool isWholeMachine() const { return port == 0; }
	bool isValid() const { return ip.isValid() || port != 0; }

	bool excludes(NetworkAddress const& addr) const {
		if (isWholeMachine())
			return ip == addr.ip;
		return ip == addr.ip && port == addr.port;
	}

	// This is for debugging and IS NOT to be used for serialization to persistant state
	std::string toString() const {
		if (!isWholeMachine())
			return formatIpPort(ip, port);
		return ip.toString();
	}

	static AddressExclusion parse(StringRef const&);

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ip, port);
	}
};

inline bool addressExcluded(std::set<AddressExclusion> const& exclusions, NetworkAddress const& addr) {
	return exclusions.count(AddressExclusion(addr.ip, addr.port)) || exclusions.count(AddressExclusion(addr.ip));
}

struct ClusterControllerPriorityInfo {
	enum DCFitness {
		FitnessPrimary,
		FitnessRemote,
		FitnessPreferred,
		FitnessUnknown,
		FitnessNotPreferred,
		FitnessBad
	}; // cannot be larger than 7 because of leader election mask

	static DCFitness calculateDCFitness(Optional<Key> const& dcId, std::vector<Optional<Key>> const& dcPriority) {
		if (!dcPriority.size()) {
			return FitnessUnknown;
		} else if (dcPriority.size() == 1) {
			if (dcId == dcPriority[0]) {
				return FitnessPreferred;
			} else {
				return FitnessNotPreferred;
			}
		} else {
			if (dcId == dcPriority[0]) {
				return FitnessPrimary;
			} else if (dcId == dcPriority[1]) {
				return FitnessRemote;
			} else {
				return FitnessBad;
			}
		}
	}

	uint8_t processClassFitness;
	bool isExcluded;
	uint8_t dcFitness;

	bool operator==(ClusterControllerPriorityInfo const& r) const {
		return processClassFitness == r.processClassFitness && isExcluded == r.isExcluded && dcFitness == r.dcFitness;
	}
	bool operator!=(ClusterControllerPriorityInfo const& r) const { return !(*this == r); }
	ClusterControllerPriorityInfo()
	  : ClusterControllerPriorityInfo(/*ProcessClass::UnsetFit*/ 2,
	                                  false,
	                                  ClusterControllerPriorityInfo::FitnessUnknown) {}
	ClusterControllerPriorityInfo(uint8_t processClassFitness, bool isExcluded, uint8_t dcFitness)
	  : processClassFitness(processClassFitness), isExcluded(isExcluded), dcFitness(dcFitness) {}

	// To change this serialization, ProtocolVersion::ClusterControllerPriorityInfo must be updated, and downgrades need
	// to be considered
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, processClassFitness, isExcluded, dcFitness);
	}
};

class Database;

struct HealthMetrics {
	struct StorageStats {
		int64_t storageQueue;
		int64_t storageDurabilityLag;
		double diskUsage;
		double cpuUsage;

		bool operator==(StorageStats const& r) const {
			return ((storageQueue == r.storageQueue) && (storageDurabilityLag == r.storageDurabilityLag) &&
			        (diskUsage == r.diskUsage) && (cpuUsage == r.cpuUsage));
		}

		template <class Ar>
		void serialize(Ar& ar) {
			serializer(ar, storageQueue, storageDurabilityLag, diskUsage, cpuUsage);
		}
	};

	int64_t worstStorageQueue;
	int64_t limitingStorageQueue;
	int64_t worstStorageDurabilityLag;
	int64_t limitingStorageDurabilityLag;
	int64_t worstTLogQueue;
	double tpsLimit;
	bool batchLimited;
	std::map<UID, StorageStats> storageStats;
	std::map<UID, int64_t> tLogQueue;

	HealthMetrics()
	  : worstStorageQueue(0), limitingStorageQueue(0), worstStorageDurabilityLag(0), limitingStorageDurabilityLag(0),
	    worstTLogQueue(0), tpsLimit(0.0), batchLimited(false) {}

	void update(const HealthMetrics& hm, bool detailedInput, bool detailedOutput) {
		worstStorageQueue = hm.worstStorageQueue;
		limitingStorageQueue = hm.limitingStorageQueue;
		worstStorageDurabilityLag = hm.worstStorageDurabilityLag;
		limitingStorageDurabilityLag = hm.limitingStorageDurabilityLag;
		worstTLogQueue = hm.worstTLogQueue;
		tpsLimit = hm.tpsLimit;
		batchLimited = hm.batchLimited;

		if (!detailedOutput) {
			storageStats.clear();
			tLogQueue.clear();
		} else if (detailedInput) {
			storageStats = hm.storageStats;
			tLogQueue = hm.tLogQueue;
		}
	}

	bool operator==(HealthMetrics const& r) const {
		return (worstStorageQueue == r.worstStorageQueue && limitingStorageQueue == r.limitingStorageQueue &&
		        worstStorageDurabilityLag == r.worstStorageDurabilityLag &&
		        limitingStorageDurabilityLag == r.limitingStorageDurabilityLag && worstTLogQueue == r.worstTLogQueue &&
		        storageStats == r.storageStats && tLogQueue == r.tLogQueue && batchLimited == r.batchLimited);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           worstStorageQueue,
		           worstStorageDurabilityLag,
		           worstTLogQueue,
		           tpsLimit,
		           batchLimited,
		           storageStats,
		           tLogQueue,
		           limitingStorageQueue,
		           limitingStorageDurabilityLag);
	}
};

struct DDMetricsRef {
	int64_t shardBytes;
	KeyRef beginKey;

	DDMetricsRef() : shardBytes(0) {}
	DDMetricsRef(int64_t bytes, KeyRef begin) : shardBytes(bytes), beginKey(begin) {}
	DDMetricsRef(Arena& a, const DDMetricsRef& copyFrom)
	  : shardBytes(copyFrom.shardBytes), beginKey(a, copyFrom.beginKey) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, shardBytes, beginKey);
	}
};

struct WorkerBackupStatus {
	LogEpoch epoch;
	Version version;
	Tag tag;
	int32_t totalTags;

	WorkerBackupStatus() : epoch(0), version(invalidVersion) {}
	WorkerBackupStatus(LogEpoch e, Version v, Tag t, int32_t total) : epoch(e), version(v), tag(t), totalTags(total) {}

	// To change this serialization, ProtocolVersion::BackupProgressValue must be updated, and downgrades need to be
	// considered
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, epoch, version, tag, totalTags);
	}
};

enum class TransactionPriority : uint8_t { BATCH, DEFAULT, IMMEDIATE, MIN = BATCH, MAX = IMMEDIATE };

const std::array<TransactionPriority, (int)TransactionPriority::MAX + 1> allTransactionPriorities = {
	TransactionPriority::BATCH,
	TransactionPriority::DEFAULT,
	TransactionPriority::IMMEDIATE
};

inline const char* transactionPriorityToString(TransactionPriority priority, bool capitalize = true) {
	switch (priority) {
	case TransactionPriority::BATCH:
		return capitalize ? "Batch" : "batch";
	case TransactionPriority::DEFAULT:
		return capitalize ? "Default" : "default";
	case TransactionPriority::IMMEDIATE:
		return capitalize ? "Immediate" : "immediate";
	}

	ASSERT(false);
	throw internal_error();
}

struct StorageMigrationType {
	// These enumerated values are stored in the database configuration, so can NEVER be changed.  Only add new ones
	// just before END.
	enum MigrationType { DEFAULT = 1, UNSET = 0, DISABLED = 1, AGGRESSIVE = 2, GRADUAL = 3, END = 4 };

	StorageMigrationType() : type(UNSET) {}
	StorageMigrationType(MigrationType type) : type(type) {
		if ((uint32_t)type >= END) {
			this->type = UNSET;
		}
	}
	operator MigrationType() const { return MigrationType(type); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, type);
	}

	std::string toString() const {
		switch (type) {
		case DISABLED:
			return "disabled";
		case AGGRESSIVE:
			return "aggressive";
		case GRADUAL:
			return "gradual";
		case UNSET:
			return "unset";
		default:
			ASSERT(false);
		}
		return "";
	}

	uint32_t type;
};

struct TenantMode {
	// These enumerated values are stored in the database configuration, so can NEVER be changed.  Only add new ones
	// just before END.
	// Note: OPTIONAL_TENANT is not named OPTIONAL because of a collision with a Windows macro.
	enum Mode { DISABLED = 0, OPTIONAL_TENANT = 1, REQUIRED = 2, END = 3 };

	TenantMode() : mode(DISABLED) {}
	TenantMode(Mode mode) : mode(mode) {
		if ((uint32_t)mode >= END) {
			this->mode = DISABLED;
		}
	}
	operator Mode() const { return Mode(mode); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, mode);
	}

	std::string toString() const {
		switch (mode) {
		case DISABLED:
			return "disabled";
		case OPTIONAL_TENANT:
			return "optional_experimental";
		case REQUIRED:
			return "required_experimental";
		default:
			ASSERT(false);
		}
		return "";
	}

	uint32_t mode;
};

inline bool isValidPerpetualStorageWiggleLocality(std::string locality) {
	int pos = locality.find(':');
	// locality should be either 0 or in the format '<non_empty_string>:<non_empty_string>'
	return ((pos > 0 && pos < locality.size() - 1) || locality == "0");
}

// matches what's in fdb_c.h
struct ReadBlobGranuleContext {
	// User context to pass along to functions
	void* userContext;

	// Returns a unique id for the load. Asynchronous to support queueing multiple in parallel.
	int64_t (*start_load_f)(const char* filename,
	                        int filenameLength,
	                        int64_t offset,
	                        int64_t length,
	                        int64_t fullFileLength,
	                        void* context);

	// Returns data for the load. Pass the loadId returned by start_load_f
	uint8_t* (*get_load_f)(int64_t loadId, void* context);

	// Frees data from load. Pass the loadId returned by start_load_f
	void (*free_load_f)(int64_t loadId, void* context);

	// Set this to true for testing if you don't want to read the granule files,
	// just do the request to the blob workers
	bool debugNoMaterialize;

	// number of granules to load in parallel (default 1)
	int granuleParallelism = 1;
};

// Store metadata associated with each storage server. Now it only contains data be used in perpetual storage wiggle.
struct StorageMetadataType {
	constexpr static FileIdentifier file_identifier = 732123;
	// when the SS is initialized
	uint64_t createdTime; // comes from currentTime()
	StorageMetadataType() : createdTime(0) {}
	StorageMetadataType(uint64_t t) : createdTime(t) {}

	static uint64_t currentTime() { return g_network->timer() * 1e9; }

	// To change this serialization, ProtocolVersion::StorageMetadata must be updated, and downgrades need
	// to be considered
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, createdTime);
	}
};

// store metadata of wiggle action
struct StorageWiggleValue {
	constexpr static FileIdentifier file_identifier = 732124;
	UID id; // storage id

	StorageWiggleValue(UID id = UID(0, 0)) : id(id) {}

	// To change this serialization, ProtocolVersion::PerpetualWiggleMetadata must be updated, and downgrades need
	// to be considered
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id);
	}
};
#endif
