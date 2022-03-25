/*
 * FDBLoanerTypes.h
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

#ifndef FDB_FLOW_LOANER_TYPES_H
#define FDB_FLOW_LOANER_TYPES_H

namespace FDB {
typedef StringRef KeyRef;
typedef StringRef ValueRef;

typedef int64_t Version;

typedef Standalone<KeyRef> Key;
typedef Standalone<ValueRef> Value;

inline Key keyAfter(const KeyRef& key) {
	if (key == LiteralStringRef("\xff\xff"))
		return key;

	Standalone<StringRef> r;
	uint8_t* s = new (r.arena()) uint8_t[key.size() + 1];
	memcpy(s, key.begin(), key.size());
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

struct KeySelectorRef {
	KeyRef key; // Find the last item less than key
	bool orEqual; // (or equal to key, if this is true)
	int offset; // and then move forward this many items (or backward if negative)
	KeySelectorRef() {}
	KeySelectorRef(const KeyRef& key, bool orEqual, int offset) : key(key), orEqual(orEqual), offset(offset) {}

	KeySelectorRef(Arena& arena, const KeySelectorRef& copyFrom)
	  : key(arena, copyFrom.key), orEqual(copyFrom.orEqual), offset(copyFrom.offset) {}
	int expectedSize() const { return key.expectedSize(); }

	// std::string toString() const {
	// 	if (offset > 0) {
	// 		if (orEqual) return format("firstGreaterThan(%s)%+d", printable(key).c_str(), offset-1);
	// 		else return format("firstGreaterOrEqual(%s)%+d", printable(key).c_str(), offset-1);
	// 	} else {
	// 		if (orEqual) return format("lastLessOrEqual(%s)%+d", printable(key).c_str(), offset);
	// 		else return format("lastLessThan(%s)%+d", printable(key).c_str(), offset);
	// 	}
	// }

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
	return lhs.key == rhs.key && lhs.orEqual == rhs.orEqual && lhs.offset == rhs.offset;
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
	return KeySelectorRef(s.key, s.orEqual, s.offset + off);
}
inline KeySelectorRef operator-(const KeySelectorRef& s, int off) {
	return KeySelectorRef(s.key, s.orEqual, s.offset - off);
}

typedef Standalone<KeySelectorRef> KeySelector;

struct KeyValueRef {
	KeyRef key;
	ValueRef value;
	KeyValueRef() {}
	KeyValueRef(const KeyRef& key, const ValueRef& value) : key(key), value(value) {}
	KeyValueRef(Arena& a, const KeyValueRef& copyFrom) : key(a, copyFrom.key), value(a, copyFrom.value) {}
	bool operator==(const KeyValueRef& r) const { return key == r.key && value == r.value; }

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

typedef Standalone<KeyValueRef> KeyValue;

struct RangeResultRef : VectorRef<KeyValueRef> {
	bool more; // True if (but not necessarily only if) values remain in the *key* range requested (possibly beyond the
	           // limits requested)
	// False implies that no such values remain
	Optional<KeyRef> readThrough; // Only present when 'more' is true. When present, this value represent the end (or
	                              // beginning if reverse) of the range
	// which was read to produce these results. This is guaranteed to be less than the requested range.
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
};

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

	// True if either the row or byte limit has been reached
	bool isReached();

	// True if data would cause the row or byte limit to be reached
	bool reachedBy(VectorRef<KeyValueRef> const& data);

	bool hasByteLimit();
	bool hasRowLimit();

	bool hasSatisfiedMinRows();
	bool isValid() {
		return (rows >= 0 || rows == ROW_LIMIT_UNLIMITED) && (bytes >= 0 || bytes == BYTE_LIMIT_UNLIMITED) &&
		       minRows >= 0 && (minRows <= rows || rows == ROW_LIMIT_UNLIMITED);
	}
};

struct KeyRangeRef {
	const KeyRef begin, end;
	KeyRangeRef() {}
	KeyRangeRef(const KeyRef& begin, const KeyRef& end) : begin(begin), end(end) {
		if (begin > end) {
			throw inverted_range();
		}
	}
	KeyRangeRef(Arena& a, const KeyRangeRef& copyFrom) : begin(a, copyFrom.begin), end(a, copyFrom.end) {}
	bool operator==(const KeyRangeRef& r) const { return begin == r.begin && end == r.end; }
	bool operator!=(const KeyRangeRef& r) const { return begin != r.begin || end != r.end; }
	bool contains(const KeyRef& key) const { return begin <= key && key < end; }
	bool contains(const KeyRangeRef& keys) const { return begin <= keys.begin && keys.end <= end; }
	bool intersects(const KeyRangeRef& keys) const { return begin < keys.end && keys.begin < end; }
	bool empty() const { return begin == end; }

	Standalone<KeyRangeRef> withPrefix(const StringRef& prefix) const {
		return KeyRangeRef(begin.withPrefix(prefix), end.withPrefix(prefix));
	}

	const KeyRangeRef& operator=(const KeyRangeRef& rhs) {
		const_cast<KeyRef&>(begin) = rhs.begin;
		const_cast<KeyRef&>(end) = rhs.end;
		return *this;
	}

	int expectedSize() const { return begin.expectedSize() + end.expectedSize(); }

	template <class Ar>
	force_inline void serialize(Ar& ar) {
		serializer(ar, const_cast<KeyRef&>(begin), const_cast<KeyRef&>(end));
		if (begin > end) {
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
};

inline KeyRangeRef operator&(const KeyRangeRef& lhs, const KeyRangeRef& rhs) {
	KeyRef b = std::max(lhs.begin, rhs.begin), e = std::min(lhs.end, rhs.end);
	if (e < b)
		return KeyRangeRef();
	return KeyRangeRef(b, e);
}

typedef Standalone<KeyRangeRef> KeyRange;

template <class T>
static std::string describe(T const& item) {
	return item.toString();
}
template <class K, class V>
static std::string describe(std::map<K, V> const& items, int max_items = -1) {
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
static std::string describeList(T const& items, int max_items) {
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
static std::string describe(std::vector<T> const& items, int max_items = -1) {
	return describeList(items, max_items);
}

template <class T>
static std::string describe(std::set<T> const& items, int max_items = -1) {
	return describeList(items, max_items);
}

template <class T1, class T2>
static std::string describe(std::pair<T1, T2> const& pair) {
	return "first: " + describe(pair.first) + " second: " + describe(pair.second);
}
} // namespace FDB

#endif /* FDB_LOANER_TYPES_H */
