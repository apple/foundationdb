/*
 * Arena.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef FLOW_ARENA_H
#define FLOW_ARENA_H
#pragma once

#include "flow/BooleanParam.h"
#include "flow/Error.h"
#include "flow/FastAlloc.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/ObjectSerializerTraits.h"
#include "flow/FileIdentifier.h"
#include "flow/swift_support.h"
#include "flow/Optional.h"
#include "flow/Traceable.h"
#include <algorithm>
#include <array>
#include <boost/functional/hash.hpp>
#include <iterator>
#include <stdint.h>
#include <string_view>
#include <string>
#include <cstring>
#include <limits>
#include <optional>
#include <set>
#include <type_traits>
#include <sstream>
#include <string_view>
#include <fmt/format.h>

// TrackIt is a zero-size class for tracking constructions, destructions, and assignments of instances
// of a class.  Just inherit TrackIt<T> from T to enable tracking of construction and destruction of
// T, and use the TRACKIT_ASSIGN(rhs) macro in any operator= definitions to enable assignment tracking.
//
// TrackIt writes to standard output because the trace log isn't available early in execution
// so applying TrackIt to StringRef or VectorRef, for example, would a segfault using the trace log.
//
// The template parameter enables TrackIt to be inherited multiple times in the ancestry
// of a class without producing an "inaccessible due to ambiguity" error.
template <class T>
struct TrackIt {
	typedef TrackIt<T> TrackItType;
// Put TRACKIT_ASSIGN into any operator= functions for which you want assignments tracked
#define TRACKIT_ASSIGN(o) *(TrackItType*)this = *(TrackItType*)&(o)

	// The type name T is in the TrackIt output so that objects that inherit TrackIt multiple times
	// can be tracked properly, otherwise the create and delete addresses appear duplicative.
	// This function returns just the string "T]" parsed from the __PRETTY_FUNCTION__ macro.  There
	// doesn't seem to be a better portable way to do this.
	static const char* __trackit__type() {
		const char* s = __PRETTY_FUNCTION__ + sizeof(__PRETTY_FUNCTION__);
		while (*--s != '=')
			;
		return s + 2;
	}

	TrackIt() { printf("TrackItCreate\t%s\t%p\t%s\n", __trackit__type(), this, platform::get_backtrace().c_str()); }
	TrackIt(const TrackIt& o) : TrackIt() {}
	TrackIt(const TrackIt&& o) : TrackIt() {}
	TrackIt& operator=(const TrackIt& o) {
		printf("TrackItAssigned\t%s\t%p<%p\t%s\n", __trackit__type(), this, &o, platform::get_backtrace().c_str());
		return *this;
	}
	TrackIt& operator=(const TrackIt&& o) { return *this = (const TrackIt&)o; }
	~TrackIt() { printf("TrackItDestroy\t%s\t%p\n", __trackit__type(), this); }
};

class NonCopyable {
protected:
	NonCopyable() = default;
	~NonCopyable() = default; /// Protected non-virtual destructor
	NonCopyable(NonCopyable&&) = default;
	NonCopyable& operator=(NonCopyable&&) = default;
	NonCopyable(const NonCopyable&) = delete;
	NonCopyable& operator=(const NonCopyable&) = delete;
};

FDB_BOOLEAN_PARAM(FastInaccurateEstimate);

// Tag struct to indicate that the block containing allocated memory needs to be zero-ed out after use
struct WipeAfterUse {};

// An Arena is a custom allocator that consists of a set of ArenaBlocks.  Allocation is performed by bumping a pointer
// on the most recent ArenaBlock until the block is unable to service the next allocation request.  When the current
// ArenaBlock is full, a new (larger) one is added to the Arena.  Deallocation is not directly supported.  Instead,
// memory is freed by deleting the entire Arena at once. See flow/README.md for details on using Arenas.
class Arena {
public:
	constexpr static auto fb_must_appear_last = true;
	Arena();
	explicit Arena(size_t reservedSize);
	//~Arena();
	Arena(const Arena&);
	Arena(Arena&& r) noexcept;
	Arena& operator=(const Arena&);
	Arena& operator=(Arena&&) noexcept;

	void dependsOn(const Arena& p);
	void* allocate4kAlignedBuffer(uint32_t size);

	// If fastInaccurateEstimate is true this operation is O(1) but it is inaccurate in that it
	// will omit memory added to this Arena's block tree using Arena handles which reference
	// non-root nodes in this Arena's block tree.
	// When fastInaccurateEstimate is false, all estimates in the block tree will be updated to
	// be accurate.
	size_t getSize(FastInaccurateEstimate = FastInaccurateEstimate::False) const;

	bool hasFree(size_t size, const void* address);

	friend void* operator new(size_t size, Arena& p);
	friend void* operator new[](size_t size, Arena& p);
	friend void* operator new(size_t size, Arena& p, struct WipeAfterUse);
	friend void* operator new[](size_t size, Arena& p, struct WipeAfterUse);

	bool sameArena(const Arena& other) const { return impl.getPtr() == other.impl.getPtr(); }

private:
	Reference<struct ArenaBlock> impl;
};

template <>
struct scalar_traits<Arena> : std::true_type {
	constexpr static size_t size = 0;
	template <class Context>
	static void save(uint8_t*, const Arena&, Context&) {}
	// Context is an arbitrary type that is plumbed by reference throughout
	// the load call tree.
	template <class Context>
	static void load(const uint8_t*, Arena& arena, Context& context) {
		context.addArena(arena);
	}
};

struct ArenaBlockRef {
	union {
		ArenaBlock* next;
		void* aligned4kBuffer;
	};

	// Only one of (next, aligned4kBuffer) is valid at any one time, as they occupy the same space.
	// If aligned4kBufferSize is not 0, aligned4kBuffer is valid, otherwise next is valid.
	uint32_t aligned4kBufferSize;

	uint32_t nextBlockOffset;
};

FDB_BOOLEAN_PARAM(IsSecureMem);

struct ArenaBlock : NonCopyable, ThreadSafeReferenceCounted<ArenaBlock> {
	enum {
		SMALL = 64,
		LARGE = 8193 // If size == used == LARGE, then use hugeSize, hugeUsed
	};

	enum { NOT_TINY = 127, TINY_HEADER = 6 };

	// int32_t referenceCount;	  // 4 bytes (in ThreadSafeReferenceCounted)
	bool secure : 1; // If this is set, block is zero-ed out after use
	uint8_t tinySize : 7, tinyUsed; // If these == NOT_TINY, use bigSize, bigUsed instead
	// if tinySize != NOT_TINY, following variables aren't used
	uint32_t bigSize, bigUsed; // include block header
	uint32_t nextBlockOffset;
	mutable size_t totalSizeEstimate; // Estimate of the minimum total size of arena blocks this one reaches

	void addref();
	void delref();
	bool isSecure() const;
	bool isTiny() const;
	int size() const;
	int used() const;
	int unused() const;
	const void* getData() const;
	const void* getNextData() const;
	size_t totalSize() const;
	size_t estimatedTotalSize() const;
	void wipeUsed();
	// just for debugging:
	void getUniqueBlocks(std::set<ArenaBlock*>& a);
	int addUsed(int bytes);
	void makeReference(ArenaBlock* next);
	void* make4kAlignedBuffer(uint32_t size);
	static void dependOn(Reference<ArenaBlock>& self, ArenaBlock* other);
	static void* dependOn4kAlignedBuffer(Reference<ArenaBlock>& self, uint32_t size);
	static void* allocate(Reference<ArenaBlock>& self, int bytes, IsSecureMem isSecure = IsSecureMem::False);
	// Return an appropriately-sized ArenaBlock to store the given data
	static ArenaBlock* create(int dataSize, Reference<ArenaBlock>& next);
	void destroy();
	void destroyLeaf();
	static void* operator new(size_t s) = delete;
};

inline void* operator new(size_t size, Arena& p) {
	UNSTOPPABLE_ASSERT(size < std::numeric_limits<int>::max());
	return ArenaBlock::allocate(p.impl, (int)size);
}
inline void operator delete(void*, Arena& p) {}
inline void* operator new[](size_t size, Arena& p) {
	UNSTOPPABLE_ASSERT(size < std::numeric_limits<int>::max());
	return ArenaBlock::allocate(p.impl, (int)size);
}
inline void operator delete[](void*, Arena& p) {}

inline void* operator new(size_t size, Arena& p, struct WipeAfterUse) {
	UNSTOPPABLE_ASSERT(size < std::numeric_limits<int>::max());
	return ArenaBlock::allocate(p.impl, (int)size, IsSecureMem::True);
}

inline void operator delete(void*, Arena& p, struct WipeAfterUse) {}

inline void* operator new[](size_t size, Arena& p, struct WipeAfterUse) {
	UNSTOPPABLE_ASSERT(size < std::numeric_limits<int>::max());
	return ArenaBlock::allocate(p.impl, (int)size, IsSecureMem::True);
}
inline void operator delete[](void*, Arena& p, struct WipeAfterUse) {}

template <class Archive>
inline void load(Archive& ar, Arena& p) {
	p = ar.arena();
}
template <class Archive>
inline void save(Archive& ar, const Arena& p) {
	// No action required
}

template <class Archive, class T>
inline void load(Archive& ar, Optional<T>& value) {
	bool valid;
	ar >> valid;
	if (valid) {
		T t;
		ar >> t;
		value = Optional<T>(t);
	} else {
		value.reset();
	}
}

template <class Archive, class T>
inline void save(Archive& ar, const Optional<T>& value) {
	ar << value.present();
	if (value.present()) {
		ar << value.get();
	}
}

template <class T>
struct union_like_traits<Optional<T>> : std::true_type {
	using Member = Optional<T>;
	using alternatives = pack<T>;

	template <class Context>
	static uint8_t index(const Member& variant, Context&) {
		return 0;
	}
	template <class Context>
	static bool empty(const Member& variant, Context&) {
		return !variant.present();
	}

	template <int i, class Context>
	static const T& get(const Member& variant, Context&) {
		static_assert(i == 0);
		return variant.get();
	}

	template <size_t i, class U, class Context>
	static void assign(Member& member, const U& t, Context&) {
		member = t;
	}
};

// #define STANDALONE_ALWAYS_COPY

template <class T>
class Standalone : private Arena, public T {
public:
	using RefType = T;

	constexpr static auto fb_must_appear_last = false;

	// T must have no destructor
	Arena& arena() { return *(Arena*)this; }
	const Arena& arena() const { return *(const Arena*)this; }

	T& contents() { return *(T*)this; }
	T const& contents() const { return *(T const*)this; }

	Standalone() {}
	Standalone(const T& t) : Arena(t.expectedSize()), T(arena(), t) {}
	Standalone<T>& operator=(const T& t) {
		Arena old = std::move(arena()); // We want to defer the destruction of the arena until after we have copied t,
		                                // in case it cross-references our previous value
		*(Arena*)this = Arena(t.expectedSize());
		*(T*)this = T(arena(), t);
		return *this;
	}

// Always-copy mode was meant to make alloc instrumentation more useful by making allocations occur at the final resting
// place of objects leaked It doesn't actually work because some uses of Standalone things assume the object's memory
// will not change on copy or assignment
#ifdef STANDALONE_ALWAYS_COPY
	// Treat Standalone<T>'s as T's in construction and assignment so the memory is copied
	Standalone(const T& t, const Arena& arena) : Standalone(t) {}
	Standalone(const Standalone<T>& t) : Standalone((T const&)t) {}
	Standalone(const Standalone<T>&& t) : Standalone((T const&)t) {}
	Standalone<T>& operator=(const Standalone<T>&& t) {
		*this = (T const&)t;
		return *this;
	}
	Standalone<T>& operator=(const Standalone<T>& t) {
		*this = (T const&)t;
		return *this;
	}
#else
	Standalone(const T& t, const Arena& arena) : Arena(arena), T(t) {}
	Standalone(const Standalone<T>&) = default;
	Standalone<T>& operator=(const Standalone<T>&) = default;
	Standalone(Standalone<T>&&) = default;
	Standalone<T>& operator=(Standalone<T>&&) = default;
	~Standalone() = default;
#endif

	template <class U>
	Standalone<U> castTo() const {
		return Standalone<U>(*this, arena());
	}

	template <class Archive>
	void serialize(Archive& ar) {
		// FIXME: something like BinaryReader(ar) >> arena >> *(T*)this; to guarantee standalone arena???
		// T tmp;
		// ar >> tmp;
		//*this = tmp;
		serializer(ar, (*(T*)this), arena());
	}

	/*static Standalone<T> fakeStandalone( const T& t ) {
	    Standalone<T> x;
	    *(T*)&x = t;
	    return x;
	}*/
private:
	template <class U>
	Standalone(Standalone<U> const&); // unimplemented
	template <class U>
	Standalone<T> const& operator=(Standalone<U> const&); // unimplemented
};

extern std::string format(const char* form, ...);

#pragma pack(push, 4)
class StringRef {
public:
	constexpr static FileIdentifier file_identifier = 13300811;
	StringRef() : data(0), length(0) {}
	StringRef(Arena& p, const StringRef& toCopy) : data(new(p) uint8_t[toCopy.size()]), length(toCopy.size()) {
		if (length > 0) {
			memcpy((void*)data, toCopy.data, length);
		}
	}
	StringRef(Arena& p, const std::string& toCopy) : length((int)toCopy.size()) {
		UNSTOPPABLE_ASSERT(toCopy.size() <= std::numeric_limits<int>::max());
		data = new (p) uint8_t[toCopy.size()];
		if (length)
			memcpy((void*)data, &toCopy[0], length);
	}
	StringRef(Arena& p, const uint8_t* toCopy, int length) : data(new(p) uint8_t[length]), length(length) {
		if (length > 0) {
			memcpy((void*)data, toCopy, length);
		}
	}
	StringRef(const uint8_t* data, int length) : data(data), length(length) {}
	StringRef(const std::string& s) : data((const uint8_t*)s.c_str()), length((int)s.size()) {
		if (s.size() > std::numeric_limits<int>::max())
			abort();
	}
	// StringRef( const StringRef& p );

	const uint8_t* begin() const { return data; }
	const uint8_t* end() const { return data + length; }
	int size() const { return length; }
	bool empty() const { return length == 0; }

	uint8_t operator[](int i) const { return data[i]; }

	StringRef substr(int start) const { return StringRef(data + start, length - start); }
	StringRef substr(int start, int size) const { return StringRef(data + start, size); }
	bool startsWith(const StringRef& s) const {
		// Avoid UB - can't pass nullptr to memcmp
		if (s.size() == 0) {
			return true;
		}
		return size() >= s.size() && !memcmp(begin(), s.begin(), s.size());
	}
	bool endsWith(const StringRef& s) const {
		// Avoid UB - can't pass nullptr to memcmp
		if (s.size() == 0) {
			return true;
		}
		return size() >= s.size() && !memcmp(end() - s.size(), s.begin(), s.size());
	}

	StringRef withPrefix(const StringRef& prefix, Arena& arena) const {
		uint8_t* s = new (arena) uint8_t[prefix.size() + size()];
		if (prefix.size() > 0) {
			memcpy(s, prefix.begin(), prefix.size());
		}
		if (size() > 0) {
			memcpy(s + prefix.size(), begin(), size());
		}
		return StringRef(s, prefix.size() + size());
	}

	StringRef withSuffix(const StringRef& suffix, Arena& arena) const {
		uint8_t* s = new (arena) uint8_t[suffix.size() + size()];
		if (size() > 0) {
			memcpy(s, begin(), size());
		}
		if (suffix.size() > 0) {
			memcpy(s + size(), suffix.begin(), suffix.size());
		}
		return StringRef(s, suffix.size() + size());
	}

	Standalone<StringRef> withPrefix(const StringRef& prefix) const {
		Standalone<StringRef> r;
		r.contents() = withPrefix(prefix, r.arena());
		return r;
	}

	Standalone<StringRef> withSuffix(const StringRef& suffix) const {
		Standalone<StringRef> r;
		r.contents() = withSuffix(suffix, r.arena());
		return r;
	}

	StringRef removePrefix(const StringRef& s) const {
		// pre: startsWith(s)
		UNSTOPPABLE_ASSERT(s.size() <= size()); //< In debug mode, we could check startsWith()
		return substr(s.size());
	}

	StringRef removeSuffix(const StringRef& s) const {
		// pre: endsWith(s)
		UNSTOPPABLE_ASSERT(s.size() <= size()); //< In debug mode, we could check endsWith()
		return substr(0, size() - s.size());
	}

	std::string toString() const { return std::string(reinterpret_cast<const char*>(data), length); }

	std::string_view toStringView() const { return std::string_view(reinterpret_cast<const char*>(data), length); }

	static bool isPrintable(char c) { return c > 32 && c < 127; }
	inline std::string printable() const;

	std::string toHexString(int limit = -1) const {
		if (limit < 0)
			limit = length;
		if (length > limit) {
			// If limit is high enough split it so that 2/3 of limit is used to show prefix bytes and the rest is used
			// for suffix bytes
			if (limit >= 9) {
				int suffix = limit / 3;
				return substr(0, limit - suffix).toHexString() + "..." + substr(length - suffix, suffix).toHexString() +
				       format(" [%d bytes]", length);
			}
			return substr(0, limit).toHexString() + format("...[%d]", length);
		}

		std::string s;
		s.reserve(length * 7);
		for (int i = 0; i < length; i++) {
			uint8_t b = (*this)[i];
			if (isalnum(b))
				s.append(format("%02x (%c) ", b, b));
			else
				s.append(format("%02x ", b));
		}
		if (s.size() > 0)
			s.resize(s.size() - 1);
		return s;
	}

	std::string toHexStringPlain() const {
		std::string s;
		s.reserve(length * 7);
		for (int i = 0; i < length; i++) {
			uint8_t b = (*this)[i];
			s.append(format("%02x ", b));
		}
		if (s.size() > 0)
			s.resize(s.size() - 1);
		return s;
	}

	int expectedSize() const { return size(); }

	int compare(StringRef const& other) const {
		auto minSize = static_cast<int>(std::min(size(), other.size()));
		if (minSize != 0) {
			int c = memcmp(begin(), other.begin(), minSize);
			if (c != 0)
				return c;
		}
		return ::compare(size(), other.size());
	}

	int compareSuffix(StringRef const& other, int prefixLen) const {
		// pre: prefixLen <= size() && prefixLen <= other.size()
		size_t minSuffixSize = std::min(size(), other.size()) - prefixLen;
		if (minSuffixSize != 0) {
			int c = memcmp(begin() + prefixLen, other.begin() + prefixLen, minSuffixSize);
			if (c != 0)
				return c;
		}
		return ::compare(size(), other.size());
	}

	// Removes bytes from begin up to and including the sep string, returns StringRef of the part before sep
	StringRef eat(StringRef sep, bool* foundSep = nullptr) {
		for (int i = 0, iend = size() - sep.size(); i <= iend; ++i) {
			if (sep.compare(substr(i, sep.size())) == 0) {
				if (foundSep) {
					*foundSep = true;
				}
				StringRef token = substr(0, i);
				*this = substr(i + sep.size());
				return token;
			}
		}
		if (foundSep) {
			*foundSep = false;
		}
		return eat();
	}
	StringRef eat() {
		StringRef r = *this;
		*this = StringRef();
		return r;
	}
	StringRef eat(const char* sep, bool* foundSep = nullptr) {
		return eat(StringRef((const uint8_t*)sep, (int)strlen(sep)), foundSep);
	}
	// Return StringRef of bytes from begin() up to but not including the first byte matching any byte in sep,
	// and remove that sequence (including the sep byte) from *this
	// Returns and removes all bytes from *this if no bytes within sep were found
	StringRef eatAny(StringRef sep, uint8_t* foundSeparator) {
		auto iSep = std::find_first_of(begin(), end(), sep.begin(), sep.end());
		if (iSep != end()) {
			if (foundSeparator != nullptr) {
				*foundSeparator = *iSep;
			}
			const int i = iSep - begin();
			StringRef token = substr(0, i);
			*this = substr(i + 1);
			return token;
		}
		return eat();
	}
	StringRef eatAny(const char* sep, uint8_t* foundSeparator) {
		return eatAny(StringRef((const uint8_t*)sep, strlen(sep)), foundSeparator);
	}

	uint8_t back() {
		UNSTOPPABLE_ASSERT(!empty());
		return data[length - 1];
	}

	void popBack() {
		UNSTOPPABLE_ASSERT(!empty());
		--length;
	}

	// Copies string contents to dst and returns a pointer to the next byte after
	uint8_t* copyTo(uint8_t* dst) const {
		if (length > 0) {
			memcpy(dst, data, length);
		}
		return dst + length;
	}

	std::vector<StringRef> splitAny(StringRef sep) const {
		StringRef r = *this;
		std::vector<StringRef> tokens;
		while (r.size()) {
			tokens.push_back(r.eatAny(sep, nullptr));
		}
		return tokens;
	}

	// True if both StringRefs reference exactly the same memory
	bool same(const StringRef& s) const { return data == s.data && length == s.length; }

private:
	// Unimplemented; blocks conversion through std::string
	StringRef(char*);

	const uint8_t* data;
	int length;
};
#pragma pack(pop)

namespace std {
template <>
struct hash<StringRef> {
	static constexpr std::hash<std::string_view> hashFunc{};
	std::size_t operator()(StringRef const& tag) const {
		return hashFunc(std::string_view((const char*)tag.begin(), tag.size()));
	}
};
} // namespace std

namespace std {
template <>
struct hash<Standalone<StringRef>> {
	static constexpr std::hash<std::string_view> hashFunc{};
	std::size_t operator()(Standalone<StringRef> const& tag) const {
		return hashFunc(std::string_view((const char*)tag.begin(), tag.size()));
	}
};
} // namespace std

namespace std {
template <class T>
struct hash<Optional<T>> {
	std::size_t operator()(Optional<T> const& val) const { return val.hash(); }
};
} // namespace std

template <class T, class V = std::void_t<>>
struct boost_hashable : std::false_type {};

template <class T>
struct boost_hashable<T, std::void_t<decltype(boost::hash_value(std::declval<T>()))>> : std::true_type {};

// Using boost hash functions on types that depend on member hashes (e.g. std::pair) expect the members
// to be boost hashable. This provides a default boost hash function based on std::hash.
template <class T>
std::enable_if_t<!boost_hashable<T>::value, std::size_t> hash_value(const T& v) {
	return std::hash<T>{}(v);
}

template <>
struct TraceableString<StringRef> {
	static const char* begin(StringRef value) { return reinterpret_cast<const char*>(value.begin()); }

	static bool atEnd(const StringRef& value, const char* iter) {
		return iter == reinterpret_cast<const char*>(value.end());
	}

	static std::string toString(const StringRef& value) { return value.toString(); }
};

template <>
struct Traceable<StringRef> : TraceableStringImpl<StringRef> {};

template <>
struct fmt::formatter<StringRef> : FormatUsingTraceable<StringRef> {};

inline std::string StringRef::printable() const {
	return Traceable<StringRef>::toString(*this);
}

template <class T>
struct Traceable<Standalone<T>> : Traceable<T> {};

template <class T>
struct fmt::formatter<Standalone<T>> : fmt::formatter<T> {};

#define __FILE__sr StringRef(reinterpret_cast<const uint8_t*>(__FILE__), sizeof(__FILE__) - 1)
#define __FUNCTION__sr StringRef(reinterpret_cast<const uint8_t*>(__FUNCTION__), sizeof(__FUNCTION__) - 1)

inline StringRef operator"" _sr(const char* str, size_t size) {
	return StringRef(reinterpret_cast<const uint8_t*>(str), size);
}

inline static uintptr_t getAlignedUpperBound(uintptr_t value, uintptr_t alignment) {
	return ((value + alignment - 1) / alignment) * alignment;
}

// makeString is used to allocate a Standalone<StringRef> of a known length for later
// mutation (via mutateString).  If you need to append to a string of unknown length,
// consider factoring StringBuffer from DiskQueue.actor.cpp.
inline static Standalone<StringRef> makeString(int length) {
	Standalone<StringRef> returnString;
	uint8_t* outData = new (returnString.arena()) uint8_t[length];
	((StringRef&)returnString) = StringRef(outData, length);
	return returnString;
}

inline static Standalone<StringRef> makeAlignedString(int alignment, int length) {
	Standalone<StringRef> returnString;
	uint8_t* outData = new (returnString.arena()) uint8_t[alignment + length];
	outData = (uint8_t*)getAlignedUpperBound((uintptr_t)outData, alignment);
	((StringRef&)returnString) = StringRef(outData, length);
	return returnString;
}

inline static StringRef makeString(int length, Arena& arena) {
	uint8_t* outData = new (arena) uint8_t[length];
	return StringRef(outData, length);
}

inline static StringRef makeAlignedString(int alignment, int length, Arena& arena) {
	uint8_t* outData = new (arena) uint8_t[alignment + length];
	outData = (uint8_t*)getAlignedUpperBound((uintptr_t)outData, alignment);
	return StringRef(outData, length);
}

// mutateString() simply casts away const and returns a pointer that can be used to mutate the
// contents of the given StringRef (it will also accept Standalone<StringRef>).  Obviously this
// is only legitimate if you know where the StringRef's memory came from and that it is not shared!
inline static uint8_t* mutateString(StringRef& s) {
	return const_cast<uint8_t*>(s.begin());
}

template <class... StringRefType>
static Standalone<StringRef> concatenateStrings(StringRefType... strs) {
	int totalSize = 0;
	for (auto const& s : { strs... }) {
		totalSize += s.size();
	}

	Standalone<StringRef> str = makeString(totalSize);
	uint8_t* buf = mutateString(str);

	for (auto const& s : { strs... }) {
		buf = s.copyTo(buf);
	}

	return str;
}

template <class... StringRefType>
static StringRef concatenateStrings(Arena& arena, StringRefType... strs) {
	int totalSize = 0;
	for (auto const& s : { strs... }) {
		totalSize += s.size();
	}

	StringRef str = makeString(totalSize, arena);
	uint8_t* buf = mutateString(str);

	for (auto const& s : { strs... }) {
		buf = s.copyTo(buf);
	}

	return str;
}

template <class Archive>
inline void load(Archive& ar, StringRef& value) {
	uint32_t length;
	ar >> length;
	value = StringRef(ar.arenaRead(length), length);
}
template <class Archive>
inline void save(Archive& ar, const StringRef& value) {
	ar << (uint32_t)value.size();
	ar.serializeBytes(value.begin(), value.size());
}

template <>
struct dynamic_size_traits<StringRef> : std::true_type {
	template <class Context>
	static size_t size(const StringRef& t, Context&) {
		return t.size();
	}
	template <class Context>
	static void save(uint8_t* out, const StringRef& t, Context&) {
		std::copy(t.begin(), t.end(), out);
	}

	template <class Context>
	static void load(const uint8_t* ptr, size_t sz, StringRef& str, Context& context) {
		str = StringRef(context.tryReadZeroCopy(ptr, sz), sz);
	}
};

inline bool operator==(const StringRef& lhs, const StringRef& rhs) {
	if (lhs.size() == 0 && rhs.size() == 0) {
		return true;
	}
	ASSERT(lhs.size() >= 0);
	return lhs.size() == rhs.size() && memcmp(lhs.begin(), rhs.begin(), static_cast<unsigned int>(lhs.size())) == 0;
}
template <int N>
inline bool operator==(const StringRef& lhs, const char (&rhs)[N]) {
	return lhs == StringRef(reinterpret_cast<const uint8_t*>(rhs), N - 1);
}
inline bool operator<(const StringRef& lhs, const StringRef& rhs) {
	if (std::min(lhs.size(), rhs.size()) > 0) {
		int c = memcmp(lhs.begin(), rhs.begin(), std::min(lhs.size(), rhs.size()));
		if (c != 0)
			return c < 0;
	}
	return lhs.size() < rhs.size();
}
inline bool operator>(const StringRef& lhs, const StringRef& rhs) {
	if (std::min(lhs.size(), rhs.size()) > 0) {
		int c = memcmp(lhs.begin(), rhs.begin(), std::min(lhs.size(), rhs.size()));
		if (c != 0)
			return c > 0;
	}
	return lhs.size() > rhs.size();
}
inline bool operator!=(const StringRef& lhs, const StringRef& rhs) {
	return !(lhs == rhs);
}
inline bool operator<=(const StringRef& lhs, const StringRef& rhs) {
	return !(lhs > rhs);
}
inline bool operator>=(const StringRef& lhs, const StringRef& rhs) {
	return !(lhs < rhs);
}

typedef uint64_t Word;
// Get the number of prefix bytes that are the same between a and b, up to their common length of cl
static inline int commonPrefixLength(uint8_t const* ap, uint8_t const* bp, int cl) {
	int i = 0;
	const int wordEnd = cl - sizeof(Word) + 1;

	for (; i < wordEnd; i += sizeof(Word)) {
		Word a = *(Word*)ap;
		Word b = *(Word*)bp;
		if (a != b) {
			return i + ctzll(a ^ b) / 8;
		}
		ap += sizeof(Word);
		bp += sizeof(Word);
	}

	for (; i < cl; i++) {
		if (*ap != *bp) {
			return i;
		}
		++ap;
		++bp;
	}
	return cl;
}

static inline int commonPrefixLength(const StringRef& a, const StringRef& b) {
	return commonPrefixLength(a.begin(), b.begin(), std::min(a.size(), b.size()));
}

static inline int commonPrefixLength(const StringRef& a, const StringRef& b, int skipLen) {
	return commonPrefixLength(a.begin() + skipLen, b.begin() + skipLen, std::min(a.size(), b.size()) - skipLen);
}

// This trait is used by VectorRef to determine if deep copy constructor should recursively
// call deep copies of each element.
//
// TODO: There should be an easier way to identify the difference between flow_ref and non-flow_ref types.
// std::is_trivially_copyable does not work because some flow_ref types are trivially copyable
// and some non-flow_ref types are not trivially copyable.
template <typename T>
struct flow_ref : std::integral_constant<bool, !std::is_fundamental_v<T>> {};

template <>
struct flow_ref<UID> : std::integral_constant<bool, false> {};

template <class A, class B>
struct flow_ref<std::pair<A, B>> : std::integral_constant<bool, false> {};

template <class T>
struct string_serialized_traits : std::false_type {
	int32_t getSize(const T& item) const { return 0; }

	uint32_t save(uint8_t* out, const T& t) const { return 0; }

	template <class Context>
	uint32_t load(const uint8_t* data, T& t, Context& context) {
		return 0;
	}
};

enum class VecSerStrategy { FlatBuffers, String };

template <class T, VecSerStrategy>
struct VectorRefPreserializer {
	VectorRefPreserializer() {}
	VectorRefPreserializer(const VectorRefPreserializer<T, VecSerStrategy::FlatBuffers>&) noexcept {}
	VectorRefPreserializer& operator=(const VectorRefPreserializer<T, VecSerStrategy::FlatBuffers>&) noexcept {
		return *this;
	}
	VectorRefPreserializer(const VectorRefPreserializer<T, VecSerStrategy::String>&) noexcept {}
	VectorRefPreserializer& operator=(const VectorRefPreserializer<T, VecSerStrategy::String>&) noexcept {
		return *this;
	}

	void invalidate() {}
	void add(const T& item) {}
	void remove(const T& item) {}
	void reset() {}
};

template <class T>
struct VectorRefPreserializer<T, VecSerStrategy::String> {
	mutable int32_t _cached_size; // -1 means unknown
	string_serialized_traits<T> _string_traits;

	VectorRefPreserializer() : _cached_size(0) {}
	VectorRefPreserializer(const VectorRefPreserializer<T, VecSerStrategy::String>& other) noexcept
	  : _cached_size(other._cached_size) {}
	VectorRefPreserializer& operator=(const VectorRefPreserializer<T, VecSerStrategy::String>& other) noexcept {
		_cached_size = other._cached_size;
		return *this;
	}
	VectorRefPreserializer(const VectorRefPreserializer<T, VecSerStrategy::FlatBuffers>&) noexcept : _cached_size(-1) {}
	VectorRefPreserializer& operator=(const VectorRefPreserializer<T, VecSerStrategy::FlatBuffers>&) noexcept {
		_cached_size = -1;
		return *this;
	}

	void invalidate() { _cached_size = -1; }
	void add(const T& item) {
		if (_cached_size > 0) {
			_cached_size += _string_traits.getSize(item);
		}
	}
	void remove(const T& item) {
		if (_cached_size > 0) {
			_cached_size -= _string_traits.getSize(item);
		}
	}
	void reset() { _cached_size = 0; }
};

template <class T, VecSerStrategy SerStrategy = VecSerStrategy::FlatBuffers>
class VectorRef : public ComposedIdentifier<T, 3>, public VectorRefPreserializer<T, SerStrategy> {
	using VPS = VectorRefPreserializer<T, SerStrategy>;
	friend class VectorRef<T,
	                       SerStrategy == VecSerStrategy::FlatBuffers ? VecSerStrategy::String
	                                                                  : VecSerStrategy::FlatBuffers>;

public:
	using value_type = T;
	static_assert(SerStrategy == VecSerStrategy::FlatBuffers || string_serialized_traits<T>::value);

	// T must be trivially copyable!
	// T must be trivially destructible, because ~T is never called
	static_assert(std::is_trivially_destructible_v<T>);
	VectorRef() : data(0), m_size(0), m_capacity(0) {}

	template <VecSerStrategy S>
	VectorRef(const VectorRef<T, S>& other)
	  : VPS(other), data(other.data), m_size(other.m_size), m_capacity(other.m_capacity) {}
	template <VecSerStrategy S>
	VectorRef& operator=(const VectorRef<T, S>& other) {
		*static_cast<VPS*>(this) = other;
		data = other.data;
		m_size = other.m_size;
		m_capacity = other.m_capacity;
		return *this;
	}

	// Arena constructor for non-Ref types, identified by !flow_ref
	template <class T2 = T, VecSerStrategy S>
	VectorRef(Arena& p, const VectorRef<T, S>& toCopy, typename std::enable_if<!flow_ref<T2>::value, int>::type = 0)
	  : VPS(toCopy), data((T*)new(p) uint8_t[sizeof(T) * toCopy.size()]), m_size(toCopy.size()),
	    m_capacity(toCopy.size()) {
		if (m_size > 0) {
			std::copy(toCopy.data, toCopy.data + m_size, data);
		}
	}

	// Arena constructor for Ref types, which must have an Arena constructor
	template <class T2 = T, VecSerStrategy S>
	VectorRef(Arena& p, const VectorRef<T, S>& toCopy, typename std::enable_if<flow_ref<T2>::value, int>::type = 0)
	  : VPS(), data((T*)new(p) uint8_t[sizeof(T) * toCopy.size()]), m_size(toCopy.size()), m_capacity(toCopy.size()) {
		for (int i = 0; i < m_size; i++) {
			auto ptr = new (&data[i]) T(p, toCopy[i]);
			VPS::add(*ptr);
		}
	}

	template <class It>
	VectorRef(Arena& arena, It first, It last) {
		if constexpr (flow_ref<T>::value) {
			append_deep(arena, first, std::distance(first, last));
		} else {
			append(arena, first, std::distance(first, last));
		}
	}

	VectorRef(T* data, int size) : data(data), m_size(size), m_capacity(size) {}
	VectorRef(T* data, int size, int capacity) : data(data), m_size(size), m_capacity(capacity) {}
	// VectorRef( const VectorRef<T>& toCopy ) : data( toCopy.data ), m_size( toCopy.m_size ), m_capacity(
	// toCopy.m_capacity ) {} VectorRef<T>& operator=( const VectorRef<T>& );

	template <VecSerStrategy S = SerStrategy>
	typename std::enable_if<S == VecSerStrategy::String, uint32_t>::type serializedSize() const {
		uint32_t result = sizeof(uint32_t);
		string_serialized_traits<T> t;
		if (VPS::_cached_size >= 0) {
			return result + VPS::_cached_size;
		}
		for (const auto& v : *this) {
			result += t.getSize(v);
		}
		VPS::_cached_size = result - sizeof(uint32_t);
		return result;
	}

	const T* begin() const { return data; }
	const T* end() const { return data + m_size; }
	T const& front() const { return *begin(); }
	T const& back() const { return end()[-1]; }
	int size() const { return m_size; }
	bool empty() const { return m_size == 0; }
	const T& operator[](int i) const { return data[i]; }

	// const versions of some VectorRef operators
	const T* cbegin() const { return data; }
	const T* cend() const { return data + m_size; }
	T const& cfront() const { return *begin(); }
	T const& cback() const { return end()[-1]; }

	std::reverse_iterator<const T*> rbegin() const { return std::reverse_iterator<const T*>(end()); }
	std::reverse_iterator<const T*> rend() const { return std::reverse_iterator<const T*>(begin()); }

	template <VecSerStrategy S = SerStrategy>
	typename std::enable_if<S == VecSerStrategy::FlatBuffers, VectorRef>::type slice(int begin, int end) const {
		return VectorRef(data + begin, end - begin);
	}

	template <VecSerStrategy S>
	bool operator==(VectorRef<T, S> const& rhs) const {
		if (size() != rhs.size())
			return false;
		for (int i = 0; i < m_size; i++)
			if ((*this)[i] != rhs[i])
				return false;
		return true;
	}
	template <VecSerStrategy S>
	bool operator!=(VectorRef<T, S> const& rhs) const {
		return !(*this == rhs);
	}

	// Warning: Do not mutate a VectorRef that has previously been copy constructed or assigned,
	// since copies will share data
	T* begin() {
		VPS::invalidate();
		return data;
	}
	T* end() {
		VPS::invalidate();
		return data + m_size;
	}
	T& front() {
		VPS::invalidate();
		return *begin();
	}
	T& back() {
		VPS::invalidate();
		return end()[-1];
	}
	T& operator[](int i) {
		VPS::invalidate();
		return data[i];
	}
	void push_back(Arena& p, const T& value) {
		if (m_size + 1 > m_capacity)
			reallocate(p, m_size + 1);
		auto ptr = new (&data[m_size]) T(value);
		VPS::add(*ptr);
		m_size++;
	}

	template <class... Us>
	T& emplace_back(Arena& p, Us&&... args) {
		if (m_size + 1 > m_capacity)
			reallocate(p, m_size + 1);
		auto ptr = new (&data[m_size]) T(std::forward<Us>(args)...);
		VPS::add(*ptr);
		m_size++;
		return *ptr;
	}

	// invokes the "Deep copy constructor" T(Arena&, const T&) moving T entirely into arena
	void push_back_deep(Arena& p, const T& value) {
		if (m_size + 1 > m_capacity)
			reallocate(p, m_size + 1);
		auto ptr = new (&data[m_size]) T(p, value);
		VPS::add(*ptr);
		m_size++;
	}

	// invokes the "Deep copy constructor" T(Arena&, U&&) moving T entirely into arena
	template <class... Us>
	T& emplace_back_deep(Arena& p, Us&&... args) {
		if (m_size + 1 > m_capacity)
			reallocate(p, m_size + 1);
		auto ptr = new (&data[m_size]) T(p, std::forward<Us>(args)...);
		VPS::add(*ptr);
		m_size++;
		return *ptr;
	}

	template <class It>
	void append(Arena& p, It begin, int count) {
		if (m_size + count > m_capacity)
			reallocate(p, m_size + count);
		VPS::invalidate();
		if (count > 0) {
			std::copy(begin, begin + count, data + m_size);
		}
		m_size += count;
	}
	template <class It>
	void append_deep(Arena& p, It begin, int count) {
		if (m_size + count > m_capacity)
			reallocate(p, m_size + count);
		for (int i = 0; i < count; i++) {
			auto ptr = new (&data[m_size + i]) T(p, *begin++);
			VPS::add(*ptr);
		}
		m_size += count;
	}
	void pop_back() {
		VPS::remove(back());
		m_size--;
	}

	void pop_front(int count = 1) {
		VPS::invalidate();
		count = std::min(m_size, count);

		data += count;
		m_size -= count;
		m_capacity -= count;
	}

	void resize(Arena& p, int size) {
		if (size > m_capacity)
			reallocate(p, size);
		for (int i = m_size; i < size; i++) {
			auto ptr = new (&data[i]) T();
			VPS::add(*ptr);
		}
		m_size = size;
	}

	void clear() {
		VPS::reset();
		m_size = 0;
	}

	void reserve(Arena& p, int size) {
		if (size > m_capacity)
			reallocate(p, size);
	}

	// expectedSize() for non-Ref types, identified by !flow_ref
	template <class T2 = T>
	typename std::enable_if<!flow_ref<T2>::value, size_t>::type expectedSize() const {
		return sizeof(T) * m_size;
	}

	// expectedSize() for Ref types, which must in turn have expectedSize() implemented.
	template <class T2 = T>
	typename std::enable_if<flow_ref<T2>::value, size_t>::type expectedSize() const {
		size_t t = sizeof(T) * m_size;
		for (int i = 0; i < m_size; i++)
			t += data[i].expectedSize();
		return t;
	}

	int capacity() const { return m_capacity; }

	void extendUnsafeNoReallocNoInit(int amount) { m_size += amount; }

protected:
	T* data;
	int m_size, m_capacity;

	void reallocate(Arena& p, int requiredCapacity) {
		requiredCapacity = std::max(m_capacity * 2, requiredCapacity);
		// SOMEDAY: Maybe we are right at the end of the arena and can expand cheaply
		T* newData = new (p) T[requiredCapacity];
		if (m_size > 0) {
			std::move(data, data + m_size, newData);
		}
		data = newData;
		m_capacity = requiredCapacity;
	}
};

// This is a VectorRef that optimizes for tiny to small sizes.
// It keeps the first #InlineMembers on the stack - which means
// that all of them are always copied. This should be faster
// when you expect the vector to be usually very small as it
// won't need allocations in these cases.
template <class T, int InlineMembers = 1>
class SmallVectorRef {
	static_assert(InlineMembers >= 0);

public:
	// types
	template <bool isConst>
	class iterator_impl {
		using self_t = iterator_impl<isConst>;
		using VecType = SmallVectorRef<T, InlineMembers>;
		std::conditional_t<isConst, const VecType*, VecType*> vec = nullptr;
		int idx = 0;

	public:
		using iterator_category = std::random_access_iterator_tag;
		using value_type = std::conditional_t<isConst, const T, T>;
		using difference_type = int;
		using pointer = value_type*;
		using reference = value_type&;
		friend class SmallVectorRef<T, InlineMembers>;
		friend bool operator<(const self_t& lhs, const self_t& rhs) {
			ASSERT(lhs.vec == rhs.vec);
			return lhs.idx < rhs.idx;
		}
		friend bool operator>(const self_t& lhs, const self_t& rhs) {
			ASSERT(lhs.vec == rhs.vec);
			return lhs.idx > rhs.idx;
		}
		friend bool operator<=(const self_t& lhs, const self_t& rhs) {
			ASSERT(lhs.vec == rhs.vec);
			return lhs.idx <= rhs.idx;
		}
		friend bool operator>=(const self_t& lhs, const self_t& rhs) {
			ASSERT(lhs.vec == rhs.vec);
			return lhs.idx >= rhs.idx;
		}
		friend self_t operator+(const self_t& lhs, difference_type diff) {
			auto res = lhs;
			res.idx += diff;
			return res;
		}
		friend self_t operator+(difference_type diff, const self_t& lhs) {
			auto res = lhs;
			res.idx += diff;
			return res;
		}
		friend self_t operator-(const self_t& lhs, difference_type diff) {
			auto res = lhs;
			res.idx -= diff;
			return res;
		}
		friend difference_type operator-(const self_t& lhs, const self_t& rhs) {
			ASSERT(lhs.vec == rhs.vec);
			return lhs.idx - rhs.idx;
		}

		self_t& operator++() {
			++idx;
			return *this;
		}
		self_t operator++(int) {
			auto res = *this;
			++(*this);
			return res;
		}
		self_t& operator--() {
			--idx;
			return *this;
		}
		self_t operator--(int) {
			auto res = *this;
			--(*this);
			return res;
		}
		self_t& operator+=(difference_type diff) {
			idx += diff;
			return *this;
		}
		self_t& operator-=(difference_type diff) {
			idx -= diff;
			return *this;
		}
		bool operator!=(self_t const& o) const { return vec != o.vec || idx != o.idx; }
		bool operator==(self_t const& o) const { return vec == o.vec && idx == o.idx; }
		reference operator[](difference_type i) const { return get(idx + i); }
		reference& get(int i) const {
			if (i < InlineMembers) {
				return vec->arr[i];
			} else {
				return vec->data[i - InlineMembers];
			}
		}
		reference get() const { return get(idx); }
		reference operator*() const { return get(); }
		pointer operator->() const { return &get(); }
	};
	using const_iterator = iterator_impl<true>;
	using iterator = iterator_impl<false>;
	using reverse_iterator = std::reverse_iterator<iterator>;
	using const_reverse_iterator = std::reverse_iterator<const_iterator>;

public: // Construction
	static_assert(std::is_trivially_destructible_v<T>);
	SmallVectorRef() {}
	SmallVectorRef(const SmallVectorRef<T, InlineMembers>& other)
	  : m_size(other.m_size), arr(other.arr), data(other.data) {}
	SmallVectorRef& operator=(const SmallVectorRef<T, InlineMembers>& other) {
		m_size = other.m_size;
		arr = other.arr;
		data = other.data;
		return *this;
	}

	template <class T2 = T, int IM = InlineMembers>
	SmallVectorRef(Arena& arena,
	               const SmallVectorRef<T, IM>& toCopy,
	               typename std::enable_if<!flow_ref<T2>::value, int>::type = 0)
	  : m_size(0) {
		append(arena, toCopy.begin(), toCopy.size());
	}

	template <class T2 = T, int IM = InlineMembers>
	SmallVectorRef(Arena& arena,
	               const SmallVectorRef<T2, IM>& toCopy,
	               typename std::enable_if<flow_ref<T2>::value, int>::type = 0)
	  : m_size(0) {
		append_deep(arena, toCopy.begin(), toCopy.size());
	}

	template <class It>
	SmallVectorRef(Arena& arena, It first, It last) : m_size(0) {
		if constexpr (flow_ref<T>::value) {
			append_deep(arena, first, std::distance(first, last));
		} else {
			append(arena, first, std::distance(first, last));
		}
	}

public: // information
	int size() const { return m_size; }
	int capacity() const { return InlineMembers + data.capacity(); }
	bool empty() const { return m_size == 0; }

public: // element access
	T const& front() const { return *cbegin(); }
	T const& back() const { return *crbegin(); }
	T& front() { return *begin(); }
	T& back() { return *rbegin(); }
	T const& operator[](int i) const {
		if (i < InlineMembers) {
			return arr[i];
		} else {
			return data[i - InlineMembers];
		}
	}

public: // Modification
	void push_back(Arena& arena, T const& value) {
		if (m_size < InlineMembers) {
			new (&arr[m_size++]) T(value);
			return;
		}
		++m_size;
		data.push_back(arena, value);
	}

	void push_back_deep(Arena& arena, T const& value) {
		if (m_size < InlineMembers) {
			new (&arr[m_size++]) T(arena, value);
			return;
		}
		++m_size;
		data.push_back_deep(arena, value);
	}

	void pop_back() { --m_size; }

	template <class It>
	void append(Arena& arena, It first, int count) {
		ASSERT(count >= 0);
		while (count > 0 && m_size < InlineMembers) {
			new (&(arr[m_size++])) T(*(first++));
			--count;
		}
		data.append(arena, first, count);
		m_size += count;
	}

	template <class It>
	void append_deep(Arena& arena, It first, int count) {
		ASSERT(count >= 0);
		while (count > 0 && m_size < InlineMembers) {
			new (&(arr[m_size++])) T(arena, *(first++));
			--count;
		}
		data.append_deep(arena, first, count);
		m_size += count;
	}

public: // iterator access
	iterator begin() {
		iterator res;
		res.vec = this;
		res.idx = 0;
		return res;
	}

	const_iterator cbegin() const {
		const_iterator res;
		res.vec = this;
		res.idx = 0;
		return res;
	}

	const_iterator begin() const { return cbegin(); }

	iterator end() {
		iterator res;
		res.vec = this;
		res.idx = m_size;
		return res;
	}

	const_iterator cend() const {
		const_iterator res;
		res.vec = this;
		res.idx = m_size;
		return res;
	}

	const_iterator end() const { return cend(); }

	reverse_iterator rbegin() { return reverse_iterator(end()); }

	const_reverse_iterator crbegin() const { return const_reverse_iterator(cend()); }

	const_reverse_iterator rbegin() const { return crbegin(); }

	reverse_iterator rend() { return reverse_iterator(begin()); }

	const_reverse_iterator crend() const { return const_reverse_iterator(begin()); }

	const_reverse_iterator rend() const { return crend(); }

private:
	int m_size = 0;
	std::array<T, InlineMembers> arr;
	VectorRef<T> data;
};

template <class T>
struct Traceable<VectorRef<T>> {
	constexpr static bool value = Traceable<T>::value;

	static std::string toString(const VectorRef<T>& value) {
		std::stringstream ss;
		bool first = true;
		for (const auto& v : value) {
			if (first) {
				first = false;
			} else {
				ss << ' ';
			}
			ss << Traceable<T>::toString(v);
		}
		return ss.str();
	}
};

template <class Archive, class T, VecSerStrategy S>
inline void load(Archive& ar, VectorRef<T, S>& value) {
	// FIXME: range checking for length, here and in other serialize code
	uint32_t length;
	ar >> length;
	UNSTOPPABLE_ASSERT(length * sizeof(T) < (100 << 20));
	// SOMEDAY: Can we avoid running constructors for all the values?
	value.resize(ar.arena(), length);
	for (uint32_t i = 0; i < length; i++)
		ar >> value[i];
}
template <class Archive, class T, VecSerStrategy S>
inline void save(Archive& ar, const VectorRef<T, S>& value) {
	uint32_t length = value.size();
	ar << length;
	for (uint32_t i = 0; i < length; i++)
		ar << value[i];
}

template <class T>
struct vector_like_traits<VectorRef<T, VecSerStrategy::FlatBuffers>> : std::true_type {
	using Vec = VectorRef<T>;
	using value_type = typename Vec::value_type;
	using iterator = const T*;
	using insert_iterator = T*;

	template <class Context>
	static size_t num_entries(const VectorRef<T>& v, Context&) {
		return v.size();
	}

	// Return an insert_iterator starting with an empty vector. |size| is the
	// number of elements to be inserted. Implementations may want to allocate
	// enough memory up front to hold |size| elements.
	template <class Context>
	static insert_iterator insert(Vec& v, size_t s, Context& context) {
		// Logically v should be empty after this function returns, but since we're going to
		// insert s times into the raw pointer insert_iterator it will end up
		// with the correct size after deserialization finishes.
		v.resize(context.arena(), s);
		return v.begin();
	}

	template <class Context>
	static iterator begin(const Vec& v, Context&) {
		return v.begin();
	}
};

template <class V>
struct dynamic_size_traits<VectorRef<V, VecSerStrategy::String>> : std::true_type {
	using T = VectorRef<V, VecSerStrategy::String>;
	// May be called multiple times during one serialization
	template <class Context>
	static size_t size(const T& t, Context&) {
		return t.serializedSize();
	}

	// Guaranteed to be called only once during serialization
	template <class Context>
	static void save(uint8_t* out, const T& t, Context&) {
		string_serialized_traits<V> traits;
		auto* p = out;
		uint32_t length = t.size();
		*reinterpret_cast<decltype(length)*>(out) = length;
		out += sizeof(length);
		for (const auto& item : t) {
			out += traits.save(out, item);
		}
		ASSERT(out - p == t._cached_size + sizeof(uint32_t));
	}

	// Context is an arbitrary type that is plumbed by reference throughout the
	// load call tree.
	template <class Context>
	static void load(const uint8_t* data, size_t size, T& t, Context& context) {
		string_serialized_traits<V> traits;
		auto* p = data;
		uint32_t num_elements;
		memcpy(&num_elements, data, sizeof(num_elements));
		data += sizeof(num_elements);
		t.resize(context.arena(), num_elements);
		for (unsigned i = 0; i < num_elements; ++i) {
			data += traits.load(data, t[i], context);
		}
		ASSERT(data - p == size);
		t._cached_size = size - sizeof(uint32_t);
	}
};

#endif
