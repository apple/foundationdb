/*
 * serialize.h
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

#ifndef FLOW_SERIALIZE_H
#define FLOW_SERIALIZE_H
#include <unordered_set>
#pragma once

#include <algorithm>
#include <array>
#include <boost/container/flat_map.hpp>
#include <deque>
#include <set>
#include <stdint.h>

#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/FileIdentifier.h"
#include "flow/ObjectSerializer.h"
#include "flow/ProtocolVersion.h"
#include "flow/network.h"

// Though similar, is_binary_serializable cannot be replaced by std::is_pod, as doing so would prefer
// memcpy over a defined serialize() method on a POD struct.  As not all of our structs are packed,
// this would both inflate message sizes by transmitting padding, and mean that we're transmitting
// undefined bytes over the wire.
// A more intelligent SFINAE that does "binarySerialize if POD and no serialize() is defined" could
// replace the usage of is_binary_serializable.
template <class T>
struct is_binary_serializable : std::false_type {};

#define BINARY_SERIALIZABLE(T)                                                                                         \
	template <>                                                                                                        \
	struct is_binary_serializable<T> : std::true_type {};

BINARY_SERIALIZABLE(int8_t);
BINARY_SERIALIZABLE(uint8_t);
BINARY_SERIALIZABLE(int16_t);
BINARY_SERIALIZABLE(uint16_t);
BINARY_SERIALIZABLE(int32_t);
BINARY_SERIALIZABLE(uint32_t);
BINARY_SERIALIZABLE(int64_t);
BINARY_SERIALIZABLE(uint64_t);
BINARY_SERIALIZABLE(bool);
BINARY_SERIALIZABLE(double);
BINARY_SERIALIZABLE(ProtocolVersion);

template <>
struct scalar_traits<ProtocolVersion> : std::true_type {
	constexpr static size_t size = sizeof(uint64_t);

	static void save(uint8_t* out, const ProtocolVersion& v) {
		*reinterpret_cast<uint64_t*>(out) = v.versionWithFlags();
	}

	template <class Context>
	static void load(const uint8_t* i, ProtocolVersion& out, Context& context) {
		const uint64_t* in = reinterpret_cast<const uint64_t*>(i);
		out = ProtocolVersion(*in);
	}
};

template <class Archive, class Item>
inline typename Archive::WRITER& operator<<(Archive& ar, const Item& item) {
	save(ar, item);
	return ar;
}

template <class Archive, class Item>
inline typename Archive::READER& operator>>(Archive& ar, Item& item) {
	ar.deserialize(item);
	return ar;
}

template <class Archive, class Item, class... Items>
typename Archive::WRITER& serializer(Archive& ar, const Item& item, const Items&... items) {
	save(ar, item);
	if constexpr (sizeof...(Items) > 0) {
		serializer(ar, items...);
	}
	return ar;
}

template <class Archive, class Item, class... Items>
typename Archive::READER& serializer(Archive& ar, Item& item, Items&... items) {
	load(ar, item);
	if constexpr (sizeof...(Items) > 0) {
		serializer(ar, items...);
	}
	return ar;
}

template <class Archive, class T, class Enable = void>
class Serializer {
public:
	static void serialize(Archive& ar, T& t) {
		t.serialize(ar);
		ASSERT(ar.protocolVersion().isValid());
	}
};

template <class Ar, class T>
inline void save(Ar& ar, const T& value) {
	Serializer<Ar, T>::serialize(ar, const_cast<T&>(value));
}

template <class Ar, class T>
inline void load(Ar& ar, T& value) {
	Serializer<Ar, T>::serialize(ar, value);
}

template <class CharT, class Traits, class Allocator>
struct FileIdentifierFor<std::basic_string<CharT, Traits, Allocator>> {
	constexpr static FileIdentifier value = 15694229;
};

template <class Archive>
inline void load(Archive& ar, std::string& value) {
	int32_t length;
	ar >> length;
	value.resize(length);
	ar.serializeBytes(&value[0], (int)value.length());
	ASSERT(ar.protocolVersion().isValid());
}

template <class Archive>
inline void save(Archive& ar, const std::string& value) {
	ar << (int32_t)value.length();
	ar.serializeBytes((void*)&value[0], (int)value.length());
	ASSERT(ar.protocolVersion().isValid());
}

template <class Archive, class T>
class Serializer<Archive, T, typename std::enable_if_t<is_binary_serializable<T>::value>> {
public:
	static void serialize(Archive& ar, T& t) { ar.serializeBinaryItem(t); }
};

template <class Archive, class T>
class Serializer<Archive, T, typename std::enable_if_t<std::is_enum_v<T>>> {
public:
	static void serialize(Archive& ar, T& t) {
		static_assert(is_binary_serializable<std::underlying_type_t<T>>::value);
		ar.serializeBinaryItem(reinterpret_cast<std::underlying_type_t<T>&>(t));
	}
};

template <class Archive, class T1, class T2>
class Serializer<Archive, std::pair<T1, T2>, void> {
public:
	static void serialize(Archive& ar, std::pair<T1, T2>& p) { serializer(ar, p.first, p.second); }
};

template <class T, class Allocator>
struct FileIdentifierFor<std::vector<T, Allocator>> : ComposedIdentifierExternal<T, 5> {};

template <class T, class Allocator>
struct CompositionDepthFor<std::vector<T, Allocator>> : std::integral_constant<int, CompositionDepthFor<T>::value + 1> {
};

template <class Key, class Hash, class KeyEqual, class Allocator>
struct FileIdentifierFor<std::unordered_set<Key, Hash, KeyEqual, Allocator>> : ComposedIdentifierExternal<Key, 6> {};

template <class Key, class Hash, class KeyEqual, class Allocator>
struct CompositionDepthFor<std::unordered_set<Key, Hash, KeyEqual, Allocator>>
  : std::integral_constant<int, CompositionDepthFor<Key>::value + 1> {};

template <class Archive, class T>
inline void save(Archive& ar, const std::vector<T>& value) {
	ar << (int)value.size();
	for (auto it = value.begin(); it != value.end(); ++it)
		ar << *it;
	ASSERT(ar.protocolVersion().isValid());
}
template <class Archive, class T>
inline void load(Archive& ar, std::vector<T>& value) {
	int s;
	ar >> s;
	value.clear();
	value.reserve(s);
	for (int i = 0; i < s; i++) {
		value.push_back(T());
		ar >> value[i];
	}
	ASSERT(ar.protocolVersion().isValid());
}

template <class Archive, class T>
inline void save(Archive& ar, const std::deque<T>& value) {
	ar << (int)value.size();
	for (auto it = value.begin(); it != value.end(); ++it)
		ar << *it;
	ASSERT(ar.protocolVersion().isValid());
}

template <class Archive, class T>
inline void load(Archive& ar, std::deque<T>& value) {
	int s;
	ar >> s;
	value.clear();
	value.reserve(s);
	for (int i = 0; i < s; i++) {
		value.push_back(T());
		ar >> value[i];
	}
	ASSERT(ar.protocolVersion().isValid());
}

template <class Archive, class T, size_t N>
inline void save(Archive& ar, const std::array<T, N>& value) {
	for (int ii = 0; ii < N; ++ii)
		ar << value[ii];
	ASSERT(ar.protocolVersion().isValid());
}
template <class Archive, class T, size_t N>
inline void load(Archive& ar, std::array<T, N>& value) {
	for (int ii = 0; ii < N; ii++) {
		ar >> value[ii];
	}
	ASSERT(ar.protocolVersion().isValid());
}

template <class Archive, class T>
inline void save(Archive& ar, const std::set<T>& value) {
	ar << (int)value.size();
	for (auto it = value.begin(); it != value.end(); ++it)
		ar << *it;
	ASSERT(ar.protocolVersion().isValid());
}
template <class Archive, class T>
inline void load(Archive& ar, std::set<T>& value) {
	int s;
	ar >> s;
	value.clear();
	T currentValue;
	for (int i = 0; i < s; i++) {
		ar >> currentValue;
		value.insert(currentValue);
	}
	ASSERT(ar.protocolVersion().isValid());
}

template <class Archive, class K, class V>
inline void save(Archive& ar, const std::map<K, V>& value) {
	ar << (int)value.size();
	for (const auto& it : value) {
		ar << it.first << it.second;
	}
	ASSERT(ar.protocolVersion().isValid());
}
template <class Archive, class K, class V>
inline void load(Archive& ar, std::map<K, V>& value) {
	int s;
	ar >> s;
	value.clear();
	for (int i = 0; i < s; ++i) {
		std::pair<K, V> p;
		ar >> p.first >> p.second;
		value.emplace(p);
	}
	ASSERT(ar.protocolVersion().isValid());
}

template <class Archive, class K, class V>
inline void save(Archive& ar, const boost::container::flat_map<K, V>& value) {
	ar << (int)value.size();
	for (const auto& it : value) {
		ar << it.first << it.second;
	}
	ASSERT(ar.protocolVersion().isValid());
}
template <class Archive, class K, class V>
inline void load(Archive& ar, boost::container::flat_map<K, V>& value) {
	int s;
	ar >> s;
	value.clear();
	for (int i = 0; i < s; ++i) {
		std::pair<K, V> p;
		ar >> p.first >> p.second;
		value.emplace(p);
	}
	ASSERT(ar.protocolVersion().isValid());
}

#ifdef _MSC_VER
#pragma intrinsic(memcpy)
#endif

#if VALGRIND
static bool valgrindCheck(const void* data, int bytes, const char* context) {
	auto first = VALGRIND_CHECK_MEM_IS_DEFINED(data, bytes);
	if (first) {
		int und = 0;
		for (int b = 0; b < bytes; b++)
			if (VALGRIND_CHECK_MEM_IS_DEFINED((uint8_t*)data + b, 1))
				und++;
		TraceEvent(SevError, "UndefinedData")
		    .detail("In", context)
		    .detail("Size", bytes)
		    .detail("Undefined", und)
		    .detail("FirstAt", (int64_t)first - (int64_t)data);
		return false;
	}
	return true;
}
#else
static inline bool valgrindCheck(const void* data, int bytes, const char* context) {
	return true;
}
#endif

struct _IncludeVersion {
	ProtocolVersion v;
	explicit _IncludeVersion(ProtocolVersion defaultVersion) : v(defaultVersion) { ASSERT(defaultVersion.isValid()); }
	template <class Ar>
	void write(Ar& ar) {
		ar.setProtocolVersion(v);
		ar << v;
	}
	template <class Ar>
	void read(Ar& ar) {
		ar >> v;
		if (!v.isValid()) {
			auto err = incompatible_protocol_version();
			TraceEvent(SevWarnAlways, "InvalidSerializationVersion")
			    .error(err)
			    .detailf("Version", "%llx", v.versionWithFlags());
			throw err;
		}
		if (v >= minInvalidProtocolVersion) {
			// The version v is too large to be downgraded from.
			auto err = incompatible_protocol_version();
			TraceEvent(SevError, "FutureProtocolVersion").error(err).detailf("Version", "%llx", v.versionWithFlags());
			throw err;
		}
		ar.setProtocolVersion(v);
	}
};
struct _AssumeVersion {
	ProtocolVersion v;
	explicit _AssumeVersion(ProtocolVersion version);
	template <class Ar>
	void write(Ar& ar) {
		ar.setProtocolVersion(v);
	}
	template <class Ar>
	void read(Ar& ar) {
		ar.setProtocolVersion(v);
	}
};
struct _Unversioned {
	template <class Ar>
	void write(Ar& ar) {}
	template <class Ar>
	void read(Ar& ar) {}
};

// These functions return valid options to the VersionOptions parameter of the constructor of each archive type
inline _IncludeVersion IncludeVersion(ProtocolVersion defaultVersion = currentProtocolVersion) {
	return _IncludeVersion(defaultVersion);
}
inline _AssumeVersion AssumeVersion(ProtocolVersion version) {
	return _AssumeVersion(version);
}
inline _Unversioned Unversioned() {
	return _Unversioned();
}

// static uint64_t size_limits[] = { 0ULL, 255ULL, 65535ULL, 16777215ULL, 4294967295ULL, 1099511627775ULL,
// 281474976710655ULL, 72057594037927935ULL, 18446744073709551615ULL };

class BinaryWriter : NonCopyable {
public:
	static const int isDeserializing = 0;
	static constexpr bool isSerializing = true;
	typedef BinaryWriter WRITER;

	void serializeBytes(StringRef bytes) { serializeBytes(bytes.begin(), bytes.size()); }
	void serializeBytes(const void* data, int bytes) {
		if (bytes > 0) {
			valgrindCheck(data, bytes, "serializeBytes");
			void* p = writeBytes(bytes);
			memcpy(p, data, bytes);
		}
	}
	template <class T>
	void serializeBinaryItem(const T& t) {
		static_assert(is_binary_serializable<T>::value,
		              "Object must be binary serializable, see BINARY_SERIALIZABLE macro");
		*(T*)writeBytes(sizeof(T)) = t;
	}
	void* getData() { return data; }
	int getLength() const { return size; }
	Standalone<StringRef> toValue() const { return Standalone<StringRef>(StringRef(data, size), arena); }
	template <class VersionOptions>
	explicit BinaryWriter(VersionOptions vo) : data(nullptr), size(0), allocated(0) {
		vo.write(*this);
	}
	BinaryWriter(BinaryWriter&& rhs)
	  : arena(std::move(rhs.arena)), data(rhs.data), size(rhs.size), allocated(rhs.allocated),
	    m_protocolVersion(rhs.m_protocolVersion) {
		rhs.size = 0;
		rhs.allocated = 0;
		rhs.data = nullptr;
	}
	void operator=(BinaryWriter&& r) {
		arena = std::move(r.arena);
		data = r.data;
		size = r.size;
		allocated = r.allocated;
		m_protocolVersion = r.m_protocolVersion;
		r.size = 0;
		r.allocated = 0;
		r.data = nullptr;
	}

	template <class T, class VersionOptions>
	static Standalone<StringRef> toValue(T const& t, VersionOptions vo) {
		BinaryWriter wr(vo);
		wr << t;
		return wr.toValue();
	}

	static int bytesNeeded(uint64_t val) {
		int n;
		for (n = 1; n < 8 && (val >> (n * 8)); ++n)
			;
		return n;
	}

	void serializeAsTuple(StringRef str) {
		size_t last_pos = 0;

		serializeBytes(LiteralStringRef("\x01"));

		for (size_t pos = 0; pos < str.size(); ++pos) {
			if (str[pos] == '\x00') {
				serializeBytes(str.substr(last_pos, pos - last_pos));
				serializeBytes(LiteralStringRef("\x00\xff"));
				last_pos = pos + 1;
			}
		}
		serializeBytes(str.substr(last_pos, str.size() - last_pos));
		serializeBytes(LiteralStringRef("\x00"));
	}

	void serializeAsTuple(bool t) {
		if (!t) {
			void* p = writeBytes(1);
			((uint8_t*)p)[0] = (uint8_t)20;
		} else {
			void* p = writeBytes(2);
			((uint8_t*)p)[0] = (uint8_t)21;
			((uint8_t*)p)[1] = (uint8_t)1;
		}
	}

	void serializeAsTuple(uint64_t t) {
		if (t == 0) {
			void* p = writeBytes(1);
			((uint8_t*)p)[0] = (uint8_t)20;
			return;
		}

		// int n = ( std::lower_bound(size_limits, size_limits+9, t) - size_limits );
		// ASSERT( n <= 8 );
		int n = bytesNeeded(t);
		void* p = writeBytes(n + 1);
		((uint8_t*)p)[0] = (uint8_t)(20 + n);
		uint64_t x = bigEndian64(t);
		memcpy((uint8_t*)p + 1, (uint8_t*)&x + (8 - n), n);
	}

	void serializeAsTuple(int64_t t) {
		if (t == 0) {
			void* p = writeBytes(1);
			((uint8_t*)p)[0] = (uint8_t)20;
		} else if (t > 0) {
			// int n = ( std::lower_bound(size_limits, size_limits+9, t) - size_limits );
			// ASSERT( n <= 9 );
			int n = bytesNeeded(t);

			void* p = writeBytes(n + 1);
			((uint8_t*)p)[0] = (uint8_t)(20 + n);
			uint64_t x = bigEndian64((uint64_t)t);
			memcpy((uint8_t*)p + 1, (uint8_t*)&x + (8 - n), n);
		} else {
			// int n = ( std::lower_bound(size_limits, size_limits+9, -t) - size_limits );
			// ASSERT( n <= 9 );
			int n = bytesNeeded(-t);

			void* p = writeBytes(n + 1);
			((uint8_t*)p)[0] = (uint8_t)(20 - n);
			uint64_t x = bigEndian64(t - 1);
			memcpy((uint8_t*)p + 1, (uint8_t*)&x + (8 - n), n);
		}
	}

	ProtocolVersion protocolVersion() const { return m_protocolVersion; }
	void setProtocolVersion(ProtocolVersion pv) { m_protocolVersion = pv; }

private:
	Arena arena;
	uint8_t* data;
	int size, allocated;
	ProtocolVersion m_protocolVersion;

	void* writeBytes(int s) {
		int p = size;
		size += s;
		if (size > allocated) {
			if (size <= 512 - sizeof(ArenaBlock)) {
				allocated = 512 - sizeof(ArenaBlock);
			} else if (size <= 4096 - sizeof(ArenaBlock)) {
				allocated = 4096 - sizeof(ArenaBlock);
			} else {
				allocated = std::max(allocated * 2, size);
			}
			Arena newArena;
			uint8_t* newData = new (newArena) uint8_t[allocated];
			if (p > 0) {
				memcpy(newData, data, p);
			}
			arena = newArena;
			data = newData;
		}
		return data + p;
	}
};

// A known-length memory segment and an unknown-length memory segment which can be written to as a whole.
struct SplitBuffer {
	void write(const void* data, int length);
	void write(const void* data, int length, int offset);
	void writeAndShrink(const void* data, int length);
	uint8_t *begin, *next;
	int first_length;
};

// A writer that can serialize to a SplitBuffer
class OverWriter {
public:
	typedef OverWriter WRITER;

	template <class VersionOptions>
	explicit OverWriter(SplitBuffer buf, VersionOptions vo) : len(std::numeric_limits<int>::max()), buf(buf) {
		vo.write(*this);
	}

	template <class VersionOptions>
	explicit OverWriter(void* ptr, int len, VersionOptions vo) : len(len) {
		buf.begin = (uint8_t*)ptr;
		buf.first_length = len;
		vo.write(*this);
	}

	void serializeBytes(StringRef bytes) { serializeBytes(bytes.begin(), bytes.size()); }
	void serializeBytes(const void* data, int bytes) {
		valgrindCheck(data, bytes, "serializeBytes");
		writeBytes(data, bytes);
	}
	template <class T>
	void serializeBinaryItem(const T& t) {
		static_assert(is_binary_serializable<T>::value,
		              "Object must be binary serializable, see BINARY_SERIALIZABLE macro");
		writeBytes(&t, sizeof(T));
	}

	ProtocolVersion protocolVersion() const { return m_protocolVersion; }
	void setProtocolVersion(ProtocolVersion pv) { m_protocolVersion = pv; }

private:
	int len;
	SplitBuffer buf;
	ProtocolVersion m_protocolVersion;

	void writeBytes(const void* data, int wlen) {
		ASSERT(wlen <= len);
		buf.writeAndShrink(data, wlen);
		len -= wlen;
	}
};

template <class Impl>
class _Reader {
public:
	static const int isDeserializing = 1;
	static constexpr bool isSerializing = false;
	using READER = Impl;

	const void* peekBytes(int bytes) const {
		ASSERT(begin + bytes <= end);
		return begin;
	}

	void serializeBytes(void* data, int bytes) { memcpy(data, static_cast<Impl*>(this)->readBytes(bytes), bytes); }

	template <class T>
	void serializeBinaryItem(T& t) {
		static_assert(is_binary_serializable<T>::value,
		              "Object must be binary serializable, see BINARY_SERIALIZABLE macro");
		t = *(T*)(static_cast<Impl*>(this)->readBytes(sizeof(T)));
	}

	Arena& arena() { return m_pool; }

	ProtocolVersion protocolVersion() const { return m_protocolVersion; }
	void setProtocolVersion(ProtocolVersion pv) { m_protocolVersion = pv; }

	bool empty() const { return begin == end; }

	void checkpoint() { check = begin; }

	void rewind() {
		ASSERT(check != nullptr);
		begin = check;
		check = nullptr;
	}

protected:
	_Reader(const char* begin, const char* end) : begin(begin), end(end) {}
	_Reader(const char* begin, const char* end, const Arena& arena) : begin(begin), end(end), m_pool(arena) {}

	const char *begin, *end;
	const char* check = nullptr;
	Arena m_pool;
	ProtocolVersion m_protocolVersion;
};

class ArenaReader : public _Reader<ArenaReader> {
	Optional<ArenaObjectReader> arenaObjectReader;

public:
	const void* readBytes(int bytes) {
		const char* b = begin;
		const char* e = b + bytes;
		ASSERT(e <= end);
		begin = e;
		return b;
	}

	const uint8_t* arenaRead(int bytes) { return (const uint8_t*)readBytes(bytes); }

	const void* peekBytes(int bytes) const {
		ASSERT(begin + bytes <= end);
		return begin;
	}

	StringRef arenaReadAll() const { return StringRef(reinterpret_cast<const uint8_t*>(begin), end - begin); }

	template <class VersionOptions>
	ArenaReader(Arena const& arena, const StringRef& input, VersionOptions vo)
	  : _Reader(reinterpret_cast<const char*>(input.begin()), reinterpret_cast<const char*>(input.end()), arena) {
		vo.read(*this);
		if (m_protocolVersion.hasObjectSerializerFlag()) {
			arenaObjectReader = ArenaObjectReader(arena, input, vo);
		}
	}

	template <class T>
	void deserialize(T& t) {
		if constexpr (HasFileIdentifier<T>::value) {
			if (arenaObjectReader.present()) {
				arenaObjectReader.get().deserialize(t);
			} else {
				load(*this, t);
			}
		} else {
			load(*this, t);
		}
	}
};

class BinaryReader : public _Reader<BinaryReader> {
	Optional<ObjectReader> objectReader;

public:
	const void* readBytes(int bytes);

	const uint8_t* arenaRead(int bytes) {
		// Reads and returns the next bytes.
		// The returned pointer has the lifetime of this.arena()
		// Could be implemented zero-copy if [begin,end) was in this.arena() already; for now is a copy
		if (!bytes)
			return nullptr;
		uint8_t* dat = new (arena()) uint8_t[bytes];
		serializeBytes(dat, bytes);
		return dat;
	}

	template <class T, class VersionOptions>
	static T fromStringRef(StringRef sr, VersionOptions vo) {
		T t;
		BinaryReader r(sr, vo);
		r >> t;
		return t;
	}

	ProtocolVersion protocolVersion() const { return m_protocolVersion; }
	void setProtocolVersion(ProtocolVersion pv) { m_protocolVersion = pv; }

	void assertEnd() const { ASSERT(begin == end); }

	bool empty() const { return begin == end; }

	template <class VersionOptions>
	BinaryReader(const void* data, int length, VersionOptions vo)
	  : _Reader(reinterpret_cast<const char*>(data), reinterpret_cast<const char*>(data) + length) {
		readVersion(vo);
	}
	template <class VersionOptions>
	BinaryReader(const StringRef& s, VersionOptions vo)
	  : _Reader(reinterpret_cast<const char*>(s.begin()), reinterpret_cast<const char*>(s.end())) {
		readVersion(vo);
	}
	template <class VersionOptions>
	BinaryReader(const std::string& s, VersionOptions vo) : _Reader(s.c_str(), s.c_str() + s.size()) {
		readVersion(vo);
	}

	template <class T>
	void deserialize(T& t) {
		if constexpr (HasFileIdentifier<T>::value) {
			if (objectReader.present()) {
				objectReader.get().deserialize(t);
			} else {
				load(*this, t);
			}
		} else {
			load(*this, t);
		}
	}

private:
	template <class VersionOptions>
	void readVersion(VersionOptions vo) {
		vo.read(*this);
		if (m_protocolVersion.hasObjectSerializerFlag()) {
			objectReader = ObjectReader(reinterpret_cast<const uint8_t*>(begin), AssumeVersion(m_protocolVersion));
		}
	}
};

class SendBuffer {
protected:
	uint8_t* _data;

public:
	inline uint8_t const* data() const { return _data; }
	inline uint8_t* data() { return _data; }
	SendBuffer* next;
	int bytes_written, bytes_sent;
	int bytes_unsent() const { return bytes_written - bytes_sent; }
};

struct PacketBuffer : SendBuffer {
private:
	int reference_count;
	uint32_t const size_;
	static constexpr size_t PACKET_BUFFER_MIN_SIZE = 16384;
	static constexpr size_t PACKET_BUFFER_OVERHEAD = 40;

public:
	double const enqueue_time;

	size_t size() const { return size_; }

private:
	explicit PacketBuffer(size_t size) : reference_count(1), size_(size), enqueue_time(g_network->now()) {
		next = nullptr;
		bytes_written = bytes_sent = 0;
		_data = reinterpret_cast<uint8_t*>(this + 1);
		static_assert(sizeof(PacketBuffer) == PACKET_BUFFER_OVERHEAD);
	}

public:
	static PacketBuffer* create(size_t size = 0) {
		size = std::max(size, PACKET_BUFFER_MIN_SIZE - PACKET_BUFFER_OVERHEAD);
		uint8_t* mem = new uint8_t[size + PACKET_BUFFER_OVERHEAD];
		return new (mem) PacketBuffer{ size };
	}
	PacketBuffer* nextPacketBuffer() { return static_cast<PacketBuffer*>(next); }
	void addref() { ++reference_count; }
	void delref() {
		if (!--reference_count) {
			delete[] reinterpret_cast<uint8_t*>(this);
		}
	}
	int bytes_unwritten() const { return size_ - bytes_written; }
};

struct PacketWriter {
	static constexpr int isDeserializing = 0;
	static constexpr bool isSerializing = true;
	typedef PacketWriter WRITER;

	PacketBuffer* buffer;
	struct ReliablePacket*
	    reliable; // nullptr if this is unreliable; otherwise the last entry in the ReliablePacket::cont chain
	int length;
	ProtocolVersion m_protocolVersion;

	// reliable is nullptr if this is an unreliable packet, or points to a ReliablePacket.  PacketWriter is responsible
	//   for filling in reliable->buffer, ->cont, ->begin, and ->end, but not ->prev or ->next.
	template <class VersionOptions>
	PacketWriter(PacketBuffer* buf, ReliablePacket* reliable, VersionOptions vo) {
		init(buf, reliable);
		vo.read(*this);
	}

	void serializeBytes(const void* data, int bytes) {
		if (bytes <= buffer->bytes_unwritten()) {
			memcpy(buffer->data() + buffer->bytes_written, data, bytes);
			buffer->bytes_written += bytes;
		} else {
			serializeBytesAcrossBoundary(data, bytes);
		}
	}
	void writeAhead(int bytes, struct SplitBuffer*);
	PacketBuffer* finish();
	int size() const { return length; }

	void serializeBytes(StringRef bytes) { serializeBytes(bytes.begin(), bytes.size()); }
	template <class T>
	void serializeBinaryItem(const T& t) {
		static_assert(is_binary_serializable<T>::value,
		              "Object must be binary serializable, see BINARY_SERIALIZABLE macro");
		if (sizeof(T) <= buffer->bytes_unwritten()) {
			*(T*)(buffer->data() + buffer->bytes_written) = t;
			buffer->bytes_written += sizeof(T);
		} else {
			serializeBytesAcrossBoundary(&t, sizeof(T));
		}
	}
	ProtocolVersion protocolVersion() const { return m_protocolVersion; }
	void setProtocolVersion(ProtocolVersion pv) { m_protocolVersion = pv; }

private:
	void serializeBytesAcrossBoundary(const void* data, int bytes);
	void nextBuffer(size_t size = 0 /* downstream it will default to at least 4k minus some padding */);
	uint8_t* writeBytes(size_t size) {
		if (size > buffer->bytes_unwritten()) {
			nextBuffer(size);
			ASSERT(buffer->size() >= size);
		}
		uint8_t* result = buffer->data() + buffer->bytes_written;
		buffer->bytes_written += size;
		return result;
	}

	template <class, class>
	friend class MakeSerializeSource;

	void init(PacketBuffer* buf, ReliablePacket* reliable);
};

struct ISerializeSource {
	virtual void serializePacketWriter(PacketWriter&) const = 0;
	virtual void serializeObjectWriter(ObjectWriter&) const = 0;
};

template <class T, class V>
class MakeSerializeSource : public ISerializeSource {
public:
	using value_type = V;
	void serializePacketWriter(PacketWriter& w) const override {
		ObjectWriter writer([&](size_t size) { return w.writeBytes(size); }, AssumeVersion(w.protocolVersion()));
		writer.serialize(get()); // Writes directly into buffer supplied by |w|
	}
	virtual value_type const& get() const = 0;
};

template <class T>
struct SerializeSource : MakeSerializeSource<SerializeSource<T>, T> {
	using value_type = T;
	T const& value;
	SerializeSource(T const& value) : value(value) {}
	void serializeObjectWriter(ObjectWriter& w) const override { w.serialize(value); }
	T const& get() const override { return value; }
};

#endif
