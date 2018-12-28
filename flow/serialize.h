/*
 * serialize.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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
#pragma once

#include <stdint.h>
#include <set>
#include "flow/Error.h"
#include "flow/Arena.h"
#include <algorithm>

// Though similar, is_binary_serializable cannot be replaced by std::is_pod, as doing so would prefer
// memcpy over a defined serialize() method on a POD struct.  As not all of our structs are packed,
// this would both inflate message sizes by transmitting padding, and mean that we're transmitting
// undefined bytes over the wire.
// A more intelligent SFINAE that does "binarySerialize if POD and no serialize() is defined" could
// replace the usage of is_binary_serializable.
template <class T>
struct is_binary_serializable { enum { value = 0 }; };

#define BINARY_SERIALIZABLE( T ) template<> struct is_binary_serializable<T> { enum { value = 1 }; };

BINARY_SERIALIZABLE( int8_t );
BINARY_SERIALIZABLE( uint8_t );
BINARY_SERIALIZABLE( int16_t );
BINARY_SERIALIZABLE( uint16_t );
BINARY_SERIALIZABLE( int32_t );
BINARY_SERIALIZABLE( uint32_t );
BINARY_SERIALIZABLE( int64_t );
BINARY_SERIALIZABLE( uint64_t );
BINARY_SERIALIZABLE( bool );
BINARY_SERIALIZABLE( double );

template <class Archive, class Item>
inline typename Archive::WRITER& operator << (Archive& ar, const Item& item ) {
	save(ar, const_cast<Item&>(item));
	return ar;
}

template <class Archive, class Item>
inline typename Archive::READER& operator >> (Archive& ar, Item& item ) {
	load(ar, item);
	return ar;
}

template <class Archive>
void serializer(Archive& ar) {}

template <class Archive, class Item, class... Items>
typename Archive::WRITER& serializer(Archive& ar, const Item& item, const Items&... items) {
	save(ar, item);
	serializer(ar, items...);
	return ar;
}

template <class Archive, class Item, class... Items>
typename Archive::READER& serializer(Archive& ar, Item& item, Items&... items) {
	load(ar, item);
	serializer(ar, items...);
	return ar;
}

template <class Archive, class T, class Enable = void>
class Serializer {
public:
	static void serialize( Archive& ar, T& t ) {
		t.serialize(ar);
		ASSERT( ar.protocolVersion() != 0 );
	}
};

template <class Ar, class T>
inline void save( Ar& ar, const T& value ) {
	Serializer<Ar,T>::serialize(ar, const_cast<T&>(value));
}

template <class Ar, class T>
inline void load( Ar& ar, T& value ) {
	Serializer<Ar,T>::serialize(ar, value);
}

template <class Archive>
inline void load( Archive& ar, std::string& value ) {
	int32_t length;
	ar >> length;
	value.resize(length);
	ar.serializeBytes( &value[0], (int)value.length() );
	ASSERT( ar.protocolVersion() != 0 );
}

template <class Archive>
inline void save( Archive& ar, const std::string& value ) {
	ar << (int32_t)value.length();
	ar.serializeBytes( (void*)&value[0], (int)value.length() );
	ASSERT( ar.protocolVersion() != 0 );
}

template <class Archive, class T>
class Serializer< Archive, T, typename std::enable_if< is_binary_serializable<T>::value >::type> {
public:
	static void serialize( Archive& ar, T& t ) {
		ar.serializeBinaryItem(t);
	}
};

template <class Archive, class T1, class T2>
class Serializer< Archive, std::pair<T1,T2>, void > {
public:
	static void serialize( Archive& ar, std::pair<T1, T2>& p ) {
		serializer(ar, p.first, p.second);
	}
};

template <class Archive, class T>
inline void save( Archive& ar, const std::vector<T>& value ) {
	ar << (int)value.size();
	for(auto it = value.begin(); it != value.end(); ++it)
		ar << *it;
	ASSERT( ar.protocolVersion() != 0 );
}
template <class Archive, class T>
inline void load( Archive& ar, std::vector<T>& value ) {
	int s;
	ar >> s;
	value.clear();
	value.reserve(s);
	for (int i = 0; i < s; i++) {
		value.push_back(T());
		ar >> value[i];
	}
	ASSERT( ar.protocolVersion() != 0 );
}

template <class Archive, class T>
inline void save( Archive& ar, const std::set<T>& value ) {
	ar << (int)value.size();
	for(auto it = value.begin(); it != value.end(); ++it)
		ar << *it;
	ASSERT( ar.protocolVersion() != 0 );
}
template <class Archive, class T>
inline void load( Archive& ar, std::set<T>& value ) {
	int s;
	ar >> s;
	value.clear();
	T currentValue;
	for (int i = 0; i < s; i++) {
		ar >> currentValue;
		value.insert(currentValue);
	}
	ASSERT( ar.protocolVersion() != 0 );
}

#pragma intrinsic (memcpy)

#if VALGRIND
static bool valgrindCheck( const void* data, int bytes, const char* context ) {
	auto first= VALGRIND_CHECK_MEM_IS_DEFINED( data, bytes );
	if (first) {
		int und=0;
		for(int b=0; b<bytes; b++)
			if (VALGRIND_CHECK_MEM_IS_DEFINED( (uint8_t*)data+b, 1 ))
				und++;
		TraceEvent(SevError, "UndefinedData").detail("In", context).detail("Size", bytes).detail("Undefined", und).detail("FirstAt", (int64_t)first-(int64_t)data);
		return false;
	}
	return true;

}
#else
static inline bool valgrindCheck( const void* data, int bytes, const char* context ) { return true; }
#endif

extern const uint64_t currentProtocolVersion;
extern const uint64_t minValidProtocolVersion;
extern const uint64_t compatibleProtocolVersionMask;

struct _IncludeVersion {
	uint64_t v;
	explicit _IncludeVersion( uint64_t defaultVersion ) : v(defaultVersion) {
		ASSERT( defaultVersion >= minValidProtocolVersion );
	}
	template <class Ar>
	void write( Ar& ar ) {
		ar.setProtocolVersion(v);
		ar << v;
	}
	template <class Ar>
	void read( Ar& ar ) {
		ar >> v;
		if (v < minValidProtocolVersion) {
			auto err = incompatible_protocol_version();
			TraceEvent(SevError, "InvalidSerializationVersion").error(err).detailf("Version", "%llx", v);
			throw err;
		}
		if (v > currentProtocolVersion) {
			// For now, no forward compatibility whatsoever is supported.  In the future, this check may be weakened for
			// particular data structures (e.g. to support mismatches between client and server versions when the client
			// must deserialize zookeeper and database structures)
			auto err = incompatible_protocol_version();
			TraceEvent(SevError, "FutureProtocolVersion").error(err).detailf("Version", "%llx", v);
			throw err;
		}
		ar.setProtocolVersion(v);
	}
};
struct _AssumeVersion {
	uint64_t v;
	explicit _AssumeVersion( uint64_t version ) : v(version) {
		ASSERT( version >= minValidProtocolVersion );
	}
	template <class Ar> void write( Ar& ar ) { ar.setProtocolVersion(v); }
	template <class Ar> void read( Ar& ar ) { ar.setProtocolVersion(v); }
};
struct _Unversioned {
	template <class Ar> void write( Ar& ar ) { ar.setProtocolVersion(0); }
	template <class Ar> void read( Ar& ar ) { ar.setProtocolVersion(0); }
};

// These functions return valid options to the VersionOptions parameter of the constructor of each archive type
inline _IncludeVersion IncludeVersion( uint64_t defaultVersion = currentProtocolVersion ) { return _IncludeVersion(defaultVersion); }
inline _AssumeVersion AssumeVersion( uint64_t version ) { return _AssumeVersion(version); }
inline _Unversioned Unversioned() { return _Unversioned(); }

static uint64_t size_limits[] = { 0ULL, 255ULL, 65535ULL, 16777215ULL, 4294967295ULL, 1099511627775ULL, 281474976710655ULL, 72057594037927935ULL, 18446744073709551615ULL };

class BinaryWriter : NonCopyable {
public:
	static const int isDeserializing = 0;
	typedef BinaryWriter WRITER;

	void serializeBytes( StringRef bytes ) {
		serializeBytes(bytes.begin(), bytes.size());
	}
	void serializeBytes(const void* data, int bytes) {
		valgrindCheck( data, bytes, "serializeBytes" );
		void* p = writeBytes(bytes);
		memcpy(p, data, bytes);
	}
	template <class T>
	void serializeBinaryItem( const T& t ) {
		*(T*)writeBytes(sizeof(T)) = t;
	}
	void* getData() { return data; }
	int getLength() { return size; }
	StringRef toStringRef() { return StringRef(data,size); }
	template <class VersionOptions>
	explicit BinaryWriter( VersionOptions vo ) : data(NULL), size(0), allocated(0) { vo.write(*this); }
	BinaryWriter( BinaryWriter&& rhs ) : data(rhs.data), size(rhs.size), allocated(rhs.allocated), m_protocolVersion(rhs.m_protocolVersion) {
		rhs.size = 0;
		rhs.allocated = 0;
		rhs.data = 0;
	}
	void operator=( BinaryWriter&& r) {
		delete[] data;
		data = r.data;
		size = r.size;
		allocated = r.allocated;
		m_protocolVersion = r.m_protocolVersion;
		r.size = 0;
		r.allocated = 0;
		r.data = 0;
	}
	~BinaryWriter() { delete[] data; }

	template <class T, class VersionOptions>
	static Standalone<StringRef> toValue( T const& t, VersionOptions vo ) {
		BinaryWriter wr(vo);
		wr << t;
		return wr.toStringRef();
	}

	static int bytesNeeded( uint64_t val ) {
		int n;
		for( n=1; n<8 && (val>>(n*8)); ++n );
		return n;
	}

	void serializeAsTuple( StringRef str ) {
		size_t last_pos = 0;

		serializeBytes(LiteralStringRef("\x01"));

		for (size_t pos = 0; pos < str.size(); ++pos) {
			if (str[pos] == '\x00') {
				serializeBytes(str.substr(last_pos,pos - last_pos));
				serializeBytes(LiteralStringRef("\x00\xff"));
				last_pos = pos + 1;
			}
		}
		serializeBytes(str.substr(last_pos,str.size() - last_pos));
		serializeBytes(LiteralStringRef("\x00"));
	}

	void serializeAsTuple( bool t ) {
		if(!t) {
			void* p = writeBytes(1);
			((uint8_t*)p)[0] = (uint8_t)20;
		} else {
			void* p = writeBytes(2);
			((uint8_t*)p)[0] = (uint8_t)21;
			((uint8_t*)p)[1] = (uint8_t)1;
		}
	}
	
	void serializeAsTuple( uint64_t t ) {
		if(t == 0) {
			void* p = writeBytes(1);
			((uint8_t*)p)[0] = (uint8_t)20;
			return;
		}


		//int n = ( std::lower_bound(size_limits, size_limits+9, t) - size_limits );
		//ASSERT( n <= 8 );
		int n = bytesNeeded(t);
		void* p = writeBytes(n+1);
		((uint8_t*)p)[0] = (uint8_t)(20+n);
		uint64_t x = bigEndian64(t);
		memcpy((uint8_t*)p+1, (uint8_t*)&x+(8-n), n);
	}

	void serializeAsTuple( int64_t t ) {
		if(t == 0) {
			void* p = writeBytes(1);
			((uint8_t*)p)[0] = (uint8_t)20;
		} else if(t > 0) {
			//int n = ( std::lower_bound(size_limits, size_limits+9, t) - size_limits );
			//ASSERT( n <= 9 );
			int n = bytesNeeded(t);

			void* p = writeBytes(n+1);
			((uint8_t*)p)[0] = (uint8_t)(20+n);
			uint64_t x = bigEndian64((uint64_t)t);
			memcpy((uint8_t*)p+1, (uint8_t*)&x+(8-n), n);
		} else {
			//int n = ( std::lower_bound(size_limits, size_limits+9, -t) - size_limits );
			//ASSERT( n <= 9 );
			int n = bytesNeeded(-t);

			void* p = writeBytes(n+1);
			((uint8_t*)p)[0] = (uint8_t)(20-n);
			uint64_t x = bigEndian64(t-1);
			memcpy((uint8_t*)p+1, (uint8_t*)&x+(8-n), n);
		}
	}

	uint64_t protocolVersion() const { return m_protocolVersion; }
	void setProtocolVersion(uint64_t pv) { m_protocolVersion = pv; }
private:
	uint8_t* data;
	int size, allocated;
	uint64_t m_protocolVersion;

	void* writeBytes(int s) {
		int p = size;
		size += s;
		if (size > allocated) {
			allocated = std::max(allocated*2, size);
			uint8_t* newData = new uint8_t[allocated];
			memcpy(newData, data, p);
			delete[] data;
			data = newData;
		}
		return data+p;
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
	explicit OverWriter(SplitBuffer buf, VersionOptions vo) : buf(buf), len(std::numeric_limits<int>::max()) { vo.write(*this); }

	template <class VersionOptions>
	explicit OverWriter(void *ptr, int len, VersionOptions vo) : len(len) {
		buf.begin = (uint8_t *)ptr;
		buf.first_length = len;
		vo.write(*this);
	}

	void serializeBytes( StringRef bytes ) {
		serializeBytes(bytes.begin(), bytes.size());
	}
	void serializeBytes(const void* data, int bytes) {
		valgrindCheck( data, bytes, "serializeBytes" );
		writeBytes(data, bytes);
	}
	template <class T>
	void serializeBinaryItem( const T& t ) {
		writeBytes(&t, sizeof(T));
	}

	uint64_t protocolVersion() const { return m_protocolVersion; }
	void setProtocolVersion(uint64_t pv) { m_protocolVersion = pv; }

private:
	int len;
	SplitBuffer buf;
	uint64_t m_protocolVersion;

	void writeBytes(const void *data, int wlen) {
		ASSERT(wlen <= len);
		buf.writeAndShrink(data, wlen);
		len -= wlen;
	}
};


class ArenaReader {
public:
	static const int isDeserializing = 1;
	typedef ArenaReader READER;

	const void* readBytes( int bytes ) {
		const char* b = begin;
		const char* e = b + bytes;
		ASSERT( e <= end );
		begin = e;
		return b;
	}

	const void* peekBytes( int bytes ) {
		ASSERT( begin + bytes <= end );
		return begin;
	}

	void serializeBytes(void* data, int bytes) {
		memcpy(data, readBytes(bytes), bytes);
	}
	
	const uint8_t* arenaRead( int bytes ) {
		return (const uint8_t*)readBytes(bytes);
	}

	template <class T>
	void serializeBinaryItem( T& t ) {
		t = *(T*)readBytes(sizeof(T));
	}

	template <class VersionOptions>
	ArenaReader( Arena const& arena, const StringRef& input, VersionOptions vo ) : m_pool(arena), check(NULL) {
		begin = (const char*)input.begin();
		end = begin + input.size();
		vo.read(*this);
	}

	Arena& arena() { return m_pool; }

	uint64_t protocolVersion() const { return m_protocolVersion; }
	void setProtocolVersion(uint64_t pv) { m_protocolVersion = pv; }

	bool empty() const { return begin == end; }

	void checkpoint() {
		check = begin;
	}

	void rewind() {
		ASSERT(check != NULL);
		begin = check;
		check = NULL;
	}

private:
	const char *begin, *end, *check;
	Arena m_pool;
	uint64_t m_protocolVersion;
};

class BinaryReader {
public:
	static const int isDeserializing = 1;
	typedef BinaryReader READER;

	const void* readBytes( int bytes ) {
		const char* b = begin;
		const char* e = b + bytes;
		ASSERT( e <= end );
		begin = e;
		return b;
	}

	void serializeBytes(void* data, int bytes) {
		memcpy(data, readBytes(bytes), bytes);
	}
	
	template <class T>
	void serializeBinaryItem( T& t ) {
		t = *(T*)readBytes(sizeof(T));
	}

	const uint8_t* arenaRead( int bytes ) {
		// Reads and returns the next bytes.
		// The returned pointer has the lifetime of this.arena()
		// Could be implemented zero-copy if [begin,end) was in this.arena() already; for now is a copy
		if (!bytes) return NULL;
		uint8_t* dat = new (arena()) uint8_t[ bytes ];
		serializeBytes( dat, bytes );
		return dat;
	}

	template <class VersionOptions>
	BinaryReader( const void* data, int length, VersionOptions vo ) {
		begin = (const char*)data;
		end = begin + length;
		vo.read(*this);
	}
	template <class VersionOptions>
	BinaryReader( const StringRef& s, VersionOptions vo ) { begin = (const char*)s.begin(); end = begin + s.size(); vo.read(*this); }
	template <class VersionOptions>
	BinaryReader( const std::string& v, VersionOptions vo ) { begin = v.c_str(); end = begin + v.size(); vo.read(*this); }

	Arena& arena() { return m_pool; }

	template <class T, class VersionOptions>
	static T fromStringRef( StringRef sr, VersionOptions vo ) {
		T t;
		BinaryReader r(sr, vo);
		r >> t;
		return t;
	}

	uint64_t protocolVersion() const { return m_protocolVersion; }
	void setProtocolVersion(uint64_t pv) { m_protocolVersion = pv; }

	void assertEnd() { ASSERT( begin == end ); }

	bool empty() const { return begin == end; }

private:
	const char *begin, *end;
	Arena m_pool;
	uint64_t m_protocolVersion;
};

struct SendBuffer {
	int bytes_written, bytes_sent;
	uint8_t const* data;
	SendBuffer* next;
};

struct PacketBuffer : SendBuffer, FastAllocated<PacketBuffer> {
	int reference_count;
	enum { DATA_SIZE = 4096 - 28 };
	uint8_t data[ DATA_SIZE ];

	PacketBuffer() : reference_count(1) {
		next = 0;
		bytes_written = bytes_sent = 0;
		((SendBuffer*)this)->data = data;
		static_assert( sizeof(PacketBuffer) == 4096, "PacketBuffer size mismatch" );
	}
	PacketBuffer* nextPacketBuffer() { return (PacketBuffer*)next; }
	void addref() { ++reference_count; }
	void delref() { if (!--reference_count) delete this; }
	int bytes_unwritten() const { return DATA_SIZE-bytes_written; }
};

struct PacketWriter {
	static const int isDeserializing = 0;
	typedef PacketWriter WRITER;

	PacketBuffer* buffer;
	struct ReliablePacket *reliable;  // NULL if this is unreliable; otherwise the last entry in the ReliablePacket::cont chain
	int length;
	uint64_t m_protocolVersion;

	// reliable is NULL if this is an unreliable packet, or points to a ReliablePacket.  PacketWriter is responsible
	//   for filling in reliable->buffer, ->cont, ->begin, and ->end, but not ->prev or ->next.
	template <class VersionOptions>
	PacketWriter(PacketBuffer* buf, ReliablePacket* reliable, VersionOptions vo) { init(buf, reliable); vo.read(*this); }

	void serializeBytes(const void* data, int bytes) {
		if (bytes <= buffer->bytes_unwritten()) {
			memcpy(buffer->data + buffer->bytes_written, data, bytes);
			buffer->bytes_written += bytes;
		} else {
			serializeBytesAcrossBoundary(data, bytes);
		}
	}
	void serializeBytesAcrossBoundary(const void* data, int bytes);
	void writeAhead( int bytes, struct SplitBuffer* );
	void nextBuffer();
	PacketBuffer* finish();
	int size() { return length; }

	void serializeBytes( StringRef bytes ) {
		serializeBytes(bytes.begin(), bytes.size());
	}
	template <class T>
	void serializeBinaryItem( const T& t ) {
		if (sizeof(T) <= buffer->bytes_unwritten()) {
			*(T*)(buffer->data + buffer->bytes_written) = t;
			buffer->bytes_written += sizeof(T);
		} else {
			serializeBytesAcrossBoundary(&t, sizeof(T));
		}
	}
	uint64_t protocolVersion() const { return m_protocolVersion; }
	void setProtocolVersion(uint64_t pv) { m_protocolVersion = pv; }
private:
	void init( PacketBuffer* buf, ReliablePacket* reliable );
};

struct ISerializeSource {
	virtual void serializePacketWriter( PacketWriter& ) const = 0;
	virtual void serializeBinaryWriter( BinaryWriter& ) const = 0;
};

template <class T>
struct MakeSerializeSource : ISerializeSource {
	virtual void serializePacketWriter( PacketWriter& w ) const { ((T const*)this)->serialize(w); }
	virtual void serializeBinaryWriter( BinaryWriter& w ) const { ((T const*)this)->serialize(w); }
};

template <class T>
struct SerializeSource : MakeSerializeSource<SerializeSource<T>> {
	T const& value;
	SerializeSource(T const& value) : value(value) {}
	template <class Ar> void serialize(Ar& ar) const { ar << value; }
};

template <class T>
struct SerializeBoolAnd : MakeSerializeSource<SerializeBoolAnd<T>> {
	bool b;
	T const& value;
	SerializeBoolAnd( bool b, T const& value ) : b(b), value(value) {}
	template <class Ar> void serialize(Ar& ar) const { ar << b << value; }
};

struct SerializeSourceRaw : MakeSerializeSource<SerializeSourceRaw> {
	StringRef data;
	SerializeSourceRaw(StringRef data) : data(data) {}
	template <class Ar> void serialize(Ar& ar) const { ar.serializeBytes(data); }
};

#endif
