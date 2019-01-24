/*
 * Arena.h
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

#ifndef FLOW_ARENA_H
#define FLOW_ARENA_H
#pragma once

#include "flow/FastAlloc.h"
#include "flow/FastRef.h"
#include "flow/Error.h"
#include "flow/Trace.h"
#include <algorithm>
#include <stdint.h>
#include <string>
#include <cstring>
#include <limits>
#include <set>
#include <type_traits>

// TrackIt is a zero-size class for tracking constructions, destructions, and assignments of instances
// of a class.  Just inherit TrackIt<T> from T to enable tracking of construction and destruction of
// T, and use the TRACKIT_ASSIGN(rhs) macro in any operator= definitions to enable assignment tracking.
//
// TrackIt writes to standard output because the trace log isn't available early in execution
// so applying TrackIt to StringRef or VectorRef, for example, would a segfault using the trace log.
//
// The template parameter enables TrackIt to be inherited multiple times in the ancestry
// of a class without producing an "inaccessible due to ambiguity" error.
template<class T>
struct TrackIt {
	typedef TrackIt<T> TrackItType;
	// Put TRACKIT_ASSIGN into any operator= functions for which you want assignments tracked
	#define TRACKIT_ASSIGN(o) *(TrackItType *)this = *(TrackItType *)&(o)

	// The type name T is in the TrackIt output so that objects that inherit TrackIt multiple times
	// can be tracked propertly, otherwise the create and delete addresses appear duplicative.
	// This function returns just the string "T]" parsed from the __PRETTY_FUNCTION__ macro.  There
	// doesn't seem to be a better portable way to do this.
	static const char * __trackit__type() {
		const char *s = __PRETTY_FUNCTION__ + sizeof(__PRETTY_FUNCTION__);
		while(*--s != '=');
		return s + 2;
	}

	TrackIt() {
		printf("TrackItCreate\t%s\t%p\t%s\n", __trackit__type(), this, platform::get_backtrace().c_str());
	}
	TrackIt(const TrackIt &o) : TrackIt() {}
	TrackIt(const TrackIt &&o) : TrackIt() {}
	TrackIt & operator=(const TrackIt &o) {
		printf("TrackItAssigned\t%s\t%p<%p\t%s\n", __trackit__type(), this, &o, platform::get_backtrace().c_str());
		return *this;
	}
	TrackIt & operator=(const TrackIt &&o) {
		return *this = (const TrackIt &)o;
	}
	~TrackIt() {
		printf("TrackItDestroy\t%s\t%p\n", __trackit__type(), this);
	}
};

class NonCopyable
{
  protected:
	NonCopyable () {}
	~NonCopyable () {} /// Protected non-virtual destructor
  private:
	NonCopyable (const NonCopyable &);
	NonCopyable & operator = (const NonCopyable &);
};

class Arena {
public:
	inline Arena();
	inline explicit Arena( size_t reservedSize );
	//~Arena();
	Arena(const Arena&);
	Arena(Arena && r) noexcept(true);
	Arena& operator=(const Arena&);
	Arena& operator=(Arena&&) noexcept(true);

	inline void dependsOn( const Arena& p );
	inline size_t getSize() const;

	inline bool hasFree( size_t size, const void *address );

	friend void* operator new ( size_t size, Arena& p );
	friend void* operator new[] ( size_t size, Arena& p );
//private:
	Reference<struct ArenaBlock> impl;
};

struct ArenaBlockRef {
	ArenaBlock* next;
	uint32_t nextBlockOffset;
};

struct ArenaBlock : NonCopyable, ThreadSafeReferenceCounted<ArenaBlock>
{
	enum {
		SMALL = 64,
		LARGE = 4097 // If size == used == LARGE, then use hugeSize, hugeUsed
	};

	enum { NOT_TINY = 255, TINY_HEADER = 6 };

	// int32_t referenceCount;	  // 4 bytes (in ThreadSafeReferenceCounted)
	uint8_t tinySize, tinyUsed;   // If these == NOT_TINY, use bigSize, bigUsed instead
	// if tinySize != NOT_TINY, following variables aren't used
	uint32_t bigSize, bigUsed;	  // include block header
	uint32_t nextBlockOffset;

	void delref() {
		if (delref_no_destroy())
			destroy();
	}

	bool isTiny() const { return tinySize != NOT_TINY; }
	int size() const { if (isTiny()) return tinySize; else return bigSize; }
	int used() const { if (isTiny()) return tinyUsed; else return bigUsed; }
	inline int unused() const { if (isTiny()) return tinySize-tinyUsed; else return bigSize-bigUsed; }
	const void* getData() const { return this; }
	const void* getNextData() const { return (const uint8_t*)getData() + used(); }
	size_t totalSize() {
		if (isTiny()) return size();

		size_t s = size();
		int o = nextBlockOffset;
		while (o) {
			ArenaBlockRef* r = (ArenaBlockRef*)((char*)getData() + o);
			s += r->next->totalSize();
			o = r->nextBlockOffset;
		}
		return s;
	}
	// just for debugging:
	void getUniqueBlocks(std::set<ArenaBlock*>& a) {
		a.insert(this);
		if (isTiny()) return;

		int o = nextBlockOffset;
		while (o) {
			ArenaBlockRef* r = (ArenaBlockRef*)((char*)getData() + o);
			r->next->getUniqueBlocks(a);
			o = r->nextBlockOffset;
		}
		return;
	}

	inline int addUsed( int bytes ) {
		if (isTiny()) {
			int t = tinyUsed;
			tinyUsed += bytes;
			return t;
		} else {
			int t = bigUsed;
			bigUsed += bytes;
			return t;
		}
	}

	void makeReference( ArenaBlock* next ) {
		ArenaBlockRef* r = (ArenaBlockRef*)((char*)getData() + bigUsed);
		r->next = next;
		r->nextBlockOffset = nextBlockOffset;
		nextBlockOffset = bigUsed;
		bigUsed += sizeof(ArenaBlockRef);
	}

	static void dependOn( Reference<ArenaBlock>& self, ArenaBlock* other ) {
		other->addref();
		if (!self || self->isTiny() || self->unused() < sizeof(ArenaBlockRef))
			create( SMALL, self )->makeReference(other);
		else
			self->makeReference( other );
	}

	static inline void* allocate( Reference<ArenaBlock>& self, int bytes ) {
		ArenaBlock* b = self.getPtr();
		if (!self || self->unused() < bytes)
			b = create( bytes, self );

		return (char*)b->getData() + b->addUsed(bytes);
	}

	// Return an appropriately-sized ArenaBlock to store the given data
	static ArenaBlock* create( int dataSize, Reference<ArenaBlock>& next ) {
		ArenaBlock* b;
		if (dataSize <= SMALL-TINY_HEADER && !next) {
			if (dataSize <= 16-TINY_HEADER) { b = (ArenaBlock*)FastAllocator<16>::allocate(); b->tinySize = 16; INSTRUMENT_ALLOCATE("Arena16"); }
			else if (dataSize <= 32-TINY_HEADER) { b = (ArenaBlock*)FastAllocator<32>::allocate(); b->tinySize = 32; INSTRUMENT_ALLOCATE("Arena32"); }
			else { b = (ArenaBlock*)FastAllocator<64>::allocate(); b->tinySize=64; INSTRUMENT_ALLOCATE("Arena64"); }
			b->tinyUsed = TINY_HEADER;

		} else {
			int reqSize = dataSize + sizeof(ArenaBlock);
			if (next) reqSize += sizeof(ArenaBlockRef);

			if (reqSize < LARGE) {
				// Each block should be larger than the previous block, up to a limit, to minimize allocations
				// Worst-case allocation pattern: 1 +10 +17 +42 +67 +170 +323 +681 +1348 +2728 +2210 +2211 (+1K +3K+1 +4K)*
				// Overhead: 4X for small arenas, 3X intermediate, 1.33X for large arenas
				int prevSize = next ? next->size() : 0;
				reqSize = std::max( reqSize, std::min( prevSize*2, std::max( LARGE-1, reqSize*4 ) ) );
			}

			if (reqSize < LARGE) {
				if (reqSize <= 128) { b = (ArenaBlock*)FastAllocator<128>::allocate(); b->bigSize = 128; INSTRUMENT_ALLOCATE("Arena128"); }
				else if (reqSize <= 256) { b = (ArenaBlock*)FastAllocator<256>::allocate(); b->bigSize = 256; INSTRUMENT_ALLOCATE("Arena256"); }
				else if (reqSize <= 512) { b = (ArenaBlock*)FastAllocator<512>::allocate(); b->bigSize = 512; INSTRUMENT_ALLOCATE("Arena512"); }
				else if (reqSize <= 1024) { b = (ArenaBlock*)FastAllocator<1024>::allocate(); b->bigSize = 1024; INSTRUMENT_ALLOCATE("Arena1024"); }
				else if (reqSize <= 2048) { b = (ArenaBlock*)FastAllocator<2048>::allocate(); b->bigSize = 2048; INSTRUMENT_ALLOCATE("Arena2048"); }
				else { b = (ArenaBlock*)FastAllocator<4096>::allocate(); b->bigSize = 4096; INSTRUMENT_ALLOCATE("Arena4096"); }
				b->tinySize = b->tinyUsed = NOT_TINY;
				b->bigUsed = sizeof(ArenaBlock);
			} else {
				#ifdef ALLOC_INSTRUMENTATION
					allocInstr[ "ArenaHugeKB" ].alloc( (reqSize+1023)>>10 );
				#endif
				b = (ArenaBlock*)new uint8_t[ reqSize ];
				b->tinySize = b->tinyUsed = NOT_TINY;
				b->bigSize = reqSize;
				b->bigUsed = sizeof(ArenaBlock);

				// If the new block has less free space than the old block, make the old block depend on it
				if (next && !next->isTiny() && next->unused() >= reqSize-dataSize) {
					b->nextBlockOffset = 0;
					b->setrefCountUnsafe(1);
					next->makeReference(b);
					return b;
				}
			}
			b->nextBlockOffset = 0;
			if (next) b->makeReference(next.getPtr());
		}
		b->setrefCountUnsafe(1);
		next.setPtrUnsafe(b);
		return b;
	}

	inline void destroy();

	void destroyLeaf() {
		if (isTiny()) {
			if (tinySize <= 16) { FastAllocator<16>::release(this); INSTRUMENT_RELEASE("Arena16");}
			else if (tinySize <= 32) { FastAllocator<32>::release(this); INSTRUMENT_RELEASE("Arena32"); }
			else { FastAllocator<64>::release(this); INSTRUMENT_RELEASE("Arena64"); }
		} else {
			if (bigSize <= 128) { FastAllocator<128>::release(this); INSTRUMENT_RELEASE("Arena128"); }
			else if (bigSize <= 256) { FastAllocator<256>::release(this); INSTRUMENT_RELEASE("Arena256"); }
			else if (bigSize <= 512) { FastAllocator<512>::release(this); INSTRUMENT_RELEASE("Arena512"); }
			else if (bigSize <= 1024) { FastAllocator<1024>::release(this); INSTRUMENT_RELEASE("Arena1024"); }
			else if (bigSize <= 2048) { FastAllocator<2048>::release(this); INSTRUMENT_RELEASE("Arena2048"); }
			else if (bigSize <= 4096) { FastAllocator<4096>::release(this); INSTRUMENT_RELEASE("Arena4096"); }
			else {
				#ifdef ALLOC_INSTRUMENTATION
					allocInstr[ "ArenaHugeKB" ].dealloc( (bigSize+1023)>>10 );
				#endif
				delete[] (uint8_t*)this;
			}
		}
	}
private:
	static void* operator new(size_t s);  // not implemented
};

inline Arena::Arena() : impl( NULL ) {}
inline Arena::Arena(size_t reservedSize) : impl( 0 ) {
	UNSTOPPABLE_ASSERT( reservedSize < std::numeric_limits<int>::max() );
	if (reservedSize)
		ArenaBlock::create((int)reservedSize,impl);
}
inline Arena::Arena( const Arena& r ) : impl( r.impl ) {}
inline Arena::Arena(Arena && r) noexcept(true) : impl(std::move(r.impl)) {}
inline Arena& Arena::operator=(const Arena& r) {
	impl = r.impl;
	return *this;
}
inline Arena& Arena::operator=(Arena&& r) noexcept(true) {
	impl = std::move(r.impl);
	return *this;
}
inline void Arena::dependsOn( const Arena& p ) {
	if (p.impl)
		ArenaBlock::dependOn( impl, p.impl.getPtr() );
}
inline size_t Arena::getSize() const { return impl ? impl->totalSize() : 0; }
inline bool Arena::hasFree( size_t size, const void *address ) { return impl && impl->unused() >= size && impl->getNextData() == address; }
inline void* operator new ( size_t size, Arena& p ) {
	UNSTOPPABLE_ASSERT( size < std::numeric_limits<int>::max() );
	return ArenaBlock::allocate( p.impl, (int)size );
}
inline void operator delete( void*, Arena& p ) {}
inline void* operator new[] ( size_t size, Arena& p ) {
	UNSTOPPABLE_ASSERT( size < std::numeric_limits<int>::max() );
	return ArenaBlock::allocate( p.impl, (int)size );
}
inline void operator delete[]( void*, Arena& p ) {}

template <class Archive>
inline void load( Archive& ar, Arena& p ) {
	p = ar.arena();
}
template <class Archive>
inline void save( Archive& ar, const Arena& p ) {
	// No action required
}

//#define STANDALONE_ALWAYS_COPY

template <class T>
class Standalone : private Arena, public T {
public:
	// T must have no destructor
	Arena& arena() { return *(Arena*)this; }
	const Arena& arena() const { return *(const Arena*)this; }

	T& contents() { return *(T*)this; }
	T const& contents() const { return *(T const*)this; }

	Standalone() {}
	Standalone( const T& t ) : Arena( t.expectedSize() ), T( arena(), t ) {}
	Standalone<T>& operator=( const T& t ) {
		Arena old = std::move( arena() );	// We want to defer the destruction of the arena until after we have copied t, in case it cross-references our previous value
		*(Arena*)this = Arena(t.expectedSize());
		*(T*)this = T( arena(), t );
		return *this;
	}

// Always-copy mode was meant to make alloc instrumentation more useful by making allocations occur at the final resting place of objects leaked
// It doesn't actually work because some uses of Standalone things assume the object's memory will not change on copy or assignment
#ifdef STANDALONE_ALWAYS_COPY
	// Treat Standalone<T>'s as T's in construction and assignment so the memory is copied
	Standalone( const T& t, const Arena& arena ) : Standalone(t) {}
	Standalone( const Standalone<T> & t ) : Standalone((T const&)t) {}
	Standalone( const Standalone<T> && t ) : Standalone((T const&)t) {}
	Standalone<T>& operator=( const Standalone<T> &&t ) {
		*this = (T const&)t;
		return *this;
	}
	Standalone<T>& operator=( const Standalone<T> &t ) {
		*this = (T const&)t;
		return *this;
	}
#else
	Standalone( const T& t, const Arena& arena ) : Arena( arena ), T( t ) {}
	Standalone( const Standalone<T> & t ) : Arena((Arena const&)t), T((T const&)t) {}
	Standalone<T>& operator=( const Standalone<T> & t ) {
		*(Arena*)this = (Arena const&)t;
		*(T*)this = (T const&)t;
		return *this;
	}
#endif

	template <class Archive>
	void serialize(Archive& ar) {
		// FIXME: something like BinaryReader(ar) >> arena >> *(T*)this; to guarantee standalone arena???
		//T tmp;
		//ar >> tmp;
		//*this = tmp;
		serializer(ar, (*(T*)this), arena());
	}

	/*static Standalone<T> fakeStandalone( const T& t ) {
		Standalone<T> x;
		*(T*)&x = t;
		return x;
	}*/
private:
	template <class U> Standalone( Standalone<U> const& );  // unimplemented
	template <class U> Standalone<T> const& operator=( Standalone<U> const& );  // unimplemented
};

extern std::string format(const char* form, ...);

#pragma pack( push, 4 )
class StringRef {
public:
	StringRef() : data(0), length(0) {}
	StringRef( Arena& p, const StringRef& toCopy ) : data( new (p) uint8_t[toCopy.size()] ), length( toCopy.size() ) {
		memcpy( (void*)data, toCopy.data, length );
	}
	StringRef( Arena& p, const std::string& toCopy ) : length( (int)toCopy.size() ) {
		UNSTOPPABLE_ASSERT( toCopy.size() <= std::numeric_limits<int>::max());
		data = new (p) uint8_t[toCopy.size()];
		if (length) memcpy( (void*)data, &toCopy[0], length );
	}
	StringRef( Arena& p, const uint8_t* toCopy, int length ) : data( new (p) uint8_t[length] ), length(length) {
		memcpy( (void*)data, toCopy, length );
	}
	StringRef( const uint8_t* data, int length ) : data(data), length(length) {}
	StringRef( const std::string& s ) : data((const uint8_t*)s.c_str()), length((int)s.size()) {
		if (s.size() > std::numeric_limits<int>::max()) abort();
	}
	//StringRef( const StringRef& p );

	const uint8_t* begin() const { return data; }
	const uint8_t* end() const { return data + length; }
	int size() const { return length; }

	uint8_t operator[](int i) const { return data[i]; }

	StringRef substr(int start) const { return StringRef( data + start, length - start ); }
	StringRef substr(int start, int size) const { return StringRef( data + start, size ); }
	bool startsWith( const StringRef& s ) const { return size() >= s.size() && !memcmp(begin(), s.begin(), s.size()); }
	bool endsWith( const StringRef& s ) const { return size() >= s.size() && !memcmp(end()-s.size(), s.begin(), s.size()); }

	StringRef withPrefix( const StringRef& prefix, Arena& arena ) const {
		uint8_t* s = new (arena) uint8_t[ prefix.size() + size() ];
		memcpy(s, prefix.begin(), prefix.size());
		memcpy(s+prefix.size(), begin(), size());
		return StringRef(s,prefix.size() + size());
	}

	StringRef withSuffix( const StringRef& suffix, Arena& arena ) const {
		uint8_t* s = new (arena) uint8_t[ suffix.size() + size() ];
		memcpy(s, begin(), size());
		memcpy(s+size(), suffix.begin(), suffix.size());
		return StringRef(s,suffix.size() + size());
	}

	Standalone<StringRef> withPrefix( const StringRef& prefix ) const {
		Standalone<StringRef> r;
		r.contents() = withPrefix(prefix, r.arena());
		return r;
	}

	Standalone<StringRef> withSuffix( const StringRef& suffix ) const {
		Standalone<StringRef> r;
		r.contents() = withSuffix(suffix, r.arena());
		return r;
	}

	StringRef removePrefix( const StringRef& s ) const {
		// pre: startsWith(s)
		UNSTOPPABLE_ASSERT( s.size() <= size() );  //< In debug mode, we could check startsWith()
		return substr( s.size() );
	}

	StringRef removeSuffix( const StringRef& s ) const {
		// pre: endsWith(s)
		UNSTOPPABLE_ASSERT( s.size() <= size() );  //< In debug mode, we could check endsWith()
		return substr( 0, size() - s.size() );
	}

	std::string toString() const { return std::string( (const char*)data, length ); }
	std::string printable() const {
		std::string s;
		for (int i = 0; i<length; i++) {
			uint8_t b = (*this)[i];
			if (b >= 32 && b < 127 && b != '\\') s += (char)b;
			else if (b == '\\') s += "\\\\";
			else s += format("\\x%02x", b);
		}
		return s;
	}

	std::string toHexString(int limit = -1) const {
		if(limit < 0)
			limit = length;
		if(length > limit) {
			// If limit is high enough split it so that 2/3 of limit is used to show prefix bytes and the rest is used for suffix bytes
			if(limit >= 9) {
				int suffix = limit / 3;
				return substr(0, limit - suffix).toHexString() + "..." + substr(length - suffix, suffix).toHexString() + format(" [%d bytes]", length);
			}
			return substr(0, limit).toHexString() + format("...[%d]", length);
		}

		std::string s;
		s.reserve(length * 7);
		for (int i = 0; i<length; i++) {
			uint8_t b = (*this)[i];
			if(isalnum(b))
				s.append(format("%02x (%c) ", b, b));
			else
				s.append(format("%02x ", b));
		}
		if(s.size() > 0)
			s.resize(s.size() - 1);
		return s;
	}

	int expectedSize() const { return size(); }

	int compare( StringRef const& other ) const {
		int c = memcmp( begin(), other.begin(), std::min( size(), other.size() ) );
		if (c!=0) return c;
		return size() - other.size();
	}

	// Removes bytes from begin up to and including the sep string, returns StringRef of the part before sep
	StringRef eat(StringRef sep) {
		for(int i = 0, iend = size() - sep.size(); i <= iend; ++i) {
			if(sep.compare(substr(i, sep.size())) == 0) {
				StringRef token = substr(0, i);
				*this = substr(i + sep.size());
				return token;
			}
		}
		return eat();
	}
	StringRef eat() {
		StringRef r = *this;
		*this = StringRef();
		return r;
	}
	StringRef eat(const char *sep) {
		return eat(StringRef((const uint8_t *)sep, strlen(sep)));
	}

private:
	// Unimplemented; blocks conversion through std::string
	StringRef( char* );

	const uint8_t* data;
	int length;
};
#pragma pack( pop )

#define LiteralStringRef( str ) StringRef( (const uint8_t*)(str), sizeof((str))-1 )

// makeString is used to allocate a Standalone<StringRef> of a known length for later
// mutation (via mutateString).  If you need to append to a string of unknown length,
// consider factoring StringBuffer from DiskQueue.actor.cpp.
inline static Standalone<StringRef> makeString( int length ) {
	Standalone<StringRef> returnString;
	uint8_t *outData = new (returnString.arena()) uint8_t[length];
	((StringRef&)returnString) = StringRef(outData, length);
	return returnString;
}

inline static StringRef makeString( int length, Arena& arena ) {
	uint8_t *outData = new (arena) uint8_t[length];
	return StringRef(outData, length);
}

// mutateString() simply casts away const and returns a pointer that can be used to mutate the
// contents of the given StringRef (it will also accept Standalone<StringRef>).  Obviously this
// is only legitimate if you know where the StringRef's memory came from and that it is not shared!
inline static uint8_t* mutateString( StringRef& s ) { return const_cast<uint8_t*>(s.begin()); }

template <class Archive>
inline void load( Archive& ar, StringRef& value ) {
	uint32_t length;
	ar >> length;
	value = StringRef(ar.arenaRead(length), length);
}
template <class Archive>
inline void save( Archive& ar, const StringRef& value ) {
	ar << (uint32_t)value.size();
	ar.serializeBytes( value.begin(), value.size() );
}
inline bool operator == (const StringRef& lhs, const StringRef& rhs ) {
	return lhs.size() == rhs.size() && !memcmp(lhs.begin(), rhs.begin(), lhs.size());
}
inline bool operator < ( const StringRef& lhs, const StringRef& rhs ) {
	int c = memcmp( lhs.begin(), rhs.begin(), std::min(lhs.size(), rhs.size()) );
	if (c!=0) return c<0;
	return lhs.size() < rhs.size();
}
inline bool operator > ( const StringRef& lhs, const StringRef& rhs ) {
	int c = memcmp( lhs.begin(), rhs.begin(), std::min(lhs.size(), rhs.size()) );
	if (c!=0) return c>0;
	return lhs.size() > rhs.size();
}
inline bool operator != (const StringRef& lhs, const StringRef& rhs ) { return !(lhs==rhs); }
inline bool operator <= ( const StringRef& lhs, const StringRef& rhs ) { return !(lhs>rhs); }
inline bool operator >= ( const StringRef& lhs, const StringRef& rhs ) { return !(lhs<rhs); }

// This trait is used by VectorRef to determine if it should just memcpy the vector contents.
// FIXME:  VectorRef really should use std::is_trivially_copyable for this BUT that is not implemented
// in gcc c++0x so instead we will use this custom trait which defaults to std::is_trivial, which
// handles most situations but others will have to be specialized.
template <typename T>
struct memcpy_able : std::is_trivial<T> {};

template <>
struct memcpy_able<UID> : std::integral_constant<bool, true> {};

template <class T>
class VectorRef {
public:
	// T must be trivially destructible (and copyable)!
	VectorRef() : data(0), m_size(0), m_capacity(0) {}

	// Arena constructor for non-Ref types, identified by memcpy_able
	template<class T2 = T>
	VectorRef( Arena& p, const VectorRef<T>& toCopy, typename std::enable_if<memcpy_able<T2>::value, int>::type = 0)
	  : data( (T*)new (p) uint8_t[sizeof(T)*toCopy.size()] ),
	    m_size( toCopy.size() ), m_capacity( toCopy.size() )
	{
		memcpy(data, toCopy.data, m_size * sizeof(T));
	}

	// Arena constructor for Ref types, which must have an Arena constructor
	template<class T2 = T>
	VectorRef( Arena& p, const VectorRef<T>& toCopy, typename std::enable_if<!memcpy_able<T2>::value, int>::type = 0)
		: data( (T*)new (p) uint8_t[sizeof(T)*toCopy.size()] ),
		  m_size( toCopy.size() ), m_capacity( toCopy.size() )
	{
		for(int i=0; i<m_size; i++)
			new (&data[i]) T(p, toCopy[i]);
	}

	VectorRef( T* data, int size ) : data(data), m_size(size), m_capacity(size) {}
	VectorRef( T* data, int size, int capacity ) : data(data), m_size(size), m_capacity(capacity) {}
	//VectorRef( const VectorRef<T>& toCopy ) : data( toCopy.data ), m_size( toCopy.m_size ), m_capacity( toCopy.m_capacity ) {}
	//VectorRef<T>& operator=( const VectorRef<T>& );

	const T* begin() const { return data; }
	const T* end() const { return data + m_size; }
	T const& front() const { return *begin(); }
	T const& back() const { return end()[-1]; }
	int size() const { return m_size; }
	bool empty() const { return m_size == 0; }
	const T& operator[](int i) const { return data[i]; }

	std::reverse_iterator<const T*> rbegin() const { return std::reverse_iterator<const T*>( end() ); }
	std::reverse_iterator<const T*> rend() const { return std::reverse_iterator<const T*>( begin() ); }

	VectorRef slice(int begin, int end) const { return VectorRef(data+begin, end-begin); }

	bool operator == ( VectorRef<T> const& rhs ) const {
		if (size() != rhs.size()) return false;
		for(int i=0; i<m_size; i++)
			if ( (*this)[i] != rhs[i] )
				return false;
		return true;
	}

	// Warning: Do not mutate a VectorRef that has previously been copy constructed or assigned,
	// since copies will share data
	T* begin() { return data; }
	T* end() { return data + m_size; }
	T& front() { return *begin(); }
	T& back() { return end()[-1]; }
	T& operator[](int i) { return data[i]; }
	void push_back( Arena& p, const T& value ) {
		if (m_size + 1 > m_capacity) reallocate(p, m_size+1);
		new (&data[m_size]) T(value);
		m_size++;
	}
	// invokes the "Deep copy constructor" T(Arena&, const T&) moving T entirely into arena
	void push_back_deep( Arena& p, const T& value ) {
		if (m_size + 1 > m_capacity) reallocate(p, m_size+1);
		new (&data[m_size]) T(p, value);
		m_size++;
	}
	void append( Arena& p, const T* begin, int count ) {
		if (m_size + count > m_capacity) reallocate(p, m_size + count);
		memcpy( data+m_size, begin, sizeof(T)*count );
		m_size += count;
	}
	template <class It>
	void append_deep( Arena& p, It begin, int count ) {
		if (m_size + count > m_capacity) reallocate(p, m_size + count);
		for(int i=0; i<count; i++)
			new (&data[m_size+i]) T( p, *begin++ );
		m_size += count;
	}
	void pop_back() { m_size--; }

	void pop_front( int count ) {
		count = std::min(m_size, count);

		data += count;
		m_size -= count;
		m_capacity -= count;
	}

	void resize( Arena& p, int size ) {
		if (size > m_capacity) reallocate(p, size);
		for(int i=m_size; i<size; i++)
			new (&data[i]) T();
		m_size = size;
	}

	void reserve( Arena& p, int size ) {
		if (size > m_capacity) reallocate(p, size);
	}

	// expectedSize() for non-Ref types, identified by memcpy_able
	template<class T2 = T>
	typename std::enable_if<memcpy_able<T2>::value, size_t>::type expectedSize() const {
		return sizeof(T)*m_size;
	}

	// expectedSize() for Ref types, which must in turn have expectedSize() implemented.
	template<class T2 = T>
	typename std::enable_if<!memcpy_able<T2>::value, size_t>::type expectedSize() const
	{
		size_t t = sizeof(T)*m_size;
		for(int i=0; i<m_size; i++)
			t += data[i].expectedSize();
		return t;
	}

	int capacity() const {
		return m_capacity;
	}

	void extendUnsafeNoReallocNoInit(int amount) {
		m_size += amount;
	}

private:
	T* data;
	int m_size, m_capacity;

	void reallocate(Arena& p, int requiredCapacity) {
		requiredCapacity = std::max( m_capacity*2, requiredCapacity );
		// SOMEDAY: Maybe we are right at the end of the arena and can expand cheaply
		T* newData = (T*)new (p) uint8_t[ requiredCapacity * sizeof(T) ];
		memcpy(newData, data, m_size*sizeof(T));
		data = newData;
		m_capacity = requiredCapacity;
	}
};
template <class Archive, class T>
inline void load( Archive& ar, VectorRef<T>& value ) {
	// FIXME: range checking for length, here and in other serialize code
	uint32_t length;
	ar >> length;
	UNSTOPPABLE_ASSERT( length*sizeof(T) < (100<<20) );
	// SOMEDAY: Can we avoid running constructors for all the values?
	value.resize(ar.arena(), length);
	for(uint32_t i=0; i<length; i++)
		ar >> value[i];
}
template <class Archive, class T>
inline void save( Archive& ar, const VectorRef<T>& value ) {
	uint32_t length = value.size();
	ar << length;
	for(uint32_t i=0; i<length; i++)
		ar << value[i];
}

 void ArenaBlock::destroy() {
	// If the stack never contains more than one item, nothing will be allocated from stackArena.
	// If stackArena is used, it will always be a linked list, so destroying *it* will not create another arena
	ArenaBlock* tinyStack = this;
	Arena stackArena;
	VectorRef<ArenaBlock*> stack( &tinyStack, 1 );

	while (stack.size()) {
		ArenaBlock* b = stack.end()[-1];
		stack.pop_back();

		if (!b->isTiny()) {
			int o = b->nextBlockOffset;
			while (o) {
				ArenaBlockRef* br = (ArenaBlockRef*)((char*)b->getData() + o);
				if (br->next->delref_no_destroy())
					stack.push_back( stackArena, br->next );
				o = br->nextBlockOffset;
			}
		}
		b->destroyLeaf();
	}
}

#endif
