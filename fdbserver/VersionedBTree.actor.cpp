/*
 * VersionedBTree.actor.cpp
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

#include "flow/flow.h"
#include "fdbserver/IVersionedStore.h"
#include "fdbserver/IPager.h"
#include "fdbclient/Tuple.h"
#include "flow/serialize.h"
#include "flow/genericactors.actor.h"
#include "flow/UnitTest.h"
#include "fdbserver/MemoryPager.h"
#include "fdbserver/IndirectShadowPager.h"
#include <map>
#include <vector>
#include "fdbclient/CommitTransaction.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/DeltaTree.h"
#include <string.h>
#include "flow/actorcompiler.h"
#include <cinttypes>

// TODO: Move this to a flow header once it is mature.
struct SplitStringRef {
	StringRef a;
	StringRef b;

	SplitStringRef(StringRef a = StringRef(), StringRef b = StringRef()) : a(a), b(b) {
	}

	SplitStringRef(Arena &arena, const SplitStringRef &toCopy)
	  : a(toStringRef(arena)), b() {
	}

	SplitStringRef prefix(int len) const {
		if(len <= a.size()) {
			return SplitStringRef(a.substr(0, len));
		}
		len -= a.size();
		return SplitStringRef(a, b.substr(0, len));
	}

	StringRef toStringRef(Arena &arena) const {
		StringRef c = makeString(size(), arena);
		memcpy(mutateString(c), a.begin(), a.size());
		memcpy(mutateString(c) + a.size(), b.begin(), b.size());
		return c;
	}

	Standalone<StringRef> toStringRef() const {
		Arena a;
		return Standalone<StringRef>(toStringRef(a), a);
	}

	int size() const {
		return a.size() + b.size();
	}

	int expectedSize() const {
		return size();
	}

	std::string toString() const {
		return format("%s%s", a.toString().c_str(), b.toString().c_str());
	}

	std::string toHexString() const {
		return format("%s%s", a.toHexString().c_str(), b.toHexString().c_str());
	}

	struct const_iterator {
		const uint8_t *ptr;
		const uint8_t *end;
		const uint8_t *next;

		inline bool operator==(const const_iterator &rhs) const {
			return ptr == rhs.ptr;
		}

		inline const_iterator & operator++() {
			++ptr;
			if(ptr == end) {
				ptr = next;
			}
			return *this;
		}

		inline const_iterator & operator+(int n) {
			ptr += n;
			if(ptr >= end) {
				ptr = next + (ptr - end);
			}
			return *this;
		}

		inline uint8_t operator *() const {
			return *ptr;
		}
	};

	inline const_iterator begin() const {
		return {a.begin(), a.end(), b.begin()};
	}

	inline const_iterator end() const {
		return {b.end()};
	}

	template<typename StringT>
	int compare(const StringT &rhs) const {
		auto j = begin();
		auto k = rhs.begin();
		auto jEnd = end();
		auto kEnd = rhs.end();

		while(j != jEnd && k != kEnd) {
			int cmp = *j - *k;
			if(cmp != 0) {
				return cmp;
			}
		}

		// If we've reached the end of *this, then values are equal if rhs is also exhausted, otherwise *this is less than rhs
		if(j == jEnd) {
			return k == kEnd ? 0 : -1;
		}

		return 1;
	}

};

#define STR(x) LiteralStringRef(x)
struct RedwoodRecordRef {
	typedef uint8_t byte;

	RedwoodRecordRef(KeyRef key = KeyRef(), Version ver = 0, Optional<ValueRef> value = {}, uint32_t chunkTotal = 0, uint32_t chunkStart = 0)
		: key(key), version(ver), value(value), chunk({chunkTotal, chunkStart})
	{
	}

	RedwoodRecordRef(Arena &arena, const RedwoodRecordRef &toCopy)
	  : key(arena, toCopy.key), version(toCopy.version), chunk(toCopy.chunk)
	{
		if(toCopy.value.present()) {
			if(toCopy.localValue()) {
				setPageID(toCopy.getPageID());
			}
			else {
				value = ValueRef(arena, toCopy.value.get());
			}
		}
	}

	RedwoodRecordRef(KeyRef key, Optional<ValueRef> value, const byte intFields[14])
		: key(key), value(value)
	{
		deserializeIntFields(intFields);
	}

	RedwoodRecordRef(const RedwoodRecordRef &toCopy) : key(toCopy.key), version(toCopy.version), chunk(toCopy.chunk) {
		if(toCopy.value.present()) {
			if(toCopy.localValue()) {
				setPageID(toCopy.getPageID());
			}
			else {
				value = toCopy.value;
			}
		}
	}

	RedwoodRecordRef & operator= (const RedwoodRecordRef &toCopy) {
		key = toCopy.key;
		version = toCopy.version;
		chunk = toCopy.chunk;
		if(toCopy.value.present()) {
			if(toCopy.localValue()) {
				setPageID(toCopy.getPageID());
			}
			else {
				value = toCopy.value;
			}
		}

		return *this;
	}

	bool localValue() const {
		return value.get().begin() == bigEndianPageIDSpace;
	}

	// RedwoodRecordRefs are used for both internal and leaf pages of the BTree.
	// Boundary records in internal pages are made from leaf records.
	// These functions make creating and working with internal page records more convenient.
	inline LogicalPageID getPageID() const {
		ASSERT(value.present());
		return bigEndian32(*(LogicalPageID *)value.get().begin());
	}

	inline void setPageID(LogicalPageID id) {
		*(LogicalPageID *)bigEndianPageIDSpace = bigEndian32(id);
		value = ValueRef(bigEndianPageIDSpace, sizeof(bigEndianPageIDSpace));
	}

	inline RedwoodRecordRef withPageID(LogicalPageID id) const {
		RedwoodRecordRef rec(key, version, {}, chunk.total, chunk.start);
		rec.setPageID(id);
		return rec;
	}

	inline RedwoodRecordRef withoutValue() const {
		return RedwoodRecordRef(key, version, {}, chunk.total, chunk.start);
	}

	// Returns how many bytes are in common between the integer fields of *this and other, assuming that 
	// all values are BigEndian, version is 64 bits, chunk total is 24 bits, and chunk start is 24 bits
	int getCommonIntFieldPrefix(const RedwoodRecordRef &other) const {
		if(version != other.version) {
			return clzll(version ^ other.version) >> 3;
		}

		if(chunk.total != other.chunk.total) {
			// the -1 is because we are only considering the lower 3 bytes
			return 8 + (clz(chunk.total ^ other.chunk.total) >> 3) - 1;
		}
		
		if(chunk.start != other.chunk.start) {
			// the -1 is because we are only considering the lower 3 bytes
			return 11 + (clz(chunk.start ^ other.chunk.start) >> 3) - 1;
		}

		return 14;
	}

	// Truncate (key, version, chunk.total, chunk.start) tuple to len bytes.
	void truncate(int len) {
		if(len <= key.size()) {
			key = key.substr(0, len);
			version = 0;
			chunk.total = 0;
			chunk.start = 0;
		}
		else {
			byte fields[intFieldArraySize];
			serializeIntFields(fields);
			int end = len - key.size();
			for(int i = intFieldArraySize - 1; i >= end; --i) {
				fields[i] = 0;
			}
		}
	}

	// Find the common prefix between two records, assuming that the first
	// skip bytes are the same.
	inline int getCommonPrefixLen(const RedwoodRecordRef &other, int skip) const {
		int skipStart = std::min(skip, key.size());
		int common = skipStart + commonPrefixLength(key.begin() + skipStart, other.key.begin() + skipStart, std::min(other.key.size(), key.size()) - skipStart);

		if(common == key.size() && key.size() == other.key.size()) {
			common += getCommonIntFieldPrefix(other);
		}

		return common;
	}

	static const int intFieldArraySize = 14;

	// Write big endian values of version (64 bits), total (24 bits), and start (24 bits) fields
	// to an array of 14 bytes
	void serializeIntFields(byte *dst) const {
		*(uint32_t *)(dst + 10) = bigEndian32(chunk.start);
		*(uint32_t *)(dst + 7) = bigEndian32(chunk.total);
		*(uint64_t *)dst = bigEndian64(version);
	}

	// Initialize int fields from the array format that serializeIntFields produces
	void deserializeIntFields(const byte *src) {
		version = bigEndian64(*(uint64_t *)src);
		chunk.total = bigEndian32(*(uint32_t *)(src + 7)) & 0xffffff;
		chunk.start = bigEndian32(*(uint32_t *)(src + 10)) & 0xffffff;
	}

	// TODO: Use SplitStringRef (unless it ends up being slower)
	KeyRef key;
	Optional<ValueRef> value;
	Version version;
	struct {
		uint32_t total;
		// TODO:  Change start to chunk number.
		uint32_t start;
	} chunk;

	// If the value is a page ID it will be stored here
	uint8_t bigEndianPageIDSpace[sizeof(LogicalPageID)];

	int expectedSize() const {
		return key.expectedSize() + value.expectedSize();
	}

	bool isMultiPart() const {
		return chunk.total != 0;
	}

	// Generate a kv shard from a complete kv
	RedwoodRecordRef split(int start, int len) {
		ASSERT(!isMultiPart());
		return RedwoodRecordRef(key, version, value.get().substr(start, len), value.get().size(), start);
	}

	class Writer {
	public:
		Writer(byte *ptr) : wptr(ptr) {}

		byte *wptr;

		template<typename T> void write(const T &in) {
			*(T *)wptr = in;
			wptr += sizeof(T);
		}

		// Write a big endian 1 or 2 byte integer using the high bit of the first byte as an "extension" bit.
		// Values > 15 bits in length are not valid input but this is not checked for.
		void writeVarInt(int x) {
			if(x >= 128) {
				*wptr++ = (uint8_t)( (x >> 8) | 0x80 );
			}
			*wptr++ = (uint8_t)x;
		}

		void writeString(StringRef s) {
			memcpy(wptr, s.begin(), s.size());
			wptr += s.size();
		}

	};

	class Reader {
	public:
		Reader(const void *ptr) : rptr((const byte *)ptr) {}

		const byte *rptr;

		template<typename T> T read() {
			T r = *(const T *)rptr;
			rptr += sizeof(T);
			return r;
		}

		// Read a big endian 1 or 2 byte integer using the high bit of the first byte as an "extension" bit.
		int readVarInt() {
			int x = *rptr++;
			// If the high bit is set
			if(x & 0x80) {
				// Clear the high bit
				x &= 0x7f;
				// Shift low byte left
				x <<= 8;
				// Read the new low byte and OR it in
				x |= *rptr++;
			}

			return x;
		}

		StringRef readString(int len) {
			StringRef s(rptr, len);
			rptr += len;
			return s;
		}

		const byte * readBytes(int len) {
			const byte *b = rptr;
			rptr += len;
			return b;
		}
	};

#pragma pack(push,1)
	struct Delta {

		// Serialized Format
		//
		// 1 byte for Flags + a 4 bit length
		//    borrow source is prev ancestor - 0 or 1
		//    has_key_suffix
		//    has_value
		//    has_version
		//    other_fields suffix len - 4 bits
		//
		// If has value and value is not 4 bytes
		//    1 byte value length
		//
		// 1 or 2 bytes for Prefix Borrow Length (hi bit indicates presence of second byte)
		//
		// IF has_key_suffix is set
		//    1 or 2 bytes for Key Suffix Length
		//
		// Key suffix bytes
		// Meta suffix bytes
		// Value bytes
		//
		// For a series of RedwoodRecordRef's containing shards of the same KV pair where the key size is < 104 bytes,
		// the overhead per middle chunk is 7 bytes:
		//   4 bytes of child pointers in the DeltaTree Node
		//   1 flag byte
		//   1 prefix borrow length byte
		//   1 meta suffix byte describing chunk start position
 
		enum EFlags {
			PREFIX_SOURCE = 0x80,
			HAS_KEY_SUFFIX = 0x40,
			HAS_VALUE = 0x20,
			HAS_VERSION = 0x10,
			INT_FIELD_SUFFIX_BITS = 0x0f
		};

		uint8_t flags;

		inline byte * data() {
			return (byte *)(this + 1);
		}

		inline const byte * data() const {
			return (const byte *)(this + 1);
		}

		void setPrefixSource(bool val) {
			if(val) {
				flags |= PREFIX_SOURCE;
			}
			else {
				flags &= ~PREFIX_SOURCE;
			}
		}

		bool getPrefixSource() const {
			return flags & PREFIX_SOURCE;
		}

		RedwoodRecordRef apply(const RedwoodRecordRef &base, Arena &arena) const {
			Reader r(data());

			int intFieldSuffixLen = flags & INT_FIELD_SUFFIX_BITS;
			int prefixLen = r.readVarInt();
			int valueLen = (flags & HAS_VALUE) ? r.read<uint8_t>() : 0;

			StringRef k;

			int keyPrefixLen = std::min(prefixLen, base.key.size());
			int intFieldPrefixLen = prefixLen - keyPrefixLen;
			int keySuffixLen = (flags & HAS_KEY_SUFFIX) ? r.readVarInt() : 0;

			if(keySuffixLen > 0) {
				k = makeString(keyPrefixLen + keySuffixLen, arena);
				memcpy(mutateString(k), base.key.begin(), keyPrefixLen);
				memcpy(mutateString(k) + keyPrefixLen, r.readString(keySuffixLen).begin(), keySuffixLen);
			}
			else {
				k = base.key.substr(0, keyPrefixLen);
			}

			// Now decode the integer fields
			const byte *intFieldSuffix = r.readBytes(intFieldSuffixLen);

			// Create big endian array in which to reassemble the integer fields from prefix and suffix bytes
			byte intFields[intFieldArraySize];

			// If borrowing any bytes, get the source's integer field array
			if(intFieldPrefixLen > 0) {
				base.serializeIntFields(intFields);
			}
			else {
				memset(intFields, 0, intFieldArraySize);
			}

			// Version offset is used to skip the version bytes in the int field array when version is missing (aka 0)
			int versionOffset = ( (intFieldPrefixLen == 0) && (~flags & HAS_VERSION) ) ? 8 : 0;

			// If there are suffix bytes, copy those into place after the prefix
			if(intFieldSuffixLen > 0) {
				memcpy(intFields + versionOffset + intFieldPrefixLen, intFieldSuffix, intFieldSuffixLen);
			}

			// Zero out any remaining bytes if the array was initialized from base
			if(intFieldPrefixLen > 0) {
				for(int i = versionOffset + intFieldPrefixLen + intFieldSuffixLen; i < intFieldArraySize; ++i) {
					intFields[i] = 0;
				}
			}

			return RedwoodRecordRef(k, flags & HAS_VALUE ? r.readString(valueLen) : Optional<ValueRef>(), intFields);
		}

		int size() const {
			Reader r(data());

			int intFieldSuffixLen = flags & INT_FIELD_SUFFIX_BITS;
			r.readVarInt();  // prefixlen
			int valueLen = (flags & HAS_VALUE) ? r.read<uint8_t>() : 0;
			int keySuffixLen = (flags & HAS_KEY_SUFFIX) ? r.readVarInt() : 0;

			return sizeof(Delta) + r.rptr - data() + intFieldSuffixLen + valueLen + keySuffixLen;
		}

		// Delta can't be determined without the RedwoodRecordRef upon which the Delta is based.
		std::string toString() const {
			Reader r(data());

			std::string flagString = " ";
			if(flags & PREFIX_SOURCE) flagString += "prefixSource ";
			if(flags & HAS_KEY_SUFFIX) flagString += "keySuffix ";
			if(flags & HAS_VERSION) flagString += "Version ";
			if(flags & HAS_VALUE) flagString += "Value ";

			int intFieldSuffixLen = flags & INT_FIELD_SUFFIX_BITS;
			int prefixLen = r.readVarInt();
			int valueLen = (flags & HAS_VALUE) ? r.read<uint8_t>() : 0;
			int keySuffixLen = (flags & HAS_KEY_SUFFIX) ? r.readVarInt() : 0;

			return format("len: %d  flags: %s prefixLen: %d  keySuffixLen: %d  intFieldSuffix: %d  valueLen %d  raw: %s",
				size(), flagString.c_str(), prefixLen, keySuffixLen, intFieldSuffixLen, valueLen, StringRef((const uint8_t *)this, size()).toHexString().c_str());
		}
	};
#pragma pack(pop)

	// Compares and orders by key, version, chunk.start, chunk.total.
	// Value is not considered, as it is does not make sense for a container
	// to have two records which differ only in value.
	int compare(const RedwoodRecordRef &rhs) const {
		int cmp = key.compare(rhs.key);
		if(cmp == 0) {
			cmp = version - rhs.version;
			if(cmp == 0) {
				// It is assumed that in any data set there will never be more than one
				// unique chunk total size for the same key and version, so sort by start, total 
				// Chunked (represented by chunk.total > 0) sorts higher than whole
				cmp = chunk.start - rhs.chunk.start;
				if(cmp == 0) {
					cmp = chunk.total - rhs.chunk.total;
				}
			}
		}
		return cmp;
	}

	// Compares key fields and value for equality
	bool identical(const RedwoodRecordRef &rhs) const {
		return compare(rhs) == 0 && value == rhs.value;
	}

	bool operator==(const RedwoodRecordRef &rhs) const {
		return compare(rhs) == 0;
	}

	bool operator!=(const RedwoodRecordRef &rhs) const {
		return compare(rhs) != 0;
	}

		bool operator<(const RedwoodRecordRef &rhs) const {
		return compare(rhs) < 0;
	}

	bool operator>(const RedwoodRecordRef &rhs) const {
		return compare(rhs) > 0;
	}

	bool operator<=(const RedwoodRecordRef &rhs) const {
		return compare(rhs) <= 0;
	}

	bool operator>=(const RedwoodRecordRef &rhs) const {
		return compare(rhs) >= 0;
	}

	int deltaSize(const RedwoodRecordRef &base, bool worstCase = true) const {
		int size = sizeof(Delta);

		if(value.present()) {
			size += value.get().size();
			++size;
		}

		int prefixLen = getCommonPrefixLen(base, 0);
		size += (worstCase || prefixLen >= 128) ? 2 : 1;

		int intFieldPrefixLen;

		// Currently using a worst-guess guess where int fields in suffix are stored in their entirety if nonzero.
		if(prefixLen < key.size()) {
			int keySuffixLen = key.size() - prefixLen;
			size += (worstCase || keySuffixLen >= 128) ? 2 : 1;
			size += keySuffixLen;
			intFieldPrefixLen = 0;
		}
		else {
			intFieldPrefixLen = prefixLen - key.size();
			if(worstCase) {
				size += 2;
			}
		}

		if(version == 0 && chunk.total == 0 && chunk.start == 0) {
			// No int field suffix needed
		}
		else {
			byte fields[intFieldArraySize];
			serializeIntFields(fields);

			const byte *end = fields + intFieldArraySize - 1;
			int trailingNulls = 0;
			while(*end-- == 0) {
				++trailingNulls;
			}

			size += std::max(0, intFieldArraySize - intFieldPrefixLen - trailingNulls);
			if(intFieldPrefixLen == 0 && version == 0) {
				size -= 8;
			}
		}

		return size;
	}

	// commonPrefix between *this and base can be passed if known
	int writeDelta(Delta &d, const RedwoodRecordRef &base, int commonPrefix = -1) const {
		d.flags = version == 0 ? 0 : Delta::HAS_VERSION;

		if(commonPrefix < 0) {
			commonPrefix = getCommonPrefixLen(base, 0);
		}

		Writer w(d.data());

		// prefixLen
		w.writeVarInt(commonPrefix);

		// valueLen
		if(value.present()) {
			d.flags |= Delta::HAS_VALUE;
			w.write<uint8_t>(value.get().size());
		}

		// keySuffixLen
		if(key.size() > commonPrefix) {
			d.flags |= Delta::HAS_KEY_SUFFIX;

			StringRef keySuffix = key.substr(commonPrefix);
			w.writeVarInt(keySuffix.size());

			// keySuffix
			w.writeString(keySuffix);
		}

		// This is a common case, where no int suffix is needed
		if(version == 0 && chunk.total == 0 && chunk.start == 0) {
			// The suffixLen bits in flags are already zero, so nothing to do here.
		}
		else {
			byte fields[intFieldArraySize];
			serializeIntFields(fields);

			// Find the position of the first null byte from the right
			// This for loop has no endPos > 0 check because it is known that the array contains non-null bytes
			int endPos;
			for(endPos = intFieldArraySize; fields[endPos - 1] == 0; --endPos);

			// Start copying after any prefix bytes that matched the int fields of the base
			int intFieldPrefixLen = std::max(0, commonPrefix - key.size());
			int startPos = intFieldPrefixLen + (intFieldPrefixLen == 0 && version == 0 ? 8 : 0);
			int suffixLen = std::max(0, endPos - startPos);

			if(suffixLen > 0) {
				w.writeString(StringRef(fields + startPos, suffixLen));
				d.flags |= suffixLen;
			}
		}

		if(value.present()) {
			w.writeString(value.get());
		}

		return w.wptr - d.data() + sizeof(Delta);
	}

	template<typename StringRefT>
	static std::string kvformat(StringRefT s, int hexLimit = -1) {
		bool hex = false;

		for(auto c : s) {
			if(!isprint(c)) {
				hex = true;
				break;
			}
		}

		return hex ? s.toHexString(hexLimit) : s.toString();
	}

	std::string toString(int hexLimit = 15) const {
		std::string r;
		r += format("'%s'@%" PRId64, kvformat(key, hexLimit).c_str(), version);
		r += format("[%u/%u]->", chunk.start, chunk.total);
		if(value.present()) {
			// Assume that values the size of a page ID are page IDs.  It's not perfect but it's just for debugging.
			if(value.get().size() == sizeof(LogicalPageID)) {
				r += format("[PageID=%u]", getPageID());
			}
			else {
				r += format("'%s'", kvformat(value.get(), hexLimit).c_str());
			}
		}
		else {
			r += "null";
		}
		return r;
	}
};

struct BTreePage {

	enum EPageFlags { IS_LEAF = 1};

	typedef DeltaTree<RedwoodRecordRef> BinaryTree;

#pragma pack(push,1)
	struct {
		uint8_t flags;
		uint16_t count;
		uint32_t kvBytes;
		uint8_t extensionPageCount;
	};
#pragma pack(pop)

	inline LogicalPageID * extensionPages() {
		return (LogicalPageID *)(this + 1);
	}

	inline const LogicalPageID * extensionPages() const {
		return (const LogicalPageID *)(this + 1);
	}

	int size() const {
		const BinaryTree *t = &tree();
		return (uint8_t *)t - (uint8_t *)this + t->size();
	}

	bool isLeaf() const {
		return flags & IS_LEAF;
	}

	BinaryTree & tree() {
		return *(BinaryTree *)(extensionPages() + extensionPageCount);
	}

	const BinaryTree & tree() const {
		return *(const BinaryTree *)(extensionPages() + extensionPageCount);
	}

	static inline int GetHeaderSize(int extensionPages = 0) {
		return sizeof(BTreePage) + (extensionPages * sizeof(LogicalPageID));
	}

	std::string toString(bool write, LogicalPageID id, Version ver, const RedwoodRecordRef *lowerBound, const RedwoodRecordRef *upperBound) const {
		std::string r;
		r += format("BTreePage op=%s id=%d ver=%" PRId64 " ptr=%p flags=0x%X count=%d kvBytes=%d extPages=%d\n  lowerBound: %s\n  upperBound: %s\n",
					write ? "write" : "read", id, ver, this, (int)flags, (int)count, (int)kvBytes, (int)extensionPageCount,
					lowerBound->toString().c_str(), upperBound->toString().c_str());
		try {
			if(count > 0) {
				// This doesn't use the cached reader for the page but it is only for debugging purposes
				BinaryTree::Reader reader(&tree(), lowerBound, upperBound);
				BinaryTree::Cursor c = reader.getCursor();

				c.moveFirst();
				ASSERT(c.valid());

				bool anyOutOfRange = false;
				do {
					r += "  ";
					r += c.get().toString();

					bool tooLow = c.get().key < lowerBound->key;
					bool tooHigh = c.get().key > upperBound->key;
					if(tooLow || tooHigh) {
						anyOutOfRange = true;
						if(tooLow) {
							r += " (too low)";
						}
						if(tooHigh) {
							r += " (too high)";
						}
					}
					r += "\n";

				} while(c.moveNext());
				ASSERT(!anyOutOfRange);
			}
		} catch (Error& e) {
			debug_printf("BTreePage::toString ERROR: %s\n", e.what());
			debug_printf("BTreePage::toString partial result: %s\n", r.c_str());
			throw;
		}

		return r;
	}
};

static void makeEmptyPage(Reference<IPage> page, uint8_t newFlags, int pageSize) {
	VALGRIND_MAKE_MEM_DEFINED(page->begin(), page->size());
	BTreePage *btpage = (BTreePage *)page->begin();
	btpage->flags = newFlags;
	btpage->kvBytes = 0;
	btpage->count = 0;
	btpage->extensionPageCount = 0;
	btpage->tree().build(nullptr, nullptr, nullptr, nullptr);
}

BTreePage::BinaryTree::Reader * getReader(Reference<const IPage> page) {
	return (BTreePage::BinaryTree::Reader *)page->userData;
}

struct BoundaryAndPage {
	Standalone<RedwoodRecordRef> lowerBound;
	// Only firstPage or multiPage will be in use at once
	Reference<IPage> firstPage;
	std::vector<Reference<IPage>> extPages;
};

// Returns a std::vector of pairs of lower boundary key indices within kvPairs and encoded pages.
// TODO:  Refactor this as an accumulator you add sorted keys to which makes pages.
template<typename Allocator>
static std::vector<BoundaryAndPage> buildPages(bool minimalBoundaries, const RedwoodRecordRef &lowerBound, const RedwoodRecordRef &upperBound,  std::vector<RedwoodRecordRef> entries, uint8_t newFlags, Allocator const &newBlockFn, int usableBlockSize) {
	// This is how much space for the binary tree exists in the page, after the header
	int pageSize = usableBlockSize - BTreePage::GetHeaderSize();

	// Each new block adds (usableBlockSize - sizeof(LogicalPageID)) more net usable space *for the binary tree* to pageSize.
	int netTreeBlockSize = usableBlockSize - sizeof(LogicalPageID);

	int blockCount = 1;
	std::vector<BoundaryAndPage> pages;

	int kvBytes = 0;
	int compressedBytes = BTreePage::BinaryTree::GetTreeOverhead();

	int start = 0;
	int i = 0;
	const int iEnd = entries.size();
	// Lower bound of the page being added to
	RedwoodRecordRef pageLowerBound = lowerBound.withoutValue();
	RedwoodRecordRef pageUpperBound;

	while(i <= iEnd) {
		bool end = i == iEnd;
		bool flush = end;

		// If not the end, add i to the page if necessary
		if(end) {
			pageUpperBound = upperBound.withoutValue();
		}
		else {
			// Get delta from previous record
			const RedwoodRecordRef &entry = entries[i];
			int deltaSize = entry.deltaSize((i == start) ? pageLowerBound : entries[i - 1]);
			int keySize = entry.key.size();
			int valueSize = entry.value.present() ? entry.value.get().size() : 0;

			int spaceNeeded = sizeof(BTreePage::BinaryTree::Node) + deltaSize;

			debug_printf("Trying to add record %3d of %3lu (i=%3d) klen %4d  vlen %3d  deltaSize %4d  spaceNeeded %4d  compressed %4d / page %4d bytes  %s\n",
				i + 1, entries.size(), i, keySize, valueSize, deltaSize,
				spaceNeeded, compressedBytes, pageSize, entry.toString().c_str());

			int spaceAvailable = pageSize - compressedBytes;

			// Does it fit?
			bool fits = spaceAvailable >= spaceNeeded;

			// If it doesn't fit, either end the current page or increase the page size
			if(!fits) {
				// For leaf level where minimal boundaries are used require at least 1 entry, otherwise require 4 to enforce a minimum branching factor
				int minimumEntries = minimalBoundaries ? 1 : 4;
				int count = i - start;

				// If not enough entries or page less than half full, increase page size to make the entry fit
				if(count < minimumEntries || spaceAvailable > pageSize / 2) {
					// Figure out how many additional whole or partial blocks are needed
					int newBlocks = 1 + (spaceNeeded - spaceAvailable - 1) / netTreeBlockSize;
					int newPageSize = pageSize + (newBlocks * netTreeBlockSize);
					if(newPageSize <= BTreePage::BinaryTree::MaximumTreeSize()) {
						blockCount += newBlocks;
						pageSize = newPageSize;
						fits = true;
					}
				}
				if(!fits) {
					pageUpperBound = entry.withoutValue();
				}
			}

			// If the record fits then add it to the page set
			if(fits) {
				kvBytes += keySize + valueSize;
				compressedBytes += spaceNeeded;
				++i;
			}

			flush = !fits;
		}

		// If flush then write a page using records from start to i.  It's guaranteed that pageUpperBound has been set above.
		if(flush) {
			end = i == iEnd;  // i could have been moved above

			int count = i - start;
			// If not writing the final page, reduce entry count of page by a third
			if(!end) {
				i -= count / 3;
				pageUpperBound = entries[i].withoutValue();
			}

			// If this isn't the final page, shorten the upper boundary
			if(!end && minimalBoundaries) {
				int commonPrefix = pageUpperBound.getCommonPrefixLen(entries[i - 1], 0);
				pageUpperBound.truncate(commonPrefix + 1);
			}

			debug_printf("Flushing page start=%d i=%d count=%d\nlower: %s\nupper: %s\n", start, i, count, pageLowerBound.toString().c_str(), pageUpperBound.toString().c_str());
#if REDWOOD_DEBUG
			for(int j = start; j < i; ++j) {
				debug_printf(" %3d: %s\n", j, entries[j].toString().c_str());
				if(j > start) {
					//ASSERT(entries[j] > entries[j - 1]);
				}
			}
			ASSERT(pageLowerBound.key <= pageUpperBound.key);
#endif

			union {
				BTreePage *btPage;
				uint8_t *btPageMem;
			};

			int allocatedSize;
			if(blockCount == 1) {
				Reference<IPage> page = newBlockFn();
				VALGRIND_MAKE_MEM_DEFINED(page->begin(), page->size());
				btPageMem = page->mutate();
				allocatedSize = page->size();
				pages.push_back({pageLowerBound, page});
			}
			else {
				ASSERT(blockCount > 1);
				allocatedSize = usableBlockSize * blockCount;
				btPageMem = new uint8_t[allocatedSize];
				VALGRIND_MAKE_MEM_DEFINED(btPageMem, allocatedSize);
			}

			btPage->flags = newFlags;
			btPage->kvBytes = kvBytes;
			btPage->count = i - start;
			btPage->extensionPageCount = blockCount - 1;

			int written = btPage->tree().build(&entries[start], &entries[i], &pageLowerBound, &pageUpperBound);
			if(written > pageSize) {
				fprintf(stderr, "ERROR:  Wrote %d bytes to %d byte page (%d blocks). recs %d  kvBytes %d  compressed %d\n", written, pageSize, blockCount, i - start, kvBytes, compressedBytes);
				ASSERT(false);
			}

			if(blockCount != 1) {
				Reference<IPage> page = newBlockFn();
				VALGRIND_MAKE_MEM_DEFINED(page->begin(), page->size());

				const uint8_t *rptr = btPageMem;
				memcpy(page->mutate(), rptr, usableBlockSize);
				rptr += usableBlockSize;
				
				std::vector<Reference<IPage>> extPages;
				for(int b = 1; b < blockCount; ++b) {
					Reference<IPage> extPage = newBlockFn();
					VALGRIND_MAKE_MEM_DEFINED(page->begin(), page->size());

					//debug_printf("block %d write offset %d\n", b, firstBlockSize + (b - 1) * usableBlockSize);
					memcpy(extPage->mutate(), rptr, usableBlockSize);
					rptr += usableBlockSize;
					extPages.push_back(std::move(extPage));
				}

				pages.push_back({std::move(pageLowerBound), std::move(page), std::move(extPages)});
				delete btPageMem;
			}

			if(end)
				break;
			start = i;
			kvBytes = 0;
			compressedBytes = BTreePage::BinaryTree::GetTreeOverhead();
			pageLowerBound = pageUpperBound.withoutValue();
		}
	}

	//debug_printf("buildPages: returning pages.size %lu, kvpairs %lu\n", pages.size(), kvPairs.size());
	return pages;
}

#define NOT_IMPLEMENTED { UNSTOPPABLE_ASSERT(false); }

class VersionedBTree : public IVersionedStore {
public:
	// The first possible internal record possible in the tree
	static RedwoodRecordRef dbBegin;
	// A record which is greater than the last possible record in the tree
	static RedwoodRecordRef dbEnd;

	struct Counts {
		Counts() {
			memset(this, 0, sizeof(Counts));
		}

		void clear() {
			*this = Counts();
		}

		int64_t pageReads;
		int64_t extPageReads;
		int64_t setBytes;
		int64_t pageWrites;
		int64_t extPageWrites;
		int64_t sets;
		int64_t clears;
		int64_t commits;
		int64_t gets;
		int64_t getRanges;
		int64_t commitToPage;
		int64_t commitToPageStart;

		std::string toString(bool clearAfter = false) {
			std::string s = format("set=%" PRId64 " clear=%" PRId64 " get=%" PRId64 " getRange=%" PRId64 " commit=%" PRId64 " pageRead=%" PRId64 " extPageRead=%" PRId64 " pageWrite=%" PRId64 " extPageWrite=%" PRId64 " commitPage=%" PRId64 " commitPageStart=%" PRId64 "", 
				sets, clears, gets, getRanges, commits, pageReads, extPageReads, pageWrites, extPageWrites, commitToPage, commitToPageStart);
			if(clearAfter) {
				clear();
			}
			return s;
		}
	};

	// Using a static for metrics because a single process shouldn't normally have multiple storage engines
	static Counts counts;

	// All async opts on the btree are based on pager reads, writes, and commits, so
	// we can mostly forward these next few functions to the pager
	virtual Future<Void> getError() {
		return m_pager->getError();
	}

	virtual Future<Void> onClosed() {
		return m_pager->onClosed();
	}

	void close_impl(bool dispose) {
		IPager *pager = m_pager;
		delete this;
		if(dispose)
			pager->dispose();
		else
			pager->close();
	}

	virtual void dispose() {
		return close_impl(true);
	}

	virtual void close() {
		return close_impl(false);
	}

	virtual KeyValueStoreType getType() NOT_IMPLEMENTED
	virtual bool supportsMutation(int op) NOT_IMPLEMENTED
	virtual StorageBytes getStorageBytes() {
		return m_pager->getStorageBytes();
	}

	// Writes are provided in an ordered stream.
	// A write is considered part of (a change leading to) the version determined by the previous call to setWriteVersion()
	// A write shall not become durable until the following call to commit() begins, and shall be durable once the following call to commit() returns
	virtual void set(KeyValueRef keyValue) {
		++counts.sets;
		SingleKeyMutationsByVersion &changes = insertMutationBoundary(keyValue.key)->second.startKeyMutations;

		if(singleVersion) {
			if(changes.empty()) {
				changes[0] = SingleKeyMutation(keyValue.value);
			}
			else {
				changes.begin()->second = SingleKeyMutation(keyValue.value);
			}
		}
		else {
			// Add the set if the changes set is empty or the last entry isn't a set to exactly the same value
			if(changes.empty() || !changes.rbegin()->second.equalToSet(keyValue.value)) {
				changes[m_writeVersion] = SingleKeyMutation(keyValue.value);
			}
		}
	}
	virtual void clear(KeyRangeRef range) {
		++counts.clears;
		MutationBufferT::iterator iBegin = insertMutationBoundary(range.begin);
		MutationBufferT::iterator iEnd = insertMutationBoundary(range.end);

		// In single version mode, clear all pending updates in the affected range
		if(singleVersion) {
			RangeMutation &range = iBegin->second;
			range.startKeyMutations.clear();
			range.startKeyMutations[0] = SingleKeyMutation();
			range.rangeClearVersion = 0;
			++iBegin;
			m_pBuffer->erase(iBegin, iEnd);
		}
		else {
			// For each boundary in the cleared range
			while(iBegin != iEnd) {
				RangeMutation &range = iBegin->second;

				// Set the rangeClearedVersion if not set
				if(!range.rangeClearVersion.present())
					range.rangeClearVersion = m_writeVersion;

				// Add a clear to the startKeyMutations map if it's empty or the last item is not a clear
				if(range.startKeyMutations.empty() || !range.startKeyMutations.rbegin()->second.isClear())
					range.startKeyMutations[m_writeVersion] = SingleKeyMutation();

				++iBegin;
			}
		}
	}

	virtual void mutate(int op, StringRef param1, StringRef param2) NOT_IMPLEMENTED

	// Versions [begin, end) no longer readable
	virtual void forgetVersions(Version begin, Version end) NOT_IMPLEMENTED

	virtual Future<Version> getLatestVersion() {
		if(m_writeVersion != invalidVersion)
			return m_writeVersion;
		return m_pager->getLatestVersion();
	}

	Version getWriteVersion() {
		return m_writeVersion;
	}

	Version getLastCommittedVersion() {
		return m_lastCommittedVersion;
	}

	VersionedBTree(IPager *pager, std::string name, bool singleVersion = false, int target_page_size = -1)
	  : m_pager(pager),
		m_writeVersion(invalidVersion),
		m_usablePageSizeOverride(pager->getUsablePageSize()),
		m_lastCommittedVersion(invalidVersion),
		m_pBuffer(nullptr),
		m_name(name),
		singleVersion(singleVersion)
	{
		if(target_page_size > 0 && target_page_size < m_usablePageSizeOverride)
			m_usablePageSizeOverride = target_page_size;
		m_init = init_impl(this);
		m_latestCommit = m_init;
	}

	ACTOR static Future<Void> init_impl(VersionedBTree *self) {
		self->m_root = 0;
		state Version latest = wait(self->m_pager->getLatestVersion());
		if(latest == 0) {
			++latest;
			Reference<IPage> page = self->m_pager->newPageBuffer();
			makeEmptyPage(page, BTreePage::IS_LEAF, self->m_usablePageSizeOverride);
			self->writePage(self->m_root, page, latest, &dbBegin, &dbEnd);
			self->m_pager->setLatestVersion(latest);
			wait(self->m_pager->commit());
		}
		self->m_lastCommittedVersion = latest;
		return Void();
	}

	Future<Void> init() { return m_init; }

	virtual ~VersionedBTree() {
		// This probably shouldn't be called directly (meaning deleting an instance directly) but it should be safe,
		// it will cancel init and commit and leave the pager alive but with potentially an incomplete set of 
		// uncommitted writes so it should not be committed.
		m_init.cancel();
		m_latestCommit.cancel();
	}

	// readAtVersion() may only be called on a version which has previously been passed to setWriteVersion() and never previously passed
	//   to forgetVersion.  The returned results when violating this precondition are unspecified; the store is not required to be able to detect violations.
	// The returned read cursor provides a consistent snapshot of the versioned store, corresponding to all the writes done with write versions less
	//   than or equal to the given version.
	// If readAtVersion() is called on the *current* write version, the given read cursor MAY reflect subsequent writes at the same
	//   write version, OR it may represent a snapshot as of the call to readAtVersion().
	virtual Reference<IStoreCursor> readAtVersion(Version v) {
		// TODO: Use the buffer to return uncommitted data
		// For now, only committed versions can be read.
		Version recordVersion = singleVersion ? 0 : v;
		ASSERT(v <= m_lastCommittedVersion);
		if(singleVersion) {
			ASSERT(v == m_lastCommittedVersion);
		}
		return Reference<IStoreCursor>(new Cursor(m_pager->getReadSnapshot(v), m_root, recordVersion, m_usablePageSizeOverride));
	}

	// Must be nondecreasing
	virtual void setWriteVersion(Version v) {
		ASSERT(v > m_lastCommittedVersion);
		// If there was no current mutation buffer, create one in the buffer map and update m_pBuffer
		if(m_pBuffer == nullptr) {
			// When starting a new mutation buffer its start version must be greater than the last write version
			ASSERT(v > m_writeVersion);
			m_pBuffer = &m_mutationBuffers[v];
			// Create range representing the entire keyspace.  This reduces edge cases to applying mutations
			// because now all existing keys are within some range in the mutation map.
			(*m_pBuffer)[dbBegin.key];
			(*m_pBuffer)[dbEnd.key];
		}
		else {
			// It's OK to set the write version to the same version repeatedly so long as m_pBuffer is not null
			ASSERT(v >= m_writeVersion);
		}
		m_writeVersion = v;
	}

	virtual Future<Void> commit() {
		if(m_pBuffer == nullptr)
			return m_latestCommit;
		return commit_impl(this);
	}

	bool isSingleVersion() const {
		return singleVersion;
	}

private:
	void writePage(LogicalPageID id, Reference<IPage> page, Version ver, const RedwoodRecordRef *pageLowerBound, const RedwoodRecordRef *pageUpperBound) {
		debug_printf("writePage(): %s\n", ((const BTreePage *)page->begin())->toString(true, id, ver, pageLowerBound, pageUpperBound).c_str());
		m_pager->writePage(id, page, ver);
	}

	LogicalPageID m_root;

	// TODO: Don't use Standalone
	struct VersionedChildPageSet {
		Version version;
		std::vector<Standalone<RedwoodRecordRef>> children;
		Standalone<RedwoodRecordRef> upperBound;
	};

	typedef std::vector<VersionedChildPageSet> VersionedChildrenT;

	// Utility class for building a vector of internal page entries.
	// Entries must be added in version order.  Modified will be set to true
	// if any entries differ from the original ones.  Additional entries will be
	// added when necessary to reconcile differences between the upper and lower
	// boundaries of consecutive entries.
	struct InternalPageBuilder {
		// Cursor must be at first entry in page
		InternalPageBuilder(const BTreePage::BinaryTree::Cursor &c)
		 : cursor(c), modified(false), childPageCount(0)
		{
		}

		inline void addEntry(const RedwoodRecordRef &rec) {
			if(rec.value.present()) {
				++childPageCount;
			}

			// If no modification detected yet then check that this record is identical to the next
			// record from the original page which is at the current cursor position.
			if(!modified) {
				if(cursor.valid()) {
					if(!rec.identical(cursor.get())) {
						debug_printf("InternalPageBuilder: Found internal page difference.  new: %s  old: %s\n", rec.toString().c_str(), cursor.get().toString().c_str());
						modified = true;
					}
					else {
						cursor.moveNext();
					}
				}
				else {
					debug_printf("InternalPageBuilder: Found internal page difference.  new: %s  old: <end>\n", rec.toString().c_str());
					modified = true;
				}
			}

			entries.push_back(rec);
		}

		void addEntries(const VersionedChildPageSet &newSet) {
			// If there are already entries, the last one links to a child page, and its upper bound is not the same
			// as the first lowerBound in newSet (or newSet is empty, as the next newSet is necessarily greater)
			// then add the upper bound of the previous set as a value-less record so that on future reads
			// the previous child page can be decoded correctly.
			if(!entries.empty() && entries.back().value.present()
				&& (newSet.children.empty() || newSet.children.front() != lastUpperBound))
			{
				debug_printf("InternalPageBuilder: Added placeholder %s\n", lastUpperBound.withoutValue().toString().c_str());
				addEntry(lastUpperBound.withoutValue());
			}

			for(auto &child : newSet.children) {
				debug_printf("InternalPageBuilder: Adding child entry %s\n", child.toString().c_str());
				addEntry(child);
			}

			lastUpperBound = newSet.upperBound;
			debug_printf("InternalPageBuilder: New upper bound: %s\n", lastUpperBound.toString().c_str());
		}

		// Finish comparison to existing data if necesary.
		// Handle possible page upper bound changes.
		// If modified is set (see below) and our rightmost entry has a child page and its upper bound
		// (currently in lastUpperBound) does not match the new desired page upper bound, passed as newUpperBound,
		// then write lastUpperBound with no value to allow correct decoding of the rightmost entry.
		// This is only done if modified is set to avoid rewriting this page for this purpose only.
		//
		// After this call, lastUpperBound is internal page's upper bound.
		void finalize(const RedwoodRecordRef &upperBound, const RedwoodRecordRef &decodeUpperBound) {
			debug_printf("InternalPageBuilder::end  modified=%d  upperBound=%s  decodeUpperBound=%s  lastUpperBound=%s\n", modified, upperBound.toString().c_str(), decodeUpperBound.toString().c_str(), lastUpperBound.toString().c_str());
			modified = modified || cursor.valid();
			debug_printf("InternalPageBuilder::end  modified=%d after cursor check\n", modified);

			// If there are boundary key entries and the last one has a child page then the 
			// upper bound for this internal page must match the required upper bound for
			// the last child entry.
			if(!entries.empty() && entries.back().value.present()) {
				debug_printf("InternalPageBuilder::end  last entry is not null\n");

				// If the page contents were not modified so far and the upper bound required
				// for the last child page (lastUpperBound) does not match what the page
				// was encoded with then the page must be modified.
				if(!modified && lastUpperBound != decodeUpperBound) {
					debug_printf("InternalPageBuilder::end  modified set true because lastUpperBound does not match decodeUpperBound\n");
					modified = true;
				}

				if(modified && lastUpperBound != upperBound) {
					debug_printf("InternalPageBuilder::end  Modified is true but lastUpperBound does not match upperBound so adding placeholder\n");
					addEntry(lastUpperBound.withoutValue());
					lastUpperBound = upperBound;
				}
			}
			debug_printf("InternalPageBuilder::end  exit.  modified=%d  upperBound=%s  decodeUpperBound=%s  lastUpperBound=%s\n", modified, upperBound.toString().c_str(), decodeUpperBound.toString().c_str(), lastUpperBound.toString().c_str());
		}

		BTreePage::BinaryTree::Cursor cursor;
		std::vector<Standalone<RedwoodRecordRef>> entries;
		Standalone<RedwoodRecordRef> lastUpperBound;
		bool modified;
		int childPageCount;
		Arena arena;
	};


	template<typename T>
	static std::string toString(const T &o) {
		return o.toString();
	}

	static std::string toString(const VersionedChildPageSet &c) {
		return format("Version=%" PRId64 " children=%s upperBound=%s", c.version, toString(c.children).c_str(), c.upperBound.toString().c_str());
	}

	template<typename T>
	static std::string toString(const std::vector<T> &v) {
		std::string r = "{ ";
		for(auto &o : v) {
			r += toString(o) + ", ";
		}
		return r + " }";
	}

	// Represents a change to a single key - set, clear, or atomic op
	struct SingleKeyMutation {
		// Clear
		SingleKeyMutation() : op(MutationRef::ClearRange) {}
		// Set
		SingleKeyMutation(Value val) : op(MutationRef::SetValue), value(val) {}
		// Atomic Op
		SingleKeyMutation(MutationRef::Type op, Value val) : op(op), value(val) {}

		MutationRef::Type op;
		Value value;

		inline bool isClear() const { return op == MutationRef::ClearRange; }
		inline bool isSet() const { return op == MutationRef::SetValue; }
		inline bool isAtomicOp() const { return !isSet() && !isClear(); }

		inline bool equalToSet(ValueRef val) { return isSet() && value == val; }

		inline RedwoodRecordRef toRecord(KeyRef userKey, Version version) const {
			// No point in serializing an atomic op, it needs to be coalesced to a real value.
			ASSERT(!isAtomicOp());

			if(isClear())
				return RedwoodRecordRef(userKey, version);

			return RedwoodRecordRef(userKey, version, value);
		}

		std::string toString() const {
			return format("op=%d val='%s'", op, printable(value).c_str());
		}
	};

	// Represents mutations on a single key and a possible clear to a range that begins
	// immediately after that key
	typedef std::map<Version, SingleKeyMutation> SingleKeyMutationsByVersion;
	struct RangeMutation {
		// Mutations for exactly the start key
		SingleKeyMutationsByVersion startKeyMutations;
		// A clear range version, if cleared, for the range starting immediately AFTER the start key
		Optional<Version> rangeClearVersion;

		// Returns true if this RangeMutation doesn't actually mutate anything
		bool noChanges() const {
			return !rangeClearVersion.present() && startKeyMutations.empty();
		}

		std::string toString() const {
			std::string result;
			result.append("rangeClearVersion: ");
			if(rangeClearVersion.present())
				result.append(format("%" PRId64 "", rangeClearVersion.get()));
			else
				result.append("<not present>");
			result.append("  startKeyMutations: ");
			for(SingleKeyMutationsByVersion::value_type const &m : startKeyMutations)
				result.append(format("[%" PRId64 " => %s] ", m.first, m.second.toString().c_str()));
			return result;
		}
	};

	typedef std::map<Key, RangeMutation> MutationBufferT;

	/* Mutation Buffer Overview
	 *
	 * This structure's organization is meant to put pending updates for the btree in an order
	 * that makes it efficient to query all pending mutations across all pending versions which are
	 * relevant to a particular subtree of the btree.
	 * 
	 * At the top level, it is a map of the start of a range being modified to a RangeMutation.
	 * The end of the range is map key (which is the next range start in the map).
	 * 
	 * - The buffer starts out with keys '' and endKVV.key already populated.
	 *
	 * - When a new key is inserted into the buffer map, it is by definition
	 *   splitting an existing range so it should take on the rangeClearVersion of
	 *   the immediately preceding key which is the start of that range
	 *
	 * - Keys are inserted into the buffer map for every individual operation (set/clear/atomic)
	 *   key and for both the start and end of a range clear.
	 *
	 * - To apply a single clear, add it to the individual ops only if the last entry is not also a clear.
	 *
	 * - To apply a range clear, after inserting the new range boundaries do the following to the start
	 *   boundary and all successive boundaries < end
	 *      - set the range clear version if not already set
	 *      - add a clear to the startKeyMutations if the final entry is not a clear.
	 *
	 * - Note that there are actually TWO valid ways to represent
	 *       set c = val1 at version 1
	 *       clear c\x00 to z at version 2
	 *   with this model.  Either
	 *      c =     { rangeClearVersion = 2, startKeyMutations = { 1 => val1 }
	 *      z =     { rangeClearVersion = <not present>, startKeyMutations = {}
	 *   OR
	 *      c =     { rangeClearVersion = <not present>, startKeyMutations = { 1 => val1 }
	 *      c\x00 = { rangeClearVersion = 2, startKeyMutations = { 2 => <not present> }
	 *      z =     { rangeClearVersion = <not present>, startKeyMutations = {}
	 *
	 *   This is because the rangeClearVersion applies to a range begining with the first
	 *   key AFTER the start key, so that the logic for reading the start key is more simple
	 *   as it only involves consulting startKeyMutations.  When adding a clear range, the
	 *   boundary key insert/split described above is valid, and is what is currently done,
	 *   but it would also be valid to see if the last key before startKey is equal to
	 *   keyBefore(startKey), and if so that mutation buffer boundary key can be used instead
	 *   without adding an additional key to the buffer.

	 * TODO: A possible optimization here could be to only use existing btree leaf page boundaries as keys,
	 * with mutation point keys being stored in an unsorted strucutre under those boundary map keys,
	 * to be sorted later just before being merged into the existing leaf page.
	 */

	IPager *m_pager;
	MutationBufferT *m_pBuffer;
	std::map<Version, MutationBufferT> m_mutationBuffers;

	Version m_writeVersion;
	Version m_lastCommittedVersion;
	Future<Void> m_latestCommit;
	int m_usablePageSizeOverride;
	Future<Void> m_init;
	std::string m_name;
	bool singleVersion;

	void printMutationBuffer(MutationBufferT::const_iterator begin, MutationBufferT::const_iterator end) const {
#if REDWOOD_DEBUG
		debug_printf("-------------------------------------\n");
		debug_printf("BUFFER\n");
		while(begin != end) {
			debug_printf("'%s':  %s\n", printable(begin->first).c_str(), begin->second.toString().c_str());
			++begin;
		}
		debug_printf("-------------------------------------\n");
#endif
	}

	void printMutationBuffer(MutationBufferT *buf) const {
		return printMutationBuffer(buf->begin(), buf->end());
	}

	// Find or create a mutation buffer boundary for bound and return an iterator to it
	MutationBufferT::iterator insertMutationBoundary(Key boundary) {
		ASSERT(m_pBuffer != nullptr);

		// Find the first split point in buffer that is >= key
		MutationBufferT::iterator ib = m_pBuffer->lower_bound(boundary);

		// Since the initial state of the mutation buffer contains the range '' through
		// the maximum possible key, our search had to have found something.
		ASSERT(ib != m_pBuffer->end());

		// If we found the boundary we are looking for, return its iterator
		if(ib->first == boundary)
			return ib;

		// ib is our insert hint.  Insert the new boundary and set ib to its entry
		ib = m_pBuffer->insert(ib, {boundary, RangeMutation()});

		// ib is certainly > begin() because it is guaranteed that the empty string
		// boundary exists and the only way to have found that is to look explicitly
		// for it in which case we would have returned above.
		MutationBufferT::iterator iPrevious = ib;
		--iPrevious;
		if(iPrevious->second.rangeClearVersion.present()) {
			ib->second.rangeClearVersion = iPrevious->second.rangeClearVersion;
			ib->second.startKeyMutations[iPrevious->second.rangeClearVersion.get()] = SingleKeyMutation();
		}

		return ib;
	}

	void buildNewRoot(Version version, std::vector<BoundaryAndPage> &pages, std::vector<LogicalPageID> &logicalPageIDs, const BTreePage *pPage) {
		//debug_printf("buildNewRoot start %lu\n", pages.size());
		// While there are multiple child pages for this version we must write new tree levels.
		while(pages.size() > 1) {
			std::vector<RedwoodRecordRef> childEntries;
			for(int i=0; i<pages.size(); i++) {
				RedwoodRecordRef entry = pages[i].lowerBound.withPageID(logicalPageIDs[i]);
				debug_printf("Added new root entry %s\n", entry.toString().c_str());
				childEntries.push_back(entry);
			}

			pages = buildPages(false, dbBegin, dbEnd, childEntries, 0, [=](){ return m_pager->newPageBuffer(); }, m_usablePageSizeOverride);

			debug_printf("Writing a new root level at version %" PRId64 " with %lu children across %lu pages\n", version, childEntries.size(), pages.size());

			logicalPageIDs = writePages(pages, version, m_root, pPage, &dbEnd, nullptr);
		}
	}

	std::vector<LogicalPageID> writePages(std::vector<BoundaryAndPage> pages, Version version, LogicalPageID originalID, const BTreePage *originalPage, const RedwoodRecordRef *upperBound, void *actor_debug) {
		debug_printf("%p: writePages(): %u @%" PRId64 " -> %lu replacement pages\n", actor_debug, originalID, version, pages.size());

		ASSERT(version != 0 || pages.size() == 1);

		std::vector<LogicalPageID> primaryLogicalPageIDs;

		// Reuse original primary page ID if it's not the root or if only one page is being written.
		if(originalID != m_root || pages.size() == 1)
			primaryLogicalPageIDs.push_back(originalID);

		// Allocate a primary page ID for each page to be written
		while(primaryLogicalPageIDs.size() < pages.size()) {
			primaryLogicalPageIDs.push_back(m_pager->allocateLogicalPage());
		}

		debug_printf("%p: writePages(): Writing %lu replacement pages for %d at version %" PRId64 "\n", actor_debug, pages.size(), originalID, version);
		for(int i=0; i<pages.size(); i++) {
			++counts.pageWrites;

			// Allocate page number for main page first
			LogicalPageID id = primaryLogicalPageIDs[i];

			// Check for extension pages, if they exist assign IDs for them and write them at version
			auto const &extPages = pages[i].extPages;
			// If there are extension pages, write all pages using pager directly because this->writePage() is for whole primary pages
			if(extPages.size() != 0) {
				BTreePage *newPage = (BTreePage *)pages[i].firstPage->mutate();
				ASSERT(newPage->extensionPageCount == extPages.size());

				for(int e = 0, eEnd = extPages.size(); e < eEnd; ++e) {
					LogicalPageID eid = m_pager->allocateLogicalPage();
					debug_printf("%p: writePages(): Writing extension page op=write id=%u @%" PRId64 " (%d of %lu) referencePageID=%u\n", actor_debug, eid, version, e + 1, extPages.size(), id);
					newPage->extensionPages()[e] = bigEndian32(eid);
					// If replacing the primary page below (version == 0) then pass the primary page's ID as the reference page ID
					m_pager->writePage(eid, extPages[e], version, (version == 0) ? id : invalidLogicalPageID);
					++counts.extPageWrites;
				}

				debug_printf("%p: writePages(): Writing primary page op=write id=%u @%" PRId64 " (+%lu extension pages)\n", actor_debug, id, version, extPages.size());
				m_pager->writePage(id, pages[i].firstPage, version);
			}
			else {
				debug_printf("%p: writePages(): Writing normal page op=write id=%u @%" PRId64 "\n", actor_debug, id, version);
				writePage(id, pages[i].firstPage, version, &pages[i].lowerBound, (i == pages.size() - 1) ? upperBound : &pages[i + 1].lowerBound);
			}
		}

		// Free the old extension pages now that all replacement pages have been written
		for(int i = 0; i < originalPage->extensionPageCount; ++i) {
			//debug_printf("%p: writePages(): Freeing old extension op=del id=%u @latest\n", actor_debug, bigEndian32(originalPage->extensionPages()[i]));
			//m_pager->freeLogicalPage(bigEndian32(originalPage->extensionPages()[i]), version);
		}

		return primaryLogicalPageIDs;
	}

	class SuperPage : public IPage, ReferenceCounted<SuperPage> {
	public:
		SuperPage(std::vector<Reference<const IPage>> pages, int usablePageSize)
		  : m_size(pages.size() * usablePageSize) {
			m_data = new uint8_t[m_size];
			uint8_t *wptr = m_data;
			for(auto &p : pages) {
				memcpy(wptr, p->begin(), usablePageSize);
				wptr += usablePageSize;
			}
		}

		virtual ~SuperPage() {
			delete m_data;
		}

		virtual void addref() const {
			ReferenceCounted<SuperPage>::addref();
		}

		virtual void delref() const {
			ReferenceCounted<SuperPage>::delref();
		}

		virtual int size() const {
			return m_size;
		}

		virtual uint8_t const* begin() const {
			return m_data;
		}

		virtual uint8_t* mutate() {
			return m_data;
		}

	private:
		uint8_t *m_data;
		const int m_size;
	};

	ACTOR static Future<Reference<const IPage>> readPage(Reference<IPagerSnapshot> snapshot, LogicalPageID id, int usablePageSize, const RedwoodRecordRef *lowerBound, const RedwoodRecordRef *upperBound) {
		debug_printf("readPage() op=read id=%u @%" PRId64 " lower=%s upper=%s\n", id, snapshot->getVersion(), lowerBound->toString().c_str(), upperBound->toString().c_str());
		wait(delay(0, TaskPriority::DiskRead));

		state Reference<const IPage> result = wait(snapshot->getPhysicalPage(id));
		++counts.pageReads;
		state const BTreePage *pTreePage = (const BTreePage *)result->begin();

		if(pTreePage->extensionPageCount == 0) {
			debug_printf("readPage() Found normal page for op=read id=%u @%" PRId64 "\n", id, snapshot->getVersion());
		}
		else {
			std::vector<Future<Reference<const IPage>>> pageGets;
			pageGets.push_back(std::move(result));

			for(int i = 0; i < pTreePage->extensionPageCount; ++i) {
				debug_printf("readPage() Reading extension page op=read id=%u @%" PRId64 " ext=%d/%d\n", bigEndian32(pTreePage->extensionPages()[i]), snapshot->getVersion(), i + 1, (int)pTreePage->extensionPageCount);
				pageGets.push_back(snapshot->getPhysicalPage(bigEndian32(pTreePage->extensionPages()[i])));
			}

			std::vector<Reference<const IPage>> pages = wait(getAll(pageGets));
			counts.extPageReads += pTreePage->extensionPageCount;
			result = Reference<const IPage>(new SuperPage(pages, usablePageSize));
			pTreePage = (const BTreePage *)result->begin();
		}

		if(result->userData == nullptr) {
			debug_printf("readPage() Creating Reader for PageID=%u @%" PRId64 " lower=%s upper=%s\n", id, snapshot->getVersion(), lowerBound->toString().c_str(), upperBound->toString().c_str());
			result->userData = new BTreePage::BinaryTree::Reader(&pTreePage->tree(), lowerBound, upperBound);
			result->userDataDestructor = [](void *ptr) { delete (BTreePage::BinaryTree::Reader *)ptr; };
		}

		debug_printf("readPage() %s\n", pTreePage->toString(false, id, snapshot->getVersion(), lowerBound, upperBound).c_str());

		// Nothing should attempt to read bytes in the page outside the BTreePage structure
		VALGRIND_MAKE_MEM_UNDEFINED(result->begin() + pTreePage->size(), result->size() - pTreePage->size());

		return result;
	}

	// Returns list of (version, list of (lower_bound, list of children) )
	// TODO:  Probably should pass prev/next records by pointer in many places
	ACTOR static Future<VersionedChildrenT> commitSubtree(VersionedBTree *self, MutationBufferT *mutationBuffer, Reference<IPagerSnapshot> snapshot, LogicalPageID root, const RedwoodRecordRef *lowerBound, const RedwoodRecordRef *upperBound, const RedwoodRecordRef *decodeLowerBound, const RedwoodRecordRef *decodeUpperBound) {
		state std::string context;
		if(REDWOOD_DEBUG) {
			context = format("CommitSubtree(root=%u): ", root);
		}

		debug_printf("%s root=%d lower=%s upper=%s\n", context.c_str(), root, lowerBound->toString().c_str(), upperBound->toString().c_str());
		debug_printf("%s root=%d decodeLower=%s decodeUpper=%s\n", context.c_str(), root, decodeLowerBound->toString().c_str(), decodeUpperBound->toString().c_str());
		self->counts.commitToPageStart++;

		// If a boundary changed, the page must be rewritten regardless of KV mutations
		state bool boundaryChanged = (lowerBound != decodeLowerBound) || (upperBound != decodeUpperBound);
		debug_printf("%s id=%u boundaryChanged=%d\n", context.c_str(), root, boundaryChanged);

		// Find the slice of the mutation buffer that is relevant to this subtree
		// TODO:  Rather than two lower_bound searches, perhaps just compare each mutation to the upperBound key while iterating
		state MutationBufferT::const_iterator iMutationBoundary = mutationBuffer->upper_bound(lowerBound->key);
		--iMutationBoundary;
		state MutationBufferT::const_iterator iMutationBoundaryEnd = mutationBuffer->lower_bound(upperBound->key);

		if(REDWOOD_DEBUG) {
			self->printMutationBuffer(iMutationBoundary, iMutationBoundaryEnd);
		}

		// If the boundary range iterators are the same then upperbound and lowerbound have the same key.
		// If the key is being mutated, them remove this subtree.
		if(iMutationBoundary == iMutationBoundaryEnd) {
			if(!iMutationBoundary->second.startKeyMutations.empty()) {
				VersionedChildrenT c;
				debug_printf("%s id=%u lower and upper bound key/version match and key is modified so deleting page, returning %s\n", context.c_str(), root, toString(c).c_str());
				return c;
			}

			// If there are no forced boundary changes then this subtree is unchanged.
			if(!boundaryChanged) {
				VersionedChildrenT c({ {0, {*decodeLowerBound}, *decodeUpperBound} });
				debug_printf("%s id=%d page contains a single key '%s' which is not changing, returning %s\n", context.c_str(), root, lowerBound->key.toString().c_str(), toString(c).c_str());
				return c;
			}
		}

		// Another way to have no mutations is to have a single mutation range cover this
		// subtree but have no changes in it
		MutationBufferT::const_iterator iMutationBoundaryNext = iMutationBoundary;
		++iMutationBoundaryNext;
		if(!boundaryChanged && iMutationBoundaryNext == iMutationBoundaryEnd && 
			( iMutationBoundary->second.noChanges() || 
			  ( !iMutationBoundary->second.rangeClearVersion.present() &&
			    iMutationBoundary->first < lowerBound->key)
			)
		) {
			VersionedChildrenT c({ {0, {*decodeLowerBound}, *decodeUpperBound} });
			debug_printf("%s no changes because sole mutation range was not cleared, returning %s\n", context.c_str(), toString(c).c_str());
			return c;
		}

		self->counts.commitToPage++;
		state Reference<const IPage> rawPage = wait(readPage(snapshot, root, self->m_usablePageSizeOverride, decodeLowerBound, decodeUpperBound));
		state BTreePage *page = (BTreePage *) rawPage->begin();
		debug_printf("%s commitSubtree(): %s\n", context.c_str(), page->toString(false, root, snapshot->getVersion(), decodeLowerBound, decodeUpperBound).c_str());

		state BTreePage::BinaryTree::Cursor cursor = getReader(rawPage)->getCursor();
		cursor.moveFirst();

		// Leaf Page
		if(page->flags & BTreePage::IS_LEAF) {
			VersionedChildrenT results;
			std::vector<RedwoodRecordRef> merged;

			debug_printf("%s id=%u MERGING EXISTING DATA WITH MUTATIONS:\n", context.c_str(), root);
			if(REDWOOD_DEBUG) {
				self->printMutationBuffer(iMutationBoundary, iMutationBoundaryEnd);
			}

			// It's a given that the mutation map is not empty so it's safe to do this
			Key mutationRangeStart = iMutationBoundary->first;

			// If replacement pages are written they will be at the minimum version seen in the mutations for this leaf
			Version minVersion = invalidVersion;
			int changes = 0;

			// Now, process each mutation range and merge changes with existing data.
			while(iMutationBoundary != iMutationBoundaryEnd) {
				debug_printf("%s New mutation boundary: '%s': %s\n", context.c_str(), printable(iMutationBoundary->first).c_str(), iMutationBoundary->second.toString().c_str());

				SingleKeyMutationsByVersion::const_iterator iMutations;

				// If the mutation boundary key is less than the lower bound key then skip startKeyMutations for
				// this bounary, we're only processing this mutation range here to apply any clears to existing data.
				if(iMutationBoundary->first < lowerBound->key) {
					iMutations = iMutationBoundary->second.startKeyMutations.end();
				}
				// If the mutation boundary key is the same as the page lowerBound key then start reading single
				// key mutations at the first version greater than the lowerBound key's version.
				else if(!self->singleVersion && iMutationBoundary->first == lowerBound->key) {
					iMutations = iMutationBoundary->second.startKeyMutations.upper_bound(lowerBound->version);
				}
				else {
					iMutations = iMutationBoundary->second.startKeyMutations.begin();
				}

				SingleKeyMutationsByVersion::const_iterator iMutationsEnd = iMutationBoundary->second.startKeyMutations.end();

				// Iterate over old versions of the mutation boundary key, outputting if necessary
				while(cursor.valid() && cursor.get().key == iMutationBoundary->first) {
					// If not in single version mode or there were no changes to the key
					if(!self->singleVersion || iMutationBoundary->second.noChanges()) {
						merged.push_back(cursor.get());
						debug_printf("%s Added %s [existing, boundary start]\n", context.c_str(), merged.back().toString().c_str());
					}
					else {
						ASSERT(self->singleVersion);
						debug_printf("%s Skipped %s [existing, boundary start, singleVersion mode]\n", context.c_str(), cursor.get().toString().c_str());
						minVersion = 0;
					}
					cursor.moveNext();
				}

				// TODO:  If a mutation set is equal to the previous existing value of the key, maybe don't write it.
				// Output mutations for the mutation boundary start key
				while(iMutations != iMutationsEnd) {
					const SingleKeyMutation &m = iMutations->second;
					int maxPartSize = std::min(255, self->m_usablePageSizeOverride / 5);
					if(m.isClear() || m.value.size() <= maxPartSize) {
						if(iMutations->first < minVersion || minVersion == invalidVersion)
							minVersion = iMutations->first;
						++changes;
						merged.push_back(m.toRecord(iMutationBoundary->first, iMutations->first));
						debug_printf("%s Added non-split %s [mutation, boundary start]\n", context.c_str(), merged.back().toString().c_str());
					}
					else {
						if(iMutations->first < minVersion || minVersion == invalidVersion)
							minVersion = iMutations->first;
						++changes;
						int bytesLeft = m.value.size();
						int start = 0;
						RedwoodRecordRef whole(iMutationBoundary->first, iMutations->first, m.value);
						while(bytesLeft > 0) {
							int partSize = std::min(bytesLeft, maxPartSize);
							// Don't copy the value chunk because this page will stay in memory until after we've built new version(s) of it
							merged.push_back(whole.split(start, partSize));
							bytesLeft -= partSize;
							start += partSize;
							debug_printf("%s Added split %s [mutation, boundary start]\n", context.c_str(), merged.back().toString().c_str());
						}
					}
					++iMutations;
				}

				// Get the clear version for this range, which is the last thing that we need from it,
				Optional<Version> clearRangeVersion = iMutationBoundary->second.rangeClearVersion;
				// Advance to the next boundary because we need to know the end key for the current range.
				++iMutationBoundary;

				debug_printf("%s Mutation range end: '%s'\n", context.c_str(), printable(iMutationBoundary->first).c_str());

				// Write existing keys which are less than the next mutation boundary key, clearing if needed.
				while(cursor.valid() && cursor.get().key < iMutationBoundary->first) {
					// TODO:  Remove old versions that are too old

					bool remove = self->singleVersion && clearRangeVersion.present();
					if(!remove) {
						merged.push_back(cursor.get());
						debug_printf("%s Added %s [existing, middle]\n", context.c_str(), merged.back().toString().c_str());
					}
					else {
						ASSERT(self->singleVersion);
						debug_printf("%s Skipped %s [existing, boundary start, singleVersion mode]\n", context.c_str(), cursor.get().toString().c_str());
						Version clearVersion = clearRangeVersion.get();
						if(clearVersion < minVersion || minVersion == invalidVersion)
							minVersion = clearVersion;
					}

					// If keeping version history, write clears for records that exist in this range if the range was cleared
					if(!self->singleVersion) {
						// Write a clear of this key if needed.  A clear is required if clearRangeVersion is set and the next cursor
						// key is different than the current one.  If the last cursor key in the page is different from the
						// first key in the right sibling page then the page's upper bound will reflect that.
						auto nextCursor = cursor;
						nextCursor.moveNext();

						if(clearRangeVersion.present() && cursor.get().key != nextCursor.getOrUpperBound().key) {
							Version clearVersion = clearRangeVersion.get();
							if(clearVersion < minVersion || minVersion == invalidVersion)
								minVersion = clearVersion;
							++changes;
							merged.push_back(RedwoodRecordRef(cursor.get().key, clearVersion));
							debug_printf("%s Added %s [existing, middle clear]\n", context.c_str(), merged.back().toString().c_str());
						}
						cursor = nextCursor;
					}
					else {
						cursor.moveNext();
					}
				}
			}

			// Write any remaining existing keys, which are not subject to clears as they are beyond the cleared range.
			while(cursor.valid()) {
				merged.push_back(cursor.get());
				debug_printf("%s Added %s [existing, tail]\n", context.c_str(), merged.back().toString().c_str());
				cursor.moveNext();
			}

			debug_printf("%s Done merging mutations into existing leaf contents, made %d changes\n", context.c_str(), changes);

			// No changes were actually made.  This could happen if the only mutations are clear ranges which do not match any records.
			// But if a boundary was changed then we must rewrite the page anyway.
			if(!boundaryChanged && minVersion == invalidVersion) {
				VersionedChildrenT c({ {0, {*decodeLowerBound}, *decodeUpperBound} });
				debug_printf("%s No changes were made during mutation merge, returning %s\n", context.c_str(), toString(c).c_str());
				ASSERT(changes == 0);
				return c;
			}

			// TODO: Make version and key splits based on contents of merged list, if keeping history

			// If everything in the page was deleted then this page should be deleted as of the new version
			// Note that if a single range clear covered the entire page then we should not get this far
			if(merged.empty() && root != 0) {
				// TODO:  For multi version mode only delete this page as of the new version
				VersionedChildrenT c({});
				debug_printf("%s id=%u All leaf page contents were cleared, returning %s\n", context.c_str(), root, toString(c).c_str());
				return c;
			}

			IPager *pager = self->m_pager;
			std::vector<BoundaryAndPage> pages = buildPages(true, *lowerBound, *upperBound, merged, BTreePage::IS_LEAF, [pager](){ return pager->newPageBuffer(); }, self->m_usablePageSizeOverride);

			if(!self->singleVersion) {
				ASSERT(false);
// 				// If there isn't still just a single page of data then this page became too large and was split.
// 				// The new split pages will be valid as of minVersion, but the old page remains valid at the old version
// 				if(pages.size() != 1) {
// 					results.push_back( {0, {*decodeLowerBound}, ??} );
// 					debug_printf("%s Added versioned child set #1: %s\n", context.c_str(), toString(results.back()).c_str());
// 				}
// 				else {
// 					// The page was updated but not size-split or version-split so the last page version's data
// 					// can be replaced with the new page contents
// 					if(pages.size() == 1)
// 						minVersion = 0;
// 				}
			}

			// Write page(s), get new page IDs
			Version writeVersion = self->singleVersion ? self->getLastCommittedVersion() + 1 : minVersion;
			std::vector<LogicalPageID> newPageIDs = self->writePages(pages, writeVersion, root, page, upperBound, THIS);

			// If this commitSubtree() is operating on the root, write new levels if needed until until we're returning a single page
			if(root == self->m_root && pages.size() > 1) {
				debug_printf("%s Building new root\n", context.c_str());
				self->buildNewRoot(writeVersion, pages, newPageIDs, page);
			}

			results.push_back({writeVersion, {}, *upperBound});
			for(int i=0; i<pages.size(); i++) {
				// The lower bound of the first page is the lower bound of the subtree, not the first entry in the page
				const RedwoodRecordRef &lower = (i == 0) ? *lowerBound : pages[i].lowerBound;
				RedwoodRecordRef entry = lower.withPageID(newPageIDs[i]);
				debug_printf("%s Adding child page link: %s\n", context.c_str(), entry.toString().c_str());
				results.back().children.push_back(entry);
			}
			debug_printf("%s Merge complete, returning %s\n", context.c_str(), toString(results).c_str());

			debug_printf("%s DONE.\n", context.c_str());
			return results;
		}
		else {
			// Internal Page

			// TODO:  Combine these into one vector and/or do something more elegant
			state std::vector<Future<VersionedChildrenT>> futureChildren;

			bool first = true;
			while(cursor.valid()) {
				// The lower bound for the first child is the lowerBound arg
				const RedwoodRecordRef &childLowerBound = first ? *lowerBound : cursor.get();
				first = false;

				// Skip over any children that do not link to a page.  They exist to preserve the ancestors from
				// which adjacent children can borrow prefix bytes.
				// If there are any, then the first valid child page will incur a boundary change to move
				// its lower bound to the left so we can delete the non-linking entry from this page to free up space.
				while(!cursor.get().value.present()) {
					// There should never be an internal page written that has no valid child pages. This loop will find
					// the first valid child link, and if there are no more then execution will not return to this loop.
					ASSERT(cursor.moveNext());
				}

				ASSERT(cursor.valid());

				const RedwoodRecordRef &decodeChildLowerBound = cursor.get();

				LogicalPageID pageID = cursor.get().getPageID();
				ASSERT(pageID != 0);

				const RedwoodRecordRef &decodeChildUpperBound = cursor.moveNext() ? cursor.get() : *decodeUpperBound;

				// Skip over any next-children which do not actually link to child pages
				while(cursor.valid() && !cursor.get().value.present()) {
					cursor.moveNext();
				}

				const RedwoodRecordRef &childUpperBound = cursor.valid() ? cursor.get() : *upperBound;

				debug_printf("%s recursing to PageID=%u lower=%s upper=%s decodeLower=%s decodeUpper=%s\n",
					context.c_str(), pageID, childLowerBound.toString().c_str(), childUpperBound.toString().c_str(), decodeChildLowerBound.toString().c_str(), decodeChildUpperBound.toString().c_str());

				/*
				// TODO: If lower bound and upper bound have the same key, do something intelligent if possible
				// 
				if(childLowerBound.key == childUpperBound.key) {
					if(key is modified or cleared) {
						if(self->singleVersion) {
							// In single version mode, don't keep any records with the old key if the key is modified, so return
							// an empty page set to replace the child page
							futureChildren.push_back(VersionedChildrenT({ {0,{} } }));
						}
						else {
							// In versioned mode, there is no need to recurse to this page because new versions of key
							// will go in the right most page that has the same lowerBound key, but since the key is
							// being changed the new version of this page should exclude the old subtree

						}
						else {
							// Return the child page as-is, no need to visit it
							futureChildren.push_back(VersionedChildrenT({ {0,{{childLowerBound, pageID}}} }));
						}
					}
					else {
						// No changes
						futureChildren.push_back(VersionedChildrenT({ {0,{{childLowerBound, pageID}}} }));
					}
				}
				else {
					futureChildren.push_back(self->commitSubtree(self, mutationBuffer, snapshot, pageID, &childLowerBound, &childUpperBound));
				}
				*/
				futureChildren.push_back(self->commitSubtree(self, mutationBuffer, snapshot, pageID, &childLowerBound, &childUpperBound, &decodeChildLowerBound, &decodeChildUpperBound));
			}

			// Waiting one at a time makes debugging easier
			// TODO:  Is it better to use waitForAll()?
			state int k;
			for(k = 0; k < futureChildren.size(); ++k) {
				wait(success(futureChildren[k]));
			}

			if(REDWOOD_DEBUG) {
 				debug_printf("%s Subtree update results for root PageID=%u\n", context.c_str(), root);
				for(int i = 0; i < futureChildren.size(); ++i) {
					debug_printf("%s subtree result %s\n", context.c_str(), toString(futureChildren[i].get()).c_str());
				}
			}

			// TODO:  Handle multi-versioned results
			ASSERT(self->singleVersion);
			cursor.moveFirst();
			InternalPageBuilder pageBuilder(cursor);

			for(int i = 0; i < futureChildren.size(); ++i) {
				const VersionedChildrenT &versionedChildren = futureChildren[i].get();
				ASSERT(versionedChildren.size() <= 1);

				if(!versionedChildren.empty()) {
					pageBuilder.addEntries(versionedChildren.front());
				}
			}

			pageBuilder.finalize(*upperBound, *decodeUpperBound);

			// If page contents have changed
			if(pageBuilder.modified) {
				// If the page now has no children
				if(pageBuilder.childPageCount == 0) {
					// If we are the root, write a new empty btree
					if(root == 0) {
						Reference<IPage> page = self->m_pager->newPageBuffer();
						makeEmptyPage(page, BTreePage::IS_LEAF, self->m_usablePageSizeOverride);
						RedwoodRecordRef rootEntry = dbBegin.withPageID(0);
						self->writePage(0, page, self->getLastCommittedVersion() + 1, &dbBegin, &dbEnd);
						VersionedChildrenT c({ {0, {dbBegin}, dbEnd } });
						debug_printf("%s id=%u All root page children were deleted, rewrote root as leaf, returning %s\n", context.c_str(), root, toString(c).c_str());
						return c;
					}
					else {
						VersionedChildrenT c({});
						debug_printf("%s id=%u All internal page children were deleted #1 so deleting this page too, returning %s\n", context.c_str(), root, toString(c).c_str());
						return c;
					}
				}
				else {
					debug_printf("%s Internal PageID=%u modified, creating replacements.\n", context.c_str(), root);
					debug_printf("%s newChildren=%s  lastUpperBound=%s  upperBound=%s\n", context.c_str(), toString(pageBuilder.entries).c_str(), pageBuilder.lastUpperBound.toString().c_str(), upperBound->toString().c_str());

					ASSERT(pageBuilder.lastUpperBound == *upperBound);

					// TODO: Don't do this!
					std::vector<RedwoodRecordRef> entries;
					for(auto &o : pageBuilder.entries) {
						entries.push_back(o);
					}

					std::vector<BoundaryAndPage> pages = buildPages(false, *lowerBound, *upperBound, entries, 0, [=](){ return self->m_pager->newPageBuffer(); }, self->m_usablePageSizeOverride);

					Version writeVersion = self->getLastCommittedVersion() + 1;
					std::vector<LogicalPageID> newPageIDs = self->writePages(pages, writeVersion, root, page, upperBound, THIS);

					// If this commitSubtree() is operating on the root, write new levels if needed until until we're returning a single page
					if(root == self->m_root) {
						self->buildNewRoot(writeVersion, pages, newPageIDs, page);
					}

					VersionedChildrenT vc(1);
					vc.resize(1);
					VersionedChildPageSet &c = vc.front();
					c.version = writeVersion;
					c.upperBound = *upperBound;

					for(int i=0; i<pages.size(); i++) {
						c.children.push_back(pages[i].lowerBound.withPageID(newPageIDs[i]));
					}

					debug_printf("%s Internal PageID=%u modified, returning %s\n", context.c_str(), root, toString(c).c_str());
					return vc;
				}
			}
			else {
				VersionedChildrenT c( { {0, {*decodeLowerBound}, *decodeUpperBound} });
				debug_printf("%s PageID=%u has no changes, returning %s\n", context.c_str(), root, toString(c).c_str());
				return c;
			}
		}
	}

	ACTOR static Future<Void> commit_impl(VersionedBTree *self) {
		state MutationBufferT *mutations = self->m_pBuffer;

		// No more mutations are allowed to be written to this mutation buffer we will commit
		// at m_writeVersion, which we must save locally because it could change during commit.
		self->m_pBuffer = nullptr;
		state Version writeVersion = self->m_writeVersion;

		// The latest mutation buffer start version is the one we will now (or eventually) commit.
		state Version mutationBufferStartVersion = self->m_mutationBuffers.rbegin()->first;

		// Replace the lastCommit future with a new one and then wait on the old one
		state Promise<Void> committed;
		Future<Void> previousCommit = self->m_latestCommit;
		self->m_latestCommit = committed.getFuture();

		// Wait for the latest commit that started to be finished.
		wait(previousCommit);
		debug_printf("%s: Beginning commit of version %" PRId64 "\n", self->m_name.c_str(), writeVersion);

		// Get the latest version from the pager, which is what we will read at
		Version latestVersion = wait(self->m_pager->getLatestVersion());
		debug_printf("%s: pager latestVersion %" PRId64 "\n", self->m_name.c_str(), latestVersion);

		if(REDWOOD_DEBUG) {
			self->printMutationBuffer(mutations);
		}

		VersionedChildrenT newRoot = wait(commitSubtree(self, mutations, self->m_pager->getReadSnapshot(latestVersion), self->m_root, &dbBegin, &dbEnd, &dbBegin, &dbEnd));

		self->m_pager->setLatestVersion(writeVersion);
		debug_printf("%s: Committing pager %" PRId64 "\n", self->m_name.c_str(), writeVersion);
		wait(self->m_pager->commit());
		debug_printf("%s: Committed version %" PRId64 "\n", self->m_name.c_str(), writeVersion);

		// Now that everything is committed we must delete the mutation buffer.
		// Our buffer's start version should be the oldest mutation buffer version in the map.
		ASSERT(mutationBufferStartVersion == self->m_mutationBuffers.begin()->first);
		self->m_mutationBuffers.erase(self->m_mutationBuffers.begin());

		self->m_lastCommittedVersion = writeVersion;
		++self->counts.commits;
printf("\nCommitted: %s\n", self->counts.toString(true).c_str());
		committed.send(Void());

		return Void();
	}

	// InternalCursor is for seeking to and iterating over the 'internal' records (not user-visible) in the Btree.
	// These records are versioned and they can represent deletedness or partial values.
	struct InternalCursor {
	private:
		// Each InternalCursor's position is represented by a reference counted PageCursor, which links
		// to its parent PageCursor, up to a PageCursor representing a cursor on the root page.
		// PageCursors can be shared by many InternalCursors, making InternalCursor copying low overhead
		struct PageCursor : ReferenceCounted<PageCursor>, FastAllocated<PageCursor> {
			Reference<PageCursor> parent;
			LogicalPageID pageID;       // Only needed for debugging purposes
			Reference<const IPage> page;
			BTreePage::BinaryTree::Cursor cursor;

			PageCursor(LogicalPageID id, Reference<const IPage> page, Reference<PageCursor> parent = {})
				: pageID(id), page(page), parent(parent), cursor(getReader().getCursor())
			{
			}

			PageCursor(const PageCursor &toCopy) : parent(toCopy.parent), pageID(toCopy.pageID), page(toCopy.page), cursor(toCopy.cursor) {
			}

			// Convenience method for copying a PageCursor
			Reference<PageCursor> copy() const {
				return Reference<PageCursor>(new PageCursor(*this));
			}

			// Multiple InternalCursors can share a Page 
			BTreePage::BinaryTree::Reader & getReader() const {
				return *(BTreePage::BinaryTree::Reader *)page->userData;
			}

			bool isLeaf() const {
				const BTreePage *p = ((const BTreePage *)page->begin());
				return p->isLeaf();
			}

			Future<Reference<PageCursor>> getChild(Reference<IPagerSnapshot> pager, int usablePageSizeOverride) {
				ASSERT(!isLeaf());
				BTreePage::BinaryTree::Cursor next = cursor;
				next.moveNext();
				const RedwoodRecordRef &rec = cursor.get();
				LogicalPageID id = rec.getPageID();
				Future<Reference<const IPage>> child = readPage(pager, id, usablePageSizeOverride, &rec, &next.getOrUpperBound());
				return map(child, [=](Reference<const IPage> page) {
					return Reference<PageCursor>(new PageCursor(id, page, Reference<PageCursor>::addRef(this)));
				});
			}

			std::string toString() const {
				return format("PageID=%u, %s", pageID, cursor.valid() ? cursor.get().toString().c_str() : "<invalid>");
			}
		};

		LogicalPageID rootPageID;
		int usablePageSizeOverride;
		Reference<IPagerSnapshot> pager;
		Reference<PageCursor> pageCursor;

	public:
		InternalCursor() {
		}

		InternalCursor(Reference<IPagerSnapshot> pager, LogicalPageID root, int usablePageSizeOverride)
			: pager(pager), rootPageID(root), usablePageSizeOverride(usablePageSizeOverride) {
		}

		std::string toString() const {
			std::string r;

			Reference<PageCursor> c = pageCursor;
			int maxDepth = 0;
			while(c) {
				c = c->parent;
				++maxDepth;
			}

			c = pageCursor;
			int depth = maxDepth;
			while(c) {
				r = format("[%d/%d: %s] ", depth--, maxDepth, c->toString().c_str()) + r;
				c = c->parent;
			}
			return r;
		}

		// Returns true if cursor position is a valid leaf page record
		bool valid() const {
			return pageCursor && pageCursor->isLeaf() && pageCursor->cursor.valid();
		}

		// Returns true if cursor position is valid() and has a present record value
		bool present() {
			return valid() && pageCursor->cursor.get().value.present();
		}

		// Returns true if cursor position is present() and has an effective version <= v
		bool presentAtVersion(Version v) {
			return present() && pageCursor->cursor.get().version <= v;
		}

		// Returns true if cursor position is present() and has an effective version <= v
		bool validAtVersion(Version v) {
			return valid() && pageCursor->cursor.get().version <= v;
		}

		const RedwoodRecordRef & get() const {
			return pageCursor->cursor.get();
		}

		// Ensure that pageCursor is not shared with other cursors so we can modify it
		void ensureUnshared() {
			if(!pageCursor->isSoleOwner()) {
				pageCursor = pageCursor->copy();
			}
		}

		Future<Void> moveToRoot() {
			// If pageCursor exists follow parent links to the root
			if(pageCursor) {
				while(pageCursor->parent) {
					pageCursor = pageCursor->parent;
				}
				return Void();
			}

			// Otherwise read the root page
			Future<Reference<const IPage>> root = readPage(pager, rootPageID, usablePageSizeOverride, &dbBegin, &dbEnd);
			return map(root, [=](Reference<const IPage> p) {
				pageCursor = Reference<PageCursor>(new PageCursor(rootPageID, p));
				return Void();
			});
		}

		ACTOR Future<bool> seekLessThanOrEqual_impl(InternalCursor *self, RedwoodRecordRef query) {
			Future<Void> f = self->moveToRoot();

			// f will almost always be ready
			if(!f.isReady()) {
				wait(f);
			}

			self->ensureUnshared();

			loop {
				bool success = self->pageCursor->cursor.seekLessThanOrEqual(query);

				// Skip backwards over internal page entries that do not link to child pages
				if(!self->pageCursor->isLeaf()) {
					// While record has no value, move again
					while(success && !self->pageCursor->cursor.get().value.present()) {
						success = self->pageCursor->cursor.movePrev();
					}
				}

				if(success) {
					// If we found a record <= query at a leaf page then return success
					if(self->pageCursor->isLeaf()) {
						return true;
					}

					Reference<PageCursor> child = wait(self->pageCursor->getChild(self->pager, self->usablePageSizeOverride));
					self->pageCursor = child;
				}
				else {
					// No records <= query on this page, so move to immediate previous record at leaf level
					bool success = wait(self->move(false));
					return success;
				}
			}
		}

		Future<bool> seekLTE(RedwoodRecordRef query) {
			return seekLessThanOrEqual_impl(this, query);
		}

		ACTOR Future<bool> move_impl(InternalCursor *self, bool forward) {
			// Try to move pageCursor, if it fails to go parent, repeat until it works or root cursor can't be moved
			while(1) {
				self->ensureUnshared();
				bool success = self->pageCursor->cursor.valid() && (forward ? self->pageCursor->cursor.moveNext() : self->pageCursor->cursor.movePrev());

				// Skip over internal page entries that do not link to child pages
				if(!self->pageCursor->isLeaf()) {
					// While record has no value, move again
					while(success && !self->pageCursor->cursor.get().value.present()) {
						success = forward ? self->pageCursor->cursor.moveNext() : self->pageCursor->cursor.movePrev();
					}
				}

				// Stop if successful or there's no parent to move to
				if(success || !self->pageCursor->parent) {
					break;
				}

				// Move to parent
				self->pageCursor = self->pageCursor->parent;
			}

			// If pageCursor not valid we've reached an end of the tree
			if(!self->pageCursor->cursor.valid()) {
				return false;
			}

			// While not on a leaf page, move down to get to one.
			while(!self->pageCursor->isLeaf()) {
				// Skip over internal page entries that do not link to child pages
				while(!self->pageCursor->cursor.get().value.present()) {
					bool success = forward ? self->pageCursor->cursor.moveNext() : self->pageCursor->cursor.movePrev();
					if(!success) {
						return false;
					}
				}

				Reference<PageCursor> child = wait(self->pageCursor->getChild(self->pager, self->usablePageSizeOverride));
				forward ? child->cursor.moveFirst() : child->cursor.moveLast();
				self->pageCursor = child;
			}

			return true;
		}

		Future<bool> move(bool forward) {
			return move_impl(this, forward);
		}

		Future<bool> moveNext() {
			return move_impl(this, true);
		}
		Future<bool> movePrev() {
			return move_impl(this, false);
		}

		// Move to the first or last record of the database.
		ACTOR Future<bool> move_end(InternalCursor *self, bool begin) {
			Future<Void> f = self->moveToRoot();

			// f will almost always be ready
			if(!f.isReady()) {
				wait(f);
			}

			self->ensureUnshared();

			loop {
				// Move to first or last record in the page
				bool success = begin ? self->pageCursor->cursor.moveFirst() : self->pageCursor->cursor.moveLast();

				// Skip over internal page entries that do not link to child pages
				if(!self->pageCursor->isLeaf()) {
					// While record has no value, move past it
					while(success && !self->pageCursor->cursor.get().value.present()) {
						success = begin ? self->pageCursor->cursor.moveNext() : self->pageCursor->cursor.movePrev();
					}
				}

				// If it worked, return true if we've reached a leaf page otherwise go to the next child
				if(success) {
					if(self->pageCursor->isLeaf()) {
						return true;
					}

					Reference<PageCursor> child = wait(self->pageCursor->getChild(self->pager, self->usablePageSizeOverride));
					self->pageCursor = child;
				}
				else {
					return false;
				}
			}
		}

		Future<bool> moveFirst() {
			return move_end(this, true);
		}
		Future<bool> moveLast() {
			return move_end(this, false);
		}

	};

	// Cursor is for reading and interating over user visible KV pairs at a specific version
	// KeyValueRefs returned become invalid once the cursor is moved
	class Cursor : public IStoreCursor, public ReferenceCounted<Cursor>, public FastAllocated<Cursor>, NonCopyable  {
	public:
		Cursor(Reference<IPagerSnapshot> pageSource, LogicalPageID root, Version recordVersion, int usablePageSizeOverride)
			: m_version(recordVersion),
			m_cur1(pageSource, root, usablePageSizeOverride),
			m_cur2(m_cur1)
		{
		}

		void addref() { ReferenceCounted<Cursor>::addref(); }
		void delref() { ReferenceCounted<Cursor>::delref(); }

	private:
		Version m_version;
		// If kv is valid
		//   - kv.key references memory held by cur1
		//   - If cur1 points to a non split KV pair
		//       - kv.value references memory held by cur1
		//       - cur2 points to the next internal record after cur1
		//     Else
		//       - kv.value references memory in arena
		//       - cur2 points to the first internal record of the split KV pair
		InternalCursor m_cur1;
		InternalCursor m_cur2;
		Arena m_arena;
		Optional<KeyValueRef> m_kv;

	public:
		virtual Future<Void> findEqual(KeyRef key) { return find_impl(this, key, true, 0); }
		virtual Future<Void> findFirstEqualOrGreater(KeyRef key, bool needValue, int prefetchNextBytes) { return find_impl(this, key, needValue, 1); }
		virtual Future<Void> findLastLessOrEqual(KeyRef key, bool needValue, int prefetchPriorBytes) { return find_impl(this, key, needValue, -1); }

		virtual Future<Void> next(bool needValue) { return move(this, true, needValue); }
		virtual Future<Void> prev(bool needValue) { return move(this, false, needValue); }

		virtual bool isValid() {
			return m_kv.present();
		}

		virtual KeyRef getKey() {
			return m_kv.get().key;
		}

		//virtual StringRef getCompressedKey() = 0;
		virtual ValueRef getValue() {
			return m_kv.get().value;
		}

		// TODO: Either remove this method or change the contract so that key and value strings returned are still valid after the cursor is
		// moved and allocate them in some arena that this method resets.
		virtual void invalidateReturnedStrings() {
		}

		std::string toString() const {
			std::string r;
			r += format("Cursor(%p) ver: %" PRId64 " ", this, m_version);
			if(m_kv.present()) {
				r += format("  KV: '%s' -> '%s'\n", m_kv.get().key.printable().c_str(), m_kv.get().value.printable().c_str());
			}
			else {
				r += "  KV: <np>\n";
			}
			r += format("  Cur1: %s\n", m_cur1.toString().c_str());
			r += format("  Cur2: %s\n", m_cur2.toString().c_str());

			return r;
		}

	private:
		// find key in tree closest to or equal to key (at this cursor's version)
		// for less than or equal use cmp < 0
		// for greater than or equal use cmp > 0
		// for equal use cmp == 0
		ACTOR static Future<Void> find_impl(Cursor *self, KeyRef key, bool needValue, int cmp) {
			// Search for the last key at or before (key, version, \xff)
			state RedwoodRecordRef query(key, self->m_version, {}, 0, std::numeric_limits<int32_t>::max());
			self->m_kv.reset();

			wait(success(self->m_cur1.seekLTE(query)));
			debug_printf("find%sE(%s): %s\n", cmp > 0 ? "GT" : (cmp == 0 ? "" : "LT"), query.toString().c_str(), self->toString().c_str());

			// If we found the target key with a present value then return it as it is valid for any cmp type
			if(self->m_cur1.present() && self->m_cur1.get().key == key) {
				debug_printf("Target key found, reading full KV pair.  Cursor: %s\n", self->toString().c_str());
				wait(self->readFullKVPair(self));
				return Void();
			}

			// Mode is ==, so if we're still here we didn't find it.
			if(cmp == 0) {
				return Void();
			}

			// Mode is >=, so if we're here we have to go to the next present record at the target version
			// because the seek done above was <= query
			if(cmp > 0) {
				// icur is at a record < query or invalid.

				// If cursor is invalid, try to go to start of tree
				if(!self->m_cur1.valid()) {
					bool valid = wait(self->m_cur1.moveFirst());
					if(!valid) {
						self->m_kv.reset();
						return Void();
					}
				}
				else {
					loop {
						bool valid = wait(self->m_cur1.move(true));
						if(!valid) {
							self->m_kv.reset();
							return Void();
						}

						if(self->m_cur1.get().key > key) {
							break;
						}
					}
				}

				// Get the next present key at the target version.  Handles invalid cursor too.
				wait(self->next(needValue));
			}
			else if(cmp < 0) {
				// Mode is <=, which is the same as the seekLTE(query)
				if(!self->m_cur1.valid()) {
					self->m_kv.reset();
					return Void();
				}

				// Move to previous present kv pair at the target version
				wait(self->prev(needValue));
			}

			return Void();
		}

		// TODO: use needValue
		ACTOR static Future<Void> move(Cursor *self, bool fwd, bool needValue) {
			debug_printf("Cursor::move(%d): Cursor = %s\n", fwd, self->toString().c_str());
			ASSERT(self->m_cur1.valid());

			// If kv is present then the key/version at cur1 was already returned so move to a new key
			// Move cur1 until failure or a new key is found, keeping prior record visited in cur2
			if(self->m_kv.present()) {
				ASSERT(self->m_cur1.valid());
				loop {
					self->m_cur2 = self->m_cur1;
					bool valid = wait(self->m_cur1.move(fwd));
					if(!valid || self->m_cur1.get().key != self->m_cur2.get().key) {
						break;
					}
				}
			}

			// Given two consecutive cursors c1 and c2, c1 represents a returnable record if
			//    c1.presentAtVersion(v) || (!c2.validAtVersion() || c2.get().key != c1.get().key())
			// Note the distinction between 'present' and 'valid'.  Present means the value for the key
			// exists at the version (but could be the empty string) while valid just means the internal
			// record is in effect at that version but it could indicate that the key was cleared and
			// no longer exists from the user's perspective at that version
			//
			// cur2 must be the record immediately after cur1
			// TODO:  This may already be the case, store state to track this condition and avoid the reset here
			if(self->m_cur1.valid()) {
				self->m_cur2 = self->m_cur1;
				wait(success(self->m_cur2.move(true)));
			}

			while(self->m_cur1.valid()) {

				if(self->m_cur1.presentAtVersion(self->m_version) &&
					(!self->m_cur2.validAtVersion(self->m_version) ||
					self->m_cur2.get().key != self->m_cur1.get().key)
				) {
					wait(readFullKVPair(self));
					return Void();
				}

				if(fwd) {
					// Moving forward, move cur2 forward and keep cur1 pointing to the prior (predecessor) record
					debug_printf("Cursor::move(%d): Moving forward, Cursor = %s\n", fwd, self->toString().c_str());
					self->m_cur1 = self->m_cur2;
					wait(success(self->m_cur2.move(true)));
				}
				else {
					// Moving backward, move cur1 backward and keep cur2 pointing to the prior (successor) record
					debug_printf("Cursor::move(%d): Moving backward, Cursor = %s\n", fwd, self->toString().c_str());
					self->m_cur2 = self->m_cur1;
					wait(success(self->m_cur1.move(false)));
				}

			}

			self->m_kv.reset();
			debug_printf("Cursor::move(%d): Exit, end of db reached.  Cursor = %s\n", fwd, self->toString().c_str());
			return Void();
		}

		// Read all of the current key-value record starting at cur1 into kv
		ACTOR static Future<Void> readFullKVPair(Cursor *self) {
			self->m_arena = Arena();
			const RedwoodRecordRef &rec = self->m_cur1.get();
	
			debug_printf("readFullKVPair:  Starting at %s\n", self->toString().c_str());

			// Unsplit value, cur1 will hold the key and value memory
			if(!rec.isMultiPart()) {
				self->m_kv = KeyValueRef(rec.key, rec.value.get());
				debug_printf("readFullKVPair:  Unsplit, exit.  %s\n", self->toString().c_str());

				return Void();
			}

			// Split value, need to coalesce split value parts into a buffer in arena,
			// after which cur1 will point to the first part and kv.key will reference its key
			ASSERT(rec.chunk.start + rec.value.get().size() == rec.chunk.total);

			debug_printf("readFullKVPair:  Split, totalsize %d  %s\n", rec.chunk.total, self->toString().c_str());

			// Allocate space for the entire value in the same arena as the key
			state int bytesLeft = rec.chunk.total;
			state StringRef dst = makeString(bytesLeft, self->m_arena);

			loop {
				const RedwoodRecordRef &rec = self->m_cur1.get();

				debug_printf("readFullKVPair:  Adding chunk %s\n", rec.toString().c_str());

				int partSize = rec.value.get().size();
				memcpy(mutateString(dst) + rec.chunk.start, rec.value.get().begin(), partSize);
				bytesLeft -= partSize;
				if(bytesLeft == 0) {
					self->m_kv = KeyValueRef(rec.key, dst);
					return Void();
				}
				ASSERT(bytesLeft > 0);
				// Move backward
				bool success = wait(self->m_cur1.move(false));
				ASSERT(success);
			}
		}
	};

};

RedwoodRecordRef VersionedBTree::dbBegin(StringRef(), 0);
RedwoodRecordRef VersionedBTree::dbEnd(LiteralStringRef("\xff\xff\xff\xff\xff"));
VersionedBTree::Counts VersionedBTree::counts;

ACTOR template<class T>
Future<T> catchError(Promise<Void> error, Future<T> f) {
	try {
		T result = wait(f);
		return result;
	} catch(Error &e) {
		if(e.code() != error_code_actor_cancelled && error.canBeSet())
			error.sendError(e);
		throw;
	}
}

class KeyValueStoreRedwoodUnversioned : public IKeyValueStore {
public:
	KeyValueStoreRedwoodUnversioned(std::string filePrefix, UID logID) : m_filePrefix(filePrefix) {
		// TODO: This constructor should really just take an IVersionedStore
		IPager *pager = new IndirectShadowPager(filePrefix);
		m_tree = new VersionedBTree(pager, filePrefix, true, pager->getUsablePageSize());
		m_init = catchError(init_impl(this));
	}

	virtual Future<Void> init() {
		return m_init;
	}

	ACTOR Future<Void> init_impl(KeyValueStoreRedwoodUnversioned *self) {
		TraceEvent(SevInfo, "RedwoodInit").detail("FilePrefix", self->m_filePrefix);
		wait(self->m_tree->init());
		Version v = wait(self->m_tree->getLatestVersion());
		self->m_tree->setWriteVersion(v + 1);
		TraceEvent(SevInfo, "RedwoodInitComplete").detail("FilePrefix", self->m_filePrefix);
		return Void();
	}

	ACTOR void shutdown(KeyValueStoreRedwoodUnversioned *self, bool dispose) {
		TraceEvent(SevInfo, "RedwoodShutdown").detail("FilePrefix", self->m_filePrefix).detail("Dispose", dispose);
		if(self->m_error.canBeSet()) {
			self->m_error.sendError(actor_cancelled());  // Ideally this should be shutdown_in_progress
		}
		self->m_init.cancel();
		Future<Void> closedFuture = self->m_tree->onClosed();
		if(dispose)
			self->m_tree->dispose();
		else
			self->m_tree->close();
		wait(closedFuture);
		self->m_closed.send(Void());
		TraceEvent(SevInfo, "RedwoodShutdownComplete").detail("FilePrefix", self->m_filePrefix).detail("Dispose", dispose);
		delete self;
	}

	virtual void close() {
		shutdown(this, false);
	}

	virtual void dispose() {
		shutdown(this, true);
	}

	virtual Future< Void > onClosed() {
		return m_closed.getFuture();
	}

	Future<Void> commit(bool sequential = false) {
		Future<Void> c = m_tree->commit();
		m_tree->setWriteVersion(m_tree->getWriteVersion() + 1);
		return catchError(c);
	}

	virtual KeyValueStoreType getType() {
		return KeyValueStoreType::SSD_REDWOOD_V1;
	}

	virtual StorageBytes getStorageBytes() {
		return m_tree->getStorageBytes();
	}

	virtual Future< Void > getError() {
		return delayed(m_error.getFuture());
	};

	void clear(KeyRangeRef range, const Arena* arena = 0) {
		debug_printf("CLEAR %s\n", printable(range).c_str());
		m_tree->clear(range);
	}

    virtual void set( KeyValueRef keyValue, const Arena* arena = NULL ) {
		debug_printf("SET %s\n", keyValue.key.printable().c_str());
		m_tree->set(keyValue);
	}

	virtual Future< Standalone< VectorRef< KeyValueRef > > > readRange(KeyRangeRef keys, int rowLimit = 1<<30, int byteLimit = 1<<30) {
		debug_printf("READRANGE %s\n", printable(keys).c_str());
		return catchError(readRange_impl(this, keys, rowLimit, byteLimit));
	}

	ACTOR static Future< Standalone< VectorRef< KeyValueRef > > > readRange_impl(KeyValueStoreRedwoodUnversioned *self, KeyRange keys, int rowLimit, int byteLimit) {
		self->m_tree->counts.getRanges++;
		state Standalone<VectorRef<KeyValueRef>> result;
		state int accumulatedBytes = 0;
		ASSERT( byteLimit > 0 );

		state Reference<IStoreCursor> cur = self->m_tree->readAtVersion(self->m_tree->getLastCommittedVersion());

		if(rowLimit >= 0) {
			wait(cur->findFirstEqualOrGreater(keys.begin, true, 0));
			while(cur->isValid() && cur->getKey() < keys.end) {
				KeyValueRef kv(KeyRef(result.arena(), cur->getKey()), ValueRef(result.arena(), cur->getValue()));
				accumulatedBytes += kv.expectedSize();
				result.push_back(result.arena(), kv);
				if(--rowLimit == 0 || accumulatedBytes >= byteLimit) {
					break;
				}
				wait(cur->next(true));
			}
		} else {
			wait(cur->findLastLessOrEqual(keys.end, true, 0));
			if(cur->isValid() && cur->getKey() == keys.end)
				wait(cur->prev(true));

			while(cur->isValid() && cur->getKey() >= keys.begin) {
				KeyValueRef kv(KeyRef(result.arena(), cur->getKey()), ValueRef(result.arena(), cur->getValue()));
				accumulatedBytes += kv.expectedSize();
				result.push_back(result.arena(), kv);
				if(--rowLimit == 0 || accumulatedBytes >= byteLimit) {
					break;
				}
				wait(cur->prev(true));
			}
		}
		return result;
	}

	ACTOR static Future< Optional<Value> > readValue_impl(KeyValueStoreRedwoodUnversioned *self, Key key, Optional< UID > debugID) {
		self->m_tree->counts.gets++;
		state Reference<IStoreCursor> cur = self->m_tree->readAtVersion(self->m_tree->getLastCommittedVersion());

		wait(cur->findEqual(key));
		if(cur->isValid()) {
			return cur->getValue();
		}
		return Optional<Value>();
	}

	virtual Future< Optional< Value > > readValue(KeyRef key, Optional< UID > debugID = Optional<UID>()) {
		return catchError(readValue_impl(this, key, debugID));
	}

	ACTOR static Future< Optional<Value> > readValuePrefix_impl(KeyValueStoreRedwoodUnversioned *self, Key key, int maxLength, Optional< UID > debugID) {
		self->m_tree->counts.gets++;
		state Reference<IStoreCursor> cur = self->m_tree->readAtVersion(self->m_tree->getLastCommittedVersion());

		wait(cur->findEqual(key));
		if(cur->isValid()) {
			Value v = cur->getValue();
			int len = std::min(v.size(), maxLength);
			return Value(cur->getValue().substr(0, len));
		}
		return Optional<Value>();
	}

	virtual Future< Optional< Value > > readValuePrefix(KeyRef key, int maxLength, Optional< UID > debugID = Optional<UID>()) {
		return catchError(readValuePrefix_impl(this, key, maxLength, debugID));
	}

	virtual ~KeyValueStoreRedwoodUnversioned() {
	};

private:
	std::string m_filePrefix;
	VersionedBTree *m_tree;
	Future<Void> m_init;
	Promise<Void> m_closed;
	Promise<Void> m_error;

	template <typename T> inline Future<T> catchError(Future<T> f) {
		return ::catchError(m_error, f);
	}
};

IKeyValueStore* keyValueStoreRedwoodV1( std::string const& filename, UID logID) {
	return new KeyValueStoreRedwoodUnversioned(filename, logID);
}

int randomSize(int max) {
	int n = pow(deterministicRandom()->random01(), 3) * max;
	return n;
}

StringRef randomString(Arena &arena, int len, char firstChar = 'a', char lastChar = 'z') {
	++lastChar;
	StringRef s = makeString(len, arena);
	for(int i = 0; i < len; ++i) {
		*(uint8_t *)(s.begin() + i) = (uint8_t)deterministicRandom()->randomInt(firstChar, lastChar);
	}
	return s;
}

Standalone<StringRef> randomString(int len, char firstChar = 'a', char lastChar = 'z') {
	Standalone<StringRef> s;
	(StringRef &)s = randomString(s.arena(), len, firstChar, lastChar);
	return s;
}

KeyValue randomKV(int maxKeySize = 10, int maxValueSize = 5) {
	int kLen = randomSize(1 + maxKeySize);
	int vLen = maxValueSize > 0 ? randomSize(maxValueSize) : 0;

	KeyValue kv;

	kv.key = randomString(kv.arena(), kLen, 'a', 'm');
	for(int i = 0; i < kLen; ++i)
		mutateString(kv.key)[i] = (uint8_t)deterministicRandom()->randomInt('a', 'm');

	if(vLen > 0) {
		kv.value = randomString(kv.arena(), vLen, 'n', 'z');
		for(int i = 0; i < vLen; ++i)
			mutateString(kv.value)[i] = (uint8_t)deterministicRandom()->randomInt('o', 'z');
	}

	return kv;
}

ACTOR Future<int> verifyRange(VersionedBTree *btree, Key start, Key end, Version v, std::map<std::pair<std::string, Version>, Optional<std::string>> *written, int *pErrorCount) {
	state int errors = 0;
	if(end <= start)
		end = keyAfter(start);

	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator i =    written->lower_bound(std::make_pair(start.toString(), 0));
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iEnd = written->upper_bound(std::make_pair(end.toString(),   0));
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iLast;

	state Reference<IStoreCursor> cur = btree->readAtVersion(v);
	debug_printf("VerifyRange(@%" PRId64 ", %s, %s): Start cur=%p\n", v, start.toString().c_str(), end.toString().c_str(), cur.getPtr());

	// Randomly use the cursor for something else first.
	if(deterministicRandom()->coinflip()) {
		state Key randomKey = randomKV().key;
		debug_printf("VerifyRange(@%" PRId64 ", %s, %s): Dummy seek to '%s'\n", v, start.toString().c_str(), end.toString().c_str(), randomKey.toString().c_str());
		wait(deterministicRandom()->coinflip() ? cur->findFirstEqualOrGreater(randomKey, true, 0) : cur->findLastLessOrEqual(randomKey, true, 0));
	}

	debug_printf("VerifyRange(@%" PRId64 ", %s, %s): Actual seek\n", v, start.toString().c_str(), end.toString().c_str());
	wait(cur->findFirstEqualOrGreater(start, true, 0));

	state std::vector<KeyValue> results;

	while(cur->isValid() && cur->getKey() < end) {
		// Find the next written kv pair that would be present at this version
		while(1) {
			iLast = i;
			if(i == iEnd)
				break;
			++i;

			if(iLast->first.second <= v
				&& iLast->second.present()
				&& (
					i == iEnd
					|| i->first.first != iLast->first.first
					|| i->first.second > v
				)
			) {
				debug_printf("VerifyRange(@%" PRId64 ", %s, %s) Found key in written map: %s\n", v, start.toString().c_str(), end.toString().c_str(), iLast->first.first.c_str());
				break;
			}
		}

		if(iLast == iEnd) {
			++errors;
			++*pErrorCount;
			printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' vs nothing in written map.\n", v, start.toString().c_str(), end.toString().c_str(), cur->getKey().toString().c_str());
			break;
		}

		if(cur->getKey() != iLast->first.first) {
			++errors;
			++*pErrorCount;
			printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' vs written '%s'\n", v, start.toString().c_str(), end.toString().c_str(), cur->getKey().toString().c_str(), iLast->first.first.c_str());
			break;
		}
		if(cur->getValue() != iLast->second.get()) {
			++errors;
			++*pErrorCount;
			printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' has tree value '%s' vs written '%s'\n", v, start.toString().c_str(), end.toString().c_str(), cur->getKey().toString().c_str(), cur->getValue().toString().c_str(), iLast->second.get().c_str());
			break;
		}

		ASSERT(errors == 0);

		results.push_back(KeyValue(KeyValueRef(cur->getKey(), cur->getValue())));
		wait(cur->next(true));
	}

	// Make sure there are no further written kv pairs that would be present at this version.
	while(1) {
		iLast = i;
		if(i == iEnd)
			break;
		++i;
		if(iLast->first.second <= v
			&& iLast->second.present()
			&& (
				i == iEnd
				|| i->first.first != iLast->first.first
				|| i->first.second > v
			)
		)
			break;
	}

	if(iLast != iEnd) {
		++errors;
		++*pErrorCount;
		printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree range ended but written has @%" PRId64 " '%s'\n", v, start.toString().c_str(), end.toString().c_str(), iLast->first.second, iLast->first.first.c_str());
	}

	debug_printf("VerifyRangeReverse(@%" PRId64 ", %s, %s): start\n", v, start.toString().c_str(), end.toString().c_str());

	// Randomly use a new cursor for the reverse range read but only if version history is available
	if(!btree->isSingleVersion() && deterministicRandom()->coinflip()) {
		cur = btree->readAtVersion(v);
	}

	// Now read the range from the tree in reverse order and compare to the saved results
	wait(cur->findLastLessOrEqual(end, true, 0));
	if(cur->isValid() && cur->getKey() == end)
		wait(cur->prev(true));

	state std::vector<KeyValue>::const_reverse_iterator r = results.rbegin();

	while(cur->isValid() && cur->getKey() >= start) {
		if(r == results.rend()) {
			++errors;
			++*pErrorCount;
			printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' vs nothing in written map.\n", v, start.toString().c_str(), end.toString().c_str(), cur->getKey().toString().c_str());
			break;
		}

		if(cur->getKey() != r->key) {
			++errors;
			++*pErrorCount;
			printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' vs written '%s'\n", v, start.toString().c_str(), end.toString().c_str(), cur->getKey().toString().c_str(), r->key.toString().c_str());
			break;
		}
		if(cur->getValue() != r->value) {
			++errors;
			++*pErrorCount;
			printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' has tree value '%s' vs written '%s'\n", v, start.toString().c_str(), end.toString().c_str(), cur->getKey().toString().c_str(), cur->getValue().toString().c_str(), r->value.toString().c_str());
			break;
		}

		++r;
		wait(cur->prev(true));
	}

	if(r != results.rend()) {
		++errors;
		++*pErrorCount;
		printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR: Tree range ended but written has '%s'\n", v, start.toString().c_str(), end.toString().c_str(), r->key.toString().c_str());
	}

	return errors;
}

ACTOR Future<int> verifyAll(VersionedBTree *btree, Version maxCommittedVersion, std::map<std::pair<std::string, Version>, Optional<std::string>> *written, int *pErrorCount) {
	// Read back every key at every version set or cleared and verify the result.
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator i = written->cbegin();
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iEnd = written->cend();
	state int errors = 0;

	while(i != iEnd) {
		state std::string key = i->first.first;
		state Version ver = i->first.second;
		if(ver <= maxCommittedVersion) {
			state Optional<std::string> val = i->second;

			state Reference<IStoreCursor> cur = btree->readAtVersion(ver);

			debug_printf("Verifying @%" PRId64 " '%s'\n", ver, key.c_str());
			state Arena arena;
			wait(cur->findEqual(KeyRef(arena, key)));

			if(val.present()) {
				if(!(cur->isValid() && cur->getKey() == key && cur->getValue() == val.get())) {
					++errors;
					++*pErrorCount;
					if(!cur->isValid())
						printf("Verify ERROR: key_not_found: '%s' -> '%s' @%" PRId64 "\n", key.c_str(), val.get().c_str(), ver);
					else if(cur->getKey() != key)
						printf("Verify ERROR: key_incorrect: found '%s' expected '%s' @%" PRId64 "\n", cur->getKey().toString().c_str(), key.c_str(), ver);
					else if(cur->getValue() != val.get())
						printf("Verify ERROR: value_incorrect: for '%s' found '%s' expected '%s' @%" PRId64 "\n", cur->getKey().toString().c_str(), cur->getValue().toString().c_str(), val.get().c_str(), ver);
				}
			} else {
				if(cur->isValid() && cur->getKey() == key) {
					++errors;
					++*pErrorCount;
					printf("Verify ERROR: cleared_key_found: '%s' -> '%s' @%" PRId64 "\n", key.c_str(), cur->getValue().toString().c_str(), ver);
				}
			}
		}
		++i;
	}
	return errors;
}

ACTOR Future<Void> verify(VersionedBTree *btree, FutureStream<Version> vStream, std::map<std::pair<std::string, Version>, Optional<std::string>> *written, int *pErrorCount, bool serial) {
	state Future<int> vall;
	state Future<int> vrange;

	try {
		loop {
			state Version v = waitNext(vStream);

			if(btree->isSingleVersion()) {
				v = btree->getLastCommittedVersion();
				debug_printf("Verifying at latest committed version %" PRId64 "\n", v);
				vall = verifyRange(btree, LiteralStringRef(""), LiteralStringRef("\xff\xff"), v, written, pErrorCount);
				if(serial) {
					wait(success(vall));
				}
				vrange = verifyRange(btree, randomKV().key, randomKV().key, v, written, pErrorCount);
				if(serial) {
					wait(success(vrange));
				}
			}
			else {
				debug_printf("Verifying through version %" PRId64 "\n", v);
				vall = verifyAll(btree, v, written, pErrorCount);
				if(serial) {
					wait(success(vall));
				}
				vrange = verifyRange(btree, randomKV().key, randomKV().key, deterministicRandom()->randomInt(1, v + 1), written, pErrorCount);
				if(serial) {
					wait(success(vrange));
				}
			}
			wait(success(vall) && success(vrange));

			debug_printf("Verified through version %" PRId64 ", %d errors\n", v, *pErrorCount);

			if(*pErrorCount != 0)
				break;
		}
	} catch(Error &e) {
		if(e.code() != error_code_end_of_stream) {
			throw;
		}
	}
	return Void();
}

// Does a random range read, doesn't trap/report errors
ACTOR Future<Void> randomReader(VersionedBTree *btree) {
	state Reference<IStoreCursor> cur;
	loop {
		wait(yield());
		if(!cur || deterministicRandom()->random01() > .1) {
			Version v = btree->getLastCommittedVersion();
			if(!btree->isSingleVersion()) {
				 v = deterministicRandom()->randomInt(1, v + 1);
			}
			cur = btree->readAtVersion(v);
		}

		state KeyValue kv = randomKV(10, 0);
		wait(cur->findFirstEqualOrGreater(kv.key, true, 0));
		state int c = deterministicRandom()->randomInt(0, 100);
		while(cur->isValid() && c-- > 0) {
			wait(success(cur->next(true)));
			wait(yield());
		}
	}
}

struct IntIntPair {
	IntIntPair() {}
	IntIntPair(int k, int v) : k(k), v(v) {}

	IntIntPair(Arena &arena, const IntIntPair &toCopy) {
		*this = toCopy;
	}

	struct Delta {
		bool prefixSource;
		int dk;
		int dv;

		IntIntPair apply(const IntIntPair &base, Arena &arena) {
			return {base.k + dk, base.v + dv};
		}

		void setPrefixSource(bool val) {
			prefixSource = val;
		}

		bool getPrefixSource() const {
			return prefixSource;
		}

		int size() const {
			return sizeof(Delta);
		}

		std::string toString() const {
			return format("DELTA{prefixSource=%d dk=%d(0x%x) dv=%d(0x%x)}", prefixSource, dk, dk, dv, dv);
		}
	};

	int compare(const IntIntPair &rhs) const {
		//printf("compare %s to %s\n", toString().c_str(), rhs.toString().c_str());
		return k - rhs.k;
	}

	bool operator==(const IntIntPair &rhs) const {
		return k == rhs.k;
	}

	int getCommonPrefixLen(const IntIntPair &other, int skip) const {
		return 0;
	}

	int deltaSize(const IntIntPair &base) const {
		return sizeof(Delta);
	}

	int writeDelta(Delta &d, const IntIntPair &base, int commonPrefix = -1) const {
		d.dk = k - base.k;
		d.dv = v - base.v;
		return sizeof(Delta);
	}

	int k;
	int v;

	std::string toString() const {
		return format("{k=%d(0x%x) v=%d(0x%x)}", k, k, v, v);
	}
};

int getCommonIntFieldPrefix2(const RedwoodRecordRef &a, const RedwoodRecordRef &b) {
	RedwoodRecordRef::byte aFields[RedwoodRecordRef::intFieldArraySize];
	RedwoodRecordRef::byte bFields[RedwoodRecordRef::intFieldArraySize];

	a.serializeIntFields(aFields);
	b.serializeIntFields(bFields);

	//printf("a: %s\n", StringRef(aFields, RedwoodRecordRef::intFieldArraySize).toHexString().c_str());
	//printf("b: %s\n", StringRef(bFields, RedwoodRecordRef::intFieldArraySize).toHexString().c_str());

	int i = 0;
	while(i < RedwoodRecordRef::intFieldArraySize && aFields[i] == bFields[i]) {
		++i;
	}

	//printf("%d\n", i);
	return i;
}

void deltaTest(RedwoodRecordRef rec, RedwoodRecordRef base) {
	char buf[500];
	RedwoodRecordRef::Delta &d = *(RedwoodRecordRef::Delta *)buf;

	Arena mem;
	int expectedSize = rec.deltaSize(base, false);
	int deltaSize = rec.writeDelta(d, base);
	RedwoodRecordRef decoded = d.apply(base, mem);

	if(decoded != rec || expectedSize != deltaSize) {
		printf("\n");
		printf("Base:         %s\n", base.toString().c_str());
		printf("ExpectedSize: %d\n", expectedSize);
		printf("DeltaSize:    %d\n", deltaSize);
		printf("Delta:        %s\n", d.toString().c_str());
		printf("Record:       %s\n", rec.toString().c_str());
		printf("Decoded:      %s\n", decoded.toString().c_str());
		printf("RedwoodRecordRef::Delta test failure!\n");
		ASSERT(false);
	}
}

Standalone<RedwoodRecordRef> randomRedwoodRecordRef(int maxKeySize = 3, int maxValueSize = 255) {
	RedwoodRecordRef rec;
	KeyValue kv = randomKV(3, 10);
	rec.key = kv.key;

	if(deterministicRandom()->random01() < .9) {
		rec.value = kv.value;
	}

	rec.version = deterministicRandom()->coinflip() ? 0 : deterministicRandom()->randomInt64(0, std::numeric_limits<Version>::max());

	if(deterministicRandom()->coinflip()) {
		rec.chunk.total = deterministicRandom()->randomInt(1, 100000);
		rec.chunk.start = deterministicRandom()->randomInt(0, rec.chunk.total);
	}

	return Standalone<RedwoodRecordRef>(rec, kv.arena());
}

TEST_CASE("!/redwood/correctness/unit/RedwoodRecordRef") {

	// Test pageID stuff.
	{
		LogicalPageID id = 1;
		RedwoodRecordRef r;
		r.setPageID(id);
		ASSERT(r.getPageID() == id);
		RedwoodRecordRef s;
		s = r;
		ASSERT(s.getPageID() == id);
		RedwoodRecordRef t(r);
		ASSERT(t.getPageID() == id);
		r.setPageID(id + 1);
		ASSERT(s.getPageID() == id);
		ASSERT(t.getPageID() == id);
	}

	// Testing common prefix calculation for integer fields using the member function that calculates this directly
	// and by serializing the integer fields to arrays and finding the common prefix length of the two arrays

	deltaTest(RedwoodRecordRef(LiteralStringRef(""), 0, LiteralStringRef(""), 0, 0),
			  RedwoodRecordRef(LiteralStringRef(""), 0, LiteralStringRef(""), 0, 0)
	);

	deltaTest(RedwoodRecordRef(LiteralStringRef("abc"), 0, LiteralStringRef(""), 0, 0),
			  RedwoodRecordRef(LiteralStringRef("abc"), 0, LiteralStringRef(""), 0, 0)
	);

	deltaTest(RedwoodRecordRef(LiteralStringRef("abc"),  0, LiteralStringRef(""), 0, 0),
			  RedwoodRecordRef(LiteralStringRef("abcd"), 0, LiteralStringRef(""), 0, 0)
	);

	deltaTest(RedwoodRecordRef(LiteralStringRef("abc"), 2, LiteralStringRef(""), 0, 0),
			  RedwoodRecordRef(LiteralStringRef("abc"), 2, LiteralStringRef(""), 0, 0)
	);

	deltaTest(RedwoodRecordRef(LiteralStringRef("abc"), 2, LiteralStringRef(""), 0, 0),
			  RedwoodRecordRef(LiteralStringRef("ab"),  2, LiteralStringRef(""), 1, 3)
	);

	deltaTest(RedwoodRecordRef(LiteralStringRef("abc"), 2, LiteralStringRef(""), 5, 0),
			  RedwoodRecordRef(LiteralStringRef("abc"), 2, LiteralStringRef(""), 5, 1)
	);

	RedwoodRecordRef::byte varInts[100];
	RedwoodRecordRef::Writer w(varInts);
	RedwoodRecordRef::Reader r(varInts);
	w.writeVarInt(1);
	w.writeVarInt(128);
	w.writeVarInt(32000);
	ASSERT(r.readVarInt() == 1);
	ASSERT(r.readVarInt() == 128);
	ASSERT(r.readVarInt() == 32000);

	RedwoodRecordRef rec1;
	RedwoodRecordRef rec2;

	rec1.version = 0x12345678;
	rec2.version = 0x12995678;
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == 5);
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == getCommonIntFieldPrefix2(rec1, rec2));

	rec1.version = 0x12345678;
	rec2.version = 0x12345678;
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == 14);
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == getCommonIntFieldPrefix2(rec1, rec2));

	rec1.version = invalidVersion;
	rec2.version = 0;
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == 0);
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == getCommonIntFieldPrefix2(rec1, rec2));

	rec1.version = 0x12345678;
	rec2.version = 0x12345678;
	rec1.chunk.total = 4;
	rec2.chunk.total = 4;
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == 14);
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == getCommonIntFieldPrefix2(rec1, rec2));

	rec1.version = 0x12345678;
	rec2.version = 0x12345678;
	rec1.chunk.start = 4;
	rec2.chunk.start = 4;
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == 14);
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == getCommonIntFieldPrefix2(rec1, rec2));

	rec1.version = 0x12345678;
	rec2.version = 0x12345678;
	rec1.chunk.start = 4;
	rec2.chunk.start = 5;
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == 13);
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == getCommonIntFieldPrefix2(rec1, rec2));

	rec1.version = 0x12345678;
	rec2.version = 0x12345678;
	rec1.chunk.total = 256;
	rec2.chunk.total = 512;
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == 9);
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == getCommonIntFieldPrefix2(rec1, rec2));

	Arena mem;
	double start;
	uint64_t total;
	uint64_t count;
	uint64_t i;

	start = timer();
	total = 0;
	count = 1e9;
	for(i = 0; i < count; ++i) {
		rec1.chunk.total = i & 0xffffff;
		rec2.chunk.total = i & 0xffffff;
		rec1.chunk.start = i & 0xffffff;
		rec2.chunk.start = (i + 1) & 0xffffff;
		total += rec1.getCommonIntFieldPrefix(rec2);
	}
	printf("%" PRId64 " getCommonIntFieldPrefix() %g M/s\n", total, count / (timer() - start) / 1e6);

	rec1.key = LiteralStringRef("alksdfjaklsdfjlkasdjflkasdjfklajsdflk;ajsdflkajdsflkjadsf");
	rec2.key = LiteralStringRef("alksdfjaklsdfjlkasdjflkasdjfklajsdflk;ajsdflkajdsflkjadsf");

	start = timer();
	total = 0;
	count = 1e9;
	for(i = 0; i < count; ++i) {
		RedwoodRecordRef::byte fields[RedwoodRecordRef::intFieldArraySize];
		rec1.chunk.start = i & 0xffffff;
		rec2.chunk.start = (i + 1) & 0xffffff;
		rec1.serializeIntFields(fields);
		total += fields[RedwoodRecordRef::intFieldArraySize - 1];
	}
	printf("%" PRId64 " serializeIntFields() %g M/s\n", total, count / (timer() - start) / 1e6);

	start = timer();
	total = 0;
	count = 100e6;
	for(i = 0; i < count; ++i) {
		rec1.chunk.start = i & 0xffffff;
		rec2.chunk.start = (i + 1) & 0xffffff;
		total += rec1.getCommonPrefixLen(rec2, 50);
	}
	printf("%" PRId64 " getCommonPrefixLen(skip=50) %g M/s\n", total, count / (timer() - start) / 1e6);

	start = timer();
	total = 0;
	count = 100e6;
	for(i = 0; i < count; ++i) {
		rec1.chunk.start = i & 0xffffff;
		rec2.chunk.start = (i + 1) & 0xffffff;
		total += rec1.getCommonPrefixLen(rec2, 0);
	}
	printf("%" PRId64 " getCommonPrefixLen(skip=0) %g M/s\n", total, count / (timer() - start) / 1e6);

	char buf[1000];
	RedwoodRecordRef::Delta &d = *(RedwoodRecordRef::Delta *)buf;

	start = timer();
	total = 0;
	count = 100e6;
	int commonPrefix = rec1.getCommonPrefixLen(rec2, 0);

	for(i = 0; i < count; ++i) {
		rec1.chunk.start = i & 0xffffff;
		rec2.chunk.start = (i + 1) & 0xffffff;
		total += rec1.writeDelta(d, rec2, commonPrefix);
	}
	printf("%" PRId64 " writeDelta(commonPrefix=%d) %g M/s\n", total, commonPrefix, count / (timer() - start) / 1e6);

	start = timer();
	total = 0;
	count = 10e6;
	for(i = 0; i < count; ++i) {
		rec1.chunk.start = i & 0xffffff;
		rec2.chunk.start = (i + 1) & 0xffffff;
		total += rec1.writeDelta(d, rec2);
	}
	printf("%" PRId64 " writeDelta() %g M/s\n", total, count / (timer() - start) / 1e6);

	start = timer();
	total = 0;
	count = 1e6;
	for(i = 0; i < count; ++i) {
		Standalone<RedwoodRecordRef> a = randomRedwoodRecordRef();
		Standalone<RedwoodRecordRef> b = randomRedwoodRecordRef();
		deltaTest(a, b);
	}
	printf("Random deltaTest() %g M/s\n", count / (timer() - start) / 1e6);

	return Void();
}

TEST_CASE("!/redwood/correctness/unit/deltaTree/RedwoodRecordRef") {
	const int N = 200;

	RedwoodRecordRef prev;
	RedwoodRecordRef next(LiteralStringRef("\xff\xff\xff\xff"));

	Arena arena;
	std::vector<RedwoodRecordRef> items;
	for(int i = 0; i < N; ++i) {
		std::string k = deterministicRandom()->randomAlphaNumeric(30);
		std::string v = deterministicRandom()->randomAlphaNumeric(30);
		RedwoodRecordRef rec;
		rec.key = StringRef(arena, k);
		rec.version = deterministicRandom()->coinflip() ? deterministicRandom()->randomInt64(0, std::numeric_limits<Version>::max()) : invalidVersion;
		if(deterministicRandom()->coinflip()) {
			rec.value = StringRef(arena, v);
			if(deterministicRandom()->coinflip()) {
				rec.chunk.start = deterministicRandom()->randomInt(0, 100000);
				rec.chunk.total = rec.chunk.start + v.size() + deterministicRandom()->randomInt(0, 100000);
			}
		}
		items.push_back(rec);
		//printf("i=%d %s\n", i, items.back().toString().c_str());
	}
	std::sort(items.begin(), items.end());

	DeltaTree<RedwoodRecordRef> *tree = (DeltaTree<RedwoodRecordRef> *) new uint8_t[N * 100];

	tree->build(&items[0], &items[items.size()], &prev, &next);

	printf("Count=%d  Size=%d  InitialDepth=%d\n", (int)items.size(), (int)tree->size(), (int)tree->initialDepth);
	debug_printf("Data(%p): %s\n", tree, StringRef((uint8_t *)tree, tree->size()).toHexString().c_str());

	DeltaTree<RedwoodRecordRef>::Reader r(tree, &prev, &next);
	DeltaTree<RedwoodRecordRef>::Cursor fwd = r.getCursor();
	DeltaTree<RedwoodRecordRef>::Cursor rev = r.getCursor();

	ASSERT(fwd.moveFirst());
	ASSERT(rev.moveLast());
	int i = 0;
	while(1) {
		if(fwd.get() != items[i]) {
			printf("forward iterator i=%d\n  %s found\n  %s expected\n", i, fwd.get().toString().c_str(), items[i].toString().c_str());
			printf("Delta: %s\n", fwd.node->raw->delta().toString().c_str());
			ASSERT(false);
		}
		if(rev.get() != items[items.size() - 1 - i]) {
			printf("reverse iterator i=%d\n  %s found\n  %s expected\n", i, rev.get().toString().c_str(), items[items.size() - 1 - i].toString().c_str());
			printf("Delta: %s\n", rev.node->raw->delta().toString().c_str());
			ASSERT(false);
		}
		++i;
		ASSERT(fwd.moveNext() == rev.movePrev());
		ASSERT(fwd.valid() == rev.valid());
		if(!fwd.valid()) {
			break;
		}
	}
	ASSERT(i == items.size());

	double start = timer();
	DeltaTree<RedwoodRecordRef>::Cursor c = r.getCursor();

	for(int i = 0; i < 20000000; ++i) {
		const RedwoodRecordRef &query = items[deterministicRandom()->randomInt(0, items.size())];
		if(!c.seekLessThanOrEqual(query)) {
			printf("Not found!  query=%s\n", query.toString().c_str());
			ASSERT(false);
		}
		if(c.get() != query) {
			printf("Found incorrect node!  query=%s  found=%s\n", query.toString().c_str(), c.get().toString().c_str());
			ASSERT(false);
		}
	}
	double elapsed = timer() - start;
	printf("Elapsed %f\n", elapsed);

	return Void();
}

TEST_CASE("!/redwood/correctness/unit/deltaTree/IntIntPair") {
	const int N = 200;
	IntIntPair prev = {0, 0};
	IntIntPair next = {1000, 0};

	std::vector<IntIntPair> items;
	for(int i = 0; i < N; ++i) {
		items.push_back({i*10, i*1000});
		//printf("i=%d %s\n", i, items.back().toString().c_str());
	}

	DeltaTree<IntIntPair> *tree = (DeltaTree<IntIntPair> *) new uint8_t[10000];

	tree->build(&items[0], &items[items.size()], &prev, &next);

	printf("Count=%d  Size=%d  InitialDepth=%d\n", (int)items.size(), (int)tree->size(), (int)tree->initialDepth);
	debug_printf("Data(%p): %s\n", tree, StringRef((uint8_t *)tree, tree->size()).toHexString().c_str());

	DeltaTree<IntIntPair>::Reader r(tree, &prev, &next);
	DeltaTree<IntIntPair>::Cursor fwd = r.getCursor();
	DeltaTree<IntIntPair>::Cursor rev = r.getCursor();

	ASSERT(fwd.moveFirst());
	ASSERT(rev.moveLast());
	int i = 0;
	while(1) {
		if(fwd.get() != items[i]) {
			printf("forward iterator i=%d\n  %s found\n  %s expected\n", i, fwd.get().toString().c_str(), items[i].toString().c_str());
			ASSERT(false);
		}
		if(rev.get() != items[items.size() - 1 - i]) {
			printf("reverse iterator i=%d\n  %s found\n  %s expected\n", i, rev.get().toString().c_str(), items[items.size() - 1 - i].toString().c_str());
			ASSERT(false);
		}
		++i;
		ASSERT(fwd.moveNext() == rev.movePrev());
		ASSERT(fwd.valid() == rev.valid());
		if(!fwd.valid()) {
			break;
		}
	}
	ASSERT(i == items.size());

	DeltaTree<IntIntPair>::Cursor c = r.getCursor();

	double start = timer();
	for(int i = 0; i < 20000000; ++i) {
		IntIntPair p({deterministicRandom()->randomInt(0, items.size() * 10), 0});
		if(!c.seekLessThanOrEqual(p)) {
			printf("Not found!  query=%s\n", p.toString().c_str());
			ASSERT(false);
		}
		if(c.get().k != (p.k - (p.k % 10))) {
			printf("Found incorrect node!  query=%s  found=%s\n", p.toString().c_str(), c.get().toString().c_str());
			ASSERT(false);
		}
	}
	double elapsed = timer() - start;
	printf("Elapsed %f\n", elapsed);

	return Void();
}

struct SimpleCounter {
	SimpleCounter() : x(0), xt(0), t(timer()), start(t) {}
	void operator+=(int n) { x += n; }
	void operator++() { x++; }
	int64_t get() { return x; }
	double rate() {
		double t2 = timer();
		int r = (x - xt) / (t2 - t);
		xt = x;
		t = t2;
		return r;
	}
	double avgRate() { return x / (timer() - start); }
	int64_t x;
	double t;
	double start;
	int64_t xt;
	std::string toString() { return format("%" PRId64 "/%.2f/%.2f", x, rate() / 1e6, avgRate() / 1e6); }
};

TEST_CASE("!/redwood/correctness/btree") {
	state bool useDisk = true;  // MemoryPager is not being maintained currently.

	state std::string pagerFile = "unittest_pageFile";
	IPager *pager;

	state bool serialTest = deterministicRandom()->coinflip();
	state bool shortTest = deterministicRandom()->coinflip();
	state bool singleVersion = true; // Multi-version mode is broken / not finished
	state double startTime = now();

	printf("serialTest: %d  shortTest: %d  singleVersion: %d\n", serialTest, shortTest, singleVersion);

	if(useDisk) {
		printf("Deleting existing test data...\n");
		deleteFile(pagerFile);
		deleteFile(pagerFile + "0.pagerlog");
		deleteFile(pagerFile + "1.pagerlog");
		pager = new IndirectShadowPager(pagerFile);
	}
	else
		pager = createMemoryPager();

	printf("Initializing...\n");
	state int pageSize = shortTest ? 200 : (deterministicRandom()->coinflip() ? pager->getUsablePageSize() : deterministicRandom()->randomInt(200, 400));
	state VersionedBTree *btree = new VersionedBTree(pager, pagerFile, singleVersion, pageSize);
	wait(btree->init());

	// We must be able to fit at least two any two keys plus overhead in a page to prevent
	// a situation where the tree cannot be grown upward with decreasing level size.
	// TODO:  Handle arbitrarily large keys
	state int maxKeySize = deterministicRandom()->randomInt(4, pageSize * 2);
	state int maxValueSize = deterministicRandom()->randomInt(0, pageSize * 4);
	state int maxCommitSize = shortTest ? 1000 : randomSize(10e6);
	state int mutationBytesTarget = shortTest ? 5000 : randomSize(50e6);
	state double clearChance = deterministicRandom()->random01() * .1;

	printf("Using page size %d, max key size %d, max value size %d, clearchance %f, total mutation byte target %d\n", pageSize, maxKeySize, maxValueSize, clearChance, mutationBytesTarget);

	state std::map<std::pair<std::string, Version>, Optional<std::string>> written;
	state std::set<Key> keys;

	state Version lastVer = wait(btree->getLatestVersion());
	printf("Starting from version: %" PRId64 "\n", lastVer);

	state Version version = lastVer + 1;
	btree->setWriteVersion(version);

	state SimpleCounter mutationBytes;
	state SimpleCounter keyBytesInserted;
	state SimpleCounter valueBytesInserted;
	state SimpleCounter sets;
	state SimpleCounter rangeClears;
	state SimpleCounter keyBytesCleared;
	state int errorCount;
	state int mutationBytesThisCommit = 0;
	state int mutationBytesTargetThisCommit = randomSize(maxCommitSize);

	state PromiseStream<Version> committedVersions;
	state Future<Void> verifyTask = verify(btree, committedVersions.getFuture(), &written, &errorCount, serialTest);
	state Future<Void> randomTask = serialTest ? Void() : (randomReader(btree) || btree->getError());

	state Future<Void> commit = Void();

	while(mutationBytes.get() < mutationBytesTarget) {
		if(now() - startTime > 600) {
			mutationBytesTarget = mutationBytes.get();
		}

		// Sometimes advance the version
		if(deterministicRandom()->random01() < 0.10) {
			++version;
			btree->setWriteVersion(version);
		}

		// Sometimes do a clear range
		if(deterministicRandom()->random01() < clearChance) {
			Key start = randomKV(maxKeySize, 1).key;
			Key end = (deterministicRandom()->random01() < .01) ? keyAfter(start) : randomKV(maxKeySize, 1).key;

			// Sometimes replace start and/or end with a close actual (previously used) value
			if(deterministicRandom()->random01() < .10) {
				auto i = keys.upper_bound(start);
				if(i != keys.end())
					start = *i;
			}
			if(deterministicRandom()->random01() < .10) {
				auto i = keys.upper_bound(end);
				if(i != keys.end())
					end = *i;
			}

			if(end == start)
				end = keyAfter(start);
			else if(end < start) {
				std::swap(end, start);
			}

			++rangeClears;
			KeyRangeRef range(start, end);
			debug_printf("      Mutation:  Clear '%s' to '%s' @%" PRId64 "\n", start.toString().c_str(), end.toString().c_str(), version);
			auto e = written.lower_bound(std::make_pair(start.toString(), 0));
			if(e != written.end()) {
				auto last = e;
				auto eEnd = written.lower_bound(std::make_pair(end.toString(), 0));
				while(e != eEnd) {
					auto w = *e;
					++e;
					// If e key is different from last and last was present then insert clear for last's key at version
					if(last != eEnd && ((e == eEnd || e->first.first != last->first.first) && last->second.present())) {
						debug_printf("      Mutation:    Clearing key '%s' @%" PRId64 "\n", last->first.first.c_str(), version);

						keyBytesCleared += last->first.first.size();
						mutationBytes += last->first.first.size();
						mutationBytesThisCommit += last->first.first.size();

						// If the last set was at version then just make it not present
						if(last->first.second == version) {
							last->second.reset();
						}
						else {
							written[std::make_pair(last->first.first, version)].reset();
						}
					}
					last = e;
				}
			}

			btree->clear(range);
		}
		else {
			// Set a key
			KeyValue kv = randomKV(maxKeySize, maxValueSize);
			// Sometimes change key to a close previously used key
			if(deterministicRandom()->random01() < .01) {
				auto i = keys.upper_bound(kv.key);
				if(i != keys.end())
					kv.key = StringRef(kv.arena(), *i);
			}

			debug_printf("      Mutation:  Set '%s' -> '%s' @%" PRId64 "\n", kv.key.toString().c_str(), kv.value.toString().c_str(), version);

			++sets;
			keyBytesInserted += kv.key.size();
			valueBytesInserted += kv.value.size();
			mutationBytes += (kv.key.size() + kv.value.size());
			mutationBytesThisCommit += (kv.key.size() + kv.value.size());

			btree->set(kv);
			written[std::make_pair(kv.key.toString(), version)] = kv.value.toString();
			keys.insert(kv.key);
		}

		// Commit at end or after this commit's mutation bytes are reached
		if(mutationBytes.get() >= mutationBytesTarget || mutationBytesThisCommit >= mutationBytesTargetThisCommit) {
			// Wait for previous commit to finish
			wait(commit);
			printf("Committed.  Next commit %d bytes, %" PRId64 "/%d (%.2f%%)  Stats: Insert %.2f MB/s  ClearedKeys %.2f MB/s  Total %.2f\n",
				mutationBytesThisCommit,
				mutationBytes.get(),
				mutationBytesTarget,
				(double)mutationBytes.get() / mutationBytesTarget * 100,
				(keyBytesInserted.rate() + valueBytesInserted.rate()) / 1e6,
				keyBytesCleared.rate() / 1e6,
				mutationBytes.rate() / 1e6
			);

			Version v = version;  // Avoid capture of version as a member of *this

			commit = map(btree->commit(), [=](Void) {
				// Notify the background verifier that version is committed and therefore readable
				committedVersions.send(v);
				return Void();
			});

			if(serialTest) {
				// Wait for commit, wait for verification, then start new verification
				wait(commit);
				committedVersions.sendError(end_of_stream());
				debug_printf("Waiting for verification to complete.\n");
				wait(verifyTask);
				committedVersions = PromiseStream<Version>();
				verifyTask = verify(btree, committedVersions.getFuture(), &written, &errorCount, serialTest);
			}

			mutationBytesThisCommit = 0;
			mutationBytesTargetThisCommit = randomSize(maxCommitSize);

			// Recover from disk at random
			if(!serialTest && useDisk && deterministicRandom()->random01() < .02) {
				printf("Recovering from disk.\n");

				// Wait for outstanding commit
				debug_printf("Waiting for outstanding commit\n");
				wait(commit);

				// Stop and wait for the verifier task
				committedVersions.sendError(end_of_stream());
				debug_printf("Waiting for verification to complete.\n");
				wait(verifyTask);

				Future<Void> closedFuture = btree->onClosed();
				btree->close();
				wait(closedFuture);

				debug_printf("Reopening btree\n");
				IPager *pager = new IndirectShadowPager(pagerFile);
				btree = new VersionedBTree(pager, pagerFile, singleVersion, pageSize);
				wait(btree->init());

				Version v = wait(btree->getLatestVersion());
				ASSERT(v == version);
				printf("Recovered from disk.  Latest version %" PRId64 "\n", v);

				// Create new promise stream and start the verifier again
				committedVersions = PromiseStream<Version>();
				verifyTask = verify(btree, committedVersions.getFuture(), &written, &errorCount, serialTest);
				randomTask = randomReader(btree) || btree->getError();
			}

			++version;
			btree->setWriteVersion(version);
		}

		// Check for errors
		if(errorCount != 0)
			throw internal_error();
	}

	debug_printf("Waiting for outstanding commit\n");
	wait(commit);
	committedVersions.sendError(end_of_stream());
	debug_printf("Waiting for verification to complete.\n");
	wait(verifyTask);

	// Check for errors
	if(errorCount != 0)
		throw internal_error();

	Future<Void> closedFuture = btree->onClosed();
	btree->close();
	wait(closedFuture);

	return Void();
}

ACTOR Future<Void> randomSeeks(VersionedBTree *btree, int count) {
	state Version readVer = wait(btree->getLatestVersion());
	state int c = 0;
	state double readStart = timer();
	printf("Executing %d random seeks\n", count);
	state Reference<IStoreCursor> cur = btree->readAtVersion(readVer);
	while(c < count) {
		state Key k = randomString(20, 'a', 'b');
		wait(success(cur->findFirstEqualOrGreater(k, false, 0)));
		++c;
	}
	double elapsed = timer() - readStart;
	printf("Point read speed %d/s\n", int(count / elapsed));
	return Void();
}

	
TEST_CASE("!/redwood/performance/set") {
	state std::string pagerFile = "unittest_pageFile";
	printf("Deleting old test data\n");
	deleteFile(pagerFile);
	deleteFile(pagerFile + "0.pagerlog");
	deleteFile(pagerFile + "1.pagerlog");

	IPager *pager = new IndirectShadowPager(pagerFile);
	state bool singleVersion = true;
	state VersionedBTree *btree = new VersionedBTree(pager, "unittest_pageFile", singleVersion);
	wait(btree->init());

	state int nodeCount = 1e9;
	state int maxChangesPerVersion = 500000;
	state int64_t kvBytesTarget = 200e6;
	state int maxKeyPrefixSize = 50;
	state int maxValueSize = 100;
	state int maxConsecutiveRun = 1;
	state int64_t kvBytes = 0;
	state int64_t kvBytesTotal = 0;
	state int records = 0;
	state Future<Void> commit = Void();
	state std::string value(maxValueSize, 'v');

	printf("Starting.\n");
	state double intervalStart = timer();
	state double start = intervalStart;

	while(kvBytesTotal < kvBytesTarget) {
		Version lastVer = wait(btree->getLatestVersion());
		state Version version = lastVer + 1;
		btree->setWriteVersion(version);
		int changes = deterministicRandom()->randomInt(0, maxChangesPerVersion);

		while(changes > 0) {
			KeyValue kv;
			kv.key = randomString(kv.arena(), deterministicRandom()->randomInt(sizeof(uint32_t), maxKeyPrefixSize + sizeof(uint32_t) + 1), 'a', 'b');
			int32_t index = deterministicRandom()->randomInt(0, nodeCount);
			int runLength = deterministicRandom()->randomInt(1, maxConsecutiveRun + 1);

			while(runLength > 0 && changes > 0) {
				*(uint32_t *)(kv.key.end() - sizeof(uint32_t)) = bigEndian32(index++);
				kv.value = StringRef((uint8_t *)value.data(), deterministicRandom()->randomInt(0, value.size()));

				btree->set(kv);

				--runLength;
				--changes;
				kvBytes += kv.key.size() + kv.value.size();
				++records;
			}
		}

		if(kvBytes > 2e6) {
			wait(commit);
			printf("Cumulative %.2f MB keyValue bytes written at %.2f MB/s\n", kvBytesTotal / 1e6, kvBytesTotal / (timer() - start) / 1e6);

			// Avoid capturing via this to freeze counter values
			int recs = records;
			int kvb = kvBytes;

			// Capturing invervalStart via this->intervalStart makes IDE's unhappy as they do not know about the actor state object
			double *pIntervalStart = &intervalStart;

			commit = map(btree->commit(), [=](Void result) {
				double elapsed = timer() - *pIntervalStart;
				printf("Committed %d kvBytes in %d records in %f seconds, %.2f MB/s\n", kvb, recs, elapsed, kvb / elapsed / 1e6);
				*pIntervalStart = timer();
				return Void();
			});

			kvBytesTotal += kvBytes;
			kvBytes = 0;
			records = 0;
		}
	}

	wait(commit);
	printf("Cumulative %.2f MB keyValue bytes written at %.2f MB/s\n", kvBytesTotal / 1e6, kvBytesTotal / (timer() - start) / 1e6);

	state int reads = 30000;
	wait(randomSeeks(btree, reads) && randomSeeks(btree, reads) && randomSeeks(btree, reads));

	Future<Void> closedFuture = btree->onClosed();
	btree->close();
	wait(closedFuture);

	return Void();
}
