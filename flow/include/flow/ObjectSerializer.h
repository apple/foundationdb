/*
 * serialize.h
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

#pragma once
#include "flow/Error.h"
#include "flow/Knobs.h"
#include "flow/Arena.h"
#include "flow/flat_buffers.h"
#include "flow/ProtocolVersion.h"

#include <unordered_map>
#include <any>
#include <iostream>

template <class Ar>
struct LoadContext {
	Ar* ar;

	explicit LoadContext(Ar* ar) : ar(ar) {}

	Arena& arena() { return ar->arena(); }

	ProtocolVersion protocolVersion() const { return ar->protocolVersion(); }

	const uint8_t* tryReadZeroCopy(const uint8_t* ptr, unsigned len) {
		if constexpr (Ar::ownsUnderlyingMemory) {
			return ptr;
		} else {
			if (len == 0)
				return nullptr;
			uint8_t* dat = new (arena()) uint8_t[len];
			std::copy(ptr, ptr + len, dat);
			return dat;
		}
	}

	void addArena(Arena& arena) { arena = ar->arena(); }

	// Throws serialization_failed unless [ptr, ptr + len) lies inside the buffer being loaded.
	void checkRange(const uint8_t* ptr, size_t len) const { ar->checkRange(ptr, len); }

	[[noreturn]] void loadFailed(const char* reason) const { Ar::loadFailed(reason); }

	LoadContext& context() { return *this; }
};

template <class ReaderImpl>
class _ObjectReader {
	friend struct _IncludeVersion;
	_ObjectReader& operator>>(ProtocolVersion& version) {
		uint64_t result;
		if (static_cast<size_t>(_end - _data) < sizeof(result)) {
			ReaderImpl::loadFailed("buffer too short for protocol version");
		}
		memcpy(&result, _data, sizeof(result));
		_data += sizeof(result);
		version = ProtocolVersion(result);
		return *this;
	}

protected:
	Optional<ProtocolVersion> mProtocolVersion;
	const uint8_t* _data;
	const uint8_t* _begin; // start of the flatbuffer, after any protocol version prefix
	const uint8_t* _end;
	bool _checksEnabled; // Cached so each field read avoids the knob lookup.

	template <class VersionOptions>
	_ObjectReader(const StringRef& input, VersionOptions vo)
	  : _data(input.begin()), _end(input.end()), _checksEnabled(FLOW_KNOBS->FLATBUFFERS_BOUNDS_CHECKS_ENABLED) {
		vo.read(*this);
		_begin = _data;
	}

public:
	ProtocolVersion protocolVersion() const { return mProtocolVersion.get(); }
	void setProtocolVersion(ProtocolVersion v) { mProtocolVersion = v; }

	const uint8_t* data() { return _data; }

	void checkRange(const uint8_t* ptr, size_t len) const {
		if (!_checksEnabled || len == 0) {
			return;
		}
		auto p = reinterpret_cast<uintptr_t>(ptr);
		auto b = reinterpret_cast<uintptr_t>(_begin);
		auto e = reinterpret_cast<uintptr_t>(_end);
		if (p < b || p > e || len > e - p) {
			ReaderImpl::loadFailed("read outside of buffer");
		}
	}

	template <class... Items>
	void deserialize(FileIdentifier file_identifier, Items&... items) {
		LoadContext<ReaderImpl> context(static_cast<ReaderImpl*>(this));
		const uint8_t* data = static_cast<ReaderImpl*>(this)->data();
		if (static_cast<size_t>(_end - data) < 2 * sizeof(uint32_t)) { // root offset and file identifier
			ReaderImpl::loadFailed("buffer too short for header");
		}
		if (read_file_identifier(data) != file_identifier) {
			// Some file identifiers are changed in 7.0, so file identifier mismatches
			// are expected during a downgrade from 7.0 to 6.3
			bool expectMismatch = mProtocolVersion.get() >= ProtocolVersion(0x0FDB00B070000000LL) &&
			                      currentProtocolVersion() < ProtocolVersion(0x0FDB00B070000000LL);
			{
				TraceEvent te(expectMismatch ? SevInfo : SevWarnAlways, "MismatchedFileIdentifier");
				te.suppressFor(1.0).detail("Expected", file_identifier).detail("Read", read_file_identifier(data));
			}
			if (!expectMismatch) {
				if constexpr (ReaderImpl::failsFatally) {
					ASSERT(false);
				} else {
					detail::flat_buffer_load_failed("file identifier mismatch");
				}
			}
		}
		load_members(data, context, items...);
	}

	template <class Item>
	void deserialize(Item& item) {
		deserialize(FileIdentifierFor<Item>::value, item);
	}
};

class ObjectReader : public _ObjectReader<ObjectReader> {
public:
	static constexpr bool ownsUnderlyingMemory = false;
	// ObjectReader has no error path for a malformed buffer, so it aborts rather than throw.
	static constexpr bool failsFatally = true;
	[[noreturn]] static void loadFailed(const char* reason) { detail::flat_buffer_load_fatal(reason); }

	template <class VersionOptions>
	ObjectReader(const StringRef& input, VersionOptions vo) : _ObjectReader(input, vo) {}

	template <class T, class VersionOptions>
	static T fromStringRef(StringRef sr, VersionOptions vo) {
		T t;
		ObjectReader reader(sr, vo);
		reader.deserialize(t);
		return t;
	}

	Arena& arena() { return _arena; }

private:
	Arena _arena;
};

class ArenaObjectReader : public _ObjectReader<ArenaObjectReader> {
public:
	static constexpr bool ownsUnderlyingMemory = true;
	static constexpr bool failsFatally = false;
	[[noreturn]] static void loadFailed(const char* reason) { detail::flat_buffer_load_failed(reason); }

	template <class VersionOptions>
	ArenaObjectReader(Arena const& arena, const StringRef& input, VersionOptions vo)
	  : _ObjectReader(input, vo), _arena(arena) {}

	template <class VersionOptions>
	ArenaObjectReader(Arena&& arena, const StringRef& input, VersionOptions vo)
	  : _ObjectReader(input, vo), _arena(std::move(arena)) {}

	Arena& arena() { return _arena; }

private:
	Arena _arena;
};

// A single-use class for serializing an object with a serialize() member function or a serializable trait
// Allocates from arena by default, with the ability to
// a) optionally override default allocation function, and/or
// b) optionally mark parts of memory after use for wiping: i.e. zeroing out
// both a) and b) requires passing a dedicated function pointer for each operation.
// Optionally, a pointer to an allocation context shared by both function may be passed.
// Allocation is expected to happen exactly once during serialization.
class ObjectWriter {
	friend struct _IncludeVersion;
	bool writeProtocolVersion = false;
	ObjectWriter& operator<<(const ProtocolVersion& version) {
		writeProtocolVersion = true;
		return *this;
	}
	ProtocolVersion mProtocolVersion;

	class MemoryHelper {
	public:
		explicit MemoryHelper(ObjectWriter* pObjectWriter) : pObjectWriter(pObjectWriter), numAllocations(0) {}

		// expected to be called exactly once
		uint8_t* allocate(const size_t size) {
			++numAllocations;

			pObjectWriter->size = size + (pObjectWriter->writeProtocolVersion ? sizeof(uint64_t) : 0);
			if (pObjectWriter->allocatorFunc) {
				pObjectWriter->data =
				    pObjectWriter->allocatorFunc(pObjectWriter->size, pObjectWriter->allocatorContext);
			} else {
				pObjectWriter->data = new (pObjectWriter->arena) uint8_t[pObjectWriter->size];
			}
			if (pObjectWriter->writeProtocolVersion) {
				auto v = pObjectWriter->protocolVersion().versionWithFlags();
				::memcpy(pObjectWriter->data, &v, sizeof(uint64_t));
				return pObjectWriter->data + sizeof(uint64_t);
			}
			return pObjectWriter->data;
		}

		void markForWipe(uint8_t* begin, size_t size) {
			if (pObjectWriter->markForWipeFunc) {
				pObjectWriter->markForWipeFunc(begin, size, pObjectWriter->allocatorContext);
			}
		}

		int getNumAllocations() const { return numAllocations; }

	private:
		ObjectWriter* pObjectWriter;
		int numAllocations;
	};

	friend class MemoryHelper;

public:
	class SaveContext {
	private:
		ObjectWriter* ar;
		MemoryHelper& memoryHelper;

	public:
		SaveContext(ObjectWriter* ar, MemoryHelper& memoryHelper) : ar(ar), memoryHelper(memoryHelper) {}

		ProtocolVersion protocolVersion() const { return ar->protocolVersion(); }

		void addArena(Arena& arena) {}

		uint8_t* allocate(size_t s) { return memoryHelper.allocate(s); }

		void markForWipe(uint8_t* begin, size_t size) { memoryHelper.markForWipe(begin, size); }

		SaveContext& context() { return *this; }
	};

	// takes (object size, allocator context pointer), returns pointer to allocated memory
	using AllocatorFuncType = uint8_t* (*)(const size_t, void*);

	// takes (wipe begin pointer, wipe length, allocator context pointer)
	using MarkForWipeFuncType = void (*)(uint8_t*, size_t, void*);

	// Overload that enables serializer traits to mark the buffers for wiping (zeroing out) after use.
	// MarkForWipeFunc shares allocator context with allocatorFunc
	// Simpler (lambda wrapped in std::function) was avoided by past PR to reduce compilation time
	template <class VersionOptions>
	explicit ObjectWriter(AllocatorFuncType allocatorFunc,
	                      MarkForWipeFuncType markForWipeFunc,
	                      void* allocatorContext,
	                      VersionOptions vo)
	  : arena(), allocatorFunc(allocatorFunc), markForWipeFunc(markForWipeFunc), allocatorContext(allocatorContext),
	    data(nullptr), size(0) {
		vo.write(*this);
	}

	template <class VersionOptions>
	explicit ObjectWriter(AllocatorFuncType allocatorFunc, void* allocatorContext, VersionOptions vo)
	  : ObjectWriter(allocatorFunc, nullptr /*markForWipeFunc*/, allocatorContext, vo) {}

	template <class VersionOptions>
	explicit ObjectWriter(VersionOptions vo)
	  : ObjectWriter(nullptr /*allocatorFunc*/, nullptr /*markForWipeFunc*/, nullptr /*allocatorContext*/, vo) {}

	template <class... Items>
	void serialize(FileIdentifier file_identifier, Items const&... items) {
		ASSERT(data == nullptr); // object serializer can only serialize one object
		MemoryHelper memoryHelper(this);
		SaveContext context(this, memoryHelper);
		save_members(context, file_identifier, items...);
		ASSERT(memoryHelper.getNumAllocations() == 1);
	}

	template <class Item>
	void serialize(Item const& item) {
		serialize(FileIdentifierFor<Item>::value, item);
	}

	StringRef toStringRef() const { return StringRef(data, size); }

	Standalone<StringRef> toString() const {
		ASSERT(!allocatorFunc);
		return Standalone<StringRef>(toStringRef(), arena);
	}

	template <class Item, class VersionOptions>
	static Standalone<StringRef> toValue(Item const& item, VersionOptions vo) {
		ObjectWriter writer(vo);
		writer.serialize(item);
		return writer.toString();
	}

	ProtocolVersion protocolVersion() const { return mProtocolVersion; }

	void setProtocolVersion(ProtocolVersion v) {
		mProtocolVersion = v;
		ASSERT(mProtocolVersion.isValid());
	}

private:
	Arena arena;
	AllocatorFuncType allocatorFunc;
	MarkForWipeFuncType markForWipeFunc;
	void* allocatorContext;
	uint8_t* data;
	int size;
};

// this special case is needed - the code expects
// Standalone<T> and T to be equivalent for serialization
namespace detail {

template <class T, class Context>
struct LoadSaveHelper<Standalone<T>, Context> : Context {
	explicit LoadSaveHelper(const Context& context) : Context(context), helper(context) {}

	void load(Standalone<T>& member, const uint8_t* current) {
		helper.load(member.contents(), current);
		this->addArena(member.arena());
	}

	template <class Writer>
	RelativeOffset save(const Standalone<T>& member, Writer& writer, const VTableSet* vtables) {
		return helper.save(member.contents(), writer, vtables);
	}

private:
	LoadSaveHelper<T, Context> helper;
};

} // namespace detail
