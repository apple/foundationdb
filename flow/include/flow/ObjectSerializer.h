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

#pragma once
#include "flow/Error.h"
#include "flow/Arena.h"
#include "flow/flat_buffers.h"
#include "flow/ProtocolVersion.h"

#include <unordered_map>
#include <any>

using ContextVariableMap = std::unordered_map<std::string_view, std::any>;

template <class T>
struct HasVariableMap_t : std::false_type {};

template <class T>
constexpr bool HasVariableMap = HasVariableMap_t<T>::value;

template <class Ar>
struct LoadContext {
	Ar* ar;

	LoadContext(Ar* ar) : ar(ar) {}

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

	LoadContext& context() { return *this; }

	template <class Archiver = Ar>
	std::enable_if_t<HasVariableMap<Archiver>, std::any&> variable(std::string_view name) {
		return ar->variable(name);
	}
};

template <class ReaderImpl>
class _ObjectReader {
protected:
	Optional<ProtocolVersion> mProtocolVersion;
	std::shared_ptr<ContextVariableMap> variables;

public:
	ProtocolVersion protocolVersion() const { return mProtocolVersion.get(); }
	void setProtocolVersion(ProtocolVersion v) { mProtocolVersion = v; }
	void setContextVariableMap(std::shared_ptr<ContextVariableMap> const& cvm) { variables = cvm; }

	template <class... Items>
	void deserialize(FileIdentifier file_identifier, Items&... items) {
		LoadContext<ReaderImpl> context(static_cast<ReaderImpl*>(this));
		const uint8_t* data = static_cast<ReaderImpl*>(this)->data();
		if (read_file_identifier(data) != file_identifier) {
			// Some file identifiers are changed in 7.0, so file identifier mismatches
			// are expected during a downgrade from 7.0 to 6.3
			bool expectMismatch = mProtocolVersion.get() >= ProtocolVersion(0x0FDB00B070000000LL) &&
			                      currentProtocolVersion() < ProtocolVersion(0x0FDB00B070000000LL);
			{
				TraceEvent te(expectMismatch ? SevInfo : SevError, "MismatchedFileIdentifier");
				if (expectMismatch) {
					te.suppressFor(1.0);
				}
				te.detail("Expected", file_identifier).detail("Read", read_file_identifier(data));
			}
			if (!expectMismatch) {
				ASSERT(false);
			}
		}
		load_members(data, context, items...);
	}

	template <class Item>
	void deserialize(Item& item) {
		deserialize(FileIdentifierFor<Item>::value, item);
	}

	std::any& variable(std::string_view name) { return variables->at(name); }

	std::any const& variable(std::string_view name) const { return variables->at(name); }
};

class ObjectReader : public _ObjectReader<ObjectReader> {
	friend struct _IncludeVersion;
	ObjectReader& operator>>(ProtocolVersion& version) {
		uint64_t result;
		memcpy(&result, _data, sizeof(result));
		_data += sizeof(result);
		version = ProtocolVersion(result);
		return *this;
	}

public:
	static constexpr bool ownsUnderlyingMemory = false;

	template <class VersionOptions>
	ObjectReader(const uint8_t* data, VersionOptions vo) : _data(data) {
		vo.read(*this);
	}

	template <class T, class VersionOptions>
	static T fromStringRef(StringRef sr, VersionOptions vo) {
		T t;
		ObjectReader reader(sr.begin(), vo);
		reader.deserialize(t);
		return t;
	}

	const uint8_t* data() { return _data; }

	Arena& arena() { return _arena; }

private:
	const uint8_t* _data;
	Arena _arena;
};

class ArenaObjectReader : public _ObjectReader<ArenaObjectReader> {
	friend struct _IncludeVersion;
	ArenaObjectReader& operator>>(ProtocolVersion& version) {
		uint64_t result;
		memcpy(&result, _data, sizeof(result));
		_data += sizeof(result);
		version = ProtocolVersion(result);
		return *this;
	}

public:
	static constexpr bool ownsUnderlyingMemory = true;

	template <class VersionOptions>
	ArenaObjectReader(Arena const& arena, const StringRef& input, VersionOptions vo)
	  : _data(input.begin()), _arena(arena) {
		vo.read(*this);
	}

	const uint8_t* data() { return _data; }

	Arena& arena() { return _arena; }

private:
	const uint8_t* _data;
	Arena _arena;
};

class ObjectWriter {
	friend struct _IncludeVersion;
	bool writeProtocolVersion = false;
	ObjectWriter& operator<<(const ProtocolVersion& version) {
		writeProtocolVersion = true;
		return *this;
	}
	ProtocolVersion mProtocolVersion;

	class AllocateFunctor {
	public:
		AllocateFunctor(ObjectWriter* pObjectWriter) : pObjectWriter(pObjectWriter) {}

		uint8_t* operator()(const size_t size) {
			++numAllocations;

			pObjectWriter->size = size + (pObjectWriter->writeProtocolVersion ? sizeof(uint64_t) : 0);
			if (pObjectWriter->customAllocator != nullptr) {
				pObjectWriter->data =
				    pObjectWriter->customAllocator(pObjectWriter->size, pObjectWriter->customAllocatorContext);
			} else {
				pObjectWriter->data = new (pObjectWriter->arena) uint8_t[pObjectWriter->size];
			}
			if (pObjectWriter->writeProtocolVersion) {
				auto v = pObjectWriter->protocolVersion().versionWithFlags();
				memcpy(pObjectWriter->data, &v, sizeof(uint64_t));
				return pObjectWriter->data + sizeof(uint64_t);
			}
			return pObjectWriter->data;
		}

		int getNumAllocations() const { return numAllocations; }

	private:
		ObjectWriter* pObjectWriter;
		int numAllocations;
	};

	friend class AllocateFunctor;

	class SaveContext {
	private:
		ObjectWriter* ar;
		AllocateFunctor allocator;

	public:
		SaveContext(ObjectWriter* ar, const AllocateFunctor& allocator) : ar(ar), allocator(allocator) {}

		ProtocolVersion protocolVersion() const { return ar->protocolVersion(); }

		void addArena(Arena& arena) {}

		uint8_t* allocate(size_t s) { return allocator(s); }

		SaveContext& context() { return *this; }
	};

public:
	template <class VersionOptions>
	explicit ObjectWriter(VersionOptions vo) : customAllocator(nullptr) {
		vo.write(*this);
	}

	// NOTE: It is known that clang compiler will spend long time on instantiating std::function objects when there is
	// capture. By downgrading it to a function pointer, the compile time can be reduced. The trade is an additional
	// void* must be used to carry the captured environment.
	template <class VersionOptions>
	explicit ObjectWriter(uint8_t* (*customAllocator_)(size_t, void*), void* customAllocatorContext_, VersionOptions vo)
	  : customAllocator(customAllocator_), customAllocatorContext(customAllocatorContext_) {
		vo.write(*this);
	}

	template <class... Items>
	void serialize(FileIdentifier file_identifier, Items const&... items) {
		ASSERT(data == nullptr); // object serializer can only serialize one object
		AllocateFunctor allocator(this);
		SaveContext context(this, allocator);
		save_members(context, file_identifier, items...);
		ASSERT(allocator.getNumAllocations() == 1);
	}

	template <class Item>
	void serialize(Item const& item) {
		serialize(FileIdentifierFor<Item>::value, item);
	}

	StringRef toStringRef() const { return StringRef(data, size); }

	Standalone<StringRef> toString() const {
		ASSERT(!customAllocator);
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
	uint8_t* (*customAllocator)(size_t, void*);
	void* customAllocatorContext;
	uint8_t* data = nullptr;
	int size = 0;
};

template <>
struct HasVariableMap_t<ObjectReader> : std::true_type {};
template <>
struct HasVariableMap_t<ArenaObjectReader> : std::true_type {};

// this special case is needed - the code expects
// Standalone<T> and T to be equivalent for serialization
namespace detail {

template <class T, class Context>
struct LoadSaveHelper<Standalone<T>, Context> : Context {
	LoadSaveHelper(const Context& context) : Context(context), helper(context) {}

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
