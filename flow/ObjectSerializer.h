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

#pragma once
#include "flow/Error.h"
#include "flow/Arena.h"
#include "flow/flat_buffers.h"

template <class Ar>
struct LoadContext {
	Ar& ar;
	std::vector<std::function<void()>> doAfter;
	LoadContext(Ar& ar) : ar(ar) {}
	Arena& arena() { return ar.arena(); }

	const uint8_t* tryReadZeroCopy(const uint8_t* ptr, unsigned len) {
		if constexpr (Ar::ownsUnderlyingMemory) {
			return ptr;
		} else {
			if (len == 0) return nullptr;
			uint8_t* dat = new (arena()) uint8_t[len];
			std::copy(ptr, ptr + len, dat);
			return dat;
		}
	}

	void done() const {
		for (auto& f : doAfter) {
			f();
		}
	}
	void addArena(Arena& arena) { arena = ar.arena(); }
};

template <class ReaderImpl>
class _ObjectReader {
public:
	template <class... Items>
	void deserialize(FileIdentifier file_identifier, Items&... items) {
		const uint8_t* data = static_cast<ReaderImpl*>(this)->data();
		LoadContext<ReaderImpl> context(*static_cast<ReaderImpl*>(this));
		ASSERT(read_file_identifier(data) == file_identifier);
		load_members(data, context, items...);
		context.done();
	}

	template <class Item>
	void deserialize(Item& item) {
		deserialize(FileIdentifierFor<Item>::value, item);
	}
};

class ObjectReader : public _ObjectReader<ObjectReader> {
public:
	static constexpr bool ownsUnderlyingMemory = false;

	ObjectReader(const uint8_t* data) : _data(data) {}

	const uint8_t* data() { return _data; }

	Arena& arena() { return _arena; }

private:
	const uint8_t* _data;
	Arena _arena;
};

class ArenaObjectReader : public _ObjectReader<ArenaObjectReader> {
public:
	static constexpr bool ownsUnderlyingMemory = true;

	ArenaObjectReader(Arena const& arena, const StringRef& input) : _data(input.begin()), _arena(arena) {}

	const uint8_t* data() { return _data; }

	Arena& arena() { return _arena; }

private:
	const uint8_t* _data;
	Arena _arena;
};

class ObjectWriter {
public:
	template <class... Items>
	void serialize(FileIdentifier file_identifier, Items const&... items) {
		ASSERT(data == nullptr); // object serializer can only serialize one object
		int allocations = 0;
		auto allocator = [this, &allocations](size_t size_) {
			++allocations;
			size = size_;
			data = new uint8_t[size];
			return data;
		};
		auto res = save_members(allocator, file_identifier, items...);
		ASSERT(allocations == 1);
	}

	template <class Item>
	void serialize(Item const& item) {
		serialize(FileIdentifierFor<Item>::value, item);
	}

	StringRef toStringRef() const {
		return StringRef(data, size);
	}

private:
	uint8_t* data = nullptr;
	int size = 0;
};
