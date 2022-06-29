/*
 * MutationList.h
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

#ifndef FLOW_FDBCLIENT_MUTATIONLIST_H
#define FLOW_FDBCLIENT_MUTATIONLIST_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"

struct MutationListRef {
	// Represents an ordered, but not random-access, list of mutations that can be O(1) deserialized and
	// quickly serialized, (forward) iterated or appended to.
	// MutationListRef is a list of struct Blob
	// Each blob has a struct Header following by the mutation's param1 and param2 content.
	// The Header has the mutation's type and the length of param1 and param2

public:
	struct Blob {
		// StringRef data Format: |type|p1len|p2len|p1_content|p2_content|
		// |type|p1len|p2len| is the header; p1_content has p1len length; p2_content has p2len length
		StringRef data;
		Blob* next;
	};
	Blob* blob_begin;

private:
	struct Header {
		int type, p1len, p2len;
		const uint8_t* p1begin() const {
			//(this+1) moves the pointer by Header size and get to the beginning of p1_content
			return (const uint8_t*)(this + 1);
		}
		const uint8_t* p2begin() const { return (const uint8_t*)(this + 1) + p1len; }
		const uint8_t* end() const { return (const uint8_t*)(this + 1) + p1len + p2len; }
	};
	static_assert(sizeof(Header) == 12, "Header packing problem");
	static_assert(sizeof(Header) == MutationRef::OVERHEAD_BYTES, "Invalid MutationRef Overhead Bytes");

public:
	struct Iterator {
		const MutationRef& operator*() { return item; }
		const MutationRef* operator->() { return &item; }
		void operator++() {
			ASSERT(blob->data.size() > 0);
			auto e = ptr->end(); // e points to the end of the current blob
			if (e == blob->data.end()) { // the condition sanity checks e is at the end of current blob
				blob = blob->next;
				e = blob ? blob->data.begin() : nullptr;
			}
			ptr = (Header*)e;
			decode();
		}

		bool operator==(Iterator const& i) const { return ptr == i.ptr; }
		bool operator!=(Iterator const& i) const { return ptr != i.ptr; }
		explicit operator bool() const { return blob != nullptr; }

		typedef std::forward_iterator_tag iterator_category;
		typedef const MutationRef value_type;
		typedef int64_t difference_type;
		typedef const MutationRef* pointer;
		typedef const MutationRef& reference;

		Iterator(Blob* blob, const Header* ptr) : blob(blob), ptr(ptr) { decode(); }
		Iterator() : blob(nullptr), ptr(nullptr) {}

	private:
		friend struct MutationListRef;
		const Blob* blob; // The blob containing the indicated mutation
		const Header* ptr; // The header of the indicated mutation
		MutationRef item;

		void decode() {
			if (!ptr)
				return;
			item.type = (MutationRef::Type)ptr->type;
			item.param1 = StringRef(ptr->p1begin(), ptr->p1len);
			item.param2 = StringRef(ptr->p2begin(), ptr->p2len);
		}
	};

	MutationListRef() : blob_begin(nullptr), blob_end(nullptr), totalBytes(0) {}
	MutationListRef(Arena& ar, MutationListRef const& r) : blob_begin(nullptr), blob_end(nullptr), totalBytes(0) {
		append_deep(ar, r.begin(), r.end());
	}
	Iterator begin() const {
		if (blob_begin)
			return Iterator(blob_begin, (Header*)blob_begin->data.begin());
		return Iterator(nullptr, nullptr);
	}
	Iterator end() const { return Iterator(nullptr, nullptr); }
	size_t expectedSize() const { return sizeof(Blob) + totalBytes; }
	int totalSize() const { return totalBytes; }

	MutationRef push_back_deep(Arena& arena, MutationRef const& m) {
		int mutationSize = sizeof(Header) + m.param1.size() + m.param2.size();
		Header* p = (Header*)allocate(arena, mutationSize);
		p->type = m.type;
		p->p1len = m.param1.size();
		p->p2len = m.param2.size();
		memcpy(p + 1, m.param1.begin(), p->p1len);
		memcpy((uint8_t*)(p + 1) + p->p1len, m.param2.begin(), p->p2len);
		totalBytes += mutationSize;
		return MutationRef(
		    (MutationRef::Type)p->type, StringRef(p->p1begin(), p->p1len), StringRef(p->p2begin(), p->p2len));
	}
	void append_deep(Arena& arena, Iterator begin, Iterator end) {
		for (auto blob = begin.blob; blob; blob = blob->next) {
			const uint8_t* b = blob == begin.blob ? (const uint8_t*)begin.ptr : blob->data.begin();
			const uint8_t* e = blob == end.blob ? (const uint8_t*)end.ptr : blob->data.end();
			int len = e - b;
			if (len > 0) {
				void* a = allocate(arena, len);
				memcpy(a, b, len);
				totalBytes += len;
			}

			if (blob == end.blob)
				break;
		}
	}
	void append_deep(Arena& arena, MutationRef const* begin, int count) {
		// FIXME: More efficient?  Eliminate?
		for (int i = 0; i < count; i++)
			push_back_deep(arena, begin[i]);
	}

	template <class Ar>
	void serialize_load(Ar& ar) {
		serializer(ar, totalBytes);

		if (totalBytes > 0) {
			blob_begin = blob_end = new (ar.arena()) Blob;
			blob_begin->next = nullptr;
			blob_begin->data = StringRef((const uint8_t*)ar.arenaRead(totalBytes),
			                             totalBytes); // Zero-copy read when deserializing from an ArenaReader
		}
	}

	// FIXME: this is re-implemented on the commit proxy to include a yield, any changes to this function should also
	// done there
	template <class Ar>
	void serialize_save(Ar& ar) const {
		serializer(ar, totalBytes);
		for (auto b = blob_begin; b; b = b->next)
			ar.serializeBytes(b->data);
	}

private:
	void* allocate(Arena& arena, int bytes) {
		bool useBlob = false;
		if (!blob_end)
			blob_begin = blob_end = new (arena) Blob;
		else if (!arena.hasFree(bytes, blob_end->data.end())) {
			blob_end->next = new (arena) Blob;
			blob_end = blob_end->next;
		} else
			useBlob = true;

		uint8_t* b = new (arena) uint8_t[bytes];

		if (useBlob) {
			ASSERT(b == blob_end->data.end());
			blob_end->data = StringRef(blob_end->data.begin(), blob_end->data.size() + bytes);
			return b;
		}

		blob_end->data = StringRef(b, bytes);
		blob_end->next = nullptr;
		return b;
	}

	Blob* blob_end;
	int totalBytes;
};
typedef Standalone<MutationListRef> MutationList;

template <class Ar>
void load(Ar& ar, MutationListRef& r) {
	r.serialize_load(ar);
}
template <class Ar>
void save(Ar& ar, MutationListRef const& r) {
	r.serialize_save(ar);
}

#endif
