/*
 * ArtMutationBuffer.h
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
#ifndef ART_MUTATION_BUFFER
#define ART_MUTATION_BUFFER

#include "art.h"
#include "flow/Arena.h"

struct MutationBufferART {

private:
	Arena arena;
	art_tree* mutations;

public:
	struct const_iterator {
		art_iterator artIterator;

		const_iterator() = default;

		const_iterator(art_iterator i) { artIterator = i; }

		const_iterator(const const_iterator& i) { artIterator = i.artIterator; }

		const KeyRef& key() { return artIterator.key(); }

		const RangeMutation& mutation() { return (const RangeMutation&)(*((RangeMutation*)artIterator.value())); }

		bool operator==(const const_iterator& other) const { return (artIterator) == (other.artIterator); }
		bool operator!=(const const_iterator& other) const { return !(*this == other); }

		const_iterator& operator++() {
			++(artIterator);
			return *this;
		}

		const_iterator& operator--() {
			--(artIterator);
			return *this;
		}
	};

	struct iterator {

		art_iterator artIterator;

		iterator() = default;

		iterator(art_iterator i) { artIterator = i; }

		iterator(const iterator& i) { artIterator = i.artIterator; }

		const KeyRef& key() { return artIterator.key(); }

		RangeMutation& mutation() { return (RangeMutation&)(*((RangeMutation*)artIterator.value())); }

		void** value_ptr() { return artIterator.value_ptr(); }

		bool operator==(const iterator& other) const { return (artIterator) == (other.artIterator); }

		bool operator==(const const_iterator& other) const { return (artIterator) == (other.artIterator); }

		iterator& operator++() {
			++(artIterator);
			return *this;
		}

		iterator& operator--() {
			--(artIterator);
			return *this;
		}

		// Enable implicit conversion
		operator const_iterator() { return const_iterator(this->artIterator); }
	};

	MutationBufferART() {
		mutations = new (arena) art_tree(arena);
		// Create range representing the entire keyspace.  This reduces edge cases to applying mutations
		// because now all existing keys are within some range in the mutation map.

		mutations->insert(dbBegin.key, new (arena) RangeMutation());

		// Setting the dbEnd key to be cleared prevents having to treat a range clear to dbEnd as a special
		// case in order to avoid traversing down the rightmost edge of the tree.
		RangeMutation* rm = new (arena) RangeMutation();
		rm->clearBoundary();

		mutations->insert(dbEnd.key, rm);
	}

	// Return a T constructed in arena
	template <typename T>
	T copyToArena(const T& object) {
		return T(arena, object);
	}

	const_iterator upper_bound(const KeyRef& k) const { return const_iterator(mutations->upper_bound(k)); }

	const_iterator lower_bound(const KeyRef& k) const { return const_iterator(mutations->lower_bound(k)); }

	// erase [begin, end) from the mutation map
	void erase(const const_iterator& begin, const const_iterator& end) {
		art_iterator it = begin.artIterator;
		art_iterator next = it;
		++next;
		while (end.artIterator != it) {
			mutations->erase(it);
			it = next;
			++next;
		}
	}

	// Find or create a mutation buffer boundary for bound and return an iterator to it
	iterator insert(KeyRef boundary) {
		int already_present = 0;
		iterator ib = iterator(mutations->insert_if_absent(boundary, nullptr, &already_present));
		if (already_present) {
			return ib;
		}
		*(ib.value_ptr()) = new (arena) RangeMutation();
		iterator iPrevious = ib;
		--iPrevious;
		if (iPrevious.mutation().clearAfterBoundary) {
			ib.mutation().clearAll();
		}
		return ib;
	}
};

#endif // ART_MUTATION_BUFFER
