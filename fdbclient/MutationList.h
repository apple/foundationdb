/*
 * MutationList.h
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

#ifndef FLOW_FDBCLIENT_MUTATIONLIST_H
#define FLOW_FDBCLIENT_MUTATIONLIST_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"

class MutationListRef : private VectorRef<MutationRef> {
	// Represents an ordered, but not random-access, list of mutations that can be O(1) deserialized and
	// quickly serialized, (forward) iterated or appended to.
	
	using parent = VectorRef<MutationRef>;
	size_t mTotalSize = 0;
public:
	MutationListRef() : parent() {}
	MutationListRef(Arena& p, const MutationListRef& toCopy)
		: parent(toCopy)
	{}
	void resize( Arena& p, int size ) {
		parent::resize(p, size);
	}
	const MutationRef& operator[](int i) const { return parent::operator[](i); }
	MutationRef& operator[](int i) { return parent::operator[](i); }
	MutationRef& push_back_deep(Arena& p, const MutationRef& value) {
		parent::push_back_deep(p, value);
		mTotalSize += sizeof(MutationRef) + value.param1.size() + value.param2.size();
		return parent::back();
	}
	void append_deep( Arena& p, const MutationRef* begin, int count ) {
		parent::append_deep(p, begin, count);
		for (int i = 0; i < count; ++i) {
			mTotalSize += sizeof(MutationRef) + begin->param1.size() + begin->param2.size();
			++begin;
		}
	}

	MutationRef* begin() { return parent::begin(); }
	const MutationRef* begin() const { return parent::begin(); }
	MutationRef* end() { return parent::end(); }
	const MutationRef* end() const { return parent::end(); }
	size_t totalSize() const { return mTotalSize; }
	size_t expectedSize() const { return mTotalSize + sizeof(MutationListRef); }
	int size() const { return parent::size(); }
};

template <class Archive>
inline void load( Archive& ar, MutationListRef& value ) {
	// FIXME: range checking for length, here and in other serialize code
	uint32_t length;
	ar >> length;
	UNSTOPPABLE_ASSERT( length*sizeof(MutationRef) < (100<<20) );
	// SOMEDAY: Can we avoid running constructors for all the values?
	value.resize(ar.arena(), length);
	for(uint32_t i=0; i<length; i++)
		ar >> value[i];
}
template <class Archive>
inline void save( Archive& ar, const MutationListRef& value ) {
	uint32_t length = value.size();
	ar << length;
	for(uint32_t i=0; i<length; i++)
		ar << value[i];
}
typedef Standalone<MutationListRef> MutationList;

#endif
