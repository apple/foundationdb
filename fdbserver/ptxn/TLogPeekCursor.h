/*
 * TLogPeekCursor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_PTXN_TLOGPEEKCURSOR_H
#define FDBSERVER_PTXN_TLOGPEEKCURSOR_H

#pragma once

#include <initializer_list>
#include <functional>
#include <list>
#include <queue>
#include <vector>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/ptxn/MessageTypes.h"
#include "fdbserver/ptxn/TLogInterface.h"
#include "fdbserver/ptxn/TLogStorageServerPeekMessageSerializer.h"
#include "flow/Arena.h"

namespace ptxn {

class PeekCursorBase {
public:
	// Iterator for pulled mutations. This is not a standard input iterator as it is not duplicable, thus no postfix
	// operator++ support. NOTE It will *NOT* trigger remoteMoreAvailable(). It will only iterate over the serialized
	// data that are already peeked from TLog.
	class iterator : public ConstInputIteratorBase<VersionSubsequenceMutation> {
		friend class PeekCursorBase;

		PeekCursorBase* pCursor;
		bool isEndIterator = false;

		iterator(PeekCursorBase*, bool);

	public:
		// Since the iterator will change the interanl state of the cursor, duplication is prohibited.
		iterator& operator=(const iterator&) = delete;

		bool operator==(const iterator&) const;
		bool operator!=(const iterator&) const;

		reference operator*() const;
		pointer operator->() const;

		// Prefix incremental
		void operator++();

		// Postfix incremental is disabled since duplication is prohibited.
	};

	PeekCursorBase(const Version&);

	// Returns the begin verion for the cursor. The cursor will start from the begin version.
	const Version& getBeginVersion() const;

	// Returns the last version being pulled
	const Version& getLastVersion() const;

	// Checks if there is any more messages in the remote TLog(s), if so, retrieve the messages to local. Will
	// invalidate the iterator, if exists.
	Future<bool> remoteMoreAvailable();

	// Gets one mutation
	const VersionSubsequenceMutation& get() const;

	// Moves to the next mutation, return false if there is no more mutation
	void next();

	// Any remaining mutation *LOCALLY* available
	bool hasRemaining() const;

	// Returns an iterator that represents the begin of the data still undeserialized.
	iterator begin() { return iterator(this, false); }

	// Returns an iterator that represents the end of the data
	const iterator& end() const { return endIterator; }

protected:
	// Checks if there is any mutations remotely
	virtual Future<bool> remoteMoreAvailableImpl() = 0;

	// Steps the local cursor
	virtual void nextImpl() = 0;

	// Gets the message
	virtual const VersionSubsequenceMutation& getImpl() const = 0;

	// Checks if any remaining mutations
	virtual bool hasRemainingImpl() const = 0;

	// Last version processed
	Version lastVersion;

private:
	// The version the cursor starts
	const Version beginVersion;

	// The iterator that represents the end of the unserialized data
	const iterator endIterator;
};

// Connect to a given TLog server and peeks for mutations with a given TeamID
class ServerTeamPeekCursor : public PeekCursorBase {
	const StorageTeamID teamID;
	std::vector<TLogInterfaceBase*> pTLogInterfaces;

	// The arena used to store incoming serialized data, if not nullptr, TLogPeekReply arenas will be attached to this
	// arena, enables the access of deserialized data even the cursor is destroyed.
	Arena* pAttachArena;
	TLogStorageServerMessageDeserializer deserializer;
	TLogStorageServerMessageDeserializer::iterator deserializerIter;

public:
	// version_ is the version the cursor starts with
	// teamID_ is the teamID
	// pTLogInterface_ is the interface to the specific TLog server
	// pArena_ is used to store the serialized data for further use, e.g. making MutationRefs still available after the
	// cursor is destroyed.
	ServerTeamPeekCursor(const Version& version_,
	                     const StorageTeamID& teamID_,
	                     TLogInterfaceBase* pTLogInterface_,
	                     Arena* arena_ = nullptr);

	ServerTeamPeekCursor(const Version& version_,
	                     const StorageTeamID& teamID_,
	                     const std::vector<TLogInterfaceBase*>& pTLogInterfaces_,
	                     Arena* arena_ = nullptr);

	const StorageTeamID& getTeamID() const;

protected:
	virtual Future<bool> remoteMoreAvailableImpl() override;
	virtual void nextImpl() override;
	virtual const VersionSubsequenceMutation& getImpl() const override;
	virtual bool hasRemainingImpl() const override;
};

// This class defines the index of a cursor. For a given mutation, it has an unique cursor index. The index is totally
// ordered.
struct IndexedCursor {
	Version version;
	Subsequence subsequence;
	PeekCursorBase* pCursor;

	IndexedCursor(const Version&, const Subsequence&, PeekCursorBase*);

	// TODO the return type should be std::strong_ordering
	friend constexpr int operatorSpaceship(const IndexedCursor&, const IndexedCursor&);

	// TODO Replace the code with real spaceship operator when C++20 is allowed
	bool operator>(const IndexedCursor& another) const { return operatorSpaceship(*this, another) == 1; }
	bool operator>=(const IndexedCursor& another) const { return operatorSpaceship(*this, another) >= 0; }
	bool operator<(const IndexedCursor& another) const { return operatorSpaceship(*this, another) == -1; }
	bool operator<=(const IndexedCursor& another) const { return operatorSpaceship(*this, another) <= 0; }
	bool operator!=(const IndexedCursor& another) const { return operatorSpaceship(*this, another) != 0; }
	bool operator==(const IndexedCursor& another) const { return operatorSpaceship(*this, another) == 0; }
};

// Returns
//   0 if  c1 == c2
//   1 if  c1 >  c2
//  -1 if  c1 <  c2
inline constexpr int operatorSpaceship(const IndexedCursor& c1, const IndexedCursor& c2) {
	if (c1.version > c2.version) {
		return 1;
	} else if (c1.version < c2.version) {
		return -1;
	} else {
		if (c1.subsequence > c2.subsequence) {
			return 1;
		} else if (c1.subsequence < c2.subsequence) {
			return -1;
		}

		return 0;
	}
}

// TODO Use Reference or std::shared_ptr
using PeekCursorBasePtr = PeekCursorBase*;

namespace {

template <typename Iterator>
Version getMinimalVersionInCursors(const Iterator& begin, const Iterator& end) {
	Version min = MAX_VERSION;
	for (Iterator iter = begin; iter != end; ++iter) {
		min = std::min(min, (*iter)->getBeginVersion());
	}
	return min;
}

} // anonymous namespace

// Merge several cursors, return a single cursor
class MergedPeekCursor : public PeekCursorBase {
public:
	// NOTE std::priority_queue is a max-heap by default. We need to use std::greater to turn it to a min-heap.
	using Heap = std::priority_queue<IndexedCursor, std::vector<IndexedCursor>, std::greater<IndexedCursor>>;
private:
	// In cursorPtrs, we maintain a list of cursors that are still not exhausted. Any cursor that is exhausted will be
	// removed from this list. Exhaust means the cursor have no extra data to be deserialized, nor it could receive
	// more data from TLog.
	std::list<PeekCursorBasePtr> cursorPtrs;
	// The cursorHeap also maintains non-exhausting cursors only.
	Heap cursorHeap;

public:
	// With MergedPeekCursor, it joins multiple cursors, merge the mutations, and allowing iteratively accesing them. To
	// ensure the merge cursor works properly, all cursors must be in "local exhausted" state, i.e.
	//   pCursor->begin() == pCursor->end()
	MergedPeekCursor(std::initializer_list<PeekCursorBasePtr>);
	// Dereferencing the iterator should result a PeekCursorBasePtr object.
	template <typename Iterator>
	MergedPeekCursor(const Iterator& begin_, const Iterator& end_)
	  : PeekCursorBase(getMinimalVersionInCursors(begin_, end_)), cursorPtrs(begin_, end_) {}

	// Get the number of cursors that are still not exhausted
	size_t getNumActiveCursor() const;

protected:
	virtual Future<bool> remoteMoreAvailableImpl() override;
	virtual void nextImpl() override;
	virtual const VersionSubsequenceMutation& getImpl() const override;
	virtual bool hasRemainingImpl() const override;
};

} // namespace ptxn

#endif // FDBSERVER_PTXN_TLOGPEEKCURSOR_H