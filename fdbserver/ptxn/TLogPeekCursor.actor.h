/*
 * TLogPeekCursor.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_PTXN_TLOGPEEKCURSOR_ACTOR_G_H)
#define FDBSERVER_PTXN_TLOGPEEKCURSOR_ACTOR_G_H
#include "fdbserver/ptxn/TLogPeekCursor.actor.g.h"
#elif !defined(FDBSERVER_PTXN_TLOGPEEKCURSOR_ACTOR_H)
#define FDBSERVER_PTXN_TLOGPEEKCURSOR_ACTOR_H

#include <functional>
#include <list>
#include <memory>
#include <queue>
#include <unordered_map>
#include <vector>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/ptxn/MessageTypes.h"
#include "fdbserver/ptxn/TLogInterface.h"
#include "fdbserver/ptxn/TLogStorageServerPeekMessageSerializer.h"
#include "flow/Arena.h"

#include "flow/actorcompiler.h" // has to be the last file included

namespace ptxn {

class PeekCursorBase {
public:
	// Iterator for pulled mutations. This is not a standard input iterator as it is not duplicable, thus no postfix
	// operator++ support. NOTE It will *NOT* trigger remoteMoreAvailable(). It will only iterate over the serialized
	// data that are already peeked from TLog.
	// NOTE: Duplicating the iterator may led to unexpected behavior, i.e. step one iterator causes the other iterators
	// being stepped. It is discouraged to explicitly duplicate the iterator.
	class iterator : public ConstInputIteratorBase<VersionSubsequenceMutation> {
		friend class PeekCursorBase;

		PeekCursorBase* pCursor;
		bool isEndIterator = false;

		// pCursor_ is the pointer to the PeekCursorBase
		// isEndIterator_ is used to determine if the iterator is served as the end of the stream. We never know where
		// the end is when we iterate over the mutations, until we reached the end, thus we check if we are at the end
		// of the stream when we compare the iterator with a special iterator pointing to nothing but having the flag
		// isEndIterator on, and during the comparision we determine if the current iterator is reaching the end. See
		// iterator::operator== for implementation details.
		iterator(PeekCursorBase* pCursor_, bool isEndIterator_);

	public:
		bool operator==(const iterator&) const;
		bool operator!=(const iterator&) const;

		reference operator*() const;
		pointer operator->() const;

		// Prefix incremental
		void operator++();

		// Postfix incremental is disabled since duplication is prohibited.
	};

	PeekCursorBase();

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

private:
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

	// Returns the begin verion for the cursor. The cursor will start from the begin version.
	const Version& getBeginVersion() const;

	// The version the cursor starts
	const Version beginVersion;

	// Last version processed
	Version lastVersion;

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

	// Returns the last version being pulled
	const Version& getLastVersion() const;

protected:
	virtual Future<bool> remoteMoreAvailableImpl() override;
	virtual void nextImpl() override;
	virtual const VersionSubsequenceMutation& getImpl() const override;
	virtual bool hasRemainingImpl() const override;
};

using CursorContainer = std::list<std::unique_ptr<PeekCursorBase>>;

// This class defines the index of a cursor. For a given mutation, it has an unique cursor index. The index is totally
// ordered.
struct IndexedCursor {
	Version version;
	Subsequence subsequence;
	CursorContainer::iterator pCursorPtr;

	IndexedCursor(const Version&, const Subsequence&, CursorContainer::iterator);

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

// Merges multiple cursors, return a single cursor, and reorder the mutations by version/subsequence, in an ascending
// order.
// When peeking for mutations, it is usually acrossing multiple teams, e.g. when StorageServer peeks for
// mutations, it will peek for mutations over all teams it is assigned. MergedPeekCursor will extract the mutations from
// multiple cursors, reorder them by version/subsequence using a heap, and output the result.
class MergedPeekCursor : public PeekCursorBase {
public:
	// NOTE std::priority_queue is a max-heap by default. We need to use std::greater to turn it into a min-heap.
	using CursorHeap = std::priority_queue<IndexedCursor, std::vector<IndexedCursor>, std::greater<IndexedCursor>>;
	using CursorContainer = ptxn::CursorContainer;

protected:
	// In cursorPtrs, we maintain a list of cursors that are still not exhausted. Any cursor that is exhausted will be
	// removed from this list. Exhaust means the cursor have no extra data to be deserialized, nor it could receive
	// more data from TLog.
	CursorContainer cursorPtrs;
	// The cursorHeap also maintains non-exhausting cursors only.
	CursorHeap cursorHeap;

	// Add a cursor to the heap. The cursor must hasRemaining
	void addCursorToCursorHeap(CursorContainer::iterator iter);

private:
	template <typename Iterator>
	void moveUniquePtrsInConstructor(Iterator& begin, Iterator& end) {
		auto iter = begin;
		while (iter != end) {
			cursorPtrs.emplace_back(std::move(*iter));
			++iter;
		}
	}

public:
	// Constructs a MergedPeekCursor holding no cursors. This is also valid and being used by subclasses.
	MergedPeekCursor();

	// Constructs a MergedPeekCursor using a range of std::unique_ptr<PeekCursorBase> objects.
	// Dereferencing the iterator should result a std::unique_ptr<PeekCursorBase> object. The MergedPeekCursor will take
	// over the ownership of the object. All cursors in the container will be taken by MergedPeekCursor.
	template <typename Iterator>
	MergedPeekCursor(Iterator&& begin_, Iterator&& end_) : PeekCursorBase() {
		moveUniquePtrsInConstructor(begin_, end_);
	}

	// Get the number of cursors that are still not exhausted
	size_t getNumActiveCursors() const;

	// Add a new cusror to the merged cursor list
	template <typename T>
	void addCursor(std::unique_ptr<T>&& pCursor) {
		// NOTE: since ASSERT is a macro, it interprets
		//   std::is_base_of<PeekCursorBase, T>::value
		// as two parameters, one is "std:is_base_of<PeekCursorBase" and the other is "T>". Thus additional
		// parenthesises must be used.
		ASSERT((std::is_base_of<PeekCursorBase, T>::value));
		addCursorImpl(std::unique_ptr<PeekCursorBase>(std::move(pCursor)));
	}

protected:
	virtual CursorContainer::iterator addCursorImpl(std::unique_ptr<PeekCursorBase>&&);

	virtual Future<bool> remoteMoreAvailableImpl() override;
	virtual void nextImpl() override;
	virtual const VersionSubsequenceMutation& getImpl() const override;
	virtual bool hasRemainingImpl() const override;
};

// Merges multiple ServerTeamPeekCursor, allowing adding/removing by TeamID.
class MergedServerTeamPeekCursor : public MergedPeekCursor {
private:
	std::unordered_map<StorageTeamID, CursorContainer::iterator> teamIDCursorMapper;

public:
	// Construct a MergedServerTeamPeekCursor holding no cursors.
	MergedServerTeamPeekCursor();

	// Construct a MergedServerTeamPeekCursor using a range of std::unique_ptr<PeekCursorBase> objects.
	template <typename Iterator>
	MergedServerTeamPeekCursor(Iterator&& begin, Iterator&& end) : MergedPeekCursor(begin, end) {
		for (auto iter = std::begin(cursorPtrs); iter != std::end(cursorPtrs); ++iter) {
			const auto* pCursor = dynamic_cast<ServerTeamPeekCursor*>((*iter).get());
			ASSERT(pCursor != nullptr);
			// NOTE std::dynamic_pointer_cast is not supporting std::unique_ptr.
			const auto& teamID = pCursor->getTeamID();
			teamIDCursorMapper[teamID] = iter;
		}
	}

	// Remove an existing ServerTeamPeekCursor by its TeamID
	std::unique_ptr<PeekCursorBase> removeCursor(const StorageTeamID&);

	// Get all TeamIDs for currently active cursors
	std::vector<StorageTeamID> getCursorTeamIDs();

protected:
	virtual CursorContainer::iterator addCursorImpl(std::unique_ptr<PeekCursorBase>&&) override;

	virtual Future<bool> remoteMoreAvailableImpl() override;
};

// Advances the cursor to the given version/subsequence
ACTOR Future<Void> advanceTo(PeekCursorBase* cursor, Version version, Subsequence subsequence = 0);

} // namespace ptxn

#include "flow/unactorcompiler.h"
#endif // FDBSERVER_PTXN_TLOGPEEKCURSOR_ACTOR_H