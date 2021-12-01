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

#include <deque>
#include <list>
#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/ptxn/MessageTypes.h"
#include "fdbserver/ptxn/MessageSerializer.h"
#include "fdbserver/ptxn/TLogInterface.h"
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
	class iterator : public ConstInputIteratorBase<VersionSubsequenceMessage> {
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
		iterator& operator++(int) = delete;
	};

	PeekCursorBase();

	// Checks if there are any more messages in the remote TLog(s), if so, retrieve the messages to local. Will
	// invalidate the existing iterators.
	Future<bool> remoteMoreAvailable();

	// Gets one mutation, the behavior is undefined if hasRemaining is not called or returning false.
	const VersionSubsequenceMessage& get() const;

	// Moves to the next mutation, the behavior is undefined if hasRemaining is not called or returning false.
	void next();

	// Any remaining mutation *LOCALLY* available. This *MUST* be verified prior to get or next, otherwise the
	// behavior is undefined.
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
	virtual const VersionSubsequenceMessage& getImpl() const = 0;

	// Checks if any remaining mutations
	virtual bool hasRemainingImpl() const = 0;

private:
	// The iterator that represents the end of the unserialized data
	const iterator endIterator;
};

namespace details {

class VersionSubsequencePeekCursorBase : public PeekCursorBase {
public:
	VersionSubsequencePeekCursorBase(const Version version_ = invalidVersion,
	                                 const Subsequence subsequence_ = invalidSubsequence);

	// Returns the commit version of the current message,
	// If there is no message under cursor, the behavior is undefined.
	const Version& getVersion() const;

	// Returns the subsequence of the current message,
	// If there is no message under cursor, the behavior is undefined.
	const Subsequence& getSubsequence() const;

	// FIXME use C++20 operator<=>
	// Orders two cursors using the message version/subsequence
	int operatorSpaceship(const VersionSubsequencePeekCursorBase& other) const;

	bool operator<(const VersionSubsequencePeekCursorBase& other) { return operatorSpaceship(other) < 0; }
	bool operator<=(const VersionSubsequencePeekCursorBase& other) { return operatorSpaceship(other) <= 0; }
	bool operator>(const VersionSubsequencePeekCursorBase& other) { return operatorSpaceship(other) > 0; }
	bool operator>=(const VersionSubsequencePeekCursorBase& other) { return operatorSpaceship(other) >= 0; }
	bool operator==(const VersionSubsequencePeekCursorBase& other) { return operatorSpaceship(other) == 0; }
	bool operator!=(const VersionSubsequencePeekCursorBase& other) { return operatorSpaceship(other) != 0; }
};

// Stores an object with type ObjectType. The object should have an arena() method which returns a reference to the
// internal arena it is using.
// For an arena A, one would expect using B.dependsOn(A) would extent A's lifecycle to B's lifecycle. However this is
// not quite true. Internally arenas are implemented as linked lists, or
//
//  A.impl =      ArenaBlock -> ArenaBlock -> ...
//
// where each ArenaBlock is a piece of allocated memory managed by reference counting. Applying B.dependsOn(A) would
// cause B refers to the first ArenaBlock in A, or
//
//            B----|
//                 V
//  A.impl =      ArenaBlock -> ArenaBlock -> ...
//
// so when A destructs, there is still at least one extra reference to the first ArenaBlock, prevening the release of
// the chained blocks
// However, when additional memory is requested to A, A will create additional ArenaBlocks (see
// Arena.cpp:ArenaBlock::create), and the new ArenaBlock might be inserted *prior* to the existing ArenaBlock, i.e.
//
//                           B----|
//                                V
//  A.impl =      ArenaBlock -> ArenaBlock -> ArenaBlock ...
//                   ^- new created
// and when A is destructed, the unreferrenced (hereby the first one) will be destructed. This causes B depends on only
// part of A. The only way to ensure all blocks in A have the same life cycle to B is to let B.dependsOn(A) get called
// when A is stable, or no extra ArenaBlocks will be created. In this implementation, we only call dependsOn when the
// original object is about to be destructed, thus effectivley extend the lifecycle of the internal arena.
template <typename ObjectType>
class ArenaWrapper {
	ObjectType object;
	Arena* pAttachArena;

	void attachArena() {
		if (pAttachArena) {
			pAttachArena->dependsOn(object.arena());
		}
	}

public:
	explicit ArenaWrapper(const ObjectType& object_, Arena* pAttachArena_ = nullptr)
	  : object(object_), pAttachArena(pAttachArena_) {}
	explicit ArenaWrapper(ObjectType&& object_, Arena* pAttachArena_ = nullptr)
	  : object(std::move(object_)), pAttachArena(pAttachArena_) {}
	ArenaWrapper& operator=(const ObjectType& object_) {
		attachArena();
		object = object_;
		return *this;
	}
	ArenaWrapper& operator=(ObjectType&& object_) {
		attachArena();
		object = std::move(object_);
		return *this;
	}

	~ArenaWrapper() { attachArena(); }

	ObjectType& get() { return object; }
	const ObjectType& get() const { return object; }
};

} // namespace details

// Connect to given TLog server(s) and peeks for mutations with a given TeamID
class StorageTeamPeekCursor : public details::VersionSubsequencePeekCursorBase {

private:
	const StorageTeamID storageTeamID;
	std::vector<TLogInterfaceBase*> pTLogInterfaces;

	// The arena used to store incoming serialized data, if not nullptr, TLogPeekReply arenas will be attached to this
	// arena, enables the access of deserialized data even the cursor is destroyed.
	Arena* pAttachArena;

	SubsequencedMessageDeserializer deserializer;
	// The iterator will deserialize the incoming data to an arena on-the-fly. The life-cycle of the arena is the same
	// to the iterator. If the deserialized data need to be referred in the future, the arena needs to be dependOn
	// another arena, which is the *pArena in the class. pArena cannot immediately dependOn the arena in the iterator,
	// so ArenaWrapper has to be used. See the documenation of ArenaWrapper.
	mutable details::ArenaWrapper<SubsequencedMessageDeserializer::iterator> wrappedDeserializerIter;

	// When the cursor checks remoteMoreAvailable, it depends on an ACTOR which accepts TLogPeekReply. TLogPeekReply
	// will include an Arena and a StringRef, representing serialized data. We need to add a reference to the Arena so
	// it will not be GCed after the ACTOR terminates.
	Arena workArena;

	// The version the cursor begins, all versions that are smaller than beginVersion will be ignored by remote TLog
	const Version beginVersion;

	// If true, will return a EmptyMessage when the cursor meets a version without messages, Otherwise the empty
	// version will be ignored
	bool reportEmptyVersion;

	// The last version that the cursor have received, the next remote RPC will return versions larger than lastVersion,
	// if available.
	Version lastVersion;

public:
	// version_ is the version the cursor begins with
	// storageTeamID_ is the storageTeamID
	// pTLogInterface_ is the interface to the specific TLog server
	// pArena_ is used to store the serialized data for further use, e.g. making MutationRefs still available after the
	// cursor is destroyed. If pArena_ is nullptr, any reference to the peeked data will be invalidated after the cursor
	// is destructed.
	// reportEmptyVersion_ flag will allow an empty version trigger an EmptyMessage, if set true
	StorageTeamPeekCursor(const Version& version_,
	                      const StorageTeamID& storageTeamID_,
	                      TLogInterfaceBase* pTLogInterface_,
	                      Arena* pArena_ = nullptr,
	                      const bool reportEmptyVersion_ = false);

	StorageTeamPeekCursor(const Version& version_,
	                      const StorageTeamID& storageTeamID_,
	                      const std::vector<TLogInterfaceBase*>& pTLogInterfaces_,
	                      Arena* pArena_ = nullptr,
	                      const bool reportEmptyVersion_ = false);

	bool isEmptyVersionsIgnored() const { return !reportEmptyVersion; }

	const StorageTeamID& getStorageTeamID() const;

	// Returns the beginning verion for the cursor. The cursor will start from the begin version.
	const Version& getBeginVersion() const;

protected:
	virtual Future<bool> remoteMoreAvailableImpl() override;
	virtual void nextImpl() override;
	virtual const VersionSubsequenceMessage& getImpl() const override;
	virtual bool hasRemainingImpl() const override;
};

// Cursor that merges multiple cursors into one
namespace merged {

namespace details {

class CursorContainerBase {
public:
	// Type of the cursor
	using element_t = std::shared_ptr<ptxn::details::VersionSubsequencePeekCursorBase>;

	// Type of the container
	using container_t = std::deque<element_t>;

	// Type of the container iterator
	using iterator_t = typename container_t::const_iterator;

protected:
	container_t container;

	virtual void pushImpl(const element_t& pCursor) = 0;
	virtual void popImpl() = 0;

public:
	// Accesses the first element in the container
	// Here "first" is implementation-specific, see the documentation of subclass
	element_t& front() { return container.front(); }

	// Accesses the first element in the container
	// Here "first" is implementation-specific, see the documenation of subclass
	const element_t& front() const { return container.front(); }

	// Returns an iterator points to the beginning of the container
	// The iterator does not imply any order of the cursors
	iterator_t begin() const { return std::cbegin(container); }

	// Returns an iterator points to the end of the container
	iterator_t end() const { return std::cend(container); }

	// Returns true if there is no element in the container
	bool empty() const { return container.empty(); }

	// Returns the count of the elements in the container
	int size() const { return container.size(); }

	// Adds a new cursor (in pointer type) to the container
	void push(const element_t& pCursor) { pushImpl(pCursor); }

	// Remove the first element in the container
	// Here "first" is implementation-specific, see the documentation of subclass
	void pop() { popImpl(); }
};

// Provides an ordered container of cursors. In the container the first element is defined as the container that is
// the "smallest" in the container, where `smallest" is defined by the implementation of operator< method of the cursor.
class OrderedCursorContainer : public CursorContainerBase {
	// Compare two elements, by default C++ implements max heap, while what needed here is a min heap, the comparator
	// implements a greater-than algorithm.
	static bool heapElementComparator(element_t e1, element_t e2);

protected:
	virtual void pushImpl(const element_t& pCursor) override;
	virtual void popImpl() override;

public:
	OrderedCursorContainer();
};

// Provides an unordered container of cursors, the container behaves like a FIFO-queue. The first element is defined as
// the earliest element in the container.
class UnorderedCursorContainer : public CursorContainerBase {
protected:
	virtual void pushImpl(const element_t& pCursor) override;
	virtual void popImpl() override;
};

// Provides a Storage Team ID to StorageTeamPeekCursor mapping functionality
class StorageTeamIDCursorMapper {
public:
	using StorageTeamIDCursorMapper_t = std::unordered_map<StorageTeamID, std::shared_ptr<StorageTeamPeekCursor>>;

private:
	StorageTeamIDCursorMapper_t mapper;

public:
	// Moves a cursor to the mapping system, the original cursor is invalidated
	void addCursor(std::shared_ptr<StorageTeamPeekCursor>&& cursor);

	// Removes a cursor from the mapping system
	std::shared_ptr<StorageTeamPeekCursor> removeCursor(const StorageTeamID& storageTeamID);

	// Checks if a storage team ID is in the mapping system
	bool isCursorExists(const StorageTeamID&) const;

	// Gets the cursor for the given storage team ID
	StorageTeamPeekCursor& getCursor(const StorageTeamID& storageTeamID);

	// Gets the cursor for the given storage team ID
	const StorageTeamPeekCursor& getCursor(const StorageTeamID& storageTeamID) const;

	// Gets the number of storage teams
	int getNumCursors() const;

	// Returns the iterator points at the beginning of the (Storage Team ID, cursor) pair list.
	StorageTeamIDCursorMapper_t::iterator cursorsBegin();

	// Returns the iterator points at the end of the (Storage Team ID, cursor) pair list.
	StorageTeamIDCursorMapper_t::iterator cursorsEnd();

protected:
	virtual void addCursorImpl(std::shared_ptr<StorageTeamPeekCursor>&& cursor);
	virtual std::shared_ptr<StorageTeamPeekCursor> removeCursorImpl(const StorageTeamID& cursor);

	// Gets the shared_ptr for the given storage team ID
	std::shared_ptr<StorageTeamPeekCursor> getCursorPtr(const StorageTeamID&);
};

} // namespace details

// Base class for peeking data from multiple storage teams, when all storage teams are notified when a commit is done.
// In this case, even different teams might have different team versions, they will be informed when the commit version
// has changed.
class BroadcastedStorageTeamPeekCursorBase : public ptxn::details::VersionSubsequencePeekCursorBase,
                                             protected details::StorageTeamIDCursorMapper {
protected:
	std::unique_ptr<details::CursorContainerBase> pCursorContainer;

	// The current version the cursorContainer is using
	Version currentVersion;

	// The list of cursors that requires RPC
	std::list<StorageTeamID> emptyCursorStorageTeamIDs;

	// The list of cursors that has finished epoch
	std::list<StorageTeamID> retiredCursorStorageTeamIDs;

protected:
	BroadcastedStorageTeamPeekCursorBase(std::unique_ptr<details::CursorContainerBase>&& pCursorContainer_)
	  : pCursorContainer(std::move(pCursorContainer_)) {}

	// Tries to fill the cursor heap, returns true if the cursorContainer is filled with cursors.
	// If cursorContainer is not empty, the behavior is undefined.
	bool tryFillCursorContainer();

	virtual Future<bool> remoteMoreAvailableImpl() override;
	virtual const VersionSubsequenceMessage& getImpl() const override;
	virtual bool hasRemainingImpl() const override;

	virtual void addCursorImpl(std::shared_ptr<StorageTeamPeekCursor>&& cursor) override;

public:
	using details::StorageTeamIDCursorMapper::addCursor;
	using details::StorageTeamIDCursorMapper::getNumCursors;
	using details::StorageTeamIDCursorMapper::isCursorExists;
};

// Merge multiple storage team peek cursor into one. The version is the barrier, i.e., a version is complete iff all
// cursors has the version and currently locate at the version. The messages will be iteratived in the order of (commit
// version, subsequence)
class BroadcastedStorageTeamPeekCursor_Ordered : public BroadcastedStorageTeamPeekCursorBase {
protected:
	virtual void nextImpl() override;

public:
	BroadcastedStorageTeamPeekCursor_Ordered()
	  : BroadcastedStorageTeamPeekCursorBase(std::make_unique<details::OrderedCursorContainer>()) {}
};

// Merge multiple storage team peek cursor into one. The version is the barrier, i.e., a version is complete iff all
// cursors has the version and currently locate at the version. The messages will be iteratived per storage team. Within
// the storage team, the messages will be ordered by subsequence; after all messages in the team are consumed, the
// cursor will move to the next storage team.
class BroadcastedStorageTeamPeekCursor_Unordered : public BroadcastedStorageTeamPeekCursorBase {
protected:
	virtual void nextImpl() override;

public:
	BroadcastedStorageTeamPeekCursor_Unordered()
	  : BroadcastedStorageTeamPeekCursorBase(std::make_unique<details::UnorderedCursorContainer>()) {}
};

} // namespace merged

// Advances the cursor to the given version/subsequence
ACTOR Future<Void> advanceTo(PeekCursorBase* cursor, Version version, Subsequence subsequence = 0);

//////////////////////////////////////////////////////////////////////////////////
// ServerPeekCursor used for demo
//////////////////////////////////////////////////////////////////////////////////

struct ServerPeekCursor final : ILogSystem::IPeekCursor, ReferenceCounted<ServerPeekCursor> {
	Reference<AsyncVar<OptionalInterface<TLogInterface_PassivelyPull>>> interf;
	const Tag tag;
	const StorageTeamID storageTeamId;
	const TLogGroupID tLogGroupID;

	ptxn::TLogPeekReply results;
	ArenaReader rd;

	// Current message version (initialized by begin version).
	LogMessageVersion messageVersion;

	// Exclusive end version of the cursor
	LogMessageVersion end;

	Version poppedVersion = 0;
	TagsAndMessage messageAndTags; // TODO: do we still have tag concept in a message
	bool hasMsg = false;
	Future<Void> more;
	UID dbgid; // i.e., unique debugID of this cursor.
	bool returnIfBlocked = false;

	int numMessagesInCurrentVersion = 0;
	int messageIndexInCurrentVersion = 0;

	bool onlySpilled = false;
	bool parallelGetMore = false;
	int sequence = 0;
	Deque<Future<ptxn::TLogPeekReply>> futureResults;
	Future<Void> interfaceChanged;

	double lastReset = 0;
	Future<Void> resetCheck = Void();
	int slowReplies = 0;
	int fastReplies = 0;
	int unknownReplies = 0;

	ServerPeekCursor(Reference<AsyncVar<OptionalInterface<TLogInterface_PassivelyPull>>> interf,
	                 Tag tag,
	                 StorageTeamID storageTeamID,
	                 TLogGroupID tLogGroupID,
	                 Version begin,
	                 Version end,
	                 bool returnIfBlocked,
	                 bool parallelGetMore);
	ServerPeekCursor(TLogPeekReply const& results,
	                 LogMessageVersion const& messageVersion,
	                 LogMessageVersion const& end,
	                 TagsAndMessage const& message,
	                 bool hasMsg,
	                 Version poppedVersion,
	                 Tag tag,
	                 StorageTeamID storageTeamID,
	                 TLogGroupID tLogGroupID);

	Reference<IPeekCursor> cloneNoMore() override;
	void setProtocolVersion(ProtocolVersion version) override;
	Arena& arena() override;
	ArenaReader* reader() override;
	bool hasMessage() const override;
	void nextMessage() override;
	StringRef getMessage() override;
	StringRef getMessageWithTags() override;
	VectorRef<Tag> getTags() const override;
	void advanceTo(LogMessageVersion n) override;
	Future<Void> getMore(TaskPriority taskID = TaskPriority::TLogPeekReply) override;
	Future<Void> onFailed() override;
	bool isActive() const override;
	bool isExhausted() const override;
	const LogMessageVersion& version() const override;
	Version popped() const override;
	Version getMinKnownCommittedVersion() const override;
	Optional<UID> getPrimaryPeekLocation() const override;
	Optional<UID> getCurrentPeekLocation() const override;

	void addref() override { ReferenceCounted<ServerPeekCursor>::addref(); }

	void delref() override { ReferenceCounted<ServerPeekCursor>::delref(); }

	Version getMaxKnownVersion() const override { return results.maxKnownVersion; }
};

//////////////////////////////////////////////////////////////////////////////////
// ServerPeekCursor used for demo -- end
//////////////////////////////////////////////////////////////////////////////////

} // namespace ptxn

#include "flow/unactorcompiler.h"
#endif // FDBSERVER_PTXN_TLOGPEEKCURSOR_ACTOR_H
