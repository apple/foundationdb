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

	// Gets one mutation
	const VersionSubsequenceMessage& get() const;

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
	virtual const VersionSubsequenceMessage& getImpl() const = 0;

	// Checks if any remaining mutations
	virtual bool hasRemainingImpl() const = 0;

private:
	// The iterator that represents the end of the unserialized data
	const iterator endIterator;
};

// Connect to given TLog server(s) and peeks for mutations with a given TeamID
class StorageTeamPeekCursor : public PeekCursorBase {
	const StorageTeamID storageTeamID;
	std::vector<TLogInterfaceBase*> pTLogInterfaces;

	// The arena used to store incoming serialized data, if not nullptr, TLogPeekReply arenas will be attached to this
	// arena, enables the access of deserialized data even the cursor is destroyed.
	Arena* pAttachArena;
	SubsequencedMessageDeserializer deserializer;
	SubsequencedMessageDeserializer::iterator deserializerIter;

	// When the cursor checks remoteMoreAvailable, it depends on an ACTOR which accepts TLogPeekReply. TLogPeekReply will
	// include an Arena and a StringRef, representing serialized data. We need to add a reference to the Arena so it will
	// not be GCed after the ACTOR terminates.
	Arena workArena;

	// Returns the begin verion for the cursor. The cursor will start from the begin version.
	const Version& getBeginVersion() const;

	// The version the cursor starts
	const Version beginVersion;

	// Last version processed
	Version lastVersion;

public:
	// version_ is the version the cursor starts with
	// storageTeamID_ is the storageTeamID
	// pTLogInterface_ is the interface to the specific TLog server
	// pArena_ is used to store the serialized data for further use, e.g. making MutationRefs still available after the
	// cursor is destroyed. If pArena_ is nullptr, any reference to the peeked data will be invalidated after the cursor
	// is destructed.
	StorageTeamPeekCursor(const Version& version_,
	                      const StorageTeamID& storageTeamID_,
	                      TLogInterfaceBase* pTLogInterface_,
	                      Arena* pArena_ = nullptr);

	StorageTeamPeekCursor(const Version& version_,
	                      const StorageTeamID& storageTeamID_,
	                      const std::vector<TLogInterfaceBase*>& pTLogInterfaces_,
	                      Arena* arena_ = nullptr);

	const StorageTeamID& getStorageTeamID() const;

	// Returns the last version being pulled
	const Version& getLastVersion() const;

protected:
	virtual Future<bool> remoteMoreAvailableImpl() override;
	virtual void nextImpl() override;
	virtual const VersionSubsequenceMessage& getImpl() const override;
	virtual bool hasRemainingImpl() const override;
};

struct ServerPeekCursor final : ILogSystem::IPeekCursor, ReferenceCounted<ServerPeekCursor> {
	Reference<AsyncVar<OptionalInterface<TLogInterface_PassivelyPull>>> interf;
	const Tag tag;
	const StorageTeamID storageTeamId;
	const TLogGroupID tLogGroupID;

	TLogPeekReply results;
	ArenaReader rd;
	LogMessageVersion messageVersion, end;
	Version poppedVersion;
	TagsAndMessage messageAndTags; // TODO: do we still have tag concept in a message
	bool hasMsg;
	Future<Void> more;
	UID randomID; // TODO: figure out what's this
	bool returnIfBlocked;

	bool onlySpilled;
	bool parallelGetMore;
	int sequence;
	Deque<Future<TLogPeekReply>> futureResults;
	Future<Void> interfaceChanged;

	double lastReset;
	Future<Void> resetCheck;
	int slowReplies;
	int fastReplies;
	int unknownReplies;

	ServerPeekCursor(Reference<AsyncVar<OptionalInterface<TLogInterface_PassivelyPull>>> const& interf,
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

} // namespace ptxn

#include "flow/unactorcompiler.h"
#endif // FDBSERVER_PTXN_TLOGPEEKCURSOR_ACTOR_H
