/*
 * LogSystem.h
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

#ifndef FDBSERVER_LOGSYSTEM_H
#define FDBSERVER_LOGSYSTEM_H
#pragma once

#include "TLogInterface.h"
#include "WorkerInterface.h"
#include "DatabaseConfiguration.h"
#include "flow/IndexedSet.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/Replication.h"

struct DBCoreState;

struct ILogSystem {
	// Represents a particular (possibly provisional) epoch of the log subsystem


	struct IPeekCursor {
		//clones the peek cursor, however you cannot call getMore() on the cloned cursor.
		virtual Reference<IPeekCursor> cloneNoMore() = 0;

		virtual void setProtocolVersion( uint64_t version ) = 0;

		//if hasMessage() returns true, getMessage() or reader() can be called.
		//does not modify the cursor
		virtual bool hasMessage() = 0;

		//pre: only callable if hasMessage() returns true
		//returns the arena containing the contents of getMessage() and reader()
		virtual Arena& arena() = 0;

		//pre: only callable if hasMessage() returns true
		//returns an arena reader for the next message
		//caller cannot call both getMessage() and reader()
		//the caller must advance the reader before calling nextMessage()
		virtual ArenaReader* reader() = 0;

		//pre: only callable if hasMessage() returns true
		//caller cannot call both getMessage() and reader()
		//return the contents of the message for the current sequence
		virtual StringRef getMessage() = 0;

		//pre: only callable after getMessage() or reader()
		//post: hasMessage() and version() have been updated
		//hasMessage() will never return false "in the middle" of a version (that is, if it does return false, version().subsequence will be zero)  < FIXME: Can we lose this property?
		virtual void nextMessage() = 0;

		//advances the cursor to the supplied LogMessageVersion, and updates hasMessage
		virtual void advanceTo(LogMessageVersion n) = 0;

		//returns immediately if hasMessage() returns true.
		//returns when either the result of hasMessage() or version() has changed.
		virtual Future<Void> getMore(int taskID = TaskTLogPeekReply) = 0;

		//returns when the failure monitor detects that the servers associated with the cursor are failed
		virtual Future<Void> onFailed() = 0;

		//returns false if:
		// (1) the failure monitor detects that the servers associated with the cursor is failed
		// (2) the interface is not present
		// (3) the cursor cannot return any more results
		virtual bool isActive() = 0;

		// Returns the smallest possible message version which the current message (if any) or a subsequent message might have
		// (If hasMessage(), this is therefore the message version of the current message)
		virtual LogMessageVersion version() = 0;

		//So far, the cursor has returned all messages which both satisfy the criteria passed to peek() to create the cursor AND have (popped(),0) <= message version number <= version()
		//Other messages might have been skipped
		virtual Version popped() = 0;

		// Returns the maximum version known to have been pushed (not necessarily durably) into the log system (0 is always a possible result!)
		virtual Version getMaxKnownVersion() { return 0; }

		virtual void addref() = 0;

		virtual void delref() = 0;
	};

	struct ServerPeekCursor : IPeekCursor, ReferenceCounted<ServerPeekCursor> {
		Reference<AsyncVar<OptionalInterface<TLogInterface>>> interf;
		Tag tag;

		TLogPeekReply results;
		ArenaReader rd;
		LogMessageVersion messageVersion, end;
		Version poppedVersion;
		int32_t messageLength;
		bool hasMsg;
		Future<Void> more;
		UID randomID;
		bool returnIfBlocked;

		bool parallelGetMore;
		int sequence;
		Deque<Future<TLogPeekReply>> futureResults;
		Future<Void> interfaceChanged;

		ServerPeekCursor( Reference<AsyncVar<OptionalInterface<TLogInterface>>> const& interf, Tag tag, Version begin, Version end, bool returnIfBlocked, bool parallelGetMore );

		ServerPeekCursor( TLogPeekReply const& results, LogMessageVersion const& messageVersion, LogMessageVersion const& end, int32_t messageLength, bool hasMsg, Version poppedVersion );

		virtual Reference<IPeekCursor> cloneNoMore();

		virtual void setProtocolVersion( uint64_t version );

		virtual Arena& arena();

		virtual ArenaReader* reader();

		virtual bool hasMessage();

		virtual void nextMessage();

		virtual StringRef getMessage();

		virtual void advanceTo(LogMessageVersion n);

		virtual Future<Void> getMore(int taskID = TaskTLogPeekReply);

		virtual Future<Void> onFailed();

		virtual bool isActive();

		virtual LogMessageVersion version();

		virtual Version popped();

		virtual void addref() {
			ReferenceCounted<ServerPeekCursor>::addref();
		}

		virtual void delref() {
			ReferenceCounted<ServerPeekCursor>::delref();
		}

		virtual Version getMaxKnownVersion() { return results.maxKnownVersion; }
	};

	struct MergedPeekCursor : IPeekCursor, ReferenceCounted<MergedPeekCursor> {
		LocalityGroup localityGroup;
		std::vector< std::pair<LogMessageVersion, int> > sortedVersions;
		vector< Reference<IPeekCursor> > serverCursors;
		Tag tag;
		int bestServer, currentCursor, readQuorum;
		Optional<LogMessageVersion> nextVersion;
		LogMessageVersion messageVersion;
		bool hasNextMessage;
		UID randomID;
		int tLogReplicationFactor;
		IRepPolicyRef tLogPolicy;
		std::vector< LocalityData > tLogLocalities;

		MergedPeekCursor( std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> const& logServers, int bestServer, int readQuorum, Tag tag, Version begin, Version end, bool parallelGetMore, std::vector< LocalityData > const& tLogLocalities, IRepPolicyRef const tLogPolicy, int tLogReplicationFactor );

		MergedPeekCursor( vector< Reference<IPeekCursor> > const& serverCursors, LogMessageVersion const& messageVersion, int bestServer, int readQuorum, Optional<LogMessageVersion> nextVersion, std::vector< LocalityData > const& tLogLocalities, IRepPolicyRef const tLogPolicy, int tLogReplicationFactor );

		// if server_cursors[c]->hasMessage(), then nextSequence <= server_cursors[c]->sequence() and there are no messages known to that server with sequences in [nextSequence,server_cursors[c]->sequence())

		virtual Reference<IPeekCursor> cloneNoMore();

		virtual void setProtocolVersion( uint64_t version );

		virtual Arena& arena();

		virtual ArenaReader* reader();

		void calcHasMessage();

		void updateMessage(bool usePolicy);

		virtual bool hasMessage();

		virtual void nextMessage();

		virtual StringRef getMessage();

		virtual void advanceTo(LogMessageVersion n);

		virtual Future<Void> getMore(int taskID = TaskTLogPeekReply);

		virtual Future<Void> onFailed();

		virtual bool isActive();

		virtual LogMessageVersion version();

		virtual Version popped();

		virtual void addref() {
			ReferenceCounted<MergedPeekCursor>::addref();
		}

		virtual void delref() {
			ReferenceCounted<MergedPeekCursor>::delref();
		}
	};

	struct MultiCursor : IPeekCursor, ReferenceCounted<MultiCursor> {
		std::vector<Reference<IPeekCursor>> cursors;
		std::vector<LogMessageVersion> epochEnds;
		Version poppedVersion;

		MultiCursor( std::vector<Reference<IPeekCursor>> cursors, std::vector<LogMessageVersion> epochEnds );

		virtual Reference<IPeekCursor> cloneNoMore();

		virtual void setProtocolVersion( uint64_t version );

		virtual Arena& arena();

		virtual ArenaReader* reader();

		virtual bool hasMessage();

		virtual void nextMessage();

		virtual StringRef getMessage();

		virtual void advanceTo(LogMessageVersion n);

		virtual Future<Void> getMore(int taskID = TaskTLogPeekReply);

		virtual Future<Void> onFailed();

		virtual bool isActive();

		virtual LogMessageVersion version();

		virtual Version popped();

		virtual void addref() {
			ReferenceCounted<MultiCursor>::addref();
		}

		virtual void delref() {
			ReferenceCounted<MultiCursor>::delref();
		}
	};

	virtual void addref() = 0;
	virtual void delref() = 0;

	virtual std::string describe() = 0;
	virtual UID getDebugID() = 0;

	virtual void toCoreState( DBCoreState& ) = 0;

	virtual Future<Void> onCoreStateChanged() = 0;
		// Returns if and when the output of toCoreState() would change (for example, when older logs can be discarded from the state)

	virtual void coreStateWritten( DBCoreState const& newState ) = 0;
	    // Called when a core state has been written to the coordinators

	virtual Future<Void> onError() = 0;
		// Never returns normally, but throws an error if the subsystem stops working

	//Future<Void> push( UID bundle, int64_t seq, VectorRef<TaggedMessageRef> messages );
	virtual Future<Void> push( Version prevVersion, Version version, Version knownCommittedVersion, struct LogPushData& data, Optional<UID> debugID = Optional<UID>() ) = 0;
		// Waits for the version number of the bundle (in this epoch) to be prevVersion (i.e. for all pushes ordered earlier)
		// Puts the given messages into the bundle, each with the given tags, and with message versions (version, 0) - (version, N)
		// Changes the version number of the bundle to be version (unblocking the next push)
		// Returns when the preceding changes are durable.  (Later we will need multiple return signals for diffferent durability levels)
		// If the current epoch has ended, push will not return, and the pushed messages will not be visible in any subsequent epoch (but may become visible in this epoch)

	//Future<PeekResults> peek( int64_t begin_epoch, int64_t begin_seq, int tag );
	virtual Reference<IPeekCursor> peek( Version begin, Tag tag, bool parallelGetMore = false ) = 0;
		// Returns (via cursor interface) a stream of messages with the given tag and message versions >= (begin, 0), ordered by message version
		// If pop was previously or concurrently called with upTo > begin, the cursor may not return all such messages.  In that case cursor->popped() will
		// be greater than begin to reflect that.

	virtual Reference<IPeekCursor> peekSingle( Version begin, Tag tag ) = 0;
		// Same contract as peek(), but blocks until the preferred log server(s) for the given tag are available (and is correspondingly less expensive)

	virtual void pop( Version upTo, Tag tag ) = 0;
		// Permits, but does not require, the log subsystem to strip `tag` from any or all messages with message versions < (upTo,0)
		// The popping of any given message may be arbitrarily delayed.

	virtual Future<Void> confirmEpochLive( Optional<UID> debugID = Optional<UID>() ) = 0;
		// Returns success after confirming that pushes in the current epoch are still possible

	static Reference<ILogSystem> fromServerDBInfo( UID const& dbgid, struct ServerDBInfo const& db );
	static Reference<ILogSystem> fromLogSystemConfig( UID const& dbgid, struct LocalityData const&, struct LogSystemConfig const& );
		// Constructs a new ILogSystem implementation from the given ServerDBInfo/LogSystemConfig.  Might return a null reference if there isn't a fully recovered log system available.
		// The caller can peek() the returned log system and can push() if it has version numbers reserved for it and prevVersions

	static Reference<ILogSystem> fromOldLogSystemConfig( UID const& dbgid, struct LocalityData const&, struct LogSystemConfig const& );
		// Constructs a new ILogSystem implementation from the old log data within a ServerDBInfo/LogSystemConfig.  Might return a null reference if there isn't a fully recovered log system available.

	static Future<Void> recoverAndEndEpoch(Reference<AsyncVar<Reference<ILogSystem>>> const& outLogSystem, UID const& dbgid, DBCoreState const& oldState, FutureStream<TLogRejoinRequest> const& rejoins, LocalityData const& locality);
		// Constructs a new ILogSystem implementation based on the given oldState and rejoining log servers
		// Ensures that any calls to push or confirmEpochLive in the current epoch but strictly later than change_epoch will not return
		// Whenever changes in the set of available log servers require restarting recovery with a different end sequence, outLogSystem will be changed to a new ILogSystem

	virtual Version getEnd() = 0;
		// Call only on an ILogSystem obtained from recoverAndEndEpoch()
		// Returns the first unreadable version number of the recovered epoch (i.e. message version numbers < (get_end(), 0) will be readable)

	virtual Future<Reference<ILogSystem>> newEpoch( vector<WorkerInterface> availableLogServers, DatabaseConfiguration const& config, LogEpoch recoveryCount ) = 0;
		// Call only on an ILogSystem obtained from recoverAndEndEpoch()
		// Returns an ILogSystem representing a new epoch immediately following this one.  The new epoch is only provisional until the caller updates the coordinated DBCoreState

	virtual LogSystemConfig getLogSystemConfig() = 0;
		// Returns the physical configuration of this LogSystem, that could be used to construct an equivalent LogSystem using fromLogSystemConfig()

	virtual Standalone<StringRef> getLogsValue() = 0;

	virtual Future<Void> onLogSystemConfigChange() = 0;
		// Returns when the log system configuration has changed due to a tlog rejoin.

	virtual int getLogServerCount() = 0;
		// Used by LogPushData; returns the number of log servers

	virtual void getPushLocations( std::vector<Tag> const& tags, vector<int>& locations ) = 0;

	virtual void stopRejoins() = 0;
};

struct LogPushData : NonCopyable {
	// Log subsequences have to start at 1 (the MergedPeekCursor relies on this to make sure we never have !hasMessage() in the middle of data for a version

	explicit LogPushData(Reference<ILogSystem> logSystem) : logSystem(logSystem), subsequence(1) {
		tags.resize( logSystem->getLogSystemConfig().tLogs.size() );
		for(int i = 0; i < tags.size(); i++) {
			messagesWriter.push_back( BinaryWriter( AssumeVersion(currentProtocolVersion) ) );
		}
	}

	// addTag() adds a tag for the *next* message to be added
	void addTag( Tag tag ) {
		next_message_tags.push_back( tag );
	}

	void addMessage( StringRef rawMessageWithoutLength, bool usePreviousLocations = false ) {
		if( !usePreviousLocations ) {
			msg_locations.clear();
			logSystem->getPushLocations( next_message_tags, msg_locations );
		}
		uint32_t subseq = this->subsequence++;
		for(int loc : msg_locations) {
			for(auto& tag : next_message_tags)
				addTagToLoc( tag, loc );

			messagesWriter[loc] << uint32_t(rawMessageWithoutLength.size() + sizeof(subseq)) << subseq;
			messagesWriter[loc].serializeBytes(rawMessageWithoutLength);
		}
		next_message_tags.clear();
	}

	template <class T>
	void addTypedMessage( T const& item ) {
		msg_locations.clear();
		logSystem->getPushLocations( next_message_tags, msg_locations );
		uint32_t subseq = this->subsequence++;
		for(int loc : msg_locations) {
			for(auto& tag : next_message_tags)
				addTagToLoc( tag, loc );

			// FIXME: memcpy after the first time
			BinaryWriter& wr = messagesWriter[loc];
			int offset = wr.getLength();
			wr << uint32_t(0) << subseq << item;
			*(uint32_t*)((uint8_t*)wr.getData() + offset) = wr.getLength() - offset - sizeof(uint32_t);
		}
		next_message_tags.clear();
	}

	Arena getArena() { return arena; }
	StringRef getMessages(int loc) {
		return StringRef( arena, messagesWriter[loc].toStringRef() );  // FIXME: Unnecessary copy!
	}
	VectorRef<TagMessagesRef> getTags(int loc) {
		VectorRef<TagMessagesRef> r;
		for(auto& t : tags[loc])
			r.push_back( arena, t.value );
		return r;
	}

private:
	void addTagToLoc( Tag tag, int loc ) {
		auto it = tags[loc].find(tag);
		if (it == tags[loc].end()) {
			it = tags[loc].insert(mapPair( tag, TagMessagesRef() ));
			it->value.tag = it->key;
		}
		it->value.messageOffsets.push_back( arena, messagesWriter[loc].getLength() );
	}

	Reference<ILogSystem> logSystem;
	Arena arena;
	vector<Tag> next_message_tags;
	vector<Map<Tag, TagMessagesRef>> tags;
	vector<BinaryWriter> messagesWriter;
	vector<int> msg_locations;
	uint32_t subsequence;
};

#endif
