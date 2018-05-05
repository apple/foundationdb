/*
 * LogSystemDiskQueueAdapter.h
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

#ifndef FDBSERVER_LOGSYSTEMDISKQUEUEADAPTER_H
#define FDBSERVER_LOGSYSTEMDISKQUEUEADAPTER_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "IDiskQueue.h"

class LogSystemDiskQueueAdapter : public IDiskQueue {
public:
	// This adapter is designed to let KeyValueStoreMemory use ILogSystem
	// as a backing store, so that the transaction subsystem can in
	// turn use KeyValueStoreMemory to track configuration information as of
	// the database version and recover it from the logging subsystem as necessary.

	// Because the transaction subsystem will need to control the actual pushing of
	// committed information to the ILogSystem, commit() in this interface doesn't directly
	// call ILogSystem::push().  Instead it makes a commit message available through 
	// getCommitMessage(), and doesn't return until its acknowledge promise is set.
	// The caller is responsible for calling ILogSystem::push() and ILogSystem::pop() with the results.

	// It does, however, peek the specified tag directly at recovery time.

	LogSystemDiskQueueAdapter( Reference<ILogSystem> logSystem, Tag tag, bool recover=true ) : logSystem(logSystem), tag(tag), enableRecovery(recover), recoveryLoc(1), recoveryQueueLoc(1), poppedUpTo(0), nextCommit(1), recoveryQueueDataSize(0) {
		if (enableRecovery)
			cursor = logSystem->peek( 0, tag, true );
	}

	struct CommitMessage {
		Standalone<VectorRef<VectorRef<uint8_t>>> messages;    // push this into the logSystem with `tag`
		Version popTo;                                         // pop this from the logSystem with `tag`
		Promise<Void> acknowledge;                             // then send Void to this, so commit() can return
	};

	// Set the version of the next push or commit (or a lower version)
	// If lower, locations returned by the IDiskQueue interface will be conservative, so things that could be popped might not be
	void setNextVersion( Version next ) { nextCommit = next; }

	// Return the next commit message resulting from a call to commit().
	Future<CommitMessage> getCommitMessage();

	// IClosable interface
	virtual Future<Void> getError();
	virtual Future<Void> onClosed();
	virtual void dispose();
	virtual void close();

	// IDiskQueue interface
	virtual Future<Standalone<StringRef>> readNext( int bytes );
	virtual IDiskQueue::location getNextReadLocation();
	virtual IDiskQueue::location push( StringRef contents );
	virtual void pop( IDiskQueue::location upTo );
	virtual Future<Void> commit();
	virtual StorageBytes getStorageBytes() { ASSERT(false); throw internal_error(); }
	virtual int getCommitOverhead() { return 0; } //SOMEDAY: could this be more accurate?

private:
	Reference<ILogSystem::IPeekCursor> cursor;
	Tag tag;

	// Recovery state (used while readNext() is being called repeatedly)
	bool enableRecovery;
	Reference<ILogSystem> logSystem;
	Version recoveryLoc, recoveryQueueLoc;
	std::vector<Standalone<StringRef>> recoveryQueue;
	int recoveryQueueDataSize;

	// State for next commit() call
	Standalone<VectorRef<VectorRef<uint8_t>>> pushedData;  // SOMEDAY: better representation?
	Version poppedUpTo;
	std::deque< Promise<CommitMessage> > commitMessages;
	Version nextCommit;

	friend class LogSystemDiskQueueAdapterImpl;
};

LogSystemDiskQueueAdapter* openDiskQueueAdapter( Reference<ILogSystem> logSystem, Tag tag );

#endif