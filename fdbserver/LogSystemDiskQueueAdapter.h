/*
 * LogSystemDiskQueueAdapter.h
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

#ifndef FDBSERVER_LOGSYSTEMDISKQUEUEADAPTER_H
#define FDBSERVER_LOGSYSTEMDISKQUEUEADAPTER_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbserver/IDiskQueue.h"

struct PeekTxsInfo {
	int8_t primaryLocality;
	int8_t secondaryLocality;
	Version knownCommittedVersion;

	bool operator==(const PeekTxsInfo& r) const {
		return primaryLocality == r.primaryLocality && secondaryLocality == r.secondaryLocality &&
		       knownCommittedVersion == r.knownCommittedVersion;
	}
	bool operator!=(const PeekTxsInfo& r) const { return !(*this == r); }

	PeekTxsInfo(int8_t primaryLocality, int8_t secondaryLocality, Version knownCommittedVersion)
	  : primaryLocality(primaryLocality), secondaryLocality(secondaryLocality),
	    knownCommittedVersion(knownCommittedVersion) {}
};

class LogSystemDiskQueueAdapter final : public IDiskQueue {
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

	LogSystemDiskQueueAdapter(Reference<ILogSystem> logSystem,
	                          Reference<AsyncVar<PeekTxsInfo>> peekLocality,
	                          Version txsPoppedVersion,
	                          bool recover)
	  : peekLocality(peekLocality), peekTypeSwitches(0), enableRecovery(recover), logSystem(logSystem),
	    recoveryLoc(txsPoppedVersion), recoveryQueueLoc(txsPoppedVersion), recoveryQueueDataSize(0), poppedUpTo(0),
	    nextCommit(1), hasDiscardedData(false), totalRecoveredBytes(0) {
		if (enableRecovery) {
			localityChanged = peekLocality ? peekLocality->onChange() : Never();
			cursor = logSystem->peekTxs(UID(),
			                            txsPoppedVersion,
			                            peekLocality ? peekLocality->get().primaryLocality : tagLocalityInvalid,
			                            peekLocality ? peekLocality->get().knownCommittedVersion : invalidVersion,
			                            true);
		}
	}

	struct CommitMessage {
		Standalone<VectorRef<VectorRef<uint8_t>>> messages; // push this into the logSystem with `tag`
		Version popTo; // pop this from the logSystem with `tag`
		Promise<Void> acknowledge; // then send Void to this, so commit() can return
	};

	// Set the version of the next push or commit (or a lower version)
	// If lower, locations returned by the IDiskQueue interface will be conservative, so things that could be popped
	// might not be
	void setNextVersion(Version next) { nextCommit = next; }

	// Return the next commit message resulting from a call to commit().
	Future<CommitMessage> getCommitMessage();

	// IClosable interface
	Future<Void> getError() const override;
	Future<Void> onClosed() const override;
	void dispose() override;
	void close() override;

	// IDiskQueue interface
	Future<bool> initializeRecovery(location recoverAt) override { return false; }
	Future<Standalone<StringRef>> readNext(int bytes) override;
	IDiskQueue::location getNextReadLocation() const override;
	IDiskQueue::location getNextCommitLocation() const override {
		ASSERT(false);
		throw internal_error();
	}
	IDiskQueue::location getNextPushLocation() const override {
		ASSERT(false);
		throw internal_error();
	}
	Future<Standalone<StringRef>> read(location start, location end, CheckHashes ch) override {
		ASSERT(false);
		throw internal_error();
	}
	IDiskQueue::location push(StringRef contents) override;
	void pop(IDiskQueue::location upTo) override;
	Future<Void> commit() override;
	StorageBytes getStorageBytes() const override {
		ASSERT(false);
		throw internal_error();
	}
	int getCommitOverhead() const override { return 0; } // SOMEDAY: could this be more accurate?

private:
	Reference<AsyncVar<PeekTxsInfo>> peekLocality;
	Future<Void> localityChanged;
	Reference<ILogSystem::IPeekCursor> cursor;
	int peekTypeSwitches;

	// Recovery state (used while readNext() is being called repeatedly)
	bool enableRecovery;
	Reference<ILogSystem> logSystem;
	Version recoveryLoc, recoveryQueueLoc;
	std::vector<Standalone<StringRef>> recoveryQueue;
	int recoveryQueueDataSize;

	// State for next commit() call
	Standalone<VectorRef<VectorRef<uint8_t>>> pushedData; // SOMEDAY: better representation?
	Version poppedUpTo;
	std::deque<Promise<CommitMessage>> commitMessages;
	Version nextCommit;
	bool hasDiscardedData;
	int totalRecoveredBytes;

	friend class LogSystemDiskQueueAdapterImpl;
};

LogSystemDiskQueueAdapter* openDiskQueueAdapter(Reference<ILogSystem> logSystem,
                                                Reference<AsyncVar<PeekTxsInfo>> peekLocality,
                                                Version txsPoppedVersion);

#endif
