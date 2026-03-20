/*
 * BackupWorkerRangePartitioned.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/JsonBuilder.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tracing.h"
#include "BackupPartitionMap.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/LogSystem.h"
#include "fdbserver/logsystem/LogSystemFactory.h"
#include "fdbserver/core/WaitFailure.h"
#include "flow/CoroUtils.h"

#define SevDebugMemory SevVerbose

struct RangePartitionedVersionedMessage {
	LogMessageVersion version;
	StringRef message;
	VectorRef<Tag> tags;
	Arena arena;

	RangePartitionedVersionedMessage(LogMessageVersion v, StringRef m, const VectorRef<Tag>& t, const Arena& a)
	  : version(v), message(m), tags(t), arena(a) {}

	Version getVersion() const { return version.version; }
	size_t getEstimatedSize() const { return message.size() + TagsAndMessage::getHeaderSize(6); }
};

struct BackupRangePartitionedData {
	const UID myId;
	const Tag tag; // tag for this backup worker
	const int totalTags; // Total backup worker tags
	const Version startVersion; // This worker's start version
	const Optional<Version> endVersion; // old epoch's end version (inclusive), or empty for current epoch
	const LogEpoch recruitedEpoch; // current epoch whose tLogs are receiving mutations
	const LogEpoch backupEpoch; // the epoch workers should pull mutations
	Version minKnownCommittedVersion;
	Version savedVersion; // Largest version saved to blob storage
	NotifiedVersion pulledVersion;
	Version logFolderBaseVersion;
	AsyncVar<Reference<ILogSystem>> logSystem;
	AsyncVar<bool> paused; // Track if "backupPausedKey" is set.
	Reference<FlowLock> lock;
	AsyncTrigger doneTrigger;
	Database cx;
	std::vector<RangePartitionedVersionedMessage> messages;
	// Key range to partition ID map, used to determine which partition a mutation belongs to based on its key.
	KeyRangeMap<int> keyRangeToPartitionId;

	struct PerBackupInfo {
		PerBackupInfo() = default;
		PerBackupInfo(BackupRangePartitionedData* data, UID uid, Version v) : self(data), startVersion(v) {
			// Open the container and get the key ranges.
			BackupConfig config(uid);
			container = config.backupContainer().get(data->cx.getReference());
			ranges = config.backupRanges().get(data->cx.getReference());
			TraceEvent("BWRangePartitionedAddBackup", data->myId).detail("BackupID", uid).detail("Version", v);
		}

		BackupRangePartitionedData* self = nullptr;
		Future<Optional<std::vector<KeyRange>>> ranges; // Key ranges of this backup
		Future<Optional<Reference<IBackupContainer>>> container;
		// Backup request's commit version. Mutations are logged at some version after this.
		Version startVersion = invalidVersion;
		bool stopped = false;
	};

	// TODO akanksha: Add backups in this map when backup worker receives backup request.
	std::unordered_map<UID, PerBackupInfo> backups; // Backup UID to infos

	explicit BackupRangePartitionedData(UID id,
	                                    Reference<AsyncVar<ServerDBInfo> const> db,
	                                    const InitializeBackupRequest& req)
	  : myId(id), tag(req.routerTag), totalTags(req.totalTags), startVersion(req.startVersion),
	    endVersion(req.endVersion), recruitedEpoch(req.recruitedEpoch), backupEpoch(req.backupEpoch),
	    minKnownCommittedVersion(invalidVersion), savedVersion(req.startVersion - 1), pulledVersion(0),
	    logFolderBaseVersion(invalidVersion), paused(false),
	    lock(new FlowLock(SERVER_KNOBS->BACKUP_WORKER_LOCK_BYTES)) {
		cx = openDBOnServer(db, TaskPriority::DefaultEndpoint, LockAware::True);
	}

	bool pullFinished() const { return endVersion.present() && pulledVersion.get() > endVersion.get(); }

	void eraseMessagesAfterEndVersion() {
		ASSERT(endVersion.present());
		const Version ver = endVersion.get();
		while (!messages.empty()) {
			if (messages.back().getVersion() > ver) {
				size_t bytes = messages.back().getEstimatedSize();
				TraceEvent(SevDebugMemory, "BWRangePartitionedMemory", myId).detail("Release", bytes);
				lock->release(bytes);
				messages.pop_back();
			} else {
				break;
			}
		}
	}

	void pop() {
		if (!logSystem.get()) {
			return;
		}
		logSystem.get()->pop(savedVersion, tag);
	}
};

Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo> const> db,
                          LogEpoch recoveryCount,
                          BackupRangePartitionedData* self) {
	while (true) {
		bool isDisplaced =
		    db->get().recoveryCount > recoveryCount && db->get().recoveryState != RecoveryState::UNINITIALIZED;
		if (isDisplaced) {
			TraceEvent("BWRangePartitionedDisplaced", self->myId)
			    .detail("RecoveryCount", recoveryCount)
			    .detail("RecoveryState", (int)db->get().recoveryState);
			throw worker_removed();
		}
		co_await db->onChange();
	}
}

Future<Void> backupWorkerRangePartitioned(BackupInterface interf,
                                          InitializeBackupRequest req,
                                          Reference<AsyncVar<ServerDBInfo> const> db) {
	BackupRangePartitionedData self(interf.id(), db, req);
	PromiseStream<Future<Void>> addActor;
	Future<Void> error = actorCollection(addActor.getFuture());
	Future<Void> dbInfoChange = Void();
	Future<Void> done;
	Error err;

	TraceEvent("BWRangePartitionedStart", self.myId)
	    .detail("Tag", req.routerTag.toString())
	    .detail("TotalTags", req.totalTags)
	    .detail("StartVersion", req.startVersion)
	    .detail("EndVersion", req.endVersion.present() ? req.endVersion.get() : -1)
	    .detail("LogEpoch", req.recruitedEpoch)
	    .detail("BackupEpoch", req.backupEpoch);

	try {
		addActor.send(checkRemoved(db, req.recruitedEpoch, &self));
		addActor.send(waitFailureServer(interf.waitFailure.getFuture()));

		while (true) {
			auto res = co_await race(dbInfoChange, done, error);
			if (res.index() == 0) {
				dbInfoChange = db->onChange();
				[[maybe_unused]] Reference<ILogSystem> ls = makeLogSystemFromServerDBInfo(self.myId, db->get(), true);
			} else if (res.index() == 1) {
				TraceEvent("BWRangePartitionedDone", self.myId).detail("BackupEpoch", self.backupEpoch);
				// Notify master so that this worker can be removed from log system, then this
				// worker (for an old epoch's unfinished work) can safely exit.
				co_await brokenPromiseToNever(db->get().clusterInterface.notifyBackupWorkerDone.getReply(
				    BackupWorkerDoneRequest(self.myId, self.backupEpoch)));
				break;
			} else if (res.index() != 2) {
				UNREACHABLE();
			}
		}
		co_return;
	} catch (Error& e) {
		err = e;
	}

	if (err.code() == error_code_worker_removed) {
		try {
			co_await done;
		} catch (Error& shutdownErr) {
			TraceEvent("BWRangePartitionedShutdownError", self.myId).errorUnsuppressed(shutdownErr);
		}
	}
	TraceEvent("BWRangePartitionedTerminated", self.myId).errorUnsuppressed(err);
	if (err.code() != error_code_actor_cancelled && err.code() != error_code_worker_removed) {
		throw err;
	}
}

Future<Version> pullPartitionMapFromTLog(BackupRangePartitionedData* self, PartitionMap* outPartitionMap) {
	Reference<ILogSystem::IPeekCursor> cursor;
	Version partitionMapVersion = invalidVersion;
	Future<Void> logSystemChange = Void();

	while (true) {
		while (true) {
			auto res = co_await race(cursor ? cursor->getMore() : Never(), logSystemChange);
			if (res.index() == 0) {
				break;
			} else if (res.index() == 1) {
				if (self->logSystem.get()) {
					cursor = self->logSystem.get()->peekSingle(self->myId, self->startVersion, self->tag);
				} else {
					cursor = Reference<ILogSystem::IPeekCursor>();
				}
				logSystemChange = self->logSystem.onChange();
			} else {
				UNREACHABLE();
			}
		}
		co_await cursor->getMore();
		if (!cursor->hasMessage()) {
			continue;
		}
		for (; cursor->hasMessage(); cursor->nextMessage()) {
			Version msgVersion = cursor->version().version;
			StringRef message = cursor->getMessage();
			Arena arena = cursor->arena();
			ArenaReader reader(arena, message, AssumeVersion(g_network->protocolVersion()));
			if (reader.protocolVersion().hasSpanContext() && SpanContextMessage::isNextIn(reader)) {
				cursor->nextMessage();
				continue;
			}
			if (reader.protocolVersion().hasOTELSpanContext() && OTELSpanContextMessage::isNextIn(reader)) {
				cursor->nextMessage();
				continue;
			}
			// TODO akanksha: Uncomment once PartitionMapMessage is implemented.
			// bool isPartitionMap = PartitionMapMessage::isNextIn(reader);
			bool isPartitionMap = true;
			if (!isPartitionMap) {
				TraceEvent(SevError, "BWRangeParitionedPartitionMapNotReceived", self->myId)
				    .detail("Version", msgVersion)
				    .detail("Tag", self->tag.toString())
				    .detail("MessageSize", message.size());
				throw worker_removed();
			}

			// TODO akanksha: 1. std::unordered_map is not supported by ArenaReader right now, need to implement custom
			// deserialization logic for it. Or we can switch to std::map which is supported by ArenaReader.
			// 2. Uncomment and update the code once the deserialization logic is implemented.
			/*
			PartitionMapMessage pmMsg;
			reader >> pmMsg;
			*outPartitionMap  = pmMsg.partitionMap;
			*/
			partitionMapVersion = msgVersion;
			co_return partitionMapVersion;
		}
	}
}

Future<Void> uploadPartitionList(BackupRangePartitionedData* self, PartitionMap partitionMap) {
	std::vector<Future<Void>> fileFutures;
	auto it = self->backups.begin();

	std::string jsonContent = serializePartitionListJSON(partitionMap);

	for (; it != self->backups.end();) {
		if (it->second.stopped || !it->second.container.get().present()) {
			TraceEvent("BWRangePartitionedRemoveContainer", self->myId).detail("BackupId", it->first);
			it = self->backups.erase(it);
			continue;
		}
		Reference<IBackupContainer> container = it->second.container.get().get();
		fileFutures.push_back(container->writePartitionListFile(self->logFolderBaseVersion, jsonContent));
		it++;
	}
	if (fileFutures.empty()) {
		TraceEvent("BWRangePartitionedNoContainers", self->myId);
		co_return;
	}

	co_await waitForAll(fileFutures);
}

// TODO akanksha -> Need to figure out if
// 1. For new requests -> PartitionMap in TLOG will be same for all containers
// 2. For older epochs with different containers is PartitionMap specific to container or same for all.
// Right now assumption is that PartitionMap will be passed by TLOG with the first message after start version for both
// older epochs and newer epochs.
Future<Void> waitAndProcessPartitionMap(BackupRangePartitionedData* self) {
	TraceEvent("BWRangeParitionedWaitingForPartitionMap", self->myId)
	    .detail("Tag", self->tag.toString())
	    .detail("StartVersion", self->startVersion);
	PartitionMap partitionMap;

	Version partitionMapVersion = co_await pullPartitionMapFromTLog(self, &partitionMap);
	self->logFolderBaseVersion = partitionMapVersion + 1;

	ASSERT(partitionMap.find(self->tag) != partitionMap.end());
	TraceEvent("BWRangeParitionedPulledPartitionMap", self->myId)
	    .detail("Version", partitionMapVersion)
	    .detail("NumTags", partitionMap.size())
	    .detail("Tag", self->tag.toString())
	    .detail("NumPartitions", partitionMap[self->tag].size());

	self->keyRangeToPartitionId.clear();
	ASSERT_GT(partitionMap[self->tag].size(), 0);
	for (auto& partition : partitionMap[self->tag]) {
		self->keyRangeToPartitionId.insert(partition.ranges, partition.partitionId);
	}

	Key doneKey = backupRangePartitionedMapUploadedKeyFor(partitionMapVersion);
	ReadYourWritesTransaction tr(self->cx);

	// TODO akanksha: Check what will be the tag id once tags are implemented for backup workers and update the
	// condition accordingly.
	// Also add background actor to clean up the done key at regular
	if (self->tag.id == 0) {
		co_await uploadPartitionList(self, partitionMap);
		TraceEvent("BWRangePartitionedPartitionMapUploaded", self->myId)
		    .detail("Version", partitionMapVersion)
		    .detail("NumBackups", self->backups.size());
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.set(doneKey, "1"_sr);
				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	} else {
		// All other backup workers waits for done key to be set by the worker with tag id 0, then start pulling
		// mutations. This is to make sure partition map is uploaded before any worker starts pulling mutations.
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE); // More efficient for reads
				Optional<Value> v = co_await tr.get(doneKey);
				if (v.present()) {
					break;
				}
				Future<Void> watchFuture = tr.watch(doneKey);
				co_await tr.commit();
				co_await watchFuture;
				tr.reset();
				continue;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}
	self->pulledVersion.set(partitionMapVersion);
	self->savedVersion = partitionMapVersion;
	self->pop();
}

// Pulls mutations from TLog servers.
Future<Void> pullAsyncData(BackupRangePartitionedData* self) {
	Future<Void> logSystemChange = Void();
	Reference<ILogSystem::IPeekCursor> cursor;

	Version tagAt = std::max({ self->pulledVersion.get(), self->startVersion, self->savedVersion });

	TraceEvent("BWRangePartitionedPull", self->myId)
	    .detail("Tag", self->tag)
	    .detail("Version", tagAt)
	    .detail("StartVersion", self->startVersion)
	    .detail("SavedVersion", self->savedVersion);

	while (true) {
		while (self->paused.get()) {
			co_await self->paused.onChange();
		}

		while (true) {
			auto res = co_await race(cursor ? cursor->getMore(TaskPriority::TLogCommit) : Never(), logSystemChange);
			if (res.index() == 0) {
				DisabledTraceEvent("BWRangePartitionedGotMore", self->myId)
				    .detail("Tag", self->tag)
				    .detail("CursorVersion", cursor->version().version);
				break;
			} else if (res.index() == 1) {
				if (self->logSystem.get()) {
					// TODO akanksha: Use peekSingle as of now instead of peekLogRouter and later confirm if it works as
					// expected.
					cursor = self->logSystem.get()->peekSingle(self->myId, tagAt, self->tag);
				} else {
					cursor = Reference<ILogSystem::IPeekCursor>();
				}
				logSystemChange = self->logSystem.onChange();
			} else {
				UNREACHABLE();
			}
		}

		if (cursor->popped() > 0) {
			TraceEvent(SevError, "BWRangePartitionedDataPopped", self->myId)
			    .detail("Popped", cursor->popped())
			    .detail("Expected", tagAt);
			throw worker_removed();
		}

		self->minKnownCommittedVersion =
		    std::max(self->minKnownCommittedVersion, cursor->getMinKnownCommittedVersion());
		int64_t peekedBytes = 0;

		// Hold messages until we know how many we can take, self->messages always
		// contains messages that we have reserved memory for. Therefore, lock->release()
		// will always encounter message with reserved memory.
		std::vector<RangePartitionedVersionedMessage> tmpMessages;

		// Messages may be prefetched in peek here, but uncommitted messages should not be uploaded in uploadData().
		while (cursor->hasMessage()) {
			auto msg = RangePartitionedVersionedMessage(
			    cursor->version(), cursor->getMessage(), cursor->getTags(), cursor->arena());
			tmpMessages.emplace_back(std::move(msg));
			peekedBytes += tmpMessages.back().getEstimatedSize();
			cursor->nextMessage();
		}

		if (peekedBytes > 0) {
			TraceEvent(SevDebugMemory, "BWRangePartitionedMemory", self->myId)
			    .detail("Take", peekedBytes)
			    .detail("Current", self->lock->activePermits());
			co_await self->lock->take(TaskPriority::DefaultYield, peekedBytes);
			self->messages.insert(self->messages.end(),
			                      std::make_move_iterator(tmpMessages.begin()),
			                      std::make_move_iterator(tmpMessages.end()));
		}

		tagAt = cursor->version().version;
		self->pulledVersion.set(tagAt);
		TraceEvent("BWRangePartitionedGot", self->myId).suppressFor(1.0).detail("LatestPulledVersion", tagAt);

		// For older epochs, we may have an end version to stop at.
		if (self->pullFinished()) {
			self->eraseMessagesAfterEndVersion();
			self->doneTrigger.trigger();
			TraceEvent("BWRangePartitionedFinishPull", self->myId)
			    .detail("Tag", self->tag.toString())
			    .detail("VersionGot", tagAt)
			    .detail("EndVersion", self->endVersion.get())
			    .detail("LogEpoch", self->recruitedEpoch)
			    .detail("BackupEpoch", self->backupEpoch);
			co_return;
		}
		co_await yield();
	}
}
