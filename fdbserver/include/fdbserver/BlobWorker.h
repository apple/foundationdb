/*
 * BlobWorker.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_BLOBWORKER_H
#define FDBSERVER_BLOBWORKER_H

#include "fdbclient/BlobWorkerCommon.h"

#include "fdbserver/BlobGranuleServerCommon.actor.h"
#include "fdbserver/Knobs.h"

#include <vector>

#include "flow/actorcompiler.h" // has to be last include

struct GranuleStartState {
	UID granuleID;
	Version changeFeedStartVersion;
	Version previousDurableVersion;
	Optional<std::pair<KeyRange, UID>> splitParentGranule;
	bool doSnapshot;
	std::vector<GranuleFiles> blobFilesToSnapshot;
	Optional<GranuleFiles> existingFiles;
	Optional<GranuleHistory> history;
};

// TODO: add more (blob file request cost, in-memory mutations vs blob delta file, etc...)
struct GranuleReadStats {
	int64_t deltaBytesRead;

	void reset() { deltaBytesRead = 0; }

	GranuleReadStats() { reset(); }
};

struct GranuleMetadata : NonCopyable, ReferenceCounted<GranuleMetadata> {
	KeyRange keyRange;

	GranuleFiles files;
	Standalone<GranuleDeltas>
	    currentDeltas; // only contain deltas in pendingDeltaVersion + 1 through bufferedDeltaVersion

	uint64_t bytesInNewDeltaFiles = 0;
	uint64_t bufferedDeltaBytes = 0;
	uint16_t newDeltaFileCount = 0;

	// for client to know when it is safe to read a certain version and from where (check waitForVersion)
	Version bufferedDeltaVersion; // largest delta version in currentDeltas (including empty versions)
	Version pendingDeltaVersion = 0; // largest version in progress writing to s3/fdb
	NotifiedVersion durableDeltaVersion; // largest version persisted in s3/fdb
	NotifiedVersion durableSnapshotVersion; // same as delta vars, except for snapshots
	Version pendingSnapshotVersion = 0;
	Version initialSnapshotVersion = invalidVersion;
	Version historyVersion = invalidVersion;
	Version knownCommittedVersion;
	NotifiedVersion forceFlushVersion; // Version to force a flush at, if necessary
	Version forceCompactVersion = invalidVersion;

	int64_t originalEpoch;
	int64_t originalSeqno;
	int64_t continueEpoch;
	int64_t continueSeqno;

	Promise<Void> cancelled;
	Promise<Void> readable;
	Promise<Void> historyLoaded;

	Promise<Void> resumeSnapshot;

	AsyncVar<Reference<ChangeFeedData>> activeCFData;

	AssignBlobRangeRequest originalReq;

	GranuleReadStats readStats;
	bool rdcCandidate;
	Promise<Void> runRDC;

	void resume();
	void resetReadStats();

	// determine eligibility (>1) and priority for re-snapshotting this granule
	double weightRDC();

	bool isEligibleRDC() const;

	bool updateReadStats(Version readVersion, const BlobGranuleChunkRef& chunk);

	inline bool doEarlyReSnapshot() {
		return runRDC.isSet() ||
		       (forceCompactVersion <= pendingDeltaVersion && forceCompactVersion > pendingSnapshotVersion);
	}
};

struct GranuleRangeMetadata {
	int64_t lastEpoch;
	int64_t lastSeqno;
	Reference<GranuleMetadata> activeMetadata;

	Future<GranuleStartState> assignFuture;
	Future<Void> fileUpdaterFuture;
	Future<Void> historyLoaderFuture;

	void cancel();
	GranuleRangeMetadata() : lastEpoch(0), lastSeqno(0) {}
	GranuleRangeMetadata(int64_t epoch, int64_t seqno, Reference<GranuleMetadata> activeMetadata)
	  : lastEpoch(epoch), lastSeqno(seqno), activeMetadata(activeMetadata) {}
};

// represents a previous version of a granule, and optionally the files that compose it.
struct GranuleHistoryEntry : NonCopyable, ReferenceCounted<GranuleHistoryEntry> {
	KeyRange range;
	UID granuleID;
	Version startVersion; // version of the first snapshot
	Version endVersion; // version of the last delta file

	// load files lazily, and allows for clearing old cold-queried files to save memory
	// FIXME: add memory limit and evictor for old cached files
	Future<GranuleFiles> files;

	// FIXME: do skip pointers with single back-pointer and neighbor pointers
	std::vector<Reference<GranuleHistoryEntry>> parentGranules;

	GranuleHistoryEntry() : startVersion(invalidVersion), endVersion(invalidVersion) {}
	GranuleHistoryEntry(KeyRange range, UID granuleID, Version startVersion, Version endVersion)
	  : range(range), granuleID(granuleID), startVersion(startVersion), endVersion(endVersion) {}
};

struct BlobWorkerData : NonCopyable, ReferenceCounted<BlobWorkerData> {
	UID id;
	Database db;
	IKeyValueStore* storage;

	PromiseStream<Future<Void>> addActor;

	LocalityData locality;
	int64_t currentManagerEpoch = -1;

	AsyncVar<ReplyPromiseStream<GranuleStatusReply>> currentManagerStatusStream;
	bool statusStreamInitialized = false;

	// FIXME: refactor out the parts of this that are just for interacting with blob stores from the backup business
	// logic
	Reference<BlobConnectionProvider> bstore;
	KeyRangeMap<GranuleRangeMetadata> granuleMetadata;
	BGTenantMap tenantData;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;

	// contains the history of completed granules before the existing ones. Maps to the latest one, and has
	// back-pointers to earlier granules
	// FIXME: expire from map after a delay when granule is revoked and the history is no longer needed
	KeyRangeMap<Reference<GranuleHistoryEntry>> granuleHistory;

	PromiseStream<AssignBlobRangeRequest> granuleUpdateErrors;

	Promise<Void> doGRVCheck;
	NotifiedVersion grvVersion;
	std::deque<Version> prevGRVVersions;
	Promise<Void> fatalError;
	Promise<Void> simInjectFailure;
	Promise<Void> doReadDrivenCompaction;

	Reference<FlowLock> initialSnapshotLock;
	Reference<FlowLock> resnapshotBudget;
	Reference<FlowLock> deltaWritesBudget;

	BlobWorkerStats stats;

	bool shuttingDown = false;

	// FIXME: have cap on this independent of delta file size for larger granules
	int changeFeedStreamReplyBufferSize = SERVER_KNOBS->BG_DELTA_FILE_TARGET_BYTES / 4;

	Optional<EncryptionAtRestMode> persistedEncryptMode;
	EncryptionAtRestMode encryptMode;
	bool buggifyFull = false;

	int64_t memoryFullThreshold =
	    (int64_t)(SERVER_KNOBS->BLOB_WORKER_REJECT_WHEN_FULL_THRESHOLD * SERVER_KNOBS->SERVER_MEM_LIMIT);
	int64_t lastResidentMemory = 0;
	double lastResidentMemoryCheckTime = -100.0;

	bool isFullRestoreMode = false;

	BlobWorkerData(UID id, Reference<AsyncVar<ServerDBInfo> const> dbInfo, Database db, IKeyValueStore* storage)
	  : id(id), db(db), storage(storage), tenantData(BGTenantMap(dbInfo)), dbInfo(dbInfo),
	    initialSnapshotLock(new FlowLock(SERVER_KNOBS->BLOB_WORKER_INITIAL_SNAPSHOT_PARALLELISM)),
	    resnapshotBudget(new FlowLock(SERVER_KNOBS->BLOB_WORKER_RESNAPSHOT_BUDGET_BYTES)),
	    deltaWritesBudget(new FlowLock(SERVER_KNOBS->BLOB_WORKER_DELTA_WRITE_BUDGET_BYTES)),
	    stats(id,
	          SERVER_KNOBS->WORKER_LOGGING_INTERVAL,
	          initialSnapshotLock,
	          resnapshotBudget,
	          deltaWritesBudget,
	          SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	          SERVER_KNOBS->FILE_LATENCY_SKETCH_ACCURACY,
	          SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
	    encryptMode(EncryptionAtRestMode::DISABLED) {}

	bool managerEpochOk(int64_t epoch);
	bool isFull();
	void triggerReadDrivenCompaction();
	void addGRVHistory(Version readVersion);
	bool maybeInjectTargetedRestart();
};

#endif