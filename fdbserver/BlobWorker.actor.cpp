/*
 * BlobWorker.actor.cpp
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

#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/BlobCipher.h"
#include "fdbclient/BlobGranuleFiles.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BlobConnectionProvider.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/BlobGranuleReader.actor.h"
#include "fdbclient/BlobMetadataUtils.h"
#include "fdbclient/BlobWorkerCommon.h"
#include "fdbclient/BlobWorkerInterface.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Notified.h"

#include "fdbserver/BlobGranuleServerCommon.actor.h"
#include "fdbserver/EncryptionOpsUtils.h"
#include "fdbclient/GetEncryptCipherKeys.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/MutationTracking.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/WaitFailure.h"

#include "flow/Arena.h"
#include "flow/CompressionUtils.h"
#include "flow/EncryptUtils.h"
#include "flow/Error.h"
#include "flow/flow.h"
#include "flow/IRandom.h"
#include "flow/network.h"
#include "flow/Trace.h"
#include "flow/xxhash.h"

#include "fmt/format.h"

#include <limits>
#include <tuple>
#include <utility>
#include <vector>

#include "flow/actorcompiler.h" // has to be last include

#define BW_DEBUG false
#define BW_HISTORY_DEBUG false
#define BW_REQUEST_DEBUG false

/*
 * The Blob Worker is a stateless role assigned a set of granules by the Blob Manager.
 * It is responsible for managing the change feeds for those granules, and for consuming the mutations from
 * those change feeds and writing them out as files to blob storage.
 */

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

	void resume() {
		if (resumeSnapshot.canBeSet()) {
			resumeSnapshot.send(Void());
		}
	}

	void resetReadStats() {
		rdcCandidate = false;
		readStats.reset();
		runRDC.reset();
	}

	// determine eligibility (>1) and priority for re-snapshotting this granule
	double weightRDC() {
		// ratio of read amp to write amp that would be incurred by re-snapshotting now
		int64_t lastSnapshotSize = (files.snapshotFiles.empty()) ? 0 : files.snapshotFiles.back().length;
		int64_t minSnapshotSize = SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES / 2;
		lastSnapshotSize = std::max(minSnapshotSize, lastSnapshotSize);

		int64_t writeAmp = lastSnapshotSize + bufferedDeltaBytes + bytesInNewDeltaFiles;
		// read amp is deltaBytesRead. Read amp must be READ_FACTOR times larger than write amp
		return (1.0 * readStats.deltaBytesRead) / (writeAmp * SERVER_KNOBS->BG_RDC_READ_FACTOR);
	}

	bool isEligibleRDC() const {
		// granule should be reasonably read-hot to be eligible
		int64_t bytesWritten = bufferedDeltaBytes + bytesInNewDeltaFiles;
		return bytesWritten * SERVER_KNOBS->BG_RDC_READ_FACTOR < readStats.deltaBytesRead;
	}

	bool updateReadStats(Version readVersion, const BlobGranuleChunkRef& chunk) {
		// Only update stats for re-compacting for at-latest reads that have to do snapshot + delta merge
		if (!SERVER_KNOBS->BG_ENABLE_READ_DRIVEN_COMPACTION || !chunk.snapshotFile.present() ||
		    pendingSnapshotVersion != durableSnapshotVersion.get() || readVersion <= pendingSnapshotVersion) {
			return false;
		}

		if (chunk.newDeltas.empty() && chunk.deltaFiles.empty()) {
			return false;
		}

		readStats.deltaBytesRead += chunk.newDeltas.expectedSize();
		for (auto& it : chunk.deltaFiles) {
			readStats.deltaBytesRead += it.length;
		}

		if (rdcCandidate) {
			return false;
		}

		if (isEligibleRDC() && weightRDC() > 1.0) {
			rdcCandidate = true;
			CODE_PROBE(true, "Granule read triggering read-driven compaction");
			if (BW_DEBUG) {
				fmt::print("Triggering read-driven compaction of [{0} - {1})\n",
				           keyRange.begin.printable(),
				           keyRange.end.printable());
			}
			return true;
		}
		return false;
	}

	inline bool doReadDrivenCompaction() { return runRDC.isSet(); }
};

struct GranuleRangeMetadata {
	int64_t lastEpoch;
	int64_t lastSeqno;
	Reference<GranuleMetadata> activeMetadata;

	Future<GranuleStartState> assignFuture;
	Future<Void> fileUpdaterFuture;
	Future<Void> historyLoaderFuture;

	void cancel() {
		if (activeMetadata->cancelled.canBeSet()) {
			activeMetadata->cancelled.send(Void());
		}
		activeMetadata.clear();
		assignFuture.cancel();
		historyLoaderFuture.cancel();
		fileUpdaterFuture.cancel();
	}

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
	Promise<Void> fatalError;
	Promise<Void> simInjectFailure;
	Promise<Void> doReadDrivenCompaction;

	Reference<FlowLock> initialSnapshotLock;
	Reference<FlowLock> resnapshotLock;
	Reference<FlowLock> deltaWritesLock;

	BlobWorkerStats stats;

	bool shuttingDown = false;

	int changeFeedStreamReplyBufferSize = SERVER_KNOBS->BG_DELTA_FILE_TARGET_BYTES / 4;

	bool isEncryptionEnabled = false;
	bool buggifyFull = false;

	int64_t memoryFullThreshold =
	    (int64_t)(SERVER_KNOBS->BLOB_WORKER_REJECT_WHEN_FULL_THRESHOLD * SERVER_KNOBS->SERVER_MEM_LIMIT);
	int64_t lastResidentMemory = 0;
	double lastResidentMemoryCheckTime = -100.0;

	bool isFullRestoreMode = false;

	BlobWorkerData(UID id, Reference<AsyncVar<ServerDBInfo> const> dbInfo, Database db)
	  : id(id), db(db), tenantData(BGTenantMap(dbInfo)), dbInfo(dbInfo),
	    initialSnapshotLock(new FlowLock(SERVER_KNOBS->BLOB_WORKER_INITIAL_SNAPSHOT_PARALLELISM)),
	    resnapshotLock(new FlowLock(SERVER_KNOBS->BLOB_WORKER_RESNAPSHOT_PARALLELISM)),
	    deltaWritesLock(new FlowLock(SERVER_KNOBS->BLOB_WORKER_DELTA_FILE_WRITE_PARALLELISM)),
	    stats(id,
	          SERVER_KNOBS->WORKER_LOGGING_INTERVAL,
	          initialSnapshotLock,
	          resnapshotLock,
	          deltaWritesLock,
	          SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	          SERVER_KNOBS->FILE_LATENCY_SAMPLE_SIZE,
	          SERVER_KNOBS->LATENCY_SAMPLE_SIZE),
	    isEncryptionEnabled(isEncryptionOpSupported(EncryptOperationType::BLOB_GRANULE_ENCRYPTION)) {}

	bool managerEpochOk(int64_t epoch) {
		if (epoch < currentManagerEpoch) {
			if (BW_DEBUG) {
				fmt::print("BW {0} got request from old epoch {1}, notifying them they are out of date\n",
				           id.toString(),
				           epoch);
			}
			return false;
		} else {
			if (epoch > currentManagerEpoch) {
				currentManagerEpoch = epoch;
				if (BW_DEBUG) {
					fmt::print("BW {0} found new manager epoch {1}\n", id.toString(), currentManagerEpoch);
				}
				TraceEvent(SevDebug, "BlobWorkerFoundNewManager", id).detail("Epoch", epoch);
			}

			return true;
		}
	}

	bool isFull() {
		if (!SERVER_KNOBS->BLOB_WORKER_DO_REJECT_WHEN_FULL) {
			return false;
		}
		if (g_network->isSimulated()) {
			if (g_simulator->speedUpSimulation) {
				return false;
			}
			return buggifyFull;
		}

		// TODO knob?
		if (now() >= 1.0 + lastResidentMemoryCheckTime) {
			// fdb as of 7.1 limits on resident memory instead of virtual memory
			stats.lastResidentMemory = getResidentMemoryUsage();
			lastResidentMemoryCheckTime = now();
		}

		// if we are already over threshold, no need to estimate extra memory
		if (stats.lastResidentMemory >= memoryFullThreshold) {
			return true;
		}

		// FIXME: since this isn't tested in simulation, could unit test this
		// Try to model how much memory we *could* use given the already existing assignments and workload on this blob
		// worker, before agreeing to take on a new assignment, given that several large sources of memory can grow and
		// change post-assignment

		// estimate slack in bytes buffered as max(0, assignments * (delta file size / 2) - bytesBuffered)
		int64_t expectedExtraBytesBuffered = std::max(
		    0, stats.numRangesAssigned * (SERVER_KNOBS->BG_DELTA_FILE_TARGET_BYTES / 2) - stats.mutationBytesBuffered);
		// estimate slack in potential pending delta file writes
		int64_t maximumExtraDeltaWrite = SERVER_KNOBS->BG_DELTA_FILE_TARGET_BYTES * deltaWritesLock->available();
		// estimate slack in potential pending resnapshot
		int64_t maximumExtraReSnapshot =
		    (SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES + SERVER_KNOBS->BG_DELTA_BYTES_BEFORE_COMPACT) * 2 *
		    resnapshotLock->available();

		int64_t totalExtra = expectedExtraBytesBuffered + maximumExtraDeltaWrite + maximumExtraReSnapshot;
		// assumes initial snapshot parallelism is small enough and uncommon enough to not add it to this computation
		stats.estimatedMaxResidentMemory = stats.lastResidentMemory + totalExtra;

		return stats.estimatedMaxResidentMemory >= memoryFullThreshold;
	}

	void triggerReadDrivenCompaction() {
		Promise<Void> doRDC = doReadDrivenCompaction;
		if (doRDC.canBeSet()) {
			doRDC.send(Void());
		}
	}

	bool maybeInjectTargetedRestart() {
		// inject a BW restart at most once per test
		if (g_network->isSimulated() && !g_simulator->speedUpSimulation &&
		    now() > g_simulator->injectTargetedBWRestartTime) {
			CODE_PROBE(true, "Injecting BW targeted restart");
			TraceEvent("SimBWInjectTargetedRestart", id);
			g_simulator->injectTargetedBWRestartTime = std::numeric_limits<double>::max();
			simInjectFailure.send(Void());
			return true;
		}
		return false;
	}
};

namespace {

Optional<CompressionFilter> getBlobFileCompressFilter() {
	Optional<CompressionFilter> compFilter;
	if (SERVER_KNOBS->ENABLE_BLOB_GRANULE_COMPRESSION) {
		compFilter = CompressionUtils::fromFilterString(SERVER_KNOBS->BLOB_GRANULE_COMPRESSION_FILTER);
	}
	return compFilter;
}

// returns true if we can acquire it
void acquireGranuleLock(int64_t epoch, int64_t seqno, int64_t prevOwnerEpoch, int64_t prevOwnerSeqno) {
	// returns true if our lock (E, S) >= (Eprev, Sprev)
	if (epoch < prevOwnerEpoch || (epoch == prevOwnerEpoch && seqno < prevOwnerSeqno)) {
		if (BW_DEBUG) {
			fmt::print("Lock acquire check failed. Proposed ({0}, {1}) < previous ({2}, {3})\n",
			           epoch,
			           seqno,
			           prevOwnerEpoch,
			           prevOwnerSeqno);
		}
		throw granule_assignment_conflict();
	}
}

void checkGranuleLock(int64_t epoch, int64_t seqno, int64_t ownerEpoch, int64_t ownerSeqno) {
	// sanity check - lock value should never go backwards because of acquireGranuleLock
	ASSERT(epoch <= ownerEpoch);
	ASSERT(epoch < ownerEpoch || (epoch == ownerEpoch && seqno <= ownerSeqno));

	// returns true if we still own the lock, false if someone else does
	if (epoch != ownerEpoch || seqno != ownerSeqno) {
		if (BW_DEBUG) {
			fmt::print("Lock assignment check failed. Expected ({0}, {1}), got ({2}, {3})\n",
			           epoch,
			           seqno,
			           ownerEpoch,
			           ownerSeqno);
		}
		throw granule_assignment_conflict();
	}
}
} // namespace

// Below actors asssit in fetching/lookup desired encryption keys. Following steps are done for an encryption key
// lookup:
// 1. Lookup proccess local in-memory cache `BlobCipherKeyCache` to check if desired EK is 'present' and 'valid'. Given
//    FDB supports 'revocable' & 'non-revocable' EKs; a cached EK can also be 'invalid'.
// 2. Local cache miss will follow with a RPC call to EncryptKeyProxy process (EKP), EKP maintain an in-memory cache of
//    KMS BaseCipher details with KMS defined TTL if applicable. The lookup call can either to serviced by EKP or would
//    lead to desired KMS endpoint invocation.
//
// In most of the cases, the EK lookup should be satisfied by process local in-memory cache and/or EKP in-memory cache,
// unless cluster and/or a process crash/restart.

ACTOR Future<BlobGranuleCipherKeysCtx> getLatestGranuleCipherKeys(Reference<BlobWorkerData> bwData,
                                                                  KeyRange keyRange,
                                                                  Arena* arena) {
	state BlobGranuleCipherKeysCtx cipherKeysCtx;
	state Reference<GranuleTenantData> tenantData = wait(bwData->tenantData.getDataForGranule(keyRange));

	ASSERT(tenantData.isValid());

	std::unordered_set<EncryptCipherDomainId> domainIds;
	domainIds.emplace(tenantData->entry.id);
	std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> domainKeyMap =
	    wait(getLatestEncryptCipherKeys(bwData->dbInfo, domainIds, BlobCipherMetrics::BLOB_GRANULE));

	auto domainKeyItr = domainKeyMap.find(tenantData->entry.id);
	ASSERT(domainKeyItr != domainKeyMap.end());
	cipherKeysCtx.textCipherKey = BlobGranuleCipherKey::fromBlobCipherKey(domainKeyItr->second, *arena);

	TextAndHeaderCipherKeys systemCipherKeys =
	    wait(getLatestSystemEncryptCipherKeys(bwData->dbInfo, BlobCipherMetrics::BLOB_GRANULE));
	cipherKeysCtx.headerCipherKey = BlobGranuleCipherKey::fromBlobCipherKey(systemCipherKeys.cipherHeaderKey, *arena);

	cipherKeysCtx.ivRef = makeString(AES_256_IV_LENGTH, *arena);
	deterministicRandom()->randomBytes(mutateString(cipherKeysCtx.ivRef), AES_256_IV_LENGTH);

	if (BG_ENCRYPT_COMPRESS_DEBUG) {
		TraceEvent(SevDebug, "GetLatestGranuleCipherKey")
		    .detail("TextDomainId", cipherKeysCtx.textCipherKey.encryptDomainId)
		    .detail("TextBaseCipherId", cipherKeysCtx.textCipherKey.baseCipherId)
		    .detail("TextSalt", cipherKeysCtx.textCipherKey.salt)
		    .detail("HeaderDomainId", cipherKeysCtx.textCipherKey.encryptDomainId)
		    .detail("HeaderBaseCipherId", cipherKeysCtx.textCipherKey.baseCipherId)
		    .detail("HeaderSalt", cipherKeysCtx.textCipherKey.salt)
		    .detail("IVChksum", XXH3_64bits(cipherKeysCtx.ivRef.begin(), cipherKeysCtx.ivRef.size()));
	}

	return cipherKeysCtx;
}

ACTOR Future<BlobGranuleCipherKey> lookupCipherKey(Reference<BlobWorkerData> bwData,
                                                   BlobCipherDetails cipherDetails,
                                                   Arena* arena) {
	std::unordered_set<BlobCipherDetails> cipherDetailsSet;
	cipherDetailsSet.emplace(cipherDetails);
	state std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>> cipherKeyMap =
	    wait(getEncryptCipherKeys(bwData->dbInfo, cipherDetailsSet, BlobCipherMetrics::BLOB_GRANULE));

	ASSERT(cipherKeyMap.size() == 1);

	auto cipherKeyMapItr = cipherKeyMap.find(cipherDetails);
	if (cipherKeyMapItr == cipherKeyMap.end()) {
		TraceEvent(SevError, "CipherKeyLookup_Failure")
		    .detail("EncryptDomainId", cipherDetails.encryptDomainId)
		    .detail("BaseCipherId", cipherDetails.baseCipherId)
		    .detail("Salt", cipherDetails.salt);
		throw encrypt_keys_fetch_failed();
	}

	return BlobGranuleCipherKey::fromBlobCipherKey(cipherKeyMapItr->second, *arena);
}

ACTOR Future<BlobGranuleCipherKeysCtx> getGranuleCipherKeysImpl(Reference<BlobWorkerData> bwData,
                                                                BlobCipherDetails textCipherDetails,
                                                                BlobCipherDetails headerCipherDetails,
                                                                StringRef ivRef,
                                                                Arena* arena) {
	state BlobGranuleCipherKeysCtx cipherKeysCtx;

	// Fetch 'textCipher' key
	BlobGranuleCipherKey textCipherKey = wait(lookupCipherKey(bwData, textCipherDetails, arena));
	cipherKeysCtx.textCipherKey = textCipherKey;

	// Fetch 'headerCipher' key
	BlobGranuleCipherKey headerCipherKey = wait(lookupCipherKey(bwData, headerCipherDetails, arena));
	cipherKeysCtx.headerCipherKey = headerCipherKey;

	// Populate 'Intialization Vector'
	ASSERT_EQ(ivRef.size(), AES_256_IV_LENGTH);
	cipherKeysCtx.ivRef = StringRef(*arena, ivRef);

	if (BG_ENCRYPT_COMPRESS_DEBUG) {
		TraceEvent(SevDebug, "GetGranuleCipherKey")
		    .detail("TextDomainId", cipherKeysCtx.textCipherKey.encryptDomainId)
		    .detail("TextBaseCipherId", cipherKeysCtx.textCipherKey.baseCipherId)
		    .detail("TextSalt", cipherKeysCtx.textCipherKey.salt)
		    .detail("HeaderDomainId", cipherKeysCtx.textCipherKey.encryptDomainId)
		    .detail("HeaderBaseCipherId", cipherKeysCtx.textCipherKey.baseCipherId)
		    .detail("HeaderSalt", cipherKeysCtx.textCipherKey.salt)
		    .detail("IVChksum", XXH3_64bits(cipherKeysCtx.ivRef.begin(), cipherKeysCtx.ivRef.size()));
	}

	return cipherKeysCtx;
}

Future<BlobGranuleCipherKeysCtx> getGranuleCipherKeysFromKeysMeta(Reference<BlobWorkerData> bwData,
                                                                  BlobGranuleCipherKeysMeta cipherKeysMeta,
                                                                  Arena* arena) {
	BlobCipherDetails textCipherDetails(
	    cipherKeysMeta.textDomainId, cipherKeysMeta.textBaseCipherId, cipherKeysMeta.textSalt);

	BlobCipherDetails headerCipherDetails(
	    cipherKeysMeta.headerDomainId, cipherKeysMeta.headerBaseCipherId, cipherKeysMeta.headerSalt);

	StringRef ivRef = StringRef(*arena, cipherKeysMeta.ivStr);

	return getGranuleCipherKeysImpl(bwData, textCipherDetails, headerCipherDetails, ivRef, arena);
}

Future<BlobGranuleCipherKeysCtx> getGranuleCipherKeysFromKeysMetaRef(Reference<BlobWorkerData> bwData,
                                                                     BlobGranuleCipherKeysMetaRef cipherKeysMetaRef,
                                                                     Arena* arena) {
	BlobCipherDetails textCipherDetails(
	    cipherKeysMetaRef.textDomainId, cipherKeysMetaRef.textBaseCipherId, cipherKeysMetaRef.textSalt);

	BlobCipherDetails headerCipherDetails(
	    cipherKeysMetaRef.headerDomainId, cipherKeysMetaRef.headerBaseCipherId, cipherKeysMetaRef.headerSalt);

	return getGranuleCipherKeysImpl(bwData, textCipherDetails, headerCipherDetails, cipherKeysMetaRef.ivRef, arena);
}

ACTOR Future<Void> readAndCheckGranuleLock(Reference<ReadYourWritesTransaction> tr,
                                           KeyRange granuleRange,
                                           int64_t epoch,
                                           int64_t seqno) {
	state Key lockKey = blobGranuleLockKeyFor(granuleRange);
	Optional<Value> lockValue = wait(tr->get(lockKey));

	if (!lockValue.present()) {
		// FIXME: could add some validation for simulation that a force purge was initiated
		// for lock to be deleted out from under an active granule means a force purge must have happened.
		throw granule_assignment_conflict();
	}

	std::tuple<int64_t, int64_t, UID> currentOwner = decodeBlobGranuleLockValue(lockValue.get());
	checkGranuleLock(epoch, seqno, std::get<0>(currentOwner), std::get<1>(currentOwner));

	// if we still own the lock, add a conflict range in case anybody else takes it over while we add this file
	// FIXME: we don't need these conflict ranges
	tr->addReadConflictRange(singleKeyRange(lockKey));

	return Void();
}

// Read snapshot and delta files for granule history, for completed granule
// Retries on error local to this function
ACTOR Future<GranuleFiles> loadHistoryFiles(Reference<BlobWorkerData> bwData, UID granuleID) {
	state Transaction tr(bwData->db);
	state KeyRange range = blobGranuleFileKeyRangeFor(granuleID);
	state Key startKey = range.begin;
	state GranuleFiles files;
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(readGranuleFiles(&tr, &startKey, range.end, &files, granuleID));
			return files;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// read snapshot and delta files from previous owner of the active granule
// This is separated out from above because this is done as part of granule open transaction
ACTOR Future<GranuleFiles> loadPreviousFiles(Transaction* tr, UID granuleID) {
	state KeyRange range = blobGranuleFileKeyRangeFor(granuleID);
	// no need to add conflict range for read b/c of granule lock
	state Key startKey = range.begin;
	state GranuleFiles files;
	wait(readGranuleFiles(tr, &startKey, range.end, &files, granuleID));
	return files;
}

// To cleanup of the old change feed for the old granule range, all new sub-granules split from the old range must
// update shared state to coordinate when it is safe to clean up the old change feed.
//  his goes through 3 phases for each new sub-granule:
//  1. Starting - the blob manager writes all sub-granules with this state as a durable intent to split the range
//  2. Assigned - a worker that is assigned a sub-granule updates that granule's state here. This means that the
//  worker
//     has started a new change feed for the new sub-granule, but still needs to consume from the old change feed.
//  3. Done - the worker that is assigned this sub-granule has persisted all of the data from its part of the old
//  change
//     feed in delta files. From this granule's perspective, it is safe to clean up the old change feed.

// Once all sub-granules have reached step 2 (Assigned), the change feed can be safely "stopped" - it needs to
// continue to serve the mutations it has seen so far, but will not need any new mutations after this version. The
// last sub-granule to reach this step is responsible for commiting the change feed stop as part of its transaction.
// Because this change feed stops commits in the same transaction as the worker's new change feed start, it is
// guaranteed that no versions are missed between the old and new change feed.
//
// Once all sub-granules have reached step 3 (Done), the change feed can be safely destroyed, as all of the
// mutations in the old change feed are guaranteed to be persisted in delta files. The last sub-granule to reach
// this step is responsible for committing the change feed destroy, and for cleaning up the split state for all
// sub-granules as part of its transaction.

ACTOR Future<Void> updateGranuleSplitState(Transaction* tr,
                                           KeyRange parentGranuleRange,
                                           UID parentGranuleID,
                                           UID currentGranuleID,
                                           BlobGranuleSplitState newState) {
	state KeyRange currentRange = blobGranuleSplitKeyRangeFor(parentGranuleID);

	state RangeResult totalState = wait(tr->getRange(currentRange, SERVER_KNOBS->BG_MAX_SPLIT_FANOUT + 1));
	// FIXME: remove above conflict range?
	tr->addWriteConflictRange(currentRange);
	ASSERT_WE_THINK(!totalState.more && totalState.size() <= SERVER_KNOBS->BG_MAX_SPLIT_FANOUT);
	// maybe someone decreased the knob, we should gracefully handle it not in simulation
	if (totalState.more || totalState.size() > SERVER_KNOBS->BG_MAX_SPLIT_FANOUT) {
		RangeResult tryAgain = wait(tr->getRange(currentRange, 10000));
		ASSERT(!tryAgain.more);
		totalState = tryAgain;
	}

	if (totalState.empty()) {
		ASSERT(newState == BlobGranuleSplitState::Done);
		if (BW_DEBUG) {
			fmt::print("Found empty split state for parent granule {0}\n", parentGranuleID.toString());
		}
		// must have retried and successfully nuked everything
		return Void();
	}
	ASSERT(totalState.size() >= 2);

	int total = totalState.size();
	int totalStarted = 0;
	int totalDone = 0;
	BlobGranuleSplitState currentState = BlobGranuleSplitState::Unknown;
	for (auto& it : totalState) {
		UID pid;
		UID cid;
		std::pair<UID, UID> k = decodeBlobGranuleSplitKey(it.key);
		pid = k.first;
		cid = k.second;
		ASSERT(pid == parentGranuleID);

		BlobGranuleSplitState st = decodeBlobGranuleSplitValue(it.value).first;
		ASSERT(st != BlobGranuleSplitState::Unknown);
		if (st == BlobGranuleSplitState::Initialized) {
			totalStarted++;
		} else if (st == BlobGranuleSplitState::Done) {
			totalDone++;
		}
		if (cid == currentGranuleID) {
			ASSERT(currentState == BlobGranuleSplitState::Unknown);
			currentState = st;
		}
	}

	ASSERT(currentState != BlobGranuleSplitState::Unknown);

	if (currentState < newState) {
		if (BW_DEBUG) {
			fmt::print("Updating granule {0} split state from {1} {2} -> {3}\n",
			           currentGranuleID.toString(),
			           parentGranuleID.toString(),
			           currentState,
			           newState);
		}

		Key myStateKey = blobGranuleSplitKeyFor(parentGranuleID, currentGranuleID);
		if (newState == BlobGranuleSplitState::Done && currentState == BlobGranuleSplitState::Assigned &&
		    totalDone == total - 1) {
			// we are the last one to change from Assigned -> Done, so everything can be cleaned up for the old
			// change feed and splitting state
			if (BW_DEBUG) {
				fmt::print("{0} destroying old granule {1}\n", currentGranuleID.toString(), parentGranuleID.toString());
			}

			wait(updateChangeFeed(tr, granuleIDToCFKey(parentGranuleID), ChangeFeedStatus::CHANGE_FEED_DESTROY));

			Key oldGranuleLockKey = blobGranuleLockKeyFor(parentGranuleRange);
			// FIXME: deleting granule lock can cause races where another granule with the same range starts way later
			// and thinks it can own the granule! Need to change file cleanup to destroy these, if there is no more
			// granule in the history with that exact key range!
			// Alternative fix could be to, on granule open, query for all overlapping granule locks and ensure none of
			// them have higher (epoch, seqno), but that is much more expensive

			// tr->clear(singleKeyRange(oldGranuleLockKey));
			tr->clear(currentRange);
			CODE_PROBE(true, "Granule split cleanup on last delta file persisted");
		} else {
			tr->atomicOp(myStateKey, blobGranuleSplitValueFor(newState), MutationRef::SetVersionstampedValue);
			if (newState == BlobGranuleSplitState::Assigned && currentState == BlobGranuleSplitState::Initialized &&
			    totalStarted == 1) {
				// We are the last one to change from Start -> Assigned, so we can stop the parent change feed.
				if (BW_DEBUG) {
					fmt::print("{0} stopping change feed for old granule {1}\n",
					           currentGranuleID.toString().c_str(),
					           parentGranuleID.toString().c_str());
				}

				wait(updateChangeFeed(
				    tr, KeyRef(granuleIDToCFKey(parentGranuleID)), ChangeFeedStatus::CHANGE_FEED_STOP));
			}
			CODE_PROBE(true, "Granule split stopping change feed");
		}
	} else if (BW_DEBUG) {
		CODE_PROBE(true, "Out of order granule split state updates ignored", probe::decoration::rare);
		fmt::print("Ignoring granule {0} split state from {1} {2} -> {3}\n",
		           currentGranuleID.toString(),
		           parentGranuleID.toString(),
		           currentState,
		           newState);
	}

	return Void();
}

// Returns the split state for a given granule on granule reassignment, or unknown if it doesn't exist (meaning the
// granule splitting finished)
ACTOR Future<std::pair<BlobGranuleSplitState, Version>> getGranuleSplitState(Transaction* tr,
                                                                             UID parentGranuleID,
                                                                             UID currentGranuleID) {
	Key myStateKey = blobGranuleSplitKeyFor(parentGranuleID, currentGranuleID);

	Optional<Value> st = wait(tr->get(myStateKey));
	if (st.present()) {
		return decodeBlobGranuleSplitValue(st.get());
	} else {
		return std::pair(BlobGranuleSplitState::Unknown, invalidVersion);
	}
}

// tries to use writeEntireFile if possible, but if too big falls back to multi-part upload
ACTOR Future<Void> writeFile(Reference<BackupContainerFileSystem> writeBStore, std::string fname, Value serialized) {
	if (!SERVER_KNOBS->BG_WRITE_MULTIPART) {
		try {
			state std::string fileContents = serialized.toString();
			wait(writeBStore->writeEntireFile(fname, fileContents));
			return Void();
		} catch (Error& e) {
			// error_code_file_too_large means it was too big to do with single write
			if (e.code() != error_code_file_too_large) {
				throw e;
			}
		}
	}

	state Reference<IBackupFile> objectFile = wait(writeBStore->writeFile(fname));
	wait(objectFile->append(serialized.begin(), serialized.size()));
	wait(objectFile->finish());

	return Void();
}

// writeDelta file writes speculatively in the common case to optimize throughput. It creates the s3 object even though
// the data in it may not yet be committed, and even though previous delta files with lower versioned data may still be
// in flight. The synchronization happens after the s3 file is written, but before we update the FDB index of what files
// exist. Before updating FDB, we ensure the version is committed and all previous delta files have updated FDB.
ACTOR Future<BlobFileIndex> writeDeltaFile(Reference<BlobWorkerData> bwData,
                                           Reference<BlobConnectionProvider> bstore,
                                           KeyRange keyRange,
                                           UID granuleID,
                                           int64_t epoch,
                                           int64_t seqno,
                                           Standalone<GranuleDeltas> deltasToWrite,
                                           Version currentDeltaVersion,
                                           Future<BlobFileIndex> previousDeltaFileFuture,
                                           Future<Void> waitCommitted,
                                           Optional<std::pair<KeyRange, UID>> oldGranuleComplete,
                                           Future<Void> startDeltaFileWrite) {
	wait(startDeltaFileWrite);
	state FlowLock::Releaser holdingLock(*bwData->deltaWritesLock);

	wait(delay(0, TaskPriority::BlobWorkerUpdateStorage));

	state std::string fileName = randomBGFilename(bwData->id, granuleID, currentDeltaVersion, ".delta");

	state Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx;
	state Optional<BlobGranuleCipherKeysMeta> cipherKeysMeta;
	state Arena arena;

	if (bwData->isEncryptionEnabled) {
		BlobGranuleCipherKeysCtx ciphKeysCtx = wait(getLatestGranuleCipherKeys(bwData, keyRange, &arena));
		cipherKeysCtx = std::move(ciphKeysCtx);
		cipherKeysMeta = BlobGranuleCipherKeysCtx::toCipherKeysMeta(cipherKeysCtx.get());
	}

	state Optional<CompressionFilter> compressFilter = getBlobFileCompressFilter();
	state Value serialized = serializeChunkedDeltaFile(StringRef(fileName),
	                                                   deltasToWrite,
	                                                   keyRange,
	                                                   SERVER_KNOBS->BG_DELTA_FILE_TARGET_CHUNK_BYTES,
	                                                   compressFilter,
	                                                   cipherKeysCtx);
	state size_t serializedSize = serialized.size();
	bwData->stats.compressionBytesRaw += deltasToWrite.expectedSize();
	bwData->stats.compressionBytesFinal += serializedSize;

	// Free up deltasToWrite here to reduce memory
	deltasToWrite = Standalone<GranuleDeltas>();

	state Reference<BackupContainerFileSystem> writeBStore;
	state std::string fname;
	std::tie(writeBStore, fname) = bstore->createForWrite(fileName);

	state double writeStartTimer = g_network->timer();

	wait(writeFile(writeBStore, fname, serialized));

	++bwData->stats.s3PutReqs;
	++bwData->stats.deltaFilesWritten;
	bwData->stats.deltaBytesWritten += serializedSize;
	double duration = g_network->timer() - writeStartTimer;
	bwData->stats.deltaBlobWriteLatencySample.addMeasurement(duration);

	// free serialized since it is persisted in blob
	serialized = Value();

	// now that all buffered memory from file is gone, we can release memory flow lock
	// we must unblock here to allow feed to continue to consume, so that waitCommitted returns
	holdingLock.release();

	state int numIterations = 0;
	try {
		// before updating FDB, wait for the delta file version to be committed and previous delta files to finish
		wait(waitCommitted);
		BlobFileIndex prev = wait(previousDeltaFileFuture);
		wait(delay(0, TaskPriority::BlobWorkerUpdateFDB));

		// update FDB with new file
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);
		loop {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				wait(readAndCheckGranuleLock(tr, keyRange, epoch, seqno));
				numIterations++;

				Key dfKey = blobGranuleFileKeyFor(granuleID, currentDeltaVersion, 'D');
				// TODO change once we support file multiplexing
				Value dfValue = blobGranuleFileValueFor(fname, 0, serializedSize, serializedSize, cipherKeysMeta);
				tr->set(dfKey, dfValue);

				if (oldGranuleComplete.present()) {
					wait(updateGranuleSplitState(&tr->getTransaction(),
					                             oldGranuleComplete.get().first,
					                             oldGranuleComplete.get().second,
					                             granuleID,
					                             BlobGranuleSplitState::Done));
				}

				wait(tr->commit());
				if (BW_DEBUG) {
					fmt::print(
					    "Granule {0} [{1} - {2}) updated fdb with delta file {3} of size {4} at version {5}, cv={6}\n",
					    granuleID.toString(),
					    keyRange.begin.printable(),
					    keyRange.end.printable(),
					    fname,
					    serializedSize,
					    currentDeltaVersion,
					    tr->getCommittedVersion());
				}

				if (BUGGIFY && bwData->maybeInjectTargetedRestart()) {
					wait(delay(0)); // should be cancelled
					ASSERT(false);
				}

				if (BUGGIFY_WITH_PROB(0.01)) {
					wait(delay(deterministicRandom()->random01()));
				}

				if (BW_DEBUG) {
					TraceEvent(SevDebug, "DeltaFileWritten")
					    .detail("FileName", fname)
					    .detail("Encrypted", cipherKeysCtx.present())
					    .detail("Compressed", compressFilter.present());
				}

				// FIXME: change when we implement multiplexing
				return BlobFileIndex(currentDeltaVersion, fname, 0, serializedSize, serializedSize, cipherKeysMeta);
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	} catch (Error& e) {
		// If this actor was cancelled, doesn't own the granule anymore, or got some other error before trying to
		// commit a transaction, we can and want to safely delete the file we wrote. Otherwise, we may have updated FDB
		// with file and cannot safely delete it.
		if (numIterations > 0) {
			CODE_PROBE(true, "Granule potentially leaving orphaned delta file");
			throw e;
		}
		if (BW_DEBUG) {
			fmt::print("deleting delta file {0} after error {1}\n", fname, e.name());
		}
		CODE_PROBE(true, "Granule cleaning up delta file after error");
		++bwData->stats.s3DeleteReqs;
		bwData->addActor.send(writeBStore->deleteFile(fname));
		throw e;
	}
}

ACTOR Future<Void> reevaluateInitialSplit(Reference<BlobWorkerData> bwData,
                                          UID granuleID,
                                          KeyRange keyRange,
                                          int64_t epoch,
                                          int64_t seqno,
                                          Key proposedSplitKey);

ACTOR Future<BlobFileIndex> writeSnapshot(Reference<BlobWorkerData> bwData,
                                          Reference<BlobConnectionProvider> bstore,
                                          KeyRange keyRange,
                                          UID granuleID,
                                          int64_t epoch,
                                          int64_t seqno,
                                          Version version,
                                          PromiseStream<RangeResult> rows,
                                          bool initialSnapshot) {
	state std::string fileName = randomBGFilename(bwData->id, granuleID, version, ".snapshot");
	state Standalone<GranuleSnapshot> snapshot;
	state int64_t bytesRead = 0;
	state bool injectTooBig = initialSnapshot && g_network->isSimulated() && BUGGIFY_WITH_PROB(0.1);

	wait(delay(0, TaskPriority::BlobWorkerUpdateStorage));

	loop {
		try {
			if (initialSnapshot && snapshot.size() > 1 &&
			    (injectTooBig || bytesRead >= 3 * SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES)) {
				// throw transaction too old either on injection for simulation, or if snapshot would be too large now
				throw transaction_too_old();
			}
			RangeResult res = waitNext(rows.getFuture());
			snapshot.arena().dependsOn(res.arena());
			snapshot.append(snapshot.arena(), res.begin(), res.size());
			bytesRead += res.expectedSize();
			wait(yield(TaskPriority::BlobWorkerUpdateStorage));
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				break;
			}
			// if we got transaction_too_old naturally, have lower threshold for re-evaluating (2xlimit)
			if (initialSnapshot && snapshot.size() > 1 && e.code() == error_code_transaction_too_old &&
			    (injectTooBig || bytesRead >= 2 * SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES)) {
				// idle this actor, while we tell the manager this is too big and to re-evaluate granules and revoke us
				if (BW_DEBUG) {
					fmt::print("Granule [{0} - {1}) re-evaluating snapshot after {2} bytes ({3} limit) {4}\n",
					           keyRange.begin.printable(),
					           keyRange.end.printable(),
					           bytesRead,
					           SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES,
					           injectTooBig ? "(injected)" : "");
				}
				wait(reevaluateInitialSplit(
				    bwData, granuleID, keyRange, epoch, seqno, snapshot[snapshot.size() / 2].key));
				ASSERT(false);
			} else {
				throw e;
			}
		}
	}

	wait(delay(0, TaskPriority::BlobWorkerUpdateStorage));

	if (BW_DEBUG) {
		fmt::print("Granule [{0} - {1}) read {2} snapshot rows ({3} bytes)\n",
		           keyRange.begin.printable(),
		           keyRange.end.printable(),
		           snapshot.size(),
		           bytesRead);
	}

	if (g_network->isSimulated()) {
		if (snapshot.size() > 0) {
			ASSERT(keyRange.begin <= snapshot[0].key);
			ASSERT(keyRange.end > snapshot[snapshot.size() - 1].key);
		}
		for (int i = 0; i < snapshot.size() - 1; i++) {
			if (snapshot[i].key >= snapshot[i + 1].key) {
				fmt::print("SORT ORDER VIOLATION IN SNAPSHOT FILE: {0}, {1}\n",
				           snapshot[i].key.printable(),
				           snapshot[i + 1].key.printable());
			}
			ASSERT(snapshot[i].key < snapshot[i + 1].key);
		}
	}

	state Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx;
	state Optional<BlobGranuleCipherKeysMeta> cipherKeysMeta;
	state Arena arena;

	if (bwData->isEncryptionEnabled) {
		BlobGranuleCipherKeysCtx ciphKeysCtx = wait(getLatestGranuleCipherKeys(bwData, keyRange, &arena));
		cipherKeysCtx = std::move(ciphKeysCtx);
		cipherKeysMeta = BlobGranuleCipherKeysCtx::toCipherKeysMeta(cipherKeysCtx.get());
	}

	state Optional<CompressionFilter> compressFilter = getBlobFileCompressFilter();
	state Value serialized = serializeChunkedSnapshot(StringRef(fileName),
	                                                  snapshot,
	                                                  SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_CHUNK_BYTES,
	                                                  compressFilter,
	                                                  cipherKeysCtx);
	state size_t serializedSize = serialized.size();
	bwData->stats.compressionBytesRaw += snapshot.expectedSize();
	bwData->stats.compressionBytesFinal += serializedSize;

	// free snapshot to reduce memory
	snapshot = Standalone<GranuleSnapshot>();

	// write to blob using multi part upload
	state Reference<BackupContainerFileSystem> writeBStore;
	state std::string fname;
	std::tie(writeBStore, fname) = bstore->createForWrite(fileName);

	state double writeStartTimer = g_network->timer();

	wait(writeFile(writeBStore, fname, serialized));

	++bwData->stats.s3PutReqs;
	++bwData->stats.snapshotFilesWritten;
	bwData->stats.snapshotBytesWritten += serializedSize;
	double duration = g_network->timer() - writeStartTimer;
	bwData->stats.snapshotBlobWriteLatencySample.addMeasurement(duration);

	// free serialized since it is persisted in blob
	serialized = Value();

	wait(delay(0, TaskPriority::BlobWorkerUpdateFDB));
	// object uploaded successfully, save it to system key space

	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);
	state int numIterations = 0;

	try {
		loop {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				wait(readAndCheckGranuleLock(tr, keyRange, epoch, seqno));
				numIterations++;
				Key snapshotFileKey = blobGranuleFileKeyFor(granuleID, version, 'S');
				// TODO change once we support file multiplexing
				Key snapshotFileValue =
				    blobGranuleFileValueFor(fname, 0, serializedSize, serializedSize, cipherKeysMeta);
				tr->set(snapshotFileKey, snapshotFileValue);
				// create granule history at version if this is a new granule with the initial dump from FDB
				if (initialSnapshot) {
					Key historyKey = blobGranuleHistoryKeyFor(keyRange, version);
					Standalone<BlobGranuleHistoryValue> historyValue;
					historyValue.granuleID = granuleID;
					tr->set(historyKey, blobGranuleHistoryValueFor(historyValue));
				}
				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	} catch (Error& e) {
		// If this actor was cancelled, doesn't own the granule anymore, or got some other error before trying to
		// commit a transaction, we can and want to safely delete the file we wrote. Otherwise, we may have updated FDB
		// with file and cannot safely delete it.
		if (numIterations > 0) {
			CODE_PROBE(true, "Granule potentially leaving orphaned snapshot file");
			throw e;
		}
		if (BW_DEBUG) {
			fmt::print("deleting snapshot file {0} after error {1}\n", fname, e.name());
		}
		CODE_PROBE(true, "Granule deleting snapshot file after error");
		++bwData->stats.s3DeleteReqs;
		bwData->addActor.send(writeBStore->deleteFile(fname));
		throw e;
	}

	if (BW_DEBUG) {
		fmt::print("Granule [{0} - {1}) committed new snapshot file {2} with {3} bytes\n\n",
		           keyRange.begin.printable(),
		           keyRange.end.printable(),
		           fname,
		           serializedSize);
	}

	if (BUGGIFY_WITH_PROB(0.1)) {
		wait(delay(deterministicRandom()->random01()));
	}

	if (BW_DEBUG) {
		TraceEvent(SevDebug, "SnapshotFileWritten")
		    .detail("FileName", fileName)
		    .detail("Encrypted", cipherKeysCtx.present())
		    .detail("Compressed", compressFilter.present());
	}

	if (BUGGIFY && bwData->maybeInjectTargetedRestart()) {
		wait(delay(0)); // should be cancelled
		ASSERT(false);
	}

	// FIXME: change when we implement multiplexing
	return BlobFileIndex(version, fname, 0, serializedSize, serializedSize, cipherKeysMeta);
}

ACTOR Future<BlobFileIndex> dumpInitialSnapshotFromFDB(Reference<BlobWorkerData> bwData,
                                                       Reference<BlobConnectionProvider> bstore,
                                                       Reference<GranuleMetadata> metadata,
                                                       UID granuleID,
                                                       Key cfKey,
                                                       std::deque<Future<Void>>* inFlightPops) {
	if (BW_DEBUG) {
		fmt::print("Dumping snapshot from FDB for [{0} - {1})\n",
		           metadata->keyRange.begin.printable(),
		           metadata->keyRange.end.printable());
	}
	wait(bwData->initialSnapshotLock->take());
	state FlowLock::Releaser holdingLock(*bwData->initialSnapshotLock);

	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);
	state int retries = 0;
	state Version lastReadVersion = invalidVersion;
	state Version readVersion = invalidVersion;

	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::RAW_ACCESS);
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			Version rv = wait(tr->getReadVersion());
			readVersion = rv;
			ASSERT(lastReadVersion <= readVersion);
			state PromiseStream<RangeResult> rowsStream;
			state Future<BlobFileIndex> snapshotWriter = writeSnapshot(bwData,
			                                                           bstore,
			                                                           metadata->keyRange,
			                                                           granuleID,
			                                                           metadata->originalEpoch,
			                                                           metadata->originalSeqno,
			                                                           readVersion,
			                                                           rowsStream,
			                                                           true);
			Future<Void> streamFuture =
			    tr->getTransaction().getRangeStream(rowsStream, metadata->keyRange, GetRangeLimits(), Snapshot::True);
			wait(streamFuture && success(snapshotWriter));
			TraceEvent(SevDebug, "BlobGranuleSnapshotFile", bwData->id)
			    .detail("Granule", metadata->keyRange)
			    .detail("Version", readVersion);
			DEBUG_KEY_RANGE("BlobWorkerFDBSnapshot", readVersion, metadata->keyRange, bwData->id);

			if (BUGGIFY && bwData->maybeInjectTargetedRestart()) {
				wait(delay(0)); // should be cancelled
				ASSERT(false);
			}

			// initial snapshot is committed in fdb, we can pop the change feed up to this version
			inFlightPops->push_back(bwData->db->popChangeFeedMutations(cfKey, readVersion + 1));
			return snapshotWriter.get();
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw e;
			}
			if (BW_DEBUG) {
				fmt::print("Dumping snapshot {0} from FDB for [{1} - {2}) got error {3}\n",
				           retries + 1,
				           metadata->keyRange.begin.printable(),
				           metadata->keyRange.end.printable(),
				           e.name());
			}
			state Error err = e;
			if (e.code() == error_code_server_overloaded) {
				wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
			} else {
				wait(tr->onError(e));
			}
			retries++;
			CODE_PROBE(true, "Granule initial snapshot failed");
			TraceEvent(retries < 10 ? SevDebug : SevWarn, "BlobGranuleInitialSnapshotRetry", bwData->id)
			    .error(err)
			    .detail("Granule", metadata->keyRange)
			    .detail("Count", retries);
			lastReadVersion = readVersion;
			// Pop change feed up to readVersion, because that data will be before the next snapshot
			// Do this to prevent a large amount of CF data from accumulating if we have consecutive failures to
			// snapshot
			// Also somewhat servers as a rate limiting function and checking that the database is available for this
			// key range
			// FIXME: can't do this because this granule might not own the shard anymore and another worker might have
			// successfully snapshotted already, but if it got an error before even getting to the lock check, it
			// wouldn't realize wait(bwData->db->popChangeFeedMutations(cfKey, readVersion));
		}
	}
}

// files might not be the current set of files in metadata, in the case of doing the initial snapshot of a granule that
// was split.
ACTOR Future<BlobFileIndex> compactFromBlob(Reference<BlobWorkerData> bwData,
                                            Reference<BlobConnectionProvider> bstore,
                                            Reference<GranuleMetadata> metadata,
                                            UID granuleID,
                                            std::vector<GranuleFiles> fileSet,
                                            Version version) {
	wait(bwData->resnapshotLock->take());
	state FlowLock::Releaser holdingLock(*bwData->resnapshotLock);
	wait(delay(0, TaskPriority::BlobWorkerUpdateStorage));
	if (BW_DEBUG) {
		fmt::print("Compacting snapshot from blob for [{0} - {1}) @ {2}\n",
		           metadata->keyRange.begin.printable().c_str(),
		           metadata->keyRange.end.printable().c_str(),
		           version);
	}

	state Arena filenameArena;
	state std::vector<Future<RangeResult>> chunksToRead;
	state int64_t compactBytesRead = 0;
	state double resnapshotStartTimer = g_network->timer();

	for (auto& f : fileSet) {
		ASSERT(!f.snapshotFiles.empty());
		ASSERT(!f.deltaFiles.empty());

		state BlobGranuleChunkRef chunk;
		state GranuleFiles files = f;
		state Version snapshotVersion = files.snapshotFiles.back().version;
		state BlobFileIndex snapshotF = files.snapshotFiles.back();

		if (snapshotVersion >= version) {
			fmt::print("Chunk snapshot version [{0} - {1}) @ {2} >= compact version {3}\n",
			           chunk.keyRange.begin.printable().c_str(),
			           chunk.keyRange.end.printable().c_str(),
			           snapshotVersion,
			           version);
		}
		ASSERT(snapshotVersion < version);

		state Optional<BlobGranuleCipherKeysCtx> snapCipherKeysCtx;
		if (g_network && g_network->isSimulated() &&
		    isEncryptionOpSupported(EncryptOperationType::BLOB_GRANULE_ENCRYPTION) &&
		    !snapshotF.cipherKeysMeta.present()) {
			ASSERT(false);
		}
		if (snapshotF.cipherKeysMeta.present()) {
			ASSERT(isEncryptionOpSupported(EncryptOperationType::BLOB_GRANULE_ENCRYPTION));
			CODE_PROBE(true, "fetching cipher keys for blob snapshot file");
			BlobGranuleCipherKeysCtx keysCtx =
			    wait(getGranuleCipherKeysFromKeysMeta(bwData, snapshotF.cipherKeysMeta.get(), &filenameArena));
			snapCipherKeysCtx = std::move(keysCtx);
		}

		chunk.snapshotFile = BlobFilePointerRef(filenameArena,
		                                        snapshotF.filename,
		                                        snapshotF.offset,
		                                        snapshotF.length,
		                                        snapshotF.fullFileLength,
		                                        snapCipherKeysCtx);

		compactBytesRead += snapshotF.length;
		state int deltaIdx = files.deltaFiles.size() - 1;
		while (deltaIdx >= 0 && files.deltaFiles[deltaIdx].version > snapshotVersion) {
			deltaIdx--;
		}
		deltaIdx++;
		state Version lastDeltaVersion = snapshotVersion;
		state BlobFileIndex deltaF;
		while (deltaIdx < files.deltaFiles.size() && lastDeltaVersion < version) {
			state Optional<BlobGranuleCipherKeysCtx> deltaCipherKeysCtx;

			deltaF = files.deltaFiles[deltaIdx];

			if (g_network && g_network->isSimulated() &&
			    isEncryptionOpSupported(EncryptOperationType::BLOB_GRANULE_ENCRYPTION) &&
			    !deltaF.cipherKeysMeta.present()) {
				ASSERT(false);
			}

			if (deltaF.cipherKeysMeta.present()) {
				ASSERT(isEncryptionOpSupported(EncryptOperationType::BLOB_GRANULE_ENCRYPTION));
				CODE_PROBE(true, "fetching cipher keys for delta file");
				BlobGranuleCipherKeysCtx keysCtx =
				    wait(getGranuleCipherKeysFromKeysMeta(bwData, deltaF.cipherKeysMeta.get(), &filenameArena));
				deltaCipherKeysCtx = std::move(keysCtx);
			}

			chunk.deltaFiles.emplace_back_deep(filenameArena,
			                                   deltaF.filename,
			                                   deltaF.offset,
			                                   deltaF.length,
			                                   deltaF.fullFileLength,
			                                   deltaCipherKeysCtx);
			compactBytesRead += deltaF.length;
			lastDeltaVersion = files.deltaFiles[deltaIdx].version;
			deltaIdx++;
		}
		ASSERT(lastDeltaVersion >= version);
		chunk.includedVersion = version;
		chunksToRead.push_back(readBlobGranule(chunk, metadata->keyRange, 0, version, bstore, &bwData->stats));
	}

	if (BW_DEBUG) {
		fmt::print("Re-snapshotting [{0} - {1}) @ {2} from blob\n",
		           metadata->keyRange.begin.printable(),
		           metadata->keyRange.end.printable(),
		           version);
	}

	try {
		state PromiseStream<RangeResult> rowsStream;
		state Future<BlobFileIndex> snapshotWriter = writeSnapshot(bwData,
		                                                           bstore,
		                                                           metadata->keyRange,
		                                                           granuleID,
		                                                           metadata->originalEpoch,
		                                                           metadata->originalSeqno,
		                                                           version,
		                                                           rowsStream,
		                                                           false);
		state int resultIdx;
		for (resultIdx = 0; resultIdx < chunksToRead.size(); resultIdx++) {
			RangeResult newGranuleChunk = wait(chunksToRead[resultIdx]);
			rowsStream.send(std::move(newGranuleChunk));
		}

		bwData->stats.bytesReadFromS3ForCompaction += compactBytesRead;
		rowsStream.sendError(end_of_stream());

		BlobFileIndex f = wait(snapshotWriter);
		DEBUG_KEY_RANGE("BlobWorkerBlobSnapshot", version, metadata->keyRange, bwData->id);
		double duration = g_network->timer() - resnapshotStartTimer;
		bwData->stats.reSnapshotLatencySample.addMeasurement(duration);
		return f;
	} catch (Error& e) {
		if (BW_DEBUG) {
			fmt::print("Compacting snapshot from blob for [{0} - {1}) got error {2}\n",
			           metadata->keyRange.begin.printable(),
			           metadata->keyRange.end.printable(),
			           e.name());
		}
		throw e;
	}
}

struct CounterHolder {
	int* counter;
	bool completed;

	CounterHolder() : counter(nullptr), completed(true) {}
	CounterHolder(int* counter) : counter(counter), completed(false) { (*counter)++; }

	void complete() {
		if (!completed) {
			completed = true;
			(*counter)--;
		}
	}

	~CounterHolder() { complete(); }
};

ACTOR Future<BlobFileIndex> checkSplitAndReSnapshot(Reference<BlobWorkerData> bwData,
                                                    Reference<BlobConnectionProvider> bstore,
                                                    Reference<GranuleMetadata> metadata,
                                                    UID granuleID,
                                                    int64_t bytesInNewDeltaFiles,
                                                    Future<BlobFileIndex> lastDeltaBeforeSnapshot,
                                                    int64_t versionsSinceLastSnapshot) {

	BlobFileIndex lastDeltaIdx = wait(lastDeltaBeforeSnapshot);
	state Version reSnapshotVersion = lastDeltaIdx.version;
	while (!bwData->statusStreamInitialized) {
		wait(bwData->currentManagerStatusStream.onChange());
	}

	wait(delay(0, TaskPriority::BlobWorkerUpdateFDB));

	state CounterHolder pendingCounter(&bwData->stats.granulesPendingSplitCheck);

	if (BW_DEBUG) {
		fmt::print("Granule [{0} - {1}) checking with BM for re-snapshot after {2} bytes\n",
		           metadata->keyRange.begin.printable(),
		           metadata->keyRange.end.printable(),
		           metadata->bytesInNewDeltaFiles);
	}

	TraceEvent(SevDebug, "BlobGranuleSnapshotCheck", bwData->id)
	    .detail("Granule", metadata->keyRange)
	    .detail("Version", reSnapshotVersion);

	// Save these from the start so repeated requests are idempotent
	// Need to retry in case response is dropped or manager changes. Eventually, a manager will
	// either reassign the range with continue=true, or will revoke the range. But, we will keep the
	// range open at this version for reads until that assignment change happens
	metadata->resumeSnapshot.reset();
	state int64_t statusEpoch = metadata->continueEpoch;
	state int64_t statusSeqno = metadata->continueSeqno;

	// If two snapshots happen without a split within a low time interval, this granule is "write-hot"
	// FIXME: If a rollback happens, this could incorrectly identify a hot granule as not hot. This should be
	// rare though and is just less efficient.
	state bool writeHot = versionsSinceLastSnapshot <= SERVER_KNOBS->BG_HOT_SNAPSHOT_VERSIONS;
	// FIXME: could probably refactor all of this logic into one large choose/when state machine that's less
	// complex
	loop {
		loop {
			try {
				// wait for manager stream to become ready, and send a message
				loop {
					choose {
						when(wait(bwData->currentManagerStatusStream.get().onReady())) { break; }
						when(wait(bwData->currentManagerStatusStream.onChange())) {}
						when(wait(metadata->resumeSnapshot.getFuture())) { break; }
					}
				}
				if (metadata->resumeSnapshot.isSet()) {
					break;
				}

				bwData->currentManagerStatusStream.get().send(GranuleStatusReply(metadata->keyRange,
				                                                                 true,
				                                                                 writeHot,
				                                                                 false,
				                                                                 statusEpoch,
				                                                                 statusSeqno,
				                                                                 granuleID,
				                                                                 metadata->historyVersion,
				                                                                 reSnapshotVersion,
				                                                                 false,
				                                                                 metadata->originalEpoch,
				                                                                 metadata->originalSeqno));
				break;
			} catch (Error& e) {
				if (e.code() == error_code_operation_cancelled) {
					throw e;
				}
				CODE_PROBE(true, "Blob worker re-sending split evaluation to manager after not error/not hearing back");
				// if we got broken promise while waiting, the old stream was killed, so we don't need to wait
				// on change, just retry
				if (e.code() == error_code_broken_promise) {
					wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				} else {
					wait(bwData->currentManagerStatusStream.onChange());
				}
			}
		}

		// wait for manager reply (which will either cancel this future or call resumeSnapshot), or re-send on
		// manager change/no response
		choose {
			when(wait(bwData->currentManagerStatusStream.onChange())) {}
			when(wait(metadata->resumeSnapshot.getFuture())) { break; }
			when(wait(delay(1.0))) {}
		}

		if (BW_DEBUG) {
			fmt::print("Granule [{0} - {1}), hasn't heard back from BM in BW {2}, re-sending status\n",
			           metadata->keyRange.begin.printable(),
			           metadata->keyRange.end.printable(),
			           bwData->id.toString());
		}
	}

	pendingCounter.complete();

	if (BW_DEBUG) {
		fmt::print("Granule [{0} - {1}) re-snapshotting after {2} bytes\n",
		           metadata->keyRange.begin.printable(),
		           metadata->keyRange.end.printable(),
		           bytesInNewDeltaFiles);
	}
	TraceEvent(SevDebug, "BlobGranuleSnapshotFile", bwData->id)
	    .detail("Granule", metadata->keyRange)
	    .detail("Version", metadata->durableDeltaVersion.get());

	// wait for file updater to make sure that last delta file is in the metadata before
	while (metadata->files.deltaFiles.empty() || metadata->files.deltaFiles.back().version < reSnapshotVersion) {
		wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
	}
	std::vector<GranuleFiles> toSnapshot;
	toSnapshot.push_back(metadata->files);
	BlobFileIndex reSnapshotIdx =
	    wait(compactFromBlob(bwData, bstore, metadata, granuleID, toSnapshot, reSnapshotVersion));
	return reSnapshotIdx;
}

ACTOR Future<BlobFileIndex> reSnapshotNoCheck(Reference<BlobWorkerData> bwData,
                                              Reference<BlobConnectionProvider> bstore,
                                              Reference<GranuleMetadata> metadata,
                                              UID granuleID,
                                              Future<BlobFileIndex> lastDeltaBeforeSnapshot) {
	BlobFileIndex lastDeltaIdx = wait(lastDeltaBeforeSnapshot);
	state Version reSnapshotVersion = lastDeltaIdx.version;
	wait(delay(0, TaskPriority::BlobWorkerUpdateFDB));

	CODE_PROBE(true, "re-snapshotting without BM check because still on old change feed!");

	if (BW_DEBUG) {
		fmt::print("Granule [{0} - {1}) re-snapshotting @ {2} WITHOUT checking with BM, because it is still on old "
		           "change feed!\n",
		           metadata->keyRange.begin.printable(),
		           metadata->keyRange.end.printable(),
		           reSnapshotVersion);
	}

	TraceEvent(SevDebug, "BlobGranuleReSnapshotOldFeed", bwData->id)
	    .detail("Granule", metadata->keyRange)
	    .detail("Version", reSnapshotVersion);

	// wait for file updater to make sure that last delta file is in the metadata before
	while (metadata->files.deltaFiles.empty() || metadata->files.deltaFiles.back().version < reSnapshotVersion) {
		wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
	}

	std::vector<GranuleFiles> toSnapshot;
	toSnapshot.push_back(metadata->files);
	BlobFileIndex reSnapshotIdx =
	    wait(compactFromBlob(bwData, bstore, metadata, granuleID, toSnapshot, reSnapshotVersion));

	return reSnapshotIdx;
}

// wait indefinitely to tell manager to re-evaluate this split, until the granule is revoked
ACTOR Future<Void> reevaluateInitialSplit(Reference<BlobWorkerData> bwData,
                                          UID granuleID,
                                          KeyRange keyRange,
                                          int64_t epoch,
                                          int64_t seqno,
                                          Key proposedSplitKey) {
	// wait for first stream to be initialized
	while (!bwData->statusStreamInitialized) {
		wait(bwData->currentManagerStatusStream.onChange());
	}
	loop {
		try {
			// wait for manager stream to become ready, and send a message
			loop {
				choose {
					when(wait(bwData->currentManagerStatusStream.get().onReady())) { break; }
					when(wait(bwData->currentManagerStatusStream.onChange())) {}
				}
			}

			GranuleStatusReply reply(keyRange,
			                         true,
			                         false,
			                         true,
			                         epoch,
			                         seqno,
			                         granuleID,
			                         invalidVersion,
			                         invalidVersion,
			                         false,
			                         epoch,
			                         seqno);
			reply.proposedSplitKey = proposedSplitKey;
			bwData->currentManagerStatusStream.get().send(reply);
			if (BUGGIFY && bwData->maybeInjectTargetedRestart()) {
				wait(delay(0)); // should be cancelled
				ASSERT(false);
			}
			// if a new manager appears, also tell it about this granule being splittable, or retry after a certain
			// amount of time of not hearing back
			wait(success(timeout(bwData->currentManagerStatusStream.onChange(), 10.0)));
			wait(delay(0));
			CODE_PROBE(true, "Blob worker re-sending initialsplit too big");
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw e;
			}

			CODE_PROBE(true, "Blob worker re-sending merge candidate to manager after not error/not hearing back");

			// if we got broken promise while waiting, the old stream was killed, so we don't need to wait
			// on change, just retry
			if (e.code() == error_code_broken_promise) {
				wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
			} else {
				wait(bwData->currentManagerStatusStream.onChange());
			}
		}
	}
}

ACTOR Future<Void> granuleCheckMergeCandidate(Reference<BlobWorkerData> bwData,
                                              Reference<GranuleMetadata> metadata,
                                              UID granuleID,
                                              Future<Void> waitStart) {
	if (!SERVER_KNOBS->BG_ENABLE_MERGING) {
		return Void();
	}
	// wait for the last snapshot to finish, so that the delay is from the last snapshot
	wait(waitStart);
	double jitter = deterministicRandom()->random01() * 0.8 * SERVER_KNOBS->BG_MERGE_CANDIDATE_DELAY_SECONDS;
	wait(delay(SERVER_KNOBS->BG_MERGE_CANDIDATE_THRESHOLD_SECONDS + jitter));
	loop {
		// this actor will be cancelled if a split check happened, or if the granule was moved away, so this
		// being here means that granule is cold enough during that period. Now we just need to check if it is
		// also small enough to be a merge candidate.
		StorageMetrics currentMetrics = wait(bwData->db->getStorageMetrics(metadata->keyRange, CLIENT_KNOBS->TOO_MANY));

		// FIXME: maybe separate knob and/or value for write rate?
		if (currentMetrics.bytes >= SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES / 2 ||
		    currentMetrics.bytesPerKSecond >= SERVER_KNOBS->SHARD_MIN_BYTES_PER_KSEC) {
			wait(delayJittered(SERVER_KNOBS->BG_MERGE_CANDIDATE_THRESHOLD_SECONDS / 2.0));
			CODE_PROBE(true, "wait and check later to see if granule got smaller or colder");
			continue;
		}

		CODE_PROBE(true, "Blob Worker identified merge candidate granule");

		// if we are a merge candidate, send a message to the BM. Once successful, this actor is complete
		while (!bwData->statusStreamInitialized) {
			wait(bwData->currentManagerStatusStream.onChange());
		}

		state double sendTimeGiveUp = now() + SERVER_KNOBS->BG_MERGE_CANDIDATE_THRESHOLD_SECONDS / 2.0;
		loop {
			try {
				// wait for manager stream to become ready, and send a message
				loop {
					choose {
						when(wait(delay(std::max(0.0, sendTimeGiveUp - now())))) { break; }
						when(wait(bwData->currentManagerStatusStream.get().onReady())) { break; }
						when(wait(bwData->currentManagerStatusStream.onChange())) {}
					}
				}

				if (now() >= sendTimeGiveUp) {
					CODE_PROBE(true, "Blob worker could not send merge candidate in time, re-checking status");
					break;
				}

				bwData->currentManagerStatusStream.get().send(GranuleStatusReply(metadata->keyRange,
				                                                                 false,
				                                                                 false,
				                                                                 false,
				                                                                 metadata->continueEpoch,
				                                                                 metadata->continueSeqno,
				                                                                 granuleID,
				                                                                 metadata->historyVersion,
				                                                                 invalidVersion,
				                                                                 true,
				                                                                 metadata->originalEpoch,
				                                                                 metadata->originalSeqno));
				// if a new manager appears, also tell it about this granule being mergeable
				// or if a new stream from the existing manager, it may have missed the message due to a network issue
				wait(bwData->currentManagerStatusStream.onChange());
				wait(delay(0));
				CODE_PROBE(true, "Blob worker re-sending merge candidate to new manager");
			} catch (Error& e) {
				if (e.code() == error_code_operation_cancelled) {
					throw e;
				}

				CODE_PROBE(true, "Blob worker re-sending merge candidate to manager after not error/not hearing back");

				// if we got broken promise while waiting, the old stream was killed, so we don't need to wait
				// on change, just retry
				if (e.code() == error_code_broken_promise) {
					wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				} else {
					wait(bwData->currentManagerStatusStream.onChange());
				}
			}
		}
	}
}

namespace {
void handleCompletedDeltaFile(Reference<BlobWorkerData> bwData,
                              Reference<GranuleMetadata> metadata,
                              BlobFileIndex completedDeltaFile,
                              Key cfKey,
                              Version cfStartVersion,
                              std::deque<std::pair<Version, Version>>* rollbacksCompleted,
                              std::deque<Future<Void>>& inFlightPops) {
	metadata->files.deltaFiles.push_back(completedDeltaFile);
	ASSERT(metadata->durableDeltaVersion.get() < completedDeltaFile.version);
	metadata->durableDeltaVersion.set(completedDeltaFile.version);

	if (completedDeltaFile.version > cfStartVersion) {
		if (BW_DEBUG) {
			fmt::print("Popping change feed {0} at {1}\n",
			           cfKeyToGranuleID(cfKey).toString().c_str(),
			           completedDeltaFile.version);
		}
		// FIXME: for a write-hot shard, we could potentially batch these and only pop the largest one after
		// several have completed
		// FIXME: since this is async, and worker could die, new blob worker that opens granule should probably
		// kick off an async pop at its previousDurableVersion after opening the granule to guarantee it is
		// eventually popped?
		Future<Void> popFuture = bwData->db->popChangeFeedMutations(cfKey, completedDeltaFile.version + 1);
		// Do pop asynchronously
		inFlightPops.push_back(popFuture);
	}
	while (!rollbacksCompleted->empty() && completedDeltaFile.version >= rollbacksCompleted->front().second) {
		if (BW_DEBUG) {
			fmt::print("Granule [{0} - {1}) on BW {2} completed rollback {3} -> {4} with delta file {5}\n",
			           metadata->keyRange.begin.printable().c_str(),
			           metadata->keyRange.end.printable().c_str(),
			           bwData->id.toString().substr(0, 5).c_str(),
			           rollbacksCompleted->front().second,
			           rollbacksCompleted->front().first,
			           completedDeltaFile.version);
		}
		rollbacksCompleted->pop_front();
	}
}

// if we get an i/o error updating files, or a rollback, reassign the granule to ourselves and start fresh
bool granuleCanRetry(const Error& e) {
	switch (e.code()) {
	case error_code_io_error:
	case error_code_io_timeout:
	// FIXME: handle connection errors in tighter retry loop around individual files.
	// FIXME: if these requests fail at a high enough rate, the whole worker should be marked as unhealthy and
	// its granules should be moved away, as there may be some problem with this host contacting blob storage
	case error_code_http_request_failed:
	case error_code_connection_failed:
	case error_code_lookup_failed: // dns
	case error_code_platform_error: // injected faults
		return true;
	default:
		return false;
	};
}
} // namespace

struct InFlightFile {
	Future<BlobFileIndex> future;
	Version version;
	uint64_t bytes;
	bool snapshot;

	InFlightFile(Future<BlobFileIndex> future, Version version, uint64_t bytes, bool snapshot)
	  : future(future), version(version), bytes(bytes), snapshot(snapshot) {}
};

namespace {
Version doGranuleRollback(Reference<GranuleMetadata> metadata,
                          Version mutationVersion,
                          Version rollbackVersion,
                          std::deque<InFlightFile>& inFlightFiles,
                          std::deque<std::pair<Version, Version>>& rollbacksInProgress,
                          std::deque<std::pair<Version, Version>>& rollbacksCompleted) {
	Version cfRollbackVersion;
	if (metadata->pendingDeltaVersion > rollbackVersion) {
		// if we already started writing mutations to a delta or snapshot file with version > rollbackVersion,
		// we need to rescind those delta file writes
		ASSERT(!inFlightFiles.empty());
		cfRollbackVersion = metadata->durableDeltaVersion.get();
		metadata->pendingSnapshotVersion = metadata->durableSnapshotVersion.get();
		int toPop = 0;
		bool pendingSnapshot = false;
		for (auto& f : inFlightFiles) {
			if (f.snapshot) {
				if (f.version > rollbackVersion) {
					CODE_PROBE(true, "Granule rollback cancelling snapshot file");
					if (BW_DEBUG) {
						fmt::print("[{0} - {1}) rollback cancelling snapshot file @ {2}\n",
						           metadata->keyRange.begin.printable(),
						           metadata->keyRange.end.printable(),
						           f.version);
					}
					f.future.cancel();
					toPop++;
				} else {
					metadata->pendingSnapshotVersion = f.version;
					metadata->bytesInNewDeltaFiles = 0;
					pendingSnapshot = true;
				}
			} else {
				if (f.version > rollbackVersion) {
					f.future.cancel();
					if (!pendingSnapshot) {
						metadata->bytesInNewDeltaFiles -= f.bytes;
					}
					toPop++;
					CODE_PROBE(true, "Granule rollback cancelling delta file");
					if (BW_DEBUG) {
						fmt::print("[{0} - {1}) rollback cancelling delta file @ {2}\n",
						           metadata->keyRange.begin.printable(),
						           metadata->keyRange.end.printable(),
						           f.version);
					}
				} else {
					ASSERT(f.version > cfRollbackVersion);
					cfRollbackVersion = f.version;
					if (pendingSnapshot) {
						metadata->bytesInNewDeltaFiles += f.bytes;
					}
				}
			}
		}
		ASSERT(toPop > 0);
		while (toPop > 0) {
			inFlightFiles.pop_back();
			toPop--;
		}
		metadata->pendingDeltaVersion = cfRollbackVersion;
		if (BW_DEBUG) {
			fmt::print("[{0} - {1}) rollback discarding all {2} in-memory mutations",
			           metadata->keyRange.begin.printable(),
			           metadata->keyRange.end.printable(),
			           metadata->currentDeltas.size());
			if (metadata->currentDeltas.size()) {
				fmt::print(
				    " {0} - {1}", metadata->currentDeltas.front().version, metadata->currentDeltas.back().version);
			}
			fmt::print("\n");
		}

		// discard all in-memory mutations
		metadata->currentDeltas = Standalone<GranuleDeltas>();
		metadata->bufferedDeltaBytes = 0;
		metadata->bufferedDeltaVersion = cfRollbackVersion;

		// Track that this rollback happened, since we have to re-read mutations up to the rollback
		// Add this rollback to in progress, and put all completed ones back in progress
		rollbacksInProgress.push_back(std::pair(rollbackVersion, mutationVersion));
		while (!rollbacksCompleted.empty()) {
			if (rollbacksCompleted.back().first >= cfRollbackVersion) {
				rollbacksInProgress.push_front(rollbacksCompleted.back());
				rollbacksCompleted.pop_back();
			} else {
				// some rollbacks in completed could still have a delta file in flight after this rollback, they
				// should remain in completed
				break;
			}
		}

	} else {
		// No pending delta files to discard, just in-memory mutations
		CODE_PROBE(true, "Granule rollback discarding in memory mutations");

		// FIXME: could binary search?
		int mIdx = metadata->currentDeltas.size() - 1;
		Version firstDiscarded = invalidVersion;
		Version lastDiscarded = invalidVersion;
		while (mIdx >= 0) {
			if (metadata->currentDeltas[mIdx].version <= rollbackVersion) {
				break;
			}
			for (auto& m : metadata->currentDeltas[mIdx].mutations) {
				metadata->bufferedDeltaBytes -= m.totalSize();
			}
			if (firstDiscarded == invalidVersion) {
				firstDiscarded = metadata->currentDeltas[mIdx].version;
			}
			lastDiscarded = metadata->currentDeltas[mIdx].version;
			mIdx--;
		}

		if (BW_DEBUG) {
			fmt::print("[{0} - {1}) rollback discarding {2} in-memory mutations",
			           metadata->keyRange.begin.printable(),
			           metadata->keyRange.end.printable(),
			           metadata->currentDeltas.size() - mIdx - 1);

			if (firstDiscarded != invalidVersion) {
				fmt::print(" {0} - {1}", lastDiscarded, firstDiscarded);
			}

			fmt::print(", {0} mutations", mIdx);
			if (mIdx >= 0) {
				fmt::print(
				    " ({0} - {1})", metadata->currentDeltas.front().version, metadata->currentDeltas[mIdx].version);
			}
			fmt::print(" and {0} bytes left\n", metadata->bufferedDeltaBytes);
		}

		if (mIdx < 0) {
			metadata->currentDeltas = Standalone<GranuleDeltas>();
			metadata->bufferedDeltaBytes = 0;
		} else {
			metadata->currentDeltas.resize(metadata->currentDeltas.arena(), mIdx + 1);
		}

		// delete all deltas in rollback range, but we can optimize here to just skip the uncommitted mutations
		// directly and immediately pop the rollback out of inProgress to completed

		metadata->bufferedDeltaVersion = rollbackVersion;
		cfRollbackVersion = mutationVersion;
		rollbacksCompleted.push_back(std::pair(rollbackVersion, mutationVersion));
	}

	if (BW_DEBUG) {
		fmt::print("[{0} - {1}) finishing rollback to {2}\n",
		           metadata->keyRange.begin.printable(),
		           metadata->keyRange.end.printable(),
		           cfRollbackVersion);
	}

	return cfRollbackVersion;
}
} // namespace

ACTOR Future<Void> waitOnCFVersion(Reference<GranuleMetadata> metadata, Version waitVersion) {
	loop {
		try {
			// if not valid, we're about to be cancelled anyway
			state Future<Void> atLeast = metadata->activeCFData.get().isValid()
			                                 ? metadata->activeCFData.get()->whenAtLeast(waitVersion)
			                                 : Never();
			choose {
				when(wait(atLeast)) { break; }
				when(wait(metadata->activeCFData.onChange())) {}
			}
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled || e.code() == error_code_change_feed_popped) {
				throw e;
			}

			// if waiting on a parent granule change feed and we change to the child, the parent will get
			// end_of_stream, which could cause this waiting whenAtLeast to get change_feed_cancelled. We should
			// simply retry and wait a bit, as blobGranuleUpdateFiles will switch to the new change feed
			wait(delay(0.05));
		}
	}

	// stop after change feed callback
	wait(delay(0, TaskPriority::BlobWorkerReadChangeFeed));

	return Void();
}

ACTOR Future<Void> waitCommittedGrv(Reference<BlobWorkerData> bwData,
                                    Reference<GranuleMetadata> metadata,
                                    Version version) {
	if (version > bwData->grvVersion.get()) {
		// this order is important, since we need to register a waiter on the notified version before waking the
		// GRV actor
		Future<Void> grvAtLeast = bwData->grvVersion.whenAtLeast(version);
		Promise<Void> doGrvCheck = bwData->doGRVCheck;
		if (doGrvCheck.canBeSet()) {
			doGrvCheck.send(Void());
		}
		wait(grvAtLeast);
	}

	Version grvVersion = bwData->grvVersion.get();
	wait(waitOnCFVersion(metadata, grvVersion));
	return Void();
}

ACTOR Future<Void> waitVersionCommitted(Reference<BlobWorkerData> bwData,
                                        Reference<GranuleMetadata> metadata,
                                        Version version) {
	// If GRV is way in the future, we know we can't roll back more than 5 seconds (or whatever this knob is set
	// to) worth of versions
	wait(waitCommittedGrv(bwData, metadata, version) ||
	     waitOnCFVersion(metadata, version + SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS));
	if (version > metadata->knownCommittedVersion) {
		metadata->knownCommittedVersion = version;
	}
	return Void();
}

ACTOR Future<bool> checkFileNotFoundForcePurgeRace(Reference<BlobWorkerData> bwData, KeyRange range) {
	state Transaction tr(bwData->db);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			ForcedPurgeState purgeState = wait(getForcePurgedState(&tr, range));
			return purgeState != ForcedPurgeState::NonePurged;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Does a force flush after consuming all data post-split from the parent granule's old change feed.
// This guarantees that all of the data from the old change feed gets persisted to blob storage, and the old feed can be
// cleaned up. This is particularly necessary for sequential workloads, where only one child granule after the split has
// new writes. Adds a delay so that, if the granule is not write-cold and would have written a delta file soon anyway,
// this does not add any extra overhead.
ACTOR Future<Void> forceFlushCleanup(Reference<BlobWorkerData> bwData, Reference<GranuleMetadata> metadata, Version v) {
	double cleanupDelay = SERVER_KNOBS->BLOB_WORKER_FORCE_FLUSH_CLEANUP_DELAY;
	if (cleanupDelay < 0) {
		return Void();
	}
	wait(delay(cleanupDelay));
	if (metadata->forceFlushVersion.get() < v && metadata->pendingDeltaVersion < v) {
		metadata->forceFlushVersion.set(v);
		++bwData->stats.forceFlushCleanups;
		if (BW_DEBUG) {
			fmt::print("Granule [{0} - {1}) forcing flush cleanup @ {2}\n",
			           metadata->keyRange.begin.printable(),
			           metadata->keyRange.end.printable(),
			           v);
		}
	}
	return Void();
}

// updater for a single granule
// TODO: this is getting kind of large. Should try to split out this actor if it continues to grow?
ACTOR Future<Void> blobGranuleUpdateFiles(Reference<BlobWorkerData> bwData,
                                          Reference<GranuleMetadata> metadata,
                                          Future<GranuleStartState> assignFuture,
                                          Future<Reference<BlobConnectionProvider>> bstoreFuture) {
	state Reference<BlobConnectionProvider> bstore;
	state std::deque<InFlightFile> inFlightFiles;
	state std::deque<Future<Void>> inFlightPops;
	state Future<Void> oldChangeFeedFuture;
	state Future<Void> changeFeedFuture;
	state Future<Void> checkMergeCandidate;
	state Future<Void> forceFlushCleanupFuture;
	state GranuleStartState startState;
	state bool readOldChangeFeed;
	state Key cfKey;
	state Optional<Key> oldCFKey;
	state int pendingSnapshots = 0;
	state Version lastForceFlushVersion = invalidVersion;
	state std::deque<Version> forceFlushVersions;
	state Future<Void> nextForceFlush = metadata->forceFlushVersion.whenAtLeast(1);

	state std::deque<std::pair<Version, Version>> rollbacksInProgress;
	state std::deque<std::pair<Version, Version>> rollbacksCompleted;
	state Future<Void> startDeltaFileWrite = Future<Void>(Void());

	state bool snapshotEligible; // just wrote a delta file or just took granule over from another worker
	state bool justDidRollback = false;

	try {
		// set resume snapshot so it's not valid until we pause to ask the blob manager for a re-snapshot
		metadata->resumeSnapshot.send(Void());

		// before starting, make sure worker persists range assignment, acquires the granule lock, and has a
		// blob store
		wait(store(startState, assignFuture));
		wait(store(bstore, bstoreFuture));

		wait(delay(0, TaskPriority::BlobWorkerUpdateStorage));

		cfKey = granuleIDToCFKey(startState.granuleID);
		if (startState.splitParentGranule.present()) {
			oldCFKey = granuleIDToCFKey(startState.splitParentGranule.get().second);
		}

		if (BW_DEBUG) {
			fmt::print("Granule File Updater Starting for [{0} - {1}) @ ({2}, {3}):\n",
			           metadata->keyRange.begin.printable(),
			           metadata->keyRange.end.printable(),
			           metadata->originalEpoch,
			           metadata->originalSeqno);
			fmt::print("  CFID: {} ({})\n", startState.granuleID.toString(), cfKey.printable());
			fmt::print("  CF Start Version: {}\n", startState.changeFeedStartVersion);
			fmt::print("  Previous Durable Version: {}\n", startState.previousDurableVersion);
			fmt::print("  doSnapshot={}\n", startState.doSnapshot ? "T" : "F");
			fmt::print("  Prev CFID: {}\n",
			           startState.splitParentGranule.present()
			               ? startState.splitParentGranule.get().second.toString().c_str()
			               : "");
			fmt::print("  blobFilesToSnapshot={}\n", startState.blobFilesToSnapshot.size());
		}

		state Version startVersion;
		state BlobFileIndex newSnapshotFile;

		// if this is a reassign, calculate how close to a snapshot the previous owner was
		if (startState.existingFiles.present()) {
			GranuleFiles files = startState.existingFiles.get();
			if (!files.snapshotFiles.empty() && !files.deltaFiles.empty()) {
				Version snapshotVersion = files.snapshotFiles.back().version;
				for (int i = files.deltaFiles.size() - 1; i >= 0; i--) {
					if (files.deltaFiles[i].version > snapshotVersion) {
						metadata->bytesInNewDeltaFiles += files.deltaFiles[i].length;
					}
				}
			}

			metadata->files = startState.existingFiles.get();
			snapshotEligible = true;
		}

		if (!startState.doSnapshot) {
			CODE_PROBE(true, "Granule moved without split");
			startVersion = startState.previousDurableVersion;
			ASSERT(!metadata->files.snapshotFiles.empty());
			metadata->pendingSnapshotVersion = metadata->files.snapshotFiles.back().version;
			metadata->durableSnapshotVersion.set(metadata->pendingSnapshotVersion);
			metadata->initialSnapshotVersion = metadata->files.snapshotFiles.front().version;
			metadata->historyVersion = startState.history.get().version;
		} else {
			if (!startState.blobFilesToSnapshot.empty()) {
				Version minDurableSnapshotV = MAX_VERSION;
				for (auto& it : startState.blobFilesToSnapshot) {
					minDurableSnapshotV = std::min(minDurableSnapshotV, it.snapshotFiles.back().version);
				}
				startVersion = startState.previousDurableVersion;
				Future<BlobFileIndex> inFlightBlobSnapshot = compactFromBlob(
				    bwData, bstore, metadata, startState.granuleID, startState.blobFilesToSnapshot, startVersion);
				inFlightFiles.push_back(InFlightFile(inFlightBlobSnapshot, startVersion, 0, true));
				pendingSnapshots++;

				metadata->durableSnapshotVersion.set(minDurableSnapshotV);
			} else {
				ASSERT(startState.previousDurableVersion == invalidVersion);
				BlobFileIndex fromFDB = wait(
				    dumpInitialSnapshotFromFDB(bwData, bstore, metadata, startState.granuleID, cfKey, &inFlightPops));
				newSnapshotFile = fromFDB;
				ASSERT(startState.changeFeedStartVersion <= fromFDB.version);
				startVersion = newSnapshotFile.version;
				metadata->files.snapshotFiles.push_back(newSnapshotFile);
				metadata->durableSnapshotVersion.set(startVersion);

				wait(yield(TaskPriority::BlobWorkerUpdateStorage));
			}
			metadata->initialSnapshotVersion = startVersion;
			metadata->pendingSnapshotVersion = startVersion;
			metadata->historyVersion = startState.history.present() ? startState.history.get().version : startVersion;
		}

		// No need to start Change Feed in full restore mode
		if (bwData->isFullRestoreMode)
			return Void();

		checkMergeCandidate = granuleCheckMergeCandidate(bwData,
		                                                 metadata,
		                                                 startState.granuleID,
		                                                 inFlightFiles.empty() ? Future<Void>(Void())
		                                                                       : success(inFlightFiles.back().future));

		metadata->durableDeltaVersion.set(startVersion);
		metadata->pendingDeltaVersion = startVersion;
		metadata->bufferedDeltaVersion = startVersion;
		metadata->knownCommittedVersion = startVersion;
		metadata->resetReadStats();

		Reference<ChangeFeedData> cfData = makeReference<ChangeFeedData>(bwData->db.getPtr());

		if (startState.splitParentGranule.present() && startVersion + 1 < startState.changeFeedStartVersion) {
			// read from parent change feed up until our new change feed is started
			// Required to have canReadPopped = false, otherwise another granule can take over the change feed,
			// and pop it. That could cause this worker to think it has the full correct set of data if it then
			// reads the data, until it checks the granule lock again. passing false for canReadPopped means we
			// will get an exception if we try to read any popped data, killing this actor
			readOldChangeFeed = true;

			// because several feeds will be reading the same version range of this change feed at the same time, set
			// cache result to true
			oldChangeFeedFuture = bwData->db->getChangeFeedStream(cfData,
			                                                      oldCFKey.get(),
			                                                      startVersion + 1,
			                                                      startState.changeFeedStartVersion,
			                                                      metadata->keyRange,
			                                                      bwData->changeFeedStreamReplyBufferSize,
			                                                      false,
			                                                      { ReadType::NORMAL, CacheResult::True });

		} else {
			readOldChangeFeed = false;
			changeFeedFuture = bwData->db->getChangeFeedStream(cfData,
			                                                   cfKey,
			                                                   startVersion + 1,
			                                                   MAX_VERSION,
			                                                   metadata->keyRange,
			                                                   bwData->changeFeedStreamReplyBufferSize,
			                                                   false);
			// in case previous worker died before popping the latest version, start another pop
			if (startState.previousDurableVersion != invalidVersion) {
				ASSERT(startState.previousDurableVersion + 1 >= startState.changeFeedStartVersion);
				Future<Void> popFuture =
				    bwData->db->popChangeFeedMutations(cfKey, startState.previousDurableVersion + 1);
				inFlightPops.push_back(popFuture);
			}
		}

		// Start actors BEFORE setting new change feed data to ensure the change feed data is properly
		// initialized by the client
		metadata->activeCFData.set(cfData);

		ASSERT(metadata->readable.canBeSet());
		metadata->readable.send(Void());

		loop {
			// check outstanding snapshot/delta files for completion
			while (inFlightFiles.size() > 0) {
				if (inFlightFiles.front().future.isReady()) {
					BlobFileIndex completedFile = wait(inFlightFiles.front().future);
					if (inFlightFiles.front().snapshot) {
						if (metadata->files.deltaFiles.empty()) {
							ASSERT(completedFile.version == metadata->initialSnapshotVersion);
						} else {
							ASSERT(completedFile.version == metadata->files.deltaFiles.back().version);
						}

						metadata->files.snapshotFiles.push_back(completedFile);
						metadata->durableSnapshotVersion.set(completedFile.version);
						pendingSnapshots--;
					} else {
						handleCompletedDeltaFile(bwData,
						                         metadata,
						                         completedFile,
						                         cfKey,
						                         startState.changeFeedStartVersion,
						                         &rollbacksCompleted,
						                         inFlightPops);
					}

					inFlightFiles.pop_front();
					wait(yield(TaskPriority::BlobWorkerUpdateStorage));
				} else {
					break;
				}
			}

			// also check outstanding pops for errors
			while (!inFlightPops.empty() && inFlightPops.front().isReady()) {
				wait(inFlightPops.front());
				inFlightPops.pop_front();
			}

			// inject delay into reading change feed stream
			if (BUGGIFY_WITH_PROB(0.001)) {
				wait(delay(deterministicRandom()->random01(), TaskPriority::BlobWorkerReadChangeFeed));
			} else {
				// FIXME: if we're already BlobWorkerReadChangeFeed, don't do a delay?
				wait(delay(0, TaskPriority::BlobWorkerReadChangeFeed));
			}

			state Standalone<VectorRef<MutationsAndVersionRef>> mutations;
			try {
				// Even if there are no new mutations, there still might be readers waiting on
				// durableDeltaVersion to advance. We need to check whether any outstanding files have finished
				// so we don't wait on mutations forever
				choose {
					when(Standalone<VectorRef<MutationsAndVersionRef>> _mutations =
					         waitNext(metadata->activeCFData.get()->mutations.getFuture())) {
						mutations = _mutations;
						ASSERT(!mutations.empty());
						if (readOldChangeFeed) {
							ASSERT(mutations.back().version < startState.changeFeedStartVersion);
						} else {
							ASSERT(mutations.front().version >= startState.changeFeedStartVersion);
						}

						if (mutations.front().version <= metadata->bufferedDeltaVersion) {
							fmt::print("ERROR: Mutations went backwards for granule [{0} - {1}). "
							           "bufferedDeltaVersion={2}, mutationVersion={3} !!!\n",
							           metadata->keyRange.begin.printable(),
							           metadata->keyRange.end.printable(),
							           metadata->bufferedDeltaVersion,
							           mutations.front().version);
						}
						ASSERT(mutations.front().version > metadata->bufferedDeltaVersion);

						// Check to see if change feed was popped while reading. If so, someone else owns this
						// granule and we are missing data. popVersion is exclusive, so last delta @ V means
						// popped up to V+1 is ok. Or in other words, if the last delta @ V, we only missed data
						// at V+1 onward if popVersion >= V+2
						if (metadata->bufferedDeltaVersion < metadata->activeCFData.get()->popVersion - 1) {
							CODE_PROBE(true, "Blob Worker detected popped", probe::decoration::rare);
							TraceEvent("BlobWorkerChangeFeedPopped", bwData->id)
							    .detail("Granule", metadata->keyRange)
							    .detail("GranuleID", startState.granuleID)
							    .detail("BufferedDeltaVersion", metadata->bufferedDeltaVersion)
							    .detail("MutationVersion", mutations.front().version)
							    .detail("PopVersion", metadata->activeCFData.get()->popVersion);
							throw change_feed_popped();
						}
					}
					when(wait(inFlightFiles.empty() ? Never() : success(inFlightFiles.front().future))) {}
					when(wait(nextForceFlush)) {
						if (forceFlushVersions.empty() ||
						    forceFlushVersions.back() < metadata->forceFlushVersion.get()) {
							forceFlushVersions.push_back(metadata->forceFlushVersion.get());
						}
						if (metadata->forceFlushVersion.get() > lastForceFlushVersion) {
							lastForceFlushVersion = metadata->forceFlushVersion.get();
						}
						nextForceFlush = metadata->forceFlushVersion.whenAtLeast(lastForceFlushVersion + 1);
					}
					when(wait(metadata->runRDC.getFuture())) {
						// return control flow back to the triggering actor before continuing
						wait(delay(0));
					}
				}
			} catch (Error& e) {
				// only error we should expect here is when we finish consuming old change feed
				if (e.code() != error_code_end_of_stream) {
					throw;
				}
				ASSERT(readOldChangeFeed);

				readOldChangeFeed = false;
				if (BW_DEBUG) {
					fmt::print("Granule [{0} - {1}) switching to new change feed {2} @ {3}, {4}\n",
					           metadata->keyRange.begin.printable(),
					           metadata->keyRange.end.printable(),
					           startState.granuleID.toString(),
					           metadata->bufferedDeltaVersion,
					           metadata->activeCFData.get()->getVersion());
				}
				ASSERT(metadata->bufferedDeltaVersion <= metadata->activeCFData.get()->getVersion());
				// update this for change feed popped detection
				metadata->bufferedDeltaVersion = metadata->activeCFData.get()->getVersion();
				forceFlushCleanupFuture = forceFlushCleanup(bwData, metadata, metadata->bufferedDeltaVersion);

				Reference<ChangeFeedData> cfData = makeReference<ChangeFeedData>(bwData->db.getPtr());

				changeFeedFuture = bwData->db->getChangeFeedStream(cfData,
				                                                   cfKey,
				                                                   startState.changeFeedStartVersion,
				                                                   MAX_VERSION,
				                                                   metadata->keyRange,
				                                                   bwData->changeFeedStreamReplyBufferSize,
				                                                   false);

				// Start actors BEFORE setting new change feed data to ensure the change feed data is properly
				// initialized by the client
				metadata->activeCFData.set(cfData);
			}

			// process mutations
			Version lastDeltaVersion = invalidVersion;
			bool processedAnyMutations = false;
			if (!mutations.empty()) {
				for (MutationsAndVersionRef deltas : mutations) {

					// Buffer mutations at this version. There should not be multiple MutationsAndVersionRef
					// with the same version
					ASSERT(deltas.version > metadata->bufferedDeltaVersion);
					ASSERT(deltas.version > lastDeltaVersion);
					// FIXME: this assert isn't true - why
					// ASSERT(!deltas.mutations.empty());
					if (!deltas.mutations.empty()) {
						if (deltas.mutations.size() == 1 && deltas.mutations.back().param1 == lastEpochEndPrivateKey) {
							// Note rollbackVerision is durable, [rollbackVersion+1 - deltas.version] needs to
							// be tossed For correctness right now, there can be no waits and yields either in
							// rollback handling or in handleBlobGranuleFileRequest once waitForVersion has
							// succeeded, otherwise this will race and clobber results
							Version rollbackVersion;
							BinaryReader br(deltas.mutations[0].param2, Unversioned());
							br >> rollbackVersion;

							ASSERT(rollbackVersion >= metadata->durableDeltaVersion.get());

							if (!rollbacksInProgress.empty()) {
								ASSERT(rollbacksInProgress.front().first == rollbackVersion);
								ASSERT(rollbacksInProgress.front().second == deltas.version);
								if (BW_DEBUG) {
									fmt::print("Passed rollback {0} -> {1}\n", deltas.version, rollbackVersion);
								}
								rollbacksCompleted.push_back(rollbacksInProgress.front());
								rollbacksInProgress.pop_front();
							} else {
								// FIXME: add counter for granule rollbacks and rollbacks skipped?
								// explicitly check last delta in currentDeltas because lastVersion and
								// bufferedDeltaVersion include empties
								if (metadata->pendingDeltaVersion <= rollbackVersion &&
								    (metadata->currentDeltas.empty() ||
								     metadata->currentDeltas.back().version <= rollbackVersion)) {
									CODE_PROBE(true, "Granule ignoring rollback");

									if (BW_DEBUG) {
										fmt::print("Granule [{0} - {1}) on BW {2} skipping rollback {3} -> {4} "
										           "completely\n",
										           metadata->keyRange.begin.printable().c_str(),
										           metadata->keyRange.end.printable().c_str(),
										           bwData->id.toString().substr(0, 5).c_str(),
										           deltas.version,
										           rollbackVersion);
									}
									// Still have to add to rollbacksCompleted. If we later roll the granule
									// back past this because of cancelling a delta file, we need to count this
									// as in progress so we can match the rollback mutation to a
									// rollbackInProgress when we restart the stream.
									rollbacksCompleted.push_back(std::pair(rollbackVersion, deltas.version));
								} else {
									CODE_PROBE(true, "Granule processing rollback");
									if (BW_DEBUG) {
										fmt::print("[{0} - {1}) on BW {2} ROLLBACK @ {3} -> {4}\n",
										           metadata->keyRange.begin.printable(),
										           metadata->keyRange.end.printable(),
										           bwData->id.toString().substr(0, 5).c_str(),
										           deltas.version,
										           rollbackVersion);
										TraceEvent(SevDebug, "GranuleRollback", bwData->id)
										    .detail("Granule", metadata->keyRange)
										    .detail("Version", deltas.version)
										    .detail("RollbackVersion", rollbackVersion);
									}

									Version oldPendingSnapshot = metadata->pendingSnapshotVersion;
									Version cfRollbackVersion = doGranuleRollback(metadata,
									                                              deltas.version,
									                                              rollbackVersion,
									                                              inFlightFiles,
									                                              rollbacksInProgress,
									                                              rollbacksCompleted);

									if (oldPendingSnapshot > metadata->pendingSnapshotVersion) {
										// If rollback cancelled in-flight snapshot, merge candidate checker also got
										// cancelled. Restart it
										CODE_PROBE(true,
										           "Restarting merge candidate checker after rolling back snapshot");
										checkMergeCandidate = granuleCheckMergeCandidate(
										    bwData,
										    metadata,
										    startState.granuleID,
										    inFlightFiles.empty() ? Future<Void>(Void())
										                          : success(inFlightFiles.back().future));
										metadata->resetReadStats();
									}
									// reset force flush state, requests should retry and add it back once feed is ready
									forceFlushVersions.clear();
									lastForceFlushVersion = 0;
									metadata->forceFlushVersion = NotifiedVersion();
									nextForceFlush = metadata->forceFlushVersion.whenAtLeast(1);

									Reference<ChangeFeedData> cfData =
									    makeReference<ChangeFeedData>(bwData->db.getPtr());

									if (!readOldChangeFeed &&
									    cfRollbackVersion + 1 < startState.changeFeedStartVersion) {
										// It isn't possible to roll back across the parent/child feed boundary,
										// but as part of rolling back we may need to cancel in-flight delta
										// files, and those delta files may include stuff from before the
										// parent/child boundary. So we have to go back to reading the old
										// change feed
										ASSERT(cfRollbackVersion >= startState.previousDurableVersion);
										ASSERT(cfRollbackVersion >= metadata->durableDeltaVersion.get());
										CODE_PROBE(true, "rollback crossed change feed boundaries");
										readOldChangeFeed = true;
										forceFlushCleanupFuture = Never();
									}

									if (readOldChangeFeed) {
										ASSERT(cfRollbackVersion + 1 < startState.changeFeedStartVersion);
										ASSERT(oldCFKey.present());
										// because several feeds will be reading the same version range of this change
										// feed at the same time, set cache result to true
										oldChangeFeedFuture =
										    bwData->db->getChangeFeedStream(cfData,
										                                    oldCFKey.get(),
										                                    cfRollbackVersion + 1,
										                                    startState.changeFeedStartVersion,
										                                    metadata->keyRange,
										                                    bwData->changeFeedStreamReplyBufferSize,
										                                    false,
										                                    { ReadType::NORMAL, CacheResult::True });

									} else {
										if (cfRollbackVersion + 1 < startState.changeFeedStartVersion) {
											fmt::print("Rollback past CF start??. rollback={0}, start={1}\n",
											           cfRollbackVersion,
											           startState.changeFeedStartVersion);
										}
										ASSERT(cfRollbackVersion + 1 >= startState.changeFeedStartVersion);

										changeFeedFuture =
										    bwData->db->getChangeFeedStream(cfData,
										                                    cfKey,
										                                    cfRollbackVersion + 1,
										                                    MAX_VERSION,
										                                    metadata->keyRange,
										                                    bwData->changeFeedStreamReplyBufferSize,
										                                    false);
									}

									// Start actors BEFORE setting new change feed data to ensure the change
									// feed data is properly initialized by the client
									metadata->activeCFData.set(cfData);

									justDidRollback = true;
									lastDeltaVersion = cfRollbackVersion;
									break;
								}
							}
						} else if (!rollbacksInProgress.empty() && rollbacksInProgress.front().first < deltas.version &&
						           rollbacksInProgress.front().second > deltas.version) {
							CODE_PROBE(true, "Granule skipping mutations b/c prior rollback");
							if (BW_DEBUG) {
								fmt::print("Skipping mutations @ {} b/c prior rollback\n", deltas.version);
							}
						} else {
							for (auto& delta : deltas.mutations) {
								metadata->bufferedDeltaBytes += delta.totalSize();
								bwData->stats.changeFeedInputBytes += delta.totalSize();
								bwData->stats.mutationBytesBuffered += delta.totalSize();

								DEBUG_MUTATION("BlobWorkerBuffer", deltas.version, delta, bwData->id)
								    .detail("Granule", metadata->keyRange)
								    .detail("ChangeFeedID",
								            cfKeyToGranuleID(readOldChangeFeed ? oldCFKey.get() : cfKey))
								    .detail("OldChangeFeed", readOldChangeFeed ? "T" : "F");
							}
							metadata->currentDeltas.push_back_deep(metadata->currentDeltas.arena(), deltas);

							processedAnyMutations = true;
							ASSERT(deltas.version != invalidVersion);
							ASSERT(deltas.version > lastDeltaVersion);
							lastDeltaVersion = deltas.version;
						}
					}
					if (justDidRollback) {
						break;
					}
				}
			}
			if (!justDidRollback && processedAnyMutations) {
				// update buffered version
				ASSERT(lastDeltaVersion != invalidVersion);
				ASSERT(lastDeltaVersion > metadata->bufferedDeltaVersion);

				// Update buffered delta version so new waitForVersion checks can bypass waiting entirely
				metadata->bufferedDeltaVersion = lastDeltaVersion;
			}
			justDidRollback = false;

			// Write a new delta file IF we have enough bytes OR force flush.
			// The force flush contract is a version cannot be put in forceFlushVersion unless the change feed
			// is already whenAtLeast that version
			bool forceFlush = !forceFlushVersions.empty() && forceFlushVersions.back() > metadata->pendingDeltaVersion;
			bool doReadDrivenFlush = !metadata->currentDeltas.empty() && metadata->doReadDrivenCompaction();
			CODE_PROBE(forceFlush, "Force flushing granule");
			if (metadata->bufferedDeltaBytes >= SERVER_KNOBS->BG_DELTA_FILE_TARGET_BYTES || forceFlush ||
			    doReadDrivenFlush) {
				TraceEvent(SevDebug, "BlobGranuleDeltaFile", bwData->id)
				    .detail("Granule", metadata->keyRange)
				    .detail("Version", lastDeltaVersion);

				// sanity check for version order
				if (forceFlush || doReadDrivenFlush) {
					if (lastDeltaVersion == invalidVersion) {
						lastDeltaVersion = metadata->bufferedDeltaVersion;
					}
					if (!forceFlushVersions.empty() && lastDeltaVersion < forceFlushVersions.back()) {
						if (BW_DEBUG) {
							fmt::print("Granule [{0} - {1}) force flushing delta version {2} -> {3}\n",
							           metadata->keyRange.begin.printable(),
							           metadata->keyRange.end.printable(),
							           lastDeltaVersion,
							           forceFlushVersions.back());
						}
						lastDeltaVersion = forceFlushVersions.back();
					}
				}
				if (!metadata->currentDeltas.empty()) {
					ASSERT(lastDeltaVersion >= metadata->currentDeltas.back().version);
					ASSERT(metadata->pendingDeltaVersion < metadata->currentDeltas.front().version);
				} else {
					// FIXME: could always write special metadata for empty file, so we don't actually
					// write/read a bunch of empty blob files
					ASSERT(forceFlush);
					ASSERT(!forceFlushVersions.empty());
					CODE_PROBE(true, "Force flushing empty delta file!");
				}

				// launch pipelined, but wait for previous operation to complete before persisting to FDB
				Future<BlobFileIndex> previousFuture;
				if (!inFlightFiles.empty()) {
					previousFuture = inFlightFiles.back().future;
				} else {
					previousFuture = Future<BlobFileIndex>(BlobFileIndex());
				}

				// The last version included in the old change feed is startState.cfStartVersion - 1.
				// So if the previous delta file did not include this version, and the new delta file does, the old
				// change feed is considered complete.
				Optional<std::pair<KeyRange, UID>> oldChangeFeedDataComplete;
				if (startState.splitParentGranule.present() &&
				    metadata->pendingDeltaVersion + 1 < startState.changeFeedStartVersion &&
				    lastDeltaVersion + 1 >= startState.changeFeedStartVersion) {
					oldChangeFeedDataComplete = startState.splitParentGranule.get();
				}

				if (BW_DEBUG) {
					fmt::print("Granule [{0} - {1}) flushing delta file after {2} bytes @ {3} {4}\n",
					           metadata->keyRange.begin.printable(),
					           metadata->keyRange.end.printable(),
					           metadata->bufferedDeltaBytes,
					           lastDeltaVersion,
					           oldChangeFeedDataComplete.present() ? ". Finalizing " : "");
				}

				startDeltaFileWrite = bwData->deltaWritesLock->take();
				Future<BlobFileIndex> dfFuture =
				    writeDeltaFile(bwData,
				                   bstore,
				                   metadata->keyRange,
				                   startState.granuleID,
				                   metadata->originalEpoch,
				                   metadata->originalSeqno,
				                   metadata->currentDeltas,
				                   lastDeltaVersion,
				                   previousFuture,
				                   waitVersionCommitted(bwData, metadata, lastDeltaVersion),
				                   oldChangeFeedDataComplete,
				                   startDeltaFileWrite);
				inFlightFiles.push_back(InFlightFile(dfFuture, lastDeltaVersion, metadata->bufferedDeltaBytes, false));

				// add new pending delta file
				ASSERT(metadata->pendingDeltaVersion < lastDeltaVersion);
				metadata->pendingDeltaVersion = lastDeltaVersion;
				ASSERT(metadata->bufferedDeltaVersion <= lastDeltaVersion);
				metadata->bufferedDeltaVersion = lastDeltaVersion; // In case flush was forced at non-mutation version
				metadata->bytesInNewDeltaFiles += metadata->bufferedDeltaBytes;

				bwData->stats.mutationBytesBuffered -= metadata->bufferedDeltaBytes;

				// reset current deltas
				metadata->currentDeltas = Standalone<GranuleDeltas>();
				metadata->bufferedDeltaBytes = 0;

				while (!forceFlushVersions.empty() && forceFlushVersions.front() <= lastDeltaVersion) {
					forceFlushVersions.pop_front();
				}

				// if we just wrote a delta file, check if we need to compact here.
				// exhaust old change feed before compacting - otherwise we could end up with an endlessly
				// growing list of previous change feeds in the worst case.
				snapshotEligible = true;

				// Wait on delta file starting here. If we have too many pending delta file writes, we need to not
				// continue to consume from the change feed, as that will pile on even more delta files to write
				wait(startDeltaFileWrite);
			} else if (metadata->doReadDrivenCompaction()) {
				ASSERT(metadata->currentDeltas.empty());
				snapshotEligible = true;
			}

			// FIXME: if we're still reading from old change feed, we should probably compact if we're
			// making a bunch of extra delta files at some point, even if we don't consider it for a split
			// yet

			// If we have enough delta files, try to re-snapshot
			if (snapshotEligible && (metadata->doReadDrivenCompaction() ||
			                         metadata->bytesInNewDeltaFiles >= SERVER_KNOBS->BG_DELTA_BYTES_BEFORE_COMPACT)) {
				if (BW_DEBUG && !inFlightFiles.empty()) {
					fmt::print("Granule [{0} - {1}) ready to re-snapshot at {2} after {3} > {4} bytes, "
					           "waiting for "
					           "outstanding {5} files to finish\n",
					           metadata->keyRange.begin.printable(),
					           metadata->keyRange.end.printable(),
					           metadata->pendingDeltaVersion,
					           metadata->bytesInNewDeltaFiles,
					           SERVER_KNOBS->BG_DELTA_BYTES_BEFORE_COMPACT,
					           inFlightFiles.size());
				}

				// cancel previous candidate checker
				checkMergeCandidate.cancel();

				// Speculatively assume we will get the range back. This is both a performance optimization,
				// and necessary to keep consuming versions from the change feed so that we can realize our
				// last delta file is committed and write it

				Future<BlobFileIndex> previousFuture;
				if (!inFlightFiles.empty()) {
					previousFuture = inFlightFiles.back().future;
					ASSERT(!inFlightFiles.back().snapshot);
				} else {
					previousFuture = Future<BlobFileIndex>(metadata->files.deltaFiles.back());
				}
				int64_t versionsSinceLastSnapshot = metadata->pendingDeltaVersion - metadata->pendingSnapshotVersion;
				Future<BlobFileIndex> inFlightBlobSnapshot;
				if (metadata->pendingDeltaVersion >= startState.changeFeedStartVersion) {
					inFlightBlobSnapshot = checkSplitAndReSnapshot(bwData,
					                                               bstore,
					                                               metadata,
					                                               startState.granuleID,
					                                               metadata->bytesInNewDeltaFiles,
					                                               previousFuture,
					                                               versionsSinceLastSnapshot);
				} else {
					inFlightBlobSnapshot =
					    reSnapshotNoCheck(bwData, bstore, metadata, startState.granuleID, previousFuture);
				}
				inFlightFiles.push_back(InFlightFile(inFlightBlobSnapshot, metadata->pendingDeltaVersion, 0, true));
				pendingSnapshots++;

				metadata->pendingSnapshotVersion = metadata->pendingDeltaVersion;

				// reset metadata
				metadata->bytesInNewDeltaFiles = 0;
				metadata->resetReadStats();

				// If we have more than one snapshot file and that file is unblocked (committedVersion >=
				// snapshotVersion), wait for it to finish

				if (pendingSnapshots > 1) {
					state int waitIdx = 0;
					int idx = 0;
					Version safeVersion =
					    std::max(metadata->knownCommittedVersion,
					             metadata->bufferedDeltaVersion - SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS);
					for (auto& f : inFlightFiles) {
						if (f.snapshot && f.version < metadata->pendingSnapshotVersion && f.version <= safeVersion) {
							if (BW_DEBUG) {
								fmt::print("[{0} - {1}) Waiting on previous snapshot file @ {2} <= {3}\n",
								           metadata->keyRange.begin.printable(),
								           metadata->keyRange.end.printable(),
								           f.version,
								           safeVersion);
							}
							waitIdx = idx + 1;
						}
						idx++;
					}
					while (waitIdx > 0) {
						CODE_PROBE(true, "Granule blocking on previous snapshot");
						// TODO don't duplicate code
						BlobFileIndex completedFile = wait(inFlightFiles.front().future);
						if (inFlightFiles.front().snapshot) {
							if (metadata->files.deltaFiles.empty()) {
								ASSERT(completedFile.version == metadata->initialSnapshotVersion);
							} else {
								ASSERT(completedFile.version == metadata->files.deltaFiles.back().version);
							}
							metadata->files.snapshotFiles.push_back(completedFile);
							metadata->durableSnapshotVersion.set(completedFile.version);
							pendingSnapshots--;
						} else {
							handleCompletedDeltaFile(bwData,
							                         metadata,
							                         completedFile,
							                         cfKey,
							                         startState.changeFeedStartVersion,
							                         &rollbacksCompleted,
							                         inFlightPops);
						}

						inFlightFiles.pop_front();
						waitIdx--;
						wait(yield(TaskPriority::BlobWorkerUpdateStorage));
					}
				}

				// restart merge candidate checker
				checkMergeCandidate = granuleCheckMergeCandidate(
				    bwData,
				    metadata,
				    startState.granuleID,
				    inFlightFiles.empty() ? Future<Void>(Void()) : success(inFlightFiles.back().future));

			} else if (snapshotEligible &&
			           metadata->bytesInNewDeltaFiles >= SERVER_KNOBS->BG_DELTA_BYTES_BEFORE_COMPACT) {
				// if we're in the old change feed case and can't snapshot but we have enough data to, don't
				// queue too many files in parallel, and slow down change feed consuming to let file writing
				// catch up

				CODE_PROBE(true, "Granule processing long tail of old change feed", probe::decoration::rare);
				if (inFlightFiles.size() > 10 && inFlightFiles.front().version <= metadata->knownCommittedVersion) {
					if (BW_DEBUG) {
						fmt::print("[{0} - {1}) Waiting on delta file b/c old change feed\n",
						           metadata->keyRange.begin.printable(),
						           metadata->keyRange.end.printable());
					}
					choose {
						when(BlobFileIndex completedDeltaFile = wait(inFlightFiles.front().future)) {}
						when(wait(delay(0.1))) {}
					}
				}
			}
			snapshotEligible = false;
		}
	} catch (Error& e) {
		if (BW_DEBUG) {
			fmt::print("Granule file updater for [{0} - {1}) got error {2}, exiting\n",
			           metadata->keyRange.begin.printable(),
			           metadata->keyRange.end.printable(),
			           e.name());
		}
		// Free last change feed data
		metadata->activeCFData.set(Reference<ChangeFeedData>());

		if (e.code() == error_code_operation_cancelled) {
			throw;
		}

		if (metadata->cancelled.canBeSet()) {
			metadata->cancelled.send(Void());
		}

		if (e.code() == error_code_granule_assignment_conflict) {
			TraceEvent("GranuleAssignmentConflict", bwData->id)
			    .detail("Granule", metadata->keyRange)
			    .detail("GranuleID", startState.granuleID);
			return Void();
		}
		if (e.code() == error_code_change_feed_popped) {
			TraceEvent("GranuleChangeFeedPopped", bwData->id)
			    .detail("Granule", metadata->keyRange)
			    .detail("GranuleID", startState.granuleID);
			return Void();
		}
		if (e.code() == error_code_change_feed_not_registered) {
			TraceEvent(SevInfo, "GranuleDestroyed", bwData->id)
			    .detail("Granule", metadata->keyRange)
			    .detail("GranuleID", startState.granuleID);
			return Void();
		}
		++bwData->stats.granuleUpdateErrors;

		if (granuleCanRetry(e)) {
			CODE_PROBE(true, "Granule close and re-open on error");
			TraceEvent("GranuleFileUpdaterRetriableError", bwData->id)
			    .error(e)
			    .detail("Granule", metadata->keyRange)
			    .detail("GranuleID", startState.granuleID);
			// explicitly cancel all outstanding write futures BEFORE updating promise stream, to ensure
			// they can't update files after the re-assigned granule acquires the lock do it backwards
			// though because future depends on previous one, so it could cause a cascade
			for (int i = inFlightFiles.size() - 1; i >= 0; i--) {
				inFlightFiles[i].future.cancel();
			}

			// if we retry and re-open, we need to use a normal request (no continue) and update the
			// seqno
			metadata->originalReq.managerEpoch = metadata->continueEpoch;
			metadata->originalReq.managerSeqno = metadata->continueSeqno;
			metadata->originalReq.type = AssignRequestType::Normal;

			bwData->granuleUpdateErrors.send(metadata->originalReq);
			throw e;
		}

		state Error e2 = e;
		if (e.code() == error_code_file_not_found) {
			// FIXME: better way to fix this?
			bool isForcePurging = wait(checkFileNotFoundForcePurgeRace(bwData, metadata->keyRange));
			if (isForcePurging) {
				CODE_PROBE(true, "Granule got file not found from force purge", probe::decoration::rare);
				TraceEvent("GranuleFileUpdaterFileNotFoundForcePurge", bwData->id)
				    .error(e2)
				    .detail("KeyRange", metadata->keyRange)
				    .detail("GranuleID", startState.granuleID);
				return Void();
			}
		}

		TraceEvent(SevError, "GranuleFileUpdaterUnexpectedError", bwData->id)
		    .error(e2)
		    .detail("Granule", metadata->keyRange)
		    .detail("GranuleID", startState.granuleID);
		ASSERT_WE_THINK(false);

		// if not simulation, kill the BW
		if (bwData->fatalError.canBeSet()) {
			bwData->fatalError.sendError(e2);
		}
		throw e2;
	}
}

// <start version, granule id>
using OrderedHistoryKey = std::pair<Version, UID>;

struct ForwardHistoryValue {
	std::vector<OrderedHistoryKey> childGranules;
	Reference<GranuleHistoryEntry> entry;
};

static int64_t nextHistoryLoadId = 0;

// walk graph back to previous known version
// Once loaded, go reverse direction, inserting each into the graph and setting its parent pointer.
// If a racing granule already loaded a prefix of the history, skip inserting entries already present
// For the optimization that future and racing loads can reuse previous loads, a granule load must load all
// transitive parent granules, not just ones that intersect its own range.
ACTOR Future<Void> blobGranuleLoadHistory(Reference<BlobWorkerData> bwData,
                                          Reference<GranuleMetadata> metadata,
                                          Future<GranuleStartState> assignFuture) {
	try {
		GranuleStartState startState = wait(assignFuture);
		state Optional<GranuleHistory> activeHistory = startState.history;

		if (activeHistory.present() && activeHistory.get().value.parentVersions.size() > 0) {
			state int64_t loadId = nextHistoryLoadId++;
			if (BW_HISTORY_DEBUG) {
				fmt::print("HL {0} {1}) Loading history data for [{2} - {3})\n",
				           bwData->id.shortString().substr(0, 5),
				           loadId,
				           metadata->keyRange.begin.printable(),
				           metadata->keyRange.end.printable());
			}
			state std::unordered_map<UID, ForwardHistoryValue> forwardHistory;
			state std::deque<GranuleHistory> queue;
			// important this sorts by lower version
			state
			    std::priority_queue<OrderedHistoryKey, std::vector<OrderedHistoryKey>, std::greater<OrderedHistoryKey>>
			        rootGranules;
			state Transaction tr(bwData->db);
			if (!activeHistory.get().value.parentVersions.empty()) {
				if (BW_HISTORY_DEBUG) {
					fmt::print("HL {0} {1}) Starting history [{2} - {3}) @ {4}\n",
					           bwData->id.shortString().substr(0, 5),
					           loadId,
					           activeHistory.get().range.begin.printable(),
					           activeHistory.get().range.end.printable(),
					           activeHistory.get().version);
				}
				queue.push_back(activeHistory.get());
			}

			// while the start version of the current granule is not past already loaded metadata, walk
			// backwards
			while (!queue.empty()) {
				state GranuleHistory curHistory = queue.front();
				queue.pop_front();
				state GranuleHistory next;

				if (BW_HISTORY_DEBUG) {
					fmt::print("HL {0} {1}) [{2} - {3}) @ {4}: Loading\n",
					           bwData->id.shortString().substr(0, 5),
					           loadId,
					           curHistory.range.begin.printable(),
					           curHistory.range.end.printable(),
					           curHistory.version);
				}

				auto prev = bwData->granuleHistory.intersectingRanges(curHistory.range);
				bool allLess = true; // if the version is less than all existing granules
				for (auto& it : prev) {
					if (it.cvalue().isValid() && curHistory.version >= it.cvalue()->endVersion) {
						allLess = false;
						break;
					} else if (!it.cvalue().isValid()) {
						allLess = false;
						break;
					} else {
						if (BW_HISTORY_DEBUG) {
							fmt::print("HL {0} {1}) [{2} - {3}) @ {4}:    Superceded by existing [{5} - "
							           "{6}) @ {7}\n",
							           bwData->id.shortString().substr(0, 5),
							           loadId,
							           curHistory.range.begin.printable(),
							           curHistory.range.end.printable(),
							           curHistory.version,
							           it.cvalue()->range.begin.printable(),
							           it.cvalue()->range.end.printable(),
							           it.cvalue()->endVersion);
						}
					}
				}
				if (allLess) {
					if (BW_HISTORY_DEBUG) {
						fmt::print("HL {0} {1}) [{2} - {3}) @ {4}: root b/c superceded\n",
						           bwData->id.shortString().substr(0, 5),
						           loadId,
						           curHistory.range.begin.printable(),
						           curHistory.range.end.printable(),
						           curHistory.version);
					}
					rootGranules.push(OrderedHistoryKey(curHistory.version, curHistory.value.granuleID));
					continue;
				}

				state int pIdx = 0;
				state bool anyParentsMissing = curHistory.value.parentVersions.empty();
				state std::vector<GranuleHistory> nexts;
				nexts.reserve(curHistory.value.parentVersions.size());
				// FIXME: parallelize this for all parents/all entries in queue?
				loop {
					if (pIdx >= curHistory.value.parentVersions.size()) {
						break;
					}
					try {
						tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
						tr.setOption(FDBTransactionOptions::LOCK_AWARE);
						state KeyRangeRef parentRange(curHistory.value.parentBoundaries[pIdx],
						                              curHistory.value.parentBoundaries[pIdx + 1]);
						state Version parentVersion = curHistory.value.parentVersions[pIdx];
						Optional<Value> v = wait(tr.get(blobGranuleHistoryKeyFor(parentRange, parentVersion)));
						if (v.present()) {
							next = GranuleHistory(parentRange, parentVersion, decodeBlobGranuleHistoryValue(v.get()));
							ASSERT(next.version != invalidVersion);

							auto inserted = forwardHistory.insert({ next.value.granuleID, ForwardHistoryValue() });
							inserted.first->second.childGranules.push_back(
							    OrderedHistoryKey(curHistory.version, curHistory.value.granuleID));
							if (inserted.second) {
								// granule next.granuleID goes from the version range [next.version,
								// curHistory.version]
								inserted.first->second.entry = makeReference<GranuleHistoryEntry>(
								    next.range, next.value.granuleID, next.version, curHistory.version);
								nexts.push_back(next);
								if (BW_HISTORY_DEBUG) {
									fmt::print("HL {0} {1}) [{2} - {3}) @ {4}: loaded parent [{5} - {6}) @ {7}\n",
									           bwData->id.shortString().substr(0, 5),
									           loadId,
									           curHistory.range.begin.printable(),
									           curHistory.range.end.printable(),
									           curHistory.version,
									           next.range.begin.printable(),
									           next.range.end.printable(),
									           next.version);
								}
							} else {
								CODE_PROBE(true, "duplicate parent in granule history (split then merge)");
								if (BW_HISTORY_DEBUG) {
									fmt::print("HL {0} {1}) [{2} - {3}) @ {4}: duplicate parent [{5} - "
									           "{6}) @ {7}\n",
									           bwData->id.shortString().substr(0, 5),
									           loadId,
									           curHistory.range.begin.printable(),
									           curHistory.range.end.printable(),
									           curHistory.version,
									           next.range.begin.printable(),
									           next.range.end.printable(),
									           next.version);
								}
								ASSERT(inserted.first->second.entry->endVersion == curHistory.version);
							}
						} else {
							anyParentsMissing = true;
						}

						pIdx++;
					} catch (Error& e) {
						wait(tr.onError(e));
					}
				}

				if (anyParentsMissing) {
					if (BW_HISTORY_DEBUG) {
						fmt::print("HL {0} {1}) [{2} - {3}) @ {4}: root b/c parents missing\n",
						           bwData->id.shortString().substr(0, 5),
						           loadId,
						           curHistory.range.begin.printable(),
						           curHistory.range.end.printable(),
						           curHistory.version);
					}

					rootGranules.push(OrderedHistoryKey(curHistory.version, curHistory.value.granuleID));
				} else {
					for (auto& it : nexts) {
						queue.push_back(it);
					}
				}
			}

			if (BW_HISTORY_DEBUG) {
				fmt::print("HL {0} {1}) Done loading, processing {2}\n",
				           bwData->id.shortString().substr(0, 5),
				           loadId,
				           rootGranules.size());
			}

			state int loadedCount = 0;
			state int skippedCount = 0;
			while (!rootGranules.empty()) {
				OrderedHistoryKey cur = rootGranules.top();
				rootGranules.pop();

				if (BW_HISTORY_DEBUG) {
					fmt::print("HL {0} {1}): Checking process {2}\n",
					           bwData->id.shortString().substr(0, 5),
					           loadId,
					           cur.second.shortString().substr(0, 6));
				}

				auto val = forwardHistory.find(cur.second);
				if (val == forwardHistory.end()) {
					continue;
				}

				if (BW_HISTORY_DEBUG) {
					fmt::print("HL {0} {1}) [{2} - {3}) @ {4}: Processing {5}\n",
					           bwData->id.shortString().substr(0, 5),
					           loadId,
					           val->second.entry->range.begin.printable(),
					           val->second.entry->range.end.printable(),
					           cur.first,
					           cur.second.shortString().substr(0, 6));
				}

				auto intersectingRanges = bwData->granuleHistory.intersectingRanges(val->second.entry->range);

				int intersectingCount = 0;
				bool foundDuplicate = false;
				std::vector<std::pair<KeyRange, Reference<GranuleHistoryEntry>>> newerHistory;
				for (auto& r : intersectingRanges) {
					intersectingCount++;
					if (r.cvalue().isValid() && r.cvalue()->endVersion == val->second.entry->endVersion) {
						// this granule is already in the history.
						foundDuplicate = true;
						break;
					} else if (r.cvalue().isValid() && r.cvalue()->endVersion > val->second.entry->endVersion) {
						if (BW_HISTORY_DEBUG) {
							fmt::print("HL {0} {1}) [{2} - {3}) @ {4}:    Superceded by existing [{5} - "
							           "{6}) @ {7}\n",
							           bwData->id.shortString().substr(0, 5),
							           loadId,
							           val->second.entry->range.begin.printable(),
							           val->second.entry->range.end.printable(),
							           cur.first,
							           r.cvalue()->range.begin.printable(),
							           r.cvalue()->range.end.printable(),
							           r.cvalue()->endVersion);
						}
						newerHistory.push_back(std::make_pair(r.range(), r.value()));
					} else if (r.value().isValid() && r.cvalue()->endVersion == val->second.entry->startVersion) {
						if (BW_HISTORY_DEBUG) {
							fmt::print("HL {0} {1}) [{2} - {3}) @ {4}:    Adding existing parent [{5} - "
							           "{6}) @ {7}\n",
							           bwData->id.shortString().substr(0, 5),
							           loadId,
							           val->second.entry->range.begin.printable(),
							           val->second.entry->range.end.printable(),
							           cur.first,
							           r.cvalue()->range.begin.printable(),
							           r.cvalue()->range.end.printable(),
							           r.cvalue()->endVersion);
						}
						val->second.entry->parentGranules.push_back(r.value());
					}
				}

				if (!foundDuplicate && !val->second.entry->parentGranules.empty() &&
				    val->second.entry->parentGranules.size() < intersectingCount) {
					// parents did not cover whole granule space, then no parents (could have been pruned or
					// other issues)
					val->second.entry->parentGranules.clear();
					if (BW_HISTORY_DEBUG) {
						fmt::print("HL {0} {1}) [{2} - {3}) @ {4}:    Clearing parents\n",
						           bwData->id.shortString().substr(0, 5),
						           loadId,
						           val->second.entry->range.begin.printable(),
						           val->second.entry->range.end.printable(),
						           cur.first);
					}
				}

				// only insert if this granule is not already in the history, or its key range is not
				// covered by later child granules in the history
				if (!foundDuplicate && newerHistory.size() < intersectingCount) {
					loadedCount++;
					if (BW_HISTORY_DEBUG) {
						fmt::print("HL {0} {1}) [{2} - {3}) @ {4}:    Adding to history\n",
						           bwData->id.shortString().substr(0, 5),
						           loadId,
						           val->second.entry->range.begin.printable(),
						           val->second.entry->range.end.printable(),
						           cur.first);
					}
					// not all granules newer, insert this guy
					bwData->granuleHistory.insert(val->second.entry->range, val->second.entry);

					// insert any newer granules over this one to maintain history space
					for (auto& it : newerHistory) {
						bwData->granuleHistory.insert(it.first, it.second);
					}
				} else {
					if (BW_HISTORY_DEBUG) {
						fmt::print("HL {0} {1}) [{2} - {3}) @ {4}:    Skipping\n",
						           bwData->id.shortString().substr(0, 5),
						           loadId,
						           val->second.entry->range.begin.printable(),
						           val->second.entry->range.end.printable(),
						           cur.first);
					}
					skippedCount++;
				}

				for (auto& c : val->second.childGranules) {
					// TODO: check for visited (and erasing) before push instead of before pop - a bit more
					// efficient
					if (BW_HISTORY_DEBUG) {
						fmt::print("HL {0} {1}) [{2} - {3}) @ {4}:    Queueing child {5} @ {6}\n",
						           bwData->id.shortString().substr(0, 5),
						           loadId,
						           val->second.entry->range.begin.printable(),
						           val->second.entry->range.end.printable(),
						           cur.first,
						           c.second.shortString().substr(0, 6),
						           c.first);
					}
					rootGranules.push(c);
				}

				// erase this granule so we don't re-process
				forwardHistory.erase(val);

				wait(yield());
			}

			if (BW_HISTORY_DEBUG) {
				fmt::print("Loaded {0} history entries for granule [{1} - {2}) ({3} skipped)\n",
				           loadedCount,
				           metadata->keyRange.begin.printable(),
				           metadata->keyRange.end.printable(),
				           skippedCount);
			}
		}

		metadata->historyLoaded.send(Void());
		return Void();
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled) {
			throw e;
		}
		if (e.code() == error_code_granule_assignment_conflict) {
			return Void();
		}
		// SplitStorageMetrics explicitly has a SevError if it gets an error, so no errors should propagate
		// here
		TraceEvent(SevError, "BlobWorkerUnexpectedErrorLoadGranuleHistory", bwData->id).error(e);
		ASSERT_WE_THINK(false);

		// if not simulation, kill the BW
		if (bwData->fatalError.canBeSet()) {
			bwData->fatalError.sendError(e);
		}
		throw e;
	}
}

struct sort_result_chunks {
	inline bool operator()(const std::pair<KeyRange, Future<GranuleFiles>>& chunk1,
	                       const std::pair<KeyRange, Future<GranuleFiles>>& chunk2) {
		return (chunk1.first.begin < chunk2.first.begin);
	}
};

namespace {
int64_t nextHistoryQueryId = 0;
std::vector<std::pair<KeyRange, Future<GranuleFiles>>> loadHistoryChunks(Reference<BlobWorkerData> bwData,
                                                                         Version expectedEndVersion,
                                                                         KeyRange keyRange,
                                                                         Version readVersion) {
	std::unordered_set<UID> visited;
	std::deque<Reference<GranuleHistoryEntry>> queue;
	std::vector<std::pair<KeyRange, Future<GranuleFiles>>> resultChunks;
	int64_t hqId = nextHistoryQueryId++;

	if (BW_HISTORY_DEBUG) {
		fmt::print("HQ {0} {1}) [{2} - {3}) @ {4}: Starting Query\n",
		           bwData->id.shortString().substr(0, 5),
		           hqId,
		           keyRange.begin.printable(),
		           keyRange.end.printable(),
		           readVersion);
	}

	auto parents = bwData->granuleHistory.intersectingRanges(keyRange);

	for (auto it : parents) {
		if (!it.cvalue().isValid()) {
			throw blob_granule_transaction_too_old();
		}
		if (expectedEndVersion > it.cvalue()->endVersion) {
			if (BW_DEBUG) {
				// history must have been pruned up to live granule, but BW still has previous history cached.
				fmt::print("live granule history version {0} for [{1} - {2}) != history end version {3} for "
				           "[{4} - {5}) on BW {6}\n",
				           expectedEndVersion,
				           keyRange.begin.printable(),
				           keyRange.end.printable(),
				           it.cvalue()->endVersion,
				           it.begin().printable(),
				           it.end().printable(),
				           bwData->id.toString().substr(0, 5));
			}
			throw blob_granule_transaction_too_old();
		}
		visited.insert(it.cvalue()->granuleID);
		queue.push_back(it.cvalue());

		if (BW_HISTORY_DEBUG) {
			fmt::print("HQ {0} {1}) [{2} - {3}) @ {4}:     Adding immediate parent [{5} - {6}) @ {7} - {8}\n",
			           bwData->id.shortString().substr(0, 5),
			           hqId,
			           keyRange.begin.printable(),
			           keyRange.end.printable(),
			           readVersion,
			           it.cvalue()->range.begin.printable(),
			           it.cvalue()->range.end.printable(),
			           it.cvalue()->startVersion,
			           it->cvalue()->endVersion);
		}
	}

	while (!queue.empty()) {
		auto cur = queue.front();
		queue.pop_front();
		ASSERT(cur.isValid());

		if (BW_HISTORY_DEBUG) {
			fmt::print("HQ {0} {1}) [{2} - {3}) @ {4}: Processing [{5} - {6}) @ {7}, {8} parents\n",
			           bwData->id.shortString().substr(0, 5),
			           hqId,
			           keyRange.begin.printable(),
			           keyRange.end.printable(),
			           readVersion,
			           cur->range.begin.printable(),
			           cur->range.end.printable(),
			           cur->startVersion,
			           cur->parentGranules.size());
		}

		if (readVersion >= cur->startVersion) {
			if (BW_HISTORY_DEBUG) {
				fmt::print("HQ {0} {1}) [{2} - {3}) @ {4}:     Granule included!\n",
				           bwData->id.shortString().substr(0, 5),
				           hqId,
				           keyRange.begin.printable(),
				           keyRange.end.printable(),
				           readVersion);
			}
			// part of request
			ASSERT(cur->endVersion > readVersion);
			if (!cur->files.isValid() || cur->files.isError()) {
				cur->files = loadHistoryFiles(bwData->db, cur->granuleID);
			}
			resultChunks.push_back(std::pair(cur->range, cur->files));
		} else if (cur->parentGranules.empty()) {
			throw blob_granule_transaction_too_old();
		} else {
			for (auto it : cur->parentGranules) {
				if (!it.isValid()) {
					throw blob_granule_transaction_too_old();
				}
				if (BW_HISTORY_DEBUG) {
					fmt::print("HQ {0} {1}) [{2} - {3}) @ {4}:   Considering parent [{5} - {6}) @ {7} - {8}\n",
					           bwData->id.shortString().substr(0, 5),
					           hqId,
					           keyRange.begin.printable(),
					           keyRange.end.printable(),
					           readVersion,
					           it->range.begin.printable(),
					           it->range.end.printable(),
					           it->startVersion,
					           it->endVersion);
				}
				ASSERT(cur->startVersion == it->endVersion);
				if (it->range.intersects(keyRange) && visited.insert(it->granuleID).second) {
					queue.push_back(it);
					if (BW_HISTORY_DEBUG) {
						fmt::print("HQ {0} {1}) [{2} - {3}) @ {4}:        Adding parent [{5} - {6}) @ {7} - {8}\n",
						           bwData->id.shortString().substr(0, 5),
						           hqId,
						           keyRange.begin.printable(),
						           keyRange.end.printable(),
						           readVersion,
						           it->range.begin.printable(),
						           it->range.end.printable(),
						           it->startVersion,
						           it->endVersion);
					}
				}
			}
		}
	}

	ASSERT(!resultChunks.empty());
	if (resultChunks.size() >= 2) {
		CODE_PROBE(true, "Multiple history chunks for time travel query");
		std::sort(resultChunks.begin(), resultChunks.end(), sort_result_chunks());
		// Assert contiguous
		for (int i = 0; i < resultChunks.size() - 1; i++) {
			if (resultChunks[i].first.end != resultChunks[i + 1].first.begin) {
				fmt::print("HQ {0} {1}) ERROR: history chunks {2} and {3} not contiguous!! ({4}, {5})\n",
				           bwData->id.shortString().substr(0, 5),
				           hqId,
				           i,
				           i + 1,
				           resultChunks[i].first.end.printable(),
				           resultChunks[i + 1].first.begin.printable());
				fmt::print("Chunks: {0}\n", resultChunks.size());
				for (auto& it : resultChunks) {
					fmt::print("    [{0} - {1})\n", it.first.begin.printable(), it.first.end.printable());
				}
			}
			ASSERT(resultChunks[i].first.end == resultChunks[i + 1].first.begin);
		}
		ASSERT(resultChunks.front().first.begin <= keyRange.begin);
		ASSERT(resultChunks.back().first.end >= keyRange.end);
	}

	if (BW_REQUEST_DEBUG) {
		fmt::print("[{0} - {1}) @ {2} time traveled back to {3} granules [{4} - {5})\n",
		           keyRange.begin.printable(),
		           keyRange.end.printable(),
		           readVersion,
		           resultChunks.size(),
		           resultChunks.front().first.begin.printable(),
		           resultChunks.back().first.end.printable());
	}

	return resultChunks;
}

// TODO might want to separate this out for valid values for range assignments vs read requests. Assignment
// conflict and blob_worker_full isn't valid for read requests but is for assignments
bool canReplyWith(Error e) {
	switch (e.code()) {
	case error_code_blob_granule_transaction_too_old:
	case error_code_transaction_too_old:
	case error_code_future_version:
	case error_code_wrong_shard_server:
	case error_code_process_behind: // not thrown yet
	case error_code_blob_worker_full:
		return true;
	default:
		return false;
	};
}
} // namespace

// assumes metadata is already readable and the query is reading from the active granule, not a history one
ACTOR Future<Void> waitForVersion(Reference<GranuleMetadata> metadata, Version v) {
	// if we don't have to wait for change feed version to catch up or wait for any pending file writes to
	// complete, nothing to do

	if (BW_REQUEST_DEBUG) {
		fmt::print("WFV {0}) CF={1}, pendingD={2}, durableD={3}, pendingS={4}, durableS={5}\n",
		           v,
		           metadata->activeCFData.get()->getVersion(),
		           metadata->pendingDeltaVersion,
		           metadata->durableDeltaVersion.get(),
		           metadata->pendingSnapshotVersion,
		           metadata->durableSnapshotVersion.get());
	}

	ASSERT(metadata->activeCFData.get().isValid());

	if (v <= metadata->activeCFData.get()->getVersion() &&
	    (v <= metadata->durableDeltaVersion.get() ||
	     metadata->durableDeltaVersion.get() == metadata->pendingDeltaVersion) &&
	    (v <= metadata->durableSnapshotVersion.get() ||
	     metadata->durableSnapshotVersion.get() == metadata->pendingSnapshotVersion)) {
		CODE_PROBE(true, "Granule read not waiting");
		return Void();
	}

	// wait for change feed version to catch up to ensure we have all data
	if (metadata->activeCFData.get()->getVersion() < v) {
		// FIXME: add future version timeout and throw here, same as SS
		wait(metadata->activeCFData.get()->whenAtLeast(v));
		ASSERT(metadata->activeCFData.get()->getVersion() >= v);
	}

	// wait for any pending delta and snapshot files as of the moment the change feed version caught up.
	state Version pendingDeltaV = metadata->pendingDeltaVersion;
	state Version pendingSnapshotV = metadata->pendingSnapshotVersion;

	// If there are mutations that are no longer buffered but have not been
	// persisted to a delta file that are necessary for the query, wait for them
	if (pendingDeltaV > metadata->durableDeltaVersion.get() && v > metadata->durableDeltaVersion.get()) {
		CODE_PROBE(true, "Granule read waiting for pending delta");
		wait(metadata->durableDeltaVersion.whenAtLeast(pendingDeltaV));
		ASSERT(metadata->durableDeltaVersion.get() >= pendingDeltaV);
	}

	// This isn't strictly needed, but if we're in the process of re-snapshotting, we'd likely rather
	// return that snapshot file than the previous snapshot file and all its delta files.
	if (pendingSnapshotV > metadata->durableSnapshotVersion.get() && v > metadata->durableSnapshotVersion.get()) {
		CODE_PROBE(true, "Granule read waiting for pending snapshot");
		wait(metadata->durableSnapshotVersion.whenAtLeast(pendingSnapshotV));
		ASSERT(metadata->durableSnapshotVersion.get() >= pendingSnapshotV);
	}

	// There is a race here - we wait for pending delta files before this to finish, but while we do, we
	// kick off another delta file and roll the mutations. In that case, we must return the new delta
	// file instead of in memory mutations, so we wait for that delta file to complete

	while (v > metadata->durableDeltaVersion.get() && metadata->pendingDeltaVersion > pendingDeltaV) {
		CODE_PROBE(true, "Granule mutations flushed while waiting for files to complete");
		Version waitVersion = std::min(v, metadata->pendingDeltaVersion);
		pendingDeltaV = metadata->pendingDeltaVersion;
		wait(metadata->durableDeltaVersion.whenAtLeast(waitVersion));
	}

	return Void();
}

ACTOR Future<Void> doBlobGranuleFileRequest(Reference<BlobWorkerData> bwData, BlobGranuleFileRequest req) {
	if (BW_REQUEST_DEBUG) {
		fmt::print("BW {0} processing blobGranuleFileRequest for range [{1} - {2}) @ ",
		           bwData->id.toString(),
		           req.keyRange.begin.printable(),
		           req.keyRange.end.printable());
		if (req.beginVersion > 0) {
			fmt::print("{0} - {1}\n", req.beginVersion, req.readVersion);
		} else {
			fmt::print("{}\n", req.readVersion);
		}
	}

	state Optional<Key> tenantPrefix;
	state Arena arena;
	if (req.tenantInfo.name.present()) {
		ASSERT(req.tenantInfo.tenantId != TenantInfo::INVALID_TENANT);
		Optional<TenantMapEntry> tenantEntry = bwData->tenantData.getTenantById(req.tenantInfo.tenantId);
		if (tenantEntry.present()) {
			ASSERT(tenantEntry.get().id == req.tenantInfo.tenantId);
			tenantPrefix = tenantEntry.get().prefix;
		} else {
			CODE_PROBE(true, "Blob worker unknown tenant");
			// FIXME - better way. Wait on retry here, or just have better model for tenant metadata?
			// Just throw wrong_shard_server and make the client retry and assume we load it later
			TraceEvent(SevDebug, "BlobWorkerRequestUnknownTenant", bwData->id)
			    .suppressFor(5.0)
			    .detail("TenantName", req.tenantInfo.name.get())
			    .detail("TenantId", req.tenantInfo.tenantId);
			throw unknown_tenant();
		}
		req.keyRange = KeyRangeRef(req.keyRange.begin.withPrefix(tenantPrefix.get(), req.arena),
		                           req.keyRange.end.withPrefix(tenantPrefix.get(), req.arena));
	}

	state bool didCollapse = false;
	try {
		// TODO remove requirement for canCollapseBegin once we implement early replying
		ASSERT(req.beginVersion == 0 || req.canCollapseBegin);
		if (req.beginVersion != 0) {
			ASSERT(req.beginVersion > 0);
		}
		state BlobGranuleFileReply rep;
		state std::vector<Reference<GranuleMetadata>> granules;

		if (tenantPrefix.present()) {
			rep.arena.dependsOn(tenantPrefix.get().arena());
		}

		auto checkRanges = bwData->granuleMetadata.intersectingRanges(req.keyRange);
		// check for gaps as errors and copy references to granule metadata before yielding or doing any
		// work
		KeyRef lastRangeEnd = req.keyRange.begin;

		for (auto& r : checkRanges) {
			bool isValid = r.value().activeMetadata.isValid();
			if (lastRangeEnd < r.begin() || !isValid) {
				if (BW_REQUEST_DEBUG) {
					fmt::print("No {0} blob data for [{1} - {2}) in request range [{3} - {4}), skipping request\n",
					           isValid ? "" : "valid",
					           lastRangeEnd.printable(),
					           r.begin().printable(),
					           req.keyRange.begin.printable(),
					           req.keyRange.end.printable());
				}

				throw wrong_shard_server();
			}
			granules.push_back(r.value().activeMetadata);
			lastRangeEnd = r.end();
		}
		if (lastRangeEnd < req.keyRange.end) {
			if (BW_REQUEST_DEBUG) {
				fmt::print("No blob data for [{0} - {1}) in request range [{2} - {3}), skipping request\n",
				           lastRangeEnd.printable(),
				           req.keyRange.end.printable(),
				           req.keyRange.begin.printable(),
				           req.keyRange.end.printable());
			}

			throw wrong_shard_server();
		}

		// do work for each range
		state Key readThrough = req.keyRange.begin;
		for (auto m : granules) {
			if (readThrough >= m->keyRange.end) {
				// previous read did time travel that already included this granule
				continue;
			}
			state Reference<GranuleMetadata> metadata = m;
			// state Version granuleBeginVersion = req.beginVersion;
			// skip waiting for CF ready for recovery mode
			if (!bwData->isFullRestoreMode) {
				choose {
					when(wait(metadata->readable.getFuture())) {}
					when(wait(metadata->cancelled.getFuture())) { throw wrong_shard_server(); }
				}
			}

			// in case both readable and cancelled are ready, check cancelled
			if (!metadata->cancelled.canBeSet()) {
				throw wrong_shard_server();
			}

			state std::vector<std::pair<KeyRange, GranuleFiles>> rangeGranulePair;

			if (req.readVersion < metadata->historyVersion) {
				CODE_PROBE(true, "Granule Time Travel Read");
				// this is a time travel query, find previous granule
				if (metadata->historyLoaded.canBeSet()) {
					choose {
						when(wait(metadata->historyLoaded.getFuture())) {}
						when(wait(metadata->cancelled.getFuture())) { throw wrong_shard_server(); }
					}
				}

				state std::vector<std::pair<KeyRange, Future<GranuleFiles>>> finalChunks = loadHistoryChunks(
				    bwData, metadata->historyVersion, req.keyRange & metadata->keyRange, req.readVersion);
				state int chunkIdx;
				for (chunkIdx = 0; chunkIdx < finalChunks.size(); chunkIdx++) {
					choose {
						when(GranuleFiles f = wait(finalChunks[chunkIdx].second)) {
							rangeGranulePair.push_back(std::pair(finalChunks[chunkIdx].first, f));
						}
						when(wait(metadata->cancelled.getFuture())) { throw wrong_shard_server(); }
					}

					if (rangeGranulePair.back().second.snapshotFiles.empty()) {
						// a snapshot file must have been purged
						throw blob_granule_transaction_too_old();
					}

					ASSERT(!rangeGranulePair.back().second.deltaFiles.empty());
					ASSERT(rangeGranulePair.back().second.deltaFiles.back().version > req.readVersion);
					if (rangeGranulePair.back().second.snapshotFiles.front().version > req.readVersion) {
						// a snapshot file must have been purged
						throw blob_granule_transaction_too_old();
					}
				}

			} else {
				if (req.readVersion < metadata->initialSnapshotVersion) {
					// a snapshot file must have been pruned
					throw blob_granule_transaction_too_old();
				}

				CODE_PROBE(true, "Granule Active Read");
				// this is an active granule query
				loop {
					// skip check since CF doesn't start for bare metal recovery mode
					if (bwData->isFullRestoreMode) {
						break;
					}
					if (!metadata->activeCFData.get().isValid() || !metadata->cancelled.canBeSet()) {
						throw wrong_shard_server();
					}
					Future<Void> waitForVersionFuture = waitForVersion(metadata, req.readVersion);
					if (waitForVersionFuture.isReady() && !waitForVersionFuture.isError()) {
						// didn't wait, so no need to check rollback stuff
						break;
					}
					// rollback resets all of the version information, so we have to redo wait for
					// version on rollback
					try {
						choose {
							when(wait(waitForVersionFuture)) { break; }
							when(wait(metadata->activeCFData.onChange())) {}
							when(wait(metadata->cancelled.getFuture())) { throw wrong_shard_server(); }
						}
					} catch (Error& e) {
						// We can get change feed cancelled from whenAtLeast. This means the change feed may
						// retry, or may be cancelled. Wait a bit and try again to see
						if (e.code() == error_code_change_feed_popped) {
							CODE_PROBE(true, "Change feed popped while read waiting", probe::decoration::rare);
							throw wrong_shard_server();
						}
						if (e.code() != error_code_change_feed_cancelled) {
							throw e;
						}
						CODE_PROBE(true, "Change feed switched while read waiting");
						// wait 1ms and try again
						wait(delay(0.001));
					}
					if ((BW_REQUEST_DEBUG) && metadata->activeCFData.get().isValid()) {
						fmt::print("{0} - {1}) @ {2} hit CF change, restarting waitForVersion\n",
						           req.keyRange.begin.printable().c_str(),
						           req.keyRange.end.printable().c_str(),
						           req.readVersion);
					}
				}
				// if feed was popped by another worker and BW only got empty versions, it wouldn't itself see that it
				// got popped, but we can still reject the in theory this should never happen with other protections but
				// it's a useful and inexpensive sanity check
				if (!bwData->isFullRestoreMode) {
					Version emptyVersion = metadata->activeCFData.get()->popVersion - 1;
					if (req.readVersion > metadata->durableDeltaVersion.get() &&
					    emptyVersion > metadata->bufferedDeltaVersion) {
						CODE_PROBE(true,
						           "feed popped for read but granule updater didn't notice yet",
						           probe::decoration::rare);
						// FIXME: could try to cancel the actor here somehow, but it should find out eventually
						throw wrong_shard_server();
					}
				}
				rangeGranulePair.push_back(std::pair(metadata->keyRange, metadata->files));
			}

			if (!metadata->cancelled.canBeSet()) {
				fmt::print("ERROR: Request [{0} - {1}) @ {2} cancelled for granule [{3} - {4}) after "
				           "waitForVersion!\n",
				           req.keyRange.begin.printable(),
				           req.keyRange.end.printable(),
				           req.readVersion,
				           metadata->keyRange.begin.printable(),
				           metadata->keyRange.end.printable());
			}

			// granule is up to date, do read
			ASSERT(metadata->cancelled.canBeSet());

			for (auto& item : rangeGranulePair) {
				Version granuleBeginVersion = req.beginVersion;
				// Right now we force a collapse if the version range crosses granule boundaries, for simplicity
				if (granuleBeginVersion > 0 && granuleBeginVersion <= item.second.snapshotFiles.front().version) {
					CODE_PROBE(true, "collapsed begin version request because of boundaries");
					didCollapse = true;
					granuleBeginVersion = 0;
				}
				state BlobGranuleChunkRef chunk;
				// TODO change with early reply

				chunk.keyRange =
				    KeyRangeRef(StringRef(rep.arena, item.first.begin), StringRef(rep.arena, item.first.end));
				if (tenantPrefix.present()) {
					chunk.tenantPrefix = Optional<StringRef>(tenantPrefix.get());
				}

				int64_t deltaBytes = 0;
				item.second.getFiles(granuleBeginVersion,
				                     req.readVersion,
				                     req.canCollapseBegin,
				                     chunk,
				                     rep.arena,
				                     deltaBytes,
				                     req.summarize);
				bwData->stats.readReqDeltaBytesReturned += deltaBytes;
				if (granuleBeginVersion > 0 && chunk.snapshotFile.present()) {
					CODE_PROBE(true, "collapsed begin version request for efficiency");
					didCollapse = true;
				}

				if (!req.summarize) {
					chunk.includedVersion = req.readVersion;
					// Invoke calls to populate 'EncryptionKeysCtx' for snapshot and/or deltaFiles asynchronously
					state Optional<Future<BlobGranuleCipherKeysCtx>> snapCipherKeysCtx;
					if (chunk.snapshotFile.present()) {
						const bool encrypted = chunk.snapshotFile.get().cipherKeysMetaRef.present();

						if (BW_DEBUG) {
							TraceEvent("DoBlobGranuleFileRequestDelta_KeysCtxPrepare")
							    .detail("FileName", chunk.snapshotFile.get().filename.toString())
							    .detail("Encrypted", encrypted);
						}

						if (g_network && g_network->isSimulated() &&
						    isEncryptionOpSupported(EncryptOperationType::BLOB_GRANULE_ENCRYPTION) && !encrypted) {
							ASSERT(false);
						}
						if (encrypted) {
							ASSERT(isEncryptionOpSupported(EncryptOperationType::BLOB_GRANULE_ENCRYPTION));
							ASSERT(!chunk.snapshotFile.get().cipherKeysCtx.present());
							CODE_PROBE(true, "fetching cipher keys from meta ref for snapshot file");
							snapCipherKeysCtx = getGranuleCipherKeysFromKeysMetaRef(
							    bwData, chunk.snapshotFile.get().cipherKeysMetaRef.get(), &rep.arena);
						}
					}
					state std::unordered_map<int, Future<BlobGranuleCipherKeysCtx>> deltaCipherKeysCtxs;
					for (int deltaIdx = 0; deltaIdx < chunk.deltaFiles.size(); deltaIdx++) {
						const bool encrypted = chunk.deltaFiles[deltaIdx].cipherKeysMetaRef.present();

						if (BW_DEBUG) {
							TraceEvent("DoBlobGranuleFileRequestDelta_KeysCtxPrepare")
							    .detail("FileName", chunk.deltaFiles[deltaIdx].filename.toString())
							    .detail("Encrypted", encrypted);
						}

						if (g_network && g_network->isSimulated() &&
						    isEncryptionOpSupported(EncryptOperationType::BLOB_GRANULE_ENCRYPTION) && !encrypted) {
							ASSERT(false);
						}
						if (encrypted) {
							ASSERT(isEncryptionOpSupported(EncryptOperationType::BLOB_GRANULE_ENCRYPTION));
							ASSERT(!chunk.deltaFiles[deltaIdx].cipherKeysCtx.present());
							CODE_PROBE(true, "fetching cipher keys from meta ref for delta files");
							deltaCipherKeysCtxs.emplace(
							    deltaIdx,
							    getGranuleCipherKeysFromKeysMetaRef(
							        bwData, chunk.deltaFiles[deltaIdx].cipherKeysMetaRef.get(), &rep.arena));
						}
					}

					// new deltas (if version is larger than version of last delta file)
					// FIXME: do trivial key bounds here if key range is not fully contained in request key
					// range
					if (req.readVersion > metadata->durableDeltaVersion.get() && !metadata->currentDeltas.empty()) {
						if (metadata->durableDeltaVersion.get() != metadata->pendingDeltaVersion) {
							fmt::print(
							    "real-time read [{0} - {1}) @ {2} doesn't have mutations!! durable={3}, pending={4}\n",
							    metadata->keyRange.begin.printable(),
							    metadata->keyRange.end.printable(),
							    req.readVersion,
							    metadata->durableDeltaVersion.get(),
							    metadata->pendingDeltaVersion);
						}

						// prune mutations based on begin version, if possible
						ASSERT(metadata->durableDeltaVersion.get() == metadata->pendingDeltaVersion);
						MutationsAndVersionRef* mutationIt = metadata->currentDeltas.begin();
						if (granuleBeginVersion > metadata->currentDeltas.back().version) {
							CODE_PROBE(true, "beginVersion pruning all in-memory mutations");
							mutationIt = metadata->currentDeltas.end();
						} else if (granuleBeginVersion > metadata->currentDeltas.front().version) {
							// binary search for beginVersion
							CODE_PROBE(true, "beginVersion pruning some in-memory mutations");
							mutationIt = std::lower_bound(metadata->currentDeltas.begin(),
							                              metadata->currentDeltas.end(),
							                              MutationsAndVersionRef(granuleBeginVersion, 0),
							                              MutationsAndVersionRef::OrderByVersion());
						}

						// add mutations to response
						while (mutationIt != metadata->currentDeltas.end()) {
							if (mutationIt->version > req.readVersion) {
								CODE_PROBE(true, "readVersion pruning some in-memory mutations");
								break;
							}
							chunk.newDeltas.push_back_deep(rep.arena, *mutationIt);
							mutationIt++;
						}
					}

					// Update EncryptionKeysCtx information for the chunk->snapshotFile
					if (chunk.snapshotFile.present() && snapCipherKeysCtx.present()) {
						ASSERT(chunk.snapshotFile.get().cipherKeysMetaRef.present());

						BlobGranuleCipherKeysCtx keysCtx = wait(snapCipherKeysCtx.get());
						chunk.snapshotFile.get().cipherKeysCtx = std::move(keysCtx);
						// reclaim memory from non-serializable field
						chunk.snapshotFile.get().cipherKeysMetaRef.reset();

						if (BW_DEBUG) {
							TraceEvent("DoBlobGranuleFileRequestSnap_KeysCtxDone")
							    .detail("FileName", chunk.snapshotFile.get().filename.toString());
						}
					}

					// Update EncryptionKeysCtx information for the chunk->deltaFiles
					if (!deltaCipherKeysCtxs.empty()) {
						ASSERT(!chunk.deltaFiles.empty());

						state std::unordered_map<int, Future<BlobGranuleCipherKeysCtx>>::const_iterator itr;
						for (itr = deltaCipherKeysCtxs.begin(); itr != deltaCipherKeysCtxs.end(); itr++) {
							BlobGranuleCipherKeysCtx keysCtx = wait(itr->second);
							chunk.deltaFiles[itr->first].cipherKeysCtx = std::move(keysCtx);
							// reclaim memory from non-serializable field
							chunk.deltaFiles[itr->first].cipherKeysMetaRef.reset();

							if (BW_DEBUG) {
								TraceEvent("DoBlobGranuleFileRequestDelta_KeysCtxDone")
								    .detail("FileName", chunk.deltaFiles[itr->first].filename.toString());
							}
						}
					}

					// don't update read stats on a summarize read
					if (metadata->updateReadStats(req.readVersion, chunk)) {
						bwData->triggerReadDrivenCompaction();
					}
				}

				rep.chunks.push_back(rep.arena, chunk);

				bwData->stats.readReqTotalFilesReturned += chunk.deltaFiles.size() + int(chunk.snapshotFile.present());
				readThrough = chunk.keyRange.end;
			}

			wait(yield(TaskPriority::DefaultEndpoint));
		}
		// do these together to keep them synchronous
		if (req.beginVersion != 0) {
			++bwData->stats.readRequestsWithBegin;
		}
		if (didCollapse) {
			++bwData->stats.readRequestsCollapsed;
		}

		double duration = g_network->timer() - req.requestTime();
		bwData->stats.readLatencySample.addMeasurement(duration);

		ASSERT(!req.reply.isSet());
		req.reply.send(rep);
		--bwData->stats.activeReadRequests;
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled) {
			req.reply.sendError(wrong_shard_server());
			throw;
		}

		if (e.code() == error_code_wrong_shard_server) {
			++bwData->stats.wrongShardServer;
		}
		--bwData->stats.activeReadRequests;
		if (canReplyWith(e)) {
			req.reply.sendError(e);
		} else {
			throw e;
		}
	}
	return Void();
}

ACTOR Future<Void> handleBlobGranuleFileRequest(Reference<BlobWorkerData> bwData, BlobGranuleFileRequest req) {
	++bwData->stats.readRequests;
	++bwData->stats.activeReadRequests;
	if (req.summarize) {
		++bwData->stats.summaryReads;
	}
	choose {
		when(wait(doBlobGranuleFileRequest(bwData, req))) {}
		when(wait(delay(SERVER_KNOBS->BLOB_WORKER_REQUEST_TIMEOUT))) {
			if (!req.reply.isSet()) {
				CODE_PROBE(true, "Blob Worker request timeout hit", probe::decoration::rare);
				if (BW_DEBUG) {
					fmt::print("BW {0} request [{1} - {2}) @ {3} timed out, sending WSS\n",
					           bwData->id.toString().substr(0, 5),
					           req.keyRange.begin.printable(),
					           req.keyRange.end.printable(),
					           req.readVersion);
				}
				--bwData->stats.activeReadRequests;
				++bwData->stats.granuleRequestTimeouts;

				// return wrong_shard_server because it's possible that someone else actually owns the
				// granule now
				req.reply.sendError(wrong_shard_server());
			}
		}
	}
	return Void();
}

ACTOR Future<GranuleFiles> loadParentGranuleForMergeSnapshot(Transaction* tr, KeyRange range, Version historyVersion) {
	// translate key range to granule id
	Optional<Value> historyParentValue = wait(tr->get(blobGranuleHistoryKeyFor(range, historyVersion)));
	ASSERT(historyParentValue.present());
	Standalone<BlobGranuleHistoryValue> val = decodeBlobGranuleHistoryValue(historyParentValue.get());
	UID parentGranuleID = val.granuleID;

	// load previous files for granule
	GranuleFiles prevFiles = wait(loadPreviousFiles(tr, parentGranuleID));
	return prevFiles;
}

// FIXME: move this up by other granule state stuff like BGUF
ACTOR Future<GranuleStartState> openGranule(Reference<BlobWorkerData> bwData, AssignBlobRangeRequest req) {
	ASSERT(req.type != AssignRequestType::Continue);
	state Transaction tr(bwData->db);
	state Key lockKey = blobGranuleLockKeyFor(req.keyRange);
	state UID newGranuleID = deterministicRandom()->randomUniqueID();

	if (BW_DEBUG) {
		fmt::print("{0} [{1} - {2}) open ({3}, {4})\n",
		           bwData->id.toString().substr(0, 5),
		           req.keyRange.begin.printable(),
		           req.keyRange.end.printable(),
		           req.managerEpoch,
		           req.managerSeqno);
	}

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			state GranuleStartState info;
			info.changeFeedStartVersion = invalidVersion;

			state Future<Optional<Value>> fLockValue = tr.get(lockKey);
			state Future<ForcedPurgeState> fForcedPurgeState = getForcePurgedState(&tr, req.keyRange);
			Future<Optional<GranuleHistory>> fHistory = getLatestGranuleHistory(&tr, req.keyRange);

			Optional<GranuleHistory> history = wait(fHistory);
			info.history = history;

			ForcedPurgeState purgeState = wait(fForcedPurgeState);
			if (purgeState != ForcedPurgeState::NonePurged) {
				CODE_PROBE(true, "Worker trying to open force purged granule", probe::decoration::rare);
				if (BW_DEBUG) {
					fmt::print("Granule [{0} - {1}) is force purged on BW {2}, abandoning\n",
					           req.keyRange.begin.printable(),
					           req.keyRange.end.printable(),
					           bwData->id.toString().substr(0, 5));
				}
				throw granule_assignment_conflict();
			}

			bool isFullRestore = wait(isFullRestoreMode(bwData->db, req.keyRange));
			bwData->isFullRestoreMode = isFullRestore;

			Optional<Value> prevLockValue = wait(fLockValue);
			state bool hasPrevOwner = prevLockValue.present();
			state bool createChangeFeed = false;

			if (hasPrevOwner) {
				CODE_PROBE(true, "Granule open found previous owner");
				std::tuple<int64_t, int64_t, UID> prevOwner = decodeBlobGranuleLockValue(prevLockValue.get());

				info.granuleID = std::get<2>(prevOwner);
				state bool doLockCheck = true;
				// if it's the first snapshot of a new granule, history won't be present
				if (info.history.present()) {
					if (info.granuleID != info.history.get().value.granuleID) {
						CODE_PROBE(true, "Blob Worker re-opening granule after merge+resplit", probe::decoration::rare);
						// The only case this can happen is when a granule was merged into a larger granule,
						// then split back out to the same one. Validate that this is a new granule that was
						// split previously. Just check lock based on epoch, since seqno is intentionally
						// changed
						ASSERT(std::get<1>(prevOwner) == std::numeric_limits<int64_t>::max());
						if (req.managerEpoch < std::get<0>(prevOwner)) {
							throw granule_assignment_conflict();
						}
						doLockCheck = false;
						info.granuleID = info.history.get().value.granuleID;
						createChangeFeed = true;
					}
				}
				if (doLockCheck) {
					acquireGranuleLock(
					    req.managerEpoch, req.managerSeqno, std::get<0>(prevOwner), std::get<1>(prevOwner));
				}

				GranuleFiles granuleFiles = wait(loadPreviousFiles(&tr, info.granuleID));
				info.existingFiles = granuleFiles;
				info.doSnapshot = false;

				if (!doLockCheck) {
					// validate new granule id is empty
					ASSERT(granuleFiles.snapshotFiles.empty());
					ASSERT(granuleFiles.deltaFiles.empty());
				}

				if (!info.history.present()) {
					// the only time history can be not present if a lock already exists is if it's a
					// new granule and it died before it could persist the initial snapshot from FDB
					ASSERT(info.existingFiles.get().snapshotFiles.empty());
				}

				if (info.existingFiles.get().snapshotFiles.empty()) {
					ASSERT(info.existingFiles.get().deltaFiles.empty());
					info.previousDurableVersion = invalidVersion;
					info.doSnapshot = true;
				} else if (info.existingFiles.get().deltaFiles.empty()) {
					info.previousDurableVersion = info.existingFiles.get().snapshotFiles.back().version;
				} else {
					info.previousDurableVersion = info.existingFiles.get().deltaFiles.back().version;
				}

				// for the non-splitting cases, this doesn't need to be 100% accurate, it just needs to
				// be smaller than the next delta file write.
				info.changeFeedStartVersion = info.previousDurableVersion;
			} else {
				// else we are first, no need to check for owner conflict
				if (info.history.present()) {
					// if this granule is derived from a split or merge, this history entry is already
					// present (written by the blob manager)
					info.granuleID = info.history.get().value.granuleID;
				} else {
					// FIXME: could avoid max uid for granule ids here
					// if this granule is not derived from a split or merge, use new granule id
					info.granuleID = newGranuleID;
				}

				// for recovery mode - don't create change feed, don't create snapshot
				if (bwData->isFullRestoreMode) {
					createChangeFeed = false;
					info.doSnapshot = false;
					GranuleFiles granuleFiles = wait(loadPreviousFiles(&tr, info.granuleID));
					info.existingFiles = granuleFiles;

					if (info.existingFiles.get().snapshotFiles.empty()) {
						ASSERT(info.existingFiles.get().deltaFiles.empty());
						info.previousDurableVersion = invalidVersion;
					} else if (info.existingFiles.get().deltaFiles.empty()) {
						info.previousDurableVersion = info.existingFiles.get().snapshotFiles.back().version;
					} else {
						info.previousDurableVersion = info.existingFiles.get().deltaFiles.back().version;
					}
					info.changeFeedStartVersion = info.previousDurableVersion;
				} else {
					createChangeFeed = true;
					info.doSnapshot = true;
					info.previousDurableVersion = invalidVersion;
				}
			}

			if (createChangeFeed && !bwData->isFullRestoreMode) {
				// create new change feed for new version of granule
				wait(updateChangeFeed(
				    &tr, granuleIDToCFKey(info.granuleID), ChangeFeedStatus::CHANGE_FEED_CREATE, req.keyRange));
			}

			tr.set(lockKey, blobGranuleLockValueFor(req.managerEpoch, req.managerSeqno, info.granuleID));
			wait(krmSetRange(&tr, blobGranuleMappingKeys.begin, req.keyRange, blobGranuleMappingValueFor(bwData->id)));

			// If anything in previousGranules, need to do the handoff logic and set
			// ret.previousChangeFeedId, and the previous durable version will come from the previous
			// granules
			if (info.history.present() && info.history.get().value.parentVersions.size() > 0 &&
			    !bwData->isFullRestoreMode) {
				CODE_PROBE(true, "Granule open found parent");
				if (info.history.get().value.parentVersions.size() == 1) { // split
					state KeyRangeRef parentRange(info.history.get().value.parentBoundaries[0],
					                              info.history.get().value.parentBoundaries[1]);
					state Version parentVersion = info.history.get().value.parentVersions[0];
					state Key parentHistoryKey = blobGranuleHistoryKeyFor(parentRange, parentVersion);

					Optional<Value> historyParentValue = wait(tr.get(parentHistoryKey));

					if (historyParentValue.present()) {
						Standalone<BlobGranuleHistoryValue> val =
						    decodeBlobGranuleHistoryValue(historyParentValue.get());
						UID parentGranuleID = val.granuleID;

						info.splitParentGranule = std::pair(parentRange, parentGranuleID);

						state std::pair<BlobGranuleSplitState, Version> granuleSplitState =
						    std::pair(BlobGranuleSplitState::Initialized, invalidVersion);
						if (hasPrevOwner) {
							std::pair<BlobGranuleSplitState, Version> _gss =
							    wait(getGranuleSplitState(&tr, parentGranuleID, info.granuleID));
							granuleSplitState = _gss;
						}

						if (granuleSplitState.first == BlobGranuleSplitState::Assigned) {
							CODE_PROBE(true, "Granule open found granule in assign state");
							// was already assigned, use change feed start version
							ASSERT(granuleSplitState.second > 0);
							info.changeFeedStartVersion = granuleSplitState.second;
						} else if (granuleSplitState.first == BlobGranuleSplitState::Initialized) {
							CODE_PROBE(true, "Granule open found granule in initialized state");
							wait(updateGranuleSplitState(&tr,
							                             info.splitParentGranule.get().first,
							                             info.splitParentGranule.get().second,
							                             info.granuleID,
							                             BlobGranuleSplitState::Assigned));
							// change feed was created as part of this transaction, changeFeedStartVersion
							// will be set later
						} else {
							CODE_PROBE(true, "Granule open found granule in done state");
							// this sub-granule is done splitting, no need for split logic.
							info.splitParentGranule.reset();
						}
					}

					if (info.doSnapshot) {
						ASSERT(info.splitParentGranule.present());
						// only need to do snapshot if no files exist yet for this granule.
						ASSERT(info.previousDurableVersion == invalidVersion);
						GranuleFiles prevFiles = wait(loadPreviousFiles(&tr, info.splitParentGranule.get().second));
						ASSERT(!prevFiles.snapshotFiles.empty() || !prevFiles.deltaFiles.empty());

						info.blobFilesToSnapshot.push_back(prevFiles);
						info.previousDurableVersion = info.blobFilesToSnapshot[0].deltaFiles.empty()
						                                  ? info.blobFilesToSnapshot[0].snapshotFiles.back().version
						                                  : info.blobFilesToSnapshot[0].deltaFiles.back().version;
					}
				} else if (info.doSnapshot) {
					CODE_PROBE(true, "merge needs to snapshot at start");
					state std::vector<Future<GranuleFiles>> parentGranulesToSnapshot;
					ASSERT(info.previousDurableVersion == invalidVersion);
					// need first snapshot to be at history version so this granule can serve the full range
					// of data for its version range, even if the previous granule happened to persist data
					// beyond that
					info.previousDurableVersion = info.history.get().version;
					// Can't roll back past re-snapshot version
					info.changeFeedStartVersion = info.history.get().version;

					for (int i = 0; i < info.history.get().value.parentVersions.size(); i++) {
						KeyRangeRef parentRange(info.history.get().value.parentBoundaries[i],
						                        info.history.get().value.parentBoundaries[i + 1]);
						Version parentVersion = info.history.get().value.parentVersions[i];
						parentGranulesToSnapshot.push_back(
						    loadParentGranuleForMergeSnapshot(&tr, parentRange, parentVersion));
					}

					state int pIdx;
					for (pIdx = 0; pIdx < parentGranulesToSnapshot.size(); pIdx++) {
						GranuleFiles parentFiles = wait(parentGranulesToSnapshot[pIdx]);
						info.blobFilesToSnapshot.push_back(parentFiles);
						ASSERT(!parentFiles.deltaFiles.empty());
						ASSERT(parentFiles.deltaFiles.back().version >= info.previousDurableVersion);
					}
				}
			}
			wait(tr.commit());

			if (info.changeFeedStartVersion == invalidVersion) {
				info.changeFeedStartVersion = tr.getCommittedVersion();
			}

			TraceEvent openEv("GranuleOpen", bwData->id);
			openEv.detail("GranuleID", info.granuleID)
			    .detail("Granule", req.keyRange)
			    .detail("Epoch", req.managerEpoch)
			    .detail("Seqno", req.managerSeqno)
			    .detail("CFStartVersion", info.changeFeedStartVersion)
			    .detail("PreviousDurableVersion", info.previousDurableVersion);
			if (info.splitParentGranule.present()) {
				openEv.detail("SplitParentGranuleID", info.splitParentGranule.get().second);
			}

			if (BUGGIFY && bwData->maybeInjectTargetedRestart()) {
				wait(delay(0)); // should be cancelled
				ASSERT(false);
			}

			return info;
		} catch (Error& e) {
			if (e.code() == error_code_granule_assignment_conflict) {
				throw e;
			}
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Reference<BlobConnectionProvider>> loadBStoreForTenant(Reference<BlobWorkerData> bwData,
                                                                    KeyRange keyRange) {
	state int retryCount = 0;
	loop {
		state Reference<GranuleTenantData> data;
		wait(store(data, bwData->tenantData.getDataForGranule(keyRange)));
		if (data.isValid()) {
			wait(data->bstoreLoaded.getFuture());
			wait(delay(0));
			return data->bstore;
		} else {
			CODE_PROBE(true, "bstore for unknown tenant");
			// Assume not loaded yet, just wait a bit. Could do sophisticated mechanism but will redo tenant
			// loading to be versioned anyway. 10 retries means it's likely not a transient race with
			// loading tenants, and instead a persistent issue.
			retryCount++;
			TraceEvent(retryCount <= 10 ? SevDebug : SevWarn, "BlobWorkerUnknownTenantForGranule", bwData->id)
			    .detail("KeyRange", keyRange);
			wait(delay(0.1));
		}
	}
}

ACTOR Future<Void> start(Reference<BlobWorkerData> bwData, GranuleRangeMetadata* meta, AssignBlobRangeRequest req) {
	ASSERT(meta->activeMetadata.isValid());

	Future<Reference<BlobConnectionProvider>> loadBStore;
	if (SERVER_KNOBS->BG_METADATA_SOURCE != "tenant") {
		loadBStore = Future<Reference<BlobConnectionProvider>>(bwData->bstore); // done
	} else {
		loadBStore = loadBStoreForTenant(bwData, req.keyRange);
	}

	meta->activeMetadata->originalReq = req;
	meta->assignFuture = openGranule(bwData, req);
	meta->fileUpdaterFuture = blobGranuleUpdateFiles(bwData, meta->activeMetadata, meta->assignFuture, loadBStore);
	meta->historyLoaderFuture = blobGranuleLoadHistory(bwData, meta->activeMetadata, meta->assignFuture);
	wait(success(meta->assignFuture));
	return Void();
}

namespace {
GranuleRangeMetadata constructActiveBlobRange(Reference<BlobWorkerData> bwData,
                                              KeyRange keyRange,
                                              int64_t epoch,
                                              int64_t seqno) {

	Reference<GranuleMetadata> newMetadata = makeReference<GranuleMetadata>();
	newMetadata->keyRange = keyRange;
	// FIXME: original Epoch/Seqno is now not necessary with originalReq
	newMetadata->originalEpoch = epoch;
	newMetadata->originalSeqno = seqno;
	newMetadata->continueEpoch = epoch;
	newMetadata->continueSeqno = seqno;

	return GranuleRangeMetadata(epoch, seqno, newMetadata);
}

GranuleRangeMetadata constructInactiveBlobRange(int64_t epoch, int64_t seqno) {
	return GranuleRangeMetadata(epoch, seqno, Reference<GranuleMetadata>());
}

// ignore stale assignments and make repeating the same one idempotent
bool newerRangeAssignment(GranuleRangeMetadata oldMetadata, int64_t epoch, int64_t seqno) {
	return epoch > oldMetadata.lastEpoch || (epoch == oldMetadata.lastEpoch && seqno > oldMetadata.lastSeqno);
}

// TODO unit test this assignment, particularly out-of-order insertions!

// The contract from the blob manager is:
// If a key range [A, B) was assigned to the worker at seqno S1, no part of the keyspace that intersects
// [A, B] may be re-assigned to the worker until the range has been revoked from this worker. This
// revoking can either happen by the blob manager willingly relinquishing the range, or by the blob
// manager reassigning it somewhere else. This means that if the worker gets an assignment for any range
// that intersects [A, B) at S3, there must have been a revoke message for [A, B) with seqno S3 where S1
// < S2 < S3, that was delivered out of order. This means that if there are any intersecting but not
// fully overlapping ranges with a new range assignment, they had already been revoked. So the worker
// will mark them as revoked, but leave the sequence number as S1, so that when the actual revoke
// message comes in, it is a no-op, but updates the sequence number. Similarly, if a worker gets an
// assign message for any range that already has a higher sequence number, that range was either
// revoked, or revoked and then re-assigned. Either way, this assignment is no longer valid.

// Returns future to wait on to ensure prior work of other granules is done before responding to the
// manager with a successful assignment And if the change produced a new granule that needs to start
// doing work, returns the new granule so that the caller can start() it with the appropriate starting
// state.

// Not an actor because we need to guarantee it changes the synchronously as part of the request
bool changeBlobRange(Reference<BlobWorkerData> bwData,
                     KeyRange keyRange,
                     int64_t epoch,
                     int64_t seqno,
                     bool active,
                     bool disposeOnCleanup,
                     bool selfReassign,
                     std::vector<Future<Void>>& toWaitOut,
                     Optional<AssignRequestType> assignType = Optional<AssignRequestType>()) {
	ASSERT(active == assignType.present());

	if (BW_DEBUG) {
		fmt::print("{0} range for [{1} - {2}): {3} @ ({4}, {5})\n",
		           selfReassign ? "Re-assigning" : "Changing",
		           keyRange.begin.printable(),
		           keyRange.end.printable(),
		           active ? "T" : "F",
		           epoch,
		           seqno);
	}

	// For each range that intersects this update:
	// If the identical range already exists at the same assignment sequence number and it is not a
	// self-reassign, this is a noop. Otherwise, this will consist of a series of ranges that are either
	// older, or newer. For each older range, cancel it if it is active. Insert the current range.
	// Re-insert all newer ranges over the current range.

	std::vector<std::pair<KeyRange, GranuleRangeMetadata>> newerRanges;

	auto ranges = bwData->granuleMetadata.intersectingRanges(keyRange);
	bool alreadyAssigned = false;
	for (auto& r : ranges) {
		bool thisAssignmentNewer = newerRangeAssignment(r.value(), epoch, seqno);
		if (BW_DEBUG) {
			fmt::print("thisAssignmentNewer={}\n", thisAssignmentNewer ? "true" : "false");
		}

		if (BW_DEBUG) {
			fmt::print("last: ({0}, {1}). now: ({2}, {3})\n", r.value().lastEpoch, r.value().lastSeqno, epoch, seqno);
		}

		if (r.value().lastEpoch == epoch && r.value().lastSeqno == seqno) {
			// the range in our map can be different if later the range was split, but then an old request
			// gets retried. Assume that it's the same as initially

			if (selfReassign) {
				thisAssignmentNewer = true;
			} else {
				if (BW_DEBUG) {
					printf("same assignment\n");
				}
				// applied the same assignment twice, make idempotent
				if (r.value().activeMetadata.isValid()) {
					toWaitOut.push_back(success(r.value().assignFuture));
				}
				alreadyAssigned = true;
				break;
			}
		}

		if (r.value().activeMetadata.isValid() && thisAssignmentNewer) {
			// cancel actors for old range and clear reference
			if (BW_DEBUG) {
				fmt::print("  [{0} - {1}): @ ({2}, {3}) (cancelling)\n",
				           r.begin().printable(),
				           r.end().printable(),
				           r.value().lastEpoch,
				           r.value().lastSeqno);
			}
			if (!active) {
				bwData->stats.numRangesAssigned--;
			}
			r.value().cancel();
		} else if (!thisAssignmentNewer) {
			// re-insert the known newer range over this existing range
			newerRanges.push_back(std::pair(r.range(), r.value()));
		}
	}

	if (alreadyAssigned) {
		return false;
	}

	// if range is active, and isn't surpassed by a newer range already, insert an active range
	GranuleRangeMetadata newMetadata = (active && newerRanges.empty())
	                                       ? constructActiveBlobRange(bwData, keyRange, epoch, seqno)
	                                       : constructInactiveBlobRange(epoch, seqno);

	bwData->granuleMetadata.insert(keyRange, newMetadata);
	if (BW_DEBUG) {
		fmt::print("Inserting new range [{0} - {1}): {2} @ ({3}, {4})\n",
		           keyRange.begin.printable(),
		           keyRange.end.printable(),
		           newMetadata.activeMetadata.isValid() ? "T" : "F",
		           newMetadata.lastEpoch,
		           newMetadata.lastSeqno);
	}

	for (auto& it : newerRanges) {
		if (BW_DEBUG) {
			fmt::print("Re-inserting newer range [{0} - {1}): {2} @ ({3}, {4})\n",
			           it.first.begin.printable(),
			           it.first.end.printable(),
			           it.second.activeMetadata.isValid() ? "T" : "F",
			           it.second.lastEpoch,
			           it.second.lastSeqno);
		}
		bwData->granuleMetadata.insert(it.first, it.second);
	}

	return newerRanges.size() == 0;
}

bool resumeBlobRange(Reference<BlobWorkerData> bwData, KeyRange keyRange, int64_t epoch, int64_t seqno) {
	auto existingRange = bwData->granuleMetadata.rangeContaining(keyRange.begin);
	// if range boundaries don't match, or this (epoch, seqno) is old or the granule is inactive, ignore
	if (keyRange.begin != existingRange.begin() || keyRange.end != existingRange.end() ||
	    existingRange.value().lastEpoch > epoch ||
	    (existingRange.value().lastEpoch == epoch && existingRange.value().lastSeqno > seqno) ||
	    !existingRange.value().activeMetadata.isValid()) {

		if (BW_DEBUG) {
			fmt::print("BW {0} got out of date resume range for [{1} - {2}) @ ({3}, {4}). Currently  [{5} "
			           "- {6}) @ ({7}, "
			           "{8}): {9}\n",
			           bwData->id.toString(),
			           existingRange.begin().printable(),
			           existingRange.end().printable(),
			           existingRange.value().lastEpoch,
			           existingRange.value().lastSeqno,
			           keyRange.begin.printable(),
			           keyRange.end.printable(),
			           epoch,
			           seqno,
			           existingRange.value().activeMetadata.isValid() ? "T" : "F");
		}

		return false;
	}
	if (existingRange.value().lastEpoch != epoch || existingRange.value().lastSeqno != seqno) {
		// update the granule metadata map, and the continueEpoch/seqno.  Saves an extra transaction
		existingRange.value().lastEpoch = epoch;
		existingRange.value().lastSeqno = seqno;
		existingRange.value().activeMetadata->continueEpoch = epoch;
		existingRange.value().activeMetadata->continueSeqno = seqno;
		existingRange.value().activeMetadata->resume();
	}
	// else we already processed this continue, do nothing
	return true;
}
} // namespace

// the contract of handleRangeAssign and handleRangeRevoke is that they change the mapping before doing any
// waiting. This ensures GetGranuleAssignment returns an up-to-date set of ranges
ACTOR Future<Void> handleRangeAssign(Reference<BlobWorkerData> bwData,
                                     AssignBlobRangeRequest req,
                                     bool isSelfReassign) {
	try {
		if (req.type == AssignRequestType::Continue) {
			resumeBlobRange(bwData, req.keyRange, req.managerEpoch, req.managerSeqno);
		} else {
			if (!isSelfReassign && bwData->isFull()) {
				if (BW_DEBUG) {
					fmt::print("BW {0}: rejecting assignment [{1} - {2}) b/c full\n",
					           bwData->id.toString().substr(0, 6),
					           req.keyRange.begin.printable(),
					           req.keyRange.end.printable());
				}
				++bwData->stats.fullRejections;
				req.reply.sendError(blob_worker_full());
				return Void();
			}
			std::vector<Future<Void>> toWait;
			state bool shouldStart = changeBlobRange(bwData,
			                                         req.keyRange,
			                                         req.managerEpoch,
			                                         req.managerSeqno,
			                                         true,
			                                         false,
			                                         isSelfReassign,
			                                         toWait,
			                                         req.type);
			wait(waitForAll(toWait));

			if (shouldStart) {
				if (!isSelfReassign) {
					bwData->stats.numRangesAssigned++;
				}
				auto m = bwData->granuleMetadata.rangeContaining(req.keyRange.begin);
				ASSERT(m.begin() == req.keyRange.begin && m.end() == req.keyRange.end);
				if (m.value().activeMetadata.isValid()) {
					wait(start(bwData, &m.value(), req));
				}
			}
		}
		if (!isSelfReassign) {
			ASSERT(!req.reply.isSet());
			req.reply.send(Void());
		}
		return Void();
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled) {
			if (!bwData->shuttingDown && !isSelfReassign) {
				// the cancelled was because the granule open was cancelled, not because the whole blob
				// worker was.
				ASSERT(!req.reply.isSet());
				req.reply.sendError(granule_assignment_conflict());
			}
			throw e;
		}
		if (BW_DEBUG) {
			fmt::print("AssignRange [{0} - {1}) ({2}, {3}) in BW {4} got error {5}\n",
			           req.keyRange.begin.printable().c_str(),
			           req.keyRange.end.printable().c_str(),
			           req.managerEpoch,
			           req.managerSeqno,
			           bwData->id.toString().c_str(),
			           e.name());
		}

		if (!isSelfReassign) {
			if (e.code() == error_code_granule_assignment_conflict) {
				req.reply.sendError(e);
				bwData->stats.numRangesAssigned--;
				return Void();
			}

			if (canReplyWith(e)) {
				req.reply.sendError(e);
			}
		}

		TraceEvent(SevError, "BlobWorkerUnexpectedErrorRangeAssign", bwData->id)
		    .error(e)
		    .detail("Range", req.keyRange)
		    .detail("ManagerEpoch", req.managerEpoch)
		    .detail("SeqNo", req.managerSeqno);
		ASSERT_WE_THINK(false);

		// if not simulation, kill the BW
		if (bwData->fatalError.canBeSet()) {
			bwData->fatalError.sendError(e);
		}
		throw e;
	}
}

ACTOR Future<Void> handleRangeRevoke(Reference<BlobWorkerData> bwData, RevokeBlobRangeRequest req) {
	try {
		std::vector<Future<Void>> toWait;
		changeBlobRange(bwData, req.keyRange, req.managerEpoch, req.managerSeqno, false, req.dispose, false, toWait);
		wait(waitForAll(toWait));
		req.reply.send(Void());
		return Void();
	} catch (Error& e) {
		// FIXME: retry on error if dispose fails?
		if (BW_DEBUG) {
			fmt::print("RevokeRange [{0} - {1}) ({2}, {3}) got error {4}\n",
			           req.keyRange.begin.printable(),
			           req.keyRange.end.printable(),
			           req.managerEpoch,
			           req.managerSeqno,
			           e.name());
		}
		if (canReplyWith(e)) {
			req.reply.sendError(e);
		}
		throw;
	}
}

void handleBlobVersionRequest(Reference<BlobWorkerData> bwData, MinBlobVersionRequest req) {
	bwData->db->setDesiredChangeFeedVersion(
	    std::max<Version>(0, req.grv - (SERVER_KNOBS->TARGET_BW_LAG_UPDATE * SERVER_KNOBS->VERSIONS_PER_SECOND)));
	MinBlobVersionReply rep;
	rep.version = bwData->db->getMinimumChangeFeedVersion();
	bwData->stats.minimumCFVersion = rep.version;
	bwData->stats.cfVersionLag = std::max((Version)0, req.grv - rep.version);
	bwData->stats.notAtLatestChangeFeeds = bwData->db->notAtLatestChangeFeeds.size();
	req.reply.send(rep);
}

ACTOR Future<Void> registerBlobWorker(Reference<BlobWorkerData> bwData, BlobWorkerInterface interf) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);
	TraceEvent("BlobWorkerRegister", bwData->id);
	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			Key blobWorkerListKey = blobWorkerListKeyFor(interf.id());
			// FIXME: should be able to remove this conflict range
			tr->addReadConflictRange(singleKeyRange(blobWorkerListKey));
			tr->set(blobWorkerListKey, blobWorkerListValue(interf));

			// Get manager lock from DB
			Optional<Value> currentLockValue = wait(tr->get(blobManagerEpochKey));
			ASSERT(currentLockValue.present());
			int64_t currentEpoch = decodeBlobManagerEpochValue(currentLockValue.get());
			bwData->managerEpochOk(currentEpoch);

			wait(tr->commit());

			if (BW_DEBUG) {
				fmt::print("Registered blob worker {}\n", interf.id().toString());
			}
			TraceEvent("BlobWorkerRegistered", bwData->id);
			return Void();
		} catch (Error& e) {
			if (BW_DEBUG) {
				fmt::print("Registering blob worker {0} got error {1}\n", interf.id().toString(), e.name());
			}
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> monitorRemoval(Reference<BlobWorkerData> bwData) {
	state Key blobWorkerListKey = blobWorkerListKeyFor(bwData->id);
	loop {
		loop {
			state ReadYourWritesTransaction tr(bwData->db);
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);

				Optional<Value> val = wait(tr.get(blobWorkerListKey));
				if (!val.present()) {
					CODE_PROBE(true, "Blob worker found out BM killed it from reading DB");
					return Void();
				}

				state Future<Void> watchFuture = tr.watch(blobWorkerListKey);

				wait(tr.commit());
				wait(watchFuture);
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
}

// Because change feeds send uncommitted data and explicit rollback messages, we speculatively buffer/write
// uncommitted data. This means we must ensure the data is actually committed before "committing" those
// writes in the blob granule. The simplest way to do this is to have the blob worker do a periodic GRV,
// which is guaranteed to be an earlier committed version. Then, once the change feed has consumed up
// through the GRV's data, we can guarantee nothing will roll back the in-memory mutations
ACTOR Future<Void> runGRVChecks(Reference<BlobWorkerData> bwData) {
	state Transaction tr(bwData->db);
	loop {
		// only do grvs to get committed version if we need it to persist delta files
		while (bwData->grvVersion.numWaiting() == 0) {
			wait(bwData->doGRVCheck.getFuture());
			bwData->doGRVCheck = Promise<Void>();
		}

		// batch potentially multiple delta files into one GRV, and also rate limit GRVs for this worker
		wait(delay(SERVER_KNOBS->BLOB_WORKER_BATCH_GRV_INTERVAL));

		tr.reset();
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Version readVersion = wait(tr.getReadVersion());
			ASSERT(readVersion >= bwData->grvVersion.get());
			bwData->grvVersion.set(readVersion);

			++bwData->stats.commitVersionChecks;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

struct RDCEntry {
	double weight;
	Reference<GranuleMetadata> granule;
	RDCEntry(double weight, Reference<GranuleMetadata> granule) : weight(weight), granule(granule) {}
};

// for a top-k algorithm, we actually want a min-heap, so reverse the sort order
struct OrderForTopK {
	bool operator()(RDCEntry const& a, RDCEntry const& b) const { return b.weight - a.weight; }
};

typedef std::priority_queue<RDCEntry, std::vector<RDCEntry>, OrderForTopK> TopKPQ;

ACTOR Future<Void> runReadDrivenCompaction(Reference<BlobWorkerData> bwData) {
	state bool processedAll = true;
	loop {
		if (processedAll) {
			wait(bwData->doReadDrivenCompaction.getFuture());
			bwData->doReadDrivenCompaction.reset();
			wait(delay(0));
		}

		TopKPQ topK;

		// FIXME: possible to scan candidates instead of all granules?
		int candidates = 0;
		auto allRanges = bwData->granuleMetadata.intersectingRanges(normalKeys);
		for (auto& it : allRanges) {
			if (it.value().activeMetadata.isValid() && it.value().activeMetadata->cancelled.canBeSet()) {
				auto metadata = it.value().activeMetadata;
				if (metadata->rdcCandidate && metadata->isEligibleRDC() && metadata->runRDC.canBeSet() &&
				    metadata->pendingSnapshotVersion == metadata->durableSnapshotVersion.get()) {
					candidates++;
					double weight = metadata->weightRDC();
					if (weight > 1.0 &&
					    (topK.size() < SERVER_KNOBS->BLOB_WORKER_RDC_PARALLELISM || weight > topK.top().weight)) {
						if (topK.size() == SERVER_KNOBS->BLOB_WORKER_RDC_PARALLELISM) {
							topK.pop();
						}
						topK.push(RDCEntry(weight, metadata));
					}
				}
			}
		}

		CODE_PROBE(candidates > topK.size(), "Too many read-driven compaction candidates for one cycle");

		std::vector<Future<Void>> futures;
		futures.reserve(topK.size());
		while (!topK.empty()) {
			++bwData->stats.readDrivenCompactions;
			Promise<Void> runRDC = topK.top().granule->runRDC;
			ASSERT(runRDC.canBeSet());
			Future<Void> waitForSnapshotComplete = topK.top().granule->durableSnapshotVersion.whenAtLeast(
			                                           topK.top().granule->durableSnapshotVersion.get() + 1) ||
			                                       topK.top().granule->cancelled.getFuture();
			futures.push_back(waitForSnapshotComplete);
			topK.pop();
			runRDC.send(Void());
		}
		processedAll = futures.empty();
		if (!futures.empty()) {
			// wait at least one second to throttle this actor a bit
			wait(waitForAll(futures) && delay(1.0));
		}
	}
}

// FIXME: better way to do this?
// monitor system keyspace for new tenants
ACTOR Future<Void> monitorTenants(Reference<BlobWorkerData> bwData) {
	loop {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				state KeyBackedRangeResult<std::pair<TenantName, TenantMapEntry>> tenantResults;
				wait(store(tenantResults,
				           TenantMetadata::tenantMap().getRange(tr,
				                                                Optional<TenantName>(),
				                                                Optional<TenantName>(),
				                                                CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1)));
				ASSERT(tenantResults.results.size() <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER && !tenantResults.more);

				std::vector<std::pair<TenantName, TenantMapEntry>> tenants;
				for (auto& it : tenantResults.results) {
					// FIXME: handle removing/moving tenants!
					tenants.push_back(std::pair(it.first, it.second));
				}
				bwData->tenantData.addTenants(tenants);

				state Future<Void> watchChange = tr->watch(TenantMetadata::lastTenantId().key);
				wait(tr->commit());
				wait(watchChange);
				tr->reset();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}
}

namespace {
void handleGetGranuleAssignmentsRequest(Reference<BlobWorkerData> self, const GetGranuleAssignmentsRequest& req) {
	GetGranuleAssignmentsReply reply;
	auto allRanges = self->granuleMetadata.intersectingRanges(normalKeys);
	for (auto& it : allRanges) {
		if (it.value().activeMetadata.isValid() && it.value().activeMetadata->cancelled.canBeSet()) {
			// range is active, copy into reply's arena
			StringRef start = StringRef(reply.arena, it.begin());
			StringRef end = StringRef(reply.arena, it.end());

			reply.assignments.push_back(
			    reply.arena, GranuleAssignmentRef(KeyRangeRef(start, end), it.value().lastEpoch, it.value().lastSeqno));
		}
	}
	if (BW_DEBUG) {
		fmt::print("Worker {0} sending {1} granule assignments back to BM {2}\n",
		           self->id.toString(),
		           reply.assignments.size(),
		           req.managerEpoch);
	}
	req.reply.send(reply);
}
} // namespace

ACTOR Future<Void> handleFlushGranuleReq(Reference<BlobWorkerData> self, FlushGranuleRequest req) {
	++self->stats.flushGranuleReqs;

	auto myGranule = self->granuleMetadata.rangeContaining(req.granuleRange.begin);
	state Reference<GranuleMetadata> metadata = myGranule.cvalue().activeMetadata;
	if (req.granuleRange != myGranule.range() || !metadata.isValid() || !metadata->cancelled.canBeSet()) {
		if (BW_DEBUG) {
			fmt::print("BW {0} cannot flush granule [{1} - {2})\n",
			           self->id.toString().substr(0, 5),
			           req.granuleRange.begin.printable(),
			           req.granuleRange.end.printable());
		}
		req.reply.sendError(wrong_shard_server());
		return Void();
	}

	if (metadata->durableDeltaVersion.get() < req.flushVersion) {
		try {
			if (BW_DEBUG) {
				fmt::print("BW {0} flushing granule [{1} - {2}) @ {3}\n",
				           self->id.toString().substr(0, 5),
				           req.granuleRange.begin.printable(),
				           req.granuleRange.end.printable(),
				           req.flushVersion);
			}
			state Promise<Void> granuleCancelled = metadata->cancelled;
			choose {
				when(wait(metadata->readable.getFuture())) {}
				when(wait(granuleCancelled.getFuture())) {
					if (BW_DEBUG) {
						fmt::print("BW {0} flush granule [{1} - {2}) cancelled 2\n",
						           self->id.toString().substr(0, 5),
						           req.granuleRange.begin.printable(),
						           req.granuleRange.end.printable());
					}
					req.reply.sendError(wrong_shard_server());
					return Void();
				}
			}

			loop {
				// force granule to flush at this version, and wait
				if (req.flushVersion > metadata->pendingDeltaVersion) {
					// first, wait for granule active
					if (!metadata->activeCFData.get().isValid()) {
						req.reply.sendError(wrong_shard_server());
						return Void();
					}

					// wait for change feed version to catch up to ensure we have all data
					if (metadata->activeCFData.get()->getVersion() < req.flushVersion) {
						if (BW_DEBUG) {
							fmt::print("BW {0} flushing granule [{1} - {2}) @ {3}: waiting for CF version "
							           "(currently {4})\n",
							           self->id.toString().substr(0, 5),
							           req.granuleRange.begin.printable(),
							           req.granuleRange.end.printable(),
							           req.flushVersion,
							           metadata->activeCFData.get()->getVersion());
						}

						loop {
							choose {
								when(wait(metadata->activeCFData.get().isValid()
								              ? metadata->activeCFData.get()->whenAtLeast(req.flushVersion)
								              : Never())) {
									break;
								}
								when(wait(metadata->activeCFData.onChange())) {}
								when(wait(granuleCancelled.getFuture())) {
									if (BW_DEBUG) {
										fmt::print("BW {0} flush granule [{1} - {2}) cancelled 2\n",
										           self->id.toString().substr(0, 5),
										           req.granuleRange.begin.printable(),
										           req.granuleRange.end.printable());
									}
									req.reply.sendError(wrong_shard_server());
									return Void();
								}
							}
						}

						ASSERT(metadata->activeCFData.get()->getVersion() >= req.flushVersion);
						if (BW_DEBUG) {
							fmt::print("BW {0} flushing granule [{1} - {2}) @ {3}: got CF version\n",
							           self->id.toString().substr(0, 5),
							           req.granuleRange.begin.printable(),
							           req.granuleRange.end.printable(),
							           req.flushVersion);
						}
					}

					if (req.flushVersion > metadata->pendingDeltaVersion) {
						if (BW_DEBUG) {
							fmt::print("BW {0} flushing granule [{1} - {2}) @ {3}: setting force flush version\n",
							           self->id.toString().substr(0, 5),
							           req.granuleRange.begin.printable(),
							           req.granuleRange.end.printable(),
							           req.flushVersion);
						}
						// if after waiting for CF version, flushVersion still higher than pendingDeltaVersion,
						// set forceFlushVersion
						metadata->forceFlushVersion.set(req.flushVersion);
					}
				}

				if (BW_DEBUG) {
					fmt::print("BW {0} flushing granule [{1} - {2}) @ {3}: waiting durable\n",
					           self->id.toString().substr(0, 5),
					           req.granuleRange.begin.printable(),
					           req.granuleRange.end.printable(),
					           req.flushVersion);
				}
				choose {
					when(wait(metadata->durableDeltaVersion.whenAtLeast(req.flushVersion))) {
						if (BW_DEBUG) {
							fmt::print("BW {0} flushing granule [{1} - {2}) @ {3}: got durable\n",
							           self->id.toString().substr(0, 5),
							           req.granuleRange.begin.printable(),
							           req.granuleRange.end.printable(),
							           req.flushVersion);
						}

						req.reply.send(Void());
						return Void();
					}
					when(wait(metadata->activeCFData.onChange())) {
						// if a rollback happens, need to restart flush process
					}
					when(wait(granuleCancelled.getFuture())) {
						if (BW_DEBUG) {
							fmt::print("BW {0} flush granule [{1} - {2}) cancelled 3\n",
							           self->id.toString().substr(0, 5),
							           req.granuleRange.begin.printable(),
							           req.granuleRange.end.printable());
						}
						req.reply.sendError(wrong_shard_server());
						return Void();
					}
				}
			}
		} catch (Error& e) {
			if (BW_DEBUG) {
				fmt::print("BW {0} flushing granule [{1} - {2}) @ {3}: got unexpected error {4}\n",
				           self->id.toString().substr(0, 5),
				           req.granuleRange.begin.printable(),
				           req.granuleRange.end.printable(),
				           req.flushVersion,
				           e.name());
			}
			throw e;
		}
	} else {
		if (BW_DEBUG) {
			fmt::print("BW {0} already flushed granule [{1} - {2}) @ {3}\n",
			           self->id.toString().substr(0, 5),
			           req.granuleRange.begin.printable(),
			           req.granuleRange.end.printable(),
			           req.flushVersion);
		}

		req.reply.send(Void());
		return Void();
	}
}

ACTOR Future<Void> simForceFileWriteContention(Reference<BlobWorkerData> bwData) {
	// take the file write contention lock down to just 1 or 2 open writes
	int numToLeave = deterministicRandom()->randomInt(1, 3);
	state int numToTake = SERVER_KNOBS->BLOB_WORKER_DELTA_FILE_WRITE_PARALLELISM - numToLeave;
	ASSERT(bwData->deltaWritesLock->available() >= numToTake);

	if (numToTake <= 0) {
		return Void();
	}
	if (BW_DEBUG) {
		fmt::print("BW {0} forcing file contention down to {1}\n", bwData->id.toString().substr(0, 5), numToTake);
	}

	wait(bwData->deltaWritesLock->take(TaskPriority::DefaultYield, numToTake));
	if (BW_DEBUG) {
		fmt::print("BW {0} force acquired {1} file writes\n", bwData->id.toString().substr(0, 5), numToTake);
	}
	state FlowLock::Releaser holdingLock(*bwData->deltaWritesLock, numToTake);
	state Future<Void> delayFor = delay(deterministicRandom()->randomInt(10, 60));
	loop {
		choose {
			when(wait(delayFor)) {
				if (BW_DEBUG) {
					fmt::print("BW {0} releasing {1} file writes\n", bwData->id.toString().substr(0, 5), numToTake);
				}
				return Void();
			}
			// check for speed up sim
			when(wait(delay(5.0))) {
				if (g_simulator->speedUpSimulation) {
					if (BW_DEBUG) {
						fmt::print("BW {0} releasing {1} file writes b/c speed up simulation\n",
						           bwData->id.toString().substr(0, 5),
						           numToTake);
					}
					return Void();
				}
			}
		}
	}
}

ACTOR Future<Void> simForceFullMemory(Reference<BlobWorkerData> bwData) {
	// instead of randomly rejecting each request or not, simulate periods in which BW is full
	loop {
		wait(delayJittered(deterministicRandom()->randomInt(5, 20)));
		if (g_simulator->speedUpSimulation) {
			bwData->buggifyFull = false;
			if (BW_DEBUG) {
				fmt::print("BW {0}: ForceFullMemory exiting\n", bwData->id.toString().substr(0, 6));
			}
			return Void();
		}
		bwData->buggifyFull = !bwData->buggifyFull;
		if (BW_DEBUG) {
			fmt::print("BW {0}: ForceFullMemory {1}\n",
			           bwData->id.toString().substr(0, 6),
			           bwData->buggifyFull ? "starting" : "stopping");
		}
	}
}

ACTOR Future<Void> blobWorker(BlobWorkerInterface bwInterf,
                              ReplyPromise<InitializeBlobWorkerReply> recruitReply,
                              Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	state Reference<BlobWorkerData> self(new BlobWorkerData(
	    bwInterf.id(), dbInfo, openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True)));
	self->id = bwInterf.id();
	self->locality = bwInterf.locality;

	state Future<Void> collection = actorCollection(self->addActor.getFuture());

	if (BW_DEBUG) {
		printf("Initializing blob worker s3 stuff\n");
	}

	try {
		if (SERVER_KNOBS->BG_METADATA_SOURCE != "tenant") {
			if (BW_DEBUG) {
				fmt::print("BW constructing backup container from {0}\n", SERVER_KNOBS->BG_URL);
			}
			self->bstore = BlobConnectionProvider::newBlobConnectionProvider(SERVER_KNOBS->BG_URL);
			if (BW_DEBUG) {
				printf("BW constructed backup container\n");
			}
		}

		// register the blob worker to the system keyspace
		wait(registerBlobWorker(self, bwInterf));
	} catch (Error& e) {
		if (BW_DEBUG) {
			fmt::print("BW got init error {0}\n", e.name());
		}
		// if any errors came up while initializing the blob worker, let the blob manager know
		// that recruitment failed
		if (!recruitReply.isSet()) {
			recruitReply.sendError(recruitment_failed());
		}
		throw e;
	}

	// By now, we know that initialization was successful, so
	// respond to the initialization request with the interface itself
	// Note: this response gets picked up by the blob manager
	InitializeBlobWorkerReply rep;
	rep.interf = bwInterf;
	recruitReply.send(rep);

	self->addActor.send(waitFailureServer(bwInterf.waitFailure.getFuture()));
	self->addActor.send(runGRVChecks(self));
	self->addActor.send(monitorTenants(self));
	self->addActor.send(runReadDrivenCompaction(self));
	state Future<Void> selfRemoved = monitorRemoval(self);
	if (g_network->isSimulated() && BUGGIFY_WITH_PROB(0.25)) {
		self->addActor.send(simForceFileWriteContention(self));
	}
	if (g_network->isSimulated() && SERVER_KNOBS->BLOB_WORKER_DO_REJECT_WHEN_FULL && BUGGIFY_WITH_PROB(0.25)) {
		self->addActor.send(simForceFullMemory(self));
	}

	TraceEvent("BlobWorkerInit", self->id).log();

	try {
		loop choose {
			when(BlobGranuleFileRequest req = waitNext(bwInterf.blobGranuleFileRequest.getFuture())) {
				self->addActor.send(handleBlobGranuleFileRequest(self, req));
			}
			when(state GranuleStatusStreamRequest req = waitNext(bwInterf.granuleStatusStreamRequest.getFuture())) {
				if (self->managerEpochOk(req.managerEpoch)) {
					if (BW_DEBUG) {
						fmt::print("Worker {0} got new granule status endpoint {1} from BM {2}\n",
						           self->id.toString(),
						           req.reply.getEndpoint().token.toString().c_str(),
						           req.managerEpoch);
					}

					// send an error to the old stream before closing it, so it doesn't get broken_promise
					// and mark this endpoint as failed
					self->currentManagerStatusStream.get().sendError(connection_failed());

					// hold a copy of the previous stream if it exists, so any waiting send calls don't get
					// proken_promise before onChange
					ReplyPromiseStream<GranuleStatusReply> copy;
					if (self->statusStreamInitialized) {
						copy = self->currentManagerStatusStream.get();
					}
					req.reply.setByteLimit(SERVER_KNOBS->BLOBWORKERSTATUSSTREAM_LIMIT_BYTES);
					self->statusStreamInitialized = true;

					self->currentManagerStatusStream.set(req.reply);
				} else {
					req.reply.sendError(blob_manager_replaced());
				}
			}
			when(AssignBlobRangeRequest _req = waitNext(bwInterf.assignBlobRangeRequest.getFuture())) {
				++self->stats.rangeAssignmentRequests;
				state AssignBlobRangeRequest assignReq = _req;
				if (BW_DEBUG) {
					fmt::print("Worker {0} assigned range [{1} - {2}) @ ({3}, {4}):\n  type={5}\n",
					           self->id.toString(),
					           assignReq.keyRange.begin.printable(),
					           assignReq.keyRange.end.printable(),
					           assignReq.managerEpoch,
					           assignReq.managerSeqno,
					           assignReq.type);
				}

				if (self->managerEpochOk(assignReq.managerEpoch)) {
					self->addActor.send(handleRangeAssign(self, assignReq, false));
				} else {
					assignReq.reply.sendError(blob_manager_replaced());
				}
			}
			when(RevokeBlobRangeRequest _req = waitNext(bwInterf.revokeBlobRangeRequest.getFuture())) {
				state RevokeBlobRangeRequest revokeReq = _req;
				if (BW_DEBUG) {
					fmt::print("Worker {0} revoked range [{1} - {2}) @ ({3}, {4}):\n  dispose={5}\n",
					           self->id.toString(),
					           revokeReq.keyRange.begin.printable(),
					           revokeReq.keyRange.end.printable(),
					           revokeReq.managerEpoch,
					           revokeReq.managerSeqno,
					           revokeReq.dispose ? "T" : "F");
				}

				if (self->managerEpochOk(revokeReq.managerEpoch)) {
					self->addActor.send(handleRangeRevoke(self, revokeReq));
				} else {
					revokeReq.reply.sendError(blob_manager_replaced());
				}
			}
			when(AssignBlobRangeRequest granuleToReassign = waitNext(self->granuleUpdateErrors.getFuture())) {
				self->addActor.send(handleRangeAssign(self, granuleToReassign, true));
			}
			when(GetGranuleAssignmentsRequest req = waitNext(bwInterf.granuleAssignmentsRequest.getFuture())) {
				// if request isn't from a manager and is just validation, let it check
				if (req.managerEpoch == -1 || self->managerEpochOk(req.managerEpoch)) {
					if (BW_DEBUG) {
						fmt::print("Worker {0} got granule assignments request from BM {1}\n",
						           self->id.toString(),
						           req.managerEpoch);
					}
					handleGetGranuleAssignmentsRequest(self, req);
				} else {
					req.reply.sendError(blob_manager_replaced());
				}
			}
			when(HaltBlobWorkerRequest req = waitNext(bwInterf.haltBlobWorker.getFuture())) {
				if (self->managerEpochOk(req.managerEpoch)) {
					TraceEvent("BlobWorkerHalted", self->id)
					    .detail("ReqID", req.requesterID)
					    .detail("ManagerEpoch", req.managerEpoch);
					if (BW_DEBUG) {
						fmt::print("BW {0} was halted by manager {1}\n", bwInterf.id().toString(), req.managerEpoch);
					}
					req.reply.send(Void());
					break;
				} else {
					req.reply.sendError(blob_manager_replaced());
				}
			}
			when(MinBlobVersionRequest req = waitNext(bwInterf.minBlobVersionRequest.getFuture())) {
				handleBlobVersionRequest(self, req);
			}
			when(FlushGranuleRequest req = waitNext(bwInterf.flushGranuleRequest.getFuture())) {
				if (self->managerEpochOk(req.managerEpoch)) {
					if (BW_DEBUG) {
						fmt::print("BW {0} got flush granule req from {1}: [{2} - {3}) @ {4}\n",
						           bwInterf.id().toString(),
						           req.managerEpoch,
						           req.granuleRange.begin.printable(),
						           req.granuleRange.end.printable(),
						           req.flushVersion);
					}
					self->addActor.send(handleFlushGranuleReq(self, req));
				} else {
					req.reply.sendError(blob_manager_replaced());
				}
			}
			when(wait(collection)) {
				self->shuttingDown = true;
				TraceEvent("BlobWorkerActorCollectionError", self->id);
				ASSERT(false);
				throw internal_error();
			}
			when(wait(selfRemoved)) {
				if (BW_DEBUG) {
					printf("Blob worker detected removal. Exiting...\n");
				}
				TraceEvent("BlobWorkerRemoved", self->id);
				break;
			}
			when(wait(self->simInjectFailure.getFuture())) {
				// wait to let triggering actor finish to prevent weird shutdown races
				wait(delay(0));
				if (BW_DEBUG) {
					printf("Blob worker simulation injected failure. Exiting...\n");
				}
				TraceEvent("BlobWorkerSimRemoved", self->id);
				break;
			}
			when(wait(self->fatalError.getFuture())) {
				TraceEvent(SevError, "BlobWorkerActorCollectionFatalErrorNotError", self->id);
				ASSERT(false);
			}
		}
	} catch (Error& e) {
		self->shuttingDown = true;
		if (e.code() == error_code_operation_cancelled) {
			self->granuleMetadata.clear();
			throw;
		}
		if (BW_DEBUG) {
			printf("Blob worker got error %s. Exiting...\n", e.name());
		}
		TraceEvent("BlobWorkerDied", self->id).errorUnsuppressed(e);
	}

	self->shuttingDown = true;

	wait(self->granuleMetadata.clearAsync());
	return Void();
}

// TODO add unit tests for assign/revoke range, especially version ordering
