/*
 * BlobMigrator.actor.cpp
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
#include <algorithm>
#include <cmath>
#include <string>
#include "fdbclient/ClientBooleanParams.h"
#include "fdbserver/RestoreUtil.h"
#include "flow/CodeProbe.h"
#include "flow/network.h"
#include "flow/flow.h"
#include "flow/ActorCollection.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/BlobConnectionProvider.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/BlobGranuleServerCommon.actor.h"
#include "fdbserver/BlobMigratorInterface.h"
#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h" // has to be last include

#define ENABLE_DEBUG_MG true

template <typename... T>
static inline void dprint(fmt::format_string<T...> fmt, T&&... args) {
	if (ENABLE_DEBUG_MG)
		fmt::print(fmt, std::forward<T>(args)...);
}

// BlobMigrator manages data migration from blob storage to storage server. It implements a minimal set of
// StorageServerInterface APIs which are needed for DataDistributor to start data migration.
class BlobMigrator : public NonCopyable, public ReferenceCounted<BlobMigrator> {
public:
	BlobMigrator(Reference<AsyncVar<ServerDBInfo> const> dbInfo, BlobMigratorInterface interf)
	  : interf_(interf), actors_(false) {
		blobConn_ = BlobConnectionProvider::newBlobConnectionProvider(SERVER_KNOBS->BLOB_RESTORE_MANIFEST_URL);
		db_ = openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True);
	}
	~BlobMigrator() {}

	// Start migration
	ACTOR static Future<Void> start(Reference<BlobMigrator> self) {
		wait(checkIfReadyForMigration(self));
		wait(lockDatabase(self->db_, self->interf_.id()));
		wait(prepare(self, normalKeys));
		wait(advanceVersion(self));
		wait(serverLoop(self));
		return Void();
	}

	ACTOR static Future<Void> updateStatus(Reference<BlobMigrator> self, KeyRange keys, BlobRestoreStatus status) {
		wait(updateRestoreStatus(self->db_, keys, status, {}));
		return Void();
	}

private:
	// Check if blob manifest is loaded so that blob migration can start
	ACTOR static Future<Void> checkIfReadyForMigration(Reference<BlobMigrator> self) {
		loop {
			Optional<BlobRestoreStatus> status = wait(getRestoreStatus(self->db_, normalKeys));
			ASSERT(status.present());
			BlobRestorePhase phase = status.get().phase;
			if (phase == BlobRestorePhase::LOADED_MANIFEST) {
				BlobGranuleRestoreVersionVector granules = wait(listBlobGranules(self->db_, self->blobConn_));
				if (!granules.empty()) {
					self->blobGranules_ = granules;
					for (BlobGranuleRestoreVersion granule : granules) {
						TraceEvent("RestorableGranule", self->interf_.id())
						    .detail("GranuleId", granule.granuleID.toString())
						    .detail("KeyRange", granule.keyRange.toString())
						    .detail("Version", granule.version)
						    .detail("SizeInBytes", granule.sizeInBytes);
					}
					wait(updateRestoreStatus(self->db_,
					                         normalKeys,
					                         BlobRestoreStatus(BlobRestorePhase::COPYING_DATA),
					                         BlobRestorePhase::LOADED_MANIFEST));
					return Void();
				}
			} else if (phase >= BlobRestorePhase::COPYING_DATA && phase < BlobRestorePhase::DONE) {
				TraceEvent("BlobMigratorUnexpectedPhase", self->interf_.id()).detail("Phase", status.get().phase);
				throw restore_error();
			}
			wait(delay(SERVER_KNOBS->BLOB_MIGRATOR_CHECK_INTERVAL));
		}
	}

	// Prepare for data migration for given key range.
	ACTOR static Future<Void> prepare(Reference<BlobMigrator> self, KeyRangeRef keys) {
		wait(waitForDataMover(self));
		state int oldMode = wait(setDDMode(self->db_, 0));
		// Register as a storage server, so that DataDistributor could start data movement after
		std::pair<Version, Tag> verAndTag = wait(addStorageServer(self->db_, self->interf_.ssi));
		dprint("Started storage server interface {} {}\n", verAndTag.first, verAndTag.second.toString());

		// Reassign key ranges to the storage server
		// It'll restart DataDistributor so that internal data structures like ShardTracker, ShardsAffectedByTeamFailure
		// could be re-initialized. Ideally it should be done within DataDistributor, then we don't need to
		// restart DataDistributor
		wait(unassignServerKeys(self, keys));
		wait(assignKeysToServer(self, keys, self->interf_.ssi.id()));
		wait(success(setDDMode(self->db_, oldMode)));
		return Void();
	}

	// Wait until all pending data moving is done before doing full restore.
	ACTOR static Future<Void> waitForDataMover(Reference<BlobMigrator> self) {
		state int retries = 0;
		loop {
			state Transaction tr(self->db_);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				RangeResult dms = wait(tr.getRange(dataMoveKeys, CLIENT_KNOBS->TOO_MANY));
				if (dms.size() == 0) {
					return Void();
				} else {
					dprint("Wait pending data moving {}\n", dms.size());
					wait(delay(2));
					if (++retries > SERVER_KNOBS->BLOB_MIGRATOR_ERROR_RETRIES) {
						throw restore_error();
					}
				}
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Assign given key range to specified storage server.
	ACTOR static Future<Void> assignKeysToServer(Reference<BlobMigrator> self, KeyRangeRef keys, UID serverUID) {
		state Transaction tr(self->db_);
		loop {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				state Value value = keyServersValue(std::vector<UID>({ serverUID }), std::vector<UID>(), UID(), UID());
				wait(krmSetRangeCoalescing(&tr, keyServersPrefix, keys, allKeys, value));
				wait(krmSetRangeCoalescing(&tr, serverKeysPrefixFor(serverUID), keys, allKeys, serverKeysTrue));
				wait(tr.commit());
				dprint("Assign {} to server {}\n", normalKeys.toString(), serverUID.toString());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Unassign given key range from its current storage servers
	ACTOR static Future<Void> unassignServerKeys(Reference<BlobMigrator> self, KeyRangeRef keys) {
		state int retries = 0;
		loop {
			state Transaction tr(self->db_);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				state RangeResult serverList =
				    wait(tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY, Snapshot::True));
				ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);
				for (auto& server : serverList) {
					state UID id = decodeServerListValue(server.value).id();
					Optional<Value> tag = wait(tr.get(serverTagKeyFor(id)));
					if (!tag.present()) {
						dprint("Server {} no tag\n", id.shortString());
						continue;
					}
					if (id == self->interf_.id()) {
						continue;
					}
					RangeResult ranges = wait(krmGetRanges(&tr, serverKeysPrefixFor(id), keys));

					bool owning = false;
					for (auto& r : ranges) {
						if (r.value != serverKeysFalse) {
							owning = true;
							break;
						}
					}
					if (owning) {
						wait(krmSetRangeCoalescing(&tr, serverKeysPrefixFor(id), keys, allKeys, serverKeysFalse));
						dprint("Unassign {} from storage server {}\n", keys.toString(), id.toString());
						TraceEvent("UnassignKeys", self->interf_.id()).detail("Keys", keys).detail("SS", id);
					}
				}
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
				if (++retries > SERVER_KNOBS->BLOB_MIGRATOR_ERROR_RETRIES) {
					throw restore_error();
				}
			}
		}
	}

	// Print migration progress periodically
	ACTOR static Future<Void> logProgress(Reference<BlobMigrator> self) {
		loop {
			bool done = wait(checkProgress(self));
			if (done) {
				wait(updateRestoreStatus(self->db_,
				                         normalKeys,
				                         BlobRestoreStatus(BlobRestorePhase::APPLYING_MLOGS),
				                         BlobRestorePhase::COPYING_DATA));
				wait(unlockDatabase(self->db_, self->interf_.id()));
				wait(applyMutationLogs(self));

				wait(updateRestoreStatus(self->db_,
				                         normalKeys,
				                         BlobRestoreStatus(BlobRestorePhase::DONE),
				                         BlobRestorePhase::APPLYING_MLOGS));
				return Void();
			}
			wait(delay(SERVER_KNOBS->BLOB_MIGRATOR_CHECK_INTERVAL));
		}
	}

	// Check key ranges that are migrated. Return true if all ranges are done
	ACTOR static Future<bool> checkProgress(Reference<BlobMigrator> self) {
		state Transaction tr(self->db_);
		loop {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				// Get key ranges that are still owned by the migrator. Those ranges are
				// incompleted migrations
				state UID serverID = self->interf_.ssi.id();
				RangeResult ranges = wait(krmGetRanges(&tr, serverKeysPrefixFor(serverID), normalKeys));

				// Count incompleted size
				int64_t incompleted = 0;
				for (auto i = 0; i < ranges.size(); ++i) {
					if (ranges[i].value == serverKeysTrue) {
						KeyRef end = normalKeys.end;
						if (i < ranges.size() - 1) {
							end = ranges[i + 1].key;
						}
						KeyRangeRef range(ranges[i].key, end);
						int64_t bytes = sizeInBytes(self, range);
						dprint("   incompleted {}, size: {}\n", range.toString(), bytes);
						incompleted += bytes;
					}
				}

				// Calculated progress
				int64_t total = sizeInBytes(self);
				int progress = (total - incompleted) * 100 / total;
				state bool done = incompleted == 0;
				dprint("Migration progress :{}%. done {}\n", progress, done);
				TraceEvent("BlobMigratorProgress", self->interf_.id()).detail("Progress", progress);
				BlobRestoreStatus status(BlobRestorePhase::COPYING_DATA, progress);
				wait(updateRestoreStatus(self->db_, normalKeys, status, BlobRestorePhase::COPYING_DATA));
				return done;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Advance version, so that future commits will have a larger version than the restored data
	ACTOR static Future<Void> advanceVersion(Reference<BlobMigrator> self) {
		state Transaction tr(self->db_);
		loop {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				Version current = wait(tr.getRawReadVersion());
				Version expected = maxVersion(self);
				if (current <= expected) {
					tr.set(minRequiredCommitVersionKey, BinaryWriter::toValue(expected + 1, Unversioned()));
					dprint("Advance version from {} to {}\n", current, expected);
					TraceEvent("AdvanceVersion", self->interf_.id()).detail("From", current).detail("To", expected);
					wait(tr.commit());
				} else {
					dprint("Skip advancing version {}. current {}\n", expected, current);
					TraceEvent("SkipAdvanceVersion", self->interf_.id()).detail("From", current).detail("To", expected);
				}
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Check if we need to apply mutation logs. If all granules have data up to targetVersion, we don't need to apply
	// mutation logs
	static bool needApplyLogs(Reference<BlobMigrator> self, Version targetVersion) {
		for (auto& granule : self->blobGranules_) {
			if (granule.version < targetVersion) {
				// at least one granule doesn't have data up to target version, so we'll need to apply mutation logs
				return true;
			}
		}
		return false;
	}

	// Apply mutation logs to blob granules so that they reach to a consistent version for all blob granules
	ACTOR static Future<Void> applyMutationLogs(Reference<BlobMigrator> self) {
		// check last version in mutation logs
		state std::string mlogsUrl = wait(getMutationLogUrl());
		state Reference<IBackupContainer> bc = IBackupContainer::openContainer(mlogsUrl, {}, {});
		BackupDescription desc = wait(bc->describeBackup());
		if (!desc.contiguousLogEnd.present()) {
			TraceEvent("InvalidMutationLogs").detail("Url", SERVER_KNOBS->BLOB_RESTORE_MLOGS_URL);
			throw blob_restore_missing_logs();
		}
		if (!desc.minLogBegin.present()) {
			TraceEvent("InvalidMutationLogs").detail("Url", SERVER_KNOBS->BLOB_RESTORE_MLOGS_URL);
			throw blob_restore_missing_logs();
		}
		state Version minLogVersion = desc.minLogBegin.get();
		state Version maxLogVersion = desc.contiguousLogEnd.get() - 1;
		state Version targetVersion = wait(getRestoreTargetVersion(self->db_, normalKeys, maxLogVersion));
		if (targetVersion < maxLogVersion) {
			if (!needApplyLogs(self, targetVersion)) {
				TraceEvent("SkipMutationLogs").detail("TargetVersion", targetVersion);
				dprint("Skip mutation logs as all granules are at version {}\n", targetVersion);
				return Void();
			}
		}

		if (targetVersion < minLogVersion) {
			TraceEvent("MissingMutationLogs")
			    .detail("MinLogVersion", minLogVersion)
			    .detail("TargetVersion", maxLogVersion);
			throw blob_restore_missing_logs();
		}
		if (targetVersion > maxLogVersion) {
			TraceEvent("SkipTargetVersion")
			    .detail("MaxLogVersion", maxLogVersion)
			    .detail("TargetVersion", targetVersion);
		}

		// restore to target version
		state Standalone<VectorRef<KeyRangeRef>> ranges;
		state Standalone<VectorRef<Version>> beginVersions;
		for (auto& granule : self->blobGranules_) {
			if (granule.version < minLogVersion || granule.version > maxLogVersion) {
				TraceEvent("InvalidMutationLogs")
				    .detail("Granule", granule.granuleID)
				    .detail("GranuleVersion", granule.version)
				    .detail("MinLogVersion", minLogVersion)
				    .detail("MaxLogVersion", maxLogVersion)
				    .detail("TargetVersion", targetVersion);
				throw blob_restore_corrupted_logs();
			}
			// no need to apply mutation logs if granule is already on that version
			if (granule.version < targetVersion) {
				ranges.push_back(ranges.arena(), granule.keyRange);
				// Blob granule ends at granule.version(inclusive), so we need to apply mutation logs
				// after granule.version(exclusive).
				beginVersions.push_back(beginVersions.arena(), granule.version);
				TraceEvent("ApplyMutationLogVersion").detail("GID", granule.granuleID).detail("Ver", granule.version);
			}
		}
		Optional<RestorableFileSet> restoreSet =
		    wait(bc->getRestoreSet(maxLogVersion, ranges, OnlyApplyMutationLogs::True, minLogVersion));
		if (!restoreSet.present()) {
			TraceEvent("InvalidMutationLogs")
			    .detail("MinLogVersion", minLogVersion)
			    .detail("MaxLogVersion", maxLogVersion);
			throw blob_restore_corrupted_logs();
		}
		std::string tagName = "blobrestore-" + self->interf_.id().shortString();
		TraceEvent("ApplyMutationLogs", self->interf_.id())
		    .detail("MinVer", minLogVersion)
		    .detail("MaxVer", maxLogVersion);

		wait(submitRestore(self, KeyRef(tagName), KeyRef(mlogsUrl), ranges, beginVersions, targetVersion));
		return Void();
	}

	// Submit restore task to backup agent
	ACTOR static Future<Void> submitRestore(Reference<BlobMigrator> self,
	                                        Key tagName,
	                                        Key mutationLogsUrl,
	                                        Standalone<VectorRef<KeyRangeRef>> ranges,
	                                        Standalone<VectorRef<Version>> beginVersions,
	                                        Version endVersion) {
		state Optional<std::string> proxy; // unused
		state Optional<Database> origDb; // unused

		TraceEvent("ApplyMutationLogsStart", self->interf_.id()).detail("Tag", tagName);
		Version version = wait(self->backupAgent_.restore(self->db_,
		                                                  origDb,
		                                                  KeyRef(tagName),
		                                                  KeyRef(mutationLogsUrl),
		                                                  proxy,
		                                                  ranges,
		                                                  beginVersions,
		                                                  WaitForComplete::True,
		                                                  endVersion,
		                                                  Verbose::True,
		                                                  ""_sr, // addPrefix
		                                                  ""_sr, // removePrefix
		                                                  LockDB::True,
		                                                  UnlockDB::True,
		                                                  OnlyApplyMutationLogs::True));
		TraceEvent("ApplyMutationLogsComplete", self->interf_.id()).detail("Version", version);
		dprint("Restore to version {} done. Target version {} \n", version, endVersion);
		return Void();
	}

	// Main server loop
	ACTOR static Future<Void> serverLoop(Reference<BlobMigrator> self) {
		self->actors_.add(waitFailureServer(self->interf_.waitFailure.getFuture()));
		self->actors_.add(logProgress(self));
		self->actors_.add(handleRequest(self));
		self->actors_.add(handleUnsupportedRequest(self));
		loop {
			try {
				choose {
					when(HaltBlobMigratorRequest req = waitNext(self->interf_.haltBlobMigrator.getFuture())) {
						dprint("Stopping blob migrator {}\n", self->interf_.id().toString());
						req.reply.send(Void());
						TraceEvent("BlobMigratorHalted", self->interf_.id()).detail("ReqID", req.requesterID);
						break;
					}
					when(wait(self->actors_.getResult())) {}
				}
			} catch (Error& e) {
				dprint("Unexpected serverLoop error {}\n", e.what());
				throw;
			}
		}
		self->actors_.clear(true);
		dprint("Stopped blob migrator {}\n", self->interf_.id().toString());
		return Void();
	}

	// Handle StorageServerInterface APIs
	ACTOR static Future<Void> handleRequest(Reference<BlobMigrator> self) {
		state StorageServerInterface ssi = self->interf_.ssi;
		loop {
			try {
				choose {
					when(GetShardStateRequest req = waitNext(ssi.getShardState.getFuture())) {
						dprint("Handle GetShardStateRequest\n");
						Version version = maxVersion(self);
						GetShardStateReply rep(version, version);
						req.reply.send(rep); // return empty shards
					}
					when(WaitMetricsRequest req = waitNext(ssi.waitMetrics.getFuture())) {
						// dprint("Handle WaitMetricsRequest\n");
						self->actors_.add(processWaitMetricsRequest(self, req));
					}
					when(SplitMetricsRequest req = waitNext(ssi.splitMetrics.getFuture())) {
						dprint("Handle SplitMetrics {} limit {} bytes\n", req.keys.toString(), req.limits.bytes);
						processSplitMetricsRequest(self, req);
					}
					when(GetStorageMetricsRequest req = waitNext(ssi.getStorageMetrics.getFuture())) {
						StorageMetrics metrics;
						metrics.bytes = sizeInBytes(self);
						GetStorageMetricsReply resp;
						resp.load = metrics;
						resp.available = StorageMetrics();
						resp.capacity = StorageMetrics();
						resp.bytesInputRate = 0;
						resp.versionLag = 0;
						resp.lastUpdate = now();
						req.reply.send(resp);
					}
					when(ReplyPromise<KeyValueStoreType> reply = waitNext(ssi.getKeyValueStoreType.getFuture())) {
						dprint("Handle KeyValueStoreType\n");
						reply.send(KeyValueStoreType::MEMORY);
					}
				}
			} catch (Error& e) {
				dprint("Unexpected blob migrator request error {}\n", e.what());
				throw;
			}
		}
	}

	// Handle StorageServerInterface APIs that are not supported. Simply log and return error
	ACTOR static Future<Void> handleUnsupportedRequest(Reference<BlobMigrator> self) {
		state StorageServerInterface ssi = self->interf_.ssi;
		loop {
			try {
				choose {
					when(SplitRangeRequest req = waitNext(ssi.getRangeSplitPoints.getFuture())) {
						dprint("Unsupported SplitRangeRequest\n");
						req.reply.sendError(broken_promise());
					}
					when(StorageQueuingMetricsRequest req = waitNext(ssi.getQueuingMetrics.getFuture())) {
						self->actors_.add(processStorageQueuingMetricsRequest(req));
					}
					when(ReadHotSubRangeRequest req = waitNext(ssi.getReadHotRanges.getFuture())) {
						dprint("Unsupported ReadHotSubRange\n");
						req.reply.sendError(unsupported_operation());
					}
					when(GetKeyValuesStreamRequest req = waitNext(ssi.getKeyValuesStream.getFuture())) {
						dprint("Unsupported GetKeyValuesStreamRequest\n");
						req.reply.sendError(unsupported_operation());
					}
					when(GetKeyRequest req = waitNext(ssi.getKey.getFuture())) {
						dprint("Unsupported GetKeyRequest\n");
						req.reply.sendError(unsupported_operation());
					}
					when(GetKeyValuesRequest req = waitNext(ssi.getKeyValues.getFuture())) {
						dprint("Unsupported GetKeyValuesRequest {} - {} @ {}\n",
						       req.begin.getKey().printable(),
						       req.end.getKey().printable(),
						       req.version);
						// A temp fix to send back broken promise error so that fetchKey can switch to another
						// storage server. We should remove the storage server interface after
						// restore is done
						req.reply.sendError(broken_promise());
					}
					when(GetValueRequest req = waitNext(ssi.getValue.getFuture())) {
						dprint("Unsupported GetValueRequest\n");
						req.reply.sendError(unsupported_operation());
					}
					when(GetCheckpointRequest req = waitNext(ssi.checkpoint.getFuture())) {
						dprint("Unsupported GetCheckpoint \n");
						req.reply.sendError(unsupported_operation());
					}
					when(FetchCheckpointRequest req = waitNext(ssi.fetchCheckpoint.getFuture())) {
						dprint("Unsupported FetchCheckpointRequest\n");
						req.reply.sendError(unsupported_operation());
					}
					when(UpdateCommitCostRequest req = waitNext(ssi.updateCommitCostRequest.getFuture())) {
						// dprint("Unsupported UpdateCommitCostRequest\n");
						req.reply.sendError(unsupported_operation());
					}
					when(FetchCheckpointKeyValuesRequest req = waitNext(ssi.fetchCheckpointKeyValues.getFuture())) {
						dprint("Unsupported FetchCheckpointKeyValuesRequest\n");
						req.reply.sendError(unsupported_operation());
					}
				}
			} catch (Error& e) {
				dprint("Unexpected request handling error {}\n", e.what());
				throw;
			}
		}
	}

	// This API is used by DD to figure out split points for data movement.
	static void processSplitMetricsRequest(Reference<BlobMigrator> self, SplitMetricsRequest req) {
		SplitMetricsReply rep;
		int64_t bytes = 0; // number of bytes accumulated for current split
		for (auto& granule : self->blobGranules_) {
			if (!req.keys.contains(granule.keyRange)) {
				continue;
			}
			bytes += granule.sizeInBytes;
			if (bytes < req.limits.bytes) {
				continue;
			}
			// Add a split point if the key range exceeds expected minimal size in bytes
			rep.splits.push_back_deep(rep.splits.arena(), granule.keyRange.end);
			bytes = 0;
			// Limit number of splits in single response for fast RPC processing
			if (rep.splits.size() > SERVER_KNOBS->SPLIT_METRICS_MAX_ROWS) {
				CODE_PROBE(true, "Blob Migrator SplitMetrics API has more");
				TraceEvent("BlobMigratorSplitMetricsContinued", self->interf_.id())
				    .detail("Range", req.keys)
				    .detail("Splits", rep.splits.size());
				rep.more = true;
				break;
			}
		}
		req.reply.send(rep);
	}

	ACTOR static Future<Void> processWaitMetricsRequest(Reference<BlobMigrator> self, WaitMetricsRequest req) {
		state WaitMetricsRequest waitMetricsRequest = req;
		// FIXME get rid of this delay. it's a temp solution to avoid starvaion scheduling of DD
		// processes
		wait(delay(1));
		StorageMetrics metrics;
		metrics.bytes = sizeInBytes(self, waitMetricsRequest.keys);
		waitMetricsRequest.reply.send(metrics);
		return Void();
	}

	ACTOR static Future<Void> processStorageQueuingMetricsRequest(StorageQueuingMetricsRequest req) {
		// dprint("Unsupported StorageQueuingMetricsRequest\n");
		//  FIXME get rid of this delay. it's a temp solution to avoid starvaion scheduling of DD
		//  processes
		wait(delay(1));
		req.reply.sendError(unsupported_operation());
		return Void();
	}

	// Return total storage size in bytes for migration
	static int64_t sizeInBytes(Reference<BlobMigrator> self) { return sizeInBytes(self, normalKeys); }

	// Return storage size in bytes for given key range
	static int64_t sizeInBytes(Reference<BlobMigrator> self, KeyRangeRef range) {
		int64_t bytes = 0;
		for (auto granule : self->blobGranules_) {
			if (range.contains(granule.keyRange))
				bytes += granule.sizeInBytes;
		}
		return bytes;
	}

	// Return max version for all blob granules
	static Version maxVersion(Reference<BlobMigrator> self) {
		Version max = 0;
		for (auto granule : self->blobGranules_) {
			max = std::max(granule.version, max);
		}
		return max;
	}

private:
	Database db_;
	Reference<BlobConnectionProvider> blobConn_;
	BlobGranuleRestoreVersionVector blobGranules_;
	BlobMigratorInterface interf_;
	ActorCollection actors_;
	FileBackupAgent backupAgent_;
};

// Main entry point
ACTOR Future<Void> blobMigrator(BlobMigratorInterface interf, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	TraceEvent("StartBlobMigrator", interf.id()).detail("Interface", interf.id().toString());
	dprint("Starting blob migrator {}\n", interf.id().toString());
	state Reference<BlobMigrator> self = makeReference<BlobMigrator>(dbInfo, interf);
	try {
		wait(BlobMigrator::start(self));
	} catch (Error& e) {
		dprint("Unexpected blob migrator error {}\n", e.what());
		TraceEvent("BlobMigratorError", interf.id()).error(e);
		wait(BlobMigrator::updateStatus(self, normalKeys, BlobRestoreStatus(BlobRestorePhase::ERROR, e.code())));
	}
	return Void();
}
