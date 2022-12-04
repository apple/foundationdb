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
#include <string>
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
		if (!blobConn_.isValid() && SERVER_KNOBS->BG_METADATA_SOURCE != "tenant") {
			blobConn_ = BlobConnectionProvider::newBlobConnectionProvider(SERVER_KNOBS->BG_URL);
		}
		db_ = openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True);
	}
	~BlobMigrator() {}

	// Start migration
	ACTOR static Future<Void> start(Reference<BlobMigrator> self) {
		wait(checkIfReadyForMigration(self));
		wait(prepare(self, normalKeys));
		wait(advanceVersion(self));
		wait(serverLoop(self));
		return Void();
	}

private:
	// Check if blob manifest is loaded so that blob migration can start
	ACTOR static Future<Void> checkIfReadyForMigration(Reference<BlobMigrator> self) {
		loop {
			Optional<BlobRestoreStatus> status = wait(getRestoreStatus(self->db_, normalKeys));
			if (canStartMigration(status)) {
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

					BlobRestoreStatus status(BlobRestorePhase::MIGRATE, 0);
					wait(updateRestoreStatus(self->db_, normalKeys, status));
					return Void();
				}
			}
			wait(delay(SERVER_KNOBS->BLOB_MIGRATOR_CHECK_INTERVAL));
		}
	}

	// Check if we should start migration. Migration can be started after manifest is fully loaded
	static bool canStartMigration(Optional<BlobRestoreStatus> status) {
		if (status.present()) {
			BlobRestoreStatus value = status.get();
			return value.phase == BlobRestorePhase::MANIFEST_DONE; // manifest is loaded successfully
		}
		return false;
	}

	// Prepare for data migration for given key range.
	ACTOR static Future<Void> prepare(Reference<BlobMigrator> self, KeyRangeRef keys) {
		// Register as a storage server, so that DataDistributor could start data movement after
		std::pair<Version, Tag> verAndTag = wait(addStorageServer(self->db_, self->interf_.ssi));
		dprint("Started storage server interface {} {}\n", verAndTag.first, verAndTag.second.toString());

		// Reassign key ranges to the storage server
		// It'll restart DataDistributor so that internal data structures like ShardTracker, ShardsAffectedByTeamFailure
		// could be re-initialized. Ideally it should be done within DataDistributor, then we don't need to
		// restart DataDistributor
		state int oldMode = wait(setDDMode(self->db_, 0));
		wait(unassignServerKeys(self, keys));
		wait(assignKeysToServer(self, keys, self->interf_.ssi.id()));
		wait(success(setDDMode(self->db_, oldMode)));
		return Void();
	}

	// Assign given key range to specified storage server. Subsquent
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
		state Transaction tr(self->db_);
		loop {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				state RangeResult serverList = wait(tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);
				for (auto& server : serverList) {
					state UID id = decodeServerListValue(server.value).id();
					RangeResult ranges = wait(krmGetRanges(&tr, serverKeysPrefixFor(id), keys));
					bool owning = false;
					for (auto& r : ranges) {
						if (r.value == serverKeysTrue) {
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
			}
		}
	}

	// Print migration progress periodically
	ACTOR static Future<Void> logProgress(Reference<BlobMigrator> self) {
		loop {
			bool done = wait(checkProgress(self));
			if (done) {
				wait(updateRestoreStatus(self->db_, normalKeys, BlobRestoreStatus(BlobRestorePhase::APPLY_MLOGS)));
				wait(applyMutationLogs(self));
				wait(updateRestoreStatus(self->db_, normalKeys, BlobRestoreStatus(BlobRestorePhase::DONE)));
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
				for (auto i = 0; i < ranges.size() - 1; ++i) {
					if (ranges[i].value == serverKeysTrue) {
						KeyRangeRef range(ranges[i].key, ranges[i + 1].key);
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
				BlobRestoreStatus status(BlobRestorePhase::MIGRATE, progress);
				wait(updateRestoreStatus(self->db_, normalKeys, status));
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
				}
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Find mutation logs url
	static std::string mlogsUrl(Reference<BlobMigrator> self) {
		std::string url = SERVER_KNOBS->BLOB_RESTORE_MLOGS_URL;

		// A special case for local directory.
		// See FileBackupAgent.actor.cpp. if the container string describes a local directory then "/backup-<timestamp>"
		// will be added to it. so we need to append this directory name to the url
		if (url.find("file://") == 0) {
			std::string path = url.substr(7);
			path.erase(path.find_last_not_of("\\/") + 1); // Remove trailing slashes
			std::vector<std::string> dirs = platform::listDirectories(path);
			if (dirs.empty()) {
				TraceEvent(SevError, "BlobMigratorMissingMutationLogs").detail("Url", url);
				throw restore_missing_data();
			}
			// Pick the newest backup folder
			std::sort(dirs.begin(), dirs.end());
			std::string name = dirs.back();
			url.erase(url.find_last_not_of("\\/") + 1); // Remove trailing slashes
			return url + "/" + name;
		}
		return url;
	}

	// Apply mutation logs to blob granules so that they reach to a consistent version for all blob granules
	ACTOR static Future<Void> applyMutationLogs(Reference<BlobMigrator> self) {
		state std::string mutationLogsUrl = mlogsUrl(self);
		TraceEvent("ApplyMutationLogs", self->interf_.id()).detail("Url", mutationLogsUrl);

		// check last version in mutation logs
		Optional<std::string> proxy; // unused
		Optional<std::string> encryptionKeyFile; // unused
		Reference<IBackupContainer> bc = IBackupContainer::openContainer(mutationLogsUrl, proxy, encryptionKeyFile);
		BackupDescription desc = wait(bc->describeBackup());
		if (!desc.contiguousLogEnd.present()) {
			TraceEvent(SevError, "BlobMigratorInvalidMutationLogs").detail("Url", mutationLogsUrl);
			throw restore_missing_data();
		}
		Version targetVersion = desc.contiguousLogEnd.get() - 1;
		TraceEvent("ApplyMutationLogs", self->interf_.id()).detail("Version", targetVersion);

		// restore to target version
		Standalone<VectorRef<KeyRangeRef>> ranges;
		Standalone<VectorRef<Version>> beginVersions;
		for (auto& granule : self->blobGranules_) {
			ranges.push_back(ranges.arena(), granule.keyRange);
			beginVersions.push_back(beginVersions.arena(), granule.version);
		}
		std::string tagName = "blobrestore-" + self->interf_.id().shortString();
		wait(submitRestore(self, KeyRef(tagName), KeyRef(mutationLogsUrl), ranges, beginVersions));
		return Void();
	}

	// Submit restore task to backup agent
	ACTOR static Future<Void> submitRestore(Reference<BlobMigrator> self,
	                                        Key tagName,
	                                        Key mutationLogsUrl,
	                                        Standalone<VectorRef<KeyRangeRef>> ranges,
	                                        Standalone<VectorRef<Version>> beginVersions) {
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
		                                                  invalidVersion,
		                                                  Verbose::False,
		                                                  ""_sr, // addPrefix
		                                                  ""_sr, // removePrefix
		                                                  LockDB::False,
		                                                  UnlockDB::False,
		                                                  OnlyApplyMutationLogs::True));
		TraceEvent("ApplyMutationLogsComplete", self->interf_.id()).detail("Version", version);
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
						dprint("Handle SplitMetrics {}\n", req.keys.toString());
						SplitMetricsReply rep;
						for (auto granule : self->blobGranules_) {
							// TODO: Use granule boundary as split point. A better approach is to split by size
							if (granule.keyRange.begin > req.keys.begin && granule.keyRange.end < req.keys.end)
								rep.splits.push_back_deep(rep.splits.arena(), granule.keyRange.begin);
						}
						req.reply.send(rep);
					}
					when(GetStorageMetricsRequest req = waitNext(ssi.getStorageMetrics.getFuture())) {
						// fmt::print("Handle GetStorageMetrics\n");
						StorageMetrics metrics;
						metrics.bytes = sizeInBytes(self);
						GetStorageMetricsReply resp;
						resp.load = metrics;
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
						req.reply.sendError(unsupported_operation());
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
						/* dprint("Unsupported GetKeyValuesRequest {} - {} @ {}\n",
						       req.begin.getKey().printable(),
						       req.end.getKey().printable(),
						       req.version); */
						req.reply.sendError(unsupported_operation());
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
			if (range.intersects(granule.keyRange))
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
	try {
		Reference<BlobMigrator> self = makeReference<BlobMigrator>(dbInfo, interf);
		wait(BlobMigrator::start(self));
	} catch (Error& e) {
		dprint("Unexpected blob migrator error {}\n", e.what());
		TraceEvent("BlobMigratorError", interf.id()).error(e);
	}
	return Void();
}
