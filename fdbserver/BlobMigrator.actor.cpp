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

#include "flow/ActorCollection.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/BlobConnectionProvider.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/BlobGranuleServerCommon.actor.h"
#include "fdbserver/BlobMigratorInterface.h"
#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h" // has to be last include
#include "flow/network.h"
#include <algorithm>
#include <string>

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
		if (!isFullRestoreMode()) {
			return Void();
		}
		wait(delay(10)); // TODO need to wait for a signal for readiness of blob manager

		BlobGranuleRestoreVersionVector granules = wait(listBlobGranules(self->db_, self->blobConn_));
		self->blobGranules_ = granules;

		wait(prepare(self, normalKeys));
		wait(advanceVersion(self));
		wait(serverLoop(self));
		return Void();
	}

private:
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
				wait(krmSetRange(&tr, keyServersPrefix, keys, value));
				wait(krmSetRange(&tr, serverKeysPrefixFor(serverUID), keys, serverKeysTrue));
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
						dprint("Unassign {} from storage server {}\n", keys.toString(), id.toString());
						wait(krmSetRange(&tr, serverKeysPrefixFor(id), keys, serverKeysFalse));
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
			if (done)
				return Void();
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
				bool done = incompleted == 0;
				dprint("Progress {} :{}%. done {}\n", serverID.toString(), progress, done);
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
				Version currentVersion = wait(tr.getRawReadVersion());
				Version expectedVersion = maxVersion(self);
				if (currentVersion <= expectedVersion) {
					tr.set(minRequiredCommitVersionKey, BinaryWriter::toValue(expectedVersion + 1, Unversioned()));
					dprint("Advance version from {} to {}\n", currentVersion, expectedVersion);
					wait(tr.commit());
				}
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Main server loop
	ACTOR static Future<Void> serverLoop(Reference<BlobMigrator> self) {
		self->actors_.add(waitFailureServer(self->interf_.ssi.waitFailure.getFuture()));
		self->actors_.add(logProgress(self));
		self->actors_.add(handleRequest(self));
		self->actors_.add(handleUnsupportedRequest(self));
		loop {
			try {
				choose {
					when(HaltBlobMigratorRequest req = waitNext(self->interf_.haltBlobMigrator.getFuture())) {
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
						fmt::print("Handle GetStorageMetrics\n");
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
						dprint("Unsupported UpdateCommitCostRequest\n");
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
		dprint("Unsupported StorageQueuingMetricsRequest\n");
		// FIXME get rid of this delay. it's a temp solution to avoid starvaion scheduling of DD
		// processes
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
};

// Main entry point
ACTOR Future<Void> blobMigrator(BlobMigratorInterface interf, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	fmt::print("Start blob migrator {} \n", interf.id().toString());
	try {
		Reference<BlobMigrator> self = makeReference<BlobMigrator>(dbInfo, interf);
		wait(BlobMigrator::start(self));
	} catch (Error& e) {
		dprint("Unexpected blob migrator error {}\n", e.what());
		TraceEvent("BlobMigratorError", interf.id()).error(e);
	}
	return Void();
}
