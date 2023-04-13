/*
 * Ratekeeper.actor.cpp
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

#include "fdbclient/ClientKnobs.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/Ratekeeper.h"
#include "fdbserver/TagThrottler.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/QuietDatabase.h"
#include "flow/OwningResource.h"

#include "flow/actorcompiler.h" // must be last include

const char* limitReasonName[] = { "workload",
	                              "storage_server_write_queue_size",
	                              "storage_server_write_bandwidth_mvcc",
	                              "storage_server_readable_behind",
	                              "log_server_mvcc_write_bandwidth",
	                              "log_server_write_queue",
	                              "storage_server_min_free_space",
	                              "storage_server_min_free_space_ratio",
	                              "log_server_min_free_space",
	                              "log_server_min_free_space_ratio",
	                              "storage_server_durability_lag",
	                              "storage_server_list_fetch_failed",
	                              "blob_worker_lag",
	                              "blob_worker_missing" };
static_assert(sizeof(limitReasonName) / sizeof(limitReasonName[0]) == limitReason_t_end, "limitReasonDesc table size");

int limitReasonEnd = limitReason_t_end;

// NOTE: This has a corresponding table in Script.cs (see RatekeeperReason graph)
// IF UPDATING THIS ARRAY, UPDATE SCRIPT.CS!
const char* limitReasonDesc[] = { "Workload or read performance.",
	                              "Storage server performance (storage queue).",
	                              "Storage server MVCC memory.",
	                              "Storage server version falling behind.",
	                              "Log server MVCC memory.",
	                              "Storage server performance (log queue).",
	                              "Storage server running out of space (approaching 100MB limit).",
	                              "Storage server running out of space (approaching 5% limit).",
	                              "Log server running out of space (approaching 100MB limit).",
	                              "Log server running out of space (approaching 5% limit).",
	                              "Storage server durable version falling behind.",
	                              "Unable to fetch storage server list.",
	                              "Blob worker granule version falling behind.",
	                              "No blob workers are reporting metrics." };

static_assert(sizeof(limitReasonDesc) / sizeof(limitReasonDesc[0]) == limitReason_t_end, "limitReasonDesc table size");

class RatekeeperImpl {
public:
	ACTOR static Future<bool> checkAnyBlobRanges(Database db) {
		state Transaction tr(db);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				// FIXME: check if any active ranges. This still returns true if there are inactive ranges, but it
				// mostly serves its purpose to allow setting blob_granules_enabled=1 on a cluster that has no blob
				// workers currently.
				RangeResult anyData = wait(tr.getRange(blobRangeKeys, 1));
				return !anyData.empty();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> monitorBlobWorkers(Ratekeeper* self, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
		state std::vector<BlobWorkerInterface> blobWorkers;
		state int workerFetchCount = 0;
		state double lastStartTime = 0;
		state double startTime = 0;
		state bool blobWorkerDead = false;
		state double lastLoggedTime = 0;

		loop {
			while (!self->configurationMonitor->areBlobGranulesEnabled()) {
				// FIXME: clear blob worker state if granules were previously enabled?
				wait(delay(SERVER_KNOBS->SERVER_LIST_DELAY));
			}

			state Version grv;
			state Future<Void> blobWorkerDelay =
			    delay(SERVER_KNOBS->METRIC_UPDATE_RATE * FLOW_KNOBS->DELAY_JITTER_OFFSET);
			int fetchAmount = SERVER_KNOBS->BW_FETCH_WORKERS_INTERVAL /
			                  (SERVER_KNOBS->METRIC_UPDATE_RATE * FLOW_KNOBS->DELAY_JITTER_OFFSET);
			if (++workerFetchCount == fetchAmount || blobWorkerDead) {
				workerFetchCount = 0;
				state Future<bool> anyBlobRangesCheck = checkAnyBlobRanges(self->db);
				wait(store(blobWorkers, getBlobWorkers(self->db, true, &grv)));
				wait(store(self->anyBlobRanges, anyBlobRangesCheck));
			} else {
				grv = self->recoveryTracker->getMaxVersion();
			}

			lastStartTime = startTime;
			startTime = now();

			if (blobWorkers.size() > 0) {
				state Future<Optional<BlobManagerBlockedReply>> blockedAssignments;
				if (dbInfo->get().blobManager.present()) {
					blockedAssignments =
					    timeout(brokenPromiseToNever(dbInfo->get().blobManager.get().blobManagerBlockedReq.getReply(
					                BlobManagerBlockedRequest())),
					            SERVER_KNOBS->BLOB_WORKER_TIMEOUT);
				}
				state std::vector<Future<Optional<MinBlobVersionReply>>> aliveVersions;
				aliveVersions.reserve(blobWorkers.size());
				for (auto& it : blobWorkers) {
					MinBlobVersionRequest req;
					req.grv = grv;
					aliveVersions.push_back(timeout(brokenPromiseToNever(it.minBlobVersionRequest.getReply(req)),
					                                SERVER_KNOBS->BLOB_WORKER_TIMEOUT));
				}
				if (blockedAssignments.isValid()) {
					wait(success(blockedAssignments));
					if (blockedAssignments.get().present() && blockedAssignments.get().get().blockedAssignments == 0) {
						self->unblockedAssignmentTime = now();
					}
				}
				wait(waitForAll(aliveVersions));
				Version minVer = grv;
				blobWorkerDead = false;
				int minIdx = 0;
				for (int i = 0; i < blobWorkers.size(); i++) {
					if (aliveVersions[i].get().present()) {
						if (aliveVersions[i].get().get().version < minVer) {
							minVer = aliveVersions[i].get().get().version;
							minIdx = i;
						}
					} else {
						blobWorkerDead = true;
						minVer = 0;
						minIdx = i;
						break;
					}
				}
				if (minVer > 0 && blobWorkers.size() > 0 &&
				    now() - self->unblockedAssignmentTime < SERVER_KNOBS->BW_MAX_BLOCKED_INTERVAL) {
					while (!self->blobWorkerVersionHistory.empty() &&
					       minVer < self->blobWorkerVersionHistory.back().second) {
						self->blobWorkerVersionHistory.pop_back();
					}
					self->blobWorkerVersionHistory.push_back(std::make_pair(now(), minVer));
				}
				while (self->blobWorkerVersionHistory.size() > SERVER_KNOBS->MIN_BW_HISTORY &&
				       self->blobWorkerVersionHistory[1].first <
				           self->blobWorkerVersionHistory.back().first - SERVER_KNOBS->BW_ESTIMATION_INTERVAL) {
					self->blobWorkerVersionHistory.pop_front();
				}
				if (now() - lastLoggedTime > SERVER_KNOBS->BW_RW_LOGGING_INTERVAL) {
					lastLoggedTime = now();
					TraceEvent("RkMinBlobWorkerVersion")
					    .detail("BWVersion", minVer)
					    .detail("MaxVer", self->recoveryTracker->getMaxVersion())
					    .detail("MinId", blobWorkers.size() > 0 ? blobWorkers[minIdx].id() : UID())
					    .detail("BMBlocked",
					            now() - self->unblockedAssignmentTime >= SERVER_KNOBS->BW_MAX_BLOCKED_INTERVAL);
				}
			}
			wait(blobWorkerDelay);
		}
	}

	ACTOR static Future<Void> run(RatekeeperInterface rkInterf, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
		state Ratekeeper self(
		    rkInterf.id(), openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True), dbInfo, rkInterf);
		state Future<Void> timeout = Void();
		state Future<Void> collection = actorCollection(self.addActor.getFuture());

		TraceEvent("RatekeeperStarting", rkInterf.id());
		self.addActor.send(waitFailureServer(rkInterf.waitFailure.getFuture()));
		self.addActor.send(self.configurationMonitor->run());

		self.addActor.send(self.metricsTracker->run());
		self.addActor.send(traceRole(Role::RATEKEEPER, rkInterf.id()));

		self.addActor.send(self.rateServer->run(
		    *self.normalRateUpdater, *self.batchRateUpdater, *self.tagThrottler, *self.recoveryTracker));

		if (SERVER_KNOBS->GLOBAL_TAG_THROTTLING) {
			self.addActor.send(self.quotaCache->run());
		}
		self.addActor.send(self.tagThrottler->monitorThrottlingChanges());
		if (SERVER_KNOBS->BW_THROTTLING_ENABLED) {
			self.addActor.send(self.monitorBlobWorkers(dbInfo));
		}

		TraceEvent("RkTLogQueueSizeParameters", rkInterf.id())
		    .detail("Target", SERVER_KNOBS->TARGET_BYTES_PER_TLOG)
		    .detail("Spring", SERVER_KNOBS->SPRING_BYTES_TLOG)
		    .detail(
		        "Rate",
		        (SERVER_KNOBS->TARGET_BYTES_PER_TLOG - SERVER_KNOBS->SPRING_BYTES_TLOG) /
		            ((((double)SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) / SERVER_KNOBS->VERSIONS_PER_SECOND) +
		             2.0));

		TraceEvent("RkStorageServerQueueSizeParameters", rkInterf.id())
		    .detail("Target", SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER)
		    .detail("Spring", SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER)
		    .detail("EBrake", SERVER_KNOBS->STORAGE_HARD_LIMIT_BYTES)
		    .detail(
		        "Rate",
		        (SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER - SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER) /
		            ((((double)SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) / SERVER_KNOBS->VERSIONS_PER_SECOND) +
		             2.0));

		try {
			loop choose {
				when(wait(timeout)) {
					double actualTps = self.rateServer->getSmoothReleasedTransactionRate();
					actualTps = std::max(std::max(1.0, actualTps),
					                     self.metricsTracker->getSmoothTotalDurableBytesRate() /
					                         CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT);

					if (self.actualTpsHistory.size() > SERVER_KNOBS->MAX_TPS_HISTORY_SAMPLES) {
						self.actualTpsHistory.pop_front();
					}
					self.actualTpsHistory.push_back(actualTps);

					self.recoveryTracker->cleanupOldRecoveries();

					self.normalRateUpdater->update(*self.metricsTracker,
					                               *self.rateServer,
					                               *self.tagThrottler,
					                               *self.configurationMonitor,
					                               *self.recoveryTracker,
					                               self.actualTpsHistory,
					                               self.anyBlobRanges,
					                               self.blobWorkerVersionHistory,
					                               self.blobWorkerTime,
					                               self.unblockedAssignmentTime);
					self.batchRateUpdater->update(*self.metricsTracker,
					                              *self.rateServer,
					                              *self.tagThrottler,
					                              *self.configurationMonitor,
					                              *self.recoveryTracker,
					                              self.actualTpsHistory,
					                              self.anyBlobRanges,
					                              self.blobWorkerVersionHistory,
					                              self.blobWorkerTime,
					                              self.unblockedAssignmentTime);
					self.tryUpdateAutoTagThrottling();

					self.rateServer->updateLastLimited(self.batchRateUpdater->getTpsLimit());
					self.rateServer->cleanupExpiredGrvProxies();
					timeout = delayJittered(SERVER_KNOBS->METRIC_UPDATE_RATE);
				}
				when(HaltRatekeeperRequest req = waitNext(rkInterf.haltRatekeeper.getFuture())) {
					req.reply.send(Void());
					TraceEvent("RatekeeperHalted", rkInterf.id()).detail("ReqID", req.requesterID);
					break;
				}
				when(wait(collection)) {
					ASSERT(false);
					throw internal_error();
				}
			}
		} catch (Error& err) {
			TraceEvent("RatekeeperDied", rkInterf.id()).errorUnsuppressed(err);
		}
		return Void();
	}
}; // class RatekeeperImpl

Future<Void> Ratekeeper::monitorBlobWorkers(Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	return RatekeeperImpl::monitorBlobWorkers(this, dbInfo);
}

Future<Void> Ratekeeper::run(RatekeeperInterface rkInterf, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	return RatekeeperImpl::run(rkInterf, dbInfo);
}

Ratekeeper::Ratekeeper(UID id,
                       Database db,
                       Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                       RatekeeperInterface rkInterf)
  : id(id), db(db), blobWorkerTime(now()), unblockedAssignmentTime(now()), anyBlobRanges(false) {
	if (SERVER_KNOBS->GLOBAL_TAG_THROTTLING) {
		quotaCache = std::make_unique<RKThroughputQuotaCache>(id, db);
		tagThrottler = std::make_unique<GlobalTagThrottler>(*quotaCache, id, SERVER_KNOBS->MAX_MACHINES_FALLING_BEHIND);
	} else {
		tagThrottler = std::make_unique<TagThrottler>(db, id);
	}
	metricsTracker =
	    std::make_unique<RKMetricsTracker>(id, db, rkInterf.reportCommitCostEstimation.getFuture(), dbInfo);
	configurationMonitor = std::make_unique<RKConfigurationMonitor>(db, dbInfo);
	recoveryTracker = std::make_unique<RKRecoveryTracker>(IAsyncListener<bool>::create(
	    dbInfo, [](auto const& info) { return info.recoveryState < RecoveryState::ACCEPTING_COMMITS; }));
	rateServer = std::make_unique<RKRateServer>(rkInterf.getRateInfo.getFuture());

	RatekeeperLimits normalLimits(TransactionPriority::DEFAULT,
	                              "",
	                              SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER,
	                              SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER,
	                              SERVER_KNOBS->TARGET_BYTES_PER_TLOG,
	                              SERVER_KNOBS->SPRING_BYTES_TLOG,
	                              SERVER_KNOBS->MAX_TL_SS_VERSION_DIFFERENCE,
	                              SERVER_KNOBS->TARGET_DURABILITY_LAG_VERSIONS,
	                              SERVER_KNOBS->TARGET_BW_LAG);
	RatekeeperLimits batchLimits(TransactionPriority::BATCH,
	                             "Batch",
	                             SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER_BATCH,
	                             SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER_BATCH,
	                             SERVER_KNOBS->TARGET_BYTES_PER_TLOG_BATCH,
	                             SERVER_KNOBS->SPRING_BYTES_TLOG_BATCH,
	                             SERVER_KNOBS->MAX_TL_SS_VERSION_DIFFERENCE_BATCH,
	                             SERVER_KNOBS->TARGET_DURABILITY_LAG_VERSIONS_BATCH,
	                             SERVER_KNOBS->TARGET_BW_LAG_BATCH);
	normalRateUpdater = std::make_unique<RKRateUpdater>(id, normalLimits);
	batchRateUpdater = std::make_unique<RKRateUpdater>(id, batchLimits);
}

void Ratekeeper::tryUpdateAutoTagThrottling() {
	auto const& storageQueueInfo = metricsTracker->getStorageQueueInfo();
	for (auto i = storageQueueInfo.begin(); i != storageQueueInfo.end(); ++i) {
		auto const& ss = i->value;
		addActor.send(tagThrottler->tryUpdateAutoThrottling(ss));
	}
}

ACTOR Future<Void> ratekeeper(RatekeeperInterface rkInterf, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	wait(Ratekeeper::run(rkInterf, dbInfo));
	return Void();
}
