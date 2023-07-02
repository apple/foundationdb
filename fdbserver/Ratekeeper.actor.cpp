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

ACTOR static Future<Void> splitError(Future<Void> in, Promise<Void> errOut) {
	try {
		wait(in);
		return Void();
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled && !errOut.isSet())
			errOut.sendError(e);
		throw;
	}
}

class RatekeeperImpl {
public:
	ACTOR static Future<Void> configurationMonitor(Ratekeeper* self) {
		loop {
			state ReadYourWritesTransaction tr(self->db);

			loop {
				try {
					tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					RangeResult results = wait(tr.getRange(configKeys, CLIENT_KNOBS->TOO_MANY));
					ASSERT(!results.more && results.size() < CLIENT_KNOBS->TOO_MANY);

					self->configuration.fromKeyValues((VectorRef<KeyValueRef>)results);

					state Future<Void> watchFuture =
					    tr.watch(moveKeysLockOwnerKey) || tr.watch(excludedServersVersionKey) ||
					    tr.watch(failedServersVersionKey) || tr.watch(excludedLocalityVersionKey) ||
					    tr.watch(failedLocalityVersionKey);
					wait(tr.commit());
					wait(watchFuture);
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}
	}

	ACTOR static Future<Void> monitorServerListChange(
	    Ratekeeper* self,
	    PromiseStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges) {
		state std::map<UID, StorageServerInterface> oldServers;
		state Transaction tr(self->db);

		loop {
			try {
				if (now() - self->lastSSListFetchedTimestamp > 2 * SERVER_KNOBS->SERVER_LIST_DELAY) {
					TraceEvent(SevWarnAlways, "RatekeeperGetSSListLongLatency", self->id)
					    .detail("Latency", now() - self->lastSSListFetchedTimestamp);
				}
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				std::vector<std::pair<StorageServerInterface, ProcessClass>> results =
				    wait(NativeAPI::getServerListAndProcessClasses(&tr));
				self->lastSSListFetchedTimestamp = now();

				std::map<UID, StorageServerInterface> newServers;
				for (const auto& [ssi, _] : results) {
					const UID serverId = ssi.id();
					newServers[serverId] = ssi;

					if (oldServers.count(serverId)) {
						if (ssi.getValue.getEndpoint() != oldServers[serverId].getValue.getEndpoint() ||
						    ssi.isAcceptingRequests() != oldServers[serverId].isAcceptingRequests()) {
							serverChanges.send(std::make_pair(serverId, Optional<StorageServerInterface>(ssi)));
						}
						oldServers.erase(serverId);
					} else {
						serverChanges.send(std::make_pair(serverId, Optional<StorageServerInterface>(ssi)));
					}
				}

				for (const auto& it : oldServers) {
					serverChanges.send(std::make_pair(it.first, Optional<StorageServerInterface>()));
				}

				oldServers.swap(newServers);
				tr = Transaction(self->db);
				wait(delay(SERVER_KNOBS->SERVER_LIST_DELAY));
			} catch (Error& e) {
				if (e.code() != error_code_actor_cancelled) {
					TraceEvent("RatekeeperGetSSListError", self->id).errorUnsuppressed(e).suppressFor(1.0);
				}
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> trackStorageServerQueueInfo(ActorWeakSelfRef<Ratekeeper> self,
	                                                      StorageServerInterface ssi) {
		self->storageQueueInfo.insert(mapPair(ssi.id(), StorageQueueInfo(self->id, ssi.id(), ssi.locality)));
		TraceEvent("RkTracking", self->id)
		    .detail("StorageServer", ssi.id())
		    .detail("Locality", ssi.locality.toString());
		try {
			loop {
				ErrorOr<StorageQueuingMetricsReply> reply = wait(ssi.getQueuingMetrics.getReplyUnlessFailedFor(
				    StorageQueuingMetricsRequest(), 0, 0)); // SOMEDAY: or tryGetReply?
				Map<UID, StorageQueueInfo>::iterator myQueueInfo = self->storageQueueInfo.find(ssi.id());
				if (reply.present()) {
					myQueueInfo->value.update(reply.get(), self->smoothTotalDurableBytes);
					myQueueInfo->value.acceptingRequests = ssi.isAcceptingRequests();
				} else {
					if (myQueueInfo->value.valid) {
						TraceEvent("RkStorageServerDidNotRespond", self->id).detail("StorageServer", ssi.id());
					}
					myQueueInfo->value.valid = false;
				}

				wait(delayJittered(SERVER_KNOBS->METRIC_UPDATE_RATE) &&
				     IFailureMonitor::failureMonitor().onStateEqual(ssi.getQueuingMetrics.getEndpoint(),
				                                                    FailureStatus(false)));
			}
		} catch (...) {
			// including cancellation
			self->storageQueueInfo.erase(ssi.id());
			self->storageServerInterfaces.erase(ssi.id());
			throw;
		}
	}

	ACTOR static Future<Void> trackTLogQueueInfo(Ratekeeper* self, TLogInterface tli) {
		self->tlogQueueInfo.insert(mapPair(tli.id(), TLogQueueInfo(tli.id())));
		state Map<UID, TLogQueueInfo>::iterator myQueueInfo = self->tlogQueueInfo.find(tli.id());
		TraceEvent("RkTracking", self->id).detail("TransactionLog", tli.id());
		try {
			loop {
				ErrorOr<TLogQueuingMetricsReply> reply = wait(tli.getQueuingMetrics.getReplyUnlessFailedFor(
				    TLogQueuingMetricsRequest(), 0, 0)); // SOMEDAY: or tryGetReply?
				if (reply.present()) {
					myQueueInfo->value.update(reply.get(), self->smoothTotalDurableBytes);
				} else {
					if (myQueueInfo->value.valid) {
						TraceEvent("RkTLogDidNotRespond", self->id).detail("TransactionLog", tli.id());
					}
					myQueueInfo->value.valid = false;
				}

				wait(delayJittered(SERVER_KNOBS->METRIC_UPDATE_RATE) &&
				     IFailureMonitor::failureMonitor().onStateEqual(tli.getQueuingMetrics.getEndpoint(),
				                                                    FailureStatus(false)));
			}
		} catch (...) {
			// including cancellation
			self->tlogQueueInfo.erase(myQueueInfo);
			throw;
		}
	}

	ACTOR static Future<Void> trackEachStorageServer(
	    ActorWeakSelfRef<Ratekeeper> self,
	    FutureStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges) {

		state std::unordered_map<UID, Future<Void>> storageServerTrackers;
		state Promise<Void> err;

		loop choose {
			when(state std::pair<UID, Optional<StorageServerInterface>> change = waitNext(serverChanges)) {
				wait(delay(0)); // prevent storageServerTracker from getting cancelled while on the call stack

				const UID& id = change.first;
				if (change.second.present()) {
					if (!change.second.get().isTss()) {

						auto& a = storageServerTrackers[change.first];
						a = Future<Void>();
						a = splitError(trackStorageServerQueueInfo(self, change.second.get()), err);

						self->storageServerInterfaces[id] = change.second.get();
					}
				} else {
					storageServerTrackers.erase(id);

					self->storageServerInterfaces.erase(id);
				}
			}
			when(wait(err.getFuture())) {}
		}
	}

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
			while (!self->configuration.blobGranulesEnabled) {
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
				grv = self->maxVersion;
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
					    .detail("MaxVer", self->maxVersion)
					    .detail("MinId", blobWorkers.size() > 0 ? blobWorkers[minIdx].id() : UID())
					    .detail("BMBlocked",
					            now() - self->unblockedAssignmentTime >= SERVER_KNOBS->BW_MAX_BLOCKED_INTERVAL);
				}
			}
			wait(blobWorkerDelay);
		}
	}

	ACTOR static Future<Void> run(RatekeeperInterface rkInterf, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
		state ActorOwningSelfRef<Ratekeeper> pSelf(
		    new Ratekeeper(rkInterf.id(), openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True)));
		state Ratekeeper& self = *pSelf;
		state Future<Void> timeout = Void();
		state std::vector<Future<Void>> tlogTrackers;
		state std::vector<TLogInterface> tlogInterfs;
		state Promise<Void> err;
		state Future<Void> collection = actorCollection(self.addActor.getFuture());

		TraceEvent("RatekeeperStarting", rkInterf.id());
		self.addActor.send(waitFailureServer(rkInterf.waitFailure.getFuture()));
		self.addActor.send(self.configurationMonitor());

		PromiseStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges;
		self.addActor.send(self.monitorServerListChange(serverChanges));
		self.addActor.send(RatekeeperImpl::trackEachStorageServer(pSelf, serverChanges.getFuture()));
		self.addActor.send(traceRole(Role::RATEKEEPER, rkInterf.id()));

		self.addActor.send(self.monitorThrottlingChanges());
		if (SERVER_KNOBS->BW_THROTTLING_ENABLED) {
			self.addActor.send(self.monitorBlobWorkers(dbInfo));
		}
		self.addActor.send(self.refreshStorageServerCommitCosts());

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

		tlogInterfs = dbInfo->get().logSystemConfig.allLocalLogs();
		tlogTrackers.reserve(tlogInterfs.size());
		for (int i = 0; i < tlogInterfs.size(); i++) {
			tlogTrackers.push_back(splitError(self.trackTLogQueueInfo(tlogInterfs[i]), err));
		}

		self.remoteDC = dbInfo->get().logSystemConfig.getRemoteDcId();

		state bool recovering = dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS;
		state Version recoveryVersion = std::numeric_limits<Version>::max();
		if (recovering) {
			self.version_recovery[recoveryVersion] = std::make_pair(now(), Optional<double>());
		}

		try {
			state bool lastLimited = false;
			loop choose {
				when(wait(timeout)) {
					double actualTps = self.smoothReleasedTransactions.smoothRate();
					actualTps =
					    std::max(std::max(1.0, actualTps),
					             self.smoothTotalDurableBytes.smoothRate() / CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT);

					if (self.actualTpsHistory.size() > SERVER_KNOBS->MAX_TPS_HISTORY_SAMPLES) {
						self.actualTpsHistory.pop_front();
					}
					self.actualTpsHistory.push_back(actualTps);

					if (self.configuration.blobGranulesEnabled && SERVER_KNOBS->BW_THROTTLING_ENABLED) {
						Version maxVersion = 0;
						int64_t totalReleased = 0;
						int64_t batchReleased = 0;
						for (auto& it : self.grvProxyInfo) {
							maxVersion = std::max(maxVersion, it.second.version);
							totalReleased += it.second.totalTransactions;
							batchReleased += it.second.batchTransactions;
						}
						self.version_transactions[maxVersion] =
						    Ratekeeper::VersionInfo(totalReleased, batchReleased, now());

						loop {
							auto secondEntry = self.version_transactions.begin();
							++secondEntry;
							if (secondEntry != self.version_transactions.end() &&
							    secondEntry->second.created < now() - (2 * SERVER_KNOBS->TARGET_BW_LAG) &&
							    (self.blobWorkerVersionHistory.empty() ||
							     secondEntry->first < self.blobWorkerVersionHistory.front().second)) {
								self.version_transactions.erase(self.version_transactions.begin());
							} else {
								break;
							}
						}
					}
					while (self.version_recovery.size() > CLIENT_KNOBS->MAX_GENERATIONS) {
						self.version_recovery.erase(self.version_recovery.begin());
					}

					self.updateRate(&self.normalLimits);
					self.updateRate(&self.batchLimits);

					lastLimited = self.smoothReleasedTransactions.smoothRate() >
					              SERVER_KNOBS->LAST_LIMITED_RATIO * self.batchLimits.tpsLimit;
					double tooOld = now() - 1.0;
					for (auto p = self.grvProxyInfo.begin(); p != self.grvProxyInfo.end();) {
						if (p->second.lastUpdateTime < tooOld)
							p = self.grvProxyInfo.erase(p);
						else
							++p;
					}
					timeout = delayJittered(SERVER_KNOBS->METRIC_UPDATE_RATE);
				}
				when(GetRateInfoRequest req = waitNext(rkInterf.getRateInfo.getFuture())) {
					GetRateInfoReply reply;

					auto& p = self.grvProxyInfo[req.requesterID];
					//TraceEvent("RKMPU", req.requesterID).detail("TRT", req.totalReleasedTransactions).detail("Last", p.totalTransactions).detail("Delta", req.totalReleasedTransactions - p.totalTransactions);
					if (p.totalTransactions > 0) {
						self.smoothReleasedTransactions.addDelta(req.totalReleasedTransactions - p.totalTransactions);

						for (auto const& [tag, count] : req.throttledTagCounts) {
							self.tagThrottler->addRequests(tag, count);
						}
					}
					if (p.batchTransactions > 0) {
						self.smoothBatchReleasedTransactions.addDelta(req.batchReleasedTransactions -
						                                              p.batchTransactions);
					}

					p.totalTransactions = req.totalReleasedTransactions;
					p.batchTransactions = req.batchReleasedTransactions;
					p.version = req.version;
					self.maxVersion = std::max(self.maxVersion, req.version);

					if (recoveryVersion == std::numeric_limits<Version>::max() &&
					    self.version_recovery.count(recoveryVersion)) {
						recoveryVersion = self.maxVersion;
						self.version_recovery[recoveryVersion] =
						    self.version_recovery[std::numeric_limits<Version>::max()];
						self.version_recovery.erase(std::numeric_limits<Version>::max());
					}

					p.lastUpdateTime = now();

					reply.transactionRate = self.normalLimits.tpsLimit / self.grvProxyInfo.size();
					reply.batchTransactionRate = self.batchLimits.tpsLimit / self.grvProxyInfo.size();
					reply.leaseDuration = SERVER_KNOBS->METRIC_UPDATE_RATE;

					if (p.lastThrottledTagChangeId != self.tagThrottler->getThrottledTagChangeId() ||
					    now() > p.lastTagPushTime + SERVER_KNOBS->TAG_THROTTLE_PUSH_INTERVAL) {
						p.lastThrottledTagChangeId = self.tagThrottler->getThrottledTagChangeId();
						p.lastTagPushTime = now();

						bool returningTagsToProxy{ false };
						if (SERVER_KNOBS->ENFORCE_TAG_THROTTLING_ON_PROXIES) {
							auto proxyThrottledTags = self.tagThrottler->getProxyRates(self.grvProxyInfo.size());
							if (!SERVER_KNOBS->GLOBAL_TAG_THROTTLING_REPORT_ONLY) {
								returningTagsToProxy = proxyThrottledTags.size() > 0;
								reply.proxyThrottledTags = std::move(proxyThrottledTags);
							}
						} else {
							auto clientThrottledTags = self.tagThrottler->getClientRates();
							if (!SERVER_KNOBS->GLOBAL_TAG_THROTTLING_REPORT_ONLY) {
								returningTagsToProxy = clientThrottledTags.size() > 0;
								reply.clientThrottledTags = std::move(clientThrottledTags);
							}
						}
						CODE_PROBE(returningTagsToProxy, "Returning tag throttles to a proxy");
					}

					reply.healthMetrics.update(self.healthMetrics, true, req.detailed);
					reply.healthMetrics.tpsLimit = self.normalLimits.tpsLimit;
					reply.healthMetrics.batchLimited = lastLimited;

					req.reply.send(reply);
				}
				when(HaltRatekeeperRequest req = waitNext(rkInterf.haltRatekeeper.getFuture())) {
					req.reply.send(Void());
					TraceEvent("RatekeeperHalted", rkInterf.id()).detail("ReqID", req.requesterID);
					break;
				}
				when(ReportCommitCostEstimationRequest req =
				         waitNext(rkInterf.reportCommitCostEstimation.getFuture())) {
					self.updateCommitCostEstimation(req.ssTrTagCommitCost);
					req.reply.send(Void());
				}
				when(wait(err.getFuture())) {}
				when(wait(dbInfo->onChange())) {
					if (!recovering && dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
						recovering = true;
						recoveryVersion = self.maxVersion;
						if (recoveryVersion == 0) {
							recoveryVersion = std::numeric_limits<Version>::max();
						}
						if (self.version_recovery.count(recoveryVersion)) {
							auto& it = self.version_recovery[recoveryVersion];
							double existingEnd = it.second.present() ? it.second.get() : now();
							double existingDuration = existingEnd - it.first;
							self.version_recovery[recoveryVersion] =
							    std::make_pair(now() - existingDuration, Optional<double>());
						} else {
							self.version_recovery[recoveryVersion] = std::make_pair(now(), Optional<double>());
						}
					}
					if (recovering && dbInfo->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS) {
						recovering = false;
						self.version_recovery[recoveryVersion].second = now();
					}

					if (tlogInterfs != dbInfo->get().logSystemConfig.allLocalLogs()) {
						tlogInterfs = dbInfo->get().logSystemConfig.allLocalLogs();
						tlogTrackers = std::vector<Future<Void>>();
						for (int i = 0; i < tlogInterfs.size(); i++)
							tlogTrackers.push_back(splitError(self.trackTLogQueueInfo(tlogInterfs[i]), err));
					}
					self.remoteDC = dbInfo->get().logSystemConfig.getRemoteDcId();
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

	ACTOR static Future<Void> refreshStorageServerCommitCosts(Ratekeeper* self) {
		state double lastBusiestCommitTagPick;
		state std::vector<Future<Void>> replies;
		loop {
			lastBusiestCommitTagPick = now();
			wait(delay(SERVER_KNOBS->TAG_MEASUREMENT_INTERVAL));

			replies.clear();

			double elapsed = now() - lastBusiestCommitTagPick;
			// for each SS, select the busiest commit tag from ssTrTagCommitCost
			for (auto& [ssId, ssQueueInfo] : self->storageQueueInfo) {
				// NOTE: In some cases, for unknown reason SS will not respond to the updateCommitCostRequest. Since the
				// information is not time-sensitive, we do not wait for the replies.
				replies.push_back(self->storageServerInterfaces[ssId].updateCommitCostRequest.getReply(
				    ssQueueInfo.refreshCommitCost(elapsed)));
			}
		}
	}
}; // class RatekeeperImpl

Future<Void> Ratekeeper::configurationMonitor() {
	return RatekeeperImpl::configurationMonitor(this);
}

Future<Void> Ratekeeper::monitorServerListChange(
    PromiseStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges) {
	return RatekeeperImpl::monitorServerListChange(this, serverChanges);
}

Future<Void> Ratekeeper::trackTLogQueueInfo(TLogInterface tli) {
	return RatekeeperImpl::trackTLogQueueInfo(this, tli);
}

Future<Void> Ratekeeper::monitorThrottlingChanges() {
	return tagThrottler->monitorThrottlingChanges();
}

Future<Void> Ratekeeper::monitorBlobWorkers(Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	return RatekeeperImpl::monitorBlobWorkers(this, dbInfo);
}

Future<Void> Ratekeeper::run(RatekeeperInterface rkInterf, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	return RatekeeperImpl::run(rkInterf, dbInfo);
}

Ratekeeper::Ratekeeper(UID id, Database db)
  : id(id), db(db), smoothReleasedTransactions(SERVER_KNOBS->SMOOTHING_AMOUNT),
    smoothBatchReleasedTransactions(SERVER_KNOBS->SMOOTHING_AMOUNT),
    smoothTotalDurableBytes(SERVER_KNOBS->SLOW_SMOOTHING_AMOUNT), actualTpsMetric("Ratekeeper.ActualTPS"_sr),
    lastWarning(0), lastSSListFetchedTimestamp(now()), normalLimits(TransactionPriority::DEFAULT,
                                                                    "",
                                                                    SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER,
                                                                    SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER,
                                                                    SERVER_KNOBS->TARGET_BYTES_PER_TLOG,
                                                                    SERVER_KNOBS->SPRING_BYTES_TLOG,
                                                                    SERVER_KNOBS->MAX_TL_SS_VERSION_DIFFERENCE,
                                                                    SERVER_KNOBS->TARGET_DURABILITY_LAG_VERSIONS,
                                                                    SERVER_KNOBS->TARGET_BW_LAG),
    batchLimits(TransactionPriority::BATCH,
                "Batch",
                SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER_BATCH,
                SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER_BATCH,
                SERVER_KNOBS->TARGET_BYTES_PER_TLOG_BATCH,
                SERVER_KNOBS->SPRING_BYTES_TLOG_BATCH,
                SERVER_KNOBS->MAX_TL_SS_VERSION_DIFFERENCE_BATCH,
                SERVER_KNOBS->TARGET_DURABILITY_LAG_VERSIONS_BATCH,
                SERVER_KNOBS->TARGET_BW_LAG_BATCH),
    maxVersion(0), blobWorkerTime(now()), unblockedAssignmentTime(now()), anyBlobRanges(false) {
	if (SERVER_KNOBS->GLOBAL_TAG_THROTTLING) {
		tagThrottler = std::make_unique<GlobalTagThrottler>(db, id, SERVER_KNOBS->MAX_MACHINES_FALLING_BEHIND);
	} else {
		tagThrottler = std::make_unique<TagThrottler>(db, id);
	}
}

void Ratekeeper::updateCommitCostEstimation(
    UIDTransactionTagMap<TransactionCommitCostEstimation> const& costEstimation) {
	for (auto it = storageQueueInfo.begin(); it != storageQueueInfo.end(); ++it) {
		auto tagCostIt = costEstimation.find(it->key);
		if (tagCostIt == costEstimation.end())
			continue;
		for (const auto& [tagName, cost] : tagCostIt->second) {
			it->value.addCommitCost(tagName, cost);
		}
	}
}

void Ratekeeper::updateRate(RatekeeperLimits* limits) {
	// double controlFactor = ;  // dt / eFoldingTime

	double actualTps = smoothReleasedTransactions.smoothRate();
	actualTpsMetric = (int64_t)actualTps;
	// SOMEDAY: Remove the max( 1.0, ... ) since the below calculations _should_ be able to recover back
	// up from this value
	actualTps =
	    std::max(std::max(1.0, actualTps), smoothTotalDurableBytes.smoothRate() / CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT);

	limits->tpsLimit = std::numeric_limits<double>::infinity();
	UID reasonID = UID();
	limitReason_t limitReason = limitReason_t::unlimited;

	int sscount = 0;

	int64_t worstFreeSpaceStorageServer = std::numeric_limits<int64_t>::max();
	int64_t worstStorageQueueStorageServer = 0;
	int64_t limitingStorageQueueStorageServer = 0;
	int64_t worstDurabilityLag = 0;

	std::multimap<double, StorageQueueInfo const*> storageTpsLimitReverseIndex;
	std::multimap<int64_t, StorageQueueInfo const*> storageDurabilityLagReverseIndex;

	std::map<UID, limitReason_t> ssReasons;

	bool printRateKeepLimitReasonDetails =
	    SERVER_KNOBS->RATEKEEPER_PRINT_LIMIT_REASON &&
	    (deterministicRandom()->random01() < SERVER_KNOBS->RATEKEEPER_LIMIT_REASON_SAMPLE_RATE);

	// Look at each storage server's write queue and local rate, compute and store the desired rate
	// ratio
	for (auto i = storageQueueInfo.begin(); i != storageQueueInfo.end(); ++i) {
		auto const& ss = i->value;
		if (!ss.valid || !ss.acceptingRequests || (remoteDC.present() && ss.locality.dcId() == remoteDC))
			continue;
		++sscount;

		limitReason_t ssLimitReason = limitReason_t::unlimited;

		int64_t minFreeSpace = std::max(SERVER_KNOBS->MIN_AVAILABLE_SPACE,
		                                (int64_t)(SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO * ss.getSmoothTotalSpace()));

		worstFreeSpaceStorageServer =
		    std::min(worstFreeSpaceStorageServer, (int64_t)ss.getSmoothFreeSpace() - minFreeSpace);

		int64_t springBytes = std::max<int64_t>(
		    1, std::min<int64_t>(limits->storageSpringBytes, (ss.getSmoothFreeSpace() - minFreeSpace) * 0.2));
		int64_t targetBytes =
		    std::max<int64_t>(1, std::min(limits->storageTargetBytes, (int64_t)ss.getSmoothFreeSpace() - minFreeSpace));
		if (targetBytes != limits->storageTargetBytes) {
			if (minFreeSpace == SERVER_KNOBS->MIN_AVAILABLE_SPACE) {
				ssLimitReason = limitReason_t::storage_server_min_free_space;
			} else {
				ssLimitReason = limitReason_t::storage_server_min_free_space_ratio;
			}
			if (printRateKeepLimitReasonDetails) {
				TraceEvent("RatekeeperLimitReasonDetails")
				    .detail("Reason", ssLimitReason)
				    .detail("SSID", ss.id)
				    .detail("SSSmoothTotalSpace", ss.getSmoothTotalSpace())
				    .detail("SSSmoothFreeSpace", ss.getSmoothFreeSpace())
				    .detail("TargetBytes", targetBytes)
				    .detail("LimitsStorageTargetBytes", limits->storageTargetBytes)
				    .detail("MinFreeSpace", minFreeSpace);
			}
		}

		int64_t storageQueue = ss.getStorageQueueBytes();
		worstStorageQueueStorageServer = std::max(worstStorageQueueStorageServer, storageQueue);

		int64_t storageDurabilityLag = ss.getDurabilityLag();
		worstDurabilityLag = std::max(worstDurabilityLag, storageDurabilityLag);

		storageDurabilityLagReverseIndex.insert(std::make_pair(-1 * storageDurabilityLag, &ss));

		auto& ssMetrics = healthMetrics.storageStats[ss.id];
		ssMetrics.storageQueue = storageQueue;
		ssMetrics.storageDurabilityLag = storageDurabilityLag;
		ssMetrics.cpuUsage = ss.lastReply.cpuUsage;
		ssMetrics.diskUsage = ss.lastReply.diskUsage;

		double targetRateRatio = std::min((storageQueue - targetBytes + springBytes) / (double)springBytes, 2.0);

		if (limits->priority == TransactionPriority::DEFAULT) {
			addActor.send(tagThrottler->tryUpdateAutoThrottling(ss));
		}

		double inputRate = ss.getSmoothInputBytesRate();
		// inputRate = std::max( inputRate, actualTps / SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE );

		/*if( deterministicRandom()->random01() < 0.1 ) {
		  std::string name = "RatekeeperUpdateRate" + limits.context;
		  TraceEvent(name, ss.id)
		  .detail("MinFreeSpace", minFreeSpace)
		  .detail("SpringBytes", springBytes)
		  .detail("TargetBytes", targetBytes)
		  .detail("SmoothTotalSpaceTotal", ss.getSmoothTotalSpace())
		  .detail("SmoothFreeSpaceTotal", ss.getSmoothFreeSpace())
		  .detail("LastReplyBytesInput", ss.lastReply.bytesInput)
		  .detail("SmoothDurableBytesTotal", ss.getSmoothDurableBytes())
		  .detail("TargetRateRatio", targetRateRatio)
		  .detail("SmoothInputBytesRate", ss.getSmoothInputBytesRate())
		  .detail("ActualTPS", actualTps)
		  .detail("InputRate", inputRate)
		  .detail("VerySmoothDurableBytesRate", ss.getVerySmoothDurableBytesRate())
		  .detail("B", b);
		  }*/

		// Don't let any storage server use up its target bytes faster than its MVCC window!
		double maxBytesPerSecond =
		    (targetBytes - springBytes) /
		    ((((double)SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) / SERVER_KNOBS->VERSIONS_PER_SECOND) + 2.0);
		double limitTps = std::min(actualTps * maxBytesPerSecond / std::max(1.0e-8, inputRate),
		                           maxBytesPerSecond * SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE);
		if (ssLimitReason == limitReason_t::unlimited)
			ssLimitReason = limitReason_t::storage_server_write_bandwidth_mvcc;

		if (targetRateRatio > 0 && inputRate > 0) {
			ASSERT(inputRate != 0);
			double smoothedRate =
			    std::max(ss.getVerySmoothDurableBytesRate(), actualTps / SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE);
			double x = smoothedRate / (inputRate * targetRateRatio);
			double lim = actualTps * x;
			if (lim < limitTps) {
				double oldLimitTps = limitTps;
				limitTps = lim;
				if (ssLimitReason == limitReason_t::unlimited ||
				    ssLimitReason == limitReason_t::storage_server_write_bandwidth_mvcc) {
					if (printRateKeepLimitReasonDetails) {
						TraceEvent("RatekeeperLimitReasonDetails")
						    .detail("Reason", limitReason_t::storage_server_write_queue_size)
						    .detail("FromReason", ssLimitReason)
						    .detail("SSID", ss.id)
						    .detail("SSSmoothTotalSpace", ss.getSmoothTotalSpace())
						    .detail("LimitsStorageTargetBytes", limits->storageTargetBytes)
						    .detail("LimitsStorageSpringBytes", limits->storageSpringBytes)
						    .detail("SSSmoothFreeSpace", ss.getSmoothFreeSpace())
						    .detail("MinFreeSpace", minFreeSpace)
						    .detail("SSLastReplyBytesInput", ss.lastReply.bytesInput)
						    .detail("SSSmoothDurableBytes", ss.getSmoothDurableBytes())
						    .detail("StorageQueue", storageQueue)
						    .detail("TargetBytes", targetBytes)
						    .detail("SpringBytes", springBytes)
						    .detail("SSVerySmoothDurableBytesRate", ss.getVerySmoothDurableBytesRate())
						    .detail("SmoothedRate", smoothedRate)
						    .detail("X", x)
						    .detail("ActualTps", actualTps)
						    .detail("Lim", lim)
						    .detail("LimitTps", oldLimitTps)
						    .detail("InputRate", inputRate)
						    .detail("TargetRateRatio", targetRateRatio);
					}
					ssLimitReason = limitReason_t::storage_server_write_queue_size;
				}
			}
		}

		storageTpsLimitReverseIndex.insert(std::make_pair(limitTps, &ss));

		if (limitTps < limits->tpsLimit && (ssLimitReason == limitReason_t::storage_server_min_free_space ||
		                                    ssLimitReason == limitReason_t::storage_server_min_free_space_ratio)) {
			reasonID = ss.id;
			limits->tpsLimit = limitTps;
			limitReason = ssLimitReason;
		}

		ssReasons[ss.id] = ssLimitReason;
	}

	std::set<Optional<Standalone<StringRef>>> ignoredMachines;
	for (auto ss = storageTpsLimitReverseIndex.begin();
	     ss != storageTpsLimitReverseIndex.end() && ss->first < limits->tpsLimit;
	     ++ss) {
		if (ignoredMachines.size() <
		    std::min(configuration.storageTeamSize - 1, SERVER_KNOBS->MAX_MACHINES_FALLING_BEHIND)) {
			ignoredMachines.insert(ss->second->locality.zoneId());
			continue;
		}
		if (ignoredMachines.count(ss->second->locality.zoneId()) > 0) {
			continue;
		}

		limitingStorageQueueStorageServer = ss->second->lastReply.bytesInput - ss->second->getSmoothDurableBytes();
		limits->tpsLimit = ss->first;
		reasonID = storageTpsLimitReverseIndex.begin()->second->id; // Although we aren't controlling based on the worst
		// SS, we still report it as the limiting process
		limitReason = ssReasons[reasonID];
		break;
	}

	// Calculate limited durability lag
	int64_t limitingDurabilityLag = 0;

	std::set<Optional<Standalone<StringRef>>> ignoredDurabilityLagMachines;
	for (auto ss = storageDurabilityLagReverseIndex.begin(); ss != storageDurabilityLagReverseIndex.end(); ++ss) {
		if (ignoredDurabilityLagMachines.size() <
		    std::min(configuration.storageTeamSize - 1, SERVER_KNOBS->MAX_MACHINES_FALLING_BEHIND)) {
			ignoredDurabilityLagMachines.insert(ss->second->locality.zoneId());
			continue;
		}
		if (ignoredDurabilityLagMachines.count(ss->second->locality.zoneId()) > 0) {
			continue;
		}

		limitingDurabilityLag = -1 * ss->first;
		if (limitingDurabilityLag > limits->durabilityLagTargetVersions &&
		    actualTpsHistory.size() > SERVER_KNOBS->NEEDED_TPS_HISTORY_SAMPLES) {
			if (limits->durabilityLagLimit == std::numeric_limits<double>::infinity()) {
				double maxTps = 0;
				for (int i = 0; i < actualTpsHistory.size(); i++) {
					maxTps = std::max(maxTps, actualTpsHistory[i]);
				}
				limits->durabilityLagLimit = SERVER_KNOBS->INITIAL_DURABILITY_LAG_MULTIPLIER * maxTps;
			}
			if (limitingDurabilityLag > limits->lastDurabilityLag) {
				limits->durabilityLagLimit = SERVER_KNOBS->DURABILITY_LAG_REDUCTION_RATE * limits->durabilityLagLimit;
			}
			if (limits->durabilityLagLimit < limits->tpsLimit) {
				if (printRateKeepLimitReasonDetails) {
					TraceEvent("RatekeeperLimitReasonDetails")
					    .detail("SSID", ss->second->id)
					    .detail("Reason", limitReason_t::storage_server_durability_lag)
					    .detail("LimitsDurabilityLagLimit", limits->durabilityLagLimit)
					    .detail("LimitsTpsLimit", limits->tpsLimit)
					    .detail("LimitingDurabilityLag", limitingDurabilityLag)
					    .detail("LimitsLastDurabilityLag", limits->lastDurabilityLag);
				}
				limits->tpsLimit = limits->durabilityLagLimit;
				limitReason = limitReason_t::storage_server_durability_lag;
			}
		} else if (limits->durabilityLagLimit != std::numeric_limits<double>::infinity() &&
		           limitingDurabilityLag >
		               limits->durabilityLagTargetVersions - SERVER_KNOBS->DURABILITY_LAG_UNLIMITED_THRESHOLD) {
			limits->durabilityLagLimit = SERVER_KNOBS->DURABILITY_LAG_INCREASE_RATE * limits->durabilityLagLimit;
		} else {
			limits->durabilityLagLimit = std::numeric_limits<double>::infinity();
		}
		limits->lastDurabilityLag = limitingDurabilityLag;
		break;
	}

	if (configuration.blobGranulesEnabled && SERVER_KNOBS->BW_THROTTLING_ENABLED && anyBlobRanges) {
		Version lastBWVer = 0;
		auto lastIter = version_transactions.end();
		if (!blobWorkerVersionHistory.empty()) {
			lastBWVer = blobWorkerVersionHistory.back().second;
			lastIter = version_transactions.lower_bound(lastBWVer);
			if (lastIter != version_transactions.end()) {
				blobWorkerTime = lastIter->second.created;
			} else {
				blobWorkerTime = std::max(blobWorkerTime,
				                          now() - (maxVersion - lastBWVer) / (double)SERVER_KNOBS->VERSIONS_PER_SECOND);
			}
		}
		double blobWorkerLag = (now() - blobWorkerTime) - getRecoveryDuration(lastBWVer);
		if (blobWorkerLag > limits->bwLagTarget / 2 && !blobWorkerVersionHistory.empty()) {
			double elapsed = blobWorkerVersionHistory.back().first - blobWorkerVersionHistory.front().first;
			Version firstBWVer = blobWorkerVersionHistory.front().second;
			ASSERT(lastBWVer >= firstBWVer);
			if (elapsed > SERVER_KNOBS->BW_ESTIMATION_INTERVAL / 2) {
				auto firstIter = version_transactions.upper_bound(firstBWVer);
				if (lastIter != version_transactions.end() && firstIter != version_transactions.begin()) {
					--firstIter;
					double targetRateRatio;
					if (blobWorkerLag > 3 * limits->bwLagTarget) {
						targetRateRatio = 0;
						ASSERT(!g_network->isSimulated() || limits->bwLagTarget != SERVER_KNOBS->TARGET_BW_LAG ||
						       now() < FLOW_KNOBS->SIM_SPEEDUP_AFTER_SECONDS + SERVER_KNOBS->BW_RK_SIM_QUIESCE_DELAY);
					} else if (blobWorkerLag > limits->bwLagTarget) {
						targetRateRatio = SERVER_KNOBS->BW_LAG_DECREASE_AMOUNT;
					} else {
						targetRateRatio = SERVER_KNOBS->BW_LAG_INCREASE_AMOUNT;
					}
					int64_t totalTransactions =
					    lastIter->second.totalTransactions - firstIter->second.totalTransactions;
					int64_t batchTransactions =
					    lastIter->second.batchTransactions - firstIter->second.batchTransactions;
					int64_t normalTransactions = totalTransactions - batchTransactions;
					double bwTPS;
					if (limits->bwLagTarget == SERVER_KNOBS->TARGET_BW_LAG) {
						bwTPS = targetRateRatio * (totalTransactions) / elapsed;
					} else {
						bwTPS = std::max(0.0, ((targetRateRatio * (totalTransactions)) - normalTransactions) / elapsed);
					}

					if (bwTPS < limits->tpsLimit) {
						if (printRateKeepLimitReasonDetails) {
							TraceEvent("RatekeeperLimitReasonDetails")
							    .detail("Reason", limitReason_t::blob_worker_lag)
							    .detail("BWLag", blobWorkerLag)
							    .detail("BWRate", bwTPS)
							    .detail("Ratio", targetRateRatio)
							    .detail("Released", totalTransactions)
							    .detail("Elapsed", elapsed)
							    .detail("LastVer", lastBWVer)
							    .detail("RecoveryDuration", getRecoveryDuration(lastBWVer));
						}
						limits->tpsLimit = bwTPS;
						limitReason = limitReason_t::blob_worker_lag;
					}
				} else if (blobWorkerLag > limits->bwLagTarget) {
					double maxTps = 0;
					for (int i = 0; i < actualTpsHistory.size(); i++) {
						maxTps = std::max(maxTps, actualTpsHistory[i]);
					}
					double bwProgress =
					    std::min(elapsed, (lastBWVer - firstBWVer) / (double)SERVER_KNOBS->VERSIONS_PER_SECOND);
					double bwTPS = maxTps * bwProgress / elapsed;

					if (blobWorkerLag > 3 * limits->bwLagTarget) {
						limits->tpsLimit = 0.0;
						if (printRateKeepLimitReasonDetails) {
							TraceEvent("RatekeeperLimitReasonDetails")
							    .detail("Reason", limitReason_t::blob_worker_missing)
							    .detail("LastValid", lastIter != version_transactions.end())
							    .detail("FirstValid", firstIter != version_transactions.begin())
							    .detail("FirstVersion",
							            version_transactions.size() ? version_transactions.begin()->first : -1)
							    .detail("FirstBWVer", firstBWVer)
							    .detail("LastBWVer", lastBWVer)
							    .detail("VerTransactions", version_transactions.size())
							    .detail("RecoveryDuration", getRecoveryDuration(lastBWVer));
						}
						limitReason = limitReason_t::blob_worker_missing;
						ASSERT(!g_network->isSimulated() || limits->bwLagTarget != SERVER_KNOBS->TARGET_BW_LAG ||
						       now() < FLOW_KNOBS->SIM_SPEEDUP_AFTER_SECONDS + SERVER_KNOBS->BW_RK_SIM_QUIESCE_DELAY);
					} else if (bwTPS < limits->tpsLimit) {
						if (printRateKeepLimitReasonDetails) {
							TraceEvent("RatekeeperLimitReasonDetails")
							    .detail("Reason", limitReason_t::blob_worker_lag)
							    .detail("BWLag", blobWorkerLag)
							    .detail("BWRate", bwTPS)
							    .detail("MaxTPS", maxTps)
							    .detail("Progress", bwProgress)
							    .detail("Elapsed", elapsed);
						}
						limits->tpsLimit = bwTPS;
						limitReason = limitReason_t::blob_worker_lag;
					}
				}
			} else if (blobWorkerLag > 3 * limits->bwLagTarget) {
				limits->tpsLimit = 0.0;
				if (printRateKeepLimitReasonDetails) {
					TraceEvent("RatekeeperLimitReasonDetails")
					    .detail("Reason", limitReason_t::blob_worker_missing)
					    .detail("Elapsed", elapsed)
					    .detail("LastVer", lastBWVer)
					    .detail("FirstVer", firstBWVer)
					    .detail("BWLag", blobWorkerLag)
					    .detail("RecoveryDuration", getRecoveryDuration(lastBWVer));
					;
				}
				limitReason = limitReason_t::blob_worker_missing;
				ASSERT(!g_network->isSimulated() || limits->bwLagTarget != SERVER_KNOBS->TARGET_BW_LAG ||
				       now() < FLOW_KNOBS->SIM_SPEEDUP_AFTER_SECONDS + SERVER_KNOBS->BW_RK_SIM_QUIESCE_DELAY);
			}
		} else if (blobWorkerLag > 3 * limits->bwLagTarget) {
			limits->tpsLimit = 0.0;
			if (printRateKeepLimitReasonDetails) {
				TraceEvent("RatekeeperLimitReasonDetails")
				    .detail("Reason", limitReason_t::blob_worker_missing)
				    .detail("BWLag", blobWorkerLag)
				    .detail("RecoveryDuration", getRecoveryDuration(lastBWVer))
				    .detail("HistorySize", blobWorkerVersionHistory.size());
			}
			limitReason = limitReason_t::blob_worker_missing;
			ASSERT(!g_network->isSimulated() || limits->bwLagTarget != SERVER_KNOBS->TARGET_BW_LAG ||
			       now() < FLOW_KNOBS->SIM_SPEEDUP_AFTER_SECONDS + SERVER_KNOBS->BW_RK_SIM_QUIESCE_DELAY);
		}
	} else {
		blobWorkerTime = now();
		unblockedAssignmentTime = now();
	}

	healthMetrics.worstStorageQueue = worstStorageQueueStorageServer;
	healthMetrics.limitingStorageQueue = limitingStorageQueueStorageServer;
	healthMetrics.worstStorageDurabilityLag = worstDurabilityLag;
	healthMetrics.limitingStorageDurabilityLag = limitingDurabilityLag;

	double writeToReadLatencyLimit = 0;
	Version worstVersionLag = 0;
	Version limitingVersionLag = 0;

	{
		Version minSSVer = std::numeric_limits<Version>::max();
		Version minLimitingSSVer = std::numeric_limits<Version>::max();
		for (const auto& it : storageQueueInfo) {
			auto& ss = it.value;
			if (!ss.valid || (remoteDC.present() && ss.locality.dcId() == remoteDC))
				continue;

			minSSVer = std::min(minSSVer, ss.lastReply.version);

			// Machines that ratekeeper isn't controlling can fall arbitrarily far behind
			if (ignoredMachines.count(it.value.locality.zoneId()) == 0) {
				minLimitingSSVer = std::min(minLimitingSSVer, ss.lastReply.version);
			}
		}

		Version maxTLVer = std::numeric_limits<Version>::min();
		for (const auto& it : tlogQueueInfo) {
			auto& tl = it.value;
			if (!tl.valid)
				continue;
			maxTLVer = std::max(maxTLVer, tl.getLastCommittedVersion());
		}

		if (minSSVer != std::numeric_limits<Version>::max() && maxTLVer != std::numeric_limits<Version>::min()) {
			// writeToReadLatencyLimit: 0 = infinite speed; 1 = TL durable speed ; 2 = half TL durable
			// speed
			writeToReadLatencyLimit =
			    ((maxTLVer - minLimitingSSVer) - limits->maxVersionDifference / 2) / (limits->maxVersionDifference / 4);
			worstVersionLag = std::max((Version)0, maxTLVer - minSSVer);
			limitingVersionLag = std::max((Version)0, maxTLVer - minLimitingSSVer);
		}
	}

	int64_t worstFreeSpaceTLog = std::numeric_limits<int64_t>::max();
	int64_t worstStorageQueueTLog = 0;
	int tlcount = 0;
	for (auto& it : tlogQueueInfo) {
		auto const& tl = it.value;
		if (!tl.valid)
			continue;
		++tlcount;

		limitReason_t tlogLimitReason = limitReason_t::log_server_write_queue;

		int64_t minFreeSpace = std::max(SERVER_KNOBS->MIN_AVAILABLE_SPACE,
		                                (int64_t)(SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO * tl.getSmoothTotalSpace()));

		worstFreeSpaceTLog = std::min(worstFreeSpaceTLog, (int64_t)tl.getSmoothFreeSpace() - minFreeSpace);

		int64_t springBytes = std::max<int64_t>(
		    1, std::min<int64_t>(limits->logSpringBytes, (tl.getSmoothFreeSpace() - minFreeSpace) * 0.2));
		int64_t targetBytes =
		    std::max<int64_t>(1, std::min(limits->logTargetBytes, (int64_t)tl.getSmoothFreeSpace() - minFreeSpace));
		if (targetBytes != limits->logTargetBytes) {
			if (minFreeSpace == SERVER_KNOBS->MIN_AVAILABLE_SPACE) {
				tlogLimitReason = limitReason_t::log_server_min_free_space;
			} else {
				tlogLimitReason = limitReason_t::log_server_min_free_space_ratio;
			}
			if (printRateKeepLimitReasonDetails) {
				TraceEvent("RatekeeperLimitReasonDetails")
				    .detail("TLogID", tl.id)
				    .detail("Reason", tlogLimitReason)
				    .detail("TLSmoothFreeSpace", tl.getSmoothFreeSpace())
				    .detail("TLSmoothTotalSpace", tl.getSmoothTotalSpace())
				    .detail("LimitsLogTargetBytes", limits->logTargetBytes)
				    .detail("TargetBytes", targetBytes)
				    .detail("MinFreeSpace", minFreeSpace);
			}
		}

		int64_t queue = tl.lastReply.bytesInput - tl.getSmoothDurableBytes();
		healthMetrics.tLogQueue[tl.id] = queue;
		int64_t b = queue - targetBytes;
		worstStorageQueueTLog = std::max(worstStorageQueueTLog, queue);

		if (tl.lastReply.bytesInput - tl.lastReply.bytesDurable > tl.lastReply.storageBytes.free - minFreeSpace / 2) {
			if (now() - lastWarning > 5.0) {
				lastWarning = now();
				TraceEvent(SevWarnAlways, "RkTlogMinFreeSpaceZero", id).detail("ReasonId", tl.id);
			}
			reasonID = tl.id;
			limitReason = limitReason_t::log_server_min_free_space;
			limits->tpsLimit = 0.0;
		}

		double targetRateRatio = std::min((b + springBytes) / (double)springBytes, 2.0);

		if (writeToReadLatencyLimit > targetRateRatio) {
			if (printRateKeepLimitReasonDetails) {
				TraceEvent("RatekeeperLimitReasonDetails")
				    .detail("TLogID", tl.id)
				    .detail("Reason", limitReason_t::storage_server_readable_behind)
				    .detail("TLSmoothFreeSpace", tl.getSmoothFreeSpace())
				    .detail("TLSmoothTotalSpace", tl.getSmoothTotalSpace())
				    .detail("LimitsLogSpringBytes", limits->logSpringBytes)
				    .detail("LimitsLogTargetBytes", limits->logTargetBytes)
				    .detail("SpringBytes", springBytes)
				    .detail("TargetBytes", targetBytes)
				    .detail("TLLastReplyBytesInput", tl.lastReply.bytesInput)
				    .detail("TLSmoothDurableBytes", tl.getSmoothDurableBytes())
				    .detail("Queue", queue)
				    .detail("B", b)
				    .detail("TargetRateRatio", targetRateRatio)
				    .detail("WriteToReadLatencyLimit", writeToReadLatencyLimit)
				    .detail("MinFreeSpace", minFreeSpace)
				    .detail("LimitsMaxVersionDifference", limits->maxVersionDifference);
			}
			targetRateRatio = writeToReadLatencyLimit;
			tlogLimitReason = limitReason_t::storage_server_readable_behind;
		}

		double inputRate = tl.getSmoothInputBytesRate();

		if (targetRateRatio > 0) {
			double smoothedRate =
			    std::max(tl.getVerySmoothDurableBytesRate(), actualTps / SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE);
			double x = smoothedRate / (inputRate * targetRateRatio);
			if (targetRateRatio < .75) //< FIXME: KNOB for 2.0
				x = std::max(x, 0.95);
			double lim = actualTps * x;
			if (lim < limits->tpsLimit) {
				limits->tpsLimit = lim;
				reasonID = tl.id;
				limitReason = tlogLimitReason;
			}
		}
		if (inputRate > 0) {
			// Don't let any tlogs use up its target bytes faster than its MVCC window!
			double x =
			    ((targetBytes - springBytes) /
			     ((((double)SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) / SERVER_KNOBS->VERSIONS_PER_SECOND) +
			      2.0)) /
			    inputRate;
			double lim = actualTps * x;
			if (lim < limits->tpsLimit) {
				if (printRateKeepLimitReasonDetails) {
					TraceEvent("RatekeeperLimitReasonDetails")
					    .detail("Reason", limitReason_t::log_server_mvcc_write_bandwidth)
					    .detail("TLogID", tl.id)
					    .detail("MinFreeSpace", minFreeSpace)
					    .detail("TLSmoothFreeSpace", tl.getSmoothFreeSpace())
					    .detail("TLSmoothTotalSpace", tl.getSmoothTotalSpace())
					    .detail("LimitsLogSpringBytes", limits->logSpringBytes)
					    .detail("LimitsLogTargetBytes", limits->logTargetBytes)
					    .detail("SpringBytes", springBytes)
					    .detail("TargetBytes", targetBytes)
					    .detail("InputRate", inputRate)
					    .detail("X", x)
					    .detail("ActualTps", actualTps)
					    .detail("Lim", lim)
					    .detail("LimitsTpsLimit", limits->tpsLimit);
				}
				limits->tpsLimit = lim;
				reasonID = tl.id;
				limitReason = limitReason_t::log_server_mvcc_write_bandwidth;
			}
		}
	}

	healthMetrics.worstTLogQueue = worstStorageQueueTLog;

	limits->tpsLimit = std::max(limits->tpsLimit, 0.0);

	if (g_network->isSimulated() && g_simulator->speedUpSimulation) {
		limits->tpsLimit = std::max(limits->tpsLimit, 100.0);
	}

	int64_t totalDiskUsageBytes = 0;
	for (auto& t : tlogQueueInfo) {
		if (t.value.valid) {
			totalDiskUsageBytes += t.value.lastReply.storageBytes.used;
		}
	}
	for (auto& s : storageQueueInfo) {
		if (s.value.valid) {
			totalDiskUsageBytes += s.value.lastReply.storageBytes.used;
		}
	}

	if (now() - lastSSListFetchedTimestamp > SERVER_KNOBS->STORAGE_SERVER_LIST_FETCH_TIMEOUT) {
		limits->tpsLimit = 0.0;
		limitReason = limitReason_t::storage_server_list_fetch_failed;
		reasonID = UID();
		TraceEvent(SevWarnAlways, "RkSSListFetchTimeout", id).suppressFor(1.0);
	} else if (limits->tpsLimit == std::numeric_limits<double>::infinity()) {
		limits->tpsLimit = SERVER_KNOBS->RATEKEEPER_DEFAULT_LIMIT;
	}

	limits->tpsLimitMetric = std::min(limits->tpsLimit, 1e6);
	limits->reasonMetric = limitReason;

	if (limits->priority == TransactionPriority::DEFAULT) {
		limits->tpsLimit = std::max(limits->tpsLimit, SERVER_KNOBS->RATEKEEPER_MIN_RATE);
		limits->tpsLimit = std::min(limits->tpsLimit, SERVER_KNOBS->RATEKEEPER_MAX_RATE);
	} else if (limits->priority == TransactionPriority::BATCH) {
		limits->tpsLimit = std::max(limits->tpsLimit, SERVER_KNOBS->RATEKEEPER_BATCH_MIN_RATE);
		limits->tpsLimit = std::min(limits->tpsLimit, SERVER_KNOBS->RATEKEEPER_BATCH_MAX_RATE);
	}

	if (deterministicRandom()->random01() < 0.1) {
		const std::string& name = limits->rkUpdateEventCacheHolder.getPtr()->trackingKey;
		TraceEvent(name.c_str(), id)
		    .detail("TPSLimit", limits->tpsLimit)
		    .detail("Reason", limitReason)
		    .detail("ReasonServerID", reasonID == UID() ? std::string() : Traceable<UID>::toString(reasonID))
		    .detail("ReleasedTPS", smoothReleasedTransactions.smoothRate())
		    .detail("ReleasedBatchTPS", smoothBatchReleasedTransactions.smoothRate())
		    .detail("TPSBasis", actualTps)
		    .detail("StorageServers", sscount)
		    .detail("GrvProxies", grvProxyInfo.size())
		    .detail("TLogs", tlcount)
		    .detail("WorstFreeSpaceStorageServer", worstFreeSpaceStorageServer)
		    .detail("WorstFreeSpaceTLog", worstFreeSpaceTLog)
		    .detail("WorstStorageServerQueue", worstStorageQueueStorageServer)
		    .detail("LimitingStorageServerQueue", limitingStorageQueueStorageServer)
		    .detail("WorstTLogQueue", worstStorageQueueTLog)
		    .detail("TotalDiskUsageBytes", totalDiskUsageBytes)
		    .detail("WorstStorageServerVersionLag", worstVersionLag)
		    .detail("LimitingStorageServerVersionLag", limitingVersionLag)
		    .detail("WorstStorageServerDurabilityLag", worstDurabilityLag)
		    .detail("LimitingStorageServerDurabilityLag", limitingDurabilityLag)
		    .detail("TagsAutoThrottled", tagThrottler->autoThrottleCount())
		    .detail("TagsAutoThrottledBusyRead", tagThrottler->busyReadTagCount())
		    .detail("TagsAutoThrottledBusyWrite", tagThrottler->busyWriteTagCount())
		    .detail("TagsManuallyThrottled", tagThrottler->manualThrottleCount())
		    .detail("AutoThrottlingEnabled", tagThrottler->isAutoThrottlingEnabled())
		    .trackLatest(name);
	}
}

Future<Void> Ratekeeper::refreshStorageServerCommitCosts() {
	return RatekeeperImpl::refreshStorageServerCommitCosts(this);
}

ACTOR Future<Void> ratekeeper(RatekeeperInterface rkInterf, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	wait(Ratekeeper::run(rkInterf, dbInfo));
	return Void();
}

StorageQueueInfo::StorageQueueInfo(const UID& ratekeeperID_, const UID& id_, const LocalityData& locality_)
  : valid(false), ratekeeperID(ratekeeperID_), id(id_), locality(locality_), acceptingRequests(false),
    smoothDurableBytes(SERVER_KNOBS->SMOOTHING_AMOUNT), smoothInputBytes(SERVER_KNOBS->SMOOTHING_AMOUNT),
    verySmoothDurableBytes(SERVER_KNOBS->SLOW_SMOOTHING_AMOUNT), smoothDurableVersion(SERVER_KNOBS->SMOOTHING_AMOUNT),
    smoothLatestVersion(SERVER_KNOBS->SMOOTHING_AMOUNT), smoothFreeSpace(SERVER_KNOBS->SMOOTHING_AMOUNT),
    smoothTotalSpace(SERVER_KNOBS->SMOOTHING_AMOUNT), limitReason(limitReason_t::unlimited) {
	// FIXME: this is a tacky workaround for a potential uninitialized use in trackStorageServerQueueInfo
	lastReply.instanceID = -1;
}

StorageQueueInfo::StorageQueueInfo(const UID& id_, const LocalityData& locality_)
  : StorageQueueInfo(UID(), id_, locality_) {}

void StorageQueueInfo::addCommitCost(TransactionTagRef tagName, TransactionCommitCostEstimation const& cost) {
	tagCostEst[tagName] += cost;
	totalWriteCosts += cost.getCostSum();
	totalWriteOps += cost.getOpsSum();
}

void StorageQueueInfo::update(StorageQueuingMetricsReply const& reply, Smoother& smoothTotalDurableBytes) {
	valid = true;
	auto prevReply = std::move(lastReply);
	lastReply = reply;
	if (prevReply.instanceID != reply.instanceID) {
		smoothDurableBytes.reset(reply.bytesDurable);
		verySmoothDurableBytes.reset(reply.bytesDurable);
		smoothInputBytes.reset(reply.bytesInput);
		smoothFreeSpace.reset(reply.storageBytes.available);
		smoothTotalSpace.reset(reply.storageBytes.total);
		smoothDurableVersion.reset(reply.durableVersion);
		smoothLatestVersion.reset(reply.version);
	} else {
		smoothTotalDurableBytes.addDelta(reply.bytesDurable - prevReply.bytesDurable);
		smoothDurableBytes.setTotal(reply.bytesDurable);
		verySmoothDurableBytes.setTotal(reply.bytesDurable);
		smoothInputBytes.setTotal(reply.bytesInput);
		smoothFreeSpace.setTotal(reply.storageBytes.available);
		smoothTotalSpace.setTotal(reply.storageBytes.total);
		smoothDurableVersion.setTotal(reply.durableVersion);
		smoothLatestVersion.setTotal(reply.version);
	}

	busiestReadTags = reply.busiestTags;
}

UpdateCommitCostRequest StorageQueueInfo::refreshCommitCost(double elapsed) {
	busiestWriteTags.clear();
	TransactionTag busiestTag;
	TransactionCommitCostEstimation maxCost;
	double maxRate = 0, maxBusyness = 0;
	for (const auto& [tag, cost] : tagCostEst) {
		double rate = cost.getCostSum() / elapsed;
		if (rate > maxRate) {
			busiestTag = tag;
			maxRate = rate;
			maxCost = cost;
		}
	}
	if (maxRate > SERVER_KNOBS->MIN_TAG_WRITE_PAGES_RATE * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE) {
		// TraceEvent("RefreshSSCommitCost").detail("TotalWriteCost", totalWriteCost).detail("TotalWriteOps",totalWriteOps);
		ASSERT_GT(totalWriteCosts, 0);
		maxBusyness = double(maxCost.getCostSum()) / totalWriteCosts;
		busiestWriteTags.emplace_back(busiestTag, maxRate, maxBusyness);
	}

	UpdateCommitCostRequest updateCommitCostRequest{ ratekeeperID,
		                                             now(),
		                                             elapsed,
		                                             busiestTag,
		                                             maxCost.getOpsSum(),
		                                             maxCost.getCostSum(),
		                                             totalWriteCosts,
		                                             !busiestWriteTags.empty(),
		                                             ReplyPromise<Void>() };

	// reset statistics
	tagCostEst.clear();
	totalWriteOps = 0;
	totalWriteCosts = 0;

	return updateCommitCostRequest;
}

Optional<double> StorageQueueInfo::getTagThrottlingRatio(int64_t storageTargetBytes, int64_t storageSpringBytes) const {
	auto const storageQueue = getStorageQueueBytes();
	// TODO: Remove duplicate calculation from Ratekeeper::updateRate
	double inverseResult = std::min(
	    2.0, (storageQueue - storageTargetBytes + storageSpringBytes) / static_cast<double>(storageSpringBytes));
	if (inverseResult > 0) {
		return 1.0 / inverseResult;
	} else {
		return {};
	}
}

TLogQueueInfo::TLogQueueInfo(UID id)
  : valid(false), id(id), smoothDurableBytes(SERVER_KNOBS->SMOOTHING_AMOUNT),
    smoothInputBytes(SERVER_KNOBS->SMOOTHING_AMOUNT), verySmoothDurableBytes(SERVER_KNOBS->SLOW_SMOOTHING_AMOUNT),
    smoothFreeSpace(SERVER_KNOBS->SMOOTHING_AMOUNT), smoothTotalSpace(SERVER_KNOBS->SMOOTHING_AMOUNT) {
	// FIXME: this is a tacky workaround for a potential uninitialized use in trackTLogQueueInfo (copied
	// from storageQueueInfO)
	lastReply.instanceID = -1;
}

void TLogQueueInfo::update(TLogQueuingMetricsReply const& reply, Smoother& smoothTotalDurableBytes) {
	valid = true;
	auto prevReply = std::move(lastReply);
	lastReply = reply;
	if (prevReply.instanceID != reply.instanceID) {
		smoothDurableBytes.reset(reply.bytesDurable);
		verySmoothDurableBytes.reset(reply.bytesDurable);
		smoothInputBytes.reset(reply.bytesInput);
		smoothFreeSpace.reset(reply.storageBytes.available);
		smoothTotalSpace.reset(reply.storageBytes.total);
	} else {
		smoothTotalDurableBytes.addDelta(reply.bytesDurable - prevReply.bytesDurable);
		smoothDurableBytes.setTotal(reply.bytesDurable);
		verySmoothDurableBytes.setTotal(reply.bytesDurable);
		smoothInputBytes.setTotal(reply.bytesInput);
		smoothFreeSpace.setTotal(reply.storageBytes.available);
		smoothTotalSpace.setTotal(reply.storageBytes.total);
	}
}

RatekeeperLimits::RatekeeperLimits(TransactionPriority priority,
                                   std::string context,
                                   int64_t storageTargetBytes,
                                   int64_t storageSpringBytes,
                                   int64_t logTargetBytes,
                                   int64_t logSpringBytes,
                                   double maxVersionDifference,
                                   int64_t durabilityLagTargetVersions,
                                   double bwLagTarget)
  : tpsLimit(std::numeric_limits<double>::infinity()), tpsLimitMetric(StringRef("Ratekeeper.TPSLimit" + context)),
    reasonMetric(StringRef("Ratekeeper.Reason" + context)), storageTargetBytes(storageTargetBytes),
    storageSpringBytes(storageSpringBytes), logTargetBytes(logTargetBytes), logSpringBytes(logSpringBytes),
    maxVersionDifference(maxVersionDifference),
    durabilityLagTargetVersions(durabilityLagTargetVersions +
                                SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS), // The read transaction life versions
                                                                                   // are expected to not
    // be durable on the storage servers
    lastDurabilityLag(0), durabilityLagLimit(std::numeric_limits<double>::infinity()), bwLagTarget(bwLagTarget),
    priority(priority), context(context),
    rkUpdateEventCacheHolder(makeReference<EventCacheHolder>("RkUpdate" + context)) {}
