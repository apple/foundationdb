/*
 * RatekeeperData.actor.cpp
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

#include "fdbserver/RatekeeperData.h"
#include "flow/actorcompiler.h" // must be last include

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

class RatekeeperDataImpl {
public:
	ACTOR static Future<Void> configurationMonitor(RatekeeperData* self) {
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
	    RatekeeperData* self,
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
				    wait(getServerListAndProcessClasses(&tr));
				self->lastSSListFetchedTimestamp = now();

				std::map<UID, StorageServerInterface> newServers;
				for (const auto& [ssi, _] : results) {
					const UID serverId = ssi.id();
					newServers[serverId] = ssi;

					if (oldServers.count(serverId)) {
						if (ssi.getValue.getEndpoint() != oldServers[serverId].getValue.getEndpoint()) {
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
				TraceEvent("RatekeeperGetSSListError", self->id).error(e).suppressFor(1.0);
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> trackStorageServerQueueInfo(RatekeeperData* self, StorageServerInterface ssi) {
		self->storageQueueInfo.insert(mapPair(ssi.id(), StorageQueueInfo(ssi.id(), ssi.locality)));
		state Map<UID, StorageQueueInfo>::iterator myQueueInfo = self->storageQueueInfo.find(ssi.id());
		TraceEvent("RkTracking", self->id)
		    .detail("StorageServer", ssi.id())
		    .detail("Locality", ssi.locality.toString());
		try {
			loop {
				ErrorOr<StorageQueuingMetricsReply> reply = wait(ssi.getQueuingMetrics.getReplyUnlessFailedFor(
				    StorageQueuingMetricsRequest(), 0, 0)); // SOMEDAY: or tryGetReply?
				if (reply.present()) {
					myQueueInfo->value.valid = true;
					myQueueInfo->value.prevReply = myQueueInfo->value.lastReply;
					myQueueInfo->value.lastReply = reply.get();
					if (myQueueInfo->value.prevReply.instanceID != reply.get().instanceID) {
						myQueueInfo->value.smoothDurableBytes.reset(reply.get().bytesDurable);
						myQueueInfo->value.verySmoothDurableBytes.reset(reply.get().bytesDurable);
						myQueueInfo->value.smoothInputBytes.reset(reply.get().bytesInput);
						myQueueInfo->value.smoothFreeSpace.reset(reply.get().storageBytes.available);
						myQueueInfo->value.smoothTotalSpace.reset(reply.get().storageBytes.total);
						myQueueInfo->value.smoothDurableVersion.reset(reply.get().durableVersion);
						myQueueInfo->value.smoothLatestVersion.reset(reply.get().version);
					} else {
						self->smoothTotalDurableBytes.addDelta(reply.get().bytesDurable -
						                                       myQueueInfo->value.prevReply.bytesDurable);
						myQueueInfo->value.smoothDurableBytes.setTotal(reply.get().bytesDurable);
						myQueueInfo->value.verySmoothDurableBytes.setTotal(reply.get().bytesDurable);
						myQueueInfo->value.smoothInputBytes.setTotal(reply.get().bytesInput);
						myQueueInfo->value.smoothFreeSpace.setTotal(reply.get().storageBytes.available);
						myQueueInfo->value.smoothTotalSpace.setTotal(reply.get().storageBytes.total);
						myQueueInfo->value.smoothDurableVersion.setTotal(reply.get().durableVersion);
						myQueueInfo->value.smoothLatestVersion.setTotal(reply.get().version);
					}

					myQueueInfo->value.busiestReadTag = reply.get().busiestTag;
					myQueueInfo->value.busiestReadTagFractionalBusyness = reply.get().busiestTagFractionalBusyness;
					myQueueInfo->value.busiestReadTagRate = reply.get().busiestTagRate;
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
			self->storageQueueInfo.erase(myQueueInfo);
			throw;
		}
	}

	ACTOR static Future<Void> trackTLogQueueInfo(RatekeeperData* self, TLogInterface tli) {
		self->tlogQueueInfo.insert(mapPair(tli.id(), TLogQueueInfo(tli.id())));
		state Map<UID, TLogQueueInfo>::iterator myQueueInfo = self->tlogQueueInfo.find(tli.id());
		TraceEvent("RkTracking", self->id).detail("TransactionLog", tli.id());
		try {
			loop {
				ErrorOr<TLogQueuingMetricsReply> reply = wait(tli.getQueuingMetrics.getReplyUnlessFailedFor(
				    TLogQueuingMetricsRequest(), 0, 0)); // SOMEDAY: or tryGetReply?
				if (reply.present()) {
					myQueueInfo->value.valid = true;
					myQueueInfo->value.prevReply = myQueueInfo->value.lastReply;
					myQueueInfo->value.lastReply = reply.get();
					if (myQueueInfo->value.prevReply.instanceID != reply.get().instanceID) {
						myQueueInfo->value.smoothDurableBytes.reset(reply.get().bytesDurable);
						myQueueInfo->value.verySmoothDurableBytes.reset(reply.get().bytesDurable);
						myQueueInfo->value.smoothInputBytes.reset(reply.get().bytesInput);
						myQueueInfo->value.smoothFreeSpace.reset(reply.get().storageBytes.available);
						myQueueInfo->value.smoothTotalSpace.reset(reply.get().storageBytes.total);
					} else {
						self->smoothTotalDurableBytes.addDelta(reply.get().bytesDurable -
						                                       myQueueInfo->value.prevReply.bytesDurable);
						myQueueInfo->value.smoothDurableBytes.setTotal(reply.get().bytesDurable);
						myQueueInfo->value.verySmoothDurableBytes.setTotal(reply.get().bytesDurable);
						myQueueInfo->value.smoothInputBytes.setTotal(reply.get().bytesInput);
						myQueueInfo->value.smoothFreeSpace.setTotal(reply.get().storageBytes.available);
						myQueueInfo->value.smoothTotalSpace.setTotal(reply.get().storageBytes.total);
					}
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
	    RatekeeperData* self,
	    FutureStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges) {
		state Map<UID, Future<Void>> actors;
		state Promise<Void> err;
		loop choose {
			when(state std::pair<UID, Optional<StorageServerInterface>> change = waitNext(serverChanges)) {
				wait(delay(0)); // prevent storageServerTracker from getting cancelled while on the call stack
				if (change.second.present()) {
					if (!change.second.get().isTss()) {
						auto& a = actors[change.first];
						a = Future<Void>();
						a = splitError(trackStorageServerQueueInfo(self, change.second.get()), err);
					}
				} else
					actors.erase(change.first);
			}
			when(wait(err.getFuture())) {}
		}
	}

}; // class RatekeeperDataImpl

Future<Void> RatekeeperData::configurationMonitor() {
	return RatekeeperDataImpl::configurationMonitor(this);
}

Future<Void> RatekeeperData::monitorServerListChange(
    PromiseStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges) {
	return RatekeeperDataImpl::monitorServerListChange(this, serverChanges);
}

Future<Void> RatekeeperData::trackEachStorageServer(
    FutureStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges) {
	return RatekeeperDataImpl::trackEachStorageServer(this, serverChanges);
}

Future<Void> RatekeeperData::trackStorageServerQueueInfo(StorageServerInterface ssi) {
	return RatekeeperDataImpl::trackStorageServerQueueInfo(this, ssi);
}

Future<Void> RatekeeperData::trackTLogQueueInfo(TLogInterface tli) {
	return RatekeeperDataImpl::trackTLogQueueInfo(this, tli);
}

RatekeeperData::RatekeeperData(UID id, Database db)
  : id(id), db(db), smoothReleasedTransactions(SERVER_KNOBS->SMOOTHING_AMOUNT),
    smoothBatchReleasedTransactions(SERVER_KNOBS->SMOOTHING_AMOUNT),
    smoothTotalDurableBytes(SERVER_KNOBS->SLOW_SMOOTHING_AMOUNT),
    actualTpsMetric(LiteralStringRef("Ratekeeper.ActualTPS")), lastWarning(0), lastSSListFetchedTimestamp(now()),
    lastBusiestCommitTagPick(0), throttledTagChangeId(0), normalLimits(TransactionPriority::DEFAULT,
                                                                       "",
                                                                       SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER,
                                                                       SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER,
                                                                       SERVER_KNOBS->TARGET_BYTES_PER_TLOG,
                                                                       SERVER_KNOBS->SPRING_BYTES_TLOG,
                                                                       SERVER_KNOBS->MAX_TL_SS_VERSION_DIFFERENCE,
                                                                       SERVER_KNOBS->TARGET_DURABILITY_LAG_VERSIONS),
    batchLimits(TransactionPriority::BATCH,
                "Batch",
                SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER_BATCH,
                SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER_BATCH,
                SERVER_KNOBS->TARGET_BYTES_PER_TLOG_BATCH,
                SERVER_KNOBS->SPRING_BYTES_TLOG_BATCH,
                SERVER_KNOBS->MAX_TL_SS_VERSION_DIFFERENCE_BATCH,
                SERVER_KNOBS->TARGET_DURABILITY_LAG_VERSIONS_BATCH),
    autoThrottlingEnabled(false) {
	expiredTagThrottleCleanup = recurring([this]() { ThrottleApi::expire(this->db.getReference()); },
	                                      SERVER_KNOBS->TAG_THROTTLE_EXPIRED_CLEANUP_INTERVAL);
}

void RatekeeperData::updateCommitCostEstimation(
    UIDTransactionTagMap<TransactionCommitCostEstimation> const& costEstimation) {
	for (auto it = storageQueueInfo.begin(); it != storageQueueInfo.end(); ++it) {
		auto tagCostIt = costEstimation.find(it->key);
		if (tagCostIt == costEstimation.end())
			continue;
		for (const auto& [tagName, cost] : tagCostIt->second) {
			it->value.tagCostEst[tagName] += cost;
			it->value.totalWriteCosts += cost.getCostSum();
			it->value.totalWriteOps += cost.getOpsSum();
		}
	}
}

void RatekeeperData::updateRate(RatekeeperLimits* limits) {
	// double controlFactor = ;  // dt / eFoldingTime

	double actualTps = smoothReleasedTransactions.smoothRate();
	actualTpsMetric = (int64_t)actualTps;
	// SOMEDAY: Remove the max( 1.0, ... ) since the below calculations _should_ be able to recover back up from this
	// value
	actualTps =
	    std::max(std::max(1.0, actualTps), smoothTotalDurableBytes.smoothRate() / CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT);

	if (actualTpsHistory.size() > SERVER_KNOBS->MAX_TPS_HISTORY_SAMPLES) {
		actualTpsHistory.pop_front();
	}
	actualTpsHistory.push_back(actualTps);

	limits->tpsLimit = std::numeric_limits<double>::infinity();
	UID reasonID = UID();
	limitReason_t limitReason = limitReason_t::unlimited;

	int sscount = 0;

	int64_t worstFreeSpaceStorageServer = std::numeric_limits<int64_t>::max();
	int64_t worstStorageQueueStorageServer = 0;
	int64_t limitingStorageQueueStorageServer = 0;
	int64_t worstDurabilityLag = 0;

	std::multimap<double, StorageQueueInfo*> storageTpsLimitReverseIndex;
	std::multimap<int64_t, StorageQueueInfo*> storageDurabilityLagReverseIndex;

	std::map<UID, limitReason_t> ssReasons;

	bool printRateKeepLimitReasonDetails =
	    SERVER_KNOBS->RATEKEEPER_PRINT_LIMIT_REASON &&
	    (deterministicRandom()->random01() < SERVER_KNOBS->RATEKEEPER_LIMIT_REASON_SAMPLE_RATE);

	// Look at each storage server's write queue and local rate, compute and store the desired rate ratio
	for (auto i = storageQueueInfo.begin(); i != storageQueueInfo.end(); ++i) {
		auto& ss = i->value;
		if (!ss.valid || (remoteDC.present() && ss.locality.dcId() == remoteDC))
			continue;
		++sscount;

		limitReason_t ssLimitReason = limitReason_t::unlimited;

		int64_t minFreeSpace =
		    std::max(SERVER_KNOBS->MIN_AVAILABLE_SPACE,
		             (int64_t)(SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO * ss.smoothTotalSpace.smoothTotal()));

		worstFreeSpaceStorageServer =
		    std::min(worstFreeSpaceStorageServer, (int64_t)ss.smoothFreeSpace.smoothTotal() - minFreeSpace);

		int64_t springBytes = std::max<int64_t>(
		    1, std::min<int64_t>(limits->storageSpringBytes, (ss.smoothFreeSpace.smoothTotal() - minFreeSpace) * 0.2));
		int64_t targetBytes = std::max<int64_t>(
		    1, std::min(limits->storageTargetBytes, (int64_t)ss.smoothFreeSpace.smoothTotal() - minFreeSpace));
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
				    .detail("SSSmoothTotalSpace", ss.smoothTotalSpace.smoothTotal())
				    .detail("SSSmoothFreeSpace", ss.smoothFreeSpace.smoothTotal())
				    .detail("TargetBytes", targetBytes)
				    .detail("LimitsStorageTargetBytes", limits->storageTargetBytes)
				    .detail("MinFreeSpace", minFreeSpace);
			}
		}

		int64_t storageQueue = ss.lastReply.bytesInput - ss.smoothDurableBytes.smoothTotal();
		worstStorageQueueStorageServer = std::max(worstStorageQueueStorageServer, storageQueue);

		int64_t storageDurabilityLag = ss.smoothLatestVersion.smoothTotal() - ss.smoothDurableVersion.smoothTotal();
		worstDurabilityLag = std::max(worstDurabilityLag, storageDurabilityLag);

		storageDurabilityLagReverseIndex.insert(std::make_pair(-1 * storageDurabilityLag, &ss));

		auto& ssMetrics = healthMetrics.storageStats[ss.id];
		ssMetrics.storageQueue = storageQueue;
		ssMetrics.storageDurabilityLag = storageDurabilityLag;
		ssMetrics.cpuUsage = ss.lastReply.cpuUsage;
		ssMetrics.diskUsage = ss.lastReply.diskUsage;

		double targetRateRatio = std::min((storageQueue - targetBytes + springBytes) / (double)springBytes, 2.0);

		if (limits->priority == TransactionPriority::DEFAULT) {
			tryAutoThrottleTag(ss, storageQueue, storageDurabilityLag);
		}

		double inputRate = ss.smoothInputBytes.smoothRate();
		// inputRate = std::max( inputRate, actualTps / SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE );

		/*if( deterministicRandom()->random01() < 0.1 ) {
		  std::string name = "RatekeeperUpdateRate" + limits.context;
		  TraceEvent(name, ss.id)
		  .detail("MinFreeSpace", minFreeSpace)
		  .detail("SpringBytes", springBytes)
		  .detail("TargetBytes", targetBytes)
		  .detail("SmoothTotalSpaceTotal", ss.smoothTotalSpace.smoothTotal())
		  .detail("SmoothFreeSpaceTotal", ss.smoothFreeSpace.smoothTotal())
		  .detail("LastReplyBytesInput", ss.lastReply.bytesInput)
		  .detail("SmoothDurableBytesTotal", ss.smoothDurableBytes.smoothTotal())
		  .detail("TargetRateRatio", targetRateRatio)
		  .detail("SmoothInputBytesRate", ss.smoothInputBytes.smoothRate())
		  .detail("ActualTPS", actualTps)
		  .detail("InputRate", inputRate)
		  .detail("VerySmoothDurableBytesRate", ss.verySmoothDurableBytes.smoothRate())
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
			    std::max(ss.verySmoothDurableBytes.smoothRate(), actualTps / SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE);
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
						    .detail("SSSmoothTotalSpace", ss.smoothTotalSpace.smoothTotal())
						    .detail("LimitsStorageTargetBytes", limits->storageTargetBytes)
						    .detail("LimitsStorageSpringBytes", limits->storageSpringBytes)
						    .detail("SSSmoothFreeSpace", ss.smoothFreeSpace.smoothTotal())
						    .detail("MinFreeSpace", minFreeSpace)
						    .detail("SSLastReplyBytesInput", ss.lastReply.bytesInput)
						    .detail("SSSmoothDurableBytes", ss.smoothDurableBytes.smoothTotal())
						    .detail("StorageQueue", storageQueue)
						    .detail("TargetBytes", targetBytes)
						    .detail("SpringBytes", springBytes)
						    .detail("SSVerySmoothDurableBytesSmoothRate", ss.verySmoothDurableBytes.smoothRate())
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

		limitingStorageQueueStorageServer =
		    ss->second->lastReply.bytesInput - ss->second->smoothDurableBytes.smoothTotal();
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
			maxTLVer = std::max(maxTLVer, tl.lastReply.v);
		}

		if (minSSVer != std::numeric_limits<Version>::max() && maxTLVer != std::numeric_limits<Version>::min()) {
			// writeToReadLatencyLimit: 0 = infinte speed; 1 = TL durable speed ; 2 = half TL durable speed
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
		auto& tl = it.value;
		if (!tl.valid)
			continue;
		++tlcount;

		limitReason_t tlogLimitReason = limitReason_t::log_server_write_queue;

		int64_t minFreeSpace =
		    std::max(SERVER_KNOBS->MIN_AVAILABLE_SPACE,
		             (int64_t)(SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO * tl.smoothTotalSpace.smoothTotal()));

		worstFreeSpaceTLog = std::min(worstFreeSpaceTLog, (int64_t)tl.smoothFreeSpace.smoothTotal() - minFreeSpace);

		int64_t springBytes = std::max<int64_t>(
		    1, std::min<int64_t>(limits->logSpringBytes, (tl.smoothFreeSpace.smoothTotal() - minFreeSpace) * 0.2));
		int64_t targetBytes = std::max<int64_t>(
		    1, std::min(limits->logTargetBytes, (int64_t)tl.smoothFreeSpace.smoothTotal() - minFreeSpace));
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
				    .detail("TLSmoothFreeSpace", tl.smoothFreeSpace.smoothTotal())
				    .detail("TLSmoothTotalSpace", tl.smoothTotalSpace.smoothTotal())
				    .detail("LimitsLogTargetBytes", limits->logTargetBytes)
				    .detail("TargetBytes", targetBytes)
				    .detail("MinFreeSpace", minFreeSpace);
			}
		}

		int64_t queue = tl.lastReply.bytesInput - tl.smoothDurableBytes.smoothTotal();
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
				    .detail("TLSmoothFreeSpace", tl.smoothFreeSpace.smoothTotal())
				    .detail("TLSmoothTotalSpace", tl.smoothTotalSpace.smoothTotal())
				    .detail("LimitsLogSpringBytes", limits->logSpringBytes)
				    .detail("LimitsLogTargetBytes", limits->logTargetBytes)
				    .detail("SpringBytes", springBytes)
				    .detail("TargetBytes", targetBytes)
				    .detail("TLLastReplyBytesInput", tl.lastReply.bytesInput)
				    .detail("TLSmoothDurableBytes", tl.smoothDurableBytes.smoothTotal())
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

		double inputRate = tl.smoothInputBytes.smoothRate();

		if (targetRateRatio > 0) {
			double smoothedRate =
			    std::max(tl.verySmoothDurableBytes.smoothRate(), actualTps / SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE);
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
					    .detail("TLSmoothFreeSpace", tl.smoothFreeSpace.smoothTotal())
					    .detail("TLSmoothTotalSpace", tl.smoothTotalSpace.smoothTotal())
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

	if (g_network->isSimulated() && g_simulator.speedUpSimulation) {
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
		    .detail("TagsAutoThrottled", throttledTags.autoThrottleCount())
		    .detail("TagsAutoThrottledBusyRead", throttledTags.busyReadTagCount)
		    .detail("TagsAutoThrottledBusyWrite", throttledTags.busyWriteTagCount)
		    .detail("TagsManuallyThrottled", throttledTags.manualThrottleCount())
		    .detail("AutoThrottlingEnabled", autoThrottlingEnabled)
		    .trackLatest(name);
	}
}

Future<Void> RatekeeperData::refreshStorageServerCommitCost() {
	if (lastBusiestCommitTagPick == 0) { // the first call should be skipped
		lastBusiestCommitTagPick = now();
		return Void();
	}
	double elapsed = now() - lastBusiestCommitTagPick;
	// for each SS, select the busiest commit tag from ssTrTagCommitCost
	for (auto it = storageQueueInfo.begin(); it != storageQueueInfo.end(); ++it) {
		it->value.busiestWriteTag.reset();
		TransactionTag busiestTag;
		TransactionCommitCostEstimation maxCost;
		double maxRate = 0, maxBusyness = 0;
		for (const auto& [tag, cost] : it->value.tagCostEst) {
			double rate = cost.getCostSum() / elapsed;
			if (rate > maxRate) {
				busiestTag = tag;
				maxRate = rate;
				maxCost = cost;
			}
		}
		if (maxRate > SERVER_KNOBS->MIN_TAG_WRITE_PAGES_RATE) {
			it->value.busiestWriteTag = busiestTag;
			// TraceEvent("RefreshSSCommitCost").detail("TotalWriteCost", it->value.totalWriteCost).detail("TotalWriteOps",it->value.totalWriteOps);
			ASSERT(it->value.totalWriteCosts > 0);
			maxBusyness = double(maxCost.getCostSum()) / it->value.totalWriteCosts;
			it->value.busiestWriteTagFractionalBusyness = maxBusyness;
			it->value.busiestWriteTagRate = maxRate;
		}

		TraceEvent("BusiestWriteTag", it->key)
		    .detail("Elapsed", elapsed)
		    .detail("Tag", printable(busiestTag))
		    .detail("TagOps", maxCost.getOpsSum())
		    .detail("TagCost", maxCost.getCostSum())
		    .detail("TotalCost", it->value.totalWriteCosts)
		    .detail("Reported", it->value.busiestWriteTag.present())
		    .trackLatest(it->value.busiestWriteTagEventHolder->trackingKey);

		// reset statistics
		it->value.tagCostEst.clear();
		it->value.totalWriteOps = 0;
		it->value.totalWriteCosts = 0;
	}
	lastBusiestCommitTagPick = now();
	return Void();
}

void RatekeeperData::tryAutoThrottleTag(TransactionTag tag, double rate, double busyness, TagThrottledReason reason) {
	// NOTE: before the comparison with MIN_TAG_COST, the busiest tag rate also compares with MIN_TAG_PAGES_RATE
	// currently MIN_TAG_PAGES_RATE > MIN_TAG_COST in our default knobs.
	if (busyness > SERVER_KNOBS->AUTO_THROTTLE_TARGET_TAG_BUSYNESS && rate > SERVER_KNOBS->MIN_TAG_COST) {
		TEST(true); // Transaction tag auto-throttled
		Optional<double> clientRate = throttledTags.autoThrottleTag(id, tag, busyness);
		if (clientRate.present()) {
			TagSet tags;
			tags.addTag(tag);

			Reference<DatabaseContext> dbRef = Reference<DatabaseContext>::addRef(db.getPtr());
			addActor.send(ThrottleApi::throttleTags(dbRef,
			                                        tags,
			                                        clientRate.get(),
			                                        SERVER_KNOBS->AUTO_TAG_THROTTLE_DURATION,
			                                        TagThrottleType::AUTO,
			                                        TransactionPriority::DEFAULT,
			                                        now() + SERVER_KNOBS->AUTO_TAG_THROTTLE_DURATION,
			                                        reason));
		}
	}
}

void RatekeeperData::tryAutoThrottleTag(StorageQueueInfo& ss, int64_t storageQueue, int64_t storageDurabilityLag) {
	// NOTE: we just keep it simple and don't differentiate write-saturation and read-saturation at the moment. In most
	// of situation, this works. More indicators besides queue size and durability lag could be investigated in the
	// future
	if (storageQueue > SERVER_KNOBS->AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES ||
	    storageDurabilityLag > SERVER_KNOBS->AUTO_TAG_THROTTLE_DURABILITY_LAG_VERSIONS) {
		if (ss.busiestWriteTag.present()) {
			tryAutoThrottleTag(ss.busiestWriteTag.get(),
			                   ss.busiestWriteTagRate,
			                   ss.busiestWriteTagFractionalBusyness,
			                   TagThrottledReason::BUSY_WRITE);
		}
		if (ss.busiestReadTag.present()) {
			tryAutoThrottleTag(ss.busiestReadTag.get(),
			                   ss.busiestReadTagRate,
			                   ss.busiestReadTagFractionalBusyness,
			                   TagThrottledReason::BUSY_READ);
		}
	}
}
