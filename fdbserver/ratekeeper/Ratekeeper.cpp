/*
 * Ratekeeper.cpp
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

#include <algorithm>

#include "fdbclient/Knobs.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/core/WaitFailure.h"
#include "fdbserver/ratekeeper/Ratekeeper.h"
#include "Ratekeeper.h"
#include "TagThrottler.h"
#include "flow/OwningResource.h"

#include "flow/CoroUtils.h"

static Future<Void> splitError(Future<Void> in, Promise<Void> errOut) {
	try {
		co_await in;
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled && !errOut.isSet())
			errOut.sendError(e);
		throw;
	}
}

Future<Void> Ratekeeper::configurationMonitor() {
	while (true) {
		ReadYourWritesTransaction tr(db);

		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				RangeResult results = co_await tr.getRange(configKeys, CLIENT_KNOBS->TOO_MANY);
				ASSERT(!results.more && results.size() < CLIENT_KNOBS->TOO_MANY);

				configuration.fromKeyValues((VectorRef<KeyValueRef>)results);

				Future<Void> watchFuture = tr.watch(moveKeysLockOwnerKey) || tr.watch(excludedServersVersionKey) ||
				                           tr.watch(failedServersVersionKey) || tr.watch(excludedLocalityVersionKey) ||
				                           tr.watch(failedLocalityVersionKey);
				co_await tr.commit();
				co_await watchFuture;
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}
}

Future<Void> Ratekeeper::monitorServerListChange(
    PromiseStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges) {
	std::map<UID, StorageServerInterface> oldServers;
	Transaction tr(db);

	while (true) {
		Error err;
		try {
			if (now() - lastSSListFetchedTimestamp > 2 * SERVER_KNOBS->SERVER_LIST_DELAY) {
				TraceEvent(SevWarnAlways, "RatekeeperGetSSListLongLatency", id)
				    .detail("Latency", now() - lastSSListFetchedTimestamp);
			}
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			std::vector<std::pair<StorageServerInterface, ProcessClass>> results =
			    co_await NativeAPI::getServerListAndProcessClasses(&tr);
			lastSSListFetchedTimestamp = now();

			std::map<UID, StorageServerInterface> newServers;
			for (const auto& [ssi, _] : results) {
				const UID serverId = ssi.id();
				newServers[serverId] = ssi;

				if (oldServers.contains(serverId)) {
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
			tr = Transaction(db);
			co_await delay(SERVER_KNOBS->SERVER_LIST_DELAY);
			continue;
		} catch (Error& e) {
			err = e;
		}
		if (err.code() != error_code_actor_cancelled) {
			TraceEvent("RatekeeperGetSSListError", id).errorUnsuppressed(err).suppressFor(1.0);
		}
		co_await tr.onError(err);
	}
}

Future<Void> Ratekeeper::trackStorageServerQueueInfo(StorageServerInterface ssi) {
	storageQueueInfo.insert(mapPair(ssi.id(), StorageQueueInfo(id, ssi.id(), ssi.locality)));
	healthMetrics.storageStats[ssi.id()] = HealthMetrics::StorageStats();
	TraceEvent("RkTrackStorageStart", id).detail("StorageServer", ssi.id()).detail("Locality", ssi.locality.toString());

	while (true) {
		try {
			ErrorOr<StorageQueuingMetricsReply> reply = co_await ssi.getQueuingMetrics.getReplyUnlessFailedFor(
			    StorageQueuingMetricsRequest(), 0, 0); // SOMEDAY: or tryGetReply?
			Map<UID, StorageQueueInfo>::iterator myQueueInfo = storageQueueInfo.find(ssi.id());
			ASSERT(myQueueInfo != storageQueueInfo.end());
			if (reply.present()) {
				myQueueInfo->value.update(reply.get(), smoothTotalDurableBytes);
				myQueueInfo->value.acceptingRequests = ssi.isAcceptingRequests();

				// Update health stats.
				auto ssMetrics = healthMetrics.storageStats.find(ssi.id());
				ASSERT(ssMetrics != healthMetrics.storageStats.end());
				ssMetrics->second.storageQueue = myQueueInfo->value.getStorageQueueBytes();
				ssMetrics->second.storageDurabilityLag = myQueueInfo->value.getDurabilityLag();
				ssMetrics->second.cpuUsage = reply.get().cpuUsage;
				ssMetrics->second.diskUsage = reply.get().diskUsage;
			} else {
				if (myQueueInfo->value.valid) {
					TraceEvent("RkStorageServerDidNotRespond", id).detail("StorageServer", ssi.id());
				}
				myQueueInfo->value.valid = false;
			}

			co_await (delayJittered(SERVER_KNOBS->METRIC_UPDATE_RATE) &&
			          IFailureMonitor::failureMonitor().onStateEqual(ssi.getQueuingMetrics.getEndpoint(),
			                                                         FailureStatus(false)));
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				storageQueueInfo.erase(ssi.id());
				healthMetrics.storageStats.erase(ssi.id());
				throw;
			}
			// Do no stop tracking storage server. The error might be recoverable.
			Map<UID, StorageQueueInfo>::iterator myQueueInfo = storageQueueInfo.find(ssi.id());
			ASSERT(myQueueInfo != storageQueueInfo.end());
			myQueueInfo->value.valid = false;
			TraceEvent("RkTrackStorageError", id)
			    .detail("StorageServer", ssi.id())
			    .detail("Locality", ssi.locality.toString())
			    .errorUnsuppressed(e);
		}
	}
}

// works with ExcludeIncludeStorageServersWorkload.actor.cpp to make sure the size of SS list is bounded
Future<Void> Ratekeeper::monitorStorageServerQueueSizeInSimulation() {
	if (!g_network->isSimulated()) {
		co_return;
	}
	int threshold = SERVER_KNOBS->RATEKEEPER_MONITOR_SS_THRESHOLD;
	int cnt = 0;
	int maxCount = 5;
	Transaction tr(db);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			std::vector<std::pair<StorageServerInterface, ProcessClass>> results =
			    co_await NativeAPI::getServerListAndProcessClasses(&tr);
			int serverListSize = results.size();
			int interfaceSize = storageServerInterfaces.size();
			int metricSize = healthMetrics.storageStats.size();
			if (interfaceSize - serverListSize > threshold || metricSize - serverListSize > threshold) {
				cnt++;
			} else {
				cnt = 0;
			}
			if (cnt > maxCount) {
				TraceEvent(SevError, "TooManySSInRK")
				    .detail("Threshold", threshold)
				    .detail("ServerListSize", serverListSize)
				    .detail("InterfaceSize", interfaceSize)
				    .detail("MetricSize", metricSize)
				    .log();
			}

			// Storage related stats should be consistent.
			for (const auto& [id, _] : storageServerInterfaces) {
				ASSERT(storageQueueInfo.find(id) != storageQueueInfo.end());
				ASSERT(healthMetrics.storageStats.find(id) != healthMetrics.storageStats.end());
			}

			// wait for monitorServerListChange to remove the interface
			co_await delay(SERVER_KNOBS->RATEKEEPER_MONITOR_SS_DELAY);
			tr = Transaction(db);
			continue;
		} catch (Error& e) {
			err = e;
		}
		TraceEvent("RateKeeperFailedToReadSSList").log();
		co_await tr.onError(err);
	}
}

Future<Void> Ratekeeper::trackTLogQueueInfo(TLogInterface tli) {
	tlogQueueInfo.insert(mapPair(tli.id(), TLogQueueInfo(tli.id())));
	Map<UID, TLogQueueInfo>::iterator myQueueInfo = tlogQueueInfo.find(tli.id());
	TraceEvent("RkTrackTLog", id).detail("TransactionLog", tli.id());
	try {
		while (true) {
			ErrorOr<TLogQueuingMetricsReply> reply = co_await tli.getQueuingMetrics.getReplyUnlessFailedFor(
			    TLogQueuingMetricsRequest(), 0, 0); // SOMEDAY: or tryGetReply?
			if (reply.present()) {
				myQueueInfo->value.update(reply.get(), smoothTotalDurableBytes);
				myQueueInfo->value.valid = true;
			} else {
				if (myQueueInfo->value.valid) {
					TraceEvent("RkTLogDidNotRespond", id).detail("TransactionLog", tli.id());
				}
				myQueueInfo->value.valid = false;
			}

			co_await (delayJittered(SERVER_KNOBS->METRIC_UPDATE_RATE) &&
			          IFailureMonitor::failureMonitor().onStateEqual(tli.getQueuingMetrics.getEndpoint(),
			                                                         FailureStatus(false)));
		}
	} catch (Error& e) {
		// including cancellation
		tlogQueueInfo.erase(myQueueInfo);
		if (e.code() != error_code_actor_cancelled) {
			TraceEvent("RkTrackTLogError", id).detail("TransactionLog", tli.id()).errorUnsuppressed(e);
		}
		throw;
	}
}

Future<Void> Ratekeeper::trackEachStorageServer(
    FutureStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges) {
	std::unordered_map<UID, Future<Void>> storageServerTrackers;
	Promise<Void> err;

	while (true) {
		auto res = co_await race(serverChanges, err.getFuture());
		ASSERT_EQ(res.index(), 0);
		std::pair<UID, Optional<StorageServerInterface>> change = std::get<0>(std::move(res));

		co_await delay(0); // prevent storageServerTracker from getting cancelled while on the call stack

		const UID& id = change.first;
		if (change.second.present()) {
			if (!change.second.get().isTss()) {

				auto& a = storageServerTrackers[change.first];
				a = Future<Void>();
				a = splitError(trackStorageServerQueueInfo(change.second.get()), err);

				storageServerInterfaces[id] = change.second.get();
			}
		} else {
			storageServerTrackers.erase(id);
			storageServerInterfaces.erase(id);
		}
	}
}

Future<Void> Ratekeeper::monitorHotShards(Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	while (true) {
		co_await delay(SERVER_KNOBS->HOT_SHARD_MONITOR_FREQUENCY);
		if (!ssHighWriteQueue.present()) {
			continue;
		}

		UID ssi = ssHighWriteQueue.get();
		SetThrottledShardRequest setReq;

		// TraceEvent(SevDebug, "SendGetHotShardsRequest");
		try {
			GetHotShardsRequest getReq;
			GetHotShardsReply reply = co_await storageServerInterfaces[ssi].getHotShards.getReply(getReq);

			// Backup's restore range can't be throttled, otherwise restore would fail,
			// i.e., "ApplyMutationsError".
			KeyRangeRef applyMutationRange("\xfe\xff\xfe"_sr, "\xfe\xff\xff\xff"_sr);
			for (const auto& shard : reply.hotShards) {
				if (!shard.intersects(applyMutationRange)) {
					setReq.throttledShards.push_back(shard);
				} else {
					TraceEvent("IgnoreHotShard").detail("Shard", shard);
				}
			}
		} catch (Error& e) {
			TraceEvent(SevWarn, "CannotMonitorHotShardForSS").detail("SS", ssi);
			continue;
		}
		if (setReq.throttledShards.empty()) {
			continue;
		}
		setReq.expirationTime = now() + SERVER_KNOBS->HOT_SHARD_THROTTLING_EXPIRE_AFTER;
		for (auto& shard : setReq.throttledShards) {
			TraceEvent(SevInfo, "SendRequestThrottleHotShard")
			    .detail("Shard", shard)
			    .detail("DelayUntil", setReq.expirationTime);
		}
		for (const auto& cpi : dbInfo->get().client.commitProxies) {
			cpi.setThrottledShard.send(setReq);
		}
	}
}

Future<Void> Ratekeeper::handleReportCommitCostEstimationReqs(RatekeeperInterface rkInterf) {
	while (true) {
		ReportCommitCostEstimationRequest req = co_await rkInterf.reportCommitCostEstimation.getFuture();
		updateCommitCostEstimation(req.ssTrTagCommitCost);
		req.reply.send(Void());
	}
}

Future<Void> Ratekeeper::handleGetSSVersionLagReqs(RatekeeperInterface rkInterf) {
	while (true) {
		GetSSVersionLagRequest req = co_await rkInterf.getSSVersionLag.getFuture();
		GetSSVersionLagReply reply;
		getSSVersionLag(reply.maxPrimarySSVersion, reply.maxRemoteSSVersion);
		req.reply.send(reply);
	}
}

Future<Void> Ratekeeper::handleGetRateInfoReqs(RatekeeperInterface rkInterf,
                                               Version* recoveryVersion,
                                               bool* lastLimited) {
	while (true) {
		GetRateInfoRequest req = co_await rkInterf.getRateInfo.getFuture();
		GetRateInfoReply reply;

		auto& p = grvProxyInfo[req.requesterID];
		//TraceEvent("RKMPU", req.requesterID).detail("TRT", req.totalReleasedTransactions).detail("Last", p.totalTransactions).detail("Delta", req.totalReleasedTransactions - p.totalTransactions);
		if (p.totalTransactions > 0) {
			smoothReleasedTransactions.addDelta(req.totalReleasedTransactions - p.totalTransactions);

			for (auto const& [tag, count] : req.throttledTagCounts) {
				tagThrottler->addRequests(tag, count);
			}
		}
		if (p.batchTransactions > 0) {
			smoothBatchReleasedTransactions.addDelta(req.batchReleasedTransactions - p.batchTransactions);
		}

		p.totalTransactions = req.totalReleasedTransactions;
		p.batchTransactions = req.batchReleasedTransactions;
		p.version = req.version;
		maxVersion = std::max(maxVersion, req.version);

		if (*recoveryVersion == std::numeric_limits<Version>::max() && version_recovery.contains(*recoveryVersion)) {
			*recoveryVersion = maxVersion;
			version_recovery[*recoveryVersion] = version_recovery[std::numeric_limits<Version>::max()];
			version_recovery.erase(std::numeric_limits<Version>::max());
		}

		p.lastUpdateTime = now();

		reply.transactionRate = normalLimits.tpsLimit / grvProxyInfo.size();
		reply.batchTransactionRate = batchLimits.tpsLimit / grvProxyInfo.size();
		reply.leaseDuration = SERVER_KNOBS->METRIC_UPDATE_RATE;

		if (p.lastThrottledTagChangeId != tagThrottler->getThrottledTagChangeId() ||
		    now() > p.lastTagPushTime + SERVER_KNOBS->TAG_THROTTLE_PUSH_INTERVAL) {
			p.lastThrottledTagChangeId = tagThrottler->getThrottledTagChangeId();
			p.lastTagPushTime = now();

			bool returningTagsToProxy{ false };
			if (SERVER_KNOBS->ENFORCE_TAG_THROTTLING_ON_PROXIES) {
				auto proxyThrottledTags = tagThrottler->getProxyRates(grvProxyInfo.size());
				if (!SERVER_KNOBS->GLOBAL_TAG_THROTTLING_REPORT_ONLY) {
					returningTagsToProxy = !proxyThrottledTags.empty();
					reply.proxyThrottledTags = std::move(proxyThrottledTags);
				}
			} else {
				auto clientThrottledTags = tagThrottler->getClientRates();
				if (!SERVER_KNOBS->GLOBAL_TAG_THROTTLING_REPORT_ONLY) {
					returningTagsToProxy = !clientThrottledTags.empty();
					reply.clientThrottledTags = std::move(clientThrottledTags);
				}
			}
			CODE_PROBE(returningTagsToProxy, "Returning tag throttles to a proxy");
		}

		reply.healthMetrics.update(healthMetrics, true, req.detailed);
		reply.healthMetrics.tpsLimit = normalLimits.tpsLimit;
		reply.healthMetrics.batchLimited = *lastLimited;

		req.reply.send(reply);
	}
}

Future<Void> Ratekeeper::handleDBInfoChanges(Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                                             bool* recovering,
                                             Version* recoveryVersion,
                                             std::vector<TLogInterface>* tlogInterfs,
                                             std::vector<Future<Void>>* tlogTrackers,
                                             Promise<Void>* err) {
	while (true) {
		co_await dbInfo->onChange();

		if (!*recovering && dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
			*recovering = true;
			*recoveryVersion = maxVersion;
			if (*recoveryVersion == 0) {
				*recoveryVersion = std::numeric_limits<Version>::max();
			}
			if (version_recovery.contains(*recoveryVersion)) {
				auto& it = version_recovery[*recoveryVersion];
				double existingEnd = it.second.present() ? it.second.get() : now();
				double existingDuration = existingEnd - it.first;
				version_recovery[*recoveryVersion] = std::make_pair(now() - existingDuration, Optional<double>());
			} else {
				version_recovery[*recoveryVersion] = std::make_pair(now(), Optional<double>());
			}
		}
		if (*recovering && dbInfo->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS) {
			*recovering = false;
			version_recovery[*recoveryVersion].second = now();
		}

		if (*tlogInterfs != dbInfo->get().logSystemConfig.allLocalLogs()) {
			*tlogInterfs = dbInfo->get().logSystemConfig.allLocalLogs();
			*tlogTrackers = std::vector<Future<Void>>();
			for (int i = 0; i < tlogInterfs->size(); i++) {
				tlogTrackers->push_back(splitError(trackTLogQueueInfo((*tlogInterfs)[i]), *err));
			}
		}
		remoteDC = dbInfo->get().logSystemConfig.getRemoteDcId();
	}
}

Future<Void> Ratekeeper::rateUpdater(bool* lastLimited) {
	while (true) {
		double actualTps = smoothReleasedTransactions.smoothRate();
		actualTps = std::max(std::max(1.0, actualTps),
		                     smoothTotalDurableBytes.smoothRate() / CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT);

		if (actualTpsHistory.size() > SERVER_KNOBS->MAX_TPS_HISTORY_SAMPLES) {
			actualTpsHistory.pop_front();
		}
		actualTpsHistory.push_back(actualTps);

		while (version_recovery.size() > CLIENT_KNOBS->MAX_GENERATIONS) {
			version_recovery.erase(version_recovery.begin());
		}

		updateRate(&normalLimits);
		updateRate(&batchLimits);

		*lastLimited =
		    smoothReleasedTransactions.smoothRate() > SERVER_KNOBS->LAST_LIMITED_RATIO * batchLimits.tpsLimit;
		double tooOld = now() - 1.0;
		for (auto p = grvProxyInfo.begin(); p != grvProxyInfo.end();) {
			if (p->second.lastUpdateTime < tooOld)
				p = grvProxyInfo.erase(p);
			else
				++p;
		}
		co_await delayJittered(SERVER_KNOBS->METRIC_UPDATE_RATE);
	}
}

Future<Void> Ratekeeper::run(RatekeeperInterface rkInterf, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	ActorOwningSelfRef<Ratekeeper> pSelf(
	    new Ratekeeper(rkInterf.id(), openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True)));
	Ratekeeper& self = *pSelf;
	std::vector<Future<Void>> tlogTrackers;
	std::vector<TLogInterface> tlogInterfs;
	Promise<Void> err;
	Future<Void> collection = actorCollection(self.addActor.getFuture());

	TraceEvent("RatekeeperStarting", rkInterf.id());
	self.addActor.send(waitFailureServer(rkInterf.waitFailure.getFuture()));
	self.addActor.send(self.configurationMonitor());

	PromiseStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges;
	self.addActor.send(self.monitorServerListChange(serverChanges));
	self.addActor.send(self.trackEachStorageServer(serverChanges.getFuture()));
	self.addActor.send(self.monitorStorageServerQueueSizeInSimulation());
	self.addActor.send(traceRole(Role::RATEKEEPER, rkInterf.id()));

	self.addActor.send(self.monitorThrottlingChanges());

	if (SERVER_KNOBS->HOT_SHARD_THROTTLING_ENABLED) {
		self.addActor.send(self.monitorHotShards(dbInfo));
	}

	self.addActor.send(self.refreshStorageServerCommitCosts());
	self.addActor.send(self.handleReportCommitCostEstimationReqs(rkInterf));
	self.addActor.send(self.handleGetSSVersionLagReqs(rkInterf));

	TraceEvent("RkTLogQueueSizeParameters", rkInterf.id())
	    .detail("Target", SERVER_KNOBS->TARGET_BYTES_PER_TLOG)
	    .detail("Spring", SERVER_KNOBS->SPRING_BYTES_TLOG)
	    .detail("Rate",
	            (SERVER_KNOBS->TARGET_BYTES_PER_TLOG - SERVER_KNOBS->SPRING_BYTES_TLOG) /
	                ((((double)SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) / SERVER_KNOBS->VERSIONS_PER_SECOND) +
	                 2.0));

	TraceEvent("RkStorageServerQueueSizeParameters", rkInterf.id())
	    .detail("Target", SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER)
	    .detail("Spring", SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER)
	    .detail("EBrake", SERVER_KNOBS->STORAGE_HARD_LIMIT_BYTES)
	    .detail("Rate",
	            (SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER - SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER) /
	                ((((double)SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) / SERVER_KNOBS->VERSIONS_PER_SECOND) +
	                 2.0));

	tlogInterfs = dbInfo->get().logSystemConfig.allLocalLogs();
	tlogTrackers.reserve(tlogInterfs.size());
	for (int i = 0; i < tlogInterfs.size(); i++) {
		tlogTrackers.push_back(splitError(self.trackTLogQueueInfo(tlogInterfs[i]), err));
	}

	self.remoteDC = dbInfo->get().logSystemConfig.getRemoteDcId();

	bool recovering = dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS;
	Version recoveryVersion = std::numeric_limits<Version>::max();
	if (recovering) {
		self.version_recovery[recoveryVersion] = std::make_pair(now(), Optional<double>());
	}

	bool lastLimited = false;
	self.addActor.send(self.handleGetRateInfoReqs(rkInterf, &recoveryVersion, &lastLimited));
	self.addActor.send(
	    self.handleDBInfoChanges(dbInfo, &recovering, &recoveryVersion, &tlogInterfs, &tlogTrackers, &err));
	self.addActor.send(self.rateUpdater(&lastLimited));

	try {
		auto res = co_await race(rkInterf.haltRatekeeper.getFuture(), err.getFuture(), collection);
		ASSERT_EQ(res.index(), 0);
		HaltRatekeeperRequest req = std::get<0>(std::move(res));
		req.reply.send(Void());
		TraceEvent("RatekeeperHalted", rkInterf.id()).detail("ReqID", req.requesterID);
	} catch (Error& err) {
		TraceEvent("RatekeeperDied", rkInterf.id()).errorUnsuppressed(err);
	}
}

Future<Void> Ratekeeper::refreshStorageServerCommitCosts() {
	double lastBusiestCommitTagPick{ 0 };
	std::vector<Future<Void>> replies;
	while (true) {
		lastBusiestCommitTagPick = now();
		co_await delay(SERVER_KNOBS->TAG_MEASUREMENT_INTERVAL);

		replies.clear();

		double elapsed = now() - lastBusiestCommitTagPick;
		// for each SS, select the busiest commit tag from ssTrTagCommitCost
		for (auto& [ssId, ssQueueInfo] : storageQueueInfo) {
			// NOTE: In some cases, for unknown reason SS will not respond to the updateCommitCostRequest. Since the
			// information is not time-sensitive, we do not wait for the replies.
			replies.push_back(
			    storageServerInterfaces[ssId].updateCommitCostRequest.getReply(ssQueueInfo.refreshCommitCost(elapsed)));
		}
	}
}

Future<Void> Ratekeeper::monitorThrottlingChanges() {
	return tagThrottler->monitorThrottlingChanges();
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
		tagThrottler = std::make_unique<GlobalTagThrottler>(
		    db, id, SERVER_KNOBS->MAX_MACHINES_FALLING_BEHIND, SERVER_KNOBS->GLOBAL_TAG_THROTTLING_LIMITING_THRESHOLD);
	} else {
		tagThrottler = std::make_unique<TagThrottler>(db, id);
	}
}

void Ratekeeper::updateCommitCostEstimation(
    UIDTransactionTagMap<TransactionCommitCostEstimation> const& costEstimation) {
	for (auto it = storageQueueInfo.begin(); it != storageQueueInfo.end(); ++it) {
		auto tagCostIt = costEstimation.find(it->key);
		if (tagCostIt == costEstimation.end()) {
			continue;
		}
		for (const auto& [tagName, cost] : tagCostIt->second) {
			it->value.addCommitCost(tagName, cost);
		}
	}
}

static std::string getIgnoredZonesReasons(
    std::set<Optional<Standalone<StringRef>>>& ignoredMachines,
    std::map<Optional<Standalone<StringRef>>, std::set<limitReason_t>>& zoneReasons) {
	std::string ignoredZoneReasons;
	for (auto zone : ignoredMachines) {
		ignoredZoneReasons += zone.get().toString() + " [";
		for (auto reason : zoneReasons[zone]) {
			ignoredZoneReasons += std::to_string(reason) + " ";
		}
		ignoredZoneReasons += "] ";
	}
	return !ignoredZoneReasons.empty() ? ignoredZoneReasons : "None";
}

void Ratekeeper::updateRate(RatekeeperLimits* limits) {
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
	std::map<Optional<Standalone<StringRef>>, std::set<limitReason_t>> zoneReasons;

	bool printRateKeepLimitReasonDetails =
	    SERVER_KNOBS->RATEKEEPER_PRINT_LIMIT_REASON &&
	    (deterministicRandom()->random01() < SERVER_KNOBS->RATEKEEPER_LIMIT_REASON_SAMPLE_RATE);

	// Look at each storage server's write queue and local rate, compute and store the desired rate
	// ratio
	for (auto i = storageQueueInfo.begin(); i != storageQueueInfo.end(); ++i) {
		auto const& ss = i->value;
		if (!ss.valid || !ss.acceptingRequests || (remoteDC.present() && ss.locality.dcId() == remoteDC)) {
			continue;
		}
		++sscount;

		limitReason_t ssLimitReason = limitReason_t::unlimited;

		int64_t minFreeSpace = std::max(SERVER_KNOBS->MIN_AVAILABLE_SPACE,
		                                (int64_t)(SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO * ss.getSmoothTotalSpace()));

		worstFreeSpaceStorageServer = std::min(worstFreeSpaceStorageServer,
		                                       std::max((int64_t)ss.getSmoothFreeSpace() - minFreeSpace, (int64_t)0));

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

		double targetRateRatio = std::min((storageQueue - targetBytes + springBytes) / (double)springBytes, 2.0);

		if (limits->priority == TransactionPriority::DEFAULT) {
			addActor.send(tagThrottler->tryUpdateAutoThrottling(ss));
		}

		double inputRate = ss.getSmoothInputBytesRate();

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
		if (ssLimitReason == limitReason_t::unlimited) {
			ssLimitReason = limitReason_t::storage_server_write_bandwidth_mvcc;
		}

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
		zoneReasons[ss.locality.zoneId()].insert(ssLimitReason);
	}

	tagThrottler->updateThrottling(storageQueueInfo);

	std::set<Optional<Standalone<StringRef>>> ignoredMachines;
	for (auto ss = storageTpsLimitReverseIndex.begin();
	     ss != storageTpsLimitReverseIndex.end() && ss->first < limits->tpsLimit;
	     ++ss) {
		if (ignoredMachines.size() <
		    std::min(configuration.storageTeamSize - 1, SERVER_KNOBS->MAX_MACHINES_FALLING_BEHIND)) {
			ignoredMachines.insert(ss->second->locality.zoneId());
			continue;
		}
		if (ignoredMachines.contains(ss->second->locality.zoneId())) {
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
		if (ignoredDurabilityLagMachines.contains(ss->second->locality.zoneId())) {
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
			// Keep version-lag based throttling consistent with queue/durability lag calculations:
			// ignore storage servers until they are accepting requests.
			if (!ss.valid || !ss.acceptingRequests || (remoteDC.present() && ss.locality.dcId() == remoteDC)) {
				continue;
			}

			minSSVer = std::min(minSSVer, ss.lastReply.version);

			// Machines that ratekeeper isn't controlling can fall arbitrarily far behind
			if (!ignoredMachines.contains(it.value.locality.zoneId())) {
				minLimitingSSVer = std::min(minLimitingSSVer, ss.lastReply.version);
			}
		}

		Version maxTLVer = std::numeric_limits<Version>::min();
		for (const auto& it : tlogQueueInfo) {
			auto& tl = it.value;
			if (!tl.valid) {
				continue;
			}
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
		if (!tl.valid) {
			continue;
		}
		++tlcount;

		limitReason_t tlogLimitReason = limitReason_t::log_server_write_queue;

		const auto smoothTotalSpace = tl.getSmoothTotalSpace();
		const auto smoothFreeSpace = tl.getSmoothFreeSpace();
		const auto minFreeSpaceByRatio =
		    static_cast<int64_t>(SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO * smoothTotalSpace);
		const auto minFreeSpace = std::max(SERVER_KNOBS->MIN_AVAILABLE_SPACE, minFreeSpaceByRatio);
		// Start shrinking the tlog queue budget once available space drops below the configured ratio,
		// then ramp down linearly until reaching minFreeSpace.
		const auto throttleStartSpaceByRatio =
		    static_cast<int64_t>(SERVER_KNOBS->TLOG_THROTTLE_START_AVAILABLE_SPACE_RATIO * smoothTotalSpace);
		const auto throttleStartSpace = std::max(minFreeSpace + 1, throttleStartSpaceByRatio);
		const auto availableAboveMin = smoothFreeSpace - minFreeSpace;
		const auto availableAboveMinBytes = static_cast<int64_t>(availableAboveMin);
		const auto throttleWindow = static_cast<double>(throttleStartSpace - minFreeSpace);
		const auto diskBudgetRatio = std::clamp(availableAboveMin / throttleWindow, 0.0, 1.0);

		worstFreeSpaceTLog = std::min(worstFreeSpaceTLog, std::max(availableAboveMinBytes, int64_t{ 0 }));

		const auto scaledSpringBytes = static_cast<int64_t>(limits->logSpringBytes * diskBudgetRatio);
		const auto springBytes =
		    std::max(int64_t{ 1 }, std::min(std::max(int64_t{ 1 }, scaledSpringBytes), availableAboveMinBytes));
		const auto scaledTargetBytes = static_cast<int64_t>(limits->logTargetBytes * diskBudgetRatio);
		const auto targetBytes =
		    std::max(int64_t{ 1 }, std::min(std::max(int64_t{ 1 }, scaledTargetBytes), availableAboveMinBytes));

		CODE_PROBE(diskBudgetRatio < 1.0, "Ratekeeper tlog disk budget ratio below one");
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
				    .detail("TLSmoothFreeSpace", smoothFreeSpace)
				    .detail("TLSmoothTotalSpace", smoothTotalSpace)
				    .detail("LimitsLogTargetBytes", limits->logTargetBytes)
				    .detail("ThrottleStartSpace", throttleStartSpace)
				    .detail("DiskBudgetRatio", diskBudgetRatio)
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
				TraceEvent(SevWarnAlways, "RkTLogMinFreeSpaceZero", id).detail("ReasonId", tl.id);
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
			if (targetRateRatio < .75) { //< FIXME: KNOB for 2.0
				x = std::max(x, 0.95);
			}
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
		    .detail("IgnoredZonesReasons", getIgnoredZonesReasons(ignoredMachines, zoneReasons))
		    .detail("TagsAutoThrottled", tagThrottler->autoThrottleCount())
		    .detail("TagsAutoThrottledBusyRead", tagThrottler->busyReadTagCount())
		    .detail("TagsAutoThrottledBusyWrite", tagThrottler->busyWriteTagCount())
		    .detail("TagsManuallyThrottled", tagThrottler->manualThrottleCount())
		    .detail("AutoThrottlingEnabled", tagThrottler->isAutoThrottlingEnabled())
		    .trackLatest(name);
	}
	ssHighWriteQueue.reset();
	if (limitReason == limitReason_t::storage_server_write_queue_size) {
		ssHighWriteQueue = reasonID;
	}
}

void Ratekeeper::getSSVersionLag(Version& maxSSPrimaryVersion, Version& maxSSRemoteVersion) {
	maxSSPrimaryVersion = maxSSRemoteVersion = invalidVersion;
	for (auto i = storageQueueInfo.begin(); i != storageQueueInfo.end(); ++i) {
		auto const& ss = i->value;
		if (!ss.valid || !ss.acceptingRequests) {
			continue;
		}

		if (remoteDC.present() && ss.locality.dcId() == remoteDC) {
			maxSSRemoteVersion = std::max(ss.getLatestVersion(), maxSSRemoteVersion);
		} else {
			maxSSPrimaryVersion = std::max(ss.getLatestVersion(), maxSSPrimaryVersion);
		}
	}
}

Future<Void> ratekeeper(RatekeeperInterface rkInterf, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	co_await Ratekeeper::run(rkInterf, dbInfo);
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
	double maxRate = 0;
	std::priority_queue<BusyTagInfo, std::vector<BusyTagInfo>, std::greater<BusyTagInfo>> topKWriters;
	for (const auto& [tag, cost] : tagCostEst) {
		double rate = cost.getCostSum() / elapsed;
		double busyness = static_cast<double>(maxCost.getCostSum()) / totalWriteCosts;
		if (rate < SERVER_KNOBS->MIN_TAG_WRITE_PAGES_RATE * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE) {
			continue;
		}
		if (topKWriters.size() < SERVER_KNOBS->SS_THROTTLE_TAGS_TRACKED) {
			topKWriters.emplace(tag, rate, busyness);
		} else if (topKWriters.top().rate < rate) {
			topKWriters.pop();
			topKWriters.emplace(tag, rate, busyness);
		}

		if (rate > maxRate) {
			busiestTag = tag;
			maxRate = rate;
			maxCost = cost;
		}
	}

	while (!topKWriters.empty()) {
		busiestWriteTags.push_back(std::move(topKWriters.top()));
		topKWriters.pop();
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
