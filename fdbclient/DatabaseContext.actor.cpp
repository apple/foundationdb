/*
 * DatabaseContext.cpp
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

// TODO: prune down the list of includes. This was copied from NativeAPI.actor.cpp.
#include "fdbclient/NativeAPI.actor.h"

#include <algorithm>
#include <cstdio>
#include <iterator>
#include <limits>
#include <memory>
#include <regex>
#include <string>
#include <unordered_set>
#include <tuple>
#include <utility>
#include <vector>

#include "boost/algorithm/string.hpp"

#include "fdbclient/Knobs.h"
#include "flow/CodeProbe.h"
#include "fmt/format.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/MultiInterface.h"
#include "fdbrpc/TenantInfo.h"

#include "fdbclient/ActorLineageProfiler.h"
#include "fdbclient/AnnotateActor.h"
#include "fdbclient/Atomic.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/BlobGranuleRequest.actor.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/ClusterConnectionFile.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/GlobalConfig.actor.h"
#include "fdbclient/IKnobCollection.h"
#include "fdbclient/JsonBuilder.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NameLineage.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/MonitorLeader.h"
#include "fdbclient/MutationList.h"
#include "fdbclient/ParallelStream.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantSpecialKeys.actor.h"
#include "fdbclient/TransactionLineage.h"
#include "fdbclient/versions.h"
#include "fdbrpc/WellKnownEndpoints.h"
#include "fdbrpc/LoadBalance.h"
#include "fdbrpc/Net2FileSystem.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/sim_validation.h"
#include "flow/Arena.h"
#include "flow/ActorCollection.h"
#include "flow/DeterministicRandom.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/GetSourceVersion.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/ProtocolVersion.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"
#include "flow/Knobs.h"
#include "flow/Platform.h"
#include "flow/SystemMonitor.h"
#include "flow/TLSConfig.actor.h"
#include "fdbclient/Tracing.h"
#include "flow/UnitTest.h"
#include "flow/network.h"
#include "flow/serialize.h"

#ifdef ADDRESS_SANITIZER
#include <sanitizer/lsan_interface.h>
#endif

#ifdef WIN32
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
#undef min
#undef max
#else
#include <time.h>
#endif
#include "flow/actorcompiler.h" // This must be the last #include.

Reference<WatchMetadata> DatabaseContext::getWatchMetadata(int64_t tenantId, KeyRef key) const {
	const auto it = watchMap.find(std::make_pair(tenantId, key));
	if (it == watchMap.end())
		return Reference<WatchMetadata>();
	return it->second;
}

void DatabaseContext::setWatchMetadata(Reference<WatchMetadata> metadata) {
	const WatchMapKey key(metadata->parameters->tenant.tenantId, metadata->parameters->key);
	watchMap[key] = metadata;
	// NOTE Here we do *NOT* update/reset the reference count for the key, see the source code in getWatchFuture.
	// Basically the reference count could be increased, or the same watch is refreshed, or the watch might be cancelled
}

int32_t DatabaseContext::increaseWatchRefCount(const int64_t tenantID, KeyRef key, const Version& version) {
	const WatchMapKey mapKey(tenantID, key);
	watchCounterMap[mapKey].insert(version);
	return watchCounterMap[mapKey].size();
}

int32_t DatabaseContext::decreaseWatchRefCount(const int64_t tenantID, KeyRef key, const Version& version) {
	const WatchMapKey mapKey(tenantID, key);
	auto mapKeyIter = watchCounterMap.find(mapKey);
	if (mapKeyIter == std::end(watchCounterMap)) {
		// Key does not exist. The metadata might be removed by deleteWatchMetadata already.
		return 0;
	}

	auto& versionSet = mapKeyIter->second;
	auto versionIter = versionSet.find(version);

	if (versionIter == std::end(versionSet)) {
		// Version not found, the watch might be cleared before.
		return versionSet.size();
	}
	versionSet.erase(versionIter);

	const auto count = versionSet.size();
	// The metadata might be deleted somewhere else, before calling this decreaseWatchRefCount
	if (auto metadata = getWatchMetadata(tenantID, key); metadata.isValid() && versionSet.size() == 0) {
		// It is a *must* to cancel the watchFutureSS manually. watchFutureSS waits for watchStorageServerResp, which
		// holds a reference to the metadata. If the ACTOR is not cancelled, it indirectly holds a Future waiting for
		// itself.
		metadata->watchFutureSS.cancel();
		deleteWatchMetadata(tenantID, key);
	}

	return count;
}

void DatabaseContext::deleteWatchMetadata(int64_t tenantId, KeyRef key, bool removeReferenceCount) {
	const WatchMapKey mapKey(tenantId, key);
	watchMap.erase(mapKey);
	if (removeReferenceCount) {
		watchCounterMap.erase(mapKey);
	}
}

void DatabaseContext::addTssMapping(StorageServerInterface const& ssi, StorageServerInterface const& tssi) {
	auto result = tssMapping.find(ssi.id());
	// Update tss endpoint mapping if ss isn't in mapping, or the interface it mapped to changed
	if (result == tssMapping.end() ||
	    result->second.getValue.getEndpoint().token.first() != tssi.getValue.getEndpoint().token.first()) {
		Reference<TSSMetrics> metrics;
		if (result == tssMapping.end()) {
			// new TSS pairing
			metrics = makeReference<TSSMetrics>();
			tssMetrics[tssi.id()] = metrics;
			tssMapping[ssi.id()] = tssi;
		} else {
			ASSERT(result->second.id() == tssi.id());
			metrics = tssMetrics[tssi.id()];
			result->second = tssi;
		}

		// data requests duplicated for load and data comparison
		queueModel.updateTssEndpoint(ssi.getValue.getEndpoint().token.first(),
		                             TSSEndpointData(tssi.id(), tssi.getValue.getEndpoint(), metrics));
		queueModel.updateTssEndpoint(ssi.getKey.getEndpoint().token.first(),
		                             TSSEndpointData(tssi.id(), tssi.getKey.getEndpoint(), metrics));
		queueModel.updateTssEndpoint(ssi.getKeyValues.getEndpoint().token.first(),
		                             TSSEndpointData(tssi.id(), tssi.getKeyValues.getEndpoint(), metrics));
		queueModel.updateTssEndpoint(ssi.getMappedKeyValues.getEndpoint().token.first(),
		                             TSSEndpointData(tssi.id(), tssi.getMappedKeyValues.getEndpoint(), metrics));
		queueModel.updateTssEndpoint(ssi.getKeyValuesStream.getEndpoint().token.first(),
		                             TSSEndpointData(tssi.id(), tssi.getKeyValuesStream.getEndpoint(), metrics));
		queueModel.updateTssEndpoint(ssi.changeFeedStream.getEndpoint().token.first(),
		                             TSSEndpointData(tssi.id(), tssi.changeFeedStream.getEndpoint(), metrics));

		// non-data requests duplicated for load
		queueModel.updateTssEndpoint(ssi.watchValue.getEndpoint().token.first(),
		                             TSSEndpointData(tssi.id(), tssi.watchValue.getEndpoint(), metrics));
		queueModel.updateTssEndpoint(ssi.splitMetrics.getEndpoint().token.first(),
		                             TSSEndpointData(tssi.id(), tssi.splitMetrics.getEndpoint(), metrics));
		queueModel.updateTssEndpoint(ssi.getReadHotRanges.getEndpoint().token.first(),
		                             TSSEndpointData(tssi.id(), tssi.getReadHotRanges.getEndpoint(), metrics));
		queueModel.updateTssEndpoint(ssi.getRangeSplitPoints.getEndpoint().token.first(),
		                             TSSEndpointData(tssi.id(), tssi.getRangeSplitPoints.getEndpoint(), metrics));
		queueModel.updateTssEndpoint(ssi.overlappingChangeFeeds.getEndpoint().token.first(),
		                             TSSEndpointData(tssi.id(), tssi.overlappingChangeFeeds.getEndpoint(), metrics));

		// duplicated to ensure feed data cleanup
		queueModel.updateTssEndpoint(ssi.changeFeedPop.getEndpoint().token.first(),
		                             TSSEndpointData(tssi.id(), tssi.changeFeedPop.getEndpoint(), metrics));
	}
}

void DatabaseContext::removeTssMapping(StorageServerInterface const& ssi) {
	auto result = tssMapping.find(ssi.id());
	if (result != tssMapping.end()) {
		tssMetrics.erase(ssi.id());
		tssMapping.erase(result);
		queueModel.removeTssEndpoint(ssi.getValue.getEndpoint().token.first());
		queueModel.removeTssEndpoint(ssi.getKey.getEndpoint().token.first());
		queueModel.removeTssEndpoint(ssi.getKeyValues.getEndpoint().token.first());
		queueModel.removeTssEndpoint(ssi.getMappedKeyValues.getEndpoint().token.first());
		queueModel.removeTssEndpoint(ssi.getKeyValuesStream.getEndpoint().token.first());

		queueModel.removeTssEndpoint(ssi.watchValue.getEndpoint().token.first());
		queueModel.removeTssEndpoint(ssi.splitMetrics.getEndpoint().token.first());
		queueModel.removeTssEndpoint(ssi.getReadHotRanges.getEndpoint().token.first());
		queueModel.removeTssEndpoint(ssi.getRangeSplitPoints.getEndpoint().token.first());
	}
}

void DatabaseContext::addSSIdTagMapping(const UID& uid, const Tag& tag) {
	ssidTagMapping[uid] = tag;
}

void DatabaseContext::getLatestCommitVersionForSSID(const UID& ssid, Tag& tag, Version& commitVersion) {
	// initialization
	tag = invalidTag;
	commitVersion = invalidVersion;

	auto iter = ssidTagMapping.find(ssid);
	if (iter != ssidTagMapping.end()) {
		tag = iter->second;

		if (ssVersionVectorCache.hasVersion(tag)) {
			commitVersion = ssVersionVectorCache.getVersion(tag);
		}
	}
}

void DatabaseContext::getLatestCommitVersion(const StorageServerInterface& ssi,
                                             Version readVersion,
                                             VersionVector& latestCommitVersion) {
	latestCommitVersion.clear();

	if (ssVersionVectorCache.getMaxVersion() == invalidVersion) {
		return;
	}

	// Error checking (based on the assumption that the read version was not obtained
	// from the client's grv cache).
	if (readVersion > ssVersionVectorCache.getMaxVersion()) {
		TraceEvent(SevError, "ReadVersionExceedsVersionVectorMax")
		    .detail("ReadVersion", readVersion)
		    .detail("VersionVector", ssVersionVectorCache.toString());
		if (g_network->isSimulated()) {
			ASSERT(false);
		} else {
			return; // Do not return a stale commit version in production.
		}
	}

	Tag tag = invalidTag;
	Version commitVersion = invalidVersion;
	getLatestCommitVersionForSSID(ssi.id(), tag, commitVersion);

	if (tag != invalidTag && commitVersion != invalidVersion && commitVersion < readVersion) {
		latestCommitVersion.setVersion(tag, commitVersion);
	}
}

void DatabaseContext::getLatestCommitVersions(const Reference<LocationInfo>& locationInfo,
                                              Reference<TransactionState> info,
                                              VersionVector& latestCommitVersions) {
	latestCommitVersions.clear();

	if (info->readOptions.present() && info->readOptions.get().debugID.present()) {
		g_traceBatch.addEvent(
		    "TransactionDebug", info->readOptions.get().debugID.get().first(), "NativeAPI.getLatestCommitVersions");
	}

	if (!info->readVersionObtainedFromGrvProxy) {
		return;
	}

	if (ssVersionVectorCache.getMaxVersion() == invalidVersion) {
		return;
	}

	if (info->readVersion() > ssVersionVectorCache.getMaxVersion()) {
		if (!CLIENT_KNOBS->FORCE_GRV_CACHE_OFF && !info->options.skipGrvCache && info->options.useGrvCache) {
			return;
		} else {
			TraceEvent(SevError, "GetLatestCommitVersions")
			    .detail("ReadVersion", info->readVersion())
			    .detail("VersionVector", ssVersionVectorCache.toString());
			ASSERT(false);
		}
	}

	std::map<Version, std::set<Tag>> versionMap; // order the versions to be returned
	for (int i = 0; i < locationInfo->locations()->size(); i++) {
		Tag tag = invalidTag;
		Version commitVersion = invalidVersion; // latest commit version
		getLatestCommitVersionForSSID(locationInfo->locations()->getId(i), tag, commitVersion);

		bool updatedVersionMap = false;
		if (tag != invalidTag && commitVersion != invalidVersion && commitVersion < info->readVersion()) {
			updatedVersionMap = true;
			versionMap[commitVersion].insert(tag);
		}

		// Do not log if commitVersion >= readVersion.
		if (!updatedVersionMap && commitVersion == invalidVersion) {
			TraceEvent(SevDebug, "CommitVersionNotFoundForSS")
			    .detail("InSSIDMap", tag != invalidTag ? 1 : 0)
			    .detail("Tag", tag)
			    .detail("CommitVersion", commitVersion)
			    .detail("ReadVersion", info->readVersion())
			    .detail("VersionVector", ssVersionVectorCache.toString())
			    .setMaxEventLength(11000)
			    .setMaxFieldLength(10000);
			++transactionCommitVersionNotFoundForSS;
		}
	}

	// insert the commit versions in the version vector.
	for (auto& iter : versionMap) {
		latestCommitVersions.setVersion(iter.second, iter.first);
	}
}

void updateCachedReadVersionShared(double t, Version v, DatabaseSharedState* p) {
	MutexHolder mutex(p->mutexLock);
	if (v >= p->grvCacheSpace.cachedReadVersion) {
		//TraceEvent(SevDebug, "CacheReadVersionUpdate")
		//    .detail("Version", v)
		//    .detail("CurTime", t)
		//    .detail("LastVersion", p->grvCacheSpace.cachedReadVersion)
		//    .detail("LastTime", p->grvCacheSpace.lastGrvTime);
		p->grvCacheSpace.cachedReadVersion = v;
		if (t > p->grvCacheSpace.lastGrvTime) {
			p->grvCacheSpace.lastGrvTime = t;
		}
	}
}

void DatabaseContext::updateCachedReadVersion(double t, Version v) {
	if (sharedStatePtr) {
		return updateCachedReadVersionShared(t, v, sharedStatePtr);
	}
	if (v >= cachedReadVersion) {
		//TraceEvent(SevDebug, "CachedReadVersionUpdate")
		//    .detail("Version", v)
		//    .detail("GrvStartTime", t)
		//    .detail("LastVersion", cachedReadVersion)
		//    .detail("LastTime", lastGrvTime);
		cachedReadVersion = v;
		// Since the time is based on the start of the request, it's possible that we
		// get a newer version with an older time.
		// (Request started earlier, but was latest to reach the proxy)
		// Only update time when strictly increasing (?)
		if (t > lastGrvTime) {
			lastGrvTime = t;
		}
	}
}

Version DatabaseContext::getCachedReadVersion() {
	if (sharedStatePtr) {
		MutexHolder mutex(sharedStatePtr->mutexLock);
		return sharedStatePtr->grvCacheSpace.cachedReadVersion;
	}
	return cachedReadVersion;
}

double DatabaseContext::getLastGrvTime() {
	if (sharedStatePtr) {
		MutexHolder mutex(sharedStatePtr->mutexLock);
		return sharedStatePtr->grvCacheSpace.lastGrvTime;
	}
	return lastGrvTime;
}

Reference<StorageServerInfo> StorageServerInfo::getInterface(DatabaseContext* cx,
                                                             StorageServerInterface const& ssi,
                                                             LocalityData const& locality) {
	auto it = cx->server_interf.find(ssi.id());
	if (it != cx->server_interf.end()) {
		if (it->second->interf.getValue.getEndpoint().token != ssi.getValue.getEndpoint().token) {
			if (it->second->interf.locality == ssi.locality) {
				// FIXME: load balance holds pointers to individual members of the interface, and this assignment will
				// swap out the object they are
				//       pointing to. This is technically correct, but is very unnatural. We may want to refactor load
				//       balance to take an AsyncVar<Reference<Interface>> so that it is notified when the interface
				//       changes.

				it->second->interf = ssi;
			} else {
				it->second->notifyContextDestroyed();
				Reference<StorageServerInfo> loc(new StorageServerInfo(cx, ssi, locality));
				cx->server_interf[ssi.id()] = loc.getPtr();
				return loc;
			}
		}

		return Reference<StorageServerInfo>::addRef(it->second);
	}

	Reference<StorageServerInfo> loc(new StorageServerInfo(cx, ssi, locality));
	cx->server_interf[ssi.id()] = loc.getPtr();
	return loc;
}

void StorageServerInfo::notifyContextDestroyed() {
	cx = nullptr;
}

StorageServerInfo::~StorageServerInfo() {
	if (cx) {
		auto it = cx->server_interf.find(interf.id());
		if (it != cx->server_interf.end())
			cx->server_interf.erase(it);
		cx = nullptr;
	}
}

void DatabaseContext::validateVersion(Version version) const {
	// Version could be 0 if the INITIALIZE_NEW_DATABASE option is set. In that case, it is illegal to perform any
	// reads. We throw client_invalid_operation because the caller didn't directly set the version, so the
	// version_invalid error might be confusing.
	if (version == 0) {
		throw client_invalid_operation();
	}
	if (switchable && version < minAcceptableReadVersion) {
		CODE_PROBE(true, "Attempted to read a version lower than any this client has seen from the current cluster");
		throw transaction_too_old();
	}

	ASSERT(version > 0 || version == latestVersion);
}

inline HealthMetrics populateHealthMetrics(const HealthMetrics& detailedMetrics, bool detailedOutput) {
	if (detailedOutput) {
		return detailedMetrics;
	} else {
		HealthMetrics result;
		result.update(detailedMetrics, false, false);
		return result;
	}
}

ACTOR static Future<HealthMetrics> getHealthMetricsActor(DatabaseContext* cx, bool detailed, bool sendDetailedRequest) {
	loop {
		choose {
			when(wait(cx->onProxiesChanged())) {}
			when(GetHealthMetricsReply rep = wait(basicLoadBalance(cx->getGrvProxies(UseProvisionalProxies::False),
			                                                       &GrvProxyInterface::getHealthMetrics,
			                                                       GetHealthMetricsRequest(sendDetailedRequest)))) {
				cx->healthMetrics.update(rep.healthMetrics, sendDetailedRequest, true);
				cx->healthMetricsLastUpdated = now();
				if (sendDetailedRequest) {
					cx->detailedHealthMetricsLastUpdated = now();
				}
				return populateHealthMetrics(cx->healthMetrics, detailed);
			}
		}
	}
}

Future<HealthMetrics> DatabaseContext::getHealthMetrics(bool detailed = false) {
	if (now() - healthMetricsLastUpdated < CLIENT_KNOBS->AGGREGATE_HEALTH_METRICS_MAX_STALENESS) {
		return populateHealthMetrics(healthMetrics, detailed);
	}
	bool sendDetailedRequest =
	    detailed && now() - detailedHealthMetricsLastUpdated > CLIENT_KNOBS->DETAILED_HEALTH_METRICS_MAX_STALENESS;
	return getHealthMetricsActor(this, detailed, sendDetailedRequest);
}

Future<Optional<HealthMetrics::StorageStats>> DatabaseContext::getStorageStats(const UID& id, double maxStaleness) {
	if (now() - detailedHealthMetricsLastUpdated < maxStaleness) {
		auto it = healthMetrics.storageStats.find(id);
		return it == healthMetrics.storageStats.end() ? Optional<HealthMetrics::StorageStats>() : it->second;
	}

	return map(getHealthMetricsActor(this, true, true), [&id](auto metrics) -> Optional<HealthMetrics::StorageStats> {
		auto it = metrics.storageStats.find(id);
		return it == metrics.storageStats.end() ? Optional<HealthMetrics::StorageStats>() : it->second;
	});
}

// register a special key(s) implementation under the specified module
void DatabaseContext::registerSpecialKeysImpl(SpecialKeySpace::MODULE module,
                                              SpecialKeySpace::IMPLTYPE type,
                                              std::unique_ptr<SpecialKeyRangeReadImpl>&& impl,
                                              int deprecatedVersion) {
	// if deprecated, add the implementation when the api version is less than the deprecated version
	if (deprecatedVersion == -1 || apiVersion.version() < deprecatedVersion) {
		specialKeySpace->registerKeyRange(module, type, impl->getKeyRange(), impl.get());
		specialKeySpaceModules.push_back(std::move(impl));
	}
}

void traceTSSErrors(const char* name, UID tssId, const std::unordered_map<int, uint64_t>& errorsByCode) {
	TraceEvent ev(name, tssId);
	for (auto& it : errorsByCode) {
		ev.detail("E" + std::to_string(it.first), it.second);
	}
}

/*
    For each request type, this will produce
    <Type>Count
    <Type>{SS,TSS}{Mean,P50,P90,P99}
    Example:
    GetValueLatencySSMean
*/
void traceSSOrTSSPercentiles(TraceEvent& ev, const std::string name, DDSketch<double>& sample) {
	ev.detail(name + "Mean", sample.mean());
	// don't log the larger percentiles unless we actually have enough samples to log the accurate percentile instead of
	// the largest sample in this window
	if (sample.getPopulationSize() >= 3) {
		ev.detail(name + "P50", sample.median());
	}
	if (sample.getPopulationSize() >= 10) {
		ev.detail(name + "P90", sample.percentile(0.90));
	}
	if (sample.getPopulationSize() >= 100) {
		ev.detail(name + "P99", sample.percentile(0.99));
	}
}

void traceTSSPercentiles(TraceEvent& ev,
                         const std::string name,
                         DDSketch<double>& ssSample,
                         DDSketch<double>& tssSample) {
	ASSERT(ssSample.getPopulationSize() == tssSample.getPopulationSize());
	ev.detail(name + "Count", ssSample.getPopulationSize());
	if (ssSample.getPopulationSize() > 0) {
		traceSSOrTSSPercentiles(ev, name + "SS", ssSample);
		traceSSOrTSSPercentiles(ev, name + "TSS", tssSample);
	}
}

ACTOR Future<Void> tssLogger(DatabaseContext* cx) {
	state double lastLogged = 0;
	loop {
		wait(delay(CLIENT_KNOBS->TSS_METRICS_LOGGING_INTERVAL, TaskPriority::FlushTrace));

		// Log each TSS pair separately
		for (const auto& it : cx->tssMetrics) {
			if (it.second->detailedMismatches.size()) {
				cx->tssMismatchStream.send(
				    std::pair<UID, std::vector<DetailedTSSMismatch>>(it.first, it.second->detailedMismatches));
			}

			// Do error histograms as separate event
			if (it.second->ssErrorsByCode.size()) {
				traceTSSErrors("TSS_SSErrors", it.first, it.second->ssErrorsByCode);
			}

			if (it.second->tssErrorsByCode.size()) {
				traceTSSErrors("TSS_TSSErrors", it.first, it.second->tssErrorsByCode);
			}

			TraceEvent tssEv("TSSClientMetrics", cx->dbId);
			tssEv.detail("TSSID", it.first)
			    .detail("Elapsed", (lastLogged == 0) ? 0 : now() - lastLogged)
			    .detail("Internal", cx->internal);

			it.second->cc.logToTraceEvent(tssEv);

			traceTSSPercentiles(tssEv, "GetValueLatency", it.second->SSgetValueLatency, it.second->TSSgetValueLatency);
			traceTSSPercentiles(
			    tssEv, "GetKeyValuesLatency", it.second->SSgetKeyValuesLatency, it.second->TSSgetKeyValuesLatency);
			traceTSSPercentiles(tssEv, "GetKeyLatency", it.second->SSgetKeyLatency, it.second->TSSgetKeyLatency);
			traceTSSPercentiles(tssEv,
			                    "GetMappedKeyValuesLatency",
			                    it.second->SSgetMappedKeyValuesLatency,
			                    it.second->TSSgetMappedKeyValuesLatency);

			it.second->clear();
		}

		lastLogged = now();
	}
}

ACTOR Future<Void> databaseLogger(DatabaseContext* cx) {
	state double lastLogged = 0;
	loop {
		wait(delay(CLIENT_KNOBS->SYSTEM_MONITOR_INTERVAL, TaskPriority::FlushTrace));

		bool logTraces = !g_network->isSimulated() || BUGGIFY_WITH_PROB(0.01);
		if (logTraces) {
			TraceEvent ev("TransactionMetrics", cx->dbId);

			ev.detail("Elapsed", (lastLogged == 0) ? 0 : now() - lastLogged)
			    .detail("Cluster",
			            cx->getConnectionRecord()
			                ? cx->getConnectionRecord()->getConnectionString().clusterKeyName().toString()
			                : "")
			    .detail("Internal", cx->internal);

			cx->cc.logToTraceEvent(ev);

			ev.detail("LocationCacheEntryCount", cx->locationCache.size());
			ev.detail("MeanLatency", cx->latencies.mean())
			    .detail("MedianLatency", cx->latencies.median())
			    .detail("Latency90", cx->latencies.percentile(0.90))
			    .detail("Latency98", cx->latencies.percentile(0.98))
			    .detail("MaxLatency", cx->latencies.max())
			    .detail("MeanRowReadLatency", cx->readLatencies.mean())
			    .detail("MedianRowReadLatency", cx->readLatencies.median())
			    .detail("MaxRowReadLatency", cx->readLatencies.max())
			    .detail("MeanGRVLatency", cx->GRVLatencies.mean())
			    .detail("MedianGRVLatency", cx->GRVLatencies.median())
			    .detail("MaxGRVLatency", cx->GRVLatencies.max())
			    .detail("MeanCommitLatency", cx->commitLatencies.mean())
			    .detail("MedianCommitLatency", cx->commitLatencies.median())
			    .detail("MaxCommitLatency", cx->commitLatencies.max())
			    .detail("MeanMutationsPerCommit", cx->mutationsPerCommit.mean())
			    .detail("MedianMutationsPerCommit", cx->mutationsPerCommit.median())
			    .detail("MaxMutationsPerCommit", cx->mutationsPerCommit.max())
			    .detail("MeanBytesPerCommit", cx->bytesPerCommit.mean())
			    .detail("MedianBytesPerCommit", cx->bytesPerCommit.median())
			    .detail("MaxBytesPerCommit", cx->bytesPerCommit.max())
			    .detail("NumLocalityCacheEntries", cx->locationCache.size());
		}

		if (cx->usedAnyChangeFeeds && logTraces) {
			TraceEvent feedEv("ChangeFeedClientMetrics", cx->dbId);

			feedEv.detail("Elapsed", (lastLogged == 0) ? 0 : now() - lastLogged)
			    .detail("Cluster",
			            cx->getConnectionRecord()
			                ? cx->getConnectionRecord()->getConnectionString().clusterKeyName().toString()
			                : "")
			    .detail("Internal", cx->internal);

			cx->ccFeed.logToTraceEvent(feedEv);
		}

		if (cx->anyBGReads && logTraces) {
			TraceEvent bgReadEv("BlobGranuleReadMetrics", cx->dbId);

			bgReadEv.detail("Elapsed", (lastLogged == 0) ? 0 : now() - lastLogged)
			    .detail("Cluster",
			            cx->getConnectionRecord()
			                ? cx->getConnectionRecord()->getConnectionString().clusterKeyName().toString()
			                : "")
			    .detail("Internal", cx->internal);

			// add counters
			cx->ccBG.logToTraceEvent(bgReadEv);

			// add latencies
			bgReadEv.detail("MeanBGLatency", cx->bgLatencies.mean())
			    .detail("MedianBGLatency", cx->bgLatencies.median())
			    .detail("MaxBGLatency", cx->bgLatencies.max())
			    .detail("MeanBGGranulesPerRequest", cx->bgGranulesPerRequest.mean())
			    .detail("MedianBGGranulesPerRequest", cx->bgGranulesPerRequest.median())
			    .detail("MaxBGGranulesPerRequest", cx->bgGranulesPerRequest.max());
		}

		cx->latencies.clear();
		cx->readLatencies.clear();
		cx->GRVLatencies.clear();
		cx->commitLatencies.clear();
		cx->mutationsPerCommit.clear();
		cx->bytesPerCommit.clear();
		cx->bgLatencies.clear();
		cx->bgGranulesPerRequest.clear();

		lastLogged = now();
	}
}

struct TrInfoChunk {
	ValueRef value;
	Key key;
};

static const Key CLIENT_LATENCY_INFO_PREFIX = "client_latency/"_sr;
static const Key CLIENT_LATENCY_INFO_CTR_PREFIX = "client_latency_counter/"_sr;

ACTOR static Future<Void> transactionInfoCommitActor(Transaction* tr, std::vector<TrInfoChunk>* chunks) {
	state const Key clientLatencyAtomicCtr = CLIENT_LATENCY_INFO_CTR_PREFIX.withPrefix(fdbClientInfoPrefixRange.begin);
	state int retryCount = 0;
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			state Future<Standalone<StringRef>> vstamp = tr->getVersionstamp();
			int64_t numCommitBytes = 0;
			for (auto& chunk : *chunks) {
				tr->atomicOp(chunk.key, chunk.value, MutationRef::SetVersionstampedKey);
				numCommitBytes += chunk.key.size() + chunk.value.size() -
				                  4; // subtract number of bytes of key that denotes version stamp index
			}
			tr->atomicOp(clientLatencyAtomicCtr, StringRef((uint8_t*)&numCommitBytes, 8), MutationRef::AddValue);
			wait(tr->commit());
			return Void();
		} catch (Error& e) {
			retryCount++;
			if (retryCount == 10)
				throw;
			wait(tr->onError(e));
		}
	}
}

ACTOR static Future<Void> delExcessClntTxnEntriesActor(Transaction* tr, int64_t clientTxInfoSizeLimit) {
	state const Key clientLatencyName = CLIENT_LATENCY_INFO_PREFIX.withPrefix(fdbClientInfoPrefixRange.begin);
	state const Key clientLatencyAtomicCtr = CLIENT_LATENCY_INFO_CTR_PREFIX.withPrefix(fdbClientInfoPrefixRange.begin);
	TraceEvent(SevInfo, "DelExcessClntTxnEntriesCalled").log();

	// If we don't limit it with retries, the DatabaseContext will never cleanup as Transaction
	// object will be alive and hold reference to DatabaseContext.
	state int retries = 0;
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> ctrValue = wait(tr->get(KeyRef(clientLatencyAtomicCtr), Snapshot::True));
			if (!ctrValue.present()) {
				TraceEvent(SevInfo, "NumClntTxnEntriesNotFound").log();
				return Void();
			}
			state int64_t txInfoSize = 0;
			ASSERT(ctrValue.get().size() == sizeof(int64_t));
			memcpy(&txInfoSize, ctrValue.get().begin(), ctrValue.get().size());
			if (txInfoSize < clientTxInfoSizeLimit)
				return Void();
			int getRangeByteLimit = (txInfoSize - clientTxInfoSizeLimit) < CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT
			                            ? (txInfoSize - clientTxInfoSizeLimit)
			                            : CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT;
			GetRangeLimits limit(GetRangeLimits::ROW_LIMIT_UNLIMITED, getRangeByteLimit);
			RangeResult txEntries =
			    wait(tr->getRange(KeyRangeRef(clientLatencyName, strinc(clientLatencyName)), limit));
			state int64_t numBytesToDel = 0;
			KeyRef endKey;
			for (auto& kv : txEntries) {
				endKey = kv.key;
				numBytesToDel += kv.key.size() + kv.value.size();
				if (txInfoSize - numBytesToDel <= clientTxInfoSizeLimit)
					break;
			}
			if (numBytesToDel) {
				tr->clear(KeyRangeRef(txEntries[0].key, strinc(endKey)));
				TraceEvent(SevInfo, "DeletingExcessCntTxnEntries").detail("BytesToBeDeleted", numBytesToDel);
				int64_t bytesDel = -numBytesToDel;

				tr->atomicOp(clientLatencyAtomicCtr, StringRef((uint8_t*)&bytesDel, 8), MutationRef::AddValue);
				wait(tr->commit());
			}
			if (txInfoSize - numBytesToDel <= clientTxInfoSizeLimit)
				return Void();
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled || retries++ >= 10) {
				throw;
			}

			wait(tr->onError(e));
		}
	}
}

// The reason for getting a pointer to DatabaseContext instead of a reference counted object is because reference
// counting will increment reference count for DatabaseContext which holds the future of this actor. This creates a
// cyclic reference and hence this actor and Database object will not be destroyed at all.
ACTOR static Future<Void> clientStatusUpdateActor(DatabaseContext* cx) {
	state const std::string clientLatencyName =
	    CLIENT_LATENCY_INFO_PREFIX.withPrefix(fdbClientInfoPrefixRange.begin).toString();
	state Transaction tr;
	state std::vector<TrInfoChunk> commitQ;
	state int txBytes = 0;

	loop {
		// Make sure we are connected to the server. Otherwise we may just try to keep reconnecting
		// with incompatible clusters.
		wait(cx->onConnected());

		// Need to make sure that we eventually destroy tr. We can't rely on getting cancelled to do
		// this because of the cyclic reference to self.
		wait(refreshTransaction(cx, &tr));
		try {
			ASSERT(cx->clientStatusUpdater.outStatusQ.empty());
			cx->clientStatusUpdater.inStatusQ.swap(cx->clientStatusUpdater.outStatusQ);
			// Split Transaction Info into chunks
			state std::vector<TrInfoChunk> trChunksQ;
			for (auto& entry : cx->clientStatusUpdater.outStatusQ) {
				auto& bw = entry.second;
				int64_t value_size_limit = BUGGIFY
				                               ? deterministicRandom()->randomInt(1e3, CLIENT_KNOBS->VALUE_SIZE_LIMIT)
				                               : CLIENT_KNOBS->VALUE_SIZE_LIMIT;
				int num_chunks = (bw.getLength() + value_size_limit - 1) / value_size_limit;
				std::string random_id = deterministicRandom()->randomAlphaNumeric(16);
				std::string user_provided_id = entry.first.size() ? entry.first + "/" : "";
				for (int i = 0; i < num_chunks; i++) {
					TrInfoChunk chunk;
					BinaryWriter chunkBW(Unversioned());
					chunkBW << bigEndian32(i + 1) << bigEndian32(num_chunks);
					chunk.key = KeyRef(clientLatencyName + std::string(10, '\x00') + "/" + random_id + "/" +
					                   chunkBW.toValue().toString() + "/" + user_provided_id + std::string(4, '\x00'));
					int32_t pos = littleEndian32(clientLatencyName.size());
					memcpy(mutateString(chunk.key) + chunk.key.size() - sizeof(int32_t), &pos, sizeof(int32_t));
					if (i == num_chunks - 1) {
						chunk.value = ValueRef(static_cast<uint8_t*>(bw.getData()) + (i * value_size_limit),
						                       bw.getLength() - (i * value_size_limit));
					} else {
						chunk.value =
						    ValueRef(static_cast<uint8_t*>(bw.getData()) + (i * value_size_limit), value_size_limit);
					}
					trChunksQ.push_back(std::move(chunk));
				}
			}

			// Commit the chunks splitting into different transactions if needed
			state int64_t dataSizeLimit =
			    BUGGIFY ? deterministicRandom()->randomInt(200e3, 1.5 * CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT)
			            : 0.8 * CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT;
			state std::vector<TrInfoChunk>::iterator tracking_iter = trChunksQ.begin();
			ASSERT(commitQ.empty() && (txBytes == 0));
			loop {
				state std::vector<TrInfoChunk>::iterator iter = tracking_iter;
				txBytes = 0;
				commitQ.clear();
				try {
					while (iter != trChunksQ.end()) {
						if (iter->value.size() + iter->key.size() + txBytes > dataSizeLimit) {
							wait(transactionInfoCommitActor(&tr, &commitQ));
							tracking_iter = iter;
							commitQ.clear();
							txBytes = 0;
						}
						commitQ.push_back(*iter);
						txBytes += iter->value.size() + iter->key.size();
						++iter;
					}
					if (!commitQ.empty()) {
						wait(transactionInfoCommitActor(&tr, &commitQ));
						commitQ.clear();
						txBytes = 0;
					}
					break;
				} catch (Error& e) {
					if (e.code() == error_code_transaction_too_large) {
						dataSizeLimit /= 2;
						ASSERT(dataSizeLimit >= CLIENT_KNOBS->VALUE_SIZE_LIMIT + CLIENT_KNOBS->KEY_SIZE_LIMIT);
					} else {
						TraceEvent(SevWarnAlways, "ClientTrInfoErrorCommit").error(e).detail("TxBytes", txBytes);
						commitQ.clear();
						txBytes = 0;
						throw;
					}
				}
			}
			cx->clientStatusUpdater.outStatusQ.clear();
			wait(cx->globalConfig->onInitialized());
			double sampleRate =
			    cx->globalConfig->get<double>(fdbClientInfoTxnSampleRate, std::numeric_limits<double>::infinity());
			double clientSamplingProbability =
			    std::isinf(sampleRate) ? CLIENT_KNOBS->CSI_SAMPLING_PROBABILITY : sampleRate;
			int64_t sizeLimit = cx->globalConfig->get<int64_t>(fdbClientInfoTxnSizeLimit, -1);
			int64_t clientTxnInfoSizeLimit = sizeLimit == -1 ? CLIENT_KNOBS->CSI_SIZE_LIMIT : sizeLimit;
			if (!trChunksQ.empty() && deterministicRandom()->random01() < clientSamplingProbability)
				wait(delExcessClntTxnEntriesActor(&tr, clientTxnInfoSizeLimit));

			// Cleanup Transaction sooner than later, so that we don't hold reference to context.
			tr = Transaction();
			wait(delay(CLIENT_KNOBS->CSI_STATUS_DELAY));
		} catch (Error& e) {
			TraceEvent(SevWarnAlways, "UnableToWriteClientStatus").error(e);
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			cx->clientStatusUpdater.outStatusQ.clear();

			// Cleanup Transaction sooner than later, so that we don't hold reference to context.
			tr = Transaction();
			wait(delay(10.0));
		}
	}
}

ACTOR Future<Void> assertFailure(GrvProxyInterface remote, Future<ErrorOr<GetReadVersionReply>> reply) {
	try {
		ErrorOr<GetReadVersionReply> res = wait(reply);
		if (!res.isError()) {
			TraceEvent(SevError, "GotStaleReadVersion")
			    .detail("Remote", remote.getConsistentReadVersion.getEndpoint().addresses.address.toString())
			    .detail("Provisional", remote.provisional)
			    .detail("ReadVersion", res.get().version);
			ASSERT_WE_THINK(false);
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		// we want this to fail -- so getting here is good, we'll just ignore the error.
	}
	return Void();
}

Future<Void> attemptGRVFromOldProxies(std::vector<GrvProxyInterface> oldProxies,
                                      std::vector<GrvProxyInterface> newProxies) {
	auto debugID = nondeterministicRandom()->randomUniqueID();
	g_traceBatch.addEvent("AttemptGRVFromOldProxyDebug", debugID.first(), "NativeAPI.attemptGRVFromOldProxies.Start");
	Span span("NAPI:VerifyCausalReadRisky"_loc);
	std::vector<Future<Void>> replies;
	replies.reserve(oldProxies.size());
	GetReadVersionRequest req(
	    span.context, 1, TransactionPriority::IMMEDIATE, GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY);
	TraceEvent evt("AttemptGRVFromOldProxies");
	evt.detail("NumOldProxies", oldProxies.size()).detail("NumNewProxies", newProxies.size());
	auto traceProxies = [&](std::vector<GrvProxyInterface>& proxies, std::string const& key) {
		for (int i = 0; i < proxies.size(); ++i) {
			auto k = key + std::to_string(i);
			evt.detail(k.c_str(), proxies[i].id());
		}
	};
	traceProxies(oldProxies, "OldProxy"s);
	traceProxies(newProxies, "NewProxy"s);
	evt.log();
	for (auto& i : oldProxies) {
		req.reply = ReplyPromise<GetReadVersionReply>();
		replies.push_back(assertFailure(i, i.getConsistentReadVersion.tryGetReply(req)));
	}
	return waitForAll(replies);
}

ACTOR static Future<Void> monitorClientDBInfoChange(DatabaseContext* cx,
                                                    Reference<AsyncVar<ClientDBInfo> const> clientDBInfo,
                                                    AsyncTrigger* proxiesChangeTrigger) {
	state std::vector<CommitProxyInterface> curCommitProxies;
	state std::vector<GrvProxyInterface> curGrvProxies;
	state ActorCollection actors(false);
	state Future<Void> clientDBInfoOnChange = clientDBInfo->onChange();
	curCommitProxies = clientDBInfo->get().commitProxies;
	curGrvProxies = clientDBInfo->get().grvProxies;

	loop {
		choose {
			when(wait(clientDBInfoOnChange)) {
				clientDBInfoOnChange = clientDBInfo->onChange();
				if (clientDBInfo->get().commitProxies != curCommitProxies ||
				    clientDBInfo->get().grvProxies != curGrvProxies) {
					// This condition is a bit complicated. Here we want to verify that we're unable to receive a read
					// version from a proxy of an old generation after a successful recovery. The conditions are:
					// 1. We only do this with a configured probability.
					// 2. If the old set of Grv proxies is empty, there's nothing to do
					// 3. If the new set of Grv proxies is empty, it means the recovery is not complete. So if an old
					//    Grv proxy still gives out read versions, this would be correct behavior.
					// 4. If we see a provisional proxy, it means the recovery didn't complete yet, so the same as (3)
					//    applies.
					if (deterministicRandom()->random01() < cx->verifyCausalReadsProp && !curGrvProxies.empty() &&
					    !clientDBInfo->get().grvProxies.empty() && !clientDBInfo->get().grvProxies[0].provisional) {
						actors.add(attemptGRVFromOldProxies(curGrvProxies, clientDBInfo->get().grvProxies));
					}
					curCommitProxies = clientDBInfo->get().commitProxies;
					curGrvProxies = clientDBInfo->get().grvProxies;
					// Commits in the previous epoch may have been recovered but not included in the version vector.
					// Clear the version vector to ensure the latest commit versions are received.
					cx->ssVersionVectorCache.clear();
					proxiesChangeTrigger->trigger();
				}
			}
			when(wait(actors.getResult())) {
				UNSTOPPABLE_ASSERT(false);
			}
		}
	}
}

void updateLocationCacheWithCaches(DatabaseContext* self,
                                   const std::map<UID, StorageServerInterface>& removed,
                                   const std::map<UID, StorageServerInterface>& added) {
	// TODO: this needs to be more clever in the future
	auto ranges = self->locationCache.ranges();
	for (auto iter = ranges.begin(); iter != ranges.end(); ++iter) {
		if (iter->value() && iter->value()->hasCaches) {
			auto& val = iter->value();
			std::vector<Reference<ReferencedInterface<StorageServerInterface>>> interfaces;
			interfaces.reserve(val->size() - removed.size() + added.size());
			for (int i = 0; i < val->size(); ++i) {
				const auto& interf = (*val)[i];
				if (removed.count(interf->interf.id()) == 0) {
					interfaces.emplace_back(interf);
				}
			}
			for (const auto& p : added) {
				interfaces.push_back(makeReference<ReferencedInterface<StorageServerInterface>>(p.second));
			}
			iter->value() = makeReference<LocationInfo>(interfaces, true);
		}
	}
}

Reference<LocationInfo> addCaches(const Reference<LocationInfo>& loc,
                                  const std::vector<Reference<ReferencedInterface<StorageServerInterface>>>& other) {
	std::vector<Reference<ReferencedInterface<StorageServerInterface>>> interfaces;
	interfaces.reserve(loc->size() + other.size());
	for (int i = 0; i < loc->size(); ++i) {
		interfaces.emplace_back((*loc)[i]);
	}
	interfaces.insert(interfaces.end(), other.begin(), other.end());
	return makeReference<LocationInfo>(interfaces, true);
}

ACTOR Future<Void> updateCachedRanges(DatabaseContext* self, std::map<UID, StorageServerInterface>* cacheServers) {
	state Transaction tr;
	state Value trueValue = storageCacheValue(std::vector<uint16_t>{ 0 });
	state Value falseValue = storageCacheValue(std::vector<uint16_t>{});
	try {
		loop {
			// Need to make sure that we eventually destroy tr. We can't rely on getting cancelled to do this because of
			// the cyclic reference to self.
			tr = Transaction();
			wait(delay(0)); // Give ourselves the chance to get cancelled if self was destroyed
			wait(brokenPromiseToNever(self->updateCache.onTrigger())); // brokenPromiseToNever because self might get
			                                                           // destroyed elsewhere while we're waiting here.
			tr = Transaction(Database(Reference<DatabaseContext>::addRef(self)));
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			try {
				RangeResult range = wait(tr.getRange(storageCacheKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!range.more);
				std::vector<Reference<ReferencedInterface<StorageServerInterface>>> cacheInterfaces;
				cacheInterfaces.reserve(cacheServers->size());
				for (const auto& p : *cacheServers) {
					cacheInterfaces.push_back(makeReference<ReferencedInterface<StorageServerInterface>>(p.second));
				}
				bool currCached = false;
				KeyRef begin, end;
				for (const auto& kv : range) {
					// These booleans have to flip consistently
					ASSERT(currCached == (kv.value == falseValue));
					if (kv.value == trueValue) {
						begin = kv.key.substr(storageCacheKeys.begin.size());
						currCached = true;
					} else {
						currCached = false;
						end = kv.key.substr(storageCacheKeys.begin.size());
						KeyRangeRef cachedRange{ begin, end };
						auto ranges = self->locationCache.containedRanges(cachedRange);
						KeyRef containedRangesBegin, containedRangesEnd, prevKey;
						if (!ranges.empty()) {
							containedRangesBegin = ranges.begin().range().begin;
						}
						for (auto iter = ranges.begin(); iter != ranges.end(); ++iter) {
							containedRangesEnd = iter->range().end;
							if (iter->value() && !iter->value()->hasCaches) {
								iter->value() = addCaches(iter->value(), cacheInterfaces);
							}
						}
						auto iter = self->locationCache.rangeContaining(begin);
						if (iter->value() && !iter->value()->hasCaches) {
							if (end >= iter->range().end) {
								Key endCopy = iter->range().end; // Copy because insertion invalidates iterator
								self->locationCache.insert(KeyRangeRef{ begin, endCopy },
								                           addCaches(iter->value(), cacheInterfaces));
							} else {
								self->locationCache.insert(KeyRangeRef{ begin, end },
								                           addCaches(iter->value(), cacheInterfaces));
							}
						}
						iter = self->locationCache.rangeContainingKeyBefore(end);
						if (iter->value() && !iter->value()->hasCaches) {
							Key beginCopy = iter->range().begin; // Copy because insertion invalidates iterator
							self->locationCache.insert(KeyRangeRef{ beginCopy, end },
							                           addCaches(iter->value(), cacheInterfaces));
						}
					}
				}
				wait(delay(2.0)); // we want to wait at least some small amount of time before
				// updating this list again
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	} catch (Error& e) {
		TraceEvent(SevError, "UpdateCachedRangesFailed").error(e);
		throw;
	}
}

// The reason for getting a pointer to DatabaseContext instead of a reference counted object is because reference
// counting will increment reference count for DatabaseContext which holds the future of this actor. This creates a
// cyclic reference and hence this actor and Database object will not be destroyed at all.
ACTOR Future<Void> monitorCacheList(DatabaseContext* self) {
	state Transaction tr;
	state std::map<UID, StorageServerInterface> cacheServerMap;
	state Future<Void> updateRanges = updateCachedRanges(self, &cacheServerMap);
	state Backoff backoff;
	// if no caches are configured, we don't want to run this actor at all
	// so we just wait for the first trigger from a storage server
	wait(self->updateCache.onTrigger());
	try {
		loop {
			// Need to make sure that we eventually destroy tr. We can't rely on getting cancelled to do this because of
			// the cyclic reference to self.
			wait(refreshTransaction(self, &tr));
			try {
				RangeResult cacheList = wait(tr.getRange(storageCacheServerKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!cacheList.more);
				bool hasChanges = false;
				std::map<UID, StorageServerInterface> allCacheServers;
				for (auto kv : cacheList) {
					auto ssi = BinaryReader::fromStringRef<StorageServerInterface>(kv.value, IncludeVersion());
					allCacheServers.emplace(ssi.id(), ssi);
				}
				std::map<UID, StorageServerInterface> newCacheServers;
				std::map<UID, StorageServerInterface> deletedCacheServers;
				std::set_difference(allCacheServers.begin(),
				                    allCacheServers.end(),
				                    cacheServerMap.begin(),
				                    cacheServerMap.end(),
				                    std::insert_iterator<std::map<UID, StorageServerInterface>>(
				                        newCacheServers, newCacheServers.begin()));
				std::set_difference(cacheServerMap.begin(),
				                    cacheServerMap.end(),
				                    allCacheServers.begin(),
				                    allCacheServers.end(),
				                    std::insert_iterator<std::map<UID, StorageServerInterface>>(
				                        deletedCacheServers, deletedCacheServers.begin()));
				hasChanges = !(newCacheServers.empty() && deletedCacheServers.empty());
				if (hasChanges) {
					updateLocationCacheWithCaches(self, deletedCacheServers, newCacheServers);
				}
				cacheServerMap = std::move(allCacheServers);
				wait(delay(5.0));
				backoff = Backoff();
			} catch (Error& e) {
				wait(tr.onError(e));
				wait(backoff.onError());
			}
		}
	} catch (Error& e) {
		TraceEvent(SevError, "MonitorCacheListFailed").error(e);
		throw;
	}
}

ACTOR static Future<Void> handleTssMismatches(DatabaseContext* cx) {
	state Reference<ReadYourWritesTransaction> tr;
	state KeyBackedMap<UID, UID> tssMapDB = KeyBackedMap<UID, UID>(tssMappingKeys.begin);
	state KeyBackedMap<Tuple, std::string> tssMismatchDB = KeyBackedMap<Tuple, std::string>(tssMismatchKeys.begin);
	loop {
		// return to calling actor, cx might be destroyed already with the tr reset below.
		// This gives ~DatabaseContext a chance to cancel this actor.
		wait(delay(0));

		// <tssid, list of detailed mismatch data>
		state std::pair<UID, std::vector<DetailedTSSMismatch>> data = waitNext(cx->tssMismatchStream.getFuture());
		// find ss pair id so we can remove it from the mapping
		state UID tssPairID;
		bool found = false;
		for (const auto& it : cx->tssMapping) {
			if (it.second.id() == data.first) {
				tssPairID = it.first;
				found = true;
				break;
			}
		}
		if (found) {
			state bool quarantine = CLIENT_KNOBS->QUARANTINE_TSS_ON_MISMATCH;
			TraceEvent(SevWarnAlways, quarantine ? "TSS_QuarantineMismatch" : "TSS_KillMismatch")
			    .detail("TSSID", data.first.toString());
			CODE_PROBE(quarantine, "Quarantining TSS because it got mismatch");
			CODE_PROBE(!quarantine, "Killing TSS because it got mismatch");

			tr = makeReference<ReadYourWritesTransaction>(Database(Reference<DatabaseContext>::addRef(cx)));
			state int tries = 0;
			loop {
				try {
					tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					if (quarantine) {
						tr->set(tssQuarantineKeyFor(data.first), ""_sr);
					} else {
						tr->clear(serverTagKeyFor(data.first));
					}
					tssMapDB.erase(tr, tssPairID);

					for (const DetailedTSSMismatch& d : data.second) {
						// <tssid, time, mismatchid> -> mismatch data
						tssMismatchDB.set(tr,
						                  Tuple::makeTuple(data.first.toString(), d.timestamp, d.mismatchId.toString()),
						                  d.traceString);
					}

					wait(tr->commit());

					break;
				} catch (Error& e) {
					wait(tr->onError(e));
				}
				tries++;
				if (tries > 10) {
					// Give up, it'll get another mismatch or a human will investigate eventually
					TraceEvent("TSS_MismatchGaveUp").detail("TSSID", data.first.toString());
					break;
				}
			}
			// clear out txn so that the extra DatabaseContext ref gets decref'd and we can free cx
			tr = makeReference<ReadYourWritesTransaction>();
		} else {
			CODE_PROBE(true, "Not handling TSS with mismatch because it's already gone");
		}
	}
}

ACTOR Future<Optional<Value>> getJSON(Database db, std::string jsonField = "");

struct SingleSpecialKeyImpl : SpecialKeyRangeReadImpl {
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override {
		ASSERT(kr.contains(k));
		return map(f(ryw), [k = k](Optional<Value> v) {
			RangeResult result;
			if (v.present()) {
				result.push_back_deep(result.arena(), KeyValueRef(k, v.get()));
			}
			return result;
		});
	}

	SingleSpecialKeyImpl(KeyRef k,
	                     const std::function<Future<Optional<Value>>(ReadYourWritesTransaction*)>& f,
	                     bool supportsTenants = false)
	  : SpecialKeyRangeReadImpl(singleKeyRange(k)), k(k), f(f), tenantSupport(supportsTenants) {}

	bool supportsTenants() const override {
		CODE_PROBE(tenantSupport, "Single special key in tenant");
		return tenantSupport;
	};

private:
	Key k;
	std::function<Future<Optional<Value>>(ReadYourWritesTransaction*)> f;
	bool tenantSupport;
};

class HealthMetricsRangeImpl : public SpecialKeyRangeAsyncImpl {
public:
	explicit HealthMetricsRangeImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
};

static RangeResult healthMetricsToKVPairs(const HealthMetrics& metrics, KeyRangeRef kr) {
	RangeResult result;
	if (CLIENT_BUGGIFY)
		return result;
	if (kr.contains("\xff\xff/metrics/health/aggregate"_sr) && metrics.worstStorageDurabilityLag != 0) {
		json_spirit::mObject statsObj;
		statsObj["batch_limited"] = metrics.batchLimited;
		statsObj["tps_limit"] = metrics.tpsLimit;
		statsObj["worst_storage_durability_lag"] = metrics.worstStorageDurabilityLag;
		statsObj["limiting_storage_durability_lag"] = metrics.limitingStorageDurabilityLag;
		statsObj["worst_storage_queue"] = metrics.worstStorageQueue;
		statsObj["limiting_storage_queue"] = metrics.limitingStorageQueue;
		statsObj["worst_log_queue"] = metrics.worstTLogQueue;
		std::string statsString =
		    json_spirit::write_string(json_spirit::mValue(statsObj), json_spirit::Output_options::raw_utf8);
		ValueRef bytes(result.arena(), statsString);
		result.push_back(result.arena(), KeyValueRef("\xff\xff/metrics/health/aggregate"_sr, bytes));
	}
	// tlog stats
	{
		int phase = 0; // Avoid comparing twice per loop iteration
		for (const auto& [uid, logStats] : metrics.tLogQueue) {
			StringRef k{ StringRef(uid.toString()).withPrefix("\xff\xff/metrics/health/log/"_sr, result.arena()) };
			if (phase == 0 && k >= kr.begin) {
				phase = 1;
			}
			if (phase == 1) {
				if (k < kr.end) {
					json_spirit::mObject statsObj;
					statsObj["log_queue"] = logStats;
					std::string statsString =
					    json_spirit::write_string(json_spirit::mValue(statsObj), json_spirit::Output_options::raw_utf8);
					ValueRef bytes(result.arena(), statsString);
					result.push_back(result.arena(), KeyValueRef(k, bytes));
				} else {
					break;
				}
			}
		}
	}
	// Storage stats
	{
		int phase = 0; // Avoid comparing twice per loop iteration
		for (const auto& [uid, storageStats] : metrics.storageStats) {
			StringRef k{ StringRef(uid.toString()).withPrefix("\xff\xff/metrics/health/storage/"_sr, result.arena()) };
			if (phase == 0 && k >= kr.begin) {
				phase = 1;
			}
			if (phase == 1) {
				if (k < kr.end) {
					json_spirit::mObject statsObj;
					statsObj["storage_durability_lag"] = storageStats.storageDurabilityLag;
					statsObj["storage_queue"] = storageStats.storageQueue;
					statsObj["cpu_usage"] = storageStats.cpuUsage;
					statsObj["disk_usage"] = storageStats.diskUsage;
					std::string statsString =
					    json_spirit::write_string(json_spirit::mValue(statsObj), json_spirit::Output_options::raw_utf8);
					ValueRef bytes(result.arena(), statsString);
					result.push_back(result.arena(), KeyValueRef(k, bytes));
				} else {
					break;
				}
			}
		}
	}
	return result;
}

ACTOR static Future<RangeResult> healthMetricsGetRangeActor(ReadYourWritesTransaction* ryw, KeyRangeRef kr) {
	HealthMetrics metrics = wait(ryw->getDatabase()->getHealthMetrics(
	    /*detailed ("per process")*/ kr.intersects(
	        KeyRangeRef("\xff\xff/metrics/health/storage/"_sr, "\xff\xff/metrics/health/storage0"_sr)) ||
	    kr.intersects(KeyRangeRef("\xff\xff/metrics/health/log/"_sr, "\xff\xff/metrics/health/log0"_sr))));
	return healthMetricsToKVPairs(metrics, kr);
}

HealthMetricsRangeImpl::HealthMetricsRangeImpl(KeyRangeRef kr) : SpecialKeyRangeAsyncImpl(kr) {}

Future<RangeResult> HealthMetricsRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                     KeyRangeRef kr,
                                                     GetRangeLimits limitsHint) const {
	return healthMetricsGetRangeActor(ryw, kr);
}

ACTOR Future<UID> getClusterId(Database db) {
	while (!db->clientInfo->get().clusterId.isValid()) {
		wait(db->clientInfo->onChange());
	}
	return db->clientInfo->get().clusterId;
}

void DatabaseContext::initializeSpecialCounters() {
	specialCounter(cc, "OutstandingWatches", [this] { return outstandingWatches; });
	specialCounter(cc, "WatchMapSize", [this] { return watchMap.size(); });
}

DatabaseContext::DatabaseContext(Reference<AsyncVar<Reference<IClusterConnectionRecord>>> connectionRecord,
                                 Reference<AsyncVar<ClientDBInfo>> clientInfo,
                                 Reference<AsyncVar<Optional<ClientLeaderRegInterface>> const> coordinator,
                                 Future<Void> clientInfoMonitor,
                                 TaskPriority taskID,
                                 LocalityData const& clientLocality,
                                 EnableLocalityLoadBalance enableLocalityLoadBalance,
                                 LockAware lockAware,
                                 IsInternal internal,
                                 int _apiVersion,
                                 IsSwitchable switchable,
                                 Optional<TenantName> defaultTenant)
  : dbId(deterministicRandom()->randomUniqueID()), lockAware(lockAware), switchable(switchable),
    connectionRecord(connectionRecord), proxyProvisional(false), clientLocality(clientLocality),
    enableLocalityLoadBalance(enableLocalityLoadBalance), defaultTenant(defaultTenant), internal(internal),
    cc("TransactionMetrics", dbId.toString()), transactionReadVersions("ReadVersions", cc),
    transactionReadVersionsThrottled("ReadVersionsThrottled", cc),
    transactionReadVersionsCompleted("ReadVersionsCompleted", cc),
    transactionReadVersionBatches("ReadVersionBatches", cc),
    transactionBatchReadVersions("BatchPriorityReadVersions", cc),
    transactionDefaultReadVersions("DefaultPriorityReadVersions", cc),
    transactionImmediateReadVersions("ImmediatePriorityReadVersions", cc),
    transactionBatchReadVersionsCompleted("BatchPriorityReadVersionsCompleted", cc),
    transactionDefaultReadVersionsCompleted("DefaultPriorityReadVersionsCompleted", cc),
    transactionImmediateReadVersionsCompleted("ImmediatePriorityReadVersionsCompleted", cc),
    transactionLogicalReads("LogicalUncachedReads", cc), transactionPhysicalReads("PhysicalReadRequests", cc),
    transactionPhysicalReadsCompleted("PhysicalReadRequestsCompleted", cc),
    transactionGetKeyRequests("GetKeyRequests", cc), transactionGetValueRequests("GetValueRequests", cc),
    transactionGetRangeRequests("GetRangeRequests", cc),
    transactionGetMappedRangeRequests("GetMappedRangeRequests", cc),
    transactionGetRangeStreamRequests("GetRangeStreamRequests", cc), transactionWatchRequests("WatchRequests", cc),
    transactionGetAddressesForKeyRequests("GetAddressesForKeyRequests", cc), transactionBytesRead("BytesRead", cc),
    transactionKeysRead("KeysRead", cc), transactionMetadataVersionReads("MetadataVersionReads", cc),
    transactionCommittedMutations("CommittedMutations", cc),
    transactionCommittedMutationBytes("CommittedMutationBytes", cc), transactionSetMutations("SetMutations", cc),
    transactionClearMutations("ClearMutations", cc), transactionAtomicMutations("AtomicMutations", cc),
    transactionsCommitStarted("CommitStarted", cc), transactionsCommitCompleted("CommitCompleted", cc),
    transactionKeyServerLocationRequests("KeyServerLocationRequests", cc),
    transactionKeyServerLocationRequestsCompleted("KeyServerLocationRequestsCompleted", cc),
    transactionBlobGranuleLocationRequests("BlobGranuleLocationRequests", cc),
    transactionBlobGranuleLocationRequestsCompleted("BlobGranuleLocationRequestsCompleted", cc),
    transactionStatusRequests("StatusRequests", cc), transactionTenantLookupRequests("TenantLookupRequests", cc),
    transactionTenantLookupRequestsCompleted("TenantLookupRequestsCompleted", cc), transactionsTooOld("TooOld", cc),
    transactionsFutureVersions("FutureVersions", cc), transactionsNotCommitted("NotCommitted", cc),
    transactionsMaybeCommitted("MaybeCommitted", cc), transactionsResourceConstrained("ResourceConstrained", cc),
    transactionsProcessBehind("ProcessBehind", cc), transactionsThrottled("Throttled", cc),
    transactionsLockRejected("LockRejected", cc),
    transactionsExpensiveClearCostEstCount("ExpensiveClearCostEstCount", cc),
    transactionGrvFullBatches("NumGrvFullBatches", cc), transactionGrvTimedOutBatches("NumGrvTimedOutBatches", cc),
    transactionCommitVersionNotFoundForSS("CommitVersionNotFoundForSS", cc), anyBGReads(false),
    ccBG("BlobGranuleReadMetrics", dbId.toString()), bgReadInputBytes("BGReadInputBytes", ccBG),
    bgReadOutputBytes("BGReadOutputBytes", ccBG), bgReadSnapshotRows("BGReadSnapshotRows", ccBG),
    bgReadRowsCleared("BGReadRowsCleared", ccBG), bgReadRowsInserted("BGReadRowsInserted", ccBG),
    bgReadRowsUpdated("BGReadRowsUpdated", ccBG), bgLatencies(), bgGranulesPerRequest(), usedAnyChangeFeeds(false),
    ccFeed("ChangeFeedClientMetrics", dbId.toString()), feedStreamStarts("FeedStreamStarts", ccFeed),
    feedMergeStreamStarts("FeedMergeStreamStarts", ccFeed), feedErrors("FeedErrors", ccFeed),
    feedNonRetriableErrors("FeedNonRetriableErrors", ccFeed), feedPops("FeedPops", ccFeed),
    feedPopsFallback("FeedPopsFallback", ccFeed), latencies(), readLatencies(), commitLatencies(), GRVLatencies(),
    mutationsPerCommit(), bytesPerCommit(), outstandingWatches(0), sharedStatePtr(nullptr), lastGrvTime(0.0),
    cachedReadVersion(0), lastRkBatchThrottleTime(0.0), lastRkDefaultThrottleTime(0.0), lastProxyRequestTime(0.0),
    transactionTracingSample(false), taskID(taskID), clientInfo(clientInfo), clientInfoMonitor(clientInfoMonitor),
    coordinator(coordinator), apiVersion(_apiVersion), mvCacheInsertLocation(0), healthMetricsLastUpdated(0),
    detailedHealthMetricsLastUpdated(0), smoothMidShardSize(CLIENT_KNOBS->SHARD_STAT_SMOOTH_AMOUNT),
    specialKeySpace(std::make_unique<SpecialKeySpace>(specialKeys.begin, specialKeys.end, /* test */ false)),
    connectToDatabaseEventCacheHolder(format("ConnectToDatabase/%s", dbId.toString().c_str())) {

	DisabledTraceEvent("DatabaseContextCreated", dbId).backtrace();

	connected = (clientInfo->get().commitProxies.size() && clientInfo->get().grvProxies.size())
	                ? Void()
	                : clientInfo->onChange();

	metadataVersionCache.resize(CLIENT_KNOBS->METADATA_VERSION_CACHE_SIZE);
	maxOutstandingWatches = CLIENT_KNOBS->DEFAULT_MAX_OUTSTANDING_WATCHES;

	snapshotRywEnabled = apiVersion.hasSnapshotRYW() ? 1 : 0;

	logger = databaseLogger(this) && tssLogger(this);
	locationCacheSize = g_network->isSimulated() ? CLIENT_KNOBS->LOCATION_CACHE_EVICTION_SIZE_SIM
	                                             : CLIENT_KNOBS->LOCATION_CACHE_EVICTION_SIZE;

	getValueSubmitted.init("NativeAPI.GetValueSubmitted"_sr);
	getValueCompleted.init("NativeAPI.GetValueCompleted"_sr);

	clientDBInfoMonitor = monitorClientDBInfoChange(this, clientInfo, &proxiesChangeTrigger);
	tssMismatchHandler = handleTssMismatches(this);
	clientStatusUpdater.actor = clientStatusUpdateActor(this);
	cacheListMonitor = monitorCacheList(this);

	smoothMidShardSize.reset(CLIENT_KNOBS->INIT_MID_SHARD_BYTES);
	globalConfig = std::make_unique<GlobalConfig>(this);

	if (apiVersion.version() >= 740) {
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::METRICS,
		    SpecialKeySpace::IMPLTYPE::READONLY,
		    std::make_unique<FaultToleranceMetricsImpl>(
		        singleKeyRange("fault_tolerance_metrics_json"_sr)
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::METRICS).begin)));
	}

	if (apiVersion.version() >= 700) {
		registerSpecialKeysImpl(SpecialKeySpace::MODULE::ERRORMSG,
		                        SpecialKeySpace::IMPLTYPE::READONLY,
		                        std::make_unique<SingleSpecialKeyImpl>(
		                            SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin,
		                            [](ReadYourWritesTransaction* ryw) -> Future<Optional<Value>> {
			                            if (ryw->getSpecialKeySpaceErrorMsg().present())
				                            return Optional<Value>(ryw->getSpecialKeySpaceErrorMsg().get());
			                            else
				                            return Optional<Value>();
		                            },
		                            true));
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<ManagementCommandsOptionsImpl>(
		        KeyRangeRef("options/"_sr, "options0"_sr)
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<ExcludeServersRangeImpl>(SpecialKeySpace::getManagementApiCommandRange("exclude")));
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<FailedServersRangeImpl>(SpecialKeySpace::getManagementApiCommandRange("failed")));
		registerSpecialKeysImpl(SpecialKeySpace::MODULE::MANAGEMENT,
		                        SpecialKeySpace::IMPLTYPE::READWRITE,
		                        std::make_unique<ExcludedLocalitiesRangeImpl>(
		                            SpecialKeySpace::getManagementApiCommandRange("excludedlocality")));
		registerSpecialKeysImpl(SpecialKeySpace::MODULE::MANAGEMENT,
		                        SpecialKeySpace::IMPLTYPE::READWRITE,
		                        std::make_unique<FailedLocalitiesRangeImpl>(
		                            SpecialKeySpace::getManagementApiCommandRange("failedlocality")));
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READONLY,
		    std::make_unique<ExclusionInProgressRangeImpl>(
		        KeyRangeRef("in_progress_exclusion/"_sr, "in_progress_exclusion0"_sr)
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::CONFIGURATION,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<ProcessClassRangeImpl>(
		        KeyRangeRef("process/class_type/"_sr, "process/class_type0"_sr)
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin)));
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::CONFIGURATION,
		    SpecialKeySpace::IMPLTYPE::READONLY,
		    std::make_unique<ProcessClassSourceRangeImpl>(
		        KeyRangeRef("process/class_source/"_sr, "process/class_source0"_sr)
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin)));
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<LockDatabaseImpl>(
		        singleKeyRange("db_locked"_sr)
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<ConsistencyCheckImpl>(
		        singleKeyRange("consistency_check_suspended"_sr)
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::GLOBALCONFIG,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<GlobalConfigImpl>(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::GLOBALCONFIG)));
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::TRACING,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<TracingOptionsImpl>(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::TRACING)));
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::CONFIGURATION,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<CoordinatorsImpl>(
		        KeyRangeRef("coordinators/"_sr, "coordinators0"_sr)
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin)));
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READONLY,
		    std::make_unique<CoordinatorsAutoImpl>(
		        singleKeyRange("auto_coordinators"_sr)
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<AdvanceVersionImpl>(
		        singleKeyRange("min_required_commit_version"_sr)
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<VersionEpochImpl>(
		        singleKeyRange("version_epoch"_sr)
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<ClientProfilingImpl>(
		        KeyRangeRef("profiling/"_sr, "profiling0"_sr)
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)),
		    /* deprecated */ ApiVersion::withClientProfilingDeprecated().version());
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<MaintenanceImpl>(
		        KeyRangeRef("maintenance/"_sr, "maintenance0"_sr)
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<DataDistributionImpl>(
		        KeyRangeRef("data_distribution/"_sr, "data_distribution0"_sr)
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::ACTORLINEAGE,
		    SpecialKeySpace::IMPLTYPE::READONLY,
		    std::make_unique<ActorLineageImpl>(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ACTORLINEAGE)));
		registerSpecialKeysImpl(SpecialKeySpace::MODULE::ACTOR_PROFILER_CONF,
		                        SpecialKeySpace::IMPLTYPE::READWRITE,
		                        std::make_unique<ActorProfilerConf>(
		                            SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ACTOR_PROFILER_CONF)));
	}
	if (apiVersion.version() >= 630) {
		registerSpecialKeysImpl(SpecialKeySpace::MODULE::TRANSACTION,
		                        SpecialKeySpace::IMPLTYPE::READONLY,
		                        std::make_unique<ConflictingKeysImpl>(conflictingKeysRange));
		registerSpecialKeysImpl(SpecialKeySpace::MODULE::TRANSACTION,
		                        SpecialKeySpace::IMPLTYPE::READONLY,
		                        std::make_unique<ReadConflictRangeImpl>(readConflictRangeKeysRange));
		registerSpecialKeysImpl(SpecialKeySpace::MODULE::TRANSACTION,
		                        SpecialKeySpace::IMPLTYPE::READONLY,
		                        std::make_unique<WriteConflictRangeImpl>(writeConflictRangeKeysRange));
		registerSpecialKeysImpl(SpecialKeySpace::MODULE::METRICS,
		                        SpecialKeySpace::IMPLTYPE::READONLY,
		                        std::make_unique<DDStatsRangeImpl>(ddStatsRange));
		registerSpecialKeysImpl(SpecialKeySpace::MODULE::METRICS,
		                        SpecialKeySpace::IMPLTYPE::READONLY,
		                        std::make_unique<HealthMetricsRangeImpl>(
		                            KeyRangeRef("\xff\xff/metrics/health/"_sr, "\xff\xff/metrics/health0"_sr)));
		registerSpecialKeysImpl(SpecialKeySpace::MODULE::WORKERINTERFACE,
		                        SpecialKeySpace::IMPLTYPE::READONLY,
		                        std::make_unique<WorkerInterfacesSpecialKeyImpl>(
		                            KeyRangeRef("\xff\xff/worker_interfaces/"_sr, "\xff\xff/worker_interfaces0"_sr)));
		registerSpecialKeysImpl(SpecialKeySpace::MODULE::STATUSJSON,
		                        SpecialKeySpace::IMPLTYPE::READONLY,
		                        std::make_unique<SingleSpecialKeyImpl>(
		                            "\xff\xff/status/json"_sr,
		                            [](ReadYourWritesTransaction* ryw) -> Future<Optional<Value>> {
			                            if (ryw->getDatabase().getPtr() && ryw->getDatabase()->getConnectionRecord()) {
				                            ++ryw->getDatabase()->transactionStatusRequests;
				                            return getJSON(ryw->getDatabase());
			                            } else {
				                            return Optional<Value>();
			                            }
		                            },
		                            true));
		registerSpecialKeysImpl(SpecialKeySpace::MODULE::CLUSTERFILEPATH,
		                        SpecialKeySpace::IMPLTYPE::READONLY,
		                        std::make_unique<SingleSpecialKeyImpl>(
		                            "\xff\xff/cluster_file_path"_sr,
		                            [](ReadYourWritesTransaction* ryw) -> Future<Optional<Value>> {
			                            try {
				                            if (ryw->getDatabase().getPtr() &&
				                                ryw->getDatabase()->getConnectionRecord()) {
					                            Optional<Value> output =
					                                StringRef(ryw->getDatabase()->getConnectionRecord()->getLocation());
					                            return output;
				                            }
			                            } catch (Error& e) {
				                            return e;
			                            }
			                            return Optional<Value>();
		                            },
		                            true));

		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::CONNECTIONSTRING,
		    SpecialKeySpace::IMPLTYPE::READONLY,
		    std::make_unique<SingleSpecialKeyImpl>(
		        "\xff\xff/connection_string"_sr,
		        [](ReadYourWritesTransaction* ryw) -> Future<Optional<Value>> {
			        try {
				        if (ryw->getDatabase().getPtr() && ryw->getDatabase()->getConnectionRecord()) {
					        Reference<IClusterConnectionRecord> f = ryw->getDatabase()->getConnectionRecord();
					        Optional<Value> output = StringRef(f->getConnectionString().toString());
					        return output;
				        }
			        } catch (Error& e) {
				        return e;
			        }
			        return Optional<Value>();
		        },
		        true));
		registerSpecialKeysImpl(SpecialKeySpace::MODULE::CLUSTERID,
		                        SpecialKeySpace::IMPLTYPE::READONLY,
		                        std::make_unique<SingleSpecialKeyImpl>(
		                            "\xff\xff/cluster_id"_sr,
		                            [](ReadYourWritesTransaction* ryw) -> Future<Optional<Value>> {
			                            try {
				                            if (ryw->getDatabase().getPtr()) {
					                            return map(getClusterId(ryw->getDatabase()), [](UID id) {
						                            return Optional<Value>(StringRef(id.toString()));
					                            });
				                            }
			                            } catch (Error& e) {
				                            return e;
			                            }
			                            return Optional<Value>();
		                            },
		                            true));

		registerSpecialKeysImpl(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<TenantRangeImpl>(SpecialKeySpace::getManagementApiCommandRange("tenant")));
	}
	throttleExpirer = recurring([this]() { expireThrottles(); }, CLIENT_KNOBS->TAG_THROTTLE_EXPIRATION_INTERVAL);

	if (BUGGIFY) {
		DatabaseContext::debugUseTags = true;
	}

	initializeSpecialCounters();
}

DatabaseContext::DatabaseContext(const Error& err)
  : deferredError(err), internal(IsInternal::False), cc("TransactionMetrics"),
    transactionReadVersions("ReadVersions", cc), transactionReadVersionsThrottled("ReadVersionsThrottled", cc),
    transactionReadVersionsCompleted("ReadVersionsCompleted", cc),
    transactionReadVersionBatches("ReadVersionBatches", cc),
    transactionBatchReadVersions("BatchPriorityReadVersions", cc),
    transactionDefaultReadVersions("DefaultPriorityReadVersions", cc),
    transactionImmediateReadVersions("ImmediatePriorityReadVersions", cc),
    transactionBatchReadVersionsCompleted("BatchPriorityReadVersionsCompleted", cc),
    transactionDefaultReadVersionsCompleted("DefaultPriorityReadVersionsCompleted", cc),
    transactionImmediateReadVersionsCompleted("ImmediatePriorityReadVersionsCompleted", cc),
    transactionLogicalReads("LogicalUncachedReads", cc), transactionPhysicalReads("PhysicalReadRequests", cc),
    transactionPhysicalReadsCompleted("PhysicalReadRequestsCompleted", cc),
    transactionGetKeyRequests("GetKeyRequests", cc), transactionGetValueRequests("GetValueRequests", cc),
    transactionGetRangeRequests("GetRangeRequests", cc),
    transactionGetMappedRangeRequests("GetMappedRangeRequests", cc),
    transactionGetRangeStreamRequests("GetRangeStreamRequests", cc), transactionWatchRequests("WatchRequests", cc),
    transactionGetAddressesForKeyRequests("GetAddressesForKeyRequests", cc), transactionBytesRead("BytesRead", cc),
    transactionKeysRead("KeysRead", cc), transactionMetadataVersionReads("MetadataVersionReads", cc),
    transactionCommittedMutations("CommittedMutations", cc),
    transactionCommittedMutationBytes("CommittedMutationBytes", cc), transactionSetMutations("SetMutations", cc),
    transactionClearMutations("ClearMutations", cc), transactionAtomicMutations("AtomicMutations", cc),
    transactionsCommitStarted("CommitStarted", cc), transactionsCommitCompleted("CommitCompleted", cc),
    transactionKeyServerLocationRequests("KeyServerLocationRequests", cc),
    transactionKeyServerLocationRequestsCompleted("KeyServerLocationRequestsCompleted", cc),
    transactionBlobGranuleLocationRequests("BlobGranuleLocationRequests", cc),
    transactionBlobGranuleLocationRequestsCompleted("BlobGranuleLocationRequestsCompleted", cc),
    transactionStatusRequests("StatusRequests", cc), transactionTenantLookupRequests("TenantLookupRequests", cc),
    transactionTenantLookupRequestsCompleted("TenantLookupRequestsCompleted", cc), transactionsTooOld("TooOld", cc),
    transactionsFutureVersions("FutureVersions", cc), transactionsNotCommitted("NotCommitted", cc),
    transactionsMaybeCommitted("MaybeCommitted", cc), transactionsResourceConstrained("ResourceConstrained", cc),
    transactionsProcessBehind("ProcessBehind", cc), transactionsThrottled("Throttled", cc),
    transactionsLockRejected("LockRejected", cc),
    transactionsExpensiveClearCostEstCount("ExpensiveClearCostEstCount", cc),
    transactionGrvFullBatches("NumGrvFullBatches", cc), transactionGrvTimedOutBatches("NumGrvTimedOutBatches", cc),
    transactionCommitVersionNotFoundForSS("CommitVersionNotFoundForSS", cc), anyBGReads(false),
    ccBG("BlobGranuleReadMetrics"), bgReadInputBytes("BGReadInputBytes", ccBG),
    bgReadOutputBytes("BGReadOutputBytes", ccBG), bgReadSnapshotRows("BGReadSnapshotRows", ccBG),
    bgReadRowsCleared("BGReadRowsCleared", ccBG), bgReadRowsInserted("BGReadRowsInserted", ccBG),
    bgReadRowsUpdated("BGReadRowsUpdated", ccBG), bgLatencies(), bgGranulesPerRequest(), usedAnyChangeFeeds(false),
    ccFeed("ChangeFeedClientMetrics"), feedStreamStarts("FeedStreamStarts", ccFeed),
    feedMergeStreamStarts("FeedMergeStreamStarts", ccFeed), feedErrors("FeedErrors", ccFeed),
    feedNonRetriableErrors("FeedNonRetriableErrors", ccFeed), feedPops("FeedPops", ccFeed),
    feedPopsFallback("FeedPopsFallback", ccFeed), latencies(), readLatencies(), commitLatencies(), GRVLatencies(),
    mutationsPerCommit(), bytesPerCommit(), sharedStatePtr(nullptr), transactionTracingSample(false),
    smoothMidShardSize(CLIENT_KNOBS->SHARD_STAT_SMOOTH_AMOUNT),
    connectToDatabaseEventCacheHolder(format("ConnectToDatabase/%s", dbId.toString().c_str())), outstandingWatches(0) {
	initializeSpecialCounters();
}

// Static constructor used by server processes to create a DatabaseContext
// For internal (fdbserver) use only
Database DatabaseContext::create(Reference<AsyncVar<ClientDBInfo>> clientInfo,
                                 Future<Void> clientInfoMonitor,
                                 LocalityData clientLocality,
                                 EnableLocalityLoadBalance enableLocalityLoadBalance,
                                 TaskPriority taskID,
                                 LockAware lockAware,
                                 int apiVersion,
                                 IsSwitchable switchable) {
	return Database(new DatabaseContext(Reference<AsyncVar<Reference<IClusterConnectionRecord>>>(),
	                                    clientInfo,
	                                    makeReference<AsyncVar<Optional<ClientLeaderRegInterface>>>(),
	                                    clientInfoMonitor,
	                                    taskID,
	                                    clientLocality,
	                                    enableLocalityLoadBalance,
	                                    lockAware,
	                                    IsInternal::True,
	                                    apiVersion,
	                                    switchable));
}

DatabaseContext::~DatabaseContext() {
	cacheListMonitor.cancel();
	clientDBInfoMonitor.cancel();
	monitorTssInfoChange.cancel();
	tssMismatchHandler.cancel();
	initializeChangeFeedCache = Void();
	storage = nullptr;
	changeFeedStorageCommitter = Void();
	if (grvUpdateHandler.isValid()) {
		grvUpdateHandler.cancel();
	}
	if (sharedStatePtr) {
		sharedStatePtr->delRef(sharedStatePtr);
	}
	for (auto it = server_interf.begin(); it != server_interf.end(); it = server_interf.erase(it))
		it->second->notifyContextDestroyed();
	ASSERT_ABORT(server_interf.empty());
	locationCache.insert(allKeys, Reference<LocationInfo>());
	for (auto& it : notAtLatestChangeFeeds) {
		it.second->context = nullptr;
	}
	for (auto& it : changeFeedUpdaters) {
		it.second->context = nullptr;
	}

	DisabledTraceEvent("DatabaseContextDestructed", dbId).backtrace();
}
