/*
 * NativeAPI.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"

#include <algorithm>
#include <iterator>
#include <memory>
#include <regex>
#include <unordered_set>
#include <tuple>
#include <utility>
#include <vector>

#include "boost/algorithm/string.hpp"
#include "contrib/fmt-8.1.1/include/fmt/format.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/MultiInterface.h"

#include "fdbclient/ActorLineageProfiler.h"
#include "fdbclient/AnnotateActor.h"
#include "fdbclient/Atomic.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/ClusterConnectionFile.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/GlobalConfig.actor.h"
#include "fdbclient/IKnobCollection.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/JsonBuilder.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NameLineage.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/MonitorLeader.h"
#include "fdbclient/MutationList.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/ParallelStream.actor.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/TransactionLineage.h"
#include "fdbclient/versions.h"
#include "fdbclient/WellKnownEndpoints.h"
#include "fdbrpc/LoadBalance.h"
#include "fdbrpc/Net2FileSystem.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/sim_validation.h"
#include "flow/Arena.h"
#include "flow/ActorCollection.h"
#include "flow/DeterministicRandom.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/ProtocolVersion.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"
#include "flow/Knobs.h"
#include "flow/Platform.h"
#include "flow/SystemMonitor.h"
#include "flow/TLSConfig.actor.h"
#include "flow/Tracing.h"
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

extern const char* getSourceVersion();

namespace {

TransactionLineageCollector transactionLineageCollector;
NameLineageCollector nameLineageCollector;

template <class Interface, class Request>
Future<REPLY_TYPE(Request)> loadBalance(
    DatabaseContext* ctx,
    const Reference<LocationInfo> alternatives,
    RequestStream<Request> Interface::*channel,
    const Request& request = Request(),
    TaskPriority taskID = TaskPriority::DefaultPromiseEndpoint,
    AtMostOnce atMostOnce =
        AtMostOnce::False, // if true, throws request_maybe_delivered() instead of retrying automatically
    QueueModel* model = nullptr) {
	if (alternatives->hasCaches) {
		return loadBalance(alternatives->locations(), channel, request, taskID, atMostOnce, model);
	}
	return fmap(
	    [ctx](auto const& res) {
		    if (res.cached) {
			    ctx->updateCache.trigger();
		    }
		    return res;
	    },
	    loadBalance(alternatives->locations(), channel, request, taskID, atMostOnce, model));
}
} // namespace

FDB_BOOLEAN_PARAM(TransactionRecordLogInfo);
FDB_DEFINE_BOOLEAN_PARAM(UseProvisionalProxies);

// Used to determine whether or not client will load balance based on the number of GRVs released by each proxy
FDB_DEFINE_BOOLEAN_PARAM(BalanceOnRequests);

// Whether or not a request should include the tenant name
FDB_BOOLEAN_PARAM(UseTenant);

NetworkOptions networkOptions;
TLSConfig tlsConfig(TLSEndpointType::CLIENT);

// The default values, TRACE_DEFAULT_ROLL_SIZE and TRACE_DEFAULT_MAX_LOGS_SIZE are located in Trace.h.
NetworkOptions::NetworkOptions()
  : traceRollSize(TRACE_DEFAULT_ROLL_SIZE), traceMaxLogsSize(TRACE_DEFAULT_MAX_LOGS_SIZE), traceLogGroup("default"),
    traceFormat("xml"), traceClockSource("now"),
    supportedVersions(new ReferencedObject<Standalone<VectorRef<ClientVersionRef>>>()), runLoopProfilingEnabled(false),
    primaryClient(true) {}

static const Key CLIENT_LATENCY_INFO_PREFIX = LiteralStringRef("client_latency/");
static const Key CLIENT_LATENCY_INFO_CTR_PREFIX = LiteralStringRef("client_latency_counter/");

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
			if (result->second.id() == tssi.id()) {
				metrics = tssMetrics[tssi.id()];
			} else {
				TEST(true); // SS now maps to new TSS! This will probably never happen in practice
				tssMetrics.erase(result->second.id());
				metrics = makeReference<TSSMetrics>();
				tssMetrics[tssi.id()] = metrics;
			}
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

		// non-data requests duplicated for load
		queueModel.updateTssEndpoint(ssi.watchValue.getEndpoint().token.first(),
		                             TSSEndpointData(tssi.id(), tssi.watchValue.getEndpoint(), metrics));
		queueModel.updateTssEndpoint(ssi.splitMetrics.getEndpoint().token.first(),
		                             TSSEndpointData(tssi.id(), tssi.splitMetrics.getEndpoint(), metrics));
		queueModel.updateTssEndpoint(ssi.getReadHotRanges.getEndpoint().token.first(),
		                             TSSEndpointData(tssi.id(), tssi.getReadHotRanges.getEndpoint(), metrics));
		queueModel.updateTssEndpoint(ssi.getRangeSplitPoints.getEndpoint().token.first(),
		                             TSSEndpointData(tssi.id(), tssi.getRangeSplitPoints.getEndpoint(), metrics));
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
                                              Version readVersion,
                                              Reference<TransactionState> info,
                                              VersionVector& latestCommitVersions) {
	latestCommitVersions.clear();

	if (info->debugID.present()) {
		g_traceBatch.addEvent("TransactionDebug", info->debugID.get().first(), "NativeAPI.getLatestCommitVersions");
	}

	if (!info->readVersionObtainedFromGrvProxy) {
		return;
	}

	if (ssVersionVectorCache.getMaxVersion() == invalidVersion) {
		return;
	}

	if (readVersion > ssVersionVectorCache.getMaxVersion()) {
		if (!CLIENT_KNOBS->FORCE_GRV_CACHE_OFF && !info->options.skipGrvCache && info->options.useGrvCache) {
			return;
		} else {
			TraceEvent(SevError, "GetLatestCommitVersions")
			    .detail("ReadVersion", readVersion)
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
		if (tag != invalidTag && commitVersion != invalidVersion && commitVersion < readVersion) {
			updatedVersionMap = true;
			versionMap[commitVersion].insert(tag);
		}

		// Do not log if commitVersion >= readVersion.
		if (!updatedVersionMap && commitVersion == invalidVersion) {
			TraceEvent(SevDebug, "CommitVersionNotFoundForSS")
			    .detail("InSSIDMap", tag != invalidTag ? 1 : 0)
			    .detail("Tag", tag)
			    .detail("CommitVersion", commitVersion)
			    .detail("ReadVersion", readVersion)
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

std::string printable(const VectorRef<KeyValueRef>& val) {
	std::string s;
	for (int i = 0; i < val.size(); i++)
		s = s + printable(val[i].key) + format(":%d ", val[i].value.size());
	return s;
}

std::string printable(const KeyValueRef& val) {
	return printable(val.key) + format(":%d ", val.value.size());
}

std::string printable(const VectorRef<StringRef>& val) {
	std::string s;
	for (int i = 0; i < val.size(); i++)
		s = s + printable(val[i]) + " ";
	return s;
}

std::string printable(const StringRef& val) {
	return val.printable();
}
std::string printable(const std::string& str) {
	return StringRef(str).printable();
}

std::string printable(const KeyRangeRef& range) {
	return printable(range.begin) + " - " + printable(range.end);
}

std::string printable(const VectorRef<KeyRangeRef>& val) {
	std::string s;
	for (int i = 0; i < val.size(); i++)
		s = s + printable(val[i]) + " ";
	return s;
}

int unhex(char c) {
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;
	UNREACHABLE();
}

std::string unprintable(std::string const& val) {
	std::string s;
	for (int i = 0; i < val.size(); i++) {
		char c = val[i];
		if (c == '\\') {
			if (++i == val.size())
				ASSERT(false);
			if (val[i] == '\\') {
				s += '\\';
			} else if (val[i] == 'x') {
				if (i + 2 >= val.size())
					ASSERT(false);
				s += char((unhex(val[i + 1]) << 4) + unhex(val[i + 2]));
				i += 2;
			} else
				ASSERT(false);
		} else
			s += c;
	}
	return s;
}

void DatabaseContext::validateVersion(Version version) const {
	// Version could be 0 if the INITIALIZE_NEW_DATABASE option is set. In that case, it is illegal to perform any
	// reads. We throw client_invalid_operation because the caller didn't directly set the version, so the
	// version_invalid error might be confusing.
	if (version == 0) {
		throw client_invalid_operation();
	}
	if (switchable && version < minAcceptableReadVersion) {
		TEST(true); // Attempted to read a version lower than any this client has seen from the current cluster
		throw transaction_too_old();
	}

	ASSERT(version > 0 || version == latestVersion);
}

void validateOptionValuePresent(Optional<StringRef> value) {
	if (!value.present()) {
		throw invalid_option_value();
	}
}

void validateOptionValueNotPresent(Optional<StringRef> value) {
	if (value.present() && value.get().size() > 0) {
		throw invalid_option_value();
	}
}

void dumpMutations(const MutationListRef& mutations) {
	for (auto m = mutations.begin(); m; ++m) {
		switch (m->type) {
		case MutationRef::SetValue:
			printf("  '%s' := '%s'\n", printable(m->param1).c_str(), printable(m->param2).c_str());
			break;
		case MutationRef::AddValue:
			printf("  '%s' += '%s'", printable(m->param1).c_str(), printable(m->param2).c_str());
			break;
		case MutationRef::ClearRange:
			printf("  Clear ['%s','%s')\n", printable(m->param1).c_str(), printable(m->param2).c_str());
			break;
		default:
			printf("  Unknown mutation %d('%s','%s')\n",
			       m->type,
			       printable(m->param1).c_str(),
			       printable(m->param2).c_str());
			break;
		}
	}
}

template <>
void addref(DatabaseContext* ptr) {
	ptr->addref();
}
template <>
void delref(DatabaseContext* ptr) {
	ptr->delref();
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
void traceSSOrTSSPercentiles(TraceEvent& ev, const std::string name, ContinuousSample<double>& sample) {
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
                         ContinuousSample<double>& ssSample,
                         ContinuousSample<double>& tssSample) {
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

		if (!g_network->isSimulated()) {
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
			if (cx->anyBlobGranuleRequests) {
				ev.detail("MeanBGLatency", cx->bgLatencies.mean())
				    .detail("MedianBGLatency", cx->bgLatencies.median())
				    .detail("MaxBGLatency", cx->bgLatencies.max())
				    .detail("MeanBGGranulesPerRequest", cx->bgGranulesPerRequest.mean())
				    .detail("MedianBGGranulesPerRequest", cx->bgGranulesPerRequest.median())
				    .detail("MaxBGGranulesPerRequest", cx->bgGranulesPerRequest.max());
			}
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
			wait(tr->onError(e));
		}
	}
}

// Delref and addref self to give self a chance to get destroyed.
ACTOR static Future<Void> refreshTransaction(DatabaseContext* self, Transaction* tr) {
	*tr = Transaction();
	wait(delay(0)); // Give ourselves the chance to get cancelled if self was destroyed
	*tr = Transaction(Database(Reference<DatabaseContext>::addRef(self)));
	return Void();
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
		// Need to make sure that we eventually destroy tr. We can't rely on getting cancelled to do this because of
		// the cyclic reference to self.
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

			wait(delay(CLIENT_KNOBS->CSI_STATUS_DELAY));
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			cx->clientStatusUpdater.outStatusQ.clear();
			TraceEvent(SevWarnAlways, "UnableToWriteClientStatus").error(e);
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
	Span span(deterministicRandom()->randomUniqueID(), "VerifyCausalReadRisky"_loc);
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
			} catch (Error& e) {
				wait(tr.onError(e));
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
		// <tssid, list of detailed mismatch data>
		state std::pair<UID, std::vector<DetailedTSSMismatch>> data = waitNext(cx->tssMismatchStream.getFuture());
		// return to calling actor, don't do this as part of metrics loop
		wait(delay(0));
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
			TEST(quarantine); // Quarantining TSS because it got mismatch
			TEST(!quarantine); // Killing TSS because it got mismatch

			tr = makeReference<ReadYourWritesTransaction>(Database(Reference<DatabaseContext>::addRef(cx)));
			state int tries = 0;
			loop {
				try {
					tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					if (quarantine) {
						tr->set(tssQuarantineKeyFor(data.first), LiteralStringRef(""));
					} else {
						tr->clear(serverTagKeyFor(data.first));
					}
					tssMapDB.erase(tr, tssPairID);

					for (const DetailedTSSMismatch& d : data.second) {
						// <tssid, time, mismatchid> -> mismatch data
						tssMismatchDB.set(
						    tr,
						    Tuple().append(data.first.toString()).append(d.timestamp).append(d.mismatchId.toString()),
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
			TEST(true); // Not handling TSS with mismatch because it's already gone
		}
	}
}

ACTOR static Future<Void> backgroundGrvUpdater(DatabaseContext* cx) {
	state Transaction tr;
	state double grvDelay = 0.001;
	try {
		loop {
			if (CLIENT_KNOBS->FORCE_GRV_CACHE_OFF)
				return Void();
			wait(refreshTransaction(cx, &tr));
			state double curTime = now();
			state double lastTime = cx->getLastGrvTime();
			state double lastProxyTime = cx->lastProxyRequestTime;
			TraceEvent(SevDebug, "BackgroundGrvUpdaterBefore")
			    .detail("CurTime", curTime)
			    .detail("LastTime", lastTime)
			    .detail("GrvDelay", grvDelay)
			    .detail("CachedReadVersion", cx->getCachedReadVersion())
			    .detail("CachedTime", cx->getLastGrvTime())
			    .detail("Gap", curTime - lastTime)
			    .detail("Bound", CLIENT_KNOBS->MAX_VERSION_CACHE_LAG - grvDelay);
			if (curTime - lastTime >= (CLIENT_KNOBS->MAX_VERSION_CACHE_LAG - grvDelay) ||
			    curTime - lastProxyTime > CLIENT_KNOBS->MAX_PROXY_CONTACT_LAG) {
				try {
					tr.setOption(FDBTransactionOptions::SKIP_GRV_CACHE);
					wait(success(tr.getReadVersion()));
					cx->lastProxyRequestTime = curTime;
					grvDelay = (grvDelay + (now() - curTime)) / 2.0;
					TraceEvent(SevDebug, "BackgroundGrvUpdaterSuccess")
					    .detail("GrvDelay", grvDelay)
					    .detail("CachedReadVersion", cx->getCachedReadVersion())
					    .detail("CachedTime", cx->getLastGrvTime());
				} catch (Error& e) {
					TraceEvent(SevInfo, "BackgroundGrvUpdaterTxnError").errorUnsuppressed(e);
					wait(tr.onError(e));
				}
			} else {
				wait(
				    delay(std::max(0.001,
				                   std::min(CLIENT_KNOBS->MAX_PROXY_CONTACT_LAG - (curTime - lastProxyTime),
				                            (CLIENT_KNOBS->MAX_VERSION_CACHE_LAG - grvDelay) - (curTime - lastTime)))));
			}
		}
	} catch (Error& e) {
		TraceEvent(SevInfo, "BackgroundGrvUpdaterFailed").errorUnsuppressed(e);
		throw;
	}
}

ACTOR static Future<HealthMetrics> getHealthMetricsActor(DatabaseContext* cx, bool detailed) {
	if (now() - cx->healthMetricsLastUpdated < CLIENT_KNOBS->AGGREGATE_HEALTH_METRICS_MAX_STALENESS) {
		if (detailed) {
			return cx->healthMetrics;
		} else {
			HealthMetrics result;
			result.update(cx->healthMetrics, false, false);
			return result;
		}
	}
	state bool sendDetailedRequest =
	    detailed && now() - cx->detailedHealthMetricsLastUpdated > CLIENT_KNOBS->DETAILED_HEALTH_METRICS_MAX_STALENESS;
	loop {
		choose {
			when(wait(cx->onProxiesChanged())) {}
			when(GetHealthMetricsReply rep = wait(basicLoadBalance(cx->getGrvProxies(UseProvisionalProxies::False),
			                                                       &GrvProxyInterface::getHealthMetrics,
			                                                       GetHealthMetricsRequest(sendDetailedRequest)))) {
				cx->healthMetrics.update(rep.healthMetrics, detailed, true);
				if (detailed) {
					cx->healthMetricsLastUpdated = now();
					cx->detailedHealthMetricsLastUpdated = now();
					return cx->healthMetrics;
				} else {
					cx->healthMetricsLastUpdated = now();
					HealthMetrics result;
					result.update(cx->healthMetrics, false, false);
					return result;
				}
			}
		}
	}
}

Future<HealthMetrics> DatabaseContext::getHealthMetrics(bool detailed = false) {
	return getHealthMetricsActor(this, detailed);
}

void DatabaseContext::registerSpecialKeySpaceModule(SpecialKeySpace::MODULE module,
                                                    SpecialKeySpace::IMPLTYPE type,
                                                    std::unique_ptr<SpecialKeyRangeReadImpl>&& impl) {
	specialKeySpace->registerKeyRange(module, type, impl->getKeyRange(), impl.get());
	specialKeySpaceModules.push_back(std::move(impl));
}

ACTOR Future<RangeResult> getWorkerInterfaces(Reference<IClusterConnectionRecord> clusterRecord);
ACTOR Future<Optional<Value>> getJSON(Database db);

struct WorkerInterfacesSpecialKeyImpl : SpecialKeyRangeReadImpl {
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override {
		if (ryw->getDatabase().getPtr() && ryw->getDatabase()->getConnectionRecord()) {
			Key prefix = Key(getKeyRange().begin);
			return map(getWorkerInterfaces(ryw->getDatabase()->getConnectionRecord()),
			           [prefix = prefix, kr = KeyRange(kr)](const RangeResult& in) {
				           RangeResult result;
				           for (const auto& [k_, v] : in) {
					           auto k = k_.withPrefix(prefix);
					           if (kr.contains(k))
						           result.push_back_deep(result.arena(), KeyValueRef(k, v));
				           }

				           std::sort(result.begin(), result.end(), KeyValueRef::OrderByKey{});
				           return result;
			           });
		} else {
			return RangeResult();
		}
	}

	explicit WorkerInterfacesSpecialKeyImpl(KeyRangeRef kr) : SpecialKeyRangeReadImpl(kr) {}
};

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

	SingleSpecialKeyImpl(KeyRef k, const std::function<Future<Optional<Value>>(ReadYourWritesTransaction*)>& f)
	  : SpecialKeyRangeReadImpl(singleKeyRange(k)), k(k), f(f) {}

private:
	Key k;
	std::function<Future<Optional<Value>>(ReadYourWritesTransaction*)> f;
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
	if (kr.contains(LiteralStringRef("\xff\xff/metrics/health/aggregate")) && metrics.worstStorageDurabilityLag != 0) {
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
		result.push_back(result.arena(), KeyValueRef(LiteralStringRef("\xff\xff/metrics/health/aggregate"), bytes));
	}
	// tlog stats
	{
		int phase = 0; // Avoid comparing twice per loop iteration
		for (const auto& [uid, logStats] : metrics.tLogQueue) {
			StringRef k{
				StringRef(uid.toString()).withPrefix(LiteralStringRef("\xff\xff/metrics/health/log/"), result.arena())
			};
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
			StringRef k{ StringRef(uid.toString())
				             .withPrefix(LiteralStringRef("\xff\xff/metrics/health/storage/"), result.arena()) };
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
	    /*detailed ("per process")*/ kr.intersects(KeyRangeRef(LiteralStringRef("\xff\xff/metrics/health/storage/"),
	                                                           LiteralStringRef("\xff\xff/metrics/health/storage0"))) ||
	    kr.intersects(KeyRangeRef(LiteralStringRef("\xff\xff/metrics/health/log/"),
	                              LiteralStringRef("\xff\xff/metrics/health/log0")))));
	return healthMetricsToKVPairs(metrics, kr);
}

HealthMetricsRangeImpl::HealthMetricsRangeImpl(KeyRangeRef kr) : SpecialKeyRangeAsyncImpl(kr) {}

Future<RangeResult> HealthMetricsRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                     KeyRangeRef kr,
                                                     GetRangeLimits limitsHint) const {
	return healthMetricsGetRangeActor(ryw, kr);
}

KeyRangeRef toRelativeRange(KeyRangeRef range, KeyRef prefix) {
	if (prefix.empty()) {
		return range;
	} else {
		KeyRef begin = range.begin.startsWith(prefix) ? range.begin.removePrefix(prefix) : allKeys.begin;
		KeyRef end = range.end.startsWith(prefix) ? range.end.removePrefix(prefix) : allKeys.end;
		return KeyRangeRef(begin, end);
	}
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
                                 int apiVersion,
                                 IsSwitchable switchable,
                                 Optional<TenantName> defaultTenant)
  : lockAware(lockAware), switchable(switchable), connectionRecord(connectionRecord), proxyProvisional(false),
    clientLocality(clientLocality), enableLocalityLoadBalance(enableLocalityLoadBalance), defaultTenant(defaultTenant),
    internal(internal), cc("TransactionMetrics"), transactionReadVersions("ReadVersions", cc),
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
    transactionStatusRequests("StatusRequests", cc), transactionsTooOld("TooOld", cc),
    transactionsFutureVersions("FutureVersions", cc), transactionsNotCommitted("NotCommitted", cc),
    transactionsMaybeCommitted("MaybeCommitted", cc), transactionsResourceConstrained("ResourceConstrained", cc),
    transactionsProcessBehind("ProcessBehind", cc), transactionsThrottled("Throttled", cc),
    transactionsExpensiveClearCostEstCount("ExpensiveClearCostEstCount", cc),
    transactionGrvFullBatches("NumGrvFullBatches", cc), transactionGrvTimedOutBatches("NumGrvTimedOutBatches", cc),
    transactionCommitVersionNotFoundForSS("CommitVersionNotFoundForSS", cc), latencies(1000), readLatencies(1000),
    commitLatencies(1000), GRVLatencies(1000), mutationsPerCommit(1000), bytesPerCommit(1000), bgLatencies(1000),
    bgGranulesPerRequest(1000), outstandingWatches(0), sharedStatePtr(nullptr), lastGrvTime(0.0), cachedReadVersion(0),
    lastRkBatchThrottleTime(0.0), lastRkDefaultThrottleTime(0.0), lastProxyRequestTime(0.0),
    transactionTracingSample(false), taskID(taskID), clientInfo(clientInfo), clientInfoMonitor(clientInfoMonitor),
    coordinator(coordinator), apiVersion(apiVersion), mvCacheInsertLocation(0), healthMetricsLastUpdated(0),
    detailedHealthMetricsLastUpdated(0), smoothMidShardSize(CLIENT_KNOBS->SHARD_STAT_SMOOTH_AMOUNT),
    specialKeySpace(std::make_unique<SpecialKeySpace>(specialKeys.begin, specialKeys.end, /* test */ false)),
    connectToDatabaseEventCacheHolder(format("ConnectToDatabase/%s", dbId.toString().c_str())) {

	dbId = deterministicRandom()->randomUniqueID();

	TraceEvent("DatabaseContextCreated", dbId).backtrace();

	connected = (clientInfo->get().commitProxies.size() && clientInfo->get().grvProxies.size())
	                ? Void()
	                : clientInfo->onChange();

	metadataVersionCache.resize(CLIENT_KNOBS->METADATA_VERSION_CACHE_SIZE);
	maxOutstandingWatches = CLIENT_KNOBS->DEFAULT_MAX_OUTSTANDING_WATCHES;

	snapshotRywEnabled = apiVersionAtLeast(300) ? 1 : 0;

	logger = databaseLogger(this) && tssLogger(this);
	locationCacheSize = g_network->isSimulated() ? CLIENT_KNOBS->LOCATION_CACHE_EVICTION_SIZE_SIM
	                                             : CLIENT_KNOBS->LOCATION_CACHE_EVICTION_SIZE;
	tenantCacheSize = g_network->isSimulated() ? CLIENT_KNOBS->TENANT_CACHE_EVICTION_SIZE_SIM
	                                           : CLIENT_KNOBS->TENANT_CACHE_EVICTION_SIZE;

	getValueSubmitted.init(LiteralStringRef("NativeAPI.GetValueSubmitted"));
	getValueCompleted.init(LiteralStringRef("NativeAPI.GetValueCompleted"));

	clientDBInfoMonitor = monitorClientDBInfoChange(this, clientInfo, &proxiesChangeTrigger);
	tssMismatchHandler = handleTssMismatches(this);
	clientStatusUpdater.actor = clientStatusUpdateActor(this);
	cacheListMonitor = monitorCacheList(this);

	smoothMidShardSize.reset(CLIENT_KNOBS->INIT_MID_SHARD_BYTES);
	globalConfig = std::make_unique<GlobalConfig>(this);

	if (apiVersionAtLeast(710)) {
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<TenantMapRangeImpl>(SpecialKeySpace::getManagementApiCommandRange("tenantmap")));
	}
	if (apiVersionAtLeast(700)) {
		registerSpecialKeySpaceModule(SpecialKeySpace::MODULE::ERRORMSG,
		                              SpecialKeySpace::IMPLTYPE::READONLY,
		                              std::make_unique<SingleSpecialKeyImpl>(
		                                  SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin,
		                                  [](ReadYourWritesTransaction* ryw) -> Future<Optional<Value>> {
			                                  if (ryw->getSpecialKeySpaceErrorMsg().present())
				                                  return Optional<Value>(ryw->getSpecialKeySpaceErrorMsg().get());
			                                  else
				                                  return Optional<Value>();
		                                  }));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<ManagementCommandsOptionsImpl>(
		        KeyRangeRef(LiteralStringRef("options/"), LiteralStringRef("options0"))
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<ExcludeServersRangeImpl>(SpecialKeySpace::getManagementApiCommandRange("exclude")));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<FailedServersRangeImpl>(SpecialKeySpace::getManagementApiCommandRange("failed")));
		registerSpecialKeySpaceModule(SpecialKeySpace::MODULE::MANAGEMENT,
		                              SpecialKeySpace::IMPLTYPE::READWRITE,
		                              std::make_unique<ExcludedLocalitiesRangeImpl>(
		                                  SpecialKeySpace::getManagementApiCommandRange("excludedlocality")));
		registerSpecialKeySpaceModule(SpecialKeySpace::MODULE::MANAGEMENT,
		                              SpecialKeySpace::IMPLTYPE::READWRITE,
		                              std::make_unique<FailedLocalitiesRangeImpl>(
		                                  SpecialKeySpace::getManagementApiCommandRange("failedlocality")));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READONLY,
		    std::make_unique<ExclusionInProgressRangeImpl>(
		        KeyRangeRef(LiteralStringRef("in_progress_exclusion/"), LiteralStringRef("in_progress_exclusion0"))
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::CONFIGURATION,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<ProcessClassRangeImpl>(
		        KeyRangeRef(LiteralStringRef("process/class_type/"), LiteralStringRef("process/class_type0"))
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin)));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::CONFIGURATION,
		    SpecialKeySpace::IMPLTYPE::READONLY,
		    std::make_unique<ProcessClassSourceRangeImpl>(
		        KeyRangeRef(LiteralStringRef("process/class_source/"), LiteralStringRef("process/class_source0"))
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin)));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<LockDatabaseImpl>(
		        singleKeyRange(LiteralStringRef("db_locked"))
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<ConsistencyCheckImpl>(
		        singleKeyRange(LiteralStringRef("consistency_check_suspended"))
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::GLOBALCONFIG,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<GlobalConfigImpl>(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::GLOBALCONFIG)));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::TRACING,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<TracingOptionsImpl>(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::TRACING)));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::CONFIGURATION,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<CoordinatorsImpl>(
		        KeyRangeRef(LiteralStringRef("coordinators/"), LiteralStringRef("coordinators0"))
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin)));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READONLY,
		    std::make_unique<CoordinatorsAutoImpl>(
		        singleKeyRange(LiteralStringRef("auto_coordinators"))
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<AdvanceVersionImpl>(
		        singleKeyRange(LiteralStringRef("min_required_commit_version"))
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<VersionEpochImpl>(
		        singleKeyRange(LiteralStringRef("version_epoch"))
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<ClientProfilingImpl>(
		        KeyRangeRef(LiteralStringRef("profiling/"), LiteralStringRef("profiling0"))
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<MaintenanceImpl>(
		        KeyRangeRef(LiteralStringRef("maintenance/"), LiteralStringRef("maintenance0"))
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::MANAGEMENT,
		    SpecialKeySpace::IMPLTYPE::READWRITE,
		    std::make_unique<DataDistributionImpl>(
		        KeyRangeRef(LiteralStringRef("data_distribution/"), LiteralStringRef("data_distribution0"))
		            .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::ACTORLINEAGE,
		    SpecialKeySpace::IMPLTYPE::READONLY,
		    std::make_unique<ActorLineageImpl>(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ACTORLINEAGE)));
		registerSpecialKeySpaceModule(SpecialKeySpace::MODULE::ACTOR_PROFILER_CONF,
		                              SpecialKeySpace::IMPLTYPE::READWRITE,
		                              std::make_unique<ActorProfilerConf>(SpecialKeySpace::getModuleRange(
		                                  SpecialKeySpace::MODULE::ACTOR_PROFILER_CONF)));
	}
	if (apiVersionAtLeast(630)) {
		registerSpecialKeySpaceModule(SpecialKeySpace::MODULE::TRANSACTION,
		                              SpecialKeySpace::IMPLTYPE::READONLY,
		                              std::make_unique<ConflictingKeysImpl>(conflictingKeysRange));
		registerSpecialKeySpaceModule(SpecialKeySpace::MODULE::TRANSACTION,
		                              SpecialKeySpace::IMPLTYPE::READONLY,
		                              std::make_unique<ReadConflictRangeImpl>(readConflictRangeKeysRange));
		registerSpecialKeySpaceModule(SpecialKeySpace::MODULE::TRANSACTION,
		                              SpecialKeySpace::IMPLTYPE::READONLY,
		                              std::make_unique<WriteConflictRangeImpl>(writeConflictRangeKeysRange));
		registerSpecialKeySpaceModule(SpecialKeySpace::MODULE::METRICS,
		                              SpecialKeySpace::IMPLTYPE::READONLY,
		                              std::make_unique<DDStatsRangeImpl>(ddStatsRange));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::METRICS,
		    SpecialKeySpace::IMPLTYPE::READONLY,
		    std::make_unique<HealthMetricsRangeImpl>(KeyRangeRef(LiteralStringRef("\xff\xff/metrics/health/"),
		                                                         LiteralStringRef("\xff\xff/metrics/health0"))));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::WORKERINTERFACE,
		    SpecialKeySpace::IMPLTYPE::READONLY,
		    std::make_unique<WorkerInterfacesSpecialKeyImpl>(KeyRangeRef(
		        LiteralStringRef("\xff\xff/worker_interfaces/"), LiteralStringRef("\xff\xff/worker_interfaces0"))));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::STATUSJSON,
		    SpecialKeySpace::IMPLTYPE::READONLY,
		    std::make_unique<SingleSpecialKeyImpl>(LiteralStringRef("\xff\xff/status/json"),
		                                           [](ReadYourWritesTransaction* ryw) -> Future<Optional<Value>> {
			                                           if (ryw->getDatabase().getPtr() &&
			                                               ryw->getDatabase()->getConnectionRecord()) {
				                                           ++ryw->getDatabase()->transactionStatusRequests;
				                                           return getJSON(ryw->getDatabase());
			                                           } else {
				                                           return Optional<Value>();
			                                           }
		                                           }));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::CLUSTERFILEPATH,
		    SpecialKeySpace::IMPLTYPE::READONLY,
		    std::make_unique<SingleSpecialKeyImpl>(
		        LiteralStringRef("\xff\xff/cluster_file_path"),
		        [](ReadYourWritesTransaction* ryw) -> Future<Optional<Value>> {
			        try {
				        if (ryw->getDatabase().getPtr() && ryw->getDatabase()->getConnectionRecord()) {
					        Optional<Value> output =
					            StringRef(ryw->getDatabase()->getConnectionRecord()->getLocation());
					        return output;
				        }
			        } catch (Error& e) {
				        return e;
			        }
			        return Optional<Value>();
		        }));

		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::CONNECTIONSTRING,
		    SpecialKeySpace::IMPLTYPE::READONLY,
		    std::make_unique<SingleSpecialKeyImpl>(
		        LiteralStringRef("\xff\xff/connection_string"),
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
		        }));
	}
	throttleExpirer = recurring([this]() { expireThrottles(); }, CLIENT_KNOBS->TAG_THROTTLE_EXPIRATION_INTERVAL);

	if (BUGGIFY) {
		DatabaseContext::debugUseTags = true;
	}
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
    transactionStatusRequests("StatusRequests", cc), transactionsTooOld("TooOld", cc),
    transactionsFutureVersions("FutureVersions", cc), transactionsNotCommitted("NotCommitted", cc),
    transactionsMaybeCommitted("MaybeCommitted", cc), transactionsResourceConstrained("ResourceConstrained", cc),
    transactionsProcessBehind("ProcessBehind", cc), transactionsThrottled("Throttled", cc),
    transactionsExpensiveClearCostEstCount("ExpensiveClearCostEstCount", cc),
    transactionGrvFullBatches("NumGrvFullBatches", cc), transactionGrvTimedOutBatches("NumGrvTimedOutBatches", cc),
    transactionCommitVersionNotFoundForSS("CommitVersionNotFoundForSS", cc), latencies(1000), readLatencies(1000),
    commitLatencies(1000), GRVLatencies(1000), mutationsPerCommit(1000), bytesPerCommit(1000), bgLatencies(1000),
    bgGranulesPerRequest(1000), transactionTracingSample(false),
    smoothMidShardSize(CLIENT_KNOBS->SHARD_STAT_SMOOTH_AMOUNT),
    connectToDatabaseEventCacheHolder(format("ConnectToDatabase/%s", dbId.toString().c_str())) {}

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

	TraceEvent("DatabaseContextDestructed", dbId).backtrace();
}

Optional<KeyRangeLocationInfo> DatabaseContext::getCachedLocation(const Optional<TenantName>& tenantName,
                                                                  const KeyRef& key,
                                                                  Reverse isBackward) {
	TenantMapEntry tenantEntry;
	Arena arena;
	KeyRef resolvedKey = key;

	if (tenantName.present()) {
		auto itr = tenantCache.find(tenantName.get());
		if (itr != tenantCache.end()) {
			tenantEntry = itr->second;
			resolvedKey = resolvedKey.withPrefix(tenantEntry.prefix, arena);
		} else {
			return Optional<KeyRangeLocationInfo>();
		}
	}

	auto range =
	    isBackward ? locationCache.rangeContainingKeyBefore(resolvedKey) : locationCache.rangeContaining(resolvedKey);
	if (range->value()) {
		return KeyRangeLocationInfo(tenantEntry, toRelativeRange(range->range(), tenantEntry.prefix), range->value());
	}

	return Optional<KeyRangeLocationInfo>();
}

bool DatabaseContext::getCachedLocations(const Optional<TenantName>& tenantName,
                                         const KeyRangeRef& range,
                                         std::vector<KeyRangeLocationInfo>& result,
                                         int limit,
                                         Reverse reverse) {
	result.clear();

	TenantMapEntry tenantEntry;
	Arena arena;
	KeyRangeRef resolvedRange = range;

	if (tenantName.present()) {
		auto itr = tenantCache.find(tenantName.get());
		if (itr != tenantCache.end()) {
			tenantEntry = itr->second;
			resolvedRange = resolvedRange.withPrefix(tenantEntry.prefix, arena);
		} else {
			return false;
		}
	}

	auto begin = locationCache.rangeContaining(resolvedRange.begin);
	auto end = locationCache.rangeContainingKeyBefore(resolvedRange.end);

	loop {
		auto r = reverse ? end : begin;
		if (!r->value()) {
			TEST(result.size()); // had some but not all cached locations
			result.clear();
			return false;
		}
		result.emplace_back(tenantEntry, toRelativeRange(r->range() & resolvedRange, tenantEntry.prefix), r->value());
		if (result.size() == limit || begin == end) {
			break;
		}

		if (reverse)
			--end;
		else
			++begin;
	}

	return true;
}

void DatabaseContext::cacheTenant(const TenantName& tenant, const TenantMapEntry& tenantEntry) {
	if (tenantCacheSize > 0) {
		// Naive cache eviction just erases the entire cache when it gets full.
		// We don't expect a single client to fill the tenant cache typically, so this should work reasonably well.
		if (tenantCache.size() > tenantCacheSize) {
			tenantCache.clear();
		}

		tenantCache[tenant] = tenantEntry;
	}
}

Reference<LocationInfo> DatabaseContext::setCachedLocation(const Optional<TenantName>& tenant,
                                                           const TenantMapEntry& tenantEntry,
                                                           const KeyRangeRef& absoluteKeys,
                                                           const std::vector<StorageServerInterface>& servers) {
	if (tenant.present()) {
		cacheTenant(tenant.get(), tenantEntry);
	}

	std::vector<Reference<ReferencedInterface<StorageServerInterface>>> serverRefs;
	serverRefs.reserve(servers.size());
	for (const auto& interf : servers) {
		serverRefs.push_back(StorageServerInfo::getInterface(this, interf, clientLocality));
	}

	int maxEvictionAttempts = 100, attempts = 0;
	auto loc = makeReference<LocationInfo>(serverRefs);
	while (locationCache.size() > locationCacheSize && attempts < maxEvictionAttempts) {
		TEST(true); // NativeAPI storage server locationCache entry evicted
		attempts++;
		auto r = locationCache.randomRange();
		Key begin = r.begin(), end = r.end(); // insert invalidates r, so can't be passed a mere reference into it
		locationCache.insert(KeyRangeRef(begin, end), Reference<LocationInfo>());
	}
	locationCache.insert(absoluteKeys, loc);
	return loc;
}

void DatabaseContext::invalidateCachedTenant(const TenantNameRef& tenant) {
	tenantCache.erase(tenant);
}

void DatabaseContext::invalidateCache(const KeyRef& tenantPrefix, const KeyRef& key, Reverse isBackward) {
	Arena arena;
	KeyRef resolvedKey = key;
	if (!tenantPrefix.empty()) {
		resolvedKey = resolvedKey.withPrefix(tenantPrefix, arena);
	}

	if (isBackward) {
		locationCache.rangeContainingKeyBefore(resolvedKey)->value() = Reference<LocationInfo>();
	} else {
		locationCache.rangeContaining(resolvedKey)->value() = Reference<LocationInfo>();
	}
}

void DatabaseContext::invalidateCache(const KeyRef& tenantPrefix, const KeyRangeRef& keys) {
	Arena arena;
	KeyRangeRef resolvedKeys = keys;
	if (!tenantPrefix.empty()) {
		resolvedKeys = resolvedKeys.withPrefix(tenantPrefix, arena);
	}

	auto rs = locationCache.intersectingRanges(resolvedKeys);
	Key begin = rs.begin().begin(),
	    end = rs.end().begin(); // insert invalidates rs, so can't be passed a mere reference into it
	locationCache.insert(KeyRangeRef(begin, end), Reference<LocationInfo>());
}

void DatabaseContext::setFailedEndpointOnHealthyServer(const Endpoint& endpoint) {
	if (failedEndpointsOnHealthyServersInfo.find(endpoint) == failedEndpointsOnHealthyServersInfo.end()) {
		failedEndpointsOnHealthyServersInfo[endpoint] =
		    EndpointFailureInfo{ .startTime = now(), .lastRefreshTime = now() };
	}
}

void DatabaseContext::updateFailedEndpointRefreshTime(const Endpoint& endpoint) {
	if (failedEndpointsOnHealthyServersInfo.find(endpoint) == failedEndpointsOnHealthyServersInfo.end()) {
		// The endpoint is not failed. Nothing to update.
		return;
	}
	failedEndpointsOnHealthyServersInfo[endpoint].lastRefreshTime = now();
}

Optional<EndpointFailureInfo> DatabaseContext::getEndpointFailureInfo(const Endpoint& endpoint) {
	if (failedEndpointsOnHealthyServersInfo.find(endpoint) == failedEndpointsOnHealthyServersInfo.end()) {
		return Optional<EndpointFailureInfo>();
	}
	return failedEndpointsOnHealthyServersInfo[endpoint];
}

void DatabaseContext::clearFailedEndpointOnHealthyServer(const Endpoint& endpoint) {
	failedEndpointsOnHealthyServersInfo.erase(endpoint);
}

Future<Void> DatabaseContext::onProxiesChanged() {
	backoffDelay = 0.0;
	return this->proxiesChangeTrigger.onTrigger();
}

bool DatabaseContext::sampleReadTags() const {
	double sampleRate = globalConfig->get(transactionTagSampleRate, CLIENT_KNOBS->READ_TAG_SAMPLE_RATE);
	return sampleRate > 0 && deterministicRandom()->random01() <= sampleRate;
}

bool DatabaseContext::sampleOnCost(uint64_t cost) const {
	double sampleCost = globalConfig->get<double>(transactionTagSampleCost, CLIENT_KNOBS->COMMIT_SAMPLE_COST);
	if (sampleCost <= 0)
		return false;
	return deterministicRandom()->random01() <= (double)cost / sampleCost;
}

int64_t extractIntOption(Optional<StringRef> value, int64_t minValue, int64_t maxValue) {
	validateOptionValuePresent(value);
	if (value.get().size() != 8) {
		throw invalid_option_value();
	}

	int64_t passed = *((int64_t*)(value.get().begin()));
	if (passed > maxValue || passed < minValue) {
		throw invalid_option_value();
	}

	return passed;
}

uint64_t extractHexOption(StringRef value) {
	char* end;
	uint64_t id = strtoull(value.toString().c_str(), &end, 16);
	if (*end)
		throw invalid_option_value();
	return id;
}

void DatabaseContext::setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value) {
	int defaultFor = FDBDatabaseOptions::optionInfo.getMustExist(option).defaultFor;
	if (defaultFor >= 0) {
		ASSERT(FDBTransactionOptions::optionInfo.find((FDBTransactionOptions::Option)defaultFor) !=
		       FDBTransactionOptions::optionInfo.end());
		TraceEvent(SevDebug, "DatabaseContextSetPersistentOption").detail("Option", option).detail("Value", value);
		transactionDefaults.addOption((FDBTransactionOptions::Option)defaultFor, value.castTo<Standalone<StringRef>>());
	} else {
		switch (option) {
		case FDBDatabaseOptions::LOCATION_CACHE_SIZE:
			locationCacheSize = (int)extractIntOption(value, 0, std::numeric_limits<int>::max());
			break;
		case FDBDatabaseOptions::MACHINE_ID:
			clientLocality =
			    LocalityData(clientLocality.processId(),
			                 value.present() ? Standalone<StringRef>(value.get()) : Optional<Standalone<StringRef>>(),
			                 clientLocality.machineId(),
			                 clientLocality.dcId());
			if (clientInfo->get().commitProxies.size())
				commitProxies = makeReference<CommitProxyInfo>(clientInfo->get().commitProxies);
			if (clientInfo->get().grvProxies.size())
				grvProxies = makeReference<GrvProxyInfo>(clientInfo->get().grvProxies, BalanceOnRequests::True);
			server_interf.clear();
			locationCache.insert(allKeys, Reference<LocationInfo>());
			break;
		case FDBDatabaseOptions::MAX_WATCHES:
			maxOutstandingWatches = (int)extractIntOption(value, 0, CLIENT_KNOBS->ABSOLUTE_MAX_WATCHES);
			break;
		case FDBDatabaseOptions::DATACENTER_ID:
			clientLocality =
			    LocalityData(clientLocality.processId(),
			                 clientLocality.zoneId(),
			                 clientLocality.machineId(),
			                 value.present() ? Standalone<StringRef>(value.get()) : Optional<Standalone<StringRef>>());
			if (clientInfo->get().commitProxies.size())
				commitProxies = makeReference<CommitProxyInfo>(clientInfo->get().commitProxies);
			if (clientInfo->get().grvProxies.size())
				grvProxies = makeReference<GrvProxyInfo>(clientInfo->get().grvProxies, BalanceOnRequests::True);
			server_interf.clear();
			locationCache.insert(allKeys, Reference<LocationInfo>());
			break;
		case FDBDatabaseOptions::SNAPSHOT_RYW_ENABLE:
			validateOptionValueNotPresent(value);
			snapshotRywEnabled++;
			break;
		case FDBDatabaseOptions::SNAPSHOT_RYW_DISABLE:
			validateOptionValueNotPresent(value);
			snapshotRywEnabled--;
			break;
		case FDBDatabaseOptions::USE_CONFIG_DATABASE:
			validateOptionValueNotPresent(value);
			useConfigDatabase = true;
			break;
		case FDBDatabaseOptions::TEST_CAUSAL_READ_RISKY:
			verifyCausalReadsProp = double(extractIntOption(value, 0, 100)) / 100.0;
			break;
		default:
			break;
		}
	}
}

void DatabaseContext::addWatch() {
	if (outstandingWatches >= maxOutstandingWatches)
		throw too_many_watches();

	++outstandingWatches;
}

void DatabaseContext::removeWatch() {
	--outstandingWatches;
	ASSERT(outstandingWatches >= 0);
}

Future<Void> DatabaseContext::onConnected() {
	return connected;
}

ACTOR static Future<Void> switchConnectionRecordImpl(Reference<IClusterConnectionRecord> connRecord,
                                                     DatabaseContext* self) {
	TEST(true); // Switch connection file
	TraceEvent("SwitchConnectionRecord")
	    .detail("ClusterFile", connRecord->toString())
	    .detail("ConnectionString", connRecord->getConnectionString().toString());

	// Reset state from former cluster.
	self->commitProxies.clear();
	self->grvProxies.clear();
	self->minAcceptableReadVersion = std::numeric_limits<Version>::max();
	self->tenantCache.clear();
	self->invalidateCache(Key(), allKeys);

	self->ssVersionVectorCache.clear();

	auto clearedClientInfo = self->clientInfo->get();
	clearedClientInfo.commitProxies.clear();
	clearedClientInfo.grvProxies.clear();
	clearedClientInfo.id = deterministicRandom()->randomUniqueID();
	self->clientInfo->set(clearedClientInfo);
	self->connectionRecord->set(connRecord);

	state Database db(Reference<DatabaseContext>::addRef(self));
	state Transaction tr(db);
	loop {
		tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
		try {
			TraceEvent("SwitchConnectionRecordAttemptingGRV").log();
			Version v = wait(tr.getReadVersion());
			TraceEvent("SwitchConnectionRecordGotRV")
			    .detail("ReadVersion", v)
			    .detail("MinAcceptableReadVersion", self->minAcceptableReadVersion);
			ASSERT(self->minAcceptableReadVersion != std::numeric_limits<Version>::max());
			self->connectionFileChangedTrigger.trigger();
			return Void();
		} catch (Error& e) {
			TraceEvent("SwitchConnectionRecordError").detail("Error", e.what());
			wait(tr.onError(e));
		}
	}
}

Reference<IClusterConnectionRecord> DatabaseContext::getConnectionRecord() {
	if (connectionRecord) {
		return connectionRecord->get();
	}
	return Reference<IClusterConnectionRecord>();
}

Future<Void> DatabaseContext::switchConnectionRecord(Reference<IClusterConnectionRecord> standby) {
	ASSERT(switchable);
	return switchConnectionRecordImpl(standby, this);
}

Future<Void> DatabaseContext::connectionFileChanged() {
	return connectionFileChangedTrigger.onTrigger();
}

void DatabaseContext::expireThrottles() {
	for (auto& priorityItr : throttledTags) {
		for (auto tagItr = priorityItr.second.begin(); tagItr != priorityItr.second.end();) {
			if (tagItr->second.expired()) {
				TEST(true); // Expiring client throttle
				tagItr = priorityItr.second.erase(tagItr);
			} else {
				++tagItr;
			}
		}
	}
}

extern IPAddress determinePublicIPAutomatically(ClusterConnectionString& ccs);

// Creates a database object that represents a connection to a cluster
// This constructor uses a preallocated DatabaseContext that may have been created
// on another thread
Database Database::createDatabase(Reference<IClusterConnectionRecord> connRecord,
                                  int apiVersion,
                                  IsInternal internal,
                                  LocalityData const& clientLocality,
                                  DatabaseContext* preallocatedDb) {
	if (!g_network)
		throw network_not_setup();

	ASSERT(TraceEvent::isNetworkThread());

	platform::ImageInfo imageInfo = platform::getImageInfo();

	if (connRecord) {
		if (networkOptions.traceDirectory.present() && !traceFileIsOpen()) {
			g_network->initMetrics();
			FlowTransport::transport().initMetrics();
			initTraceEventMetrics();

			auto publicIP = determinePublicIPAutomatically(connRecord->getConnectionString());
			selectTraceFormatter(networkOptions.traceFormat);
			selectTraceClockSource(networkOptions.traceClockSource);
			addUniversalTraceField("ClientDescription",
			                       format("%s-%s-%" PRIu64,
			                              networkOptions.primaryClient ? "primary" : "external",
			                              FDB_VT_VERSION,
			                              getTraceThreadId()));

			openTraceFile(NetworkAddress(publicIP, ::getpid()),
			              networkOptions.traceRollSize,
			              networkOptions.traceMaxLogsSize,
			              networkOptions.traceDirectory.get(),
			              "trace",
			              networkOptions.traceLogGroup,
			              networkOptions.traceFileIdentifier,
			              networkOptions.tracePartialFileSuffix);

			TraceEvent("ClientStart")
			    .detail("SourceVersion", getSourceVersion())
			    .detail("Version", FDB_VT_VERSION)
			    .detail("PackageName", FDB_VT_PACKAGE_NAME)
			    .detailf("ActualTime", "%lld", DEBUG_DETERMINISM ? 0 : time(nullptr))
			    .detail("ApiVersion", apiVersion)
			    .detail("ClientLibrary", imageInfo.fileName)
			    .detailf("ImageOffset", "%p", imageInfo.offset)
			    .detail("Primary", networkOptions.primaryClient)
			    .trackLatest("ClientStart");

			initializeSystemMonitorMachineState(SystemMonitorMachineState(IPAddress(publicIP)));

			systemMonitor();
			uncancellable(recurring(&systemMonitor, CLIENT_KNOBS->SYSTEM_MONITOR_INTERVAL, TaskPriority::FlushTrace));
		}
	}

	g_network->initTLS();

	auto clientInfo = makeReference<AsyncVar<ClientDBInfo>>();
	auto coordinator = makeReference<AsyncVar<Optional<ClientLeaderRegInterface>>>();
	auto connectionRecord = makeReference<AsyncVar<Reference<IClusterConnectionRecord>>>();
	connectionRecord->set(connRecord);
	Future<Void> clientInfoMonitor = monitorProxies(connectionRecord,
	                                                clientInfo,
	                                                coordinator,
	                                                networkOptions.supportedVersions,
	                                                StringRef(networkOptions.traceLogGroup),
	                                                internal);

	DatabaseContext* db;
	if (preallocatedDb) {
		db = new (preallocatedDb) DatabaseContext(connectionRecord,
		                                          clientInfo,
		                                          coordinator,
		                                          clientInfoMonitor,
		                                          TaskPriority::DefaultEndpoint,
		                                          clientLocality,
		                                          EnableLocalityLoadBalance::True,
		                                          LockAware::False,
		                                          internal,
		                                          apiVersion,
		                                          IsSwitchable::True);
	} else {
		db = new DatabaseContext(connectionRecord,
		                         clientInfo,
		                         coordinator,
		                         clientInfoMonitor,
		                         TaskPriority::DefaultEndpoint,
		                         clientLocality,
		                         EnableLocalityLoadBalance::True,
		                         LockAware::False,
		                         internal,
		                         apiVersion,
		                         IsSwitchable::True);
	}

	auto database = Database(db);
	database->globalConfig->init(Reference<AsyncVar<ClientDBInfo> const>(clientInfo),
	                             std::addressof(clientInfo->get()));
	database->globalConfig->trigger(samplingFrequency, samplingProfilerUpdateFrequency);
	database->globalConfig->trigger(samplingWindow, samplingProfilerUpdateWindow);

	TraceEvent("ConnectToDatabase", database->dbId)
	    .detail("Version", FDB_VT_VERSION)
	    .detail("ClusterFile", connRecord ? connRecord->toString() : "None")
	    .detail("ConnectionString", connRecord ? connRecord->getConnectionString().toString() : "None")
	    .detail("ClientLibrary", imageInfo.fileName)
	    .detail("Primary", networkOptions.primaryClient)
	    .detail("Internal", internal)
	    .trackLatest(database->connectToDatabaseEventCacheHolder.trackingKey);

	return database;
}

Database Database::createDatabase(std::string connFileName,
                                  int apiVersion,
                                  IsInternal internal,
                                  LocalityData const& clientLocality) {
	Reference<IClusterConnectionRecord> rccr = Reference<IClusterConnectionRecord>(
	    new ClusterConnectionFile(ClusterConnectionFile::lookupClusterFileName(connFileName).first));
	return Database::createDatabase(rccr, apiVersion, internal, clientLocality);
}

Reference<WatchMetadata> DatabaseContext::getWatchMetadata(int64_t tenantId, KeyRef key) const {
	const auto it = watchMap.find(std::make_pair(tenantId, key));
	if (it == watchMap.end())
		return Reference<WatchMetadata>();
	return it->second;
}

void DatabaseContext::setWatchMetadata(Reference<WatchMetadata> metadata) {
	watchMap[std::make_pair(metadata->parameters->tenant.tenantId, metadata->parameters->key)] = metadata;
}

void DatabaseContext::deleteWatchMetadata(int64_t tenantId, KeyRef key) {
	watchMap.erase(std::make_pair(tenantId, key));
}

void DatabaseContext::clearWatchMetadata() {
	watchMap.clear();
}

const UniqueOrderedOptionList<FDBTransactionOptions>& Database::getTransactionDefaults() const {
	ASSERT(db);
	return db->transactionDefaults;
}

void setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value) {
	std::regex identifierRegex("^[a-zA-Z0-9_]*$");
	switch (option) {
	// SOMEDAY: If the network is already started, should these five throw an error?
	case FDBNetworkOptions::TRACE_ENABLE:
		networkOptions.traceDirectory = value.present() ? value.get().toString() : "";
		break;
	case FDBNetworkOptions::TRACE_ROLL_SIZE:
		validateOptionValuePresent(value);
		networkOptions.traceRollSize = extractIntOption(value, 0, std::numeric_limits<int64_t>::max());
		break;
	case FDBNetworkOptions::TRACE_MAX_LOGS_SIZE:
		validateOptionValuePresent(value);
		networkOptions.traceMaxLogsSize = extractIntOption(value, 0, std::numeric_limits<int64_t>::max());
		break;
	case FDBNetworkOptions::TRACE_FORMAT:
		validateOptionValuePresent(value);
		networkOptions.traceFormat = value.get().toString();
		if (!validateTraceFormat(networkOptions.traceFormat)) {
			fprintf(stderr, "Unrecognized trace format: `%s'\n", networkOptions.traceFormat.c_str());
			throw invalid_option_value();
		}
		break;
	case FDBNetworkOptions::TRACE_FILE_IDENTIFIER:
		validateOptionValuePresent(value);
		networkOptions.traceFileIdentifier = value.get().toString();
		if (networkOptions.traceFileIdentifier.length() > CLIENT_KNOBS->TRACE_LOG_FILE_IDENTIFIER_MAX_LENGTH) {
			fprintf(stderr, "Trace file identifier provided is too long.\n");
			throw invalid_option_value();
		} else if (!std::regex_match(networkOptions.traceFileIdentifier, identifierRegex)) {
			fprintf(stderr, "Trace file identifier should only contain alphanumerics and underscores.\n");
			throw invalid_option_value();
		}
		break;

	case FDBNetworkOptions::TRACE_LOG_GROUP:
		if (value.present()) {
			if (traceFileIsOpen()) {
				setTraceLogGroup(value.get().toString());
			} else {
				networkOptions.traceLogGroup = value.get().toString();
			}
		}
		break;
	case FDBNetworkOptions::TRACE_CLOCK_SOURCE:
		validateOptionValuePresent(value);
		networkOptions.traceClockSource = value.get().toString();
		if (!validateTraceClockSource(networkOptions.traceClockSource)) {
			fprintf(stderr, "Unrecognized trace clock source: `%s'\n", networkOptions.traceClockSource.c_str());
			throw invalid_option_value();
		}
		break;
	case FDBNetworkOptions::TRACE_PARTIAL_FILE_SUFFIX:
		validateOptionValuePresent(value);
		networkOptions.tracePartialFileSuffix = value.get().toString();
		break;
	case FDBNetworkOptions::KNOB: {
		validateOptionValuePresent(value);

		std::string optionValue = value.get().toString();
		TraceEvent("SetKnob").detail("KnobString", optionValue);

		size_t eq = optionValue.find_first_of('=');
		if (eq == optionValue.npos) {
			TraceEvent(SevWarnAlways, "InvalidKnobString").detail("KnobString", optionValue);
			throw invalid_option_value();
		}

		std::string knobName = optionValue.substr(0, eq);
		std::string knobValueString = optionValue.substr(eq + 1);

		try {
			auto knobValue = IKnobCollection::parseKnobValue(knobName, knobValueString, IKnobCollection::Type::CLIENT);
			if (g_network) {
				IKnobCollection::getMutableGlobalKnobCollection().setKnob(knobName, knobValue);
			} else {
				networkOptions.knobs[knobName] = knobValue;
			}
		} catch (Error& e) {
			TraceEvent(SevWarnAlways, "UnrecognizedKnob").detail("Knob", knobName.c_str());
			fprintf(stderr, "FoundationDB client ignoring unrecognized knob option '%s'\n", knobName.c_str());
		}
		break;
	}
	case FDBNetworkOptions::TLS_PLUGIN:
		validateOptionValuePresent(value);
		break;
	case FDBNetworkOptions::TLS_CERT_PATH:
		validateOptionValuePresent(value);
		tlsConfig.setCertificatePath(value.get().toString());
		break;
	case FDBNetworkOptions::TLS_CERT_BYTES: {
		validateOptionValuePresent(value);
		tlsConfig.setCertificateBytes(value.get().toString());
		break;
	}
	case FDBNetworkOptions::TLS_CA_PATH: {
		validateOptionValuePresent(value);
		tlsConfig.setCAPath(value.get().toString());
		break;
	}
	case FDBNetworkOptions::TLS_CA_BYTES: {
		validateOptionValuePresent(value);
		tlsConfig.setCABytes(value.get().toString());
		break;
	}
	case FDBNetworkOptions::TLS_PASSWORD:
		validateOptionValuePresent(value);
		tlsConfig.setPassword(value.get().toString());
		break;
	case FDBNetworkOptions::TLS_KEY_PATH:
		validateOptionValuePresent(value);
		tlsConfig.setKeyPath(value.get().toString());
		break;
	case FDBNetworkOptions::TLS_KEY_BYTES: {
		validateOptionValuePresent(value);
		tlsConfig.setKeyBytes(value.get().toString());
		break;
	}
	case FDBNetworkOptions::TLS_VERIFY_PEERS:
		validateOptionValuePresent(value);
		tlsConfig.clearVerifyPeers();
		tlsConfig.addVerifyPeers(value.get().toString());
		break;
	case FDBNetworkOptions::CLIENT_BUGGIFY_ENABLE:
		enableBuggify(true, BuggifyType::Client);
		break;
	case FDBNetworkOptions::CLIENT_BUGGIFY_DISABLE:
		enableBuggify(false, BuggifyType::Client);
		break;
	case FDBNetworkOptions::CLIENT_BUGGIFY_SECTION_ACTIVATED_PROBABILITY:
		validateOptionValuePresent(value);
		clearBuggifySections(BuggifyType::Client);
		P_BUGGIFIED_SECTION_ACTIVATED[int(BuggifyType::Client)] = double(extractIntOption(value, 0, 100)) / 100.0;
		break;
	case FDBNetworkOptions::CLIENT_BUGGIFY_SECTION_FIRED_PROBABILITY:
		validateOptionValuePresent(value);
		P_BUGGIFIED_SECTION_FIRES[int(BuggifyType::Client)] = double(extractIntOption(value, 0, 100)) / 100.0;
		break;
	case FDBNetworkOptions::DISABLE_CLIENT_STATISTICS_LOGGING:
		validateOptionValueNotPresent(value);
		networkOptions.logClientInfo = false;
		break;
	case FDBNetworkOptions::SUPPORTED_CLIENT_VERSIONS: {
		// The multi-version API should be providing us these guarantees
		ASSERT(g_network);
		ASSERT(value.present());

		Standalone<VectorRef<ClientVersionRef>> supportedVersions;
		std::vector<StringRef> supportedVersionsStrings = value.get().splitAny(LiteralStringRef(";"));
		for (StringRef versionString : supportedVersionsStrings) {
#ifdef ADDRESS_SANITIZER
			__lsan_disable();
#endif
			// LSAN reports that we leak this allocation in client
			// tests, but I cannot seem to figure out why. AFAICT
			// it's not actually leaking. If it is a leak, it's only a few bytes.
			supportedVersions.push_back_deep(supportedVersions.arena(), ClientVersionRef(versionString));
#ifdef ADDRESS_SANITIZER
			__lsan_enable();
#endif
		}

		ASSERT(supportedVersions.size() > 0);
		networkOptions.supportedVersions->set(supportedVersions);

		break;
	}
	case FDBNetworkOptions::ENABLE_RUN_LOOP_PROFILING: // Same as ENABLE_SLOW_TASK_PROFILING
		validateOptionValueNotPresent(value);
		networkOptions.runLoopProfilingEnabled = true;
		break;
	case FDBNetworkOptions::DISTRIBUTED_CLIENT_TRACER: {
		validateOptionValuePresent(value);
		std::string tracer = value.get().toString();
		if (tracer == "none" || tracer == "disabled") {
			openTracer(TracerType::DISABLED);
		} else if (tracer == "logfile" || tracer == "file" || tracer == "log_file") {
			openTracer(TracerType::LOG_FILE);
		} else if (tracer == "network_lossy") {
			openTracer(TracerType::NETWORK_LOSSY);
		} else {
			fprintf(stderr, "ERROR: Unknown or unsupported tracer: `%s'", tracer.c_str());
			throw invalid_option_value();
		}
		break;
	}
	case FDBNetworkOptions::EXTERNAL_CLIENT:
		networkOptions.primaryClient = false;
		break;
	default:
		break;
	}
}

// update the network busyness on a 1s cadence
ACTOR Future<Void> monitorNetworkBusyness() {
	state double prevTime = now();
	loop {
		wait(delay(CLIENT_KNOBS->NETWORK_BUSYNESS_MONITOR_INTERVAL, TaskPriority::FlushTrace));
		double elapsed = now() - prevTime; // get elapsed time from last execution
		prevTime = now();
		struct NetworkMetrics::PriorityStats& tracker = g_network->networkInfo.metrics.starvationTrackerNetworkBusyness;

		if (tracker.active) { // update metrics
			tracker.duration += now() - tracker.windowedTimer;
			tracker.maxDuration = std::max(tracker.maxDuration, now() - tracker.timer);
			tracker.windowedTimer = now();
		}

		double busyFraction = std::min(elapsed, tracker.duration) / elapsed;

		// The burstiness score is an indicator of the maximum busyness spike over the measurement interval.
		// It scales linearly from 0 to 1 as the largest burst goes from the start to the saturation threshold.
		// This allows us to account for saturation that happens in smaller bursts than the measurement interval.
		//
		// Burstiness will not be calculated if the saturation threshold is smaller than the start threshold or
		// if either value is negative.
		double burstiness = 0;
		if (CLIENT_KNOBS->BUSYNESS_SPIKE_START_THRESHOLD >= 0 &&
		    CLIENT_KNOBS->BUSYNESS_SPIKE_SATURATED_THRESHOLD >= CLIENT_KNOBS->BUSYNESS_SPIKE_START_THRESHOLD) {
			burstiness = std::min(1.0,
			                      std::max(0.0, tracker.maxDuration - CLIENT_KNOBS->BUSYNESS_SPIKE_START_THRESHOLD) /
			                          std::max(1e-6,
			                                   CLIENT_KNOBS->BUSYNESS_SPIKE_SATURATED_THRESHOLD -
			                                       CLIENT_KNOBS->BUSYNESS_SPIKE_START_THRESHOLD));
		}

		g_network->networkInfo.metrics.networkBusyness = std::max(busyFraction, burstiness);

		tracker.duration = 0;
		tracker.maxDuration = 0;
	}
}

static void setupGlobalKnobs() {
	IKnobCollection::setGlobalKnobCollection(IKnobCollection::Type::CLIENT, Randomize::False, IsSimulated::False);
	for (const auto& [knobName, knobValue] : networkOptions.knobs) {
		IKnobCollection::getMutableGlobalKnobCollection().setKnob(knobName, knobValue);
	}
}

// Setup g_network and start monitoring for network busyness
void setupNetwork(uint64_t transportId, UseMetrics useMetrics) {
	if (g_network)
		throw network_already_setup();

	if (!networkOptions.logClientInfo.present())
		networkOptions.logClientInfo = true;

	setupGlobalKnobs();
	g_network = newNet2(tlsConfig, false, useMetrics || networkOptions.traceDirectory.present());
	g_network->addStopCallback(Net2FileSystem::stop);
	FlowTransport::createInstance(true, transportId, WLTOKEN_RESERVED_COUNT);
	Net2FileSystem::newFileSystem();

	uncancellable(monitorNetworkBusyness());
}

void runNetwork() {
	if (!g_network) {
		throw network_not_setup();
	}

	if (!g_network->checkRunnable()) {
		throw network_cannot_be_restarted();
	}

	if (networkOptions.traceDirectory.present() && networkOptions.runLoopProfilingEnabled) {
		setupRunLoopProfiler();
	}

	g_network->run();

	if (networkOptions.traceDirectory.present())
		systemMonitor();
}

void stopNetwork() {
	if (!g_network)
		throw network_not_setup();

	TraceEvent("ClientStopNetwork");
	g_network->stop();
	closeTraceFile();
}

void DatabaseContext::updateProxies() {
	if (proxiesLastChange == clientInfo->get().id)
		return;
	proxiesLastChange = clientInfo->get().id;
	commitProxies.clear();
	grvProxies.clear();
	ssVersionVectorCache.clear();
	bool commitProxyProvisional = false, grvProxyProvisional = false;
	if (clientInfo->get().commitProxies.size()) {
		commitProxies = makeReference<CommitProxyInfo>(clientInfo->get().commitProxies);
		commitProxyProvisional = clientInfo->get().commitProxies[0].provisional;
	}
	if (clientInfo->get().grvProxies.size()) {
		grvProxies = makeReference<GrvProxyInfo>(clientInfo->get().grvProxies, BalanceOnRequests::True);
		grvProxyProvisional = clientInfo->get().grvProxies[0].provisional;
	}
	if (clientInfo->get().commitProxies.size() && clientInfo->get().grvProxies.size()) {
		ASSERT(commitProxyProvisional == grvProxyProvisional);
		proxyProvisional = commitProxyProvisional;
	}
}

Reference<CommitProxyInfo> DatabaseContext::getCommitProxies(UseProvisionalProxies useProvisionalProxies) {
	updateProxies();
	if (proxyProvisional && !useProvisionalProxies) {
		return Reference<CommitProxyInfo>();
	}
	return commitProxies;
}

Reference<GrvProxyInfo> DatabaseContext::getGrvProxies(UseProvisionalProxies useProvisionalProxies) {
	updateProxies();
	if (proxyProvisional && !useProvisionalProxies) {
		return Reference<GrvProxyInfo>();
	}
	return grvProxies;
}

bool DatabaseContext::isCurrentGrvProxy(UID proxyId) const {
	for (const auto& proxy : clientInfo->get().grvProxies) {
		if (proxy.id() == proxyId)
			return true;
	}
	TEST(true); // stale GRV proxy detected
	return false;
}

// Actor which will wait until the MultiInterface<CommitProxyInterface> returned by the DatabaseContext cx is not
// nullptr
ACTOR Future<Reference<CommitProxyInfo>> getCommitProxiesFuture(DatabaseContext* cx,
                                                                UseProvisionalProxies useProvisionalProxies) {
	loop {
		Reference<CommitProxyInfo> commitProxies = cx->getCommitProxies(useProvisionalProxies);
		if (commitProxies)
			return commitProxies;
		wait(cx->onProxiesChanged());
	}
}

// Returns a future which will not be set until the CommitProxyInfo of this DatabaseContext is not nullptr
Future<Reference<CommitProxyInfo>> DatabaseContext::getCommitProxiesFuture(
    UseProvisionalProxies useProvisionalProxies) {
	return ::getCommitProxiesFuture(this, useProvisionalProxies);
}

void GetRangeLimits::decrement(VectorRef<KeyValueRef> const& data) {
	if (rows != GetRangeLimits::ROW_LIMIT_UNLIMITED) {
		ASSERT(data.size() <= rows);
		rows -= data.size();
	}

	minRows = std::max(0, minRows - data.size());

	if (bytes != GetRangeLimits::BYTE_LIMIT_UNLIMITED)
		bytes = std::max(0, bytes - (int)data.expectedSize() - (8 - (int)sizeof(KeyValueRef)) * data.size());
}

void GetRangeLimits::decrement(KeyValueRef const& data) {
	minRows = std::max(0, minRows - 1);
	if (rows != GetRangeLimits::ROW_LIMIT_UNLIMITED)
		rows--;
	if (bytes != GetRangeLimits::BYTE_LIMIT_UNLIMITED)
		bytes = std::max(0, bytes - (int)8 - (int)data.expectedSize());
}

void GetRangeLimits::decrement(VectorRef<MappedKeyValueRef> const& data) {
	if (rows != GetRangeLimits::ROW_LIMIT_UNLIMITED) {
		ASSERT(data.size() <= rows);
		rows -= data.size();
	}

	minRows = std::max(0, minRows - data.size());

	// TODO: For now, expectedSize only considers the size of the original key values, but not the underlying queries or
	// results. Also, double check it is correct when dealing with sizeof(MappedKeyValueRef).
	if (bytes != GetRangeLimits::BYTE_LIMIT_UNLIMITED)
		bytes = std::max(0, bytes - (int)data.expectedSize() - (8 - (int)sizeof(MappedKeyValueRef)) * data.size());
}

void GetRangeLimits::decrement(MappedKeyValueRef const& data) {
	minRows = std::max(0, minRows - 1);
	if (rows != GetRangeLimits::ROW_LIMIT_UNLIMITED)
		rows--;
	// TODO: For now, expectedSize only considers the size of the original key values, but not the underlying queries or
	// results. Also, double check it is correct when dealing with sizeof(MappedKeyValueRef).
	if (bytes != GetRangeLimits::BYTE_LIMIT_UNLIMITED)
		bytes = std::max(0, bytes - (int)8 - (int)data.expectedSize());
}

// True if either the row or byte limit has been reached
bool GetRangeLimits::isReached() {
	return rows == 0 || (bytes == 0 && minRows == 0);
}

// True if data would cause the row or byte limit to be reached
bool GetRangeLimits::reachedBy(VectorRef<KeyValueRef> const& data) {
	return (rows != GetRangeLimits::ROW_LIMIT_UNLIMITED && data.size() >= rows) ||
	       (bytes != GetRangeLimits::BYTE_LIMIT_UNLIMITED &&
	        (int)data.expectedSize() + (8 - (int)sizeof(KeyValueRef)) * data.size() >= bytes && data.size() >= minRows);
}

bool GetRangeLimits::hasByteLimit() {
	return bytes != GetRangeLimits::BYTE_LIMIT_UNLIMITED;
}

bool GetRangeLimits::hasRowLimit() {
	return rows != GetRangeLimits::ROW_LIMIT_UNLIMITED;
}

bool GetRangeLimits::hasSatisfiedMinRows() {
	return hasByteLimit() && minRows == 0;
}

AddressExclusion AddressExclusion::parse(StringRef const& key) {
	// Must not change: serialized to the database!
	auto parsedIp = IPAddress::parse(key.toString());
	if (parsedIp.present()) {
		return AddressExclusion(parsedIp.get());
	}

	// Not a whole machine, includes `port'.
	try {
		auto addr = NetworkAddress::parse(key.toString());
		if (addr.isTLS()) {
			TraceEvent(SevWarnAlways, "AddressExclusionParseError")
			    .detail("String", key)
			    .detail("Description", "Address inclusion string should not include `:tls' suffix.");
			return AddressExclusion();
		}
		return AddressExclusion(addr.ip, addr.port);
	} catch (Error&) {
		TraceEvent(SevWarnAlways, "AddressExclusionParseError").detail("String", key);
		return AddressExclusion();
	}
}

Future<Optional<Value>> getValue(Reference<TransactionState> const& trState,
                                 Key const& key,
                                 Future<Version> const& version,
                                 UseTenant const& useTenant = UseTenant::True,
                                 TransactionRecordLogInfo const& recordLogInfo = TransactionRecordLogInfo::True);

Future<RangeResult> getRange(Reference<TransactionState> const& trState,
                             Future<Version> const& fVersion,
                             KeySelector const& begin,
                             KeySelector const& end,
                             GetRangeLimits const& limits,
                             Reverse const& reverse,
                             UseTenant const& useTenant);

ACTOR Future<Optional<StorageServerInterface>> fetchServerInterface(Reference<TransactionState> trState,
                                                                    Future<Version> ver,
                                                                    UID id) {
	Optional<Value> val =
	    wait(getValue(trState, serverListKeyFor(id), ver, UseTenant::False, TransactionRecordLogInfo::False));

	if (!val.present()) {
		// A storage server has been removed from serverList since we read keyServers
		return Optional<StorageServerInterface>();
	}

	return decodeServerListValue(val.get());
}

ACTOR Future<Optional<std::vector<StorageServerInterface>>>
transactionalGetServerInterfaces(Reference<TransactionState> trState, Future<Version> ver, std::vector<UID> ids) {
	state std::vector<Future<Optional<StorageServerInterface>>> serverListEntries;
	serverListEntries.reserve(ids.size());
	for (int s = 0; s < ids.size(); s++) {
		serverListEntries.push_back(fetchServerInterface(trState, ver, ids[s]));
	}

	std::vector<Optional<StorageServerInterface>> serverListValues = wait(getAll(serverListEntries));
	std::vector<StorageServerInterface> serverInterfaces;
	for (int s = 0; s < serverListValues.size(); s++) {
		if (!serverListValues[s].present()) {
			// A storage server has been removed from ServerList since we read keyServers
			return Optional<std::vector<StorageServerInterface>>();
		}
		serverInterfaces.push_back(serverListValues[s].get());
	}
	return serverInterfaces;
}

void updateTssMappings(Database cx, const GetKeyServerLocationsReply& reply) {
	// Since a ss -> tss mapping is included in resultsTssMapping iff that SS is in results and has a tss pair,
	// all SS in results that do not have a mapping present must not have a tss pair.
	std::unordered_map<UID, const StorageServerInterface*> ssiById;
	for (const auto& [_, shard] : reply.results) {
		for (auto& ssi : shard) {
			ssiById[ssi.id()] = &ssi;
		}
	}

	for (const auto& mapping : reply.resultsTssMapping) {
		auto ssi = ssiById.find(mapping.first);
		ASSERT(ssi != ssiById.end());
		cx->addTssMapping(*ssi->second, mapping.second);
		ssiById.erase(mapping.first);
	}

	// if SS didn't have a mapping above, it's still in the ssiById map, so remove its tss mapping
	for (const auto& it : ssiById) {
		cx->removeTssMapping(*it.second);
	}
}

void updateTagMappings(Database cx, const GetKeyServerLocationsReply& reply) {
	for (const auto& mapping : reply.resultsTagMapping) {
		cx->addSSIdTagMapping(mapping.first, mapping.second);
	}
}

// If isBackward == true, returns the shard containing the key before 'key' (an infinitely long, inexpressible key).
// Otherwise returns the shard containing key
ACTOR Future<KeyRangeLocationInfo> getKeyLocation_internal(Database cx,
                                                           Optional<TenantName> tenant,
                                                           Key key,
                                                           SpanID spanID,
                                                           Optional<UID> debugID,
                                                           UseProvisionalProxies useProvisionalProxies,
                                                           Reverse isBackward,
                                                           Version version) {

	state Span span("NAPI:getKeyLocation"_loc, spanID);
	if (isBackward) {
		ASSERT(key != allKeys.begin && key <= allKeys.end);
	} else {
		ASSERT(key < allKeys.end);
	}

	if (debugID.present())
		g_traceBatch.addEvent("TransactionDebug", debugID.get().first(), "NativeAPI.getKeyLocation.Before");

	loop {
		try {
			wait(cx->getBackoff());
			++cx->transactionKeyServerLocationRequests;
			choose {
				when(wait(cx->onProxiesChanged())) {}
				when(GetKeyServerLocationsReply rep =
				         wait(basicLoadBalance(cx->getCommitProxies(useProvisionalProxies),
				                               &CommitProxyInterface::getKeyServersLocations,
				                               GetKeyServerLocationsRequest(span.context,
				                                                            tenant.castTo<TenantNameRef>(),
				                                                            key,
				                                                            Optional<KeyRef>(),
				                                                            100,
				                                                            isBackward,
				                                                            version,
				                                                            key.arena()),
				                               TaskPriority::DefaultPromiseEndpoint))) {
					++cx->transactionKeyServerLocationRequestsCompleted;
					if (debugID.present())
						g_traceBatch.addEvent(
						    "TransactionDebug", debugID.get().first(), "NativeAPI.getKeyLocation.After");
					ASSERT(rep.results.size() == 1);

					auto locationInfo =
					    cx->setCachedLocation(tenant, rep.tenantEntry, rep.results[0].first, rep.results[0].second);
					updateTssMappings(cx, rep);
					updateTagMappings(cx, rep);

					cx->updateBackoff(success());
					return KeyRangeLocationInfo(
					    rep.tenantEntry,
					    KeyRange(toRelativeRange(rep.results[0].first, rep.tenantEntry.prefix), rep.arena),
					    locationInfo);
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_proxy_memory_limit_exceeded) {
				// Eats proxy_memory_limit_exceeded error from commit proxies
				TraceEvent(SevWarnAlways, "CommitProxyOverloadedForKeyLocation").suppressFor(5);
				cx->updateBackoff(e);
				continue;
			}
			if (e.code() == error_code_tenant_not_found) {
				ASSERT(tenant.present());
				cx->invalidateCachedTenant(tenant.get());
			}

			throw;
		}
	}
}

// Checks if `endpoint` is failed on a healthy server or not. Returns true if we need to refresh the location cache for
// the endpoint.
bool checkOnlyEndpointFailed(const Database& cx, const Endpoint& endpoint) {
	if (IFailureMonitor::failureMonitor().onlyEndpointFailed(endpoint)) {
		// This endpoint is failed, but the server is still healthy. There are two cases this can happen:
		//    - There is a recent bounce in the cluster where the endpoints in SSes get updated.
		//    - The SS is failed and terminated on a server, but the server is kept running.
		// To account for the first case, we invalidate the cache and issue GetKeyLocation requests to the proxy to
		// update the cache with the new SS points. However, if the failure is caused by the second case, the
		// requested key location will continue to be the failed endpoint until the data movement is finished. But
		// every read will generate a GetKeyLocation request to the proxies (and still getting the failed endpoint
		// back), which may overload the proxy and affect data movement speed. Therefore, we only refresh the
		// location cache for short period of time, and after the initial grace period that we keep retrying
		// resolving key location, we will slow it down to resolve it only once every
		// `LOCATION_CACHE_FAILED_ENDPOINT_RETRY_INTERVAL`.
		cx->setFailedEndpointOnHealthyServer(endpoint);
		const auto& failureInfo = cx->getEndpointFailureInfo(endpoint);
		ASSERT(failureInfo.present());
		if (now() - failureInfo.get().startTime < CLIENT_KNOBS->LOCATION_CACHE_ENDPOINT_FAILURE_GRACE_PERIOD ||
		    now() - failureInfo.get().lastRefreshTime > CLIENT_KNOBS->LOCATION_CACHE_FAILED_ENDPOINT_RETRY_INTERVAL) {
			cx->updateFailedEndpointRefreshTime(endpoint);
			return true;
		}
	} else {
		cx->clearFailedEndpointOnHealthyServer(endpoint);
	}
	return false;
}

template <class F>
Future<KeyRangeLocationInfo> getKeyLocation(Database const& cx,
                                            Optional<TenantName> const& tenant,
                                            Key const& key,
                                            F StorageServerInterface::*member,
                                            SpanID spanID,
                                            Optional<UID> debugID,
                                            UseProvisionalProxies useProvisionalProxies,
                                            Reverse isBackward,
                                            Version version) {
	// we first check whether this range is cached
	Optional<KeyRangeLocationInfo> locationInfo = cx->getCachedLocation(tenant, key, isBackward);
	if (!locationInfo.present()) {
		return getKeyLocation_internal(cx, tenant, key, spanID, debugID, useProvisionalProxies, isBackward, version);
	}

	bool onlyEndpointFailedAndNeedRefresh = false;
	for (int i = 0; i < locationInfo.get().locations->size(); i++) {
		if (checkOnlyEndpointFailed(cx, locationInfo.get().locations->get(i, member).getEndpoint())) {
			onlyEndpointFailedAndNeedRefresh = true;
		}
	}

	if (onlyEndpointFailedAndNeedRefresh) {
		cx->invalidateCache(locationInfo.get().tenantEntry.prefix, key);

		// Refresh the cache with a new getKeyLocations made to proxies.
		return getKeyLocation_internal(cx, tenant, key, spanID, debugID, useProvisionalProxies, isBackward, version);
	}

	return locationInfo.get();
}

template <class F>
Future<KeyRangeLocationInfo> getKeyLocation(Reference<TransactionState> trState,
                                            Key const& key,
                                            F StorageServerInterface::*member,
                                            Reverse isBackward,
                                            UseTenant useTenant,
                                            Version version) {
	auto f = getKeyLocation(trState->cx,
	                        useTenant ? trState->tenant() : Optional<TenantName>(),
	                        key,
	                        member,
	                        trState->spanID,
	                        trState->debugID,
	                        trState->useProvisionalProxies,
	                        isBackward,
	                        version);

	if (trState->tenant().present() && useTenant) {
		return map(f, [trState](const KeyRangeLocationInfo& locationInfo) {
			trState->tenantId = locationInfo.tenantEntry.id;
			return locationInfo;
		});
	} else {
		return f;
	}
}

void DatabaseContext::updateBackoff(const Error& err) {
	switch (err.code()) {
	case error_code_success:
		backoffDelay = backoffDelay / CLIENT_KNOBS->BACKOFF_GROWTH_RATE;
		if (backoffDelay < CLIENT_KNOBS->DEFAULT_BACKOFF) {
			backoffDelay = 0.0;
		}
		break;

	case error_code_proxy_memory_limit_exceeded:
		++transactionsResourceConstrained;
		if (backoffDelay == 0.0) {
			backoffDelay = CLIENT_KNOBS->DEFAULT_BACKOFF;
		} else {
			backoffDelay = std::min(backoffDelay * CLIENT_KNOBS->BACKOFF_GROWTH_RATE,
			                        CLIENT_KNOBS->RESOURCE_CONSTRAINED_MAX_BACKOFF);
		}
		break;

	default:
		ASSERT_WE_THINK(false);
	}
}

ACTOR Future<std::vector<KeyRangeLocationInfo>> getKeyRangeLocations_internal(
    Database cx,
    Optional<TenantName> tenant,
    KeyRange keys,
    int limit,
    Reverse reverse,
    SpanID spanID,
    Optional<UID> debugID,
    UseProvisionalProxies useProvisionalProxies,
    Version version) {
	state Span span("NAPI:getKeyRangeLocations"_loc, spanID);
	if (debugID.present())
		g_traceBatch.addEvent("TransactionDebug", debugID.get().first(), "NativeAPI.getKeyLocations.Before");

	loop {
		try {
			wait(cx->getBackoff());
			++cx->transactionKeyServerLocationRequests;
			choose {
				when(wait(cx->onProxiesChanged())) {}
				when(GetKeyServerLocationsReply _rep =
				         wait(basicLoadBalance(cx->getCommitProxies(useProvisionalProxies),
				                               &CommitProxyInterface::getKeyServersLocations,
				                               GetKeyServerLocationsRequest(span.context,
				                                                            tenant.castTo<TenantNameRef>(),
				                                                            keys.begin,
				                                                            keys.end,
				                                                            limit,
				                                                            reverse,
				                                                            version,
				                                                            keys.arena()),
				                               TaskPriority::DefaultPromiseEndpoint))) {
					++cx->transactionKeyServerLocationRequestsCompleted;
					state GetKeyServerLocationsReply rep = _rep;
					if (debugID.present())
						g_traceBatch.addEvent(
						    "TransactionDebug", debugID.get().first(), "NativeAPI.getKeyLocations.After");
					ASSERT(rep.results.size());

					state std::vector<KeyRangeLocationInfo> results;
					state int shard = 0;
					for (; shard < rep.results.size(); shard++) {
						// FIXME: these shards are being inserted into the map sequentially, it would be much more
						// CPU efficient to save the map pairs and insert them all at once.
						results.emplace_back(
						    rep.tenantEntry,
						    (toRelativeRange(rep.results[shard].first, rep.tenantEntry.prefix) & keys),
						    cx->setCachedLocation(
						        tenant, rep.tenantEntry, rep.results[shard].first, rep.results[shard].second));
						wait(yield());
					}
					updateTssMappings(cx, rep);
					updateTagMappings(cx, rep);

					cx->updateBackoff(success());
					return results;
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_proxy_memory_limit_exceeded) {
				// Eats proxy_memory_limit_exceeded error from commit proxies
				TraceEvent(SevWarnAlways, "CommitProxyOverloadedForRangeLocation").suppressFor(5);
				cx->updateBackoff(e);
				continue;
			}
			if (e.code() == error_code_tenant_not_found) {
				ASSERT(tenant.present());
				cx->invalidateCachedTenant(tenant.get());
			}

			throw;
		}
	}
}

// Get the SS locations for each shard in the 'keys' key-range;
// Returned vector size is the number of shards in the input keys key-range.
// Returned vector element is <ShardRange, storage server location info> pairs, where
// ShardRange is the whole shard key-range, not a part of the given key range.
// Example: If query the function with  key range (b, d), the returned list of pairs could be something like:
// [([a, b1), locationInfo), ([b1, c), locationInfo), ([c, d1), locationInfo)].
template <class F>
Future<std::vector<KeyRangeLocationInfo>> getKeyRangeLocations(Database const& cx,
                                                               Optional<TenantName> tenant,
                                                               KeyRange const& keys,
                                                               int limit,
                                                               Reverse reverse,
                                                               F StorageServerInterface::*member,
                                                               SpanID const& spanID,
                                                               Optional<UID> const& debugID,
                                                               UseProvisionalProxies useProvisionalProxies,
                                                               Version version) {

	ASSERT(!keys.empty());

	std::vector<KeyRangeLocationInfo> locations;
	if (!cx->getCachedLocations(tenant, keys, locations, limit, reverse)) {
		return getKeyRangeLocations_internal(
		    cx, tenant, keys, limit, reverse, spanID, debugID, useProvisionalProxies, version);
	}

	bool foundFailed = false;
	for (const auto& locationInfo : locations) {
		bool onlyEndpointFailedAndNeedRefresh = false;
		for (int i = 0; i < locationInfo.locations->size(); i++) {
			if (checkOnlyEndpointFailed(cx, locationInfo.locations->get(i, member).getEndpoint())) {
				onlyEndpointFailedAndNeedRefresh = true;
			}
		}

		if (onlyEndpointFailedAndNeedRefresh) {
			cx->invalidateCache(locationInfo.tenantEntry.prefix, locationInfo.range.begin);
			foundFailed = true;
		}
	}

	if (foundFailed) {
		// Refresh the cache with a new getKeyRangeLocations made to proxies.
		return getKeyRangeLocations_internal(
		    cx, tenant, keys, limit, reverse, spanID, debugID, useProvisionalProxies, version);
	}

	return locations;
}

template <class F>
Future<std::vector<KeyRangeLocationInfo>> getKeyRangeLocations(Reference<TransactionState> trState,
                                                               KeyRange const& keys,
                                                               int limit,
                                                               Reverse reverse,
                                                               F StorageServerInterface::*member,
                                                               UseTenant useTenant,
                                                               Version version) {
	auto f = getKeyRangeLocations(trState->cx,
	                              useTenant ? trState->tenant() : Optional<TenantName>(),
	                              keys,
	                              limit,
	                              reverse,
	                              member,
	                              trState->spanID,
	                              trState->debugID,
	                              trState->useProvisionalProxies,
	                              version);

	if (trState->tenant().present() && useTenant) {
		return map(f, [trState](const std::vector<KeyRangeLocationInfo>& locationInfo) {
			ASSERT(!locationInfo.empty());
			trState->tenantId = locationInfo[0].tenantEntry.id;
			return locationInfo;
		});
	} else {
		return f;
	}
}

ACTOR Future<Void> warmRange_impl(Reference<TransactionState> trState, KeyRange keys, Future<Version> fVersion) {
	state int totalRanges = 0;
	state int totalRequests = 0;

	state Version version = wait(fVersion);

	loop {
		std::vector<KeyRangeLocationInfo> locations =
		    wait(getKeyRangeLocations_internal(trState->cx,
		                                       trState->tenant(),
		                                       keys,
		                                       CLIENT_KNOBS->WARM_RANGE_SHARD_LIMIT,
		                                       Reverse::False,
		                                       trState->spanID,
		                                       trState->debugID,
		                                       trState->useProvisionalProxies,
		                                       version));
		totalRanges += CLIENT_KNOBS->WARM_RANGE_SHARD_LIMIT;
		totalRequests++;
		if (locations.size() == 0 || totalRanges >= trState->cx->locationCacheSize ||
		    locations[locations.size() - 1].range.end >= keys.end)
			break;

		keys = KeyRangeRef(locations[locations.size() - 1].range.end, keys.end);

		if (totalRequests % 20 == 0) {
			// To avoid blocking the proxies from starting other transactions, occasionally get a read version.
			state Transaction tr(trState->cx, trState->tenant());
			loop {
				try {
					tr.setOption(FDBTransactionOptions::LOCK_AWARE);
					tr.setOption(FDBTransactionOptions::CAUSAL_READ_RISKY);
					wait(success(tr.getReadVersion()));
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}
	}

	return Void();
}

SpanID generateSpanID(bool transactionTracingSample, SpanID parentContext = SpanID()) {
	uint64_t txnId = deterministicRandom()->randomUInt64();
	if (parentContext.isValid()) {
		if (parentContext.first() > 0) {
			txnId = parentContext.first();
		}
		uint64_t tokenId = parentContext.second() > 0 ? deterministicRandom()->randomUInt64() : 0;
		return SpanID(txnId, tokenId);
	} else if (transactionTracingSample) {
		uint64_t tokenId = deterministicRandom()->random01() <= FLOW_KNOBS->TRACING_SAMPLE_RATE
		                       ? deterministicRandom()->randomUInt64()
		                       : 0;
		return SpanID(txnId, tokenId);
	} else {
		return SpanID(txnId, 0);
	}
}

TransactionState::TransactionState(Database cx,
                                   Optional<TenantName> tenant,
                                   TaskPriority taskID,
                                   SpanID spanID,
                                   Reference<TransactionLogInfo> trLogInfo)
  : cx(cx), trLogInfo(trLogInfo), options(cx), taskID(taskID), spanID(spanID), readVersionObtainedFromGrvProxy(true),
    tenant_(tenant), tenantSet(tenant.present()) {}

Reference<TransactionState> TransactionState::cloneAndReset(Reference<TransactionLogInfo> newTrLogInfo,
                                                            bool generateNewSpan) const {

	SpanID newSpanID = generateNewSpan ? generateSpanID(cx->transactionTracingSample) : spanID;
	Reference<TransactionState> newState =
	    makeReference<TransactionState>(cx, tenant_, cx->taskID, newSpanID, newTrLogInfo);

	if (!cx->apiVersionAtLeast(16)) {
		newState->options = options;
	}

	newState->numErrors = numErrors;
	newState->startTime = startTime;
	newState->committedVersion = committedVersion;
	newState->conflictingKeys = conflictingKeys;
	newState->tenantSet = tenantSet;

	return newState;
}

TenantInfo TransactionState::getTenantInfo() {
	Optional<TenantName> const& t = tenant();

	if (options.rawAccess) {
		return TenantInfo();
	} else if (!cx->internal && cx->clientInfo->get().tenantMode == TenantMode::REQUIRED && !t.present()) {
		throw tenant_name_required();
	} else if (!t.present()) {
		return TenantInfo();
	} else if (cx->clientInfo->get().tenantMode == TenantMode::DISABLED && t.present()) {
		throw tenants_disabled();
	}

	ASSERT(tenantId != TenantInfo::INVALID_TENANT);
	return TenantInfo(t.get(), tenantId);
}

Optional<TenantName> const& TransactionState::tenant() {
	if (tenantSet) {
		return tenant_;
	} else {
		if (!tenant_.present() && !options.rawAccess) {
			tenant_ = cx->defaultTenant;
		}
		tenantSet = true;
		return tenant_;
	}
}

bool TransactionState::hasTenant() const {
	return tenantSet && tenant_.present();
}

Future<Void> Transaction::warmRange(KeyRange keys) {
	return warmRange_impl(trState, keys, getReadVersion());
}

ACTOR Future<Optional<Value>> getValue(Reference<TransactionState> trState,
                                       Key key,
                                       Future<Version> version,
                                       UseTenant useTenant,
                                       TransactionRecordLogInfo recordLogInfo) {
	state Version ver = wait(version);
	state Span span("NAPI:getValue"_loc, trState->spanID);
	if (useTenant && trState->tenant().present()) {
		span.addTag("tenant"_sr, trState->tenant().get());
	}

	span.addTag("key"_sr, key);
	trState->cx->validateVersion(ver);

	loop {
		state KeyRangeLocationInfo locationInfo =
		    wait(getKeyLocation(trState, key, &StorageServerInterface::getValue, Reverse::False, useTenant, ver));

		state Optional<UID> getValueID = Optional<UID>();
		state uint64_t startTime;
		state double startTimeD;
		state VersionVector ssLatestCommitVersions;
		trState->cx->getLatestCommitVersions(locationInfo.locations, ver, trState, ssLatestCommitVersions);
		try {
			if (trState->debugID.present()) {
				getValueID = nondeterministicRandom()->randomUniqueID();

				g_traceBatch.addAttach("GetValueAttachID", trState->debugID.get().first(), getValueID.get().first());
				g_traceBatch.addEvent("GetValueDebug",
				                      getValueID.get().first(),
				                      "NativeAPI.getValue.Before"); //.detail("TaskID", g_network->getCurrentTask());
				/*TraceEvent("TransactionDebugGetValueInfo", getValueID.get())
				    .detail("Key", key)
				    .detail("ReqVersion", ver)
				    .detail("Servers", describe(ssi.second->get()));*/
			}

			++trState->cx->getValueSubmitted;
			startTime = timer_int();
			startTimeD = now();
			++trState->cx->transactionPhysicalReads;

			state GetValueReply reply;
			try {
				if (CLIENT_BUGGIFY_WITH_PROB(.01)) {
					throw deterministicRandom()->randomChoice(
					    std::vector<Error>{ transaction_too_old(), future_version() });
				}
				choose {
					when(wait(trState->cx->connectionFileChanged())) {
						throw transaction_too_old();
					}
					when(GetValueReply _reply = wait(loadBalance(
					         trState->cx.getPtr(),
					         locationInfo.locations,
					         &StorageServerInterface::getValue,
					         GetValueRequest(span.context,
					                         useTenant ? trState->getTenantInfo() : TenantInfo(),
					                         key,
					                         ver,
					                         trState->cx->sampleReadTags() ? trState->options.readTags
					                                                       : Optional<TagSet>(),
					                         getValueID,
					                         ssLatestCommitVersions),
					         TaskPriority::DefaultPromiseEndpoint,
					         AtMostOnce::False,
					         trState->cx->enableLocalityLoadBalance ? &trState->cx->queueModel : nullptr))) {
						reply = _reply;
					}
				}
				++trState->cx->transactionPhysicalReadsCompleted;
			} catch (Error&) {
				++trState->cx->transactionPhysicalReadsCompleted;
				throw;
			}

			double latency = now() - startTimeD;
			trState->cx->readLatencies.addSample(latency);
			if (trState->trLogInfo && recordLogInfo) {
				int valueSize = reply.value.present() ? reply.value.get().size() : 0;
				trState->trLogInfo->addLog(FdbClientLogEvents::EventGet(
				    startTimeD, trState->cx->clientLocality.dcId(), latency, valueSize, key, trState->tenant()));
			}
			trState->cx->getValueCompleted->latency = timer_int() - startTime;
			trState->cx->getValueCompleted->log();

			if (getValueID.present()) {
				g_traceBatch.addEvent("GetValueDebug",
				                      getValueID.get().first(),
				                      "NativeAPI.getValue.After"); //.detail("TaskID", g_network->getCurrentTask());
				/*TraceEvent("TransactionDebugGetValueDone", getValueID.get())
				    .detail("Key", key)
				    .detail("ReqVersion", ver)
				    .detail("ReplySize", reply.value.present() ? reply.value.get().size() : -1);*/
			}

			trState->cx->transactionBytesRead += reply.value.present() ? reply.value.get().size() : 0;
			++trState->cx->transactionKeysRead;
			return reply.value;
		} catch (Error& e) {
			trState->cx->getValueCompleted->latency = timer_int() - startTime;
			trState->cx->getValueCompleted->log();
			if (getValueID.present()) {
				g_traceBatch.addEvent("GetValueDebug",
				                      getValueID.get().first(),
				                      "NativeAPI.getValue.Error"); //.detail("TaskID", g_network->getCurrentTask());
				/*TraceEvent("TransactionDebugGetValueDone", getValueID.get())
				    .detail("Key", key)
				    .detail("ReqVersion", ver)
				    .detail("ReplySize", reply.value.present() ? reply.value.get().size() : -1);*/
			}
			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed ||
			    (e.code() == error_code_transaction_too_old && ver == latestVersion)) {
				trState->cx->invalidateCache(locationInfo.tenantEntry.prefix, key);
				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, trState->taskID));
			} else if (e.code() == error_code_unknown_tenant) {
				ASSERT(useTenant && trState->tenant().present());
				trState->cx->invalidateCachedTenant(trState->tenant().get());
				wait(delay(CLIENT_KNOBS->UNKNOWN_TENANT_RETRY_DELAY, trState->taskID));
			} else {
				if (trState->trLogInfo && recordLogInfo)
					trState->trLogInfo->addLog(FdbClientLogEvents::EventGetError(startTimeD,
					                                                             trState->cx->clientLocality.dcId(),
					                                                             static_cast<int>(e.code()),
					                                                             key,
					                                                             trState->tenant()));
				throw e;
			}
		}
	}
}

ACTOR Future<Key> getKey(Reference<TransactionState> trState,
                         KeySelector k,
                         Future<Version> version,
                         UseTenant useTenant = UseTenant::True) {
	wait(success(version));

	state Optional<UID> getKeyID = Optional<UID>();
	state Span span("NAPI:getKey"_loc, trState->spanID);
	if (trState->debugID.present()) {
		getKeyID = nondeterministicRandom()->randomUniqueID();

		g_traceBatch.addAttach("GetKeyAttachID", trState->debugID.get().first(), getKeyID.get().first());
		g_traceBatch.addEvent(
		    "GetKeyDebug",
		    getKeyID.get().first(),
		    "NativeAPI.getKey.AfterVersion"); //.detail("StartKey",
		                                      // k.getKey()).detail("Offset",k.offset).detail("OrEqual",k.orEqual);
	}

	loop {
		if (k.getKey() == allKeys.end) {
			if (k.offset > 0) {
				return allKeys.end;
			}
			k.orEqual = false;
		} else if (k.getKey() == allKeys.begin && k.offset <= 0) {
			return Key();
		}

		Key locationKey(k.getKey(), k.arena());
		state KeyRangeLocationInfo locationInfo = wait(getKeyLocation(trState,
		                                                              locationKey,
		                                                              &StorageServerInterface::getKey,
		                                                              Reverse{ k.isBackward() },
		                                                              useTenant,
		                                                              version.get()));

		state VersionVector ssLatestCommitVersions;
		trState->cx->getLatestCommitVersions(locationInfo.locations, version.get(), trState, ssLatestCommitVersions);

		try {
			if (getKeyID.present())
				g_traceBatch.addEvent(
				    "GetKeyDebug",
				    getKeyID.get().first(),
				    "NativeAPI.getKey.Before"); //.detail("StartKey",
				                                // k.getKey()).detail("Offset",k.offset).detail("OrEqual",k.orEqual);
			++trState->cx->transactionPhysicalReads;

			GetKeyRequest req(span.context,
			                  useTenant ? trState->getTenantInfo() : TenantInfo(),
			                  k,
			                  version.get(),
			                  trState->cx->sampleReadTags() ? trState->options.readTags : Optional<TagSet>(),
			                  getKeyID,
			                  ssLatestCommitVersions);
			req.arena.dependsOn(k.arena());

			state GetKeyReply reply;
			try {
				choose {
					when(wait(trState->cx->connectionFileChanged())) {
						throw transaction_too_old();
					}
					when(GetKeyReply _reply = wait(loadBalance(
					         trState->cx.getPtr(),
					         locationInfo.locations,
					         &StorageServerInterface::getKey,
					         req,
					         TaskPriority::DefaultPromiseEndpoint,
					         AtMostOnce::False,
					         trState->cx->enableLocalityLoadBalance ? &trState->cx->queueModel : nullptr))) {
						reply = _reply;
					}
				}
				++trState->cx->transactionPhysicalReadsCompleted;
			} catch (Error&) {
				++trState->cx->transactionPhysicalReadsCompleted;
				throw;
			}
			if (getKeyID.present())
				g_traceBatch.addEvent("GetKeyDebug",
				                      getKeyID.get().first(),
				                      "NativeAPI.getKey.After"); //.detail("NextKey",reply.sel.key).detail("Offset",
				                                                 // reply.sel.offset).detail("OrEqual", k.orEqual);
			k = reply.sel;
			if (!k.offset && k.orEqual) {
				return k.getKey();
			}
		} catch (Error& e) {
			if (getKeyID.present())
				g_traceBatch.addEvent("GetKeyDebug", getKeyID.get().first(), "NativeAPI.getKey.Error");
			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed) {
				trState->cx->invalidateCache(locationInfo.tenantEntry.prefix, k.getKey(), Reverse{ k.isBackward() });

				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, trState->taskID));
			} else if (e.code() == error_code_unknown_tenant) {
				ASSERT(useTenant && trState->tenant().present());
				trState->cx->invalidateCachedTenant(trState->tenant().get());
				wait(delay(CLIENT_KNOBS->UNKNOWN_TENANT_RETRY_DELAY, trState->taskID));
			} else {
				TraceEvent(SevInfo, "GetKeyError").error(e).detail("AtKey", k.getKey()).detail("Offset", k.offset);
				throw e;
			}
		}
	}
}

ACTOR Future<Version> waitForCommittedVersion(Database cx, Version version, SpanID spanContext) {
	state Span span("NAPI:waitForCommittedVersion"_loc, { spanContext });
	loop {
		try {
			choose {
				when(wait(cx->onProxiesChanged())) {}
				when(GetReadVersionReply v = wait(basicLoadBalance(
				         cx->getGrvProxies(UseProvisionalProxies::False),
				         &GrvProxyInterface::getConsistentReadVersion,
				         GetReadVersionRequest(
				             span.context, 0, TransactionPriority::IMMEDIATE, cx->ssVersionVectorCache.getMaxVersion()),
				         cx->taskID))) {
					cx->minAcceptableReadVersion = std::min(cx->minAcceptableReadVersion, v.version);
					if (v.midShardSize > 0)
						cx->smoothMidShardSize.setTotal(v.midShardSize);
					if (cx->versionVectorCacheActive(v.ssVersionVectorDelta)) {
						if (cx->isCurrentGrvProxy(v.proxyId)) {
							cx->ssVersionVectorCache.applyDelta(v.ssVersionVectorDelta);
						} else {
							cx->ssVersionVectorCache.clear();
						}
					}
					if (v.version >= version)
						return v.version;
					// SOMEDAY: Do the wait on the server side, possibly use less expensive source of committed version
					// (causal consistency is not needed for this purpose)
					wait(delay(CLIENT_KNOBS->FUTURE_VERSION_RETRY_DELAY, cx->taskID));
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_batch_transaction_throttled ||
			    e.code() == error_code_proxy_memory_limit_exceeded) {
				// GRV Proxy returns an error
				wait(delayJittered(CLIENT_KNOBS->GRV_ERROR_RETRY_DELAY));
			} else {
				TraceEvent(SevError, "WaitForCommittedVersionError").error(e);
				throw;
			}
		}
	}
}

ACTOR Future<Version> getRawVersion(Reference<TransactionState> trState) {
	state Span span("NAPI:getRawVersion"_loc, { trState->spanID });
	loop {
		choose {
			when(wait(trState->cx->onProxiesChanged())) {}
			when(GetReadVersionReply v =
			         wait(basicLoadBalance(trState->cx->getGrvProxies(UseProvisionalProxies::False),
			                               &GrvProxyInterface::getConsistentReadVersion,
			                               GetReadVersionRequest(trState->spanID,
			                                                     0,
			                                                     TransactionPriority::IMMEDIATE,
			                                                     trState->cx->ssVersionVectorCache.getMaxVersion()),
			                               trState->cx->taskID))) {
				if (trState->cx->versionVectorCacheActive(v.ssVersionVectorDelta)) {
					if (trState->cx->isCurrentGrvProxy(v.proxyId)) {
						trState->cx->ssVersionVectorCache.applyDelta(v.ssVersionVectorDelta);
					} else {
						trState->cx->ssVersionVectorCache.clear();
					}
				}
				return v.version;
			}
		}
	}
}

ACTOR Future<Void> readVersionBatcher(
    DatabaseContext* cx,
    FutureStream<std::pair<Promise<GetReadVersionReply>, Optional<UID>>> versionStream,
    uint32_t flags);

ACTOR Future<Version> watchValue(Database cx, Reference<const WatchParameters> parameters) {
	state Span span("NAPI:watchValue"_loc, parameters->spanID);
	state Version ver = parameters->version;
	cx->validateVersion(parameters->version);
	ASSERT(parameters->version != latestVersion);

	loop {
		state KeyRangeLocationInfo locationInfo = wait(getKeyLocation(cx,
		                                                              parameters->tenant.name,
		                                                              parameters->key,
		                                                              &StorageServerInterface::watchValue,
		                                                              parameters->spanID,
		                                                              parameters->debugID,
		                                                              parameters->useProvisionalProxies,
		                                                              Reverse::False,
		                                                              parameters->version));

		try {
			state Optional<UID> watchValueID = Optional<UID>();
			if (parameters->debugID.present()) {
				watchValueID = nondeterministicRandom()->randomUniqueID();

				g_traceBatch.addAttach(
				    "WatchValueAttachID", parameters->debugID.get().first(), watchValueID.get().first());
				g_traceBatch.addEvent("WatchValueDebug",
				                      watchValueID.get().first(),
				                      "NativeAPI.watchValue.Before"); //.detail("TaskID", g_network->getCurrentTask());
			}
			state WatchValueReply resp;
			choose {
				when(WatchValueReply r = wait(
				         loadBalance(cx.getPtr(),
				                     locationInfo.locations,
				                     &StorageServerInterface::watchValue,
				                     WatchValueRequest(span.context,
				                                       parameters->tenant,
				                                       parameters->key,
				                                       parameters->value,
				                                       ver,
				                                       cx->sampleReadTags() ? parameters->tags : Optional<TagSet>(),
				                                       watchValueID),
				                     TaskPriority::DefaultPromiseEndpoint))) {
					resp = r;
				}
				when(wait(cx->connectionRecord ? cx->connectionRecord->onChange() : Never())) {
					wait(Never());
				}
			}
			if (watchValueID.present()) {
				g_traceBatch.addEvent("WatchValueDebug", watchValueID.get().first(), "NativeAPI.watchValue.After");
			}

			// FIXME: wait for known committed version on the storage server before replying,
			// cannot do this until the storage server is notified on knownCommittedVersion changes from tlog (faster
			// than the current update loop)
			Version v = wait(waitForCommittedVersion(cx, resp.version, span.context));

			// False if there is a master failure between getting the response and getting the committed version,
			// Dependent on SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT
			if (v - resp.version < 50000000) {
				return resp.version;
			}
			ver = v;
		} catch (Error& e) {
			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed) {
				cx->invalidateCache(locationInfo.tenantEntry.prefix, parameters->key);
				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, parameters->taskID));
			} else if (e.code() == error_code_unknown_tenant) {
				ASSERT(parameters->tenant.name.present());
				cx->invalidateCachedTenant(parameters->tenant.name.get());
				wait(delay(CLIENT_KNOBS->UNKNOWN_TENANT_RETRY_DELAY, parameters->taskID));
			} else if (e.code() == error_code_watch_cancelled || e.code() == error_code_process_behind) {
				// clang-format off
				TEST(e.code() == error_code_watch_cancelled); // Too many watches on the storage server, poll for changes instead
				TEST(e.code() == error_code_process_behind); // The storage servers are all behind
				// clang-format on
				wait(delay(CLIENT_KNOBS->WATCH_POLLING_TIME, parameters->taskID));
			} else if (e.code() == error_code_timed_out) { // The storage server occasionally times out watches in case
				                                           // it was cancelled
				TEST(true); // A watch timed out
				wait(delay(CLIENT_KNOBS->FUTURE_VERSION_RETRY_DELAY, parameters->taskID));
			} else {
				state Error err = e;
				wait(delay(CLIENT_KNOBS->FUTURE_VERSION_RETRY_DELAY, parameters->taskID));
				throw err;
			}
		}
	}
}

ACTOR Future<Void> watchStorageServerResp(int64_t tenantId, Key key, Database cx) {
	loop {
		try {
			state Reference<WatchMetadata> metadata = cx->getWatchMetadata(tenantId, key);
			if (!metadata.isValid())
				return Void();

			Version watchVersion = wait(watchValue(cx, metadata->parameters));

			metadata = cx->getWatchMetadata(tenantId, key);
			if (!metadata.isValid())
				return Void();

			// case 1: version_1 (SS) >= version_2 (map)
			if (watchVersion >= metadata->parameters->version) {
				cx->deleteWatchMetadata(tenantId, key);
				if (metadata->watchPromise.canBeSet())
					metadata->watchPromise.send(watchVersion);
			}
			// ABA happens
			else {
				TEST(true); // ABA issue where the version returned from the server is less than the version in the map

				// case 2: version_1 < version_2 and future_count == 1
				if (metadata->watchPromise.getFutureReferenceCount() == 1) {
					cx->deleteWatchMetadata(tenantId, key);
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw e;
			}

			Reference<WatchMetadata> metadata = cx->getWatchMetadata(tenantId, key);
			if (!metadata.isValid()) {
				return Void();
			} else if (metadata->watchPromise.getFutureReferenceCount() == 1) {
				cx->deleteWatchMetadata(tenantId, key);
				return Void();
			} else if (e.code() == error_code_future_version) {
				continue;
			}
			cx->deleteWatchMetadata(tenantId, key);
			metadata->watchPromise.sendError(e);
			throw e;
		}
	}
}

ACTOR Future<Void> sameVersionDiffValue(Database cx, Reference<WatchParameters> parameters) {
	state ReadYourWritesTransaction tr(cx, parameters->tenant.name);
	loop {
		try {
			if (!parameters->tenant.name.present()) {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			}

			state Optional<Value> valSS = wait(tr.get(parameters->key));
			Reference<WatchMetadata> metadata = cx->getWatchMetadata(parameters->tenant.tenantId, parameters->key);

			// val_3 != val_1 (storage server value doesn't match value in map)
			if (metadata.isValid() && valSS != metadata->parameters->value) {
				cx->deleteWatchMetadata(parameters->tenant.tenantId, parameters->key);

				metadata->watchPromise.send(parameters->version);
				metadata->watchFutureSS.cancel();
			}

			// val_3 == val_2 (storage server value matches value passed into the function -> new watch)
			if (valSS == parameters->value && tr.getTransactionState()->tenantId == parameters->tenant.tenantId) {
				metadata = makeReference<WatchMetadata>(parameters);
				cx->setWatchMetadata(metadata);

				metadata->watchFutureSS = watchStorageServerResp(parameters->tenant.tenantId, parameters->key, cx);
			}

			// if val_3 != val_2
			if (valSS != parameters->value)
				return Void();

			// val_3 == val_2
			wait(success(metadata->watchPromise.getFuture()));

			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

Future<Void> getWatchFuture(Database cx, Reference<WatchParameters> parameters) {
	Reference<WatchMetadata> metadata = cx->getWatchMetadata(parameters->tenant.tenantId, parameters->key);

	// case 1: key not in map
	if (!metadata.isValid()) {
		metadata = makeReference<WatchMetadata>(parameters);
		cx->setWatchMetadata(metadata);

		metadata->watchFutureSS = watchStorageServerResp(parameters->tenant.tenantId, parameters->key, cx);
		return success(metadata->watchPromise.getFuture());
	}
	// case 2: val_1 == val_2 (received watch with same value as key already in the map so just update)
	else if (metadata->parameters->value == parameters->value) {
		if (parameters->version > metadata->parameters->version) {
			metadata->parameters = parameters;
		}

		return success(metadata->watchPromise.getFuture());
	}
	// case 3: val_1 != val_2 && version_2 > version_1 (received watch with different value and a higher version so
	// recreate in SS)
	else if (parameters->version > metadata->parameters->version) {
		TEST(true); // Setting a watch that has a different value than the one in the map but a higher version (newer)
		cx->deleteWatchMetadata(parameters->tenant.tenantId, parameters->key);

		metadata->watchPromise.send(parameters->version);
		metadata->watchFutureSS.cancel();

		metadata = makeReference<WatchMetadata>(parameters);
		cx->setWatchMetadata(metadata);

		metadata->watchFutureSS = watchStorageServerResp(parameters->tenant.tenantId, parameters->key, cx);

		return success(metadata->watchPromise.getFuture());
	}
	// case 5: val_1 != val_2 && version_1 == version_2 (received watch with different value but same version)
	else if (metadata->parameters->version == parameters->version) {
		TEST(true); // Setting a watch which has a different value than the one in the map but the same version
		return sameVersionDiffValue(cx, parameters);
	}
	TEST(true); // Setting a watch which has a different value than the one in the map but a lower version (older)

	// case 4: val_1 != val_2 && version_2 < version_1
	return Void();
}

ACTOR Future<Void> watchValueMap(Future<Version> version,
                                 TenantInfo tenant,
                                 Key key,
                                 Optional<Value> value,
                                 Database cx,
                                 TagSet tags,
                                 SpanID spanID,
                                 TaskPriority taskID,
                                 Optional<UID> debugID,
                                 UseProvisionalProxies useProvisionalProxies) {
	state Version ver = wait(version);

	wait(getWatchFuture(
	    cx,
	    makeReference<WatchParameters>(tenant, key, value, ver, tags, spanID, taskID, debugID, useProvisionalProxies)));

	return Void();
}

template <class GetKeyValuesFamilyRequest>
void transformRangeLimits(GetRangeLimits limits, Reverse reverse, GetKeyValuesFamilyRequest& req) {
	if (limits.bytes != 0) {
		if (!limits.hasRowLimit())
			req.limit = CLIENT_KNOBS->REPLY_BYTE_LIMIT; // Can't get more than this many rows anyway
		else
			req.limit = std::min(CLIENT_KNOBS->REPLY_BYTE_LIMIT, limits.rows);

		if (reverse)
			req.limit *= -1;

		if (!limits.hasByteLimit())
			req.limitBytes = CLIENT_KNOBS->REPLY_BYTE_LIMIT;
		else
			req.limitBytes = std::min(CLIENT_KNOBS->REPLY_BYTE_LIMIT, limits.bytes);
	} else {
		req.limitBytes = CLIENT_KNOBS->REPLY_BYTE_LIMIT;
		req.limit = reverse ? -limits.minRows : limits.minRows;
	}
}

template <class GetKeyValuesFamilyRequest>
RequestStream<GetKeyValuesFamilyRequest> StorageServerInterface::*getRangeRequestStream() {
	if constexpr (std::is_same<GetKeyValuesFamilyRequest, GetKeyValuesRequest>::value) {
		return &StorageServerInterface::getKeyValues;
	} else if (std::is_same<GetKeyValuesFamilyRequest, GetMappedKeyValuesRequest>::value) {
		return &StorageServerInterface::getMappedKeyValues;
	} else {
		UNREACHABLE();
	}
}

ACTOR template <class GetKeyValuesFamilyRequest, class GetKeyValuesFamilyReply, class RangeResultFamily>
Future<RangeResultFamily> getExactRange(Reference<TransactionState> trState,
                                        Version version,
                                        KeyRange keys,
                                        Key mapper,
                                        GetRangeLimits limits,
                                        Reverse reverse,
                                        UseTenant useTenant) {
	state RangeResultFamily output;
	state Span span("NAPI:getExactRange"_loc, trState->spanID);

	if (useTenant && trState->tenant().present()) {
		span.addTag("tenant"_sr, trState->tenant().get());
	}

	// printf("getExactRange( '%s', '%s' )\n", keys.begin.toString().c_str(), keys.end.toString().c_str());
	loop {
		state std::vector<KeyRangeLocationInfo> locations =
		    wait(getKeyRangeLocations(trState,
		                              keys,
		                              CLIENT_KNOBS->GET_RANGE_SHARD_LIMIT,
		                              reverse,
		                              getRangeRequestStream<GetKeyValuesFamilyRequest>(),
		                              useTenant,
		                              version));
		ASSERT(locations.size());
		state int shard = 0;
		loop {
			const KeyRangeRef& range = locations[shard].range;

			GetKeyValuesFamilyRequest req;
			req.mapper = mapper;
			req.arena.dependsOn(mapper.arena());

			req.tenantInfo = useTenant ? trState->getTenantInfo() : TenantInfo();
			req.version = version;
			req.begin = firstGreaterOrEqual(range.begin);
			req.end = firstGreaterOrEqual(range.end);
			req.spanContext = span.context;
			trState->cx->getLatestCommitVersions(
			    locations[shard].locations, req.version, trState, req.ssLatestCommitVersions);

			// keep shard's arena around in case of async tss comparison
			req.arena.dependsOn(locations[shard].range.arena());

			transformRangeLimits(limits, reverse, req);
			ASSERT(req.limitBytes > 0 && req.limit != 0 && req.limit < 0 == reverse);

			// FIXME: buggify byte limits on internal functions that use them, instead of globally
			req.tags = trState->cx->sampleReadTags() ? trState->options.readTags : Optional<TagSet>();
			req.debugID = trState->debugID;

			try {
				if (trState->debugID.present()) {
					g_traceBatch.addEvent(
					    "TransactionDebug", trState->debugID.get().first(), "NativeAPI.getExactRange.Before");
					/*TraceEvent("TransactionDebugGetExactRangeInfo", trState->debugID.get())
					    .detail("ReqBeginKey", req.begin.getKey())
					    .detail("ReqEndKey", req.end.getKey())
					    .detail("ReqLimit", req.limit)
					    .detail("ReqLimitBytes", req.limitBytes)
					    .detail("ReqVersion", req.version)
					    .detail("Reverse", reverse)
					    .detail("Servers", locations[shard].second->description());*/
				}
				++trState->cx->transactionPhysicalReads;
				state GetKeyValuesFamilyReply rep;
				try {
					choose {
						when(wait(trState->cx->connectionFileChanged())) {
							throw transaction_too_old();
						}
						when(GetKeyValuesFamilyReply _rep = wait(loadBalance(
						         trState->cx.getPtr(),
						         locations[shard].locations,
						         getRangeRequestStream<GetKeyValuesFamilyRequest>(),
						         req,
						         TaskPriority::DefaultPromiseEndpoint,
						         AtMostOnce::False,
						         trState->cx->enableLocalityLoadBalance ? &trState->cx->queueModel : nullptr))) {
							rep = _rep;
						}
					}
					++trState->cx->transactionPhysicalReadsCompleted;
				} catch (Error&) {
					++trState->cx->transactionPhysicalReadsCompleted;
					throw;
				}
				if (trState->debugID.present())
					g_traceBatch.addEvent(
					    "TransactionDebug", trState->debugID.get().first(), "NativeAPI.getExactRange.After");
				output.arena().dependsOn(rep.arena);
				output.append(output.arena(), rep.data.begin(), rep.data.size());

				if (limits.hasRowLimit() && rep.data.size() > limits.rows) {
					TraceEvent(SevError, "GetExactRangeTooManyRows")
					    .detail("RowLimit", limits.rows)
					    .detail("DeliveredRows", output.size());
					ASSERT(false);
				}
				limits.decrement(rep.data);

				if (limits.isReached()) {
					output.more = true;
					return output;
				}

				bool more = rep.more;
				// If the reply says there is more but we know that we finished the shard, then fix rep.more
				if (reverse && more && rep.data.size() > 0 &&
				    output[output.size() - 1].key == locations[shard].range.begin)
					more = false;

				if (more) {
					if (!rep.data.size()) {
						TraceEvent(SevError, "GetExactRangeError")
						    .detail("Reason", "More data indicated but no rows present")
						    .detail("LimitBytes", limits.bytes)
						    .detail("LimitRows", limits.rows)
						    .detail("OutputSize", output.size())
						    .detail("OutputBytes", output.expectedSize())
						    .detail("BlockSize", rep.data.size())
						    .detail("BlockBytes", rep.data.expectedSize());
						ASSERT(false);
					}
					TEST(true); // GetKeyValuesFamilyReply.more in getExactRange
					// Make next request to the same shard with a beginning key just after the last key returned
					if (reverse)
						locations[shard].range =
						    KeyRangeRef(locations[shard].range.begin, output[output.size() - 1].key);
					else
						locations[shard].range =
						    KeyRangeRef(keyAfter(output[output.size() - 1].key), locations[shard].range.end);
				}

				if (!more || locations[shard].range.empty()) {
					TEST(true); // getExactrange (!more || locations[shard].first.empty())
					if (shard == locations.size() - 1) {
						const KeyRangeRef& range = locations[shard].range;
						KeyRef begin = reverse ? keys.begin : range.end;
						KeyRef end = reverse ? range.begin : keys.end;

						if (begin >= end) {
							output.more = false;
							return output;
						}
						TEST(true); // Multiple requests of key locations

						keys = KeyRangeRef(begin, end);
						break;
					}

					++shard;
				}

				// Soft byte limit - return results early if the user specified a byte limit and we got results
				// This can prevent problems where the desired range spans many shards and would be too slow to
				// fetch entirely.
				if (limits.hasSatisfiedMinRows() && output.size() > 0) {
					output.more = true;
					return output;
				}

			} catch (Error& e) {
				if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed) {
					const KeyRangeRef& range = locations[shard].range;

					if (reverse)
						keys = KeyRangeRef(keys.begin, range.end);
					else
						keys = KeyRangeRef(range.begin, keys.end);

					trState->cx->invalidateCache(locations[0].tenantEntry.prefix, keys);

					wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, trState->taskID));
					break;
				} else if (e.code() == error_code_unknown_tenant) {
					ASSERT(useTenant && trState->tenant().present());
					trState->cx->invalidateCachedTenant(trState->tenant().get());
					wait(delay(CLIENT_KNOBS->UNKNOWN_TENANT_RETRY_DELAY, trState->taskID));
					break;
				} else {
					TraceEvent(SevInfo, "GetExactRangeError")
					    .error(e)
					    .detail("Tenant", trState->tenant())
					    .detail("ShardBegin", locations[shard].range.begin)
					    .detail("ShardEnd", locations[shard].range.end);
					throw;
				}
			}
		}
	}
}

Future<Key> resolveKey(Reference<TransactionState> trState,
                       KeySelector const& key,
                       Version const& version,
                       UseTenant useTenant) {
	if (key.isFirstGreaterOrEqual())
		return Future<Key>(key.getKey());

	if (key.isFirstGreaterThan())
		return Future<Key>(keyAfter(key.getKey()));

	return getKey(trState, key, version, useTenant);
}

ACTOR template <class GetKeyValuesFamilyRequest, class GetKeyValuesFamilyReply, class RangeResultFamily>
Future<RangeResultFamily> getRangeFallback(Reference<TransactionState> trState,
                                           Version version,
                                           KeySelector begin,
                                           KeySelector end,
                                           Key mapper,
                                           GetRangeLimits limits,
                                           Reverse reverse,
                                           UseTenant useTenant) {
	if (version == latestVersion) {
		state Transaction transaction(trState->cx);
		transaction.setOption(FDBTransactionOptions::CAUSAL_READ_RISKY);
		transaction.setOption(FDBTransactionOptions::LOCK_AWARE);
		transaction.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		Version ver = wait(transaction.getReadVersion());
		version = ver;
	}

	Future<Key> fb = resolveKey(trState, begin, version, useTenant);
	state Future<Key> fe = resolveKey(trState, end, version, useTenant);

	state Key b = wait(fb);
	state Key e = wait(fe);
	if (b >= e) {
		return RangeResultFamily();
	}

	// if e is allKeys.end, we have read through the end of the database/tenant
	// if b is allKeys.begin, we have either read through the beginning of the database/tenant,
	// or allKeys.begin exists in the database/tenant and will be part of the conflict range anyways

	RangeResultFamily _r = wait(getExactRange<GetKeyValuesFamilyRequest, GetKeyValuesFamilyReply, RangeResultFamily>(
	    trState, version, KeyRangeRef(b, e), mapper, limits, reverse, useTenant));
	RangeResultFamily r = _r;

	if (b == allKeys.begin && ((reverse && !r.more) || !reverse))
		r.readToBegin = true;

	// TODO: this currently causes us to have a conflict range that is too large if our end key resolves to the
	// key after the last key in the database. In that case, we don't need a conflict between the last key and
	// the end of the database.
	//
	// If fixed, the ConflictRange test can be updated to stop checking for this condition.
	if (e == allKeys.end && ((!reverse && !r.more) || reverse))
		r.readThroughEnd = true;

	ASSERT(!limits.hasRowLimit() || r.size() <= limits.rows);

	// If we were limiting bytes and the returned range is twice the request (plus 10K) log a warning
	if (limits.hasByteLimit() &&
	    r.expectedSize() >
	        size_t(limits.bytes + CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT + CLIENT_KNOBS->VALUE_SIZE_LIMIT + 1) &&
	    limits.minRows == 0) {
		TraceEvent(SevWarnAlways, "GetRangeFallbackTooMuchData")
		    .detail("LimitBytes", limits.bytes)
		    .detail("DeliveredBytes", r.expectedSize())
		    .detail("LimitRows", limits.rows)
		    .detail("DeliveredRows", r.size());
	}

	return r;
}

int64_t inline getRangeResultFamilyBytes(RangeResultRef result) {
	return result.expectedSize();
}

int64_t inline getRangeResultFamilyBytes(MappedRangeResultRef result) {
	int64_t bytes = 0;
	for (const MappedKeyValueRef& mappedKeyValue : result) {
		bytes += mappedKeyValue.key.size() + mappedKeyValue.value.size();

		auto& reqAndResult = mappedKeyValue.reqAndResult;
		if (std::holds_alternative<GetValueReqAndResultRef>(reqAndResult)) {
			auto getValue = std::get<GetValueReqAndResultRef>(reqAndResult);
			bytes += getValue.expectedSize();
		} else if (std::holds_alternative<GetRangeReqAndResultRef>(reqAndResult)) {
			auto getRange = std::get<GetRangeReqAndResultRef>(reqAndResult);
			bytes += getRange.result.expectedSize();
		} else {
			throw internal_error();
		}
	}
	return bytes;
}

// TODO: Client should add mapped keys to conflict ranges.
template <class RangeResultFamily> // RangeResult or MappedRangeResult
void getRangeFinished(Reference<TransactionState> trState,
                      double startTime,
                      KeySelector begin,
                      KeySelector end,
                      Snapshot snapshot,
                      Promise<std::pair<Key, Key>> conflictRange,
                      Reverse reverse,
                      RangeResultFamily result) {
	int64_t bytes = getRangeResultFamilyBytes(result);

	trState->cx->transactionBytesRead += bytes;
	trState->cx->transactionKeysRead += result.size();

	if (trState->trLogInfo) {
		trState->trLogInfo->addLog(FdbClientLogEvents::EventGetRange(startTime,
		                                                             trState->cx->clientLocality.dcId(),
		                                                             now() - startTime,
		                                                             bytes,
		                                                             begin.getKey(),
		                                                             end.getKey(),
		                                                             trState->tenant()));
	}

	if (!snapshot) {
		Key rangeBegin;
		Key rangeEnd;

		if (result.readToBegin) {
			rangeBegin = allKeys.begin;
		} else if (((!reverse || !result.more || begin.offset > 1) && begin.offset > 0) || result.size() == 0) {
			rangeBegin = Key(begin.getKey(), begin.arena());
		} else {
			rangeBegin = reverse ? result.end()[-1].key : result[0].key;
		}

		if (end.offset > begin.offset && end.getKey() < rangeBegin) {
			rangeBegin = Key(end.getKey(), end.arena());
		}

		if (result.readThroughEnd) {
			rangeEnd = allKeys.end;
		} else if (((reverse || !result.more || end.offset <= 0) && end.offset <= 1) || result.size() == 0) {
			rangeEnd = Key(end.getKey(), end.arena());
		} else {
			rangeEnd = keyAfter(reverse ? result[0].key : result.end()[-1].key);
		}

		if (begin.offset < end.offset && begin.getKey() > rangeEnd) {
			rangeEnd = Key(begin.getKey(), begin.arena());
		}

		conflictRange.send(std::make_pair(rangeBegin, rangeEnd));
	}
}

ACTOR template <class GetKeyValuesFamilyRequest, // GetKeyValuesRequest or GetMappedKeyValuesRequest
                class GetKeyValuesFamilyReply, // GetKeyValuesReply or GetMappedKeyValuesReply (It would be nice if
                                               // we could use REPLY_TYPE(GetKeyValuesFamilyRequest) instead of specify
                                               // it as a separate template element)
                class RangeResultFamily // RangeResult or MappedRangeResult
                >
Future<RangeResultFamily> getRange(Reference<TransactionState> trState,
                                   Future<Version> fVersion,
                                   KeySelector begin,
                                   KeySelector end,
                                   Key mapper,
                                   GetRangeLimits limits,
                                   Promise<std::pair<Key, Key>> conflictRange,
                                   Snapshot snapshot,
                                   Reverse reverse,
                                   UseTenant useTenant = UseTenant::True) {
	//	state using RangeResultRefFamily = typename RangeResultFamily::RefType;
	state GetRangeLimits originalLimits(limits);
	state KeySelector originalBegin = begin;
	state KeySelector originalEnd = end;
	state RangeResultFamily output;
	state Span span("NAPI:getRange"_loc, trState->spanID);
	if (useTenant && trState->tenant().present()) {
		span.addTag("tenant"_sr, trState->tenant().get());
	}

	try {
		state Version version = wait(fVersion);
		trState->cx->validateVersion(version);

		state double startTime = now();
		state Version readVersion = version; // Needed for latestVersion requests; if more, make future requests at the
		                                     // version that the first one completed
		                                     // FIXME: Is this really right?  Weaken this and see if there is a problem;
		                                     // if so maybe there is a much subtler problem even with this.

		if (begin.getKey() == allKeys.begin && begin.offset < 1) {
			output.readToBegin = true;
			begin = KeySelector(firstGreaterOrEqual(begin.getKey()), begin.arena());
		}

		ASSERT(!limits.isReached());
		ASSERT((!limits.hasRowLimit() || limits.rows >= limits.minRows) && limits.minRows >= 0);

		loop {
			if (end.getKey() == allKeys.begin && (end.offset < 1 || end.isFirstGreaterOrEqual())) {
				getRangeFinished(
				    trState, startTime, originalBegin, originalEnd, snapshot, conflictRange, reverse, output);
				return output;
			}

			Key locationKey = reverse ? Key(end.getKey(), end.arena()) : Key(begin.getKey(), begin.arena());
			Reverse locationBackward{ reverse ? (end - 1).isBackward() : begin.isBackward() };
			state KeyRangeLocationInfo beginServer =
			    wait(getKeyLocation(trState,
			                        locationKey,
			                        getRangeRequestStream<GetKeyValuesFamilyRequest>(),
			                        locationBackward,
			                        useTenant,
			                        version));
			state KeyRange shard = beginServer.range;
			state bool modifiedSelectors = false;
			state GetKeyValuesFamilyRequest req;
			req.mapper = mapper;
			req.arena.dependsOn(mapper.arena());

			req.tenantInfo = useTenant ? trState->getTenantInfo() : TenantInfo();
			req.isFetchKeys = (trState->taskID == TaskPriority::FetchKeys);
			req.version = readVersion;

			trState->cx->getLatestCommitVersions(
			    beginServer.locations, req.version, trState, req.ssLatestCommitVersions);

			// In case of async tss comparison, also make req arena depend on begin, end, and/or shard's arena depending
			// on which  is used
			bool dependOnShard = false;
			if (reverse && (begin - 1).isDefinitelyLess(shard.begin) &&
			    (!begin.isFirstGreaterOrEqual() ||
			     begin.getKey() != shard.begin)) { // In this case we would be setting modifiedSelectors to true, but
				                                   // not modifying anything

				req.begin = firstGreaterOrEqual(shard.begin);
				modifiedSelectors = true;
				req.arena.dependsOn(shard.arena());
				dependOnShard = true;
			} else {
				req.begin = begin;
				req.arena.dependsOn(begin.arena());
			}

			if (!reverse && end.isDefinitelyGreater(shard.end)) {
				req.end = firstGreaterOrEqual(shard.end);
				modifiedSelectors = true;
				if (!dependOnShard) {
					req.arena.dependsOn(shard.arena());
				}
			} else {
				req.end = end;
				req.arena.dependsOn(end.arena());
			}

			transformRangeLimits(limits, reverse, req);
			ASSERT(req.limitBytes > 0 && req.limit != 0 && req.limit < 0 == reverse);

			req.tags = trState->cx->sampleReadTags() ? trState->options.readTags : Optional<TagSet>();
			req.debugID = trState->debugID;
			req.spanContext = span.context;
			try {
				if (trState->debugID.present()) {
					g_traceBatch.addEvent(
					    "TransactionDebug", trState->debugID.get().first(), "NativeAPI.getRange.Before");
					/*TraceEvent("TransactionDebugGetRangeInfo", trState->debugID.get())
					    .detail("ReqBeginKey", req.begin.getKey())
					    .detail("ReqEndKey", req.end.getKey())
					    .detail("OriginalBegin", originalBegin.toString())
					    .detail("OriginalEnd", originalEnd.toString())
					    .detail("Begin", begin.toString())
					    .detail("End", end.toString())
					    .detail("Shard", shard)
					    .detail("ReqLimit", req.limit)
					    .detail("ReqLimitBytes", req.limitBytes)
					    .detail("ReqVersion", req.version)
					    .detail("Reverse", reverse)
					    .detail("ModifiedSelectors", modifiedSelectors)
					    .detail("Servers", beginServer.second->description());*/
				}

				++trState->cx->transactionPhysicalReads;
				state GetKeyValuesFamilyReply rep;
				try {
					if (CLIENT_BUGGIFY_WITH_PROB(.01)) {
						throw deterministicRandom()->randomChoice(
						    std::vector<Error>{ transaction_too_old(), future_version() });
					}
					// state AnnotateActor annotation(currentLineage);
					GetKeyValuesFamilyReply _rep =
					    wait(loadBalance(trState->cx.getPtr(),
					                     beginServer.locations,
					                     getRangeRequestStream<GetKeyValuesFamilyRequest>(),
					                     req,
					                     TaskPriority::DefaultPromiseEndpoint,
					                     AtMostOnce::False,
					                     trState->cx->enableLocalityLoadBalance ? &trState->cx->queueModel : nullptr));
					rep = _rep;
					++trState->cx->transactionPhysicalReadsCompleted;
				} catch (Error&) {
					++trState->cx->transactionPhysicalReadsCompleted;
					throw;
				}

				if (trState->debugID.present()) {
					g_traceBatch.addEvent("TransactionDebug",
					                      trState->debugID.get().first(),
					                      "NativeAPI.getRange.After"); //.detail("SizeOf", rep.data.size());
					/*TraceEvent("TransactionDebugGetRangeDone", trState->debugID.get())
					    .detail("ReqBeginKey", req.begin.getKey())
					    .detail("ReqEndKey", req.end.getKey())
					    .detail("RepIsMore", rep.more)
					    .detail("VersionReturned", rep.version)
					    .detail("RowsReturned", rep.data.size());*/
				}

				ASSERT(!rep.more || rep.data.size());
				ASSERT(!limits.hasRowLimit() || rep.data.size() <= limits.rows);

				limits.decrement(rep.data);

				if (reverse && begin.isLastLessOrEqual() && rep.data.size() &&
				    rep.data.end()[-1].key == begin.getKey()) {
					modifiedSelectors = false;
				}

				bool finished = limits.isReached() || (!modifiedSelectors && !rep.more) || limits.hasSatisfiedMinRows();
				bool readThrough = modifiedSelectors && !rep.more;

				// optimization: first request got all data--just return it
				if (finished && !output.size()) {
					bool readToBegin = output.readToBegin;
					bool readThroughEnd = output.readThroughEnd;

					using RangeResultRefFamily = typename RangeResultFamily::RefType;
					output = RangeResultFamily(
					    RangeResultRefFamily(rep.data, modifiedSelectors || limits.isReached() || rep.more), rep.arena);
					output.readToBegin = readToBegin;
					output.readThroughEnd = readThroughEnd;

					if (BUGGIFY && limits.hasByteLimit() && output.size() > std::max(1, originalLimits.minRows) &&
					    (!std::is_same<GetKeyValuesFamilyRequest, GetMappedKeyValuesRequest>::value)) {
						// Copy instead of resizing because TSS maybe be using output's arena for comparison. This only
						// happens in simulation so it's fine
						// disable it on prefetch, because boundary entries serve as continuations
						RangeResultFamily copy;
						int newSize =
						    deterministicRandom()->randomInt(std::max(1, originalLimits.minRows), output.size());
						for (int i = 0; i < newSize; i++) {
							copy.push_back_deep(copy.arena(), output[i]);
						}
						output = copy;
						output.more = true;

						getRangeFinished(
						    trState, startTime, originalBegin, originalEnd, snapshot, conflictRange, reverse, output);
						return output;
					}

					if (readThrough) {
						output.arena().dependsOn(shard.arena());
						output.readThrough = reverse ? shard.begin : shard.end;
					}

					getRangeFinished(
					    trState, startTime, originalBegin, originalEnd, snapshot, conflictRange, reverse, output);
					return output;
				}

				output.arena().dependsOn(rep.arena);
				output.append(output.arena(), rep.data.begin(), rep.data.size());

				if (finished) {
					if (readThrough) {
						output.arena().dependsOn(shard.arena());
						output.readThrough = reverse ? shard.begin : shard.end;
					}
					output.more = modifiedSelectors || limits.isReached() || rep.more;

					getRangeFinished(
					    trState, startTime, originalBegin, originalEnd, snapshot, conflictRange, reverse, output);
					return output;
				}

				if (readVersion == latestVersion) {
					readVersion = rep.version; // see above comment
				}

				if (!rep.more) {
					ASSERT(modifiedSelectors);
					TEST(true); // !GetKeyValuesFamilyReply.more and modifiedSelectors in getRange

					if (!rep.data.size()) {
						RangeResultFamily result = wait(
						    getRangeFallback<GetKeyValuesFamilyRequest, GetKeyValuesFamilyReply, RangeResultFamily>(
						        trState,
						        readVersion,
						        originalBegin,
						        originalEnd,
						        mapper,
						        originalLimits,
						        reverse,
						        useTenant));
						getRangeFinished(
						    trState, startTime, originalBegin, originalEnd, snapshot, conflictRange, reverse, result);
						return result;
					}

					if (reverse)
						end = firstGreaterOrEqual(shard.begin);
					else
						begin = firstGreaterOrEqual(shard.end);
				} else {
					TEST(true); // GetKeyValuesFamilyReply.more in getRange
					if (reverse)
						end = firstGreaterOrEqual(output[output.size() - 1].key);
					else
						begin = firstGreaterThan(output[output.size() - 1].key);
				}

			} catch (Error& e) {
				if (trState->debugID.present()) {
					g_traceBatch.addEvent(
					    "TransactionDebug", trState->debugID.get().first(), "NativeAPI.getRange.Error");
					TraceEvent("TransactionDebugError", trState->debugID.get()).error(e);
				}
				if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed ||
				    (e.code() == error_code_transaction_too_old && readVersion == latestVersion)) {
					trState->cx->invalidateCache(beginServer.tenantEntry.prefix,
					                             reverse ? end.getKey() : begin.getKey(),
					                             Reverse{ reverse ? (end - 1).isBackward() : begin.isBackward() });

					if (e.code() == error_code_wrong_shard_server) {
						RangeResultFamily result = wait(
						    getRangeFallback<GetKeyValuesFamilyRequest, GetKeyValuesFamilyReply, RangeResultFamily>(
						        trState,
						        version,
						        originalBegin,
						        originalEnd,
						        mapper,
						        originalLimits,
						        reverse,
						        useTenant));
						getRangeFinished(
						    trState, startTime, originalBegin, originalEnd, snapshot, conflictRange, reverse, result);
						return result;
					}

					wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, trState->taskID));
				} else if (e.code() == error_code_unknown_tenant) {
					ASSERT(useTenant && trState->tenant().present());
					trState->cx->invalidateCachedTenant(trState->tenant().get());
					wait(delay(CLIENT_KNOBS->UNKNOWN_TENANT_RETRY_DELAY, trState->taskID));
				} else {
					if (trState->trLogInfo)
						trState->trLogInfo->addLog(
						    FdbClientLogEvents::EventGetRangeError(startTime,
						                                           trState->cx->clientLocality.dcId(),
						                                           static_cast<int>(e.code()),
						                                           begin.getKey(),
						                                           end.getKey(),
						                                           trState->tenant()));

					throw e;
				}
			}
		}
	} catch (Error& e) {
		if (conflictRange.canBeSet()) {
			conflictRange.send(std::make_pair(Key(), Key()));
		}

		throw;
	}
}

template <class StreamReply>
struct TSSDuplicateStreamData {
	PromiseStream<StreamReply> stream;
	Promise<Void> tssComparisonDone;

	// empty constructor for optional?
	TSSDuplicateStreamData() {}

	TSSDuplicateStreamData(PromiseStream<StreamReply> stream) : stream(stream) {}

	bool done() { return tssComparisonDone.getFuture().isReady(); }

	void setDone() {
		if (tssComparisonDone.canBeSet()) {
			tssComparisonDone.send(Void());
		}
	}

	~TSSDuplicateStreamData() {}
};

// Error tracking here is weird, and latency doesn't really mean the same thing here as it does with normal tss
// comparisons, so this is pretty much just counting mismatches
ACTOR template <class Request>
static Future<Void> tssStreamComparison(Request request,
                                        TSSDuplicateStreamData<REPLYSTREAM_TYPE(Request)> streamData,
                                        ReplyPromiseStream<REPLYSTREAM_TYPE(Request)> tssReplyStream,
                                        TSSEndpointData tssData) {
	state bool ssEndOfStream = false;
	state bool tssEndOfStream = false;
	state Optional<REPLYSTREAM_TYPE(Request)> ssReply = Optional<REPLYSTREAM_TYPE(Request)>();
	state Optional<REPLYSTREAM_TYPE(Request)> tssReply = Optional<REPLYSTREAM_TYPE(Request)>();

	loop {
		// reset replies
		ssReply = Optional<REPLYSTREAM_TYPE(Request)>();
		tssReply = Optional<REPLYSTREAM_TYPE(Request)>();

		state double startTime = now();
		// wait for ss response
		try {
			REPLYSTREAM_TYPE(Request) _ssReply = waitNext(streamData.stream.getFuture());
			ssReply = _ssReply;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				streamData.setDone();
				throw;
			}
			if (e.code() == error_code_end_of_stream) {
				// ss response will be set to empty, to compare to the SS response if it wasn't empty and cause a
				// mismatch
				ssEndOfStream = true;
			} else {
				tssData.metrics->ssError(e.code());
			}
			TEST(e.code() != error_code_end_of_stream); // SS got error in TSS stream comparison
		}

		state double sleepTime = std::max(startTime + FLOW_KNOBS->LOAD_BALANCE_TSS_TIMEOUT - now(), 0.0);
		// wait for tss response
		try {
			choose {
				when(REPLYSTREAM_TYPE(Request) _tssReply = waitNext(tssReplyStream.getFuture())) {
					tssReply = _tssReply;
				}
				when(wait(delay(sleepTime))) {
					++tssData.metrics->tssTimeouts;
					TEST(true); // Got TSS timeout in stream comparison
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				streamData.setDone();
				throw;
			}
			if (e.code() == error_code_end_of_stream) {
				// tss response will be set to empty, to compare to the SS response if it wasn't empty and cause a
				// mismatch
				tssEndOfStream = true;
			} else {
				tssData.metrics->tssError(e.code());
			}
			TEST(e.code() != error_code_end_of_stream); // TSS got error in TSS stream comparison
		}

		if (!ssEndOfStream || !tssEndOfStream) {
			++tssData.metrics->streamComparisons;
		}

		// if both are successful, compare
		if (ssReply.present() && tssReply.present()) {
			// compare results
			// FIXME: this code is pretty much identical to LoadBalance.h
			// TODO could add team check logic in if we added synchronous way to turn this into a fixed getRange request
			// and send it to the whole team and compare? I think it's fine to skip that for streaming though
			TEST(ssEndOfStream != tssEndOfStream); // SS or TSS stream finished early!

			// skip tss comparison if both are end of stream
			if ((!ssEndOfStream || !tssEndOfStream) && !TSS_doCompare(ssReply.get(), tssReply.get())) {
				TEST(true); // TSS mismatch in stream comparison
				TraceEvent mismatchEvent(
				    (g_network->isSimulated() && g_simulator.tssMode == ISimulator::TSSMode::EnabledDropMutations)
				        ? SevWarnAlways
				        : SevError,
				    TSS_mismatchTraceName(request));
				mismatchEvent.setMaxEventLength(FLOW_KNOBS->TSS_LARGE_TRACE_SIZE);
				mismatchEvent.detail("TSSID", tssData.tssId);

				if (tssData.metrics->shouldRecordDetailedMismatch()) {
					TSS_traceMismatch(mismatchEvent, request, ssReply.get(), tssReply.get());

					TEST(FLOW_KNOBS
					         ->LOAD_BALANCE_TSS_MISMATCH_TRACE_FULL); // Tracing Full TSS Mismatch in stream comparison
					TEST(!FLOW_KNOBS->LOAD_BALANCE_TSS_MISMATCH_TRACE_FULL); // Tracing Partial TSS Mismatch in stream
					                                                         // comparison and storing the rest in FDB

					if (!FLOW_KNOBS->LOAD_BALANCE_TSS_MISMATCH_TRACE_FULL) {
						mismatchEvent.disable();
						UID mismatchUID = deterministicRandom()->randomUniqueID();
						tssData.metrics->recordDetailedMismatchData(mismatchUID, mismatchEvent.getFields().toString());

						// record a summarized trace event instead
						TraceEvent summaryEvent((g_network->isSimulated() &&
						                         g_simulator.tssMode == ISimulator::TSSMode::EnabledDropMutations)
						                            ? SevWarnAlways
						                            : SevError,
						                        TSS_mismatchTraceName(request));
						summaryEvent.detail("TSSID", tssData.tssId).detail("MismatchId", mismatchUID);
					}
				} else {
					// don't record trace event
					mismatchEvent.disable();
				}
				streamData.setDone();
				return Void();
			}
		}
		if (!ssReply.present() || !tssReply.present() || ssEndOfStream || tssEndOfStream) {
			// if both streams don't still have more data, stop comparison
			streamData.setDone();
			return Void();
		}
	}
}

// Currently only used for GetKeyValuesStream but could easily be plugged for other stream types
// User of the stream has to forward the SS's responses to the returned promise stream, if it is set
template <class Request>
Optional<TSSDuplicateStreamData<REPLYSTREAM_TYPE(Request)>>
maybeDuplicateTSSStreamFragment(Request& req, QueueModel* model, RequestStream<Request> const* ssStream) {
	if (model) {
		Optional<TSSEndpointData> tssData = model->getTssData(ssStream->getEndpoint().token.first());

		if (tssData.present()) {
			TEST(true); // duplicating stream to TSS
			resetReply(req);
			// FIXME: optimize to avoid creating new netNotifiedQueueWithAcknowledgements for each stream duplication
			RequestStream<Request> tssRequestStream(tssData.get().endpoint);
			ReplyPromiseStream<REPLYSTREAM_TYPE(Request)> tssReplyStream = tssRequestStream.getReplyStream(req);
			PromiseStream<REPLYSTREAM_TYPE(Request)> ssDuplicateReplyStream;
			TSSDuplicateStreamData<REPLYSTREAM_TYPE(Request)> streamData(ssDuplicateReplyStream);
			model->addActor.send(tssStreamComparison(req, streamData, tssReplyStream, tssData.get()));
			return Optional<TSSDuplicateStreamData<REPLYSTREAM_TYPE(Request)>>(streamData);
		}
	}
	return Optional<TSSDuplicateStreamData<REPLYSTREAM_TYPE(Request)>>();
}

// Streams all of the KV pairs in a target key range into a ParallelStream fragment
ACTOR Future<Void> getRangeStreamFragment(Reference<TransactionState> trState,
                                          ParallelStream<RangeResult>::Fragment* results,
                                          Version version,
                                          KeyRange keys,
                                          GetRangeLimits limits,
                                          Snapshot snapshot,
                                          Reverse reverse,
                                          SpanID spanContext) {
	loop {
		state std::vector<KeyRangeLocationInfo> locations =
		    wait(getKeyRangeLocations(trState,
		                              keys,
		                              CLIENT_KNOBS->GET_RANGE_SHARD_LIMIT,
		                              reverse,
		                              &StorageServerInterface::getKeyValuesStream,
		                              UseTenant::True,
		                              version));
		ASSERT(locations.size());
		state int shard = 0;
		loop {
			const KeyRange& range = locations[shard].range;

			state Optional<TSSDuplicateStreamData<GetKeyValuesStreamReply>> tssDuplicateStream;
			state GetKeyValuesStreamRequest req;
			req.tenantInfo = trState->getTenantInfo();
			req.version = version;
			req.begin = firstGreaterOrEqual(range.begin);
			req.end = firstGreaterOrEqual(range.end);
			req.spanContext = spanContext;
			req.limit = reverse ? -CLIENT_KNOBS->REPLY_BYTE_LIMIT : CLIENT_KNOBS->REPLY_BYTE_LIMIT;
			req.limitBytes = std::numeric_limits<int>::max();
			trState->cx->getLatestCommitVersions(
			    locations[shard].locations, req.version, trState, req.ssLatestCommitVersions);

			// keep shard's arena around in case of async tss comparison
			req.arena.dependsOn(range.arena());

			ASSERT(req.limitBytes > 0 && req.limit != 0 && req.limit < 0 == reverse);

			// FIXME: buggify byte limits on internal functions that use them, instead of globally
			req.tags = trState->cx->sampleReadTags() ? trState->options.readTags : Optional<TagSet>();
			req.debugID = trState->debugID;

			try {
				if (trState->debugID.present()) {
					g_traceBatch.addEvent(
					    "TransactionDebug", trState->debugID.get().first(), "NativeAPI.RangeStream.Before");
				}
				++trState->cx->transactionPhysicalReads;
				state GetKeyValuesStreamReply rep;

				if (locations[shard].locations->size() == 0) {
					wait(trState->cx->connectionFileChanged());
					results->sendError(transaction_too_old());
					return Void();
				}

				state int useIdx = -1;

				loop {
					// FIXME: create a load balance function for this code so future users of reply streams do not have
					// to duplicate this code
					int count = 0;
					for (int i = 0; i < locations[shard].locations->size(); i++) {
						if (!IFailureMonitor::failureMonitor()
						         .getState(locations[shard]
						                       .locations->get(i, &StorageServerInterface::getKeyValuesStream)
						                       .getEndpoint())
						         .failed) {
							if (deterministicRandom()->random01() <= 1.0 / ++count) {
								useIdx = i;
							}
						}
					}

					if (useIdx >= 0) {
						break;
					}

					std::vector<Future<Void>> ok(locations[shard].locations->size());
					for (int i = 0; i < ok.size(); i++) {
						ok[i] = IFailureMonitor::failureMonitor().onStateEqual(
						    locations[shard]
						        .locations->get(i, &StorageServerInterface::getKeyValuesStream)
						        .getEndpoint(),
						    FailureStatus(false));
					}

					// Making this SevWarn means a lot of clutter
					if (now() - g_network->networkInfo.newestAlternativesFailure > 1 ||
					    deterministicRandom()->random01() < 0.01) {
						TraceEvent("AllAlternativesFailed")
						    .detail("Alternatives", locations[shard].locations->description());
					}

					wait(allAlternativesFailedDelay(quorum(ok, 1)));
				}

				state ReplyPromiseStream<GetKeyValuesStreamReply> replyStream =
				    locations[shard]
				        .locations->get(useIdx, &StorageServerInterface::getKeyValuesStream)
				        .getReplyStream(req);

				tssDuplicateStream = maybeDuplicateTSSStreamFragment(
				    req,
				    trState->cx->enableLocalityLoadBalance ? &trState->cx->queueModel : nullptr,
				    &locations[shard].locations->get(useIdx, &StorageServerInterface::getKeyValuesStream));

				state bool breakAgain = false;
				loop {
					wait(results->onEmpty());
					try {
						choose {
							when(wait(trState->cx->connectionFileChanged())) {
								results->sendError(transaction_too_old());
								if (tssDuplicateStream.present() && !tssDuplicateStream.get().done()) {
									tssDuplicateStream.get().stream.sendError(transaction_too_old());
								}
								return Void();
							}

							when(GetKeyValuesStreamReply _rep = waitNext(replyStream.getFuture())) {
								rep = _rep;
							}
						}
						++trState->cx->transactionPhysicalReadsCompleted;
					} catch (Error& e) {
						++trState->cx->transactionPhysicalReadsCompleted;
						if (e.code() == error_code_broken_promise) {
							if (tssDuplicateStream.present() && !tssDuplicateStream.get().done()) {
								tssDuplicateStream.get().stream.sendError(connection_failed());
							}
							throw connection_failed();
						}
						if (e.code() != error_code_end_of_stream) {
							if (tssDuplicateStream.present() && !tssDuplicateStream.get().done()) {
								tssDuplicateStream.get().stream.sendError(e);
							}
							throw;
						}
						rep = GetKeyValuesStreamReply();
					}
					if (trState->debugID.present())
						g_traceBatch.addEvent(
						    "TransactionDebug", trState->debugID.get().first(), "NativeAPI.getExactRange.After");
					RangeResult output(RangeResultRef(rep.data, rep.more), rep.arena);

					if (tssDuplicateStream.present() && !tssDuplicateStream.get().done()) {
						// shallow copy the reply with an arena depends, and send it to the duplicate stream for TSS
						GetKeyValuesStreamReply replyCopy;
						replyCopy.version = rep.version;
						replyCopy.more = rep.more;
						replyCopy.cached = rep.cached;
						replyCopy.arena.dependsOn(rep.arena);
						replyCopy.data.append(replyCopy.arena, rep.data.begin(), rep.data.size());
						tssDuplicateStream.get().stream.send(replyCopy);
					}

					int64_t bytes = 0;
					for (const KeyValueRef& kv : output) {
						bytes += kv.key.size() + kv.value.size();
					}

					trState->cx->transactionBytesRead += bytes;
					trState->cx->transactionKeysRead += output.size();

					// If the reply says there is more but we know that we finished the shard, then fix rep.more
					if (reverse && output.more && rep.data.size() > 0 &&
					    output[output.size() - 1].key == locations[shard].range.begin) {
						output.more = false;
					}

					if (output.more) {
						if (!rep.data.size()) {
							TraceEvent(SevError, "GetRangeStreamError")
							    .detail("Reason", "More data indicated but no rows present")
							    .detail("LimitBytes", limits.bytes)
							    .detail("LimitRows", limits.rows)
							    .detail("OutputSize", output.size())
							    .detail("OutputBytes", output.expectedSize())
							    .detail("BlockSize", rep.data.size())
							    .detail("BlockBytes", rep.data.expectedSize());
							ASSERT(false);
						}
						TEST(true); // GetKeyValuesStreamReply.more in getRangeStream
						// Make next request to the same shard with a beginning key just after the last key returned
						if (reverse)
							locations[shard].range =
							    KeyRangeRef(locations[shard].range.begin, output[output.size() - 1].key);
						else
							locations[shard].range =
							    KeyRangeRef(keyAfter(output[output.size() - 1].key), locations[shard].range.end);
					}

					if (locations[shard].range.empty()) {
						output.more = false;
					}

					if (!output.more) {
						const KeyRange& range = locations[shard].range;
						if (shard == locations.size() - 1) {
							KeyRef begin = reverse ? keys.begin : range.end;
							KeyRef end = reverse ? range.begin : keys.end;

							if (begin >= end) {
								if (range.begin == allKeys.begin) {
									output.readToBegin = true;
								}
								if (range.end == allKeys.end) {
									output.readThroughEnd = true;
								}
								output.arena().dependsOn(keys.arena());
								output.readThrough = reverse ? keys.begin : keys.end;
								results->send(std::move(output));
								results->finish();
								if (tssDuplicateStream.present() && !tssDuplicateStream.get().done()) {
									tssDuplicateStream.get().stream.sendError(end_of_stream());
								}
								return Void();
							}
							keys = KeyRangeRef(begin, end);
							breakAgain = true;
						} else {
							++shard;
						}
						output.arena().dependsOn(range.arena());
						output.readThrough = reverse ? range.begin : range.end;
						results->send(std::move(output));
						break;
					}

					ASSERT(output.size());
					if (keys.begin == allKeys.begin && !reverse) {
						output.readToBegin = true;
					}
					if (keys.end == allKeys.end && reverse) {
						output.readThroughEnd = true;
					}
					results->send(std::move(output));
				}
				if (breakAgain) {
					break;
				}
			} catch (Error& e) {
				// send errors to tss duplicate stream, including actor_cancelled
				if (tssDuplicateStream.present() && !tssDuplicateStream.get().done()) {
					tssDuplicateStream.get().stream.sendError(e);
				}
				if (e.code() == error_code_actor_cancelled) {
					throw;
				}
				if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed ||
				    e.code() == error_code_connection_failed) {
					const KeyRangeRef& range = locations[shard].range;

					if (reverse)
						keys = KeyRangeRef(keys.begin, range.end);
					else
						keys = KeyRangeRef(range.begin, keys.end);

					trState->cx->invalidateCache(locations[0].tenantEntry.prefix, keys);

					wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, trState->taskID));
					break;
				} else if (e.code() == error_code_unknown_tenant) {
					ASSERT(trState->tenant().present());
					trState->cx->invalidateCachedTenant(trState->tenant().get());
					wait(delay(CLIENT_KNOBS->UNKNOWN_TENANT_RETRY_DELAY, trState->taskID));
					break;
				} else {
					results->sendError(e);
					return Void();
				}
			}
		}
	}
}

ACTOR Future<Standalone<VectorRef<KeyRef>>> getRangeSplitPoints(Reference<TransactionState> trState,
                                                                KeyRange keys,
                                                                int64_t chunkSize,
                                                                Version version);

static KeyRange intersect(KeyRangeRef lhs, KeyRangeRef rhs) {
	return KeyRange(KeyRangeRef(std::max(lhs.begin, rhs.begin), std::min(lhs.end, rhs.end)));
}

// Divides the requested key range into 1MB fragments, create range streams for each fragment, and merges the results so
// the client get them in order
ACTOR Future<Void> getRangeStream(Reference<TransactionState> trState,
                                  PromiseStream<RangeResult> _results,
                                  Future<Version> fVersion,
                                  KeySelector begin,
                                  KeySelector end,
                                  GetRangeLimits limits,
                                  Promise<std::pair<Key, Key>> conflictRange,
                                  Snapshot snapshot,
                                  Reverse reverse) {
	state ParallelStream<RangeResult> results(_results, CLIENT_KNOBS->RANGESTREAM_BUFFERED_FRAGMENTS_LIMIT);

	// FIXME: better handling to disable row limits
	ASSERT(!limits.hasRowLimit());
	state Span span("NAPI:getRangeStream"_loc, trState->spanID);

	state Version version = wait(fVersion);
	trState->cx->validateVersion(version);

	Future<Key> fb = resolveKey(trState, begin, version, UseTenant::True);
	state Future<Key> fe = resolveKey(trState, end, version, UseTenant::True);

	state Key b = wait(fb);
	state Key e = wait(fe);

	if (!snapshot) {
		// FIXME: this conflict range is too large, and should be updated continously as results are returned
		conflictRange.send(std::make_pair(std::min(b, Key(begin.getKey(), begin.arena())),
		                                  std::max(e, Key(end.getKey(), end.arena()))));
	}

	if (b >= e) {
		wait(results.finish());
		return Void();
	}

	// if e is allKeys.end, we have read through the end of the database
	// if b is allKeys.begin, we have either read through the beginning of the database,
	// or allKeys.begin exists in the database and will be part of the conflict range anyways

	state std::vector<Future<Void>> outstandingRequests;
	while (b < e) {
		state KeyRangeLocationInfo locationInfo = wait(getKeyLocation(
		    trState, reverse ? e : b, &StorageServerInterface::getKeyValuesStream, reverse, UseTenant::True, version));
		state KeyRange shardIntersection = intersect(locationInfo.range, KeyRangeRef(b, e));
		state Standalone<VectorRef<KeyRef>> splitPoints =
		    wait(getRangeSplitPoints(trState, shardIntersection, CLIENT_KNOBS->RANGESTREAM_FRAGMENT_SIZE, version));
		state std::vector<KeyRange> toSend;
		// state std::vector<Future<std::list<KeyRangeRef>::iterator>> outstandingRequests;

		if (!splitPoints.empty()) {
			toSend.push_back(KeyRange(KeyRangeRef(shardIntersection.begin, splitPoints.front()), splitPoints.arena()));
			for (int i = 0; i < splitPoints.size() - 1; ++i) {
				toSend.push_back(KeyRange(KeyRangeRef(splitPoints[i], splitPoints[i + 1]), splitPoints.arena()));
			}
			toSend.push_back(KeyRange(KeyRangeRef(splitPoints.back(), shardIntersection.end), splitPoints.arena()));
		} else {
			toSend.push_back(KeyRange(KeyRangeRef(shardIntersection.begin, shardIntersection.end)));
		}

		state int idx = 0;
		state int useIdx = 0;
		for (; idx < toSend.size(); ++idx) {
			useIdx = reverse ? toSend.size() - idx - 1 : idx;
			if (toSend[useIdx].empty()) {
				continue;
			}
			ParallelStream<RangeResult>::Fragment* fragment = wait(results.createFragment());
			outstandingRequests.push_back(getRangeStreamFragment(
			    trState, fragment, version, toSend[useIdx], limits, snapshot, reverse, span.context));
		}
		if (reverse) {
			e = shardIntersection.begin;
		} else {
			b = shardIntersection.end;
		}
	}
	wait(waitForAll(outstandingRequests) && results.finish());
	return Void();
}

Future<RangeResult> getRange(Reference<TransactionState> const& trState,
                             Future<Version> const& fVersion,
                             KeySelector const& begin,
                             KeySelector const& end,
                             GetRangeLimits const& limits,
                             Reverse const& reverse,
                             UseTenant const& useTenant) {
	return getRange<GetKeyValuesRequest, GetKeyValuesReply, RangeResult>(trState,
	                                                                     fVersion,
	                                                                     begin,
	                                                                     end,
	                                                                     ""_sr,
	                                                                     limits,
	                                                                     Promise<std::pair<Key, Key>>(),
	                                                                     Snapshot::True,
	                                                                     reverse,
	                                                                     useTenant);
}

bool DatabaseContext::debugUseTags = false;
const std::vector<std::string> DatabaseContext::debugTransactionTagChoices = { "a", "b", "c", "d", "e", "f", "g",
	                                                                           "h", "i", "j", "k", "l", "m", "n",
	                                                                           "o", "p", "q", "r", "s", "t" };

void debugAddTags(Reference<TransactionState> trState) {
	int numTags = deterministicRandom()->randomInt(0, CLIENT_KNOBS->MAX_TAGS_PER_TRANSACTION + 1);
	for (int i = 0; i < numTags; ++i) {
		TransactionTag tag;
		if (deterministicRandom()->random01() < 0.7) {
			tag = TransactionTagRef(deterministicRandom()->randomChoice(DatabaseContext::debugTransactionTagChoices));
		} else {
			int length = deterministicRandom()->randomInt(1, CLIENT_KNOBS->MAX_TRANSACTION_TAG_LENGTH + 1);
			uint8_t* s = new (tag.arena()) uint8_t[length];
			for (int j = 0; j < length; ++j) {
				s[j] = (uint8_t)deterministicRandom()->randomInt(0, 256);
			}

			tag.contents() = TransactionTagRef(s, length);
		}

		if (deterministicRandom()->coinflip()) {
			trState->options.readTags.addTag(tag);
		}
		trState->options.tags.addTag(tag);
	}
}

Transaction::Transaction()
  : trState(makeReference<TransactionState>(TaskPriority::DefaultEndpoint, generateSpanID(false))) {}

Transaction::Transaction(Database const& cx, Optional<TenantName> const& tenant)
  : trState(makeReference<TransactionState>(cx,
                                            tenant,
                                            cx->taskID,
                                            generateSpanID(cx->transactionTracingSample),
                                            createTrLogInfoProbabilistically(cx))),
    span(trState->spanID, "Transaction"_loc), backoff(CLIENT_KNOBS->DEFAULT_BACKOFF), tr(trState->spanID) {
	if (DatabaseContext::debugUseTags) {
		debugAddTags(trState);
	}
}

Transaction::~Transaction() {
	flushTrLogsIfEnabled();
	cancelWatches();
}

void Transaction::operator=(Transaction&& r) noexcept {
	flushTrLogsIfEnabled();
	tr = std::move(r.tr);
	readVersion = std::move(r.readVersion);
	trState = std::move(r.trState);
	metadataVersion = std::move(r.metadataVersion);
	extraConflictRanges = std::move(r.extraConflictRanges);
	commitResult = std::move(r.commitResult);
	committing = std::move(r.committing);
	backoff = r.backoff;
	watches = r.watches;
}

void Transaction::flushTrLogsIfEnabled() {
	if (trState && trState->trLogInfo && trState->trLogInfo->logsAdded && trState->trLogInfo->trLogWriter.getData()) {
		ASSERT(trState->trLogInfo->flushed == false);
		trState->cx->clientStatusUpdater.inStatusQ.push_back(
		    { trState->trLogInfo->identifier, std::move(trState->trLogInfo->trLogWriter) });
		trState->trLogInfo->flushed = true;
	}
}

VersionVector Transaction::getVersionVector() const {
	return trState->cx->ssVersionVectorCache;
}

void Transaction::setVersion(Version v) {
	trState->startTime = now();
	if (readVersion.isValid())
		throw read_version_already_set();
	if (v <= 0)
		throw version_invalid();

	readVersion = v;
	trState->readVersionObtainedFromGrvProxy = false;
}

Future<Optional<Value>> Transaction::get(const Key& key, Snapshot snapshot) {
	++trState->cx->transactionLogicalReads;
	++trState->cx->transactionGetValueRequests;
	// ASSERT (key < allKeys.end);

	// There are no keys in the database with size greater than the max key size
	if (key.size() > getMaxReadKeySize(key)) {
		return Optional<Value>();
	}

	auto ver = getReadVersion();

	/*	if (!systemKeys.contains(key))
	        return Optional<Value>(Value()); */

	if (!snapshot)
		tr.transaction.read_conflict_ranges.push_back(tr.arena, singleKeyRange(key, tr.arena));

	UseTenant useTenant = UseTenant::True;
	if (key == metadataVersionKey) {
		// It is legal to read the metadata version key inside of a tenant.
		// This will return the global metadata version key.
		useTenant = UseTenant::False;
		++trState->cx->transactionMetadataVersionReads;
		if (!ver.isReady() || metadataVersion.isSet()) {
			return metadataVersion.getFuture();
		} else {
			if (ver.isError())
				return ver.getError();
			if (ver.get() == trState->cx->metadataVersionCache[trState->cx->mvCacheInsertLocation].first) {
				return trState->cx->metadataVersionCache[trState->cx->mvCacheInsertLocation].second;
			}

			Version v = ver.get();
			int hi = trState->cx->mvCacheInsertLocation;
			int lo = (trState->cx->mvCacheInsertLocation + 1) % trState->cx->metadataVersionCache.size();

			while (hi != lo) {
				int cu = hi > lo ? (hi + lo) / 2
				                 : ((hi + trState->cx->metadataVersionCache.size() + lo) / 2) %
				                       trState->cx->metadataVersionCache.size();
				if (v == trState->cx->metadataVersionCache[cu].first) {
					return trState->cx->metadataVersionCache[cu].second;
				}
				if (cu == lo) {
					break;
				}
				if (v < trState->cx->metadataVersionCache[cu].first) {
					hi = cu;
				} else {
					lo = (cu + 1) % trState->cx->metadataVersionCache.size();
				}
			}
		}
	}

	return getValue(trState, key, ver, useTenant);
}

void Watch::setWatch(Future<Void> watchFuture) {
	this->watchFuture = watchFuture;

	// Cause the watch loop to go around and start waiting on watchFuture
	onSetWatchTrigger.send(Void());
}

ACTOR Future<TenantInfo> getTenantMetadata(Reference<TransactionState> trState, Key key, Version version) {
	KeyRangeLocationInfo locationInfo =
	    wait(getKeyLocation(trState, key, &StorageServerInterface::getValue, Reverse::False, UseTenant::True, version));
	return trState->getTenantInfo();
}

Future<TenantInfo> populateAndGetTenant(Reference<TransactionState> trState, Key const& key, Version version) {
	if (!trState->tenant().present() || key == metadataVersionKey) {
		return TenantInfo();
	} else if (trState->tenantId != TenantInfo::INVALID_TENANT) {
		return trState->getTenantInfo();
	} else {
		return getTenantMetadata(trState, key, version);
	}
}

// FIXME: This seems pretty horrible. Now a Database can't die until all of its watches do...
ACTOR Future<Void> watch(Reference<Watch> watch,
                         Database cx,
                         Future<TenantInfo> tenant,
                         TagSet tags,
                         SpanID spanID,
                         TaskPriority taskID,
                         Optional<UID> debugID,
                         UseProvisionalProxies useProvisionalProxies) {
	try {
		choose {
			// RYOW write to value that is being watched (if applicable)
			// Errors
			when(wait(watch->onChangeTrigger.getFuture())) {}

			// NativeAPI finished commit and updated watchFuture
			when(wait(watch->onSetWatchTrigger.getFuture())) {

				state TenantInfo tenantInfo = wait(tenant);
				loop {
					choose {
						// NativeAPI watchValue future finishes or errors
						when(wait(watch->watchFuture)) {
							break;
						}

						when(wait(cx->connectionFileChanged())) {
							TEST(true); // Recreated a watch after switch
							cx->clearWatchMetadata();
							watch->watchFuture = watchValueMap(cx->minAcceptableReadVersion,
							                                   tenantInfo,
							                                   watch->key,
							                                   watch->value,
							                                   cx,
							                                   tags,
							                                   spanID,
							                                   taskID,
							                                   debugID,
							                                   useProvisionalProxies);
						}
					}
				}
			}
		}
	} catch (Error& e) {
		cx->removeWatch();
		throw;
	}

	cx->removeWatch();
	return Void();
}

Future<Version> Transaction::getRawReadVersion() {
	return ::getRawVersion(trState);
}

Future<Void> Transaction::watch(Reference<Watch> watch) {
	++trState->cx->transactionWatchRequests;

	trState->cx->addWatch();
	watches.push_back(watch);
	return ::watch(
	    watch,
	    trState->cx,
	    populateAndGetTenant(
	        trState, watch->key, readVersion.isValid() && readVersion.isReady() ? readVersion.get() : latestVersion),
	    trState->options.readTags,
	    trState->spanID,
	    trState->taskID,
	    trState->debugID,
	    trState->useProvisionalProxies);
}

ACTOR Future<Standalone<VectorRef<const char*>>> getAddressesForKeyActor(Reference<TransactionState> trState,
                                                                         Future<Version> ver,
                                                                         Key key) {
	state std::vector<StorageServerInterface> ssi;

	state Key resolvedKey = key;
	if (trState->tenant().present()) {
		state Version version = wait(ver);
		KeyRangeLocationInfo locationInfo = wait(getKeyLocation(
		    trState, ""_sr, &StorageServerInterface::getValue, Reverse::False, UseTenant::True, version));
		resolvedKey = key.withPrefix(locationInfo.tenantEntry.prefix);
	}

	// If key >= allKeys.end, then getRange will return a kv-pair with an empty value. This will result in our
	// serverInterfaces vector being empty, which will cause us to return an empty addresses list.
	state Key ksKey = keyServersKey(resolvedKey);
	state RangeResult serverTagResult = wait(getRange(trState,
	                                                  ver,
	                                                  lastLessOrEqual(serverTagKeys.begin),
	                                                  firstGreaterThan(serverTagKeys.end),
	                                                  GetRangeLimits(CLIENT_KNOBS->TOO_MANY),
	                                                  Reverse::False,
	                                                  UseTenant::False));
	ASSERT(!serverTagResult.more && serverTagResult.size() < CLIENT_KNOBS->TOO_MANY);
	Future<RangeResult> futureServerUids = getRange(trState,
	                                                ver,
	                                                lastLessOrEqual(ksKey),
	                                                firstGreaterThan(ksKey),
	                                                GetRangeLimits(1),
	                                                Reverse::False,
	                                                UseTenant::False);
	RangeResult serverUids = wait(futureServerUids);

	ASSERT(serverUids.size()); // every shard needs to have a team

	std::vector<UID> src;
	std::vector<UID> ignore; // 'ignore' is so named because it is the vector into which we decode the 'dest' servers in
	                         // the case where this key is being relocated. But 'src' is the canonical location until
	                         // the move is finished, because it could be cancelled at any time.
	decodeKeyServersValue(serverTagResult, serverUids[0].value, src, ignore);
	Optional<std::vector<StorageServerInterface>> serverInterfaces =
	    wait(transactionalGetServerInterfaces(trState, ver, src));

	ASSERT(serverInterfaces.present()); // since this is happening transactionally, /FF/keyServers and /FF/serverList
	                                    // need to be consistent with one another
	ssi = serverInterfaces.get();

	Standalone<VectorRef<const char*>> addresses;
	for (auto i : ssi) {
		std::string ipString = trState->options.includePort ? i.address().toString() : i.address().ip.toString();
		char* c_string = new (addresses.arena()) char[ipString.length() + 1];
		strcpy(c_string, ipString.c_str());
		addresses.push_back(addresses.arena(), c_string);
	}
	return addresses;
}

Future<Standalone<VectorRef<const char*>>> Transaction::getAddressesForKey(const Key& key) {
	++trState->cx->transactionLogicalReads;
	++trState->cx->transactionGetAddressesForKeyRequests;
	auto ver = getReadVersion();

	return getAddressesForKeyActor(trState, ver, key);
}

ACTOR Future<Key> getKeyAndConflictRange(Reference<TransactionState> trState,
                                         KeySelector k,
                                         Future<Version> version,
                                         Promise<std::pair<Key, Key>> conflictRange) {
	try {
		Key rep = wait(getKey(trState, k, version));
		if (k.offset <= 0)
			conflictRange.send(std::make_pair(rep, k.orEqual ? keyAfter(k.getKey()) : Key(k.getKey(), k.arena())));
		else
			conflictRange.send(
			    std::make_pair(k.orEqual ? keyAfter(k.getKey()) : Key(k.getKey(), k.arena()), keyAfter(rep)));
		return rep;
	} catch (Error& e) {
		conflictRange.send(std::make_pair(Key(), Key()));
		throw;
	}
}

Future<Key> Transaction::getKey(const KeySelector& key, Snapshot snapshot) {
	++trState->cx->transactionLogicalReads;
	++trState->cx->transactionGetKeyRequests;
	if (snapshot)
		return ::getKey(trState, key, getReadVersion());

	Promise<std::pair<Key, Key>> conflictRange;
	extraConflictRanges.push_back(conflictRange.getFuture());
	return getKeyAndConflictRange(trState, key, getReadVersion(), conflictRange);
}

template <class GetKeyValuesFamilyRequest>
void increaseCounterForRequest(Database cx) {
	if constexpr (std::is_same<GetKeyValuesFamilyRequest, GetKeyValuesRequest>::value) {
		++cx->transactionGetRangeRequests;
	} else if (std::is_same<GetKeyValuesFamilyRequest, GetMappedKeyValuesRequest>::value) {
		++cx->transactionGetMappedRangeRequests;
	} else {
		UNREACHABLE();
	}
}

template <class GetKeyValuesFamilyRequest, class GetKeyValuesFamilyReply, class RangeResultFamily>
Future<RangeResultFamily> Transaction::getRangeInternal(const KeySelector& begin,
                                                        const KeySelector& end,
                                                        const Key& mapper,
                                                        GetRangeLimits limits,
                                                        Snapshot snapshot,
                                                        Reverse reverse) {
	++trState->cx->transactionLogicalReads;
	increaseCounterForRequest<GetKeyValuesFamilyRequest>(trState->cx);

	if (limits.isReached())
		return RangeResultFamily();

	if (!limits.isValid())
		return range_limits_invalid();

	ASSERT(limits.rows != 0);

	KeySelector b = begin;
	if (b.orEqual) {
		TEST(true); // Native begin orEqual==true
		b.removeOrEqual(b.arena());
	}

	KeySelector e = end;
	if (e.orEqual) {
		TEST(true); // Native end orEqual==true
		e.removeOrEqual(e.arena());
	}

	if (b.offset >= e.offset && b.getKey() >= e.getKey()) {
		TEST(true); // Native range inverted
		return RangeResultFamily();
	}

	if (!snapshot && !std::is_same_v<GetKeyValuesFamilyRequest, GetKeyValuesRequest>) {
		// Currently, NativeAPI does not support serialization for getMappedRange. You should consider use
		// ReadYourWrites APIs which wraps around NativeAPI and provides serialization for getMappedRange. (Even if
		// you don't want RYW, you may use ReadYourWrites APIs with RYW disabled.)
		throw unsupported_operation();
	}
	Promise<std::pair<Key, Key>> conflictRange;
	if (!snapshot) {
		extraConflictRanges.push_back(conflictRange.getFuture());
	}

	return ::getRange<GetKeyValuesFamilyRequest, GetKeyValuesFamilyReply, RangeResultFamily>(
	    trState, getReadVersion(), b, e, mapper, limits, conflictRange, snapshot, reverse);
}

Future<RangeResult> Transaction::getRange(const KeySelector& begin,
                                          const KeySelector& end,
                                          GetRangeLimits limits,
                                          Snapshot snapshot,
                                          Reverse reverse) {
	return getRangeInternal<GetKeyValuesRequest, GetKeyValuesReply, RangeResult>(
	    begin, end, ""_sr, limits, snapshot, reverse);
}

Future<MappedRangeResult> Transaction::getMappedRange(const KeySelector& begin,
                                                      const KeySelector& end,
                                                      const Key& mapper,
                                                      GetRangeLimits limits,
                                                      Snapshot snapshot,
                                                      Reverse reverse) {
	return getRangeInternal<GetMappedKeyValuesRequest, GetMappedKeyValuesReply, MappedRangeResult>(
	    begin, end, mapper, limits, snapshot, reverse);
}

Future<RangeResult> Transaction::getRange(const KeySelector& begin,
                                          const KeySelector& end,
                                          int limit,
                                          Snapshot snapshot,
                                          Reverse reverse) {
	return getRange(begin, end, GetRangeLimits(limit), snapshot, reverse);
}

// A method for streaming data from the storage server that is more efficient than getRange when reading large amounts
// of data
Future<Void> Transaction::getRangeStream(const PromiseStream<RangeResult>& results,
                                         const KeySelector& begin,
                                         const KeySelector& end,
                                         GetRangeLimits limits,
                                         Snapshot snapshot,
                                         Reverse reverse) {
	++trState->cx->transactionLogicalReads;
	++trState->cx->transactionGetRangeStreamRequests;

	// FIXME: limits are not implemented yet, and this code has not be tested with reverse=true
	ASSERT(!limits.hasByteLimit() && !limits.hasRowLimit() && !reverse);

	KeySelector b = begin;
	if (b.orEqual) {
		TEST(true); // Native stream begin orEqual==true
		b.removeOrEqual(b.arena());
	}

	KeySelector e = end;
	if (e.orEqual) {
		TEST(true); // Native stream end orEqual==true
		e.removeOrEqual(e.arena());
	}

	if (b.offset >= e.offset && b.getKey() >= e.getKey()) {
		TEST(true); // Native stream range inverted
		results.sendError(end_of_stream());
		return Void();
	}

	Promise<std::pair<Key, Key>> conflictRange;
	if (!snapshot) {
		extraConflictRanges.push_back(conflictRange.getFuture());
	}

	return forwardErrors(
	    ::getRangeStream(trState, results, getReadVersion(), b, e, limits, conflictRange, snapshot, reverse), results);
}

Future<Void> Transaction::getRangeStream(const PromiseStream<RangeResult>& results,
                                         const KeySelector& begin,
                                         const KeySelector& end,
                                         int limit,
                                         Snapshot snapshot,
                                         Reverse reverse) {
	return getRangeStream(results, begin, end, GetRangeLimits(limit), snapshot, reverse);
}

void Transaction::addReadConflictRange(KeyRangeRef const& keys) {
	ASSERT(!keys.empty());

	// There aren't any keys in the database with size larger than the max key size, so if range contains large keys
	// we can translate it to an equivalent one with smaller keys
	KeyRef begin = keys.begin;
	KeyRef end = keys.end;

	int64_t beginMaxSize = getMaxReadKeySize(begin);
	int64_t endMaxSize = getMaxReadKeySize(end);
	if (begin.size() > beginMaxSize) {
		begin = begin.substr(0, beginMaxSize + 1);
	}
	if (end.size() > endMaxSize) {
		end = end.substr(0, endMaxSize + 1);
	}

	KeyRangeRef r = KeyRangeRef(begin, end);

	if (r.empty()) {
		return;
	}

	tr.transaction.read_conflict_ranges.push_back_deep(tr.arena, r);
}

void Transaction::makeSelfConflicting() {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(LiteralStringRef("\xFF/SC/"));
	wr << deterministicRandom()->randomUniqueID();
	auto r = singleKeyRange(wr.toValue(), tr.arena);
	tr.transaction.read_conflict_ranges.push_back(tr.arena, r);
	tr.transaction.write_conflict_ranges.push_back(tr.arena, r);
}

void Transaction::set(const KeyRef& key, const ValueRef& value, AddConflictRange addConflictRange) {
	++trState->cx->transactionSetMutations;
	if (key.size() > getMaxWriteKeySize(key, trState->options.rawAccess))
		throw key_too_large();
	if (value.size() > CLIENT_KNOBS->VALUE_SIZE_LIMIT)
		throw value_too_large();

	auto& req = tr;
	auto& t = req.transaction;
	auto r = singleKeyRange(key, req.arena);
	auto v = ValueRef(req.arena, value);
	t.mutations.emplace_back(req.arena, MutationRef::SetValue, r.begin, v);

	if (addConflictRange) {
		t.write_conflict_ranges.push_back(req.arena, r);
	}
}

void Transaction::atomicOp(const KeyRef& key,
                           const ValueRef& operand,
                           MutationRef::Type operationType,
                           AddConflictRange addConflictRange) {
	++trState->cx->transactionAtomicMutations;
	if (key.size() > getMaxWriteKeySize(key, trState->options.rawAccess))
		throw key_too_large();
	if (operand.size() > CLIENT_KNOBS->VALUE_SIZE_LIMIT)
		throw value_too_large();

	if (apiVersionAtLeast(510)) {
		if (operationType == MutationRef::Min)
			operationType = MutationRef::MinV2;
		else if (operationType == MutationRef::And)
			operationType = MutationRef::AndV2;
	}

	auto& req = tr;
	auto& t = req.transaction;
	auto r = singleKeyRange(key, req.arena);
	auto v = ValueRef(req.arena, operand);

	t.mutations.emplace_back(req.arena, operationType, r.begin, v);

	if (addConflictRange && operationType != MutationRef::SetVersionstampedKey)
		t.write_conflict_ranges.push_back(req.arena, r);

	TEST(true); // NativeAPI atomic operation
}

void Transaction::clear(const KeyRangeRef& range, AddConflictRange addConflictRange) {
	++trState->cx->transactionClearMutations;
	auto& req = tr;
	auto& t = req.transaction;

	KeyRef begin = range.begin;
	KeyRef end = range.end;

	// There aren't any keys in the database with size larger than the max key size, so if range contains large keys
	// we can translate it to an equivalent one with smaller keys
	int64_t beginMaxSize = getMaxClearKeySize(begin);
	int64_t endMaxSize = getMaxClearKeySize(end);
	if (begin.size() > beginMaxSize) {
		begin = begin.substr(0, beginMaxSize + 1);
	}
	if (end.size() > endMaxSize) {
		end = end.substr(0, endMaxSize + 1);
	}

	auto r = KeyRangeRef(req.arena, KeyRangeRef(begin, end));
	if (r.empty())
		return;

	t.mutations.emplace_back(req.arena, MutationRef::ClearRange, r.begin, r.end);

	if (addConflictRange)
		t.write_conflict_ranges.push_back(req.arena, r);
}
void Transaction::clear(const KeyRef& key, AddConflictRange addConflictRange) {
	++trState->cx->transactionClearMutations;
	// There aren't any keys in the database with size larger than the max key size
	if (key.size() > getMaxClearKeySize(key)) {
		return;
	}

	auto& req = tr;
	auto& t = req.transaction;

	// efficient single key range clear range mutation, see singleKeyRange
	uint8_t* data = new (req.arena) uint8_t[key.size() + 1];
	memcpy(data, key.begin(), key.size());
	data[key.size()] = 0;
	t.mutations.emplace_back(
	    req.arena, MutationRef::ClearRange, KeyRef(data, key.size()), KeyRef(data, key.size() + 1));
	if (addConflictRange)
		t.write_conflict_ranges.emplace_back(req.arena, KeyRef(data, key.size()), KeyRef(data, key.size() + 1));
}
void Transaction::addWriteConflictRange(const KeyRangeRef& keys) {
	ASSERT(!keys.empty());
	auto& req = tr;
	auto& t = req.transaction;

	// There aren't any keys in the database with size larger than the max key size, so if range contains large keys
	// we can translate it to an equivalent one with smaller keys
	KeyRef begin = keys.begin;
	KeyRef end = keys.end;

	int64_t beginMaxSize = getMaxKeySize(begin);
	int64_t endMaxSize = getMaxKeySize(end);
	if (begin.size() > beginMaxSize) {
		begin = begin.substr(0, beginMaxSize + 1);
	}
	if (end.size() > endMaxSize) {
		end = end.substr(0, endMaxSize + 1);
	}
	KeyRangeRef r = KeyRangeRef(begin, end);

	if (r.empty()) {
		return;
	}

	t.write_conflict_ranges.push_back_deep(req.arena, r);
}

double Transaction::getBackoff(int errCode) {
	double returnedBackoff = backoff;

	if (errCode == error_code_tag_throttled) {
		auto priorityItr = trState->cx->throttledTags.find(trState->options.priority);
		for (auto& tag : trState->options.tags) {
			if (priorityItr != trState->cx->throttledTags.end()) {
				auto tagItr = priorityItr->second.find(tag);
				if (tagItr != priorityItr->second.end()) {
					TEST(true); // Returning throttle backoff
					returnedBackoff = std::max(
					    returnedBackoff,
					    std::min(CLIENT_KNOBS->TAG_THROTTLE_RECHECK_INTERVAL, tagItr->second.throttleDuration()));
					if (returnedBackoff == CLIENT_KNOBS->TAG_THROTTLE_RECHECK_INTERVAL) {
						break;
					}
				}
			}
		}
	}

	returnedBackoff *= deterministicRandom()->random01();

	// Set backoff for next time
	if (errCode == error_code_proxy_memory_limit_exceeded) {
		backoff = std::min(backoff * CLIENT_KNOBS->BACKOFF_GROWTH_RATE, CLIENT_KNOBS->RESOURCE_CONSTRAINED_MAX_BACKOFF);
	} else {
		backoff = std::min(backoff * CLIENT_KNOBS->BACKOFF_GROWTH_RATE, trState->options.maxBackoff);
	}

	return returnedBackoff;
}

TransactionOptions::TransactionOptions(Database const& cx) {
	reset(cx);
	if (BUGGIFY) {
		commitOnFirstProxy = true;
	}
}

void TransactionOptions::clear() {
	maxBackoff = CLIENT_KNOBS->DEFAULT_MAX_BACKOFF;
	getReadVersionFlags = 0;
	sizeLimit = CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT;
	maxTransactionLoggingFieldLength = 0;
	checkWritesEnabled = false;
	causalWriteRisky = false;
	commitOnFirstProxy = false;
	debugDump = false;
	lockAware = false;
	readOnly = false;
	firstInBatch = false;
	includePort = false;
	reportConflictingKeys = false;
	tags = TagSet{};
	readTags = TagSet{};
	priority = TransactionPriority::DEFAULT;
	expensiveClearCostEstimation = false;
	useGrvCache = false;
	skipGrvCache = false;
	rawAccess = false;
}

TransactionOptions::TransactionOptions() {
	clear();
}

void TransactionOptions::reset(Database const& cx) {
	clear();
	lockAware = cx->lockAware;
	if (cx->apiVersionAtLeast(630)) {
		includePort = true;
	}
}

void Transaction::resetImpl(bool generateNewSpan) {
	flushTrLogsIfEnabled();
	trState = trState->cloneAndReset(createTrLogInfoProbabilistically(trState->cx), generateNewSpan);
	tr = CommitTransactionRequest(trState->spanID);
	readVersion = Future<Version>();
	metadataVersion = Promise<Optional<Key>>();
	extraConflictRanges.clear();
	commitResult = Promise<Void>();
	committing = Future<Void>();
	cancelWatches();
}

void Transaction::reset() {
	resetImpl(false);
}

void Transaction::fullReset() {
	resetImpl(true);
	span = Span(trState->spanID, "Transaction"_loc);
	backoff = CLIENT_KNOBS->DEFAULT_BACKOFF;
}

int Transaction::apiVersionAtLeast(int minVersion) const {
	return trState->cx->apiVersionAtLeast(minVersion);
}

class MutationBlock {
public:
	bool mutated;
	bool cleared;
	ValueRef setValue;

	MutationBlock() : mutated(false) {}
	MutationBlock(bool _cleared) : mutated(true), cleared(_cleared) {}
	MutationBlock(ValueRef value) : mutated(true), cleared(false), setValue(value) {}
};

bool compareBegin(KeyRangeRef lhs, KeyRangeRef rhs) {
	return lhs.begin < rhs.begin;
}

// If there is any intersection between the two given sets of ranges, returns a range that
//   falls within the intersection
Optional<KeyRangeRef> intersects(VectorRef<KeyRangeRef> lhs, VectorRef<KeyRangeRef> rhs) {
	if (lhs.size() && rhs.size()) {
		std::sort(lhs.begin(), lhs.end(), compareBegin);
		std::sort(rhs.begin(), rhs.end(), compareBegin);

		int l = 0, r = 0;
		while (l < lhs.size() && r < rhs.size()) {
			if (lhs[l].end <= rhs[r].begin)
				l++;
			else if (rhs[r].end <= lhs[l].begin)
				r++;
			else
				return lhs[l] & rhs[r];
		}
	}

	return Optional<KeyRangeRef>();
}

ACTOR void checkWrites(Reference<TransactionState> trState,
                       Future<Void> committed,
                       Promise<Void> outCommitted,
                       CommitTransactionRequest req) {
	state Version version;
	try {
		wait(committed);
		// If the commit is successful, by definition the transaction still exists for now.  Grab the version, and don't
		// use it again.
		version = trState->committedVersion;
		outCommitted.send(Void());
	} catch (Error& e) {
		outCommitted.sendError(e);
		return;
	}

	wait(delay(deterministicRandom()->random01())); // delay between 0 and 1 seconds

	state KeyRangeMap<MutationBlock> expectedValues;

	auto& mutations = req.transaction.mutations;
	state int mCount = mutations.size(); // debugging info for traceEvent

	for (int idx = 0; idx < mutations.size(); idx++) {
		if (mutations[idx].type == MutationRef::SetValue)
			expectedValues.insert(singleKeyRange(mutations[idx].param1), MutationBlock(mutations[idx].param2));
		else if (mutations[idx].type == MutationRef::ClearRange)
			expectedValues.insert(KeyRangeRef(mutations[idx].param1, mutations[idx].param2), MutationBlock(true));
	}

	try {
		state Transaction tr(trState->cx);
		tr.setVersion(version);
		state int checkedRanges = 0;
		state KeyRangeMap<MutationBlock>::Ranges ranges = expectedValues.ranges();
		state KeyRangeMap<MutationBlock>::iterator it = ranges.begin();
		for (; it != ranges.end(); ++it) {
			state MutationBlock m = it->value();
			if (m.mutated) {
				checkedRanges++;
				if (m.cleared) {
					RangeResult shouldBeEmpty = wait(tr.getRange(it->range(), 1));
					if (shouldBeEmpty.size()) {
						TraceEvent(SevError, "CheckWritesFailed")
						    .detail("Class", "Clear")
						    .detail("KeyBegin", it->range().begin)
						    .detail("KeyEnd", it->range().end);
						return;
					}
				} else {
					Optional<Value> val = wait(tr.get(it->range().begin));
					if (!val.present() || val.get() != m.setValue) {
						TraceEvent evt(SevError, "CheckWritesFailed");
						evt.detail("Class", "Set").detail("Key", it->range().begin).detail("Expected", m.setValue);
						if (!val.present())
							evt.detail("Actual", "_Value Missing_");
						else
							evt.detail("Actual", val.get());
						return;
					}
				}
			}
		}
		TraceEvent("CheckWritesSuccess")
		    .detail("Version", version)
		    .detail("MutationCount", mCount)
		    .detail("CheckedRanges", checkedRanges);
	} catch (Error& e) {
		bool ok = e.code() == error_code_transaction_too_old || e.code() == error_code_future_version;
		TraceEvent(ok ? SevWarn : SevError, "CheckWritesFailed").error(e);
		throw;
	}
}

ACTOR static Future<Void> commitDummyTransaction(Reference<TransactionState> trState, KeyRange range) {
	state Transaction tr(trState->cx);
	state int retries = 0;
	state Span span("NAPI:dummyTransaction"_loc, trState->spanID);
	tr.span.addParent(span.context);
	loop {
		try {
			TraceEvent("CommitDummyTransaction").detail("Key", range.begin).detail("Retries", retries);
			tr.trState->options = trState->options;
			tr.trState->taskID = trState->taskID;
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::CAUSAL_WRITE_RISKY);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.addReadConflictRange(range);
			tr.addWriteConflictRange(range);
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			TraceEvent("CommitDummyTransactionError")
			    .errorUnsuppressed(e)
			    .detail("Key", range.begin)
			    .detail("Retries", retries);
			wait(tr.onError(e));
		}
		++retries;
	}
}

void Transaction::cancelWatches(Error const& e) {
	for (int i = 0; i < watches.size(); ++i)
		if (!watches[i]->onChangeTrigger.isSet())
			watches[i]->onChangeTrigger.sendError(e);

	watches.clear();
}

void Transaction::setupWatches() {
	try {
		Future<Version> watchVersion = getCommittedVersion() > 0 ? getCommittedVersion() : getReadVersion();

		for (int i = 0; i < watches.size(); ++i)
			watches[i]->setWatch(watchValueMap(watchVersion,
			                                   trState->getTenantInfo(),
			                                   watches[i]->key,
			                                   watches[i]->value,
			                                   trState->cx,
			                                   trState->options.readTags,
			                                   trState->spanID,
			                                   trState->taskID,
			                                   trState->debugID,
			                                   trState->useProvisionalProxies));

		watches.clear();
	} catch (Error&) {
		ASSERT(false); // The above code must NOT throw because commit has already occured.
		throw internal_error();
	}
}

ACTOR Future<Optional<ClientTrCommitCostEstimation>> estimateCommitCosts(Reference<TransactionState> trState,
                                                                         CommitTransactionRef const* transaction) {
	state ClientTrCommitCostEstimation trCommitCosts;
	state KeyRangeRef keyRange;
	state int i = 0;

	for (; i < transaction->mutations.size(); ++i) {
		auto* it = &transaction->mutations[i];

		if (it->type == MutationRef::Type::SetValue || it->isAtomicOp()) {
			trCommitCosts.opsCount++;
			trCommitCosts.writeCosts += getWriteOperationCost(it->expectedSize());
		} else if (it->type == MutationRef::Type::ClearRange) {
			trCommitCosts.opsCount++;
			keyRange = KeyRangeRef(it->param1, it->param2);
			if (trState->options.expensiveClearCostEstimation) {
				StorageMetrics m = wait(trState->cx->getStorageMetrics(keyRange, CLIENT_KNOBS->TOO_MANY));
				trCommitCosts.clearIdxCosts.emplace_back(i, getWriteOperationCost(m.bytes));
				trCommitCosts.writeCosts += getWriteOperationCost(m.bytes);
				++trCommitCosts.expensiveCostEstCount;
				++trState->cx->transactionsExpensiveClearCostEstCount;
			} else {
				std::vector<KeyRangeLocationInfo> locations =
				    wait(getKeyRangeLocations(trState,
				                              keyRange,
				                              CLIENT_KNOBS->TOO_MANY,
				                              Reverse::False,
				                              &StorageServerInterface::getShardState,
				                              UseTenant::True,
				                              latestVersion));
				if (locations.empty()) {
					continue;
				}

				uint64_t bytes = 0;
				if (locations.size() == 1) {
					bytes = CLIENT_KNOBS->INCOMPLETE_SHARD_PLUS;
				} else { // small clear on the boundary will hit two shards but be much smaller than the shard size
					bytes = CLIENT_KNOBS->INCOMPLETE_SHARD_PLUS * 2 +
					        (locations.size() - 2) * (int64_t)trState->cx->smoothMidShardSize.smoothTotal();
				}

				trCommitCosts.clearIdxCosts.emplace_back(i, getWriteOperationCost(bytes));
				trCommitCosts.writeCosts += getWriteOperationCost(bytes);
			}
		}
	}

	// sample on written bytes
	if (!trState->cx->sampleOnCost(trCommitCosts.writeCosts))
		return Optional<ClientTrCommitCostEstimation>();

	// sample clear op: the expectation of #sampledOp is every COMMIT_SAMPLE_COST sample once
	// we also scale the cost of mutations whose cost is less than COMMIT_SAMPLE_COST as scaledCost =
	// min(COMMIT_SAMPLE_COST, cost) If we have 4 transactions: A - 100 1-cost mutations: E[sampled ops] = 1, E[sampled
	// cost] = 100 B - 1 100-cost mutation: E[sampled ops] = 1, E[sampled cost] = 100 C - 50 2-cost mutations: E[sampled
	// ops] = 1, E[sampled cost] = 100 D - 1 150-cost mutation and 150 1-cost mutations: E[sampled ops] = 3, E[sampled
	// cost] = 150cost * 1 + 150 * 100cost * 0.01 = 300
	ASSERT(trCommitCosts.writeCosts > 0);
	std::deque<std::pair<int, uint64_t>> newClearIdxCosts;
	for (const auto& [idx, cost] : trCommitCosts.clearIdxCosts) {
		if (trCommitCosts.writeCosts >= CLIENT_KNOBS->COMMIT_SAMPLE_COST) {
			double mul = trCommitCosts.writeCosts / std::max(1.0, (double)CLIENT_KNOBS->COMMIT_SAMPLE_COST);
			if (deterministicRandom()->random01() < cost * mul / trCommitCosts.writeCosts) {
				newClearIdxCosts.emplace_back(
				    idx, cost < CLIENT_KNOBS->COMMIT_SAMPLE_COST ? CLIENT_KNOBS->COMMIT_SAMPLE_COST : cost);
			}
		} else if (deterministicRandom()->random01() < (double)cost / trCommitCosts.writeCosts) {
			newClearIdxCosts.emplace_back(
			    idx, cost < CLIENT_KNOBS->COMMIT_SAMPLE_COST ? CLIENT_KNOBS->COMMIT_SAMPLE_COST : cost);
		}
	}

	trCommitCosts.clearIdxCosts.swap(newClearIdxCosts);
	return trCommitCosts;
}

// TODO: send the prefix as part of the commit request and ship it all the way
// through to the storage servers
void applyTenantPrefix(CommitTransactionRequest& req, Key tenantPrefix) {
	for (auto& m : req.transaction.mutations) {
		if (m.param1 != metadataVersionKey) {
			m.param1 = m.param1.withPrefix(tenantPrefix, req.arena);
			if (m.type == MutationRef::ClearRange) {
				m.param2 = m.param2.withPrefix(tenantPrefix, req.arena);
			} else if (m.type == MutationRef::SetVersionstampedKey) {
				uint8_t* key = mutateString(m.param1);
				int* offset = reinterpret_cast<int*>(&key[m.param1.size() - 4]);
				*offset += tenantPrefix.size();
			}
		}
	}

	for (auto& rc : req.transaction.read_conflict_ranges) {
		if (rc.begin != metadataVersionKey) {
			rc = rc.withPrefix(tenantPrefix, req.arena);
		}
	}

	for (auto& wc : req.transaction.write_conflict_ranges) {
		if (wc.begin != metadataVersionKey) {
			wc = wc.withPrefix(tenantPrefix, req.arena);
		}
	}
}

ACTOR static Future<Void> tryCommit(Reference<TransactionState> trState,
                                    CommitTransactionRequest req,
                                    Future<Version> readVersion) {
	state TraceInterval interval("TransactionCommit");
	state double startTime = now();
	state Span span("NAPI:tryCommit"_loc, trState->spanID);
	state Optional<UID> debugID = trState->debugID;
	if (debugID.present()) {
		TraceEvent(interval.begin()).detail("Parent", debugID.get());
	}
	try {
		if (CLIENT_BUGGIFY) {
			throw deterministicRandom()->randomChoice(std::vector<Error>{
			    not_committed(), transaction_too_old(), proxy_memory_limit_exceeded(), commit_unknown_result() });
		}

		if (req.tagSet.present() && trState->options.priority < TransactionPriority::IMMEDIATE) {
			wait(store(req.transaction.read_snapshot, readVersion) &&
			     store(req.commitCostEstimation, estimateCommitCosts(trState, &req.transaction)));
		} else {
			wait(store(req.transaction.read_snapshot, readVersion));
		}

		state Key tenantPrefix;
		if (trState->tenant().present()) {
			KeyRangeLocationInfo locationInfo = wait(getKeyLocation(trState,
			                                                        ""_sr,
			                                                        &StorageServerInterface::getValue,
			                                                        Reverse::False,
			                                                        UseTenant::True,
			                                                        req.transaction.read_snapshot));
			applyTenantPrefix(req, locationInfo.tenantEntry.prefix);
			tenantPrefix = locationInfo.tenantEntry.prefix;
		}

		req.tenantInfo = trState->getTenantInfo();

		startTime = now();
		state Optional<UID> commitID = Optional<UID>();

		if (debugID.present()) {
			commitID = nondeterministicRandom()->randomUniqueID();
			g_traceBatch.addAttach("CommitAttachID", debugID.get().first(), commitID.get().first());
			g_traceBatch.addEvent("CommitDebug", commitID.get().first(), "NativeAPI.commit.Before");
		}

		req.debugID = commitID;
		state Future<CommitID> reply;
		if (trState->options.commitOnFirstProxy) {
			if (trState->cx->clientInfo->get().firstCommitProxy.present()) {
				reply = throwErrorOr(brokenPromiseToMaybeDelivered(
				    trState->cx->clientInfo->get().firstCommitProxy.get().commit.tryGetReply(req)));
			} else {
				const std::vector<CommitProxyInterface>& proxies = trState->cx->clientInfo->get().commitProxies;
				reply = proxies.size() ? throwErrorOr(brokenPromiseToMaybeDelivered(proxies[0].commit.tryGetReply(req)))
				                       : Never();
			}
		} else {
			reply = basicLoadBalance(trState->cx->getCommitProxies(trState->useProvisionalProxies),
			                         &CommitProxyInterface::commit,
			                         req,
			                         TaskPriority::DefaultPromiseEndpoint,
			                         AtMostOnce::True);
		}
		state double grvTime = now();
		choose {
			when(wait(trState->cx->onProxiesChanged())) {
				reply.cancel();
				throw request_maybe_delivered();
			}
			when(CommitID ci = wait(reply)) {
				Version v = ci.version;
				if (v != invalidVersion) {
					if (CLIENT_BUGGIFY) {
						throw commit_unknown_result();
					}
					trState->cx->updateCachedReadVersion(grvTime, v);
					if (debugID.present())
						TraceEvent(interval.end()).detail("CommittedVersion", v);
					trState->committedVersion = v;
					if (v > trState->cx->metadataVersionCache[trState->cx->mvCacheInsertLocation].first) {
						trState->cx->mvCacheInsertLocation =
						    (trState->cx->mvCacheInsertLocation + 1) % trState->cx->metadataVersionCache.size();
						trState->cx->metadataVersionCache[trState->cx->mvCacheInsertLocation] =
						    std::make_pair(v, ci.metadataVersion);
					}

					Standalone<StringRef> ret = makeString(10);
					placeVersionstamp(mutateString(ret), v, ci.txnBatchId);
					trState->versionstampPromise.send(ret);

					trState->numErrors = 0;
					++trState->cx->transactionsCommitCompleted;
					trState->cx->transactionCommittedMutations += req.transaction.mutations.size();
					trState->cx->transactionCommittedMutationBytes += req.transaction.mutations.expectedSize();

					if (commitID.present())
						g_traceBatch.addEvent("CommitDebug", commitID.get().first(), "NativeAPI.commit.After");

					double latency = now() - startTime;
					trState->cx->commitLatencies.addSample(latency);
					trState->cx->latencies.addSample(now() - trState->startTime);
					if (trState->trLogInfo)
						trState->trLogInfo->addLog(
						    FdbClientLogEvents::EventCommit_V2(startTime,
						                                       trState->cx->clientLocality.dcId(),
						                                       latency,
						                                       req.transaction.mutations.size(),
						                                       req.transaction.mutations.expectedSize(),
						                                       ci.version,
						                                       req,
						                                       trState->tenant()));
					return Void();
				} else {
					// clear the RYW transaction which contains previous conflicting keys
					trState->conflictingKeys.reset();
					if (ci.conflictingKRIndices.present()) {
						trState->conflictingKeys =
						    std::make_shared<CoalescedKeyRangeMap<Value>>(conflictingKeysFalse, specialKeys.end);
						state Standalone<VectorRef<int>> conflictingKRIndices = ci.conflictingKRIndices.get();
						// drop duplicate indices and merge overlapped ranges
						// Note: addReadConflictRange in native transaction object does not merge overlapped ranges
						state std::unordered_set<int> mergedIds(conflictingKRIndices.begin(),
						                                        conflictingKRIndices.end());
						for (auto const& rCRIndex : mergedIds) {
							const KeyRangeRef kr = req.transaction.read_conflict_ranges[rCRIndex];
							const KeyRange krWithPrefix =
							    KeyRangeRef(kr.begin.removePrefix(tenantPrefix).withPrefix(conflictingKeysRange.begin),
							                kr.end.removePrefix(tenantPrefix).withPrefix(conflictingKeysRange.begin));
							trState->conflictingKeys->insert(krWithPrefix, conflictingKeysTrue);
						}
					}

					if (debugID.present())
						TraceEvent(interval.end()).detail("Conflict", 1);

					if (commitID.present())
						g_traceBatch.addEvent("CommitDebug", commitID.get().first(), "NativeAPI.commit.After");

					throw not_committed();
				}
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_request_maybe_delivered || e.code() == error_code_commit_unknown_result) {
			// We don't know if the commit happened, and it might even still be in flight.

			if (!trState->options.causalWriteRisky) {
				// Make sure it's not still in flight, either by ensuring the master we submitted to is dead, or the
				// version we submitted with is dead, or by committing a conflicting transaction successfully
				// if ( cx->getCommitProxies()->masterGeneration <= originalMasterGeneration )

				// To ensure the original request is not in flight, we need a key range which intersects its read
				// conflict ranges We pick a key range which also intersects its write conflict ranges, since that
				// avoids potentially creating conflicts where there otherwise would be none We make the range as small
				// as possible (a single key range) to minimize conflicts The intersection will never be empty, because
				// if it were (since !causalWriteRisky) makeSelfConflicting would have been applied automatically to req
				KeyRangeRef selfConflictingRange =
				    intersects(req.transaction.write_conflict_ranges, req.transaction.read_conflict_ranges).get();

				TEST(true); // Waiting for dummy transaction to report commit_unknown_result

				wait(commitDummyTransaction(trState, singleKeyRange(selfConflictingRange.begin)));
			}

			// The user needs to be informed that we aren't sure whether the commit happened.  Standard retry loops
			// retry it anyway (relying on transaction idempotence) but a client might do something else.
			throw commit_unknown_result();
		} else if (e.code() == error_code_unknown_tenant) {
			ASSERT(trState->tenant().present());
			trState->cx->invalidateCachedTenant(trState->tenant().get());
			throw;
		} else {
			if (e.code() != error_code_transaction_too_old && e.code() != error_code_not_committed &&
			    e.code() != error_code_database_locked && e.code() != error_code_proxy_memory_limit_exceeded &&
			    e.code() != error_code_batch_transaction_throttled && e.code() != error_code_tag_throttled &&
			    e.code() != error_code_process_behind && e.code() != error_code_future_version &&
			    e.code() != error_code_tenant_not_found) {
				TraceEvent(SevError, "TryCommitError").error(e);
			}
			if (trState->trLogInfo)
				trState->trLogInfo->addLog(FdbClientLogEvents::EventCommitError(
				    startTime, trState->cx->clientLocality.dcId(), static_cast<int>(e.code()), req, trState->tenant()));
			throw;
		}
	}
}

Future<Void> Transaction::commitMutations() {
	try {
		// if this is a read-only transaction return immediately
		if (!tr.transaction.write_conflict_ranges.size() && !tr.transaction.mutations.size()) {
			trState->numErrors = 0;

			trState->committedVersion = invalidVersion;
			trState->versionstampPromise.sendError(no_commit_version());
			return Void();
		}

		++trState->cx->transactionsCommitStarted;

		if (trState->options.readOnly)
			return transaction_read_only();

		trState->cx->mutationsPerCommit.addSample(tr.transaction.mutations.size());
		trState->cx->bytesPerCommit.addSample(tr.transaction.mutations.expectedSize());
		if (trState->options.tags.size())
			tr.tagSet = trState->options.tags;

		size_t transactionSize = getSize();
		if (transactionSize > (uint64_t)FLOW_KNOBS->PACKET_WARNING) {
			TraceEvent(!g_network->isSimulated() ? SevWarnAlways : SevWarn, "LargeTransaction")
			    .suppressFor(1.0)
			    .detail("Size", transactionSize)
			    .detail("NumMutations", tr.transaction.mutations.size())
			    .detail("ReadConflictSize", tr.transaction.read_conflict_ranges.expectedSize())
			    .detail("WriteConflictSize", tr.transaction.write_conflict_ranges.expectedSize())
			    .detail("DebugIdentifier", trState->trLogInfo ? trState->trLogInfo->identifier : "");
		}

		if (!apiVersionAtLeast(300)) {
			transactionSize =
			    tr.transaction.mutations.expectedSize(); // Old API versions didn't account for conflict ranges when
			                                             // determining whether to throw transaction_too_large
		}

		if (transactionSize > trState->options.sizeLimit) {
			return transaction_too_large();
		}

		if (!readVersion.isValid())
			getReadVersion(
			    GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY); // sets up readVersion field.  We had no reads, so no
			                                                    // need for (expensive) full causal consistency.

		bool isCheckingWrites = trState->options.checkWritesEnabled && deterministicRandom()->random01() < 0.01;
		for (int i = 0; i < extraConflictRanges.size(); i++)
			if (extraConflictRanges[i].isReady() &&
			    extraConflictRanges[i].get().first < extraConflictRanges[i].get().second)
				tr.transaction.read_conflict_ranges.emplace_back(
				    tr.arena, extraConflictRanges[i].get().first, extraConflictRanges[i].get().second);

		if (!trState->options.causalWriteRisky &&
		    !intersects(tr.transaction.write_conflict_ranges, tr.transaction.read_conflict_ranges).present())
			makeSelfConflicting();

		if (isCheckingWrites) {
			// add all writes into the read conflict range...
			tr.transaction.read_conflict_ranges.append(
			    tr.arena, tr.transaction.write_conflict_ranges.begin(), tr.transaction.write_conflict_ranges.size());
		}

		if (trState->options.debugDump) {
			UID u = nondeterministicRandom()->randomUniqueID();
			TraceEvent("TransactionDump", u).log();
			for (auto i = tr.transaction.mutations.begin(); i != tr.transaction.mutations.end(); ++i)
				TraceEvent("TransactionMutation", u)
				    .detail("T", i->type)
				    .detail("P1", i->param1)
				    .detail("P2", i->param2);
		}

		if (trState->options.lockAware) {
			tr.flags = tr.flags | CommitTransactionRequest::FLAG_IS_LOCK_AWARE;
		}
		if (trState->options.firstInBatch) {
			tr.flags = tr.flags | CommitTransactionRequest::FLAG_FIRST_IN_BATCH;
		}
		if (trState->options.reportConflictingKeys) {
			tr.transaction.report_conflicting_keys = true;
		}

		Future<Void> commitResult = tryCommit(trState, tr, readVersion);

		if (isCheckingWrites) {
			Promise<Void> committed;
			checkWrites(trState, commitResult, committed, tr);
			return committed.getFuture();
		}
		return commitResult;
	} catch (Error& e) {
		TraceEvent("ClientCommitError").error(e);
		return Future<Void>(e);
	} catch (...) {
		Error e(error_code_unknown_error);
		TraceEvent("ClientCommitError").error(e);
		return Future<Void>(e);
	}
}

ACTOR Future<Void> commitAndWatch(Transaction* self) {
	try {
		wait(self->commitMutations());

		self->getDatabase()->transactionTracingSample =
		    (self->getCommittedVersion() % 60000000) < (60000000 * FLOW_KNOBS->TRACING_SAMPLE_RATE);

		if (!self->watches.empty()) {
			self->setupWatches();
		}

		if (!self->apiVersionAtLeast(700)) {
			self->reset();
		}

		return Void();
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled) {
			if (!self->watches.empty()) {
				self->cancelWatches(e);
			}

			self->trState->versionstampPromise.sendError(transaction_invalid_version());

			if (!self->apiVersionAtLeast(700)) {
				self->reset();
			}
		}

		throw;
	}
}

Future<Void> Transaction::commit() {
	ASSERT(!committing.isValid());
	committing = commitAndWatch(this);
	return committing;
}

void Transaction::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	switch (option) {
	case FDBTransactionOptions::INITIALIZE_NEW_DATABASE:
		validateOptionValueNotPresent(value);
		if (readVersion.isValid())
			throw read_version_already_set();
		readVersion = Version(0);
		trState->options.causalWriteRisky = true;
		break;

	case FDBTransactionOptions::CAUSAL_READ_RISKY:
		validateOptionValueNotPresent(value);
		trState->options.getReadVersionFlags |= GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY;
		break;

	case FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE:
		validateOptionValueNotPresent(value);
		trState->options.priority = TransactionPriority::IMMEDIATE;
		break;

	case FDBTransactionOptions::PRIORITY_BATCH:
		validateOptionValueNotPresent(value);
		trState->options.priority = TransactionPriority::BATCH;
		break;

	case FDBTransactionOptions::CAUSAL_WRITE_RISKY:
		validateOptionValueNotPresent(value);
		trState->options.causalWriteRisky = true;
		break;

	case FDBTransactionOptions::COMMIT_ON_FIRST_PROXY:
		validateOptionValueNotPresent(value);
		trState->options.commitOnFirstProxy = true;
		break;

	case FDBTransactionOptions::CHECK_WRITES_ENABLE:
		validateOptionValueNotPresent(value);
		trState->options.checkWritesEnabled = true;
		break;

	case FDBTransactionOptions::DEBUG_DUMP:
		validateOptionValueNotPresent(value);
		trState->options.debugDump = true;
		break;

	case FDBTransactionOptions::TRANSACTION_LOGGING_ENABLE:
		setOption(FDBTransactionOptions::DEBUG_TRANSACTION_IDENTIFIER, value);
		setOption(FDBTransactionOptions::LOG_TRANSACTION);
		break;

	case FDBTransactionOptions::DEBUG_TRANSACTION_IDENTIFIER:
		validateOptionValuePresent(value);

		if (value.get().size() > 100 || value.get().size() == 0) {
			throw invalid_option_value();
		}

		if (trState->trLogInfo) {
			if (trState->trLogInfo->identifier.empty()) {
				trState->trLogInfo->identifier = value.get().printable();
			} else if (trState->trLogInfo->identifier != value.get().printable()) {
				TraceEvent(SevWarn, "CannotChangeDebugTransactionIdentifier")
				    .detail("PreviousIdentifier", trState->trLogInfo->identifier)
				    .detail("NewIdentifier", value.get());
				throw client_invalid_operation();
			}
		} else {
			trState->trLogInfo =
			    makeReference<TransactionLogInfo>(value.get().printable(), TransactionLogInfo::DONT_LOG);
			trState->trLogInfo->maxFieldLength = trState->options.maxTransactionLoggingFieldLength;
		}
		if (trState->debugID.present()) {
			TraceEvent(SevInfo, "TransactionBeingTraced")
			    .detail("DebugTransactionID", trState->trLogInfo->identifier)
			    .detail("ServerTraceID", trState->debugID.get());
		}
		break;

	case FDBTransactionOptions::LOG_TRANSACTION:
		validateOptionValueNotPresent(value);
		if (trState->trLogInfo && !trState->trLogInfo->identifier.empty()) {
			trState->trLogInfo->logTo(TransactionLogInfo::TRACE_LOG);
		} else {
			TraceEvent(SevWarn, "DebugTransactionIdentifierNotSet")
			    .detail("Error", "Debug Transaction Identifier option must be set before logging the transaction");
			throw client_invalid_operation();
		}
		break;

	case FDBTransactionOptions::TRANSACTION_LOGGING_MAX_FIELD_LENGTH:
		validateOptionValuePresent(value);
		{
			int maxFieldLength = extractIntOption(value, -1, std::numeric_limits<int32_t>::max());
			if (maxFieldLength == 0) {
				throw invalid_option_value();
			}
			trState->options.maxTransactionLoggingFieldLength = maxFieldLength;
		}
		if (trState->trLogInfo) {
			trState->trLogInfo->maxFieldLength = trState->options.maxTransactionLoggingFieldLength;
		}
		break;

	case FDBTransactionOptions::SERVER_REQUEST_TRACING:
		validateOptionValueNotPresent(value);
		debugTransaction(deterministicRandom()->randomUniqueID());
		if (trState->trLogInfo && !trState->trLogInfo->identifier.empty()) {
			TraceEvent(SevInfo, "TransactionBeingTraced")
			    .detail("DebugTransactionID", trState->trLogInfo->identifier)
			    .detail("ServerTraceID", trState->debugID.get());
		}
		break;

	case FDBTransactionOptions::MAX_RETRY_DELAY:
		validateOptionValuePresent(value);
		trState->options.maxBackoff = extractIntOption(value, 0, std::numeric_limits<int32_t>::max()) / 1000.0;
		break;

	case FDBTransactionOptions::SIZE_LIMIT:
		validateOptionValuePresent(value);
		trState->options.sizeLimit = extractIntOption(value, 32, CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT);
		break;

	case FDBTransactionOptions::LOCK_AWARE:
		validateOptionValueNotPresent(value);
		trState->options.lockAware = true;
		trState->options.readOnly = false;
		break;

	case FDBTransactionOptions::READ_LOCK_AWARE:
		validateOptionValueNotPresent(value);
		if (!trState->options.lockAware) {
			trState->options.lockAware = true;
			trState->options.readOnly = true;
		}
		break;

	case FDBTransactionOptions::FIRST_IN_BATCH:
		validateOptionValueNotPresent(value);
		trState->options.firstInBatch = true;
		break;

	case FDBTransactionOptions::USE_PROVISIONAL_PROXIES:
		validateOptionValueNotPresent(value);
		trState->options.getReadVersionFlags |= GetReadVersionRequest::FLAG_USE_PROVISIONAL_PROXIES;
		trState->useProvisionalProxies = UseProvisionalProxies::True;
		break;

	case FDBTransactionOptions::INCLUDE_PORT_IN_ADDRESS:
		validateOptionValueNotPresent(value);
		trState->options.includePort = true;
		break;

	case FDBTransactionOptions::TAG:
		validateOptionValuePresent(value);
		trState->options.tags.addTag(value.get());
		break;

	case FDBTransactionOptions::AUTO_THROTTLE_TAG:
		validateOptionValuePresent(value);
		trState->options.tags.addTag(value.get());
		trState->options.readTags.addTag(value.get());
		break;

	case FDBTransactionOptions::SPAN_PARENT:
		validateOptionValuePresent(value);
		if (value.get().size() != 16) {
			throw invalid_option_value();
		}
		span.addParent(BinaryReader::fromStringRef<UID>(value.get(), Unversioned()));
		break;

	case FDBTransactionOptions::REPORT_CONFLICTING_KEYS:
		validateOptionValueNotPresent(value);
		trState->options.reportConflictingKeys = true;
		break;

	case FDBTransactionOptions::EXPENSIVE_CLEAR_COST_ESTIMATION_ENABLE:
		validateOptionValueNotPresent(value);
		trState->options.expensiveClearCostEstimation = true;
		break;

	case FDBTransactionOptions::USE_GRV_CACHE:
		validateOptionValueNotPresent(value);
		if (trState->numErrors == 0) {
			trState->options.useGrvCache = true;
		}
		break;

	case FDBTransactionOptions::SKIP_GRV_CACHE:
		validateOptionValueNotPresent(value);
		trState->options.skipGrvCache = true;
		break;
	case FDBTransactionOptions::READ_SYSTEM_KEYS:
	case FDBTransactionOptions::ACCESS_SYSTEM_KEYS:
	case FDBTransactionOptions::RAW_ACCESS:
		// System key access implies raw access. Native API handles the raw access,
		// system key access is handled in RYW.
		validateOptionValueNotPresent(value);
		if (trState->hasTenant()) {
			Error e = invalid_option();
			TraceEvent(SevWarn, "TenantTransactionRawAccess").error(e).detail("Tenant", trState->tenant());
			throw e;
		}
		trState->options.rawAccess = true;
		break;

	default:
		break;
	}
}

ACTOR Future<GetReadVersionReply> getConsistentReadVersion(SpanID parentSpan,
                                                           DatabaseContext* cx,
                                                           uint32_t transactionCount,
                                                           TransactionPriority priority,
                                                           uint32_t flags,
                                                           TransactionTagMap<uint32_t> tags,
                                                           Optional<UID> debugID) {
	state Span span("NAPI:getConsistentReadVersion"_loc, parentSpan);

	++cx->transactionReadVersionBatches;
	if (debugID.present())
		g_traceBatch.addEvent("TransactionDebug", debugID.get().first(), "NativeAPI.getConsistentReadVersion.Before");
	loop {
		try {
			state GetReadVersionRequest req(span.context,
			                                transactionCount,
			                                priority,
			                                cx->ssVersionVectorCache.getMaxVersion(),
			                                flags,
			                                tags,
			                                debugID);
			state Future<Void> onProxiesChanged = cx->onProxiesChanged();

			choose {
				when(wait(onProxiesChanged)) {
					onProxiesChanged = cx->onProxiesChanged();
				}
				when(GetReadVersionReply v =
				         wait(basicLoadBalance(cx->getGrvProxies(UseProvisionalProxies(
				                                   flags & GetReadVersionRequest::FLAG_USE_PROVISIONAL_PROXIES)),
				                               &GrvProxyInterface::getConsistentReadVersion,
				                               req,
				                               cx->taskID))) {
					if (tags.size() != 0) {
						auto& priorityThrottledTags = cx->throttledTags[priority];
						for (auto& tag : tags) {
							auto itr = v.tagThrottleInfo.find(tag.first);
							if (itr == v.tagThrottleInfo.end()) {
								TEST(true); // Removing client throttle
								priorityThrottledTags.erase(tag.first);
							} else {
								TEST(true); // Setting client throttle
								auto result = priorityThrottledTags.try_emplace(tag.first, itr->second);
								if (!result.second) {
									result.first->second.update(itr->second);
								}
							}
						}
					}

					if (debugID.present())
						g_traceBatch.addEvent(
						    "TransactionDebug", debugID.get().first(), "NativeAPI.getConsistentReadVersion.After");
					ASSERT(v.version > 0);
					cx->minAcceptableReadVersion = std::min(cx->minAcceptableReadVersion, v.version);
					if (cx->versionVectorCacheActive(v.ssVersionVectorDelta)) {
						if (cx->isCurrentGrvProxy(v.proxyId)) {
							cx->ssVersionVectorCache.applyDelta(v.ssVersionVectorDelta);
						} else {
							continue; // stale GRV reply, retry
						}
					}
					return v;
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_broken_promise && e.code() != error_code_batch_transaction_throttled &&
			    e.code() != error_code_proxy_memory_limit_exceeded)
				TraceEvent(SevError, "GetConsistentReadVersionError").error(e);
			throw;
		}
	}
}

ACTOR Future<Void> readVersionBatcher(DatabaseContext* cx,
                                      FutureStream<DatabaseContext::VersionRequest> versionStream,
                                      TransactionPriority priority,
                                      uint32_t flags) {
	state std::vector<Promise<GetReadVersionReply>> requests;
	state PromiseStream<Future<Void>> addActor;
	state Future<Void> collection = actorCollection(addActor.getFuture());
	state Future<Void> timeout;
	state Optional<UID> debugID;
	state bool send_batch;
	state Reference<Histogram> batchSizeDist = Histogram::getHistogram(LiteralStringRef("GrvBatcher"),
	                                                                   LiteralStringRef("ClientGrvBatchSize"),
	                                                                   Histogram::Unit::countLinear,
	                                                                   0,
	                                                                   CLIENT_KNOBS->MAX_BATCH_SIZE * 2);
	state Reference<Histogram> batchIntervalDist =
	    Histogram::getHistogram(LiteralStringRef("GrvBatcher"),
	                            LiteralStringRef("ClientGrvBatchInterval"),
	                            Histogram::Unit::microseconds,
	                            0,
	                            CLIENT_KNOBS->GRV_BATCH_TIMEOUT * 1000000 * 2);
	state Reference<Histogram> grvReplyLatencyDist = Histogram::getHistogram(
	    LiteralStringRef("GrvBatcher"), LiteralStringRef("ClientGrvReplyLatency"), Histogram::Unit::microseconds);
	state double lastRequestTime = now();

	state TransactionTagMap<uint32_t> tags;

	// dynamic batching
	state PromiseStream<double> replyTimes;
	state PromiseStream<Error> _errorStream;
	state double batchTime = 0;
	state Span span("NAPI:readVersionBatcher"_loc);
	loop {
		send_batch = false;
		choose {
			when(DatabaseContext::VersionRequest req = waitNext(versionStream)) {
				if (req.debugID.present()) {
					if (!debugID.present()) {
						debugID = nondeterministicRandom()->randomUniqueID();
					}
					g_traceBatch.addAttach("TransactionAttachID", req.debugID.get().first(), debugID.get().first());
				}
				span.addParent(req.spanContext);
				requests.push_back(req.reply);
				for (auto tag : req.tags) {
					++tags[tag];
				}

				if (requests.size() == CLIENT_KNOBS->MAX_BATCH_SIZE) {
					send_batch = true;
					++cx->transactionGrvFullBatches;
				} else if (!timeout.isValid()) {
					timeout = delay(batchTime, TaskPriority::GetConsistentReadVersion);
				}
			}
			when(wait(timeout.isValid() ? timeout : Never())) {
				send_batch = true;
				++cx->transactionGrvTimedOutBatches;
			}
			// dynamic batching monitors reply latencies
			when(double reply_latency = waitNext(replyTimes.getFuture())) {
				double target_latency = reply_latency * 0.5;
				batchTime = std::min(0.1 * target_latency + 0.9 * batchTime, CLIENT_KNOBS->GRV_BATCH_TIMEOUT);
				grvReplyLatencyDist->sampleSeconds(reply_latency);
			}
			when(wait(collection)) {} // for errors
		}
		if (send_batch) {
			int count = requests.size();
			ASSERT(count);

			batchSizeDist->sampleRecordCounter(count);
			auto requestTime = now();
			batchIntervalDist->sampleSeconds(requestTime - lastRequestTime);
			lastRequestTime = requestTime;

			// dynamic batching
			Promise<GetReadVersionReply> GRVReply;
			requests.push_back(GRVReply);
			addActor.send(ready(timeReply(GRVReply.getFuture(), replyTimes)));

			Future<Void> batch = incrementalBroadcastWithError(
			    getConsistentReadVersion(span.context, cx, count, priority, flags, std::move(tags), std::move(debugID)),
			    std::move(requests),
			    CLIENT_KNOBS->BROADCAST_BATCH_SIZE);

			span = Span("NAPI:readVersionBatcher"_loc);
			tags.clear();
			debugID = Optional<UID>();
			requests.clear();
			addActor.send(batch);
			timeout = Future<Void>();
		}
	}
}

ACTOR Future<Version> extractReadVersion(Reference<TransactionState> trState,
                                         Location location,
                                         SpanID spanContext,
                                         Future<GetReadVersionReply> f,
                                         Promise<Optional<Value>> metadataVersion) {
	state Span span(spanContext, location, { trState->spanID });
	GetReadVersionReply rep = wait(f);
	double replyTime = now();
	double latency = replyTime - trState->startTime;
	trState->cx->lastProxyRequestTime = trState->startTime;
	trState->cx->updateCachedReadVersion(trState->startTime, rep.version);
	if (rep.rkBatchThrottled) {
		trState->cx->lastRkBatchThrottleTime = replyTime;
	}
	if (rep.rkDefaultThrottled) {
		trState->cx->lastRkDefaultThrottleTime = replyTime;
	}
	trState->cx->GRVLatencies.addSample(latency);
	if (trState->trLogInfo)
		trState->trLogInfo->addLog(FdbClientLogEvents::EventGetVersion_V3(trState->startTime,
		                                                                  trState->cx->clientLocality.dcId(),
		                                                                  latency,
		                                                                  trState->options.priority,
		                                                                  rep.version,
		                                                                  trState->tenant()));
	if (rep.locked && !trState->options.lockAware)
		throw database_locked();

	++trState->cx->transactionReadVersionsCompleted;
	switch (trState->options.priority) {
	case TransactionPriority::IMMEDIATE:
		++trState->cx->transactionImmediateReadVersionsCompleted;
		break;
	case TransactionPriority::DEFAULT:
		++trState->cx->transactionDefaultReadVersionsCompleted;
		break;
	case TransactionPriority::BATCH:
		++trState->cx->transactionBatchReadVersionsCompleted;
		break;
	default:
		ASSERT(false);
	}

	if (trState->options.tags.size() != 0) {
		auto& priorityThrottledTags = trState->cx->throttledTags[trState->options.priority];
		for (auto& tag : trState->options.tags) {
			auto itr = priorityThrottledTags.find(tag);
			if (itr != priorityThrottledTags.end()) {
				if (itr->second.expired()) {
					priorityThrottledTags.erase(itr);
				} else if (itr->second.throttleDuration() > 0) {
					TEST(true); // throttling transaction after getting read version
					++trState->cx->transactionReadVersionsThrottled;
					throw tag_throttled();
				}
			}
		}

		for (auto& tag : trState->options.tags) {
			auto itr = priorityThrottledTags.find(tag);
			if (itr != priorityThrottledTags.end()) {
				itr->second.addReleased(1);
			}
		}
	}

	if (rep.version > trState->cx->metadataVersionCache[trState->cx->mvCacheInsertLocation].first) {
		trState->cx->mvCacheInsertLocation =
		    (trState->cx->mvCacheInsertLocation + 1) % trState->cx->metadataVersionCache.size();
		trState->cx->metadataVersionCache[trState->cx->mvCacheInsertLocation] =
		    std::make_pair(rep.version, rep.metadataVersion);
	}

	metadataVersion.send(rep.metadataVersion);
	if (trState->cx->versionVectorCacheActive(rep.ssVersionVectorDelta)) {
		if (trState->cx->isCurrentGrvProxy(rep.proxyId)) {
			trState->cx->ssVersionVectorCache.applyDelta(rep.ssVersionVectorDelta);
		} else {
			trState->cx->ssVersionVectorCache.clear();
		}
	}
	return rep.version;
}

bool rkThrottlingCooledDown(DatabaseContext* cx, TransactionPriority priority) {
	if (priority == TransactionPriority::IMMEDIATE) {
		return true;
	} else if (priority == TransactionPriority::BATCH) {
		if (cx->lastRkBatchThrottleTime == 0.0) {
			return true;
		}
		return (now() - cx->lastRkBatchThrottleTime > CLIENT_KNOBS->GRV_CACHE_RK_COOLDOWN);
	} else if (priority == TransactionPriority::DEFAULT) {
		if (cx->lastRkDefaultThrottleTime == 0.0) {
			return true;
		}
		return (now() - cx->lastRkDefaultThrottleTime > CLIENT_KNOBS->GRV_CACHE_RK_COOLDOWN);
	}
	return false;
}

Future<Version> Transaction::getReadVersion(uint32_t flags) {
	if (!readVersion.isValid()) {
		if (!CLIENT_KNOBS->FORCE_GRV_CACHE_OFF && !trState->options.skipGrvCache &&
		    (deterministicRandom()->random01() <= CLIENT_KNOBS->DEBUG_USE_GRV_CACHE_CHANCE ||
		     trState->options.useGrvCache) &&
		    rkThrottlingCooledDown(getDatabase().getPtr(), trState->options.priority)) {
			// Upon our first request to use cached RVs, start the background updater
			if (!trState->cx->grvUpdateHandler.isValid()) {
				trState->cx->grvUpdateHandler = backgroundGrvUpdater(getDatabase().getPtr());
			}
			Version rv = trState->cx->getCachedReadVersion();
			double lastTime = trState->cx->getLastGrvTime();
			double requestTime = now();
			if (requestTime - lastTime <= CLIENT_KNOBS->MAX_VERSION_CACHE_LAG && rv != Version(0)) {
				ASSERT(!debug_checkVersionTime(rv, requestTime, "CheckStaleness"));
				readVersion = rv;
				return readVersion;
			} // else go through regular GRV path
		}
		++trState->cx->transactionReadVersions;
		flags |= trState->options.getReadVersionFlags;
		switch (trState->options.priority) {
		case TransactionPriority::IMMEDIATE:
			flags |= GetReadVersionRequest::PRIORITY_SYSTEM_IMMEDIATE;
			++trState->cx->transactionImmediateReadVersions;
			break;
		case TransactionPriority::DEFAULT:
			flags |= GetReadVersionRequest::PRIORITY_DEFAULT;
			++trState->cx->transactionDefaultReadVersions;
			break;
		case TransactionPriority::BATCH:
			flags |= GetReadVersionRequest::PRIORITY_BATCH;
			++trState->cx->transactionBatchReadVersions;
			break;
		default:
			ASSERT(false);
		}

		if (trState->options.tags.size() != 0) {
			double maxThrottleDelay = 0.0;
			bool canRecheck = false;

			auto& priorityThrottledTags = trState->cx->throttledTags[trState->options.priority];
			for (auto& tag : trState->options.tags) {
				auto itr = priorityThrottledTags.find(tag);
				if (itr != priorityThrottledTags.end()) {
					if (!itr->second.expired()) {
						maxThrottleDelay = std::max(maxThrottleDelay, itr->second.throttleDuration());
						canRecheck = itr->second.canRecheck();
					} else {
						priorityThrottledTags.erase(itr);
					}
				}
			}

			if (maxThrottleDelay > 0.0 && !canRecheck) { // TODO: allow delaying?
				TEST(true); // Throttling tag before GRV request
				++trState->cx->transactionReadVersionsThrottled;
				readVersion = tag_throttled();
				return readVersion;
			} else {
				TEST(maxThrottleDelay > 0.0); // Rechecking throttle
			}

			for (auto& tag : trState->options.tags) {
				auto itr = priorityThrottledTags.find(tag);
				if (itr != priorityThrottledTags.end()) {
					itr->second.updateChecked();
				}
			}
		}

		auto& batcher = trState->cx->versionBatcher[flags];
		if (!batcher.actor.isValid()) {
			batcher.actor =
			    readVersionBatcher(trState->cx.getPtr(), batcher.stream.getFuture(), trState->options.priority, flags);
		}

		Location location = "NAPI:getReadVersion"_loc;
		UID spanContext = generateSpanID(trState->cx->transactionTracingSample, trState->spanID);
		auto const req = DatabaseContext::VersionRequest(spanContext, trState->options.tags, trState->debugID);
		batcher.stream.send(req);
		trState->startTime = now();
		readVersion = extractReadVersion(trState, location, spanContext, req.reply.getFuture(), metadataVersion);
	}
	return readVersion;
}

Optional<Version> Transaction::getCachedReadVersion() const {
	if (readVersion.isValid() && readVersion.isReady() && !readVersion.isError()) {
		return readVersion.get();
	} else {
		return Optional<Version>();
	}
}

Future<Standalone<StringRef>> Transaction::getVersionstamp() {
	if (committing.isValid()) {
		return transaction_invalid_version();
	}
	return trState->versionstampPromise.getFuture();
}

// Gets the protocol version reported by a coordinator via the protocol info interface
ACTOR Future<ProtocolVersion> getCoordinatorProtocol(NetworkAddress coordinatorAddress) {
	RequestStream<ProtocolInfoRequest> requestStream(
	    Endpoint::wellKnown({ coordinatorAddress }, WLTOKEN_PROTOCOL_INFO));
	ProtocolInfoReply reply = wait(retryBrokenPromise(requestStream, ProtocolInfoRequest{}));
	return reply.version;
}

// Gets the protocol version reported by a coordinator in its connect packet
// If we are unable to get a version from the connect packet (e.g. because we lost connection with the peer), then this
// function will return with an unset result.
// If an expected version is given, this future won't return if the actual protocol version matches the expected version
ACTOR Future<Optional<ProtocolVersion>> getCoordinatorProtocolFromConnectPacket(
    NetworkAddress coordinatorAddress,
    Optional<ProtocolVersion> expectedVersion) {
	state Optional<Reference<AsyncVar<Optional<ProtocolVersion>> const>> protocolVersion =
	    FlowTransport::transport().getPeerProtocolAsyncVar(coordinatorAddress);

	if (!protocolVersion.present()) {
		TraceEvent(SevWarnAlways, "GetCoordinatorProtocolPeerMissing").detail("Address", coordinatorAddress);
		wait(delay(FLOW_KNOBS->CONNECTION_MONITOR_TIMEOUT));
		return Optional<ProtocolVersion>();
	}

	loop {
		if (protocolVersion.get()->get().present() && protocolVersion.get()->get() != expectedVersion) {
			return protocolVersion.get()->get();
		}

		Future<Void> change = protocolVersion.get()->onChange();
		if (!protocolVersion.get()->get().present()) {
			// If we still don't have any connection info after a timeout, retry sending the protocol version request
			change = timeout(change, FLOW_KNOBS->CONNECTION_MONITOR_TIMEOUT, Void());
		}

		wait(change);

		if (!protocolVersion.get()->get().present()) {
			return protocolVersion.get()->get();
		}
	}
}

// Returns the protocol version reported by the given coordinator
// If an expected version is given, the future won't return until the protocol version is different than expected
ACTOR Future<ProtocolVersion> getClusterProtocolImpl(
    Reference<AsyncVar<Optional<ClientLeaderRegInterface>> const> coordinator,
    Optional<ProtocolVersion> expectedVersion) {
	state bool needToConnect = true;
	state Future<ProtocolVersion> protocolVersion = Never();

	loop {
		if (!coordinator->get().present()) {
			wait(coordinator->onChange());
		} else {
			state NetworkAddress coordinatorAddress;
			if (coordinator->get().get().hostname.present()) {
				state Hostname h = coordinator->get().get().hostname.get();
				wait(store(coordinatorAddress, h.resolveWithRetry()));
			} else {
				coordinatorAddress = coordinator->get().get().getLeader.getEndpoint().getPrimaryAddress();
			}

			if (needToConnect) {
				// Even though we typically rely on the connect packet to get the protocol version, we need to send some
				// request in order to start a connection. This protocol version request serves that purpose.
				protocolVersion = getCoordinatorProtocol(coordinatorAddress);
				needToConnect = false;
			}
			choose {
				when(wait(coordinator->onChange())) {
					needToConnect = true;
				}

				when(ProtocolVersion pv = wait(protocolVersion)) {
					if (!expectedVersion.present() || expectedVersion.get() != pv) {
						return pv;
					}

					protocolVersion = Never();
				}

				// Older versions of FDB don't have an endpoint to return the protocol version, so we get this info from
				// the connect packet
				when(Optional<ProtocolVersion> pv =
				         wait(getCoordinatorProtocolFromConnectPacket(coordinatorAddress, expectedVersion))) {
					if (pv.present()) {
						return pv.get();
					} else {
						needToConnect = true;
					}
				}
			}
		}
	}
}

// Returns the protocol version reported by the coordinator this client is currently connected to
// If an expected version is given, the future won't return until the protocol version is different than expected
// Note: this will never return if the server is running a protocol from FDB 5.0 or older
Future<ProtocolVersion> DatabaseContext::getClusterProtocol(Optional<ProtocolVersion> expectedVersion) {
	return getClusterProtocolImpl(coordinator, expectedVersion);
}

uint32_t Transaction::getSize() {
	auto s = tr.transaction.mutations.expectedSize() + tr.transaction.read_conflict_ranges.expectedSize() +
	         tr.transaction.write_conflict_ranges.expectedSize();
	return s;
}

Future<Void> Transaction::onError(Error const& e) {
	if (g_network->isSimulated() && ++trState->numErrors % 10 == 0) {
		TraceEvent(SevWarnAlways, "TransactionTooManyRetries").detail("NumRetries", trState->numErrors);
	}
	if (e.code() == error_code_success) {
		return client_invalid_operation();
	}
	if (e.code() == error_code_not_committed || e.code() == error_code_commit_unknown_result ||
	    e.code() == error_code_database_locked || e.code() == error_code_proxy_memory_limit_exceeded ||
	    e.code() == error_code_process_behind || e.code() == error_code_batch_transaction_throttled ||
	    e.code() == error_code_tag_throttled) {
		if (e.code() == error_code_not_committed)
			++trState->cx->transactionsNotCommitted;
		else if (e.code() == error_code_commit_unknown_result)
			++trState->cx->transactionsMaybeCommitted;
		else if (e.code() == error_code_proxy_memory_limit_exceeded)
			++trState->cx->transactionsResourceConstrained;
		else if (e.code() == error_code_process_behind)
			++trState->cx->transactionsProcessBehind;
		else if (e.code() == error_code_batch_transaction_throttled || e.code() == error_code_tag_throttled) {
			++trState->cx->transactionsThrottled;
		}

		double backoff = getBackoff(e.code());
		reset();
		return delay(backoff, trState->taskID);
	}
	if (e.code() == error_code_transaction_too_old || e.code() == error_code_future_version) {
		if (e.code() == error_code_transaction_too_old)
			++trState->cx->transactionsTooOld;
		else if (e.code() == error_code_future_version)
			++trState->cx->transactionsFutureVersions;

		double maxBackoff = trState->options.maxBackoff;
		reset();
		return delay(std::min(CLIENT_KNOBS->FUTURE_VERSION_RETRY_DELAY, maxBackoff), trState->taskID);
	}
	if (e.code() == error_code_unknown_tenant) {
		double maxBackoff = trState->options.maxBackoff;
		reset();
		return delay(std::min(CLIENT_KNOBS->UNKNOWN_TENANT_RETRY_DELAY, maxBackoff), trState->taskID);
	}

	return e;
}
ACTOR Future<StorageMetrics> getStorageMetricsLargeKeyRange(Database cx, KeyRange keys);

ACTOR Future<StorageMetrics> doGetStorageMetrics(Database cx, KeyRange keys, Reference<LocationInfo> locationInfo) {
	loop {
		try {
			WaitMetricsRequest req(keys, StorageMetrics(), StorageMetrics());
			req.min.bytes = 0;
			req.max.bytes = -1;
			StorageMetrics m = wait(loadBalance(
			    locationInfo->locations(), &StorageServerInterface::waitMetrics, req, TaskPriority::DataDistribution));
			return m;
		} catch (Error& e) {
			if (e.code() != error_code_wrong_shard_server && e.code() != error_code_all_alternatives_failed) {
				TraceEvent(SevError, "WaitStorageMetricsError").error(e);
				throw;
			}
			wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskPriority::DataDistribution));
			cx->invalidateCache(Key(), keys);
			StorageMetrics m = wait(getStorageMetricsLargeKeyRange(cx, keys));
			return m;
		}
	}
}

ACTOR Future<StorageMetrics> getStorageMetricsLargeKeyRange(Database cx, KeyRange keys) {
	state Span span("NAPI:GetStorageMetricsLargeKeyRange"_loc);
	std::vector<KeyRangeLocationInfo> locations = wait(getKeyRangeLocations(cx,
	                                                                        Optional<TenantName>(),
	                                                                        keys,
	                                                                        std::numeric_limits<int>::max(),
	                                                                        Reverse::False,
	                                                                        &StorageServerInterface::waitMetrics,
	                                                                        span.context,
	                                                                        Optional<UID>(),
	                                                                        UseProvisionalProxies::False,
	                                                                        latestVersion));
	state int nLocs = locations.size();
	state std::vector<Future<StorageMetrics>> fx(nLocs);
	state StorageMetrics total;
	KeyRef partBegin, partEnd;
	for (int i = 0; i < nLocs; i++) {
		partBegin = (i == 0) ? keys.begin : locations[i].range.begin;
		partEnd = (i == nLocs - 1) ? keys.end : locations[i].range.end;
		fx[i] = doGetStorageMetrics(cx, KeyRangeRef(partBegin, partEnd), locations[i].locations);
	}
	wait(waitForAll(fx));
	for (int i = 0; i < nLocs; i++) {
		total += fx[i].get();
	}
	return total;
}

ACTOR Future<Void> trackBoundedStorageMetrics(KeyRange keys,
                                              Reference<LocationInfo> location,
                                              StorageMetrics x,
                                              StorageMetrics halfError,
                                              PromiseStream<StorageMetrics> deltaStream) {
	try {
		loop {
			WaitMetricsRequest req(keys, x - halfError, x + halfError);
			StorageMetrics nextX = wait(loadBalance(location->locations(), &StorageServerInterface::waitMetrics, req));
			deltaStream.send(nextX - x);
			x = nextX;
		}
	} catch (Error& e) {
		deltaStream.sendError(e);
		throw e;
	}
}

ACTOR Future<StorageMetrics> waitStorageMetricsMultipleLocations(std::vector<KeyRangeLocationInfo> locations,
                                                                 StorageMetrics min,
                                                                 StorageMetrics max,
                                                                 StorageMetrics permittedError) {
	state int nLocs = locations.size();
	state std::vector<Future<StorageMetrics>> fx(nLocs);
	state StorageMetrics total;
	state PromiseStream<StorageMetrics> deltas;
	state std::vector<Future<Void>> wx(fx.size());
	state StorageMetrics halfErrorPerMachine = permittedError * (0.5 / nLocs);
	state StorageMetrics maxPlus = max + halfErrorPerMachine * (nLocs - 1);
	state StorageMetrics minMinus = min - halfErrorPerMachine * (nLocs - 1);

	for (int i = 0; i < nLocs; i++) {
		WaitMetricsRequest req(locations[i].range, StorageMetrics(), StorageMetrics());
		req.min.bytes = 0;
		req.max.bytes = -1;
		fx[i] = loadBalance(locations[i].locations->locations(),
		                    &StorageServerInterface::waitMetrics,
		                    req,
		                    TaskPriority::DataDistribution);
	}
	wait(waitForAll(fx));

	// invariant: true total is between (total-permittedError/2, total+permittedError/2)
	for (int i = 0; i < nLocs; i++)
		total += fx[i].get();

	if (!total.allLessOrEqual(maxPlus))
		return total;
	if (!minMinus.allLessOrEqual(total))
		return total;

	for (int i = 0; i < nLocs; i++)
		wx[i] = trackBoundedStorageMetrics(
		    locations[i].range, locations[i].locations, fx[i].get(), halfErrorPerMachine, deltas);

	loop {
		StorageMetrics delta = waitNext(deltas.getFuture());
		total += delta;
		if (!total.allLessOrEqual(maxPlus))
			return total;
		if (!minMinus.allLessOrEqual(total))
			return total;
	}
}

ACTOR Future<StorageMetrics> extractMetrics(Future<std::pair<Optional<StorageMetrics>, int>> fMetrics) {
	std::pair<Optional<StorageMetrics>, int> x = wait(fMetrics);
	return x.first.get();
}

ACTOR Future<Standalone<VectorRef<ReadHotRangeWithMetrics>>> getReadHotRanges(Database cx, KeyRange keys) {
	state Span span("NAPI:GetReadHotRanges"_loc);
	loop {
		int64_t shardLimit = 100; // Shard limit here does not really matter since this function is currently only used
		                          // to find the read-hot sub ranges within a read-hot shard.
		std::vector<KeyRangeLocationInfo> locations =
		    wait(getKeyRangeLocations(cx,
		                              Optional<TenantName>(),
		                              keys,
		                              shardLimit,
		                              Reverse::False,
		                              &StorageServerInterface::getReadHotRanges,
		                              span.context,
		                              Optional<UID>(),
		                              UseProvisionalProxies::False,
		                              latestVersion));
		try {
			// TODO: how to handle this?
			// This function is called whenever a shard becomes read-hot. But somehow the shard was split across more
			// than one storage server after becoming read-hot and before this function is called, i.e. a race
			// condition. Should we abort and wait for the newly split shards to be hot again?
			state int nLocs = locations.size();
			// if (nLocs > 1) {
			//	TraceEvent("RHDDebug")
			//	    .detail("NumSSIs", nLocs)
			//	    .detail("KeysBegin", keys.begin.printable().c_str())
			//	    .detail("KeysEnd", keys.end.printable().c_str());
			// }
			state std::vector<Future<ReadHotSubRangeReply>> fReplies(nLocs);
			KeyRef partBegin, partEnd;
			for (int i = 0; i < nLocs; i++) {
				partBegin = (i == 0) ? keys.begin : locations[i].range.begin;
				partEnd = (i == nLocs - 1) ? keys.end : locations[i].range.end;
				ReadHotSubRangeRequest req(KeyRangeRef(partBegin, partEnd));
				fReplies[i] = loadBalance(locations[i].locations->locations(),
				                          &StorageServerInterface::getReadHotRanges,
				                          req,
				                          TaskPriority::DataDistribution);
			}

			wait(waitForAll(fReplies));

			if (nLocs == 1) {
				TEST(true); // Single-shard read hot range request
				return fReplies[0].get().readHotRanges;
			} else {
				TEST(true); // Multi-shard read hot range request
				Standalone<VectorRef<ReadHotRangeWithMetrics>> results;
				for (int i = 0; i < nLocs; i++) {
					results.append(results.arena(),
					               fReplies[i].get().readHotRanges.begin(),
					               fReplies[i].get().readHotRanges.size());
					results.arena().dependsOn(fReplies[i].get().readHotRanges.arena());
				}

				return results;
			}
		} catch (Error& e) {
			if (e.code() != error_code_wrong_shard_server && e.code() != error_code_all_alternatives_failed) {
				TraceEvent(SevError, "GetReadHotSubRangesError").error(e);
				throw;
			}
			cx->invalidateCache(Key(), keys);
			wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskPriority::DataDistribution));
		}
	}
}

ACTOR Future<std::pair<Optional<StorageMetrics>, int>> waitStorageMetrics(Database cx,
                                                                          KeyRange keys,
                                                                          StorageMetrics min,
                                                                          StorageMetrics max,
                                                                          StorageMetrics permittedError,
                                                                          int shardLimit,
                                                                          int expectedShardCount) {
	state Span span("NAPI:WaitStorageMetrics"_loc, generateSpanID(cx->transactionTracingSample));
	loop {
		std::vector<KeyRangeLocationInfo> locations = wait(getKeyRangeLocations(cx,
		                                                                        Optional<TenantName>(),
		                                                                        keys,
		                                                                        shardLimit,
		                                                                        Reverse::False,
		                                                                        &StorageServerInterface::waitMetrics,
		                                                                        span.context,
		                                                                        Optional<UID>(),
		                                                                        UseProvisionalProxies::False,
		                                                                        latestVersion));
		if (expectedShardCount >= 0 && locations.size() != expectedShardCount) {
			return std::make_pair(Optional<StorageMetrics>(), locations.size());
		}

		// SOMEDAY: Right now, if there are too many shards we delay and check again later. There may be a better
		// solution to this.
		if (locations.size() < shardLimit) {
			try {
				Future<StorageMetrics> fx;
				if (locations.size() > 1) {
					fx = waitStorageMetricsMultipleLocations(locations, min, max, permittedError);
				} else {
					WaitMetricsRequest req(keys, min, max);
					fx = loadBalance(locations[0].locations->locations(),
					                 &StorageServerInterface::waitMetrics,
					                 req,
					                 TaskPriority::DataDistribution);
				}
				StorageMetrics x = wait(fx);
				return std::make_pair(x, -1);
			} catch (Error& e) {
				if (e.code() != error_code_wrong_shard_server && e.code() != error_code_all_alternatives_failed) {
					TraceEvent(SevError, "WaitStorageMetricsError").error(e);
					throw;
				}
				cx->invalidateCache(Key(), keys);
				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskPriority::DataDistribution));
			}
		} else {
			TraceEvent(SevWarn, "WaitStorageMetricsPenalty")
			    .detail("Keys", keys)
			    .detail("Limit", CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT)
			    .detail("JitteredSecondsOfPenitence", CLIENT_KNOBS->STORAGE_METRICS_TOO_MANY_SHARDS_DELAY);
			wait(delayJittered(CLIENT_KNOBS->STORAGE_METRICS_TOO_MANY_SHARDS_DELAY, TaskPriority::DataDistribution));
			// make sure that the next getKeyRangeLocations() call will actually re-fetch the range
			cx->invalidateCache(Key(), keys);
		}
	}
}

Future<std::pair<Optional<StorageMetrics>, int>> DatabaseContext::waitStorageMetrics(
    KeyRange const& keys,
    StorageMetrics const& min,
    StorageMetrics const& max,
    StorageMetrics const& permittedError,
    int shardLimit,
    int expectedShardCount) {
	return ::waitStorageMetrics(Database(Reference<DatabaseContext>::addRef(this)),
	                            keys,
	                            min,
	                            max,
	                            permittedError,
	                            shardLimit,
	                            expectedShardCount);
}

Future<StorageMetrics> DatabaseContext::getStorageMetrics(KeyRange const& keys, int shardLimit) {
	if (shardLimit > 0) {
		StorageMetrics m;
		m.bytes = -1;
		return extractMetrics(::waitStorageMetrics(Database(Reference<DatabaseContext>::addRef(this)),
		                                           keys,
		                                           StorageMetrics(),
		                                           m,
		                                           StorageMetrics(),
		                                           shardLimit,
		                                           -1));
	} else {
		return ::getStorageMetricsLargeKeyRange(Database(Reference<DatabaseContext>::addRef(this)), keys);
	}
}

ACTOR Future<Standalone<VectorRef<DDMetricsRef>>> waitDataDistributionMetricsList(Database cx,
                                                                                  KeyRange keys,
                                                                                  int shardLimit) {
	loop {
		choose {
			when(wait(cx->onProxiesChanged())) {}
			when(ErrorOr<GetDDMetricsReply> rep =
			         wait(errorOr(basicLoadBalance(cx->getCommitProxies(UseProvisionalProxies::False),
			                                       &CommitProxyInterface::getDDMetrics,
			                                       GetDDMetricsRequest(keys, shardLimit))))) {
				if (rep.isError()) {
					throw rep.getError();
				}
				return rep.get().storageMetricsList;
			}
		}
	}
}

Future<Standalone<VectorRef<ReadHotRangeWithMetrics>>> DatabaseContext::getReadHotRanges(KeyRange const& keys) {
	return ::getReadHotRanges(Database(Reference<DatabaseContext>::addRef(this)), keys);
}

ACTOR Future<Standalone<VectorRef<KeyRef>>> getRangeSplitPoints(Reference<TransactionState> trState,
                                                                KeyRange keys,
                                                                int64_t chunkSize,
                                                                Version version) {
	state Span span("NAPI:GetRangeSplitPoints"_loc, trState->spanID);

	loop {
		state std::vector<KeyRangeLocationInfo> locations =
		    wait(getKeyRangeLocations(trState,
		                              keys,
		                              CLIENT_KNOBS->TOO_MANY,
		                              Reverse::False,
		                              &StorageServerInterface::getRangeSplitPoints,
		                              UseTenant::True,
		                              version));
		try {
			state int nLocs = locations.size();
			state std::vector<Future<SplitRangeReply>> fReplies(nLocs);
			KeyRef partBegin, partEnd;
			for (int i = 0; i < nLocs; i++) {
				partBegin = (i == 0) ? keys.begin : locations[i].range.begin;
				partEnd = (i == nLocs - 1) ? keys.end : locations[i].range.end;
				SplitRangeRequest req(trState->getTenantInfo(), KeyRangeRef(partBegin, partEnd), chunkSize);
				fReplies[i] = loadBalance(locations[i].locations->locations(),
				                          &StorageServerInterface::getRangeSplitPoints,
				                          req,
				                          TaskPriority::DataDistribution);
			}

			wait(waitForAll(fReplies));
			Standalone<VectorRef<KeyRef>> results;

			results.push_back_deep(results.arena(), keys.begin);
			for (int i = 0; i < nLocs; i++) {
				if (i > 0) {
					results.push_back_deep(results.arena(),
					                       locations[i].range.begin); // Need this shard boundary
				}
				if (fReplies[i].get().splitPoints.size() > 0) {
					results.append(
					    results.arena(), fReplies[i].get().splitPoints.begin(), fReplies[i].get().splitPoints.size());
					results.arena().dependsOn(fReplies[i].get().splitPoints.arena());
				}
			}
			if (results.back() != keys.end) {
				results.push_back_deep(results.arena(), keys.end);
			}

			return results;
		} catch (Error& e) {
			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed) {
				trState->cx->invalidateCache(locations[0].tenantEntry.prefix, keys);
				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskPriority::DataDistribution));
			} else if (e.code() == error_code_unknown_tenant) {
				ASSERT(trState->tenant().present());
				trState->cx->invalidateCachedTenant(trState->tenant().get());
				wait(delay(CLIENT_KNOBS->UNKNOWN_TENANT_RETRY_DELAY, trState->taskID));
			} else {
				TraceEvent(SevError, "GetRangeSplitPoints").error(e);
				throw;
			}
		}
	}
}

Future<Standalone<VectorRef<KeyRef>>> Transaction::getRangeSplitPoints(KeyRange const& keys, int64_t chunkSize) {
	return ::getRangeSplitPoints(
	    trState, keys, chunkSize, readVersion.isValid() && readVersion.isReady() ? readVersion.get() : latestVersion);
}

#define BG_REQUEST_DEBUG false

// the blob granule requests are a bit funky because they piggyback off the existing transaction to read from the system
// keyspace
ACTOR Future<Standalone<VectorRef<KeyRangeRef>>> getBlobGranuleRangesActor(Transaction* self, KeyRange keyRange) {
	// FIXME: use streaming range read
	state KeyRange currentRange = keyRange;
	state Standalone<VectorRef<KeyRangeRef>> results;
	if (BG_REQUEST_DEBUG) {
		fmt::print("Getting Blob Granules for [{0} - {1})\n", keyRange.begin.printable(), keyRange.end.printable());
	}
	self->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	loop {
		state RangeResult blobGranuleMapping = wait(
		    krmGetRanges(self, blobGranuleMappingKeys.begin, currentRange, 1000, GetRangeLimits::BYTE_LIMIT_UNLIMITED));

		for (int i = 0; i < blobGranuleMapping.size() - 1; i++) {
			if (blobGranuleMapping[i].value.size()) {
				results.push_back(results.arena(),
				                  KeyRangeRef(blobGranuleMapping[i].key, blobGranuleMapping[i + 1].key));
			}
		}
		results.arena().dependsOn(blobGranuleMapping.arena());
		if (blobGranuleMapping.more) {
			currentRange = KeyRangeRef(blobGranuleMapping.back().key, currentRange.end);
		} else {
			return results;
		}
	}
}

Future<Standalone<VectorRef<KeyRangeRef>>> Transaction::getBlobGranuleRanges(const KeyRange& range) {
	return ::getBlobGranuleRangesActor(this, range);
}

// hack (for now) to get blob worker interface into load balance
struct BWLocationInfo : MultiInterface<ReferencedInterface<BlobWorkerInterface>> {
	using Locations = MultiInterface<ReferencedInterface<BlobWorkerInterface>>;
	explicit BWLocationInfo(const std::vector<Reference<ReferencedInterface<BlobWorkerInterface>>>& v) : Locations(v) {}
};

ACTOR Future<Standalone<VectorRef<BlobGranuleChunkRef>>> readBlobGranulesActor(
    Transaction* self,
    KeyRange range,
    Version begin,
    Optional<Version> read,
    Version* readVersionOut) { // read not present is "use transaction version"

	state RangeResult blobGranuleMapping;
	state Key granuleStartKey;
	state Key granuleEndKey;
	state KeyRange keyRange = range;
	state UID workerId;
	state int i;
	state Version rv;

	state Standalone<VectorRef<BlobGranuleChunkRef>> results;
	state double startTime = now();

	if (read.present()) {
		rv = read.get();
	} else {
		Version _end = wait(self->getReadVersion());
		rv = _end;
	}
	self->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	// Right now just read whole blob range assignments from DB
	// FIXME: eventually we probably want to cache this and invalidate similarly to storage servers.
	// Cache misses could still read from the DB, or we could add it to the Transaction State Store and
	// have proxies serve it from memory.
	RangeResult _bgMapping =
	    wait(krmGetRanges(self, blobGranuleMappingKeys.begin, keyRange, 1000, GetRangeLimits::BYTE_LIMIT_UNLIMITED));
	blobGranuleMapping = _bgMapping;
	if (blobGranuleMapping.more) {
		if (BG_REQUEST_DEBUG) {
			fmt::print(
			    "BG Mapping for [{0} - %{1}) too large!\n", keyRange.begin.printable(), keyRange.end.printable());
		}
		TraceEvent(SevWarn, "BGMappingTooLarge").detail("Range", range).detail("Max", 1000);
		throw unsupported_operation();
	}
	ASSERT(!blobGranuleMapping.more && blobGranuleMapping.size() < CLIENT_KNOBS->TOO_MANY);

	if (blobGranuleMapping.size() == 0) {
		if (BG_REQUEST_DEBUG) {
			printf("no blob worker assignments yet\n");
		}
		throw blob_granule_transaction_too_old();
	}

	if (BG_REQUEST_DEBUG) {
		fmt::print("Doing blob granule request @ {}\n", rv);
		fmt::print("blob worker assignments:\n");
	}

	for (i = 0; i < blobGranuleMapping.size() - 1; i++) {
		granuleStartKey = blobGranuleMapping[i].key;
		granuleEndKey = blobGranuleMapping[i + 1].key;
		if (!blobGranuleMapping[i].value.size()) {
			if (BG_REQUEST_DEBUG) {
				fmt::print("Key range [{0} - {1}) missing worker assignment!\n",
				           granuleStartKey.printable(),
				           granuleEndKey.printable());
				// TODO probably new exception type instead
			}
			throw blob_granule_transaction_too_old();
		}

		workerId = decodeBlobGranuleMappingValue(blobGranuleMapping[i].value);
		if (workerId == UID()) {
			if (BG_REQUEST_DEBUG) {
				fmt::print("Key range [{0} - {1}) has no assigned worker yet!\n",
				           granuleStartKey.printable(),
				           granuleEndKey.printable());
			}
			throw blob_granule_transaction_too_old();
		}
		if (BG_REQUEST_DEBUG) {
			fmt::print(
			    "  [{0} - {1}): {2}\n", granuleStartKey.printable(), granuleEndKey.printable(), workerId.toString());
		}

		if (!self->trState->cx->blobWorker_interf.count(workerId)) {
			Optional<Value> workerInterface = wait(self->get(blobWorkerListKeyFor(workerId)));
			// from the time the mapping was read from the db, the associated blob worker
			// could have died and so its interface wouldn't be present as part of the blobWorkerList
			// we persist in the db. So throw wrong_shard_server to get the new mapping
			if (!workerInterface.present()) {
				// need to re-read mapping, throw transaction_too_old so client retries. TODO better error?
				// throw wrong_shard_server();
				throw transaction_too_old();
			}
			// FIXME: maybe just want to insert here if there are racing queries for the same worker or something?
			self->trState->cx->blobWorker_interf[workerId] = decodeBlobWorkerListValue(workerInterface.get());
			if (BG_REQUEST_DEBUG) {
				fmt::print("    decoded worker interface for {0}\n", workerId.toString());
			}
		}
	}

	// Make request for each granule
	for (i = 0; i < blobGranuleMapping.size() - 1; i++) {
		granuleStartKey = blobGranuleMapping[i].key;
		granuleEndKey = blobGranuleMapping[i + 1].key;
		// if this was a time travel and the request returned larger bounds, skip this chunk
		if (granuleEndKey <= keyRange.begin) {
			continue;
		}
		workerId = decodeBlobGranuleMappingValue(blobGranuleMapping[i].value);
		// prune first/last granules to requested range
		if (keyRange.begin > granuleStartKey) {
			granuleStartKey = keyRange.begin;
		}
		if (keyRange.end < granuleEndKey) {
			granuleEndKey = keyRange.end;
		}

		state BlobGranuleFileRequest req;
		req.keyRange = KeyRangeRef(StringRef(req.arena, granuleStartKey), StringRef(req.arena, granuleEndKey));
		req.beginVersion = begin;
		req.readVersion = rv;
		req.canCollapseBegin = true; // TODO make this a parameter once we support it

		std::vector<Reference<ReferencedInterface<BlobWorkerInterface>>> v;
		v.push_back(
		    makeReference<ReferencedInterface<BlobWorkerInterface>>(self->trState->cx->blobWorker_interf[workerId]));
		state Reference<MultiInterface<ReferencedInterface<BlobWorkerInterface>>> location =
		    makeReference<BWLocationInfo>(v);
		// use load balance with one option for now for retry and error handling
		try {
			choose {
				when(BlobGranuleFileReply rep = wait(loadBalance(location,
				                                                 &BlobWorkerInterface::blobGranuleFileRequest,
				                                                 req,
				                                                 TaskPriority::DefaultPromiseEndpoint,
				                                                 AtMostOnce::False,
				                                                 nullptr))) {
					if (BG_REQUEST_DEBUG) {
						fmt::print("Blob granule request for [{0} - {1}) @ {2} - {3} got reply from {4}:\n",
						           granuleStartKey.printable(),
						           granuleEndKey.printable(),
						           begin,
						           rv,
						           workerId.toString());
					}
					results.arena().dependsOn(rep.arena);
					for (auto& chunk : rep.chunks) {
						if (BG_REQUEST_DEBUG) {
							fmt::print(
							    "[{0} - {1})\n", chunk.keyRange.begin.printable(), chunk.keyRange.end.printable());

							fmt::print("  SnapshotFile: {0}\n    \n  DeltaFiles:\n",
							           chunk.snapshotFile.present() ? chunk.snapshotFile.get().toString().c_str()
							                                        : "<none>");
							for (auto& df : chunk.deltaFiles) {
								fmt::print("    {0}\n", df.toString());
							}
							fmt::print("  Deltas: ({0})", chunk.newDeltas.size());
							if (chunk.newDeltas.size() > 0) {
								fmt::print(" with version [{0} - {1}]",
								           chunk.newDeltas[0].version,
								           chunk.newDeltas[chunk.newDeltas.size() - 1].version);
							}
							fmt::print("  IncludedVersion: {0}\n\n\n", chunk.includedVersion);
						}

						results.push_back(results.arena(), chunk);
						keyRange = KeyRangeRef(std::min(chunk.keyRange.end, keyRange.end), keyRange.end);
					}
				}
				// if we detect that this blob worker fails, cancel the request, as otherwise load balance will
				// retry indefinitely with one option
				when(wait(IFailureMonitor::failureMonitor().onStateEqual(
				    location->get(0, &BlobWorkerInterface::blobGranuleFileRequest).getEndpoint(),
				    FailureStatus(true)))) {
					if (BG_REQUEST_DEBUG) {
						fmt::print("readBlobGranules got BW {0} failed\n", workerId.toString());
					}

					throw connection_failed();
				}
			}
		} catch (Error& e) {
			if (BG_REQUEST_DEBUG) {
				fmt::print("BGReq got error {}\n", e.name());
			}
			// worker is up but didn't actually have granule, or connection failed
			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_connection_failed) {
				// need to re-read mapping, throw transaction_too_old so client retries. TODO better error?
				throw transaction_too_old();
			}
			throw e;
		}
	}

	self->trState->cx->anyBlobGranuleRequests = true;
	self->trState->cx->bgGranulesPerRequest.addSample(results.size());
	self->trState->cx->bgLatencies.addSample(now() - startTime);

	if (readVersionOut != nullptr) {
		*readVersionOut = rv;
	}
	return results;
}

Future<Standalone<VectorRef<BlobGranuleChunkRef>>> Transaction::readBlobGranules(const KeyRange& range,
                                                                                 Version begin,
                                                                                 Optional<Version> readVersion,
                                                                                 Version* readVersionOut) {
	return readBlobGranulesActor(this, range, begin, readVersion, readVersionOut);
}

ACTOR Future<Void> setPerpetualStorageWiggle(Database cx, bool enable, LockAware lockAware) {
	state ReadYourWritesTransaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			if (lockAware) {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			}

			tr.set(perpetualStorageWiggleKey, enable ? "1"_sr : "0"_sr);
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return Void();
}

ACTOR Future<std::vector<std::pair<UID, StorageWiggleValue>>> readStorageWiggleValues(Database cx,
                                                                                      bool primary,
                                                                                      bool use_system_priority) {
	state const Key readKey = perpetualStorageWiggleIDPrefix.withSuffix(primary ? "primary/"_sr : "remote/"_sr);
	state KeyBackedObjectMap<UID, StorageWiggleValue, decltype(IncludeVersion())> metadataMap(readKey,
	                                                                                          IncludeVersion());
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	state std::vector<std::pair<UID, StorageWiggleValue>> res;
	// read the wiggling pairs
	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			if (use_system_priority) {
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			}
			wait(store(res, metadataMap.getRange(tr, UID(0, 0), Optional<UID>(), CLIENT_KNOBS->TOO_MANY)));
			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
	return res;
}

ACTOR Future<Void> splitStorageMetricsStream(PromiseStream<Key> resultStream,
                                             Database cx,
                                             KeyRange keys,
                                             StorageMetrics limit,
                                             StorageMetrics estimated) {
	state Span span("NAPI:SplitStorageMetricsStream"_loc);
	state Key beginKey = keys.begin;
	state Key globalLastKey = beginKey;
	resultStream.send(beginKey);
	// track used across loops
	state StorageMetrics globalUsed;
	loop {
		state std::vector<KeyRangeLocationInfo> locations =
		    wait(getKeyRangeLocations(cx,
		                              Optional<TenantName>(),
		                              KeyRangeRef(beginKey, keys.end),
		                              CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT,
		                              Reverse::False,
		                              &StorageServerInterface::splitMetrics,
		                              span.context,
		                              Optional<UID>(),
		                              UseProvisionalProxies::False,
		                              latestVersion));
		try {
			//TraceEvent("SplitStorageMetrics").detail("Locations", locations.size());

			state StorageMetrics localUsed = globalUsed;
			state Key localLastKey = globalLastKey;
			state Standalone<VectorRef<KeyRef>> results;
			state int i = 0;
			for (; i < locations.size(); i++) {
				SplitMetricsRequest req(locations[i].range,
				                        limit,
				                        localUsed,
				                        estimated,
				                        i == locations.size() - 1 && keys.end <= locations.back().range.end);
				SplitMetricsReply res = wait(loadBalance(locations[i].locations->locations(),
				                                         &StorageServerInterface::splitMetrics,
				                                         req,
				                                         TaskPriority::DataDistribution));
				if (res.splits.size() &&
				    res.splits[0] <= localLastKey) { // split points are out of order, possibly because
					// of moving data, throw error to retry
					ASSERT_WE_THINK(false); // FIXME: This seems impossible and doesn't seem to be covered by testing
					throw all_alternatives_failed();
				}

				if (res.splits.size()) {
					results.append(results.arena(), res.splits.begin(), res.splits.size());
					results.arena().dependsOn(res.splits.arena());
					localLastKey = res.splits.back();
				}
				localUsed = res.used;

				//TraceEvent("SplitStorageMetricsResult").detail("Used", used.bytes).detail("Location", i).detail("Size", res.splits.size());
			}

			globalUsed = localUsed;

			// only truncate split at end
			if (keys.end <= locations.back().range.end &&
			    globalUsed.allLessOrEqual(limit * CLIENT_KNOBS->STORAGE_METRICS_UNFAIR_SPLIT_LIMIT) &&
			    results.size() > 1) {
				results.resize(results.arena(), results.size() - 1);
				localLastKey = results.back();
			}
			globalLastKey = localLastKey;

			for (auto& splitKey : results) {
				resultStream.send(splitKey);
			}

			if (keys.end <= locations.back().range.end) {
				resultStream.send(keys.end);
				resultStream.sendError(end_of_stream());
				break;
			} else {
				beginKey = locations.back().range.end;
			}
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw e;
			}
			if (e.code() != error_code_wrong_shard_server && e.code() != error_code_all_alternatives_failed) {
				TraceEvent(SevError, "SplitStorageMetricsStreamError").error(e);
				resultStream.sendError(e);
				throw;
			}
			cx->invalidateCache(Key(), keys);
			wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskPriority::DataDistribution));
		}
	}
	return Void();
}

Future<Void> DatabaseContext::splitStorageMetricsStream(const PromiseStream<Key>& resultStream,
                                                        KeyRange const& keys,
                                                        StorageMetrics const& limit,
                                                        StorageMetrics const& estimated) {
	return ::splitStorageMetricsStream(
	    resultStream, Database(Reference<DatabaseContext>::addRef(this)), keys, limit, estimated);
}

ACTOR Future<Standalone<VectorRef<KeyRef>>> splitStorageMetrics(Database cx,
                                                                KeyRange keys,
                                                                StorageMetrics limit,
                                                                StorageMetrics estimated) {
	state Span span("NAPI:SplitStorageMetrics"_loc);
	loop {
		state std::vector<KeyRangeLocationInfo> locations =
		    wait(getKeyRangeLocations(cx,
		                              Optional<TenantName>(),
		                              keys,
		                              CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT,
		                              Reverse::False,
		                              &StorageServerInterface::splitMetrics,
		                              span.context,
		                              Optional<UID>(),
		                              UseProvisionalProxies::False,
		                              latestVersion));
		state StorageMetrics used;
		state Standalone<VectorRef<KeyRef>> results;

		// SOMEDAY: Right now, if there are too many shards we delay and check again later. There may be a better
		// solution to this.
		if (locations.size() == CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT) {
			wait(delay(CLIENT_KNOBS->STORAGE_METRICS_TOO_MANY_SHARDS_DELAY, TaskPriority::DataDistribution));
			cx->invalidateCache(Key(), keys);
		} else {
			results.push_back_deep(results.arena(), keys.begin);
			try {
				//TraceEvent("SplitStorageMetrics").detail("Locations", locations.size());

				state int i = 0;
				for (; i < locations.size(); i++) {
					SplitMetricsRequest req(locations[i].range, limit, used, estimated, i == locations.size() - 1);
					SplitMetricsReply res = wait(loadBalance(locations[i].locations->locations(),
					                                         &StorageServerInterface::splitMetrics,
					                                         req,
					                                         TaskPriority::DataDistribution));
					if (res.splits.size() &&
					    res.splits[0] <= results.back()) { // split points are out of order, possibly because of
						                                   // moving data, throw error to retry
						ASSERT_WE_THINK(
						    false); // FIXME: This seems impossible and doesn't seem to be covered by testing
						throw all_alternatives_failed();
					}
					if (res.splits.size()) {
						results.append(results.arena(), res.splits.begin(), res.splits.size());
						results.arena().dependsOn(res.splits.arena());
					}
					used = res.used;

					//TraceEvent("SplitStorageMetricsResult").detail("Used", used.bytes).detail("Location", i).detail("Size", res.splits.size());
				}

				if (used.allLessOrEqual(limit * CLIENT_KNOBS->STORAGE_METRICS_UNFAIR_SPLIT_LIMIT) &&
				    results.size() > 1) {
					results.resize(results.arena(), results.size() - 1);
				}

				if (keys.end <= locations.back().range.end) {
					results.push_back_deep(results.arena(), keys.end);
				}
				return results;
			} catch (Error& e) {
				if (e.code() != error_code_wrong_shard_server && e.code() != error_code_all_alternatives_failed) {
					TraceEvent(SevError, "SplitStorageMetricsError").error(e);
					throw;
				}
				cx->invalidateCache(Key(), keys);
				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskPriority::DataDistribution));
			}
		}
	}
}

Future<Standalone<VectorRef<KeyRef>>> DatabaseContext::splitStorageMetrics(KeyRange const& keys,
                                                                           StorageMetrics const& limit,
                                                                           StorageMetrics const& estimated) {
	return ::splitStorageMetrics(Database(Reference<DatabaseContext>::addRef(this)), keys, limit, estimated);
}

void Transaction::checkDeferredError() const {
	trState->cx->checkDeferredError();
}

Reference<TransactionLogInfo> Transaction::createTrLogInfoProbabilistically(const Database& cx) {
	if (!cx->isError()) {
		double clientSamplingProbability =
		    cx->globalConfig->get<double>(fdbClientInfoTxnSampleRate, CLIENT_KNOBS->CSI_SAMPLING_PROBABILITY);
		if (((networkOptions.logClientInfo.present() && networkOptions.logClientInfo.get()) || BUGGIFY) &&
		    deterministicRandom()->random01() < clientSamplingProbability &&
		    (!g_network->isSimulated() || !g_simulator.speedUpSimulation)) {
			return makeReference<TransactionLogInfo>(TransactionLogInfo::DATABASE);
		}
	}

	return Reference<TransactionLogInfo>();
}

void Transaction::setTransactionID(uint64_t id) {
	ASSERT(getSize() == 0);
	trState->spanID = SpanID(id, trState->spanID.second());
}

void Transaction::setToken(uint64_t token) {
	ASSERT(getSize() == 0);
	trState->spanID = SpanID(trState->spanID.first(), token);
}

void enableClientInfoLogging() {
	ASSERT(networkOptions.logClientInfo.present() == false);
	networkOptions.logClientInfo = true;
	TraceEvent(SevInfo, "ClientInfoLoggingEnabled").log();
}

ACTOR Future<Void> snapCreate(Database cx, Standalone<StringRef> snapCmd, UID snapUID) {
	TraceEvent("SnapCreateEnter").detail("SnapCmd", snapCmd).detail("UID", snapUID);
	try {
		loop {
			choose {
				when(wait(cx->onProxiesChanged())) {}
				when(wait(basicLoadBalance(cx->getCommitProxies(UseProvisionalProxies::False),
				                           &CommitProxyInterface::proxySnapReq,
				                           ProxySnapRequest(snapCmd, snapUID, snapUID),
				                           cx->taskID,
				                           AtMostOnce::True))) {
					TraceEvent("SnapCreateExit").detail("SnapCmd", snapCmd).detail("UID", snapUID);
					return Void();
				}
			}
		}
	} catch (Error& e) {
		TraceEvent("SnapCreateError").error(e).detail("SnapCmd", snapCmd.toString()).detail("UID", snapUID);
		throw;
	}
}

ACTOR template <class T>
static Future<Void> createCheckpointImpl(T tr, KeyRangeRef range, CheckpointFormat format) {
	ASSERT(!tr->getTenant().present());
	TraceEvent("CreateCheckpointTransactionBegin").detail("Range", range);

	state RangeResult keyServers = wait(krmGetRanges(tr, keyServersPrefix, range));
	ASSERT(!keyServers.more);

	state RangeResult UIDtoTagMap = wait(tr->getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
	ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

	for (int i = 0; i < keyServers.size() - 1; ++i) {
		KeyRangeRef shard(keyServers[i].key, keyServers[i + 1].key);
		std::vector<UID> src;
		std::vector<UID> dest;
		decodeKeyServersValue(UIDtoTagMap, keyServers[i].value, src, dest);

		// The checkpoint request is sent to all replicas, in case any of them is unhealthy.
		// An alternative is to choose a healthy replica.
		const UID checkpointID = deterministicRandom()->randomUniqueID();
		for (int idx = 0; idx < src.size(); ++idx) {
			CheckpointMetaData checkpoint(shard & range, format, src[idx], checkpointID);
			checkpoint.setState(CheckpointMetaData::Pending);
			tr->set(checkpointKeyFor(checkpointID), checkpointValue(checkpoint));
		}

		TraceEvent("CreateCheckpointTransactionShard")
		    .detail("Shard", shard)
		    .detail("SrcServers", describe(src))
		    .detail("ServerSelected", describe(src))
		    .detail("CheckpointKey", checkpointKeyFor(checkpointID))
		    .detail("ReadVersion", tr->getReadVersion().get());
	}

	return Void();
}

Future<Void> createCheckpoint(Reference<ReadYourWritesTransaction> tr, KeyRangeRef range, CheckpointFormat format) {
	return holdWhile(tr, createCheckpointImpl(tr, range, format));
}

Future<Void> createCheckpoint(Transaction* tr, KeyRangeRef range, CheckpointFormat format) {
	return createCheckpointImpl(tr, range, format);
}

// Gets CheckpointMetaData of the specific keyrange, version and format from one of the storage servers, if none of the
// servers have the checkpoint, a checkpoint_not_found error is returned.
ACTOR static Future<CheckpointMetaData> getCheckpointMetaDataInternal(GetCheckpointRequest req,
                                                                      Reference<LocationInfo> alternatives,
                                                                      double timeout) {
	TraceEvent("GetCheckpointMetaDataInternalBegin")
	    .detail("Range", req.range)
	    .detail("Version", req.version)
	    .detail("Format", static_cast<int>(req.format))
	    .detail("Locations", alternatives->description());

	state std::vector<Future<ErrorOr<CheckpointMetaData>>> futures;
	state int index = 0;
	for (index = 0; index < alternatives->size(); ++index) {
		// For each shard, all storage servers are checked, only one is required.
		futures.push_back(errorOr(timeoutError(alternatives->getInterface(index).checkpoint.getReply(req), timeout)));
	}

	state Optional<Error> error;
	wait(waitForAll(futures));
	TraceEvent("GetCheckpointMetaDataInternalWaitEnd").detail("Range", req.range).detail("Version", req.version);

	for (index = 0; index < futures.size(); ++index) {
		if (!futures[index].isReady()) {
			error = timed_out();
			TraceEvent("GetCheckpointMetaDataInternalSSTimeout")
			    .detail("Range", req.range)
			    .detail("Version", req.version)
			    .detail("StorageServer", alternatives->getInterface(index).uniqueID);
			continue;
		}

		if (futures[index].get().isError()) {
			const Error& e = futures[index].get().getError();
			TraceEvent("GetCheckpointMetaDataInternalError")
			    .errorUnsuppressed(e)
			    .detail("Range", req.range)
			    .detail("Version", req.version)
			    .detail("StorageServer", alternatives->getInterface(index).uniqueID);
			if (e.code() != error_code_checkpoint_not_found || !error.present()) {
				error = e;
			}
		} else {
			return futures[index].get().get();
		}
	}

	ASSERT(error.present());
	throw error.get();
}

ACTOR Future<std::vector<CheckpointMetaData>> getCheckpointMetaData(Database cx,
                                                                    KeyRange keys,
                                                                    Version version,
                                                                    CheckpointFormat format,
                                                                    double timeout) {
	state Span span("NAPI:GetCheckpoint"_loc);
	state int index = 0;
	state std::vector<Future<CheckpointMetaData>> futures;

	loop {
		TraceEvent("GetCheckpointBegin")
		    .detail("Range", keys.toString())
		    .detail("Version", version)
		    .detail("Format", static_cast<int>(format));

		try {
			state std::vector<KeyRangeLocationInfo> locations =
			    wait(getKeyRangeLocations(cx,
			                              Optional<TenantName>(),
			                              keys,
			                              CLIENT_KNOBS->TOO_MANY,
			                              Reverse::False,
			                              &StorageServerInterface::checkpoint,
			                              span.context,
			                              Optional<UID>(),
			                              UseProvisionalProxies::False,
			                              latestVersion));

			futures.clear();
			for (index = 0; index < locations.size(); ++index) {
				futures.push_back(getCheckpointMetaDataInternal(
				    GetCheckpointRequest(version, keys, format), locations[index].locations, timeout));
				TraceEvent("GetCheckpointShardBegin")
				    .detail("Range", locations[index].range)
				    .detail("Version", version)
				    .detail("StorageServers", locations[index].locations->description());
			}

			choose {
				when(wait(cx->connectionFileChanged())) {
					cx->invalidateCache(KeyRef(), keys);
				}
				when(wait(waitForAll(futures))) {
					break;
				}
				when(wait(delay(timeout))) {
					TraceEvent("GetCheckpointTimeout").detail("Range", keys).detail("Version", version);
				}
			}
		} catch (Error& e) {
			TraceEvent("GetCheckpointError").errorUnsuppressed(e).detail("Range", keys.toString());
			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed ||
			    e.code() == error_code_connection_failed || e.code() == error_code_broken_promise) {
				cx->invalidateCache(KeyRef(), keys);
				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY));
			} else {
				throw;
			}
		}
	}

	std::vector<CheckpointMetaData> res;
	for (index = 0; index < futures.size(); ++index) {
		TraceEvent("GetCheckpointShardEnd").detail("Checkpoint", futures[index].get().toString());
		res.push_back(futures[index].get());
	}
	return res;
}

ACTOR Future<bool> checkSafeExclusions(Database cx, std::vector<AddressExclusion> exclusions) {
	TraceEvent("ExclusionSafetyCheckBegin")
	    .detail("NumExclusion", exclusions.size())
	    .detail("Exclusions", describe(exclusions));
	state bool ddCheck;
	try {
		loop {
			choose {
				when(wait(cx->onProxiesChanged())) {}
				when(ExclusionSafetyCheckReply _ddCheck =
				         wait(basicLoadBalance(cx->getCommitProxies(UseProvisionalProxies::False),
				                               &CommitProxyInterface::exclusionSafetyCheckReq,
				                               ExclusionSafetyCheckRequest(exclusions),
				                               cx->taskID))) {
					ddCheck = _ddCheck.safe;
					break;
				}
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled) {
			TraceEvent("ExclusionSafetyCheckError")
			    .error(e)
			    .detail("NumExclusion", exclusions.size())
			    .detail("Exclusions", describe(exclusions));
		}
		throw;
	}
	TraceEvent("ExclusionSafetyCheckCoordinators").log();
	state ClientCoordinators coordinatorList(cx->getConnectionRecord());
	state std::vector<Future<Optional<LeaderInfo>>> leaderServers;
	leaderServers.reserve(coordinatorList.clientLeaderServers.size());
	for (int i = 0; i < coordinatorList.clientLeaderServers.size(); i++) {
		if (coordinatorList.clientLeaderServers[i].hostname.present()) {
			leaderServers.push_back(retryGetReplyFromHostname(GetLeaderRequest(coordinatorList.clusterKey, UID()),
			                                                  coordinatorList.clientLeaderServers[i].hostname.get(),
			                                                  WLTOKEN_CLIENTLEADERREG_GETLEADER,
			                                                  TaskPriority::CoordinationReply));
		} else {
			leaderServers.push_back(retryBrokenPromise(coordinatorList.clientLeaderServers[i].getLeader,
			                                           GetLeaderRequest(coordinatorList.clusterKey, UID()),
			                                           TaskPriority::CoordinationReply));
		}
	}
	// Wait for quorum so we don't dismiss live coordinators as unreachable by acting too fast
	choose {
		when(wait(smartQuorum(leaderServers, leaderServers.size() / 2 + 1, 1.0))) {}
		when(wait(delay(3.0))) {
			TraceEvent("ExclusionSafetyCheckNoCoordinatorQuorum").log();
			return false;
		}
	}
	int attemptCoordinatorExclude = 0;
	int coordinatorsUnavailable = 0;
	for (int i = 0; i < leaderServers.size(); i++) {
		NetworkAddress leaderAddress =
		    coordinatorList.clientLeaderServers[i].getLeader.getEndpoint().getPrimaryAddress();
		if (leaderServers[i].isReady()) {
			if ((std::count(
			         exclusions.begin(), exclusions.end(), AddressExclusion(leaderAddress.ip, leaderAddress.port)) ||
			     std::count(exclusions.begin(), exclusions.end(), AddressExclusion(leaderAddress.ip)))) {
				attemptCoordinatorExclude++;
			}
		} else {
			coordinatorsUnavailable++;
		}
	}
	int faultTolerance = (leaderServers.size() - 1) / 2 - coordinatorsUnavailable;
	bool coordinatorCheck = (attemptCoordinatorExclude <= faultTolerance);
	TraceEvent("ExclusionSafetyCheckFinish")
	    .detail("CoordinatorListSize", leaderServers.size())
	    .detail("NumExclusions", exclusions.size())
	    .detail("FaultTolerance", faultTolerance)
	    .detail("AttemptCoordinatorExclude", attemptCoordinatorExclude)
	    .detail("CoordinatorCheck", coordinatorCheck)
	    .detail("DataDistributorCheck", ddCheck);

	return (ddCheck && coordinatorCheck);
}

// returns true if we can connect to the given worker interface
ACTOR Future<bool> verifyInterfaceActor(Reference<FlowLock> connectLock, ClientWorkerInterface workerInterf) {
	wait(connectLock->take());
	state FlowLock::Releaser releaser(*connectLock);
	state ClientLeaderRegInterface leaderInterf(workerInterf.address());
	choose {
		when(Optional<LeaderInfo> rep =
		         wait(brokenPromiseToNever(leaderInterf.getLeader.getReply(GetLeaderRequest())))) {
			return true;
		}
		when(wait(delay(CLIENT_KNOBS->CLI_CONNECT_TIMEOUT))) {
			// NOTE : change timeout time here if necessary
			return false;
		}
	}
}

ACTOR static Future<int64_t> rebootWorkerActor(DatabaseContext* cx, ValueRef addr, bool check, int duration) {
	// ignore negative value
	if (duration < 0)
		duration = 0;
	if (!cx->getConnectionRecord())
		return 0;
	// fetch all workers' addresses and interfaces from CC
	RangeResult kvs = wait(getWorkerInterfaces(cx->getConnectionRecord()));
	ASSERT(!kvs.more);
	// map worker network address to its interface
	state std::map<Key, ClientWorkerInterface> workerInterfaces;
	for (const auto& it : kvs) {
		ClientWorkerInterface workerInterf =
		    BinaryReader::fromStringRef<ClientWorkerInterface>(it.value, IncludeVersion());
		Key primaryAddress =
		    it.key.endsWith(LiteralStringRef(":tls")) ? it.key.removeSuffix(LiteralStringRef(":tls")) : it.key;
		workerInterfaces[primaryAddress] = workerInterf;
		// Also add mapping from a worker's second address(if present) to its interface
		if (workerInterf.reboot.getEndpoint().addresses.secondaryAddress.present()) {
			Key secondAddress =
			    StringRef(workerInterf.reboot.getEndpoint().addresses.secondaryAddress.get().toString());
			secondAddress = secondAddress.endsWith(LiteralStringRef(":tls"))
			                    ? secondAddress.removeSuffix(LiteralStringRef(":tls"))
			                    : secondAddress;
			workerInterfaces[secondAddress] = workerInterf;
		}
	}
	// split and get all the requested addresses to send reboot requests
	state std::vector<std::string> addressesVec;
	boost::algorithm::split(addressesVec, addr.toString(), boost::is_any_of(","));
	// Note: reuse this knob from fdbcli, change it if necessary
	Reference<FlowLock> connectLock(new FlowLock(CLIENT_KNOBS->CLI_CONNECT_PARALLELISM));
	state std::vector<Future<bool>> verifyInterfs;
	for (const auto& requestedAddress : addressesVec) {
		// step 1: check that the requested address is in the worker list provided by CC
		if (!workerInterfaces.count(Key(requestedAddress)))
			return 0;
		// step 2: try to establish connections to the requested worker
		verifyInterfs.push_back(verifyInterfaceActor(connectLock, workerInterfaces[Key(requestedAddress)]));
	}
	// step 3: check if we can establish connections to all requested workers, return if not
	wait(waitForAll(verifyInterfs));
	for (const auto& f : verifyInterfs) {
		if (!f.get())
			return 0;
	}
	// step 4: After verifying we can connect to all requested workers, send reboot requests together
	for (const auto& address : addressesVec) {
		// Note: We want to make sure these requests are sent in parallel
		workerInterfaces[Key(address)].reboot.send(RebootRequest(false, check, duration));
	}
	return 1;
}

Future<int64_t> DatabaseContext::rebootWorker(StringRef addr, bool check, int duration) {
	return rebootWorkerActor(this, addr, check, duration);
}

Future<Void> DatabaseContext::forceRecoveryWithDataLoss(StringRef dcId) {
	return forceRecovery(getConnectionRecord(), dcId);
}

ACTOR static Future<Void> createSnapshotActor(DatabaseContext* cx, UID snapUID, StringRef snapCmd) {
	wait(mgmtSnapCreate(cx->clone(), snapCmd, snapUID));
	return Void();
}

Future<Void> DatabaseContext::createSnapshot(StringRef uid, StringRef snapshot_command) {
	std::string uid_str = uid.toString();
	if (!std::all_of(uid_str.begin(), uid_str.end(), [](unsigned char c) { return std::isxdigit(c); }) ||
	    uid_str.size() != 32) {
		// only 32-length hex string is considered as a valid UID
		throw snap_invalid_uid_string();
	}
	return createSnapshotActor(this, UID::fromString(uid_str), snapshot_command);
}

void sharedStateDelRef(DatabaseSharedState* ssPtr) {
	if (--ssPtr->refCount == 0) {
		delete ssPtr;
	}
}

Future<DatabaseSharedState*> DatabaseContext::initSharedState() {
	ASSERT(!sharedStatePtr); // Don't re-initialize shared state if a pointer already exists
	DatabaseSharedState* newState = new DatabaseSharedState();
	// Increment refcount by 1 on creation to account for the one held in MultiVersionApi map
	// Therefore, on initialization, refCount should be 2 (after also going to setSharedState)
	newState->refCount++;
	newState->delRef = &sharedStateDelRef;
	setSharedState(newState);
	return newState;
}

void DatabaseContext::setSharedState(DatabaseSharedState* p) {
	ASSERT(p->protocolVersion == currentProtocolVersion);
	sharedStatePtr = p;
	sharedStatePtr->refCount++;
}

ACTOR Future<Void> storageFeedVersionUpdater(StorageServerInterface interf, ChangeFeedStorageData* self) {
	state Promise<Void> destroyed = self->destroyed;
	loop {
		if (destroyed.isSet()) {
			return Void();
		}
		if (self->version.get() < self->desired.get()) {
			wait(delay(CLIENT_KNOBS->CHANGE_FEED_EMPTY_BATCH_TIME) || self->version.whenAtLeast(self->desired.get()));
			if (destroyed.isSet()) {
				return Void();
			}
			if (self->version.get() < self->desired.get()) {
				try {
					ChangeFeedVersionUpdateReply rep = wait(brokenPromiseToNever(
					    interf.changeFeedVersionUpdate.getReply(ChangeFeedVersionUpdateRequest(self->desired.get()))));

					if (rep.version > self->version.get()) {
						self->version.set(rep.version);
					}
				} catch (Error& e) {
					if (e.code() == error_code_server_overloaded) {
						if (FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY > CLIENT_KNOBS->CHANGE_FEED_EMPTY_BATCH_TIME) {
							wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY -
							           CLIENT_KNOBS->CHANGE_FEED_EMPTY_BATCH_TIME));
						}
					} else {
						throw e;
					}
				}
			}
		} else {
			wait(self->desired.whenAtLeast(self->version.get() + 1));
		}
	}
}

Reference<ChangeFeedStorageData> DatabaseContext::getStorageData(StorageServerInterface interf) {
	// use token from interface since that changes on SS restart
	UID token = interf.waitFailure.getEndpoint().token;
	auto it = changeFeedUpdaters.find(token);
	if (it == changeFeedUpdaters.end()) {
		Reference<ChangeFeedStorageData> newStorageUpdater = makeReference<ChangeFeedStorageData>();
		newStorageUpdater->id = interf.id();
		newStorageUpdater->interfToken = token;
		newStorageUpdater->updater = storageFeedVersionUpdater(interf, newStorageUpdater.getPtr());
		changeFeedUpdaters[token] = newStorageUpdater;
		return newStorageUpdater;
	}
	return it->second;
}

Version ChangeFeedData::getVersion() {
	return lastReturnedVersion.get();
}

// This function is essentially bubbling the information about what has been processed from the server through the
// change feed client. First it makes sure the server has returned all mutations up through the target version, the
// native api has consumed and processed, them, and then the fdb client has consumed all of the mutations.
ACTOR Future<Void> changeFeedWaitLatest(Reference<ChangeFeedData> self, Version version) {
	// wait on SS to have sent up through version
	std::vector<Future<Void>> allAtLeast;
	for (auto& it : self->storageData) {
		if (it->version.get() < version) {
			if (version > it->desired.get()) {
				it->desired.set(version);
			}
			allAtLeast.push_back(it->version.whenAtLeast(version));
		}
	}

	wait(waitForAll(allAtLeast));

	// then, wait on ss streams to have processed up through version
	std::vector<Future<Void>> onEmpty;
	for (auto& it : self->streams) {
		if (!it.isEmpty()) {
			onEmpty.push_back(it.onEmpty());
		}
	}

	if (onEmpty.size()) {
		wait(waitForAll(onEmpty));
	}

	if (self->mutations.isEmpty()) {
		wait(delay(0));
	}

	// wait for merge cursor to fully process everything it read from its individual promise streams, either until it is
	// done processing or we have up through the desired version
	while (self->lastReturnedVersion.get() < self->maxSeenVersion && self->lastReturnedVersion.get() < version) {
		Version target = std::min(self->maxSeenVersion, version);
		wait(self->lastReturnedVersion.whenAtLeast(target));
	}

	// then, wait for client to have consumed up through version
	if (self->maxSeenVersion >= version) {
		// merge cursor may have something buffered but has not yet sent it to self->mutations, just wait for
		// lastReturnedVersion
		wait(self->lastReturnedVersion.whenAtLeast(version));
	} else {
		// all mutations <= version are in self->mutations, wait for empty
		while (!self->mutations.isEmpty()) {
			wait(self->mutations.onEmpty());
			wait(delay(0));
		}
	}

	return Void();
}

ACTOR Future<Void> changeFeedWhenAtLatest(Reference<ChangeFeedData> self, Version version) {
	if (version >= self->endVersion) {
		return Never();
	}
	if (version <= self->getVersion()) {
		return Void();
	}
	state Future<Void> lastReturned = self->lastReturnedVersion.whenAtLeast(version);
	loop {
		// only allowed to use empty versions if you're caught up
		Future<Void> waitEmptyVersion = (self->notAtLatest.get() == 0) ? changeFeedWaitLatest(self, version) : Never();
		choose {
			when(wait(waitEmptyVersion)) {
				break;
			}
			when(wait(lastReturned)) {
				break;
			}
			when(wait(self->refresh.getFuture())) {}
			when(wait(self->notAtLatest.onChange())) {}
		}
	}

	if (self->lastReturnedVersion.get() < version) {
		self->lastReturnedVersion.set(version);
	}
	ASSERT(self->getVersion() >= version);
	return Void();
}

Future<Void> ChangeFeedData::whenAtLeast(Version version) {
	return changeFeedWhenAtLatest(Reference<ChangeFeedData>::addRef(this), version);
}

#define DEBUG_CF_CLIENT_TRACE false

ACTOR Future<Void> partialChangeFeedStream(StorageServerInterface interf,
                                           PromiseStream<Standalone<MutationsAndVersionRef>> results,
                                           ReplyPromiseStream<ChangeFeedStreamReply> replyStream,
                                           Version begin,
                                           Version end,
                                           Reference<ChangeFeedData> feedData,
                                           Reference<ChangeFeedStorageData> storageData,
                                           UID debugUID) {

	// calling lastReturnedVersion's callbacks could cause us to be cancelled
	state Promise<Void> refresh = feedData->refresh;
	state bool atLatestVersion = false;
	state Version nextVersion = begin;
	// We don't need to force every other partial stream to do an empty if we get an empty, but if we get actual
	// mutations back after sending an empty, we may need the other partial streams to get an empty, to advance the
	// merge cursor, so we can send the mutations we just got.
	// if lastEmpty != invalidVersion, we need to update the desired versions of the other streams BEFORE waiting
	// onReady once getting a reply
	state Version lastEmpty = invalidVersion;
	try {
		loop {
			if (nextVersion >= end) {
				results.sendError(end_of_stream());
				return Void();
			}
			choose {
				when(state ChangeFeedStreamReply rep = waitNext(replyStream.getFuture())) {
					// handle first empty mutation on stream establishment explicitly
					if (nextVersion == begin && rep.mutations.size() == 1 && rep.mutations[0].mutations.size() == 0 &&
					    rep.mutations[0].version == begin - 1) {
						continue;
					}

					if (DEBUG_CF_CLIENT_TRACE) {
						TraceEvent(SevDebug, "TraceChangeFeedClientMergeCursorReply", debugUID)
						    .detail("SSID", storageData->id)
						    .detail("AtLatest", atLatestVersion)
						    .detail("FirstVersion", rep.mutations.front().version)
						    .detail("LastVersion", rep.mutations.back().version)
						    .detail("Count", rep.mutations.size())
						    .detail("MinStreamVersion", rep.minStreamVersion)
						    .detail("PopVersion", rep.popVersion)
						    .detail("RepAtLatest", rep.atLatestVersion);
					}

					if (rep.mutations.back().version > feedData->maxSeenVersion) {
						feedData->maxSeenVersion = rep.mutations.back().version;
					}
					if (rep.popVersion > feedData->popVersion) {
						feedData->popVersion = rep.popVersion;
					}

					if (lastEmpty != invalidVersion && !results.isEmpty()) {
						for (auto& it : feedData->storageData) {
							if (refresh.canBeSet() && lastEmpty > it->desired.get()) {
								it->desired.set(lastEmpty);
							}
						}
						lastEmpty = invalidVersion;
					}

					state int resultLoc = 0;
					while (resultLoc < rep.mutations.size()) {
						wait(results.onEmpty());
						if (rep.mutations[resultLoc].version >= nextVersion) {
							results.send(rep.mutations[resultLoc]);

							if (DEBUG_CF_CLIENT_TRACE) {
								TraceEvent(SevDebug, "TraceChangeFeedClientMergeCursorSend", debugUID)
								    .detail("Version", rep.mutations[resultLoc].version)
								    .detail("Size", rep.mutations[resultLoc].mutations.size());
							}

							// check refresh.canBeSet so that, if we are killed after calling one of these callbacks, we
							// just skip to the next wait and get actor_cancelled
							// FIXME: this is somewhat expensive to do every mutation.
							for (auto& it : feedData->storageData) {
								if (refresh.canBeSet() && rep.mutations[resultLoc].version > it->desired.get()) {
									it->desired.set(rep.mutations[resultLoc].version);
								}
							}
						} else {
							ASSERT(rep.mutations[resultLoc].mutations.empty());
						}
						resultLoc++;
					}

					// if we got the empty version that went backwards, don't decrease nextVersion
					if (rep.mutations.back().version + 1 > nextVersion) {
						nextVersion = rep.mutations.back().version + 1;
					}

					if (refresh.canBeSet() && !atLatestVersion && rep.atLatestVersion) {
						atLatestVersion = true;
						feedData->notAtLatest.set(feedData->notAtLatest.get() - 1);
					}
					if (refresh.canBeSet() && rep.minStreamVersion > storageData->version.get()) {
						storageData->version.set(rep.minStreamVersion);
					}
					if (DEBUG_CF_CLIENT_TRACE) {
						TraceEvent(SevDebug, "TraceChangeFeedClientMergeCursorReplyDone", debugUID)
						    .detail("AtLatestNow", atLatestVersion);
					}
				}
				when(wait(atLatestVersion && replyStream.isEmpty() && results.isEmpty()
				              ? storageData->version.whenAtLeast(nextVersion)
				              : Future<Void>(Never()))) {
					MutationsAndVersionRef empty;
					empty.version = storageData->version.get();
					results.send(empty);
					nextVersion = storageData->version.get() + 1;
					if (DEBUG_CF_CLIENT_TRACE) {
						TraceEvent(SevDebug, "TraceChangeFeedClientMergeCursorSendEmpty", debugUID)
						    .detail("Version", empty.version);
					}
					lastEmpty = empty.version;
				}
				when(wait(atLatestVersion && replyStream.isEmpty() && !results.isEmpty() ? results.onEmpty()
				                                                                         : Future<Void>(Never()))) {}
			}
		}
	} catch (Error& e) {
		if (DEBUG_CF_CLIENT_TRACE) {
			TraceEvent(SevDebug, "TraceChangeFeedClientMergeCursorError", debugUID).errorUnsuppressed(e);
		}
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		results.sendError(e);
		return Void();
	}
}

ACTOR Future<Void> mergeChangeFeedStreamInternal(Reference<ChangeFeedData> results,
                                                 std::vector<std::pair<StorageServerInterface, KeyRange>> interfs,
                                                 std::vector<MutationAndVersionStream> streams,
                                                 Version* begin,
                                                 Version end,
                                                 UID mergeCursorUID) {
	state Promise<Void> refresh = results->refresh;
	// with empty version handling in the partial cursor, all streams will always have a next element with version >=
	// the minimum version of any stream's next element
	state std::priority_queue<MutationAndVersionStream, std::vector<MutationAndVersionStream>> mutations;

	if (DEBUG_CF_CLIENT_TRACE) {
		TraceEvent(SevDebug, "TraceChangeFeedClientMergeCursorStart", mergeCursorUID)
		    .detail("StreamCount", interfs.size())
		    .detail("Begin", *begin)
		    .detail("End", end);
	}

	// previous version of change feed may have put a mutation in the promise stream and then immediately died. Wait for
	// that mutation first, so the promise stream always starts empty
	wait(results->mutations.onEmpty());
	wait(delay(0));
	ASSERT(results->mutations.isEmpty());

	if (DEBUG_CF_CLIENT_TRACE) {
		TraceEvent(SevDebug, "TraceChangeFeedClientMergeCursorGotEmpty", mergeCursorUID);
	}

	// update lastReturned once the previous mutation has been consumed
	if (*begin - 1 > results->lastReturnedVersion.get()) {
		results->lastReturnedVersion.set(*begin - 1);
	}

	state int interfNum = 0;

	state std::vector<MutationAndVersionStream> streamsUsed;
	// initially, pull from all streams
	for (auto& stream : streams) {
		streamsUsed.push_back(stream);
	}

	state Version nextVersion;
	loop {
		// bring all of the streams up to date to ensure we have the latest element from each stream in mutations
		interfNum = 0;
		while (interfNum < streamsUsed.size()) {
			try {
				Standalone<MutationsAndVersionRef> res = waitNext(streamsUsed[interfNum].results.getFuture());
				streamsUsed[interfNum].next = res;
				mutations.push(streamsUsed[interfNum]);
			} catch (Error& e) {
				if (e.code() != error_code_end_of_stream) {
					throw e;
				}
			}
			interfNum++;
		}

		if (mutations.empty()) {
			throw end_of_stream();
		}

		streamsUsed.clear();

		// Without this delay, weird issues with the last stream getting on another stream's callstack can happen
		wait(delay(0));

		// pop first item off queue - this will be mutation with the lowest version
		Standalone<VectorRef<MutationsAndVersionRef>> nextOut;
		nextVersion = mutations.top().next.version;

		streamsUsed.push_back(mutations.top());
		nextOut.push_back_deep(nextOut.arena(), mutations.top().next);
		mutations.pop();

		// for each other stream that has mutations with the same version, add it to nextOut
		while (!mutations.empty() && mutations.top().next.version == nextVersion) {
			if (mutations.top().next.mutations.size() &&
			    mutations.top().next.mutations.front().param1 != lastEpochEndPrivateKey) {
				nextOut.back().mutations.append_deep(
				    nextOut.arena(), mutations.top().next.mutations.begin(), mutations.top().next.mutations.size());
			}
			streamsUsed.push_back(mutations.top());
			mutations.pop();
		}

		ASSERT(nextOut.size() == 1);
		ASSERT(nextVersion >= *begin);

		*begin = nextVersion + 1;

		if (DEBUG_CF_CLIENT_TRACE) {
			TraceEvent(SevDebug, "TraceChangeFeedClientMergeCursorSending", mergeCursorUID)
			    .detail("Count", streamsUsed.size())
			    .detail("Version", nextVersion);
		}

		// send mutations at nextVersion to the client
		if (nextOut.back().mutations.empty()) {
			ASSERT(results->mutations.isEmpty());
		} else {
			ASSERT(nextOut.back().version > results->lastReturnedVersion.get());

			results->mutations.send(nextOut);
			wait(results->mutations.onEmpty());
			wait(delay(0));
		}

		if (nextVersion > results->lastReturnedVersion.get()) {
			results->lastReturnedVersion.set(nextVersion);
		}
	}
}

ACTOR Future<Void> mergeChangeFeedStream(Reference<DatabaseContext> db,
                                         std::vector<std::pair<StorageServerInterface, KeyRange>> interfs,
                                         Reference<ChangeFeedData> results,
                                         Key rangeID,
                                         Version* begin,
                                         Version end,
                                         int replyBufferSize,
                                         bool canReadPopped) {
	state std::vector<Future<Void>> fetchers(interfs.size());
	state std::vector<Future<Void>> onErrors(interfs.size());
	state std::vector<MutationAndVersionStream> streams(interfs.size());

	TEST(interfs.size() > 10); // Large change feed merge cursor
	TEST(interfs.size() > 100); // Very large change feed merge cursor

	state UID mergeCursorUID = UID();
	state std::vector<UID> debugUIDs;
	results->streams.clear();
	for (auto& it : interfs) {
		ChangeFeedStreamRequest req;
		req.rangeID = rangeID;
		req.begin = *begin;
		req.end = end;
		req.range = it.second;
		req.canReadPopped = canReadPopped;
		// divide total buffer size among sub-streams, but keep individual streams large enough to be efficient
		req.replyBufferSize = replyBufferSize / interfs.size();
		if (replyBufferSize != -1 && req.replyBufferSize < CLIENT_KNOBS->CHANGE_FEED_STREAM_MIN_BYTES) {
			req.replyBufferSize = CLIENT_KNOBS->CHANGE_FEED_STREAM_MIN_BYTES;
		}
		req.debugUID = deterministicRandom()->randomUniqueID();
		debugUIDs.push_back(req.debugUID);
		mergeCursorUID =
		    UID(mergeCursorUID.first() ^ req.debugUID.first(), mergeCursorUID.second() ^ req.debugUID.second());

		results->streams.push_back(it.first.changeFeedStream.getReplyStream(req));
	}

	for (auto& it : results->storageData) {
		if (it->debugGetReferenceCount() == 2) {
			db->changeFeedUpdaters.erase(it->interfToken);
		}
	}
	results->maxSeenVersion = invalidVersion;
	results->storageData.clear();
	Promise<Void> refresh = results->refresh;
	results->refresh = Promise<Void>();
	for (int i = 0; i < interfs.size(); i++) {
		results->storageData.push_back(db->getStorageData(interfs[i].first));
	}
	results->notAtLatest.set(interfs.size());
	refresh.send(Void());

	for (int i = 0; i < interfs.size(); i++) {
		if (DEBUG_CF_CLIENT_TRACE) {
			TraceEvent(SevDebug, "TraceChangeFeedClientMergeCursorInit", debugUIDs[i])
			    .detail("CursorDebugUID", mergeCursorUID)
			    .detail("Idx", i)
			    .detail("FeedID", rangeID)
			    .detail("MergeRange", KeyRangeRef(interfs.front().second.begin, interfs.back().second.end))
			    .detail("PartialRange", interfs[i].second)
			    .detail("Begin", *begin)
			    .detail("End", end)
			    .detail("CanReadPopped", canReadPopped);
		}
		onErrors[i] = results->streams[i].onError();
		fetchers[i] = partialChangeFeedStream(interfs[i].first,
		                                      streams[i].results,
		                                      results->streams[i],
		                                      *begin,
		                                      end,
		                                      results,
		                                      results->storageData[i],
		                                      debugUIDs[i]);
	}

	wait(waitForAny(onErrors) || mergeChangeFeedStreamInternal(results, interfs, streams, begin, end, mergeCursorUID));

	return Void();
}

ACTOR Future<KeyRange> getChangeFeedRange(Reference<DatabaseContext> db, Database cx, Key rangeID, Version begin = 0) {
	state Transaction tr(cx);
	state Key rangeIDKey = rangeID.withPrefix(changeFeedPrefix);

	auto cacheLoc = db->changeFeedCache.find(rangeID);
	if (cacheLoc != db->changeFeedCache.end()) {
		return cacheLoc->second;
	}

	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			Version readVer = wait(tr.getReadVersion());
			if (readVer < begin) {
				wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				tr.reset();
			} else {
				Optional<Value> val = wait(tr.get(rangeIDKey));
				if (!val.present()) {
					throw change_feed_not_registered();
				}
				if (db->changeFeedCache.size() > CLIENT_KNOBS->CHANGE_FEED_CACHE_SIZE) {
					db->changeFeedCache.clear();
				}
				KeyRange range = std::get<0>(decodeChangeFeedValue(val.get()));
				db->changeFeedCache[rangeID] = range;
				return range;
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> singleChangeFeedStreamInternal(KeyRange range,
                                                  Reference<ChangeFeedData> results,
                                                  Key rangeID,
                                                  Version* begin,
                                                  Version end) {

	state Promise<Void> refresh = results->refresh;
	ASSERT(results->streams.size() == 1);
	ASSERT(results->storageData.size() == 1);
	state bool atLatest = false;

	// wait for any previous mutations in stream to be consumed
	wait(results->mutations.onEmpty());
	wait(delay(0));
	ASSERT(results->mutations.isEmpty());
	// update lastReturned once the previous mutation has been consumed
	if (*begin - 1 > results->lastReturnedVersion.get()) {
		results->lastReturnedVersion.set(*begin - 1);
	}

	loop {

		state ChangeFeedStreamReply feedReply = waitNext(results->streams[0].getFuture());
		*begin = feedReply.mutations.back().version + 1;

		if (feedReply.popVersion > results->popVersion) {
			results->popVersion = feedReply.popVersion;
		}

		// don't send completely empty set of mutations to promise stream
		bool anyMutations = false;
		for (auto& it : feedReply.mutations) {
			if (!it.mutations.empty()) {
				anyMutations = true;
				break;
			}
		}
		if (anyMutations) {
			// empty versions can come out of order, as we sometimes send explicit empty versions when restarting a
			// stream. Anything with mutations should be strictly greater than lastReturnedVersion
			ASSERT(feedReply.mutations.front().version > results->lastReturnedVersion.get());

			results->mutations.send(
			    Standalone<VectorRef<MutationsAndVersionRef>>(feedReply.mutations, feedReply.arena));

			// Because onEmpty returns here before the consuming process, we must do a delay(0)
			wait(results->mutations.onEmpty());
			wait(delay(0));
		}

		// check refresh.canBeSet so that, if we are killed after calling one of these callbacks, we just
		// skip to the next wait and get actor_cancelled
		if (feedReply.mutations.back().version > results->lastReturnedVersion.get()) {
			results->lastReturnedVersion.set(feedReply.mutations.back().version);
		}

		if (!refresh.canBeSet()) {
			try {
				// refresh is set if and only if this actor is cancelled
				wait(Future<Void>(Void()));
				// Catch any unexpected behavior if the above contract is broken
				ASSERT(false);
			} catch (Error& e) {
				ASSERT(e.code() == error_code_actor_cancelled);
				throw;
			}
		}

		if (!atLatest && feedReply.atLatestVersion) {
			atLatest = true;
			results->notAtLatest.set(0);
		}

		if (feedReply.minStreamVersion > results->storageData[0]->version.get()) {
			results->storageData[0]->version.set(feedReply.minStreamVersion);
		}
	}
}

ACTOR Future<Void> singleChangeFeedStream(Reference<DatabaseContext> db,
                                          StorageServerInterface interf,
                                          KeyRange range,
                                          Reference<ChangeFeedData> results,
                                          Key rangeID,
                                          Version* begin,
                                          Version end,
                                          int replyBufferSize,
                                          bool canReadPopped) {
	state Database cx(db);
	state ChangeFeedStreamRequest req;
	req.rangeID = rangeID;
	req.begin = *begin;
	req.end = end;
	req.range = range;
	req.canReadPopped = canReadPopped;
	req.replyBufferSize = replyBufferSize;
	req.debugUID = deterministicRandom()->randomUniqueID();

	if (DEBUG_CF_CLIENT_TRACE) {
		TraceEvent(SevDebug, "TraceChangeFeedClientSingleCursor", req.debugUID)
		    .detail("FeedID", rangeID)
		    .detail("Range", range)
		    .detail("Begin", *begin)
		    .detail("End", end)
		    .detail("CanReadPopped", canReadPopped);
	}

	results->streams.clear();

	for (auto& it : results->storageData) {
		if (it->debugGetReferenceCount() == 2) {
			db->changeFeedUpdaters.erase(it->interfToken);
		}
	}
	results->streams.push_back(interf.changeFeedStream.getReplyStream(req));

	results->maxSeenVersion = invalidVersion;
	results->storageData.clear();
	results->storageData.push_back(db->getStorageData(interf));
	Promise<Void> refresh = results->refresh;
	results->refresh = Promise<Void>();
	results->notAtLatest.set(1);
	refresh.send(Void());

	wait(results->streams[0].onError() || singleChangeFeedStreamInternal(range, results, rangeID, begin, end));

	return Void();
}

ACTOR Future<Void> getChangeFeedStreamActor(Reference<DatabaseContext> db,
                                            Reference<ChangeFeedData> results,
                                            Key rangeID,
                                            Version begin,
                                            Version end,
                                            KeyRange range,
                                            int replyBufferSize,
                                            bool canReadPopped) {
	state Database cx(db);
	state Span span("NAPI:GetChangeFeedStream"_loc);

	results->endVersion = end;

	state double sleepWithBackoff = CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY;
	state Version lastBeginVersion = invalidVersion;

	loop {
		state KeyRange keys;
		try {
			lastBeginVersion = begin;
			KeyRange fullRange = wait(getChangeFeedRange(db, cx, rangeID, begin));
			keys = fullRange & range;
			state std::vector<KeyRangeLocationInfo> locations =
			    wait(getKeyRangeLocations(cx,
			                              Optional<TenantName>(),
			                              keys,
			                              CLIENT_KNOBS->CHANGE_FEED_LOCATION_LIMIT,
			                              Reverse::False,
			                              &StorageServerInterface::changeFeedStream,
			                              span.context,
			                              Optional<UID>(),
			                              UseProvisionalProxies::False,
			                              latestVersion));

			if (locations.size() >= CLIENT_KNOBS->CHANGE_FEED_LOCATION_LIMIT) {
				ASSERT_WE_THINK(false);
				throw unknown_change_feed();
			}

			state std::vector<int> chosenLocations(locations.size());
			state int loc = 0;
			while (loc < locations.size()) {
				// FIXME: create a load balance function for this code so future users of reply streams do not have
				// to duplicate this code
				int count = 0;
				int useIdx = -1;
				for (int i = 0; i < locations[loc].locations->size(); i++) {
					if (!IFailureMonitor::failureMonitor()
					         .getState(locations[loc]
					                       .locations->get(i, &StorageServerInterface::changeFeedStream)
					                       .getEndpoint())
					         .failed) {
						if (deterministicRandom()->random01() <= 1.0 / ++count) {
							useIdx = i;
						}
					}
				}

				if (useIdx >= 0) {
					chosenLocations[loc] = useIdx;
					loc++;
					continue;
				}

				std::vector<Future<Void>> ok(locations[loc].locations->size());
				for (int i = 0; i < ok.size(); i++) {
					ok[i] = IFailureMonitor::failureMonitor().onStateEqual(
					    locations[loc].locations->get(i, &StorageServerInterface::changeFeedStream).getEndpoint(),
					    FailureStatus(false));
				}

				// Making this SevWarn means a lot of clutter
				if (now() - g_network->networkInfo.newestAlternativesFailure > 1 ||
				    deterministicRandom()->random01() < 0.01) {
					TraceEvent("AllAlternativesFailed").detail("Alternatives", locations[0].locations->description());
				}

				wait(allAlternativesFailedDelay(quorum(ok, 1)));
				loc = 0;
			}

			if (locations.size() > 1) {
				std::vector<std::pair<StorageServerInterface, KeyRange>> interfs;
				for (int i = 0; i < locations.size(); i++) {
					interfs.emplace_back(locations[i].locations->getInterface(chosenLocations[i]),
					                     locations[i].range & range);
				}
				TEST(true); // Change feed merge cursor
				// TODO (jslocum): validate connectionFileChanged behavior
				wait(
				    mergeChangeFeedStream(db, interfs, results, rangeID, &begin, end, replyBufferSize, canReadPopped) ||
				    cx->connectionFileChanged());
			} else {
				TEST(true); // Change feed single cursor
				StorageServerInterface interf = locations[0].locations->getInterface(chosenLocations[0]);
				wait(singleChangeFeedStream(
				         db, interf, range, results, rangeID, &begin, end, replyBufferSize, canReadPopped) ||
				     cx->connectionFileChanged());
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled || e.code() == error_code_change_feed_popped) {
				for (auto& it : results->storageData) {
					if (it->debugGetReferenceCount() == 2) {
						db->changeFeedUpdaters.erase(it->interfToken);
					}
				}
				results->streams.clear();
				results->storageData.clear();
				if (e.code() == error_code_change_feed_popped) {
					TEST(true); // getChangeFeedStreamActor got popped
					results->mutations.sendError(e);
					results->refresh.sendError(e);
				} else {
					results->refresh.sendError(change_feed_cancelled());
				}
				throw;
			}
			if (results->notAtLatest.get() == 0) {
				results->notAtLatest.set(1);
			}

			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed ||
			    e.code() == error_code_connection_failed || e.code() == error_code_unknown_change_feed ||
			    e.code() == error_code_broken_promise) {
				db->changeFeedCache.erase(rangeID);
				cx->invalidateCache(Key(), keys);
				if (begin == lastBeginVersion) {
					// We didn't read anything since the last failure before failing again.
					// Do exponential backoff, up to 1 second
					sleepWithBackoff = std::min(1.0, sleepWithBackoff * 1.5);
				} else {
					sleepWithBackoff = CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY;
				}
				wait(delay(sleepWithBackoff));
			} else {
				results->mutations.sendError(e);
				results->refresh.sendError(change_feed_cancelled());
				for (auto& it : results->storageData) {
					if (it->debugGetReferenceCount() == 2) {
						db->changeFeedUpdaters.erase(it->interfToken);
					}
				}
				results->streams.clear();
				results->storageData.clear();
				return Void();
			}
		}
	}
}

Future<Void> DatabaseContext::getChangeFeedStream(Reference<ChangeFeedData> results,
                                                  Key rangeID,
                                                  Version begin,
                                                  Version end,
                                                  KeyRange range,
                                                  int replyBufferSize,
                                                  bool canReadPopped) {
	return getChangeFeedStreamActor(
	    Reference<DatabaseContext>::addRef(this), results, rangeID, begin, end, range, replyBufferSize, canReadPopped);
}

ACTOR Future<std::vector<OverlappingChangeFeedEntry>> singleLocationOverlappingChangeFeeds(
    Database cx,
    Reference<LocationInfo> location,
    KeyRangeRef range,
    Version minVersion) {
	state OverlappingChangeFeedsRequest req;
	req.range = range;
	req.minVersion = minVersion;

	OverlappingChangeFeedsReply rep = wait(loadBalance(cx.getPtr(),
	                                                   location,
	                                                   &StorageServerInterface::overlappingChangeFeeds,
	                                                   req,
	                                                   TaskPriority::DefaultPromiseEndpoint,
	                                                   AtMostOnce::False,
	                                                   cx->enableLocalityLoadBalance ? &cx->queueModel : nullptr));
	return rep.rangeIds;
}

bool compareChangeFeedResult(const OverlappingChangeFeedEntry& i, const OverlappingChangeFeedEntry& j) {
	return i.rangeId < j.rangeId;
}

ACTOR Future<std::vector<OverlappingChangeFeedEntry>> getOverlappingChangeFeedsActor(Reference<DatabaseContext> db,
                                                                                     KeyRangeRef range,
                                                                                     Version minVersion) {
	state Database cx(db);
	state Span span("NAPI:GetOverlappingChangeFeeds"_loc);

	loop {
		try {
			state std::vector<KeyRangeLocationInfo> locations =
			    wait(getKeyRangeLocations(cx,
			                              Optional<TenantName>(),
			                              range,
			                              CLIENT_KNOBS->CHANGE_FEED_LOCATION_LIMIT,
			                              Reverse::False,
			                              &StorageServerInterface::overlappingChangeFeeds,
			                              span.context,
			                              Optional<UID>(),
			                              UseProvisionalProxies::False,
			                              latestVersion));

			if (locations.size() >= CLIENT_KNOBS->CHANGE_FEED_LOCATION_LIMIT) {
				TraceEvent(SevError, "OverlappingRangeTooLarge")
				    .detail("Range", range)
				    .detail("Limit", CLIENT_KNOBS->CHANGE_FEED_LOCATION_LIMIT);
				wait(delay(1.0));
				throw all_alternatives_failed();
			}

			state std::vector<Future<std::vector<OverlappingChangeFeedEntry>>> allOverlappingRequests;
			for (auto& it : locations) {
				allOverlappingRequests.push_back(
				    singleLocationOverlappingChangeFeeds(cx, it.locations, it.range & range, minVersion));
			}
			wait(waitForAll(allOverlappingRequests));

			std::vector<OverlappingChangeFeedEntry> result;
			for (auto& it : allOverlappingRequests) {
				result.insert(result.end(), it.get().begin(), it.get().end());
			}
			std::sort(result.begin(), result.end(), compareChangeFeedResult);
			result.resize(std::unique(result.begin(), result.end()) - result.begin());
			return result;
		} catch (Error& e) {
			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed) {
				cx->invalidateCache(Key(), range);
				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY));
			} else {
				throw e;
			}
		}
	}
}

Future<std::vector<OverlappingChangeFeedEntry>> DatabaseContext::getOverlappingChangeFeeds(KeyRangeRef range,
                                                                                           Version minVersion) {
	return getOverlappingChangeFeedsActor(Reference<DatabaseContext>::addRef(this), range, minVersion);
}

ACTOR static Future<Void> popChangeFeedBackup(Database cx, Key rangeID, Version version) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			state Key rangeIDKey = rangeID.withPrefix(changeFeedPrefix);
			Optional<Value> val = wait(tr.get(rangeIDKey));
			if (val.present()) {
				KeyRange range;
				Version popVersion;
				ChangeFeedStatus status;
				std::tie(range, popVersion, status) = decodeChangeFeedValue(val.get());
				if (version > popVersion) {
					tr.set(rangeIDKey, changeFeedValue(range, version, status));
				}
			} else {
				throw change_feed_not_registered();
			}
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> popChangeFeedMutationsActor(Reference<DatabaseContext> db, Key rangeID, Version version) {
	state Database cx(db);
	state Key rangeIDKey = rangeID.withPrefix(changeFeedPrefix);
	state Span span("NAPI:PopChangeFeedMutations"_loc);

	state KeyRange keys = wait(getChangeFeedRange(db, cx, rangeID));

	state std::vector<KeyRangeLocationInfo> locations =
	    wait(getKeyRangeLocations(cx,
	                              Optional<TenantName>(),
	                              keys,
	                              3,
	                              Reverse::False,
	                              &StorageServerInterface::changeFeedPop,
	                              span.context,
	                              Optional<UID>(),
	                              UseProvisionalProxies::False,
	                              latestVersion));

	if (locations.size() > 2) {
		wait(popChangeFeedBackup(cx, rangeID, version));
		return Void();
	}

	bool foundFailed = false;
	for (int i = 0; i < locations.size() && !foundFailed; i++) {
		for (int j = 0; j < locations[i].locations->size() && !foundFailed; j++) {
			if (IFailureMonitor::failureMonitor()
			        .getState(locations[i].locations->get(j, &StorageServerInterface::changeFeedPop).getEndpoint())
			        .isFailed()) {
				foundFailed = true;
			}
		}
	}

	if (foundFailed) {
		wait(popChangeFeedBackup(cx, rangeID, version));
		return Void();
	}

	try {
		// FIXME: lookup both the src and dest shards as of the pop version to ensure all locations are popped
		std::vector<Future<Void>> popRequests;
		for (int i = 0; i < locations.size(); i++) {
			for (int j = 0; j < locations[i].locations->size(); j++) {
				popRequests.push_back(locations[i].locations->getInterface(j).changeFeedPop.getReply(
				    ChangeFeedPopRequest(rangeID, version, locations[i].range)));
			}
		}
		choose {
			when(wait(waitForAll(popRequests))) {}
			when(wait(delay(CLIENT_KNOBS->CHANGE_FEED_POP_TIMEOUT))) {
				wait(popChangeFeedBackup(cx, rangeID, version));
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_unknown_change_feed && e.code() != error_code_wrong_shard_server &&
		    e.code() != error_code_all_alternatives_failed && e.code() != error_code_broken_promise &&
		    e.code() != error_code_server_overloaded) {
			throw;
		}
		db->changeFeedCache.erase(rangeID);
		cx->invalidateCache(Key(), keys);
		wait(popChangeFeedBackup(cx, rangeID, version));
	}
	return Void();
}

Future<Void> DatabaseContext::popChangeFeedMutations(Key rangeID, Version version) {
	return popChangeFeedMutationsActor(Reference<DatabaseContext>::addRef(this), rangeID, version);
}

Reference<DatabaseContext::TransactionT> DatabaseContext::createTransaction() {
	return makeReference<ReadYourWritesTransaction>(Database(Reference<DatabaseContext>::addRef(this)));
}

ACTOR Future<Key> purgeBlobGranulesActor(Reference<DatabaseContext> db,
                                         KeyRange range,
                                         Version purgeVersion,
                                         bool force) {
	state Database cx(db);
	state Transaction tr(cx);
	state Key purgeKey;

	// FIXME: implement force
	if (!force) {
		throw unsupported_operation();
	}
	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			Value purgeValue = blobGranulePurgeValueFor(purgeVersion, range, force);
			tr.atomicOp(
			    addVersionStampAtEnd(blobGranulePurgeKeys.begin), purgeValue, MutationRef::SetVersionstampedKey);
			tr.set(blobGranulePurgeChangeKey, deterministicRandom()->randomUniqueID().toString());
			state Future<Standalone<StringRef>> fTrVs = tr.getVersionstamp();
			wait(tr.commit());
			Standalone<StringRef> vs = wait(fTrVs);
			purgeKey = blobGranulePurgeKeys.begin.withSuffix(vs);
			if (BG_REQUEST_DEBUG) {
				fmt::print("purgeBlobGranules for range [{0} - {1}) at version {2} registered {3}\n",
				           range.begin.printable(),
				           range.end.printable(),
				           purgeVersion,
				           purgeKey.printable());
			}
			break;
		} catch (Error& e) {
			if (BG_REQUEST_DEBUG) {
				fmt::print("purgeBlobGranules for range [{0} - {1}) at version {2} encountered error {3}\n",
				           range.begin.printable(),
				           range.end.printable(),
				           purgeVersion,
				           e.name());
			}
			wait(tr.onError(e));
		}
	}
	return purgeKey;
}

Future<Key> DatabaseContext::purgeBlobGranules(KeyRange range, Version purgeVersion, bool force) {
	return purgeBlobGranulesActor(Reference<DatabaseContext>::addRef(this), range, purgeVersion, force);
}

ACTOR Future<Void> waitPurgeGranulesCompleteActor(Reference<DatabaseContext> db, Key purgeKey) {
	state Database cx(db);
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			Optional<Value> purgeVal = wait(tr->get(purgeKey));
			if (!purgeVal.present()) {
				if (BG_REQUEST_DEBUG) {
					fmt::print("purgeBlobGranules for {0} succeeded\n", purgeKey.printable());
				}
				return Void();
			}
			if (BG_REQUEST_DEBUG) {
				fmt::print("purgeBlobGranules for {0} watching\n", purgeKey.printable());
			}
			state Future<Void> watchFuture = tr->watch(purgeKey);
			wait(tr->commit());
			wait(watchFuture);
			tr->reset();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

Future<Void> DatabaseContext::waitPurgeGranulesComplete(Key purgeKey) {
	return waitPurgeGranulesCompleteActor(Reference<DatabaseContext>::addRef(this), purgeKey);
}

int64_t getMaxKeySize(KeyRef const& key) {
	return getMaxWriteKeySize(key, true);
}

int64_t getMaxReadKeySize(KeyRef const& key) {
	return getMaxKeySize(key);
}

int64_t getMaxWriteKeySize(KeyRef const& key, bool hasRawAccess) {
	int64_t tenantSize = hasRawAccess ? CLIENT_KNOBS->TENANT_PREFIX_SIZE_LIMIT : 0;
	return key.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT
	                                        : CLIENT_KNOBS->KEY_SIZE_LIMIT + tenantSize;
}

int64_t getMaxClearKeySize(KeyRef const& key) {
	return getMaxKeySize(key);
}
