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

#include "fdbclient/DatabaseContext.h"

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
