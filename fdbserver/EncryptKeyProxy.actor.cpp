/*
 * EncryptKeyProxy.actor.cpp
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

#include "fdbserver/EncryptKeyProxyInterface.h"

#include "fdbrpc/Locality.h"
#include "fdbrpc/Stats.h"
#include "fdbserver/KmsConnector.h"
#include "fdbserver/KmsConnectorInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RESTKmsConnector.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/SimKmsConnector.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "flow/Arena.h"
#include "flow/EncryptUtils.h"
#include "flow/Error.h"
#include "flow/EventTypes.actor.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/Knobs.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"
#include "flow/network.h"

#include <boost/mpl/not.hpp>
#include <string>
#include <utility>
#include <memory>

#include "flow/actorcompiler.h" // This must be the last #include.

namespace {
bool canReplyWith(Error e) {
	switch (e.code()) {
	case error_code_encrypt_key_not_found:
		return true;
	default:
		return false;
	}
}

} // namespace

struct EncryptBaseCipherKey {
	EncryptCipherDomainId domainId;
	Standalone<EncryptCipherDomainName> domainName;
	EncryptCipherBaseKeyId baseCipherId;
	Standalone<StringRef> baseCipherKey;
	uint64_t creationTimeSec;
	bool noExpiry;

	EncryptBaseCipherKey()
	  : domainId(0), baseCipherId(0), baseCipherKey(StringRef()), creationTimeSec(0), noExpiry(false) {}
	explicit EncryptBaseCipherKey(EncryptCipherDomainId dId,
	                              EncryptCipherDomainName dName,
	                              EncryptCipherBaseKeyId cipherId,
	                              StringRef cipherKey,
	                              bool neverExpire)
	  : domainId(dId), domainName(Standalone<StringRef>(dName)), baseCipherId(cipherId),
	    baseCipherKey(Standalone<StringRef>(cipherKey)), creationTimeSec(now()), noExpiry(neverExpire) {}

	bool isValid() { return noExpiry ? true : ((now() - creationTimeSec) < FLOW_KNOBS->ENCRYPT_CIPHER_KEY_CACHE_TTL); }
};

// TODO: could refactor both into CacheEntry<T> with T data, creationTimeSec, and noExpiry
struct BlobMetadataCacheEntry {
	Standalone<BlobMetadataDetailsRef> metadataDetails;
	uint64_t creationTimeSec;

	BlobMetadataCacheEntry() : creationTimeSec(0) {}
	explicit BlobMetadataCacheEntry(Standalone<BlobMetadataDetailsRef> metadataDetails)
	  : metadataDetails(metadataDetails), creationTimeSec(now()) {}

	bool isValid() { return (now() - creationTimeSec) < SERVER_KNOBS->BLOB_METADATA_CACHE_TTL; }
};

using EncryptBaseDomainIdCache = std::unordered_map<EncryptCipherDomainId, EncryptBaseCipherKey>;

using EncryptBaseCipherDomainIdKeyIdCacheKey = std::pair<EncryptCipherDomainId, EncryptCipherBaseKeyId>;
using EncryptBaseCipherDomainIdKeyIdCacheKeyHash = boost::hash<EncryptBaseCipherDomainIdKeyIdCacheKey>;
using EncryptBaseCipherDomainIdKeyIdCache = std::unordered_map<EncryptBaseCipherDomainIdKeyIdCacheKey,
                                                               EncryptBaseCipherKey,
                                                               EncryptBaseCipherDomainIdKeyIdCacheKeyHash>;
using BlobMetadataDomainIdCache = std::unordered_map<BlobMetadataDomainId, BlobMetadataCacheEntry>;

struct EncryptKeyProxyData : NonCopyable, ReferenceCounted<EncryptKeyProxyData> {
public:
	UID myId;
	PromiseStream<Future<Void>> addActor;
	Future<Void> encryptionKeyRefresher;
	Future<Void> blobMetadataRefresher;

	EncryptBaseDomainIdCache baseCipherDomainIdCache;
	EncryptBaseCipherDomainIdKeyIdCache baseCipherDomainIdKeyIdCache;
	BlobMetadataDomainIdCache blobMetadataDomainIdCache;

	std::unique_ptr<KmsConnector> kmsConnector;

	CounterCollection ekpCacheMetrics;

	Counter baseCipherKeyIdCacheMisses;
	Counter baseCipherKeyIdCacheHits;
	Counter baseCipherDomainIdCacheMisses;
	Counter baseCipherDomainIdCacheHits;
	Counter baseCipherKeysRefreshed;
	Counter numResponseWithErrors;
	Counter numEncryptionKeyRefreshErrors;
	Counter blobMetadataCacheHits;
	Counter blobMetadataCacheMisses;
	Counter blobMetadataRefreshed;
	Counter numBlobMetadataRefreshErrors;

	explicit EncryptKeyProxyData(UID id)
	  : myId(id), ekpCacheMetrics("EKPMetrics", myId.toString()),
	    baseCipherKeyIdCacheMisses("EKPCipherIdCacheMisses", ekpCacheMetrics),
	    baseCipherKeyIdCacheHits("EKPCipherIdCacheHits", ekpCacheMetrics),
	    baseCipherDomainIdCacheMisses("EKPCipherDomainIdCacheMisses", ekpCacheMetrics),
	    baseCipherDomainIdCacheHits("EKPCipherDomainIdCacheHits", ekpCacheMetrics),
	    baseCipherKeysRefreshed("EKPCipherKeysRefreshed", ekpCacheMetrics),
	    numResponseWithErrors("EKPNumResponseWithErrors", ekpCacheMetrics),
	    numEncryptionKeyRefreshErrors("EKPNumEncryptionKeyRefreshErrors", ekpCacheMetrics),
	    blobMetadataCacheHits("EKPBlobMetadataCacheHits", ekpCacheMetrics),
	    blobMetadataCacheMisses("EKPBlobMetadataCacheMisses", ekpCacheMetrics),
	    blobMetadataRefreshed("EKPBlobMetadataRefreshed", ekpCacheMetrics),
	    numBlobMetadataRefreshErrors("EKPBlobMetadataRefreshErrors", ekpCacheMetrics) {}

	EncryptBaseCipherDomainIdKeyIdCacheKey getBaseCipherDomainIdKeyIdCacheKey(
	    const EncryptCipherDomainId domainId,
	    const EncryptCipherBaseKeyId baseCipherId) {
		return std::make_pair(domainId, baseCipherId);
	}

	void insertIntoBaseDomainIdCache(const EncryptCipherDomainId domainId,
	                                 EncryptCipherDomainName domainName,
	                                 const EncryptCipherBaseKeyId baseCipherId,
	                                 StringRef baseCipherKey) {
		// Entries in domainId cache are eligible for periodic refreshes to support 'limiting lifetime of encryption
		// key' support if enabled on external KMS solutions.

		baseCipherDomainIdCache[domainId] =
		    EncryptBaseCipherKey(domainId, domainName, baseCipherId, baseCipherKey, false);

		// Update cached the information indexed using baseCipherId
		insertIntoBaseCipherIdCache(domainId, domainName, baseCipherId, baseCipherKey);
	}

	void insertIntoBaseCipherIdCache(const EncryptCipherDomainId domainId,
	                                 EncryptCipherDomainName domainName,
	                                 const EncryptCipherBaseKeyId baseCipherId,
	                                 const StringRef baseCipherKey) {
		// Given an cipherKey is immutable, it is OK to NOT expire cached information.
		// TODO: Update cache to support LRU eviction policy to limit the total cache size.

		EncryptBaseCipherDomainIdKeyIdCacheKey cacheKey = getBaseCipherDomainIdKeyIdCacheKey(domainId, baseCipherId);
		baseCipherDomainIdKeyIdCache[cacheKey] =
		    EncryptBaseCipherKey(domainId, domainName, baseCipherId, baseCipherKey, true);
	}

	void insertIntoBlobMetadataCache(const BlobMetadataDomainId domainId,
	                                 const Standalone<BlobMetadataDetailsRef>& entry) {
		blobMetadataDomainIdCache[domainId] = BlobMetadataCacheEntry(entry);
	}

	template <class Reply>
	using isEKPGetLatestBaseCipherKeysReply = std::is_base_of<EKPGetLatestBaseCipherKeysReply, Reply>;
	template <class Reply>
	using isEKPGetBaseCipherKeysByIdsReply = std::is_base_of<EKPGetBaseCipherKeysByIdsReply, Reply>;

	// For errors occuring due to invalid input parameters such as: invalid encryptionDomainId or
	// invalid baseCipherId, piggyback error with response to the client; approach allows clients
	// to take necessary corrective actions such as: clearing up cache with invalid ids, log relevant
	// details for further investigation etc.

	template <class Reply>
	typename std::enable_if<isEKPGetBaseCipherKeysByIdsReply<Reply>::value ||
	                            isEKPGetLatestBaseCipherKeysReply<Reply>::value,
	                        void>::type
	sendErrorResponse(const ReplyPromise<Reply>& promise, const Error& e) {
		Reply reply;
		++numResponseWithErrors;
		reply.error = e;
		promise.send(reply);
	}
};

ACTOR Future<Void> getCipherKeysByBaseCipherKeyIds(Reference<EncryptKeyProxyData> ekpProxyData,
                                                   KmsConnectorInterface kmsConnectorInf,
                                                   EKPGetBaseCipherKeysByIdsRequest req) {
	// Scan the cached cipher-keys and filter our baseCipherIds locally cached
	// for the rest, reachout to KMS to fetch the required details

	state std::unordered_map<std::pair<EncryptCipherDomainId, EncryptCipherBaseKeyId>,
	                         EKPGetBaseCipherKeysRequestInfo,
	                         boost::hash<std::pair<EncryptCipherDomainId, EncryptCipherBaseKeyId>>>
	    lookupCipherInfoMap;

	state std::vector<EKPBaseCipherDetails> cachedCipherDetails;
	state EKPGetBaseCipherKeysByIdsRequest keysByIds = req;
	state EKPGetBaseCipherKeysByIdsReply keyIdsReply;
	state Optional<TraceEvent> dbgTrace =
	    keysByIds.debugId.present() ? TraceEvent("GetByKeyIds", ekpProxyData->myId) : Optional<TraceEvent>();

	if (dbgTrace.present()) {
		dbgTrace.get().setMaxEventLength(SERVER_KNOBS->ENCRYPT_PROXY_MAX_DBG_TRACE_LENGTH);
		dbgTrace.get().detail("DbgId", keysByIds.debugId.get());
	}

	// Dedup the requested pair<baseCipherId, encryptDomainId>
	// TODO: endpoint serialization of std::unordered_set isn't working at the moment
	std::unordered_set<EKPGetBaseCipherKeysRequestInfo, EKPGetBaseCipherKeysRequestInfo_Hash> dedupedCipherInfos;
	for (const auto& item : req.baseCipherInfos) {
		dedupedCipherInfos.emplace(item);
	}

	if (dbgTrace.present()) {
		dbgTrace.get().detail("NKeys", dedupedCipherInfos.size());
		for (const auto& item : dedupedCipherInfos) {
			// Record {encryptDomainId, baseCipherId} queried
			dbgTrace.get().detail(
			    getEncryptDbgTraceKey(
			        ENCRYPT_DBG_TRACE_QUERY_PREFIX, item.domainId, item.domainName, item.baseCipherId),
			    "");
		}
	}

	for (const auto& item : dedupedCipherInfos) {
		const EncryptBaseCipherDomainIdKeyIdCacheKey cacheKey =
		    ekpProxyData->getBaseCipherDomainIdKeyIdCacheKey(item.domainId, item.baseCipherId);
		const auto itr = ekpProxyData->baseCipherDomainIdKeyIdCache.find(cacheKey);
		if (itr != ekpProxyData->baseCipherDomainIdKeyIdCache.end()) {
			ASSERT(itr->second.isValid());
			cachedCipherDetails.emplace_back(
			    itr->second.domainId, itr->second.baseCipherId, itr->second.baseCipherKey, keyIdsReply.arena);

			if (dbgTrace.present()) {
				// {encryptId, baseCipherId} forms a unique tuple across encryption domains
				dbgTrace.get().detail(getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_CACHED_PREFIX,
				                                            itr->second.domainId,
				                                            item.domainName,
				                                            itr->second.baseCipherId),
				                      "");
			}
		} else {
			lookupCipherInfoMap.emplace(std::make_pair(item.domainId, item.baseCipherId), item);
		}
	}

	ekpProxyData->baseCipherKeyIdCacheHits += cachedCipherDetails.size();
	ekpProxyData->baseCipherKeyIdCacheMisses += lookupCipherInfoMap.size();

	if (!lookupCipherInfoMap.empty()) {
		try {
			KmsConnLookupEKsByKeyIdsReq keysByIdsReq;
			for (const auto& item : lookupCipherInfoMap) {
				keysByIdsReq.encryptKeyInfos.emplace_back(KmsConnLookupKeyIdsReqInfo(
				    item.second.domainId, item.second.baseCipherId, item.second.domainName, keysByIdsReq.arena));
			}
			keysByIdsReq.debugId = keysByIds.debugId;
			KmsConnLookupEKsByKeyIdsRep keysByIdsRep = wait(kmsConnectorInf.ekLookupByIds.getReply(keysByIdsReq));

			for (const auto& item : keysByIdsRep.cipherKeyDetails) {
				keyIdsReply.baseCipherDetails.emplace_back(
				    item.encryptDomainId, item.encryptKeyId, item.encryptKey, keyIdsReply.arena);
			}

			// Record the fetched cipher details to the local cache for the future references
			// Note: cache warm-up is done after reponding to the caller

			for (auto& item : keysByIdsRep.cipherKeyDetails) {
				const auto itr = lookupCipherInfoMap.find(std::make_pair(item.encryptDomainId, item.encryptKeyId));
				if (itr == lookupCipherInfoMap.end()) {
					TraceEvent(SevError, "GetCipherKeysByKeyIds_MappingNotFound", ekpProxyData->myId)
					    .detail("DomainId", item.encryptDomainId);
					throw encrypt_keys_fetch_failed();
				}
				ekpProxyData->insertIntoBaseCipherIdCache(
				    item.encryptDomainId, itr->second.domainName, item.encryptKeyId, item.encryptKey);

				if (dbgTrace.present()) {
					// {encryptId, baseCipherId} forms a unique tuple across encryption domains
					dbgTrace.get().detail(getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_INSERT_PREFIX,
					                                            item.encryptDomainId,
					                                            itr->second.domainName,
					                                            item.encryptKeyId),
					                      "");
				}
			}
		} catch (Error& e) {
			if (!canReplyWith(e)) {
				TraceEvent("GetCipherKeysByKeyIds", ekpProxyData->myId).error(e);
				throw;
			}
			TraceEvent("GetCipherKeysByKeyIds", ekpProxyData->myId).detail("ErrorCode", e.code());
			ekpProxyData->sendErrorResponse(keysByIds.reply, e);
			return Void();
		}
	}

	// Append cached cipherKeyDetails to the result-set
	keyIdsReply.baseCipherDetails.insert(
	    keyIdsReply.baseCipherDetails.end(), cachedCipherDetails.begin(), cachedCipherDetails.end());

	keyIdsReply.numHits = cachedCipherDetails.size();
	keysByIds.reply.send(keyIdsReply);

	return Void();
}

ACTOR Future<Void> getLatestCipherKeys(Reference<EncryptKeyProxyData> ekpProxyData,
                                       KmsConnectorInterface kmsConnectorInf,
                                       EKPGetLatestBaseCipherKeysRequest req) {
	// Scan the cached cipher-keys and filter our baseCipherIds locally cached
	// for the rest, reachout to KMS to fetch the required details
	state std::vector<EKPBaseCipherDetails> cachedCipherDetails;
	state EKPGetLatestBaseCipherKeysRequest latestKeysReq = req;
	state EKPGetLatestBaseCipherKeysReply latestCipherReply;
	state Arena& arena = latestCipherReply.arena;
	state Optional<TraceEvent> dbgTrace =
	    latestKeysReq.debugId.present() ? TraceEvent("GetByDomIds", ekpProxyData->myId) : Optional<TraceEvent>();

	if (dbgTrace.present()) {
		dbgTrace.get().setMaxEventLength(SERVER_KNOBS->ENCRYPT_PROXY_MAX_DBG_TRACE_LENGTH);
		dbgTrace.get().detail("DbgId", latestKeysReq.debugId.get());
	}

	// Dedup the requested domainIds.
	// TODO: endpoint serialization of std::unordered_set isn't working at the moment
	std::unordered_map<EncryptCipherDomainId, EKPGetLatestCipherKeysRequestInfo> dedupedDomainInfos;
	for (const auto info : req.encryptDomainInfos) {
		dedupedDomainInfos.emplace(info.domainId, info);
	}

	if (dbgTrace.present()) {
		dbgTrace.get().detail("NKeys", dedupedDomainInfos.size());
		for (const auto info : dedupedDomainInfos) {
			// log encryptDomainIds queried
			dbgTrace.get().detail(
			    getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_QUERY_PREFIX, info.first, info.second.domainName), "");
		}
	}

	// First, check if the requested information is already cached by the server.
	// Ensure the cached information is within FLOW_KNOBS->ENCRYPT_CIPHER_KEY_CACHE_TTL time window.

	state std::unordered_map<EncryptCipherDomainId, EKPGetLatestCipherKeysRequestInfo> lookupCipherDomains;
	for (const auto& info : dedupedDomainInfos) {
		const auto itr = ekpProxyData->baseCipherDomainIdCache.find(info.first);
		if (itr != ekpProxyData->baseCipherDomainIdCache.end() && itr->second.isValid()) {
			cachedCipherDetails.emplace_back(info.first, itr->second.baseCipherId, itr->second.baseCipherKey, arena);

			if (dbgTrace.present()) {
				// {encryptDomainId, baseCipherId} forms a unique tuple across encryption domains
				dbgTrace.get().detail(
				    getEncryptDbgTraceKey(
				        ENCRYPT_DBG_TRACE_CACHED_PREFIX, info.first, info.second.domainName, itr->second.baseCipherId),
				    "");
			}
		} else {
			lookupCipherDomains.emplace(info.first, info.second);
		}
	}

	ekpProxyData->baseCipherDomainIdCacheHits += cachedCipherDetails.size();
	ekpProxyData->baseCipherDomainIdCacheMisses += lookupCipherDomains.size();

	if (!lookupCipherDomains.empty()) {
		try {
			KmsConnLookupEKsByDomainIdsReq keysByDomainIdReq;
			for (const auto& item : lookupCipherDomains) {
				keysByDomainIdReq.encryptDomainInfos.emplace_back(KmsConnLookupDomainIdsReqInfo(
				    item.second.domainId, item.second.domainName, keysByDomainIdReq.arena));
			}
			keysByDomainIdReq.debugId = latestKeysReq.debugId;

			KmsConnLookupEKsByDomainIdsRep keysByDomainIdRep =
			    wait(kmsConnectorInf.ekLookupByDomainIds.getReply(keysByDomainIdReq));

			for (auto& item : keysByDomainIdRep.cipherKeyDetails) {
				latestCipherReply.baseCipherDetails.emplace_back(
				    item.encryptDomainId, item.encryptKeyId, item.encryptKey, arena);

				// Record the fetched cipher details to the local cache for the future references
				const auto itr = lookupCipherDomains.find(item.encryptDomainId);
				if (itr == lookupCipherDomains.end()) {
					TraceEvent(SevError, "GetLatestCipherKeys_DomainIdNotFound", ekpProxyData->myId)
					    .detail("DomainId", item.encryptDomainId);
					throw encrypt_keys_fetch_failed();
				}
				ekpProxyData->insertIntoBaseDomainIdCache(
				    item.encryptDomainId, itr->second.domainName, item.encryptKeyId, item.encryptKey);

				if (dbgTrace.present()) {
					// {encryptDomainId, baseCipherId} forms a unique tuple across encryption domains
					dbgTrace.get().detail(getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_INSERT_PREFIX,
					                                            item.encryptDomainId,
					                                            itr->second.domainName,
					                                            item.encryptKeyId),
					                      "");
				}
			}
		} catch (Error& e) {
			if (!canReplyWith(e)) {
				TraceEvent("GetLatestCipherKeys", ekpProxyData->myId).error(e);
				throw;
			}
			TraceEvent("GetLatestCipherKeys", ekpProxyData->myId).detail("ErrorCode", e.code());
			ekpProxyData->sendErrorResponse(latestKeysReq.reply, e);
			return Void();
		}
	}

	for (auto& item : cachedCipherDetails) {
		latestCipherReply.baseCipherDetails.emplace_back(
		    item.encryptDomainId, item.baseCipherId, item.baseCipherKey, arena);
	}

	latestCipherReply.numHits = cachedCipherDetails.size();
	latestKeysReq.reply.send(latestCipherReply);

	return Void();
}

ACTOR Future<Void> refreshEncryptionKeysCore(Reference<EncryptKeyProxyData> ekpProxyData,
                                             KmsConnectorInterface kmsConnectorInf) {
	state UID debugId = deterministicRandom()->randomUniqueID();

	state TraceEvent t("RefreshEKs_Start", ekpProxyData->myId);
	t.setMaxEventLength(SERVER_KNOBS->ENCRYPT_PROXY_MAX_DBG_TRACE_LENGTH);
	t.detail("KmsConnInf", kmsConnectorInf.id());
	t.detail("DebugId", debugId);

	try {
		KmsConnLookupEKsByDomainIdsReq req;
		req.debugId = debugId;
		req.encryptDomainInfos.reserve(ekpProxyData->baseCipherDomainIdCache.size());

		for (const auto& item : ekpProxyData->baseCipherDomainIdCache) {
			req.encryptDomainInfos.emplace_back(
			    KmsConnLookupDomainIdsReqInfo(item.first, item.second.domainName, req.arena));
		}
		KmsConnLookupEKsByDomainIdsRep rep = wait(kmsConnectorInf.ekLookupByDomainIds.getReply(req));
		for (const auto& item : rep.cipherKeyDetails) {
			const auto itr = ekpProxyData->baseCipherDomainIdCache.find(item.encryptDomainId);
			if (itr == ekpProxyData->baseCipherDomainIdCache.end()) {
				TraceEvent(SevError, "RefreshEKs_DomainIdNotFound", ekpProxyData->myId)
				    .detail("DomainId", item.encryptDomainId);
				// Continue updating the cache with othe elements
				continue;
			}
			ekpProxyData->insertIntoBaseDomainIdCache(
			    item.encryptDomainId, itr->second.domainName, item.encryptKeyId, item.encryptKey);
			// {encryptDomainId, baseCipherId} forms a unique tuple across encryption domains
			t.detail(
			    getEncryptDbgTraceKey(
			        ENCRYPT_DBG_TRACE_INSERT_PREFIX, item.encryptDomainId, itr->second.domainName, item.encryptKeyId),
			    "");
		}

		ekpProxyData->baseCipherKeysRefreshed += rep.cipherKeyDetails.size();

		t.detail("nKeys", rep.cipherKeyDetails.size());
	} catch (Error& e) {
		if (!canReplyWith(e)) {
			TraceEvent("RefreshEKs_Error").error(e);
			throw e;
		}
		TraceEvent("RefreshEKs").detail("ErrorCode", e.code());
		++ekpProxyData->numEncryptionKeyRefreshErrors;
	}

	return Void();
}

void refreshEncryptionKeys(Reference<EncryptKeyProxyData> ekpProxyData, KmsConnectorInterface kmsConnectorInf) {
	Future<Void> ignored = refreshEncryptionKeysCore(ekpProxyData, kmsConnectorInf);
}

ACTOR Future<Void> getLatestBlobMetadata(Reference<EncryptKeyProxyData> ekpProxyData,
                                         KmsConnectorInterface kmsConnectorInf,
                                         EKPGetLatestBlobMetadataRequest req) {
	// Use cached metadata if it exists, otherwise reach out to KMS
	state Standalone<VectorRef<BlobMetadataDetailsRef>> metadataDetails;
	state Optional<TraceEvent> dbgTrace =
	    req.debugId.present() ? TraceEvent("GetBlobMetadata", ekpProxyData->myId) : Optional<TraceEvent>();

	if (dbgTrace.present()) {
		dbgTrace.get().setMaxEventLength(SERVER_KNOBS->ENCRYPT_PROXY_MAX_DBG_TRACE_LENGTH);
		dbgTrace.get().detail("DbgId", req.debugId.get());
	}

	// Dedup the requested domainIds.
	std::unordered_set<BlobMetadataDomainId> dedupedDomainIds;
	for (auto id : req.domainIds) {
		dedupedDomainIds.emplace(id);
	}

	if (dbgTrace.present()) {
		dbgTrace.get().detail("NKeys", dedupedDomainIds.size());
		for (BlobMetadataDomainId id : dedupedDomainIds) {
			// log domainids queried
			dbgTrace.get().detail("BMQ" + std::to_string(id), "");
		}
	}

	// First, check if the requested information is already cached by the server.
	// Ensure the cached information is within SERVER_KNOBS->BLOB_METADATA_CACHE_TTL time window.
	std::vector<BlobMetadataDomainId> lookupDomains;
	for (BlobMetadataDomainId id : dedupedDomainIds) {
		const auto itr = ekpProxyData->blobMetadataDomainIdCache.find(id);
		if (itr != ekpProxyData->blobMetadataDomainIdCache.end() && itr->second.isValid()) {
			metadataDetails.arena().dependsOn(itr->second.metadataDetails.arena());
			metadataDetails.push_back(metadataDetails.arena(), itr->second.metadataDetails);

			if (dbgTrace.present()) {
				dbgTrace.get().detail("BMC" + std::to_string(id), "");
			}
			++ekpProxyData->blobMetadataCacheHits;
		} else {
			lookupDomains.emplace_back(id);
			++ekpProxyData->blobMetadataCacheMisses;
		}
	}

	ekpProxyData->baseCipherDomainIdCacheHits += metadataDetails.size();
	ekpProxyData->baseCipherDomainIdCacheMisses += lookupDomains.size();

	if (!lookupDomains.empty()) {
		try {
			KmsConnBlobMetadataReq kmsReq(lookupDomains, req.debugId);
			KmsConnBlobMetadataRep kmsRep = wait(kmsConnectorInf.blobMetadataReq.getReply(kmsReq));
			metadataDetails.arena().dependsOn(kmsRep.metadataDetails.arena());

			for (auto& item : kmsRep.metadataDetails) {
				metadataDetails.push_back(metadataDetails.arena(), item);

				// Record the fetched metadata to the local cache for the future references
				ekpProxyData->insertIntoBlobMetadataCache(item.domainId, item);

				if (dbgTrace.present()) {
					dbgTrace.get().detail("BMI" + std::to_string(item.domainId), "");
				}
			}
		} catch (Error& e) {
			if (!canReplyWith(e)) {
				TraceEvent("GetLatestBlobMetadataUnexpectedError", ekpProxyData->myId).error(e);
				throw;
			}
			TraceEvent("GetLatestBlobMetadataExpectedError", ekpProxyData->myId).error(e);
			req.reply.sendError(e);
			return Void();
		}
	}

	req.reply.send(EKPGetLatestBlobMetadataReply(metadataDetails));

	return Void();
}

ACTOR Future<Void> refreshBlobMetadataCore(Reference<EncryptKeyProxyData> ekpProxyData,
                                           KmsConnectorInterface kmsConnectorInf) {
	state UID debugId = deterministicRandom()->randomUniqueID();

	state TraceEvent t("RefreshBlobMetadata_Start", ekpProxyData->myId);
	t.setMaxEventLength(SERVER_KNOBS->ENCRYPT_PROXY_MAX_DBG_TRACE_LENGTH);
	t.detail("KmsConnInf", kmsConnectorInf.id());
	t.detail("DebugId", debugId);

	try {
		KmsConnBlobMetadataReq req;
		req.debugId = debugId;
		req.domainIds.reserve(ekpProxyData->blobMetadataDomainIdCache.size());

		for (auto& item : ekpProxyData->blobMetadataDomainIdCache) {
			req.domainIds.emplace_back(item.first);
		}
		KmsConnBlobMetadataRep rep = wait(kmsConnectorInf.blobMetadataReq.getReply(req));
		for (auto& item : rep.metadataDetails) {
			ekpProxyData->insertIntoBlobMetadataCache(item.domainId, item);
			t.detail("BM" + std::to_string(item.domainId), "");
		}

		ekpProxyData->blobMetadataRefreshed += rep.metadataDetails.size();

		t.detail("nKeys", rep.metadataDetails.size());
	} catch (Error& e) {
		if (!canReplyWith(e)) {
			TraceEvent("RefreshBlobMetadata_Error").error(e);
			throw e;
		}
		TraceEvent("RefreshBlobMetadata").detail("ErrorCode", e.code());
		++ekpProxyData->numBlobMetadataRefreshErrors;
	}

	return Void();
}

void refreshBlobMetadata(Reference<EncryptKeyProxyData> ekpProxyData, KmsConnectorInterface kmsConnectorInf) {
	Future<Void> ignored = refreshBlobMetadataCore(ekpProxyData, kmsConnectorInf);
}

void activateKmsConnector(Reference<EncryptKeyProxyData> ekpProxyData, KmsConnectorInterface kmsConnectorInf) {
	if (g_network->isSimulated()) {
		ekpProxyData->kmsConnector = std::make_unique<SimKmsConnector>();
	} else if (SERVER_KNOBS->KMS_CONNECTOR_TYPE.compare("RESTKmsConnector")) {
		ekpProxyData->kmsConnector = std::make_unique<RESTKmsConnector>();
	} else {
		throw not_implemented();
	}

	TraceEvent("EKP_ActiveKmsConnector", ekpProxyData->myId).detail("ConnectorType", SERVER_KNOBS->KMS_CONNECTOR_TYPE);
	ekpProxyData->addActor.send(ekpProxyData->kmsConnector->connectorCore(kmsConnectorInf));
}

ACTOR Future<Void> encryptKeyProxyServer(EncryptKeyProxyInterface ekpInterface, Reference<AsyncVar<ServerDBInfo>> db) {
	state Reference<EncryptKeyProxyData> self(new EncryptKeyProxyData(ekpInterface.id()));
	state Future<Void> collection = actorCollection(self->addActor.getFuture());
	self->addActor.send(traceRole(Role::ENCRYPT_KEY_PROXY, ekpInterface.id()));

	state KmsConnectorInterface kmsConnectorInf;
	kmsConnectorInf.initEndpoints();

	TraceEvent("EKP_Start", self->myId).detail("KmsConnectorInf", kmsConnectorInf.id());

	activateKmsConnector(self, kmsConnectorInf);

	// Register a recurring task to refresh the cached Encryption keys and blob metadata.
	// Approach avoids external RPCs due to EncryptionKey refreshes for the inline write encryption codepath such as:
	// CPs, Redwood Storage Server node flush etc. The process doing the encryption refresh the cached cipher keys based
	// on FLOW_KNOB->ENCRYPTION_CIPHER_KEY_CACHE_TTL_SEC interval which is intentionally kept longer than
	// FLOW_KNOB->ENCRRYPTION_KEY_REFRESH_INTERVAL_SEC, allowing the interactions with external Encryption Key Manager
	// mostly not co-inciding with FDB process encryption key refresh attempts.

	self->encryptionKeyRefresher = recurring([&]() { refreshEncryptionKeys(self, kmsConnectorInf); },
	                                         FLOW_KNOBS->ENCRYPT_KEY_REFRESH_INTERVAL,
	                                         TaskPriority::Worker);

	self->blobMetadataRefresher = recurring([&]() { refreshBlobMetadata(self, kmsConnectorInf); },
	                                        SERVER_KNOBS->BLOB_METADATA_REFRESH_INTERVAL,
	                                        TaskPriority::Worker);

	try {
		loop choose {
			when(EKPGetBaseCipherKeysByIdsRequest req = waitNext(ekpInterface.getBaseCipherKeysByIds.getFuture())) {
				self->addActor.send(getCipherKeysByBaseCipherKeyIds(self, kmsConnectorInf, req));
			}
			when(EKPGetLatestBaseCipherKeysRequest req = waitNext(ekpInterface.getLatestBaseCipherKeys.getFuture())) {
				self->addActor.send(getLatestCipherKeys(self, kmsConnectorInf, req));
			}
			when(EKPGetLatestBlobMetadataRequest req = waitNext(ekpInterface.getLatestBlobMetadata.getFuture())) {
				self->addActor.send(getLatestBlobMetadata(self, kmsConnectorInf, req));
			}
			when(HaltEncryptKeyProxyRequest req = waitNext(ekpInterface.haltEncryptKeyProxy.getFuture())) {
				TraceEvent("EKP_Halted", self->myId).detail("ReqID", req.requesterID);
				req.reply.send(Void());
				break;
			}
			when(wait(collection)) {
				ASSERT(false);
				throw internal_error();
			}
		}
	} catch (Error& e) {
		TraceEvent("EKP_Terminated", self->myId).errorUnsuppressed(e);
	}

	return Void();
}
