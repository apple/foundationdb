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

#include "fdbclient/BlobMetadataUtils.h"
#include "fdbclient/EncryptKeyProxyInterface.h"

#include "fdbrpc/Locality.h"
#include "fdbserver/KmsConnector.h"
#include "fdbserver/KmsConnectorInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RESTKmsConnector.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/SimKmsConnector.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "flow/Arena.h"
#include "flow/CodeProbe.h"
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

#include <boost/functional/hash.hpp>
#include <boost/mpl/not.hpp>
#include <limits>
#include <string>
#include <utility>
#include <memory>

#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

const std::string REST_KMS_CONNECTOR_TYPE_STR = "RESTKmsConnector";
const std::string FDB_PREF_KMS_CONNECTOR_TYPE_STR = "FDBPerfKmsConnector";
const std::string FDB_SIM_KMS_CONNECTOR_TYPE_STR = "SimKmsConnector";

struct CipherKeyValidityTS {
	int64_t refreshAtTS;
	int64_t expAtTS;
};

bool canReplyWith(Error e) {
	switch (e.code()) {
	case error_code_encrypt_key_not_found:
	// FDB <-> KMS connection may be observing transient issues
	// Caller processes should consider reusing 'non-revocable' CipherKeys iff ONLY below error codes lead to CipherKey
	// refresh failure
	case error_code_timed_out:
	case error_code_connection_failed:
		return true;
	default:
		return false;
	}
}

int64_t computeCipherRefreshTS(Optional<int64_t> refreshInterval, int64_t currTS) {
	int64_t refreshAtTS = -1;
	const int64_t defaultTTL = FLOW_KNOBS->ENCRYPT_CIPHER_KEY_CACHE_TTL;

	if (refreshInterval.present()) {
		if (refreshInterval.get() < 0) {
			// Never refresh the CipherKey
			refreshAtTS = std::numeric_limits<int64_t>::max();
		} else if (refreshInterval.get() > 0) {
			refreshAtTS = currTS + refreshInterval.get();
		} else {
			ASSERT(refreshInterval.get() == 0);
			// Fallback to default refreshInterval if not specified
			refreshAtTS = currTS + defaultTTL;
		}
	} else {
		// Fallback to default refreshInterval if not specified
		refreshAtTS = currTS + defaultTTL;
	}

	ASSERT(refreshAtTS > 0);

	return refreshAtTS;
}

int64_t computeCipherExpireTS(Optional<int64_t> expiryInterval, int64_t currTS, int64_t refreshAtTS) {
	int64_t expireAtTS = -1;

	ASSERT(refreshAtTS > 0);

	if (expiryInterval.present()) {
		if (expiryInterval.get() < 0) {
			// Non-revocable CipherKey, never expire
			expireAtTS = std::numeric_limits<int64_t>::max();
		} else if (expiryInterval.get() > 0) {
			expireAtTS = currTS + expiryInterval.get();
		} else {
			ASSERT(expiryInterval.get() == 0);
			// None supplied, match expiry to refresh timestamp
			expireAtTS = refreshAtTS;
		}
	} else {
		// None supplied, match expiry to refresh timestamp
		expireAtTS = refreshAtTS;
	}

	ASSERT(expireAtTS > 0);

	return expireAtTS;
}

CipherKeyValidityTS getCipherKeyValidityTS(Optional<int64_t> refreshInterval, Optional<int64_t> expiryInterval) {
	int64_t currTS = (int64_t)now();

	CipherKeyValidityTS validityTS;
	validityTS.refreshAtTS = computeCipherRefreshTS(refreshInterval, currTS);
	validityTS.expAtTS = computeCipherExpireTS(expiryInterval, currTS, validityTS.refreshAtTS);

	return validityTS;
}

} // namespace

struct EncryptBaseCipherKey {
	EncryptCipherDomainId domainId;
	Standalone<EncryptCipherDomainNameRef> domainName;
	EncryptCipherBaseKeyId baseCipherId;
	Standalone<StringRef> baseCipherKey;
	// Timestamp after which the cached CipherKey is eligible for KMS refresh
	int64_t refreshAt;
	// Timestamp after which the cached CipherKey 'should' be considered as 'expired'
	// KMS can define two type of keys:
	// 1. Revocable CipherKeys    : CipherKeys that has a finite expiry interval.
	// 2. Non-revocable CipherKeys: CipherKeys which 'do not' expire, however, are still eligible for KMS refreshes to
	// support KMS CipherKey rotation.
	//
	// If/when CipherKey refresh fails due to transient outage in FDB <-> KMS connectivity, a caller is allowed to
	// leverage already cached CipherKey iff it is 'Non-revocable CipherKey'. PerpetualWiggle would update old/retired
	// CipherKeys with the latest CipherKeys sometime soon in the future.
	int64_t expireAt;

	EncryptBaseCipherKey() : domainId(0), baseCipherId(0), baseCipherKey(StringRef()), refreshAt(0), expireAt(0) {}
	explicit EncryptBaseCipherKey(EncryptCipherDomainId dId,
	                              Standalone<EncryptCipherDomainNameRef> dName,
	                              EncryptCipherBaseKeyId cipherId,
	                              Standalone<StringRef> cipherKey,
	                              int64_t refAtTS,
	                              int64_t expAtTS)
	  : domainId(dId), domainName(dName), baseCipherId(cipherId), baseCipherKey(cipherKey), refreshAt(refAtTS),
	    expireAt(expAtTS) {}

	bool isValid() const {
		int64_t currTS = (int64_t)now();
		return expireAt > currTS && refreshAt > currTS;
	}
	bool isExpired() const { return now() > expireAt; }
};

// TODO: could refactor both into CacheEntry<T> with T data, creationTimeSec, and noExpiry
struct BlobMetadataCacheEntry {
	Standalone<BlobMetadataDetailsRef> metadataDetails;
	uint64_t creationTimeSec;

	BlobMetadataCacheEntry() : creationTimeSec(0) {}
	explicit BlobMetadataCacheEntry(Standalone<BlobMetadataDetailsRef> metadataDetails)
	  : metadataDetails(metadataDetails), creationTimeSec(now()) {}

	bool isValid() const { return (now() - creationTimeSec) < SERVER_KNOBS->BLOB_METADATA_CACHE_TTL; }
};

// TODO: Bound the size of the cache (implement LRU/LFU...)
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

	LatencySample kmsLookupByIdsReqLatency;
	LatencySample kmsLookupByDomainIdsReqLatency;
	LatencySample kmsBlobMetadataReqLatency;

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
	    numBlobMetadataRefreshErrors("EKPBlobMetadataRefreshErrors", ekpCacheMetrics),
	    kmsLookupByIdsReqLatency("EKPKmsLookupByIdsReqLatency",
	                             id,
	                             SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                             SERVER_KNOBS->LATENCY_SAMPLE_SIZE),
	    kmsLookupByDomainIdsReqLatency("EKPKmsLookupByDomainIdsReqLatency",
	                                   id,
	                                   SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                                   SERVER_KNOBS->LATENCY_SAMPLE_SIZE),
	    kmsBlobMetadataReqLatency("EKPKmsBlobMetadataReqLatency",
	                              id,
	                              SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                              SERVER_KNOBS->LATENCY_SAMPLE_SIZE) {}

	EncryptBaseCipherDomainIdKeyIdCacheKey getBaseCipherDomainIdKeyIdCacheKey(
	    const EncryptCipherDomainId domainId,
	    const EncryptCipherBaseKeyId baseCipherId) {
		return std::make_pair(domainId, baseCipherId);
	}

	void insertIntoBaseDomainIdCache(const EncryptCipherDomainId domainId,
	                                 Standalone<EncryptCipherDomainNameRef> domainName,
	                                 const EncryptCipherBaseKeyId baseCipherId,
	                                 Standalone<StringRef> baseCipherKey,
	                                 int64_t refreshAtTS,
	                                 int64_t expireAtTS) {
		// Entries in domainId cache are eligible for periodic refreshes to support 'limiting lifetime of encryption
		// key' support if enabled on external KMS solutions.

		baseCipherDomainIdCache[domainId] =
		    EncryptBaseCipherKey(domainId, domainName, baseCipherId, baseCipherKey, refreshAtTS, expireAtTS);

		// Update cached the information indexed using baseCipherId
		// Cache indexed by 'baseCipherId' need not refresh cipher, however, it still needs to abide by KMS governed
		// CipherKey lifetime rules
		insertIntoBaseCipherIdCache(
		    domainId, domainName, baseCipherId, baseCipherKey, std::numeric_limits<int64_t>::max(), expireAtTS);
	}

	void insertIntoBaseCipherIdCache(const EncryptCipherDomainId domainId,
	                                 Standalone<EncryptCipherDomainNameRef> domainName,
	                                 const EncryptCipherBaseKeyId baseCipherId,
	                                 const Standalone<StringRef> baseCipherKey,
	                                 int64_t refreshAtTS,
	                                 int64_t expireAtTS) {
		// Given an cipherKey is immutable, it is OK to NOT expire cached information.
		// TODO: Update cache to support LRU eviction policy to limit the total cache size.

		EncryptBaseCipherDomainIdKeyIdCacheKey cacheKey = getBaseCipherDomainIdKeyIdCacheKey(domainId, baseCipherId);
		baseCipherDomainIdKeyIdCache[cacheKey] =
		    EncryptBaseCipherKey(domainId, domainName, baseCipherId, baseCipherKey, refreshAtTS, expireAtTS);
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
		if (itr != ekpProxyData->baseCipherDomainIdKeyIdCache.end() && itr->second.isValid()) {
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
				// TODO: Currently getEncryptCipherKeys does not pass the domain name, once that is fixed we can remove
				// the check on the empty domain name
				if (!item.second.domainName.empty()) {
					if (item.second.domainId == FDB_DEFAULT_ENCRYPT_DOMAIN_ID) {
						ASSERT(item.second.domainName == FDB_DEFAULT_ENCRYPT_DOMAIN_NAME);
					} else if (item.second.domainId == SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID) {
						ASSERT(item.second.domainName == FDB_SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_NAME);
					}
				}
				keysByIdsReq.encryptKeyInfos.emplace_back_deep(
				    keysByIdsReq.arena, item.second.domainId, item.second.baseCipherId, item.second.domainName);
			}
			keysByIdsReq.debugId = keysByIds.debugId;
			state double startTime = now();
			KmsConnLookupEKsByKeyIdsRep keysByIdsRep = wait(kmsConnectorInf.ekLookupByIds.getReply(keysByIdsReq));
			ekpProxyData->kmsLookupByIdsReqLatency.addMeasurement(now() - startTime);

			for (const auto& item : keysByIdsRep.cipherKeyDetails) {
				keyIdsReply.baseCipherDetails.emplace_back(
				    item.encryptDomainId, item.encryptKeyId, item.encryptKey, keyIdsReply.arena);
			}

			// Record the fetched cipher details to the local cache for the future references
			// Note: cache warm-up is done after reponding to the caller

			for (auto& item : keysByIdsRep.cipherKeyDetails) {
				// KMS governs lifetime of a given CipherKey, however, for non-latest CipherKey there isn't a necessity
				// to 'refresh' cipher (rotation is not applicable). But, 'expireInterval' is still valid if CipherKey
				// is a 'revocable key'

				CipherKeyValidityTS validityTS = getCipherKeyValidityTS(Optional<int64_t>(-1), item.expireAfterSec);

				const auto itr = lookupCipherInfoMap.find(std::make_pair(item.encryptDomainId, item.encryptKeyId));
				if (itr == lookupCipherInfoMap.end()) {
					TraceEvent(SevError, "GetCipherKeysByKeyIdsMappingNotFound", ekpProxyData->myId)
					    .detail("DomainId", item.encryptDomainId);
					throw encrypt_keys_fetch_failed();
				}
				ekpProxyData->insertIntoBaseCipherIdCache(item.encryptDomainId,
				                                          itr->second.domainName,
				                                          item.encryptKeyId,
				                                          item.encryptKey,
				                                          validityTS.refreshAtTS,
				                                          validityTS.expAtTS);

				if (dbgTrace.present()) {
					// {encryptId, baseCipherId} forms a unique tuple across encryption domains
					dbgTrace.get().detail(getEncryptDbgTraceKeyWithTS(ENCRYPT_DBG_TRACE_INSERT_PREFIX,
					                                                  item.encryptDomainId,
					                                                  itr->second.domainName,
					                                                  item.encryptKeyId,
					                                                  validityTS.refreshAtTS,
					                                                  validityTS.expAtTS),
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

	CODE_PROBE(!lookupCipherInfoMap.empty(), "EKP fetch cipherKeys by KeyId from KMS");

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
	for (const auto& info : req.encryptDomainInfos) {
		dedupedDomainInfos.emplace(info.domainId, info);
	}

	if (dbgTrace.present()) {
		dbgTrace.get().detail("NKeys", dedupedDomainInfos.size());
		for (const auto& info : dedupedDomainInfos) {
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
			cachedCipherDetails.emplace_back(info.first,
			                                 itr->second.baseCipherId,
			                                 itr->second.baseCipherKey,
			                                 arena,
			                                 itr->second.refreshAt,
			                                 itr->second.expireAt);

			if (dbgTrace.present()) {
				// {encryptDomainId, baseCipherId} forms a unique tuple across encryption domains
				dbgTrace.get().detail(getEncryptDbgTraceKeyWithTS(ENCRYPT_DBG_TRACE_CACHED_PREFIX,
				                                                  info.first,
				                                                  info.second.domainName,
				                                                  itr->second.baseCipherId,
				                                                  itr->second.refreshAt,
				                                                  itr->second.expireAt),
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
				if (item.second.domainId == FDB_DEFAULT_ENCRYPT_DOMAIN_ID) {
					ASSERT(item.second.domainName == FDB_DEFAULT_ENCRYPT_DOMAIN_NAME);
				} else if (item.second.domainId == SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID) {
					ASSERT(item.second.domainName == FDB_SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_NAME);
				}
				keysByDomainIdReq.encryptDomainInfos.emplace_back_deep(
				    keysByDomainIdReq.arena, item.second.domainId, item.second.domainName);
			}
			keysByDomainIdReq.debugId = latestKeysReq.debugId;

			state double startTime = now();
			KmsConnLookupEKsByDomainIdsRep keysByDomainIdRep =
			    wait(kmsConnectorInf.ekLookupByDomainIds.getReply(keysByDomainIdReq));
			ekpProxyData->kmsLookupByDomainIdsReqLatency.addMeasurement(now() - startTime);

			for (auto& item : keysByDomainIdRep.cipherKeyDetails) {
				CipherKeyValidityTS validityTS = getCipherKeyValidityTS(item.refreshAfterSec, item.expireAfterSec);

				latestCipherReply.baseCipherDetails.emplace_back(item.encryptDomainId,
				                                                 item.encryptKeyId,
				                                                 item.encryptKey,
				                                                 arena,
				                                                 validityTS.refreshAtTS,
				                                                 validityTS.expAtTS);

				// Record the fetched cipher details to the local cache for the future references
				const auto itr = lookupCipherDomains.find(item.encryptDomainId);
				if (itr == lookupCipherDomains.end()) {
					TraceEvent(SevError, "GetLatestCipherKeysDomainIdNotFound", ekpProxyData->myId)
					    .detail("DomainId", item.encryptDomainId);
					throw encrypt_keys_fetch_failed();
				}
				ekpProxyData->insertIntoBaseDomainIdCache(item.encryptDomainId,
				                                          itr->second.domainName,
				                                          item.encryptKeyId,
				                                          item.encryptKey,
				                                          validityTS.refreshAtTS,
				                                          validityTS.expAtTS);

				if (dbgTrace.present()) {
					// {encryptDomainId, baseCipherId} forms a unique tuple across encryption domains
					dbgTrace.get().detail(getEncryptDbgTraceKeyWithTS(ENCRYPT_DBG_TRACE_INSERT_PREFIX,
					                                                  item.encryptDomainId,
					                                                  itr->second.domainName,
					                                                  item.encryptKeyId,
					                                                  validityTS.refreshAtTS,
					                                                  validityTS.expAtTS),
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

	CODE_PROBE(!lookupCipherDomains.empty(), "EKP fetch latest cipherKeys from KMS");

	return Void();
}

bool isCipherKeyEligibleForRefresh(const EncryptBaseCipherKey& cipherKey, int64_t currTS) {
	// Candidate eligible for refresh iff either is true:
	// 1. CipherKey cell is either expired/needs-refresh right now.
	// 2. CipherKey cell 'will' be expired/needs-refresh before next refresh cycle interval (proactive refresh)
	if (BUGGIFY_WITH_PROB(0.01)) {
		return true;
	}
	int64_t nextRefreshCycleTS = currTS + FLOW_KNOBS->ENCRYPT_KEY_REFRESH_INTERVAL;
	return nextRefreshCycleTS > cipherKey.expireAt || nextRefreshCycleTS > cipherKey.refreshAt;
}

bool isBlobMetadataEligibleForRefresh(const BlobMetadataDetailsRef& blobMetadata, int64_t currTS) {
	if (BUGGIFY_WITH_PROB(0.01)) {
		return true;
	}
	int64_t nextRefreshCycleTS = currTS + CLIENT_KNOBS->BLOB_METADATA_REFRESH_INTERVAL;
	return nextRefreshCycleTS > blobMetadata.expireAt || nextRefreshCycleTS > blobMetadata.refreshAt;
}

ACTOR Future<Void> refreshEncryptionKeysImpl(Reference<EncryptKeyProxyData> ekpProxyData,
                                             KmsConnectorInterface kmsConnectorInf) {
	state UID debugId = deterministicRandom()->randomUniqueID();

	state TraceEvent t("RefreshEKsStart", ekpProxyData->myId);
	t.setMaxEventLength(SERVER_KNOBS->ENCRYPT_PROXY_MAX_DBG_TRACE_LENGTH);
	t.detail("KmsConnInf", kmsConnectorInf.id());
	t.detail("DebugId", debugId);

	try {
		KmsConnLookupEKsByDomainIdsReq req;
		req.debugId = debugId;
		// req.encryptDomainInfos.reserve(req.arena, ekpProxyData->baseCipherDomainIdCache.size());

		int64_t currTS = (int64_t)now();
		for (auto itr = ekpProxyData->baseCipherDomainIdCache.begin();
		     itr != ekpProxyData->baseCipherDomainIdCache.end();) {
			if (isCipherKeyEligibleForRefresh(itr->second, currTS)) {
				TraceEvent("RefreshEKs").detail("Id", itr->first);
				req.encryptDomainInfos.emplace_back_deep(req.arena, itr->first, itr->second.domainName);
			}

			// Garbage collect expired cached CipherKeys
			if (itr->second.isExpired()) {
				itr = ekpProxyData->baseCipherDomainIdCache.erase(itr);
			} else {
				itr++;
			}
		}

		state double startTime = now();
		KmsConnLookupEKsByDomainIdsRep rep = wait(kmsConnectorInf.ekLookupByDomainIds.getReply(req));
		ekpProxyData->kmsLookupByDomainIdsReqLatency.addMeasurement(now() - startTime);
		for (const auto& item : rep.cipherKeyDetails) {
			const auto itr = ekpProxyData->baseCipherDomainIdCache.find(item.encryptDomainId);
			if (itr == ekpProxyData->baseCipherDomainIdCache.end()) {
				TraceEvent(SevInfo, "RefreshEKsDomainIdNotFound", ekpProxyData->myId)
				    .detail("DomainId", item.encryptDomainId);
				// Continue updating the cache with other elements
				continue;
			}

			CipherKeyValidityTS validityTS = getCipherKeyValidityTS(item.refreshAfterSec, item.expireAfterSec);
			ekpProxyData->insertIntoBaseDomainIdCache(item.encryptDomainId,
			                                          itr->second.domainName,
			                                          item.encryptKeyId,
			                                          item.encryptKey,
			                                          validityTS.refreshAtTS,
			                                          validityTS.expAtTS);
			// {encryptDomainId, baseCipherId} forms a unique tuple across encryption domains
			t.detail(getEncryptDbgTraceKeyWithTS(ENCRYPT_DBG_TRACE_INSERT_PREFIX,
			                                     item.encryptDomainId,
			                                     itr->second.domainName,
			                                     item.encryptKeyId,
			                                     validityTS.refreshAtTS,
			                                     validityTS.expAtTS),
			         "");
		}

		ekpProxyData->baseCipherKeysRefreshed += rep.cipherKeyDetails.size();

		t.detail("NumKeys", rep.cipherKeyDetails.size());
		CODE_PROBE(!rep.cipherKeyDetails.empty(), "EKP refresh cipherKeys");
	} catch (Error& e) {
		if (!canReplyWith(e)) {
			TraceEvent(SevWarn, "RefreshEKsError").error(e);
			throw e;
		}
		TraceEvent("RefreshEKs").detail("ErrorCode", e.code());
		++ekpProxyData->numEncryptionKeyRefreshErrors;
	}

	return Void();
}

Future<Void> refreshEncryptionKeys(Reference<EncryptKeyProxyData> ekpProxyData, KmsConnectorInterface kmsConnectorInf) {
	return refreshEncryptionKeysImpl(ekpProxyData, kmsConnectorInf);
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
	std::unordered_map<BlobMetadataDomainId, BlobMetadataDomainName> dedupedDomainInfos;
	for (auto info : req.domainInfos) {
		dedupedDomainInfos.insert({ info.domainId, info.domainName });
	}

	if (dbgTrace.present()) {
		dbgTrace.get().detail("NKeys", dedupedDomainInfos.size());
		for (auto& info : dedupedDomainInfos) {
			// log domainids queried
			dbgTrace.get().detail("BMQ" + std::to_string(info.first), "");
		}
	}

	// First, check if the requested information is already cached by the server.
	// Ensure the cached information is within SERVER_KNOBS->BLOB_METADATA_CACHE_TTL time window.
	state KmsConnBlobMetadataReq kmsReq;
	kmsReq.debugId = req.debugId;

	for (auto& info : dedupedDomainInfos) {
		const auto itr = ekpProxyData->blobMetadataDomainIdCache.find(info.first);
		if (itr != ekpProxyData->blobMetadataDomainIdCache.end() && itr->second.isValid() &&
		    now() <= itr->second.metadataDetails.expireAt) {
			metadataDetails.arena().dependsOn(itr->second.metadataDetails.arena());
			metadataDetails.push_back(metadataDetails.arena(), itr->second.metadataDetails);

			if (dbgTrace.present()) {
				dbgTrace.get().detail("BMC" + std::to_string(info.first), "");
			}
		} else {
			kmsReq.domainInfos.emplace_back(kmsReq.domainInfos.arena(), info.first, info.second);
		}
	}

	ekpProxyData->blobMetadataCacheHits += metadataDetails.size();

	if (!kmsReq.domainInfos.empty()) {
		ekpProxyData->blobMetadataCacheMisses += kmsReq.domainInfos.size();
		try {
			state double startTime = now();
			KmsConnBlobMetadataRep kmsRep = wait(kmsConnectorInf.blobMetadataReq.getReply(kmsReq));
			ekpProxyData->kmsBlobMetadataReqLatency.addMeasurement(now() - startTime);
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
	state double startTime;

	state TraceEvent t("RefreshBlobMetadataStart", ekpProxyData->myId);
	t.setMaxEventLength(SERVER_KNOBS->ENCRYPT_PROXY_MAX_DBG_TRACE_LENGTH);
	t.detail("KmsConnInf", kmsConnectorInf.id());
	t.detail("DebugId", debugId);

	try {
		KmsConnBlobMetadataReq req;
		req.debugId = debugId;

		int64_t currTS = (int64_t)now();
		for (auto itr = ekpProxyData->blobMetadataDomainIdCache.begin();
		     itr != ekpProxyData->blobMetadataDomainIdCache.end();) {
			if (isBlobMetadataEligibleForRefresh(itr->second.metadataDetails, currTS)) {
				req.domainInfos.emplace_back_deep(
				    req.domainInfos.arena(), itr->first, itr->second.metadataDetails.domainName);
			}

			// Garbage collect expired cached Blob Metadata
			if (itr->second.metadataDetails.expireAt >= currTS) {
				itr = ekpProxyData->blobMetadataDomainIdCache.erase(itr);
			} else {
				itr++;
			}
		}

		if (req.domainInfos.empty()) {
			return Void();
		}

		startTime = now();
		KmsConnBlobMetadataRep rep = wait(kmsConnectorInf.blobMetadataReq.getReply(req));
		ekpProxyData->kmsBlobMetadataReqLatency.addMeasurement(now() - startTime);
		for (auto& item : rep.metadataDetails) {
			ekpProxyData->insertIntoBlobMetadataCache(item.domainId, item);
			t.detail("BM" + std::to_string(item.domainId), "");
		}

		ekpProxyData->blobMetadataRefreshed += rep.metadataDetails.size();

		t.detail("nKeys", rep.metadataDetails.size());
	} catch (Error& e) {
		if (!canReplyWith(e)) {
			TraceEvent("RefreshBlobMetadataError").error(e);
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
		ekpProxyData->kmsConnector = std::make_unique<SimKmsConnector>(FDB_SIM_KMS_CONNECTOR_TYPE_STR);
	} else if (SERVER_KNOBS->KMS_CONNECTOR_TYPE.compare(FDB_PREF_KMS_CONNECTOR_TYPE_STR) == 0) {
		ekpProxyData->kmsConnector = std::make_unique<SimKmsConnector>(FDB_PREF_KMS_CONNECTOR_TYPE_STR);
	} else if (SERVER_KNOBS->KMS_CONNECTOR_TYPE.compare(REST_KMS_CONNECTOR_TYPE_STR) == 0) {
		ekpProxyData->kmsConnector = std::make_unique<RESTKmsConnector>(REST_KMS_CONNECTOR_TYPE_STR);
	} else {
		throw not_implemented();
	}

	TraceEvent("EKPActiveKmsConnector", ekpProxyData->myId)
	    .detail("ConnectorType", ekpProxyData->kmsConnector->getConnectorStr())
	    .detail("InfId", kmsConnectorInf.id());

	ekpProxyData->addActor.send(ekpProxyData->kmsConnector->connectorCore(kmsConnectorInf));
}

ACTOR Future<Void> encryptKeyProxyServer(EncryptKeyProxyInterface ekpInterface, Reference<AsyncVar<ServerDBInfo>> db) {
	state Reference<EncryptKeyProxyData> self = makeReference<EncryptKeyProxyData>(ekpInterface.id());
	state Future<Void> collection = actorCollection(self->addActor.getFuture());
	self->addActor.send(traceRole(Role::ENCRYPT_KEY_PROXY, ekpInterface.id()));

	state KmsConnectorInterface kmsConnectorInf;
	kmsConnectorInf.initEndpoints();

	TraceEvent("EKPStart", self->myId).detail("KmsConnectorInf", kmsConnectorInf.id());

	activateKmsConnector(self, kmsConnectorInf);

	// Register a recurring task to refresh the cached Encryption keys and blob metadata.
	// Approach avoids external RPCs due to EncryptionKey refreshes for the inline write encryption codepath such as:
	// CPs, Redwood Storage Server node flush etc. The process doing the encryption refresh the cached cipher keys based
	// on FLOW_KNOB->ENCRYPTION_CIPHER_KEY_CACHE_TTL_SEC interval which is intentionally kept longer than
	// FLOW_KNOB->ENCRRYPTION_KEY_REFRESH_INTERVAL_SEC, allowing the interactions with external Encryption Key Manager
	// mostly not co-inciding with FDB process encryption key refresh attempts.

	self->encryptionKeyRefresher = recurringAsync([&]() { return refreshEncryptionKeys(self, kmsConnectorInf); },
	                                              FLOW_KNOBS->ENCRYPT_KEY_REFRESH_INTERVAL, /* interval */
	                                              true, /* absoluteIntervalDelay */
	                                              FLOW_KNOBS->ENCRYPT_KEY_REFRESH_INTERVAL, /* initialDelay */
	                                              TaskPriority::Worker);

	self->blobMetadataRefresher = recurring([&]() { refreshBlobMetadata(self, kmsConnectorInf); },
	                                        CLIENT_KNOBS->BLOB_METADATA_REFRESH_INTERVAL,
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
				TraceEvent("EKPHalted", self->myId).detail("ReqID", req.requesterID);
				req.reply.send(Void());
				break;
			}
			when(wait(collection)) {
				ASSERT(false);
				throw internal_error();
			}
		}
	} catch (Error& e) {
		TraceEvent("EKPTerminated", self->myId).errorUnsuppressed(e);
	}

	return Void();
}
