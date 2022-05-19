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

#include "fdbrpc/Locality.h"
#include "fdbrpc/Stats.h"
#include "fdbserver/EncryptKeyProxyInterface.h"
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
	EncryptCipherBaseKeyId baseCipherId;
	Standalone<StringRef> baseCipherKey;
	uint64_t creationTimeSec;
	bool noExpiry;

	EncryptBaseCipherKey()
	  : domainId(0), baseCipherId(0), baseCipherKey(StringRef()), creationTimeSec(0), noExpiry(false) {}
	explicit EncryptBaseCipherKey(EncryptCipherDomainId dId,
	                              EncryptCipherBaseKeyId cipherId,
	                              StringRef cipherKey,
	                              bool neverExpire)
	  : domainId(dId), baseCipherId(cipherId), baseCipherKey(cipherKey), creationTimeSec(now()), noExpiry(neverExpire) {
	}

	bool isValid() { return noExpiry ? true : ((now() - creationTimeSec) < FLOW_KNOBS->ENCRYPT_CIPHER_KEY_CACHE_TTL); }
};

using EncryptBaseDomainIdCache = std::unordered_map<EncryptCipherDomainId, EncryptBaseCipherKey>;

using EncryptBaseCipherDomainIdKeyIdCacheKey = std::pair<EncryptCipherDomainId, EncryptCipherBaseKeyId>;
using EncryptBaseCipherDomainIdKeyIdCacheKeyHash = boost::hash<EncryptBaseCipherDomainIdKeyIdCacheKey>;
using EncryptBaseCipherDomainIdKeyIdCache = std::unordered_map<EncryptBaseCipherDomainIdKeyIdCacheKey,
                                                               EncryptBaseCipherKey,
                                                               EncryptBaseCipherDomainIdKeyIdCacheKeyHash>;

struct EncryptKeyProxyData : NonCopyable, ReferenceCounted<EncryptKeyProxyData> {
public:
	UID myId;
	PromiseStream<Future<Void>> addActor;
	Future<Void> encryptionKeyRefresher;

	EncryptBaseDomainIdCache baseCipherDomainIdCache;
	EncryptBaseCipherDomainIdKeyIdCache baseCipherDomainIdKeyIdCache;

	std::unique_ptr<KmsConnector> kmsConnector;

	CounterCollection ekpCacheMetrics;

	Counter baseCipherKeyIdCacheMisses;
	Counter baseCipherKeyIdCacheHits;
	Counter baseCipherDomainIdCacheMisses;
	Counter baseCipherDomainIdCacheHits;
	Counter baseCipherKeysRefreshed;
	Counter numResponseWithErrors;
	Counter numEncryptionKeyRefreshErrors;

	explicit EncryptKeyProxyData(UID id)
	  : myId(id), ekpCacheMetrics("EKPMetrics", myId.toString()),
	    baseCipherKeyIdCacheMisses("EKPCipherIdCacheMisses", ekpCacheMetrics),
	    baseCipherKeyIdCacheHits("EKPCipherIdCacheHits", ekpCacheMetrics),
	    baseCipherDomainIdCacheMisses("EKPCipherDomainIdCacheMisses", ekpCacheMetrics),
	    baseCipherDomainIdCacheHits("EKPCipherDomainIdCacheHits", ekpCacheMetrics),
	    baseCipherKeysRefreshed("EKPCipherKeysRefreshed", ekpCacheMetrics),
	    numResponseWithErrors("EKPNumResponseWithErrors", ekpCacheMetrics),
	    numEncryptionKeyRefreshErrors("EKPNumEncryptionKeyRefreshErrors", ekpCacheMetrics) {}

	EncryptBaseCipherDomainIdKeyIdCacheKey getBaseCipherDomainIdKeyIdCacheKey(
	    const EncryptCipherDomainId domainId,
	    const EncryptCipherBaseKeyId baseCipherId) {
		return std::make_pair(domainId, baseCipherId);
	}

	void insertIntoBaseDomainIdCache(const EncryptCipherDomainId domainId,
	                                 const EncryptCipherBaseKeyId baseCipherId,
	                                 const StringRef baseCipherKey) {
		// Entries in domainId cache are eligible for periodic refreshes to support 'limiting lifetime of encryption
		// key' support if enabled on external KMS solutions.

		baseCipherDomainIdCache[domainId] = EncryptBaseCipherKey(domainId, baseCipherId, baseCipherKey, false);

		// Update cached the information indexed using baseCipherId
		insertIntoBaseCipherIdCache(domainId, baseCipherId, baseCipherKey);
	}

	void insertIntoBaseCipherIdCache(const EncryptCipherDomainId domainId,
	                                 const EncryptCipherBaseKeyId baseCipherId,
	                                 const StringRef baseCipherKey) {
		// Given an cipherKey is immutable, it is OK to NOT expire cached information.
		// TODO: Update cache to support LRU eviction policy to limit the total cache size.

		EncryptBaseCipherDomainIdKeyIdCacheKey cacheKey = getBaseCipherDomainIdKeyIdCacheKey(domainId, baseCipherId);
		baseCipherDomainIdKeyIdCache[cacheKey] = EncryptBaseCipherKey(domainId, baseCipherId, baseCipherKey, true);
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

	std::vector<std::pair<EncryptCipherBaseKeyId, EncryptCipherDomainId>> lookupCipherIds;

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
	std::unordered_set<std::pair<EncryptCipherBaseKeyId, EncryptCipherDomainId>,
	                   boost::hash<std::pair<EncryptCipherBaseKeyId, EncryptCipherDomainId>>>
	    dedupedCipherIds;
	for (const auto& item : req.baseCipherIds) {
		dedupedCipherIds.emplace(item);
	}

	if (dbgTrace.present()) {
		dbgTrace.get().detail("NKeys", dedupedCipherIds.size());
		for (const auto& item : dedupedCipherIds) {
			// Record {encryptDomainId, baseCipherId} queried
			dbgTrace.get().detail(getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_QUERY_PREFIX, item.second, item.first), "");
		}
	}

	for (const auto& item : dedupedCipherIds) {
		const EncryptBaseCipherDomainIdKeyIdCacheKey cacheKey =
		    ekpProxyData->getBaseCipherDomainIdKeyIdCacheKey(item.second, item.first);
		const auto itr = ekpProxyData->baseCipherDomainIdKeyIdCache.find(cacheKey);
		if (itr != ekpProxyData->baseCipherDomainIdKeyIdCache.end()) {
			ASSERT(itr->second.isValid());
			cachedCipherDetails.emplace_back(
			    itr->second.domainId, itr->second.baseCipherId, itr->second.baseCipherKey, keyIdsReply.arena);

			if (dbgTrace.present()) {
				// {encryptId, baseCipherId} forms a unique tuple across encryption domains
				dbgTrace.get().detail(getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_CACHED_PREFIX,
				                                            itr->second.domainId,
				                                            itr->second.baseCipherId),
				                      "");
			}
		} else {
			lookupCipherIds.emplace_back(std::make_pair(item.first, item.second));
		}
	}

	ekpProxyData->baseCipherKeyIdCacheHits += cachedCipherDetails.size();
	ekpProxyData->baseCipherKeyIdCacheMisses += lookupCipherIds.size();

	if (!lookupCipherIds.empty()) {
		try {
			KmsConnLookupEKsByKeyIdsReq keysByIdsReq(lookupCipherIds, keysByIds.debugId);
			KmsConnLookupEKsByKeyIdsRep keysByIdsRep = wait(kmsConnectorInf.ekLookupByIds.getReply(keysByIdsReq));

			for (const auto& item : keysByIdsRep.cipherKeyDetails) {
				keyIdsReply.baseCipherDetails.emplace_back(
				    item.encryptDomainId, item.encryptKeyId, item.encryptKey, keyIdsReply.arena);
			}

			// Record the fetched cipher details to the local cache for the future references
			// Note: cache warm-up is done after reponding to the caller

			for (auto& item : keysByIdsRep.cipherKeyDetails) {
				// DomainId isn't available here, the caller must know the encryption domainId
				ekpProxyData->insertIntoBaseCipherIdCache(item.encryptDomainId, item.encryptKeyId, item.encryptKey);

				if (dbgTrace.present()) {
					// {encryptId, baseCipherId} forms a unique tuple across encryption domains
					dbgTrace.get().detail(
					    getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_INSERT_PREFIX, item.encryptDomainId, item.encryptKeyId),
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
	std::unordered_set<EncryptCipherDomainId> dedupedDomainIds;
	for (EncryptCipherDomainId id : req.encryptDomainIds) {
		dedupedDomainIds.emplace(id);
	}

	if (dbgTrace.present()) {
		dbgTrace.get().detail("NKeys", dedupedDomainIds.size());
		for (EncryptCipherDomainId id : dedupedDomainIds) {
			// log encryptDomainIds queried
			dbgTrace.get().detail(getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_QUERY_PREFIX, id), "");
		}
	}

	// First, check if the requested information is already cached by the server.
	// Ensure the cached information is within FLOW_KNOBS->ENCRYPT_CIPHER_KEY_CACHE_TTL time window.

	std::vector<EncryptCipherDomainId> lookupCipherDomains;
	for (EncryptCipherDomainId id : dedupedDomainIds) {
		const auto itr = ekpProxyData->baseCipherDomainIdCache.find(id);
		if (itr != ekpProxyData->baseCipherDomainIdCache.end() && itr->second.isValid()) {
			cachedCipherDetails.emplace_back(id, itr->second.baseCipherId, itr->second.baseCipherKey, arena);

			if (dbgTrace.present()) {
				// {encryptDomainId, baseCipherId} forms a unique tuple across encryption domains
				dbgTrace.get().detail(
				    getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_CACHED_PREFIX, id, itr->second.baseCipherId), "");
			}
		} else {
			lookupCipherDomains.emplace_back(id);
		}
	}

	ekpProxyData->baseCipherDomainIdCacheHits += cachedCipherDetails.size();
	ekpProxyData->baseCipherDomainIdCacheMisses += lookupCipherDomains.size();

	if (!lookupCipherDomains.empty()) {
		try {
			KmsConnLookupEKsByDomainIdsReq keysByDomainIdReq(lookupCipherDomains, latestKeysReq.debugId);
			KmsConnLookupEKsByDomainIdsRep keysByDomainIdRep =
			    wait(kmsConnectorInf.ekLookupByDomainIds.getReply(keysByDomainIdReq));

			for (auto& item : keysByDomainIdRep.cipherKeyDetails) {
				latestCipherReply.baseCipherDetails.emplace_back(
				    item.encryptDomainId, item.encryptKeyId, item.encryptKey, arena);

				// Record the fetched cipher details to the local cache for the future references
				ekpProxyData->insertIntoBaseDomainIdCache(item.encryptDomainId, item.encryptKeyId, item.encryptKey);

				if (dbgTrace.present()) {
					// {encryptDomainId, baseCipherId} forms a unique tuple across encryption domains
					dbgTrace.get().detail(
					    getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_INSERT_PREFIX, item.encryptDomainId, item.encryptKeyId),
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
		req.encryptDomainIds.reserve(ekpProxyData->baseCipherDomainIdCache.size());

		for (auto& item : ekpProxyData->baseCipherDomainIdCache) {
			req.encryptDomainIds.emplace_back(item.first);
		}
		KmsConnLookupEKsByDomainIdsRep rep = wait(kmsConnectorInf.ekLookupByDomainIds.getReply(req));
		for (auto& item : rep.cipherKeyDetails) {
			ekpProxyData->insertIntoBaseDomainIdCache(item.encryptDomainId, item.encryptKeyId, item.encryptKey);
			// {encryptDomainId, baseCipherId} forms a unique tuple across encryption domains
			t.detail(getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_INSERT_PREFIX, item.encryptDomainId, item.encryptKeyId),
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

	// Register a recurring task to refresh the cached Encryption keys.
	// Approach avoids external RPCs due to EncryptionKey refreshes for the inline write encryption codepath such as:
	// CPs, Redwood Storage Server node flush etc. The process doing the encryption refresh the cached cipher keys based
	// on FLOW_KNOB->ENCRYPTION_CIPHER_KEY_CACHE_TTL_SEC interval which is intentionally kept longer than
	// FLOW_KNOB->ENCRRYPTION_KEY_REFRESH_INTERVAL_SEC, allowing the interactions with external Encryption Key Manager
	// mostly not co-inciding with FDB process encryption key refresh attempts.

	self->encryptionKeyRefresher = recurring([&]() { refreshEncryptionKeys(self, kmsConnectorInf); },
	                                         FLOW_KNOBS->ENCRYPT_KEY_REFRESH_INTERVAL,
	                                         TaskPriority::Worker);

	try {
		loop choose {
			when(EKPGetBaseCipherKeysByIdsRequest req = waitNext(ekpInterface.getBaseCipherKeysByIds.getFuture())) {
				wait(getCipherKeysByBaseCipherKeyIds(self, kmsConnectorInf, req));
			}
			when(EKPGetLatestBaseCipherKeysRequest req = waitNext(ekpInterface.getLatestBaseCipherKeys.getFuture())) {
				wait(getLatestCipherKeys(self, kmsConnectorInf, req));
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
