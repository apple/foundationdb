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
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/SimEncryptKmsProxy.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "flow/Arena.h"
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

#include "flow/actorcompiler.h" // This must be the last #include.

using EncryptDomainId = uint64_t;
using EncryptBaseCipherId = uint64_t;

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
	EncryptDomainId domainId;
	EncryptBaseCipherId baseCipherId;
	Standalone<StringRef> baseCipherKey;
	uint64_t creationTimeSec;
	bool noExpiry;

	EncryptBaseCipherKey()
	  : domainId(0), baseCipherId(0), baseCipherKey(StringRef()), creationTimeSec(0), noExpiry(false) {}
	explicit EncryptBaseCipherKey(EncryptDomainId dId,
	                              EncryptBaseCipherId cipherId,
	                              StringRef cipherKey,
	                              bool neverExpire)
	  : domainId(dId), baseCipherId(cipherId), baseCipherKey(cipherKey), creationTimeSec(now()), noExpiry(neverExpire) {
	}

	bool isValid() { return noExpiry ? true : ((now() - creationTimeSec) < FLOW_KNOBS->ENCRYPT_CIPHER_KEY_CACHE_TTL); }
};

using EncryptBaseDomainIdCache = std::unordered_map<EncryptDomainId, EncryptBaseCipherKey>;
using EncryptBaseCipherKeyIdCache = std::unordered_map<EncryptBaseCipherId, EncryptBaseCipherKey>;

struct EncryptKeyProxyData : NonCopyable, ReferenceCounted<EncryptKeyProxyData> {
public:
	UID myId;
	PromiseStream<Future<Void>> addActor;
	Future<Void> encryptionKeyRefresher;

	EncryptBaseDomainIdCache baseCipherDomainIdCache;
	EncryptBaseCipherKeyIdCache baseCipherKeyIdCache;

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

	void insertIntoBaseDomainIdCache(const EncryptDomainId domainId,
	                                 const EncryptBaseCipherId baseCipherId,
	                                 const StringRef baseCipherKey) {
		// Entries in domainId cache are eligible for periodic refreshes to support 'limiting lifetime of encryption
		// key' support if enabled on external KMS solutions.

		baseCipherDomainIdCache[domainId] = EncryptBaseCipherKey(domainId, baseCipherId, baseCipherKey, false);

		// Update cached the information indexed using baseCipherId
		insertIntoBaseCipherIdCache(domainId, baseCipherId, baseCipherKey);
	}

	void insertIntoBaseCipherIdCache(const EncryptDomainId domainId,
	                                 const EncryptBaseCipherId baseCipherId,
	                                 const StringRef baseCipherKey) {
		// Given an cipherKey is immutable, it is OK to NOT expire cached information.
		// TODO: Update cache to support LRU eviction policy to limit the total cache size.

		baseCipherKeyIdCache[baseCipherId] = EncryptBaseCipherKey(domainId, baseCipherId, baseCipherKey, true);
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
                                                   SimKmsProxyInterface simKmsInterface,
                                                   EKPGetBaseCipherKeysByIdsRequest req) {
	// Scan the cached cipher-keys and filter our baseCipherIds locally cached
	// for the rest, reachout to KMS to fetch the required details

	std::vector<EncryptBaseCipherId> lookupCipherIds;
	state std::unordered_map<EncryptBaseCipherId, Standalone<StringRef>> cachedKeys;

	for (EncryptBaseCipherId id : req.baseCipherIds) {
		const auto itr = ekpProxyData->baseCipherKeyIdCache.find(id);
		if (itr != ekpProxyData->baseCipherKeyIdCache.end()) {
			ASSERT(itr->second.isValid());
			cachedKeys.emplace(id, itr->second.baseCipherKey);
		} else {
			lookupCipherIds.push_back(id);
		}
	}

	ekpProxyData->baseCipherKeyIdCacheHits += cachedKeys.size();
	ekpProxyData->baseCipherKeyIdCacheMisses += lookupCipherIds.size();

	state EKPGetBaseCipherKeysByIdsRequest keysByIds = req;
	state EKPGetBaseCipherKeysByIdsReply keyIdsReply;

	if (g_network->isSimulated()) {
		if (!lookupCipherIds.empty()) {
			try {
				SimGetEncryptKeysByKeyIdsRequest simKeyIdsReq(lookupCipherIds);
				SimGetEncryptKeysByKeyIdsReply simKeyIdsReply =
				    wait(simKmsInterface.encryptKeyLookupByKeyIds.getReply(simKeyIdsReq));

				for (const auto& item : simKeyIdsReply.encryptKeyMap) {
					keyIdsReply.baseCipherMap.emplace(item.first, StringRef(keyIdsReply.arena, item.second));
				}

				// Record the fetched cipher details to the local cache for the future references
				// Note: cache warm-up is done after reponding to the caller

				for (auto& item : simKeyIdsReply.encryptKeyMap) {
					// DomainId isn't available here, the caller must know the encryption domainId
					ekpProxyData->insertIntoBaseCipherIdCache(0, item.first, item.second);
				}
			} catch (Error& e) {
				if (!canReplyWith(e)) {
					TraceEvent("GetCipherKeysByIds", ekpProxyData->myId).error(e);
					throw;
				}
				TraceEvent("GetCipherKeysByIds", ekpProxyData->myId).detail("ErrorCode", e.code());
				ekpProxyData->sendErrorResponse(keysByIds.reply, e);
				return Void();
			}
		}
	} else {
		// TODO: Call to non-FDB KMS connector process.
		throw not_implemented();
	}

	for (auto& item : cachedKeys) {
		keyIdsReply.baseCipherMap.emplace(item.first, item.second);
	}

	keyIdsReply.numHits = cachedKeys.size();
	keysByIds.reply.send(keyIdsReply);

	return Void();
}

ACTOR Future<Void> getLatestCipherKeys(Reference<EncryptKeyProxyData> ekpProxyData,
                                       SimKmsProxyInterface simKmsInterface,
                                       EKPGetLatestBaseCipherKeysRequest req) {
	// Scan the cached cipher-keys and filter our baseCipherIds locally cached
	// for the rest, reachout to KMS to fetch the required details

	state std::unordered_map<EncryptBaseCipherId, EKPBaseCipherDetails> cachedKeys;
	state EKPGetLatestBaseCipherKeysRequest latestKeysReq = req;
	state EKPGetLatestBaseCipherKeysReply latestCipherReply;
	state Arena& arena = latestCipherReply.arena;

	// First, check if the requested information is already cached by the server.
	// Ensure the cached information is within FLOW_KNOBS->ENCRYPT_CIPHER_KEY_CACHE_TTL time window.

	std::vector<EncryptBaseCipherId> lookupCipherDomains;
	for (EncryptDomainId id : req.encryptDomainIds) {
		const auto itr = ekpProxyData->baseCipherDomainIdCache.find(id);
		if (itr != ekpProxyData->baseCipherDomainIdCache.end() && itr->second.isValid()) {
			cachedKeys.emplace(id, EKPBaseCipherDetails(itr->second.baseCipherId, itr->second.baseCipherKey, arena));
		} else {
			lookupCipherDomains.push_back(id);
		}
	}

	ekpProxyData->baseCipherDomainIdCacheHits += cachedKeys.size();
	ekpProxyData->baseCipherDomainIdCacheMisses += lookupCipherDomains.size();

	if (g_network->isSimulated()) {
		if (!lookupCipherDomains.empty()) {
			try {
				SimGetEncryptKeysByDomainIdsRequest simKeysByDomainIdReq(lookupCipherDomains);
				SimGetEncryptKeyByDomainIdReply simKeysByDomainIdRep =
				    wait(simKmsInterface.encryptKeyLookupByDomainId.getReply(simKeysByDomainIdReq));

				for (auto& item : simKeysByDomainIdRep.encryptKeyMap) {
					latestCipherReply.baseCipherDetailMap.emplace(
					    item.first, EKPBaseCipherDetails(item.second.encryptKeyId, item.second.encryptKey, arena));

					// Record the fetched cipher details to the local cache for the future references
					ekpProxyData->insertIntoBaseDomainIdCache(
					    item.first, item.second.encryptKeyId, item.second.encryptKey);
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
	} else {
		// TODO: Call to non-FDB KMS connector process.
		throw not_implemented();
	}

	for (auto& item : cachedKeys) {
		latestCipherReply.baseCipherDetailMap.emplace(
		    item.first, EKPBaseCipherDetails(item.second.baseCipherId, item.second.baseCipherKey, arena));
	}

	latestCipherReply.numHits = cachedKeys.size();
	latestKeysReq.reply.send(latestCipherReply);

	return Void();
}

ACTOR Future<Void> refreshEncryptionKeysUsingSimKms(Reference<EncryptKeyProxyData> ekpProxyData,
                                                    SimKmsProxyInterface simKmsInterface) {

	ASSERT(g_network->isSimulated());

	TraceEvent("RefreshEKs_Start", ekpProxyData->myId).detail("Inf", simKmsInterface.id());

	try {
		SimGetEncryptKeysByDomainIdsRequest req;
		req.encryptDomainIds.reserve(ekpProxyData->baseCipherDomainIdCache.size());

		for (auto& item : ekpProxyData->baseCipherDomainIdCache) {
			req.encryptDomainIds.emplace_back(item.first);
		}
		SimGetEncryptKeyByDomainIdReply rep = wait(simKmsInterface.encryptKeyLookupByDomainId.getReply(req));
		for (auto& item : rep.encryptKeyMap) {
			ekpProxyData->insertIntoBaseDomainIdCache(item.first, item.second.encryptKeyId, item.second.encryptKey);
		}

		ekpProxyData->baseCipherKeysRefreshed += rep.encryptKeyMap.size();
		TraceEvent("RefreshEKs_Done", ekpProxyData->myId).detail("KeyCount", rep.encryptKeyMap.size());
	} catch (Error& e) {
		if (!canReplyWith(e)) {
			TraceEvent("RefreshEncryptionKeys_Error").error(e);
			throw e;
		}
		TraceEvent("RefreshEncryptionKeys").detail("ErrorCode", e.code());
		++ekpProxyData->numEncryptionKeyRefreshErrors;
	}

	return Void();
}

ACTOR Future<Void> refreshEncryptionKeysUsingKms(Reference<EncryptKeyProxyData> ekpProxyData) {
	wait(delay(0)); // compiler needs to be happy
	throw not_implemented();
}

void refreshEncryptionKeys(Reference<EncryptKeyProxyData> ekpProxyData, SimKmsProxyInterface simKmsInterface) {

	Future<Void> ignored;
	if (g_network->isSimulated()) {
		ignored = refreshEncryptionKeysUsingSimKms(ekpProxyData, simKmsInterface);
	} else {
		ignored = refreshEncryptionKeysUsingKms(ekpProxyData);
	}
}

ACTOR Future<Void> encryptKeyProxyServer(EncryptKeyProxyInterface ekpInterface, Reference<AsyncVar<ServerDBInfo>> db) {
	state Reference<EncryptKeyProxyData> self(new EncryptKeyProxyData(ekpInterface.id()));
	state PromiseStream<Future<Void>> addActor;
	state Future<Void> collection = actorCollection(self->addActor.getFuture());
	self->addActor.send(traceRole(Role::ENCRYPT_KEY_PROXY, ekpInterface.id()));

	state SimKmsProxyInterface simKmsProxyInf;

	TraceEvent("EKP_Start", self->myId).log();

	// Register a recurring task to refresh the cached Encryption keys.
	// Approach avoids external RPCs due to EncryptionKey refreshes for the inline write encryption codepath such as:
	// CPs, Redwood Storage Server node flush etc. The process doing the encryption refresh the cached cipher keys based
	// on FLOW_KNOB->ENCRYPTION_CIPHER_KEY_CACHE_TTL_SEC interval which is intentionally kept longer than
	// FLOW_KNOB->ENCRRYPTION_KEY_REFRESH_INTERVAL_SEC, allowing the interactions with external Encryption Key Manager
	// mostly not co-inciding with FDB process encryption key refresh attempts.

	if (g_network->isSimulated()) {
		// In simulation construct an Encryption KMSProxy actor to satisfy encryption keys lookups otherwise satisfied
		// by integrating external Encryption Key Management solutions.

		simKmsProxyInf.initEndpoints();
		self->addActor.send(simEncryptKmsProxyCore(simKmsProxyInf));

		TraceEvent("EKP_InitSimKmsInf", self->myId).detail("Inf", simKmsProxyInf.id());

		self->encryptionKeyRefresher = recurring([&]() { refreshEncryptionKeys(self, simKmsProxyInf); },
		                                         FLOW_KNOBS->ENCRYPT_KEY_REFRESH_INTERVAL,
		                                         TaskPriority::Worker);

	} else {
		// TODO: Add recurring actor to talk to external KMS proxy process
		throw not_implemented();
	}

	try {
		loop choose {
			when(EKPGetBaseCipherKeysByIdsRequest req = waitNext(ekpInterface.getBaseCipherKeysByIds.getFuture())) {
				wait(getCipherKeysByBaseCipherKeyIds(self, simKmsProxyInf, req));
			}
			when(EKPGetLatestBaseCipherKeysRequest req = waitNext(ekpInterface.getLatestBaseCipherKeys.getFuture())) {
				wait(getLatestCipherKeys(self, simKmsProxyInf, req));
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
