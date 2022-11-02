/*
 * GetEncryptCipherKeys.actor.h
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
#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_GETCIPHERKEYS_ACTOR_G_H)
#define FDBCLIENT_GETCIPHERKEYS_ACTOR_G_H
#include "fdbclient/GetEncryptCipherKeys.actor.g.h"
#elif !defined(FDBCLIENT_GETCIPHERKEYS_ACTOR_H)
#define FDBCLIENT_GETCIPHERKEYS_ACTOR_H

#include "fdbclient/BlobCipher.h"
#include "fdbclient/EncryptKeyProxyInterface.h"
#include "fdbrpc/Stats.h"
#include "flow/Knobs.h"
#include "flow/IRandom.h"

#include <unordered_map>
#include <unordered_set>

#include "flow/actorcompiler.h" // This must be the last #include.

template <class T>
Optional<UID> getEncryptKeyProxyId(const Reference<AsyncVar<T> const>& db) {
	return db->get().encryptKeyProxy.template map<UID>([](EncryptKeyProxyInterface proxy) { return proxy.id(); });
}

ACTOR template <class T>
Future<Void> onEncryptKeyProxyChange(Reference<AsyncVar<T> const> db) {
	state Optional<UID> previousProxyId = getEncryptKeyProxyId(db);
	state Optional<UID> currentProxyId;
	loop {
		wait(db->onChange());
		currentProxyId = getEncryptKeyProxyId(db);
		if (currentProxyId != previousProxyId) {
			break;
		}
	}
	TraceEvent("GetEncryptCipherKeys_EncryptKeyProxyChanged")
	    .detail("PreviousProxyId", previousProxyId.orDefault(UID()))
	    .detail("CurrentProxyId", currentProxyId.orDefault(UID()));
	return Void();
}

ACTOR template <class T>
Future<EKPGetLatestBaseCipherKeysReply> getUncachedLatestEncryptCipherKeys(Reference<AsyncVar<T> const> db,
                                                                           EKPGetLatestBaseCipherKeysRequest request,
                                                                           BlobCipherMetrics::UsageType usageType) {
	Optional<EncryptKeyProxyInterface> proxy = db->get().encryptKeyProxy;
	if (!proxy.present()) {
		// Wait for onEncryptKeyProxyChange.
		TraceEvent("GetLatestEncryptCipherKeys_EncryptKeyProxyNotPresent").detail("UsageType", toString(usageType));
		return Never();
	}
	request.reply.reset();
	try {
		EKPGetLatestBaseCipherKeysReply reply = wait(proxy.get().getLatestBaseCipherKeys.getReply(request));
		if (reply.error.present()) {
			TraceEvent(SevWarn, "GetLatestEncryptCipherKeys_RequestFailed").error(reply.error.get());
			throw encrypt_keys_fetch_failed();
		}
		return reply;
	} catch (Error& e) {
		TraceEvent("GetLatestEncryptCipherKeys_CaughtError").error(e);
		if (e.code() == error_code_broken_promise) {
			// Wait for onEncryptKeyProxyChange.
			return Never();
		}
		throw;
	}
}

// Get latest cipher keys for given encryption domains. It tries to get the cipher keys from local cache.
// In case of cache miss, it fetches the cipher keys from EncryptKeyProxy and put the result in the local cache
// before return.
ACTOR template <class T>
Future<std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>>> getLatestEncryptCipherKeys(
    Reference<AsyncVar<T> const> db,
    std::unordered_map<EncryptCipherDomainId, EncryptCipherDomainName> domains,
    BlobCipherMetrics::UsageType usageType) {
	state Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();
	state std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> cipherKeys;
	state EKPGetLatestBaseCipherKeysRequest request;

	if (!db.isValid()) {
		TraceEvent(SevError, "GetLatestEncryptCipherKeys_ServerDBInfoNotAvailable");
		throw encrypt_ops_error();
	}

	// Collect cached cipher keys.
	for (auto& domain : domains) {
		if (domain.first == FDB_DEFAULT_ENCRYPT_DOMAIN_ID) {
			ASSERT(domain.second == FDB_DEFAULT_ENCRYPT_DOMAIN_NAME);
		} else if (domain.first == SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID) {
			ASSERT(domain.second == FDB_SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_NAME);
		}
		Reference<BlobCipherKey> cachedCipherKey = cipherKeyCache->getLatestCipherKey(domain.first /*domainId*/);
		if (cachedCipherKey.isValid()) {
			cipherKeys[domain.first] = cachedCipherKey;
		} else {
			request.encryptDomainInfos.emplace_back(
			    request.arena, domain.first /*domainId*/, domain.second /*domainName*/);
		}
	}
	if (request.encryptDomainInfos.empty()) {
		return cipherKeys;
	}
	// Fetch any uncached cipher keys.
	state double startTime = now();
	loop choose {
		when(EKPGetLatestBaseCipherKeysReply reply = wait(getUncachedLatestEncryptCipherKeys(db, request, usageType))) {
			// Insert base cipher keys into cache and construct result.
			for (const EKPBaseCipherDetails& details : reply.baseCipherDetails) {
				EncryptCipherDomainId domainId = details.encryptDomainId;
				if (domains.count(domainId) > 0 && cipherKeys.count(domainId) == 0) {
					Reference<BlobCipherKey> cipherKey = cipherKeyCache->insertCipherKey(domainId,
					                                                                     details.baseCipherId,
					                                                                     details.baseCipherKey.begin(),
					                                                                     details.baseCipherKey.size(),
					                                                                     details.refreshAt,
					                                                                     details.expireAt);
					ASSERT(cipherKey.isValid());
					cipherKeys[domainId] = cipherKey;
				}
			}
			// Check for any missing cipher keys.
			for (auto& domain : request.encryptDomainInfos) {
				if (cipherKeys.count(domain.domainId) == 0) {
					TraceEvent(SevWarn, "GetLatestEncryptCipherKeys_KeyMissing").detail("DomainId", domain.domainId);
					throw encrypt_key_not_found();
				}
			}
			break;
		}
		// In case encryptKeyProxy has changed, retry the request.
		when(wait(onEncryptKeyProxyChange(db))) {}
	}
	double elapsed = now() - startTime;
	BlobCipherMetrics::getInstance()->getLatestCipherKeysLatency.addMeasurement(elapsed);
	BlobCipherMetrics::counters(usageType).getLatestCipherKeysLatency.addMeasurement(elapsed);
	return cipherKeys;
}

// Get latest cipher key for given a encryption domain. It tries to get the cipher key from the local cache.
// In case of cache miss, it fetches the cipher key from EncryptKeyProxy and put the result in the local cache
// before return.
ACTOR template <class T>
Future<Reference<BlobCipherKey>> getLatestEncryptCipherKey(Reference<AsyncVar<T> const> db,
                                                           EncryptCipherDomainId domainId,
                                                           EncryptCipherDomainName domainName,
                                                           BlobCipherMetrics::UsageType usageType) {
	std::unordered_map<EncryptCipherDomainId, EncryptCipherDomainName> domains({ { domainId, domainName } });
	std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> cipherKey =
	    wait(getLatestEncryptCipherKeys(db, domains, usageType));

	return cipherKey.at(domainId);
}

ACTOR template <class T>
Future<EKPGetBaseCipherKeysByIdsReply> getUncachedEncryptCipherKeys(Reference<AsyncVar<T> const> db,
                                                                    EKPGetBaseCipherKeysByIdsRequest request,
                                                                    BlobCipherMetrics::UsageType usageType) {
	Optional<EncryptKeyProxyInterface> proxy = db->get().encryptKeyProxy;
	if (!proxy.present()) {
		// Wait for onEncryptKeyProxyChange.
		TraceEvent("GetEncryptCipherKeys_EncryptKeyProxyNotPresent").detail("UsageType", toString(usageType));
		return Never();
	}
	request.reply.reset();
	try {
		EKPGetBaseCipherKeysByIdsReply reply = wait(proxy.get().getBaseCipherKeysByIds.getReply(request));
		if (reply.error.present()) {
			TraceEvent(SevWarn, "GetEncryptCipherKeys_RequestFailed").error(reply.error.get());
			throw encrypt_keys_fetch_failed();
		}
		return reply;
	} catch (Error& e) {
		TraceEvent("GetEncryptCipherKeys_CaughtError").error(e);
		if (e.code() == error_code_broken_promise) {
			// Wait for onEncryptKeyProxyChange.
			return Never();
		}
		throw;
	}
}

using BaseCipherIndex = std::pair<EncryptCipherDomainId, EncryptCipherBaseKeyId>;

// Get cipher keys specified by the list of cipher details. It tries to get the cipher keys from local cache.
// In case of cache miss, it fetches the cipher keys from EncryptKeyProxy and put the result in the local cache
// before return.
ACTOR template <class T>
Future<std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>>> getEncryptCipherKeys(
    Reference<AsyncVar<T> const> db,
    std::unordered_set<BlobCipherDetails> cipherDetails,
    BlobCipherMetrics::UsageType usageType) {
	state Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();
	state std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>> cipherKeys;
	state std::unordered_set<BaseCipherIndex, boost::hash<BaseCipherIndex>> uncachedBaseCipherIds;
	state EKPGetBaseCipherKeysByIdsRequest request;

	if (!db.isValid()) {
		TraceEvent(SevError, "GetEncryptCipherKeys_ServerDBInfoNotAvailable");
		throw encrypt_ops_error();
	}

	// Collect cached cipher keys.
	for (const BlobCipherDetails& details : cipherDetails) {
		Reference<BlobCipherKey> cachedCipherKey =
		    cipherKeyCache->getCipherKey(details.encryptDomainId, details.baseCipherId, details.salt);
		if (cachedCipherKey.isValid()) {
			cipherKeys.emplace(details, cachedCipherKey);
		} else {
			uncachedBaseCipherIds.insert(std::make_pair(details.encryptDomainId, details.baseCipherId));
		}
	}
	if (uncachedBaseCipherIds.empty()) {
		return cipherKeys;
	}
	for (const BaseCipherIndex& id : uncachedBaseCipherIds) {
		request.baseCipherInfos.emplace_back(
		    id.first /*domainId*/, id.second /*baseCipherId*/, StringRef() /*domainName*/, request.arena);
	}
	// Fetch any uncached cipher keys.
	state double startTime = now();
	loop choose {
		when(EKPGetBaseCipherKeysByIdsReply reply = wait(getUncachedEncryptCipherKeys(db, request, usageType))) {
			std::unordered_map<BaseCipherIndex, EKPBaseCipherDetails, boost::hash<BaseCipherIndex>> baseCipherKeys;
			for (const EKPBaseCipherDetails& baseDetails : reply.baseCipherDetails) {
				BaseCipherIndex baseIdx = std::make_pair(baseDetails.encryptDomainId, baseDetails.baseCipherId);
				baseCipherKeys[baseIdx] = baseDetails;
			}
			// Insert base cipher keys into cache and construct result.
			for (const BlobCipherDetails& details : cipherDetails) {
				if (cipherKeys.count(details) > 0) {
					continue;
				}
				BaseCipherIndex baseIdx = std::make_pair(details.encryptDomainId, details.baseCipherId);
				const auto& itr = baseCipherKeys.find(baseIdx);
				if (itr == baseCipherKeys.end()) {
					TraceEvent(SevError, "GetEncryptCipherKeys_KeyMissing")
					    .detail("DomainId", details.encryptDomainId)
					    .detail("BaseCipherId", details.baseCipherId);
					throw encrypt_key_not_found();
				}
				Reference<BlobCipherKey> cipherKey = cipherKeyCache->insertCipherKey(details.encryptDomainId,
				                                                                     details.baseCipherId,
				                                                                     itr->second.baseCipherKey.begin(),
				                                                                     itr->second.baseCipherKey.size(),
				                                                                     details.salt,
				                                                                     itr->second.refreshAt,
				                                                                     itr->second.expireAt);
				ASSERT(cipherKey.isValid());
				cipherKeys[details] = cipherKey;
			}
			break;
		}
		// In case encryptKeyProxy has changed, retry the request.
		when(wait(onEncryptKeyProxyChange(db))) {}
	}
	double elapsed = now() - startTime;
	BlobCipherMetrics::getInstance()->getCipherKeysLatency.addMeasurement(elapsed);
	BlobCipherMetrics::counters(usageType).getCipherKeysLatency.addMeasurement(elapsed);
	return cipherKeys;
}

struct TextAndHeaderCipherKeys {
	Reference<BlobCipherKey> cipherTextKey;
	Reference<BlobCipherKey> cipherHeaderKey;
};

ACTOR template <class T>
Future<TextAndHeaderCipherKeys> getLatestEncryptCipherKeysForDomain(Reference<AsyncVar<T> const> db,
                                                                    EncryptCipherDomainId domainId,
                                                                    EncryptCipherDomainName domainName,
                                                                    BlobCipherMetrics::UsageType usageType) {
	std::unordered_map<EncryptCipherDomainId, EncryptCipherDomainName> domains;
	domains[domainId] = domainName;
	domains[ENCRYPT_HEADER_DOMAIN_ID] = FDB_ENCRYPT_HEADER_DOMAIN_NAME;
	std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> cipherKeys =
	    wait(getLatestEncryptCipherKeys(db, domains, usageType));
	ASSERT(cipherKeys.count(domainId) > 0);
	ASSERT(cipherKeys.count(ENCRYPT_HEADER_DOMAIN_ID) > 0);
	TextAndHeaderCipherKeys result{ cipherKeys.at(domainId), cipherKeys.at(ENCRYPT_HEADER_DOMAIN_ID) };
	ASSERT(result.cipherTextKey.isValid());
	ASSERT(result.cipherHeaderKey.isValid());
	return result;
}

template <class T>
Future<TextAndHeaderCipherKeys> getLatestSystemEncryptCipherKeys(const Reference<AsyncVar<T> const>& db,
                                                                 BlobCipherMetrics::UsageType usageType) {
	return getLatestEncryptCipherKeysForDomain(
	    db, SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID, FDB_SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_NAME, usageType);
}

ACTOR template <class T>
Future<TextAndHeaderCipherKeys> getEncryptCipherKeys(Reference<AsyncVar<T> const> db,
                                                     BlobCipherEncryptHeader header,
                                                     BlobCipherMetrics::UsageType usageType) {
	std::unordered_set<BlobCipherDetails> cipherDetails{ header.cipherTextDetails };
	if (header.hasHeaderCipher()) {
		cipherDetails.emplace(header.cipherHeaderDetails);
	}
	std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>> cipherKeys =
	    wait(getEncryptCipherKeys(db, cipherDetails, usageType));
	TextAndHeaderCipherKeys result;
	ASSERT(cipherKeys.count(header.cipherTextDetails) > 0);
	result.cipherTextKey = cipherKeys.at(header.cipherTextDetails);
	ASSERT(result.cipherTextKey.isValid());
	if (header.hasHeaderCipher()) {
		ASSERT(cipherKeys.count(header.cipherHeaderDetails) > 0);
		result.cipherHeaderKey = cipherKeys.at(header.cipherHeaderDetails);
		ASSERT(result.cipherHeaderKey.isValid());
	}
	return result;
}

#include "flow/unactorcompiler.h"
#endif