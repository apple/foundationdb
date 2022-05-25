/*
 * GetCipherKeys.actor.cpp
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

#include "fdbserver/GetCipherKeys.h"

#include <boost/functional/hash.hpp>

#include "fdbserver/Knobs.h"

Optional<UID> getEncryptKeyProxyId(const Reference<AsyncVar<ServerDBInfo> const>& db) {
	return db->get().encryptKeyProxy.map<UID>([](EncryptKeyProxyInterface proxy) { return proxy.id(); });
}

ACTOR Future<Void> onEncryptKeyProxyChange(Reference<AsyncVar<ServerDBInfo> const> db) {
	state Optional<UID> previousProxyId = getEncryptKeyProxyId(db);
	state Optional<UID> currentProxyId;
	loop {
		wait(db->onChange());
		currentProxyId = getEncryptKeyProxyId(db);
		if (currentProxyId != previousProxyId) {
			break;
		}
	}
	TraceEvent("GetCipherKeys_EncryptKeyProxyChanged")
	    .detail("PreviousProxyId", previousProxyId.orDefault(UID()))
	    .detail("CurrentProxyId", currentProxyId.orDefault(UID()));
	return Void();
}

ACTOR Future<std::vector<EKPBaseCipherDetails>> getUncachedLatestCipherKeys(
    Reference<AsyncVar<ServerDBInfo> const> db,
    std::vector<EncryptCipherDomainId> uncachedIds) {
	Optional<EncryptKeyProxyInterface> proxy = db->get().encryptKeyProxy;
	if (!proxy.present()) {
		// Wait for onEncryptKeyProxyChange.
		TraceEvent("GetLatestCipherKeys_EncryptKeyProxyNotPresent");
		return Never();
	}
	EKPGetLatestBaseCipherKeysRequest req;
	req.encryptDomainIds = uncachedIds;
	try {
		EKPGetLatestBaseCipherKeysReply reply = wait(proxy.get().getLatestBaseCipherKeys.getReply(req));
		if (reply.error.present()) {
			TraceEvent("GetLatestCipherKeys_RequestFailed").error(reply.error.get());
			throw reply.error.get();
		}
		return reply.baseCipherDetails;
	} catch (Error& e) {
		TraceEvent("GetLatestCipherKeys_CaughtError").error(e);
		if (e.code() == error_code_broken_promise) {
			// Wait for onEncryptKeyProxyChange.
			return Never();
		}
		throw;
	}
}

ACTOR Future<std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>>> getLatestCipherKeys(
    Reference<AsyncVar<ServerDBInfo> const> db,
    std::unordered_set<EncryptCipherDomainId> domainIds) {
	state Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();
	state std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> cipherKeys;
	state std::vector<EncryptCipherDomainId> uncachedIds;

	if (!db.isValid()) {
		TraceEvent(SevError, "GetLatestCipherKeys_ServerDBInfoNotAvailable");
		throw encrypt_ops_error();
	}

	// Collect cached cipher keys.
	for (EncryptCipherDomainId domainId : domainIds) {
		Reference<BlobCipherKey> cachedCipherKey = cipherKeyCache->getLatestCipherKey(domainId);
		if (cachedCipherKey.isValid()) {
			cipherKeys[domainId] = cachedCipherKey;
		} else {
			uncachedIds.push_back(domainId);
		}
	}
	if (uncachedIds.empty()) {
		return cipherKeys;
	}
	// Fetch any uncached cipher keys.
	loop choose {
		when(std::vector<EKPBaseCipherDetails> baseCipherDetails = wait(getUncachedLatestCipherKeys(db, uncachedIds))) {
			// Insert base cipher keys into cache and construct result.
			for (const EKPBaseCipherDetails& details : baseCipherDetails) {
				EncryptCipherDomainId domainId = details.encryptDomainId;
				if (domainIds.count(domainId) > 0 && cipherKeys.count(domainId) == 0) {
					Reference<BlobCipherKey> cipherKey = cipherKeyCache->insertCipherKey(
					    domainId, details.baseCipherId, details.baseCipherKey.begin(), details.baseCipherKey.size());
					ASSERT(cipherKey.isValid());
					cipherKeys[domainId] = cipherKey;
				}
			}
			// Check for any missing cipher keys.
			for (EncryptCipherDomainId domainId : uncachedIds) {
				if (cipherKeys.count(domainId) == 0) {
					TraceEvent(SevWarn, "GetLatestCipherKeys_KeyMissing").detail("DomainId", domainId);
					throw encrypt_key_not_found();
				}
			}
			break;
		}
		// In case encryptKeyProxy has changed, retry the request.
		when(wait(onEncryptKeyProxyChange(db))) {}
	}
	return cipherKeys;
}

using BaseCipherIndex = std::pair<EncryptCipherBaseKeyId, EncryptCipherDomainId>;

ACTOR Future<std::vector<EKPBaseCipherDetails>> getUncachedCipherKeys(
    Reference<AsyncVar<ServerDBInfo> const> db,
    std::unordered_set<BaseCipherIndex, boost::hash<BaseCipherIndex>> uncachedIds) {
	Optional<EncryptKeyProxyInterface> proxy = db->get().encryptKeyProxy;
	if (!proxy.present()) {
		// Wait for onEncryptKeyProxyChange.
		TraceEvent("GetCipherKeys_EncryptKeyProxyNotPresent");
		return Never();
	}
	EKPGetBaseCipherKeysByIdsRequest req;
	req.baseCipherIds.assign(uncachedIds.begin(), uncachedIds.end());
	try {
		EKPGetBaseCipherKeysByIdsReply reply = wait(proxy.get().getBaseCipherKeysByIds.getReply(req));
		if (reply.error.present()) {
			TraceEvent("GetCipherKeys_RequestFailed").error(reply.error.get());
			throw reply.error.get();
		}
		return reply.baseCipherDetails;
	} catch (Error& e) {
		TraceEvent("GetCipherKeys_CaughtError").error(e);
		if (e.code() == error_code_broken_promise) {
			// Wait for onEncryptKeyProxyChange.
			return Never();
		}
		throw;
	}
}

ACTOR Future<std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>>> getCipherKeys(
    Reference<AsyncVar<ServerDBInfo> const> db,
    std::unordered_set<BlobCipherDetails> cipherDetails) {
	state Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();
	state std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>> cipherKeys;
	state std::unordered_set<BaseCipherIndex, boost::hash<BaseCipherIndex>> uncachedIds;

	if (!db.isValid()) {
		TraceEvent(SevError, "GetCipherKeys_ServerDBInfoNotAvailable");
		throw encrypt_ops_error();
	}

	// Collect cached cipher keys.
	for (const BlobCipherDetails& details : cipherDetails) {
		Reference<BlobCipherKey> cachedCipherKey =
		    cipherKeyCache->getCipherKey(details.encryptDomainId, details.baseCipherId, details.salt);
		if (cachedCipherKey.isValid()) {
			cipherKeys.emplace(details, cachedCipherKey);
		} else {
			uncachedIds.insert(std::make_pair(details.baseCipherId, details.encryptDomainId));
		}
	}
	if (uncachedIds.empty()) {
		return cipherKeys;
	}
	// Fetch any uncached cipher keys.
	loop choose {
		when(std::vector<EKPBaseCipherDetails> baseCipherDetails = wait(getUncachedCipherKeys(db, uncachedIds))) {
			std::unordered_map<BaseCipherIndex, StringRef, boost::hash<BaseCipherIndex>> baseCipherKeys;
			for (const EKPBaseCipherDetails& baseDetails : baseCipherDetails) {
				BaseCipherIndex baseIdx = std::make_pair(baseDetails.baseCipherId, baseDetails.encryptDomainId);
				baseCipherKeys[baseIdx] = baseDetails.baseCipherKey;
			}
			// Insert base cipher keys into cache and construct result.
			for (const BlobCipherDetails& details : cipherDetails) {
				if (cipherKeys.count(details) > 0) {
					continue;
				}
				BaseCipherIndex baseIdx = std::make_pair(details.baseCipherId, details.encryptDomainId);
				const auto& itr = baseCipherKeys.find(baseIdx);
				if (itr == baseCipherKeys.end()) {
					TraceEvent(SevError, "GetCipherKeys_KeyMissing")
					    .detail("DomainId", details.encryptDomainId)
					    .detail("BaseCipherId", details.baseCipherId);
					throw encrypt_key_not_found();
				}
				Reference<BlobCipherKey> cipherKey = cipherKeyCache->insertCipherKey(details.encryptDomainId,
				                                                                     details.baseCipherId,
				                                                                     itr->second.begin(),
				                                                                     itr->second.size(),
				                                                                     details.salt);
				ASSERT(cipherKey.isValid());
				cipherKeys[details] = cipherKey;
			}
			break;
		}
		// In case encryptKeyProxy has changed, retry the request.
		when(wait(onEncryptKeyProxyChange(db))) {}
	}
	return cipherKeys;
}

ACTOR Future<TextAndHeaderCipherKeys> getLatestSystemCipherKeys(Reference<AsyncVar<ServerDBInfo> const> db) {
	std::unordered_set<EncryptCipherDomainId> domainIds = { SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID,
		                                                    ENCRYPT_HEADER_DOMAIN_ID };
	std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> cipherKeys =
	    wait(getLatestCipherKeys(db, domainIds));
	ASSERT(cipherKeys.count(SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID) > 0);
	ASSERT(cipherKeys.count(ENCRYPT_HEADER_DOMAIN_ID) > 0);
	TextAndHeaderCipherKeys result{ cipherKeys.at(SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID),
		                            cipherKeys.at(ENCRYPT_HEADER_DOMAIN_ID) };
	ASSERT(result.cipherTextKey.isValid());
	ASSERT(result.cipherHeaderKey.isValid());
	return result;
}

ACTOR Future<TextAndHeaderCipherKeys> getCipherKeys(Reference<AsyncVar<ServerDBInfo> const> db,
                                                    BlobCipherEncryptHeader header) {
	std::unordered_set<BlobCipherDetails> cipherDetails{ header.cipherTextDetails, header.cipherHeaderDetails };
	std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>> cipherKeys = wait(getCipherKeys(db, cipherDetails));
	ASSERT(cipherKeys.count(header.cipherTextDetails) > 0);
	ASSERT(cipherKeys.count(header.cipherHeaderDetails) > 0);
	TextAndHeaderCipherKeys result{ cipherKeys.at(header.cipherTextDetails),
		                            cipherKeys.at(header.cipherHeaderDetails) };
	ASSERT(result.cipherTextKey.isValid());
	ASSERT(result.cipherHeaderKey.isValid());
	return result;
}
