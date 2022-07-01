/*
 * GetEncryptCipherKeys.actor.cpp
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

#include "fdbserver/GetEncryptCipherKeys.h"

#include <boost/functional/hash.hpp>

namespace {

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
	TraceEvent("GetEncryptCipherKeys_EncryptKeyProxyChanged")
	    .detail("PreviousProxyId", previousProxyId.orDefault(UID()))
	    .detail("CurrentProxyId", currentProxyId.orDefault(UID()));
	return Void();
}

ACTOR Future<EKPGetLatestBaseCipherKeysReply> getUncachedLatestEncryptCipherKeys(
    Reference<AsyncVar<ServerDBInfo> const> db,
    EKPGetLatestBaseCipherKeysRequest request) {
	Optional<EncryptKeyProxyInterface> proxy = db->get().encryptKeyProxy;
	if (!proxy.present()) {
		// Wait for onEncryptKeyProxyChange.
		TraceEvent("GetLatestEncryptCipherKeys_EncryptKeyProxyNotPresent");
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

} // anonymous namespace

ACTOR Future<std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>>> getLatestEncryptCipherKeys(
    Reference<AsyncVar<ServerDBInfo> const> db,
    std::unordered_map<EncryptCipherDomainId, EncryptCipherDomainName> domains) {
	state Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();
	state std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> cipherKeys;
	state EKPGetLatestBaseCipherKeysRequest request;

	if (!db.isValid()) {
		TraceEvent(SevError, "GetLatestEncryptCipherKeys_ServerDBInfoNotAvailable");
		throw encrypt_ops_error();
	}

	// Collect cached cipher keys.
	for (auto& domain : domains) {
		Reference<BlobCipherKey> cachedCipherKey = cipherKeyCache->getLatestCipherKey(domain.first /*domainId*/);
		if (cachedCipherKey.isValid()) {
			cipherKeys[domain.first] = cachedCipherKey;
		} else {
			request.encryptDomainInfos.emplace_back(
			    domain.first /*domainId*/, domain.second /*domainName*/, request.arena);
		}
	}
	if (request.encryptDomainInfos.empty()) {
		return cipherKeys;
	}
	// Fetch any uncached cipher keys.
	loop choose {
		when(EKPGetLatestBaseCipherKeysReply reply = wait(getUncachedLatestEncryptCipherKeys(db, request))) {
			// Insert base cipher keys into cache and construct result.
			for (const EKPBaseCipherDetails& details : reply.baseCipherDetails) {
				EncryptCipherDomainId domainId = details.encryptDomainId;
				if (domains.count(domainId) > 0 && cipherKeys.count(domainId) == 0) {
					Reference<BlobCipherKey> cipherKey = cipherKeyCache->insertCipherKey(
					    domainId, details.baseCipherId, details.baseCipherKey.begin(), details.baseCipherKey.size());
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
	return cipherKeys;
}

namespace {

ACTOR Future<EKPGetBaseCipherKeysByIdsReply> getUncachedEncryptCipherKeys(Reference<AsyncVar<ServerDBInfo> const> db,
                                                                          EKPGetBaseCipherKeysByIdsRequest request) {
	Optional<EncryptKeyProxyInterface> proxy = db->get().encryptKeyProxy;
	if (!proxy.present()) {
		// Wait for onEncryptKeyProxyChange.
		TraceEvent("GetEncryptCipherKeys_EncryptKeyProxyNotPresent");
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

} // anonymous namespace

ACTOR Future<std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>>> getEncryptCipherKeys(
    Reference<AsyncVar<ServerDBInfo> const> db,
    std::unordered_set<BlobCipherDetails> cipherDetails) {
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
	loop choose {
		when(EKPGetBaseCipherKeysByIdsReply reply = wait(getUncachedEncryptCipherKeys(db, request))) {
			std::unordered_map<BaseCipherIndex, StringRef, boost::hash<BaseCipherIndex>> baseCipherKeys;
			for (const EKPBaseCipherDetails& baseDetails : reply.baseCipherDetails) {
				BaseCipherIndex baseIdx = std::make_pair(baseDetails.encryptDomainId, baseDetails.baseCipherId);
				baseCipherKeys[baseIdx] = baseDetails.baseCipherKey;
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

ACTOR Future<TextAndHeaderCipherKeys> getLatestSystemEncryptCipherKeys(Reference<AsyncVar<ServerDBInfo> const> db) {
	static std::unordered_map<EncryptCipherDomainId, EncryptCipherDomainName> domains = {
		{ SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID, FDB_DEFAULT_ENCRYPT_DOMAIN_NAME },
		{ ENCRYPT_HEADER_DOMAIN_ID, FDB_DEFAULT_ENCRYPT_DOMAIN_NAME }
	};
	std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> cipherKeys =
	    wait(getLatestEncryptCipherKeys(db, domains));
	ASSERT(cipherKeys.count(SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID) > 0);
	ASSERT(cipherKeys.count(ENCRYPT_HEADER_DOMAIN_ID) > 0);
	TextAndHeaderCipherKeys result{ cipherKeys.at(SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID),
		                            cipherKeys.at(ENCRYPT_HEADER_DOMAIN_ID) };
	ASSERT(result.cipherTextKey.isValid());
	ASSERT(result.cipherHeaderKey.isValid());
	return result;
}

ACTOR Future<TextAndHeaderCipherKeys> getEncryptCipherKeys(Reference<AsyncVar<ServerDBInfo> const> db,
                                                           BlobCipherEncryptHeader header) {
	std::unordered_set<BlobCipherDetails> cipherDetails{ header.cipherTextDetails, header.cipherHeaderDetails };
	std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>> cipherKeys =
	    wait(getEncryptCipherKeys(db, cipherDetails));
	ASSERT(cipherKeys.count(header.cipherTextDetails) > 0);
	ASSERT(cipherKeys.count(header.cipherHeaderDetails) > 0);
	TextAndHeaderCipherKeys result{ cipherKeys.at(header.cipherTextDetails),
		                            cipherKeys.at(header.cipherHeaderDetails) };
	ASSERT(result.cipherTextKey.isValid());
	ASSERT(result.cipherHeaderKey.isValid());
	return result;
}
