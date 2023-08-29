/*
 * GetEncryptCipherKeys_impl.actor.h
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
#include "flow/EncryptUtils.h"
#include "flow/genericactors.actor.h"
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_GETCIPHERKEYS_IMPL_ACTOR_G_H)
#define FDBCLIENT_GETCIPHERKEYS_IMPL_ACTOR_G_H
#include "fdbclient/GetEncryptCipherKeys_impl.actor.g.h"
#elif !defined(FDBCLIENT_GETCIPHERKEYS_IMPL_ACTOR_H)
#define FDBCLIENT_GETCIPHERKEYS_IMPL_ACTOR_H

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/GlobalConfig.actor.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tuple.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

#define DEBUG_GET_CIPHER false

namespace {
ACTOR template <class T>
Future<T> monitorGetEncryptCipherKeys(GetEncryptCipherKeysMonitor* self, Future<T> actor) {
	Future<Void> timer = delay(CLIENT_KNOBS->ENCRYPT_GET_CIPHER_KEY_LONG_REQUEST_THRESHOLD, TaskPriority::DefaultDelay);
	choose {
		when(T t = wait(actor)) {
			return t;
		}
		when(wait(timer)) {
			// Bookkeeping number of long running actors using RAII and set degraded state accordingly.
			state ActiveCounter<int>::Releaser r = self->handleLongRunningRequest();
			T t = wait(actor);
			return t;
		}
	}
}
} // anonymous namespace

template <class T>
Future<T> GetEncryptCipherKeysMonitor::monitor(Future<T> actor) {
	return monitorGetEncryptCipherKeys(this, actor);
}

template <class T>
Optional<EncryptKeyProxyInterface> _getEncryptKeyProxyInterface(const Reference<AsyncVar<T> const>& db) {
	if constexpr (std::is_same_v<T, ClientDBInfo>) {
		return db->get().encryptKeyProxy;
	} else {
		return db->get().client.encryptKeyProxy;
	}
}

template <class T>
Optional<UID> getEncryptKeyProxyId(const Reference<AsyncVar<T> const>& db) {
	return _getEncryptKeyProxyInterface(db).map(&EncryptKeyProxyInterface::id);
}

ACTOR template <class T>
Future<Void> _onEncryptKeyProxyChange(Reference<AsyncVar<T> const> db) {
	state Optional<UID> previousProxyId = getEncryptKeyProxyId(db);
	state Optional<UID> currentProxyId;
	loop {
		wait(db->onChange());
		currentProxyId = getEncryptKeyProxyId(db);
		if (currentProxyId != previousProxyId) {
			break;
		}
	}

	if (DEBUG_GET_CIPHER) {
		TraceEvent(SevDebug, "GetEncryptCipherKeysEncryptKeyProxyChanged")
		    .detail("PreviousProxyId", previousProxyId.orDefault(UID()))
		    .detail("CurrentProxyId", currentProxyId.orDefault(UID()));
	}

	return Void();
}

ACTOR template <class T>
Future<EKPGetLatestBaseCipherKeysReply> _getUncachedLatestEncryptCipherKeys(Reference<AsyncVar<T> const> db,
                                                                            EKPGetLatestBaseCipherKeysRequest request,
                                                                            BlobCipherMetrics::UsageType usageType) {
	Optional<EncryptKeyProxyInterface> proxy = _getEncryptKeyProxyInterface(db);
	if (!proxy.present()) {
		// Wait for onEncryptKeyProxyChange.
		if (DEBUG_GET_CIPHER) {
			TraceEvent(SevDebug, "GetLatestEncryptCipherKeysEncryptKeyProxyNotPresent")
			    .detail("UsageType", toString(usageType));
		}
		return Never();
	}
	request.reply.reset();
	try {
		EKPGetLatestBaseCipherKeysReply reply = wait(proxy.get().getLatestBaseCipherKeys.getReply(request));
		if (reply.error.present()) {
			TraceEvent(SevWarn, "GetLatestEncryptCipherKeysRequestFailed").error(reply.error.get());
			throw reply.error.get();
		}
		return reply;
	} catch (Error& e) {
		TraceEvent("GetLatestEncryptCipherKeysCaughtError").error(e);
		if (e.code() == error_code_broken_promise) {
			// Wait for onEncryptKeyProxyChange.
			return Never();
		}
		throw;
	}
}

ACTOR template <class T>
Future<std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>>> _getLatestEncryptCipherKeysImpl(
    Reference<AsyncVar<T> const> db,
    std::unordered_set<EncryptCipherDomainId> domainIds,
    BlobCipherMetrics::UsageType usageType) {
	state Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();
	state std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> cipherKeys;
	state EKPGetLatestBaseCipherKeysRequest request;

	if (!db.isValid()) {
		TraceEvent(SevError, "GetLatestEncryptCipherKeysServerDBInfoNotAvailable");
		throw encrypt_ops_error();
	}

	// Collect cached cipher keys.
	for (auto& domainId : domainIds) {
		Reference<BlobCipherKey> cachedCipherKey = cipherKeyCache->getLatestCipherKey(domainId);
		if (cachedCipherKey.isValid()) {
			cipherKeys[domainId] = cachedCipherKey;
		} else {
			request.encryptDomainIds.emplace_back(domainId);
		}
	}
	if (request.encryptDomainIds.empty()) {
		return cipherKeys;
	}
	// Fetch any uncached cipher keys.
	state double startTime = now();
	loop choose {
		when(EKPGetLatestBaseCipherKeysReply reply =
		         wait(_getUncachedLatestEncryptCipherKeys(db, request, usageType))) {
			// Insert base cipher keys into cache and construct result.
			for (const EKPBaseCipherDetails& details : reply.baseCipherDetails) {
				EncryptCipherDomainId domainId = details.encryptDomainId;
				if (domainIds.count(domainId) > 0 && cipherKeys.count(domainId) == 0) {
					Reference<BlobCipherKey> cipherKey = cipherKeyCache->insertCipherKey(domainId,
					                                                                     details.baseCipherId,
					                                                                     details.baseCipherKey.begin(),
					                                                                     details.baseCipherKey.size(),
					                                                                     details.baseCipherKCV,
					                                                                     details.refreshAt,
					                                                                     details.expireAt);
					ASSERT(cipherKey.isValid());
					cipherKeys[domainId] = cipherKey;
				}
			}
			// Check for any missing cipher keys.
			for (auto domainId : request.encryptDomainIds) {
				if (cipherKeys.count(domainId) == 0) {
					TraceEvent(SevWarn, "GetLatestEncryptCipherKeysKeyMissing").detail("DomainId", domainId);
					throw encrypt_key_not_found();
				}
			}
			break;
		}
		// In case encryptKeyProxy has changed, retry the request.
		when(wait(_onEncryptKeyProxyChange(db))) {}
	}
	double elapsed = now() - startTime;
	BlobCipherMetrics::getInstance()->getLatestCipherKeysLatency.addMeasurement(elapsed);
	return cipherKeys;
}

template <class T>
Future<std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>>> _getLatestEncryptCipherKeys(
    Reference<AsyncVar<T> const> db,
    std::unordered_set<EncryptCipherDomainId> domainIds,
    BlobCipherMetrics::UsageType usageType,
    Reference<GetEncryptCipherKeysMonitor> monitor) {
	Future<std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>>> actor =
	    _getLatestEncryptCipherKeysImpl(db, domainIds, usageType);
	if (monitor.isValid()) {
		actor = monitor->monitor(actor);
	}
	return actor;
}

ACTOR template <class T>
Future<Reference<BlobCipherKey>> _getLatestEncryptCipherKey(Reference<AsyncVar<T> const> db,
                                                            EncryptCipherDomainId domainId,
                                                            BlobCipherMetrics::UsageType usageType,
                                                            Reference<GetEncryptCipherKeysMonitor> monitor) {
	std::unordered_set<EncryptCipherDomainId> domainIds{ domainId };
	std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> cipherKey =
	    wait(_getLatestEncryptCipherKeys(db, domainIds, usageType, monitor));

	return cipherKey.at(domainId);
}

ACTOR template <class T>
Future<EKPGetBaseCipherKeysByIdsReply> _getUncachedEncryptCipherKeys(Reference<AsyncVar<T> const> db,
                                                                     EKPGetBaseCipherKeysByIdsRequest request,
                                                                     BlobCipherMetrics::UsageType usageType) {
	Optional<EncryptKeyProxyInterface> proxy = _getEncryptKeyProxyInterface(db);
	if (!proxy.present()) {
		// Wait for onEncryptKeyProxyChange.
		if (DEBUG_GET_CIPHER) {
			TraceEvent(SevDebug, "GetEncryptCipherKeysEncryptKeyProxyNotPresent")
			    .detail("UsageType", toString(usageType));
		}
		return Never();
	}
	request.reply.reset();
	try {
		EKPGetBaseCipherKeysByIdsReply reply = wait(proxy.get().getBaseCipherKeysByIds.getReply(request));
		if (reply.error.present()) {
			TraceEvent(SevWarn, "GetEncryptCipherKeysRequestFailed").error(reply.error.get());
			throw reply.error.get();
		}
		// The code below is used only during simulation to test backup/restore ability to handle encryption keys
		// not being found for deleted tenants
		if (g_network && g_network->isSimulated() && usageType == BlobCipherMetrics::RESTORE) {
			std::unordered_set<int64_t> tenantIdsToDrop =
			    parseStringToUnorderedSet<int64_t>(CLIENT_KNOBS->SIMULATION_EKP_TENANT_IDS_TO_DROP, ',');
			if (!tenantIdsToDrop.count(TenantInfo::INVALID_TENANT)) {
				for (auto& baseCipherInfo : request.baseCipherInfos) {
					if (tenantIdsToDrop.count(baseCipherInfo.domainId)) {
						TraceEvent("GetEncryptCipherKeysSimulatedError").detail("DomainId", baseCipherInfo.domainId);
						if (deterministicRandom()->coinflip()) {
							throw encrypt_keys_fetch_failed();
						} else {
							throw encrypt_key_not_found();
						}
					}
				}
			}
		}
		return reply;
	} catch (Error& e) {
		TraceEvent("GetEncryptCipherKeysCaughtError").error(e);
		if (e.code() == error_code_broken_promise) {
			// Wait for onEncryptKeyProxyChange.
			return Never();
		}
		throw;
	}
}

// Get cipher keys specified by the list of cipher details. It tries to get the cipher keys from local cache.
// In case of cache miss, it fetches the cipher keys from EncryptKeyProxy and put the result in the local cache
// before return.
ACTOR template <class T>
Future<std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>>> _getEncryptCipherKeysImpl(
    Reference<AsyncVar<T> const> db,
    std::unordered_set<BlobCipherDetails> cipherDetails,
    BlobCipherMetrics::UsageType usageType) {
	state Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();
	state std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>> cipherKeys;
	state std::unordered_set<BaseCipherIndex, boost::hash<BaseCipherIndex>> uncachedBaseCipherIds;
	state EKPGetBaseCipherKeysByIdsRequest request;

	if (!db.isValid()) {
		TraceEvent(SevError, "GetEncryptCipherKeysServerDBInfoNotAvailable");
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
		request.baseCipherInfos.emplace_back(id.first /*domainId*/, id.second /*baseCipherId*/);
	}
	// Fetch any uncached cipher keys.
	state double startTime = now();
	loop choose {
		when(EKPGetBaseCipherKeysByIdsReply reply = wait(_getUncachedEncryptCipherKeys(db, request, usageType))) {
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
					TraceEvent(SevError, "GetEncryptCipherKeysKeyMissing")
					    .detail("DomainId", details.encryptDomainId)
					    .detail("BaseCipherId", details.baseCipherId);
					throw encrypt_key_not_found();
				}
				Reference<BlobCipherKey> cipherKey = cipherKeyCache->insertCipherKey(details.encryptDomainId,
				                                                                     details.baseCipherId,
				                                                                     itr->second.baseCipherKey.begin(),
				                                                                     itr->second.baseCipherKey.size(),
				                                                                     itr->second.baseCipherKCV,
				                                                                     details.salt,
				                                                                     itr->second.refreshAt,
				                                                                     itr->second.expireAt);
				ASSERT(cipherKey.isValid());
				cipherKeys[details] = cipherKey;
			}
			break;
		}
		// In case encryptKeyProxy has changed, retry the request.
		when(wait(_onEncryptKeyProxyChange(db))) {}
	}
	double elapsed = now() - startTime;
	BlobCipherMetrics::getInstance()->getCipherKeysLatency.addMeasurement(elapsed);
	return cipherKeys;
}

template <class T>
Future<std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>>> _getEncryptCipherKeys(
    Reference<AsyncVar<T> const> db,
    std::unordered_set<BlobCipherDetails> cipherDetails,
    BlobCipherMetrics::UsageType usageType,
    Reference<GetEncryptCipherKeysMonitor> monitor) {
	Future<std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>>> actor =
	    _getEncryptCipherKeysImpl(db, cipherDetails, usageType);
	if (monitor.isValid()) {
		actor = monitor->monitor(actor);
	}
	return actor;
}

ACTOR template <class T>
Future<TextAndHeaderCipherKeys> _getLatestEncryptCipherKeysForDomain(Reference<AsyncVar<T> const> db,
                                                                     EncryptCipherDomainId domainId,
                                                                     BlobCipherMetrics::UsageType usageType,
                                                                     Reference<GetEncryptCipherKeysMonitor> monitor) {
	// TODO: Do not fetch header cipher key if authentication is diabled.
	std::unordered_set<EncryptCipherDomainId> domainIds = { domainId, ENCRYPT_HEADER_DOMAIN_ID };
	std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> cipherKeys =
	    wait(_getLatestEncryptCipherKeys(db, domainIds, usageType, monitor));
	ASSERT(cipherKeys.count(domainId) > 0);
	ASSERT(cipherKeys.count(ENCRYPT_HEADER_DOMAIN_ID) > 0);
	TextAndHeaderCipherKeys result{ cipherKeys.at(domainId), cipherKeys.at(ENCRYPT_HEADER_DOMAIN_ID) };
	ASSERT(result.cipherTextKey.isValid());
	ASSERT(result.cipherHeaderKey.isValid());
	return result;
}

template <class T>
Future<TextAndHeaderCipherKeys> _getLatestSystemEncryptCipherKeys(const Reference<AsyncVar<T> const>& db,
                                                                  BlobCipherMetrics::UsageType usageType,
                                                                  Reference<GetEncryptCipherKeysMonitor> monitor) {
	return _getLatestEncryptCipherKeysForDomain(db, SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID, usageType, monitor);
}

ACTOR template <class T>
Future<TextAndHeaderCipherKeys> _getEncryptCipherKeys(Reference<AsyncVar<T> const> db,
                                                      BlobCipherEncryptHeaderRef header,
                                                      BlobCipherMetrics::UsageType usageType,
                                                      Reference<GetEncryptCipherKeysMonitor> monitor) {
	state bool authenticatedEncryption = header.getAuthTokenMode() != ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE;
	state EncryptHeaderCipherDetails details = header.getCipherDetails();

	ASSERT(details.textCipherDetails.isValid());
	ASSERT(!authenticatedEncryption ||
	       (details.headerCipherDetails.present() && details.headerCipherDetails.get().isValid()));

	std::unordered_set<BlobCipherDetails> cipherDetails{ details.textCipherDetails };
	if (authenticatedEncryption) {
		cipherDetails.insert(details.headerCipherDetails.get());
	}

	std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>> cipherKeys =
	    wait(_getEncryptCipherKeys(db, cipherDetails, usageType, monitor));
	TextAndHeaderCipherKeys result;

	auto setCipherKey = [&](const BlobCipherDetails& details, TextAndHeaderCipherKeys& result) {
		ASSERT(details.isValid());
		auto iter = cipherKeys.find(details);
		ASSERT(iter != cipherKeys.end() && iter->second.isValid());
		isEncryptHeaderDomain(details.encryptDomainId) ? result.cipherHeaderKey = iter->second
		                                               : result.cipherTextKey = iter->second;
	};
	setCipherKey(details.textCipherDetails, result);
	if (authenticatedEncryption) {
		setCipherKey(details.headerCipherDetails.get(), result);
	}
	ASSERT(result.cipherTextKey.isValid() && (!authenticatedEncryption || result.cipherHeaderKey.isValid()));

	return result;
}

template <class T>
Future<std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>>>
GetEncryptCipherKeys<T>::getLatestEncryptCipherKeys(Reference<AsyncVar<T> const> db,
                                                    std::unordered_set<EncryptCipherDomainId> domainIds,
                                                    BlobCipherMetrics::UsageType usageType,
                                                    Reference<GetEncryptCipherKeysMonitor> monitor) {
	return _getLatestEncryptCipherKeys(db, domainIds, usageType, monitor);
}

template <class T>
Future<Reference<BlobCipherKey>> GetEncryptCipherKeys<T>::getLatestEncryptCipherKey(
    Reference<AsyncVar<T> const> db,
    EncryptCipherDomainId domainId,
    BlobCipherMetrics::UsageType usageType,
    Reference<GetEncryptCipherKeysMonitor> monitor) {
	return _getLatestEncryptCipherKey(db, domainId, usageType, monitor);
}

template <class T>
Future<std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>>> GetEncryptCipherKeys<T>::getEncryptCipherKeys(
    Reference<AsyncVar<T> const> db,
    std::unordered_set<BlobCipherDetails> cipherDetails,
    BlobCipherMetrics::UsageType usageType,
    Reference<GetEncryptCipherKeysMonitor> monitor) {
	return _getEncryptCipherKeys(db, cipherDetails, usageType, monitor);
}

template <class T>
Future<TextAndHeaderCipherKeys> GetEncryptCipherKeys<T>::getLatestEncryptCipherKeysForDomain(
    Reference<AsyncVar<T> const> db,
    EncryptCipherDomainId domainId,
    BlobCipherMetrics::UsageType usageType,
    Reference<GetEncryptCipherKeysMonitor> monitor) {
	return _getLatestEncryptCipherKeysForDomain(db, domainId, usageType, monitor);
}

template <class T>
Future<TextAndHeaderCipherKeys> GetEncryptCipherKeys<T>::getLatestSystemEncryptCipherKeys(
    const Reference<AsyncVar<T> const>& db,
    BlobCipherMetrics::UsageType usageType,
    Reference<GetEncryptCipherKeysMonitor> monitor) {
	return _getLatestSystemEncryptCipherKeys(db, usageType, monitor);
}

template <class T>
Future<TextAndHeaderCipherKeys> GetEncryptCipherKeys<T>::getEncryptCipherKeys(
    Reference<AsyncVar<T> const> db,
    BlobCipherEncryptHeaderRef header,
    BlobCipherMetrics::UsageType usageType,
    Reference<GetEncryptCipherKeysMonitor> monitor) {
	return _getEncryptCipherKeys(db, header, usageType, monitor);
}

#include "flow/unactorcompiler.h"
#endif