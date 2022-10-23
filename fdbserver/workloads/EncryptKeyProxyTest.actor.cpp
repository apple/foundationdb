/*
 * EncryptKeyProxyTest.actor.cpp
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

#include "fdbclient/BlobCipher.h"
#include "fdbclient/EncryptKeyProxyInterface.h"
#include "fdbclient/GetEncryptCipherKeys.actor.h"

#include "fdbrpc/Locality.h"

#include "fdbserver/Knobs.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/Arena.h"
#include "flow/EncryptUtils.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/Trace.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/xxhash.h"

#include <atomic>
#include <boost/range/const_iterator.hpp>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "flow/actorcompiler.h" // This must be the last #include.

struct EncryptKeyProxyTestWorkload : TestWorkload {
	static constexpr auto NAME = "EncryptKeyProxyTest";
	Reference<AsyncVar<struct ServerDBInfo> const> dbInfo;
	Arena arena;
	uint64_t minDomainId;
	uint64_t maxDomainId;
	using CacheKey = std::pair<int64_t, uint64_t>;
	std::unordered_map<CacheKey, StringRef, boost::hash<CacheKey>> cipherIdMap;
	std::vector<CacheKey> cipherIds;
	int numDomains;
	std::vector<EKPGetLatestCipherKeysRequestInfo> domainInfos;
	static std::atomic<int> seed;
	bool enableTest;

	EncryptKeyProxyTestWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), dbInfo(wcx.dbInfo), enableTest(false) {
		if (wcx.clientId == 0) {
			enableTest = true;
			minDomainId = 1000 + (++seed * 30) + 1;
			maxDomainId = deterministicRandom()->randomInt(minDomainId, minDomainId + 50) + 5;
			TraceEvent("EKPTestInit").detail("MinDomainId", minDomainId).detail("MaxDomainId", maxDomainId);
		}
	}

	Future<Void> setup(Database const& ctx) override { return Void(); }

	ACTOR Future<Void> simEmptyDomainIdCache(EncryptKeyProxyTestWorkload* self) {
		TraceEvent("SimEmptyDomainIdCacheStart").log();

		state std::unordered_map<EncryptCipherDomainId, EncryptCipherDomainName> domains;
		for (int i = 0; i < self->numDomains / 2; i++) {
			const EncryptCipherDomainId domainId = self->minDomainId + i;
			domains.emplace(domainId, StringRef(std::to_string(domainId)));
		}
		std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> latestCiphers =
		    wait(getLatestEncryptCipherKeys(self->dbInfo, domains, BlobCipherMetrics::UsageType::TEST));

		ASSERT_EQ(latestCiphers.size(), domains.size());

		TraceEvent("SimEmptyDomainIdCacheDone").log();
		return Void();
	}

	ACTOR Future<Void> simPartialDomainIdCache(EncryptKeyProxyTestWorkload* self) {
		TraceEvent("SimPartialDomainIdCacheStart");

		// Construct a lookup set such that few ciphers are cached as well as few ciphers can never to cached (invalid
		// keys)
		state int expectedHits = deterministicRandom()->randomInt(1, self->numDomains / 2);
		std::unordered_map<EncryptCipherDomainId, EncryptCipherDomainName> domains;
		for (int i = 0; i < expectedHits; i++) {
			const EncryptCipherDomainId domainId = self->minDomainId + i;
			domains.emplace(domainId, StringRef(std::to_string(domainId)));
		}

		state int expectedMisses = deterministicRandom()->randomInt(1, self->numDomains / 2);
		for (int i = 0; i < expectedMisses; i++) {
			const EncryptCipherDomainId domainId = self->minDomainId + i + self->numDomains / 2 + 1;
			domains.emplace(domainId, StringRef(std::to_string(domainId)));
		}
		std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> latestCiphers =
		    wait(getLatestEncryptCipherKeys(self->dbInfo, domains, BlobCipherMetrics::UsageType::TEST));

		TraceEvent("SimPartialDomainIdCacheEnd");
		return Void();
	}

	ACTOR Future<Void> simRandomBaseCipherIdCache(EncryptKeyProxyTestWorkload* self) {
		TraceEvent("SimRandomDomainIdCacheStart");

		// Ensure BlobCipherCache is populated
		std::unordered_map<EncryptCipherDomainId, EncryptCipherDomainName> domains;
		for (int i = 0; i < self->numDomains; i++) {
			const EncryptCipherDomainId domainId = self->minDomainId + i;
			domains[domainId] = StringRef(std::to_string(domainId));
		}

		std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> latestCiphers =
		    wait(getLatestEncryptCipherKeys(self->dbInfo, domains, BlobCipherMetrics::UsageType::TEST));
		state std::vector<Reference<BlobCipherKey>> cipherKeysVec;
		for (auto item : latestCiphers) {
			cipherKeysVec.push_back(item.second);
		}

		state int numIterations = deterministicRandom()->randomInt(512, 786);
		for (; numIterations > 0;) {
			// Randomly select baseCipherIds to be lookedup in the cache
			int idx = deterministicRandom()->randomInt(1, cipherKeysVec.size());
			int nIds = deterministicRandom()->randomInt(1, cipherKeysVec.size());
			std::unordered_set<BlobCipherDetails> cipherDetails;
			for (int count = 0; count < nIds && idx < cipherKeysVec.size(); count++, idx++) {
				cipherDetails.emplace(cipherKeysVec[idx]->getDomainId(),
				                      cipherKeysVec[idx]->getBaseCipherId(),
				                      cipherKeysVec[idx]->getSalt());
			}
			ASSERT_LE(cipherDetails.size(), cipherKeysVec.size());
			TraceEvent("SimRandomDomainIdCacheStart").detail("Count", cipherDetails.size());
			if (cipherDetails.empty()) {
				// No keys to query; continue
				continue;
			} else {
				numIterations--;
			}

			std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>> cipherKeys =
			    wait(getEncryptCipherKeys(self->dbInfo, cipherDetails, BlobCipherMetrics::UsageType::TEST));
			// Ensure the sanity of the lookedup data
			for (auto item : cipherKeys) {
				bool found = false;
				for (auto key : cipherKeysVec) {
					if (key->isEqual(item.second)) {
						found = true;
						break;
					}
				}
				ASSERT(found);
			}
		}

		TraceEvent("SimRandomDomainIdCacheDone");
		return Void();
	}

	ACTOR Future<Void> simLookupInvalidKeyId(EncryptKeyProxyTestWorkload* self) {
		TraceEvent("SimLookupInvalidKeyIdStart").log();

		Arena arena;
		try {
			Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();
			// Prepare a lookup with valid and invalid keyIds - SimEncryptKmsProxy should throw
			// encrypt_key_not_found()
			std::unordered_map<EncryptCipherDomainId, EncryptCipherDomainName> domains;
			for (auto item : self->cipherIds) {
				domains[item.second] = StringRef(std::to_string(item.first));
				// Ensure the key is not 'cached'
				cipherKeyCache->resetEncryptDomainId(item.second);
			}
			domains[FDB_DEFAULT_ENCRYPT_DOMAIN_ID - 1] = StringRef(std::to_string(1));
			std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> res =
			    wait(getLatestEncryptCipherKeys(self->dbInfo, domains, BlobCipherMetrics::UsageType::TEST));
			// BlobCipherKeyCache is 'empty'; fetching invalid cipher from KMS must through 'encrypt_key_not_found'
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_encrypt_keys_fetch_failed);
		}

		TraceEvent("SimLookupInvalidKeyIdDone");
		return Void();
	}

	// Following test cases are covered:
	// 1. Simulate an empty domainIdCache.
	// 2. Simulate an mixed lookup (partial cache-hit) for domainIdCache.
	// 3. Simulate a lookup on all domainIdCache keys and validate lookup by baseCipherKeyIds.
	// 4. Simulate lookup for an invalid baseCipherKeyId.

	ACTOR Future<Void> testWorkload(Reference<AsyncVar<ServerDBInfo> const> dbInfo, EncryptKeyProxyTestWorkload* self) {
		// Ensure EncryptKeyProxy role is recruited (a singleton role)
		self->numDomains = self->maxDomainId - self->minDomainId;

		while (!self->dbInfo->get().encryptKeyProxy.present()) {
			wait(self->dbInfo->onChange());
		}

		// Simulate empty cache access
		wait(self->simEmptyDomainIdCache(self));

		// Simulate partial cache-hit usecase
		wait(self->simPartialDomainIdCache(self));

		// Warm up cached with all domain Ids and randomly access known baseCipherIds
		wait(self->simRandomBaseCipherIdCache(self));

		// Simulate lookup BaseCipherIds which aren't yet cached
		wait(self->simLookupInvalidKeyId(self));

		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (!enableTest) {
			return Void();
		}
		return testWorkload(dbInfo, this);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

std::atomic<int> EncryptKeyProxyTestWorkload::seed = 0;

WorkloadFactory<EncryptKeyProxyTestWorkload> EncryptKeyProxyTestWorkloadFactory;
