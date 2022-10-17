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

#include "fdbrpc/Locality.h"
#include "fdbclient/EncryptKeyProxyInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/EncryptUtils.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/Trace.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/xxhash.h"

#include <atomic>
#include <boost/range/const_iterator.hpp>
#include <utility>

#include "flow/actorcompiler.h" // This must be the last #include.

struct EncryptKeyProxyTestWorkload : TestWorkload {
	static constexpr auto NAME = "EncryptKeyProxyTest";
	EncryptKeyProxyInterface ekpInf;
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
		TraceEvent("SimEmptyDomainIdCache_Start").log();

		for (int i = 0; i < self->numDomains / 2; i++) {
			const EncryptCipherDomainId domainId = self->minDomainId + i;
			self->domainInfos.emplace_back(
			    EKPGetLatestCipherKeysRequestInfo(self->arena, domainId, StringRef(std::to_string(domainId))));
		}

		state int nAttempts = 0;
		loop {
			EKPGetLatestBaseCipherKeysRequest req;
			req.encryptDomainInfos = self->domainInfos;
			// if (deterministicRandom()->randomInt(0, 100) < 50) {
			req.debugId = deterministicRandom()->randomUniqueID();
			//}
			ErrorOr<EKPGetLatestBaseCipherKeysReply> rep = wait(self->ekpInf.getLatestBaseCipherKeys.tryGetReply(req));
			if (rep.present()) {

				ASSERT(!rep.get().error.present());
				ASSERT_EQ(rep.get().baseCipherDetails.size(), self->domainInfos.size());

				for (const auto& info : self->domainInfos) {
					bool found = false;
					for (const auto& item : rep.get().baseCipherDetails) {
						if (item.encryptDomainId == info.domainId) {
							found = true;
							break;
						}
					}
					ASSERT(found);
				}

				// Ensure no hits reported by the cache.
				if (nAttempts == 0) {
					ASSERT_EQ(rep.get().numHits, 0);
				} else {
					ASSERT_GE(rep.get().numHits, 0);
				}
				break;
			} else {
				nAttempts++;
				wait(delay(0.0));
			}
		}

		TraceEvent("SimEmptyDomainIdCacheDone").log();
		return Void();
	}

	ACTOR Future<Void> simPartialDomainIdCache(EncryptKeyProxyTestWorkload* self) {
		state int expectedHits;
		state int expectedMisses;

		TraceEvent("SimPartialDomainIdCacheStart").log();

		self->domainInfos.clear();

		expectedHits = deterministicRandom()->randomInt(1, self->numDomains / 2);
		for (int i = 0; i < expectedHits; i++) {
			const EncryptCipherDomainId domainId = self->minDomainId + i;
			self->domainInfos.emplace_back(
			    EKPGetLatestCipherKeysRequestInfo(self->arena, domainId, StringRef(std::to_string(domainId))));
		}

		expectedMisses = deterministicRandom()->randomInt(1, self->numDomains / 2);
		for (int i = 0; i < expectedMisses; i++) {
			const EncryptCipherDomainId domainId = self->minDomainId + i + self->numDomains / 2 + 1;
			self->domainInfos.emplace_back(
			    EKPGetLatestCipherKeysRequestInfo(self->arena, domainId, StringRef(std::to_string(domainId))));
		}

		state int nAttempts = 0;
		loop {
			// Test case given is measuring correctness for cache hit/miss scenarios is designed to have strict
			// assertions. However, in simulation runs, RPCs can be force failed to inject retries, hence, code leverage
			// tryGetReply to ensure at-most once delivery of message, further, assertions are relaxed to account of
			// cache warm-up due to retries.
			EKPGetLatestBaseCipherKeysRequest req;
			req.encryptDomainInfos = self->domainInfos;
			if (deterministicRandom()->randomInt(0, 100) < 50) {
				req.debugId = deterministicRandom()->randomUniqueID();
			}
			ErrorOr<EKPGetLatestBaseCipherKeysReply> rep = wait(self->ekpInf.getLatestBaseCipherKeys.tryGetReply(req));
			if (rep.present()) {
				ASSERT(!rep.get().error.present());
				ASSERT_EQ(rep.get().baseCipherDetails.size(), self->domainInfos.size());

				for (const auto& info : self->domainInfos) {
					bool found = false;
					for (const auto& item : rep.get().baseCipherDetails) {
						if (item.encryptDomainId == info.domainId) {
							found = true;
							break;
						}
					}
					ASSERT(found);
				}

				// Ensure desired cache-hit counts
				if (nAttempts == 0) {
					ASSERT_EQ(rep.get().numHits, expectedHits);
				} else {
					ASSERT_GE(rep.get().numHits, expectedHits);
				}
				break;
			} else {
				nAttempts++;
				wait(delay(0.0));
			}
		}
		self->domainInfos.clear();

		TraceEvent("SimPartialDomainIdCacheDone").log();
		return Void();
	}

	ACTOR Future<Void> simRandomBaseCipherIdCache(EncryptKeyProxyTestWorkload* self) {
		state int expectedHits;

		TraceEvent("SimRandomDomainIdCacheStart").log();

		self->domainInfos.clear();
		for (int i = 0; i < self->numDomains; i++) {
			const EncryptCipherDomainId domainId = self->minDomainId + i;
			self->domainInfos.emplace_back(
			    EKPGetLatestCipherKeysRequestInfo(self->arena, domainId, StringRef(std::to_string(domainId))));
		}

		EKPGetLatestBaseCipherKeysRequest req;
		req.encryptDomainInfos = self->domainInfos;
		if (deterministicRandom()->randomInt(0, 100) < 50) {
			req.debugId = deterministicRandom()->randomUniqueID();
		}
		EKPGetLatestBaseCipherKeysReply rep = wait(self->ekpInf.getLatestBaseCipherKeys.getReply(req));

		ASSERT(!rep.error.present());
		ASSERT_EQ(rep.baseCipherDetails.size(), self->domainInfos.size());
		for (const auto& info : self->domainInfos) {
			bool found = false;
			for (const auto& item : rep.baseCipherDetails) {
				if (item.encryptDomainId == info.domainId) {
					found = true;
					break;
				}
			}
			ASSERT(found);
		}

		self->cipherIdMap.clear();
		self->cipherIds.clear();
		for (auto& item : rep.baseCipherDetails) {
			CacheKey cacheKey = std::make_pair(item.encryptDomainId, item.baseCipherId);
			self->cipherIdMap.emplace(cacheKey, StringRef(self->arena, item.baseCipherKey));
			self->cipherIds.emplace_back(cacheKey);
		}

		state int numIterations = deterministicRandom()->randomInt(512, 786);
		for (; numIterations > 0;) {
			int idx = deterministicRandom()->randomInt(1, self->cipherIds.size());
			int nIds = deterministicRandom()->randomInt(1, self->cipherIds.size());

			EKPGetBaseCipherKeysByIdsRequest req;
			if (deterministicRandom()->randomInt(0, 100) < 50) {
				req.debugId = deterministicRandom()->randomUniqueID();
			}
			for (int i = idx; i < nIds && i < self->cipherIds.size(); i++) {
				req.baseCipherInfos.emplace_back(
				    EKPGetBaseCipherKeysRequestInfo(self->cipherIds[i].first,
				                                    self->cipherIds[i].second,
				                                    StringRef(std::to_string(self->cipherIds[i].first)),
				                                    req.arena));
			}
			if (req.baseCipherInfos.empty()) {
				// No keys to query; continue
				continue;
			} else {
				numIterations--;
			}

			expectedHits = req.baseCipherInfos.size();
			EKPGetBaseCipherKeysByIdsReply rep = wait(self->ekpInf.getBaseCipherKeysByIds.getReply(req));

			ASSERT(!rep.error.present());
			ASSERT_EQ(rep.baseCipherDetails.size(), expectedHits);
			ASSERT_EQ(rep.numHits, expectedHits);
			// Valdiate the 'cipherKey' content against the one read while querying by domainIds
			for (auto& item : rep.baseCipherDetails) {
				CacheKey cacheKey = std::make_pair(item.encryptDomainId, item.baseCipherId);
				const auto itr = self->cipherIdMap.find(cacheKey);
				ASSERT(itr != self->cipherIdMap.end());
				Standalone<StringRef> toCompare = self->cipherIdMap[cacheKey];
				if (toCompare.compare(item.baseCipherKey) != 0) {
					TraceEvent("Mismatch")
					    .detail("Id", item.baseCipherId)
					    .detail("CipherMapDataHash", XXH3_64bits(toCompare.begin(), toCompare.size()))
					    .detail("CipherMapSize", toCompare.size())
					    .detail("CipherMapValue", toCompare.toString())
					    .detail("ReadDataHash", XXH3_64bits(item.baseCipherKey.begin(), item.baseCipherKey.size()))
					    .detail("ReadValue", item.baseCipherKey.toString())
					    .detail("ReadDataSize", item.baseCipherKey.size());
					ASSERT(false);
				}
			}
		}

		TraceEvent("SimRandomDomainIdCacheDone").log();
		return Void();
	}

	ACTOR Future<Void> simLookupInvalidKeyId(EncryptKeyProxyTestWorkload* self) {
		Arena arena;

		TraceEvent("SimLookupInvalidKeyIdStart").log();

		// Prepare a lookup with valid and invalid keyIds - SimEncryptKmsProxy should throw encrypt_key_not_found()
		EKPGetBaseCipherKeysByIdsRequest req;
		for (auto item : self->cipherIds) {
			req.baseCipherInfos.emplace_back(EKPGetBaseCipherKeysRequestInfo(
			    item.first, item.second, StringRef(std::to_string(item.first)), req.arena));
		}
		req.baseCipherInfos.emplace_back(EKPGetBaseCipherKeysRequestInfo(
		    1, SERVER_KNOBS->SIM_KMS_MAX_KEYS + 10, StringRef(std::to_string(1)), req.arena));
		EKPGetBaseCipherKeysByIdsReply rep = wait(self->ekpInf.getBaseCipherKeysByIds.getReply(req));

		ASSERT_EQ(rep.baseCipherDetails.size(), 0);
		ASSERT(rep.error.present());
		ASSERT_EQ(rep.error.get().code(), error_code_encrypt_key_not_found);

		TraceEvent("SimLookupInvalidKeyIdDone").log();
		return Void();
	}

	// Following test cases are covered:
	// 1. Simulate an empty domainIdCache.
	// 2. Simulate an mixed lookup (partial cache-hit) for domainIdCache.
	// 3. Simulate a lookup on all domainIdCache keys and validate lookup by baseCipherKeyIds.
	// 4. Simulate lookup for an invalid baseCipherKeyId.

	ACTOR Future<Void> testWorkload(Reference<AsyncVar<ServerDBInfo> const> dbInfo, EncryptKeyProxyTestWorkload* self) {
		// Ensure EncryptKeyProxy role is recruited (a singleton role)
		while (!dbInfo->get().encryptKeyProxy.present()) {
			wait(delay(.1));
		}

		self->ekpInf = dbInfo->get().encryptKeyProxy.get();
		self->numDomains = self->maxDomainId - self->minDomainId;

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
		// TODO: Enable this workload in testing
		CODE_PROBE(true, "Running EncryptKeyProxyTest", probe::decoration::rare);
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
