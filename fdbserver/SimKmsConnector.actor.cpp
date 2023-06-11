/*
 * SimEncryptKmsProxy.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/BlobMetadataUtils.h"
#include "fdbclient/SimKmsVault.h"

#include "fdbrpc/sim_validation.h"
#include "fdbrpc/simulator.h"

#include "fdbserver/KmsConnectorInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/SimKmsConnector.h"

#include "flow/ActorCollection.h"
#include "flow/Arena.h"
#include "flow/CodeProbe.h"
#include "flow/EncryptUtils.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/ITrace.h"
#include "flow/Knobs.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/UnitTest.h"

#include "fmt/format.h"

#include <cstring>
#include <limits>
#include <memory>
#include <unordered_map>
#include <utility>

#include "flow/actorcompiler.h" // This must be the last #include.

#define DEBUG_SIM_KEY_CIPHER DEBUG_ENCRYPT_KEY_CIPHER

namespace {
Optional<int64_t> getRefreshInterval(const int64_t now, const int64_t defaultTtl) {
	if (BUGGIFY) {
		return Optional<int64_t>(now);
	}
	return Optional<int64_t>(now + defaultTtl);
}

Optional<int64_t> getExpireInterval(Optional<int64_t> refTS, const int64_t defaultTtl) {
	ASSERT(refTS.present());

	if (BUGGIFY) {
		return Optional<int64_t>(-1);
	}
	return (refTS.get() + defaultTtl);
}
} // namespace

ACTOR Future<Void> ekLookupByIds(KmsConnectorInterface interf, KmsConnLookupEKsByKeyIdsReq req) {
	state KmsConnLookupEKsByKeyIdsRep rep;
	state bool success = true;
	state Optional<TraceEvent> dbgKIdTrace =
	    req.debugId.present() ? TraceEvent("SimKmsGetByKeyIds", interf.id()) : Optional<TraceEvent>();

	if (dbgKIdTrace.present()) {
		dbgKIdTrace.get().setMaxEventLength(100000);
		dbgKIdTrace.get().detail("DbgId", req.debugId.get());
	}

	// Lookup corresponding EncryptKeyCtx for input keyId
	const int64_t currTS = (int64_t)now();
	// Fetch default TTL to avoid BUGGIFY giving different value per invocation causing refTS > expTS
	const int64_t defaultTtl = FLOW_KNOBS->ENCRYPT_CIPHER_KEY_CACHE_TTL;
	Optional<int64_t> refAtTS = getRefreshInterval(currTS, defaultTtl);
	Optional<int64_t> expAtTS = getExpireInterval(refAtTS, defaultTtl);
	TraceEvent("SimKmsEKLookupById").detail("RefreshAt", refAtTS).detail("ExpireAt", expAtTS);
	for (const auto& item : req.encryptKeyInfos) {
		Reference<SimKmsVaultKeyCtx> keyCtx = SimKmsVault::getByBaseCipherId(item.baseCipherId);
		if (keyCtx.isValid()) {
			// TODO: Relax assert if EKP APIs are updated to make 'domain_id' optional for encryption keys point lookups
			ASSERT(item.domainId.present());
			rep.cipherKeyDetails.emplace_back_deep(
			    rep.arena, item.domainId.get(), keyCtx->id, keyCtx->key, keyCtx->kcv, refAtTS, expAtTS);

			if (dbgKIdTrace.present()) {
				// {encryptDomainId, baseCipherId} forms a unique tuple across encryption domains
				dbgKIdTrace.get().detail(
				    getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_RESULT_PREFIX, item.domainId.get(), keyCtx->id), "");
			}

			if (DEBUG_SIM_KEY_CIPHER) {
				TraceEvent("SimKmsEKLookupByKeyId")
				    .detail("DomId", item.domainId.get())
				    .detail("BaseCipherId", item.baseCipherId)
				    .detail("BaseCipherKeyLen", keyCtx->keyLen)
				    .detail("BaseCipherKCV", keyCtx->kcv);
			}
		} else {
			TraceEvent("SimKmsEKLookupByIdsKeyNotFound")
			    .detail("DomId", item.domainId)
			    .detail("BaseCipherId", item.baseCipherId);
			success = false;
			break;
		}
	}

	wait(delayJittered(1.0)); // simulate network delay
	success ? req.reply.send(rep) : req.reply.sendError(encrypt_key_not_found());
	return Void();
}

ACTOR Future<Void> ekLookupByDomainIds(KmsConnectorInterface interf, KmsConnLookupEKsByDomainIdsReq req) {
	state KmsConnLookupEKsByDomainIdsRep rep;
	state bool success = true;
	state Optional<TraceEvent> dbgDIdTrace =
	    req.debugId.present() ? TraceEvent("SimKmsGetsByDomIds", interf.id()) : Optional<TraceEvent>();

	if (dbgDIdTrace.present()) {
		dbgDIdTrace.get().setMaxEventLength(16384).detail("DbgId", req.debugId.get());
	}

	// Map encryptionDomainId to corresponding EncryptKeyCtx element using a modulo operation. This
	// would mean multiple domains gets mapped to the same encryption key which is fine, the
	// EncryptKeyStore guarantees that keyId -> plaintext encryptKey mapping is idempotent.
	const int64_t currTS = (int64_t)now();
	// Fetch default TTL to avoid BUGGIFY giving different value per invocation causing refTS > expTS
	const int64_t defaultTtl = FLOW_KNOBS->ENCRYPT_CIPHER_KEY_CACHE_TTL;
	Optional<int64_t> refAtTS = getRefreshInterval(currTS, defaultTtl);
	Optional<int64_t> expAtTS = getExpireInterval(refAtTS, defaultTtl);
	TraceEvent("SimKmsEKLookupByDomainId").detail("RefreshAt", refAtTS).detail("ExpireAt", expAtTS);
	for (const auto domainId : req.encryptDomainIds) {
		// Ensure domainIds are acceptable
		if (domainId < FDB_DEFAULT_ENCRYPT_DOMAIN_ID) {
			success = false;
			break;
		}

		Reference<SimKmsVaultKeyCtx> keyCtx = SimKmsVault::getByDomainId(domainId);
		if (keyCtx.isValid()) {
			rep.cipherKeyDetails.emplace_back_deep(
			    req.arena, domainId, keyCtx->id, keyCtx->key, keyCtx->kcv, refAtTS, expAtTS);
			if (dbgDIdTrace.present()) {
				// {encryptId, baseCipherId} forms a unique tuple across encryption domains
				dbgDIdTrace.get().detail(getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_RESULT_PREFIX, domainId, keyCtx->id),
				                         "");
			}
			if (DEBUG_SIM_KEY_CIPHER) {
				TraceEvent("SimKmsEKLookupByDomainId")
				    .detail("DomId", domainId)
				    .detail("BaseCipherId", keyCtx->id)
				    .detail("BaseCipherKeyLen", keyCtx->keyLen)
				    .detail("BaseCipherKCV", keyCtx->kcv);
			}
		} else {
			TraceEvent("SimKmsEKLookupByDomainIdKeyNotFound").detail("DomId", domainId);
			success = false;
			break;
		}
	}

	wait(delayJittered(1.0)); // simulate network delay
	success ? req.reply.send(rep) : req.reply.sendError(encrypt_key_not_found());
	return Void();
}

ACTOR Future<Void> blobMetadataLookup(KmsConnectorInterface interf, KmsConnBlobMetadataReq req) {
	state KmsConnBlobMetadataRep rep;
	state Optional<TraceEvent> dbgDIdTrace =
	    req.debugId.present() ? TraceEvent("SimKmsBlobMetadataLookup", interf.id()) : Optional<TraceEvent>();
	if (dbgDIdTrace.present()) {
		dbgDIdTrace.get().detail("DbgId", req.debugId.get());
	}

	for (auto const domainId : req.domainIds) {
		Standalone<BlobMetadataDetailsRef> metadataRef = SimKmsVault::getBlobMetadata(domainId, SERVER_KNOBS->BG_URL);
		rep.metadataDetails.arena().dependsOn(metadataRef.arena());
		rep.metadataDetails.push_back(rep.metadataDetails.arena(), metadataRef);
	}

	wait(delay(deterministicRandom()->random01())); // simulate network delay

	// buggify omitted tenants, errors, or unexpectedly ordered locations in response
	if (g_network->isSimulated() && !g_simulator->speedUpSimulation && BUGGIFY_WITH_PROB(0.01)) {
		int bug = deterministicRandom()->randomInt(0, 3);
		if (bug == 0) {
			// remove some number of tenants from the response
			int targetSize = deterministicRandom()->randomInt(0, rep.metadataDetails.size());
			while (rep.metadataDetails.size() > targetSize) {
				swapAndPop(&rep.metadataDetails, deterministicRandom()->randomInt(0, rep.metadataDetails.size()));
			}
		} else if (bug == 1) {
			// send error
			req.reply.sendError(connection_failed());
			return Void();
		} else if (bug == 2) {
			int targetDetail = deterministicRandom()->randomInt(0, rep.metadataDetails.size());
			auto& locs = rep.metadataDetails[targetDetail].locations;
			if (locs.size() > 1) {
				int targetIdx1 = deterministicRandom()->randomInt(0, locs.size());
				int targetIdx2 = targetIdx1;
				while (targetIdx2 == targetIdx1) {
					targetIdx2 = deterministicRandom()->randomInt(0, locs.size());
				}
				std::swap(locs[targetIdx1], locs[targetIdx2]);
			}
		} else {
			// developer forgot to update cases
			ASSERT(false);
		}
	}

	req.reply.send(rep);

	return Void();
}

ACTOR Future<Void> simconnectorCoreImpl(KmsConnectorInterface interf) {
	TraceEvent("SimEncryptKmsProxyInit", interf.id()).detail("MaxEncryptKeys", SERVER_KNOBS->SIM_KMS_MAX_KEYS);

	state PromiseStream<Future<Void>> addActor;
	state Future<Void> collection = actorCollection(addActor.getFuture());
	loop {
		choose {
			when(KmsConnLookupEKsByKeyIdsReq req = waitNext(interf.ekLookupByIds.getFuture())) {
				addActor.send(ekLookupByIds(interf, req));
			}
			when(KmsConnLookupEKsByDomainIdsReq req = waitNext(interf.ekLookupByDomainIds.getFuture())) {
				addActor.send(ekLookupByDomainIds(interf, req));
			}
			when(KmsConnBlobMetadataReq req = waitNext(interf.blobMetadataReq.getFuture())) {
				addActor.send(blobMetadataLookup(interf, req));
			}
			when(wait(collection)) {
				// this should throw an error, not complete
				ASSERT(false);
			}
		}
	}
}

Future<Void> SimKmsConnector::connectorCore(KmsConnectorInterface interf) {
	return simconnectorCoreImpl(interf);
}
void forceLinkSimKmsConnectorTests() {}

namespace {

ACTOR Future<Void> testRunWorkload(KmsConnectorInterface inf) {
	state uint32_t maxEncryptionKeys = SimKmsVault::maxSimKeys();
	state int maxDomainIds = deterministicRandom()->randomInt(121, 295);
	state int maxIterations = deterministicRandom()->randomInt(5, 15);
	state int i;

	TraceEvent("RunWorkloadStart").detail("MaxDomainIds", maxDomainIds).detail("MaxIterations", maxIterations);

	// construct {encryptId -> baseCipherId} mapping
	state std::unordered_map<EncryptCipherDomainId, EncryptCipherBaseKeyId> domainIdMap;
	state std::vector<EncryptCipherDomainId> domainIds;
	KmsConnLookupEKsByDomainIdsReq domainIdsReq;
	for (i = 0; i < maxDomainIds; i++) {
		domainIdsReq.encryptDomainIds.emplace_back(i);
	}
	KmsConnLookupEKsByDomainIdsRep domainIdsRep = wait(inf.ekLookupByDomainIds.getReply(domainIdsReq));
	for (auto& element : domainIdsRep.cipherKeyDetails) {
		Reference<SimKmsVaultKeyCtx> keyCtx = SimKmsVault::getByDomainId(element.encryptDomainId);
		ASSERT(keyCtx.isValid());
		ASSERT_EQ(element.encryptKeyId, keyCtx->id);
		ASSERT_EQ(element.encryptKey.compare(keyCtx->key), 0);
		ASSERT_EQ(element.encryptKCV, keyCtx->kcv);
		domainIdMap[element.encryptDomainId] = keyCtx->id;
		domainIds.push_back(element.encryptDomainId);
	}

	for (i = 0; i < maxIterations; i++) {
		const int batchSize = deterministicRandom()->randomInt(1, maxEncryptionKeys);
		int domIdx = deterministicRandom()->randomInt(0, domainIds.size() - 1);
		if (deterministicRandom()->coinflip()) {
			KmsConnLookupEKsByDomainIdsReq domainIdsReq;
			for (int j = 0; j < batchSize && domIdx < domainIds.size(); j++, domIdx++) {
				domainIdsReq.encryptDomainIds.emplace_back(domIdx);
			}
			KmsConnLookupEKsByDomainIdsRep domainIdsRep = wait(inf.ekLookupByDomainIds.getReply(domainIdsReq));
			for (auto& element : domainIdsRep.cipherKeyDetails) {
				Reference<SimKmsVaultKeyCtx> keyCtx = SimKmsVault::getByDomainId(element.encryptDomainId);
				ASSERT(keyCtx.isValid());
				ASSERT_EQ(element.encryptKeyId, keyCtx->id);
				ASSERT_EQ(element.encryptKey.compare(keyCtx->key), 0);
				ASSERT_EQ(element.encryptKCV, keyCtx->kcv);
			}
		} else {
			state KmsConnLookupEKsByKeyIdsReq keyIdsReq;
			for (int j = 0; j < batchSize && domIdx < domainIds.size(); j++, domIdx++) {
				auto itr = domainIdMap.find(domIdx);
				ASSERT(itr != domainIdMap.end());
				keyIdsReq.encryptKeyInfos.emplace_back(domIdx, itr->second);
			}
			state KmsConnLookupEKsByKeyIdsRep keyIdsReply = wait(inf.ekLookupByIds.getReply(keyIdsReq));
			for (auto& element : keyIdsReply.cipherKeyDetails) {
				Reference<SimKmsVaultKeyCtx> keyCtx = SimKmsVault::getByBaseCipherId(element.encryptKeyId);
				ASSERT(keyCtx.isValid());
				ASSERT_EQ(element.encryptKeyId, keyCtx->id);
				ASSERT_EQ(element.encryptKey.compare(keyCtx->key), 0);
				ASSERT_EQ(element.encryptKCV, keyCtx->kcv);
			}
		}
	}

	{
		// Verify unknown key access returns the error
		state KmsConnLookupEKsByKeyIdsReq req;
		req.encryptKeyInfos.emplace_back(1, maxEncryptionKeys + 1);
		try {
			KmsConnLookupEKsByKeyIdsRep reply = wait(inf.ekLookupByIds.getReply(req));
		} catch (Error& e) {
			ASSERT(e.code() == error_code_encrypt_key_not_found);
		}
	}

	TraceEvent("RunWorkloadDone").log();
	return Void();
}

} // namespace

TEST_CASE("fdbserver/SimKmsConnector") {
	state KmsConnectorInterface inf;
	state SimKmsConnector connector("SimKmsConnector");

	loop choose {
		when(wait(connector.connectorCore(inf))) {
			throw internal_error();
		}
		when(wait(testRunWorkload(inf))) {
			break;
		}
	}
	return Void();
}
