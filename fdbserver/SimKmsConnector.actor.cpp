/*
 * SimEncryptKmsProxy.actor.cpp
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

#include "fdbrpc/sim_validation.h"

#include "fdbserver/KmsConnectorInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/SimKmsConnector.h"

#include "flow/ActorCollection.h"
#include "flow/Arena.h"
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

#include <limits>
#include <memory>
#include <unordered_map>
#include <utility>

#include "flow/actorcompiler.h" // This must be the last #include.

using SimEncryptKey = std::string;
struct SimEncryptKeyCtx {
	EncryptCipherBaseKeyId id;
	SimEncryptKey key;

	explicit SimEncryptKeyCtx(EncryptCipherBaseKeyId kId, const char* data) : id(kId), key(data, AES_256_KEY_LENGTH) {}
};

// The credentials may be allowed to change, but the storage locations and partitioning cannot change, even across
// restarts. Keep it as global static state in simulation.
static std::unordered_map<BlobMetadataDomainId, Standalone<BlobMetadataDetailsRef>> simBlobMetadataStore;

struct SimKmsConnectorContext : NonCopyable, ReferenceCounted<SimKmsConnectorContext> {
	uint32_t maxEncryptionKeys;
	std::unordered_map<EncryptCipherBaseKeyId, std::unique_ptr<SimEncryptKeyCtx>> simEncryptKeyStore;

	explicit SimKmsConnectorContext(uint32_t keyCount) : maxEncryptionKeys(keyCount) {
		const unsigned char SHA_KEY[] = "0c39e7906db6d51ac0573d328ce1b6be";

		// Construct encryption keyStore.
		// Note the keys generated must be the same after restart.
		for (int i = 1; i <= maxEncryptionKeys; i++) {
			uint8_t digest[AUTH_TOKEN_HMAC_SHA_SIZE];
			computeAuthToken({ { reinterpret_cast<const uint8_t*>(&i), sizeof(i) } },
			                 SHA_KEY,
			                 AES_256_KEY_LENGTH,
			                 &digest[0],
			                 EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA,
			                 AUTH_TOKEN_HMAC_SHA_SIZE);
			simEncryptKeyStore[i] = std::make_unique<SimEncryptKeyCtx>(i, reinterpret_cast<const char*>(&digest[0]));
		}
	}
};

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

ACTOR Future<Void> ekLookupByIds(Reference<SimKmsConnectorContext> ctx,
                                 KmsConnectorInterface interf,
                                 KmsConnLookupEKsByKeyIdsReq req) {
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
		const auto& itr = ctx->simEncryptKeyStore.find(item.baseCipherId);
		if (itr != ctx->simEncryptKeyStore.end()) {
			// TODO: Relax assert if EKP APIs are updated to make 'domain_id' optional for encryption keys point lookups
			ASSERT(item.domainId.present());
			rep.cipherKeyDetails.emplace_back_deep(
			    rep.arena, item.domainId.get(), itr->first, StringRef(itr->second.get()->key), refAtTS, expAtTS);

			if (dbgKIdTrace.present()) {
				// {encryptDomainId, baseCipherId} forms a unique tuple across encryption domains
				dbgKIdTrace.get().detail(
				    getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_RESULT_PREFIX, item.domainId.get(), itr->first), "");
			}
		} else {
			TraceEvent("SimKmsEKLookupByIdsKeyNotFound").detail("DomId", item.domainId).detail("BaseCipherId", item.baseCipherId);
			success = false;
			break;
		}
	}

	wait(delayJittered(1.0)); // simulate network delay
	success ? req.reply.send(rep) : req.reply.sendError(encrypt_key_not_found());
	return Void();
}

ACTOR Future<Void> ekLookupByDomainIds(Reference<SimKmsConnectorContext> ctx,
                                       KmsConnectorInterface interf,
                                       KmsConnLookupEKsByDomainIdsReq req) {
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

		EncryptCipherBaseKeyId keyId = 1 + abs(domainId) % SERVER_KNOBS->SIM_KMS_MAX_KEYS;
		const auto& itr = ctx->simEncryptKeyStore.find(keyId);
		if (itr != ctx->simEncryptKeyStore.end()) {
			rep.cipherKeyDetails.emplace_back_deep(
			    req.arena, domainId, keyId, StringRef(itr->second.get()->key), refAtTS, expAtTS);
			if (dbgDIdTrace.present()) {
				// {encryptId, baseCipherId} forms a unique tuple across encryption domains
				dbgDIdTrace.get().detail(getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_RESULT_PREFIX, domainId, keyId), "");
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
		auto it = simBlobMetadataStore.find(domainId);
		if (it == simBlobMetadataStore.end()) {
			// construct new blob metadata
			it = simBlobMetadataStore.insert({ domainId, createRandomTestBlobMetadata(SERVER_KNOBS->BG_URL, domainId) })
			         .first;
		} else if (now() >= it->second.expireAt) {
			// update random refresh and expire time
			it->second.refreshAt = now() + deterministicRandom()->random01() * 30;
			it->second.expireAt = it->second.refreshAt + deterministicRandom()->random01() * 10;
		}
		rep.metadataDetails.arena().dependsOn(it->second.arena());
		rep.metadataDetails.push_back(rep.metadataDetails.arena(), it->second);
	}

	wait(delay(deterministicRandom()->random01())); // simulate network delay

	req.reply.send(rep);

	return Void();
}

ACTOR Future<Void> simconnectorCoreImpl(KmsConnectorInterface interf) {
	TraceEvent("SimEncryptKmsProxyInit", interf.id()).detail("MaxEncryptKeys", SERVER_KNOBS->SIM_KMS_MAX_KEYS);

	state Reference<SimKmsConnectorContext> ctx = makeReference<SimKmsConnectorContext>(SERVER_KNOBS->SIM_KMS_MAX_KEYS);

	ASSERT_EQ(ctx->simEncryptKeyStore.size(), SERVER_KNOBS->SIM_KMS_MAX_KEYS);

	state PromiseStream<Future<Void>> addActor;
	state Future<Void> collection = actorCollection(addActor.getFuture());
	loop {
		choose {
			when(KmsConnLookupEKsByKeyIdsReq req = waitNext(interf.ekLookupByIds.getFuture())) {
				addActor.send(ekLookupByIds(ctx, interf, req));
			}
			when(KmsConnLookupEKsByDomainIdsReq req = waitNext(interf.ekLookupByDomainIds.getFuture())) {
				addActor.send(ekLookupByDomainIds(ctx, interf, req));
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

ACTOR Future<Void> testRunWorkload(KmsConnectorInterface inf, uint32_t nEncryptionKeys) {
	state uint32_t maxEncryptionKeys = nEncryptionKeys;
	state int maxDomainIds = deterministicRandom()->randomInt(121, 295);
	state int maxIterations = deterministicRandom()->randomInt(786, 1786);
	state std::unordered_map<EncryptCipherDomainId, std::unique_ptr<SimEncryptKeyCtx>> domainIdKeyMap;
	state int i = 0;

	TraceEvent("RunWorkloadStart").detail("MaxDomainIds", maxDomainIds).detail("MaxIterations", maxIterations);

	{
		// construct domainId to EncryptKeyCtx map
		KmsConnLookupEKsByDomainIdsReq domainIdsReq;
		for (i = 0; i < maxDomainIds; i++) {
			// domainIdsReq.encryptDomainIds.push_back(i);
			domainIdsReq.encryptDomainIds.emplace_back(i);
		}
		KmsConnLookupEKsByDomainIdsRep domainIdsRep = wait(inf.ekLookupByDomainIds.getReply(domainIdsReq));
		for (auto& element : domainIdsRep.cipherKeyDetails) {
			domainIdKeyMap.emplace(
			    element.encryptDomainId,
			    std::make_unique<SimEncryptKeyCtx>(element.encryptKeyId, element.encryptKey.toString().c_str()));
		}

		// randomly pick any domainId and validate if lookupByKeyId result matches
		state std::unordered_map<EncryptCipherBaseKeyId, StringRef> validationMap;
		std::unordered_map<EncryptCipherBaseKeyId, EncryptCipherDomainId> idsToLookup;
		for (i = 0; i < maxIterations; i++) {
			state int idx = deterministicRandom()->randomInt(0, maxDomainIds);
			state SimEncryptKeyCtx* ctx = domainIdKeyMap[idx].get();
			validationMap[ctx->id] = StringRef(ctx->key);
			idsToLookup.emplace(ctx->id, idx);
		}

		state KmsConnLookupEKsByKeyIdsReq keyIdsReq;
		for (const auto& item : idsToLookup) {
			keyIdsReq.encryptKeyInfos.emplace_back(item.second, item.first);
		}
		state KmsConnLookupEKsByKeyIdsRep keyIdsReply = wait(inf.ekLookupByIds.getReply(keyIdsReq));
		/* TraceEvent("Lookup")
		    .detail("KeyIdReqSize", keyIdsReq.encryptKeyIds.size())
		    .detail("KeyIdsRepSz", keyIdsReply.encryptKeyDetails.size())
		    .detail("ValSz", validationMap.size()); */
		ASSERT(keyIdsReply.cipherKeyDetails.size() == validationMap.size());
		for (const auto& element : keyIdsReply.cipherKeyDetails) {
			ASSERT(validationMap[element.encryptKeyId].compare(element.encryptKey) == 0);
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
	state uint32_t maxEncryptKeys = 64;
	state SimKmsConnector connector("SimKmsConnector");

	loop choose {
		when(wait(connector.connectorCore(inf))) {
			throw internal_error();
		}
		when(wait(testRunWorkload(inf, maxEncryptKeys))) {
			break;
		}
	}
	return Void();
}