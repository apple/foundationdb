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

#include "fdbserver/SimKmsConnector.h"

#include "fdbrpc/sim_validation.h"
#include "fdbserver/Knobs.h"
#include "flow/ActorCollection.h"
#include "flow/BlobCipher.h"
#include "flow/EncryptUtils.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/ITrace.h"
#include "flow/Trace.h"
#include "flow/network.h"
#include "flow/UnitTest.h"

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

struct SimKmsConnectorContext {
	uint32_t maxEncryptionKeys;
	std::unordered_map<EncryptCipherBaseKeyId, std::unique_ptr<SimEncryptKeyCtx>> simEncryptKeyStore;

	explicit SimKmsConnectorContext(uint32_t keyCount) : maxEncryptionKeys(keyCount) {
		const unsigned char SHA_KEY[] = "0c39e7906db6d51ac0573d328ce1b6be";

		// Construct encryption keyStore.
		// Note the keys generated must be the same after restart.
		for (int i = 1; i <= maxEncryptionKeys; i++) {
			Arena arena;
			StringRef digest = computeAuthToken(
			    reinterpret_cast<const unsigned char*>(&i), sizeof(i), SHA_KEY, AES_256_KEY_LENGTH, arena);
			simEncryptKeyStore[i] =
			    std::make_unique<SimEncryptKeyCtx>(i, reinterpret_cast<const char*>(digest.begin()));
		}
	}
};

ACTOR Future<Void> simKmsConnectorCore_impl(KmsConnectorInterface interf) {
	TraceEvent("SimEncryptKmsProxy_Init", interf.id()).detail("MaxEncryptKeys", SERVER_KNOBS->SIM_KMS_MAX_KEYS);

	state bool success = true;
	state std::unique_ptr<SimKmsConnectorContext> ctx =
	    std::make_unique<SimKmsConnectorContext>(SERVER_KNOBS->SIM_KMS_MAX_KEYS);

	ASSERT_EQ(ctx->simEncryptKeyStore.size(), SERVER_KNOBS->SIM_KMS_MAX_KEYS);

	loop {
		choose {
			when(KmsConnLookupEKsByKeyIdsReq req = waitNext(interf.ekLookupByIds.getFuture())) {
				state KmsConnLookupEKsByKeyIdsReq keysByIdsReq = req;
				state KmsConnLookupEKsByKeyIdsRep keysByIdsRep;
				state Optional<TraceEvent> dbgKIdTrace = keysByIdsReq.debugId.present()
				                                             ? TraceEvent("SimKmsGetByKeyIds", interf.id())
				                                             : Optional<TraceEvent>();

				if (dbgKIdTrace.present()) {
					dbgKIdTrace.get().setMaxEventLength(100000);
					dbgKIdTrace.get().detail("DbgId", keysByIdsReq.debugId.get());
				}

				// Lookup corresponding EncryptKeyCtx for input keyId
				for (const auto& item : req.encryptKeyIds) {
					const auto& itr = ctx->simEncryptKeyStore.find(item.first);
					if (itr != ctx->simEncryptKeyStore.end()) {
						keysByIdsRep.cipherKeyDetails.emplace_back(
						    item.second,
						    itr->first,
						    StringRef(keysByIdsRep.arena, itr->second.get()->key),
						    keysByIdsRep.arena);

						if (dbgKIdTrace.present()) {
							// {encryptDomainId, baseCipherId} forms a unique tuple across encryption domains
							dbgKIdTrace.get().detail(
							    getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_RESULT_PREFIX, item.second, itr->first), "");
						}
					} else {
						success = false;
						break;
					}
				}

				wait(delayJittered(1.0)); // simulate network delay

				success ? keysByIdsReq.reply.send(keysByIdsRep) : keysByIdsReq.reply.sendError(encrypt_key_not_found());
			}
			when(KmsConnLookupEKsByDomainIdsReq req = waitNext(interf.ekLookupByDomainIds.getFuture())) {
				state KmsConnLookupEKsByDomainIdsReq keysByDomainIdReq = req;
				state KmsConnLookupEKsByDomainIdsRep keysByDomainIdRep;
				state Optional<TraceEvent> dbgDIdTrace = keysByDomainIdReq.debugId.present()
				                                             ? TraceEvent("SimKmsGetsByDomIds", interf.id())
				                                             : Optional<TraceEvent>();

				if (dbgDIdTrace.present()) {
					dbgDIdTrace.get().detail("DbgId", keysByDomainIdReq.debugId.get());
				}

				// Map encryptionDomainId to corresponding EncryptKeyCtx element using a modulo operation. This
				// would mean multiple domains gets mapped to the same encryption key which is fine, the
				// EncryptKeyStore guarantees that keyId -> plaintext encryptKey mapping is idempotent.
				for (EncryptCipherDomainId domainId : req.encryptDomainIds) {
					EncryptCipherBaseKeyId keyId = 1 + abs(domainId) % SERVER_KNOBS->SIM_KMS_MAX_KEYS;
					const auto& itr = ctx->simEncryptKeyStore.find(keyId);
					if (itr != ctx->simEncryptKeyStore.end()) {
						keysByDomainIdRep.cipherKeyDetails.emplace_back(
						    domainId, keyId, StringRef(itr->second.get()->key), keysByDomainIdRep.arena);

						if (dbgDIdTrace.present()) {
							// {encryptId, baseCipherId} forms a unique tuple across encryption domains
							dbgDIdTrace.get().detail(
							    getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_RESULT_PREFIX, domainId, keyId), "");
						}
					} else {
						success = false;
						break;
					}
				}

				wait(delayJittered(1.0)); // simulate network delay

				success ? keysByDomainIdReq.reply.send(keysByDomainIdRep)
				        : keysByDomainIdReq.reply.sendError(encrypt_key_not_found());
			}
		}
	}
}

Future<Void> SimKmsConnector::connectorCore(KmsConnectorInterface interf) {
	return simKmsConnectorCore_impl(interf);
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
			domainIdsReq.encryptDomainIds.push_back(i);
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
			keyIdsReq.encryptKeyIds.emplace_back(std::make_pair(item.first, item.second));
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
		req.encryptKeyIds.emplace_back(std::make_pair(maxEncryptionKeys + 1, 1));
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
	state SimKmsConnector connector;

	loop choose {
		when(wait(connector.connectorCore(inf))) { throw internal_error(); }
		when(wait(testRunWorkload(inf, maxEncryptKeys))) { break; }
	}
	return Void();
}