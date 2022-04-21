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

#include <memory>
#include <unordered_map>
#include <utility>

#include "fdbrpc/sim_validation.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/SimEncryptKmsProxy.actor.h"
#include "flow/ActorCollection.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/ITrace.h"
#include "flow/StreamCipher.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include "flow/network.h"

struct SimEncryptKeyCtx {
	SimEncryptKeyId id;
	SimEncryptKey key;

	SimEncryptKeyCtx() : id(0) {}
	explicit SimEncryptKeyCtx(SimEncryptKeyId kId, const char* data) : id(kId), key(data) {}
};

struct SimEncryptKmsProxyContext {
	uint32_t maxEncryptionKeys;
	std::unordered_map<SimEncryptKeyId, std::unique_ptr<SimEncryptKeyCtx>> simEncryptKeyStore;

	SimEncryptKmsProxyContext() : maxEncryptionKeys(0) {}
	explicit SimEncryptKmsProxyContext(uint32_t keyCount) : maxEncryptionKeys(keyCount) {
		uint8_t buffer[AES_256_KEY_LENGTH];

		// Construct encryption keyStore.
		for (int i = 0; i < maxEncryptionKeys; i++) {
			generateRandomData(&buffer[0], AES_256_KEY_LENGTH);
			SimEncryptKeyCtx ctx(i, reinterpret_cast<const char*>(buffer));
			simEncryptKeyStore[i] = std::make_unique<SimEncryptKeyCtx>(i, reinterpret_cast<const char*>(buffer));
		}
	}
};

ACTOR Future<Void> simEncryptKmsProxyCore(SimKmsProxyInterface interf) {
	state SimEncryptKmsProxyContext kmsProxyCtx(SERVER_KNOBS->SIM_KMS_MAX_KEYS);

	ASSERT(kmsProxyCtx.simEncryptKeyStore.size() == SERVER_KNOBS->SIM_KMS_MAX_KEYS);

	TraceEvent("SimEncryptKmsProxy_Init", interf.id()).detail("MaxEncryptKeys", SERVER_KNOBS->SIM_KMS_MAX_KEYS);

	state bool success = true;
	loop {
		choose {
			when(SimGetEncryptKeysByKeyIdsRequest req = waitNext(interf.encryptKeyLookupByKeyIds.getFuture())) {
				state SimGetEncryptKeysByKeyIdsRequest keysByIdsReq = req;
				state SimGetEncryptKeysByKeyIdsReply keysByIdsRep;

				// Lookup corresponding EncryptKeyCtx for input keyId
				for (const auto& item : req.encryptKeyIds) {
					const auto& itr = kmsProxyCtx.simEncryptKeyStore.find(item.first);
					if (itr != kmsProxyCtx.simEncryptKeyStore.end()) {
						keysByIdsRep.encryptKeyDetails.emplace_back(
						    item.second,
						    itr->first,
						    StringRef(keysByIdsRep.arena, itr->second.get()->key),
						    keysByIdsRep.arena);
					} else {
						success = false;
						break;
					}
				}

				wait(delayJittered(1.0)); // simulate network delay

				success ? keysByIdsReq.reply.send(keysByIdsRep) : keysByIdsReq.reply.sendError(encrypt_key_not_found());
			}
			when(SimGetEncryptKeysByDomainIdsRequest req = waitNext(interf.encryptKeyLookupByDomainId.getFuture())) {
				state SimGetEncryptKeysByDomainIdsRequest keysByDomainIdReq = req;
				state SimGetEncryptKeyByDomainIdReply keysByDomainIdRep;

				// Map encryptionDomainId to corresponding EncryptKeyCtx element using a modulo operation. This would
				// mean multiple domains gets mapped to the same encryption key which is fine, the EncryptKeyStore
				// guarantees that keyId -> plaintext encryptKey mapping is idempotent.
				for (SimEncryptDomainId domainId : req.encryptDomainIds) {
					SimEncryptKeyId keyId = domainId % SERVER_KNOBS->SIM_KMS_MAX_KEYS;
					const auto& itr = kmsProxyCtx.simEncryptKeyStore.find(keyId);
					if (itr != kmsProxyCtx.simEncryptKeyStore.end()) {
						keysByDomainIdRep.encryptKeyDetails.emplace_back(
						    domainId, keyId, StringRef(itr->second.get()->key), keysByDomainIdRep.arena);
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

void forceLinkSimEncryptKmsProxyTests() {}

namespace {

ACTOR Future<Void> testRunWorkload(SimKmsProxyInterface inf, uint32_t nEncryptionKeys) {
	state uint32_t maxEncryptionKeys = nEncryptionKeys;
	state int maxDomainIds = deterministicRandom()->randomInt(121, 295);
	state int maxIterations = deterministicRandom()->randomInt(786, 1786);
	state std::unordered_map<SimEncryptDomainId, std::unique_ptr<SimEncryptKeyCtx>> domainIdKeyMap;
	state int i = 0;

	TraceEvent("RunWorkloadStart").detail("MaxDomainIds", maxDomainIds).detail("MaxIterations", maxIterations);

	{
		// construct domainId to EncryptKeyCtx map
		SimGetEncryptKeysByDomainIdsRequest domainIdsReq;
		for (i = 0; i < maxDomainIds; i++) {
			domainIdsReq.encryptDomainIds.push_back(i);
		}
		SimGetEncryptKeyByDomainIdReply domainIdsReply = wait(inf.encryptKeyLookupByDomainId.getReply(domainIdsReq));
		for (auto& element : domainIdsReply.encryptKeyDetails) {
			domainIdKeyMap.emplace(
			    element.encryptDomainId,
			    std::make_unique<SimEncryptKeyCtx>(element.encryptKeyId, element.encryptKey.toString().c_str()));
		}

		// randomly pick any domainId and validate if lookupByKeyId result matches
		state std::unordered_map<SimEncryptKeyId, StringRef> validationMap;
		std::unordered_map<SimEncryptKeyId, SimEncryptDomainId> idsToLookup;
		for (i = 0; i < maxIterations; i++) {
			state int idx = deterministicRandom()->randomInt(0, maxDomainIds);
			state SimEncryptKeyCtx* ctx = domainIdKeyMap[idx].get();
			validationMap[ctx->id] = StringRef(ctx->key);
			idsToLookup.emplace(ctx->id, idx);
		}

		state SimGetEncryptKeysByKeyIdsRequest keyIdsReq;
		for (const auto& item : idsToLookup) {
			keyIdsReq.encryptKeyIds.emplace_back(std::make_pair(item.first, item.second));
		}
		state SimGetEncryptKeysByKeyIdsReply keyIdsReply = wait(inf.encryptKeyLookupByKeyIds.getReply(keyIdsReq));
		/* TraceEvent("Lookup")
		    .detail("KeyIdReqSize", keyIdsReq.encryptKeyIds.size())
		    .detail("KeyIdsRepSz", keyIdsReply.encryptKeyDetails.size())
		    .detail("ValSz", validationMap.size()); */
		ASSERT(keyIdsReply.encryptKeyDetails.size() == validationMap.size());
		for (const auto& element : keyIdsReply.encryptKeyDetails) {
			ASSERT(validationMap[element.encryptDomainId].compare(element.encryptKey) == 0);
		}
	}

	{
		// Verify unknown key access returns the error
		state SimGetEncryptKeysByKeyIdsRequest req;
		req.encryptKeyIds.emplace_back(std::make_pair(maxEncryptionKeys + 1, 1));
		try {
			SimGetEncryptKeysByKeyIdsReply reply = wait(inf.encryptKeyLookupByKeyIds.getReply(req));
		} catch (Error& e) {
			ASSERT(e.code() == error_code_encrypt_key_not_found);
		}
	}

	TraceEvent("RunWorkloadDone").log();
	return Void();
}

} // namespace

TEST_CASE("fdbserver/SimEncryptKmsProxy") {
	state SimKmsProxyInterface inf;
	state uint32_t maxEncryptKeys = 64;

	loop choose {
		when(wait(simEncryptKmsProxyCore(inf))) { throw internal_error(); }
		when(wait(testRunWorkload(inf, maxEncryptKeys))) { break; }
	}
	return Void();
}