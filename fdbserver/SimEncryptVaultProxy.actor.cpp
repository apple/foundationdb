/*
 * SimEncryptVaulProxy.actor.cpp
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

#include "fdbrpc/sim_validation.h"
#include "fdbserver/SimEncryptVaultProxy.actor.h"
#include "flow/ActorCollection.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/ITrace.h"
#include "flow/StreamCipher.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct SimEncryptKeyCtx {
	SimEncryptKeyId id;
	SimEncryptKey key;

	SimEncryptKeyCtx() : id(0) {}
	explicit SimEncryptKeyCtx(SimEncryptKeyId kId, const char* data) : id(kId), key(data) {}
};

struct SimEncyrptVaultProxyContext {
	uint32_t maxEncryptionKeys;
	std::unordered_map<SimEncryptKeyId, std::unique_ptr<SimEncryptKeyCtx>> simEncryptKeyStore;

	SimEncyrptVaultProxyContext() : maxEncryptionKeys(0) {}
	explicit SimEncyrptVaultProxyContext(uint32_t keyCount) : maxEncryptionKeys(keyCount) {
		uint8_t buffer[AES_256_KEY_LENGTH];

		// Construct encryption keyStore.
		for (int i = 0; i < maxEncryptionKeys; i++) {
			generateRandomData(&buffer[0], AES_256_KEY_LENGTH);
			SimEncryptKeyCtx ctx(i, reinterpret_cast<const char*>(buffer));
			simEncryptKeyStore[i] = std::make_unique<SimEncryptKeyCtx>(i, reinterpret_cast<const char*>(buffer));
		}
	}
};

ACTOR Future<Void> simEncryptVaultProxyCore(SimEncryptVaultProxyInterface interf, uint32_t maxEncryptKeys) {
	state SimEncyrptVaultProxyContext vaultProxyCtx(maxEncryptKeys);

	ASSERT(vaultProxyCtx.simEncryptKeyStore.size() == maxEncryptKeys);

	TraceEvent("SimEncryptVaultProxy_Init", interf.id()).detail("MaxEncrptKeys", maxEncryptKeys);

	loop {
		choose {
			when(SimGetEncryptKeyByKeyIdRequest req = waitNext(interf.encryptKeyLookupByKeyId.getFuture())) {
				SimGetEncryptKeyByKeyIdReply reply;

				// Lookup corresponding EncryptKeyCtx for input keyId
				if (vaultProxyCtx.simEncryptKeyStore.find(req.encryptKeyId) != vaultProxyCtx.simEncryptKeyStore.end()) {
					reply.encryptKey = StringRef(vaultProxyCtx.simEncryptKeyStore[req.encryptKeyId].get()->key);
					req.reply.send(reply);
				} else {
					req.reply.sendError(key_not_found());
				}
			}
			when(SimGetEncryptKeyByDomainIdRequest req = waitNext(interf.encryptKeyLookupByDomainId.getFuture())) {
				SimGetEncryptKeyByDomainIdReply reply;

				// Map encryptionDomainId to corresponding EncryptKeyCtx element using a modulo operation. This would
				// mean multiple domains gets mapped to the same encryption key which is fine, the EncryptKeyStore
				// guarantees that keyId -> plaintext encryptKey mapping is idempotent.

				reply.encryptKeyId = req.encryptDomainId % maxEncryptKeys;
				reply.encryptKey = StringRef(vaultProxyCtx.simEncryptKeyStore[reply.encryptKeyId].get()->key);
				req.reply.send(reply);
			}
		}
	}
}

void forceLinkSimEncryptVaultProxyTests() {}

namespace {

ACTOR Future<Void> testRunWorkload(SimEncryptVaultProxyInterface inf, uint32_t nEncryptionKeys) {
	state uint32_t maxEncryptionKeys = nEncryptionKeys;
	state int maxDomainIds = deterministicRandom()->randomInt(121, 295);
	state int maxIterations = deterministicRandom()->randomInt(786, 1786);
	state std::unordered_map<SimEncryptDomainId, std::unique_ptr<SimEncryptKeyCtx>> domainIdKeyMap;
	state int i = 0;

	TraceEvent("RunWorkloadStart").detail("MaxDomainIds", maxDomainIds).detail("MaxIterations", maxIterations);

	{
		// construct domainId to EncryptKeyCtx map
		for (i = 0; i < maxDomainIds; i++) {
			SimGetEncryptKeyByDomainIdRequest req;
			req.encryptDomainId = i;
			SimGetEncryptKeyByDomainIdReply reply = wait(inf.encryptKeyLookupByDomainId.getReply(req));
			domainIdKeyMap[i] =
			    std::make_unique<SimEncryptKeyCtx>(reply.encryptKeyId, reply.encryptKey.toString().c_str());
		}

		// randomly pick any domainId and validate if lookupByKeyId result matches
		for (i = 0; i < maxIterations; i++) {
			state int idx = deterministicRandom()->randomInt(0, maxDomainIds);
			state SimEncryptKeyCtx* ctx = domainIdKeyMap[idx].get();
			SimGetEncryptKeyByKeyIdRequest req(ctx->id);
			SimGetEncryptKeyByKeyIdReply reply = wait(inf.encryptKeyLookupByKeyId.getReply(req));
			ASSERT(reply.encryptKey.compare(ctx->key) == 0);
		}
	}

	{
		// Verify unknown key access returns the error
		state SimGetEncryptKeyByKeyIdRequest req;
		req.encryptKeyId = maxEncryptionKeys + 1;
		try {
			SimGetEncryptKeyByKeyIdReply reply = wait(inf.encryptKeyLookupByKeyId.getReply(req));
		} catch (Error& e) {
			ASSERT(e.code() == error_code_key_not_found);
		}
	}

	TraceEvent("RunWorkloadDone").log();
	return Void();
}

} // namespace

TEST_CASE("fdbserver/SimEncryptVaultProxy") {
	state SimEncryptVaultProxyInterface inf;
	state uint32_t maxEncryptKeys = 64;

	loop choose {
		when(wait(simEncryptVaultProxyCore(inf, maxEncryptKeys))) { throw internal_error(); }
		when(wait(testRunWorkload(inf, maxEncryptKeys))) { break; }
	}
	return Void();
}