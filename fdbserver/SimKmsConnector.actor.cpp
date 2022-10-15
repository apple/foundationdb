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

#include "fmt/format.h"
#include "fdbrpc/sim_validation.h"
#include "fdbclient/BlobCipher.h"
#include "fdbserver/KmsConnectorInterface.h"
#include "fdbserver/Knobs.h"
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
	TraceEvent("SimKms.EKLookupById").detail("RefreshAt", refAtTS).detail("ExpireAt", expAtTS);
	for (const auto& item : req.encryptKeyInfos) {
		const auto& itr = ctx->simEncryptKeyStore.find(item.baseCipherId);
		if (itr != ctx->simEncryptKeyStore.end()) {
			rep.cipherKeyDetails.emplace_back_deep(
			    rep.arena, item.domainId, itr->first, StringRef(itr->second.get()->key), refAtTS, expAtTS);

			if (dbgKIdTrace.present()) {
				// {encryptDomainId, baseCipherId} forms a unique tuple across encryption domains
				dbgKIdTrace.get().detail(
				    getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_RESULT_PREFIX, item.domainId, item.domainName, itr->first),
				    "");
			}
		} else {
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
	TraceEvent("SimKms.EKLookupByDomainId").detail("RefreshAt", refAtTS).detail("ExpireAt", expAtTS);
	for (const auto& info : req.encryptDomainInfos) {
		EncryptCipherBaseKeyId keyId = 1 + abs(info.domainId) % SERVER_KNOBS->SIM_KMS_MAX_KEYS;
		const auto& itr = ctx->simEncryptKeyStore.find(keyId);
		if (itr != ctx->simEncryptKeyStore.end()) {
			rep.cipherKeyDetails.emplace_back_deep(
			    req.arena, info.domainId, keyId, StringRef(itr->second.get()->key), refAtTS, expAtTS);
			if (dbgDIdTrace.present()) {
				// {encryptId, baseCipherId} forms a unique tuple across encryption domains
				dbgDIdTrace.get().detail(
				    getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_RESULT_PREFIX, info.domainId, info.domainName, keyId), "");
			}
		} else {
			success = false;
			break;
		}
	}

	wait(delayJittered(1.0)); // simulate network delay

	success ? req.reply.send(rep) : req.reply.sendError(encrypt_key_not_found());

	return Void();
}
// TODO: switch this to use bg_url instead of hardcoding file://fdbblob, so it works as FDBPerfKmsConnector
// FIXME: make this (more) deterministic outside of simulation for FDBPerfKmsConnector
static Standalone<BlobMetadataDetailsRef> createBlobMetadata(BlobMetadataDomainId domainId,
                                                             BlobMetadataDomainName domainName) {
	Standalone<BlobMetadataDetailsRef> metadata;
	metadata.domainId = domainId;
	metadata.arena().dependsOn(domainName.arena());
	metadata.domainName = domainName;
	// 0 == no partition, 1 == suffix partitioned, 2 == storage location partitioned
	int type = deterministicRandom()->randomInt(0, 3);
	int partitionCount = (type == 0) ? 0 : deterministicRandom()->randomInt(2, 12);
	fmt::print("SimBlobMetadata ({})\n", domainId);
	TraceEvent ev(SevDebug, "SimBlobMetadata");
	ev.detail("DomainId", domainId).detail("TypeNum", type).detail("PartitionCount", partitionCount);
	if (type == 0) {
		// single storage location
		metadata.base = StringRef(metadata.arena(), "file://fdbblob/" + std::to_string(domainId) + "/");
		fmt::print("  {}\n", metadata.base.get().printable());
		ev.detail("Base", metadata.base);
	}
	if (type == 1) {
		// simulate hash prefixing in s3
		metadata.base = StringRef(metadata.arena(), "file://fdbblob/"_sr);
		ev.detail("Base", metadata.base);
		fmt::print("    {} ({})\n", metadata.base.get().printable(), partitionCount);
		for (int i = 0; i < partitionCount; i++) {
			metadata.partitions.push_back_deep(metadata.arena(),
			                                   deterministicRandom()->randomUniqueID().shortString() + "-" +
			                                       std::to_string(domainId) + "/");
			fmt::print("      {}\n", metadata.partitions.back().printable());
			ev.detail("P" + std::to_string(i), metadata.partitions.back());
		}
	}
	if (type == 2) {
		// simulate separate storage location per partition
		for (int i = 0; i < partitionCount; i++) {
			metadata.partitions.push_back_deep(
			    metadata.arena(), "file://fdbblob" + std::to_string(domainId) + "_" + std::to_string(i) + "/");
			fmt::print("      {}\n", metadata.partitions.back().printable());
			ev.detail("P" + std::to_string(i), metadata.partitions.back());
		}
	}

	// set random refresh + expire time
	if (deterministicRandom()->coinflip()) {
		metadata.refreshAt = now() + deterministicRandom()->random01() * SERVER_KNOBS->BLOB_METADATA_REFRESH_INTERVAL;
		metadata.expireAt =
		    metadata.refreshAt + deterministicRandom()->random01() * SERVER_KNOBS->BLOB_METADATA_REFRESH_INTERVAL;
	} else {
		metadata.refreshAt = std::numeric_limits<double>::max();
		metadata.expireAt = metadata.refreshAt;
	}

	return metadata;
}

ACTOR Future<Void> blobMetadataLookup(KmsConnectorInterface interf, KmsConnBlobMetadataReq req) {
	state KmsConnBlobMetadataRep rep;
	state Optional<TraceEvent> dbgDIdTrace =
	    req.debugId.present() ? TraceEvent("SimKmsBlobMetadataLookup", interf.id()) : Optional<TraceEvent>();
	if (dbgDIdTrace.present()) {
		dbgDIdTrace.get().detail("DbgId", req.debugId.get());
	}

	for (auto const& domainInfo : req.domainInfos) {
		auto it = simBlobMetadataStore.find(domainInfo.domainId);
		if (it == simBlobMetadataStore.end()) {
			// construct new blob metadata
			it = simBlobMetadataStore
			         .insert({ domainInfo.domainId, createBlobMetadata(domainInfo.domainId, domainInfo.domainName) })
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

ACTOR Future<Void> simKmsConnectorCore_impl(KmsConnectorInterface interf) {
	TraceEvent("SimEncryptKmsProxy_Init", interf.id()).detail("MaxEncryptKeys", SERVER_KNOBS->SIM_KMS_MAX_KEYS);

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
			// domainIdsReq.encryptDomainIds.push_back(i);
			EncryptCipherDomainId domainId = i;
			EncryptCipherDomainNameRef domainName = StringRef(domainIdsReq.arena, std::to_string(domainId));
			domainIdsReq.encryptDomainInfos.emplace_back(domainIdsReq.arena, i, domainName);
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
			keyIdsReq.encryptKeyInfos.emplace_back_deep(
			    keyIdsReq.arena, item.second, item.first, StringRef(std::to_string(item.second)));
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
		req.encryptKeyInfos.emplace_back_deep(
		    req.arena, 1, maxEncryptionKeys + 1, StringRef(req.arena, std::to_string(maxEncryptionKeys)));
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