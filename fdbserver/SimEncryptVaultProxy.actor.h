/*
 * SimEncryptVaultProxy.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_SIMENCRYPTVAULTPROXY_ACTOR_G_H)
#define FDBSERVER_SIMENCRYPTVAULTPROXY_ACTOR_G_H
#include "fdbserver/SimEncryptVaultProxy.actor.g.h"
#elif !defined(FDBSERVER_SIMENCRYPTVAULTPROXY_ACTOR_H)
#define FDBSERVER_SIMENCRYPTVAULTPROXY_ACTOR_H

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "flow/FileIdentifier.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/actorcompiler.h" // This must be the last #include.

using SimEncryptKeyId = uint64_t;
using SimEncryptDomainId = uint64_t;
using SimEncryptKey = std::string;

struct SimEncryptVaultProxyInterface {
	constexpr static FileIdentifier file_identifier = 2416711;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct SimGetEncryptKeyByKeyIdRequest> encryptKeyLookupByKeyId;
	RequestStream<struct SimGetEncryptKeyByDomainIdRequest> encryptKeyLookupByDomainId;

	SimEncryptVaultProxyInterface() {}

	UID id() const { return encryptKeyLookupByKeyId.getEndpoint().token; }
	template <class Archive>
	void serialize(Archive& ar) {
		if constexpr (!is_fb_function<Archive>) {
			ASSERT(ar.protocolVersion().isValid());
		}
		serializer(ar, waitFailure);
		if (Archive::isDeserializing) {
			encryptKeyLookupByKeyId =
			    RequestStream<struct GetCommitVersionRequest>(waitFailure.getEndpoint().getAdjustedEndpoint(1));
			encryptKeyLookupByDomainId =
			    RequestStream<struct GetRawCommittedVersionRequest>(waitFailure.getEndpoint().getAdjustedEndpoint(2));
		}
	}

	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(waitFailure.getReceiver());
		streams.push_back(encryptKeyLookupByKeyId.getReceiver(TaskPriority::DefaultPromiseEndpoint));
		streams.push_back(encryptKeyLookupByDomainId.getReceiver(TaskPriority::DefaultPromiseEndpoint));
		FlowTransport::transport().addEndpoints(streams);
	}
};

struct SimGetEncryptKeyByKeyIdReply {
	constexpr static FileIdentifier file_identifier = 2313778;
	Standalone<StringRef> encryptKey;

	SimGetEncryptKeyByKeyIdReply() : encryptKey(StringRef()) {}
	explicit SimGetEncryptKeyByKeyIdReply(Standalone<StringRef> key) : encryptKey(key) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptKey);
	}
};

struct SimGetEncryptKeyByKeyIdRequest {
	constexpr static FileIdentifier file_identifier = 6913396;
	SimEncryptKeyId encryptKeyId;
	ReplyPromise<SimGetEncryptKeyByKeyIdReply> reply;

	SimGetEncryptKeyByKeyIdRequest() : encryptKeyId(0) {}
	explicit SimGetEncryptKeyByKeyIdRequest(SimEncryptKeyId keyId) : encryptKeyId(keyId) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptKeyId, reply);
	}
};

struct SimGetEncryptKeyByDomainIdReply {
	constexpr static FileIdentifier file_identifier = 3009025;
	SimEncryptDomainId encryptKeyId;
	Standalone<StringRef> encryptKey;

	SimGetEncryptKeyByDomainIdReply() : encryptKeyId(0), encryptKey(StringRef()) {}
	explicit SimGetEncryptKeyByDomainIdReply(SimEncryptKeyId keyId, Standalone<StringRef> key)
	  : encryptKeyId(keyId), encryptKey(key) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptKeyId, encryptKey);
	}
};

struct SimGetEncryptKeyByDomainIdRequest {
	constexpr static FileIdentifier file_identifier = 9918682;
	SimEncryptDomainId encryptDomainId;
	ReplyPromise<SimGetEncryptKeyByDomainIdReply> reply;

	SimGetEncryptKeyByDomainIdRequest() : encryptDomainId(0) {}
	explicit SimGetEncryptKeyByDomainIdRequest(SimEncryptDomainId domainId) : encryptDomainId(domainId) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptDomainId, reply);
	}
};

ACTOR Future<Void> simEncryptVaultProxyCore(struct SimEncryptVaultProxyInterface interf, uint32_t maxEncryptKeys);

#include "flow/unactorcompiler.h"
#endif // FDBSERVER_SIMENCRYPTVAULTPROXY_ACTOR_H
