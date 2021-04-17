/*
 * ConfigTransactionInterface.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbrpc/fdbrpc.h"
#include "flow/flow.h"

struct ConfigTransactionGetVersionReply {
	static constexpr FileIdentifier file_identifier = 2934851;
	ConfigTransactionGetVersionReply() = default;
	ConfigTransactionGetVersionReply(Version version) : version(version) {}
	Version version;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version);
	}
};

struct ConfigTransactionGetVersionRequest {
	static constexpr FileIdentifier file_identifier = 138941;
	ReplyPromise<ConfigTransactionGetVersionReply> reply;
	ConfigTransactionGetVersionRequest() = default;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct ConfigTransactionGetReply {
	static constexpr FileIdentifier file_identifier = 2034110;
	Optional<Value> value;
	ConfigTransactionGetReply() = default;
	explicit ConfigTransactionGetReply(Value const& value) : value(Optional<Value>(value)) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, value);
	}
};

struct ConfigTransactionGetRequest {
	static constexpr FileIdentifier file_identifier = 923040;
	Version version;
	KeyRef key;
	ReplyPromise<ConfigTransactionGetReply> reply;

	ConfigTransactionGetRequest() = default;
	explicit ConfigTransactionGetRequest(Version version, KeyRef key) : version(version), key(key) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, key, reply);
	}
};

struct ConfigTransactionCommitRequest {
	static constexpr FileIdentifier file_identifier = 103841;
	Version version;
	Standalone<VectorRef<MutationRef>> mutations;
	ReplyPromise<Void> reply;

	ConfigTransactionCommitRequest() = default;
	ConfigTransactionCommitRequest(Version version, Standalone<VectorRef<MutationRef>> mutations)
	  : version(version), mutations(mutations) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, mutations, reply);
	}
};

struct ConfigTransactionInterface {
	static constexpr FileIdentifier file_identifier = 982485;
	struct RequestStream<ConfigTransactionGetVersionRequest> getVersion;
	struct RequestStream<ConfigTransactionGetRequest> get;
	struct RequestStream<ConfigTransactionCommitRequest> commit;

	ConfigTransactionInterface() = default;

	void setupWellKnownEndpoints() {
		getVersion.makeWellKnownEndpoint(WLTOKEN_CONFIGTXN_GETVERSION, TaskPriority::Coordination);
		get.makeWellKnownEndpoint(WLTOKEN_CONFIGTXN_GET, TaskPriority::Coordination);
		commit.makeWellKnownEndpoint(WLTOKEN_CONFIGTXN_COMMIT, TaskPriority::Coordination);
	}

	ConfigTransactionInterface(NetworkAddress const& remote)
	  : getVersion(Endpoint({ remote }, WLTOKEN_CONFIGTXN_GETVERSION)),
	    get(Endpoint({ remote }, WLTOKEN_CONFIGTXN_GET)), commit(Endpoint({ remote }, WLTOKEN_CONFIGTXN_COMMIT)) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, getVersion, get, commit);
	}
};
