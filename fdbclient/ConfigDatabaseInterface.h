/*
 * ConfigDatabaseInterface.h
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

struct ConfigDatabaseGetVersionReply {
	static constexpr FileIdentifier file_identifier = 2934851;
	ConfigDatabaseGetVersionReply() = default;
	ConfigDatabaseGetVersionReply(Version version) : version(version) {}
	Version version;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version);
	}
};

struct ConfigDatabaseGetVersionRequest {
	static constexpr FileIdentifier file_identifier = 138941;
	ReplyPromise<ConfigDatabaseGetVersionReply> reply;
	ConfigDatabaseGetVersionRequest() = default;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct ConfigDatabaseGetReply {
	static constexpr FileIdentifier file_identifier = 2034110;
	Optional<Value> value;
	ConfigDatabaseGetReply() = default;
	explicit ConfigDatabaseGetReply(Value const& value) : value(Optional<Value>(value)) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, value);
	}
};

struct ConfigDatabaseGetRequest {
	static constexpr FileIdentifier file_identifier = 923040;
	Version version;
	KeyRef key;
	ReplyPromise<ConfigDatabaseGetReply> reply;

	ConfigDatabaseGetRequest() = default;
	explicit ConfigDatabaseGetRequest(Version version, KeyRef key) : version(version), key(key) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, key, reply);
	}
};

struct ConfigDatabaseCommitRequest {
	static constexpr FileIdentifier file_identifier = 103841;
	Version version;
	VectorRef<MutationRef> mutations;
	ReplyPromise<Void> reply;

	ConfigDatabaseCommitRequest() = default;
	ConfigDatabaseCommitRequest(Version version, VectorRef<MutationRef> mutations)
	  : version(version), mutations(mutations) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, mutations, reply);
	}
};

struct ConfigDatabaseInterface {
	static constexpr FileIdentifier file_identifier = 982485;
	struct RequestStream<ConfigDatabaseGetVersionRequest> getVersion;
	struct RequestStream<ConfigDatabaseGetRequest> get;
	struct RequestStream<ConfigDatabaseCommitRequest> commit;

	ConfigDatabaseInterface() {
		getVersion.makeWellKnownEndpoint(WLTOKEN_CONFIGDB_GETVERSION, TaskPriority::Coordination);
		get.makeWellKnownEndpoint(WLTOKEN_CONFIGDB_GET, TaskPriority::Coordination);
		commit.makeWellKnownEndpoint(WLTOKEN_CONFIGDB_COMMIT, TaskPriority::Coordination);
	}

	ConfigDatabaseInterface(NetworkAddress const& remote)
	  : getVersion(Endpoint({ remote }, WLTOKEN_CONFIGDB_GETVERSION)), get(Endpoint({ remote }, WLTOKEN_CONFIGDB_GET)),
	    commit(Endpoint({ remote }, WLTOKEN_CONFIGDB_GET)) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, getVersion, get, commit);
	}
};
