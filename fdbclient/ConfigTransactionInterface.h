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
#include "fdbclient/ConfigKnobs.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbrpc/fdbrpc.h"
#include "flow/flow.h"

struct ConfigTransactionGetVersionReply {
	static constexpr FileIdentifier file_identifier = 2934851;
	ConfigTransactionGetVersionReply() = default;
	explicit ConfigTransactionGetVersionReply(Version version) : version(version) {}
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
	explicit ConfigTransactionGetReply(Optional<Value> const& value) : value(Optional<Value>(value)) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, value);
	}
};

struct ConfigTransactionGetRequest {
	static constexpr FileIdentifier file_identifier = 923040;
	Version version;
	ConfigKey key;
	ReplyPromise<ConfigTransactionGetReply> reply;

	ConfigTransactionGetRequest() = default;
	explicit ConfigTransactionGetRequest(Version version, ConfigKey key) : version(version), key(key) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, key, reply);
	}
};

struct ConfigTransactionCommitRequest {
	static constexpr FileIdentifier file_identifier = 103841;
	Version version;
	Standalone<VectorRef<ConfigMutationRef>> mutations;
	ReplyPromise<Void> reply;

	ConfigTransactionCommitRequest() = default;
	ConfigTransactionCommitRequest(Version version, Standalone<VectorRef<ConfigMutationRef>> mutations)
	  : version(version), mutations(mutations) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, mutations, reply);
	}
};

struct ConfigTransactionGetRangeReply {
	static constexpr FileIdentifier file_identifier = 430263;
	Standalone<RangeResultRef> range;

	ConfigTransactionGetRangeReply() = default;
	explicit ConfigTransactionGetRangeReply(Standalone<RangeResultRef> range) : range(range) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, range);
	}
};

struct ConfigTransactionGetRangeRequest {
	static constexpr FileIdentifier file_identifier = 987410;
	Arena arena;
	Version version;
	KeyRef configClass;
	KeyRangeRef knobNameRange;
	ReplyPromise<ConfigTransactionGetRangeReply> reply;

	ConfigTransactionGetRangeRequest() = default;
	explicit ConfigTransactionGetRangeRequest(Arena& arena,
	                                          Version version,
	                                          KeyRef configClass,
	                                          KeyRangeRef knobNameRange)
	  : arena(arena), version(version), configClass(arena, configClass), knobNameRange(arena, knobNameRange) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, arena, version, configClass, knobNameRange, reply);
	}
};

struct ConfigTransactionInterface {
	UID _id;

public:
	static constexpr FileIdentifier file_identifier = 982485;
	struct RequestStream<ConfigTransactionGetVersionRequest> getVersion;
	struct RequestStream<ConfigTransactionGetRequest> get;
	struct RequestStream<ConfigTransactionGetRangeRequest> getRange;
	struct RequestStream<ConfigTransactionCommitRequest> commit;

	ConfigTransactionInterface();
	void setupWellKnownEndpoints();
	ConfigTransactionInterface(NetworkAddress const& remote);

	bool operator==(ConfigTransactionInterface const& rhs) const;
	bool operator!=(ConfigTransactionInterface const& rhs) const;
	UID id() const { return _id; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, getVersion, get, getRange, commit);
	}
};
