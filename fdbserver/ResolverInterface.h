/*
 * ResolverInterface.h
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

#ifndef FDBSERVER_RESOLVERINTERFACE_H
#define FDBSERVER_RESOLVERINTERFACE_H
#pragma once

#include "fdbclient/FDBTypes.h"

struct ResolverInterface {
	enum { LocationAwareLoadBalance = 1 };
	enum { AlwaysFresh = 1 };

	LocalityData locality;
	UID uniqueID;
	RequestStream< struct ResolveTransactionBatchRequest > resolve;
	RequestStream< struct ResolutionMetricsRequest > metrics;
	RequestStream< struct ResolutionSplitRequest > split;

	RequestStream<ReplyPromise<Void>> waitFailure;

	ResolverInterface() : uniqueID( g_random->randomUniqueID() ) {}
	UID id() const { return uniqueID; }
	std::string toString() const { return id().shortString(); }
	bool operator == ( ResolverInterface const& r ) const { return id() == r.id(); }
	bool operator != ( ResolverInterface const& r ) const { return id() != r.id(); }
	NetworkAddress address() const { return resolve.getEndpoint().address; }
	void initEndpoints() {
		metrics.getEndpoint( TaskResolutionMetrics );
		split.getEndpoint( TaskResolutionMetrics );
	}

	template <class Ar> 
	void serialize( Ar& ar ) {
		serializer(ar, uniqueID, locality, resolve, metrics, split, waitFailure);
	}
};

struct StateTransactionRef {
	StateTransactionRef() {}
	StateTransactionRef(const bool committed, VectorRef<MutationRef> const& mutations) : committed(committed), mutations(mutations) {}
	StateTransactionRef(Arena &p, const StateTransactionRef &toCopy) : committed(toCopy.committed), mutations(p, toCopy.mutations) {}
	bool committed;
	VectorRef<MutationRef> mutations;
	size_t expectedSize() const {
		return mutations.expectedSize();
	}

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, committed, mutations);
	}
};

struct ResolveTransactionBatchReply {
	Arena arena;
	VectorRef<uint8_t> committed;
	Optional<UID> debugID;
	VectorRef<VectorRef<StateTransactionRef>> stateMutations;  // [version][transaction#] -> (committed, [mutation#])

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, committed, stateMutations, arena, debugID);
	}

};

struct ResolveTransactionBatchRequest {
	Arena arena;

	Version prevVersion;
	Version version;   // FIXME: ?
	Version lastReceivedVersion;
	VectorRef<CommitTransactionRef> transactions;
	VectorRef<int> txnStateTransactions;   // Offsets of elements of transactions that have (transaction subsystem state) mutations
	ReplyPromise<ResolveTransactionBatchReply> reply;
	Optional<UID> debugID;

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, prevVersion, version, lastReceivedVersion, transactions, txnStateTransactions, reply, arena, debugID);
	}
};

struct ResolutionMetricsRequest {
	ReplyPromise<int64_t> reply;

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, reply);
	}
};

struct ResolutionSplitReply {
	Key key;
	int64_t used;
	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, key, used);
	}

};

struct ResolutionSplitRequest {
	KeyRange range;
	int64_t offset;
	bool front;
	ReplyPromise<ResolutionSplitReply> reply;

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, range, offset, front, reply);
	}
};

#endif
