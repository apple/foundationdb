/*
 * IRpcInterface.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_IRPCINTERFACE_H
#define FDBSERVER_IRPCINTERFACE_H
#pragma once

#include "fdbrpc/fdbrpc.h"
#include "flow/flow.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"

struct RpcCreateTransactionRequest {
	RpcCreateTransactionRequest(Version readVersion) : readVersion(readVersion) {}

	Version readVersion;
	Promise<UID> reply;
};

struct RpcGetValueRequest {
	RpcGetValueRequest(UID transactionId, Key key, bool snapshot) : transactionId(transactionId), key(key), snapshot(snapshot) {}

	UID transactionId;
	Key key;
	bool snapshot;
	Promise<Optional<Value>> reply;
};

struct RpcApplyMutationsRequest {
	RpcApplyMutationsRequest(UID transactionId, Standalone<VectorRef<MutationRef>> mutations) : transactionId(transactionId), mutations(mutations) {}

	UID transactionId;
	Standalone<VectorRef<MutationRef>> mutations;
};

struct RpcCommitRequest {
	RpcCommitRequest(UID transactionId) : transactionId(transactionId) {}

	UID transactionId;
	Promise<std::pair<Version, Standalone<StringRef>>> reply;
};

class RpcInterface {
public:
	PromiseStream<RpcCreateTransactionRequest> createTransaction;
	PromiseStream<RpcGetValueRequest> getValue;
	PromiseStream<RpcApplyMutationsRequest> applyMutations;
	PromiseStream<RpcCommitRequest> commit;
};

class IRpcService {
public:
	virtual Future<Void> run(NetworkAddress address, RpcInterface const& interface) = 0;
	virtual void shutdown() = 0;

	virtual void addref() = 0;
	virtual void delref() = 0;
};

#endif
