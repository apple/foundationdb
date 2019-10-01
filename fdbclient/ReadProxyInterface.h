/*
 * ReadProxyInterface.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBCLIENT_READPROXYINTERFACE_H
#define FDBCLIENT_READPROXYINTERFACE_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbclient/StorageServerInterface.h"

struct ReadProxyInterface {
	constexpr static FileIdentifier file_identifier = 9348383; // FIXME: Fix this

	RequestStream<struct GetValueRequest> getValue;
	RequestStream<struct GetKeyRequest> getKey;
	RequestStream<struct GetKeyValuesRequest> getKeyValues;

	RequestStream<ReplyPromise<Void>> waitFailure;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, getValue, getKey, getKeyValues, waitFailure);
	}

	void initEndpoints() {
		getValue.getEndpoint( TaskPriority::LoadBalancedEndpoint );
		getKey.getEndpoint( TaskPriority::LoadBalancedEndpoint );
		getKeyValues.getEndpoint( TaskPriority::LoadBalancedEndpoint );
	}

	UID id() const { return getValue.getEndpoint().token; }
	std::string toString() const { return id().shortString(); }
	bool operator == (ReadProxyInterface const& r) const { return id() == r.id(); }
	bool operator != (ReadProxyInterface const& r) const { return id() != r.id(); }
	NetworkAddress address() const { return getValue.getEndpoint().getPrimaryAddress(); }
};

#endif // FDBCLIENT_READPROXYINTERFACE_H
