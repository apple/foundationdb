/*
 * RpcProxyInterface.h
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

#ifndef FDBSERVER_RPCPROXYINTERFACE_H
#define FDBSERVER_RPCPROXYINTERFACE_H
#pragma once

#include "fdbclient/FDBTypes.h"

struct RpcProxyInterface {
	constexpr static FileIdentifier file_identifier = 16308514;

	RequestStream<ReplyPromise<Void>> ping;

	LocalityData locality;
	UID uniqueId;

	RpcProxyInterface() {}
	explicit RpcProxyInterface(const LocalityData& locality) : uniqueId( deterministicRandom()->randomUniqueID() ), locality(locality) {}
	RpcProxyInterface(UID uniqueId, const LocalityData& locality) : uniqueId(uniqueId), locality(locality) {}
	UID id() const { return uniqueId; }
	std::string toString() const { return id().shortString(); }
	bool operator == ( RpcProxyInterface const& r ) const { return id() == r.id(); }
	NetworkAddress address() const { return ping.getEndpoint().getPrimaryAddress(); }
	void initEndpoints() {
		ping.getEndpoint( TaskPriority::DefaultPromiseEndpoint );
	}

	template <class Ar> 
	void serialize( Ar& ar ) {
		if constexpr (!is_fb_function<Ar>) {
			ASSERT(ar.isDeserializing || uniqueId != UID());
		}
		serializer(ar, uniqueId, locality, ping);
	}
};

#endif
