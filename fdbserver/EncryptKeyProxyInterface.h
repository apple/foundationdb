/*
 * EncryptKeyProxyInterface.h
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

#ifndef FDBSERVER_ENCRYPTKEYPROXYINTERFACE_H
#define FDBSERVER_ENCRYPTKEYPROXYINTERFACE_H
#include "flow/FileIdentifier.h"
#include "flow/network.h"
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"

struct EncryptKeyProxyInterface {
	constexpr static FileIdentifier file_identifier = 1303419;
	struct LocalityData locality;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct HaltEncryptKeyProxyRequest> haltEncryptKeyProxy;
	UID myId;

	EncryptKeyProxyInterface() {}
	explicit EncryptKeyProxyInterface(const struct LocalityData& loc, UID id) : locality(loc), myId(id) {}

	void initEndpoints() {}
	UID id() const { return myId; }
	NetworkAddress address() const { return waitFailure.getEndpoint().getPrimaryAddress(); }
	bool operator==(const EncryptKeyProxyInterface& toCompare) const { return myId == toCompare.myId; }
	bool operator!=(const EncryptKeyProxyInterface& toCompare) const { return !(*this == toCompare); }

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, waitFailure, locality, myId);
	}
};

struct HaltEncryptKeyProxyRequest {
	constexpr static FileIdentifier file_identifier = 2378138;
	UID requesterID;
	ReplyPromise<Void> reply;

	HaltEncryptKeyProxyRequest() {}
	explicit HaltEncryptKeyProxyRequest(UID uid) : requesterID(uid) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, requesterID, reply);
	}
};

#endif