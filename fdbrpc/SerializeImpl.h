/*
 * SerializeImpl.h
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

#ifndef FDBRPC_SERIALIZE_IMPL_H
#define FDBRPC_SERIALIZE_IMPL_H

#include "flow/SerializeImpl.h"
#include "fdbrpc/fdbrpc.h"

template <class T>
void NetworkSendAndReceive<T>::receive(NetSAV<T>* self, ArenaReader& reader) {
	auto sav = static_cast<SAV<T>*>(self);
	if (!sav->canBeSet()) return; // load balancing and retries can result in the same request being answered twice
	self->addPromiseRef();
	bool ok;
	SerializedMsg<ArenaReader, bool>::deserialize(reader, ok);
	// reader >> ok;
	if (ok) {
		T message;
		SerializedMsg<ArenaReader, T>::deserialize(reader, message);
		// reader >> message;
		sav->sendAndDelPromiseRef(message);
	} else {
		Error error;
		SerializedMsg<ArenaReader, Error>::deserialize(reader, error);
		// reader >> error;
		sav->sendErrorAndDelPromiseRef(error);
	}
}

template <class T>
void NetworkSendAndReceive<T>::receive(NetSAV<T>* self, ArenaObjectReader& reader) {
	auto sav = static_cast<SAV<T>*>(self);
	if (!sav->canBeSet()) return;
	self->addPromiseRef();
	ErrorOr<EnsureTable<T>> message;
	ObjectSerializedMsg<ErrorOr<EnsureTable<T>>>::deserialize(reader, message);
	// reader.deserialize(message);
	if (message.isError()) {
		sav->sendErrorAndDelPromiseRef(message.getError());
	} else {
		sav->sendAndDelPromiseRef(message.get().asUnderlyingType());
	}
}

template <class T>
void NetworkSendAndReceive<T>::receive(NetNotifiedQueue<T>* self, ArenaReader& reader) {
	self->addPromiseRef();
	T message;
	SerializedMsg<ArenaReader, T>::deserialize(reader, message);
	self->send(std::move(message));
	self->delPromiseRef();
}

template <class T>
void NetworkSendAndReceive<T>::receive(NetNotifiedQueue<T>* self, ArenaObjectReader& reader) {
	self->addPromiseRef();
	T message;
	ObjectSerializedMsg<T>::deserialize(reader, message);
	// reader.deserialize(message);
	self->send(std::move(message));
	self->delPromiseRef();
}

template <class T>
void NetworkSendAndReceive<T>::sendUnrealiable(const T& value, Endpoint e) {
	FlowTransport::transport().sendUnreliable(SerializeSource<T>(value), e);
}

template <class T>
PacketID NetworkSendAndReceive<T>::sendReliable(const T& value, Endpoint e) {
	return FlowTransport::transport().sendReliable(SerializeSource<T>(value), e);
}

template <class T>
void NetworkSendAndReceive<T>::sendErrorOr(const T& value, Endpoint e) {
	if (g_network->useObjectSerializer()) {
		FlowTransport::transport().sendUnreliable(SerializeSource<ErrorOr<EnsureTable<T>>>(value), e);
	} else {
		FlowTransport::transport().sendUnreliable(SerializeBoolAnd<T>(true, value), e, false);
	}
}

template <class T>
void NetworkSendAndReceive<T>::sendError(const Error& err, Endpoint endpoint) {
	if (g_network->useObjectSerializer()) {
		FlowTransport::transport().sendUnreliable(SerializeSource<ErrorOr<EnsureTable<T>>>(err), endpoint);
	} else {
		FlowTransport::transport().sendUnreliable(SerializeBoolAnd<Error>(false, err), endpoint, false);
	}
}

#define IMPLEMENT_SERIALIZATION_FOR(T) template struct NetworkSendAndReceive<T>;

#endif
