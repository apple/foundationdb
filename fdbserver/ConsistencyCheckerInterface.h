/*
 * ConsistencyCheckerInterface.h
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

#ifndef FDBSERVER_CONSISTENCYCHECKERINTERFACE_H
#define FDBSERVER_CONSISTENCYCHECKERINTERFACE_H

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"

struct ConsistencyCheckerInterface {
	constexpr static FileIdentifier file_identifier = 4983265;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct HaltConsistencyCheckerRequest> haltConsistencyChecker;
	struct LocalityData locality;
	UID myId;
	//RequestStream<struct ConsistencyCheckRequest> consistencyCheckerCheckReq;

	ConsistencyCheckerInterface() {}
	explicit ConsistencyCheckerInterface(const struct LocalityData& l, UID id) : locality(l), myId(id) {}

	void initEndpoints() {}
	UID id() const { return myId; }
	NetworkAddress address() const { return waitFailure.getEndpoint().getPrimaryAddress(); }
	bool operator==(const ConsistencyCheckerInterface& r) const { return id() == r.id(); }
	bool operator!=(const ConsistencyCheckerInterface& r) const { return !(*this == r); }

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, waitFailure, haltConsistencyChecker, locality, myId);
	}
};

struct HaltConsistencyCheckerRequest {
	constexpr static FileIdentifier file_identifier = 2323417;
	UID requesterID;
	ReplyPromise<Void> reply;

	HaltConsistencyCheckerRequest() {}
	explicit HaltConsistencyCheckerRequest(UID uid) : requesterID(uid) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, requesterID, reply);
	}
};

//TODO: NEELAM: mpst likely will be removed
//struct ConsistencyCheckRequest {
//	constexpr static FileIdentifier file_identifier = 3485239;
//	bool state;
//	double maxRate;
//	double targetInterval;
//	UID requesterID;
//	ReplyPromise<Void> reply;
//
//	explicit ConsistencyCheckRequest(Optional<UID> const& requesterID = Optional<UID>()) : requesterID(requesterID) {}
//	explicit ConsistencyCheckRequest(bool state, double maxRate, double targetInterval, UID reqUID, Optional<UID> requesterID = Optional<UID>())
//		: state(state), maxRate(maxRate), targetInterval(targetInterval), requesterID(reqUID), requesterID(requesterID) {}
//
//	template <class Ar>
//	void serialize(Ar& ar) {
//		serializer(ar, state, maxRate, targetInterval, requesterID, reply);
//	}
//};

#endif // FDBSERVER_CONSISTENCYCHECKERINTERFACE_H
