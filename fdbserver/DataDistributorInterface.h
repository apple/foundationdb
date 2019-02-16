/*
 * DataDistributorInterface.h
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

#ifndef FDBSERVER_DATADISTRIBUTORINTERFACE_H
#define FDBSERVER_DATADISTRIBUTORINTERFACE_H

#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"

struct DataDistributorInterface {
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct GetRateInfoRequest> getRateInfo;
	struct LocalityData locality;

	DataDistributorInterface() {}
	explicit DataDistributorInterface(const struct LocalityData& l) : locality(l) {}

	void initEndpoints() {}
	UID id() const { return getRateInfo.getEndpoint().token; }
	NetworkAddress address() const { return getRateInfo.getEndpoint().getPrimaryAddress(); }
	bool operator== (const DataDistributorInterface& r) const {
		return id() == r.id();
	}
	bool operator!= (const DataDistributorInterface& r) const {
		return !(*this == r);
	}

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, waitFailure, getRateInfo, locality);
	}
};

struct GetRateInfoRequest {
	UID requesterID;
	int64_t totalReleasedTransactions;
	ReplyPromise<struct GetRateInfoReply> reply;

	GetRateInfoRequest() {}
	GetRateInfoRequest( UID const& requesterID, int64_t totalReleasedTransactions ) : requesterID(requesterID), totalReleasedTransactions(totalReleasedTransactions) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, requesterID, totalReleasedTransactions, reply);
	}
};

struct GetRateInfoReply {
	double transactionRate;
	double leaseDuration;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, transactionRate, leaseDuration);
	}
};

#endif //FDBSERVER_DATADISTRIBUTORINTERFACE_H
