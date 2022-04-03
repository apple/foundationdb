
/*
 * GrvProxyInterface.h
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

#ifndef FDBCLIENT_GRVPROXYINTERFACE_H
#define FDBCLIENT_GRVPROXYINTERFACE_H
#pragma once
#include "flow/FileIdentifier.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbclient/FDBTypes.h"

// GrvProxy is proxy primarily specializing on serving GetReadVersion. It also serves health metrics since it
// communicates with RateKeeper to gather health information of the cluster.
struct GrvProxyInterface {
	constexpr static FileIdentifier file_identifier = 8743216;
	enum { LocationAwareLoadBalance = 1 };
	enum { AlwaysFresh = 1 };

	Optional<Key> processId;
	bool provisional;

	RequestStream<struct GetReadVersionRequest>
	    getConsistentReadVersion; // Returns a version which (1) is committed, and (2) is >= the latest version reported
	                              // committed (by a commit response) when this request was sent
	//   (at some point between when this request is sent and when its response is received, the latest version reported
	//   committed)
	RequestStream<ReplyPromise<Void>> waitFailure; // reports heartbeat to master.
	RequestStream<struct GetHealthMetricsRequest> getHealthMetrics;

	UID id() const { return getConsistentReadVersion.getEndpoint().token; }
	std::string toString() const { return id().shortString(); }
	bool operator==(GrvProxyInterface const& r) const { return id() == r.id(); }
	bool operator!=(GrvProxyInterface const& r) const { return id() != r.id(); }
	NetworkAddress address() const { return getConsistentReadVersion.getEndpoint().getPrimaryAddress(); }
	NetworkAddressList addresses() const { return getConsistentReadVersion.getEndpoint().addresses; }

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, processId, provisional, getConsistentReadVersion);
		if (Archive::isDeserializing) {
			waitFailure =
			    RequestStream<ReplyPromise<Void>>(getConsistentReadVersion.getEndpoint().getAdjustedEndpoint(1));
			getHealthMetrics = RequestStream<struct GetHealthMetricsRequest>(
			    getConsistentReadVersion.getEndpoint().getAdjustedEndpoint(2));
		}
	}

	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(getConsistentReadVersion.getReceiver(TaskPriority::ReadSocket));
		streams.push_back(waitFailure.getReceiver());
		streams.push_back(getHealthMetrics.getReceiver());
		FlowTransport::transport().addEndpoints(streams);
	}
};

#endif // FDBCLIENT_GRVPROXYINTERFACE_H
