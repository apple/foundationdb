/*
 * ProcessInterface.h
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

#include "fdbclient/AnnotateActor.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbclient/WellKnownEndpoints.h"

struct ProcessInterface {
	constexpr static FileIdentifier file_identifier = 985636;
	RequestStream<struct GetProcessInterfaceRequest> getInterface;
	RequestStream<struct ActorLineageRequest> actorLineage;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, actorLineage);
	}
};

struct GetProcessInterfaceRequest {
	constexpr static FileIdentifier file_identifier = 7632546;
	ReplyPromise<ProcessInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

// This type is used to send serialized sample data over the network.
struct SerializedSample {
	constexpr static FileIdentifier file_identifier = 15785634;

	double time;
	std::unordered_map<WaitState, std::string> data;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, time, data);
	}
};

struct ActorLineageReply {
	constexpr static FileIdentifier file_identifier = 1887656;
	std::vector<SerializedSample> samples;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, samples);
	}
};

struct ActorLineageRequest {
	constexpr static FileIdentifier file_identifier = 11654765;
	WaitState waitStateStart, waitStateEnd;
	time_t timeStart, timeEnd;
	ReplyPromise<ActorLineageReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, waitStateStart, waitStateEnd, timeStart, timeEnd, reply);
	}
};
