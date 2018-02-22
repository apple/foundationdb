/*
 * ClientWorkerInterface.h
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

#ifndef FDBCLIENT_CLIENTWORKERINTERFACE_H
#define FDBCLIENT_CLIENTWORKERINTERFACE_H
#pragma once

#include "FDBTypes.h"
#include "fdbrpc/FailureMonitor.h"
#include "Status.h"
#include "ClientDBInfo.h"

// Streams from WorkerInterface that are safe and useful to call from a client.
// A ClientWorkerInterface is embedded as the first element of a WorkerInterface.
struct ClientWorkerInterface {
	RequestStream< struct RebootRequest > reboot;
	RequestStream< struct ProfilerRequest > profiler;

	bool operator == (ClientWorkerInterface const& r) const { return id() == r.id(); }
	bool operator != (ClientWorkerInterface const& r) const { return id() != r.id(); }
	UID id() const { return reboot.getEndpoint().token; }
	NetworkAddress address() const { return reboot.getEndpoint().address; }

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & reboot & profiler;
	}
};

struct RebootRequest {
	bool deleteData;
	bool checkData;

	explicit RebootRequest(bool deleteData = false, bool checkData = false) : deleteData(deleteData), checkData(checkData) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & deleteData & checkData;
	}
};

struct ProfilerRequest {
	ReplyPromise<Void> reply;

	enum class Type : std::int8_t {
		GPROF = 1,
		FLOW = 2
	};

	enum class Action : std::int8_t {
		DISABLE = 0,
		ENABLE = 1,
		RUN = 2
	};

	Type type;
	Action action;
	int duration;
	Standalone<StringRef> outputFile;

	template<class Ar>
	void serialize( Ar& ar ) {
		ar & reply & type & action & duration & outputFile;
	}
};
BINARY_SERIALIZABLE( ProfilerRequest::Type );
BINARY_SERIALIZABLE( ProfilerRequest::Action );

#endif
