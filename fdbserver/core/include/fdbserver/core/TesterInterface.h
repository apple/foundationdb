/*
 * TesterInterface.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#pragma once

#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/PerfMetric.h"
#include "fdbclient/NativeAPI.actor.h"

struct CheckReply {
	constexpr static FileIdentifier file_identifier = 11;

	bool value = false;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, value);
	}
};

struct WorkloadInterface {
	constexpr static FileIdentifier file_identifier = 4454551;
	RequestStream<ReplyPromise<Void>> setup;
	RequestStream<ReplyPromise<Void>> start;
	RequestStream<ReplyPromise<CheckReply>> check;
	RequestStream<ReplyPromise<std::vector<PerfMetric>>> metrics;
	RequestStream<ReplyPromise<Void>> stop;

	UID id() const { return setup.getEndpoint().token; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, setup, start, check, metrics, stop);
	}
};

struct WorkloadRequest {
	constexpr static FileIdentifier file_identifier = 8121024;
	Arena arena;
	StringRef title;
	int timeout;
	double databasePingDelay;
	int64_t sharedRandomNumber;
	bool useDatabase;
	bool runFailureWorkloads = true;

	// The vector of option lists are to construct compound workloads.  If there
	//  is only one workload to be run...pass just one list of options!
	//
	// Options are well-defined...and each workload has different defaults
	//	 Parameter				Description
	// - testName				the name of the test to run
	// - testDuration			in seconds
	// - transactionsPerSecond
	// - actorsPerClient
	// - nodeCount

	VectorRef<VectorRef<KeyValueRef>> options;

	std::vector<KeyRange> rangesToCheck; // For consistency checker urgent

	int clientId; // the "id" of the client receiving the request (0 indexed)
	int clientCount; // the total number of test clients participating in the workload
	ReplyPromise<struct WorkloadInterface> reply;
	std::vector<std::string> disabledFailureInjectionWorkloads;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           title,
		           timeout,
		           databasePingDelay,
		           sharedRandomNumber,
		           useDatabase,
		           options,
		           clientId,
		           clientCount,
		           reply,
		           runFailureWorkloads,
		           disabledFailureInjectionWorkloads,
		           rangesToCheck,
		           arena);
	}
};

struct TesterInterface {
	constexpr static FileIdentifier file_identifier = 4465210;
	RequestStream<WorkloadRequest> recruitments;

	UID id() const { return recruitments.getEndpoint().token; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, recruitments);
	}
};
