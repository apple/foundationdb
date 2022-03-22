/*
 * TesterInterface.actor.h
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

#pragma once

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_TESTERINTERFACE_ACTOR_G_H)
#define FDBSERVER_TESTERINTERFACE_ACTOR_G_H
#include "fdbserver/TesterInterface.actor.g.h"
#elif !defined(FDBSERVER_TESTERINTERFACE_ACTOR_H)
#define FDBSERVER_TESTERINTERFACE_ACTOR_H

#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/PerfMetric.h"
#include "fdbclient/NativeAPI.actor.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // has to be last include
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
	Optional<TenantNameRef> defaultTenant;

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

	int clientId; // the "id" of the client receiving the request (0 indexed)
	int clientCount; // the total number of test clients participating in the workload
	ReplyPromise<struct WorkloadInterface> reply;

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
		           defaultTenant,
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

ACTOR Future<Void> testerServerCore(TesterInterface interf,
                                    Reference<IClusterConnectionRecord> ccr,
                                    Reference<AsyncVar<struct ServerDBInfo> const> serverDBInfo,
                                    LocalityData locality);

enum test_location_t { TEST_HERE, TEST_ON_SERVERS, TEST_ON_TESTERS };
enum test_type_t { TEST_TYPE_FROM_FILE, TEST_TYPE_CONSISTENCY_CHECK, TEST_TYPE_UNIT_TESTS };

ACTOR Future<Void> runTests(Reference<IClusterConnectionRecord> connRecord,
                            test_type_t whatToRun,
                            test_location_t whereToRun,
                            int minTestersExpected,
                            std::string fileName = std::string(),
                            StringRef startingConfiguration = StringRef(),
                            LocalityData locality = LocalityData(),
                            UnitTestParameters testOptions = UnitTestParameters(),
                            Optional<TenantName> defaultTenant = Optional<TenantName>());

#include "flow/unactorcompiler.h"
#endif
