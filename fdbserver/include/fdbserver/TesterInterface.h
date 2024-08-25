/*
 * TesterInterface.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_TESTERINTERFACE_H
#define FDBSERVER_TESTERINTERFACE_H

#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/PerfMetric.h"
#include "fdbclient/NativeAPI.actor.h"
#include "flow/UnitTest.h"

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
		           defaultTenant,
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

Future<Void> testerServerCore(const TesterInterface& interf,
                              Reference<IClusterConnectionRecord> ccr,
                              Reference<AsyncVar<struct ServerDBInfo> const> serverDBInfo,
                              LocalityData locality,
                              Optional<std::string> expectedWorkLoad = Optional<std::string>());

enum test_location_t { TEST_HERE, TEST_ON_SERVERS, TEST_ON_TESTERS };
enum test_type_t {
	TEST_TYPE_FROM_FILE,
	TEST_TYPE_CONSISTENCY_CHECK,
	TEST_TYPE_UNIT_TESTS,
	TEST_TYPE_CONSISTENCY_CHECK_URGENT
};

Future<Void> runTests(
    const Reference<IClusterConnectionRecord>& connRecord,
    test_type_t whatToRun,
    test_location_t whereToRun,
    int minTestersExpected,
    const std::string& fileName = std::string(),
    StringRef startingConfiguration = StringRef(),
    const LocalityData& locality = LocalityData(),
    const UnitTestParameters& testOptions = UnitTestParameters(),
    const Optional<TenantName>& defaultTenant = Optional<TenantName>(),
    const Standalone<VectorRef<TenantNameRef>>& tenantsToCreate = Standalone<VectorRef<TenantNameRef>>(),
    bool restartingTest = false);

#endif // FDBSERVER_TESTERINTERFACE_H
