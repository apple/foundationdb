/*
 * TesterInterface.h
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

#ifndef FDBSERVER_TESTERINTERFACE_H
#define FDBSERVER_TESTERINTERFACE_H
#pragma once

#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/PerfMetric.h"
#include "fdbclient/NativeAPI.h"

struct WorkloadInterface {
	RequestStream<ReplyPromise<Void>> setup;
	RequestStream<ReplyPromise<Void>> start;
	RequestStream<ReplyPromise<bool>> check;
	RequestStream<ReplyPromise< std::vector<PerfMetric> > > metrics;
	RequestStream<ReplyPromise<Void>> stop;

	UID id() const { return setup.getEndpoint().token; }

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, setup, start, check, metrics, stop);
	}
};

struct WorkloadRequest {
	Arena arena;
	StringRef title;
	int timeout;
	double databasePingDelay;
	int64_t sharedRandomNumber;
	bool useDatabase;

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

	VectorRef< VectorRef<KeyValueRef> > options;

	int clientId;				// the "id" of the client recieving the request (0 indexed)
	int clientCount;			// the total number of test clients participating in the workload
	ReplyPromise< struct WorkloadInterface > reply;

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, title, timeout, databasePingDelay, sharedRandomNumber, useDatabase, options, clientId, clientCount, reply, arena);
	}
};

struct TesterInterface {
	RequestStream<WorkloadRequest> recruitments;

	UID id() const { return recruitments.getEndpoint().token; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, recruitments);
	}
};

Future<Void> testerServerCore( TesterInterface const& interf, Reference<ClusterConnectionFile> const& ccf, Reference<AsyncVar<struct ServerDBInfo>> const&, LocalityData const& );

enum test_location_t { TEST_HERE, TEST_ON_SERVERS, TEST_ON_TESTERS };
enum test_type_t { TEST_TYPE_FROM_FILE, TEST_TYPE_CONSISTENCY_CHECK };

Future<Void> runTests(Reference<ClusterConnectionFile> const& connFile, test_type_t const& whatToRun,
                      test_location_t const& whereToRun, int const& minTestersExpected,
                      std::string const& fileName = std::string(), StringRef const& startingConfiguration = StringRef(),
                      LocalityData const& locality = LocalityData());

#endif
