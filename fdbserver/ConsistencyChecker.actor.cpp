/*
 * ConsistencyChecker.actor.cpp
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

#include "fdbserver/WorkerInterface.actor.h"
#include "flow/IndexedSet.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/Smoother.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/TagThrottle.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbserver/RatekeeperInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/TesterInterface.actor.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct ConsistencyCheckerData {
	UID id;
	Database db;
	// TODO: NEELAM: we probably dont need to keep the next three fields here
	//bool state;
	//double maxRate;
	//double targetInterval;

	DatabaseConfiguration configuration;
	PromiseStream<Future<Void>> addActor;

	//ConsistencyCheckerData(UID id, Database db, double maxRate, double targetInterval)
	//  : id(id), db(db), maxRate(maxRate), targetInterval(targetInterval) {}
	ConsistencyCheckerData(UID id, Database db)
        : id(id), db(db) {}
};

ACTOR Future<Void> consistencyChecker(ConsistencyCheckerInterface ckInterf, Reference<AsyncVar<ServerDBInfo> const> dbInfo,
									  double maxRate, double targetInterval,Reference<ClusterConnectionFile> connFile) {
	state ConsistencyCheckerData self(ckInterf.id(), openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True));
	state Promise<Void> err;
	state Future<Void> collection = actorCollection(self.addActor.getFuture());
	state UnitTestParameters testParams;

	TraceEvent("ConsistencyCheckerStarting", ckInterf.id()).log();
	self.addActor.send(waitFailureServer(ckInterf.waitFailure.getFuture()));
	//self.addActor.send(configurationMonitor(&self)); //TODO: NEELAM: do we need this? maybe not

	self.addActor.send(traceRole(Role::CONSISTENCYCHECKER, ckInterf.id()));

	testParams.set("maxRate", maxRate);
	testParams.set("targetInterval", targetInterval);

	try {
		loop choose {
			// Run consistency check workload. Pass in the DBConfig params as testParams
			when(wait(runTests(connFile, //Reference<ClusterConnectionFile>(new ClusterConnectionFile()),
							   TEST_TYPE_CONSISTENCY_CHECK,
							   TEST_HERE,
							   1,
							   std::string(),
							   StringRef(),
							   ckInterf.locality,
							   testParams))) {
				return Void();
			}
			when(HaltConsistencyCheckerRequest req = waitNext(ckInterf.haltConsistencyChecker.getFuture())) {
				req.reply.send(Void());
				TraceEvent("ConsistencyCheckerHalted", ckInterf.id()).detail("ReqID", req.requesterID);
				break;
			}
			when(wait(err.getFuture())) {}
			when(wait(collection)) {
				ASSERT(false);
				throw internal_error();
			}
		}
	} catch (Error& err) {
		TraceEvent("ConsistencyCheckerDied", ckInterf.id()).error(err, true);
	}
	return Void();
}
