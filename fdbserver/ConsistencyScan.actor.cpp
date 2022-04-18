/*
 * ConsistencyScan.actor.cpp
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

struct ConsistencyScanData {
	UID id;
	Database db;

	DatabaseConfiguration configuration;
	PromiseStream<Future<Void>> addActor;

	int64_t restart;
	double maxRate;
	double targetInterval;
	KeyRef progressKey;
	AsyncVar<bool> consistencyScanEnabled = false;

	ConsistencyScanData(UID id, Database db) : id(id), db(db) {}
};

ACTOR Future<Void> watchConsistencyScanInfoKey(ConsistencyScanData* self) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->db);

	TraceEvent("CCConsistencyScanWatch", self->id).log();
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			TraceEvent("ConsistencyScanWatchGettingVal", self->id).log();
			state Optional<Value> val = wait(ConsistencyScanInfo::getInfo(tr));
			if (val.present()) {
				ConsistencyScanInfo consistencyScanInfo = ObjectReader::fromStringRef<ConsistencyScanInfo>(val.get(), IncludeVersion());
				TraceEvent("ConsistencyScanWatchGotVal", self->id)
					.detail("Enabled",consistencyScanInfo.consistency_scan_enabled);
				self->consistencyScanEnabled.set(consistencyScanInfo.consistency_scan_enabled);
				self->restart = consistencyScanInfo.restart;
				self->maxRate = consistencyScanInfo.max_rate;
				self->targetInterval = consistencyScanInfo.target_interval;
				self->progressKey = consistencyScanInfo.progress_key;
			}
			state Future<Void> watch = tr->watch(consistencyScanInfoKey);
			wait(tr->commit());
			wait(watch);
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}
ACTOR Future<Void> consistencyScan(ConsistencyScanInterface csInterf,
								   Reference<AsyncVar<ServerDBInfo> const> dbInfo,
								   Reference<IClusterConnectionRecord> connRecord) {
	state ConsistencyScanData self(csInterf.id(),
								   openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True));
	state Promise<Void> err;
	state Future<Void> collection = actorCollection(self.addActor.getFuture());
	state UnitTestParameters testParams;

	TraceEvent("ConsistencyScanStarting", csInterf.id()).log();
	self.addActor.send(waitFailureServer(csInterf.waitFailure.getFuture()));

	self.addActor.send(traceRole(Role::CONSISTENCYSCAN, csInterf.id()));
	self.addActor.send(watchConsistencyScanInfoKey(&self));

	testParams.set("distributed", "false");

	loop {
		if (self.consistencyScanEnabled.get()) {
			testParams.set("restart", self.restart);
			testParams.set("maxRate", self.maxRate);
			testParams.set("targetInterval", self.targetInterval);
			testParams.set("progressKey", self.progressKey.toString());
			try {
				loop choose {
					// Run consistency check workload. Pass in the DBConfig params as testParams
					when(wait(runTests(connRecord,
									   TEST_TYPE_CONSISTENCY_CHECK,
									   TEST_HERE,
									   1,
									   std::string(),
									   StringRef(),
									   csInterf.locality,
									   testParams))) {
						return Void();
					}
					when(HaltConsistencyScanRequest req = waitNext(csInterf.haltConsistencyScan.getFuture())) {
						req.reply.send(Void());
						TraceEvent("ConsistencyScanHalted", csInterf.id()).detail("ReqID", req.requesterID);
						break;
					}
					when(wait(err.getFuture())) {}
					when(wait(collection)) {
						ASSERT(false);
						throw internal_error();
					}
				}
			} catch (Error& err) {
				if (err.code() == error_code_actor_cancelled) {
					TraceEvent("ConsistencyScanActorCanceled", csInterf.id()).errorUnsuppressed(err);
					return Void();
				}
				TraceEvent("ConsistencyScanDied", csInterf.id()).errorUnsuppressed(err);
			}
		} else {
			TraceEvent("ConsistencyScanWaitingForConfigChange", self.id).log();
			wait(self.consistencyScanEnabled.onChange());
		}
	}
}
