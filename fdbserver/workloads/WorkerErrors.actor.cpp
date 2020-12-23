/*
 * WorkerErrors.actor.cpp
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

#include "flow/ActorCollection.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/ServerDBInfo.h"
#include "flow/actorcompiler.h"  // This must be the last #include.


struct WorkerErrorsWorkload : TestWorkload {
	WorkerErrorsWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx) {}

	std::string description() const override { return "WorkerErrorsWorkload"; }
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return _start(cx, this); }
	void getMetrics(vector<PerfMetric>& m) override {}

	ACTOR Future< std::vector< TraceEventFields > > latestEventOnWorkers( std::vector<WorkerDetails> workers ) {
		state vector<Future<TraceEventFields>> eventTraces;
		for(int c = 0; c < workers.size(); c++) {
			eventTraces.push_back( workers[c].interf.eventLogRequest.getReply( EventLogRequest() ) );
		}

		wait( timeoutError( waitForAll( eventTraces ), 2.0 ) );

		vector<TraceEventFields> results;
		for(int i = 0; i < eventTraces.size(); i++) {
			results.push_back( eventTraces[i].get() );
		}

		return results;
	}

	ACTOR Future<Void> _start(Database cx, WorkerErrorsWorkload *self) {
		state vector<WorkerDetails> workers = wait( getWorkers( self->dbInfo ) );
		std::vector<TraceEventFields> errors = wait( self->latestEventOnWorkers( workers ) );
		for(auto e : errors) {
			printf("%s\n", e.toString().c_str());
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }
};

WorkloadFactory<WorkerErrorsWorkload> WorkerErrorsWorkloadFactory("WorkerErrors");
