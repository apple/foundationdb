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

#include "flow/actorcompiler.h"
#include "flow/ActorCollection.h"
#include "fdbclient/NativeAPI.h"
#include "fdbserver/TesterInterface.h"
#include "workloads.h"
#include "fdbserver/WorkerInterface.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/ServerDBInfo.h"


struct WorkerErrorsWorkload : TestWorkload {
	WorkerErrorsWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx) {}

	virtual std::string description() { return "WorkerErrorsWorkload"; }
	virtual Future<Void> setup( Database const& cx ) { 
		return Void();
	}
	virtual Future<Void> start( Database const& cx ) {
		return _start(cx, this);
	}
	virtual void getMetrics( vector<PerfMetric>& m ) {}


	ACTOR Future< std::vector< std::string > > latestEventOnWorkers( std::vector<std::pair<WorkerInterface, ProcessClass>> workers ) {
		state vector<Future<Standalone<StringRef>>> eventTraces;
		for(int c = 0; c < workers.size(); c++) {
			eventTraces.push_back( workers[c].first.eventLogRequest.getReply( EventLogRequest() ) );
		}

		Void _ = wait( timeoutError( waitForAll( eventTraces ), 2.0 ) );

		vector<std::string> results;
		for(int i = 0; i < eventTraces.size(); i++) {
			results.push_back( eventTraces[i].get().toString() );
		}

		return results;
	}

	ACTOR Future<Void> _start(Database cx, WorkerErrorsWorkload *self) {
		state vector<std::pair<WorkerInterface, ProcessClass>> workers = wait( getWorkers( self->dbInfo ) );
		std::vector<std::string> errors = wait( self->latestEventOnWorkers( workers ) );
		for(auto e : errors) {
			printf("%s\n", e.c_str());
		}
		return Void();
	}

	virtual Future<bool> check( Database const& cx ) { return true; }
};

WorkloadFactory<WorkerErrorsWorkload> WorkerErrorsWorkloadFactory("WorkerErrors");
