/*
 * QueueModel.h
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

#ifndef FLOW_QUEUEMODEL_H
#define FLOW_QUEUEMODEL_H
#pragma once

#include "flow/flow.h"
#include "Smoother.h"
#include "flow/Knobs.h"
#include "flow/ActorCollection.h"


struct QueueData {
	Smoother smoothOutstanding;
	double latency;
	double penalty;
	QueueData() : latency(0.001), penalty(1.0), smoothOutstanding(FLOW_KNOBS->QUEUE_MODEL_SMOOTHING_AMOUNT) {}
};

typedef double TimeEstimate;

class QueueModel {
public:
	void endRequest( uint64_t id, double latency, double penalty, double delta, bool clean );
	QueueData& getMeasurement( uint64_t id );
	double addRequest( uint64_t id );
	double secondMultiplier;
	double secondBudget;
	PromiseStream< Future<Void> > addActor;
	Future<Void> laggingRequests; // requests for which a different recipient already answered
	int laggingRequestCount;

	QueueModel() : secondMultiplier(1.0), secondBudget(0), laggingRequestCount(0) {
		laggingRequests = actorCollection( addActor.getFuture(), &laggingRequestCount );
	}

	~QueueModel() {
		laggingRequests.cancel();
	}
private:
	std::map<uint64_t, QueueData> data;
};

/* old queue model
class QueueModel {
public:
	QueueModel() : new_index(0) {
		total_time[0] = 0;
		total_time[1] = 0;
	}
	void addMeasurement( uint64_t id, QueueDetails qd );
	TimeEstimate getTimeEstimate( uint64_t id );
	TimeEstimate getAverageTimeEstimate();
	QueueDetails getMeasurement( uint64_t id );
	void expire();

private:
	std::map<uint64_t, QueueDetails> data[2]; 
	double total_time[2]; 
	int new_index; // data[new_index] is the new data
};
*/

#endif