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
#include "fdbrpc/Smoother.h"
#include "flow/Knobs.h"
#include "flow/ActorCollection.h"

/*
The data structure used for the client-side load balancer to decide which
storage server to read data from. Conceptually, it represents the size of
storage server read request queue. One QueueData represents one storage
server.
*/
struct QueueData {
	// The latest queue size in this storage server.
	Smoother smoothOutstanding;
	double latency;

	// The additional queue size used in the next request to this storage
	// server. This penalty is sent from the storage server from the last
	// request, which is the server side mechanism to ask the client to slow
	// down request.
	double penalty;
	
	// Do not consider this storage server if the current time hasn't reach this
	// time. This field is computed after each request to not repeatedly try the
	// same storage server that is likely not going to return a valid result.
	double failedUntil;

	// If the storage server returns a "future version" error, increase above
	// `failedUntil` by this amount to increase the backoff time.
	double futureVersionBackoff;

	// If the current time has reached this time, and this storage server still
	// hasn't returned a valid result, increase above `futureVersionBackoff`
	// to increase the future backoff amount.
	double increaseBackoffTime;
	QueueData() : latency(0.001), penalty(1.0), smoothOutstanding(FLOW_KNOBS->QUEUE_MODEL_SMOOTHING_AMOUNT), failedUntil(0), futureVersionBackoff(FLOW_KNOBS->FUTURE_VERSION_INITIAL_BACKOFF), increaseBackoffTime(0) {}
};

typedef double TimeEstimate;

class QueueModel {
public:
	// Finishes the request sent to storage server with `id`.
	//   - latency: the measured client-side latency of the request.
	//   - penalty: the server side penalty sent along with the response from
    //              the storage server.
	//   - delta: Update server `id`'s queue model by substract this amount.
	//            This value should be the value returned by `addRequest` below.
    //   - clean: indicates whether the there was an error or not.
	// 	 - futureVersion: indicates whether there was "future version" error or
	//					  not.
	void endRequest( uint64_t id, double latency, double penalty, double delta, bool clean, bool futureVersion );
	QueueData& getMeasurement( uint64_t id );

    // Starts a new request to storage server with `id`. If the storage
    // server contains a penalty, add it to the queue size, and return the
    // penalty. The returned penalty should be passed as `delta` to `endRequest`
    // to make `smoothOutstanding` to reflect the real storage queue size.
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
	std::unordered_map<uint64_t, QueueData> data;
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