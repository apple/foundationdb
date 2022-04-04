/*
 * QueueModel.h
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

#ifndef FLOW_QUEUEMODEL_H
#define FLOW_QUEUEMODEL_H
#pragma once

#include "flow/flow.h"
#include "fdbrpc/Smoother.h"
#include "flow/Knobs.h"
#include "flow/ActorCollection.h"
#include "fdbrpc/TSSComparison.h" // For TSS Metrics
#include "fdbrpc/FlowTransport.h" // For Endpoint

struct TSSEndpointData {
	UID tssId;
	Endpoint endpoint;
	Reference<TSSMetrics> metrics;

	TSSEndpointData(UID tssId, Endpoint endpoint, Reference<TSSMetrics> metrics)
	  : tssId(tssId), endpoint(endpoint), metrics(metrics) {}
};

// The data structure used for the client-side load balancing algorithm to
// decide which storage server to read data from. Conceptually, it tracks the
// number of outstanding requests the current client sent to each storage
// server. One "QueueData" represents one storage server.
struct QueueData {
	// The current outstanding requests sent by the local client to this storage
	// server. The number is smoothed out over a continuous timeline.
	Smoother smoothOutstanding;

	// The last client perceived latency to this storage server.
	double latency;

	// Represents the "cost" of each storage request. By default, the penalty is
	// 1 indicating that each outstanding request corresponds 1 outstanding
	// request. However, storage server can also increase the penalty if it
	// decides to ask the client to slow down sending requests to it. Penalty
	// is updated after each LoadBalancedReply.
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

	// a bit of a hack to store this here, but it's the only centralized place for per-endpoint tracking
	Optional<TSSEndpointData> tssData;

	QueueData()
	  : smoothOutstanding(FLOW_KNOBS->QUEUE_MODEL_SMOOTHING_AMOUNT), latency(0.001), penalty(1.0), failedUntil(0),
	    futureVersionBackoff(FLOW_KNOBS->FUTURE_VERSION_INITIAL_BACKOFF), increaseBackoffTime(0) {}
};

typedef double TimeEstimate;

class QueueModel {
public:
	// Finishes the request sent to storage server with `id`.
	//   - latency: the measured client-side latency of the request.
	//   - penalty: the server side penalty sent along with the response from
	//              the storage server. Requires >= 1.
	//   - delta: Update server `id`'s queue model by substract this amount.
	//            This value should be the value returned by `addRequest` below.
	//   - clean: indicates whether the there was an error or not.
	// 	 - futureVersion: indicates whether there was "future version" error or
	//					  not.
	void endRequest(uint64_t id, double latency, double penalty, double delta, bool clean, bool futureVersion);
	QueueData const& getMeasurement(uint64_t id);

	// Starts a new request to storage server with `id`. If the storage
	// server contains a penalty, add it to the queue size, and return the
	// penalty. The returned penalty should be passed as `delta` to `endRequest`
	// to make `smoothOutstanding` to reflect the real storage queue size.
	double addRequest(uint64_t id);
	double secondMultiplier;
	double secondBudget;
	PromiseStream<Future<Void>> addActor;
	Future<Void> laggingRequests; // requests for which a different recipient already answered
	PromiseStream<Future<Void>> addTSSActor;
	Future<Void> tssComparisons; // requests for which a different recipient already answered
	int laggingRequestCount;
	int laggingTSSCompareCount;

	// Updates this endpoint data to duplicate requests to the specified TSS endpoint
	void updateTssEndpoint(uint64_t endpointId, const TSSEndpointData& endpointData);

	// Removes the TSS mapping from this endpoint to stop duplicating requests to a TSS endpoint
	void removeTssEndpoint(uint64_t endpointId);

	// Retrieves the data for this endpoint's pair TSS endpoint, if present
	Optional<TSSEndpointData> getTssData(uint64_t endpointId);

	QueueModel() : secondMultiplier(1.0), secondBudget(0), laggingRequestCount(0) {
		laggingRequests = actorCollection(addActor.getFuture(), &laggingRequestCount);
		tssComparisons = actorCollection(addTSSActor.getFuture(), &laggingTSSCompareCount);
	}

	~QueueModel() {
		laggingRequests.cancel();
		tssComparisons.cancel();
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
