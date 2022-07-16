/*
 * QueueModel.cpp
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

#include "fdbrpc/QueueModel.h"
#include "fdbrpc/LoadBalance.h"

void QueueModel::endRequest(uint64_t id, double latency, double penalty, double delta, bool clean, bool futureVersion) {
	auto& d = data[id];

	// Remove the penalty added when starting the request.
	d.smoothOutstanding.addDelta(-delta);

	if (clean) {
		d.latency = latency;
	} else {
		d.latency = std::max(d.latency, latency);
	}

	if (futureVersion) {
		if (now() > d.increaseBackoffTime) {
			d.futureVersionBackoff = std::min(d.futureVersionBackoff * FLOW_KNOBS->FUTURE_VERSION_BACKOFF_GROWTH,
			                                  FLOW_KNOBS->FUTURE_VERSION_MAX_BACKOFF);
			d.increaseBackoffTime = now() + d.futureVersionBackoff;
		}
		d.failedUntil = now() + d.futureVersionBackoff;
	} else if (clean) {
		d.futureVersionBackoff = FLOW_KNOBS->FUTURE_VERSION_INITIAL_BACKOFF;
		d.increaseBackoffTime = 0.0;
	}

	if (penalty > 0) {
		d.penalty = penalty;
	}
}

QueueData const& QueueModel::getMeasurement(uint64_t id) {
	return data[id]; // return smoothed penalty
}

double QueueModel::addRequest(uint64_t id) {
	auto& d = data[id];
	d.smoothOutstanding.addDelta(d.penalty);
	return d.penalty;
}

void QueueModel::updateTssEndpoint(uint64_t endpointId, const TSSEndpointData& tssData) {
	auto& d = data[endpointId];
	d.tssData = tssData;
}

void QueueModel::removeTssEndpoint(uint64_t endpointId) {
	auto& d = data[endpointId];
	d.tssData = Optional<TSSEndpointData>();
}

Optional<TSSEndpointData> QueueModel::getTssData(uint64_t id) {
	return data[id].tssData;
}

Optional<LoadBalancedReply> getLoadBalancedReply(const LoadBalancedReply* reply) {
	return *reply;
}

Optional<LoadBalancedReply> getLoadBalancedReply(const void*) {
	return Optional<LoadBalancedReply>();
}

Optional<BasicLoadBalancedReply> getBasicLoadBalancedReply(const BasicLoadBalancedReply* reply) {
	return *reply;
}

Optional<BasicLoadBalancedReply> getBasicLoadBalancedReply(const void*) {
	return Optional<BasicLoadBalancedReply>();
}

/*
void QueueModel::addMeasurement( uint64_t id, QueueDetails qd ){
    if (data[new_index].count(id))
        total_time[new_index] -= data[new_index][id].queryQueueSize;
    data[new_index][id] = qd;
    total_time[new_index] += qd.queryQueueSize;
}

TimeEstimate QueueModel::getTimeEstimate( uint64_t id ){
    if (data[new_index].count(id))          // give the current estimate
        return data[new_index][id].queryQueueSize;
    else if (data[1-new_index].count(id))   // if not, old estimate
        return data[1-new_index][id].queryQueueSize;
    else									// if not, the average?
        return getAverageTimeEstimate();
}

TimeEstimate QueueModel::getAverageTimeEstimate(){
    if(data[new_index].size() + data[1-new_index].size() > 0)
        return (total_time[new_index] + total_time[1-new_index]) / (data[new_index].size() + data[1-new_index].size());
    return 0;
}

void QueueModel::expire(){
    data[1-new_index].clear();
    total_time[1-new_index] = 0;

    new_index = 1-new_index;
}
*/
