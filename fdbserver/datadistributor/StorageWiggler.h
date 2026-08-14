/*
 * StorageWiggler.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/StorageWiggleMetrics.h"
#include "fdbserver/core/DataDistributorInterface.h"
#include "flow/genericactors.h"

#include <boost/heap/policies.hpp>
#include <boost/heap/skew_heap.hpp>

#include <functional>
#include <unordered_map>
#include <utility>

class DDTeamCollection;

struct StorageWiggler : ReferenceCounted<StorageWiggler> {
	static constexpr double MIN_ON_CHECK_DELAY_SEC = 5.0;
	using State = StorageWigglerState::Value;
	static constexpr State INVALID = StorageWigglerState::INVALID;
	static constexpr State RUN = StorageWigglerState::RUN;
	static constexpr State PAUSE = StorageWigglerState::PAUSE;

	DDTeamCollection const* teamCollection;
	StorageWiggleData wiggleData; // the wiggle related data persistent in database

	StorageWiggleMetrics metrics;
	AsyncVar<bool> stopWiggleSignal;
	// data structures
	using MetadataUIDP = std::pair<StorageMetadataType, UID>;
	// min-heap
	boost::heap::skew_heap<MetadataUIDP, boost::heap::mutable_<true>, boost::heap::compare<std::greater<MetadataUIDP>>>
	    wiggle_pq;
	std::unordered_map<UID, decltype(wiggle_pq)::handle_type> pq_handles;

	State wiggleState = INVALID;
	double lastStateChangeTs = 0.0; // timestamp describes when did the state change

	explicit StorageWiggler(DDTeamCollection* collection) : teamCollection(collection), stopWiggleSignal(true) {};
	// wiggle related actors will quit when this signal is set to true
	void setStopSignal(bool value) { stopWiggleSignal.set(value); }
	bool isStopped() const { return stopWiggleSignal.get(); }
	// add server to wiggling queue
	void addServer(const UID& serverId, const StorageMetadataType& metadata);
	// remove server from wiggling queue
	void removeServer(const UID& serverId);
	// update metadata and adjust priority_queue
	void updateMetadata(const UID& serverId, const StorageMetadataType& metadata);
	bool contains(const UID& serverId) const { return pq_handles.contains(serverId); }
	bool empty() const { return wiggle_pq.empty(); }

	// It's guarantee that When a.metadata >= b.metadata, if !necessary(a) then !necessary(b)
	bool necessary(const UID& serverId, const StorageMetadataType& metadata) const;

	// try to return the next storage server that is necessary to wiggle
	Optional<UID> getNextServerId(bool necessaryOnly = true);
	// next check time to avoid busy loop
	Future<Void> onCheck() const;
	State getWiggleState() const { return wiggleState; }
	void setWiggleState(State s) {
		if (wiggleState != s) {
			wiggleState = s;
			lastStateChangeTs = g_network->now();
		}
	}
	static std::string getWiggleStateStr(State s) { return StorageWigglerState::toString(s); }

	// -- statistic update

	// reset Statistic in database when perpetual wiggle is closed by user
	Future<Void> resetStats();
	// restore Statistic from database when the perpetual wiggle is opened
	Future<Void> restoreStats();
	// called when start wiggling a SS
	Future<Void> startWiggle();
	Future<Void> finishWiggle();
	void updateFinishWiggleMetrics(double finishTime);
	bool shouldStartNewRound() const { return metrics.last_round_finish >= metrics.last_round_start; }
	bool shouldFinishRound() const {
		if (wiggle_pq.empty())
			return true;
		return (wiggle_pq.top().first.createdTime >= metrics.last_round_start);
	}
};
