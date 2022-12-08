/*
 * StorageWiggleMetrics.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_STORAGEWIGGLEMETRICS_ACTOR_G_H)
#define FDBCLIENT_STORAGEWIGGLEMETRICS_ACTOR_G_H
#include "fdbclient/StorageWiggleMetrics.actor.g.h"
#elif !defined(FDBCLIENT_STORAGEWIGGLEMETRICS_ACTOR_H)
#define FDBCLIENT_STORAGEWIGGLEMETRICS_ACTOR_H

#include "fdbrpc/Smoother.h"
#include "flow/ObjectSerializer.h"
#include "flow/serialize.h"
#include "fdbclient/Status.h"
#include "flow/actorcompiler.h" // This must be the last #include.

FDB_DECLARE_BOOLEAN_PARAM(PrimaryRegion);

struct StorageWiggleMetrics {
	constexpr static FileIdentifier file_identifier = 4728961;

	// round statistics
	// One StorageServer wiggle round is considered 'complete', when all StorageServers with creationTime < T are
	// wiggled
	// Start and finish are in epoch seconds
	double last_round_start = 0;
	double last_round_finish = 0;
	TimerSmoother smoothed_round_duration;
	int finished_round = 0; // finished round since storage wiggle is open

	// step statistics
	// 1 wiggle step as 1 storage server is wiggled in the current round
	// Start and finish are in epoch seconds
	double last_wiggle_start = 0;
	double last_wiggle_finish = 0;
	TimerSmoother smoothed_wiggle_duration;
	int finished_wiggle = 0; // finished step since storage wiggle is open

	StorageWiggleMetrics() : smoothed_round_duration(20.0 * 60), smoothed_wiggle_duration(10.0 * 60) {}

	template <class Ar>
	void serialize(Ar& ar) {
		double step_total, round_total;
		if (!ar.isDeserializing) {
			step_total = smoothed_wiggle_duration.getTotal();
			round_total = smoothed_round_duration.getTotal();
		}
		serializer(ar,
		           last_wiggle_start,
		           last_wiggle_finish,
		           step_total,
		           finished_wiggle,
		           last_round_start,
		           last_round_finish,
		           round_total,
		           finished_round);
		if (ar.isDeserializing) {
			smoothed_round_duration.reset(round_total);
			smoothed_wiggle_duration.reset(step_total);
		}
	}

	StatusObject toJSON() const {
		StatusObject result;
		result["last_round_start_datetime"] = epochsToGMTString(last_round_start);
		result["last_round_finish_datetime"] = epochsToGMTString(last_round_finish);
		result["last_round_start_timestamp"] = last_round_start;
		result["last_round_finish_timestamp"] = last_round_finish;
		result["smoothed_round_seconds"] = smoothed_round_duration.smoothTotal();
		result["finished_round"] = finished_round;

		result["last_wiggle_start_datetime"] = epochsToGMTString(last_wiggle_start);
		result["last_wiggle_finish_datetime"] = epochsToGMTString(last_wiggle_finish);
		result["last_wiggle_start_timestamp"] = last_wiggle_start;
		result["last_wiggle_finish_timestamp"] = last_wiggle_finish;
		result["smoothed_wiggle_seconds"] = smoothed_wiggle_duration.smoothTotal();
		result["finished_wiggle"] = finished_wiggle;
		return result;
	}

	void reset() {
		StorageWiggleMetrics newMetrics;
		newMetrics.smoothed_round_duration.reset(smoothed_round_duration.getTotal());
		newMetrics.smoothed_wiggle_duration.reset(smoothed_wiggle_duration.getTotal());
		*this = std::move(newMetrics);
	}
};

// read from DB
ACTOR template <typename TxnType>
Future<Optional<StorageWiggleMetrics>> loadStorageWiggleMetrics(TxnType tr, PrimaryRegion primary) {
	tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
	auto f = tr->get(perpetualStorageWiggleKeyFor(primary, PerpetualWiggleKeyType::WIGGLE_STATS));
	Optional<Value> value = wait(safeThreadFutureToFuture(f));
	if (!value.present()) {
		return Optional<StorageWiggleMetrics>();
	}
	return ObjectReader::fromStringRef<StorageWiggleMetrics>(value.get(), IncludeVersion());
}

// update the serialized metrics when the perpetual wiggle is enabled
ACTOR template <typename TxnType>
Future<Void> updateStorageWiggleMetrics(TxnType tr, StorageWiggleMetrics metrics, PrimaryRegion primary) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	auto f = tr->get(perpetualStorageWiggleKey);
	Optional<Value> v = wait(safeThreadFutureToFuture(f));
	if (v.present() && v == "1"_sr) {
		tr->set(perpetualStorageWiggleKeyFor(primary, PerpetualWiggleKeyType::WIGGLE_STATS),
		        ObjectWriter::toValue(metrics, IncludeVersion()));
	} else {
		CODE_PROBE(true, "Intend to update StorageWiggleMetrics after PW disabled");
	}
	return Void();
}

// set all fields except for smoothed durations to default values. If the metrics is not given, load from system key
// space
ACTOR template <class TrType>
Future<Void> resetStorageWiggleMetrics(TrType tr,
                                       PrimaryRegion primary,
                                       Optional<StorageWiggleMetrics> metrics = Optional<StorageWiggleMetrics>()) {

	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	if (!metrics.present()) {
		wait(store(metrics, loadStorageWiggleMetrics(tr, primary)));
	}

	if (metrics.present()) {
		metrics.get().reset();
		tr->set(perpetualStorageWiggleKeyFor(primary, PerpetualWiggleKeyType::WIGGLE_STATS),
		        ObjectWriter::toValue(metrics.get(), IncludeVersion()));
	}
	return Void();
}

#include "flow/unactorcompiler.h"
#endif