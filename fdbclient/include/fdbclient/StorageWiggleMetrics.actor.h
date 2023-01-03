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
#include "fdbclient/SystemData.h"
#include "fdbclient/KeyBackedConfig.h"
#include "fdbclient/RunTransaction.actor.h"
#include "flow/actorcompiler.h"

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

struct StorageWiggleDelay {
	constexpr static FileIdentifier file_identifier = 102937;
	double delaySeconds = 0;
	explicit StorageWiggleDelay(double sec = 0) : delaySeconds(sec) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, delaySeconds);
	}
};

namespace {
// Persistent the total delay time to the database, and return accumulated delay time.
ACTOR template <class TrType>
Future<double> addPerpetualWiggleDelay_impl(
    TrType tr,
    KeyBackedObjectProperty<StorageWiggleDelay, decltype(IncludeVersion())> delayProperty,
    double secDelta) {

	state StorageWiggleDelay delayObj = wait(delayProperty.getD(tr, Snapshot::False));
	delayObj.delaySeconds += secDelta;
	delayProperty.set(tr, delayObj);

	return delayObj.delaySeconds;
}

// set all fields except for smoothed durations to default values. If the metrics is not given, load from system key
// space
ACTOR template <class TrType>
Future<Void> resetStorageWiggleMetrics_impl(
    TrType tr,
    KeyBackedObjectProperty<StorageWiggleMetrics, decltype(IncludeVersion())> metricsProperty,
    Optional<StorageWiggleMetrics> metrics) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	if (!metrics.present()) {
		wait(store(metrics, metricsProperty.get(tr)));
	}

	if (metrics.present()) {
		metrics.get().reset();
		metricsProperty.set(tr, metrics.get());
	}
	return Void();
}
} // namespace
// After 7.3, the perpetual wiggle related keys should use format "\xff/storageWiggle/[primary | remote]/[fieldName]"
class StorageWiggleData : public KeyBackedConfig {
public:
	StorageWiggleData() : KeyBackedConfig(perpetualStorageWigglePrefix) {}

	auto perpetualWiggleSpeed() const { return KeyBackedProperty<Value, NullCodec>(perpetualStorageWiggleKey); }

	auto wigglingStorageServer(PrimaryRegion primaryDc) const {
		Key mapPrefix = perpetualStorageWiggleIDPrefix.withSuffix(primaryDc ? "primary/"_sr : "remote/"_sr);
		return KeyBackedObjectMap<UID, StorageWiggleValue, decltype(IncludeVersion())>(mapPrefix, IncludeVersion());
	}

	auto storageWiggleMetrics(PrimaryRegion primaryDc) const {
		Key key = perpetualStorageWiggleStatsPrefix.withSuffix(primaryDc ? "primary"_sr : "remote"_sr);
		return KeyBackedObjectProperty<StorageWiggleMetrics, decltype(IncludeVersion())>(key, IncludeVersion());
	}

	auto storageWiggleDelay(PrimaryRegion primaryDc) const {
		Key key = prefix.withSuffix(primaryDc ? "primary/"_sr : "remote/"_sr).withSuffix("wiggleDelay"_sr);
		return KeyBackedObjectProperty<StorageWiggleDelay, decltype(IncludeVersion())>(key, IncludeVersion());
	}

	// Persistent the total delay time to the database, and return accumulated delay time.
	template <class DB>
	Future<double> addPerpetualWiggleDelay(Reference<DB> db, PrimaryRegion primary, double secDelta) {
		return runTransaction(db, [=, self = *this](Reference<typename DB::TransactionT> tr) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			return addPerpetualWiggleDelay_impl(tr, self.storageWiggleDelay(primary), secDelta);
		});
	}

	// clear the persistent total delay in database
	template <class DB>
	Future<Void> clearPerpetualWiggleDelay(Reference<DB> db, PrimaryRegion primary) {
		return runTransaction(db, [=, self = *this](Reference<typename DB::TransactionT> tr) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			self.storageWiggleDelay(primary).clear(tr);
			return Future<Void>(Void());
		});
	}

	// set all fields except for smoothed durations to default values. If the metrics is not given, load from system key
	// space
	template <class TrType>
	Future<Void> resetStorageWiggleMetrics(TrType tr,
	                                       PrimaryRegion primary,
	                                       Optional<StorageWiggleMetrics> metrics = Optional<StorageWiggleMetrics>()) {
		return resetStorageWiggleMetrics_impl(tr, storageWiggleMetrics(primary), metrics);
	}

	ACTOR template <typename TrType>
	static Future<Void> updateStorageWiggleMetrics_impl(
	    KeyBackedProperty<Value, NullCodec> wiggleSpeed,
	    KeyBackedObjectProperty<StorageWiggleMetrics, decltype(IncludeVersion())> storageMetrics,
	    TrType tr,
	    StorageWiggleMetrics metrics,
	    PrimaryRegion primary) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		Optional<Value> v = wait(wiggleSpeed.get(tr));
		if (v.present() && v == "1"_sr) {
			storageMetrics.set(tr, metrics);
		} else {
			CODE_PROBE(true, "Intend to update StorageWiggleMetrics after PW disabled");
		}
		return Void();
	}

	// update the serialized metrics when the perpetual wiggle is enabled
	template <typename TrType>
	Future<Void> updateStorageWiggleMetrics(TrType tr, StorageWiggleMetrics metrics, PrimaryRegion primary) {
		return updateStorageWiggleMetrics_impl(
		    perpetualWiggleSpeed(), storageWiggleMetrics(primary), tr, metrics, primary);
	}
};

#include "flow/unactorcompiler.h"
#endif