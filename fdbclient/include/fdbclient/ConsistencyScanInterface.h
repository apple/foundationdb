/*
 * ConsistencyScanInterface.h
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

#ifndef FDBCLIENT_CONSISTENCYSCANINTERFACE_H
#define FDBCLIENT_CONSISTENCYSCANINTERFACE_H

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"

struct ConsistencyScanInterface {
	constexpr static FileIdentifier file_identifier = 4983265;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct HaltConsistencyScanRequest> haltConsistencyScan;
	struct LocalityData locality;
	UID myId;

	ConsistencyScanInterface() {}
	explicit ConsistencyScanInterface(const struct LocalityData& l, UID id) : locality(l), myId(id) {}

	void initEndpoints() {}
	UID id() const { return myId; }
	NetworkAddress address() const { return waitFailure.getEndpoint().getPrimaryAddress(); }
	bool operator==(const ConsistencyScanInterface& r) const { return id() == r.id(); }
	bool operator!=(const ConsistencyScanInterface& r) const { return !(*this == r); }

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, waitFailure, haltConsistencyScan, locality, myId);
	}
};

struct HaltConsistencyScanRequest {
	constexpr static FileIdentifier file_identifier = 2323417;
	UID requesterID;
	ReplyPromise<Void> reply;

	HaltConsistencyScanRequest() {}
	explicit HaltConsistencyScanRequest(UID uid) : requesterID(uid) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, requesterID, reply);
	}
};

// consistency scan configuration and metrics
struct ConsistencyScanInfo {
	constexpr static FileIdentifier file_identifier = 732125;
	bool consistency_scan_enabled = false;
	bool restart = false;
	int64_t max_rate = 0;
	int64_t target_interval = CLIENT_KNOBS->CONSISTENCY_CHECK_ONE_ROUND_TARGET_COMPLETION_TIME;
	int64_t bytes_read_prev_round = 0;
	KeyRef progress_key = KeyRef();

	// Round Metrics - one round of complete validation across all SSs
	// Start and finish are in epoch seconds
	double last_round_start = 0;
	double last_round_finish = 0;
	TimerSmoother smoothed_round_duration;
	int finished_rounds = 0;

	ConsistencyScanInfo() : smoothed_round_duration(20.0 * 60) {}
	ConsistencyScanInfo(bool enabled, bool r, uint64_t rate, uint64_t interval)
	  : consistency_scan_enabled(enabled), restart(r), max_rate(rate), target_interval(interval),
	    smoothed_round_duration(20.0 * 60) {}

	template <class Ar>
	void serialize(Ar& ar) {
		double round_total;
		if (!ar.isDeserializing) {
			round_total = smoothed_round_duration.getTotal();
		}
		serializer(ar,
		           consistency_scan_enabled,
		           restart,
		           max_rate,
		           target_interval,
		           bytes_read_prev_round,
		           last_round_start,
		           last_round_finish,
		           round_total,
		           finished_rounds);
		if (ar.isDeserializing) {
			smoothed_round_duration.reset(round_total);
		}
	}

	static Future<Void> setInfo(Reference<ReadYourWritesTransaction> tr, ConsistencyScanInfo info) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->set(consistencyScanInfoKey, ObjectWriter::toValue(info, IncludeVersion()));
		return Void();
	}

	static Future<Void> setInfo(Database cx, ConsistencyScanInfo info) {
		return runRYWTransaction(
		    cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> { return setInfo(tr, info); });
	}

	static Future<Optional<Value>> getInfo(Reference<ReadYourWritesTransaction> tr) {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
		return tr->get(consistencyScanInfoKey);
	}

	static Future<Optional<Value>> getInfo(Database cx) {
		return runRYWTransaction(
		    cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> { return getInfo(tr); });
	}

	StatusObject toJSON() const {
		StatusObject result;
		result["consistency_scan_enabled"] = consistency_scan_enabled;
		result["restart"] = restart;
		result["max_rate"] = max_rate;
		result["target_interval"] = target_interval;
		result["bytes_read_prev_round"] = bytes_read_prev_round;
		result["last_round_start_datetime"] = epochsToGMTString(last_round_start);
		result["last_round_finish_datetime"] = epochsToGMTString(last_round_finish);
		result["last_round_start_timestamp"] = last_round_start;
		result["last_round_finish_timestamp"] = last_round_finish;
		result["smoothed_round_seconds"] = smoothed_round_duration.smoothTotal();
		result["finished_rounds"] = finished_rounds;
		return result;
	}

	std::string toString() const {
		return format("consistency_scan_enabled = %d, restart =  %d, max_rate = %ld, target_interval = %ld",
		              consistency_scan_enabled,
		              restart,
		              max_rate,
		              target_interval);
	}
};

Future<Version> getVersion(Database const& cx);
Future<bool> getKeyServers(
    Database const& cx,
    Promise<std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>>> const& keyServersPromise,
    KeyRangeRef const& kr,
    bool const& performQuiescentChecks);
Future<bool> getKeyLocations(Database const& cx,
                             std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>> const& shards,
                             Promise<Standalone<VectorRef<KeyValueRef>>> const& keyLocationPromise,
                             bool const& performQuiescentChecks);
Future<bool> checkDataConsistency(Database const& cx,
                                  VectorRef<KeyValueRef> const& keyLocations,
                                  DatabaseConfiguration const& configuration,
                                  std::map<UID, StorageServerInterface> const& tssMapping,
                                  bool const& performQuiescentChecks,
                                  bool const& performTSSCheck,
                                  bool const& firstClient,
                                  bool const& failureIsError,
                                  int const& clientId,
                                  int const& clientCount,
                                  bool const& distributed,
                                  bool const& shuffleShards,
                                  int const& shardSampleFactor,
                                  int64_t const& sharedRandomNumber,
                                  int64_t const& repetitions,
                                  int64_t* const& bytesReadInPreviousRound,
                                  int const& restart,
                                  int64_t const& maxRate,
                                  int64_t const& targetInterval,
                                  KeyRef const& progressKey);

#endif // FDBCLIENT_CONSISTENCYSCANINTERFACE_H