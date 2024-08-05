/*
 * ConsistencyScanInterface.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/SystemData.h"
#include "fdbclient/json_spirit/json_spirit_value.h"
#include "flow/serialize.h"
#include "fmt/core.h"
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_CONSISTENCYSCANINTERFACE_ACTOR_G_H)
#define FDBCLIENT_CONSISTENCYSCANINTERFACE_ACTOR_G_H
#include "fdbclient/ConsistencyScanInterface.actor.g.h"
#elif !defined(FDBCLIENT_CONSISTENCYSCANINTERFACE_ACTOR_H)
#define FDBCLIENT_CONSISTENCYSCANINTERFACE_ACTOR_H

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/KeyBackedRangeMap.actor.h"

#include "flow/actorcompiler.h" // must be last include

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

// Consistency Scan State
// This class provides access to the Consistency Scan's state stored in the database.
// The state is divided into these components
//   Config - Tells the scan whether and how to run.  Written by user, read by scan.
//   RangeConfig - Tells the scan what ranges to operate on or ignore
//
//   CurrentRoundStats - Execution state and stats for the current round
//   RoundHistory - History of RoundInfo's by start version
// 	 LifetimeStats - Accumulated lifetime counts for the cluster
//
// The class trigger will only be fired by changes to Config or RangeConfig
struct ConsistencyScanState : public KeyBackedClass {
	ConsistencyScanState(Key prefix = SystemKey("\xff/consistencyScanState"_sr)) : KeyBackedClass(prefix) {}

	struct Config {
		constexpr static FileIdentifier file_identifier = 23123;

		bool enabled = false;

		// The values below are NOT being initialized from knobs because once the scan is enabled
		// changing the knobs does nothing.  The consistency check knobs are for the consistency
		// check workload, which is different from the Consistency Scan feature

		// Max byte read bandwidth allowed, default 50 MB/s
		int64_t maxReadByteRate = 50e6;
		// Target time in seconds for completion of one full scan of the database, default 30 days.
		int64_t targetRoundTimeSeconds = 60 * 60 * 24 * 30;
		// Minimum time in seconds a round should take.

		// If a round completes faster than this, the scanner will delay afterwards, though if the
		// scan role is restarted it will start a new scan.
		int64_t minRoundTimeSeconds = 60 * 60 * 24 * 30;

		// The minimum start version allowed for the current round.  If the round started before this
		// it will be ended without completion, moved to history, and a new round will begin.
		Version minStartVersion = 0;

		// Number of days of history to keep, this is enforced using CORE_VERSIONSPERSECOND
		int64_t roundHistoryDays = 90;

		json_spirit::mObject toJSON() const {
			json_spirit::mObject doc;
			doc["enabled"] = enabled;
			doc["max_rate_bytes_per_second"] = maxReadByteRate;
			doc["target_interval_seconds"] = targetRoundTimeSeconds;
			doc["min_interval_seconds"] = minRoundTimeSeconds;
			doc["min_start_version"] = minStartVersion;
			doc["round_history_days"] = roundHistoryDays;
			return doc;
		}

		template <class Ar>
		void serialize(Ar& ar) {
			serializer(ar,
			           enabled,
			           maxReadByteRate,
			           targetRoundTimeSeconds,
			           minRoundTimeSeconds,
			           minStartVersion,
			           roundHistoryDays);
		}
	};

	// Configuration value in a range map for a key range
	struct RangeConfig {
		constexpr static FileIdentifier file_identifier = 846323;

		// Whether the range is included as a configured target for the scan
		// This should normally be set by the user
		Optional<bool> included;

		// Whether the range should be currently even though it remains a scan target
		// This should be set by operations on the cluster that would make shards temporarily inconsistent.
		Optional<bool> skip;

		RangeConfig apply(RangeConfig const& rhs) const {
			RangeConfig result = *this;
			if (rhs.included.present()) {
				result.included = rhs.included;
			}
			if (rhs.skip.present()) {
				result.skip = rhs.skip;
			}
			return result;
		}

		template <class Ar>
		void serialize(Ar& ar) {
			serializer(ar, included, skip);
		}

		std::string toString() const { return fmt::format("included={} skip={}", included, skip); }
		json_spirit::mObject toJSON() const {
			json_spirit::mObject doc;
			if (included.present()) {
				doc["included"] = *included;
			}
			if (skip.present()) {
				doc["skip"] = *skip;
			}
			return doc;
		}
	};

	struct LifetimeStats {
		constexpr static FileIdentifier file_identifier = 7897646;

		// Amount of FDB keyspace read, regardless of replication
		int64_t logicalBytesScanned = 0;
		// Actual amount of data read from shard replicas
		int64_t replicatedBytesRead = 0;
		int64_t errorCount = 0;

		template <class Ar>
		void serialize(Ar& ar) {
			serializer(ar, logicalBytesScanned, replicatedBytesRead, errorCount);
		}

		json_spirit::mObject toJSON() const {
			json_spirit::mObject doc;
			doc["logical_bytes_scanned"] = logicalBytesScanned;
			doc["replicated_bytes_scanned"] = replicatedBytesRead;
			doc["errors"] = errorCount;
			return doc;
		}
	};

	struct RoundStats {
		constexpr static FileIdentifier file_identifier = 23126;

		Version startVersion = 0;
		double startTime = 0;
		Version endVersion = 0;
		double endTime = 0;
		Version lastProgressVersion = 0;
		double lastProgressTime = 0;

		// Whether the scan finished, useful for history round stats.
		bool complete = false;

		// Amount of FDB keyspace read, regardless of replication
		int64_t logicalBytesScanned = 0;
		// Actual amount of data read from shard replicas
		int64_t replicatedBytesRead = 0;
		int64_t errorCount = 0;
		int64_t skippedRanges = 0;
		// FIXME: add failed request count that we periodically save even if no progress too?

		Key lastEndKey = ""_sr;

		/*
		// TODO:  Ideas of more things to track:
		int64_t shardsScanned = 0;
		int64_t replicasScanned = 0;
		// Shards or replicas can be skipped due to being offline or locked
		int64_t shardsSkipped = 0;
		int64_t replicasSkipped = 0;
		*/

		template <class Ar>
		void serialize(Ar& ar) {
			serializer(ar,
			           startVersion,
			           startTime,
			           endVersion,
			           endTime,
			           lastProgressVersion,
			           lastProgressTime,
			           complete,
			           logicalBytesScanned,
			           replicatedBytesRead,
			           errorCount,
			           skippedRanges,
			           lastEndKey);
		}

		json_spirit::mObject toJSON() const {
			json_spirit::mObject doc;
			doc["complete"] = complete;
			doc["start_version"] = startVersion;
			if (startTime != 0) {
				doc["start_timestamp"] = startTime;
				doc["start_datetime"] = epochsToGMTString(startTime);
			}

			doc["end_version"] = endVersion;
			if (endTime != 0) {
				doc["end_timestamp"] = endTime;
				doc["end_datetime"] = epochsToGMTString(endTime);
			}

			doc["last_progress_version"] = lastProgressVersion;
			if (lastProgressTime != 0) {
				doc["last_progress_timestamp"] = lastProgressTime;
				doc["last_progress_datetime"] = epochsToGMTString(lastProgressTime);
			}

			doc["logical_bytes_scanned"] = logicalBytesScanned;
			doc["replicated_bytes_scanned"] = replicatedBytesRead;
			doc["errors"] = errorCount;
			doc["last_end_key"] = lastEndKey.toString();
			doc["skippedRanges"] = skippedRanges;
			return doc;
		}
	};

	// Range map for configuring key range options.  By default, all ranges are scanned.
	typedef KeyBackedRangeMap<Key, RangeConfig, TupleCodec<Key>, ObjectCodec<RangeConfig, _IncludeVersion>>
	    RangeConfigMap;

	// Map of scan start version to its stats so a history can be maintained.
	typedef KeyBackedObjectMap<Version, RoundStats, _IncludeVersion> StatsHistoryMap;

	RangeConfigMap rangeConfig() {
		// Updating rangeConfig updates the class trigger
		return { subspace.pack(__FUNCTION__sr), trigger, IncludeVersion() };
	}

	KeyBackedObjectProperty<Config, _IncludeVersion> config() {
		// Updating rangeConfig updates the class trigger
		return { subspace.pack(__FUNCTION__sr), IncludeVersion(), trigger };
	}

	// Updating the lifetime stats does not update the class trigger because the stats are constantly updated, but when
	// resetting them the same transaction that sets the stats value must also call trigger.update(tr) so that the scan
	// loop will restart and not overwrite the reset value with a stale copy.
	KeyBackedObjectProperty<LifetimeStats, _IncludeVersion> lifetimeStats() {
		return { subspace.pack(__FUNCTION__sr), IncludeVersion() };
	}

	KeyBackedObjectProperty<RoundStats, _IncludeVersion> currentRoundStats() {
		return { subspace.pack(__FUNCTION__sr), IncludeVersion() };
	}

	// History of scan round stats stored by their start version
	StatsHistoryMap roundStatsHistory() { return { subspace.pack(__FUNCTION__sr), IncludeVersion() }; }

	ACTOR static Future<Void> clearStatsActor(ConsistencyScanState* self, Reference<ReadYourWritesTransaction> tr) {
		// read the keyspaces so the transaction conflicts on write (key-backed properties don't expose conflict ranges,
		// and the extra work here is negligible because this is a rare manual command so performance is not a huge
		// concern)
		wait(success(self->currentRoundStats().getD(tr)) && success(self->lifetimeStats().getD(tr)) &&
		     success(self->roundStatsHistory().getRange(tr, {}, {}, 1, Snapshot::False, Reverse::False)));

		// update each of the stats keyspaces to empty
		self->currentRoundStats().set(tr, ConsistencyScanState::RoundStats());
		self->lifetimeStats().set(tr, ConsistencyScanState::LifetimeStats());
		self->roundStatsHistory().erase(tr, 0, MAX_VERSION);
		return Void();
	}

	Future<Void> clearStats(Reference<ReadYourWritesTransaction> tr) { return clearStatsActor(this, tr); }
};

/////////////////////
// Code below this line is not used by the Consistency Scan Role, only the ConsistencyCheck Workload.

ACTOR Future<Version> getVersion(Database cx);
ACTOR Future<bool> getKeyServers(
    Database cx,
    Promise<std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>>> keyServersPromise,
    KeyRangeRef kr,
    bool performQuiescentChecks,
    bool failureIsError,
    bool* success);
ACTOR Future<bool> getKeyLocations(Database cx,
                                   std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>> shards,
                                   Promise<Standalone<VectorRef<KeyValueRef>>> keyLocationPromise,
                                   bool performQuiescentChecks,
                                   bool* success);
ACTOR Future<Void> checkDataConsistency(Database cx,
                                        VectorRef<KeyValueRef> keyLocations,
                                        DatabaseConfiguration configuration,
                                        std::map<UID, StorageServerInterface> tssMapping,
                                        bool performQuiescentChecks,
                                        bool performTSSCheck,
                                        bool firstClient,
                                        bool failureIsError,
                                        int clientId,
                                        int clientCount,
                                        bool distributed,
                                        bool shuffleShards,
                                        int shardSampleFactor,
                                        int64_t sharedRandomNumber,
                                        int64_t repetitions,
                                        int64_t* bytesReadInPreviousRound,
                                        int restart,
                                        int64_t maxRate,
                                        int64_t targetInterval,
                                        bool* success);

#include "flow/unactorcompiler.h"

#endif // FDBCLIENT_CONSISTENCYSCANINTERFACE_H