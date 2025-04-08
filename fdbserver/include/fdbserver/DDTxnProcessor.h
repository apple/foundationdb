/*
 * DDTxnProcessor.h
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

#ifndef FOUNDATIONDB_DDTXNPROCESSOR_H
#define FOUNDATIONDB_DDTXNPROCESSOR_H

#include "fdbserver/Knobs.h"
#include "flow/FastRef.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/MockGlobalState.h"

FDB_BOOLEAN_PARAM(SkipDDModeCheck);

struct InitialDataDistribution;
struct DDShardInfo;

struct ServerWorkerInfos {
	std::vector<std::pair<StorageServerInterface, ProcessClass>> servers;
	Optional<Version> readVersion; // the read version of the txn reading server lists
};

/* Testability Contract:
 * a. The DataDistributor has to use this interface to interact with data-plane (aka. run transaction / use Database),
 * because the testability benefits from a mock implementation; b. Other control-plane roles should consider providing
 * its own TxnProcessor interface to provide testability, for example, Ratekeeper.
 * */
class IDDTxnProcessor : public ReferenceCounted<IDDTxnProcessor> {
public:
	struct SourceServers {
		std::vector<UID> srcServers, completeSources; // the same as RelocateData.src, RelocateData.completeSources;
	};

	struct DDRangeLocations {
		DDRangeLocations() = default;
		DDRangeLocations(KeyRangeRef range) : range(range) {}

		// A map of dcId : list of servers
		std::map<std::string, std::vector<StorageServerInterface>> servers;
		KeyRange range;
	};

	virtual Database context() const = 0;
	virtual bool isMocked() const = 0;
	// get the source server list and complete source server list for range
	virtual Future<SourceServers> getSourceServersForRange(const KeyRangeRef range) = 0;

	virtual Future<std::vector<DDRangeLocations>> getSourceServerInterfacesForRange(const KeyRangeRef range) {
		return std::vector<DDRangeLocations>();
	}

	virtual Future<Void> waitForAllDataRemoved(
	    const UID& serverID,
	    const Version& addedVersion,
	    Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure) const = 0;

	// get the storage server list and Process class, only throw transaction non-retryable exceptions
	virtual Future<ServerWorkerInfos> getServerListAndProcessClasses() = 0;

	virtual Future<Reference<InitialDataDistribution>> getInitialDataDistribution(
	    const UID& distributorId,
	    const MoveKeysLock& moveKeysLock,
	    const std::vector<Optional<Key>>& remoteDcIds,
	    const DDEnabledState* ddEnabledState,
	    SkipDDModeCheck skipDDModeCheck) = 0;

	virtual ~IDDTxnProcessor() = default;

	[[nodiscard]] virtual Future<MoveKeysLock> takeMoveKeysLock(const UID& ddId) const { return MoveKeysLock(); }

	virtual Future<DatabaseConfiguration> getDatabaseConfiguration() const = 0;

	virtual Future<Void> updateReplicaKeys(const std::vector<Optional<Key>>& primaryIds,
	                                       const std::vector<Optional<Key>>& remoteIds,
	                                       const DatabaseConfiguration& configuration) const {
		return Void();
	}

	virtual Future<int> tryUpdateReplicasKeyForDc(const Optional<Key>& dcId, const int& storageTeamSize) const {
		return storageTeamSize;
	}

	virtual Future<Void> waitForDataDistributionEnabled(const DDEnabledState* ddEnabledState) const { return Void(); };

	virtual Future<bool> isDataDistributionEnabled(const DDEnabledState* ddEnabledState) const {
		return ddEnabledState->isEnabled();
	};

	virtual Future<Void> pollMoveKeysLock(const MoveKeysLock& lock, const DDEnabledState* ddEnabledState) const = 0;

	// Remove the server from shardMapping and set serverKeysFalse to the server's serverKeys list.
	// Changes to keyServer and serverKey must happen symmetrically in this function.
	// If serverID is the last source server for a shard, the shard will be erased, and then be assigned
	// to teamForDroppedRange.
	// It's used by `exclude failed` command to bypass data movement from failed server.
	virtual Future<Void> removeKeysFromFailedServer(const UID& serverID,
	                                                const std::vector<UID>& teamForDroppedRange,
	                                                const MoveKeysLock& lock,
	                                                const DDEnabledState* ddEnabledState) const = 0;
	virtual Future<Void> removeStorageServer(const UID& serverID,
	                                         const Optional<UID>& tssPairID,
	                                         const MoveKeysLock& lock,
	                                         const DDEnabledState* ddEnabledState) const = 0;

	virtual Future<Void> moveKeys(const MoveKeysParams& params) = 0;

	// metrics.second is the number of key-ranges (i.e., shards) in the 'keys' key-range
	virtual Future<std::pair<Optional<StorageMetrics>, int>> waitStorageMetrics(KeyRange const& keys,
	                                                                            StorageMetrics const& min,
	                                                                            StorageMetrics const& max,
	                                                                            StorageMetrics const& permittedError,
	                                                                            int shardLimit,
	                                                                            int expectedShardCount) const = 0;

	virtual Future<Standalone<VectorRef<KeyRef>>> splitStorageMetrics(
	    KeyRange const& keys,
	    StorageMetrics const& limit,
	    StorageMetrics const& estimated,
	    Optional<int> const& minSplitBytes = {}) const = 0;

	virtual Future<Standalone<VectorRef<ReadHotRangeWithMetrics>>> getReadHotRanges(KeyRange const& keys) const = 0;

	virtual Future<HealthMetrics> getHealthMetrics(bool detailed = false) const = 0;

	virtual Future<Optional<Value>> readRebalanceDDIgnoreKey() const = 0;

	virtual Future<Void> waitDDTeamInfoPrintSignal() const { return Never(); }

	virtual Future<std::vector<ProcessData>> getWorkers() const = 0;

	virtual Future<Optional<HealthMetrics::StorageStats>> getStorageStats(const UID& id, double maxStaleness) const = 0;
};

class DDTxnProcessorImpl;

// run transactions over real database
class DDTxnProcessor : public IDDTxnProcessor {
	friend class DDTxnProcessorImpl;
	Database cx;

public:
	DDTxnProcessor() = default;
	explicit DDTxnProcessor(Database cx) : cx(cx) {}

	Database context() const override { return cx; };
	bool isMocked() const override { return false; };

	Future<SourceServers> getSourceServersForRange(const KeyRangeRef range) override;

	Future<std::vector<IDDTxnProcessor::DDRangeLocations>> getSourceServerInterfacesForRange(
	    const KeyRangeRef range) override;

	// Call NativeAPI implementation directly
	Future<ServerWorkerInfos> getServerListAndProcessClasses() override;

	Future<Reference<InitialDataDistribution>> getInitialDataDistribution(const UID& distributorId,
	                                                                      const MoveKeysLock& moveKeysLock,
	                                                                      const std::vector<Optional<Key>>& remoteDcIds,
	                                                                      const DDEnabledState* ddEnabledState,
	                                                                      SkipDDModeCheck skipDDModeCheck) override;

	Future<MoveKeysLock> takeMoveKeysLock(UID const& ddId) const override;

	Future<DatabaseConfiguration> getDatabaseConfiguration() const override;

	Future<Void> updateReplicaKeys(const std::vector<Optional<Key>>& primaryIds,
	                               const std::vector<Optional<Key>>& remoteIds,
	                               const DatabaseConfiguration& configuration) const override;

	Future<int> tryUpdateReplicasKeyForDc(const Optional<Key>& dcId, const int& storageTeamSize) const override;

	Future<Void> waitForDataDistributionEnabled(const DDEnabledState* ddEnabledState) const override;

	Future<bool> isDataDistributionEnabled(const DDEnabledState* ddEnabledState) const override;

	Future<Void> pollMoveKeysLock(const MoveKeysLock& lock, const DDEnabledState* ddEnabledState) const override;

	Future<Void> removeKeysFromFailedServer(const UID& serverID,
	                                        const std::vector<UID>& teamForDroppedRange,
	                                        const MoveKeysLock& lock,
	                                        const DDEnabledState* ddEnabledState) const override {
		return ::removeKeysFromFailedServer(cx, serverID, teamForDroppedRange, lock, ddEnabledState);
	}

	Future<Void> removeStorageServer(const UID& serverID,
	                                 const Optional<UID>& tssPairID,
	                                 const MoveKeysLock& lock,
	                                 const DDEnabledState* ddEnabledState) const override {
		return ::removeStorageServer(cx, serverID, tssPairID, lock, ddEnabledState);
	}

	Future<Void> moveKeys(const MoveKeysParams& params) override { return ::moveKeys(cx, params); }

	Future<std::pair<Optional<StorageMetrics>, int>> waitStorageMetrics(KeyRange const& keys,
	                                                                    StorageMetrics const& min,
	                                                                    StorageMetrics const& max,
	                                                                    StorageMetrics const& permittedError,
	                                                                    int shardLimit,
	                                                                    int expectedShardCount) const override;

	Future<Standalone<VectorRef<KeyRef>>> splitStorageMetrics(KeyRange const& keys,
	                                                          StorageMetrics const& limit,
	                                                          StorageMetrics const& estimated,
	                                                          Optional<int> const& minSplitBytes = {}) const override;

	Future<Standalone<VectorRef<ReadHotRangeWithMetrics>>> getReadHotRanges(KeyRange const& keys) const override;

	Future<HealthMetrics> getHealthMetrics(bool detailed) const override;

	Future<Optional<Value>> readRebalanceDDIgnoreKey() const override;

	Future<Void> waitDDTeamInfoPrintSignal() const override;

	Future<std::vector<ProcessData>> getWorkers() const override;

	Future<Optional<HealthMetrics::StorageStats>> getStorageStats(const UID& id, double maxStaleness) const override;

	Future<Void> waitForAllDataRemoved(
	    const UID& serverID,
	    const Version& addedVersion,
	    Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure) const override;

protected:
	Future<Void> rawStartMovement(const MoveKeysParams& params, std::map<UID, StorageServerInterface>& tssMapping);

	Future<Void> rawFinishMovement(const MoveKeysParams& params,
	                               const std::map<UID, StorageServerInterface>& tssMapping);
};

struct DDMockTxnProcessorImpl;
// A mock transaction implementation for test usage.
// Contract: every function involving mock transaction should return immediately to mimic the ACI property of real
// transaction.
class DDMockTxnProcessor : public IDDTxnProcessor {
	friend struct DDMockTxnProcessorImpl;

protected:
	std::shared_ptr<MockGlobalState> mgs;

	std::vector<DDShardInfo> getDDShardInfos() const;

public:
	explicit DDMockTxnProcessor(std::shared_ptr<MockGlobalState> mgs = nullptr) : mgs(std::move(mgs)) {};

	Future<ServerWorkerInfos> getServerListAndProcessClasses() override;

	Future<Reference<InitialDataDistribution>> getInitialDataDistribution(const UID& distributorId,
	                                                                      const MoveKeysLock& moveKeysLock,
	                                                                      const std::vector<Optional<Key>>& remoteDcIds,
	                                                                      const DDEnabledState* ddEnabledState,
	                                                                      SkipDDModeCheck skipDDModeCheck) override;

	Future<Void> removeKeysFromFailedServer(const UID& serverID,
	                                        const std::vector<UID>& teamForDroppedRange,
	                                        const MoveKeysLock& lock,
	                                        const DDEnabledState* ddEnabledState) const override;

	Future<Void> removeStorageServer(const UID& serverID,
	                                 const Optional<UID>& tssPairID,
	                                 const MoveKeysLock& lock,
	                                 const DDEnabledState* ddEnabledState) const override;

	// test only
	void setupMockGlobalState(Reference<InitialDataDistribution> initData);

	Future<Void> moveKeys(const MoveKeysParams& params) override;

	Database context() const override { UNREACHABLE(); };

	bool isMocked() const override { return true; };

	Future<std::pair<Optional<StorageMetrics>, int>> waitStorageMetrics(KeyRange const& keys,
	                                                                    StorageMetrics const& min,
	                                                                    StorageMetrics const& max,
	                                                                    StorageMetrics const& permittedError,
	                                                                    int shardLimit,
	                                                                    int expectedShardCount) const override;

	Future<Standalone<VectorRef<KeyRef>>> splitStorageMetrics(KeyRange const& keys,
	                                                          StorageMetrics const& limit,
	                                                          StorageMetrics const& estimated,
	                                                          Optional<int> const& minSplitBytes = {}) const override;

	Future<Standalone<VectorRef<ReadHotRangeWithMetrics>>> getReadHotRanges(KeyRange const& keys) const override {
		UNREACHABLE();
	}

	Future<HealthMetrics> getHealthMetrics(bool detailed = false) const override;

	Future<std::vector<ProcessData>> getWorkers() const override;

	Future<Void> pollMoveKeysLock(const MoveKeysLock& lock, const DDEnabledState* ddEnabledState) const override {
		return Never();
	}

	Future<Optional<HealthMetrics::StorageStats>> getStorageStats(const UID& id, double maxStaleness) const override;

	Future<DatabaseConfiguration> getDatabaseConfiguration() const override;

	Future<SourceServers> getSourceServersForRange(const KeyRangeRef range) override;

	Future<Optional<Value>> readRebalanceDDIgnoreKey() const override { return Optional<Value>(); }

	Future<Void> waitForAllDataRemoved(
	    const UID& serverID,
	    const Version& addedVersion,
	    Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure) const override;

protected:
	Future<Void> rawStartMovement(const MoveKeysParams& params, std::map<UID, StorageServerInterface>& tssMapping);

	Future<Void> rawFinishMovement(const MoveKeysParams& params,
	                               const std::map<UID, StorageServerInterface>& tssMapping);
};

#endif // FOUNDATIONDB_DDTXNPROCESSOR_H
