/*
 * DatabaseConfiguration.h
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

#ifndef FDBCLIENT_DATABASECONFIGURATION_H
#define FDBCLIENT_DATABASECONFIGURATION_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbclient/Status.h"

// SOMEDAY: Buggify DatabaseConfiguration

struct SatelliteInfo {
	Key dcId;
	int32_t priority;
	int32_t satelliteDesiredTLogCount = -1;

	SatelliteInfo() : priority(0) {}

	struct sort_by_priority {
		bool operator()(SatelliteInfo const& a, SatelliteInfo const& b) const { return a.priority > b.priority; }
	};

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, dcId, priority, satelliteDesiredTLogCount);
	}
};

struct RegionInfo {
	Key dcId;
	int32_t priority;

	Reference<IReplicationPolicy> satelliteTLogPolicy;

	// Number of tLogs that should be recruited in satellite datacenters.
	int32_t satelliteDesiredTLogCount;

	// Total number of copies made for each mutation across all satellite tLogs in all DCs.
	int32_t satelliteTLogReplicationFactor;

	// Number of tLog replies we can ignore when waiting for quorum. Hence, effective quorum is
	// satelliteDesiredTLogCount -  satelliteTLogWriteAntiQuorum. Locality of individual tLogs is not taken
	// into account.
	int32_t satelliteTLogWriteAntiQuorum;

	// Number of satellite datacenters for current region, as set by `satellite_redundancy_mode`.
	int32_t satelliteTLogUsableDcs;

	Reference<IReplicationPolicy> satelliteTLogPolicyFallback;
	int32_t satelliteTLogReplicationFactorFallback;
	int32_t satelliteTLogWriteAntiQuorumFallback;
	int32_t satelliteTLogUsableDcsFallback;

	std::vector<SatelliteInfo> satellites;

	RegionInfo()
	  : priority(0), satelliteDesiredTLogCount(-1), satelliteTLogReplicationFactor(0), satelliteTLogWriteAntiQuorum(0),
	    satelliteTLogUsableDcs(1), satelliteTLogReplicationFactorFallback(0), satelliteTLogWriteAntiQuorumFallback(0),
	    satelliteTLogUsableDcsFallback(0) {}

	struct sort_by_priority {
		bool operator()(RegionInfo const& a, RegionInfo const& b) const { return a.priority > b.priority; }
	};

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           dcId,
		           priority,
		           satelliteTLogPolicy,
		           satelliteDesiredTLogCount,
		           satelliteTLogReplicationFactor,
		           satelliteTLogWriteAntiQuorum,
		           satelliteTLogUsableDcs,
		           satelliteTLogPolicyFallback,
		           satelliteTLogReplicationFactorFallback,
		           satelliteTLogWriteAntiQuorumFallback,
		           satelliteTLogUsableDcsFallback,
		           satellites);
	}
};

struct DatabaseConfiguration {
	DatabaseConfiguration();

	void applyMutation(MutationRef mutation);
	bool set(KeyRef key,
	         ValueRef value); // Returns true if a configuration option that requires recovery to take effect is changed
	bool clear(KeyRangeRef keys);
	Optional<ValueRef> get(KeyRef key) const;

	bool isValid() const;

	bool initialized;

	std::string toString() const;
	StatusObject toJSON(bool noPolicies = false) const;
	StatusArray getRegionJSON() const;

	RegionInfo getRegion(Optional<Key> dcId) const {
		if (!dcId.present()) {
			return RegionInfo();
		}
		for (auto& r : regions) {
			if (r.dcId == dcId.get()) {
				return r;
			}
		}
		return RegionInfo();
	}

	int expectedLogSets(Optional<Key> dcId) const {
		int result = 1;
		if (dcId.present() && getRegion(dcId.get()).satelliteTLogReplicationFactor > 0 && usableRegions > 1) {
			result++;
		}

		if (usableRegions > 1) {
			result++;
		}
		return result;
	}

	// Counts the number of DCs required including remote and satellites for current database configuraiton.
	int32_t minDatacentersRequired() const {
		int minRequired = 0;
		for (auto& r : regions) {
			minRequired += 1 + r.satellites.size();
		}
		return minRequired;
	}

	int32_t minZonesRequiredPerDatacenter() const {
		int minRequired = std::max(remoteTLogReplicationFactor, std::max(tLogReplicationFactor, storageTeamSize));
		for (auto& r : regions) {
			minRequired =
			    std::max(minRequired, r.satelliteTLogReplicationFactor / std::max(1, r.satelliteTLogUsableDcs));
		}
		return minRequired;
	}

	// Retuns the maximum number of discrete failures a cluster can tolerate.
	// In HA mode, `fullyReplicatedRegions` is set to "1" initially when data is being
	// replicated to remote, and will be incremented later. `forAvailablity` is set to true
	// if we want to account the number for machines that can recruit new tLogs/SS after failures.
	// Killing an entire datacenter counts as killing one zone in modes that support it.
	int32_t maxZoneFailuresTolerated(int fullyReplicatedRegions, bool forAvailability) const {
		int worstSatelliteTLogReplicationFactor = regions.size() ? std::numeric_limits<int>::max() : 0;
		int regionsWithNonNegativePriority = 0;
		for (auto& r : regions) {
			if (r.priority >= 0) {
				regionsWithNonNegativePriority++;
			}
			worstSatelliteTLogReplicationFactor = std::min(
			    worstSatelliteTLogReplicationFactor, r.satelliteTLogReplicationFactor - r.satelliteTLogWriteAntiQuorum);
			if (r.satelliteTLogUsableDcsFallback > 0) {
				worstSatelliteTLogReplicationFactor =
				    std::min(worstSatelliteTLogReplicationFactor,
				             r.satelliteTLogReplicationFactorFallback - r.satelliteTLogWriteAntiQuorumFallback);
			}
		}

		if (worstSatelliteTLogReplicationFactor <= 0) {
			// HA is not enabled in this database. Return single cluster zone failures to tolerate.
			return std::min(tLogReplicationFactor - 1 - tLogWriteAntiQuorum, storageTeamSize - 1);
		}

		// Compute HA enabled database zone failure tolerance.
		auto isGeoReplicatedData = [this, &fullyReplicatedRegions]() {
			return usableRegions > 1 && fullyReplicatedRegions > 1;
		};

		if (isGeoReplicatedData() && (!forAvailability || regionsWithNonNegativePriority > 1)) {
			return 1 + std::min(std::max(tLogReplicationFactor - 1 - tLogWriteAntiQuorum,
			                             worstSatelliteTLogReplicationFactor - 1),
			                    storageTeamSize - 1);
		}
		// Primary and Satellite tLogs are synchronously replicated, hence we can lose all but 1.
		return std::min(tLogReplicationFactor + worstSatelliteTLogReplicationFactor - 1 - tLogWriteAntiQuorum,
		                storageTeamSize - 1);
	}

	// CommitProxy Servers
	int32_t commitProxyCount;
	int32_t autoCommitProxyCount;
	int32_t grvProxyCount;
	int32_t autoGrvProxyCount;

	// Resolvers
	int32_t resolverCount;
	int32_t autoResolverCount;

	// TLogs
	Reference<IReplicationPolicy> tLogPolicy;
	int32_t desiredTLogCount;
	int32_t autoDesiredTLogCount;
	int32_t tLogWriteAntiQuorum;
	int32_t tLogReplicationFactor;
	TLogVersion tLogVersion;
	KeyValueStoreType tLogDataStoreType;
	TLogSpillType tLogSpillType;

	// Storage Servers
	Reference<IReplicationPolicy> storagePolicy;
	int32_t storageTeamSize;
	KeyValueStoreType storageServerStoreType;

	// Testing StorageServers
	int32_t desiredTSSCount;
	KeyValueStoreType testingStorageServerStoreType;

	// Remote TLogs
	int32_t desiredLogRouterCount;
	int32_t remoteDesiredTLogCount;
	int32_t remoteTLogReplicationFactor;
	Reference<IReplicationPolicy> remoteTLogPolicy;

	// Backup Workers
	bool backupWorkerEnabled;

	// Data centers
	int32_t usableRegions; // Number of regions which have a replica of the database.
	int32_t repopulateRegionAntiQuorum;
	std::vector<RegionInfo> regions;

	// Perpetual Storage Setting
	int32_t perpetualStorageWiggleSpeed;
	std::string perpetualStorageWiggleLocality;

	// Storage Migration Type
	StorageMigrationType storageMigrationType;

	// Blob Granules
	bool blobGranulesEnabled;
	TenantMode tenantMode;

	// Excluded servers (no state should be here)
	bool isExcludedServer(NetworkAddressList) const;
	bool isExcludedLocality(const LocalityData& locality) const;
	bool isMachineExcluded(const LocalityData& locality) const;
	std::set<AddressExclusion> getExcludedServers() const;
	std::set<std::string> getExcludedLocalities() const;

	int32_t getDesiredCommitProxies() const {
		if (commitProxyCount == -1)
			return autoCommitProxyCount;
		return commitProxyCount;
	}
	int32_t getDesiredGrvProxies() const {
		if (grvProxyCount == -1)
			return autoGrvProxyCount;
		return grvProxyCount;
	}
	int32_t getDesiredResolvers() const {
		if (resolverCount == -1)
			return autoResolverCount;
		return resolverCount;
	}
	int32_t getDesiredLogs() const {
		if (desiredTLogCount == -1)
			return autoDesiredTLogCount;
		return desiredTLogCount;
	}
	int32_t getDesiredRemoteLogs() const {
		if (remoteDesiredTLogCount == -1)
			return getDesiredLogs();
		return remoteDesiredTLogCount;
	}
	int32_t getDesiredSatelliteLogs(Optional<Key> dcId) const {
		auto desired = getRegion(dcId).satelliteDesiredTLogCount;
		if (desired == -1)
			return autoDesiredTLogCount;
		return desired;
	}
	int32_t getRemoteTLogReplicationFactor() const {
		if (remoteTLogReplicationFactor == 0)
			return tLogReplicationFactor;
		return remoteTLogReplicationFactor;
	}
	Reference<IReplicationPolicy> getRemoteTLogPolicy() const {
		if (remoteTLogReplicationFactor == 0)
			return tLogPolicy;
		return remoteTLogPolicy;
	}

	bool operator==(DatabaseConfiguration const& rhs) const {
		const_cast<DatabaseConfiguration*>(this)->makeConfigurationImmutable();
		const_cast<DatabaseConfiguration*>(&rhs)->makeConfigurationImmutable();
		return rawConfiguration == rhs.rawConfiguration;
	}
	bool operator!=(DatabaseConfiguration const& rhs) const { return !(*this == rhs); }

	template <class Ar>
	void serialize(Ar& ar) {
		if (!ar.isDeserializing)
			makeConfigurationImmutable();
		serializer(ar, rawConfiguration);
		if (ar.isDeserializing) {
			for (auto c = rawConfiguration.begin(); c != rawConfiguration.end(); ++c)
				setInternal(c->key, c->value);
			setDefaultReplicationPolicy();
		}
	}

	void fromKeyValues(Standalone<VectorRef<KeyValueRef>> rawConfig);

private:
	Optional<std::map<std::string, std::string>> mutableConfiguration; // If present, rawConfiguration is not valid
	Standalone<VectorRef<KeyValueRef>> rawConfiguration; // sorted by key

	void makeConfigurationMutable();
	void makeConfigurationImmutable();

	bool setInternal(KeyRef key, ValueRef value);
	void resetInternal();
	void setDefaultReplicationPolicy();

	/// Check if the key is overridden by either mutableConfiguration or rawConfiguration
	bool isOverridden(std::string key) const;
	// Overwrite commitProxyCount and/or grvProxyCount if set to -1
	void overwriteProxiesCount();
};

#endif
