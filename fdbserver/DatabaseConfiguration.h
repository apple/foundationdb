/*
 * DatabaseConfiguration.h
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

#ifndef FDBSERVER_DATABASECONFIGURATION_H
#define FDBSERVER_DATABASECONFIGURATION_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbrpc/ReplicationPolicy.h"

// SOMEDAY: Buggify DatabaseConfiguration

struct DatabaseConfiguration {
	DatabaseConfiguration();

	void applyMutation( MutationRef mutation );
	bool set( KeyRef key, ValueRef value );  // Returns true if a configuration option that requires recovery to take effect is changed
	bool clear( KeyRangeRef keys );
	Optional<ValueRef> get( KeyRef key ) const;

	bool isValid() const;

	bool initialized;

	std::string toString() const;
	std::map<std::string, std::string> toMap() const;
	int expectedLogSets() {
		int result = 1;
		if( satelliteTLogReplicationFactor > 0) {
			result++;
		}
		if( remoteTLogReplicationFactor > 0) {
			result++;
		}
		return result;
	}

	// SOMEDAY: think about changing storageTeamSize to durableStorageQuorum
	int32_t minDatacentersRequired() const {
		if(!primaryDcId.present()) return 1;
		return 2 + primarySatelliteDcIds.size() + remoteSatelliteDcIds.size();
	}
	int32_t minMachinesRequiredPerDatacenter() const { return std::max( satelliteTLogReplicationFactor/std::max(1,satelliteTLogUsableDcs), std::max( remoteTLogReplicationFactor, std::max(tLogReplicationFactor, storageTeamSize) ) ); }

	//Killing an entire datacenter counts as killing one machine in modes that support it
	int32_t maxMachineFailuresTolerated() const {
		if(remoteTLogReplicationFactor > 0 && satelliteTLogReplicationFactor > 0) {
			return 1 + std::min(std::max(tLogReplicationFactor - 1 - tLogWriteAntiQuorum, satelliteTLogReplicationFactor - 1 - satelliteTLogWriteAntiQuorum), durableStorageQuorum - 1);
		} else if(satelliteTLogReplicationFactor > 0) {
			return std::min(tLogReplicationFactor + satelliteTLogReplicationFactor - 2 - tLogWriteAntiQuorum - satelliteTLogWriteAntiQuorum, durableStorageQuorum - 1);
		}
		return std::min(tLogReplicationFactor - 1 - tLogWriteAntiQuorum, durableStorageQuorum - 1);
	}

	// MasterProxy Servers
	int32_t masterProxyCount;
	int32_t autoMasterProxyCount;

	// Resolvers
	int32_t resolverCount;
	int32_t autoResolverCount;

	// TLogs
	IRepPolicyRef tLogPolicy;
	int32_t desiredTLogCount;
	int32_t autoDesiredTLogCount;
	int32_t tLogWriteAntiQuorum;
	int32_t tLogReplicationFactor;
	KeyValueStoreType tLogDataStoreType;
	Optional<Standalone<StringRef>> primaryDcId;

	// Storage Servers
	IRepPolicyRef storagePolicy;
	int32_t durableStorageQuorum;
	int32_t storageTeamSize;
	KeyValueStoreType storageServerStoreType;

	// Remote TLogs
	int32_t remoteDesiredTLogCount;
	int32_t remoteTLogReplicationFactor;
	int32_t desiredLogRouterCount;
	IRepPolicyRef remoteTLogPolicy;
	std::vector<Optional<Standalone<StringRef>>> remoteDcIds;

	// Satellite TLogs
	IRepPolicyRef satelliteTLogPolicy;
	int32_t satelliteDesiredTLogCount;
	int32_t satelliteTLogReplicationFactor;
	int32_t satelliteTLogWriteAntiQuorum;
	int32_t satelliteTLogUsableDcs;
	std::vector<Optional<Standalone<StringRef>>> primarySatelliteDcIds;
	std::vector<Optional<Standalone<StringRef>>> remoteSatelliteDcIds;

	// Excluded servers (no state should be here)
	bool isExcludedServer( NetworkAddress ) const;
	std::set<AddressExclusion> getExcludedServers() const;

	int32_t getDesiredProxies() const { if(masterProxyCount == -1) return autoMasterProxyCount; return masterProxyCount; }
	int32_t getDesiredResolvers() const { if(resolverCount == -1) return autoResolverCount; return resolverCount; }
	int32_t getDesiredLogs() const { if(desiredTLogCount == -1) return autoDesiredTLogCount; return desiredTLogCount; }
	int32_t getDesiredSatelliteLogs() const { if(satelliteDesiredTLogCount == -1) return autoDesiredTLogCount; return satelliteDesiredTLogCount; }
	int32_t getDesiredRemoteLogs() const { if(remoteDesiredTLogCount == -1) return autoDesiredTLogCount; return remoteDesiredTLogCount; }
	int32_t getDesiredLogRouters() const { if(desiredLogRouterCount == -1) return getDesiredRemoteLogs(); return desiredLogRouterCount; }

	bool operator == ( DatabaseConfiguration const& rhs ) const {
		const_cast<DatabaseConfiguration*>(this)->makeConfigurationImmutable();
		const_cast<DatabaseConfiguration*>(&rhs)->makeConfigurationImmutable();
		return rawConfiguration == rhs.rawConfiguration;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		if (!ar.isDeserializing) makeConfigurationImmutable();
		ar & rawConfiguration;
		if (ar.isDeserializing) {
			for(auto c=rawConfiguration.begin(); c!=rawConfiguration.end(); ++c)
				setInternal(c->key, c->value);
			setDefaultReplicationPolicy();
		}
	}

	void fromKeyValues( Standalone<VectorRef<KeyValueRef>> rawConfig ) {
		resetInternal();
		this->rawConfiguration = rawConfig;
		for(auto c=rawConfiguration.begin(); c!=rawConfiguration.end(); ++c)
			setInternal(c->key, c->value);
		setDefaultReplicationPolicy();
	}

private:
	Optional< std::map<std::string, std::string> > mutableConfiguration;  // If present, rawConfiguration is not valid
	Standalone<VectorRef<KeyValueRef>> rawConfiguration;   // sorted by key

	void makeConfigurationMutable();
	void makeConfigurationImmutable();

	bool setInternal( KeyRef key, ValueRef value );
	void resetInternal();
	void setDefaultReplicationPolicy();
};

#endif
