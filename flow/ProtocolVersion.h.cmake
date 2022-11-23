/*
 * ProtocolVersion.h
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
#include "flow/Trace.h"
#include <cstdint>

// This version impacts both communications and the deserialization of certain database and IKeyValueStore keys.
constexpr uint64_t defaultProtocolVersionValue = @FDB_PV_DEFAULT_VERSION@;

// The first protocol version that cannot be downgraded from. Ordinarily, this will be two release versions larger
// than the current version, meaning that we only support downgrades between consecutive release versions.
constexpr uint64_t minInvalidProtocolVersionValue = @FDB_PV_MIN_INVALID_VERSION@;

// The lowest protocol version that can be downgraded to.
constexpr uint64_t minCompatibleProtocolVersionValue = @FDB_PV_MIN_COMPATIBLE_VERSION@;

// The protocol version that will most likely follow the current one
// Used only for testing upgrades to the future version
constexpr uint64_t futureProtocolVersionValue = @FDB_PV_FUTURE_VERSION@;

// The first check second expression version doesn't need to change because it's just for earlier protocol versions.
#define PROTOCOL_VERSION_FEATURE(v, x)                                                                                 \
	static_assert((v & @FDB_PV_LSB_MASK@) == 0 || v < 0x0FDB00B071000000LL, "Unexpected feature protocol version");             \
	static_assert(v <= defaultProtocolVersionValue, "Feature protocol version too large");                             \
	struct x {                                                                                                         \
		static constexpr uint64_t protocolVersion = v;                                                                 \
	};                                                                                                                 \
	constexpr bool has##x() const { return this->version() >= x ::protocolVersion; }                                   \
	static constexpr ProtocolVersion with##x() { return ProtocolVersion(x ::protocolVersion); }

// ProtocolVersion wraps a uint64_t to make it type safe. It will know about the current versions.
// The default constructor will initialize the version to 0 (which is an invalid
// version). ProtocolVersion objects should never be compared to version numbers
// directly. Instead one should always use the type-safe version types from which
// this class inherits all.
class ProtocolVersion {
	uint64_t _version;

public: // constants
	static constexpr uint64_t versionFlagMask = 0x0FFFFFFFFFFFFFFFLL;
	static constexpr uint64_t objectSerializerFlag = 0x1000000000000000LL;
	static constexpr uint64_t compatibleProtocolVersionMask = 0xFFFFFFFFFFFF0000LL;
	static constexpr uint64_t minValidProtocolVersion = 0x0FDB00A200060001LL;
	static constexpr uint64_t invalidProtocolVersion = 0x0FDB00A100000000LL;

public:
	constexpr explicit ProtocolVersion(uint64_t version) : _version(version) {}
	constexpr ProtocolVersion() : _version(0) {}

	constexpr bool isCompatible(ProtocolVersion other) const {
		return (other.version() & compatibleProtocolVersionMask) == (version() & compatibleProtocolVersionMask);
	}

	// Returns a normalized protocol version that will be the same for all compatible versions
	constexpr ProtocolVersion normalizedVersion() const {
		return ProtocolVersion(_version & compatibleProtocolVersionMask);
	}

	constexpr bool isValid() const {
		return version() >= minValidProtocolVersion;
	}

	constexpr bool isInvalid() const { return version() == invalidProtocolVersion; }

	constexpr uint64_t version() const { return _version & versionFlagMask; }
	constexpr uint64_t versionWithFlags() const { return _version; }

	constexpr bool hasObjectSerializerFlag() const { return (_version & objectSerializerFlag) > 0; }
	constexpr void addObjectSerializerFlag() { _version = _version | objectSerializerFlag; }
	constexpr void removeObjectSerializerFlag() {
		_version = hasObjectSerializerFlag() ? _version ^ objectSerializerFlag : _version;
	}
	constexpr void removeAllFlags() { _version = version(); }

	// comparison operators
	// Comparison operators ignore the flags - this is because the version flags are stored in the
	// most significant byte which can make comparison confusing. Also, generally, when one wants to
	// compare versions, we are usually not interested in the flags.
	constexpr bool operator==(const ProtocolVersion other) const { return version() == other.version(); }
	constexpr bool operator!=(const ProtocolVersion other) const { return version() != other.version(); }
	constexpr bool operator<=(const ProtocolVersion other) const { return version() <= other.version(); }
	constexpr bool operator>=(const ProtocolVersion other) const { return version() >= other.version(); }
	constexpr bool operator<(const ProtocolVersion other) const { return version() < other.version(); }
	constexpr bool operator>(const ProtocolVersion other) const { return version() > other.version(); }

public: // introduced features
	// The 5th digit from right is dev version, for example, 2 in 0x0FDB00B061020000LL;
	// It was used to identify a protocol change (e.g., interface change) between major/minor versions (say 5.1 and 5.2)
	// We stopped using the dev version consistently in the past.
	// To ensure binaries work across patch releases (e.g., 6.2.0 to 6.2.22), we require that the protocol version be
	// the same for each of them.
	PROTOCOL_VERSION_FEATURE(@FDB_PV_WATCHES@, Watches);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_MOVABLE_COORDINATED_STATE@, MovableCoordinatedState);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_PROCESS_ID@, ProcessID);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_OPEN_DATABASE@, OpenDatabase);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_LOCALITY@, Locality);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_MULTIGENERATION_TLOG@, MultiGenerationTLog);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_SHARED_MUTATIONS@, SharedMutations);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_INEXPENSIVE_MULTIVERSION_CLIENT@, InexpensiveMultiVersionClient);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_TAG_LOCALITY@, TagLocality);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_FEARLESS@, Fearless);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_ENDPOINT_ADDR_LIST@, EndpointAddrList);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_IPV6@, IPv6);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_TLOG_VERSION@, TLogVersion);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_PSEUDO_LOCALITIES@, PseudoLocalities);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_SHARDED_TXS_TAGS@, ShardedTxsTags);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_TLOG_QUEUE_ENTRY_REF@, TLogQueueEntryRef);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_GENERATION_REG_VAL@, GenerationRegVal);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_MOVABLE_COORDINATED_STATE_V2@, MovableCoordinatedStateV2);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_KEY_SERVER_VALUE@, KeyServerValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_LOGS_VALUE@, LogsValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_SERVER_TAG_VALUE@, ServerTagValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_TAG_LOCALITY_LIST_VALUE@, TagLocalityListValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_DATACENTER_REPLICAS_VALUE@, DatacenterReplicasValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_PROCESS_CLASS_VALUE@, ProcessClassValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_WORKER_LIST_VALUE@, WorkerListValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_BACKUP_START_VALUE@, BackupStartValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_LOG_RANGE_ENCODE_VALUE@, LogRangeEncodeValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_HEALTHY_ZONE_VALUE@, HealthyZoneValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_DR_BACKUP_RANGES@, DRBackupRanges);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_REGION_CONFIGURATION@, RegionConfiguration);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_REPLICATION_POLICY@, ReplicationPolicy);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_BACKUP_MUTATIONS@, BackupMutations);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_CLUSTER_CONTROLLER_PRIORITY_INFO@, ClusterControllerPriorityInfo);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_PROCESS_ID_FILE@, ProcessIDFile);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_CLOSE_UNUSED_CONNECTION@, CloseUnusedConnection);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_DB_CORE_STATE@, DBCoreState);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_TAG_THROTTLE_VALUE@, TagThrottleValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_STORAGE_CACHE_VALUE@, StorageCacheValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_RESTORE_STATUS_VALUE@, RestoreStatusValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_RESTORE_REQUEST_VALUE@, RestoreRequestValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_RESTORE_REQUEST_DONE_VERSION_VALUE@, RestoreRequestDoneVersionValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_RESTORE_REQUEST_TRIGGER_VALUE@, RestoreRequestTriggerValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_RESTORE_WORKER_INTERFACE_VALUE@, RestoreWorkerInterfaceValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_BACKUP_PROGRESS_VALUE@, BackupProgressValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_KEY_SERVER_VALUE_V2@, KeyServerValueV2);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_UNIFIED_TLOG_SPILLING@, UnifiedTLogSpilling);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_BACKUP_WORKER@, BackupWorker);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_REPORT_CONFLICTING_KEYS@, ReportConflictingKeys);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_SMALL_ENDPOINTS@, SmallEndpoints);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_CACHE_ROLE@, CacheRole);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_STABLE_INTERFACES@, StableInterfaces);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_SERVER_LIST_VALUE@, ServerListValue);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_TAG_THROTTLE_VALUE_REASON@, TagThrottleValueReason);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_SPAN_CONTEXT@, SpanContext);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_TSS@, TSS);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_CHANGE_FEED@, ChangeFeed);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_BLOB_GRANULE@, BlobGranule);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_NETWORK_ADDRESS_HOSTNAME_FLAG@, NetworkAddressHostnameFlag);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_STORAGE_METADATA@, StorageMetadata);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_PERPETUAL_WIGGLE_METADATA@, PerpetualWiggleMetadata);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_STORAGE_INTERFACE_READINESS@, StorageInterfaceReadiness);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_RESOLVER_PRIVATE_MUTATIONS@, ResolverPrivateMutations);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_OTEL_SPAN_CONTEXT@, OTELSpanContext);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_SW_VERSION_TRACKING@, SWVersionTracking);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_ENCRYPTION_AT_REST@, EncryptionAtRest);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_SHARD_ENCODE_LOCATION_METADATA@, ShardEncodeLocationMetaData);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_TENANTS@, Tenants);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_BLOB_GRANULE_FILE@, BlobGranuleFile);
	PROTOCOL_VERSION_FEATURE(@FDB_PV_CLUSTER_ID_SPECIAL_KEY@, ClusterIdSpecialKey);
};

template <>
struct Traceable<ProtocolVersion> : std::true_type {
	static std::string toString(const ProtocolVersion& protocolVersion) {
		return format("0x%016lX", protocolVersion.version());
	}
};

constexpr ProtocolVersion defaultProtocolVersion(defaultProtocolVersionValue);
constexpr ProtocolVersion minInvalidProtocolVersion(minInvalidProtocolVersionValue);
constexpr ProtocolVersion minCompatibleProtocolVersion(minCompatibleProtocolVersionValue);

// The protocol version of the process, normally it is equivalent defaultProtocolVersion
// The currentProtocolVersion can be overridden dynamically for testing upgrades
// to a future FDB version
ProtocolVersion currentProtocolVersion();

// Assume the next future protocol version as the current one. Used for testing purposes only
void useFutureProtocolVersion();

// This assert is intended to help prevent incrementing the leftmost digits accidentally. It will probably need to
// change when we reach version 10.
static_assert(defaultProtocolVersion.version() < @FDB_PV_LEFT_MOST_CHECK@, "Unexpected protocol version");

// The last two bytes of the protocol version are currently masked out in compatibility checks. We do not use them,
// so prevent them from being inadvertently changed.
//
// We also do not modify the protocol version for patch releases, so prevent modifying the patch version digit.
static_assert((defaultProtocolVersion.version() & @FDB_PV_LSB_MASK@) == 0, "Unexpected protocol version");

// Downgrades must support at least one minor version.
static_assert(minInvalidProtocolVersion.version() >=
                  (defaultProtocolVersion.version() & 0xFFFFFFFFFF000000LL) + 0x0000000002000000,
              "Downgrades must support one minor version");

// The min invalid protocol version should be the smallest possible protocol version associated with a minor release
// version.
static_assert((minInvalidProtocolVersion.version() & 0xFFFFFFLL) == 0, "Unexpected min invalid protocol version");

struct SWVersion {
	constexpr static FileIdentifier file_identifier = 13943914;

private:
	uint64_t _newestProtocolVersion;
	uint64_t _lastRunProtocolVersion;
	uint64_t _lowestCompatibleProtocolVersion;

public:
	SWVersion() {
		_newestProtocolVersion = 0;
		_lastRunProtocolVersion = 0;
		_lowestCompatibleProtocolVersion = 0;
	}

	SWVersion(ProtocolVersion latestVersion, ProtocolVersion lastVersion, ProtocolVersion minCompatibleVersion)
	  : _newestProtocolVersion(latestVersion.version()), _lastRunProtocolVersion(lastVersion.version()),
	    _lowestCompatibleProtocolVersion(minCompatibleVersion.version()) {}

	bool isValid() const {
		return (_newestProtocolVersion != 0 && _lastRunProtocolVersion != 0 && _lowestCompatibleProtocolVersion != 0);
	}

	uint64_t newestProtocolVersion() const { return _newestProtocolVersion; }
	uint64_t lastRunProtocolVersion() const { return _lastRunProtocolVersion; }
	uint64_t lowestCompatibleProtocolVersion() const { return _lowestCompatibleProtocolVersion; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, _newestProtocolVersion, _lastRunProtocolVersion, _lowestCompatibleProtocolVersion);
	}
};

template <>
struct Traceable<SWVersion> : std::true_type {
	static std::string toString(const SWVersion& swVersion) {
		return format("Newest: 0x%016lX, Last: 0x%016lX, MinCompatible: 0x%016lX",
		              swVersion.newestProtocolVersion(),
		              swVersion.lastRunProtocolVersion(),
		              swVersion.lowestCompatibleProtocolVersion());
	}
};
