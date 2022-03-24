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
#include <cstdint>
#include "flow/Trace.h"

// This version impacts both communications and the deserialization of certain database and IKeyValueStore keys.
//
// The convention is that 'x' and 'y' should match the major and minor version of the software, and 'z' should be 0.
// To make a change without a corresponding increase to the x.y version, increment the 'dev' digit.
//
// The last 2 bytes (4 digits) of the protocol version do not affect compatibility. These two bytes are not currently
// used and should not be changed from 0.
//                                                         xyzdev
//                                                         vvvv
constexpr uint64_t currentProtocolVersionValue = 0x0FDB00B071010000LL;

// The first protocol version that cannot be downgraded from. Ordinarily, this will be two release versions larger
// than the current version, meaning that we only support downgrades between consecutive release versions.
constexpr uint64_t minInvalidProtocolVersionValue = 0x0FDB00B073000000LL;

#define PROTOCOL_VERSION_FEATURE(v, x)                                                                                 \
	static_assert((v & 0xF0FFFFLL) == 0 || v < 0x0FDB00B071000000LL, "Unexpected feature protocol version");           \
	static_assert(v <= currentProtocolVersionValue, "Feature protocol version too large");                             \
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
	constexpr bool isValid() const { return version() >= minValidProtocolVersion; }

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
	PROTOCOL_VERSION_FEATURE(0x0FDB00A200090000LL, Watches);
	PROTOCOL_VERSION_FEATURE(0x0FDB00A2000D0000LL, MovableCoordinatedState);
	PROTOCOL_VERSION_FEATURE(0x0FDB00A340000000LL, ProcessID);
	PROTOCOL_VERSION_FEATURE(0x0FDB00A400040000LL, OpenDatabase);
	PROTOCOL_VERSION_FEATURE(0x0FDB00A446020000LL, Locality);
	PROTOCOL_VERSION_FEATURE(0x0FDB00A460010000LL, MultiGenerationTLog);
	PROTOCOL_VERSION_FEATURE(0x0FDB00A460010000LL, SharedMutations);
	PROTOCOL_VERSION_FEATURE(0x0FDB00A551000000LL, InexpensiveMultiVersionClient);
	PROTOCOL_VERSION_FEATURE(0x0FDB00A560010000LL, TagLocality);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B060000000LL, Fearless);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B061020000LL, EndpointAddrList);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B061030000LL, IPv6);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B061030000LL, TLogVersion);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B061070000LL, PseudoLocalities);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B061070000LL, ShardedTxsTags);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, TLogQueueEntryRef);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, GenerationRegVal);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, MovableCoordinatedStateV2);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, KeyServerValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, LogsValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, ServerTagValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, TagLocalityListValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, DatacenterReplicasValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, ProcessClassValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, WorkerListValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, BackupStartValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, LogRangeEncodeValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, HealthyZoneValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, DRBackupRanges);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, RegionConfiguration);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, ReplicationPolicy);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, BackupMutations);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, ClusterControllerPriorityInfo);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, ProcessIDFile);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B062010001LL, CloseUnusedConnection);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B063010000LL, DBCoreState);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B063010000LL, TagThrottleValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B063010000LL, StorageCacheValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B063010000LL, RestoreStatusValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B063010000LL, RestoreRequestValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B063010000LL, RestoreRequestDoneVersionValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B063010000LL, RestoreRequestTriggerValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B063010000LL, RestoreWorkerInterfaceValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B063010000LL, BackupProgressValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B063010000LL, KeyServerValueV2);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B063000000LL, UnifiedTLogSpilling);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B063010000LL, BackupWorker);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B063010000LL, ReportConflictingKeys);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B063010000LL, SmallEndpoints);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B063010000LL, CacheRole);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B070010000LL, StableInterfaces);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B070010001LL, ServerListValue);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B070010001LL, TagThrottleValueReason);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B070010001LL, SpanContext);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B070010001LL, TSS);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B071010000LL, ChangeFeed);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B071010000LL, BlobGranule);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B071010000LL, NetworkAddressHostnameFlag);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B071010000LL, StorageMetadata);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B071010000LL, PerpetualWiggleMetadata);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B071010000LL, Tenants);
};

template <>
struct Traceable<ProtocolVersion> : std::true_type {
	static std::string toString(const ProtocolVersion& protocolVersion) {
		return format("0x%016lX", protocolVersion.version());
	}
};

constexpr ProtocolVersion currentProtocolVersion(currentProtocolVersionValue);
constexpr ProtocolVersion minInvalidProtocolVersion(minInvalidProtocolVersionValue);

// This assert is intended to help prevent incrementing the leftmost digits accidentally. It will probably need to
// change when we reach version 10.
static_assert(currentProtocolVersion.version() < 0x0FDB00B100000000LL, "Unexpected protocol version");

// The last two bytes of the protocol version are currently masked out in compatibility checks. We do not use them,
// so prevent them from being inadvertently changed.
//
// We also do not modify the protocol version for patch releases, so prevent modifying the patch version digit.
static_assert((currentProtocolVersion.version() & 0xF0FFFFLL) == 0, "Unexpected protocol version");

// Downgrades must support at least one minor version.
static_assert(minInvalidProtocolVersion.version() >=
                  (currentProtocolVersion.version() & 0xFFFFFFFFFF000000LL) + 0x0000000002000000,
              "Downgrades must support one minor version");

// The min invalid protocol version should be the smallest possible protocol version associated with a minor release
// version.
static_assert((minInvalidProtocolVersion.version() & 0xFFFFFFLL) == 0, "Unexpected min invalid protocol version");