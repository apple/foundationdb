/*
 * ProtocolVersion.h
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

#pragma once
#include <cstdint>

#define PROTOCOL_VERSION_FEATURE(v, x)                                                                                 \
	struct x {                                                                                                         \
		static constexpr uint64_t protocolVersion = v;                                                                 \
	};                                                                                                                 \
	constexpr bool has##x() const { return this->version() > x ::protocolVersion; }                                   \
	static constexpr ProtocolVersion with##x() { return ProtocolVersion(x ::protocolVersion); }

// ProtocolVersion wraps a uint64_t to make it type safe. It will know about the current versions.
// The default constuctor will initialize the version to 0 (which is an invalid
// version). ProtocolVersion objects should never be compared to version numbers
// directly. Instead one should always use the type-safe version types from which
// this class inherits all.
class ProtocolVersion {
	uint64_t _version;

public: // constants
	static constexpr uint64_t versionFlagMask = 0x0FFFFFFFFFFFFFFFLL;
	static constexpr uint64_t objectSerializerFlag = 0x1000000000000000LL;
	static constexpr uint64_t compatibleProtocolVersionMask = 0xffffffffffff0000LL;
	static constexpr uint64_t minValidProtocolVersion = 0x0FDB00A200060001LL;

public:
	constexpr explicit ProtocolVersion(uint64_t version) : _version(version) {}
	constexpr ProtocolVersion() : _version(0) {}

	constexpr bool isCompatible(ProtocolVersion other) const {
		return (other.version() & compatibleProtocolVersionMask) == (version() & compatibleProtocolVersionMask);
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
	PROTOCOL_VERSION_FEATURE(0x0FDB00A200090000LL, Watches);
	PROTOCOL_VERSION_FEATURE(0x0FDB00A2000D0000LL, MovableCoordinatedState);
	PROTOCOL_VERSION_FEATURE(0x0FDB00A340000000LL, ProcessID);
	PROTOCOL_VERSION_FEATURE(0x0FDB00A400040000LL, OpenDatabase);
	PROTOCOL_VERSION_FEATURE(0x0FDB00A446020000LL, Locality);
	PROTOCOL_VERSION_FEATURE(0x0FDB00A460010000LL, MultiGenerationTLog);
	PROTOCOL_VERSION_FEATURE(0x0FDB00A460010000LL, SharedMutations);
	PROTOCOL_VERSION_FEATURE(0x0FDB00A551000000LL, MultiVersionClient);
	PROTOCOL_VERSION_FEATURE(0x0FDB00A560010000LL, TagLocality);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B060000000LL, Fearless);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B061020000LL, EndpointAddrList);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B061030000LL, IPv6);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B061030000LL, TLogVersion);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B061070000LL, PseudoLocalities);
	PROTOCOL_VERSION_FEATURE(0x0FDB00B061070000LL, ShardedTxsTags);
};

// These impact both communications and the deserialization of certain database and IKeyValueStore keys.
//
// The convention is that 'x' and 'y' should match the major and minor version of the software, and 'z' should be 0.
// To make a change without a corresponding increase to the x.y version, increment the 'dev' digit.
//
//                                                         xyzdev
//                                                         vvvv
constexpr ProtocolVersion currentProtocolVersion(0x0FDB00B062010001LL);
// This assert is intended to help prevent incrementing the leftmost digits accidentally. It will probably need to
// change when we reach version 10.
static_assert(currentProtocolVersion.version() < 0x0FDB00B100000000LL, "Unexpected protocol version");
