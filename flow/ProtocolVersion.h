/*
 * ProtocolVersion.h
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

#pragma once
#include <cstdint>

namespace fdb_versions {

// Whenever a new breaking feature is introduced in FDB for which we need code
// that tests whether the serialized version of the code was serialized with
// that feature, the author should add this macro with the name of the feature
// to the corresponding version. Users can than later call `version->hasFeature()`
// on a deserialized `ProtocolVersion` to test for the feature.
#define VERSION_FEATURE(x)                                                                                             \
	constexpr bool has##x() const { return static_cast<const P*>(this)->version() >= protocolVersion; }

// A version-class. These need to be ordered by version and the ordering
// happens through inheritance. Each of these structs need to know about
// the `ProtocolVersion`-class (through the template parameter) and it has
// to be named `P` (otherwise the VERSION_FEATURE macro won't work anymore).
// These classes are used to determine if a feature was available at a certain
// protocol version. It does determine that through static polymorphism.
template<class P>
struct v5_5 {
	static constexpr uint64_t protocolVersion = 0x0FDB00A551000000LL;

	VERSION_FEATURE(MultiVersionClient)
};

template<class P>
struct v5_6 : v5_5<P> {
	static constexpr uint64_t protocolVersion = 0x0FDB00A560010001LL;

	VERSION_FEATURE(TagLocality)
};

template <class P>
struct v6_0 : v5_6<P> {
	static constexpr uint64_t protocolVersion = 0x0FDB00B060000001LL;

	VERSION_FEATURE(Fearless)
};

template <class P>
struct v6_1 : v6_0<P> {};

// This typedef needs to be updated whenever a new version is added
template <class P>
using latest_version = v6_1<P>;

} // namespace fdb_versions

// ProtocolVersion wraps a uint64_t to make it type safe. It has to inherit
// from the newest version object and it will know about the current version.
// The default constuctor will initialize the version to 0 (which is an invalid
// version). ProtocolVersion objects should never be compared to version numbers
// directly. Instead one should always use the type-safe version types from which
// this class inherits all.
class ProtocolVersion : public fdb_versions::latest_version<ProtocolVersion> {
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

	constexpr operator bool() const { return _version != 0; }
	// comparison operators
	constexpr bool operator==(const ProtocolVersion other) const { return version() == other.version(); }
	constexpr bool operator!=(const ProtocolVersion other) const { return version() != other.version(); }
	constexpr bool operator<=(const ProtocolVersion other) const { return version() <= other.version(); }
	constexpr bool operator>=(const ProtocolVersion other) const { return version() >= other.version(); }
	constexpr bool operator<(const ProtocolVersion other) const { return version() < other.version(); }
	constexpr bool operator>(const ProtocolVersion other) const { return version() > other.version(); }
};

// These impact both communications and the deserialization of certain database and IKeyValueStore keys.
//
// The convention is that 'x' and 'y' should match the major and minor version of the software, and 'z' should be 0.
// To make a change without a corresponding increase to the x.y version, increment the 'dev' digit.
//
//                                                        xyzdev
//                                                        vvvv
constexpr ProtocolVersion currentProtocolVersion(0x0FDB00B061070001LL);
// This assert is intended to help prevent incrementing the leftmost digits accidentally. It will probably need to
// change when we reach version 10.
static_assert(currentProtocolVersion.version() < 0x0FDB00B100000000LL, "Unexpected protocol version");
