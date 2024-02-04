/*
 * VersionVector.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBCLIENT_VERSION_VECTOR_H
#define FDBCLIENT_VERSION_VECTOR_H

#pragma once

#include <boost/container/flat_map.hpp>
#include <set>

#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"

static const int InvalidEncodedSize = 0;

struct VersionVector {
	constexpr static FileIdentifier file_identifier = 5253554;
	friend struct serializable_traits<VersionVector>;
	boost::container::flat_map<Tag, Version> versions; // An ordered map. (Note:
	                                                   // changing this to an unordered
	                                                   // map will break the
	                                                   // serialization code below.)
	Version maxVersion; // Specifies the max version in this version vector. (Note:
	                    // there may or may not be a corresponding entry for this
	                    // version in the "versions" map.)

	VersionVector() : maxVersion(invalidVersion), cachedEncodedSize(InvalidEncodedSize) {}
	VersionVector(Version version) : maxVersion(version), cachedEncodedSize(InvalidEncodedSize) {}

private:
	// Only invoked by getDelta() and applyDelta(), where tag has been validated
	// and version is guaranteed to be larger than the existing value.
	inline void setVersionNoCheck(const Tag& tag, Version version) {
		versions[tag] = version;
		invalidateCachedEncodedSize();
	}

	inline void invalidateCachedEncodedSize() { cachedEncodedSize = InvalidEncodedSize; }

	// Encoded version vector size. Introduced to help speed up serialization.
	// @note This encoded size is not meant to be kept in sync with the updates
	// that happen to the version vector.
	// @note A value of 0 (= InvalidEncodedSize) indicates that the encoded version
	// vector size is not cached.
	size_t cachedEncodedSize;

public:
	Version getMaxVersion() const { return maxVersion; }

	void setMaxVersion(Version version) { maxVersion = version; }

	int size() const { return versions.size(); }

	bool empty() const { return versions.empty(); }

	void setVersion(const Tag& tag, Version version) {
		ASSERT(tag != invalidTag);
		ASSERT(tag.locality > tagLocalityInvalid);
		ASSERT(version > maxVersion);
		versions[tag] = version;
		maxVersion = version;
		invalidateCachedEncodedSize();
	}

	void setVersion(const std::set<Tag>& tags, Version version, int8_t localityFilter = tagLocalityInvalid) {
		ASSERT(version > maxVersion);
		for (auto& tag : tags) {
			ASSERT(tag != invalidTag);
			ASSERT(tag.locality > tagLocalityInvalid);
			if (localityFilter == tagLocalityInvalid || tag.locality == localityFilter) {
				versions[tag] = version;
			}
		}
		maxVersion = version;
		invalidateCachedEncodedSize();
	}

	bool hasVersion(const Tag& tag) const {
		ASSERT(tag != invalidTag);
		return versions.find(tag) != versions.end();
	}

	// @pre assumes that the given tag has an entry in the version vector.
	Version getVersion(const Tag& tag) const {
		ASSERT(tag != invalidTag);
		auto iter = versions.find(tag);
		ASSERT(iter != versions.end());
		return iter->second;
	}

	void clear() {
		versions.clear();
		maxVersion = invalidVersion;
		invalidateCachedEncodedSize();
	}

	// @note this method, together with method applyDelta(), helps minimize
	// the number of version vector entries that get sent from sequencer to
	// grv proxy (and from grv proxy to client) on the read path.
	void getDelta(Version refVersion, VersionVector& delta) const {
		ASSERT(refVersion <= maxVersion);

		delta.clear();

		if (refVersion == maxVersion) {
			return; // return an invalid version vector
		}

		if (CLIENT_KNOBS->SEND_ENTIRE_VERSION_VECTOR) {
			delta = *this;
		} else {
			for (const auto& [tag, version] : versions) {
				if (version > refVersion) {
					delta.setVersionNoCheck(tag, version);
				}
			}
			delta.maxVersion = maxVersion;
		}
	}

	// @note this method, together with method getDelta(), helps minimize
	// the number of version vector entries that get sent from sequencer to
	// grv proxy (and from grv proxy to client) on the read path.
	void applyDelta(const VersionVector& delta) {
		if (delta.maxVersion == invalidVersion) {
			return;
		}

		if (maxVersion >= delta.maxVersion) {
			return;
		}

		if (CLIENT_KNOBS->SEND_ENTIRE_VERSION_VECTOR) {
			*this = delta;
		} else {
			for (const auto& [tag, version] : delta.versions) {
				if (version > maxVersion) {
					setVersionNoCheck(tag, version);
				}
			}
			maxVersion = delta.maxVersion;
		}
	}

	std::string toString() const {
		std::stringstream vector;
		vector << "[";
		for (const auto& [tag, version] : versions) {
			vector << '{' << tag.toString() << "," << version << '}';
		}
		vector << " maxversion: " << maxVersion << "]";
		return vector.str();
	}

	bool operator==(const VersionVector& vv) const { return maxVersion == vv.maxVersion; }
	bool operator!=(const VersionVector& vv) const { return maxVersion != vv.maxVersion; }
	bool operator<(const VersionVector& vv) const { return maxVersion < vv.maxVersion; }

	bool compare(const VersionVector& vv) {
		if (maxVersion != vv.maxVersion) {
			return false;
		}

		if (versions.size() != vv.versions.size()) {
			return false;
		}

		auto iterA = versions.begin();
		auto iterB = vv.versions.begin();
		for (; iterA != versions.end(); iterA++, iterB++) {
			if (iterA->first != iterB->first || iterA->second != iterB->second) {
				return false;
			}
		}
		ASSERT(iterB == vv.versions.end());

		return true;
	}

	//
	// Methods to set/get/check cached encoded version vector size.
	//
	void setCachedEncodedSize(size_t size) { cachedEncodedSize = size; }

	bool isEncodedSizeCached() const { return cachedEncodedSize != InvalidEncodedSize; }

	size_t getCachedEncodedSize() const {
		ASSERT(isEncodedSizeCached());
		return cachedEncodedSize;
	}

	//
	// Methods to copy an encoded version vector into the serialization buffer.
	//
	// Encoding methods used:
	//
	// - Tag localities: Run-length encoding
	// - Tag ids: Compact representation (depending on the max tag id value)
	// - Commit versions: Delta encoding
	//

	// Extracts information about tag ids, tag localities, and commit versions that are
	// captured in the version vector. This will avoid the need to make multiple iterations
	// over the contents of the version vector while (encoding and) serializing it.
	void getTagAndCommitVersionInfo(size_t& utlCount,
	                                uint16_t& maxTagId,
	                                Version& minCommitVersion,
	                                Version& maxCommitVersion) const {
		// Initialization
		utlCount = 0; // unique tag locality count
		maxTagId = 0; // the highest tag id in the version vector
		minCommitVersion = MAX_VERSION; // the lowest commit version in "VersionVector::versions"
		maxCommitVersion = invalidVersion; // the highest commit version in "VersionVector::versions"

		// Population
		int8_t locality = tagLocalityInvalid;
		for (const auto& [tag, version] : versions) {
			if (locality != tag.locality) {
				locality = tag.locality;
				utlCount++;
			}

			maxTagId = std::max(maxTagId, tag.id);
			minCommitVersion = std::min(minCommitVersion, version);
			maxCommitVersion = std::max(maxCommitVersion, version);
		}
	}

	// Calculate size of the encoded version vector.
	size_t getEncodedSize() const {
		size_t utlCount; // unique tag locality count
		uint16_t maxTagId; // the highest tag id in the version vector
		Version minVersion; // the lowest commit version in the version vector
		Version maxVersion; // the highest commit version in the version vector
		getTagAndCommitVersionInfo(utlCount, maxTagId, minVersion, maxVersion);

		// Is the version vector empty?
		if (utlCount == 0) {
			return sizeof(size_t) + /* captures unique tag locality count (= 0, in this case) */
			       sizeof(Version); /* captures VersionVector::maxVersion */
		}

		size_t tagIdSize = 0; // number of bytes needed to serialize an individual (potentially compacted) tag id
		tagIdSize = (maxTagId <= UINT8_MAX) ? sizeof(uint8_t) : sizeof(uint16_t);

		size_t commitVersionSize = 0; // number of bytes needed to serialize an individual commit version
		if ((maxVersion - minVersion) <= UINT8_MAX) {
			commitVersionSize = sizeof(uint8_t);
		} else if ((maxVersion - minVersion) <= UINT16_MAX) {
			commitVersionSize = sizeof(uint16_t);
		} else if ((maxVersion - minVersion) <= UINT32_MAX) {
			commitVersionSize = sizeof(uint32_t);
		} else {
			commitVersionSize = sizeof(uint64_t);
		}

		return sizeof(size_t) + /* unique tag locality count */
		       utlCount * (sizeof(int8_t) + sizeof(uint16_t)) + // unique tag locality values and their run lengths
		       sizeof(uint8_t) + /* number of bytes needed to serialize an individual (potentially compacted) tag id */
		       sizeof(uint8_t) + /* number of bytes needed to serialize an individual commit version */
		       sizeof(Version) + /* the lowest commit version in the version vector */
		       sizeof(size_t) + /* number of <tagid, version> pairs */
		       this->size() * (tagIdSize + commitVersionSize) + /* encoded <tagid, version> pairs */
		       sizeof(Version); /* VersionVector::maxVersion */
	}

	// Copy "value" into the serialization buffer.
	template <typename T>
	void serialize(uint8_t*& out, T value) const {
		memcpy(out, &value, sizeof(T));
		out += sizeof(T);
	}

	// Copy RLE encoded tag locality values into the serialization buffer.
	void serializeTagLocalities(size_t utlCount, uint8_t*& out) const {
		serialize<size_t>(out, utlCount); // unique tag locality count

		// Is the version vector empty?
		if (utlCount == 0) {
			return;
		}

		int8_t locality = tagLocalityInvalid;
		uint16_t localityCount = 0;
		for (const auto& [tag, version] : versions) {
			if (locality != tag.locality) {
				if (locality != tagLocalityInvalid) {
					serialize<int8_t>(out, locality); // tag locality value
					serialize<uint16_t>(out, localityCount); // run length of the locality value
				}
				locality = tag.locality;
				localityCount = 1;
			} else {
				localityCount++;
			}
		}

		if (locality != tagLocalityInvalid) {
			serialize<int8_t>(out, locality); // tag locality value
			serialize<uint16_t>(out, localityCount); // run length of the locality value
		}
	}

	// Copy encoded tag id and commit version values into the serialization buffer.
	// T: Type to be used to serialize tag ids (uint8_t/uint16_t)
	// V: Type to be used to serialize commit version deltas (uint8_t/uint16_t/uint32_t/uint64_t)
	template <typename T, typename V>
	void serializeSizedTagIdsAndSizedCommitVersions(Version minCommitVersion, uint8_t*& out) const {
		// Number of bytes that will be used to serialize an individual tag id.
		serialize<uint8_t>(out, (uint8_t)sizeof(T));
		// Number of bytes that will be used to serialize an individual commit version delta value.
		serialize<uint8_t>(out, (uint8_t)sizeof(V));
		// The lowest commit version in the version vector.
		serialize<Version>(out, minCommitVersion);
		// The number of <tagId, commitVersion> pairs.
		serialize<size_t>(out, (this->size()));

		for (const auto& [tag, version] : versions) {
			// Serialize tag id.
			serialize<T>(out, (T)tag.id);

			// Serialize commit version delta.
			serialize<V>(out, (V)(version - minCommitVersion));
		}
	}

	// Figure out the type to be used to serialize delta encoded commit version values,
	// and call the above method to do the serialization.
	// T: Type to be used to serialize tag ids (uint8_t/uint16_t)
	template <typename T>
	void serializeSizedTagIdsAndCommitVersions(Version minVersion, Version maxVersion, uint8_t*& out) const {
		if ((maxVersion - minVersion) <= UINT8_MAX) {
			serializeSizedTagIdsAndSizedCommitVersions<T, uint8_t>(minVersion, out);
		} else if ((maxVersion - minVersion) <= UINT16_MAX) {
			serializeSizedTagIdsAndSizedCommitVersions<T, uint16_t>(minVersion, out);
		} else if ((maxVersion - minVersion) <= UINT32_MAX) {
			serializeSizedTagIdsAndSizedCommitVersions<T, uint32_t>(minVersion, out);
		} else {
			serializeSizedTagIdsAndSizedCommitVersions<T, uint64_t>(minVersion, out);
		}
	}

	// Figure out the types to be used to serialize (potentially compacted) tag ids and delta
	// encoded commit version values, and call the above methods to do the serialization.
	void serializeTagIdsAndCommitVersions(uint16_t maxTagId,
	                                      Version minVersion,
	                                      Version maxVersion,
	                                      uint8_t*& out) const {
		ASSERT(!this->empty());
		if (maxTagId <= UINT8_MAX) {
			serializeSizedTagIdsAndCommitVersions<uint8_t>(minVersion, maxVersion, out);
		} else {
			serializeSizedTagIdsAndCommitVersions<uint16_t>(minVersion, maxVersion, out);
		}
	}

	//
	// Methods to load (an encoded) version vector from the serialization buffer.
	//

	// Extract "value" from the serialization buffer.
	template <typename T>
	void deserialize(const uint8_t*& data, T& value) const {
		memcpy(&value, data, sizeof(T));
		data += sizeof(T);
	}

	// Deserialize RLE encoded tag locality values.
	void deserializeLocalities(const uint8_t*& data,
	                           size_t& utlCount,
	                           std::vector<int8_t>& localities,
	                           std::vector<uint16_t>& localityCounts) {
		// Initialization
		localities.clear();
		localityCounts.clear();

		// Extract unique tag locality count from the buffer.
		deserialize<size_t>(data, utlCount);

		if (utlCount == 0) {
			return;
		}

		int8_t locality;
		uint16_t localityCount;
		localities.reserve(utlCount);
		localityCounts.reserve(utlCount);
		for (size_t i = 0; i < utlCount; i++) {
			deserialize<int8_t>(data, locality);
			localities.push_back(locality);

			deserialize<uint16_t>(data, localityCount);
			localityCounts.push_back(localityCount);
		}
	}

	// Deserialize tag ids and commit version values.
	// T: Type that was used to serialize tag ids (uint8_t/uint16_t)
	// V: Type that was used to serialize commit version deltas (uint8_t/uint16_t/uint32_t/uint64_t)
	template <typename T, typename V>
	void deserializeSizedTagIdsAndSizedCommitVersions(const uint8_t*& data,
	                                                  std::vector<int8_t>& localities,
	                                                  std::vector<uint16_t>& localityCounts) {
		Version minCommitVersion;
		deserialize<Version>(data, minCommitVersion);

		size_t pairCount; // number of serialized <tag id, commit version> pairs
		deserialize<size_t>(data, pairCount);

		T tagId;
		V versionDelta;
		for (size_t i = 0; i < localities.size(); i++) {
			for (size_t j = 0; j < localityCounts[i]; j++) {
				// Deserialize tag id.
				deserialize<T>(data, tagId);

				// Deserialize commit version delta.
				deserialize<V>(data, versionDelta);

				Tag tag(localities[i], tagId);
				setVersionNoCheck(tag, minCommitVersion + versionDelta);
			}
		}
	}

	// Figrue out the type that was used to serialize commit version deltas and call the above
	// method to do the deserialization.
	// T: Type that was used to serialize tag ids (uint8_t/uint16_t)
	template <typename T>
	void deserializeSizedTagIdsAndCommitVersions(const uint8_t*& data,
	                                             std::vector<int8_t>& localities,
	                                             std::vector<uint16_t>& localityCounts) {
		uint8_t commitVersionDeltaSize; // number of bytes that were used to serialize an individual commit version
		                                // delta value
		deserialize<uint8_t>(data, commitVersionDeltaSize);

		if (commitVersionDeltaSize == sizeof(uint8_t)) {
			deserializeSizedTagIdsAndSizedCommitVersions<T, uint8_t>(data, localities, localityCounts);
		} else if (commitVersionDeltaSize == sizeof(uint16_t)) {
			deserializeSizedTagIdsAndSizedCommitVersions<T, uint16_t>(data, localities, localityCounts);
		} else if (commitVersionDeltaSize == sizeof(uint32_t)) {
			deserializeSizedTagIdsAndSizedCommitVersions<T, uint32_t>(data, localities, localityCounts);
		} else {
			ASSERT(commitVersionDeltaSize == sizeof(uint64_t));
			deserializeSizedTagIdsAndSizedCommitVersions<T, uint64_t>(data, localities, localityCounts);
		}
	}

	// Figure out the types that were used to serialize tag ids and commit version deltas, and
	// call the above methods to do the deserialization.
	void deserializeTagIdsAndCommitVersions(const uint8_t*& data,
	                                        std::vector<int8_t>& localities,
	                                        std::vector<uint16_t>& localityCounts) {
		uint8_t tagIdSize; // number of bytes that were used to serialize an individual tag id
		deserialize<uint8_t>(data, tagIdSize);

		if (tagIdSize == sizeof(uint8_t)) {
			deserializeSizedTagIdsAndCommitVersions<uint8_t>(data, localities, localityCounts);
		} else {
			ASSERT(tagIdSize == sizeof(uint16_t));
			deserializeSizedTagIdsAndCommitVersions<uint16_t>(data, localities, localityCounts);
		}
	}
};

// @note Enabling/Disabling version vector encoding during serialization (and
// de-serialization):
// - To enable version vector encoding during serialization/de-serialization:
// derive "struct dynamic_size_traits<VersionVector>" from "std::true_type" and
// derive "struct serializable_traits<VersionVector>" from "std::false_type".
//
// - To disable version vector encoding during serialization/de-serialization::
// derive "struct dynamic_size_traits<VersionVector>" from "std::false_type" and
// derive "struct serializable_traits<VersionVector>" from "std::true_type".
template <>
struct serializable_traits<VersionVector> : std::false_type {
	template <class Archiver>
	static void serialize(Archiver& ar, VersionVector& vv) {
		serializer(ar, vv.versions, vv.maxVersion);
	}
};

template <>
struct dynamic_size_traits<VersionVector> : std::true_type {
	template <class Context>
	static size_t size(const VersionVector& vv, Context&) {
		size_t encodedSize;
		if (vv.isEncodedSizeCached()) {
			encodedSize = vv.getCachedEncodedSize();
			// @todo remove this assert before doing performance tests
			ASSERT(encodedSize == vv.getEncodedSize());
		} else {
			encodedSize = vv.getEncodedSize();
			const_cast<VersionVector&>(vv).setCachedEncodedSize(encodedSize);
		}
		return encodedSize;
	}

	template <class Context>
	static void save(uint8_t* out, const VersionVector& vv, Context&) {
		auto* begin = out;

		size_t utlCount; // unique tag locality count
		uint16_t maxTagId; // the highest tag id in the version vector
		Version minCommitVersion; // the lowest commit version in the version vector (in "VersionVector::versions")
		Version maxCommitVersion; // the highest commit version in the version vector (in "VersionVector::versions")
		vv.getTagAndCommitVersionInfo(utlCount, maxTagId, minCommitVersion, maxCommitVersion);

		vv.serializeTagLocalities(utlCount, out);
		if (!vv.empty()) {
			vv.serializeTagIdsAndCommitVersions(maxTagId, minCommitVersion, maxCommitVersion, out);
		}

		// Serialize vv::maxVersion.
		vv.serialize<Version>(out, (vv.getMaxVersion()));

		// @todo remove this assert before doing performance tests
		ASSERT(out - begin == vv.getEncodedSize());
	}

	template <class Context>
	static void load(const uint8_t* data, size_t size, VersionVector& vv, Context& context) {
		auto* p = data;

		size_t utlCount;
		std::vector<int8_t> localities;
		std::vector<uint16_t> localityCounts;
		vv.deserializeLocalities(data, utlCount, localities, localityCounts);
		if (utlCount > 0) {
			vv.deserializeTagIdsAndCommitVersions(data, localities, localityCounts);
		}

		// Deserialize VersionVector::maxVersion.
		Version maxVersion;
		vv.deserialize<Version>(data, maxVersion);
		vv.setMaxVersion(maxVersion);

		ASSERT(data - p == size);
	}
};

static const VersionVector minVersionVector{ 0 };
static const VersionVector maxVersionVector{ MAX_VERSION };
static const VersionVector invalidVersionVector{ invalidVersion };

#endif /* FDBCLIENT_VERSION_VECTOR_H */
