/*
 * VersionVector.cpp
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

#include "flow/Arena.h"
#include "flow/UnitTest.h"
#include "fdbclient/VersionVector.h"

namespace unit_tests {

struct TestContextArena {
	Arena& _arena;
	Arena& arena() { return _arena; }
	ProtocolVersion protocolVersion() const { return g_network->protocolVersion(); }
	uint8_t* allocate(size_t size) { return new (_arena) uint8_t[size]; }
};

TEST_CASE("/fdbclient/VersionVector/emptyVV") {
	Arena arena;
	TestContextArena context{ arena };

	{
		VersionVector serializedVV; // an empty version vector
		size_t size = dynamic_size_traits<VersionVector>::size(serializedVV, context);

		uint8_t* buf = context.allocate(size);
		dynamic_size_traits<VersionVector>::save(buf, serializedVV, context);

		VersionVector deserializedVV;
		dynamic_size_traits<VersionVector>::load(buf, size, deserializedVV, context);

		ASSERT(serializedVV.compare(deserializedVV));
	}

	{
		VersionVector serializedVV(133200164); // "VersionVector::maxVersion" is set, empty otherwise
		size_t size = dynamic_size_traits<VersionVector>::size(serializedVV, context);

		uint8_t* buf = context.allocate(size);
		dynamic_size_traits<VersionVector>::save(buf, serializedVV, context);

		VersionVector deserializedVV;
		dynamic_size_traits<VersionVector>::load(buf, size, deserializedVV, context);

		ASSERT(serializedVV.compare(deserializedVV));
	}

	return Void();
}

TEST_CASE("/fdbclient/VersionVector/simpleVV") {
	Arena arena;
	TestContextArena context{ arena };

	VersionVector serializedVV;
	serializedVV.setVersion(Tag(-1, 2), 3619339);
	serializedVV.setVersion(Tag(0, 13), 13292611);

	std::set<Tag> tags;
	tags.emplace(0, 2);
	tags.emplace(0, 1);
	tags.emplace(0, 0);
	serializedVV.setVersion(tags, 13391141);

	size_t size = dynamic_size_traits<VersionVector>::size(serializedVV, context);

	uint8_t* buf = context.allocate(size);
	dynamic_size_traits<VersionVector>::save(buf, serializedVV, context);

	VersionVector deserializedVV;
	dynamic_size_traits<VersionVector>::load(buf, size, deserializedVV, context);

	ASSERT(serializedVV.compare(deserializedVV));

	return Void();
}

// Populates version vector (with randomly generated tag localities, ids, and commit versions)
// based on the given specifications.
// @param vv Version vector
// @param tagCount total number of storage servers in the cluster
// @param localityCount total number of localities/regions in the cluster
// @param maxTagId maximum value of any tag id in the cluster
// @param maxCommitVersionDelta maximum difference between commit versions in the version vector
// @note assumes each locality contains the same number of tags
// @note picks locality values randomly from range [tagLocalityInvalid+1, INT8_MAX)
void populateVersionVector(VersionVector& vv,
                           int tagCount,
                           int localityCount,
                           int maxTagId,
                           const uint64_t maxCommitVersionDelta) {
	std::vector<uint16_t> ids;
	std::vector<int8_t> localities;
	Version minVersion;
	std::vector<Version> versions;
	int tagsPerLocality = tagCount / localityCount;

	// Populate localities.
	while (localities.size() < (size_t)localityCount) {
		int8_t locality = deterministicRandom()->randomInt(tagLocalityInvalid + 1, INT8_MAX);
		if (std::find(localities.begin(), localities.end(), locality) == localities.end()) {
			localities.push_back(locality);
		}
	}

	// Populate ids.
	for (int i = 0; i < tagCount; i++) {
		// Some of the ids could be duplicates, that's fine.
		ids.push_back(deterministicRandom()->randomInt(0, maxTagId));
	}

	// Choose a value for minVersion. (Choose a value in such a way that
	// "minVersion + maxCommitVersionDelta" does not exceed INT64_MAX.)
	if (maxCommitVersionDelta <= UINT16_MAX) {
		minVersion = deterministicRandom()->randomUInt32();
	} else if (maxCommitVersionDelta <= UINT32_MAX) {
		minVersion = deterministicRandom()->randomInt(0, UINT16_MAX);
	} else {
		minVersion = 0;
	}

	// Populate versions.
	Version versionDelta;
	for (int i = 0; i < tagCount; i++) {
		if (maxCommitVersionDelta <= UINT8_MAX) {
			versionDelta = deterministicRandom()->randomInt(0, UINT8_MAX);
		} else if (maxCommitVersionDelta <= UINT16_MAX) {
			versionDelta = deterministicRandom()->randomInt(0, UINT16_MAX);
		} else if (maxCommitVersionDelta <= UINT32_MAX) {
			versionDelta = deterministicRandom()->randomUInt32();
		} else {
			versionDelta = deterministicRandom()->randomInt64(0, INT64_MAX);
		}
		// Some of the versions could be duplicates, that's fine.
		versions.push_back(minVersion + versionDelta);
	}

	// Sort versions.
	std::sort(versions.begin(), versions.end());

	// Populate the version vector.
	std::set<Tag> tags;
	int tagIndex = 0;
	for (int i = 0; i < localities.size() && tagIndex < tagCount; i++) {
		for (int j = 0; j < tagsPerLocality && tagIndex < tagCount; j++, tagIndex++) {
			if (Tag(localities[i], ids[tagIndex]) == invalidTag) {
				continue; // skip this tag (this version also gets skipped, that's fine)
			}
			if (versions[tagIndex] == vv.getMaxVersion()) {
				tags.emplace(localities[i], ids[tagIndex]);
				continue; // skip this version; this tag will get the next higher version
			}
			if (tags.empty()) {
				vv.setVersion(Tag(localities[i], ids[tagIndex]), versions[tagIndex]);
			} else {
				vv.setVersion(tags, versions[tagIndex]);
				tags.clear();
			}
		}
	}
	ASSERT(tagIndex == tagCount);
}

TEST_CASE("/fdbclient/VersionVector/testA") {
	Arena arena;
	TestContextArena context{ arena };

	VersionVector serializedVV;
	// 80 storage servers spread over 2 regions, maxTagId < INT8_MAX, and
	// maxCommitVersionDelta < UINT8_MAX.
	populateVersionVector(serializedVV, 80, 2, INT8_MAX, UINT8_MAX);

	size_t size = dynamic_size_traits<VersionVector>::size(serializedVV, context);

	uint8_t* buf = context.allocate(size);
	dynamic_size_traits<VersionVector>::save(buf, serializedVV, context);

	VersionVector deserializedVV;
	dynamic_size_traits<VersionVector>::load(buf, size, deserializedVV, context);

	ASSERT(serializedVV.compare(deserializedVV));

	return Void();
}

TEST_CASE("/fdbclient/VersionVector/testB") {
	Arena arena;
	TestContextArena context{ arena };

	VersionVector serializedVV;
	// 800 storage servers spread over 2 regions, maxTagId < INT16_MAX, and
	// maxCommitVersionDelta < UINT8_MAX.
	populateVersionVector(serializedVV, 800, 2, INT16_MAX, UINT8_MAX);

	size_t size = dynamic_size_traits<VersionVector>::size(serializedVV, context);

	uint8_t* buf = context.allocate(size);
	dynamic_size_traits<VersionVector>::save(buf, serializedVV, context);

	VersionVector deserializedVV;
	dynamic_size_traits<VersionVector>::load(buf, size, deserializedVV, context);

	ASSERT(serializedVV.compare(deserializedVV));

	return Void();
}

TEST_CASE("/fdbclient/VersionVector/testC") {
	Arena arena;
	TestContextArena context{ arena };

	VersionVector serializedVV;
	// 800 storage servers spread over 2 regions, maxTagId < INT16_MAX, and
	// maxCommitVersionDelta < UINT16_MAX.
	populateVersionVector(serializedVV, 800, 2, INT16_MAX, UINT16_MAX);

	size_t size = dynamic_size_traits<VersionVector>::size(serializedVV, context);

	uint8_t* buf = context.allocate(size);
	dynamic_size_traits<VersionVector>::save(buf, serializedVV, context);

	VersionVector deserializedVV;
	dynamic_size_traits<VersionVector>::load(buf, size, deserializedVV, context);

	ASSERT(serializedVV.compare(deserializedVV));

	return Void();
}

TEST_CASE("/fdbclient/VersionVector/testD") {
	Arena arena;
	TestContextArena context{ arena };

	VersionVector serializedVV;
	// 800 storage servers spread over 2 regions, maxTagId < INT16_MAX, and
	// maxCommitVersionDelta < UINT32_MAX.
	populateVersionVector(serializedVV, 800, 2, INT16_MAX, UINT32_MAX);

	size_t size = dynamic_size_traits<VersionVector>::size(serializedVV, context);

	uint8_t* buf = context.allocate(size);
	dynamic_size_traits<VersionVector>::save(buf, serializedVV, context);

	VersionVector deserializedVV;
	dynamic_size_traits<VersionVector>::load(buf, size, deserializedVV, context);

	ASSERT(serializedVV.compare(deserializedVV));

	return Void();
}

TEST_CASE("/fdbclient/VersionVector/testE") {
	Arena arena;
	TestContextArena context{ arena };

	VersionVector serializedVV;
	// 800 storage servers spread over 2 regions, maxTagId < INT16_MAX, and
	// maxCommitVersionDelta < UINT64_MAX.
	populateVersionVector(serializedVV, 800, 2, INT16_MAX, UINT64_MAX);

	size_t size = dynamic_size_traits<VersionVector>::size(serializedVV, context);

	uint8_t* buf = context.allocate(size);
	dynamic_size_traits<VersionVector>::save(buf, serializedVV, context);

	VersionVector deserializedVV;
	dynamic_size_traits<VersionVector>::load(buf, size, deserializedVV, context);

	ASSERT(serializedVV.compare(deserializedVV));

	return Void();
}

TEST_CASE("/fdbclient/VersionVector/testF") {
	Arena arena;
	TestContextArena context{ arena };

	VersionVector serializedVV;
	// 1600 storage servers spread over 4 regions, maxTagId < INT16_MAX, and
	// maxCommitVersionDelta < UINT8_MAX.
	populateVersionVector(serializedVV, 1600, 4, INT16_MAX, UINT8_MAX);

	size_t size = dynamic_size_traits<VersionVector>::size(serializedVV, context);

	uint8_t* buf = context.allocate(size);
	dynamic_size_traits<VersionVector>::save(buf, serializedVV, context);

	VersionVector deserializedVV;
	dynamic_size_traits<VersionVector>::load(buf, size, deserializedVV, context);

	ASSERT(serializedVV.compare(deserializedVV));

	return Void();
}

TEST_CASE("/fdbclient/VersionVector/testG") {
	Arena arena;
	TestContextArena context{ arena };

	VersionVector serializedVV;
	// 1600 storage servers spread over 4 regions, maxTagId < INT16_MAX, and
	// maxCommitVersionDelta < UINT16_MAX.
	populateVersionVector(serializedVV, 1600, 4, INT16_MAX, UINT16_MAX);

	size_t size = dynamic_size_traits<VersionVector>::size(serializedVV, context);

	uint8_t* buf = context.allocate(size);
	dynamic_size_traits<VersionVector>::save(buf, serializedVV, context);

	VersionVector deserializedVV;
	dynamic_size_traits<VersionVector>::load(buf, size, deserializedVV, context);

	ASSERT(serializedVV.compare(deserializedVV));

	return Void();
}

TEST_CASE("/fdbclient/VersionVector/testH") {
	Arena arena;
	TestContextArena context{ arena };

	VersionVector serializedVV;
	// 3200 storage servers spread over 4 regions, maxTagId < INT16_MAX, and
	// maxCommitVersionDelta < UINT32_MAX.
	populateVersionVector(serializedVV, 3200, 4, INT16_MAX, UINT32_MAX);

	size_t size = dynamic_size_traits<VersionVector>::size(serializedVV, context);

	uint8_t* buf = context.allocate(size);
	dynamic_size_traits<VersionVector>::save(buf, serializedVV, context);

	VersionVector deserializedVV;
	dynamic_size_traits<VersionVector>::load(buf, size, deserializedVV, context);

	ASSERT(serializedVV.compare(deserializedVV));

	return Void();
}

TEST_CASE("/fdbclient/VersionVector/testI") {
	Arena arena;
	TestContextArena context{ arena };

	VersionVector serializedVV;
	// 3200 storage servers spread over 4 regions, maxTagId < INT16_MAX, and
	// maxCommitVersionDelta < UINT64_MAX.
	populateVersionVector(serializedVV, 3200, 4, INT16_MAX, UINT64_MAX);

	size_t size = dynamic_size_traits<VersionVector>::size(serializedVV, context);

	uint8_t* buf = context.allocate(size);
	dynamic_size_traits<VersionVector>::save(buf, serializedVV, context);

	VersionVector deserializedVV;
	dynamic_size_traits<VersionVector>::load(buf, size, deserializedVV, context);

	ASSERT(serializedVV.compare(deserializedVV));

	return Void();
}

} // namespace unit_tests

void forceLinkVersionVectorTests() {}
