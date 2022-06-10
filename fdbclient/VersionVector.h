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

struct VersionVector {
	boost::container::flat_map<Tag, Version> versions;
	Version maxVersion; // Specifies the max version in this version vector. (Note:
	                    // there may or may not be a corresponding entry for this
	                    // version in the "versions" map.)

	VersionVector() : maxVersion(invalidVersion) {}
	VersionVector(Version version) : maxVersion(version) {}

private:
	// Only invoked by getDelta() and applyDelta(), where tag has been validated
	// and version is guaranteed to be larger than the existing value.
	inline void setVersionNoCheck(const Tag& tag, Version version) { versions[tag] = version; }

public:
	Version getMaxVersion() const { return maxVersion; }

	int size() const { return versions.size(); }

	void setVersion(const Tag& tag, Version version) {
		ASSERT(tag != invalidTag);
		ASSERT(version > maxVersion);
		versions[tag] = version;
		maxVersion = version;
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
	}

	// @note this method, together with method applyDelta(), helps minimize
	// the number of version vector entries that get sent from sequencer to
	// grv proxy (and from grv proxy to client) on the read path.
	void getDelta(Version refVersion, VersionVector& delta) const {
		ASSERT(refVersion <= maxVersion);

		delta.clear();

		if (refVersion == maxVersion) {
			return; // rerurn an invalid version vector
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

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, versions, maxVersion);
	}
};

static const VersionVector minVersionVector{ 0 };
static const VersionVector maxVersionVector{ MAX_VERSION };
static const VersionVector invalidVersionVector{ invalidVersion };

#endif /* FDBCLIENT_VERSION_VECTOR_H */
