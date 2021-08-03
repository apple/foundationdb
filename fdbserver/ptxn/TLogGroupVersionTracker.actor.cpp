/*
 * TLogGroupVersionTracker.actor.cpp
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

#include <fdbserver/Knobs.h>
#include "fdbserver/ptxn/TLogGroupVersionTracker.h"

#include "fdbclient/FDBTypes.h"
#include "flow/UnitTest.h"

namespace ptxn {

TLogGroupVersionTracker::TLogGroupVersionTracker() {}

void TLogGroupVersionTracker::addGroups(const std::vector<TLogGroupID>& groups, Version beginVersion) {
	if (beginVersion > maxCV) {
		maxCV = beginVersion;
	}

	for (const auto& group : groups) {
		versions[group] = beginVersion;
	}
}

void TLogGroupVersionTracker::removeGroups(const std::vector<TLogGroupID>& groups) {
	for (const auto& group : groups) {
		versions.erase(group);
	}
}

std::map<TLogGroupID, Version> TLogGroupVersionTracker::updateGroups(const std::vector<TLogGroupID>& groups,
                                                                     Version commitVersion) {
	if (commitVersion > maxCV) {
		maxCV = commitVersion;
	}

	std::map<TLogGroupID, Version> results;
	for (const auto& group : groups) {
		auto it = versions.find(group);
		ASSERT_WE_THINK(it != versions.end() && it->second < commitVersion);
		auto pair = results.emplace(it->first, it->second);
		ASSERT_WE_THINK(pair.second); // insertion happens
		it->second = commitVersion;
	}

	if (SERVER_KNOBS->INSERT_EMPTY_TRANSACTION) {
		for (auto& it : versions) {
			if (results.count(it.first) == 0 &&
			    commitVersion - it.second >= SERVER_KNOBS->LAGGING_TLOG_GROUP_VERSION_LIMIT) {
				it.second = commitVersion;
				results.emplace(it.first, it.second);
			}
		}
	}

	return results;
}

// TODO: reduce O(N) to O(1).
std::pair<TLogGroupID, Version> TLogGroupVersionTracker::mostLaggingGroup() const {
	Version v = invalidVersion;
	TLogGroupID group;

	for (const auto& [tid, cv] : versions) {
		if (v == invalidVersion || v > cv) {
			v = cv;
			group = tid;
		}
	}
	return { group, v };
}

} // namespace ptxn

TEST_CASE("fdbserver/ptxn/test/versiontracker") {
	std::vector<Version> beginVersions(3, -1);
	beginVersions[1] = 0;
	beginVersions[2] = 3;

	for (auto beginVersion : beginVersions) {
		std::cout << "beginVersion: " << beginVersion << "\n";

		ptxn::TLogGroupVersionTracker tracker;
		std::vector<ptxn::TLogGroupID> groups;
		for (int i = 0; i < 5; i++) {
			groups.emplace_back(0, i);
		}
		tracker.addGroups(groups, beginVersion);

		ptxn::TLogGroupID a(0, 0), b(0, 1), c(0, 2), d(0, 3), e(0, 4);
		groups.clear();
		groups.push_back(a);
		const Version cv1 = 10;
		auto results = tracker.updateGroups(groups, cv1);
		ASSERT(results.size() == 1 && results[a] == beginVersion);
		ASSERT_EQ(tracker.getCommitVersion(a), cv1);
		ASSERT_EQ(tracker.getMaxCommitVersion(), cv1);

		groups.clear();
		groups.push_back(b);
		groups.push_back(c);
		const Version cv2 = 20;
		results = tracker.updateGroups(groups, cv2);
		ASSERT(results.size() == 2);
		ASSERT(results[b] == beginVersion && results[c] == beginVersion);
		ASSERT_EQ(tracker.getCommitVersion(b), cv2);
		ASSERT_EQ(tracker.getCommitVersion(c), cv2);
		ASSERT_EQ(tracker.getMaxCommitVersion(), cv2);

		groups.clear();
		groups.push_back(b);
		groups.push_back(d);
		groups.push_back(e);
		const Version cv3 = 30;
		results = tracker.updateGroups(groups, cv3);
		ASSERT(results.size() == 3);
		ASSERT(results[b] == cv2 && results[d] == beginVersion && results[e] == beginVersion);
		ASSERT_EQ(tracker.getCommitVersion(b), cv3);
		ASSERT_EQ(tracker.getCommitVersion(c), cv2);
		ASSERT_EQ(tracker.getCommitVersion(d), cv3);
		ASSERT_EQ(tracker.getMaxCommitVersion(), cv3);
		auto groupVersion = tracker.mostLaggingGroup();
		ASSERT(groupVersion.first == a && groupVersion.second == cv1);

		// Non-existent group CV == -1
		ASSERT_EQ(tracker.getCommitVersion(ptxn::TLogGroupID(1, 0)), invalidVersion);
	}

	return Void();
}
