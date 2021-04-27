/*
 * TeamVersionTracker.cpp
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

#include "fdbserver/ptxn/TeamVersionTracker.h"

#include "fdbclient/FDBTypes.h"
#include "flow/UnitTest.h"

namespace ptxn {

TeamVersionTracker::TeamVersionTracker() {}

void TeamVersionTracker::addTeams(const std::vector<TeamID>& teams, Version beginVersion) {
	if (beginVersion > maxCV) {
		maxCV = beginVersion;
	}

	for (const auto& team : teams) {
		versions[team] = beginVersion;
	}
}

std::map<TeamID, Version> TeamVersionTracker::updateTeams(const std::vector<TeamID>& teams, Version commitVersion) {
	if (commitVersion > maxCV) {
		maxCV = commitVersion;
	}

	std::map<TeamID, Version> results;
	for (const auto& team : teams) {
		auto it = versions.find(team);
		ASSERT(it != versions.end() && it->second < commitVersion);
		auto pair = results.emplace(it->first, it->second);
		ASSERT(pair.second); // insertion happens
		it->second = commitVersion;
	}
	return results;
}

// TODO: reduce O(N) to O(1).
std::pair<TeamID, Version> TeamVersionTracker::mostLaggingTeam() const {
	Version v = invalidVersion;
	TeamID team;

	for (const auto& [tid, cv] : versions) {
		if (v == invalidVersion || v > cv) {
			v = cv;
			team = tid;
		}
	}
	return {team, v};
}

} // namespace ptxn

TEST_CASE("fdbserver/ptxn/test/versiontracker") {
	ptxn::TeamVersionTracker tracker;

	std::vector<ptxn::TeamID> teams;
	for (int i = 0; i < 5; i++) {
		teams.emplace_back(0, i);
	}
	tracker.addTeams(teams, -1);

	{
		ptxn::TeamID a(0, 0), b(0, 1), c(0, 2),d(0, 3), e(0, 4);
		teams.clear();
		teams.push_back(a);
		const Version cv1 = 10;
		auto results = tracker.updateTeams(teams, cv1);
		ASSERT(results.size() == 1 && results[a] == -1);
		ASSERT_EQ(tracker.getCommitVersion(a), cv1);
		ASSERT_EQ(tracker.getMaxCommitVersion(), cv1);

		teams.clear();
		teams.push_back(b);
		teams.push_back(c);
		const Version cv2 = 20;
		results = tracker.updateTeams(teams, cv2);
		ASSERT(results.size() == 2);
		ASSERT(results[b] == -1 && results[c] == -1);
		ASSERT_EQ(tracker.getCommitVersion(b), cv2);
		ASSERT_EQ(tracker.getCommitVersion(c), cv2);
		ASSERT_EQ(tracker.getMaxCommitVersion(), cv2);

		teams.clear();
		teams.push_back(b);
		teams.push_back(d);
		teams.push_back(e);
		const Version cv3 = 30;
		results = tracker.updateTeams(teams, cv3);
		ASSERT(results.size() == 3);
		ASSERT(results[b] == cv2 && results[d] == -1 && results[e] == -1);
		ASSERT_EQ(tracker.getCommitVersion(b), cv3);
		ASSERT_EQ(tracker.getCommitVersion(c), cv2);
		ASSERT_EQ(tracker.getCommitVersion(d), cv3);
		ASSERT_EQ(tracker.getMaxCommitVersion(), cv3);
		auto teamVersion = tracker.mostLaggingTeam();
		ASSERT(teamVersion.first == a && teamVersion.second == cv1);
	}
	// Non-existent team CV == -1
	ASSERT_EQ(tracker.getCommitVersion(TeamID(1, 0)), invalidVersion);

	return Void();
}