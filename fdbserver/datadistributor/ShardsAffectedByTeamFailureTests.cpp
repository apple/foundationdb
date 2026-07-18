/*
 * ShardsAffectedByTeamFailureTests.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "ShardsAffectedByTeamFailure.h"
#include "flow/UnitTest.h"

void forceLinkShardsAffectedByTeamFailureTests() {}

TEST_CASE("/DataDistributor/ShardsAffectedByTeamFailure/SplitAndMerge") {
	ShardsAffectedByTeamFailure shards;
	shards.setCheckMode(ShardsAffectedByTeamFailure::CheckMode::ForceCheck);

	const UID source1(1, 0), source2(2, 0), left1(3, 0), left2(4, 0), right1(5, 0), right2(6, 0);
	const ShardsAffectedByTeamFailure::Team source({ source1, source2 }, true);
	const ShardsAffectedByTeamFailure::Team left({ left1, left2 }, true);
	const ShardsAffectedByTeamFailure::Team right({ right1, right2 }, true);
	const KeyRange splitRange = KeyRangeRef("a"_sr, "c"_sr);
	const KeyRange leftRange = KeyRangeRef("a"_sr, "b"_sr);
	const KeyRange rightRange = KeyRangeRef("b"_sr, "c"_sr);

	shards.assignRangeToTeams(allKeys, { source });
	shards.defineShard(splitRange);

	ASSERT_EQ(shards.getNumberOfShards(), 3);
	ASSERT_EQ(shards.getNumberOfShards(source), 3);
	ASSERT_EQ(shards.getNumberOfShards(source1), 3);
	ASSERT_EQ(shards.getNumberOfShards(source2), 3);

	shards.assignRangeToTeams(leftRange, { left });
	shards.assignRangeToTeams(rightRange, { right });

	ASSERT_EQ(shards.getNumberOfShards(), 4);
	ASSERT_EQ(shards.getNumberOfShards(source), 2);
	ASSERT_EQ(shards.getNumberOfShards(left), 1);
	ASSERT_EQ(shards.getNumberOfShards(right), 1);
	ASSERT(shards.getShardsFor(left) == std::vector<KeyRange>{ leftRange });
	ASSERT(shards.getShardsFor(right) == std::vector<KeyRange>{ rightRange });

	shards.defineShard(splitRange);

	auto teams = shards.getTeamsForFirstShard(splitRange);
	ASSERT((teams.first == std::vector<ShardsAffectedByTeamFailure::Team>{ left, right }));
	ASSERT(teams.second.empty());
	ASSERT_EQ(shards.getNumberOfShards(), 3);
	ASSERT_EQ(shards.getNumberOfShards(source), 2);
	ASSERT_EQ(shards.getNumberOfShards(left), 1);
	ASSERT_EQ(shards.getNumberOfShards(right), 1);
	ASSERT_EQ(shards.getNumberOfShards(source1), 2);
	ASSERT_EQ(shards.getNumberOfShards(source2), 2);
	ASSERT_EQ(shards.getNumberOfShards(left1), 1);
	ASSERT_EQ(shards.getNumberOfShards(left2), 1);
	ASSERT_EQ(shards.getNumberOfShards(right1), 1);
	ASSERT_EQ(shards.getNumberOfShards(right2), 1);
	ASSERT(shards.getShardsFor(left) == std::vector<KeyRange>{ splitRange });
	ASSERT(shards.getShardsFor(right) == std::vector<KeyRange>{ splitRange });

	return Void();
}

TEST_CASE("/DataDistributor/ShardsAffectedByTeamFailure/DestinationSourceTransition") {
	ShardsAffectedByTeamFailure shards;
	shards.setCheckMode(ShardsAffectedByTeamFailure::CheckMode::ForceCheck);

	const UID source1(1, 0), source2(2, 0), destination1(3, 0), destination2(4, 0), redirected1(5, 0),
	    redirected2(6, 0);
	const ShardsAffectedByTeamFailure::Team source({ source1, source2 }, true);
	const ShardsAffectedByTeamFailure::Team destination({ destination1, destination2 }, true);
	const ShardsAffectedByTeamFailure::Team redirected({ redirected1, redirected2 }, true);
	const KeyRange moveRange = KeyRangeRef("a"_sr, "c"_sr);

	shards.assignRangeToTeams(allKeys, { source });
	shards.defineShard(moveRange);
	shards.moveShard(moveRange, { destination });

	auto teams = shards.getTeamsForFirstShard(moveRange);
	ASSERT(teams.first == std::vector<ShardsAffectedByTeamFailure::Team>{ destination });
	ASSERT(teams.second == std::vector<ShardsAffectedByTeamFailure::Team>{ source });
	ASSERT((shards.getSourceServerIdsFor(moveRange.begin) == std::vector<UID>{ source1, source2 }));
	ASSERT_EQ(shards.getNumberOfShards(), 3);
	ASSERT_EQ(shards.getNumberOfShards(source), 2);
	ASSERT_EQ(shards.getNumberOfShards(destination), 1);
	ASSERT_EQ(shards.getNumberOfShards(source1), 2);
	ASSERT_EQ(shards.getNumberOfShards(source2), 2);
	ASSERT_EQ(shards.getNumberOfShards(destination1), 1);
	ASSERT_EQ(shards.getNumberOfShards(destination2), 1);
	ASSERT(shards.getShardsFor(destination) == std::vector<KeyRange>{ moveRange });

	shards.moveShard(moveRange, { redirected });

	teams = shards.getTeamsForFirstShard(moveRange);
	ASSERT(teams.first == std::vector<ShardsAffectedByTeamFailure::Team>{ redirected });
	ASSERT((teams.second == std::vector<ShardsAffectedByTeamFailure::Team>{ source, destination }));
	ASSERT((shards.getSourceServerIdsFor(moveRange.begin) ==
	        std::vector<UID>{ source1, source2, destination1, destination2 }));
	ASSERT_EQ(shards.getNumberOfShards(source), 2);
	ASSERT_EQ(shards.getNumberOfShards(destination), 0);
	ASSERT_EQ(shards.getNumberOfShards(redirected), 1);
	ASSERT_EQ(shards.getNumberOfShards(redirected1), 1);
	ASSERT_EQ(shards.getNumberOfShards(redirected2), 1);
	ASSERT(shards.getShardsFor(redirected) == std::vector<KeyRange>{ moveRange });

	shards.finishMove(moveRange);

	teams = shards.getTeamsForFirstShard(moveRange);
	ASSERT(teams.first == std::vector<ShardsAffectedByTeamFailure::Team>{ redirected });
	ASSERT(teams.second.empty());
	ASSERT((shards.getSourceServerIdsFor(moveRange.begin) == std::vector<UID>{ redirected1, redirected2 }));
	ASSERT_EQ(shards.getNumberOfShards(source), 2);
	ASSERT_EQ(shards.getNumberOfShards(destination), 0);
	ASSERT_EQ(shards.getNumberOfShards(redirected), 1);

	return Void();
}

TEST_CASE("/DataDistributor/ShardsAffectedByTeamFailure/RetryMergedShardAfterPartialMoves") {
	ShardsAffectedByTeamFailure shards;
	shards.setCheckMode(ShardsAffectedByTeamFailure::CheckMode::ForceCheck);

	const UID failed(1, 0), leftServer(2, 0), rightServer(3, 0), healthy1(4, 0), healthy2(5, 0);
	const ShardsAffectedByTeamFailure::Team left({ failed, leftServer }, true);
	const ShardsAffectedByTeamFailure::Team right({ failed, rightServer }, true);
	const ShardsAffectedByTeamFailure::Team healthy({ healthy1, healthy2 }, true);
	const KeyRange mergedRange = KeyRangeRef("a"_sr, "c"_sr);
	const KeyRange leftRange = KeyRangeRef("a"_sr, "b"_sr);
	const KeyRange rightRange = KeyRangeRef("b"_sr, "c"_sr);

	shards.assignRangeToTeams(leftRange, { left });
	shards.assignRangeToTeams(rightRange, { right });
	shards.defineShard(mergedRange);

	shards.moveShard(leftRange, { healthy });
	shards.finishMove(leftRange);
	shards.moveShard(rightRange, { healthy });
	shards.finishMove(rightRange);

	ASSERT_EQ(shards.getNumberOfShards(failed), 2);
	ASSERT(shards.getShardsFor(left) == std::vector<KeyRange>{ mergedRange });
	ASSERT(shards.getShardsFor(right) == std::vector<KeyRange>{ mergedRange });

	shards.moveShard(mergedRange, { healthy });
	shards.finishMove(mergedRange);

	ASSERT_EQ(shards.getNumberOfShards(failed), 0);
	ASSERT_EQ(shards.getNumberOfShards(left), 0);
	ASSERT_EQ(shards.getNumberOfShards(right), 0);
	ASSERT(shards.getShardsFor(healthy) == std::vector<KeyRange>{ mergedRange });

	return Void();
}
