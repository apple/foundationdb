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

TEST_CASE("/DataDistributor/ShardsAffectedByTeamFailure/CancelMove") {
	ShardsAffectedByTeamFailure shards;
	shards.setCheckMode(ShardsAffectedByTeamFailure::CheckMode::ForceCheck);

	const UID source1(1, 0), source2(2, 0), destination1(3, 0), destination2(4, 0), redirected1(5, 0),
	    redirected2(6, 0);
	const ShardsAffectedByTeamFailure::Team source({ source1, source2 }, true);
	const ShardsAffectedByTeamFailure::Team destination({ destination1, destination2 }, true);
	const ShardsAffectedByTeamFailure::Team redirected({ redirected1, redirected2 }, true);
	const KeyRange moveRange = KeyRangeRef("a"_sr, "c"_sr);
	const KeyRange leftRange = KeyRangeRef("a"_sr, "b"_sr);
	const KeyRange rightRange = KeyRangeRef("b"_sr, "c"_sr);

	shards.assignRangeToTeams(allKeys, { source });
	shards.defineShard(moveRange);
	shards.moveShard(moveRange, { destination });
	shards.defineShard(leftRange);

	auto restored = shards.cancelMove(moveRange, { destination }, { source });
	ASSERT((restored == std::vector<KeyRange>{ leftRange, rightRange }));
	ASSERT(shards.getTeamsForFirstShard(leftRange).first == std::vector<ShardsAffectedByTeamFailure::Team>{ source });
	ASSERT(shards.getTeamsForFirstShard(rightRange).first == std::vector<ShardsAffectedByTeamFailure::Team>{ source });
	ASSERT(shards.getTeamsForFirstShard(leftRange).second.empty());
	ASSERT(shards.getTeamsForFirstShard(rightRange).second.empty());
	ASSERT_EQ(shards.getNumberOfShards(destination), 0);
	ASSERT_EQ(shards.getNumberOfShards(source), 4);

	shards.moveShard(moveRange, { destination });
	shards.moveShard(rightRange, { redirected });
	restored = shards.cancelMove(moveRange, { destination }, { source });
	ASSERT((restored == std::vector<KeyRange>{ leftRange }));
	ASSERT(shards.getTeamsForFirstShard(leftRange).first == std::vector<ShardsAffectedByTeamFailure::Team>{ source });
	ASSERT(shards.getTeamsForFirstShard(rightRange).first ==
	       std::vector<ShardsAffectedByTeamFailure::Team>{ redirected });
	ASSERT_EQ(shards.getNumberOfShards(destination), 0);
	ASSERT_EQ(shards.getNumberOfShards(redirected), 1);

	shards.moveShard(moveRange, { destination });
	shards.moveShard(KeyRangeRef("aa"_sr, "ab"_sr), { redirected });
	restored = shards.cancelMove(moveRange, { destination }, { source });
	ASSERT((restored == std::vector<KeyRange>{ leftRange, rightRange }));
	ASSERT(shards.getTeamsForFirstShard(leftRange).first ==
	       std::vector<ShardsAffectedByTeamFailure::Team>{ redirected });
	ASSERT(shards.getTeamsForFirstShard(rightRange).first == std::vector<ShardsAffectedByTeamFailure::Team>{ source });
	ASSERT_EQ(shards.getNumberOfShards(destination), 0);
	ASSERT_EQ(shards.getNumberOfShards(redirected), 1);

	ShardsAffectedByTeamFailure partialShards;
	partialShards.setCheckMode(ShardsAffectedByTeamFailure::CheckMode::ForceCheck);
	partialShards.assignRangeToTeams(allKeys, { source });
	partialShards.moveShard(allKeys, { destination });
	const KeyRange partialCancelRange = KeyRangeRef("a"_sr, "b"_sr);
	restored = partialShards.cancelMove(partialCancelRange, { destination }, { source });
	ASSERT((restored == std::vector<KeyRange>{ partialCancelRange }));
	ASSERT(partialShards.getTeamsForFirstShard(partialCancelRange).first ==
	       std::vector<ShardsAffectedByTeamFailure::Team>{ source });
	ASSERT(partialShards.getTeamsForFirstShard(KeyRangeRef("b"_sr, "c"_sr)).first ==
	       std::vector<ShardsAffectedByTeamFailure::Team>{ destination });
	ASSERT_EQ(partialShards.getNumberOfShards(destination), 2);
	ASSERT_EQ(partialShards.getNumberOfShards(source), 1);

	return Void();
}

// Regression test for the graceful-exclude finalize stall: a partial-overlap (sub-range) moveShard leaves the
// drained source server counted in ShardsAffectedByTeamFailure even though its data is gone, which blocks
// removeStorageServer() indefinitely. The removal gate reconciles this by scrubbing the server from the map
// (removeFailedServerForRange) once the on-disk serverKeys confirm it owns no data. This verifies both that the
// stranding reproduces and that the scrub clears it without disturbing the surviving replica.
TEST_CASE("/DataDistributor/ShardsAffectedByTeamFailure/GateReconcilesStrandedServer") {
	ShardsAffectedByTeamFailure shards;
	shards.setCheckMode(ShardsAffectedByTeamFailure::CheckMode::ForceCheck);

	const UID excluded(9, 0);
	const ShardsAffectedByTeamFailure::Team teamWithExcluded({ UID(1, 0), UID(2, 0), excluded }, true);
	const ShardsAffectedByTeamFailure::Team replacementTeam({ UID(3, 0), UID(4, 0), UID(5, 0) }, true);

	// Reproduce the stall's stranded state: a partial-overlap move (its key range does not fully contain the
	// tracked shard) takes moveShard()'s else-branch, which appends the destination team without erasing the
	// source team -- so the excluded server stays counted even though the move relocated data off it.
	shards.assignRangeToTeams(KeyRangeRef("e"_sr, "z"_sr), { teamWithExcluded });
	ASSERT_EQ(shards.getNumberOfShards(excluded), 1);
	shards.moveShard(KeyRangeRef("e"_sr, "m"_sr), { replacementTeam }); // partial: [e,m) does not contain [e,z)
	ASSERT(shards.getNumberOfShards(excluded) > 0); // stranded: this is what blocks removeStorageServer()

	// The removal gate's reconciliation once canRemoveStorageServer() (on-disk truth) reports the server empty.
	shards.removeFailedServerForRange(allKeys, excluded);

	// The stranded count clears -> the gate opens and the server can be removed.
	ASSERT_EQ(shards.getNumberOfShards(excluded), 0);
	// The surviving replica's accounting is untouched (ForceCheck also validated the map invariant throughout).
	ASSERT(shards.getNumberOfShards(UID(3, 0)) > 0);
	shards.check();

	return Void();
}
