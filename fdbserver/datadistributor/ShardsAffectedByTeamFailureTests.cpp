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

// A relocation is frequently launched for a STRICT SUB-RANGE of a tracked shard (DDQueue truncates queued
// relocations against overlapping ones, and launches a relocator per range returned by
// getRangesAffectedByInsertion, whose leftover fragments are pieces of an already in-flight relocation). Such a
// move must still erase the source team for the range that actually moved, otherwise the drained source servers
// stay counted in storageServerShards forever and a graceful exclude of one of them never completes.
TEST_CASE("/DataDistributor/ShardsAffectedByTeamFailure/MoveSubRangeErasesSource") {
	ShardsAffectedByTeamFailure shards;
	shards.setCheckMode(ShardsAffectedByTeamFailure::CheckMode::ForceCheck);

	const UID drained(9, 0);
	const ShardsAffectedByTeamFailure::Team source({ UID(1, 0), UID(2, 0), drained }, true);
	const ShardsAffectedByTeamFailure::Team destination({ UID(3, 0), UID(4, 0), UID(5, 0) }, true);

	shards.assignRangeToTeams(KeyRangeRef("e"_sr, "z"_sr), { source });
	ASSERT_EQ(shards.getNumberOfShards(drained), 1);

	// [e,m) does not contain the tracked shard [e,z): the shard is split at the move boundary so that the
	// source team is erased for [e,m) and retained for [m,z).
	shards.moveShard(KeyRangeRef("e"_sr, "m"_sr), { destination });

	auto movedTeams = shards.getTeamsFor("e"_sr);
	ASSERT_EQ(movedTeams.first.size(), 1);
	ASSERT(movedTeams.first[0] == destination);
	auto untouchedTeams = shards.getTeamsFor("m"_sr);
	ASSERT_EQ(untouchedTeams.first.size(), 1);
	ASSERT(untouchedTeams.first[0] == source);

	// The drained server is still counted for the half that did not move, and only that half.
	ASSERT_EQ(shards.getNumberOfShards(drained), 1);
	ASSERT_EQ(shards.getNumberOfShards(source), 1);
	ASSERT_EQ(shards.getNumberOfShards(destination), 1);

	// Moving the remainder clears it entirely -- the removal gate for `drained` can now open.
	shards.moveShard(KeyRangeRef("m"_sr, "z"_sr), { destination });
	ASSERT_EQ(shards.getNumberOfShards(drained), 0);
	shards.check();

	return Void();
}

// Regression test for the graceful-exclude finalize stall: a server can remain counted in
// ShardsAffectedByTeamFailure after its data is gone, which blocks removeStorageServer() indefinitely. The
// removal gate reconciles this with scrubServer() once the on-disk serverKeys confirm the server owns no data.
// The stranded state is constructed directly (a shard attributed to two teams, one of them containing the
// excluded server) rather than by provoking a specific accounting bug, so this stays a test of the gate's
// reconciliation regardless of how the map came to be stale. Critically it also verifies that the scrub drops
// the whole stale TEAM REFERENCE rather than shrinking the team.
TEST_CASE("/DataDistributor/ShardsAffectedByTeamFailure/GateReconcilesStrandedServer") {
	ShardsAffectedByTeamFailure shards;
	shards.setCheckMode(ShardsAffectedByTeamFailure::CheckMode::ForceCheck);

	const UID excluded(9, 0);
	const ShardsAffectedByTeamFailure::Team teamWithExcluded({ UID(1, 0), UID(2, 0), excluded }, true);
	const ShardsAffectedByTeamFailure::Team replacementTeam({ UID(3, 0), UID(4, 0), UID(5, 0) }, true);
	const KeyRange shardRange = KeyRangeRef("e"_sr, "z"_sr);

	shards.assignRangeToTeams(shardRange, { teamWithExcluded, replacementTeam });
	ASSERT_EQ(shards.getNumberOfShards(excluded), 1); // stranded: this is what blocks removeStorageServer()
	ASSERT_EQ(shards.getNumberOfShards(replacementTeam), 1);

	// The removal gate's reconciliation, once canRemoveStorageServer() (on-disk truth) reports the server empty.
	auto scrub = shards.scrubServer(excluded);
	ASSERT_EQ(scrub.shardsRewritten, 1);
	ASSERT_EQ(scrub.ownerlessShards, 0);

	// The stranded count clears -> the gate opens and the server can be removed.
	ASSERT_EQ(shards.getNumberOfShards(excluded), 0);
	// The surviving replica's accounting is untouched, and -- the point of scrubServer() over
	// removeFailedServerForRange() -- the shard is left attributed to the REAL replacement team only. A shrunken
	// {UID(1,0), UID(2,0)} team here would exist nowhere in DDTeamCollection and would be re-relocated forever at
	// PRIORITY_TEAM_REDUNDANT by teamTracker()'s "team not found" path.
	auto teams = shards.getTeamsFor("e"_sr);
	ASSERT_EQ(teams.first.size(), 1);
	ASSERT(teams.first[0] == replacementTeam);
	ASSERT_EQ(shards.getNumberOfShards(replacementTeam), 1);
	ASSERT_EQ(shards.getNumberOfShards(UID(1, 0)), 0);
	for (const auto& id : shards.getSourceServerIdsFor("e"_sr)) {
		ASSERT(id != excluded);
	}
	shards.check();

	return Void();
}

// The degenerate case the scrub has to have an answer for: the stranded server's team is the ONLY team the map
// has for that shard, so there is no real owner to fall back to. Dropping the reference must still clear the
// count (otherwise the exclude hangs) and must not leave a fabricated shrunken team behind.
TEST_CASE("/DataDistributor/ShardsAffectedByTeamFailure/ScrubServerOnlyTeam") {
	ShardsAffectedByTeamFailure shards;
	shards.setCheckMode(ShardsAffectedByTeamFailure::CheckMode::ForceCheck);

	const UID excluded(9, 0);
	const ShardsAffectedByTeamFailure::Team teamWithExcluded({ UID(1, 0), UID(2, 0), excluded }, true);
	const KeyRange shardRange = KeyRangeRef("e"_sr, "z"_sr);

	shards.assignRangeToTeams(shardRange, { teamWithExcluded });
	ASSERT_EQ(shards.getNumberOfShards(excluded), 1);

	auto scrub = shards.scrubServer(excluded);
	ASSERT_EQ(scrub.shardsRewritten, 1);
	// No surviving team and no previous-source team to promote: the entry is left with an empty current-team
	// list, which is the state a freshly-initialized map is in, and the shard tracker is asked to restart.
	ASSERT_EQ(scrub.ownerlessShards, 1);
	ASSERT_EQ(shards.getNumberOfShards(excluded), 0);
	ASSERT_EQ(shards.getNumberOfShards(UID(1, 0)), 0);
	ASSERT(shards.getTeamsFor("e"_sr).first.empty());
	shards.check();

	return Void();
}

// A stranded server can also be left in the PREVIOUS-SOURCE list, which is not mirrored in team_shards (so it
// does not affect getNumberOfShards) but IS what getSourceServerIdsFor() prefers -- a drained server left there
// would be handed back out as a relocation source.
TEST_CASE("/DataDistributor/ShardsAffectedByTeamFailure/ScrubServerPreviousSources") {
	ShardsAffectedByTeamFailure shards;
	shards.setCheckMode(ShardsAffectedByTeamFailure::CheckMode::ForceCheck);

	const UID excluded(9, 0);
	const ShardsAffectedByTeamFailure::Team teamWithExcluded({ UID(1, 0), UID(2, 0), excluded }, true);
	const ShardsAffectedByTeamFailure::Team replacementTeam({ UID(3, 0), UID(4, 0), UID(5, 0) }, true);
	const KeyRange shardRange = KeyRangeRef("e"_sr, "z"_sr);

	// A full-shard move leaves the old team in the previous-source list (no finishMove yet).
	shards.assignRangeToTeams(shardRange, { teamWithExcluded });
	shards.moveShard(shardRange, { replacementTeam });
	ASSERT_EQ(shards.getNumberOfShards(excluded), 0); // the count itself was reconciled by the full-shard move
	auto sources = shards.getSourceServerIdsFor("e"_sr);
	ASSERT(std::find(sources.begin(), sources.end(), excluded) != sources.end()); // still a candidate source

	auto scrub = shards.scrubServer(excluded);
	ASSERT_EQ(scrub.shardsRewritten, 1);
	ASSERT_EQ(scrub.ownerlessShards, 0);
	ASSERT_EQ(shards.getNumberOfShards(excluded), 0);
	for (const auto& id : shards.getSourceServerIdsFor("e"_sr)) {
		ASSERT(id != excluded);
	}
	// The current team is untouched.
	auto teams = shards.getTeamsFor("e"_sr);
	ASSERT_EQ(teams.first.size(), 1);
	ASSERT(teams.first[0] == replacementTeam);
	shards.check();

	return Void();
}
