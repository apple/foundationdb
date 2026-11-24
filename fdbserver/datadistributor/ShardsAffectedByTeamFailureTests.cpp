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

TEST_CASE("/DataDistributor/ShardsAffectedByTeamFailure/RemoteOnlySources") {
	ShardsAffectedByTeamFailure shards;
	const UID first(1, 0), second(2, 0), extra(3, 0);
	const KeyRange left = KeyRangeRef("a"_sr, "c"_sr);
	const KeyRange right = KeyRangeRef("d"_sr, "f"_sr);
	shards.trackRemoteOnlyRange(KeyRangeRef("a"_sr, "b"_sr), { first });
	shards.trackRemoteOnlyRange(KeyRangeRef("b"_sr, "c"_sr), { first });
	shards.trackRemoteOnlyRange(right, { second });
	auto findCandidate = [&](const std::set<UID>& live) {
		shards.resetRemoteOnlyRangeSearch();
		return shards.findUnavailableRemoteOnlyRange([&](UID id) { return live.contains(id); });
	};

	ASSERT(!findCandidate({ first, second }).present());
	ASSERT(findCandidate({ first })->keys == right);
	auto candidate = findCandidate({ second, extra }).get();
	ASSERT(candidate.keys == left);

	// A previously unknown live source serves only the prefix. The suffix still needs a lookup.
	const KeyRange prefix = KeyRangeRef("a"_sr, "b"_sr);
	shards.updateRemoteOnlyRange(candidate, KeyRangeRef(allKeys.begin, prefix.end), { extra, first, extra });
	auto suffix = findCandidate({ second, extra }).get();
	ASSERT(suffix.keys == KeyRangeRef("b"_sr, "c"_sr));
	ASSERT(suffix.sources == std::vector<UID>{ first });

	// A later proxy reply can span multiple original shards. Retire only the candidate suffix.
	shards.updateRemoteOnlyRange(suffix, allKeys, {});
	ASSERT(!findCandidate({ second, extra }).present());
	candidate = findCandidate({ second }).get();
	ASSERT(candidate.keys == prefix);
	ASSERT((candidate.sources == std::vector<UID>{ first, extra }));
	shards.updateRemoteOnlyRange(candidate, allKeys, {});
	ASSERT(findCandidate({})->keys == right);
	shards.updateRemoteOnlyRange(findCandidate({}).get(), allKeys, {});
	ASSERT(!findCandidate({}).present());

	shards.trackRemoteOnlyRange(left, { first });
	shards.trackRemoteOnlyRange(right, { first });
	candidate = findCandidate({}).get();
	ASSERT(candidate.keys == right);
	shards.updateRemoteOnlyRange(candidate, allKeys, {});
	ASSERT(findCandidate({})->keys == left);
	return Void();
}

TEST_CASE("/DataDistributor/ShardsAffectedByTeamFailure/BoundedRemoteOnlySourceSearch") {
	ShardsAffectedByTeamFailure shards;
	for (int i = 0; i < 100; ++i) {
		Key begin = Key(format("key/%03d", i));
		Key end = Key(format("key/%03d", i + 1));
		shards.trackRemoteOnlyRange(KeyRangeRef(begin, end), { UID(i, 1) });
	}
	int checks = 0;
	std::set<UID> failed;
	auto isLive = [&](UID id) {
		++checks;
		return !failed.contains(id);
	};
	ASSERT(!shards.findUnavailableRemoteOnlyRange(isLive).present());
	ASSERT_EQ(checks, 64);
	ASSERT(shards.hasUncheckedRemoteOnlyRanges());
	ASSERT(!shards.findUnavailableRemoteOnlyRange(isLive).present());
	ASSERT_EQ(checks, 100);
	ASSERT(!shards.hasUncheckedRemoteOnlyRanges());
	ASSERT(!shards.findUnavailableRemoteOnlyRange(isLive).present());
	ASSERT_EQ(checks, 100);

	// A failure invalidates the completed sweep. A known witness retains priority over a new sweep.
	const UID last(99, 1);
	failed.insert(last);
	shards.resetRemoteOnlyRangeSearch();
	ASSERT(!shards.findUnavailableRemoteOnlyRange(isLive).present());
	auto witness = shards.findUnavailableRemoteOnlyRange(isLive).get();
	ASSERT(witness.sources == std::vector<UID>{ last });
	ASSERT_EQ(checks, 200);
	failed.insert(UID(0, 1));
	shards.resetRemoteOnlyRangeSearch();
	ASSERT(shards.findUnavailableRemoteOnlyRange(isLive)->keys == witness.keys);
	ASSERT_EQ(checks, 201);
	shards.updateRemoteOnlyRange(witness, witness.keys, {});
	witness = shards.findUnavailableRemoteOnlyRange(isLive).get();
	ASSERT(witness.sources.front() == UID(0, 1));
	ASSERT_EQ(checks, 202);
	shards.resetRemoteOnlyRangeSearch();
	shards.updateRemoteOnlyRange(witness, witness.keys, {});
	ASSERT(!shards.findUnavailableRemoteOnlyRange(isLive).present());
	ASSERT_EQ(checks, 266);
	ASSERT(shards.hasUncheckedRemoteOnlyRanges());
	return Void();
}

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
