/*
 * ResolutionBalancer.cpp
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

#include "ResolutionBalancer.h"

#include <functional>
#include <tuple>

#include "fdbclient/KeyRangeMap.h"
#include "fdbserver/core/MasterInterface.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/StorageMetrics.h"
#include "flow/flow.h"
#include "flow/UnitTest.h"

void ResolutionBalancer::setResolvers(const std::vector<ResolverInterface>& v) {
	resolvers = v;
	if (resolvers.size() > 1)
		triggerResolution.trigger();
}

void ResolutionBalancer::setChangesInReply(UID requestingProxy, GetCommitVersionReply& rep) {
	if (resolverNeedingChanges.contains(requestingProxy)) {
		rep.resolverChanges = resolverChanges.get();
		rep.resolverChangesVersion = resolverChangesVersion;
		resolverNeedingChanges.erase(requestingProxy);

		CODE_PROBE(!rep.resolverChanges.empty(), "resolution balancing moves keyranges");
		if (resolverNeedingChanges.empty())
			resolverChanges.set(Standalone<VectorRef<ResolverMoveRef>>());
	}
}

static std::pair<KeyRangeRef, bool> findRange(CoalescedKeyRangeMap<int>& key_resolver,
                                              Standalone<VectorRef<ResolverMoveRef>>& movedRanges,
                                              int src,
                                              int dest) {
	auto ranges = key_resolver.ranges();
	auto prev = ranges.begin();
	auto it = ranges.begin();
	++it;
	if (it == ranges.end()) {
		if (ranges.begin().value() != src ||
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(ranges.begin()->range(), dest)) !=
		        movedRanges.end())
			throw operation_failed();
		return std::make_pair(ranges.begin().range(), true);
	}

	std::set<int> borders;
	// If possible expand an existing boundary between the two resolvers
	for (; it != ranges.end(); ++it) {
		if (it->value() == src && prev->value() == dest &&
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(it->range(), dest)) ==
		        movedRanges.end()) {
			return std::make_pair(it->range(), true);
		}
		if (it->value() == dest && prev->value() == src &&
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(prev->range(), dest)) ==
		        movedRanges.end()) {
			return std::make_pair(prev->range(), false);
		}
		if (it->value() == dest)
			borders.insert(prev->value());
		if (prev->value() == dest)
			borders.insert(it->value());
		++prev;
	}

	prev = ranges.begin();
	it = ranges.begin();
	++it;
	// If possible create a new boundary which doesn't exist yet
	for (; it != ranges.end(); ++it) {
		if (it->value() == src && !borders.contains(prev->value()) &&
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(it->range(), dest)) ==
		        movedRanges.end()) {
			return std::make_pair(it->range(), true);
		}
		if (prev->value() == src && !borders.contains(it->value()) &&
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(prev->range(), dest)) ==
		        movedRanges.end()) {
			return std::make_pair(prev->range(), false);
		}
		++prev;
	}

	it = ranges.begin();
	for (; it != ranges.end(); ++it) {
		if (it->value() == src &&
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(it->range(), dest)) ==
		        movedRanges.end()) {
			return std::make_pair(it->range(), true);
		}
	}
	throw operation_failed(); // every range of src is already in movedRanges: being moved, or could not be cut
}

// Chooses ranges holding about `amount` of src's load to move to dest; throws operation_failed if nothing should move.
// `key_resolver` must stay alive and unchanged until the returned future is ready.
static Future<Standalone<VectorRef<ResolverMoveRef>>> chooseRangesToMove(
    CoalescedKeyRangeMap<int>& key_resolver,
    int src,
    int dest,
    int64_t amount,
    std::function<Future<ResolutionSplitReply>(ResolutionSplitRequest)> split) {
	Standalone<VectorRef<ResolverMoveRef>> movedRanges;
	Standalone<VectorRef<ResolverMoveRef>> triedRanges;
	bool skippedRange = false;
	while (true) {
		std::pair<KeyRangeRef, bool> range;
		bool noRangeLeft = false;
		try {
			range = findRange(key_resolver, triedRanges, src, dest);
		} catch (Error& e) {
			// Out of candidates. src keeps any skipped range, so the moves chosen so far can stand; without a skip they
			// would give away all of src's ranges.
			if (e.code() != error_code_operation_failed || !skippedRange || movedRanges.empty())
				throw;
			noRangeLeft = true;
		}
		if (noRangeLeft) {
			CODE_PROBE(true, "Resolution balancing keeps the moves chosen before a skipped range");
			co_return movedRanges;
		}

		ResolutionSplitRequest req;
		req.front = range.second;
		req.offset = amount;
		req.range = range.first;

		ResolutionSplitReply reply = co_await split(req);
		KeyRangeRef moveRange =
		    range.second ? KeyRangeRef(range.first.begin, reply.key) : KeyRangeRef(reply.key, range.first.end);
		triedRanges.push_back_deep(triedRanges.arena(), ResolverMoveRef(range.first, dest));
		if (moveRange.empty()) {
			// Nothing to cut (src has no sampled load before the range's end). The empty move would change nothing and
			// be chosen again every round.
			CODE_PROBE(true, "Resolution balancing skips a range the resolver cannot split");
			TraceEvent("ResolutionRangeNotSplit")
			    .suppressFor(1.0)
			    .detail("Src", src)
			    .detail("Dest", dest)
			    .detail("Amount", amount)
			    .detail("Range", range.first);
			skippedRange = true;
			continue;
		}
		TraceEvent("MovingResolutionRange")
		    .detail("Src", src)
		    .detail("Dest", dest)
		    .detail("Amount", amount)
		    .detail("StartRange", range.first)
		    .detail("MoveRange", moveRange)
		    .detail("Used", reply.used)
		    .detail("KeyResolverRanges", key_resolver.size());
		movedRanges.push_back_deep(movedRanges.arena(), ResolverMoveRef(moveRange, dest));
		amount -= reply.used;
		if (moveRange != range.first || amount <= 0)
			co_return movedRanges;
	}
}

// Balance key ranges among resolvers so that their load are evenly distributed.
Future<Void> ResolutionBalancer::resolutionBalancing() {
	co_await triggerResolution.onTrigger();

	CoalescedKeyRangeMap<int> key_resolver(
	    0, SERVER_KNOBS->PROXY_USE_RESOLVER_PRIVATE_MUTATIONS ? normalKeys.end : allKeys.end);
	key_resolver.insert(SERVER_KNOBS->PROXY_USE_RESOLVER_PRIVATE_MUTATIONS ? normalKeys : allKeys, 0);
	while (true) {
		co_await delay(SERVER_KNOBS->MIN_BALANCE_TIME, TaskPriority::ResolutionMetrics);
		while (!resolverChanges.get().empty())
			co_await resolverChanges.onChange();
		std::vector<Future<ResolutionMetricsReply>> futures;
		futures.reserve(resolvers.size());
		for (auto& p : resolvers)
			futures.push_back(
			    brokenPromiseToNever(p.metrics.getReply(ResolutionMetricsRequest(), TaskPriority::ResolutionMetrics)));
		co_await waitForAll(futures);
		IndexedSet<std::pair<int64_t, int>, NoMetric> metrics;

		int64_t total = 0;
		for (int i = 0; i < futures.size(); i++) {
			total += futures[i].get().value;
			metrics.insert(std::make_pair(futures[i].get().value, i), NoMetric());
			//TraceEvent("ResolverMetric").detail("I", i).detail("Metric", futures[i].get());
		}
		if (metrics.lastItem()->first - metrics.begin()->first > SERVER_KNOBS->MIN_BALANCE_DIFFERENCE) {
			try {
				int src = metrics.lastItem()->second;
				int dest = metrics.begin()->second;
				int64_t amount = std::min(metrics.lastItem()->first - total / resolvers.size(),
				                          total / resolvers.size() - metrics.begin()->first) /
				                 2;
				Standalone<VectorRef<ResolverMoveRef>> movedRanges = co_await chooseRangesToMove(
				    key_resolver, src, dest, amount, [this, src](ResolutionSplitRequest req) {
					    return brokenPromiseToNever(
					        resolvers[src].split.getReply(req, TaskPriority::ResolutionMetrics));
				    });
				for (auto& it : movedRanges)
					key_resolver.insert(it.range, it.dest);
				// for(auto& it : key_resolver.ranges())
				//	TraceEvent("KeyResolver").detail("Range", it.range()).detail("Value", it.value());

				resolverChangesVersion = *pVersion + 1;
				for (auto& p : commitProxies)
					resolverNeedingChanges.insert(p.id());
				resolverChanges.set(movedRanges);
			} catch (Error& e) {
				if (e.code() != error_code_operation_failed)
					throw;
			}
		}
	}
}

namespace {

Future<ResolutionSplitReply> splitWithSample(const StorageMetricSample& sample, ResolutionSplitRequest req) {
	ResolutionSplitReply reply;
	std::tie(reply.key, reply.used) = sample.splitAndEstimate(req.range, req.offset, req.front);
	return reply;
}

int64_t sampledLoadOf(CoalescedKeyRangeMap<int>& keyResolver, const StorageMetricSample& sample, int resolver) {
	int64_t load = 0;
	for (auto r : keyResolver.ranges()) {
		if (r.value() == resolver) {
			load += sample.getEstimate(r.range());
		}
	}
	return load;
}

} // namespace

TEST_CASE("/fdbserver/ResolutionBalancer/SkipRangeThatCannotBeSplit") {
	CoalescedKeyRangeMap<int> keyResolver(0, normalKeys.end);
	keyResolver.insert(normalKeys, 0);
	keyResolver.insert(KeyRangeRef("m"_sr, "t"_sr), 1);
	StorageMetricSample srcSample(1);
	srcSample.sample.insert("u"_sr, 100);
	srcSample.sample.insert("v"_sr, 100);
	srcSample.sample.insert("w"_sr, 100);
	srcSample.sample.insert("x"_sr, 100);
	auto split = [&srcSample](ResolutionSplitRequest req) { return splitWithSample(srcSample, req); };

	for (int round = 0; round < 2; ++round) {
		const int64_t before = sampledLoadOf(keyResolver, srcSample, 0);
		Standalone<VectorRef<ResolverMoveRef>> moves = co_await chooseRangesToMove(keyResolver, 0, 1, 150, split);
		for (const auto& move : moves) {
			ASSERT(!move.range.empty());
			keyResolver.insert(move.range, move.dest);
		}
		ASSERT_LT(sampledLoadOf(keyResolver, srcSample, 0), before);
	}
}

TEST_CASE("/fdbserver/ResolutionBalancer/MoveFromBorderingRange") {
	CoalescedKeyRangeMap<int> keyResolver(0, normalKeys.end);
	keyResolver.insert(normalKeys, 1);
	keyResolver.insert(KeyRangeRef(""_sr, "m"_sr), 0);
	StorageMetricSample srcSample(1);
	srcSample.sample.insert("a"_sr, 100);
	srcSample.sample.insert("k"_sr, 100);
	srcSample.sample.insert("l"_sr, 100);
	auto split = [&srcSample](ResolutionSplitRequest req) { return splitWithSample(srcSample, req); };

	Standalone<VectorRef<ResolverMoveRef>> moves = co_await chooseRangesToMove(keyResolver, 0, 1, 150, split);
	ASSERT_EQ(moves.size(), 1);
	ASSERT(moves[0].range == KeyRangeRef("k"_sr, "m"_sr));
	ASSERT_EQ(moves[0].dest, 1);
}

TEST_CASE("/fdbserver/ResolutionBalancer/NothingToMoveWhenNoRangeCanBeSplit") {
	CoalescedKeyRangeMap<int> keyResolver(0, normalKeys.end);
	keyResolver.insert(normalKeys, 1);
	keyResolver.insert(KeyRangeRef(""_sr, "m"_sr), 0);
	StorageMetricSample srcSample(1);
	srcSample.sample.insert("u"_sr, 100);
	auto split = [&srcSample](ResolutionSplitRequest req) { return splitWithSample(srcSample, req); };

	bool nothingMoved = false;
	try {
		co_await chooseRangesToMove(keyResolver, 0, 1, 150, split);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_operation_failed);
		nothingMoved = true;
	}
	ASSERT(nothingMoved);
	ASSERT_EQ(keyResolver.size(), 2);
	ASSERT_EQ(keyResolver.rangeContaining("a"_sr).value(), 0);
}

TEST_CASE("/fdbserver/ResolutionBalancer/KeepMovesWhenLaterRangeIsSkipped") {
	CoalescedKeyRangeMap<int> keyResolver(0, normalKeys.end);
	keyResolver.insert(normalKeys, 0);
	keyResolver.insert(KeyRangeRef("c"_sr, "m"_sr), 2);
	keyResolver.insert(KeyRangeRef("m"_sr, "p"_sr), 3);
	keyResolver.insert(KeyRangeRef("p"_sr, "t"_sr), 1);
	StorageMetricSample srcSample(1);
	srcSample.sample.insert("u"_sr, 100);
	auto split = [&srcSample](ResolutionSplitRequest req) { return splitWithSample(srcSample, req); };

	Standalone<VectorRef<ResolverMoveRef>> moves = co_await chooseRangesToMove(keyResolver, 0, 1, 150, split);
	ASSERT_EQ(moves.size(), 1);
	ASSERT(moves[0].range == KeyRangeRef("t"_sr, normalKeys.end));
	ASSERT_EQ(moves[0].dest, 1);
}
