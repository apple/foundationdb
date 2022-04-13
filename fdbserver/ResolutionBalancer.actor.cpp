/*
 * ResolutionBalancer.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/ResolutionBalancer.actor.h"

#include "fdbclient/KeyRangeMap.h"
#include "fdbserver/MasterInterface.h"
#include "fdbserver/Knobs.h"
#include "flow/flow.h"

#include "flow/actorcompiler.h" // This must be the last #include.

void ResolutionBalancer::setResolvers(const std::vector<ResolverInterface>& v) {
	resolvers = v;
	if (resolvers.size() > 1)
		triggerResolution.trigger();
}

void ResolutionBalancer::setChangesInReply(UID requestingProxy, GetCommitVersionReply& rep) {
	if (resolverNeedingChanges.count(requestingProxy)) {
		rep.resolverChanges = resolverChanges.get();
		rep.resolverChangesVersion = resolverChangesVersion;
		resolverNeedingChanges.erase(requestingProxy);

		TEST(!rep.resolverChanges.empty()); // resolution balancing moves keyranges
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
	// If possible create a new boundry which doesn't exist yet
	for (; it != ranges.end(); ++it) {
		if (it->value() == src && !borders.count(prev->value()) &&
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(it->range(), dest)) ==
		        movedRanges.end()) {
			return std::make_pair(it->range(), true);
		}
		if (prev->value() == src && !borders.count(it->value()) &&
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
	throw operation_failed(); // we are already attempting to move all of the data one resolver is assigned, so do not
	                          // move anything
}

// Balance key ranges among resolvers so that their load are evenly distributed.
ACTOR Future<Void> ResolutionBalancer::resolutionBalancing_impl(ResolutionBalancer* self) {
	wait(self->triggerResolution.onTrigger());

	state CoalescedKeyRangeMap<int> key_resolver(
	    0, SERVER_KNOBS->PROXY_USE_RESOLVER_PRIVATE_MUTATIONS ? normalKeys.end : allKeys.end);
	key_resolver.insert(SERVER_KNOBS->PROXY_USE_RESOLVER_PRIVATE_MUTATIONS ? normalKeys : allKeys, 0);
	loop {
		wait(delay(SERVER_KNOBS->MIN_BALANCE_TIME, TaskPriority::ResolutionMetrics));
		while (self->resolverChanges.get().size())
			wait(self->resolverChanges.onChange());
		state std::vector<Future<ResolutionMetricsReply>> futures;
		for (auto& p : self->resolvers)
			futures.push_back(
			    brokenPromiseToNever(p.metrics.getReply(ResolutionMetricsRequest(), TaskPriority::ResolutionMetrics)));
		wait(waitForAll(futures));
		state IndexedSet<std::pair<int64_t, int>, NoMetric> metrics;

		int64_t total = 0;
		for (int i = 0; i < futures.size(); i++) {
			total += futures[i].get().value;
			metrics.insert(std::make_pair(futures[i].get().value, i), NoMetric());
			//TraceEvent("ResolverMetric").detail("I", i).detail("Metric", futures[i].get());
		}
		if (metrics.lastItem()->first - metrics.begin()->first > SERVER_KNOBS->MIN_BALANCE_DIFFERENCE) {
			try {
				state int src = metrics.lastItem()->second;
				state int dest = metrics.begin()->second;
				state int64_t amount = std::min(metrics.lastItem()->first - total / self->resolvers.size(),
				                                total / self->resolvers.size() - metrics.begin()->first) /
				                       2;
				state Standalone<VectorRef<ResolverMoveRef>> movedRanges;

				loop {
					state std::pair<KeyRangeRef, bool> range = findRange(key_resolver, movedRanges, src, dest);

					ResolutionSplitRequest req;
					req.front = range.second;
					req.offset = amount;
					req.range = range.first;

					ResolutionSplitReply split =
					    wait(brokenPromiseToNever(self->resolvers[metrics.lastItem()->second].split.getReply(
					        req, TaskPriority::ResolutionMetrics)));
					KeyRangeRef moveRange = range.second ? KeyRangeRef(range.first.begin, split.key)
					                                     : KeyRangeRef(split.key, range.first.end);
					movedRanges.push_back_deep(movedRanges.arena(), ResolverMoveRef(moveRange, dest));
					TraceEvent("MovingResolutionRange")
					    .detail("Src", src)
					    .detail("Dest", dest)
					    .detail("Amount", amount)
					    .detail("StartRange", range.first)
					    .detail("MoveRange", moveRange)
					    .detail("Used", split.used)
					    .detail("KeyResolverRanges", key_resolver.size());
					amount -= split.used;
					if (moveRange != range.first || amount <= 0)
						break;
				}
				for (auto& it : movedRanges)
					key_resolver.insert(it.range, it.dest);
				// for(auto& it : key_resolver.ranges())
				//	TraceEvent("KeyResolver").detail("Range", it.range()).detail("Value", it.value());

				self->resolverChangesVersion = *self->pVersion + 1;
				for (auto& p : self->commitProxies)
					self->resolverNeedingChanges.insert(p.id());
				self->resolverChanges.set(movedRanges);
			} catch (Error& e) {
				if (e.code() != error_code_operation_failed)
					throw;
			}
		}
	}
}
