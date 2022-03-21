/*
 * masterserver.actor.cpp
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

#include <iterator>

#include "fdbrpc/sim_validation.h"
#include "fdbserver/CoordinatedState.h"
#include "fdbserver/CoordinationInterface.h" // copy constructors for ServerCoordinators class
#include "fdbserver/Knobs.h"
#include "fdbserver/MasterInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "flow/ActorCollection.h"
#include "flow/Trace.h"
#include "flow/genericactors.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct MasterData : NonCopyable, ReferenceCounted<MasterData> {
	UID dbgid;

	Version lastEpochEnd, // The last version in the old epoch not (to be) rolled back in this recovery
	    recoveryTransactionVersion; // The first version in this epoch

	Version liveCommittedVersion; // The largest live committed version reported by commit proxies.
	bool databaseLocked;
	Optional<Value> proxyMetadataVersion;
	Version minKnownCommittedVersion;

	ServerCoordinators coordinators;

	Version version; // The last version assigned to a proxy by getVersion()
	double lastVersionTime;

	std::vector<CommitProxyInterface> commitProxies;
	std::map<UID, CommitProxyVersionReplies> lastCommitProxyVersionReplies;
	std::vector<ResolverInterface> resolvers;

	MasterInterface myInterface;

	AsyncVar<Standalone<VectorRef<ResolverMoveRef>>> resolverChanges;
	Version resolverChangesVersion;
	std::set<UID> resolverNeedingChanges;
	AsyncTrigger triggerResolution;

	bool forceRecovery;

	CounterCollection cc;
	Counter getCommitVersionRequests;
	Counter getLiveCommittedVersionRequests;
	Counter reportLiveCommittedVersionRequests;

	Future<Void> logger;

	MasterData(Reference<AsyncVar<ServerDBInfo> const> const& dbInfo,
	           MasterInterface const& myInterface,
	           ServerCoordinators const& coordinators,
	           ClusterControllerFullInterface const& clusterController,
	           Standalone<StringRef> const& dbId,
	           bool forceRecovery)

	  : dbgid(myInterface.id()), lastEpochEnd(invalidVersion), recoveryTransactionVersion(invalidVersion),
	    liveCommittedVersion(invalidVersion), databaseLocked(false), minKnownCommittedVersion(invalidVersion),
	    coordinators(coordinators), version(invalidVersion), lastVersionTime(0), myInterface(myInterface),
	    forceRecovery(forceRecovery), cc("Master", dbgid.toString()),
	    getCommitVersionRequests("GetCommitVersionRequests", cc),
	    getLiveCommittedVersionRequests("GetLiveCommittedVersionRequests", cc),
	    reportLiveCommittedVersionRequests("ReportLiveCommittedVersionRequests", cc) {
		logger = traceCounters("MasterMetrics", dbgid, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "MasterMetrics");
		if (forceRecovery && !myInterface.locality.dcId().present()) {
			TraceEvent(SevError, "ForcedRecoveryRequiresDcID").log();
			forceRecovery = false;
		}
	}
	~MasterData() = default;
};

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
ACTOR Future<Void> resolutionBalancing(Reference<MasterData> self) {
	wait(self->triggerResolution.onTrigger());

	state CoalescedKeyRangeMap<int> key_resolver;
	key_resolver.insert(allKeys, 0);
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

				self->resolverChangesVersion = self->version + 1;
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

ACTOR Future<Void> getVersion(Reference<MasterData> self, GetCommitVersionRequest req) {
	state Span span("M:getVersion"_loc, { req.spanContext });
	state std::map<UID, CommitProxyVersionReplies>::iterator proxyItr =
	    self->lastCommitProxyVersionReplies.find(req.requestingProxy); // lastCommitProxyVersionReplies never changes

	++self->getCommitVersionRequests;

	if (proxyItr == self->lastCommitProxyVersionReplies.end()) {
		// Request from invalid proxy (e.g. from duplicate recruitment request)
		req.reply.send(Never());
		return Void();
	}

	TEST(proxyItr->second.latestRequestNum.get() < req.requestNum - 1); // Commit version request queued up
	wait(proxyItr->second.latestRequestNum.whenAtLeast(req.requestNum - 1));

	auto itr = proxyItr->second.replies.find(req.requestNum);
	if (itr != proxyItr->second.replies.end()) {
		TEST(true); // Duplicate request for sequence
		req.reply.send(itr->second);
	} else if (req.requestNum <= proxyItr->second.latestRequestNum.get()) {
		TEST(true); // Old request for previously acknowledged sequence - may be impossible with current FlowTransport
		ASSERT(req.requestNum <
		       proxyItr->second.latestRequestNum.get()); // The latest request can never be acknowledged
		req.reply.send(Never());
	} else {
		GetCommitVersionReply rep;

		if (self->version == invalidVersion) {
			self->lastVersionTime = now();
			self->version = self->recoveryTransactionVersion;
			rep.prevVersion = self->lastEpochEnd;
		} else {
			double t1 = now();
			if (BUGGIFY) {
				t1 = self->lastVersionTime;
			}
			rep.prevVersion = self->version;
			self->version +=
			    std::max<Version>(1,
			                      std::min<Version>(SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS,
			                                        SERVER_KNOBS->VERSIONS_PER_SECOND * (t1 - self->lastVersionTime)));

			TEST(self->version - rep.prevVersion == 1); // Minimum possible version gap

			bool maxVersionGap = self->version - rep.prevVersion == SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS;
			TEST(maxVersionGap); // Maximum possible version gap
			self->lastVersionTime = t1;

			if (self->resolverNeedingChanges.count(req.requestingProxy)) {
				rep.resolverChanges = self->resolverChanges.get();
				rep.resolverChangesVersion = self->resolverChangesVersion;
				self->resolverNeedingChanges.erase(req.requestingProxy);

				TEST(!rep.resolverChanges.empty()); // resolution balancing moves keyranges
				if (self->resolverNeedingChanges.empty())
					self->resolverChanges.set(Standalone<VectorRef<ResolverMoveRef>>());
			}
		}

		rep.version = self->version;
		rep.requestNum = req.requestNum;

		proxyItr->second.replies.erase(proxyItr->second.replies.begin(),
		                               proxyItr->second.replies.upper_bound(req.mostRecentProcessedRequestNum));
		proxyItr->second.replies[req.requestNum] = rep;
		ASSERT(rep.prevVersion >= 0);
		req.reply.send(rep);

		ASSERT(proxyItr->second.latestRequestNum.get() == req.requestNum - 1);
		proxyItr->second.latestRequestNum.set(req.requestNum);
	}

	return Void();
}

ACTOR Future<Void> provideVersions(Reference<MasterData> self) {
	state ActorCollection versionActors(false);

	for (auto& p : self->commitProxies)
		self->lastCommitProxyVersionReplies[p.id()] = CommitProxyVersionReplies();

	loop {
		choose {
			when(GetCommitVersionRequest req = waitNext(self->myInterface.getCommitVersion.getFuture())) {
				versionActors.add(getVersion(self, req));
			}
			when(wait(versionActors.getResult())) {}
		}
	}
}

ACTOR Future<Void> serveLiveCommittedVersion(Reference<MasterData> self) {
	loop {
		choose {
			when(GetRawCommittedVersionRequest req = waitNext(self->myInterface.getLiveCommittedVersion.getFuture())) {
				if (req.debugID.present())
					g_traceBatch.addEvent("TransactionDebug",
					                      req.debugID.get().first(),
					                      "MasterServer.serveLiveCommittedVersion.GetRawCommittedVersion");

				if (self->liveCommittedVersion == invalidVersion) {
					self->liveCommittedVersion = self->recoveryTransactionVersion;
				}
				++self->getLiveCommittedVersionRequests;
				GetRawCommittedVersionReply reply;
				reply.version = self->liveCommittedVersion;
				reply.locked = self->databaseLocked;
				reply.metadataVersion = self->proxyMetadataVersion;
				reply.minKnownCommittedVersion = self->minKnownCommittedVersion;
				req.reply.send(reply);
			}
			when(ReportRawCommittedVersionRequest req =
			         waitNext(self->myInterface.reportLiveCommittedVersion.getFuture())) {
				self->minKnownCommittedVersion = std::max(self->minKnownCommittedVersion, req.minKnownCommittedVersion);
				if (req.version > self->liveCommittedVersion) {
					auto curTime = now();
					// add debug here to change liveCommittedVersion to time bound of now()
					debug_advanceVersionTimestamp(self->liveCommittedVersion,
					                              curTime + CLIENT_KNOBS->MAX_VERSION_CACHE_LAG);
					// also add req.version but with no time bound
					debug_advanceVersionTimestamp(req.version, std::numeric_limits<double>::max());
					self->liveCommittedVersion = req.version;
					self->databaseLocked = req.locked;
					self->proxyMetadataVersion = req.metadataVersion;
				}
				++self->reportLiveCommittedVersionRequests;
				req.reply.send(Void());
			}
		}
	}
}

ACTOR Future<Void> updateRecoveryData(Reference<MasterData> self) {
	loop {
		UpdateRecoveryDataRequest req = waitNext(self->myInterface.updateRecoveryData.getFuture());
		TraceEvent("UpdateRecoveryData", self->dbgid)
		    .detail("RecoveryTxnVersion", req.recoveryTransactionVersion)
		    .detail("LastEpochEnd", req.lastEpochEnd)
		    .detail("NumCommitProxies", req.commitProxies.size());

		if (self->recoveryTransactionVersion == invalidVersion ||
		    req.recoveryTransactionVersion > self->recoveryTransactionVersion) {
			self->recoveryTransactionVersion = req.recoveryTransactionVersion;
		}
		if (self->lastEpochEnd == invalidVersion || req.lastEpochEnd > self->lastEpochEnd) {
			self->lastEpochEnd = req.lastEpochEnd;
		}
		if (req.commitProxies.size() > 0) {
			self->commitProxies = req.commitProxies;
			self->lastCommitProxyVersionReplies.clear();

			for (auto& p : self->commitProxies) {
				self->lastCommitProxyVersionReplies[p.id()] = CommitProxyVersionReplies();
			}
		}

		self->resolvers = req.resolvers;
		if (req.resolvers.size() > 1)
			self->triggerResolution.trigger();

		req.reply.send(Void());
	}
}

static std::set<int> const& normalMasterErrors() {
	static std::set<int> s;
	if (s.empty()) {
		s.insert(error_code_tlog_stopped);
		s.insert(error_code_tlog_failed);
		s.insert(error_code_commit_proxy_failed);
		s.insert(error_code_grv_proxy_failed);
		s.insert(error_code_resolver_failed);
		s.insert(error_code_backup_worker_failed);
		s.insert(error_code_recruitment_failed);
		s.insert(error_code_no_more_servers);
		s.insert(error_code_cluster_recovery_failed);
		s.insert(error_code_coordinated_state_conflict);
		s.insert(error_code_master_max_versions_in_flight);
		s.insert(error_code_worker_removed);
		s.insert(error_code_new_coordinators_timed_out);
		s.insert(error_code_broken_promise);
	}
	return s;
}

ACTOR Future<Void> masterServer(MasterInterface mi,
                                Reference<AsyncVar<ServerDBInfo> const> db,
                                Reference<AsyncVar<Optional<ClusterControllerFullInterface>> const> ccInterface,
                                ServerCoordinators coordinators,
                                LifetimeToken lifetime,
                                bool forceRecovery) {
	state Future<Void> ccTimeout = delay(SERVER_KNOBS->CC_INTERFACE_TIMEOUT);
	while (!ccInterface->get().present() || db->get().clusterInterface != ccInterface->get().get()) {
		wait(ccInterface->onChange() || db->onChange() || ccTimeout);
		if (ccTimeout.isReady()) {
			TraceEvent("MasterTerminated", mi.id())
			    .detail("Reason", "Timeout")
			    .detail("CCInterface", ccInterface->get().present() ? ccInterface->get().get().id() : UID())
			    .detail("DBInfoInterface", db->get().clusterInterface.id());
			return Void();
		}
	}

	state Future<Void> onDBChange = Void();
	state PromiseStream<Future<Void>> addActor;
	state Reference<MasterData> self(
	    new MasterData(db, mi, coordinators, db->get().clusterInterface, LiteralStringRef(""), forceRecovery));
	state Future<Void> collection = actorCollection(addActor.getFuture());

	addActor.send(traceRole(Role::MASTER, mi.id()));
	addActor.send(provideVersions(self));
	addActor.send(serveLiveCommittedVersion(self));
	addActor.send(updateRecoveryData(self));
	addActor.send(resolutionBalancing(self));

	TEST(!lifetime.isStillValid(db->get().masterLifetime, mi.id() == db->get().master.id())); // Master born doomed
	TraceEvent("MasterLifetime", self->dbgid).detail("LifetimeToken", lifetime.toString());

	try {
		loop choose {
			when(wait(onDBChange)) {
				onDBChange = db->onChange();
				if (!lifetime.isStillValid(db->get().masterLifetime, mi.id() == db->get().master.id())) {
					TraceEvent("MasterTerminated", mi.id())
					    .detail("Reason", "LifetimeToken")
					    .detail("MyToken", lifetime.toString())
					    .detail("CurrentToken", db->get().masterLifetime.toString());
					TEST(true); // Master replaced, dying
					if (BUGGIFY)
						wait(delay(5));
					throw worker_removed();
				}
			}
			when(wait(collection)) {
				ASSERT(false);
				throw internal_error();
			}
		}
	} catch (Error& e) {
		state Error err = e;
		if (e.code() != error_code_actor_cancelled) {
			wait(delay(0.0));
		}
		while (!addActor.isEmpty()) {
			addActor.getFuture().pop();
		}

		TEST(err.code() == error_code_tlog_failed); // Master: terminated due to tLog failure
		TEST(err.code() == error_code_commit_proxy_failed); // Master: terminated due to commit proxy failure
		TEST(err.code() == error_code_grv_proxy_failed); // Master: terminated due to GRV proxy failure
		TEST(err.code() == error_code_resolver_failed); // Master: terminated due to resolver failure
		TEST(err.code() == error_code_backup_worker_failed); // Master: terminated due to backup worker failure

		if (normalMasterErrors().count(err.code())) {
			TraceEvent("MasterTerminated", mi.id()).error(err);
			return Void();
		}
		throw err;
	}
}
