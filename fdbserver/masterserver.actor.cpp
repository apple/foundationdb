/*
 * masterserver.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include <algorithm>
#include <iterator>

#include "fdbrpc/sim_validation.h"
#include "fdbserver/CoordinatedState.h"
#include "fdbserver/CoordinationInterface.h" // copy constructors for ServerCoordinators class
#include "fdbserver/Knobs.h"
#include "fdbserver/MasterInterface.h"
#include "fdbserver/ResolutionBalancer.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "flow/ActorCollection.h"
#include "flow/Trace.h"
#include "flow/swift_support.h"
#include "fdbclient/VersionVector.h"
#include "fdbserver/MasterData.actor.h"

#ifdef WITH_SWIFT
#include "SwiftModules/FDBServer"
#endif

#include "flow/actorcompiler.h" // This must be the last #include.

// Instantiate MasterInterface related templates
template class ReplyPromise<MasterInterface>;
template struct NetSAV<MasterInterface>;

// Instantiate ServerDBInfo related templates
template class GetEncryptCipherKeys<struct ServerDBInfo>;

void updateLiveCommittedVersion(Reference<MasterData> self, ReportRawCommittedVersionRequest req);

Version figureVersionCxx(Version current,
                         double now,
                         Version reference,
                         int64_t toAdd,
                         double maxVersionRateModifier,
                         int64_t maxVersionRateOffset) {
	// Versions should roughly follow wall-clock time, based on the
	// system clock of the current machine and an FDB-specific epoch.
	// Calculate the expected version and determine whether we need to
	// hand out versions faster or slower to stay in sync with the
	// clock.
	Version expected = now * SERVER_KNOBS->VERSIONS_PER_SECOND - reference;

	// Attempt to jump directly to the expected version. But make
	// sure that versions are still being handed out at a rate
	// around VERSIONS_PER_SECOND. This rate is scaled depending on
	// how far off the calculated version is from the expected
	// version.
	int64_t maxOffset = std::min(static_cast<int64_t>(toAdd * maxVersionRateModifier), maxVersionRateOffset);
	return std::clamp(expected, current + toAdd - maxOffset, current + toAdd + maxOffset);
}

#ifdef WITH_SWIFT
Version figureVersion(Version current,
                      double now,
                      Version reference,
                      int64_t toAdd,
                      double maxVersionRateModifier,
                      int64_t maxVersionRateOffset) {
	auto impl = SERVER_KNOBS->FLOW_WITH_SWIFT ? fdbserver_swift::figureVersion : figureVersionCxx;
	return impl(current, now, reference, toAdd, maxVersionRateModifier, maxVersionRateOffset);
}
#else
Version figureVersion(Version current,
                      double now,
                      Version reference,
                      int64_t toAdd,
                      double maxVersionRateModifier,
                      int64_t maxVersionRateOffset) {
	return figureVersionCxx(current, now, reference, toAdd, maxVersionRateModifier, maxVersionRateOffset);
}
#endif

#ifdef WITH_SWIFT
SWIFT_ACTOR Future<Void> waitForPrev(Reference<MasterData> self, ReportRawCommittedVersionRequest req) {
	if (SERVER_KNOBS->FLOW_WITH_SWIFT) {
		auto future = self->swiftImpl->waitForPrev(self.getPtr(), req);
		wait(future);
	} else {
		state double startTime = now();
		wait(self->liveCommittedVersion.whenAtLeast(req.prevVersion.get()));
		double latency = now() - startTime;
		self->waitForPrevLatencies->addMeasurement(latency);
		++self->waitForPrevCommitRequests;
		updateLiveCommittedVersion(self, req);
		req.reply.send(Void());
	}

	return Void();
}
#else
ACTOR Future<Void> waitForPrev(Reference<MasterData> self, ReportRawCommittedVersionRequest req) {
	state double startTime = now();
	wait(self->liveCommittedVersion.whenAtLeast(req.prevVersion.get()));
	self->waitForPrevLatencies->addMeasurement(now() - startTime);
	++self->waitForPrevCommitRequests;
	updateLiveCommittedVersion(self, req);
	req.reply.send(Void());

	return Void();
}
#endif

#ifdef WITH_SWIFT
SWIFT_ACTOR Future<Void> getVersionSwift(Reference<MasterData> self, GetCommitVersionRequest req) {
	auto future = self->swiftImpl->getVersion(self.getPtr(), req);
	wait(future);
	return Void();
}
#endif

ACTOR Future<Void> getVersionCxx(Reference<MasterData> self, GetCommitVersionRequest req) {
	state Span span("M:getVersion"_loc, req.spanContext);
	state std::map<UID, CommitProxyVersionReplies>::iterator proxyItr =
	    self->lastCommitProxyVersionReplies.find(req.requestingProxy); // lastCommitProxyVersionReplies never changes

	++self->getCommitVersionRequests;

	if (proxyItr == self->lastCommitProxyVersionReplies.end()) {
		// Request from invalid proxy (e.g. from duplicate recruitment request)
		req.reply.send(Never());
		return Void();
	}

	CODE_PROBE(proxyItr->second.latestRequestNum.get() < req.requestNum - 1,
	           "Commit version request queued up",
	           probe::decoration::rare);
	wait(proxyItr->second.latestRequestNum.whenAtLeast(req.requestNum - 1));

	auto itr = proxyItr->second.replies.find(req.requestNum);
	if (itr != proxyItr->second.replies.end()) {
		CODE_PROBE(true, "Duplicate request for sequence");
		req.reply.send(itr->second);
	} else if (req.requestNum <= proxyItr->second.latestRequestNum.get()) {
		CODE_PROBE(true,
		           "Old request for previously acknowledged sequence - may be impossible with current FlowTransport",
		           probe::decoration::rare);
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

			Version toAdd =
			    std::max<Version>(1,
			                      std::min<Version>(SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS,
			                                        SERVER_KNOBS->VERSIONS_PER_SECOND * (t1 - self->lastVersionTime)));

			rep.prevVersion = self->version;
			if (self->referenceVersion.present()) {
				self->version = figureVersion(self->version,
				                              g_network->timer(),
				                              self->referenceVersion.get(),
				                              toAdd,
				                              SERVER_KNOBS->MAX_VERSION_RATE_MODIFIER,
				                              SERVER_KNOBS->MAX_VERSION_RATE_OFFSET);
				ASSERT_GT(self->version, rep.prevVersion);
			} else {
				self->version = self->version + toAdd;
			}

			CODE_PROBE(self->version - rep.prevVersion == 1, "Minimum possible version gap");

			bool maxVersionGap = self->version - rep.prevVersion == SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS;
			CODE_PROBE(maxVersionGap, "Maximum possible version gap");
			self->lastVersionTime = t1;

			self->resolutionBalancer.setChangesInReply(req.requestingProxy, rep);
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

#ifdef WITH_SWIFT
ACTOR Future<Void> getVersion(Reference<MasterData> self, GetCommitVersionRequest req) {
	if (SERVER_KNOBS->FLOW_WITH_SWIFT) {
		wait(getVersionSwift(self, req));
		return Void();
	} else {
		wait(getVersionCxx(self, req));
		return Void();
	}
}
#else
ACTOR Future<Void> getVersion(Reference<MasterData> self, GetCommitVersionRequest req) {
	wait(getVersionCxx(self, req));
	return Void();
}
#endif

CounterValue::CounterValue(std::string const& name, CounterCollection& collection)
  : value(std::make_shared<Counter>(name, collection)) {}

void CounterValue::operator+=(Value delta) {
	value->operator+=(delta);
}

void CounterValue::operator++() {
	value->operator++();
}
void CounterValue::clear() {
	value->clear();
}

MasterData::MasterData(Reference<AsyncVar<ServerDBInfo> const> const& dbInfo,
                       MasterInterface const& myInterface,
                       ServerCoordinators const& coordinators,
                       ClusterControllerFullInterface const& clusterController,
                       Standalone<StringRef> const& dbId,
                       PromiseStream<Future<Void>> addActor,
                       bool forceRecovery)
  : dbgid(myInterface.id()), lastEpochEnd(invalidVersion), recoveryTransactionVersion(invalidVersion),
    liveCommittedVersion(invalidVersion), databaseLocked(false), minKnownCommittedVersion(invalidVersion),
    coordinators(coordinators), version(invalidVersion), lastVersionTime(0), myInterface(myInterface),
    resolutionBalancer(&version), forceRecovery(forceRecovery), cc("Master", dbgid.toString()),
    getCommitVersionRequests("GetCommitVersionRequests", cc),
    getLiveCommittedVersionRequests("GetLiveCommittedVersionRequests", cc),
    reportLiveCommittedVersionRequests("ReportLiveCommittedVersionRequests", cc),
    waitForPrevCommitRequests("WaitForPrevCommitRequests", cc),
    nonWaitForPrevCommitRequests("NonWaitForPrevCommitRequests", cc), addActor(addActor) {
	logger = cc.traceCounters("MasterMetrics", dbgid, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, "MasterMetrics");
	if (forceRecovery && !myInterface.locality.dcId().present()) {
		TraceEvent(SevError, "ForcedRecoveryRequiresDcID").log();
		forceRecovery = false;
	}
	balancer = resolutionBalancer.resolutionBalancing();
	locality = tagLocalityInvalid;

	if (SERVER_KNOBS->ENABLE_VERSION_VECTOR) {
		versionVectorTagUpdates = std::make_unique<LatencySample>("VersionVectorTagUpdates",
		                                                          dbgid,
		                                                          SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
		                                                          SERVER_KNOBS->LATENCY_SKETCH_ACCURACY);
		versionVectorSizeOnCVReply = std::make_unique<LatencySample>("VersionVectorSizeOnCVReply",
		                                                             dbgid,
		                                                             SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
		                                                             SERVER_KNOBS->LATENCY_SKETCH_ACCURACY);
		waitForPrevLatencies = std::make_unique<LatencySample>("WaitForPrevLatencies",
		                                                       dbgid,
		                                                       SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
		                                                       SERVER_KNOBS->LATENCY_SKETCH_ACCURACY);
	}

#ifdef WITH_SWIFT
	using namespace fdbserver_swift;
	// FIXME(swift): can we make a cleaner init?
	swiftImpl.reset(new MasterDataActor((const MasterDataActor&)MasterDataActor::init()));
#endif
}

#ifdef WITH_SWIFT
void MasterData::setSwiftImpl(fdbserver_swift::MasterDataActor* impl) {
	swiftImpl.reset(impl);
}
#endif

MasterData::~MasterData() {}

ACTOR Future<Void> provideVersionsCxx(Reference<MasterData> self) {
	state ActorCollection versionActors(false);

	loop choose {
		when(GetCommitVersionRequest req = waitNext(self->myInterface.getCommitVersion.getFuture())) {
			versionActors.add(getVersion(self, req));
		}
		when(wait(versionActors.getResult())) {}
	}
}

#ifdef WITH_SWIFT
SWIFT_ACTOR Future<Void> provideVersionsSwift(Reference<MasterData> self) {
	auto future = self->swiftImpl->provideVersions(self.getPtr());
	wait(future);
	return Void();
}
#endif

#ifdef WITH_SWIFT
ACTOR Future<Void> provideVersions(Reference<MasterData> self) {
	if (SERVER_KNOBS->FLOW_WITH_SWIFT) {
		wait(provideVersionsSwift(self));
	} else {
		wait(provideVersionsCxx(self));
	}

	return Void();
}
#else
ACTOR Future<Void> provideVersions(Reference<MasterData> self) {
	wait(provideVersionsCxx(self));
	return Void();
}
#endif

#ifdef WITH_SWIFT
void updateLiveCommittedVersionSwift(Reference<MasterData> self, ReportRawCommittedVersionRequest req) {
	fdbserver_swift::updateLiveCommittedVersion(self.getPtr(), req);
}
#endif

void updateLiveCommittedVersionCxx(Reference<MasterData> self, ReportRawCommittedVersionRequest req) {
	self->minKnownCommittedVersion = std::max(self->minKnownCommittedVersion, req.minKnownCommittedVersion);

	if (req.version > self->liveCommittedVersion.get()) {
		if (SERVER_KNOBS->ENABLE_VERSION_VECTOR && req.writtenTags.present()) {
			// TraceEvent("Received ReportRawCommittedVersionRequest").detail("Version",req.version);
			int8_t primaryLocality =
			    SERVER_KNOBS->ENABLE_VERSION_VECTOR_HA_OPTIMIZATION ? self->locality : tagLocalityInvalid;
			self->ssVersionVector.setVersion(req.writtenTags.get(), req.version, primaryLocality);
			self->versionVectorTagUpdates->addMeasurement(req.writtenTags.get().size());
		}
		auto curTime = now();
		// add debug here to change liveCommittedVersion to time bound of now()
		debug_advanceVersionTimestamp(self->liveCommittedVersion.get(), curTime + CLIENT_KNOBS->MAX_VERSION_CACHE_LAG);
		// also add req.version but with no time bound
		debug_advanceVersionTimestamp(req.version, std::numeric_limits<double>::max());
		self->databaseLocked = req.locked;
		self->proxyMetadataVersion = req.metadataVersion;
		// Note the set call switches context to any waiters on liveCommittedVersion before continuing.
		self->liveCommittedVersion.set(req.version);
	}
	++self->reportLiveCommittedVersionRequests;
}

#ifdef WITH_SWIFT
void updateLiveCommittedVersion(Reference<MasterData> self, ReportRawCommittedVersionRequest req) {
	if (SERVER_KNOBS->FLOW_WITH_SWIFT) {
		return updateLiveCommittedVersionSwift(self, req);
	} else {
		return updateLiveCommittedVersionCxx(self, req);
	}
}
#else
void updateLiveCommittedVersion(Reference<MasterData> self, ReportRawCommittedVersionRequest req) {
	return updateLiveCommittedVersionCxx(self, req);
}
#endif

#ifdef WITH_SWIFT
SWIFT_ACTOR Future<Void> serveLiveCommittedVersionSwift(Reference<MasterData> self) {
	auto future = self->swiftImpl->serveLiveCommittedVersion(self.getPtr());
	wait(future);
	return Void();
}
#endif

ACTOR Future<Void> serveLiveCommittedVersionCxx(Reference<MasterData> self) {
	loop {
		choose {
			when(GetRawCommittedVersionRequest req = waitNext(self->myInterface.getLiveCommittedVersion.getFuture())) {
				if (req.debugID.present())
					g_traceBatch.addEvent("TransactionDebug",
					                      req.debugID.get().first(),
					                      "MasterServer.serveLiveCommittedVersion.GetRawCommittedVersion");

				if (self->liveCommittedVersion.get() == invalidVersion) {
					self->liveCommittedVersion.set(self->recoveryTransactionVersion);
				}
				++self->getLiveCommittedVersionRequests;
				GetRawCommittedVersionReply reply;
				reply.version = self->liveCommittedVersion.get();
				reply.locked = self->databaseLocked;
				reply.metadataVersion = self->proxyMetadataVersion;
				reply.minKnownCommittedVersion = self->minKnownCommittedVersion;
				if (SERVER_KNOBS->ENABLE_VERSION_VECTOR) {
					self->ssVersionVector.getDelta(req.maxVersion, reply.ssVersionVectorDelta);
					self->versionVectorSizeOnCVReply->addMeasurement(reply.ssVersionVectorDelta.size());
				}
				req.reply.send(reply);
			}
			when(ReportRawCommittedVersionRequest req =
			         waitNext(self->myInterface.reportLiveCommittedVersion.getFuture())) {
				if (SERVER_KNOBS->ENABLE_VERSION_VECTOR && req.prevVersion.present() &&
				    (self->liveCommittedVersion.get() != invalidVersion) &&
				    (self->liveCommittedVersion.get() < req.prevVersion.get())) {
					self->addActor.send(waitForPrev(self, req));
				} else {
					updateLiveCommittedVersion(self, req);
					++self->nonWaitForPrevCommitRequests;
					req.reply.send(Void());
				}
			}
		}
	}
}

#ifdef WITH_SWIFT
ACTOR Future<Void> serveLiveCommittedVersion(Reference<MasterData> self) {
	if (SERVER_KNOBS->FLOW_WITH_SWIFT) {
		wait(serveLiveCommittedVersionSwift(self));
	} else {
		wait(serveLiveCommittedVersionCxx(self));
	}
	return Void();
}
#else
ACTOR Future<Void> serveLiveCommittedVersion(Reference<MasterData> self) {
	wait(serveLiveCommittedVersionCxx(self));
	return Void();
}
#endif

#ifdef WITH_SWIFT
SWIFT_ACTOR Future<Void> updateRecoveryDataSwift(Reference<MasterData> self) {
	auto future = self->swiftImpl->serveUpdateRecoveryData(self.getPtr());
	wait(future);
	return Void();
}
#endif

ACTOR Future<Void> updateRecoveryDataCxx(Reference<MasterData> self) {
	loop {
		UpdateRecoveryDataRequest req = waitNext(self->myInterface.updateRecoveryData.getFuture());
		TraceEvent("UpdateRecoveryData", self->dbgid)
		    .detail("ReceivedRecoveryTxnVersion", req.recoveryTransactionVersion)
		    .detail("ReceivedLastEpochEnd", req.lastEpochEnd)
		    .detail("CurrentRecoveryTxnVersion", self->recoveryTransactionVersion)
		    .detail("CurrentLastEpochEnd", self->lastEpochEnd)
		    .detail("NumCommitProxies", req.commitProxies.size())
		    .detail("VersionEpoch", req.versionEpoch)
		    .detail("PrimaryLocality", req.primaryLocality);

		self->recoveryTransactionVersion = req.recoveryTransactionVersion;
		self->lastEpochEnd = req.lastEpochEnd;

		if (req.commitProxies.size() > 0) {
			self->lastCommitProxyVersionReplies.clear();

			for (auto& p : req.commitProxies) {
				self->lastCommitProxyVersionReplies[p.id()] = CommitProxyVersionReplies();
			}
		}
		if (req.versionEpoch.present()) {
			self->referenceVersion = req.versionEpoch.get();
		} else if (BUGGIFY) {
			// Cannot use a positive version epoch in simulation because of the
			// clock starting at 0. A positive version epoch would mean the initial
			// cluster version was negative.
			// TODO: Increase the size of this interval after fixing the issue
			// with restoring ranges with large version gaps.
			self->referenceVersion = deterministicRandom()->randomInt64(-1e6, 0);
		}

		self->resolutionBalancer.setCommitProxies(req.commitProxies);
		self->resolutionBalancer.setResolvers(req.resolvers);

		self->locality = req.primaryLocality;

		req.reply.send(Void());
	}
}

#ifdef WITH_SWIFT
ACTOR Future<Void> updateRecoveryData(Reference<MasterData> self) {
	if (SERVER_KNOBS->FLOW_WITH_SWIFT) {
		wait(updateRecoveryDataSwift(self));
	} else {
		wait(updateRecoveryDataCxx(self));
	}
	return Void();
}
#else
ACTOR Future<Void> updateRecoveryData(Reference<MasterData> self) {
	wait(updateRecoveryDataCxx(self));
	return Void();
}
#endif

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

ACTOR Future<Void> masterServerCxx(MasterInterface mi,
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
	wait(onDBChange);
	state PromiseStream<Future<Void>> addActor;
	state Reference<MasterData> self(
	    new MasterData(db, mi, coordinators, db->get().clusterInterface, ""_sr, addActor, forceRecovery));
	state Future<Void> collection = actorCollection(addActor.getFuture());

	addActor.send(traceRole(Role::MASTER, mi.id()));
	addActor.send(provideVersions(self));
	addActor.send(serveLiveCommittedVersion(self));
	addActor.send(updateRecoveryDataCxx(self));

	CODE_PROBE(!lifetime.isStillValid(db->get().masterLifetime, mi.id() == db->get().master.id()),
	           "Master born doomed");
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
					CODE_PROBE(true, "Master replaced, dying");
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

		CODE_PROBE(
		    err.code() == error_code_tlog_failed, "Master: terminated due to tLog failure", probe::decoration::rare);
		CODE_PROBE(err.code() == error_code_commit_proxy_failed,
		           "Master: terminated due to commit proxy failure",
		           probe::decoration::rare);
		CODE_PROBE(err.code() == error_code_grv_proxy_failed,
		           "Master: terminated due to GRV proxy failure",
		           probe::decoration::rare);
		CODE_PROBE(err.code() == error_code_resolver_failed,
		           "Master: terminated due to resolver failure",
		           probe::decoration::rare);
		CODE_PROBE(err.code() == error_code_backup_worker_failed,
		           "Master: terminated due to backup worker failure",
		           probe::decoration::rare);

		if (normalMasterErrors().contains(err.code())) {
			TraceEvent("MasterTerminated", mi.id()).error(err);
			return Void();
		}
		throw err;
	}
}

#ifdef WITH_SWIFT
ACTOR Future<Void> masterServerImpl(MasterInterface mi,
                                    Reference<AsyncVar<ServerDBInfo> const> db,
                                    Reference<AsyncVar<Optional<ClusterControllerFullInterface>> const> ccInterface,
                                    ServerCoordinators coordinators,
                                    LifetimeToken lifetime,
                                    bool forceRecovery) {
	if (SERVER_KNOBS->FLOW_WITH_SWIFT) {
		auto promise = Promise<Void>();
		state PromiseStream<Future<Void>> addActor;
		state Reference<MasterData> self(
		    new MasterData(db, mi, coordinators, db->get().clusterInterface, ""_sr, addActor, forceRecovery));
		fdbserver_swift::masterServerSwift(
		    mi,
		    const_cast<AsyncVar<ServerDBInfo>*>(db.getPtr()),
		    const_cast<AsyncVar<Optional<ClusterControllerFullInterface>>*>(ccInterface.getPtr()),
		    coordinators,
		    lifetime,
		    forceRecovery,
		    self.getPtr(),
		    /*result=*/promise);
		Future<Void> f = promise.getFuture();
		wait(f);
		return Void();
	} else {
		wait(masterServerCxx(mi, db, ccInterface, coordinators, lifetime, forceRecovery));
		return Void();
	}
}
#else
ACTOR Future<Void> masterServerImpl(MasterInterface mi,
                                    Reference<AsyncVar<ServerDBInfo> const> db,
                                    Reference<AsyncVar<Optional<ClusterControllerFullInterface>> const> ccInterface,
                                    ServerCoordinators coordinators,
                                    LifetimeToken lifetime,
                                    bool forceRecovery) {
	wait(masterServerCxx(mi, db, ccInterface, coordinators, lifetime, forceRecovery));
	return Void();
}
#endif

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
	wait(onDBChange);
	wait(masterServerImpl(mi, db, ccInterface, coordinators, lifetime, forceRecovery));
	return Void();
}

TEST_CASE("/fdbserver/MasterServer/FigureVersion/Simple") {
	ASSERT_EQ(
	    figureVersion(0, 1.0, 0, 1e6, SERVER_KNOBS->MAX_VERSION_RATE_MODIFIER, SERVER_KNOBS->MAX_VERSION_RATE_OFFSET),
	    1e6);
	ASSERT_EQ(figureVersion(1e6, 1.5, 0, 100, 0.1, 1e6), 1000110);
	ASSERT_EQ(figureVersion(1e6, 1.5, 0, 550000, 0.1, 1e6), 1500000);
	return Void();
}

TEST_CASE("/fdbserver/MasterServer/FigureVersion/Small") {
	// Should always advance by at least 1 version.
	ASSERT_EQ(figureVersion(1e6, 2.0, 0, 1, 0.0001, 1e6), 1000001);
	ASSERT_EQ(figureVersion(1e6, 0.0, 0, 1, 0.1, 1e6), 1000001);
	return Void();
}

TEST_CASE("/fdbserver/MasterServer/FigureVersion/MaxOffset") {
	ASSERT_EQ(figureVersion(1e6, 10.0, 0, 5e6, 0.1, 1e6), 6500000);
	ASSERT_EQ(figureVersion(1e6, 20.0, 0, 15e6, 0.1, 1e6), 17e6);
	return Void();
}

TEST_CASE("/fdbserver/MasterServer/FigureVersion/PositiveReferenceVersion") {
	ASSERT_EQ(figureVersion(1e6, 3.0, 1e6, 1e6, 0.1, 1e6), 2e6);
	ASSERT_EQ(figureVersion(1e6, 3.0, 1e6, 100, 0.1, 1e6), 1000110);
	return Void();
}

TEST_CASE("/fdbserver/MasterServer/FigureVersion/NegativeReferenceVersion") {
	ASSERT_EQ(figureVersion(0, 2.0, -1e6, 3e6, 0.1, 1e6), 3e6);
	ASSERT_EQ(figureVersion(0, 2.0, -1e6, 5e5, 0.1, 1e6), 550000);
	return Void();
}
