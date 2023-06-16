/*
 * PaxosConfigConsumer.actor.cpp
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

#include "fdbserver/PaxosConfigConsumer.h"

#include <algorithm>
#include <map>
#include <numeric>

#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h" // This must be the last #include.

using ConfigFollowerInfo = ModelInterface<ConfigFollowerInterface>;

struct CommittedVersions {
	Version secondToLastCommitted;
	Version lastCommitted;
};

struct QuorumVersion {
	CommittedVersions versions;
	bool isQuorum;
};

class GetCommittedVersionQuorum {
	// Set to the <secondToLastCommitted, lastCommitted> versions a quorum of
	// ConfigNodes agree on, otherwise unset.
	Promise<QuorumVersion> quorumVersion;
	std::vector<Future<Void>> actors;
	std::vector<ConfigFollowerInterface> cfis;
	std::map<Version, std::vector<ConfigFollowerInterface>> replies;
	std::map<Version, Version> priorVersions;
	std::map<NetworkAddress, Version> committed;
	// Need to know the largest compacted version on any node to avoid asking
	// for changes that have already been compacted.
	Version largestCompactedResponse{ 0 };
	// Last durably committed version.
	Version lastSeenVersion;
	// Largest compacted version on the existing ConfigNodes.
	Version largestCompacted;
	size_t totalRepliesReceived{ 0 };
	size_t maxAgreement{ 0 };
	// Stores the largest live version out of all the responses.
	Version largestLive{ 0 };
	// Stores the largest committed version out of all responses.
	Version largestCommitted{ 0 };
	bool allowSpecialCaseRollforward_{ false };
	// True if a quorum has zero as their committed version. See explanation
	// comment below.
	bool specialZeroQuorum{ false };

	// Sends rollback/rollforward messages to any nodes that are not up to date
	// with the latest committed version as determined by the quorum. Should
	// only be called after a committed version has been determined.
	ACTOR static Future<Void> updateNode(GetCommittedVersionQuorum* self,
	                                     CommittedVersions nodeVersion,
	                                     CommittedVersions quorumVersion,
	                                     Version lastCompacted,
	                                     ConfigFollowerInterface cfi) {
		state Version target = quorumVersion.lastCommitted;
		// TraceEvent("ConsumerUpdateNodeStart")
		// 		.detail("NodeAddress", cfi.address())
		// 		.detail("Target", target)
		// 		.detail("NodeVersionLastCommitted", nodeVersion.lastCommitted)
		// 		.detail("NodeVersionSecondToLastCommitted", nodeVersion.secondToLastCommitted)
		// 		.detail("QuorumVersionLastCommitted", quorumVersion.lastCommitted)
		// 		.detail("QuorumVersionSecondToLastCommitted", quorumVersion.secondToLastCommitted)
		// 		.detail("LargestCompacted", self->largestCompacted);
		if (nodeVersion.lastCommitted == target) {
			return Void();
		}

		if (nodeVersion.lastCommitted < target) {
			state Optional<Version> rollback;
			if (nodeVersion.lastCommitted > quorumVersion.secondToLastCommitted) {
				// If a non-quorum node has a last committed version less than
				// the last committed version on the quorum, but greater than
				// the second to last committed version on the quorum, it has
				// committed changes the quorum does not agree with. Therefore,
				// it needs to be rolled back before being rolled forward.
				rollback = quorumVersion.secondToLastCommitted;
			} else if (nodeVersion.lastCommitted < quorumVersion.secondToLastCommitted) {
				// On the other hand, if the node is on an older committed
				// version, it's possible the version it is on was never made
				// durable. To be safe, roll it back by one version.
				rollback = std::max(nodeVersion.lastCommitted - 1, self->largestCompacted);
			}

			if (rollback.present()) {
				// When a new ConfigBroadcaster is created, it may not know
				// about the last committed version on the ConfigNodes. If
				// compaction has occurred, this can cause change requests to
				// be sent to nodes asking for version 0 when the node has
				// already compacted that version, causing an error. Make sure
				// the rollback version is at least set to the last compacted
				// version to prevent this issue.
				rollback = std::max(rollback.get(), lastCompacted);
			}

			// Now roll node forward to match the largest committed version of
			// the replies.
			try {
				state std::vector<ConfigFollowerInterface> interfs = self->replies[target];
				std::vector<Future<Void>> fs;
				for (ConfigFollowerInterface& interf : interfs) {
					if (interf.hostname.present()) {
						fs.push_back(tryInitializeRequestStream(
						    &interf.getChanges, interf.hostname.get(), WLTOKEN_CONFIGFOLLOWER_GETCHANGES));
					}
				}
				wait(waitForAll(fs));
				state Reference<ConfigFollowerInfo> quorumCfi(new ConfigFollowerInfo(interfs));
				state Version lastSeenVersion = std::max(
				    rollback.present() ? rollback.get() : nodeVersion.lastCommitted, self->largestCompactedResponse);
				ConfigFollowerGetChangesReply reply =
				    wait(timeoutError(basicLoadBalance(quorumCfi,
				                                       &ConfigFollowerInterface::getChanges,
				                                       ConfigFollowerGetChangesRequest{ lastSeenVersion, target }),
				                      SERVER_KNOBS->GET_COMMITTED_VERSION_TIMEOUT));

				// TraceEvent("ConsumerUpdateNodeSendingRollforward")
				// 		.detail("NodeAddress", cfi.address())
				// 		.detail("RollbackTo", rollback)
				// 		.detail("LastKnownCommitted", nodeVersion.lastCommitted)
				// 		.detail("Target", target)
				// 		.detail("ChangesSize", reply.changes.size())
				// 		.detail("AnnotationsSize", reply.annotations.size())
				// 		.detail("LargestCompacted", self->largestCompactedResponse)
				// 		.detail("SpecialZeroQuorum", self->specialZeroQuorum);
				if (cfi.hostname.present()) {
					wait(timeoutError(
					    retryGetReplyFromHostname(ConfigFollowerRollforwardRequest{ rollback,
					                                                                nodeVersion.lastCommitted,
					                                                                target,
					                                                                reply.changes,
					                                                                reply.annotations,
					                                                                self->specialZeroQuorum },
					                              cfi.hostname.get(),
					                              WLTOKEN_CONFIGFOLLOWER_ROLLFORWARD),
					    SERVER_KNOBS->GET_COMMITTED_VERSION_TIMEOUT));
				} else {
					wait(timeoutError(
					    cfi.rollforward.getReply(ConfigFollowerRollforwardRequest{ rollback,
					                                                               nodeVersion.lastCommitted,
					                                                               target,
					                                                               reply.changes,
					                                                               reply.annotations,
					                                                               self->specialZeroQuorum }),
					    SERVER_KNOBS->GET_COMMITTED_VERSION_TIMEOUT));
				}
			} catch (Error& e) {
				if (e.code() == error_code_transaction_too_old) {
					// Seeing this trace is not necessarily a problem. There
					// are legitimate scenarios where a ConfigNode could return
					// one of these errors in response to a get changes or
					// rollforward request. The retry loop should handle this
					// case.
					TraceEvent(SevInfo, "ConsumerConfigNodeRollforwardError").error(e);
				} else {
					throw;
				}
			}
		}
		return Void();
	}

	ACTOR static Future<Void> getCommittedVersionActor(GetCommittedVersionQuorum* self, ConfigFollowerInterface cfi) {
		try {
			state ConfigFollowerGetCommittedVersionReply reply;
			if (cfi.hostname.present()) {
				wait(timeoutError(store(reply,
				                        retryGetReplyFromHostname(ConfigFollowerGetCommittedVersionRequest{},
				                                                  cfi.hostname.get(),
				                                                  WLTOKEN_CONFIGFOLLOWER_GETCOMMITTEDVERSION)),
				                  SERVER_KNOBS->GET_COMMITTED_VERSION_TIMEOUT));
			} else {
				wait(timeoutError(
				    store(reply, cfi.getCommittedVersion.getReply(ConfigFollowerGetCommittedVersionRequest{})),
				    SERVER_KNOBS->GET_COMMITTED_VERSION_TIMEOUT));
			}

			if (!reply.registered) {
				// ConfigNodes serve their GetCommittedVersion interface before
				// being registered to allow them to be rolled forward.
				// However, their responses should not count towards the
				// quorum.
				throw future_version();
			}

			++self->totalRepliesReceived;
			self->largestCompactedResponse = std::max(self->largestCompactedResponse, reply.lastCompacted);
			state Version lastCompacted = reply.lastCompacted;
			self->committed[cfi.address()] = reply.lastCommitted;
			self->largestLive = std::max(self->largestLive, reply.lastLive);
			self->largestCommitted = std::max(self->largestCommitted, reply.lastCommitted);
			state CommittedVersions committedVersions = CommittedVersions{ self->lastSeenVersion, reply.lastCommitted };
			if (self->priorVersions.find(committedVersions.lastCommitted) == self->priorVersions.end()) {
				self->priorVersions[committedVersions.lastCommitted] = self->lastSeenVersion;
			}
			auto& nodes = self->replies[committedVersions.lastCommitted];
			nodes.push_back(cfi);
			self->maxAgreement = std::max(nodes.size(), self->maxAgreement);
			// TraceEvent("ConsumerGetCommittedVersionReply")
			// 	.detail("From", cfi.address())
			// 	.detail("LastCompactedVersion", lastCompacted)
			// 	.detail("LastCommittedVersion", reply.lastCommitted)
			// 	.detail("LastSeenVersion", self->lastSeenVersion)
			// 	.detail("Replies", self->totalRepliesReceived)
			// 	.detail("RepliesMatchingVersion", nodes.size())
			// 	.detail("Coordinators", self->cfis.size())
			// 	.detail("AllowSpecialCaseRollforward", self->allowSpecialCaseRollforward_);
			if (nodes.size() >= self->cfis.size() / 2 + 1) {
				// A quorum at version 0 should use any higher committed
				// version seen instead of 0. Imagine the following scenario
				// with three coordinators:
				//
				//      t0   t1   t2  t3
				//   A  1    1    |   1
				//   B  1   dies  |   0
				//   C  0    0    |   0
				//
				// At t0, a value at version 1 is committed to A and B. At t1,
				// B dies, and now the value only exists on A. At t2, a change
				// coordinators command is executed by a client, causing a
				// recovery. When the ConfigBroadcaster comes online and
				// attempts to read the state of the previous coordinators (at
				// time t3) so it can transfer it to the new coordinators, 2/3
				// ConfigNodes are unregistered and only know about version 0.
				// Quorum logic dictates the committed version is, thus,
				// version 0. But we know a majority committed version 1. This
				// is a special case error where a ConfigNode losing data is
				// immediately followed by a coordinator change and recovery,
				// and 0 is a special case. Imagine the following if C instead
				// has had some values committed:
				//
				//      t0   t1   t2   t3  t4
				//   A  1    2    2    |   2
				//   B  1    2   dies  |   0
				//   C  1    1    1    |   1
				//
				// In this case, there is no quorum, and so all nodes would
				// (correctly) be rolled forward to version 2. Since a node
				// losing data is equivalent to saying it has a committed
				// version of 0, we must treat a quorum of nodes at version 0
				// as a special case, and instead use the largest committed
				// version we've seen as the quorum version. This does not
				// affect correctness because version 0 means nothing was
				// committed, so there shouldn't be an issue rolling those
				// nodes forward.
				if (self->allowSpecialCaseRollforward_ && committedVersions.lastCommitted == 0 &&
				    self->largestCommitted > 0) {
					self->specialZeroQuorum = true;
					committedVersions = CommittedVersions{ 0, self->largestCommitted };
				}

				// A quorum of ConfigNodes agree on the latest committed version.
				if (self->quorumVersion.canBeSet()) {
					self->quorumVersion.send(QuorumVersion{ committedVersions, true });
				}
				wait(self->updateNode(
				    self, committedVersions, self->quorumVersion.getFuture().get().versions, lastCompacted, cfi));
			} else if (self->maxAgreement >= self->cfis.size() / 2 + 1) {
				// A quorum of ConfigNodes agree on the latest committed version,
				// but the node we just got a reply from is not one of them. We may
				// need to roll it forward or back.
				QuorumVersion quorumVersion = wait(self->quorumVersion.getFuture());
				ASSERT(committedVersions.lastCommitted != quorumVersion.versions.lastCommitted ||
				       self->specialZeroQuorum);
				wait(self->updateNode(self, committedVersions, quorumVersion.versions, lastCompacted, cfi));
			} else if (self->maxAgreement + (self->cfis.size() - self->totalRepliesReceived) <
			           (self->cfis.size() / 2 + 1)) {
				// It is impossible to reach a quorum of ConfigNodes that agree
				// on the same committed version. This breaks "quorum" logic
				// slightly in that there is no quorum that agrees on a single
				// committed version. So instead we pick the highest committed
				// version among the replies and roll all nodes forward to that
				// version.
				Version largestCommitted = self->replies.rbegin()->first;
				Version largestCommittedPrior = self->priorVersions[largestCommitted];
				if (self->quorumVersion.canBeSet()) {
					self->quorumVersion.send(
					    QuorumVersion{ CommittedVersions{ largestCommittedPrior, largestCommitted }, false });
				}
				wait(self->updateNode(
				    self, committedVersions, self->quorumVersion.getFuture().get().versions, lastCompacted, cfi));
			} else {
				// Still building up responses; don't have enough data to act on
				// yet, so wait until we do.
				QuorumVersion quorumVersion = wait(self->quorumVersion.getFuture());
				wait(self->updateNode(self, committedVersions, quorumVersion.versions, lastCompacted, cfi));
			}
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw;
			}
			// Count a timeout as a reply.
			++self->totalRepliesReceived;
			// TraceEvent("ConsumerGetCommittedVersionError").error(e)
			// 	.detail("From", cfi.address())
			// 	.detail("Replies", self->totalRepliesReceived)
			// 	.detail("Coordinators", self->cfis.size());
			if (e.code() == error_code_version_already_compacted) {
				if (self->quorumVersion.canBeSet()) {
					// Calling sendError could delete self
					auto local = self->quorumVersion;
					local.sendError(e);
				}
			} else if (e.code() != error_code_timed_out && e.code() != error_code_future_version &&
			           e.code() != error_code_broken_promise) {
				if (self->quorumVersion.canBeSet()) {
					// Calling sendError could delete self
					auto local = self->quorumVersion;
					local.sendError(e);
				}
			} else if (self->totalRepliesReceived == self->cfis.size() && self->quorumVersion.canBeSet() &&
			           !self->quorumVersion.isError()) {
				size_t nonTimeoutReplies =
				    std::accumulate(self->replies.begin(), self->replies.end(), 0, [](int value, auto const& p) {
					    return value + p.second.size();
				    });

				if (nonTimeoutReplies >= self->cfis.size() / 2 + 1) {
					// Make sure to trigger the quorumVersion if a timeout
					// occurred, a quorum disagree on the committed version,
					// and there are no more incoming responses. Note that this
					// means that it is impossible to reach a quorum, so send
					// back the largest committed version seen.
					self->quorumVersion.send(
					    QuorumVersion{ CommittedVersions{ self->lastSeenVersion, self->largestCommitted }, false });

					if (e.code() == error_code_future_version) {
						wait(self->updateNode(self,
						                      CommittedVersions{ self->lastSeenVersion, self->largestCommitted },
						                      self->quorumVersion.getFuture().get().versions,
						                      self->largestCompactedResponse,
						                      cfi));
					}
				} else if (!self->quorumVersion.isSet()) {
					// Otherwise, if a quorum agree on the committed version,
					// some other occurred. Notify the caller of it.

					// Calling sendError could delete self
					auto local = self->quorumVersion;
					local.sendError(e);
				}
			}
		}
		return Void();
	}

public:
	explicit GetCommittedVersionQuorum(std::vector<ConfigFollowerInterface> const& cfis,
	                                   Version lastSeenVersion,
	                                   Version largestCompacted)
	  : cfis(cfis), lastSeenVersion(lastSeenVersion), largestCompacted(largestCompacted) {}
	Future<QuorumVersion> getCommittedVersion() {
		ASSERT(!isReady()); // ensures this function is not accidentally called before resetting state
		for (const auto& cfi : cfis) {
			actors.push_back(getCommittedVersionActor(this, cfi));
		}
		return quorumVersion.getFuture();
	}
	bool isReady() const {
		return quorumVersion.getFuture().isValid() && quorumVersion.getFuture().isReady() &&
		       !quorumVersion.getFuture().isError();
	}
	std::vector<ConfigFollowerInterface> getReadReplicas() const {
		if (quorumVersion.getFuture().isError()) {
			throw quorumVersion.getFuture().getError();
		}
		ASSERT(isReady());
		return replies.at(quorumVersion.getFuture().get().versions.lastCommitted);
	}
	Version getLargestLive() const { return largestLive; }
	Version getSmallestCommitted() const {
		if (committed.size() == cfis.size()) {
			Version smallest = MAX_VERSION;
			for (const auto& [key, value] : committed) {
				smallest = std::min(smallest, value);
			}
			return smallest;
		}
		return ::invalidVersion;
	}
	void allowSpecialCaseRollforward() { allowSpecialCaseRollforward_ = true; }
	bool isSpecialZeroQuorum() const { return specialZeroQuorum; }
	Future<Void> complete() const { return waitForAll(actors); }
};

class PaxosConfigConsumerImpl {
	std::vector<ConfigFollowerInterface> cfis;
	GetCommittedVersionQuorum getCommittedVersionQuorum;
	Version lastSeenVersion{ 0 };
	Version compactionVersion{ 0 };
	double pollingInterval;
	Optional<double> compactionInterval;
	bool allowSpecialCaseRollforward_{ false };
	bool readPreviousCoordinators{ false };
	UID id;

	ACTOR static Future<Version> getCommittedVersion(PaxosConfigConsumerImpl* self) {
		if (self->allowSpecialCaseRollforward_) {
			self->getCommittedVersionQuorum.allowSpecialCaseRollforward();
		}
		QuorumVersion quorumVersion = wait(self->getCommittedVersionQuorum.getCommittedVersion());
		if (!quorumVersion.isQuorum) {
			throw failed_to_reach_quorum();
		}
		return quorumVersion.versions.lastCommitted;
	}

	// Periodically compact knob changes on the configuration nodes. All nodes
	// must have received a version before it can be compacted.
	ACTOR static Future<Void> compactor(PaxosConfigConsumerImpl* self, ConfigBroadcaster* broadcaster) {
		if (!self->compactionInterval.present()) {
			wait(Never());
			return Void();
		}
		loop {
			state Version compactionVersion = self->compactionVersion;
			wait(delayJittered(self->compactionInterval.get()));
			std::vector<Future<Void>> compactionRequests;
			compactionRequests.reserve(compactionRequests.size());
			for (const auto& cfi : self->cfis) {
				if (cfi.hostname.present()) {
					compactionRequests.push_back(
					    retryGetReplyFromHostname(ConfigFollowerCompactRequest{ compactionVersion },
					                              cfi.hostname.get(),
					                              WLTOKEN_CONFIGFOLLOWER_COMPACT));
				} else {
					compactionRequests.push_back(
					    cfi.compact.getReply(ConfigFollowerCompactRequest{ compactionVersion }));
				}
			}
			try {
				wait(timeoutError(waitForAll(compactionRequests), 1.0));
				broadcaster->compact(compactionVersion);
			} catch (Error& e) {
				TraceEvent(SevWarn, "ErrorSendingCompactionRequest").error(e);
			}
		}
	}

	ACTOR static Future<Void> getSnapshotAndChanges(PaxosConfigConsumerImpl* self, ConfigBroadcaster* broadcaster) {
		loop {
			self->resetCommittedVersionQuorum(); // TODO: This seems to fix a segfault, investigate more
			try {
				state Version committedVersion = wait(getCommittedVersion(self));
				state std::vector<ConfigFollowerInterface> readReplicas =
				    self->getCommittedVersionQuorum.getReadReplicas();
				std::vector<Future<Void>> fs;
				for (ConfigFollowerInterface& readReplica : readReplicas) {
					if (readReplica.hostname.present()) {
						fs.push_back(tryInitializeRequestStream(&readReplica.getSnapshotAndChanges,
						                                        readReplica.hostname.get(),
						                                        WLTOKEN_CONFIGFOLLOWER_GETSNAPSHOTANDCHANGES));
					}
				}
				wait(waitForAll(fs));
				state Reference<ConfigFollowerInfo> configNodes(new ConfigFollowerInfo(readReplicas));
				ConfigFollowerGetSnapshotAndChangesReply reply =
				    wait(timeoutError(basicLoadBalance(configNodes,
				                                       &ConfigFollowerInterface::getSnapshotAndChanges,
				                                       ConfigFollowerGetSnapshotAndChangesRequest{ committedVersion }),
				                      SERVER_KNOBS->GET_SNAPSHOT_AND_CHANGES_TIMEOUT));
				Version smallestCommitted = self->getCommittedVersionQuorum.getSmallestCommitted();
				TraceEvent(SevDebug, "ConfigConsumerGotSnapshotAndChanges", self->id)
				    .detail("SnapshotVersion", reply.snapshotVersion)
				    .detail("SnapshotSize", reply.snapshot.size())
				    .detail("ChangesVersion", committedVersion)
				    .detail("ChangesSize", reply.changes.size())
				    .detail("AnnotationsSize", reply.annotations.size())
				    .detail("LargestLiveVersion", self->getCommittedVersionQuorum.getLargestLive())
				    .detail("SmallestCommitted", smallestCommitted);
				ASSERT_GE(committedVersion, self->lastSeenVersion);
				self->lastSeenVersion = std::max(self->lastSeenVersion, committedVersion);
				self->compactionVersion = std::max(self->compactionVersion, smallestCommitted);
				broadcaster->applySnapshotAndChanges(std::move(reply.snapshot),
				                                     reply.snapshotVersion,
				                                     reply.changes,
				                                     self->lastSeenVersion,
				                                     reply.annotations,
				                                     self->getCommittedVersionQuorum.getReadReplicas(),
				                                     self->getCommittedVersionQuorum.getLargestLive(),
				                                     self->readPreviousCoordinators);
				wait(self->getCommittedVersionQuorum.complete());
				if (self->allowSpecialCaseRollforward_) {
					self->allowSpecialCaseRollforward_ = false;
				}
				break;
			} catch (Error& e) {
				if (e.code() == error_code_failed_to_reach_quorum) {
					wait(self->getCommittedVersionQuorum.complete());
				} else if (e.code() != error_code_timed_out && e.code() != error_code_broken_promise &&
				           e.code() != error_code_version_already_compacted && e.code() != error_code_process_behind &&
				           e.code() != error_code_future_version) {
					throw;
				}
				wait(delayJittered(0.1));
				self->resetCommittedVersionQuorum();
			}
		}
		return Void();
	}

	ACTOR static Future<Void> fetchChanges(PaxosConfigConsumerImpl* self, ConfigBroadcaster* broadcaster) {
		wait(getSnapshotAndChanges(self, broadcaster));
		self->resetCommittedVersionQuorum();
		loop {
			try {
				state Version committedVersion = wait(getCommittedVersion(self));
				// Because the committed version returned can be a value not
				// accepted by a quorum, it is possible to read a committed
				// version less than the last seen committed version.
				// Specifically, if a new consumer starts and reads a snapshot
				// with ConfigNodes at versions 0, 1, 2, it will return a
				// committed version of 2. Later, if the configuration of the
				// ConfigNodes changes to 1, 1, 2, the committed version
				// returned would be 1.
				if (committedVersion > self->lastSeenVersion) {
					ASSERT(self->getCommittedVersionQuorum.getReadReplicas().size() >= self->cfis.size() / 2 + 1 ||
					       self->getCommittedVersionQuorum.isSpecialZeroQuorum());
					if (BUGGIFY) {
						// Inject a random delay between getting the committed
						// version and reading any changes. The goal is to
						// allow attrition to occasionally kill ConfigNodes in
						// this in-between state.
						wait(delay(deterministicRandom()->random01() * 5));
					}
					state std::vector<ConfigFollowerInterface> readReplicas =
					    self->getCommittedVersionQuorum.getReadReplicas();
					std::vector<Future<Void>> fs;
					for (ConfigFollowerInterface& readReplica : readReplicas) {
						if (readReplica.hostname.present()) {
							fs.push_back(tryInitializeRequestStream(&readReplica.getChanges,
							                                        readReplica.hostname.get(),
							                                        WLTOKEN_CONFIGFOLLOWER_GETCHANGES));
						}
					}
					wait(waitForAll(fs));
					state Reference<ConfigFollowerInfo> configNodes(new ConfigFollowerInfo(readReplicas));
					ConfigFollowerGetChangesReply reply = wait(timeoutError(
					    basicLoadBalance(configNodes,
					                     &ConfigFollowerInterface::getChanges,
					                     ConfigFollowerGetChangesRequest{ self->lastSeenVersion, committedVersion }),
					    SERVER_KNOBS->FETCH_CHANGES_TIMEOUT));
					for (const auto& versionedMutation : reply.changes) {
						TraceEvent te(SevDebug, "ConsumerFetchedMutation", self->id);
						te.detail("Version", versionedMutation.version)
						    .detail("ConfigClass", versionedMutation.mutation.getConfigClass())
						    .detail("KnobName", versionedMutation.mutation.getKnobName());
						if (versionedMutation.mutation.isSet()) {
							te.detail("Op", "Set")
							    .detail("KnobValue", versionedMutation.mutation.getValue().toString());
						} else {
							te.detail("Op", "Clear");
						}
					}
					self->lastSeenVersion = committedVersion;
					Version smallestCommitted = self->getCommittedVersionQuorum.getSmallestCommitted();
					self->compactionVersion = std::max(self->compactionVersion, smallestCommitted);
					broadcaster->applyChanges(reply.changes,
					                          self->lastSeenVersion,
					                          reply.annotations,
					                          self->getCommittedVersionQuorum.getReadReplicas());
				} else if (committedVersion == self->lastSeenVersion) {
					broadcaster->applyChanges({}, -1, {}, self->getCommittedVersionQuorum.getReadReplicas());
				}
				wait(delayJittered(self->pollingInterval));
			} catch (Error& e) {
				if (e.code() == error_code_version_already_compacted || e.code() == error_code_timed_out ||
				    e.code() == error_code_failed_to_reach_quorum || e.code() == error_code_version_already_compacted ||
				    e.code() == error_code_process_behind || e.code() == error_code_future_version) {
					CODE_PROBE(true, "PaxosConfigConsumer fetch error");
					if (e.code() == error_code_failed_to_reach_quorum) {
						try {
							wait(self->getCommittedVersionQuorum.complete());
						} catch (Error& e) {
							if (e.code() == error_code_broken_promise) {
								self->resetCommittedVersionQuorum();
								continue;
							} else {
								throw;
							}
						}
					}
					self->resetCommittedVersionQuorum();
					wait(getSnapshotAndChanges(self, broadcaster));
				} else if (e.code() == error_code_broken_promise) {
					self->resetCommittedVersionQuorum();
					continue;
				} else {
					throw;
				}
			}
			try {
				wait(self->getCommittedVersionQuorum.complete());
			} catch (Error& e) {
				if (e.code() != error_code_broken_promise) {
					throw;
				}
			}
			self->resetCommittedVersionQuorum();
		}
	}

	void resetCommittedVersionQuorum() {
		getCommittedVersionQuorum = GetCommittedVersionQuorum{ cfis, lastSeenVersion, compactionVersion };
	}

public:
	Future<Void> readSnapshot(ConfigBroadcaster& broadcaster) { return getSnapshotAndChanges(this, &broadcaster); }

	Future<Void> consume(ConfigBroadcaster& broadcaster) {
		return fetchChanges(this, &broadcaster) || compactor(this, &broadcaster);
	}

	void allowSpecialCaseRollforward() { this->allowSpecialCaseRollforward_ = true; }

	UID getID() const { return id; }

	PaxosConfigConsumerImpl(std::vector<ConfigFollowerInterface> const& cfis,
	                        double pollingInterval,
	                        Optional<double> compactionInterval,
	                        bool readPreviousCoordinators)
	  : cfis(cfis), getCommittedVersionQuorum(cfis, 0, 0), pollingInterval(pollingInterval),
	    compactionInterval(compactionInterval), readPreviousCoordinators(readPreviousCoordinators),
	    id(deterministicRandom()->randomUniqueID()) {}
};

PaxosConfigConsumer::PaxosConfigConsumer(std::vector<ConfigFollowerInterface> const& cfis,
                                         double pollingInterval,
                                         Optional<double> compactionInterval,
                                         bool readPreviousCoordinators)
  : impl(PImpl<PaxosConfigConsumerImpl>::create(cfis, pollingInterval, compactionInterval, readPreviousCoordinators)) {}

PaxosConfigConsumer::PaxosConfigConsumer(ServerCoordinators const& coordinators,
                                         double pollingInterval,
                                         Optional<double> compactionInterval,
                                         bool readPreviousCoordinators)
  : impl(PImpl<PaxosConfigConsumerImpl>::create(coordinators.configServers,
                                                pollingInterval,
                                                compactionInterval,
                                                readPreviousCoordinators)) {}

PaxosConfigConsumer::~PaxosConfigConsumer() = default;

Future<Void> PaxosConfigConsumer::readSnapshot(ConfigBroadcaster& broadcaster) {
	return impl->readSnapshot(broadcaster);
}

Future<Void> PaxosConfigConsumer::consume(ConfigBroadcaster& broadcaster) {
	return impl->consume(broadcaster);
}

void PaxosConfigConsumer::allowSpecialCaseRollforward() {
	impl->allowSpecialCaseRollforward();
}

UID PaxosConfigConsumer::getID() const {
	return impl->getID();
}
