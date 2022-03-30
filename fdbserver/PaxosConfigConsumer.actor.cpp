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
	size_t totalRepliesReceived{ 0 };
	size_t maxAgreement{ 0 };
	// Stores the largest committed version out of all responses.
	Version largestCommitted{ 0 };

	// Sends rollback/rollforward messages to any nodes that are not up to date
	// with the latest committed version as determined by the quorum. Should
	// only be called after a committed version has been determined.
	ACTOR static Future<Void> updateNode(GetCommittedVersionQuorum* self,
	                                     CommittedVersions nodeVersion,
	                                     CommittedVersions quorumVersion,
	                                     Version lastCompacted,
	                                     ConfigFollowerInterface cfi) {
		state Version target = quorumVersion.lastCommitted;
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
				rollback = std::max(nodeVersion.lastCommitted - 1, Version{ 0 });
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
			state Reference<ConfigFollowerInfo> quorumCfi(new ConfigFollowerInfo(self->replies[target]));
			try {
				state Version lastSeenVersion = std::max(
				    rollback.present() ? rollback.get() : nodeVersion.lastCommitted, self->largestCompactedResponse);
				ConfigFollowerGetChangesReply reply =
				    wait(timeoutError(basicLoadBalance(quorumCfi,
				                                       &ConfigFollowerInterface::getChanges,
				                                       ConfigFollowerGetChangesRequest{ lastSeenVersion, target }),
				                      SERVER_KNOBS->GET_COMMITTED_VERSION_TIMEOUT));
				wait(timeoutError(cfi.rollforward.getReply(ConfigFollowerRollforwardRequest{
				                      rollback, nodeVersion.lastCommitted, target, reply.changes, reply.annotations }),
				                  SERVER_KNOBS->GET_COMMITTED_VERSION_TIMEOUT));
			} catch (Error& e) {
				if (e.code() == error_code_transaction_too_old) {
					// Seeing this trace is not necessarily a problem. There
					// are legitimate scenarios where a ConfigNode could return
					// one of these errors in response to a get changes or
					// rollforward request. The retry loop should handle this
					// case.
					TraceEvent(SevInfo, "ConfigNodeRollforwardError").error(e);
				} else {
					throw;
				}
			}
		}
		return Void();
	}

	ACTOR static Future<Void> getCommittedVersionActor(GetCommittedVersionQuorum* self, ConfigFollowerInterface cfi) {
		try {
			ConfigFollowerGetCommittedVersionReply reply =
			    wait(timeoutError(cfi.getCommittedVersion.getReply(ConfigFollowerGetCommittedVersionRequest{}),
			                      SERVER_KNOBS->GET_COMMITTED_VERSION_TIMEOUT));

			++self->totalRepliesReceived;
			self->largestCompactedResponse = std::max(self->largestCompactedResponse, reply.lastCompacted);
			state Version lastCompacted = reply.lastCompacted;
			self->largestCommitted = std::max(self->largestCommitted, reply.lastCommitted);
			state CommittedVersions committedVersions = CommittedVersions{ self->lastSeenVersion, reply.lastCommitted };
			if (self->priorVersions.find(committedVersions.lastCommitted) == self->priorVersions.end()) {
				self->priorVersions[committedVersions.lastCommitted] = self->lastSeenVersion;
			}
			auto& nodes = self->replies[committedVersions.lastCommitted];
			nodes.push_back(cfi);
			self->maxAgreement = std::max(nodes.size(), self->maxAgreement);
			if (nodes.size() >= self->cfis.size() / 2 + 1) {
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
				ASSERT(committedVersions.lastCommitted != quorumVersion.versions.lastCommitted);
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
			// Count a timeout as a reply.
			++self->totalRepliesReceived;
			if (e.code() == error_code_version_already_compacted) {
				if (self->quorumVersion.canBeSet()) {
					self->quorumVersion.sendError(e);
				}
			} else if (e.code() != error_code_timed_out && e.code() != error_code_broken_promise) {
				if (self->quorumVersion.canBeSet()) {
					self->quorumVersion.sendError(e);
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
				} else if (!self->quorumVersion.isSet()) {
					// Otherwise, if a quorum agree on the committed version,
					// some other occurred. Notify the caller of it.
					self->quorumVersion.sendError(e);
				}
			}
		}
		return Void();
	}

public:
	explicit GetCommittedVersionQuorum(std::vector<ConfigFollowerInterface> const& cfis, Version lastSeenVersion)
	  : cfis(cfis), lastSeenVersion(lastSeenVersion) {}
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
	Future<Void> complete() const { return waitForAll(actors); }
};

class PaxosConfigConsumerImpl {
	std::vector<ConfigFollowerInterface> cfis;
	GetCommittedVersionQuorum getCommittedVersionQuorum;
	Version lastSeenVersion{ 0 };
	Version compactionVersion{ 0 };
	double pollingInterval;
	Optional<double> compactionInterval;
	UID id;

	ACTOR static Future<Version> getCommittedVersion(PaxosConfigConsumerImpl* self) {
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
				compactionRequests.push_back(cfi.compact.getReply(ConfigFollowerCompactRequest{ compactionVersion }));
			}
			try {
				wait(timeoutError(waitForAll(compactionRequests), 1.0));
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
				state Reference<ConfigFollowerInfo> configNodes(
				    new ConfigFollowerInfo(self->getCommittedVersionQuorum.getReadReplicas()));
				ConfigFollowerGetSnapshotAndChangesReply reply =
				    wait(timeoutError(basicLoadBalance(configNodes,
				                                       &ConfigFollowerInterface::getSnapshotAndChanges,
				                                       ConfigFollowerGetSnapshotAndChangesRequest{ committedVersion }),
				                      SERVER_KNOBS->GET_SNAPSHOT_AND_CHANGES_TIMEOUT));
				TraceEvent(SevDebug, "ConfigConsumerGotSnapshotAndChanges", self->id)
				    .detail("SnapshotVersion", reply.snapshotVersion)
				    .detail("SnapshotSize", reply.snapshot.size())
				    .detail("ChangesVersion", committedVersion)
				    .detail("ChangesSize", reply.changes.size())
				    .detail("AnnotationsSize", reply.annotations.size());
				ASSERT_GE(committedVersion, self->lastSeenVersion);
				self->lastSeenVersion = committedVersion;
				Version smallestCommitted = self->getCommittedVersionQuorum.getSmallestCommitted();
				self->compactionVersion = std::max(self->compactionVersion, smallestCommitted);
				broadcaster->applySnapshotAndChanges(std::move(reply.snapshot),
				                                     reply.snapshotVersion,
				                                     reply.changes,
				                                     committedVersion,
				                                     reply.annotations,
				                                     self->getCommittedVersionQuorum.getReadReplicas());
				wait(self->getCommittedVersionQuorum.complete());
				break;
			} catch (Error& e) {
				if (e.code() == error_code_failed_to_reach_quorum) {
					wait(self->getCommittedVersionQuorum.complete());
				} else if (e.code() != error_code_timed_out && e.code() != error_code_broken_promise &&
				           e.code() != error_code_version_already_compacted && e.code() != error_code_process_behind) {
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
					ASSERT(self->getCommittedVersionQuorum.getReadReplicas().size() >= self->cfis.size() / 2 + 1);
					state Reference<ConfigFollowerInfo> configNodes(
					    new ConfigFollowerInfo(self->getCommittedVersionQuorum.getReadReplicas()));
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
					                          committedVersion,
					                          reply.annotations,
					                          self->getCommittedVersionQuorum.getReadReplicas());
				} else if (committedVersion == self->lastSeenVersion) {
					broadcaster->applyChanges({}, -1, {}, self->getCommittedVersionQuorum.getReadReplicas());
				}
				wait(delayJittered(self->pollingInterval));
			} catch (Error& e) {
				if (e.code() == error_code_version_already_compacted || e.code() == error_code_timed_out ||
				    e.code() == error_code_failed_to_reach_quorum || e.code() == error_code_version_already_compacted ||
				    e.code() == error_code_process_behind) {
					TEST(true); // PaxosConfigConsumer get version_already_compacted error
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
		getCommittedVersionQuorum = GetCommittedVersionQuorum{ cfis, lastSeenVersion };
	}

public:
	Future<Void> consume(ConfigBroadcaster& broadcaster) {
		return fetchChanges(this, &broadcaster) || compactor(this, &broadcaster);
	}

	UID getID() const { return id; }

	PaxosConfigConsumerImpl(std::vector<ConfigFollowerInterface> const& cfis,
	                        double pollingInterval,
	                        Optional<double> compactionInterval)
	  : cfis(cfis), getCommittedVersionQuorum(cfis, 0), pollingInterval(pollingInterval),
	    compactionInterval(compactionInterval), id(deterministicRandom()->randomUniqueID()) {}
};

PaxosConfigConsumer::PaxosConfigConsumer(std::vector<ConfigFollowerInterface> const& cfis,
                                         double pollingInterval,
                                         Optional<double> compactionInterval)
  : impl(PImpl<PaxosConfigConsumerImpl>::create(cfis, pollingInterval, compactionInterval)) {}

PaxosConfigConsumer::PaxosConfigConsumer(ServerCoordinators const& coordinators,
                                         double pollingInterval,
                                         Optional<double> compactionInterval)
  : impl(PImpl<PaxosConfigConsumerImpl>::create(coordinators.configServers, pollingInterval, compactionInterval)) {}

PaxosConfigConsumer::~PaxosConfigConsumer() = default;

Future<Void> PaxosConfigConsumer::consume(ConfigBroadcaster& broadcaster) {
	return impl->consume(broadcaster);
}

UID PaxosConfigConsumer::getID() const {
	return impl->getID();
}
