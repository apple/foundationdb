/*
 * PaxosConfigConsumer.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include <map>

#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

// Maximum time to wait for a response from a ConfigNode when asking for the
// latest committed version.
const double getCommittedVersionTimeout = 3;

} // namespace

struct CommittedVersions {
	Version secondToLastCommitted;
	Version lastCommitted;
};

class GetCommittedVersionQuorum {
	std::vector<Future<Void>> actors;
	std::vector<ConfigFollowerInterface> cfis;
	std::map<Version, std::vector<ConfigFollowerInterface>> replies;
	std::map<Version, Version> priorVersions;
	// Last durably committed version.
	Version lastSeenVersion;
	size_t totalRepliesReceived{ 0 };
	size_t maxAgreement{ 0 };
	// Set to the <secondToLastCommitted, lastCommitted> versions a quorum of
	// ConfigNodes agree on, otherwise unset.
	Promise<CommittedVersions> quorumVersion;
	// Stores the largest committed version out of all responses.
	Version largestCommitted{ 0 };

	// Sends rollback/rollforward messages to any nodes that are not up to date
	// with the latest committed version as determined by the quorum. Should
	// only be called after a committed version has been determined.
	ACTOR static Future<Void> updateNode(GetCommittedVersionQuorum* self,
	                                     CommittedVersions nodeVersion,
	                                     CommittedVersions quorumVersion,
	                                     ConfigFollowerInterface cfi) {
		ASSERT(nodeVersion.lastCommitted <= self->largestCommitted);
		if (nodeVersion.lastCommitted == self->largestCommitted) {
			return Void();
		}
		if (nodeVersion.lastCommitted < self->largestCommitted) {
			state Optional<Version> rollback;
			if (nodeVersion.lastCommitted > quorumVersion.secondToLastCommitted) {
				// If a non-quorum node has a last committed version less than
				// the last committed version on the quorum, but greater than
				// the second to last committed version on the quorum, it has
				// committed changes the quorum does not agree with. Therefore,
				// it needs to be rolled back before being rolled forward.
				rollback = quorumVersion.secondToLastCommitted;
			}

			// Now roll node forward to match the largest committed version of
			// the replies.
			// TODO: Load balance over quorum. Also need to catch
			// error_code_process_behind and retry with the next ConfigNode in
			// the quorum.
			state ConfigFollowerInterface quorumCfi = self->replies[self->largestCommitted][0];
			try {
				ConfigFollowerGetChangesReply reply = wait(retryBrokenPromise(
				    quorumCfi.getChanges,
				    ConfigFollowerGetChangesRequest{ nodeVersion.lastCommitted, self->largestCommitted }));
				wait(retryBrokenPromise(cfi.rollforward,
				                        ConfigFollowerRollforwardRequest{ rollback,
				                                                          nodeVersion.lastCommitted,
				                                                          self->largestCommitted,
				                                                          reply.changes,
				                                                          reply.annotations }));
			} catch (Error& e) {
				if (e.code() == error_code_version_already_compacted) {
					TEST(true); // PaxosConfigConsumer rollforward compacted ConfigNode
					ConfigFollowerGetSnapshotAndChangesReply reply =
					    wait(retryBrokenPromise(quorumCfi.getSnapshotAndChanges,
					                            ConfigFollowerGetSnapshotAndChangesRequest{ self->largestCommitted }));
					// TODO: Send the whole snapshot to `cfi`
					ASSERT(false);
				} else if (e.code() == error_code_not_committed) {
					// Seeing this trace is not necessarily a problem. There
					// are legitimate scenarios where a ConfigNode could return
					// not_committed in response to a rollforward request.
					TraceEvent(SevInfo, "ConfigNodeRollforwardError").error(e);
				} else {
					throw e;
				}
			}
		}
		return Void();
	}

	ACTOR static Future<Void> getCommittedVersionActor(GetCommittedVersionQuorum* self, ConfigFollowerInterface cfi) {
		try {
			ConfigFollowerGetCommittedVersionReply reply =
			    wait(timeoutError(cfi.getCommittedVersion.getReply(ConfigFollowerGetCommittedVersionRequest{}),
			                      getCommittedVersionTimeout));

			++self->totalRepliesReceived;
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
					self->quorumVersion.send(committedVersions);
				}
				// TODO: We need to wait for all the responses to come in
				// before calling updateNode here. For example, imagine a
				// scenario with ConfigNodes at versions 1, 1, 2. If responses
				// were received from the two ConfigNodes at version 1 first,
				// the quorum version would be set at 1 and updateNode would be
				// called using version 1. However, in this scenario, these two
				// ConfigNodes should actually be rolled forward to version 2.
				wait(self->updateNode(self, committedVersions, self->quorumVersion.getFuture().get(), cfi));
			} else if (self->maxAgreement >= self->cfis.size() / 2 + 1) {
				// A quorum of ConfigNodes agree on the latest committed version,
				// but the node we just got a reply from is not one of them. We may
				// need to roll it forward or back.
				CommittedVersions quorumVersion = wait(self->quorumVersion.getFuture());
				ASSERT(committedVersions.lastCommitted != quorumVersion.lastCommitted);
				wait(self->updateNode(self, committedVersions, quorumVersion, cfi));
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
					self->quorumVersion.send(CommittedVersions{ largestCommittedPrior, largestCommitted });
				}
				wait(self->updateNode(self, committedVersions, self->quorumVersion.getFuture().get(), cfi));
			} else {
				// Still building up responses; don't have enough data to act on
				// yet, so wait until we do.
				CommittedVersions quorumVersion = wait(self->quorumVersion.getFuture());
				wait(self->updateNode(self, committedVersions, quorumVersion, cfi));
			}
		} catch (Error& e) {
			// Count a timeout as a reply.
			++self->totalRepliesReceived;
			if (e.code() != error_code_timed_out) {
				throw;
			} else if (self->totalRepliesReceived == self->cfis.size() && !self->quorumVersion.isSet()) {
				// Make sure to trigger the quorumVersion if a timeout
				// occurred, the quorum version hasn't been set, and there are
				// no more incoming responses. Note that this means that it is
				// impossible to reach a quorum, so send back the largest
				// committed version seen. We also need to store the interface
				// for the timed out server for future communication attempts.
				auto& nodes = self->replies[self->largestCommitted];
				nodes.push_back(cfi);
				self->quorumVersion.send(CommittedVersions{ self->lastSeenVersion, self->largestCommitted });
			}
		}
		return Void();
	}

public:
	explicit GetCommittedVersionQuorum(std::vector<ConfigFollowerInterface> const& cfis, Version lastSeenVersion)
	  : cfis(cfis), lastSeenVersion(lastSeenVersion) {}
	Future<CommittedVersions> getCommittedVersion() {
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
		ASSERT(isReady());
		return replies.at(quorumVersion.getFuture().get().lastCommitted);
	}
	Future<Void> complete() const { return waitForAll(actors); }
};

class PaxosConfigConsumerImpl {
	std::vector<ConfigFollowerInterface> cfis;
	GetCommittedVersionQuorum getCommittedVersionQuorum;
	Version lastSeenVersion{ 0 };
	double pollingInterval;
	Optional<double> compactionInterval;
	UID id;

	ACTOR static Future<Version> getCommittedVersion(PaxosConfigConsumerImpl* self) {
		CommittedVersions versions = wait(self->getCommittedVersionQuorum.getCommittedVersion());
		return versions.lastCommitted;
	}

	ACTOR static Future<Void> compactor(PaxosConfigConsumerImpl* self, ConfigBroadcaster* broadcaster) {
		if (!self->compactionInterval.present()) {
			wait(Never());
			return Void();
		}
		loop {
			state Version compactionVersion = self->lastSeenVersion;
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
		state Version committedVersion = wait(getCommittedVersion(self));
		// TODO: Load balance
		ConfigFollowerGetSnapshotAndChangesReply reply =
		    wait(retryBrokenPromise(self->getCommittedVersionQuorum.getReadReplicas()[0].getSnapshotAndChanges,
		                            ConfigFollowerGetSnapshotAndChangesRequest{ committedVersion }));
		TraceEvent(SevDebug, "ConfigConsumerGotSnapshotAndChanges", self->id)
		    .detail("SnapshotVersion", reply.snapshotVersion)
		    .detail("SnapshotSize", reply.snapshot.size())
		    .detail("ChangesVersion", committedVersion)
		    .detail("ChangesSize", reply.changes.size())
		    .detail("AnnotationsSize", reply.annotations.size());
		ASSERT_GE(committedVersion, self->lastSeenVersion);
		self->lastSeenVersion = committedVersion;
		broadcaster->applySnapshotAndChanges(
		    std::move(reply.snapshot), reply.snapshotVersion, reply.changes, committedVersion, reply.annotations);
		return Void();
	}

	ACTOR static Future<Void> fetchChanges(PaxosConfigConsumerImpl* self, ConfigBroadcaster* broadcaster) {
		wait(getSnapshotAndChanges(self, broadcaster));
		self->reset();
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
					// TODO: Load balance to avoid always hitting the
					// node at index 0 first
					ConfigFollowerGetChangesReply reply = wait(
					    retryBrokenPromise(self->getCommittedVersionQuorum.getReadReplicas()[0].getChanges,
					                       ConfigFollowerGetChangesRequest{ self->lastSeenVersion, committedVersion }));
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
					broadcaster->applyChanges(reply.changes, committedVersion, reply.annotations);
					// TODO: Catch error_code_process_behind and retry with
					// the next ConfigNode in the quorum.
				}
				wait(delayJittered(self->pollingInterval));
			} catch (Error& e) {
				if (e.code() == error_code_version_already_compacted) {
					TEST(true); // PaxosConfigConsumer get version_already_compacted error
					wait(getSnapshotAndChanges(self, broadcaster));
				} else {
					throw e;
				}
			}
			wait(self->getCommittedVersionQuorum.complete());
			self->reset();
		}
	}

	void reset() { getCommittedVersionQuorum = GetCommittedVersionQuorum{ cfis, lastSeenVersion }; }

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
