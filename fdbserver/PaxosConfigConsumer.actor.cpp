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

struct CommittedVersions {
	Version secondToLastCommitted;
	Version lastCommitted;
};

class GetCommittedVersionQuorum {
	std::vector<Future<Void>> actors;
	std::vector<ConfigFollowerInterface> cfis;
	std::map<Version, std::vector<ConfigFollowerInterface>> replies;
	std::map<Version, Version> priorVersions; // TODO: Would be nice to combine this with `replies`
	size_t totalRepliesReceived{ 0 };
	size_t maxAgreement{ 0 };
	// Set to the <secondToLastCommitted, lastCommitted> versions a quorum of
	// ConfigNodes agree on, otherwise unset.
	Promise<CommittedVersions> quorumVersion;

	// Sends rollback/rollforward messages to any nodes that are not up to date
	// with the latest committed version as determined by the quorum. Should
	// only be called after a committed version has been determined.
	ACTOR static Future<Void> updateNode(GetCommittedVersionQuorum* self,
	                                     Version secondToLastCommitted,
	                                     Version lastCommitted,
	                                     CommittedVersions quorumVersion,
	                                     ConfigFollowerInterface cfi) {
		if (lastCommitted == quorumVersion.lastCommitted) {
			return Void();
		}
		if (lastCommitted > quorumVersion.lastCommitted) {
			wait(cfi.rollback.getReply(ConfigFollowerRollbackRequest{ quorumVersion.lastCommitted }));
		} else {
			if (secondToLastCommitted > quorumVersion.secondToLastCommitted) {
				// If the non-quorum node has a last committed version less
				// than the last committed version on the quorum, but greater
				// than the second to last committed version on the quorum, it
				// needs to be rolled back before being rolled forward.
				wait(cfi.rollback.getReply(ConfigFollowerRollbackRequest{ quorumVersion.secondToLastCommitted }));
			}

			// Now roll node forward to match the last committed version of the
			// quorum.
			// TODO: Load balance over quorum
			state ConfigFollowerInterface quorumCfi = self->replies[quorumVersion.lastCommitted][0];
			try {
				ConfigFollowerGetChangesReply reply = wait(quorumCfi.getChanges.getReply(
				    ConfigFollowerGetChangesRequest{ lastCommitted, quorumVersion.lastCommitted }));
				wait(cfi.rollforward.getReply(ConfigFollowerRollforwardRequest{
				    lastCommitted, quorumVersion.lastCommitted, reply.changes, reply.annotations }));
			} catch (Error& e) {
				if (e.code() == error_code_version_already_compacted) {
					TEST(true); // PaxosConfigConsumer rollforward compacted ConfigNode
					ConfigFollowerGetSnapshotAndChangesReply reply = wait(quorumCfi.getSnapshotAndChanges.getReply(
					    ConfigFollowerGetSnapshotAndChangesRequest{ quorumVersion.lastCommitted }));
					// TODO: Send the whole snapshot to `cfi`
					ASSERT(false);
					// return cfi.rollforward.getReply(ConfigFollowerRollforwardRequest{ lastCommitted,
					// quorumVersion.second, reply.changes, reply.annotations });
				} else {
					throw e;
				}
			}
		}
		return Void();
	}

	ACTOR static Future<Void> getCommittedVersionActor(GetCommittedVersionQuorum* self, ConfigFollowerInterface cfi) {
		try {
			// TODO: Timeout value should be a variable/field
			ConfigFollowerGetCommittedVersionReply reply =
			    wait(timeoutError(cfi.getCommittedVersion.getReply(ConfigFollowerGetCommittedVersionRequest{}), 3));

			++self->totalRepliesReceived;
			state Version priorVersion = reply.secondToLastCommitted;
			state Version version = reply.lastCommitted;
			if (self->replies.find(version) == self->replies.end()) {
				self->replies[version] = {};
				self->priorVersions[version] = priorVersion;
			}
			auto& nodes = self->replies[version];
			nodes.push_back(cfi);
			self->maxAgreement = std::max(nodes.size(), self->maxAgreement);
			if (nodes.size() >= self->cfis.size() / 2 + 1) {
				// A quorum of ConfigNodes agree on the latest committed version.
				if (self->quorumVersion.canBeSet()) {
					self->quorumVersion.send(CommittedVersions{ priorVersion, version });
				}
			} else if (self->maxAgreement >= self->cfis.size() / 2 + 1) {
				// A quorum of ConfigNodes agree on the latest committed version,
				// but the node we just got a reply from is not one of them. We may
				// need to roll it forward or back.
				CommittedVersions quorumVersion = wait(self->quorumVersion.getFuture());
				ASSERT(version != quorumVersion.lastCommitted);
				wait(self->updateNode(self, priorVersion, version, quorumVersion, cfi));
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
				wait(self->updateNode(self, priorVersion, version, self->quorumVersion.getFuture().get(), cfi));
			} else {
				// Still building up responses; don't have enough data to act on
				// yet, so wait until we do.
				CommittedVersions quorumVersion = wait(self->quorumVersion.getFuture());
				wait(self->updateNode(self, priorVersion, version, quorumVersion, cfi));
			}
		} catch (Error& e) {
			if (e.code() != error_code_timed_out) {
				throw;
			}
		}
		return Void();
	}

public:
	explicit GetCommittedVersionQuorum(std::vector<ConfigFollowerInterface> const& cfis) : cfis(cfis) {}
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
	Future<Void> complete() { return waitForAll(actors); }
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
		    wait(self->getCommittedVersionQuorum.getReadReplicas()[0].getSnapshotAndChanges.getReply(
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
				ASSERT_GE(committedVersion, self->lastSeenVersion);
				if (committedVersion > self->lastSeenVersion) {
					// TODO: Load balance
					ConfigFollowerGetChangesReply reply =
					    wait(self->getCommittedVersionQuorum.getReadReplicas()[0].getChanges.getReply(
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

	void reset() { getCommittedVersionQuorum = GetCommittedVersionQuorum{ cfis }; }

public:
	Future<Void> consume(ConfigBroadcaster& broadcaster) {
		return fetchChanges(this, &broadcaster) || compactor(this, &broadcaster);
	}

	UID getID() const { return id; }

	PaxosConfigConsumerImpl(std::vector<ConfigFollowerInterface> const& cfis,
	                        double pollingInterval,
	                        Optional<double> compactionInterval)
	  : cfis(cfis), getCommittedVersionQuorum(cfis), pollingInterval(pollingInterval),
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
