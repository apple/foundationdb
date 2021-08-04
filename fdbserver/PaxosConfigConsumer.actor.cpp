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

class PaxosConfigConsumerImpl {
	std::vector<ConfigFollowerInterface> cfis;
	Version lastSeenVersion{ 0 };
	double pollingInterval;
	Optional<double> compactionInterval;
	UID id;

	ACTOR static Future<Version> getCommittedVersion(PaxosConfigConsumerImpl* self) {
		state std::vector<Future<ConfigFollowerGetCommittedVersionReply>> committedVersionFutures;
		committedVersionFutures.reserve(self->cfis.size());
		for (const auto& cfi : self->cfis) {
			committedVersionFutures.push_back(
			    cfi.getCommittedVersion.getReply(ConfigFollowerGetCommittedVersionRequest{}));
		}
		// FIXME: Must tolerate failure and disagreement
		wait(waitForAll(committedVersionFutures));
		return committedVersionFutures[0].get().version;
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
		ConfigFollowerGetSnapshotAndChangesReply reply = wait(self->cfis[0].getSnapshotAndChanges.getReply(
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
		loop {
			try {
				state Version committedVersion = wait(getCommittedVersion(self));
				ASSERT_GE(committedVersion, self->lastSeenVersion);
				if (committedVersion > self->lastSeenVersion) {
					// TODO: Load balance
					ConfigFollowerGetChangesReply reply = wait(self->cfis[0].getChanges.getReply(
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
					TEST(true); // SimpleConfigConsumer get version_already_compacted error
					wait(getSnapshotAndChanges(self, broadcaster));
				} else {
					throw e;
				}
			}
		}
	}

public:
	Future<Void> consume(ConfigBroadcaster& broadcaster) {
		return fetchChanges(this, &broadcaster) || compactor(this, &broadcaster);
	}

	UID getID() const { return id; }

	PaxosConfigConsumerImpl(std::vector<ConfigFollowerInterface> const& cfis,
	                        double pollingInterval,
	                        Optional<double> compactionInterval)
	  : cfis(cfis), pollingInterval(pollingInterval), compactionInterval(compactionInterval),
	    id(deterministicRandom()->randomUniqueID()) {}
};

PaxosConfigConsumer::PaxosConfigConsumer(std::vector<ConfigFollowerInterface> const& cfis,
                                         double pollingInterval,
                                         Optional<double> compactionInterval)
  : _impl(std::make_unique<PaxosConfigConsumerImpl>(cfis, pollingInterval, compactionInterval)) {}

PaxosConfigConsumer::PaxosConfigConsumer(ServerCoordinators const& coordinators,
                                         double pollingInterval,
                                         Optional<double> compactionInterval)
  : _impl(std::make_unique<PaxosConfigConsumerImpl>(coordinators.configServers, pollingInterval, compactionInterval)) {}

PaxosConfigConsumer::~PaxosConfigConsumer() = default;

Future<Void> PaxosConfigConsumer::consume(ConfigBroadcaster& broadcaster) {
	return impl().consume(broadcaster);
}

UID PaxosConfigConsumer::getID() const {
	return impl().getID();
}
