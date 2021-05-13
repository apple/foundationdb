/*
 * SimpleConfigConsumer.actor.cpp
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

#include "fdbserver/ConfigBroadcaster.h"
#include "fdbserver/SimpleConfigConsumer.h"

class SimpleConfigConsumerImpl {
	ConfigFollowerInterface cfi;

	Version mostRecentVersion{ 0 };

	CounterCollection cc;
	Counter compactRequest;
	Counter successfulChangeRequest;
	Counter failedChangeRequest;
	Counter snapshotRequest;
	Future<Void> logger;

	static const double POLLING_INTERVAL; // TODO: Make knob?
	static const double COMPACTION_INTERVAL; // TODO: Make knob?

	ACTOR static Future<Void> compactor(SimpleConfigConsumerImpl* self) {
		loop {
			wait(delayJittered(COMPACTION_INTERVAL));
			// TODO: Enable compaction once bugs are fixed
			// wait(self->cfi.compact.getReply(ConfigFollowerCompactRequest{ self->mostRecentVersion }));
			//++self->compactRequest;
		}
	}

	// TODO: Make static
	ACTOR template <class ConfigStore>
	Future<Void> fetchChanges(SimpleConfigConsumerImpl* self, ConfigStore* configStore) {
		loop {
			try {
				ConfigFollowerGetChangesReply reply =
				    wait(self->cfi.getChanges.getReply(ConfigFollowerGetChangesRequest{ self->mostRecentVersion, {} }));
				++self->successfulChangeRequest;
				for (const auto& versionedMutation : reply.versionedMutations) {
					TraceEvent(SevDebug, "ConsumerFetchedMutation")
					    .detail("Version", versionedMutation.version)
					    .detail("ConfigClass", versionedMutation.mutation.getConfigClass())
					    .detail("KnobName", versionedMutation.mutation.getKnobName())
					    .detail("KnobValue", versionedMutation.mutation.getValue());
				}
				self->mostRecentVersion = reply.mostRecentVersion;
				wait(configStore->addVersionedMutations(reply.versionedMutations, reply.mostRecentVersion));
				wait(delayJittered(POLLING_INTERVAL));
			} catch (Error& e) {
				++self->failedChangeRequest;
				if (e.code() == error_code_version_already_compacted) {
					ConfigFollowerGetVersionReply versionReply =
					    wait(self->cfi.getVersion.getReply(ConfigFollowerGetVersionRequest{}));
					ASSERT(versionReply.version > self->mostRecentVersion);
					self->mostRecentVersion = versionReply.version;
					ConfigFollowerGetSnapshotReply dbReply = wait(self->cfi.getSnapshot.getReply(
					    ConfigFollowerGetSnapshotRequest{ self->mostRecentVersion, {} }));
					// TODO: Remove unnecessary copy
					auto snapshot = dbReply.snapshot;
					wait(configStore->setSnapshot(std::move(snapshot), self->mostRecentVersion));
					++self->snapshotRequest;
				} else {
					throw e;
				}
			}
		}
	}

	// TODO: Make static
	ACTOR template <class ConfigStore>
	Future<Void> getInitialSnapshot(SimpleConfigConsumerImpl* self, ConfigStore* configStore) {
		ConfigFollowerGetVersionReply versionReply =
		    wait(self->cfi.getVersion.getReply(ConfigFollowerGetVersionRequest{}));
		self->mostRecentVersion = versionReply.version;
		ConfigFollowerGetSnapshotReply reply =
		    wait(self->cfi.getSnapshot.getReply(ConfigFollowerGetSnapshotRequest{ self->mostRecentVersion, {} }));
		TraceEvent(SevDebug, "ConfigGotInitialSnapshot").detail("Version", self->mostRecentVersion);
		// TODO: Remove unnecessary copy
		auto snapshot = reply.snapshot;
		wait(configStore->setSnapshot(std::move(snapshot), self->mostRecentVersion));
		return Void();
	}

	SimpleConfigConsumerImpl()
	  : mostRecentVersion(0), cc("ConfigConsumer"), compactRequest("CompactRequest", cc),
	    successfulChangeRequest("SuccessfulChangeRequest", cc), failedChangeRequest("FailedChangeRequest", cc),
	    snapshotRequest("SnapshotRequest", cc) {
		logger = traceCounters("ConfigConsumerMetrics",
		                       deterministicRandom()->randomUniqueID(),
		                       SERVER_KNOBS->WORKER_LOGGING_INTERVAL,
		                       &cc,
		                       "ConfigConsumerMetrics");
	}

public:
	SimpleConfigConsumerImpl(ConfigFollowerInterface const& cfi) : SimpleConfigConsumerImpl() { this->cfi = cfi; }

	SimpleConfigConsumerImpl(ClusterConnectionString const& ccs) : SimpleConfigConsumerImpl() {
		auto coordinators = ccs.coordinators();
		std::sort(coordinators.begin(), coordinators.end());
		cfi = ConfigFollowerInterface(coordinators[0]);
	}

	SimpleConfigConsumerImpl(ServerCoordinators const& coordinators) : SimpleConfigConsumerImpl() {
		cfi = ConfigFollowerInterface(coordinators.configServers[0]);
	}

	template <class ConfigStore>
	Future<Void> getInitialSnapshot(ConfigStore& configStore) {
		return getInitialSnapshot(this, &configStore);
	}

	template <class ConfigStore>
	Future<Void> consume(ConfigStore& configStore) {
		return fetchChanges(this, &configStore) || compactor(this);
	}
};

const double SimpleConfigConsumerImpl::POLLING_INTERVAL = 0.5;
const double SimpleConfigConsumerImpl::COMPACTION_INTERVAL = 5.0;

SimpleConfigConsumer::SimpleConfigConsumer(ConfigFollowerInterface const& cfi)
  : impl(std::make_unique<SimpleConfigConsumerImpl>(cfi)) {}

SimpleConfigConsumer::SimpleConfigConsumer(ClusterConnectionString const& ccs)
  : impl(std::make_unique<SimpleConfigConsumerImpl>(ccs)) {}

SimpleConfigConsumer::SimpleConfigConsumer(ServerCoordinators const& coordinators)
  : impl(std::make_unique<SimpleConfigConsumerImpl>(coordinators)) {}

Future<Void> SimpleConfigConsumer::getInitialSnapshot(ConfigBroadcaster& broadcaster) {
	return impl->getInitialSnapshot(broadcaster);
}

Future<Void> SimpleConfigConsumer::getInitialSnapshot(LocalConfiguration& localConfiguration) {
	return impl->getInitialSnapshot(localConfiguration);
}

Future<Void> SimpleConfigConsumer::consume(ConfigBroadcaster& broadcaster) {
	return impl->consume(broadcaster);
}

Future<Void> SimpleConfigConsumer::consume(LocalConfiguration& localConfiguration) {
	return impl->consume(localConfiguration);
}

SimpleConfigConsumer::~SimpleConfigConsumer() = default;
