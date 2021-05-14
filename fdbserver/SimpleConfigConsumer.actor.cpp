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
	Version lastSeenVersion;
	Optional<ConfigClassSet> configClassSet;
	Optional<double> pollingInterval;
	Optional<double> compactionInterval;

	UID id;
	CounterCollection cc;
	Counter compactRequest;
	Counter successfulChangeRequest;
	Counter failedChangeRequest;
	Counter snapshotRequest;
	Future<Void> logger;

	ACTOR static Future<Void> compactor(SimpleConfigConsumerImpl* self) {
		if (!self->compactionInterval.present()) {
			wait(Never());
			return Void();
		}
		loop {
			wait(delayJittered(self->compactionInterval.get()));
			// TODO: Enable compaction once bugs are fixed
			// wait(self->cfi.compact.getReply(ConfigFollowerCompactRequest{ self->lastSeenVersion }));
			//++self->compactRequest;
		}
	}

	// TODO: Make static
	ACTOR template <class ConfigStore>
	Future<Void> fetchChanges(SimpleConfigConsumerImpl* self, ConfigStore* configStore) {
		loop {
			try {
				ConfigFollowerGetChangesReply reply = wait(self->cfi.getChanges.getReply(
				    ConfigFollowerGetChangesRequest{ self->lastSeenVersion, self->configClassSet }));
				++self->successfulChangeRequest;
				for (const auto& versionedMutation : reply.versionedMutations) {
					TraceEvent(SevDebug, "ConsumerFetchedMutation", self->id)
					    .detail("Version", versionedMutation.version)
					    .detail("ConfigClass", versionedMutation.mutation.getConfigClass())
					    .detail("KnobName", versionedMutation.mutation.getKnobName())
					    .detail("KnobValue", versionedMutation.mutation.getValue());
				}
				self->lastSeenVersion = reply.mostRecentVersion;
				wait(configStore->addVersionedMutations(reply.versionedMutations, reply.mostRecentVersion));
				if (self->pollingInterval.present()) {
					wait(delayJittered(self->pollingInterval.get()));
				}
			} catch (Error& e) {
				++self->failedChangeRequest;
				if (e.code() == error_code_version_already_compacted) {
					ConfigFollowerGetVersionReply versionReply =
					    wait(self->cfi.getVersion.getReply(ConfigFollowerGetVersionRequest{}));
					ASSERT(versionReply.version > self->lastSeenVersion);
					self->lastSeenVersion = versionReply.version;
					ConfigFollowerGetSnapshotReply dbReply = wait(self->cfi.getSnapshot.getReply(
					    ConfigFollowerGetSnapshotRequest{ self->lastSeenVersion, self->configClassSet }));
					// TODO: Remove unnecessary copy
					auto snapshot = dbReply.snapshot;
					wait(configStore->setSnapshot(std::move(snapshot), self->lastSeenVersion));
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
		self->lastSeenVersion = versionReply.version;
		ConfigFollowerGetSnapshotReply reply = wait(self->cfi.getSnapshot.getReply(
		    ConfigFollowerGetSnapshotRequest{ self->lastSeenVersion, self->configClassSet }));
		TraceEvent(SevDebug, "ConfigGotInitialSnapshot").detail("Version", self->lastSeenVersion);
		// TODO: Remove unnecessary copy
		auto snapshot = reply.snapshot;
		wait(configStore->setSnapshot(std::move(snapshot), self->lastSeenVersion));
		return Void();
	}

	SimpleConfigConsumerImpl(Optional<ConfigClassSet> const& configClassSet, UID id)
	  : configClassSet(configClassSet), id(id), cc("ConfigConsumer"), compactRequest("CompactRequest", cc),
	    successfulChangeRequest("SuccessfulChangeRequest", cc), failedChangeRequest("FailedChangeRequest", cc),
	    snapshotRequest("SnapshotRequest", cc) {
		logger = traceCounters(
		    "ConfigConsumerMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "ConfigConsumerMetrics");
	}

	static ConfigFollowerInterface getConfigFollowerInterface(ConfigFollowerInterface const& cfi) { return cfi; }

	static ConfigFollowerInterface getConfigFollowerInterface(ClusterConnectionString const& ccs) {
		auto coordinators = ccs.coordinators();
		std::sort(coordinators.begin(), coordinators.end());
		return ConfigFollowerInterface(coordinators[0]);
	}

	static ConfigFollowerInterface getConfigFollowerInterface(ServerCoordinators const& coordinators) {
		return ConfigFollowerInterface(coordinators.configServers[0]);
	}

public:
	template <class InterfaceSource>
	SimpleConfigConsumerImpl(InterfaceSource const& interfaceSource,
	                         Optional<ConfigClassSet> const& configClassSet,
	                         Version lastSeenVersion,
	                         Optional<double> const& pollingInterval,
	                         Optional<double> const& compactionInterval,
	                         UID id)
	  : configClassSet(configClassSet), lastSeenVersion(lastSeenVersion), pollingInterval(pollingInterval),
	    compactionInterval(compactionInterval), id(id), cc("ConfigConsumer"), compactRequest("CompactRequest", cc),
	    successfulChangeRequest("SuccessfulChangeRequest", cc), failedChangeRequest("FailedChangeRequest", cc),
	    snapshotRequest("SnapshotRequest", cc) {
		cfi = getConfigFollowerInterface(interfaceSource);
		logger = traceCounters(
		    "ConfigConsumerMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "ConfigConsumerMetrics");
	}

	template <class ConfigStore>
	Future<Void> getInitialSnapshot(ConfigStore& configStore) {
		return getInitialSnapshot(this, &configStore);
	}

	template <class ConfigStore>
	Future<Void> consume(ConfigStore& configStore) {
		// TODO: Reenable compaction
		return fetchChanges(this, &configStore); /* ||compactor(this); */
	}

	UID getID() const { return id; }
};

SimpleConfigConsumer::SimpleConfigConsumer(ConfigFollowerInterface const& cfi,
                                           Optional<ConfigClassSet> const& configClassSet,
                                           Version lastSeenVersion,
                                           Optional<double> const& pollingInterval,
                                           Optional<double> const& compactionInterval,
                                           UID id)
  : impl(std::make_unique<SimpleConfigConsumerImpl>(cfi,
                                                    configClassSet,
                                                    lastSeenVersion,
                                                    pollingInterval,
                                                    compactionInterval,
                                                    id)) {}

SimpleConfigConsumer::SimpleConfigConsumer(ClusterConnectionString const& ccs,
                                           Optional<ConfigClassSet> const& configClassSet,
                                           Version lastSeenVersion,
                                           Optional<double> const& pollingInterval,
                                           Optional<double> const& compactionInterval,
                                           UID id)
  : impl(std::make_unique<SimpleConfigConsumerImpl>(ccs,
                                                    configClassSet,
                                                    lastSeenVersion,
                                                    pollingInterval,
                                                    compactionInterval,
                                                    id)) {}

SimpleConfigConsumer::SimpleConfigConsumer(ServerCoordinators const& coordinators,
                                           Optional<ConfigClassSet> const& configClassSet,
                                           Version lastSeenVersion,
                                           Optional<double> const& pollingInterval,
                                           Optional<double> const& compactionInterval,
                                           UID id)
  : impl(std::make_unique<SimpleConfigConsumerImpl>(coordinators,
                                                    configClassSet,
                                                    lastSeenVersion,
                                                    pollingInterval,
                                                    compactionInterval,
                                                    id)) {}

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

UID SimpleConfigConsumer::getID() const {
	return impl->getID();
}
