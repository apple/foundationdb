/*
 * ConfigBroadcaster.actor.cpp
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
#include "fdbserver/LocalConfiguration.h" // testing only
#include "fdbserver/SimpleConfigConsumer.h"
#include "flow/actorcompiler.h" // must be last include

namespace {

bool matchesConfigClass(Optional<ConfigClassSet> const& configClassSet, Optional<KeyRef> configClass) {
	return !configClassSet.present() || !configClass.present() || configClassSet.get().contains(configClass.get());
}

} // namespace

class ConfigBroadcasterImpl {
	std::map<Key, Endpoint::Token> configClassToToken;
	std::map<Endpoint::Token, ReplyPromise<ConfigFollowerGetChangesRequest>> tokenToReply;
	std::map<Endpoint::Token, std::vector<Key>> tokenToConfigClasses;
	std::map<ConfigKey, Value> snapshot;
	std::deque<Standalone<VersionedConfigMutationRef>> versionedMutations;
	Version lastCompactedVersion;
	Version mostRecentVersion;
	std::unique_ptr<IConfigConsumer> consumer;
	ActorCollection actors{ false };

	UID id;
	CounterCollection cc;
	Counter compactRequest;
	Counter successfulChangeRequest;
	Counter failedChangeRequest;
	Counter snapshotRequest;
	Future<Void> logger;

	ACTOR static Future<Void> serve(ConfigBroadcaster* self, ConfigBroadcasterImpl* impl, ConfigFollowerInterface cfi) {
		wait(impl->consumer->getInitialSnapshot(*self));
		impl->actors.add(impl->consumer->consume(*self));
		loop {
			choose {
				when(ConfigFollowerGetVersionRequest req = waitNext(cfi.getVersion.getFuture())) {
					req.reply.send(impl->mostRecentVersion);
				}
				when(ConfigFollowerGetSnapshotRequest req = waitNext(cfi.getSnapshot.getFuture())) {
					++impl->snapshotRequest;
					ConfigFollowerGetSnapshotReply reply;
					for (const auto& [key, value] : impl->snapshot) {
						if (matchesConfigClass(req.configClassSet, key.configClass)) {
							reply.snapshot[key] = value;
						}
					}
					for (const auto& versionedMutation : impl->versionedMutations) {
						const auto& version = versionedMutation.version;
						const auto& mutation = versionedMutation.mutation;
						if (version > req.version) {
							break;
						}
						if (matchesConfigClass(req.configClassSet, mutation.getConfigClass())) {
							TraceEvent(SevDebug, "ConfigBroadcasterAppendingMutationToSnapshotOutput", impl->id)
							    .detail("ReqVersion", req.version)
							    .detail("MutationVersion", version)
							    .detail("ConfigClass", mutation.getConfigClass())
							    .detail("KnobName", mutation.getKnobName())
							    .detail("KnobValue", mutation.getValue());
							if (mutation.isSet()) {
								reply.snapshot[mutation.getKey()] = mutation.getValue().get();
							} else {
								reply.snapshot.erase(mutation.getKey());
							}
						}
					}
					req.reply.send(reply);
				}
				when(ConfigFollowerGetChangesRequest req = waitNext(cfi.getChanges.getFuture())) {
					if (req.lastSeenVersion < impl->lastCompactedVersion) {
						req.reply.sendError(version_already_compacted());
						++impl->failedChangeRequest;
						continue;
					}
					ConfigFollowerGetChangesReply reply;
					reply.mostRecentVersion = impl->mostRecentVersion;
					for (const auto& versionedMutation : impl->versionedMutations) {
						if (versionedMutation.version > req.lastSeenVersion &&
						    matchesConfigClass(req.configClassSet, versionedMutation.mutation.getConfigClass())) {
							TraceEvent(SevDebug, "ConfigBroadcasterSendingChangeMutation", impl->id)
							    .detail("Version", versionedMutation.version)
							    .detail("ReqLastSeenVersion", req.lastSeenVersion)
							    .detail("ConfigClass", versionedMutation.mutation.getConfigClass())
							    .detail("KnobName", versionedMutation.mutation.getKnobName())
							    .detail("KnobValue", versionedMutation.mutation.getValue());
							reply.versionedMutations.push_back_deep(reply.versionedMutations.arena(),
							                                        versionedMutation);
						}
					}
					req.reply.send(reply);
					++impl->successfulChangeRequest;
				}
				when(ConfigFollowerCompactRequest req = waitNext(cfi.compact.getFuture())) {
					++impl->compactRequest;
					while (!impl->versionedMutations.empty()) {
						const auto& versionedMutation = impl->versionedMutations.front();
						const auto& version = versionedMutation.version;
						const auto& mutation = versionedMutation.mutation;
						if (version > req.version) {
							break;
						} else {
							TraceEvent(SevDebug, "ConfigBroadcasterCompactingMutation", impl->id)
							    .detail("ReqVersion", req.version)
							    .detail("MutationVersion", version)
							    .detail("ConfigClass", mutation.getConfigClass())
							    .detail("KnobName", mutation.getKnobName())
							    .detail("KnobValue", mutation.getValue())
							    .detail("LastCompactedVersion", impl->lastCompactedVersion);
							if (mutation.isSet()) {
								impl->snapshot[mutation.getKey()] = mutation.getValue().get();
							} else {
								impl->snapshot.erase(mutation.getKey());
							}
							impl->lastCompactedVersion = version;
							impl->versionedMutations.pop_front();
						}
					}
					req.reply.send(Void());
				}
				when(wait(impl->actors.getResult())) { ASSERT(false); }
			}
		}
	}

public:
	Future<Void> serve(ConfigBroadcaster* self, ConfigFollowerInterface const& cfi) { return serve(self, this, cfi); }

	Future<Void> addVersionedMutations(Standalone<VectorRef<VersionedConfigMutationRef>> const& versionedMutations,
	                                   Version mostRecentVersion) {
		this->versionedMutations.insert(
		    this->versionedMutations.end(), versionedMutations.begin(), versionedMutations.end());
		this->mostRecentVersion = mostRecentVersion;
		return Void();
	}

	Future<Void> setSnapshot(std::map<ConfigKey, Value>&& snapshot, Version snapshotVersion) {
		this->snapshot = std::move(snapshot);
		this->lastCompactedVersion = snapshotVersion;
		return Void();
	}

	template <class ConfigSource>
	ConfigBroadcasterImpl(ConfigSource const& configSource, UID id)
	  : id(id), lastCompactedVersion(0), mostRecentVersion(0), cc("ConfigBroadcaster"),
	    compactRequest("CompactRequest", cc), successfulChangeRequest("SuccessfulChangeRequest", cc),
	    failedChangeRequest("FailedChangeRequest", cc), snapshotRequest("SnapshotRequest", cc) {
		logger = traceCounters(
		    "ConfigBroadcasterMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "ConfigBroadcasterMetrics");
		auto consumerID = deterministicRandom()->randomUniqueID();
		TraceEvent(SevDebug, "BroadcasterStartingConsumer", id).detail("Consumer", consumerID);
		consumer = std::make_unique<SimpleConfigConsumer>(
		    configSource, Optional<ConfigClassSet>{}, 0, 0.5, Optional<double>{}, consumerID);
	}

	UID getID() const { return id; }
};

ConfigBroadcaster::ConfigBroadcaster(ConfigFollowerInterface const& cfi, UID id)
  : impl(std::make_unique<ConfigBroadcasterImpl>(cfi, id)) {}

ConfigBroadcaster::ConfigBroadcaster(ClusterConnectionString const& ccs, UID id)
  : impl(std::make_unique<ConfigBroadcasterImpl>(ccs, id)) {}

ConfigBroadcaster::ConfigBroadcaster(ServerCoordinators const& coordinators, UID id)
  : impl(std::make_unique<ConfigBroadcasterImpl>(coordinators, id)) {}

ConfigBroadcaster::ConfigBroadcaster(ConfigBroadcaster&&) = default;

ConfigBroadcaster& ConfigBroadcaster::operator=(ConfigBroadcaster&&) = default;

ConfigBroadcaster::~ConfigBroadcaster() = default;

Future<Void> ConfigBroadcaster::serve(ConfigFollowerInterface const& cfi) {
	return impl->serve(this, cfi);
}

Future<Void> ConfigBroadcaster::addVersionedMutations(
    Standalone<VectorRef<VersionedConfigMutationRef>> const& versionedMutations,
    Version mostRecentVersion) {
	return impl->addVersionedMutations(versionedMutations, mostRecentVersion);
}

Future<Void> ConfigBroadcaster::setSnapshot(std::map<ConfigKey, Value>&& snapshot, Version snapshotVersion) {
	return impl->setSnapshot(std::move(snapshot), snapshotVersion);
}

UID ConfigBroadcaster::getID() const {
	return impl->getID();
}
