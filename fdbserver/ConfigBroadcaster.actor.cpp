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

#include <algorithm>

#include "fdbserver/ConfigBroadcaster.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/IConfigConsumer.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

namespace {

bool matchesConfigClass(ConfigClassSet const& configClassSet, Optional<KeyRef> configClass) {
	return !configClass.present() || configClassSet.contains(configClass.get());
}

// Helper functions for STL containers, with flow-friendly error handling
template <class MapContainer, class K>
auto get(MapContainer& m, K const& k) -> decltype(m.at(k)) {
	auto it = m.find(k);
	ASSERT(it != m.end());
	return it->second;
}
template <class Container, class K>
void remove(Container& container, K const& k) {
	auto it = container.find(k);
	ASSERT(it != container.end());
	container.erase(it);
}

} // namespace

class ConfigBroadcasterImpl {
	// Holds information about each client connected to the broadcaster.
	struct BroadcastClientDetails {
		// Triggered when the worker dies.
		Future<Void> watcher;
		ConfigClassSet configClassSet;
		Version lastSeenVersion;
		ConfigBroadcastInterface broadcastInterface;

		bool operator==(BroadcastClientDetails const& rhs) const {
			return configClassSet == rhs.configClassSet && lastSeenVersion == rhs.lastSeenVersion &&
			       broadcastInterface == rhs.broadcastInterface;
		}
		bool operator!=(BroadcastClientDetails const& rhs) const { return !(*this == rhs); }

		BroadcastClientDetails() = default;
		BroadcastClientDetails(Future<Void> watcher,
		                       ConfigClassSet const& configClassSet,
		                       Version lastSeenVersion,
		                       ConfigBroadcastInterface broadcastInterface)
		  : watcher(watcher), configClassSet(configClassSet), lastSeenVersion(lastSeenVersion),
		    broadcastInterface(broadcastInterface) {}
	};

	std::map<ConfigKey, KnobValue> snapshot;
	std::deque<VersionedConfigMutation> mutationHistory;
	std::deque<VersionedConfigCommitAnnotation> annotationHistory;
	Version lastCompactedVersion;
	Version mostRecentVersion;
	std::unique_ptr<IConfigConsumer> consumer;
	Future<Void> consumerFuture;
	ActorCollection actors{ false };
	std::map<UID, BroadcastClientDetails> clients;
	std::map<UID, Future<Void>> clientFailures;

	UID id;
	CounterCollection cc;
	Counter compactRequest;
	Counter successfulChangeRequest;
	Counter failedChangeRequest;
	Counter snapshotRequest;
	Future<Void> logger;

	Future<Void> pushSnapshot(Version snapshotVersion, BroadcastClientDetails const& client) {
		if (client.lastSeenVersion >= snapshotVersion) {
			return Void();
		}

		++snapshotRequest;
		ConfigBroadcastSnapshotRequest request;
		for (const auto& [key, value] : snapshot) {
			if (matchesConfigClass(client.configClassSet, key.configClass)) {
				request.snapshot[key] = value;
			}
		}
		request.version = snapshotVersion;
		TraceEvent(SevDebug, "ConfigBroadcasterSnapshotRequest", id)
		    .detail("Size", request.snapshot.size())
		    .detail("Version", request.version);
		return success(brokenPromiseToNever(client.broadcastInterface.snapshot.getReply(request)));
	}

	template <class Changes>
	Future<Void> pushChanges(BroadcastClientDetails& client, Changes const& changes) {
		// Skip if client has already seen the latest version.
		if (client.lastSeenVersion >= mostRecentVersion) {
			return Void();
		}

		ConfigBroadcastChangesRequest req;
		for (const auto& versionedMutation : changes) {
			if (versionedMutation.version > client.lastSeenVersion &&
			    matchesConfigClass(client.configClassSet, versionedMutation.mutation.getConfigClass())) {
				TraceEvent te(SevDebug, "ConfigBroadcasterSendingChangeMutation", id);
				te.detail("Version", versionedMutation.version)
				    .detail("ReqLastSeenVersion", client.lastSeenVersion)
				    .detail("ConfigClass", versionedMutation.mutation.getConfigClass())
				    .detail("KnobName", versionedMutation.mutation.getKnobName())
				    .detail("ClientID", client.broadcastInterface.id());
				if (versionedMutation.mutation.isSet()) {
					te.detail("Op", "Set").detail("KnobValue", versionedMutation.mutation.getValue().toString());
				} else {
					te.detail("Op", "Clear");
				}

				req.changes.push_back_deep(req.changes.arena(), versionedMutation);
			}
		}

		if (req.changes.size() == 0) {
			return Void();
		}

		client.lastSeenVersion = mostRecentVersion;
		req.mostRecentVersion = mostRecentVersion;
		++successfulChangeRequest;
		return success(client.broadcastInterface.changes.getReply(req));
	}

	ConfigBroadcasterImpl()
	  : lastCompactedVersion(0), mostRecentVersion(0), id(deterministicRandom()->randomUniqueID()),
	    cc("ConfigBroadcaster"), compactRequest("CompactRequest", cc),
	    successfulChangeRequest("SuccessfulChangeRequest", cc), failedChangeRequest("FailedChangeRequest", cc),
	    snapshotRequest("SnapshotRequest", cc) {
		logger = traceCounters(
		    "ConfigBroadcasterMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "ConfigBroadcasterMetrics");
	}

	void addChanges(Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
	                Version mostRecentVersion,
	                Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations) {
		this->mostRecentVersion = mostRecentVersion;
		mutationHistory.insert(mutationHistory.end(), changes.begin(), changes.end());
		annotationHistory.insert(annotationHistory.end(), annotations.begin(), annotations.end());
		for (const auto& change : changes) {
			const auto& mutation = change.mutation;
			if (mutation.isSet()) {
				snapshot[mutation.getKey()] = mutation.getValue();
			} else {
				snapshot.erase(mutation.getKey());
			}
		}

		for (auto& [id, client] : clients) {
			actors.add(brokenPromiseToNever(pushChanges(client, changes)));
		}
	}

	template <class Snapshot>
	Future<Void> setSnapshot(Snapshot&& snapshot, Version snapshotVersion) {
		this->snapshot = std::forward<Snapshot>(snapshot);
		this->lastCompactedVersion = snapshotVersion;
		std::vector<Future<Void>> futures;
		for (const auto& [id, client] : clients) {
			futures.push_back(brokenPromiseToNever(pushSnapshot(snapshotVersion, client)));
		}
		return waitForAll(futures);
	}

	ACTOR template <class Snapshot>
	static Future<Void> pushSnapshotAndChanges(ConfigBroadcasterImpl* self,
	                                           Snapshot snapshot,
	                                           Version snapshotVersion,
	                                           Standalone<VectorRef<VersionedConfigMutationRef>> changes,
	                                           Version changesVersion,
	                                           Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> annotations) {
		// Make sure all snapshot messages were received before sending changes.
		wait(self->setSnapshot(snapshot, snapshotVersion));
		self->addChanges(changes, changesVersion, annotations);
		return Void();
	}

	ACTOR static Future<Void> waitForFailure(ConfigBroadcasterImpl* self, Future<Void> watcher, UID clientUID) {
		wait(watcher);
		TraceEvent(SevDebug, "ConfigBroadcastClientDied", self->id).detail("ClientID", clientUID);
		self->clients.erase(clientUID);
		// TODO: Erase clientUID from clientFailures at some point
		return Void();
	}

	ACTOR static Future<Void> registerWorker(ConfigBroadcaster* self,
	                                         ConfigBroadcasterImpl* impl,
	                                         Version lastSeenVersion,
	                                         ConfigClassSet configClassSet,
	                                         Future<Void> watcher,
	                                         ConfigBroadcastInterface broadcastInterface) {
		state BroadcastClientDetails client(
		    watcher, std::move(configClassSet), lastSeenVersion, std::move(broadcastInterface));
		if (!impl->consumerFuture.isValid()) {
			impl->consumerFuture = impl->consumer->consume(*self);
		}

		if (impl->clients.count(broadcastInterface.id())) {
			// Client already registered
			return Void();
		}

		TraceEvent(SevDebug, "ConfigBroadcasterRegisteringWorker", impl->id)
		    .detail("ClientID", broadcastInterface.id())
		    .detail("MostRecentVersion", impl->mostRecentVersion)
		    .detail("ClientLastSeenVersion", lastSeenVersion);
		// Push full snapshot to worker if it isn't up to date.
		wait(impl->pushSnapshot(impl->mostRecentVersion, client));
		impl->clients[broadcastInterface.id()] = client;
		impl->clientFailures[broadcastInterface.id()] = waitForFailure(impl, watcher, broadcastInterface.id());
		return Void();
	}

public:
	Future<Void> registerWorker(ConfigBroadcaster& self,
	                            Version lastSeenVersion,
	                            ConfigClassSet configClassSet,
	                            Future<Void> watcher,
	                            ConfigBroadcastInterface broadcastInterface) {
		return registerWorker(&self, this, lastSeenVersion, configClassSet, watcher, broadcastInterface);
	}

	void applyChanges(Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
	                  Version mostRecentVersion,
	                  Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations) {
		TraceEvent(SevDebug, "ConfigBroadcasterApplyingChanges", id)
		    .detail("ChangesSize", changes.size())
		    .detail("CurrentMostRecentVersion", this->mostRecentVersion)
		    .detail("NewMostRecentVersion", mostRecentVersion)
		    .detail("AnnotationsSize", annotations.size());
		addChanges(changes, mostRecentVersion, annotations);
	}

	template <class Snapshot>
	void applySnapshotAndChanges(Snapshot&& snapshot,
	                             Version snapshotVersion,
	                             Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
	                             Version changesVersion,
	                             Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations) {
		TraceEvent(SevDebug, "ConfigBroadcasterApplyingSnapshotAndChanges", id)
		    .detail("CurrentMostRecentVersion", this->mostRecentVersion)
		    .detail("SnapshotSize", snapshot.size())
		    .detail("SnapshotVersion", snapshotVersion)
		    .detail("ChangesSize", changes.size())
		    .detail("ChangesVersion", changesVersion)
		    .detail("AnnotationsSize", annotations.size());
		actors.add(pushSnapshotAndChanges(this, snapshot, snapshotVersion, changes, changesVersion, annotations));
	}

	ConfigBroadcasterImpl(ConfigFollowerInterface const& cfi) : ConfigBroadcasterImpl() {
		consumer = IConfigConsumer::createTestSimple(cfi, 0.5, Optional<double>{});
		TraceEvent(SevDebug, "ConfigBroadcasterStartingConsumer", id).detail("Consumer", consumer->getID());
	}

	ConfigBroadcasterImpl(ServerCoordinators const& coordinators, ConfigDBType configDBType) : ConfigBroadcasterImpl() {
		if (configDBType != ConfigDBType::DISABLED) {
			if (configDBType == ConfigDBType::SIMPLE) {
				consumer = IConfigConsumer::createSimple(coordinators, 0.5, Optional<double>{});
			} else {
				consumer = IConfigConsumer::createPaxos(coordinators, 0.5, Optional<double>{});
			}
			TraceEvent(SevDebug, "ConfigBroadcasterStartingConsumer", id)
			    .detail("Consumer", consumer->getID())
			    .detail("UsingSimpleConsumer", configDBType == ConfigDBType::SIMPLE);
		}
	}

	JsonBuilderObject getStatus() const {
		JsonBuilderObject result;
		JsonBuilderArray mutationsArray;
		for (const auto& versionedMutation : mutationHistory) {
			JsonBuilderObject mutationObject;
			mutationObject["version"] = versionedMutation.version;
			const auto& mutation = versionedMutation.mutation;
			mutationObject["config_class"] = mutation.getConfigClass().orDefault("<global>"_sr);
			mutationObject["knob_name"] = mutation.getKnobName();
			mutationObject["knob_value"] = mutation.getValue().toString();
			mutationsArray.push_back(std::move(mutationObject));
		}
		result["mutations"] = std::move(mutationsArray);
		JsonBuilderArray commitsArray;
		for (const auto& versionedAnnotation : annotationHistory) {
			JsonBuilderObject commitObject;
			commitObject["version"] = versionedAnnotation.version;
			commitObject["description"] = versionedAnnotation.annotation.description;
			commitObject["timestamp"] = versionedAnnotation.annotation.timestamp;
			commitsArray.push_back(std::move(commitObject));
		}
		result["commits"] = std::move(commitsArray);
		JsonBuilderObject snapshotObject;
		std::map<Optional<Key>, std::vector<std::pair<Key, Value>>> snapshotMap;
		for (const auto& [configKey, value] : snapshot) {
			snapshotMap[configKey.configClass.castTo<Key>()].emplace_back(configKey.knobName, value.toString());
		}
		for (const auto& [configClass, kvs] : snapshotMap) {
			JsonBuilderObject kvsObject;
			for (const auto& [knobName, knobValue] : kvs) {
				kvsObject[knobName] = knobValue;
			}
			snapshotObject[configClass.orDefault("<global>"_sr)] = std::move(kvsObject);
		}
		result["snapshot"] = std::move(snapshotObject);
		result["last_compacted_version"] = lastCompactedVersion;
		result["most_recent_version"] = mostRecentVersion;
		return result;
	}

	void compact(Version compactionVersion) {
		{
			auto it = std::find_if(mutationHistory.begin(), mutationHistory.end(), [compactionVersion](const auto& vm) {
				return vm.version > compactionVersion;
			});
			mutationHistory.erase(mutationHistory.begin(), it);
		}
		{
			auto it = std::find_if(annotationHistory.begin(),
			                       annotationHistory.end(),
			                       [compactionVersion](const auto& va) { return va.version > compactionVersion; });
			annotationHistory.erase(annotationHistory.begin(), it);
		}
	}

	Future<Void> getError() const { return consumerFuture || actors.getResult(); }

	Future<Void> getClientFailure(UID clientUID) const { return clientFailures.find(clientUID)->second; }

	UID getID() const { return id; }

	static void runPendingRequestStoreTest(bool includeGlobalMutation, int expectedMatches);
};

ConfigBroadcaster::ConfigBroadcaster(ConfigFollowerInterface const& cfi)
  : impl(PImpl<ConfigBroadcasterImpl>::create(cfi)) {}

ConfigBroadcaster::ConfigBroadcaster(ServerCoordinators const& coordinators, ConfigDBType configDBType)
  : impl(PImpl<ConfigBroadcasterImpl>::create(coordinators, configDBType)) {}

ConfigBroadcaster::ConfigBroadcaster(ConfigBroadcaster&&) = default;

ConfigBroadcaster& ConfigBroadcaster::operator=(ConfigBroadcaster&&) = default;

ConfigBroadcaster::~ConfigBroadcaster() = default;

Future<Void> ConfigBroadcaster::registerWorker(Version lastSeenVersion,
                                               ConfigClassSet const& configClassSet,
                                               Future<Void> watcher,
                                               ConfigBroadcastInterface broadcastInterface) {
	return impl->registerWorker(*this, lastSeenVersion, configClassSet, watcher, broadcastInterface);
}

void ConfigBroadcaster::applyChanges(Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
                                     Version mostRecentVersion,
                                     Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations) {
	impl->applyChanges(changes, mostRecentVersion, annotations);
}

void ConfigBroadcaster::applySnapshotAndChanges(
    std::map<ConfigKey, KnobValue> const& snapshot,
    Version snapshotVersion,
    Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
    Version changesVersion,
    Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations) {
	impl->applySnapshotAndChanges(snapshot, snapshotVersion, changes, changesVersion, annotations);
}

void ConfigBroadcaster::applySnapshotAndChanges(
    std::map<ConfigKey, KnobValue>&& snapshot,
    Version snapshotVersion,
    Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
    Version changesVersion,
    Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations) {
	impl->applySnapshotAndChanges(std::move(snapshot), snapshotVersion, changes, changesVersion, annotations);
}

Future<Void> ConfigBroadcaster::getError() const {
	return impl->getError();
}

Future<Void> ConfigBroadcaster::getClientFailure(UID clientUID) const {
	return impl->getClientFailure(clientUID);
}

UID ConfigBroadcaster::getID() const {
	return impl->getID();
}

JsonBuilderObject ConfigBroadcaster::getStatus() const {
	return impl->getStatus();
}

void ConfigBroadcaster::compact(Version compactionVersion) {
	impl->compact(compactionVersion);
}
