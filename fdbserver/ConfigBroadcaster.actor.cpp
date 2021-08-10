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
	};

	std::map<ConfigKey, KnobValue> snapshot;
	std::deque<VersionedConfigMutation> mutationHistory;
	std::deque<VersionedConfigCommitAnnotation> annotationHistory;
	Version lastCompactedVersion;
	Version mostRecentVersion;
	std::unique_ptr<IConfigConsumer> consumer;
	ActorCollection actors{ false };
	std::vector<BroadcastClientDetails> clients;

	UID id;
	CounterCollection cc;
	Counter compactRequest;
	Counter successfulChangeRequest;
	Counter failedChangeRequest;
	Counter snapshotRequest;
	Future<Void> logger;

	template <class Changes>
	Future<Void> pushChanges(BroadcastClientDetails& client, Changes const& changes) {
		if (client.watcher.isReady()) {
			clients.erase(std::remove(clients.begin(), clients.end(), client));
		}

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
				    .detail("KnobName", versionedMutation.mutation.getKnobName());
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
		// TODO: Retry in event of failure
		++successfulChangeRequest;
		return success(client.broadcastInterface.getChanges.getReply(req));
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

		for (auto& client : clients) {
			actors.add(pushChanges(client, changes));
		}
	}

	template <class Snapshot>
	Future<Void> setSnapshot(Snapshot&& snapshot, Version snapshotVersion) {
		this->snapshot = std::forward<Snapshot>(snapshot);
		this->lastCompactedVersion = snapshotVersion;
		return Void();
	}

	ACTOR static Future<Void> waitForFailure(ConfigBroadcasterImpl* self,
	                                         Future<Void> watcher,
	                                         BroadcastClientDetails* client) {
		wait(success(watcher));
		self->clients.erase(std::remove(self->clients.begin(), self->clients.end(), *client));
		return Void();
	}

public:
	Future<Void> registerWorker(ConfigBroadcaster* self,
	                            Version lastSeenVersion,
	                            ConfigClassSet configClassSet,
	                            Future<Void> watcher,
	                            ConfigBroadcastInterface broadcastInterface) {
		actors.add(consumer->consume(*self));
		clients.push_back(BroadcastClientDetails{
		    watcher, std::move(configClassSet), lastSeenVersion, std::move(broadcastInterface) });
		this->actors.add(waitForFailure(this, watcher, &clients.back()));

		// Push all dynamic knobs to worker if it isn't up to date.
		if (clients.back().lastSeenVersion >= mostRecentVersion) {
			return Void();
		}

		++snapshotRequest;
		ConfigBroadcastSnapshotRequest request;
		for (const auto& [key, value] : snapshot) {
			if (matchesConfigClass(clients.back().configClassSet, key.configClass)) {
				request.snapshot[key] = value;
			}
		}
		request.version = mostRecentVersion;
		TraceEvent(SevDebug, "ConfigBroadcasterSnapshotRequest", id)
		    .detail("Size", request.snapshot.size())
		    .detail("Version", request.version);
		return success(clients.back().broadcastInterface.getSnapshot.getReply(request));
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
		setSnapshot(std::forward<Snapshot>(snapshot), snapshotVersion);
		addChanges(changes, changesVersion, annotations);
	}

	ConfigBroadcasterImpl(ConfigFollowerInterface const& cfi) : ConfigBroadcasterImpl() {
		consumer = IConfigConsumer::createTestSimple(cfi, 0.5, Optional<double>{});
		TraceEvent(SevDebug, "ConfigBroadcasterStartingConsumer", id).detail("Consumer", consumer->getID());
	}

	ConfigBroadcasterImpl(ServerCoordinators const& coordinators, UseConfigDB useConfigDB) : ConfigBroadcasterImpl() {
		if (useConfigDB != UseConfigDB::DISABLED) {
			if (useConfigDB == UseConfigDB::SIMPLE) {
				consumer = IConfigConsumer::createSimple(coordinators, 0.5, Optional<double>{});
			} else {
				consumer = IConfigConsumer::createPaxos(coordinators, 0.5, Optional<double>{});
			}
			TraceEvent(SevDebug, "BroadcasterStartingConsumer", id)
			    .detail("Consumer", consumer->getID())
			    .detail("UsingSimpleConsumer", useConfigDB == UseConfigDB::SIMPLE);
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

	UID getID() const { return id; }

	static void runPendingRequestStoreTest(bool includeGlobalMutation, int expectedMatches);
};

ConfigBroadcaster::ConfigBroadcaster(ConfigFollowerInterface const& cfi)
  : _impl(std::make_unique<ConfigBroadcasterImpl>(cfi)) {}

ConfigBroadcaster::ConfigBroadcaster(ServerCoordinators const& coordinators, UseConfigDB useConfigDB)
  : _impl(std::make_unique<ConfigBroadcasterImpl>(coordinators, useConfigDB)) {}

ConfigBroadcaster::ConfigBroadcaster(ConfigBroadcaster&&) = default;

ConfigBroadcaster& ConfigBroadcaster::operator=(ConfigBroadcaster&&) = default;

ConfigBroadcaster::~ConfigBroadcaster() = default;

Future<Void> ConfigBroadcaster::registerWorker(Version lastSeenVersion,
                                               ConfigClassSet configClassSet,
                                               Future<Void> watcher,
                                               ConfigBroadcastInterface broadcastInterface) {
	return impl().registerWorker(this, lastSeenVersion, std::move(configClassSet), watcher, broadcastInterface);
}

void ConfigBroadcaster::applyChanges(Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
                                     Version mostRecentVersion,
                                     Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations) {
	impl().applyChanges(changes, mostRecentVersion, annotations);
}

void ConfigBroadcaster::applySnapshotAndChanges(
    std::map<ConfigKey, KnobValue> const& snapshot,
    Version snapshotVersion,
    Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
    Version changesVersion,
    Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations) {
	impl().applySnapshotAndChanges(snapshot, snapshotVersion, changes, changesVersion, annotations);
}

void ConfigBroadcaster::applySnapshotAndChanges(
    std::map<ConfigKey, KnobValue>&& snapshot,
    Version snapshotVersion,
    Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
    Version changesVersion,
    Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations) {
	impl().applySnapshotAndChanges(std::move(snapshot), snapshotVersion, changes, changesVersion, annotations);
}

UID ConfigBroadcaster::getID() const {
	return impl().getID();
}

JsonBuilderObject ConfigBroadcaster::getStatus() const {
	return impl().getStatus();
}

void ConfigBroadcaster::compact(Version compactionVersion) {
	impl().compact(compactionVersion);
}
