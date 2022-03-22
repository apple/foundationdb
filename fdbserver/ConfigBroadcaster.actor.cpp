/*
 * ConfigBroadcaster.actor.cpp
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

	ConfigDBType configDBType;
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

	int coordinators = 0;
	std::unordered_set<NetworkAddress> activeConfigNodes;
	std::unordered_set<NetworkAddress> registrationResponses;
	bool disallowUnregistered = false;
	Promise<Void> newConfigNodesAllowed;

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

	ACTOR static Future<Void> waitForFailure(ConfigBroadcasterImpl* self,
	                                         Future<Void> watcher,
	                                         UID clientUID,
	                                         NetworkAddress clientAddress) {
		wait(watcher);
		TraceEvent(SevDebug, "ConfigBroadcastClientDied", self->id)
		    .detail("ClientID", clientUID)
		    .detail("Address", clientAddress);
		self->clients.erase(clientUID);
		self->clientFailures.erase(clientUID);
		self->activeConfigNodes.erase(clientAddress);
		self->registrationResponses.erase(clientAddress);
		// See comment where this promise is reset below.
		if (self->newConfigNodesAllowed.isSet()) {
			self->newConfigNodesAllowed.reset();
		}
		return Void();
	}

	// Determines whether the registering ConfigNode is allowed to start
	// serving configuration database requests and snapshot data. In order to
	// ensure strict serializability, some nodes may be temporarily restricted
	// from participation until the other nodes in the system are brought up to
	// date.
	ACTOR static Future<Void> registerNodeInternal(ConfigBroadcasterImpl* self,
	                                               WorkerInterface w,
	                                               Version lastSeenVersion) {
		if (self->configDBType == ConfigDBType::SIMPLE) {
			wait(success(retryBrokenPromise(w.configBroadcastInterface.ready, ConfigBroadcastReadyRequest{})));
			return Void();
		}

		state NetworkAddress address = w.address();

		// Ask the registering ConfigNode whether it has registered in the past.
		ConfigBroadcastRegisteredReply reply =
		    wait(w.configBroadcastInterface.registered.getReply(ConfigBroadcastRegisteredRequest{}));
		state bool registered = reply.registered;

		if (self->activeConfigNodes.find(address) != self->activeConfigNodes.end()) {
			self->activeConfigNodes.erase(address);
			// Since a node can die and re-register before the broadcaster
			// receives notice that the node has died, we need to check for
			// re-registration of a node here. There are two places that can
			// reset the promise to allow new nodes, make sure the promise is
			// actually set before resetting it. This prevents a node from
			// dying, registering, waiting on the promise, then the broadcaster
			// receives the notification the node has died and resets the
			// promise again.
			if (self->newConfigNodesAllowed.isSet()) {
				self->newConfigNodesAllowed.reset();
			}
		}
		self->registrationResponses.insert(address);

		if (registered) {
			if (!self->disallowUnregistered) {
				self->activeConfigNodes.clear();
			}
			self->activeConfigNodes.insert(address);
			self->disallowUnregistered = true;
		} else if ((self->activeConfigNodes.size() < self->coordinators / 2 + 1 && !self->disallowUnregistered) ||
		           self->coordinators - self->registrationResponses.size() <=
		               self->coordinators / 2 + 1 - self->activeConfigNodes.size()) {
			// Received a registration request from an unregistered node. There
			// are two cases where we want to allow unregistered nodes to
			// register:
			// 	 * the cluster is just starting and no nodes are registered
			// 	 * a minority of nodes are registered and a majority are
			// 	   unregistered. This situation should only occur in rare
			// 	   circumstances where the cluster controller dies with only a
			// 	   minority of config nodes having received a
			// 	   ConfigBroadcastReadyRequest
			self->activeConfigNodes.insert(address);
			if (self->activeConfigNodes.size() >= self->coordinators / 2 + 1 &&
			    self->newConfigNodesAllowed.canBeSet()) {
				self->newConfigNodesAllowed.send(Void());
			}
		} else {
			self->disallowUnregistered = true;
		}

		if (!registered) {
			wait(self->newConfigNodesAllowed.getFuture());
		}

		wait(success(w.configBroadcastInterface.ready.getReply(ConfigBroadcastReadyRequest{})));
		return Void();
	}

	ACTOR static Future<Void> registerNode(ConfigBroadcaster* self,
	                                       ConfigBroadcasterImpl* impl,
	                                       WorkerInterface w,
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
		    .detail("MostRecentVersion", impl->mostRecentVersion);

		impl->actors.add(registerNodeInternal(impl, w, lastSeenVersion));

		// Push full snapshot to worker if it isn't up to date.
		wait(impl->pushSnapshot(impl->mostRecentVersion, client));
		impl->clients[broadcastInterface.id()] = client;
		impl->clientFailures[broadcastInterface.id()] =
		    waitForFailure(impl, watcher, broadcastInterface.id(), w.address());
		return Void();
	}

public:
	Future<Void> registerNode(ConfigBroadcaster& self,
	                          WorkerInterface const& w,
	                          Version lastSeenVersion,
	                          ConfigClassSet configClassSet,
	                          Future<Void> watcher,
	                          ConfigBroadcastInterface const& broadcastInterface) {
		return registerNode(&self, this, w, lastSeenVersion, configClassSet, watcher, broadcastInterface);
	}

	// Updates the broadcasters knowledge of which replicas are fully up to
	// date, based on data gathered by the consumer.
	void updateKnownReplicas(std::vector<ConfigFollowerInterface> const& readReplicas) {
		if (!newConfigNodesAllowed.canBeSet()) {
			return;
		}

		for (const auto& cfi : readReplicas) {
			this->activeConfigNodes.insert(cfi.address());
		}
		if (activeConfigNodes.size() >= coordinators / 2 + 1) {
			disallowUnregistered = true;
			newConfigNodesAllowed.send(Void());
		}
	}

	void applyChanges(Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
	                  Version mostRecentVersion,
	                  Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations,
	                  std::vector<ConfigFollowerInterface> const& readReplicas) {
		if (mostRecentVersion >= 0) {
			TraceEvent(SevDebug, "ConfigBroadcasterApplyingChanges", id)
			    .detail("ChangesSize", changes.size())
			    .detail("CurrentMostRecentVersion", this->mostRecentVersion)
			    .detail("NewMostRecentVersion", mostRecentVersion)
			    .detail("ActiveReplicas", readReplicas.size());
			addChanges(changes, mostRecentVersion, annotations);
		}

		updateKnownReplicas(readReplicas);
	}

	template <class Snapshot>
	void applySnapshotAndChanges(Snapshot&& snapshot,
	                             Version snapshotVersion,
	                             Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
	                             Version changesVersion,
	                             Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations,
	                             std::vector<ConfigFollowerInterface> const& readReplicas) {
		TraceEvent(SevDebug, "ConfigBroadcasterApplyingSnapshotAndChanges", id)
		    .detail("CurrentMostRecentVersion", this->mostRecentVersion)
		    .detail("SnapshotSize", snapshot.size())
		    .detail("SnapshotVersion", snapshotVersion)
		    .detail("ChangesSize", changes.size())
		    .detail("ChangesVersion", changesVersion)
		    .detail("ActiveReplicas", readReplicas.size());
		actors.add(pushSnapshotAndChanges(this, snapshot, snapshotVersion, changes, changesVersion, annotations));

		updateKnownReplicas(readReplicas);
	}

	ConfigBroadcasterImpl(ConfigFollowerInterface const& cfi) : ConfigBroadcasterImpl() {
		configDBType = ConfigDBType::SIMPLE;
		coordinators = 1;
		consumer = IConfigConsumer::createTestSimple(cfi, 0.5, Optional<double>{});
		TraceEvent(SevDebug, "ConfigBroadcasterStartingConsumer", id).detail("Consumer", consumer->getID());
	}

	ConfigBroadcasterImpl(ServerCoordinators const& coordinators, ConfigDBType configDBType) : ConfigBroadcasterImpl() {
		this->configDBType = configDBType;
		this->coordinators = coordinators.configServers.size();
		if (configDBType != ConfigDBType::DISABLED) {
			if (configDBType == ConfigDBType::SIMPLE) {
				consumer = IConfigConsumer::createSimple(coordinators, 0.5, SERVER_KNOBS->COMPACTION_INTERVAL);
			} else {
				consumer = IConfigConsumer::createPaxos(coordinators, 0.5, SERVER_KNOBS->COMPACTION_INTERVAL);
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

Future<Void> ConfigBroadcaster::registerNode(WorkerInterface const& w,
                                             Version lastSeenVersion,
                                             ConfigClassSet const& configClassSet,
                                             Future<Void> watcher,
                                             ConfigBroadcastInterface const& broadcastInterface) {
	return impl->registerNode(*this, w, lastSeenVersion, configClassSet, watcher, broadcastInterface);
}

void ConfigBroadcaster::applyChanges(Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
                                     Version mostRecentVersion,
                                     Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations,
                                     std::vector<ConfigFollowerInterface> const& readReplicas) {
	impl->applyChanges(changes, mostRecentVersion, annotations, readReplicas);
}

void ConfigBroadcaster::applySnapshotAndChanges(
    std::map<ConfigKey, KnobValue> const& snapshot,
    Version snapshotVersion,
    Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
    Version changesVersion,
    Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations,
    std::vector<ConfigFollowerInterface> const& readReplicas) {
	impl->applySnapshotAndChanges(snapshot, snapshotVersion, changes, changesVersion, annotations, readReplicas);
}

void ConfigBroadcaster::applySnapshotAndChanges(
    std::map<ConfigKey, KnobValue>&& snapshot,
    Version snapshotVersion,
    Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
    Version changesVersion,
    Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations,
    std::vector<ConfigFollowerInterface> const& readReplicas) {
	impl->applySnapshotAndChanges(
	    std::move(snapshot), snapshotVersion, changes, changesVersion, annotations, readReplicas);
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
