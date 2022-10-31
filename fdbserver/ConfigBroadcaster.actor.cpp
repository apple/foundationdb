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

#include "fdbclient/ClusterConnectionMemoryRecord.h"
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
	Version largestLiveVersion;
	Version mostRecentVersion;
	CoordinatorsHash coordinatorsHash;
	std::unique_ptr<IConfigConsumer> consumer;
	Future<Void> consumerFuture;
	ActorCollection actors{ false };
	std::unordered_map<NetworkAddress, Future<Void>> registrationActors;
	std::map<UID, BroadcastClientDetails> clients;
	std::map<UID, Future<Void>> clientFailures;

	// State related to changing coordinators

	// Used to read a snapshot from the previous coordinators after a change
	// coordinators command.
	Version maxLastSeenVersion = ::invalidVersion;
	Future<Optional<Value>> previousCoordinatorsFuture;
	std::unique_ptr<IConfigConsumer> previousCoordinatorsConsumer;
	Future<Void> previousCoordinatorsSnapshotFuture;

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
	std::unordered_set<NetworkAddress> registrationResponsesUnregistered;
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
		// TODO: Don't need a delay if there are no atomic changes.
		// Delay restarting the cluster controller to allow messages to be sent to workers.
		request.restartDelay = client.broadcastInterface.address() == g_network->getLocalAddress()
		                           ? SERVER_KNOBS->BROADCASTER_SELF_UPDATE_DELAY
		                           : 0.0;
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
		// TODO: Don't need a delay if there are no atomic changes.
		// Delay restarting the cluster controller to allow messages to be sent to workers.
		req.restartDelay = client.broadcastInterface.address() == g_network->getLocalAddress()
		                       ? SERVER_KNOBS->BROADCASTER_SELF_UPDATE_DELAY
		                       : 0.0;
		++successfulChangeRequest;
		return success(client.broadcastInterface.changes.getReply(req));
	}

	ConfigBroadcasterImpl()
	  : lastCompactedVersion(0), largestLiveVersion(0), mostRecentVersion(0),
	    id(deterministicRandom()->randomUniqueID()), cc("ConfigBroadcaster"), compactRequest("CompactRequest", cc),
	    successfulChangeRequest("SuccessfulChangeRequest", cc), failedChangeRequest("FailedChangeRequest", cc),
	    snapshotRequest("SnapshotRequest", cc) {
		logger = cc.traceCounters(
		    "ConfigBroadcasterMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, "ConfigBroadcasterMetrics");
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

	ACTOR static Future<Void> pushSnapshotAndChanges(ConfigBroadcasterImpl* self, Version snapshotVersion) {
		std::vector<Future<Void>> futures;
		for (const auto& [id, client] : self->clients) {
			futures.push_back(brokenPromiseToNever(self->pushSnapshot(snapshotVersion, client)));
		}
		wait(waitForAll(futures));
		return Void();
	}

	ACTOR static Future<Void> waitForFailure(ConfigBroadcasterImpl* self,
	                                         Future<Void> watcher,
	                                         UID clientUID,
	                                         NetworkAddress clientAddress,
	                                         bool isCoordinator) {
		wait(watcher);
		TraceEvent(SevDebug, "ConfigBroadcastClientDied", self->id)
		    .detail("ClientID", clientUID)
		    .detail("Address", clientAddress)
		    .detail("IsUnregistered",
		            self->registrationResponsesUnregistered.find(clientAddress) !=
		                self->registrationResponsesUnregistered.end())
		    .detail("IsActive", self->activeConfigNodes.find(clientAddress) != self->activeConfigNodes.end());
		self->clients.erase(clientUID);
		self->clientFailures.erase(clientUID);
		if (isCoordinator) {
			self->registrationResponses.erase(clientAddress);
			if (self->activeConfigNodes.find(clientAddress) != self->activeConfigNodes.end()) {
				self->activeConfigNodes.erase(clientAddress);
				if (self->registrationResponsesUnregistered.find(clientAddress) !=
				    self->registrationResponsesUnregistered.end()) {
					self->registrationResponsesUnregistered.erase(clientAddress);
					self->disallowUnregistered = false;
					// See comment where this promise is reset below.
					if (self->newConfigNodesAllowed.isSet()) {
						self->newConfigNodesAllowed.reset();
					}
				}
			}
		}
		return Void();
	}

	// Determines whether the registering ConfigNode is allowed to start
	// serving configuration database requests and snapshot data. In order to
	// ensure strict serializability, some nodes may be temporarily restricted
	// from participation until the other nodes in the system are brought up to
	// date.
	ACTOR static Future<Void> registerNodeInternal(ConfigBroadcaster* broadcaster,
	                                               ConfigBroadcasterImpl* self,
	                                               ConfigBroadcastInterface configBroadcastInterface) {
		if (self->configDBType == ConfigDBType::SIMPLE) {
			self->consumerFuture = self->consumer->consume(*broadcaster);
			wait(success(brokenPromiseToNever(
			    configBroadcastInterface.ready.getReply(ConfigBroadcastReadyRequest{ 0, {}, -1, -1 }))));
			return Void();
		}

		state NetworkAddress address = configBroadcastInterface.address();
		// Ask the registering ConfigNode whether it has registered in the past.
		state ConfigBroadcastRegisteredReply reply = wait(
		    brokenPromiseToNever(configBroadcastInterface.registered.getReply(ConfigBroadcastRegisteredRequest{})));
		self->maxLastSeenVersion = std::max(self->maxLastSeenVersion, reply.lastSeenVersion);
		state bool registered = reply.registered;
		TraceEvent("ConfigBroadcasterRegisterNodeReceivedRegistrationReply", self->id)
		    .detail("Address", address)
		    .detail("Registered", registered)
		    .detail("DisallowUnregistered", self->disallowUnregistered)
		    .detail("LastSeenVersion", reply.lastSeenVersion);

		if (self->activeConfigNodes.find(address) != self->activeConfigNodes.end()) {
			self->activeConfigNodes.erase(address);
			if (self->registrationResponsesUnregistered.find(address) !=
			    self->registrationResponsesUnregistered.end()) {
				self->registrationResponsesUnregistered.erase(address);
				// If an unregistered node died which was active, reset the
				// disallow unregistered flag so if it re-registers it can be
				// set as active again.
				self->disallowUnregistered = false;
				// Since a node can die and re-register before the broadcaster
				// receives notice that the node has died, we need to check for
				// re-registration of a node here. There are two places that can
				// reset the promise to allow new nodes, so make sure the promise
				// is actually set before resetting it. This prevents a node from
				// dying, registering, waiting on the promise, then the broadcaster
				// receives the notification the node has died and resets the
				// promise again.
				if (self->newConfigNodesAllowed.isSet()) {
					self->newConfigNodesAllowed.reset();
				}
			}
		}
		int responsesRemaining = self->coordinators - (int)self->registrationResponses.size();
		int nodesTillQuorum = self->coordinators / 2 + 1 - (int)self->activeConfigNodes.size();

		if (registered) {
			self->activeConfigNodes.insert(address);
			self->disallowUnregistered = true;
		} else if ((self->activeConfigNodes.size() < self->coordinators / 2 + 1 && !self->disallowUnregistered) ||
		           (self->registrationResponsesUnregistered.size() < self->coordinators / 2 &&
		            responsesRemaining <= nodesTillQuorum)) {
			// Received a registration request from an unregistered node. There
			// are two cases where we want to allow unregistered nodes to
			// register:
			// 	 * the cluster is just starting and no nodes are registered
			// 	 * there are registered and unregistered nodes, but the
			// 	   registered nodes may not represent a majority due to previous
			// 	   data loss. In this case, unregistered nodes must be allowed
			// 	   to register so they can be rolled forward and form a quorum.
			// 	   But only a minority of unregistered nodes should be allowed
			// 	   to register so they cannot override the registered nodes as
			// 	   a source of truth
			self->activeConfigNodes.insert(address);
			self->registrationResponsesUnregistered.insert(address);
			if ((self->activeConfigNodes.size() >= self->coordinators / 2 + 1 ||
			     self->registrationResponsesUnregistered.size() >= self->coordinators / 2 + 1) &&
			    self->newConfigNodesAllowed.canBeSet()) {
				self->newConfigNodesAllowed.send(Void());
			}
		} else {
			self->disallowUnregistered = true;
		}
		self->registrationResponses.insert(address);

		// Read previous coordinators and fetch snapshot from them if they
		// exist. This path should only be hit once after the coordinators are
		// changed.
		wait(yield());
		Optional<Value> previousCoordinators = wait(self->previousCoordinatorsFuture);
		TraceEvent("ConfigBroadcasterRegisterNodeReadPreviousCoordinators", self->id)
		    .detail("PreviousCoordinators", previousCoordinators)
		    .detail("HasStartedConsumer", self->previousCoordinatorsSnapshotFuture.isValid());

		if (previousCoordinators.present()) {
			if (!self->previousCoordinatorsSnapshotFuture.isValid()) {
				// Create a consumer to read a snapshot from the previous
				// coordinators. The snapshot will be forwarded to the new
				// coordinators to bring them up to date.
				size_t previousCoordinatorsHash = std::hash<std::string>()(previousCoordinators.get().toString());
				if (previousCoordinatorsHash != self->coordinatorsHash) {
					ServerCoordinators previousCoordinatorsData(Reference<IClusterConnectionRecord>(
					    new ClusterConnectionMemoryRecord(previousCoordinators.get().toString())));
					TraceEvent("ConfigBroadcasterRegisterNodeStartingConsumer", self->id).log();
					self->previousCoordinatorsConsumer = IConfigConsumer::createPaxos(
					    previousCoordinatorsData, 0.5, SERVER_KNOBS->COMPACTION_INTERVAL, true);
					self->previousCoordinatorsSnapshotFuture =
					    self->previousCoordinatorsConsumer->readSnapshot(*broadcaster);
				} else {
					// If the cluster controller restarts without a coordinator
					// change having taken place, there is no need to read a
					// previous snapshot.
					self->previousCoordinatorsSnapshotFuture = Void();
				}
			}
			wait(self->previousCoordinatorsSnapshotFuture);
		}

		state bool sendSnapshot =
		    self->previousCoordinatorsConsumer && reply.lastSeenVersion <= self->mostRecentVersion;
		// Unregistered nodes need to wait for either:
		//   1. A quorum of registered nodes to register and send their
		//      snapshots, so the unregistered nodes can be rolled forward, or
		//   2. A quorum of unregistered nodes to contact the broadcaster (this
		//      means there is no previous data in the configuration database)
		// The above conditions do not apply when changing coordinators, as a
		// snapshot of the current state of the configuration database needs to
		// be sent to all new coordinators.
		TraceEvent("ConfigBroadcasterRegisterNodeDetermineEligibility", self->id)
		    .detail("Registered", registered)
		    .detail("SendSnapshot", sendSnapshot);
		if (!registered && !sendSnapshot) {
			wait(self->newConfigNodesAllowed.getFuture());
		}

		sendSnapshot = sendSnapshot || (!registered && self->snapshot.size() > 0);
		TraceEvent("ConfigBroadcasterRegisterNodeSendingReadyRequest", self->id)
		    .detail("ConfigNodeAddress", address)
		    .detail("SendSnapshot", sendSnapshot)
		    .detail("SnapshotVersion", self->mostRecentVersion)
		    .detail("SnapshotSize", self->snapshot.size())
		    .detail("LargestLiveVersion", self->largestLiveVersion);
		if (sendSnapshot) {
			Version liveVersion = std::max(self->largestLiveVersion, self->mostRecentVersion);
			wait(success(brokenPromiseToNever(configBroadcastInterface.ready.getReply(ConfigBroadcastReadyRequest{
			    self->coordinatorsHash, self->snapshot, self->mostRecentVersion, liveVersion }))));
		} else {
			wait(success(brokenPromiseToNever(configBroadcastInterface.ready.getReply(
			    ConfigBroadcastReadyRequest{ self->coordinatorsHash, {}, -1, -1 }))));
		}

		// Start the consumer last, so at least some nodes will be registered.
		if (!self->consumerFuture.isValid()) {
			if (sendSnapshot) {
				self->consumer->allowSpecialCaseRollforward();
			}
			self->consumerFuture = self->consumer->consume(*broadcaster);
		}
		return Void();
	}

	ACTOR static Future<Void> registerNode(ConfigBroadcaster* self,
	                                       ConfigBroadcasterImpl* impl,
	                                       ConfigBroadcastInterface broadcastInterface,
	                                       Version lastSeenVersion,
	                                       ConfigClassSet configClassSet,
	                                       Future<Void> watcher,
	                                       bool isCoordinator) {
		state BroadcastClientDetails client(
		    watcher, std::move(configClassSet), lastSeenVersion, std::move(broadcastInterface));

		if (impl->clients.count(broadcastInterface.id())) {
			// Client already registered
			return Void();
		}

		TraceEvent(SevDebug, "ConfigBroadcasterRegisteringWorker", impl->id)
		    .detail("ClientID", broadcastInterface.id())
		    .detail("MostRecentVersion", impl->mostRecentVersion)
		    .detail("IsCoordinator", isCoordinator);

		if (isCoordinator) {
			// A client re-registering will cause the old actor to be
			// cancelled.
			impl->registrationActors[broadcastInterface.address()] =
			    registerNodeInternal(self, impl, broadcastInterface);
		}

		// Push full snapshot to worker if it isn't up to date.
		wait(impl->pushSnapshot(impl->mostRecentVersion, client));
		impl->clients[broadcastInterface.id()] = client;
		impl->clientFailures[broadcastInterface.id()] =
		    waitForFailure(impl, watcher, broadcastInterface.id(), broadcastInterface.address(), isCoordinator);
		return Void();
	}

public:
	Future<Void> registerNode(ConfigBroadcaster& self,
	                          ConfigBroadcastInterface const& broadcastInterface,
	                          Version lastSeenVersion,
	                          ConfigClassSet configClassSet,
	                          Future<Void> watcher,
	                          bool isCoordinator) {
		return registerNode(&self, this, broadcastInterface, lastSeenVersion, configClassSet, watcher, isCoordinator);
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
			TraceEvent(SevInfo, "ConfigBroadcasterApplyingChanges", id)
			    .detail("ChangesSize", changes.size())
			    .detail("AnnotationsSize", annotations.size())
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
	                             std::vector<ConfigFollowerInterface> const& readReplicas,
	                             Version largestLiveVersion,
	                             bool fromPreviousCoordinators) {
		TraceEvent(SevInfo, "ConfigBroadcasterApplyingSnapshotAndChanges", id)
		    .detail("CurrentMostRecentVersion", this->mostRecentVersion)
		    .detail("SnapshotSize", snapshot.size())
		    .detail("SnapshotVersion", snapshotVersion)
		    .detail("ChangesSize", changes.size())
		    .detail("ChangesVersion", changesVersion)
		    .detail("AnnotationsSize", annotations.size())
		    .detail("ActiveReplicas", readReplicas.size())
		    .detail("LargestLiveVersion", largestLiveVersion)
		    .detail("FromPreviousCoordinators", fromPreviousCoordinators);
		// Avoid updating state if the snapshot contains no mutations, or if it
		// contains old mutations. This can happen when the set of coordinators
		// is changed, and a new coordinator comes online that has not yet had
		// the current configuration database pushed to it, or when a new
		// coordinator contains state from an old configuration database
		// generation.
		if ((snapshot.size() != 0 || changes.size() != 0) &&
		    (snapshotVersion > this->mostRecentVersion || changesVersion > this->mostRecentVersion)) {
			this->snapshot = std::forward<Snapshot>(snapshot);
			this->lastCompactedVersion = snapshotVersion;
			this->largestLiveVersion = std::max(this->largestLiveVersion, largestLiveVersion);
			addChanges(changes, changesVersion, annotations);
			actors.add(pushSnapshotAndChanges(this, snapshotVersion));
		}

		if (!fromPreviousCoordinators) {
			updateKnownReplicas(readReplicas);
		}
	}

	ConfigBroadcasterImpl(ConfigFollowerInterface const& cfi) : ConfigBroadcasterImpl() {
		configDBType = ConfigDBType::SIMPLE;
		coordinators = 1;
		consumer = IConfigConsumer::createTestSimple(cfi, 0.5, Optional<double>{});
		TraceEvent(SevDebug, "ConfigBroadcasterStartingConsumer", id).detail("Consumer", consumer->getID());
	}

	ConfigBroadcasterImpl(ServerCoordinators const& coordinators,
	                      ConfigDBType configDBType,
	                      Future<Optional<Value>> previousCoordinatorsFuture)
	  : ConfigBroadcasterImpl() {
		this->configDBType = configDBType;
		this->coordinators = coordinators.configServers.size();
		if (configDBType != ConfigDBType::DISABLED) {
			if (configDBType == ConfigDBType::SIMPLE) {
				consumer = IConfigConsumer::createSimple(coordinators, 0.5, SERVER_KNOBS->COMPACTION_INTERVAL);
			} else {
				this->previousCoordinatorsFuture = previousCoordinatorsFuture;
				consumer = IConfigConsumer::createPaxos(coordinators, 0.5, SERVER_KNOBS->COMPACTION_INTERVAL);
			}

			coordinatorsHash = std::hash<std::string>()(coordinators.ccr->getConnectionString().toString());

			TraceEvent(SevInfo, "ConfigBroadcasterStartingConsumer", id)
			    .detail("Consumer", consumer->getID())
			    .detail("UsingSimpleConsumer", configDBType == ConfigDBType::SIMPLE)
			    .detail("CoordinatorsCount", this->coordinators)
			    .detail("CoordinatorsHash", coordinatorsHash)
			    .detail("CompactionInterval", SERVER_KNOBS->COMPACTION_INTERVAL);
		}
	}

	JsonBuilderObject getStatus() const {
		JsonBuilderObject result;
		JsonBuilderArray mutationsArray;
		for (const auto& versionedMutation : mutationHistory) {
			JsonBuilderObject mutationObject;
			mutationObject["version"] = versionedMutation.version;
			const auto& mutation = versionedMutation.mutation;
			mutationObject["type"] = mutation.isSet() ? "set" : "clear";
			mutationObject["config_class"] = mutation.getConfigClass().orDefault("<global>"_sr);
			mutationObject["knob_name"] = mutation.getKnobName();
			if (mutation.isSet()) {
				mutationObject["knob_value"] = mutation.getValue().toString();
			}
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
		lastCompactedVersion = compactionVersion;
	}

	Future<Void> getError() const { return consumerFuture || actors.getResult(); }

	Future<Void> getClientFailure(UID clientUID) const { return clientFailures.find(clientUID)->second; }

	UID getID() const { return id; }

	static void runPendingRequestStoreTest(bool includeGlobalMutation, int expectedMatches);
};

ConfigBroadcaster::ConfigBroadcaster() {}

ConfigBroadcaster::ConfigBroadcaster(ConfigFollowerInterface const& cfi)
  : impl(PImpl<ConfigBroadcasterImpl>::create(cfi)) {}

ConfigBroadcaster::ConfigBroadcaster(ServerCoordinators const& coordinators,
                                     ConfigDBType configDBType,
                                     Future<Optional<Value>> previousCoordinatorsFuture)
  : impl(PImpl<ConfigBroadcasterImpl>::create(coordinators, configDBType, previousCoordinatorsFuture)) {}

ConfigBroadcaster::ConfigBroadcaster(ConfigBroadcaster&&) = default;

ConfigBroadcaster& ConfigBroadcaster::operator=(ConfigBroadcaster&&) = default;

ConfigBroadcaster::~ConfigBroadcaster() = default;

Future<Void> ConfigBroadcaster::registerNode(ConfigBroadcastInterface const& broadcastInterface,
                                             Version lastSeenVersion,
                                             ConfigClassSet const& configClassSet,
                                             Future<Void> watcher,
                                             bool isCoordinator) {
	return impl->registerNode(*this, broadcastInterface, lastSeenVersion, configClassSet, watcher, isCoordinator);
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
    std::vector<ConfigFollowerInterface> const& readReplicas,
    Version largestLiveVersion,
    bool fromPreviousCoordinators) {
	impl->applySnapshotAndChanges(snapshot,
	                              snapshotVersion,
	                              changes,
	                              changesVersion,
	                              annotations,
	                              readReplicas,
	                              largestLiveVersion,
	                              fromPreviousCoordinators);
}

void ConfigBroadcaster::applySnapshotAndChanges(
    std::map<ConfigKey, KnobValue>&& snapshot,
    Version snapshotVersion,
    Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
    Version changesVersion,
    Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations,
    std::vector<ConfigFollowerInterface> const& readReplicas,
    Version largestLiveVersion,
    bool fromPreviousCoordinators) {
	impl->applySnapshotAndChanges(std::move(snapshot),
	                              snapshotVersion,
	                              changes,
	                              changesVersion,
	                              annotations,
	                              readReplicas,
	                              largestLiveVersion,
	                              fromPreviousCoordinators);
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

ACTOR static Future<Void> lockConfigNodesImpl(ServerCoordinators coordinators) {
	if (coordinators.configServers.empty()) {
		return Void();
	}

	CoordinatorsHash coordinatorsHash = std::hash<std::string>()(coordinators.ccr->getConnectionString().toString());

	std::vector<Future<Void>> lockRequests;
	lockRequests.reserve(coordinators.configServers.size());
	for (int i = 0; i < coordinators.configServers.size(); i++) {
		if (coordinators.configServers[i].hostname.present()) {
			lockRequests.push_back(retryGetReplyFromHostname(ConfigFollowerLockRequest{ coordinatorsHash },
			                                                 coordinators.configServers[i].hostname.get(),
			                                                 WLTOKEN_CONFIGFOLLOWER_LOCK));
		} else {
			lockRequests.push_back(
			    retryBrokenPromise(coordinators.configServers[i].lock, ConfigFollowerLockRequest{ coordinatorsHash }));
		}
	}
	int quorum_size = lockRequests.size() / 2 + 1;
	wait(quorum(lockRequests, quorum_size));
	return Void();
}

Future<Void> ConfigBroadcaster::lockConfigNodes(ServerCoordinators coordinators) {
	return lockConfigNodesImpl(coordinators);
}
