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
	// PendingRequestStore stores a set of pending ConfigBroadcastFollowerGetChangesRequests,
	// indexed by configuration class. When an update is received, replies are sent for all
	// pending requests with affected configuration classes
	class PendingRequestStore {
		using Req = ConfigBroadcastFollowerGetChangesRequest;
		std::map<Key, std::set<Endpoint::Token>> configClassToTokens;
		std::map<Endpoint::Token, Req> tokenToRequest;

	public:
		void addRequest(Req const& req) {
			auto token = req.reply.getEndpoint().token;
			tokenToRequest[token] = req;
			for (const auto& configClass : req.configClassSet.getClasses()) {
				configClassToTokens[configClass].insert(token);
			}
		}

		std::vector<Req> getRequestsToNotify(Standalone<VectorRef<VersionedConfigMutationRef>> const& changes) const {
			std::set<Endpoint::Token> tokenSet;
			for (const auto& change : changes) {
				if (!change.mutation.getConfigClass().present()) {
					// Update everything
					for (const auto& [token, req] : tokenToRequest) {
						if (req.lastSeenVersion < change.version) {
							tokenSet.insert(token);
						}
					}
				} else {
					Key configClass = change.mutation.getConfigClass().get();
					if (configClassToTokens.count(configClass)) {
						auto tokens = get(configClassToTokens, Key(change.mutation.getConfigClass().get()));
						for (const auto& token : tokens) {
							auto req = get(tokenToRequest, token);
							if (req.lastSeenVersion < change.version) {
								tokenSet.insert(token);
							} else {
								TEST(true); // Worker is ahead of config broadcaster
							}
						}
					}
				}
			}
			std::vector<Req> result;
			for (const auto& token : tokenSet) {
				result.push_back(get(tokenToRequest, token));
			}
			return result;
		}

		std::vector<Req> getOutdatedRequests(Version newSnapshotVersion) {
			std::vector<Req> result;
			for (const auto& [token, req] : tokenToRequest) {
				if (req.lastSeenVersion < newSnapshotVersion) {
					result.push_back(req);
				}
			}
			return result;
		}

		void removeRequest(Req const& req) {
			auto token = req.reply.getEndpoint().token;
			for (const auto& configClass : req.configClassSet.getClasses()) {
				remove(get(configClassToTokens, configClass), token);
				// TODO: Don't leak config classes
			}
			remove(tokenToRequest, token);
		}
	} pending;
	std::map<ConfigKey, KnobValue> snapshot;
	std::deque<VersionedConfigMutation> mutationHistory;
	std::deque<VersionedConfigCommitAnnotation> annotationHistory;
	Version lastCompactedVersion;
	Version mostRecentVersion;
	std::unique_ptr<IConfigConsumer> consumer;
	ActorCollection actors{ false };

	UID id;
	CounterCollection cc;
	Counter compactRequest;
	mutable Counter successfulChangeRequest;
	Counter failedChangeRequest;
	Counter snapshotRequest;
	Future<Void> logger;

	template <class Changes>
	void sendChangesReply(ConfigBroadcastFollowerGetChangesRequest const& req, Changes const& changes) const {
		ASSERT_LT(req.lastSeenVersion, mostRecentVersion);
		ConfigBroadcastFollowerGetChangesReply reply;
		reply.mostRecentVersion = mostRecentVersion;
		for (const auto& versionedMutation : changes) {
			if (versionedMutation.version > req.lastSeenVersion &&
			    matchesConfigClass(req.configClassSet, versionedMutation.mutation.getConfigClass())) {
				TraceEvent te(SevDebug, "ConfigBroadcasterSendingChangeMutation", id);
				te.detail("Version", versionedMutation.version)
				    .detail("ReqLastSeenVersion", req.lastSeenVersion)
				    .detail("ConfigClass", versionedMutation.mutation.getConfigClass())
				    .detail("KnobName", versionedMutation.mutation.getKnobName());
				if (versionedMutation.mutation.isSet()) {
					te.detail("Op", "Set").detail("KnobValue", versionedMutation.mutation.getValue().toString());
				} else {
					te.detail("Op", "Clear");
				}

				reply.changes.push_back_deep(reply.changes.arena(), versionedMutation);
			}
		}
		req.reply.send(reply);
		++successfulChangeRequest;
	}

	ACTOR static Future<Void> serve(ConfigBroadcaster* self,
	                                ConfigBroadcasterImpl* impl,
	                                ConfigBroadcastFollowerInterface cbfi) {
		impl->actors.add(impl->consumer->consume(*self));
		loop {
			choose {
				when(ConfigBroadcastFollowerGetSnapshotRequest req = waitNext(cbfi.getSnapshot.getFuture())) {
					++impl->snapshotRequest;
					ConfigBroadcastFollowerGetSnapshotReply reply;
					for (const auto& [key, value] : impl->snapshot) {
						if (matchesConfigClass(req.configClassSet, key.configClass)) {
							reply.snapshot[key] = value;
						}
					}
					reply.version = impl->mostRecentVersion;
					TraceEvent(SevDebug, "ConfigBroadcasterGotSnapshotRequest", impl->id)
					    .detail("Size", reply.snapshot.size())
					    .detail("Version", reply.version);
					req.reply.send(reply);
				}
				when(ConfigBroadcastFollowerGetChangesRequest req = waitNext(cbfi.getChanges.getFuture())) {
					if (req.lastSeenVersion < impl->lastCompactedVersion) {
						req.reply.sendError(version_already_compacted());
						++impl->failedChangeRequest;
						continue;
					}
					if (req.lastSeenVersion < impl->mostRecentVersion) {
						impl->sendChangesReply(req, impl->mutationHistory);
					} else {
						TEST(req.lastSeenVersion > impl->mostRecentVersion); // Worker is ahead of ConfigBroadcaster
						TraceEvent(SevDebug, "ConfigBroadcasterRegisteringChangeRequest", impl->id)
						    .detail("Peer", req.reply.getEndpoint().getPrimaryAddress())
						    .detail("MostRecentVersion", impl->mostRecentVersion)
						    .detail("ReqLastSeenVersion", req.lastSeenVersion)
						    .detail("ConfigClass", req.configClassSet);
						impl->pending.addRequest(req);
					}
				}
				when(wait(impl->actors.getResult())) { ASSERT(false); }
			}
		}
	}

	ConfigBroadcasterImpl()
	  : mostRecentVersion(0), lastCompactedVersion(0), id(deterministicRandom()->randomUniqueID()),
	    cc("ConfigBroadcaster"), compactRequest("CompactRequest", cc),
	    successfulChangeRequest("SuccessfulChangeRequest", cc), failedChangeRequest("FailedChangeRequest", cc),
	    snapshotRequest("SnapshotRequest", cc) {
		logger = traceCounters(
		    "ConfigBroadcasterMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "ConfigBroadcasterMetrics");
	}

	void notifyFollowers(Standalone<VectorRef<VersionedConfigMutationRef>> const& changes) {
		auto toNotify = pending.getRequestsToNotify(changes);
		TraceEvent(SevDebug, "ConfigBroadcasterNotifyingFollowers", id)
		    .detail("ChangesSize", changes.size())
		    .detail("ToNotify", toNotify.size());
		for (auto& req : toNotify) {
			sendChangesReply(req, changes);
			pending.removeRequest(req);
		}
	}

	void notifyOutdatedRequests() {
		auto outdated = pending.getOutdatedRequests(mostRecentVersion);
		for (auto& req : outdated) {
			req.reply.sendError(version_already_compacted());
			pending.removeRequest(req);
		}
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
	}

	template <class Snapshot>
	Future<Void> setSnapshot(Snapshot&& snapshot, Version snapshotVersion) {
		this->snapshot = std::forward<Snapshot>(snapshot);
		this->lastCompactedVersion = snapshotVersion;
		return Void();
	}

public:
	Future<Void> serve(ConfigBroadcaster* self, ConfigBroadcastFollowerInterface const& cbfi) {
		return serve(self, this, cbfi);
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
		notifyFollowers(changes);
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
		notifyOutdatedRequests();
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

Future<Void> ConfigBroadcaster::serve(ConfigBroadcastFollowerInterface const& cbfi) {
	return impl().serve(this, cbfi);
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

namespace {

Standalone<VectorRef<VersionedConfigMutationRef>> getTestChanges(Version version, bool includeGlobalMutation) {
	Standalone<VectorRef<VersionedConfigMutationRef>> changes;
	if (includeGlobalMutation) {
		ConfigKey key = ConfigKeyRef({}, "test_long"_sr);
		auto value = KnobValue::create(int64_t{ 5 });
		ConfigMutation mutation = ConfigMutationRef(key, value.contents());
		changes.emplace_back_deep(changes.arena(), version, mutation);
	}
	{
		ConfigKey key = ConfigKeyRef("class-A"_sr, "test_long"_sr);
		auto value = KnobValue::create(int64_t{ 5 });
		ConfigMutation mutation = ConfigMutationRef(key, value.contents());
		changes.emplace_back_deep(changes.arena(), version, mutation);
	}
	return changes;
}

ConfigBroadcastFollowerGetChangesRequest getTestRequest(Version lastSeenVersion,
                                                        std::vector<KeyRef> const& configClasses) {
	Standalone<VectorRef<KeyRef>> configClassesVector;
	for (const auto& configClass : configClasses) {
		configClassesVector.push_back_deep(configClassesVector.arena(), configClass);
	}
	return ConfigBroadcastFollowerGetChangesRequest{ lastSeenVersion, ConfigClassSet{ configClassesVector } };
}

} // namespace

void ConfigBroadcasterImpl::runPendingRequestStoreTest(bool includeGlobalMutation, int expectedMatches) {
	PendingRequestStore pending;
	for (Version v = 0; v < 5; ++v) {
		pending.addRequest(getTestRequest(v, {}));
		pending.addRequest(getTestRequest(v, { "class-A"_sr }));
		pending.addRequest(getTestRequest(v, { "class-B"_sr }));
		pending.addRequest(getTestRequest(v, { "class-A"_sr, "class-B"_sr }));
	}
	auto toNotify = pending.getRequestsToNotify(getTestChanges(0, includeGlobalMutation));
	ASSERT_EQ(toNotify.size(), 0);
	for (Version v = 1; v <= 5; ++v) {
		auto toNotify = pending.getRequestsToNotify(getTestChanges(v, includeGlobalMutation));
		ASSERT_EQ(toNotify.size(), expectedMatches);
		for (const auto& req : toNotify) {
			pending.removeRequest(req);
		}
	}
}

TEST_CASE("/fdbserver/ConfigDB/ConfigBroadcaster/Internal/PendingRequestStore/Simple") {
	ConfigBroadcasterImpl::runPendingRequestStoreTest(false, 2);
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/ConfigBroadcaster/Internal/PendingRequestStore/GlobalMutation") {
	ConfigBroadcasterImpl::runPendingRequestStoreTest(true, 4);
	return Void();
}
