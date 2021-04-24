/*
 * SimpleConfigBroadcaster.actor.cpp
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

#include "fdbserver/IConfigBroadcaster.h"

class SimpleConfigBroadcasterImpl {
	Reference<ConfigFollowerInterface> subscriber;
	std::map<Key, Value> database;
	// TODO: Should create fewer arenas
	std::deque<Standalone<VersionedMutationRef>> versionedMutations;
	Version lastCompactedVersion;
	Version mostRecentVersion;
	ActorCollection actors{ false };

	static const double POLLING_INTERVAL; // TODO: Make knob?

	ACTOR static Future<Void> fetchUpdates(SimpleConfigBroadcasterImpl *self) {
		loop {
			ConfigFollowerGetChangesReply reply = wait(
			    self->subscriber->getChanges.getReply(ConfigFollowerGetChangesRequest{ self->mostRecentVersion, {} }));
			for (const auto &versionedMutation : reply.versionedMutations) {
				self->versionedMutations.push_back(versionedMutation);
			}
			self->mostRecentVersion = reply.mostRecentVersion;
			wait(delay(POLLING_INTERVAL));
		}
	}

	void traceQueuedMutations() {
		TraceEvent te("SimpleConfigBroadcasterQueuedMutations");
		te.detail("Size", versionedMutations.size());
		int index = 0;
		for (const auto &versionedMutation : versionedMutations) {
			te.detail(format("Version%d", index), versionedMutation.version);
			te.detail(format("Mutation%d", index), versionedMutation.mutation.type);
			te.detail(format("FirstParam%d", index), versionedMutation.mutation.param1);
			te.detail(format("SecondParam%d", index), versionedMutation.mutation.param2);
			++index;
		}
	}

	static void removeRange(std::map<Key, Value> &database, KeyRef begin, KeyRef end) {
		auto b = database.lower_bound(begin);
		auto e = database.lower_bound(end);
		if (e != database.end() && e->first == end) {
			++e;
		}
		database.erase(b, e);
	}

	ACTOR static Future<Void> serve(SimpleConfigBroadcasterImpl* self, Reference<ConfigFollowerInterface> publisher) {
		ConfigFollowerGetVersionReply versionReply =
		    wait(self->subscriber->getVersion.getReply(ConfigFollowerGetVersionRequest{}));
		self->mostRecentVersion = versionReply.version;
		ConfigFollowerGetFullDatabaseReply reply = wait(self->subscriber->getFullDatabase.getReply(
		    ConfigFollowerGetFullDatabaseRequest{ self->mostRecentVersion, Optional<Value>{} }));
		self->database = reply.database;
		self->actors.add(fetchUpdates(self));
		loop {
			//self->traceQueuedMutations();
			choose {
				when(ConfigFollowerGetVersionRequest req = waitNext(publisher->getVersion.getFuture())) {
					req.reply.send(self->mostRecentVersion);
				}
				when(ConfigFollowerGetFullDatabaseRequest req = waitNext(publisher->getFullDatabase.getFuture())) {
					ConfigFollowerGetFullDatabaseReply reply;
					reply.database = self->database;
					for (const auto &versionedMutation : self->versionedMutations) {
						const auto &version = versionedMutation.version;
						const auto &mutation = versionedMutation.mutation;
						if (version > req.version) {
							break;
						}
						if (mutation.type == MutationRef::SetValue) {
							reply.database[mutation.param1] = mutation.param2;
						} else if (mutation.type == MutationRef::ClearRange) {
							removeRange(reply.database, mutation.param1, mutation.param2);
						} else {
							ASSERT(false);
						}
					}
					req.reply.send(ConfigFollowerGetFullDatabaseReply{ self->database });
				}
				when(ConfigFollowerGetChangesRequest req = waitNext(publisher->getChanges.getFuture())) {
					if (req.lastSeenVersion < self->lastCompactedVersion) {
						req.reply.sendError(version_already_compacted());
						continue;
					}
					ConfigFollowerGetChangesReply reply;
					reply.mostRecentVersion = self->mostRecentVersion;
					for (const auto &versionedMutation : self->versionedMutations) {
						if (versionedMutation.version > req.lastSeenVersion) {
							reply.versionedMutations.push_back_deep(reply.versionedMutations.arena(),
							                                        versionedMutation);
						}
					}
					req.reply.send(reply);
				}
				when(ConfigFollowerCompactRequest req = waitNext(publisher->compact.getFuture())) {
					while (!self->versionedMutations.empty()) {
						const auto& versionedMutation = self->versionedMutations.front();
						const auto& version = versionedMutation.version;
						const auto& mutation = versionedMutation.mutation;
						if (version > req.version) {
							break;
						} else if (mutation.type == MutationRef::SetValue) {
							self->database[mutation.param1] = mutation.param2;
						} else if (mutation.type == MutationRef::ClearRange) {
							removeRange(self->database, mutation.param1, mutation.param2);
						} else {
							ASSERT(false);
						}
						self->lastCompactedVersion = version;
						self->versionedMutations.pop_front();
					}
					req.reply.send(Void());
				}
				when(wait(self->actors.getResult())) { ASSERT(false); }
			}
		}
	}

public:
	SimpleConfigBroadcasterImpl(ClusterConnectionString const& ccs) : lastCompactedVersion(0), mostRecentVersion(0) {
		auto coordinators = ccs.coordinators();
		std::sort(coordinators.begin(), coordinators.end());
		subscriber = makeReference<ConfigFollowerInterface>(coordinators[0]);
	}

	Future<Void> serve(Reference<ConfigFollowerInterface> publisher) { return serve(this, publisher); }
};

const double SimpleConfigBroadcasterImpl::POLLING_INTERVAL = 0.5;

SimpleConfigBroadcaster::SimpleConfigBroadcaster(ClusterConnectionString const& ccs)
  : impl(std::make_unique<SimpleConfigBroadcasterImpl>(ccs)) {}

SimpleConfigBroadcaster::~SimpleConfigBroadcaster() = default;

Future<Void> SimpleConfigBroadcaster::serve(Reference<ConfigFollowerInterface> publisher) {
	return impl->serve(publisher);
}
