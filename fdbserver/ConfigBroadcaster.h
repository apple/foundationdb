/*
 * ConfigBroadcaster.h
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

#pragma once

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/JsonBuilder.h"
#include "fdbclient/PImpl.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbserver/ConfigBroadcastInterface.h"
#include "fdbserver/ConfigFollowerInterface.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/flow.h"
#include <memory>

/*
 * The configuration broadcaster runs on the cluster controller. The broadcaster listens uses
 * an IConfigConsumer instantiation to consume updates from the configuration database, and broadcasts
 * these updates to all workers' local configurations
 */
class ConfigBroadcaster {
	PImpl<class ConfigBroadcasterImpl> impl;

public:
	explicit ConfigBroadcaster(ServerCoordinators const&, ConfigDBType);
	ConfigBroadcaster(ConfigBroadcaster&&);
	ConfigBroadcaster& operator=(ConfigBroadcaster&&);
	~ConfigBroadcaster();
	Future<Void> registerNode(WorkerInterface const& w,
	                          Version lastSeenVersion,
	                          ConfigClassSet const& configClassSet,
	                          Future<Void> watcher,
	                          ConfigBroadcastInterface const& worker);
	void applyChanges(Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
	                  Version mostRecentVersion,
	                  Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations,
	                  std::vector<ConfigFollowerInterface> const& readReplicas);
	void applySnapshotAndChanges(std::map<ConfigKey, KnobValue> const& snapshot,
	                             Version snapshotVersion,
	                             Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
	                             Version changesVersion,
	                             Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations,
	                             std::vector<ConfigFollowerInterface> const& readReplicas);
	void applySnapshotAndChanges(std::map<ConfigKey, KnobValue>&& snapshot,
	                             Version snapshotVersion,
	                             Standalone<VectorRef<VersionedConfigMutationRef>> const& changes,
	                             Version changesVersion,
	                             Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> const& annotations,
	                             std::vector<ConfigFollowerInterface> const& readReplicas);
	Future<Void> getError() const;
	UID getID() const;
	JsonBuilderObject getStatus() const;
	void compact(Version compactionVersion);

public: // Testing
	explicit ConfigBroadcaster(ConfigFollowerInterface const&);
	Future<Void> getClientFailure(UID clientUID) const;
};
