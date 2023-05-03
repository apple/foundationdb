/*
 * DDSharedContext.h
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
#ifndef FOUNDATIONDB_DDSHAREDCONTEXT_H
#define FOUNDATIONDB_DDSHAREDCONTEXT_H
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/ShardsAffectedByTeamFailure.h"
#include "fdbserver/DDShardTracker.h"
#include "fdbserver/DDRelocationQueue.h"
#include "fdbserver/DDTeamCollection.h"
#include "fdbserver/DataDistributorInterface.h"

// The common info shared by all DD components. Normally the DD components should share the reference to the same
// context.
// NOTE: We should avoid the shared class become an insanely large class, think twice before add member to it.
class DDSharedContext : public ReferenceCounted<DDSharedContext> {
	// FIXME(xwang) mark fields privates
public:
	std::unique_ptr<DDEnabledState>
	    ddEnabledState; // Note: don't operate directly because it's shared with snapshot server
	IDDShardTracker* shardTracker = nullptr;
	IDDRelocationQueue* relocationQueue = nullptr;
	std::vector<IDDTeamCollection*> teamCollections;

	// public:
	DataDistributorInterface interface;
	UID ddId;
	MoveKeysLock lock;
	bool trackerCancelled = false;
	DatabaseConfiguration configuration;

	Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure;
	Reference<AsyncVar<bool>> processingUnhealthy, processingWiggle;

	DDSharedContext() = default;

	explicit DDSharedContext(const DataDistributorInterface& iface) : DDSharedContext(iface.id()) { interface = iface; }

	explicit DDSharedContext(UID id)
	  : ddEnabledState(new DDEnabledState), ddId(id), shardsAffectedByTeamFailure(new ShardsAffectedByTeamFailure),
	    processingUnhealthy(new AsyncVar<bool>(false)), processingWiggle(new AsyncVar<bool>(false)) {}

	UID id() const { return ddId; }

	void markTrackerCancelled() { trackerCancelled = true; }

	bool isTrackerCancelled() const { return trackerCancelled; }

	decltype(auto) usableRegions() const { return configuration.usableRegions; }

	bool isDDEnabled() const { return ddEnabledState->isEnabled(); };
};

#endif // FOUNDATIONDB_DDSHAREDCONTEXT_H
