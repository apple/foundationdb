/*
 * ResolutionBalancer.actor.h
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

#include "fdbclient/CommitProxyInterface.h"
#include "fdbserver/ResolverInterface.h"
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_RESOLUTION_BALANCER_G_H)
#define FDBSERVER_RESOLUTION_BALANCER_G_H
#include "fdbserver/ResolutionBalancer.actor.g.h"
#elif !defined(FDBSERVER_RESOLUTION_BALANCER_H)
#define FDBSERVER_RESOLUTION_BALANCER_H

#include <set>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/MasterInterface.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/genericactors.actor.h"

struct ResolutionBalancer {
	AsyncVar<Standalone<VectorRef<ResolverMoveRef>>> resolverChanges;
	Version resolverChangesVersion = invalidVersion;
	std::set<UID> resolverNeedingChanges;

	Version* pVersion; // points to MasterData::version

	std::vector<CommitProxyInterface> commitProxies;
	std::vector<ResolverInterface> resolvers;
	AsyncTrigger triggerResolution;

	ResolutionBalancer(Version* version) : pVersion(version) {}

	Future<Void> resolutionBalancing() { return resolutionBalancing_impl(this); }

	ACTOR static Future<Void> resolutionBalancing_impl(ResolutionBalancer* self);

	// Sets resolver interfaces. Trigger resolutionBalancing() actor if more
	// than one resolvers are present.
	void setResolvers(const std::vector<ResolverInterface>& resolvers);

	void setCommitProxies(const std::vector<CommitProxyInterface>& proxies) { commitProxies = proxies; }

	void setChangesInReply(UID requestingProxy, GetCommitVersionReply& rep);
};

#include "flow/unactorcompiler.h"
#endif
