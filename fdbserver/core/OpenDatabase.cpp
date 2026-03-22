/*
 * OpenDatabase.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/ActorLineageProfiler.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/GlobalConfig.h"
#include "fdbclient/MonitorLeader.h"
#include "fdbserver/core/WorkerInterface.actor.h"

static Future<Void> extractClientInfo(Reference<AsyncVar<ServerDBInfo> const> db,
                                      Reference<AsyncVar<ClientDBInfo>> info) {
	std::vector<UID> lastCommitProxyUIDs;
	std::vector<CommitProxyInterface> lastCommitProxies;
	std::vector<UID> lastGrvProxyUIDs;
	std::vector<GrvProxyInterface> lastGrvProxies;
	while (true) {
		ClientDBInfo ni = db->get().client;
		shrinkProxyList(ni, lastCommitProxyUIDs, lastCommitProxies, lastGrvProxyUIDs, lastGrvProxies);
		info->setUnconditional(ni);
		co_await db->onChange();
	}
}

Database openDBOnServer(Reference<AsyncVar<ServerDBInfo> const> const& db,
                        TaskPriority taskID,
                        LockAware lockAware,
                        EnableLocalityLoadBalance enableLocalityLoadBalance) {
	auto info = makeReference<AsyncVar<ClientDBInfo>>();
	auto cx = DatabaseContext::create(info,
	                                  extractClientInfo(db, info),
	                                  enableLocalityLoadBalance ? db->get().myLocality : LocalityData(),
	                                  enableLocalityLoadBalance,
	                                  taskID,
	                                  lockAware);
	cx->globalConfig->init(db, std::addressof(db->get().client));
	cx->globalConfig->trigger(samplingFrequency, samplingProfilerUpdateFrequency);
	cx->globalConfig->trigger(samplingWindow, samplingProfilerUpdateWindow);
	return cx;
}
