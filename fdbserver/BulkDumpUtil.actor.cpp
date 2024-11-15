/*
 * BulkDumpUtils.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/BulkDumpUtil.actor.h"
#include "flow/actorcompiler.h" // has to be last include

SSBulkDumpRequest getSSBulkDumpRequest(const std::map<std::string, std::vector<StorageServerInterface>>& locations,
                                       const BulkDumpState& bulkDumpState) {
	StorageServerInterface targetServer;
	std::vector<UID> otherServers;
	int dcid = 0;
	for (const auto& [_, dcServers] : locations) {
		if (dcid == 0) {
			const int idx = deterministicRandom()->randomInt(0, dcServers.size());
			targetServer = dcServers[idx];
		}
		for (int i = 0; i < dcServers.size(); i++) {
			if (dcServers[i].id() == targetServer.id()) {
				ASSERT_WE_THINK(dcid == 0);
			} else {
				otherServers.push_back(dcServers[i].id());
			}
		}
		dcid++;
	}
	return SSBulkDumpRequest(targetServer, otherServers, bulkDumpState);
}
