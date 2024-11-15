/*
 * BulkDumpUtil.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_BULKDUMPUTIL_ACTOR_G_H)
#define FDBSERVER_BULKDUMPUTIL_ACTOR_G_H
#include "fdbserver/BulkDumpUtil.actor.g.h"
#elif !defined(FDBSERVER_BULKDUMPUTIL_ACTOR_H)
#define FDBSERVER_BULKDUMPUTIL_ACTOR_H
#pragma once

#include "fdbclient/BulkDumping.h"
#include "fdbclient/StorageServerInterface.h"
#include "flow/actorcompiler.h" // has to be last include

struct SSBulkDumpRequest {
public:
	SSBulkDumpRequest(const StorageServerInterface& targetServer,
	                  const std::vector<UID>& otherServers,
	                  const BulkDumpState& bulkDumpState)
	  : targetServer(targetServer), otherServers(otherServers), bulkDumpState(bulkDumpState){};

	std::string toString() const {
		return "[BulkDumpState]: " + bulkDumpState.toString() + ", [TargetServer]: " + targetServer.toString() +
		       ", [OtherServers]: " + describe(otherServers);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, targetServer, otherServers, bulkDumpState);
	}

private:
	StorageServerInterface targetServer;
	std::vector<UID> otherServers;
	BulkDumpState bulkDumpState;
};

SSBulkDumpRequest getSSBulkDumpRequest(const std::map<std::string, std::vector<StorageServerInterface>>& locations,
                                       const BulkDumpState& bulkDumpState);

#include "flow/unactorcompiler.h"
#endif
