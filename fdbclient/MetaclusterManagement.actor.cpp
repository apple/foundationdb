/*
 * MetaclusterManagement.actor.cpp
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

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "flow/actorcompiler.h" // has to be last include

namespace MetaclusterAPI {

ACTOR Future<Reference<IDatabase>> openDatabase(ClusterConnectionString connectionString) {
	Reference<IClusterConnectionRecord> clusterFile = makeReference<ClusterConnectionMemoryRecord>(connectionString);
	if (g_network->isSimulated()) {
		Database nativeDb = Database::createDatabase(clusterFile, -1);
		Reference<IDatabase> threadSafeDb =
		    wait(unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(nativeDb)));
		return MultiVersionDatabase::debugCreateFromExistingDatabase(threadSafeDb);
	} else {
		return MultiVersionApi::api->createDatabase(clusterFile);
	}
}
}; // namespace MetaclusterAPI