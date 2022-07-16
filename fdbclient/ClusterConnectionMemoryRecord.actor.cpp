/*
 * ClusterConnectionMemoryRecord.actor.cpp
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

#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/MonitorLeader.h"
#include "flow/actorcompiler.h" // has to be last include

// Sets the connections string held by this object.
Future<Void> ClusterConnectionMemoryRecord::setAndPersistConnectionString(ClusterConnectionString const& conn) {
	cs = conn;
	return Void();
}

// Returns the connection string currently held in this object (there is no persistent storage).
Future<ClusterConnectionString> ClusterConnectionMemoryRecord::getStoredConnectionString() {
	return cs;
}

// Because the memory record is not persisted, it is always up to date and this returns true. The connection string
// is returned via the reference parameter connectionString.
Future<bool> ClusterConnectionMemoryRecord::upToDate(ClusterConnectionString& fileConnectionString) {
	fileConnectionString = cs;
	return true;
}

// Returns the ID of the memory record.
std::string ClusterConnectionMemoryRecord::getLocation() const {
	return id.toString();
}

// Returns a copy of this object with a modified connection string.
Reference<IClusterConnectionRecord> ClusterConnectionMemoryRecord::makeIntermediateRecord(
    ClusterConnectionString const& connectionString) const {
	return makeReference<ClusterConnectionMemoryRecord>(connectionString);
}

// Returns a string representation of this cluster connection record. This will include the type and id of the
// record.
std::string ClusterConnectionMemoryRecord::toString() const {
	return "memory://" + id.toString();
}

// This is a no-op for memory records. Returns true to indicate success.
Future<bool> ClusterConnectionMemoryRecord::persist() {
	return true;
}