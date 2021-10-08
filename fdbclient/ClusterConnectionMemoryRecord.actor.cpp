/*
 * ClusterConnectionMemoryRecord.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

ClusterConnectionString const& ClusterConnectionMemoryRecord::getConnectionString() const {
	return cs;
}

Future<bool> ClusterConnectionMemoryRecord::upToDate(ClusterConnectionString& fileConnectionString) {
	return true;
}

Future<bool> ClusterConnectionMemoryRecord::persist() {
	return true;
}

Future<Void> ClusterConnectionMemoryRecord::setConnectionString(ClusterConnectionString const& conn) {
	cs = conn;
	return Void();
}

Future<ClusterConnectionString> ClusterConnectionMemoryRecord::getStoredConnectionString() {
	return cs;
}

Standalone<StringRef> ClusterConnectionMemoryRecord::getLocation() const {
	return StringRef(format("Memory: %s", id.toString().c_str()));
}

Reference<IClusterConnectionRecord> ClusterConnectionMemoryRecord::makeIntermediateRecord(
    ClusterConnectionString const& connectionString) const {
	return makeReference<ClusterConnectionMemoryRecord>(connectionString);
}

bool ClusterConnectionMemoryRecord::isValid() const {
	return valid;
}

std::string ClusterConnectionMemoryRecord::toString() const {
	if (isValid()) {
		return "Memory: " + id.toString();
	} else {
		return "Memory: <unset>";
	}
}