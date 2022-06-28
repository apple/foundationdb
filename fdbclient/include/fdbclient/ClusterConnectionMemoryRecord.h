/*
 * ClusterConnectionMemoryRecord.h
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
#ifndef FDBCLIENT_CLUSTERCONNECTIONMEMORYRECORD_H
#define FDBCLIENT_CLUSTERCONNECTIONMEMORYRECORD_H

#include "fdbclient/CoordinationInterface.h"

// An implementation of IClusterConnectionRecord that is stored in memory only and not persisted.
class ClusterConnectionMemoryRecord : public IClusterConnectionRecord,
                                      ReferenceCounted<ClusterConnectionMemoryRecord>,
                                      NonCopyable {
public:
	// Creates a cluster file with a given connection string.
	explicit ClusterConnectionMemoryRecord(ClusterConnectionString const& contents)
	  : IClusterConnectionRecord(ConnectionStringNeedsPersisted::False), id(deterministicRandom()->randomUniqueID()) {
		cs = contents;
	}

	// Sets the connections string held by this object.
	Future<Void> setAndPersistConnectionString(ClusterConnectionString const&) override;

	// Returns the connection string currently held in this object (there is no persistent storage).
	Future<ClusterConnectionString> getStoredConnectionString() override;

	// Because the memory record is not persisted, it is always up to date and this returns true. The connection string
	// is returned via the reference parameter connectionString.
	Future<bool> upToDate(ClusterConnectionString& fileConnectionString) override;

	// Returns a location string for the memory record that includes its ID.
	std::string getLocation() const override;

	// Returns a copy of this object with a modified connection string.
	Reference<IClusterConnectionRecord> makeIntermediateRecord(
	    ClusterConnectionString const& connectionString) const override;

	// Returns a string representation of this cluster connection record. This will include the type and id of the
	// record.
	std::string toString() const override;

	void addref() override { ReferenceCounted<ClusterConnectionMemoryRecord>::addref(); }
	void delref() override { ReferenceCounted<ClusterConnectionMemoryRecord>::delref(); }

protected:
	// This is a no-op for memory records. Returns true to indicate success.
	Future<bool> persist() override;

private:
	// A unique ID for the record
	UID id;
};

#endif
