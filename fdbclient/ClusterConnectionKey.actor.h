/*
 * ClusterConnectionKey.actor.h
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_CLUSTERCONNECTIONKEY_ACTOR_G_H)
#define FDBCLIENT_CLUSTERCONNECTIONKEY_ACTOR_G_H
#include "fdbclient/ClusterConnectionKey.actor.g.h"
#elif !defined(FDBCLIENT_CLUSTERCONNECTIONKEY_ACTOR_H)
#define FDBCLIENT_CLUSTERCONNECTIONKEY_ACTOR_H

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/NativeAPI.actor.h"
#include "flow/actorcompiler.h" // has to be last include

// An implementation of IClusterConnectionRecord backed by a key in a FoundationDB database.
class ClusterConnectionKey : public IClusterConnectionRecord, ReferenceCounted<ClusterConnectionKey>, NonCopyable {
public:
	// Creates a cluster connection record with a given connection string and saves it to the specified key. Needs to be
	// persisted should be set to true unless this ClusterConnectionKey is being created with the value read from the
	// key.
	ClusterConnectionKey(Database db,
	                     Key connectionStringKey,
	                     ClusterConnectionString const& contents,
	                     ConnectionStringNeedsPersisted needsToBePersisted = ConnectionStringNeedsPersisted::True);

	// Loads and parses the connection string at the specified key, throwing errors if the file cannot be read or the
	// format is invalid.
	ACTOR static Future<Reference<ClusterConnectionKey>> loadClusterConnectionKey(Database db, Key connectionStringKey);

	// Sets the connections string held by this object and persists it.
	Future<Void> setAndPersistConnectionString(ClusterConnectionString const&) override;

	// Get the connection string stored in the database.
	Future<ClusterConnectionString> getStoredConnectionString() override;

	// Checks whether the connection string in the database matches the connection string stored in memory. The cluster
	// string stored in the database is returned via the reference parameter connectionString.
	Future<bool> upToDate(ClusterConnectionString& connectionString) override;

	// Returns the key where the connection string is stored.
	std::string getLocation() const override;

	// Creates a copy of this object with a modified connection string but that isn't persisted.
	Reference<IClusterConnectionRecord> makeIntermediateRecord(
	    ClusterConnectionString const& connectionString) const override;

	// Returns a string representation of this cluster connection record. This will include the type of record and the
	// key where the record is stored.
	std::string toString() const override;

	void addref() override { ReferenceCounted<ClusterConnectionKey>::addref(); }
	void delref() override { ReferenceCounted<ClusterConnectionKey>::delref(); }

protected:
	// Writes the connection string to the database
	Future<bool> persist() override;

private:
	ACTOR static Future<ClusterConnectionString> getStoredConnectionStringImpl(Reference<ClusterConnectionKey> self);
	ACTOR static Future<bool> upToDateImpl(Reference<ClusterConnectionKey> self,
	                                       ClusterConnectionString* connectionString);
	ACTOR static Future<bool> persistImpl(Reference<ClusterConnectionKey> self);

	// The database where the connection key is stored. Note that this does not need to be the same database as the one
	// that the connection string would connect to.
	Database db;
	Key connectionStringKey;
	Optional<Value> lastPersistedConnectionString;
};

#include "flow/unactorcompiler.h"
#endif