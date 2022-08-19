/*
 * ClusterConnectionKey.actor.cpp
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

#include "fdbclient/ClusterConnectionKey.actor.h"
#include "flow/actorcompiler.h" // has to be last include

// Creates a cluster connection record with a given connection string and saves it to the specified key. Needs to be
// persisted should be set to true unless this ClusterConnectionKey is being created with the value read from the
// key.
ClusterConnectionKey::ClusterConnectionKey(Database db,
                                           Key connectionStringKey,
                                           ClusterConnectionString const& contents,
                                           ConnectionStringNeedsPersisted needsToBePersisted)
  : IClusterConnectionRecord(needsToBePersisted), db(db), connectionStringKey(connectionStringKey) {
	if (!needsToBePersisted) {
		lastPersistedConnectionString = ValueRef(contents.toString());
	}
	cs = contents;
}

// Loads and parses the connection string at the specified key, throwing errors if the file cannot be read or the
// format is invalid.
ACTOR Future<Reference<ClusterConnectionKey>> ClusterConnectionKey::loadClusterConnectionKey(Database db,
                                                                                             Key connectionStringKey) {
	state Transaction tr(db);
	loop {
		try {
			Optional<Value> v = wait(tr.get(connectionStringKey));
			if (!v.present()) {
				throw connection_string_invalid();
			}
			return makeReference<ClusterConnectionKey>(db,
			                                           connectionStringKey,
			                                           ClusterConnectionString(v.get().toString()),
			                                           ConnectionStringNeedsPersisted::False);
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Sets the connections string held by this object and persists it.
Future<Void> ClusterConnectionKey::setAndPersistConnectionString(ClusterConnectionString const& connectionString) {
	cs = connectionString;
	return success(persist());
}

// Get the connection string stored in the database.
ACTOR Future<ClusterConnectionString> ClusterConnectionKey::getStoredConnectionStringImpl(
    Reference<ClusterConnectionKey> self) {
	Reference<ClusterConnectionKey> cck =
	    wait(ClusterConnectionKey::loadClusterConnectionKey(self->db, self->connectionStringKey));
	return cck->cs;
}

Future<ClusterConnectionString> ClusterConnectionKey::getStoredConnectionString() {
	return getStoredConnectionStringImpl(Reference<ClusterConnectionKey>::addRef(this));
}

ACTOR Future<bool> ClusterConnectionKey::upToDateImpl(Reference<ClusterConnectionKey> self,
                                                      ClusterConnectionString* connectionString) {
	try {
		// the cluster file hasn't been created yet so there's nothing to check
		if (self->needsToBePersisted())
			return true;

		Reference<ClusterConnectionKey> temp =
		    wait(ClusterConnectionKey::loadClusterConnectionKey(self->db, self->connectionStringKey));
		*connectionString = temp->getConnectionString();
		return connectionString->toString() == self->cs.toString();
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "ClusterKeyError").error(e).detail("Key", self->connectionStringKey);
		return false; // Swallow the error and report that the file is out of date
	}
}

// Checks whether the connection string in the database matches the connection string stored in memory. The cluster
// string stored in the database is returned via the reference parameter connectionString.
Future<bool> ClusterConnectionKey::upToDate(ClusterConnectionString& connectionString) {
	return upToDateImpl(Reference<ClusterConnectionKey>::addRef(this), &connectionString);
}

// Returns the key where the connection string is stored.
std::string ClusterConnectionKey::getLocation() const {
	return printable(connectionStringKey);
}

// Creates a copy of this object with a modified connection string but that isn't persisted.
Reference<IClusterConnectionRecord> ClusterConnectionKey::makeIntermediateRecord(
    ClusterConnectionString const& connectionString) const {
	return makeReference<ClusterConnectionKey>(db, connectionStringKey, connectionString);
}

// Returns a string representation of this cluster connection record. This will include the type of record and the
// key where the record is stored.
std::string ClusterConnectionKey::toString() const {
	return "fdbkey://" + printable(connectionStringKey);
}

ACTOR Future<bool> ClusterConnectionKey::persistImpl(Reference<ClusterConnectionKey> self) {
	self->setPersisted();
	state Value newConnectionString = ValueRef(self->cs.toString());

	try {
		state Transaction tr(self->db);
		loop {
			try {
				Optional<Value> existingConnectionString = wait(tr.get(self->connectionStringKey));
				// Someone has already updated the connection string to what we want
				if (existingConnectionString.present() && existingConnectionString.get() == newConnectionString) {
					self->lastPersistedConnectionString = newConnectionString;
					return true;
				}
				// Someone has updated the connection string to something we didn't expect, in which case we leave it
				// alone. It's possible this could result in the stored string getting stuck if the connection string
				// changes twice and only the first change is recorded. If the process that wrote the first change dies
				// and no other process attempts to write the intermediate state, then only a newly opened connection
				// key would be able to update the state.
				else if (existingConnectionString.present() &&
				         existingConnectionString != self->lastPersistedConnectionString) {
					TraceEvent(SevWarnAlways, "UnableToChangeConnectionKeyDueToMismatch")
					    .detail("ConnectionKey", self->connectionStringKey)
					    .detail("NewConnectionString", newConnectionString)
					    .detail("ExpectedStoredConnectionString", self->lastPersistedConnectionString)
					    .detail("ActualStoredConnectionString", existingConnectionString);
					return false;
				}
				tr.set(self->connectionStringKey, newConnectionString);
				wait(tr.commit());

				self->lastPersistedConnectionString = newConnectionString;
				return true;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "UnableToChangeConnectionKey")
		    .error(e)
		    .detail("ConnectionKey", self->connectionStringKey)
		    .detail("ConnectionString", self->cs.toString());
	}

	return false;
};

// Writes the connection string to the database
Future<bool> ClusterConnectionKey::persist() {
	return persistImpl(Reference<ClusterConnectionKey>::addRef(this));
}