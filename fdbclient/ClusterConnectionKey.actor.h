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
#include "fdbclient/GenericTransactionHelper.h"
#include "fdbclient/NativeAPI.actor.h"
#include "flow/actorcompiler.h" // has to be last include

// An implementation of IClusterConnectionRecord backed by a key in a FoundationDB database.
template <class DB>
class ClusterConnectionKey : public IClusterConnectionRecord, ReferenceCounted<ClusterConnectionKey<DB>>, NonCopyable {
public:
	// Creates a cluster connection record with a given connection string and saves it to the specified key. Needs to be
	// persisted should be set to true unless this ClusterConnectionKey is being created with the value read from the
	// key.
	ClusterConnectionKey(DB db,
	                     Key connectionStringKey,
	                     ClusterConnectionString const& contents,
	                     ConnectionStringNeedsPersisted needsToBePersisted = ConnectionStringNeedsPersisted::True)
	  : IClusterConnectionRecord(needsToBePersisted), db(db), connectionStringKey(connectionStringKey) {
		if (!needsToBePersisted) {
			lastPersistedConnectionString = ValueRef(contents.toString());
		}
		cs = contents;
	}

	// Loads and parses the connection string at the specified key, throwing errors if the file cannot be read or
	// the format is invalid.
	ACTOR static Future<Reference<ClusterConnectionKey>> loadClusterConnectionKey(DB db, Key connectionStringKey) {
		state Reference<typename DB::TransactionT> tr = db->createTransaction();
		loop {
			try {
				typename transaction_future_type<Transaction, Optional<Value>>::type f = tr->get(connectionStringKey);
				Optional<Value> v = wait(safeThreadFutureToFuture(f));
				if (!v.present()) {
					throw connection_string_invalid();
				}
				return makeReference<ClusterConnectionKey>(db,
				                                           connectionStringKey,
				                                           ClusterConnectionString(v.get().toString()),
				                                           ConnectionStringNeedsPersisted::False);
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	// Sets the connections string held by this object and persists it.
	Future<Void> setAndPersistConnectionString(ClusterConnectionString const& connectionString) override {
		cs = connectionString;
		return success(persist());
	}

	// Get the connection string stored in the database.
	Future<ClusterConnectionString> getStoredConnectionString() override {
		return getStoredConnectionStringImpl(Reference<ClusterConnectionKey>::addRef(this));
	}

	// Checks whether the connection string in the database matches the connection string stored in memory. The
	// cluster string stored in the database is returned via the reference parameter connectionString.
	Future<bool> upToDate(ClusterConnectionString& connectionString) override {
		return upToDateImpl(Reference<ClusterConnectionKey>::addRef(this), &connectionString);
	}

	// Returns the key where the connection string is stored.
	std::string getLocation() const override { return printable(connectionStringKey); }

	// Creates a copy of this object with a modified connection string but that isn't persisted.
	Reference<IClusterConnectionRecord> makeIntermediateRecord(
	    ClusterConnectionString const& connectionString) const override {
		return makeReference<ClusterConnectionKey>(db, connectionStringKey, connectionString);
	}

	// Returns a string representation of this cluster connection record. This will include the type of record and
	// the key where the record is stored.
	std::string toString() const override { return "fdbkey://" + printable(connectionStringKey); }

	void addref() override { ReferenceCounted<ClusterConnectionKey>::addref(); }
	void delref() override { ReferenceCounted<ClusterConnectionKey>::delref(); }

protected:
	// Writes the connection string to the database
	Future<bool> persist() override { return persistImpl(Reference<ClusterConnectionKey>::addRef(this)); }

private:
	ACTOR static Future<ClusterConnectionString> getStoredConnectionStringImpl(Reference<ClusterConnectionKey> self) {
		Reference<ClusterConnectionKey> cck =
		    wait(ClusterConnectionKey::loadClusterConnectionKey(self->db, self->connectionStringKey));
		return cck->cs;
	}

	ACTOR static Future<bool> upToDateImpl(Reference<ClusterConnectionKey> self,
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

	ACTOR static Future<bool> persistImpl(Reference<ClusterConnectionKey> self) {
		self->setPersisted();
		state Value newConnectionString = ValueRef(self->cs.toString());

		try {
			state Reference<typename DB::TransactionT> tr = self->db->createTransaction();
			loop {
				try {
					typename transaction_future_type<Transaction, Optional<Value>>::type f =
					    tr->get(self->connectionStringKey);
					Optional<Value> existingConnectionString = wait(safeThreadFutureToFuture(f));
					// Someone has already updated the connection string to what we want
					if (existingConnectionString.present() && existingConnectionString.get() == newConnectionString) {
						self->lastPersistedConnectionString = newConnectionString;
						return true;
					}
					// Someone has updated the connection string to something we didn't expect, in which case we leave
					// it alone. It's possible this could result in the stored string getting stuck if the connection
					// string changes twice and only the first change is recorded. If the process that wrote the first
					// change dies and no other process attempts to write the intermediate state, then only a newly
					// opened connection key would be able to update the state.
					else if (existingConnectionString.present() &&
					         existingConnectionString != self->lastPersistedConnectionString) {
						TraceEvent(SevWarnAlways, "UnableToChangeConnectionKeyDueToMismatch")
						    .detail("ConnectionKey", self->connectionStringKey)
						    .detail("NewConnectionString", newConnectionString)
						    .detail("ExpectedStoredConnectionString", self->lastPersistedConnectionString)
						    .detail("ActualStoredConnectionString", existingConnectionString);
						return false;
					}
					tr->set(self->connectionStringKey, newConnectionString);
					wait(safeThreadFutureToFuture(tr->commit()));

					self->lastPersistedConnectionString = newConnectionString;
					return true;
				} catch (Error& e) {
					wait(safeThreadFutureToFuture(tr->onError(e)));
				}
			}
		} catch (Error& e) {
			TraceEvent(SevWarnAlways, "UnableToChangeConnectionKey")
			    .error(e)
			    .detail("ConnectionKey", self->connectionStringKey)
			    .detail("ConnectionString", self->cs.toString());
		}

		return false;
	}

	// The database where the connection key is stored. Note that this does not need to be the same database as the
	// one that the connection string would connect to.
	DB db;
	Key connectionStringKey;
	Optional<Value> lastPersistedConnectionString;
};

#include "flow/unactorcompiler.h"
#endif