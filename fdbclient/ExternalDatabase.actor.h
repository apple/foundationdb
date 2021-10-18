
/*
 * ExternalDatabase.actor.h
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

#pragma once

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_EXTERNALDATABASE_ACTOR_G_H)
#define FDBCLIENT_EXTERNALDATABASE_ACTOR_G_H
#include "fdbclient/ExternalDatabase.actor.g.h"
#elif !defined(FDBCLIENT_EXTERNALDATABASE_ACTOR_H)
#define FDBCLIENT_EXTERNALDATABASE_ACTOR_H

#include "fdbclient/ClusterConnectionKey.actor.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/NativeAPI.actor.h"
#include "flow/actorcompiler.h" // has to be last include

struct ExternalDatabase : ReferenceCounted<ExternalDatabase> {
	ExternalDatabase(Database db, std::function<void()> onClose) : db(db), onClose(onClose) {}
	~ExternalDatabase() { onClose(); }

	Database db;
	std::function<void()> onClose;
};

class ExternalDatabaseMap {
public:
	ExternalDatabaseMap(UID id, LocalityData locality) : id(id), locality(locality) {}
	ExternalDatabaseMap(Database db, UID id, LocalityData locality) : db(db), id(id), locality(locality) {}

	Future<Optional<Reference<ExternalDatabase>>> getOrCreate(std::string name, std::string connectionString) {
		return getOrCreateImpl(this, name, connectionString);
	}

	Optional<Reference<ExternalDatabase>> get(std::string name) const {
		auto itr = externalDatabases.find(name);
		if (itr == externalDatabases.end() || !itr->second.second) {
			return Optional<Reference<ExternalDatabase>>();
		}

		return itr->second.second;
	}

	// Inserts an existing database into the map. The inserted database name must not already exist in the map.
	// This map will not contribute to the reference count of the inserted database, so it is necessary that the caller
	// hold the returned reference until they no longer need to store the database in the map.
	Reference<ExternalDatabase> insert(std::string name, Database db, Key key) {
		auto itr = externalDatabases.find(name);
		ASSERT(itr == externalDatabases.end());

		Reference<ExternalDatabase> externalDb(
		    new ExternalDatabase(db, [this, name, key]() { cleanupExternalDatabase(name, key); }));

		externalDatabases.try_emplace(name, Future<Void>(), Reference<ExternalDatabase>(externalDb.getPtr()));
		return externalDb;
	}

	size_t size() const { return externalDatabases.size(); }

	void setRecordDatabase(Database db) { this->db = db; }

private:
	Database db;
	UID id;
	LocalityData locality;

	// This is a map from database name to a pair with a future and a database. If the future is valid, then we will
	// need to wait until The reference included in this map does not contribute to the reference count. That means when
	// all other references are deleted, this reference will be destroyed. The destructor of the ExternalDatabase will
	// run a function to remove the now invalid entry from the map.
	std::unordered_map<std::string, std::pair<Future<Void>, Reference<ExternalDatabase>>> externalDatabases;

	// Returns a database if name doesn't exist or the connection string matches the existing entry
	ACTOR static Future<Optional<Reference<ExternalDatabase>>> getOrCreateImpl(ExternalDatabaseMap* self,
	                                                                           std::string name,
	                                                                           std::string connectionString) {
		auto itr = self->externalDatabases.find(name);
		if (itr != self->externalDatabases.end()) {
			// If we are in the process of cleaning up a prior connection, stop trying to clean up and just replace the
			// entry.
			if (itr->second.first.isValid()) {
				itr->second.first.cancel();
				self->externalDatabases.erase(itr);
			} else if (itr->second.second->db->getConnectionRecord()->getConnectionString().toString() ==
			           connectionString) {
				TraceEvent(SevDebug, "ExternalDatabaseMapReuseDatabase", self->id)
				    .detail("Name", name)
				    .detail("ConnectionString", connectionString);

				return itr->second.second;
			} else {
				TraceEvent("ExternalDatabaseMapConnectionStringMismatch", self->id)
				    .detail("Name", name)
				    .detail("ExistingConnectionString",
				            itr->second.second->db->getConnectionRecord()->getConnectionString().toString())
				    .detail("AttemptedConnectionString", connectionString);

				return Optional<Reference<ExternalDatabase>>();
			}
		}

		state Transaction tr(self->db);
		state Key dbKey = StringRef(name).withPrefix(tenantBalancerExternalDatabasePrefix);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

				Optional<Value> v = wait(tr.get(dbKey));
				ASSERT(!v.present());

				tr.set(dbKey, ValueRef(connectionString));
				wait(tr.commit());
				break;
			} catch (Error& e) {
				// TODO: timeouts?
				TraceEvent(SevDebug, "ExternalDatabaseCreateDatabaseKeyError", self->id)
				    .error(e)
				    .detail("Name", name)
				    .detail("ConnectionString", connectionString);

				wait(tr.onError(e));
			}
		}

		Database db = Database::createDatabase(
		    makeReference<ClusterConnectionKey>(self->db, dbKey, ClusterConnectionString(connectionString), true),
		    Database::API_VERSION_LATEST,
		    IsInternal::True,
		    self->locality);

		TraceEvent("ExternalDatabaseRecordCreated", self->id)
		    .detail("Name", name)
		    .detail("ConnectionString", connectionString);

		// Copy variables locally for capturing in an ACTOR.
		ExternalDatabaseMap* selfCapture = self;
		std::string nameCapture = name;
		Key dbKeyCapture = dbKey;

		Reference<ExternalDatabase> externalDb =
		    makeReference<ExternalDatabase>(db, [selfCapture, nameCapture, dbKeyCapture]() {
			    selfCapture->cleanupExternalDatabase(nameCapture, dbKeyCapture);
		    });

		// The entry in the map does not increment the reference count
		auto result =
		    self->externalDatabases.try_emplace(name, Future<Void>(), Reference<ExternalDatabase>(externalDb.getPtr()));

		ASSERT(result.second);

		return externalDb;
	}

	ACTOR static Future<Void> cleanupExternalDatabaseImpl(ExternalDatabaseMap* self, std::string name, Key key) {
		TraceEvent(SevDebug, "ExternalDatabaseMapDeleteDatabase", self->id)
		    .detail("DatabaseName", name)
		    .detail("ConnectionKey", key);

		loop {
			state Transaction tr(self->db);
			try {
				tr.clear(key);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				TraceEvent(SevDebug, "ExternalDatabasetMapDeleteDatabaseError", self->id)
				    .detail("DatabaseName", name)
				    .detail("ConnectionKey", key);
				wait(tr.onError(e));
			}
		}

		self->externalDatabases.erase(name);

		TraceEvent(SevDebug, "ExternalDatabaseMapDeleteDatabaseComplete", self->id)
		    .detail("DatabaseName", name)
		    .detail("ConnectionKey", key);

		return Void();
	}

	void cleanupExternalDatabase(std::string name, Key key) {
		Future<Void> cleanupFuture = cleanupExternalDatabaseImpl(this, name, key);
		if (!cleanupFuture.isReady()) {
			externalDatabases[name] = std::make_pair(cleanupFuture, Reference<ExternalDatabase>());
		} else {
			externalDatabases.erase(name);
		}
	}
};

#include "flow/unactorcompiler.h"
#endif