/*
 * ClusterConnectionKey.actor.cpp
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

#include "fdbclient/ClusterConnectionKey.actor.h"
#include "flow/actorcompiler.h" // has to be last include

ClusterConnectionKey::ClusterConnectionKey(Database db,
                                           Key connectionStringKey,
                                           ClusterConnectionString const& contents)
  : IClusterConnectionRecord(true), db(db), cs(contents), connectionStringKey(connectionStringKey), valid(true) {}

ACTOR Future<ClusterConnectionKey> ClusterConnectionKey::loadClusterConnectionKey(Database db,
                                                                                  Key connectionStringKey) {
	state Transaction tr(db);
	loop {
		try {
			Optional<Value> v = wait(tr.get(connectionStringKey));
			if (!v.present()) {
				throw connection_string_invalid();
			}
			return ClusterConnectionKey(db, connectionStringKey, ClusterConnectionString(v.get().toString()));
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ClusterConnectionString const& ClusterConnectionKey::getConnectionString() const {
	return cs;
}

Future<Void> ClusterConnectionKey::setConnectionString(ClusterConnectionString const& connectionString) {
	ASSERT(valid);
	cs = connectionString;
	return success(persist());
}

ACTOR Future<ClusterConnectionString> ClusterConnectionKey::getStoredConnectionStringImpl(
    Reference<ClusterConnectionKey> self) {
	ClusterConnectionKey cck =
	    wait(ClusterConnectionKey::loadClusterConnectionKey(self->db, self->connectionStringKey));
	return cck.cs;
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

		ClusterConnectionKey temp =
		    wait(ClusterConnectionKey::loadClusterConnectionKey(self->db, self->connectionStringKey));
		*connectionString = temp.getConnectionString();
		return connectionString->toString() == self->cs.toString();
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "ClusterKeyError").error(e).detail("Key", self->connectionStringKey);
		return false; // Swallow the error and report that the file is out of date
	}
}

Future<bool> ClusterConnectionKey::upToDate(ClusterConnectionString& connectionString) {
	return upToDateImpl(Reference<ClusterConnectionKey>::addRef(this), &connectionString);
}

ACTOR Future<bool> ClusterConnectionKey::persistImpl(Reference<ClusterConnectionKey> self) {
	self->setPersisted();

	if (self->valid) {
		try {
			state Transaction tr(self->db);
			loop {
				try {
					tr.set(self->connectionStringKey, StringRef(self->cs.toString()));
					wait(tr.commit());
					return true;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		} catch (Error& e) {
			TraceEvent(SevWarnAlways, "UnableToChangeConnectionKey")
			    .error(e)
			    .detail("ConnectionKey", self->connectionStringKey)
			    .detail("ConnStr", self->cs.toString());
		}
	}

	return false;
};

Future<bool> ClusterConnectionKey::persist() {
	return persistImpl(Reference<ClusterConnectionKey>::addRef(this));
}

Standalone<StringRef> ClusterConnectionKey::getLocation() const {
	return connectionStringKey;
}

Reference<IClusterConnectionRecord> ClusterConnectionKey::makeIntermediateRecord(
    ClusterConnectionString const& connectionString) const {
	return makeReference<ClusterConnectionKey>(db, connectionStringKey, connectionString);
}

bool ClusterConnectionKey::isValid() const {
	return valid;
}

std::string ClusterConnectionKey::toString() const {
	if (valid) {
		return "Key: " + printable(connectionStringKey);
	} else {
		return "Key: <unset>";
	}
}
