/*
 * ClusterConnectionKey.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_CLUSTERCONNECTIONKEY_ACTOR_G_H)
#define FDBCLIENT_CLUSTERCONNECTIONKEY_ACTOR_G_H
#include "fdbclient/ClusterConnectionKey.actor.g.h"
#elif !defined(FDBCLIENT_CLUSTERCONNECTIONKEY_ACTOR_H)
#define FDBCLIENT_CLUSTERCONNECTIONKEY_ACTOR_H

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/NativeAPI.actor.h"
#include "flow/actorcompiler.h" // has to be last include

class ClusterConnectionKey : public IClusterConnectionRecord, ReferenceCounted<ClusterConnectionKey>, NonCopyable {
public:
	ClusterConnectionKey() : IClusterConnectionRecord(false), valid(false) {}
	ClusterConnectionKey(Database db, Key connectionStringKey, ClusterConnectionString const& contents);

	ACTOR static Future<ClusterConnectionKey> loadClusterConnectionKey(Database db, Key connectionStringKey);

	ClusterConnectionString const& getConnectionString() const override;
	Future<Void> setConnectionString(ClusterConnectionString const&) override;
	Future<ClusterConnectionString> getStoredConnectionString() override;

	Future<bool> upToDate(ClusterConnectionString& connectionString) override;

	Standalone<StringRef> getLocation() const override;
	Reference<IClusterConnectionRecord> makeIntermediateRecord(
	    ClusterConnectionString const& connectionString) const override;

	bool isValid() const override;
	std::string toString() const override;

	void addref() override { ReferenceCounted<ClusterConnectionKey>::addref(); }
	void delref() override { ReferenceCounted<ClusterConnectionKey>::delref(); }

protected:
	Future<bool> persist() override;

private:
	ACTOR static Future<ClusterConnectionString> getStoredConnectionStringImpl(Reference<ClusterConnectionKey> self);
	ACTOR static Future<bool> upToDateImpl(Reference<ClusterConnectionKey> self,
	                                       ClusterConnectionString* connectionString);
	ACTOR static Future<bool> persistImpl(Reference<ClusterConnectionKey> self);

	Database db;
	ClusterConnectionString cs;
	Key connectionStringKey;
	bool valid;
};

#include "flow/unactorcompiler.h"
#endif