/*
 * TenantBalancer.actor.cpp
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

#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/ClusterConnectionKey.actor.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/TenantBalancerInterface.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// TODO: do we need any recoverable error states?
enum class MovementState { INITIALIZING, STARTED, READY_FOR_SWITCH, COMPLETED };

class SourceMovementRecord {
public:
	SourceMovementRecord() {}
	SourceMovementRecord(Standalone<StringRef> sourcePrefix,
	                     Standalone<StringRef> destinationPrefix,
	                     std::string databaseName,
	                     Database destinationDb)
	  : id(deterministicRandom()->randomUniqueID()), sourcePrefix(sourcePrefix), destinationPrefix(destinationPrefix),
	    databaseName(databaseName), destinationDb(destinationDb) {}

	Standalone<StringRef> getSourcePrefix() const { return sourcePrefix; }
	Standalone<StringRef> getDestinationPrefix() const { return destinationPrefix; }
	std::string getDatabaseName() const { return databaseName; }
	Database getDestinationDatabase() const { return destinationDb; }

	void setDestinationDatabase(Database db) { destinationDb = db; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, sourcePrefix, destinationPrefix, databaseName);
	}

	Key getKey() const { return StringRef(id.toString()).withPrefix(tenantBalancerSourceMovementPrefix); }

	Value toValue() const {
		BinaryWriter wr(IncludeVersion());
		wr << *this;
		return wr.toValue();
	}

	static SourceMovementRecord fromValue(Value value) {
		SourceMovementRecord record;
		BinaryReader rd(value, IncludeVersion());
		rd >> record;

		return record;
	}

	MovementState movementState = MovementState::INITIALIZING;

private:
	// Private variables are not intended to be modified by requests
	UID id;

	Standalone<StringRef> sourcePrefix;
	Standalone<StringRef> destinationPrefix;

	std::string databaseName;
	Database destinationDb;
};

class DestinationMovementRecord {
public:
	DestinationMovementRecord() {}
	DestinationMovementRecord(Standalone<StringRef> sourcePrefix, Standalone<StringRef> destinationPrefix)
	  : id(deterministicRandom()->randomUniqueID()), sourcePrefix(sourcePrefix), destinationPrefix(destinationPrefix) {}

	Standalone<StringRef> getSourcePrefix() const { return sourcePrefix; }
	Standalone<StringRef> getDestinationPrefix() const { return destinationPrefix; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, sourcePrefix, destinationPrefix);
	}

	Key getKey() const { return StringRef(id.toString()).withPrefix(tenantBalancerDestinationMovementPrefix); }

	Value toValue() const {
		BinaryWriter wr(IncludeVersion());
		wr << *this;
		return wr.toValue();
	}

	static DestinationMovementRecord fromValue(Value value) {
		DestinationMovementRecord record;
		BinaryReader rd(value, IncludeVersion());
		rd >> record;

		return record;
	}

	MovementState movementState = MovementState::INITIALIZING;

private:
	// Private variables are not intended to be modified by requests
	UID id;
	Standalone<StringRef> sourcePrefix;
	Standalone<StringRef> destinationPrefix;
};

ACTOR static Future<Void> extractClientInfo(Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                                            Reference<AsyncVar<ClientDBInfo>> info) {
	loop {
		ClientDBInfo clientInfo = dbInfo->get().client;
		info->set(clientInfo);
		wait(dbInfo->onChange());
	}
}

struct TenantBalancer {
	TenantBalancer(TenantBalancerInterface tbi, Reference<AsyncVar<ServerDBInfo> const> dbInfo)
	  : tbi(tbi), dbInfo(dbInfo), actors(false) {
		auto info = makeReference<AsyncVar<ClientDBInfo>>();
		db = openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::False, EnableLocalityLoadBalance::True);

		agent = DatabaseBackupAgent(db);
	}

	TenantBalancerInterface tbi;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;

	Database db;

	ActorCollection actors;
	DatabaseBackupAgent agent;

	SourceMovementRecord getOutgoingMovement(Key prefix) const {
		auto itr = outgoingMovements.find(prefix);
		if (itr == outgoingMovements.end()) {
			throw movement_not_found();
		}

		return itr->second;
	}

	DestinationMovementRecord getIncomingMovement(Key prefix) const {
		auto itr = incomingMovements.find(prefix);
		if (itr == incomingMovements.end()) {
			throw movement_not_found();
		}

		return itr->second;
	}

	ACTOR template <class Record>
	static Future<Void> persistMovementRecord(TenantBalancer* self, Record record) {
		state Transaction tr(self->db);
		state Key key = record.getKey();
		state Value value = record.toValue();

		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.set(key, value);
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	Future<Void> saveOutgoingMovement(SourceMovementRecord const& record) {
		return map(persistMovementRecord(this, record), [this, record](Void _) {
			outgoingMovements[record.getSourcePrefix()] = record;
			return Void();
		});
	}

	Future<Void> saveIncomingMovement(DestinationMovementRecord const& record) {
		return map(persistMovementRecord(this, record), [this, record](Void _) {
			incomingMovements[record.getDestinationPrefix()] = record;
			return Void();
		});
	}

	ACTOR template <class Record>
	static Future<Void> clearMovementRecord(TenantBalancer* self, Record record) {
		state Transaction tr(self->db);
		state Key key = record.getKey();

		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.clear(key);
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	Future<Void> clearOutgoingMovement(SourceMovementRecord const& record) {
		return map(clearMovementRecord(this, record), [this, record](Void _) {
			outgoingMovements.erase(record.getSourcePrefix());
			return Void();
		});
	}

	Future<Void> clearIncomingMovement(DestinationMovementRecord const& record) {
		return map(clearMovementRecord(this, record), [this, record](Void _) {
			incomingMovements.erase(record.getDestinationPrefix());
			return Void();
		});
	}

	bool hasSourceMovement(Key prefix) const { return outgoingMovements.count(prefix) > 0; }
	bool hasDestinationMovement(Key prefix) const { return incomingMovements.count(prefix) > 0; }

	// Returns a database if name doesn't exist or the connection string matches the existing entry
	ACTOR static Future<Optional<Database>> addExternalDatabaseImpl(TenantBalancer* self,
	                                                                std::string name,
	                                                                std::string connectionString) {
		auto itr = self->externalDatabases.find(name);
		if (itr != self->externalDatabases.end()) {
			if (itr->second->getConnectionRecord()->getConnectionString().toString() == connectionString) {
				return itr->second;
			}

			TraceEvent("ExternalDatabaseMismatch", self->tbi.id())
			    .detail("Name", name)
			    .detail("ExistingConnectionString",
			            itr->second->getConnectionRecord()->getConnectionString().toString())
			    .detail("AttemptedConnectionString", connectionString);

			return Optional<Database>();
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
				wait(tr.onError(e));
			}
		}

		Database externalDb = Database::createDatabase(
		    makeReference<ClusterConnectionKey>(self->db, dbKey, ClusterConnectionString(connectionString), true),
		    Database::API_VERSION_LATEST,
		    IsInternal::True,
		    self->tbi.locality);

		TraceEvent("AddedExternalDatabase", self->tbi.id())
		    .detail("Name", name)
		    .detail("ConnectionString", connectionString);

		self->externalDatabases[name] = externalDb;
		return externalDb;
	}

	Future<Optional<Database>> addExternalDatabase(std::string name, std::string connectionString) {
		return addExternalDatabaseImpl(this, name, connectionString);
	}

	Optional<Database> getExternalDatabase(std::string name) const {
		auto itr = externalDatabases.find(name);
		if (itr == externalDatabases.end()) {
			return Optional<Database>();
		}

		return itr->second;
	}

	ACTOR static Future<Void> recoverImpl(TenantBalancer* self) {
		TraceEvent("TenantBalancerRecovering", self->tbi.id());
		state Transaction tr(self->db);

		state Key begin = tenantBalancerKeys.begin;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

				// TODO: prevent simultaneous modifications to tenant balancer space?
				Standalone<RangeResultRef> result = wait(tr.getRange(KeyRangeRef(begin, tenantBalancerKeys.end), 1000));
				for (auto kv : result) {
					if (kv.key.startsWith(tenantBalancerSourceMovementPrefix)) {
						SourceMovementRecord record = SourceMovementRecord::fromValue(kv.value);
						self->outgoingMovements[record.getSourcePrefix()] = record;
					} else if (kv.key.startsWith(tenantBalancerDestinationMovementPrefix)) {
						DestinationMovementRecord record = DestinationMovementRecord::fromValue(kv.value);
						self->incomingMovements[record.getSourcePrefix()] = record;
					} else if (kv.key.startsWith(tenantBalancerExternalDatabasePrefix)) {
						std::string name = kv.key.removePrefix(tenantBalancerExternalDatabasePrefix).toString();
						self->externalDatabases[name] = Database::createDatabase(
						    makeReference<ClusterConnectionKey>(
						        self->db, kv.key, ClusterConnectionString(kv.value.toString()), true),
						    Database::API_VERSION_LATEST,
						    IsInternal::True,
						    self->tbi.locality);
					} else {
						ASSERT(false);
					}
				}

				if (result.more) {
					ASSERT(result.size() > 0);
					begin = keyAfter(result.rbegin()->key);
					tr.fullReset();
				} else {
					break;
				}
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		for (auto itr : self->outgoingMovements) {
			auto dbItr = self->externalDatabases.find(itr.second.getDatabaseName());
			ASSERT(dbItr != self->externalDatabases.end());
			itr.second.setDestinationDatabase(dbItr->second);
		}

		TraceEvent("TenantBalancerRecovered", self->tbi.id());
		return Void();
	}

	Future<Void> recover() { return recoverImpl(this); }

private:
	// TODO: ref count external databases and delete when all references are gone
	std::unordered_map<std::string, Database> externalDatabases;
	std::map<Key, SourceMovementRecord> outgoingMovements;
	std::map<Key, DestinationMovementRecord> incomingMovements;
};

ACTOR Future<Void> moveTenantToCluster(TenantBalancer* self, MoveTenantToClusterRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		MoveTenantToClusterReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> receiveTenantFromCluster(TenantBalancer* self, ReceiveTenantFromClusterRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		ReceiveTenantFromClusterReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> getActiveMovements(TenantBalancer* self, GetActiveMovementsRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		GetActiveMovementsReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> finishSourceMovement(TenantBalancer* self, FinishSourceMovementRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		FinishSourceMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> finishDestinationMovement(TenantBalancer* self, FinishDestinationMovementRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		FinishDestinationMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> abortMovement(TenantBalancer* self, AbortMovementRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		AbortMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> cleanupMovementSource(TenantBalancer* self, CleanupMovementSourceRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		CleanupMovementSourceReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> tenantBalancerCore(TenantBalancer* self) {
	TraceEvent("TenantBalancerStarting", self->tbi.id());
	loop choose {
		when(MoveTenantToClusterRequest req = waitNext(self->tbi.moveTenantToCluster.getFuture())) {
			self->actors.add(moveTenantToCluster(self, req));
		}
		when(ReceiveTenantFromClusterRequest req = waitNext(self->tbi.receiveTenantFromCluster.getFuture())) {
			self->actors.add(receiveTenantFromCluster(self, req));
		}
		when(GetActiveMovementsRequest req = waitNext(self->tbi.getActiveMovements.getFuture())) {
			self->actors.add(getActiveMovements(self, req));
		}
		when(FinishSourceMovementRequest req = waitNext(self->tbi.finishSourceMovement.getFuture())) {
			self->actors.add(finishSourceMovement(self, req));
		}
		when(FinishDestinationMovementRequest req = waitNext(self->tbi.finishDestinationMovement.getFuture())) {
			self->actors.add(finishDestinationMovement(self, req));
		}
		when(AbortMovementRequest req = waitNext(self->tbi.abortMovement.getFuture())) {
			self->actors.add(abortMovement(self, req));
		}
		when(CleanupMovementSourceRequest req = waitNext(self->tbi.cleanupMovementSource.getFuture())) {
			self->actors.add(cleanupMovementSource(self, req));
		}
		when(wait(self->actors.getResult())) {}
	}
}

ACTOR Future<Void> tenantBalancer(TenantBalancerInterface tbi, Reference<AsyncVar<ServerDBInfo> const> db) {
	state TenantBalancer self(tbi, db);

	try {
		wait(self.recover());
		wait(tenantBalancerCore(&self));
		throw internal_error();
	} catch (Error& e) {
		TraceEvent("TenantBalancerTerminated", tbi.id()).error(e);
		throw e;
	}
}