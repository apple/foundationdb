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
#include "fdbclient/ExternalDatabaseMap.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/TenantBalancerInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/ITrace.h"
#include "flow/Trace.h"
#include "fdbclient/StatusClient.h"
#include <string>
#include <unordered_map>
#include <vector>
#include "flow/actorcompiler.h" // This must be the last #include.

// TODO: do we need any recoverable error states?
static const StringRef DBMOVE_TAG_PREFIX = "MovingData/"_sr;

class SourceMovementRecord {
public:
	SourceMovementRecord() {}
	SourceMovementRecord(Standalone<StringRef> sourcePrefix,
	                     Standalone<StringRef> destinationPrefix,
	                     std::string destDatabaseName,
	                     Database destinationDb)
	  : id(deterministicRandom()->randomUniqueID()), sourcePrefix(sourcePrefix), destinationPrefix(destinationPrefix),
	    destDatabaseName(destDatabaseName), destinationDb(destinationDb) {}

	Standalone<StringRef> getSourcePrefix() const { return sourcePrefix; }
	Standalone<StringRef> getDestinationPrefix() const { return destinationPrefix; }
	Database getDestinationDatabase() const { return destinationDb; }
	std::string getDestinationDatabaseName() const { return destDatabaseName; }
	std::string getTagName() const { return DBMOVE_TAG_PREFIX.toString() + sourcePrefix.toString(); }

	void setDestinationDatabase(Database db) { destinationDb = db; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, sourcePrefix, destinationPrefix, destDatabaseName);
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
	Version switchVersion = invalidVersion;

private:
	// Private variables are not intended to be modified by requests
	UID id;

	Standalone<StringRef> sourcePrefix;
	Standalone<StringRef> destinationPrefix;

	std::string destDatabaseName;
	// TODO: leave this open, or open it at request time?
	Database destinationDb;
};

class DestinationMovementRecord {
public:
	DestinationMovementRecord() {}
	DestinationMovementRecord(Standalone<StringRef> sourcePrefix,
	                          Standalone<StringRef> destinationPrefix,
	                          std::string sourceDatabaseName,
	                          Database sourceDb)
	  : id(deterministicRandom()->randomUniqueID()), sourcePrefix(sourcePrefix), destinationPrefix(destinationPrefix),
	    sourceDatabaseName(sourceDatabaseName), sourceDb(sourceDb) {}

	Standalone<StringRef> getSourcePrefix() const { return sourcePrefix; }
	Standalone<StringRef> getDestinationPrefix() const { return destinationPrefix; }
	Database getSourceDatabase() const { return sourceDb; }
	std::string getSourceDatabaseName() const { return sourceDatabaseName; }
	std::string getTagName() const { return DBMOVE_TAG_PREFIX.toString() + sourcePrefix.toString(); }

	void setSourceDatabase(Database db) { sourceDb = db; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, sourcePrefix, destinationPrefix, sourceDatabaseName);
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
	Version switchVersion = invalidVersion;

private:
	// Private variables are not intended to be modified by requests
	UID id;
	Standalone<StringRef> sourcePrefix;
	Standalone<StringRef> destinationPrefix;
	std::string sourceDatabaseName;
	Database sourceDb;
};

ACTOR static Future<Void> extractClientInfo(Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                                            Reference<AsyncVar<ClientDBInfo>> info) {
	loop {
		ClientDBInfo clientInfo = dbInfo->get().client;
		info->set(clientInfo);
		wait(dbInfo->onChange());
	}
}

ACTOR Future<Void> checkTenantBalancerOwnership(UID id, Reference<ReadYourWritesTransaction> tr) {
	Optional<Value> value = wait(tr->get(tenantBalancerActiveProcessKey));
	if (!value.present() || value.get().toString() != id.toString()) {
		TraceEvent("TenantBalancerLostOwnership", id).detail("CurrentOwner", value.get());
		throw tenant_balancer_terminated();
	}

	return Void();
}

ACTOR template <class Result>
Future<Result> runTenantBalancerTransaction(Database db,
                                            UID id,
                                            std::string context,
                                            std::function<Future<Result>(Reference<ReadYourWritesTransaction>)> func) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);
	state int count = 0;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(checkTenantBalancerOwnership(id, tr));
			Result r = wait(func(tr));
			return r;
		} catch (Error& e) {
			TraceEvent(SevDebug, "TenantBalancerTransactionError", id)
			    .detail("Context", context)
			    .detail("ErrorCount", ++count);

			wait(tr->onError(e));
		}
	}
}

struct TenantBalancer {
	TenantBalancer(TenantBalancerInterface tbi,
	               Reference<AsyncVar<ServerDBInfo> const> dbInfo,
	               Reference<IClusterConnectionRecord> connRecord)
	  : tbi(tbi), dbInfo(dbInfo), connRecord(connRecord), actors(false),
	    tenantBalancerMetrics("TenantBalancer", tbi.id().toString()),
	    moveTenantToClusterRequests("MoveTenantToClusterRequests", tenantBalancerMetrics),
	    receiveTenantFromClusterRequests("ReceiveTenantFromClusterRequests", tenantBalancerMetrics),
	    getActiveMovementsRequests("GetActiveMovementsRequests", tenantBalancerMetrics),
	    finishSourceMovementRequests("FinishSourceMovementRequests", tenantBalancerMetrics),
	    finishDestinationMovementRequests("FinishDestinationMovementRequests", tenantBalancerMetrics),
	    abortMovementRequests("AbortMovementRequests", tenantBalancerMetrics),
	    cleanupMovementSourceRequests("CleanupMovementSourceRequests", tenantBalancerMetrics) {
		auto info = makeReference<AsyncVar<ClientDBInfo>>();
		db = openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::False, EnableLocalityLoadBalance::True);

		agent = DatabaseBackupAgent(db);

		specialCounter(tenantBalancerMetrics, "OpenDatabases", [this]() { return externalDatabases.size(); });
		specialCounter(tenantBalancerMetrics, "ActiveMovesAsSource", [this]() { return outgoingMovements.size(); });
		specialCounter(
		    tenantBalancerMetrics, "ActiveMovesAsDestination", [this]() { return incomingMovements.size(); });

		actors.add(traceCounters("TenantBalancerMetrics",
		                         tbi.id(),
		                         SERVER_KNOBS->STORAGE_LOGGING_DELAY,
		                         &tenantBalancerMetrics,
		                         tbi.id().toString() + "/TenantBalancerMetrics"));
	}

	TenantBalancerInterface tbi;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	Reference<IClusterConnectionRecord> connRecord;

	Database db;

	ActorCollection actors;
	DatabaseBackupAgent agent;

	SourceMovementRecord getOutgoingMovement(Key prefix) const {
		auto itr = outgoingMovements.find(prefix);
		if (itr == outgoingMovements.end()) {
			TraceEvent(SevWarn, "TenantBalancerGetOutgoingMovementError").detail("MissPrefix", prefix);
			throw movement_not_found();
		}

		return itr->second;
	}

	DestinationMovementRecord getIncomingMovement(Key prefix) const {
		auto itr = incomingMovements.find(prefix);
		if (itr == incomingMovements.end()) {
			TraceEvent(SevWarn, "TenantBalancerGetIncomingMovementError").detail("MissPrefix", prefix);
			throw movement_not_found();
		}

		return itr->second;
	}

	template <class Record>
	Future<Void> persistMovementRecord(Record record) {
		Key key = record.getKey();
		Value value = record.toValue();

		return runTenantBalancerTransaction<Void>(
		    db, tbi.id(), "PersistMovementRecord", [key, value](Reference<ReadYourWritesTransaction> tr) {
			    tr->set(key, value);
			    return tr->commit();
		    });
	}

	Future<Void> saveOutgoingMovement(SourceMovementRecord const& record) {
		return map(persistMovementRecord(record), [this, record](Void _) {
			outgoingMovements[record.getSourcePrefix()] = record;
			externalDatabases.addDatabaseRef(record.getDestinationDatabaseName());

			TraceEvent(SevDebug, "SaveOutgoingMovementSuccess", tbi.id())
			    .detail("SourcePrefix", record.getSourcePrefix());
			return Void();
		});
	}

	Future<Void> saveIncomingMovement(DestinationMovementRecord const& record) {
		return map(persistMovementRecord(record), [this, record](Void _) {
			incomingMovements[record.getDestinationPrefix()] = record;
			externalDatabases.addDatabaseRef(record.getSourceDatabaseName());

			TraceEvent(SevDebug, "SaveIncomingMovementSuccess", tbi.id())
			    .detail("DestinationPrefix", record.getDestinationPrefix());
			return Void();
		});
	}

	template <class Record>
	Future<Void> clearMovementRecord(Record record) {
		Key key = record.getKey();

		return runTenantBalancerTransaction<Void>(
		    db, tbi.id(), "ClearMovementRecord", [key](Reference<ReadYourWritesTransaction> tr) {
			    tr->clear(key);
			    return tr->commit();
		    });
	}

	Future<Void> clearExternalDatabase(std::string databaseName) {
		Key key = KeyRef(databaseName).withPrefix(tenantBalancerExternalDatabasePrefix);

		return runTenantBalancerTransaction<Void>(
		    db, tbi.id(), "ClearExternalDatabase", [key](Reference<ReadYourWritesTransaction> tr) {
			    // This conflict range prevents a race if this transaction gets canceled and a new
			    // value is inserted while the commit is in flight.
			    tr->addReadConflictRange(singleKeyRange(key));
			    tr->clear(key);
			    return tr->commit();
		    });
	}

	Future<Void> clearOutgoingMovement(SourceMovementRecord const& record) {
		return map(clearMovementRecord(record), [this, record](Void _) {
			outgoingMovements.erase(record.getSourcePrefix());
			if (externalDatabases.delDatabaseRef(record.getDestinationDatabaseName()) == 0) {
				externalDatabases.markDeleted(record.getDestinationDatabaseName(),
				                              clearExternalDatabase(record.getDestinationDatabaseName()));
			}
			return Void();
		});
	}

	Future<Void> clearIncomingMovement(DestinationMovementRecord const& record) {
		return map(clearMovementRecord(record), [this, record](Void _) {
			incomingMovements.erase(record.getDestinationPrefix());
			if (externalDatabases.delDatabaseRef(record.getSourceDatabaseName()) == 0) {
				externalDatabases.markDeleted(record.getSourceDatabaseName(),
				                              clearExternalDatabase(record.getSourceDatabaseName()));
			}
			return Void();
		});
	}

	bool hasSourceMovement(Key prefix) const { return outgoingMovements.count(prefix) > 0; }
	bool hasDestinationMovement(Key prefix) const { return incomingMovements.count(prefix) > 0; }

	ACTOR static Future<Void> recoverImpl(TenantBalancer* self) {
		TraceEvent("TenantBalancerRecovering", self->tbi.id());
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->db);

		state std::set<std::string> unusedDatabases;

		state Key begin = tenantBalancerKeys.begin;
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(checkTenantBalancerOwnership(self->tbi.id(), tr));
				Standalone<RangeResultRef> result =
				    wait(tr->getRange(KeyRangeRef(begin, tenantBalancerKeys.end), 1000));
				for (auto kv : result) {
					if (kv.key.startsWith(tenantBalancerSourceMovementPrefix)) {
						SourceMovementRecord record = SourceMovementRecord::fromValue(kv.value);
						self->outgoingMovements[record.getSourcePrefix()] = record;

						TraceEvent(SevDebug, "TenantBalancerRecoverSourceMove", self->tbi.id())
						    .detail("SourcePrefix", record.getSourcePrefix())
						    .detail("DestinationPrefix", record.getDestinationPrefix())
						    .detail("DatabaseName", record.getDestinationDatabaseName());
					} else if (kv.key.startsWith(tenantBalancerDestinationMovementPrefix)) {
						DestinationMovementRecord record = DestinationMovementRecord::fromValue(kv.value);

						self->incomingMovements[record.getSourcePrefix()] = record;
						TraceEvent(SevDebug, "TenantBalancerRecoverDestinationMove", self->tbi.id())
						    .detail("SourcePrefix", record.getSourcePrefix())
						    .detail("DestinationPrefix", record.getDestinationPrefix())
						    .detail("DatabaseName", record.getSourceDatabaseName());
					} else if (kv.key.startsWith(tenantBalancerExternalDatabasePrefix)) {
						std::string name = kv.key.removePrefix(tenantBalancerExternalDatabasePrefix).toString();
						Database db = Database::createDatabase(
						    makeReference<ClusterConnectionKey>(
						        self->db, kv.key, ClusterConnectionString(kv.value.toString()), true),
						    Database::API_VERSION_LATEST,
						    IsInternal::True,
						    self->tbi.locality);

						self->externalDatabases.insert(name, db);
						unusedDatabases.insert(name);

						TraceEvent(SevDebug, "TenantBalancerRecoverDatabaseConnection", self->tbi.id())
						    .detail("Name", name)
						    .detail("ConnectionString", kv.value);
					} else {
						ASSERT(kv.key == tenantBalancerActiveProcessKey);
					}
				}

				if (result.more) {
					ASSERT(result.size() > 0);
					begin = keyAfter(result.rbegin()->key);
					tr->reset();
				} else {
					break;
				}
			} catch (Error& e) {
				TraceEvent(SevDebug, "TenantBalancerRecoveryError", self->tbi.id()).error(e);
				wait(tr->onError(e));
			}
		}

		for (auto& itr : self->outgoingMovements) {
			Optional<Database> externalDb = self->externalDatabases.get(itr.second.getDestinationDatabaseName());
			ASSERT(externalDb.present());

			TraceEvent(SevDebug, "TenantBalancerRecoverOutgoingMovementDatabase", self->tbi.id())
			    .detail("DatabaseName", itr.second.getDestinationDatabaseName())
			    .detail("SourcePrefix", itr.second.getSourcePrefix());

			itr.second.setDestinationDatabase(externalDb.get());
			self->externalDatabases.addDatabaseRef(itr.second.getDestinationDatabaseName());
			unusedDatabases.erase(itr.second.getDestinationDatabaseName());
		}

		for (auto& itr : self->incomingMovements) {
			Optional<Database> externalDb = self->externalDatabases.get(itr.second.getSourceDatabaseName());
			ASSERT(externalDb.present());

			TraceEvent(SevDebug, "TenantBalancerRecoverIncomingMovementDatabase", self->tbi.id())
			    .detail("DatabaseName", itr.second.getSourceDatabaseName())
			    .detail("DestinationPrefix", itr.second.getDestinationPrefix());

			itr.second.setSourceDatabase(externalDb.get());
			self->externalDatabases.addDatabaseRef(itr.second.getSourceDatabaseName());
			unusedDatabases.erase(itr.second.getSourceDatabaseName());
		}

		for (auto dbName : unusedDatabases) {
			TraceEvent(SevDebug, "TenantBalancerRecoveredUnusedDatabase", self->tbi.id())
			    .detail("DatabaseName", dbName);
			self->externalDatabases.markDeleted(dbName, self->clearExternalDatabase(dbName));
		}

		TraceEvent("TenantBalancerRecovered", self->tbi.id());
		return Void();
	}

	Future<Void> recover() { return recoverImpl(this); }

	ACTOR static Future<Void> takeTenantBalancerOwnershipImpl(TenantBalancer* self) {
		state Transaction tr(self->db);

		TraceEvent("TenantBalancerTakeOwnership", self->tbi.id());

		loop {
			try {
				tr.set(tenantBalancerActiveProcessKey, StringRef(self->tbi.id().toString()));
				wait(tr.commit());

				TraceEvent("TenantBalancerTookOwnership", self->tbi.id());
				return Void();
			} catch (Error& e) {
				TraceEvent(SevDebug, "TenantBalancerTakeOwnershipError", self->tbi.id()).error(e);
				wait(tr.onError(e));
			}
		}
	}

	Future<Void> takeTenantBalancerOwnership() { return takeTenantBalancerOwnershipImpl(this); }

	ACTOR static Future<bool> isTenantEmpty(Reference<ReadYourWritesTransaction> tr, Key prefix) {
		state RangeResult rangeResult = wait(tr->getRange(prefixRange(prefix), 1));
		return rangeResult.empty();
	}

	Future<bool> static isTenantEmpty(Database db, Key prefix) {
		return runRYWTransaction(db,
		                         [=](Reference<ReadYourWritesTransaction> tr) { return isTenantEmpty(tr, prefix); });
	}

	std::map<Key, SourceMovementRecord> getOutgoingMovements() const { return outgoingMovements; }

	std::map<Key, DestinationMovementRecord> getIncomingMovements() const { return incomingMovements; }

	CounterCollection tenantBalancerMetrics;

	Counter moveTenantToClusterRequests;
	Counter receiveTenantFromClusterRequests;
	Counter getActiveMovementsRequests;
	Counter finishSourceMovementRequests;
	Counter finishDestinationMovementRequests;
	Counter abortMovementRequests;
	Counter cleanupMovementSourceRequests;

	ExternalDatabaseMap externalDatabases;

private:
	std::map<Key, SourceMovementRecord> outgoingMovements;
	std::map<Key, DestinationMovementRecord> incomingMovements;
};

ACTOR template <class Request>
Future<REPLY_TYPE(Request)> sendTenantBalancerRequest(Database peerDb,
                                                      Request request,
                                                      RequestStream<Request> TenantBalancerInterface::*stream) {
	state Future<ErrorOr<REPLY_TYPE(Request)>> replyFuture = Never();
	state Future<Void> initialize = Void();

	loop choose {
		when(ErrorOr<REPLY_TYPE(Request)> reply = wait(replyFuture)) {
			if (reply.isError()) {
				throw reply.getError();
			}
			return reply.get();
		}
		when(wait(peerDb->onTenantBalancerChanged() || initialize)) {
			initialize = Never();
			replyFuture = peerDb->getTenantBalancer().present()
			                  ? (peerDb->getTenantBalancer().get().*stream).tryGetReply(request)
			                  : Never();
		}
	}
}

Future<Void> abortPeer(TenantBalancer* self, Database peerDb, std::string tenantName, bool peerIsSource) {
	return success(sendTenantBalancerRequest(
	    peerDb, AbortMovementRequest(tenantName, peerIsSource), &TenantBalancerInterface::abortMovement));
}

ACTOR Future<bool> insertDbKey(Reference<ReadYourWritesTransaction> tr, Key dbKey, Value dbValue) {
	Optional<Value> existingValue = wait(tr->get(dbKey));
	if (existingValue.present() && existingValue.get() != dbValue) {
		return false;
	}

	tr->set(dbKey, dbValue);
	wait(tr->commit());

	return true;
}

ACTOR Future<Optional<Database>> getOrInsertDatabase(TenantBalancer* self,
                                                     std::string name,
                                                     std::string connectionString) {
	Optional<Database> existingDb = self->externalDatabases.get(name);
	if (existingDb.present()) {
		if (existingDb.get()->getConnectionRecord()->getConnectionString().toString() == connectionString) {
			return existingDb;
		}
		return Optional<Database>();
	}

	self->externalDatabases.cancelCleanup(name);

	state Key dbKey = KeyRef(name).withPrefix(tenantBalancerExternalDatabasePrefix);
	Key dbKeyCapture = dbKey;
	Value dbValue = ValueRef(connectionString);

	bool inserted =
	    wait(runTenantBalancerTransaction<bool>(self->db,
	                                            self->tbi.id(),
	                                            "GetOrInsertDatabase",
	                                            [dbKeyCapture, dbValue](Reference<ReadYourWritesTransaction> tr) {
		                                            return insertDbKey(tr, dbKeyCapture, dbValue);
	                                            }));

	if (!inserted) {
		return Optional<Database>();
	}

	Database db = Database::createDatabase(
	    makeReference<ClusterConnectionKey>(self->db, dbKey, ClusterConnectionString(connectionString), true),
	    Database::API_VERSION_LATEST,
	    IsInternal::True,
	    self->tbi.locality);

	if (!self->externalDatabases.insert(name, db)) {
		Optional<Database> collision = self->externalDatabases.get(name);
		ASSERT(collision.present() &&
		       collision.get()->getConnectionRecord()->getConnectionString().toString() == connectionString);

		return collision.get();
	}

	return db;
}

// src
ACTOR Future<Void> moveTenantToCluster(TenantBalancer* self, MoveTenantToClusterRequest req) {
	TraceEvent(SevDebug, "TenantBalancerMoveTenantToCluster", self->tbi.id())
	    .detail("SourcePrefix", req.sourcePrefix)
	    .detail("DestinationPrefix", req.destPrefix)
	    .detail("DestinationConnectionString", req.destConnectionString);

	++self->moveTenantToClusterRequests;

	try {
		state Optional<Database> destDatabase =
		    wait(getOrInsertDatabase(self, req.destConnectionString, req.destConnectionString));
		if (!destDatabase.present()) {
			// TODO: how to handle this?
			ASSERT(false);
		}

		state SourceMovementRecord sourceMovementRecord(
		    req.sourcePrefix, req.destPrefix, req.destConnectionString, destDatabase.get());

		wait(self->saveOutgoingMovement(sourceMovementRecord));

		// Send a request to the destination database to prepare for the move
		ReceiveTenantFromClusterReply replyFromDestinationDatabase = wait(sendTenantBalancerRequest(
		    destDatabase.get(),
		    ReceiveTenantFromClusterRequest(
		        req.sourcePrefix, req.destPrefix, self->connRecord->getConnectionString().toString()),
		    &TenantBalancerInterface::receiveTenantFromCluster));

		Standalone<VectorRef<KeyRangeRef>> backupRanges;
		backupRanges.push_back_deep(backupRanges.arena(), prefixRange(req.sourcePrefix));

		// Submit a DR to move the target range
		bool replacePrefix = req.sourcePrefix != req.destPrefix;
		wait(self->agent.submitBackup(destDatabase.get(),
		                              KeyRef(sourceMovementRecord.getTagName()),
		                              backupRanges,
		                              StopWhenDone::False,
		                              replacePrefix ? req.destPrefix : StringRef(),
		                              replacePrefix ? req.sourcePrefix : StringRef(),
		                              LockDB::False));

		// Update the state of the movement to started
		sourceMovementRecord.movementState = MovementState::STARTED;
		wait(self->saveOutgoingMovement(sourceMovementRecord));

		// Check if a DR agent is running to process the move
		state bool agentRunning = wait(self->agent.checkActive(destDatabase.get()));
		if (!agentRunning) {
			throw movement_agent_not_running();
		}

		TraceEvent(SevDebug, "TenantBalancerMoveTenantToClusterComplete", self->tbi.id())
		    .detail("SourcePrefix", req.sourcePrefix)
		    .detail("DestinationPrefix", req.destPrefix)
		    .detail("DestinationConnectionString", req.destConnectionString);

		MoveTenantToClusterReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerMoveTenantToClusterError", self->tbi.id())
		    .error(e)
		    .detail("SourcePrefix", req.sourcePrefix)
		    .detail("DestinationPrefix", req.destPrefix)
		    .detail("DestinationConnectionString", req.destConnectionString);

		req.reply.sendError(e);
	}

	return Void();
}

// dest
ACTOR Future<Void> receiveTenantFromCluster(TenantBalancer* self, ReceiveTenantFromClusterRequest req) {
	TraceEvent(SevDebug, "TenantBalancerReceiveTenantFromCluster", self->tbi.id())
	    .detail("SourcePrefix", req.sourcePrefix)
	    .detail("DestinationPrefix", req.destPrefix)
	    .detail("SourceConnectionString", req.srcConnectionString);

	++self->receiveTenantFromClusterRequests;

	try {
		state Optional<Database> srcDatabase =
		    wait(getOrInsertDatabase(self, req.srcConnectionString, req.srcConnectionString));
		if (!srcDatabase.present()) {
			// TODO: how to handle this?
			ASSERT(false);
		}
		DestinationMovementRecord destinationMovementRecord(
		    req.sourcePrefix, req.destPrefix, req.srcConnectionString, srcDatabase.get());
		wait(self->saveIncomingMovement(destinationMovementRecord));

		// 1.Lock the destination before we start the movement
		// TODO

		// 2.Check if prefix is empty.
		bool isPrefixEmpty = wait(self->isTenantEmpty(self->db, req.destPrefix));
		if (!isPrefixEmpty) {
			throw movement_dest_prefix_not_empty();
		}

		// 3.Update record
		destinationMovementRecord.movementState = MovementState::STARTED;
		wait(self->saveIncomingMovement(destinationMovementRecord));

		TraceEvent(SevDebug, "TenantBalancerReceiveTenantFromClusterComplete", self->tbi.id())
		    .detail("SourcePrefix", req.sourcePrefix)
		    .detail("DestinationPrefix", req.destPrefix)
		    .detail("SourceConnectionString", req.srcConnectionString);

		ReceiveTenantFromClusterReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerReceiveTenantFromClusterError", self->tbi.id())
		    .error(e)
		    .detail("SourcePrefix", req.sourcePrefix)
		    .detail("DestinationPrefix", req.destPrefix)
		    .detail("SourceConnectionString", req.srcConnectionString);

		req.reply.sendError(e);
	}

	return Void();
}

std::string getPrefixFromTagName(std::string tagName) {
	auto startIdx = tagName.find('/');
	// TODO think about concerns with conversion between string and key
	return startIdx == tagName.npos ? tagName : tagName.substr(startIdx + 1);
}

ACTOR Future<std::vector<TenantMovementInfo>> fetchDBMove(TenantBalancer* self, bool isSrc) {
	state std::vector<TenantMovementInfo> recorder;
	try {
		// TODO switch to another cheaper way

		state StatusRequest statusRequest;
		state Future<ErrorOr<StatusReply>> reply = Never();
		state Future<Void> initialize = Void();
		state Optional<StatusObject> statusObj;

		loop choose {
			when(ErrorOr<StatusReply> sr = wait(reply)) {
				if (!sr.isError()) {
					statusObj = sr.get().statusObj;
				} else {
					TraceEvent(SevDebug, "TenantBalancerDRStatusError", self->tbi.id()).error(sr.getError());
					// Ignore error and return movements without DR status info
				}

				break;
			}
			when(wait(self->dbInfo->onChange() || initialize)) {
				initialize = Never();
				reply = self->dbInfo->get().clusterInterface.clientInterface.databaseStatus.tryGetReply(statusRequest);
			}
		}

		// Extract DR information
		std::unordered_map<std::string, std::pair<double, std::string>>
		    prefixToDRInfo; // prefix -> {secondsBehind, backupStatus}
		if (statusObj.present()) {
			StatusObjectReader reader(statusObj.get());
			std::string context = isSrc ? "dr_backup" : "dr_backup_dest";
			std::string path = format("layers.%s.tags", context.c_str());
			StatusObjectReader tags;
			if (reader.tryGet(path, tags)) {
				for (auto itr : tags.obj()) {
					JSONDoc tag(itr.second);
					bool running = false;
					tag.tryGet("running_backup", running);
					if (!running) {
						continue;
					}
					std::string backup_state, secondsBehind;
					tag.tryGet("backup_state", backup_state);
					tag.tryGet("seconds_behind", secondsBehind);
					char* end = nullptr;
					double mulationLag = strtod(secondsBehind.c_str(), &end);
					if (end != nullptr) {
						TraceEvent(SevWarn, "SecondsBehindIllegal", self->tbi.id())
						    .detail("TagName", itr.first)
						    .detail("SecondsBehind", secondsBehind);
						continue;
					}
					prefixToDRInfo[getPrefixFromTagName(itr.first)] = { mulationLag, backup_state };
				}
			}
		}

		// Iterate movement records
		std::string curConnectionString = self->db->getConnectionRecord()->getConnectionString().toString();
		// TODO combine these two similar logics to one?
		if (isSrc) {
			for (const auto& [prefix, record] : self->getOutgoingMovements()) {
				TenantMovementInfo tenantMovementInfo;
				tenantMovementInfo.movementLocation = TenantMovementInfo::Location::SOURCE;
				tenantMovementInfo.sourceConnectionString = curConnectionString;
				tenantMovementInfo.destinationConnectionString =
				    record.getDestinationDatabase()->getConnectionRecord()->getConnectionString().toString();
				tenantMovementInfo.sourcePrefix = record.getSourcePrefix();
				tenantMovementInfo.destPrefix = record.getDestinationPrefix();
				// TODO update isSourceLocked and isDestinationLocked
				tenantMovementInfo.isSourceLocked = false;
				tenantMovementInfo.isDestinationLocked = false;
				tenantMovementInfo.movementState = record.movementState;
				tenantMovementInfo.mutationLag = prefixToDRInfo[prefix.toString()].first;
				// TODO assign databaseTimingDelay
				tenantMovementInfo.switchVersion = record.switchVersion;
				// errorMessage
			}
		} else {
			for (const auto& [prefix, record] : self->getIncomingMovements()) {
				TenantMovementInfo tenantMovementInfo;
				tenantMovementInfo.movementLocation = TenantMovementInfo::Location::DEST;
				tenantMovementInfo.sourceConnectionString =
				    record.getSourceDatabase()->getConnectionRecord()->getConnectionString().toString();
				tenantMovementInfo.destinationConnectionString = curConnectionString;
				tenantMovementInfo.sourcePrefix = record.getSourcePrefix();
				tenantMovementInfo.destPrefix = record.getDestinationPrefix();
				// TODO update isSourceLocked and isDestinationLocked
				tenantMovementInfo.isSourceLocked = false;
				tenantMovementInfo.isDestinationLocked = false;
				tenantMovementInfo.movementState = record.movementState;
				tenantMovementInfo.mutationLag = prefixToDRInfo[prefix.toString()].first;
				// TODO assign databaseTimingDelay
				tenantMovementInfo.switchVersion = record.switchVersion;
				// errorMessage
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		throw;
	}
	return recorder;
}

ACTOR Future<Void> getActiveMovements(TenantBalancer* self, GetActiveMovementsRequest req) {
	++self->getActiveMovementsRequests;

	try {
		state std::vector<TenantMovementInfo> statusAsSrc = wait(fetchDBMove(self, true));
		state std::vector<TenantMovementInfo> statusAsDest = wait(fetchDBMove(self, false));
		GetActiveMovementsReply reply;
		reply.activeMovements.insert(reply.activeMovements.end(), statusAsSrc.begin(), statusAsSrc.end());
		reply.activeMovements.insert(reply.activeMovements.end(), statusAsDest.begin(), statusAsDest.end());
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

Future<Version> lockSourceTenant(TenantBalancer* self, std::string tenant) {
	return runTenantBalancerTransaction<Version>(
	    self->db, self->tbi.id(), "LockSourceTenant", [](Reference<ReadYourWritesTransaction> tr) {
		    tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		    return tr->getReadVersion();
	    });
}

ACTOR Future<Void> finishSourceMovement(TenantBalancer* self, FinishSourceMovementRequest req) {
	++self->finishSourceMovementRequests;

	try {
		// TODO: check that the DR is ready to switch
		// TODO: check if maxLagSeconds is exceeded
		state SourceMovementRecord record = self->getOutgoingMovement(Key(req.sourceTenant));
		state std::string destinationConnectionString =
		    record.getDestinationDatabase()->getConnectionRecord()->getConnectionString().toString();
		state Optional<Database> destDatabase =
		    wait(getOrInsertDatabase(self, destinationConnectionString, destinationConnectionString));
		if (!destDatabase.present()) {
			// TODO: how to handle this?
			ASSERT(false);
		}

		// Send a request to the destination database to finish this move
		ReceiveTenantFromClusterReply replyFromDestinationDatabase = wait(sendTenantBalancerRequest(
		    destDatabase.get(),
		    ReceiveTenantFromClusterRequest(record.getSourcePrefix(),
		                                    record.getDestinationPrefix(),
		                                    self->connRecord->getConnectionString().toString()),
		    &TenantBalancerInterface::receiveTenantFromCluster));

		// Update movement record
		state Version version = wait(lockSourceTenant(self, req.sourceTenant));
		record.switchVersion = version;
		record.movementState = MovementState::SWITCHING;
		wait(self->saveOutgoingMovement(record));

		// Use DR to finish this movement
		TraceEvent(SevDebug, "TenantBalancerFinishSourceSwitch", self->tbi.id())
		    .detail("DestinationDatabaseName", record.getDestinationDatabaseName());
		Standalone<VectorRef<KeyRangeRef>> backupRanges;
		wait(self->agent.atomicSwitchover(record.getDestinationDatabase(),
		                                  KeyRef(record.getTagName()),
		                                  backupRanges,
		                                  StringRef(),
		                                  StringRef(),
		                                  ForceAction{ true },
		                                  false,
		                                  false));

		// TODO: issue finish request to destination

		record.movementState = MovementState::COMPLETED;
		wait(self->saveOutgoingMovement(record));

		wait(abortPeer(self, record.getDestinationDatabase(), req.sourceTenant, false));

		FinishSourceMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> finishDestinationMovement(TenantBalancer* self, FinishDestinationMovementRequest req) {
	++self->finishDestinationMovementRequests;

	try {
		state DestinationMovementRecord record = self->getIncomingMovement(Key(req.destinationTenant));
		record.movementState = MovementState::SWITCHING;
		record.switchVersion = req.version;
		wait(self->saveIncomingMovement(record));

		// TODO: wait for DR to flush

		// TODO: unlock DR prefix

		record.movementState = MovementState::COMPLETED;
		wait(self->saveIncomingMovement(record));

		FinishDestinationMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> abortMovement(TenantBalancer* self, AbortMovementRequest req) {
	++self->abortMovementRequests;

	try {
		state Database targetDB;
		state std::string tagName;
		if (req.isSource) {
			SourceMovementRecord srcRecord = self->getOutgoingMovement(Key(req.tenantName));
			targetDB = srcRecord.getDestinationDatabase();
			tagName = srcRecord.getTagName();
		} else {
			DestinationMovementRecord destRecord = self->getIncomingMovement(Key(req.tenantName));
			targetDB = destRecord.getSourceDatabase();
			tagName = destRecord.getTagName();
		}
		TraceEvent(SevDebug,
		           (req.isSource ? "TenantBalancerAbortSourceCluster" : "TenantBalancerAbortDestinationCluster"),
		           self->tbi.id())
		    .detail("TenantName", req.tenantName);
		// TODO: make sure the parameters in abortBackup() are correct
		wait(self->agent.abortBackup(
		    targetDB, Key(tagName), PartialBackup{ false }, AbortOldBackup::False, DstOnly{ false }));

		wait(self->agent.unlockBackup(targetDB, Key(tagName)));

		// Clear record if abort correctly
		if (req.isSource) {
			self->clearOutgoingMovement(self->getOutgoingMovement(Key(req.tenantName)));
		} else {
			self->clearIncomingMovement(self->getIncomingMovement(Key(req.tenantName)));
		}
		TraceEvent(SevDebug, "TenantBalancerAbortComplete", self->tbi.id())
		    .detail("TenantName", req.tenantName)
		    .detail("IsSource", req.isSource);
		AbortMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> cleanupMovementSource(TenantBalancer* self, CleanupMovementSourceRequest req) {
	++self->cleanupMovementSourceRequests;

	try {
		state std::string tenantName = req.tenantName;
		// TODO once the range has been unlocked, it will no longer be legal to run cleanup
		TraceEvent(SevDebug, "TenantBalancerClearErase", self->tbi.id())
		    .detail("TenantName", tenantName)
		    .detail("CleanupType", req.cleanupType);
		if (req.cleanupType != CleanupMovementSourceRequest::CleanupType::UNLOCK) {
			// erase
			wait(self->agent.clearPrefix(self->db, Key(tenantName)));
			TraceEvent("TenantBalancerClearEraseComplete").detail("TenantName", tenantName);
		}
		if (req.cleanupType != CleanupMovementSourceRequest::CleanupType::ERASE) {
			TraceEvent(SevDebug, "TenantBalancerClearUnlock", self->tbi.id())
			    .detail("Database", self->db->getConnectionRecord()->getConnectionString().toString());
			// TODO unlock tenant
			// Clear movement record if UNLOCK is specified
			self->clearOutgoingMovement(self->getOutgoingMovement(Key(tenantName)));
		}
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

ACTOR Future<Void> tenantBalancer(TenantBalancerInterface tbi,
                                  Reference<AsyncVar<ServerDBInfo> const> db,
                                  Reference<IClusterConnectionRecord> connRecord) {
	state TenantBalancer self(tbi, db, connRecord);

	try {
		wait(self.takeTenantBalancerOwnership());
		wait(self.recover());
		wait(tenantBalancerCore(&self));
		throw internal_error();
	} catch (Error& e) {
		TraceEvent("TenantBalancerTerminated", tbi.id()).error(e);
		throw e;
	}
}