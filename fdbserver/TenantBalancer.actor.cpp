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

static const StringRef DBMOVE_TAG_PREFIX = "MovingData/"_sr;

std::string TenantBalancerInterface::movementStateToString(MovementState movementState) {
	switch (movementState) {
	case MovementState::INITIALIZING:
		return "Initializing";
	case MovementState::STARTED:
		return "Started";
	case MovementState::READY_FOR_SWITCH:
		return "ReadyForSwitch";
	case MovementState::SWITCHING:
		return "Switching";
	case MovementState::COMPLETED:
		return "Completed";
	case MovementState::ERROR:
		return "Error";
	default:
		ASSERT(false);
	}
}

std::string TenantBalancerInterface::movementLocationToString(MovementLocation movementLocation) {
	switch (movementLocation) {
	case MovementLocation::SOURCE:
		return "Source";
	case MovementLocation::DEST:
		return "Destination";
	default:
		ASSERT(false);
	}
}

class MovementRecord {
public:
	MovementRecord() {}
	MovementRecord(MovementLocation movementLocation,
	               Standalone<StringRef> sourcePrefix,
	               Standalone<StringRef> destinationPrefix,
	               std::string peerDatabaseName,
	               Database peerDatabase)
	  : id(deterministicRandom()->randomUniqueID()), movementLocation(movementLocation), sourcePrefix(sourcePrefix),
	    destinationPrefix(destinationPrefix), peerDatabaseName(peerDatabaseName), peerDatabase(peerDatabase) {}

	MovementRecord(UID id,
	               MovementLocation movementLocation,
	               Standalone<StringRef> sourcePrefix,
	               Standalone<StringRef> destinationPrefix,
	               std::string peerDatabaseName,
	               Database peerDatabase)
	  : id(id), movementLocation(movementLocation), sourcePrefix(sourcePrefix), destinationPrefix(destinationPrefix),
	    peerDatabaseName(peerDatabaseName), peerDatabase(peerDatabase) {}

	UID getMovementId() const { return id; }
	MovementLocation getMovementLocation() const { return movementLocation; }

	Standalone<StringRef> getSourcePrefix() const { return sourcePrefix; }
	Standalone<StringRef> getDestinationPrefix() const { return destinationPrefix; }

	Standalone<StringRef> getLocalPrefix() const {
		return movementLocation == MovementLocation::SOURCE ? sourcePrefix : destinationPrefix;
	}
	Standalone<StringRef> getRemotePrefix() const {
		return movementLocation == MovementLocation::SOURCE ? destinationPrefix : sourcePrefix;
	}

	Database getPeerDatabase() const { return peerDatabase; }
	std::string getPeerDatabaseName() const { return peerDatabaseName; }

	Standalone<StringRef> getTagName() const { return DBMOVE_TAG_PREFIX.withSuffix(id.toString()); }

	void setPeerDatabase(Database db) { peerDatabase = db; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, sourcePrefix, destinationPrefix, peerDatabaseName);
	}

	Key getKey() const {
		if (movementLocation == MovementLocation::SOURCE) {
			return StringRef(id.toString()).withPrefix(tenantBalancerSourceMovementPrefix);
		} else {
			return StringRef(id.toString()).withPrefix(tenantBalancerDestinationMovementPrefix);
		}
	}

	Value toValue() const {
		BinaryWriter wr(IncludeVersion());
		wr << *this;
		return wr.toValue();
	}

	static MovementRecord fromValue(Value value) {
		MovementRecord record;
		BinaryReader rd(value, IncludeVersion());
		rd >> record;

		return record;
	}

	MovementState movementState = MovementState::INITIALIZING;
	Version switchVersion = invalidVersion;

private:
	// Private variables are not intended to be modified by requests
	UID id;

	MovementLocation movementLocation;
	Standalone<StringRef> sourcePrefix;
	Standalone<StringRef> destinationPrefix;

	std::string peerDatabaseName;
	Database peerDatabase;
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

	MovementRecord getMovement(MovementLocation movementLocation,
	                           Key prefix,
	                           Optional<UID> movementId = Optional<UID>()) const {
		auto& movements = movementLocation == MovementLocation::SOURCE ? outgoingMovements : incomingMovements;

		auto itr = movements.find(prefix);
		if (itr == movements.end()) {
			TraceEvent(SevWarn, "TenantBalancerMovementNotFound", tbi.id())
			    .detail("MovementLocation", TenantBalancerInterface::movementLocationToString(movementLocation))
			    .detail("Prefix", prefix)
			    .detail("MovementId", movementId);

			throw movement_not_found();
		} else if (movementId.present() && movementId.get() != itr->second.getMovementId()) {
			TraceEvent(SevWarn, "TenantBalancerMovementIdMismatch", tbi.id())
			    .detail("MovementLocation", TenantBalancerInterface::movementLocationToString(movementLocation))
			    .detail("Prefix", prefix)
			    .detail("ExpectedId", movementId)
			    .detail("ActualId", itr->second.getMovementId());

			throw movement_id_mismatch();
		}

		return itr->second;
	}

	MovementRecord getOutgoingMovement(Key prefix, Optional<UID> movementId = Optional<UID>()) const {
		return getMovement(MovementLocation::SOURCE, prefix, movementId);
	}

	MovementRecord getIncomingMovement(Key prefix, Optional<UID> movementId = Optional<UID>()) const {
		return getMovement(MovementLocation::DEST, prefix, movementId);
	}

	ACTOR Future<Void> saveMovementRecordImpl(TenantBalancer* self, MovementRecord const* record) {
		Key key = record->getKey();
		Value value = record->toValue();

		wait(runTenantBalancerTransaction<Void>(
		    self->db, self->tbi.id(), "SaveMovementRecord", [key, value](Reference<ReadYourWritesTransaction> tr) {
			    tr->set(key, value);
			    return tr->commit();
		    }));

		if (record->getMovementLocation() == MovementLocation::SOURCE) {
			self->outgoingMovements[record->getSourcePrefix()] = *record;
		} else if (record->getMovementLocation() == MovementLocation::DEST) {
			self->incomingMovements[record->getDestinationPrefix()] = *record;
		} else {
			ASSERT(false);
		}

		self->externalDatabases.addDatabaseRef(record->getPeerDatabaseName());
		TraceEvent(SevDebug, "SaveMovementSuccess", self->tbi.id())
		    .detail("MovementId", record->getMovementId())
		    .detail("MovementLocation",
		            TenantBalancerInterface::movementLocationToString(record->getMovementLocation()))
		    .detail("SourcePrefix", record->getSourcePrefix())
		    .detail("DestinationPrefix", record->getDestinationPrefix());

		return Void();
	}

	Future<Void> saveMovementRecord(MovementRecord const& record) { return saveMovementRecordImpl(this, &record); }

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

	ACTOR Future<Void> clearMovementRecordImpl(TenantBalancer* self, MovementRecord const* record) {
		Key key = record->getKey();

		wait(runTenantBalancerTransaction<Void>(
		    self->db, self->tbi.id(), "ClearMovementRecord", [key](Reference<ReadYourWritesTransaction> tr) {
			    tr->clear(key);
			    return tr->commit();
		    }));

		if (record->getMovementLocation() == MovementLocation::SOURCE) {
			self->outgoingMovements.erase(record->getSourcePrefix());
		} else if (record->getMovementLocation() == MovementLocation::DEST) {
			self->incomingMovements.erase(record->getDestinationPrefix());
		} else {
			ASSERT(false);
		}

		if (self->externalDatabases.delDatabaseRef(record->getPeerDatabaseName()) == 0) {
			self->externalDatabases.markDeleted(record->getPeerDatabaseName(),
			                                    self->clearExternalDatabase(record->getPeerDatabaseName()));
		}

		TraceEvent(SevDebug, "ClearMovementSuccess", self->tbi.id())
		    .detail("MovementId", record->getMovementId())
		    .detail("MovementLocation",
		            TenantBalancerInterface::movementLocationToString(record->getMovementLocation()))
		    .detail("SourcePrefix", record->getSourcePrefix())
		    .detail("DestinationPrefix", record->getDestinationPrefix());

		return Void();
	}

	Future<Void> clearMovementRecord(MovementRecord const& record) { return clearMovementRecordImpl(this, &record); }

	bool hasSourceMovement(Key prefix) const { return outgoingMovements.count(prefix) > 0; }
	bool hasDestinationMovement(Key prefix) const { return incomingMovements.count(prefix) > 0; }

	ACTOR static Future<Void> recoverSourceMovement(TenantBalancer* self, MovementRecord* record);
	ACTOR static Future<Void> recoverDestinationMovement(TenantBalancer* self, MovementRecord* record);

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
						MovementRecord record = MovementRecord::fromValue(kv.value);
						self->outgoingMovements[record.getSourcePrefix()] = record;

						TraceEvent(SevDebug, "TenantBalancerRecoverSourceMove", self->tbi.id())
						    .detail("SourcePrefix", record.getSourcePrefix())
						    .detail("DestinationPrefix", record.getDestinationPrefix())
						    .detail("DatabaseName", record.getPeerDatabaseName());
					} else if (kv.key.startsWith(tenantBalancerDestinationMovementPrefix)) {
						MovementRecord record = MovementRecord::fromValue(kv.value);

						self->incomingMovements[record.getSourcePrefix()] = record;
						TraceEvent(SevDebug, "TenantBalancerRecoverDestinationMove", self->tbi.id())
						    .detail("SourcePrefix", record.getSourcePrefix())
						    .detail("DestinationPrefix", record.getDestinationPrefix())
						    .detail("DatabaseName", record.getPeerDatabaseName());
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

		state std::map<Key, MovementRecord>::iterator movementItr = self->outgoingMovements.begin();
		while (movementItr != self->outgoingMovements.end()) {
			Optional<Database> externalDb = self->externalDatabases.get(movementItr->second.getPeerDatabaseName());
			ASSERT(externalDb.present());

			TraceEvent(SevDebug, "TenantBalancerRecoverOutgoingMovementDatabase", self->tbi.id())
			    .detail("MovementId", movementItr->second.getMovementId())
			    .detail("DatabaseName", movementItr->second.getPeerDatabaseName())
			    .detail("DestinationConnectionString",
			            externalDb.get()->getConnectionRecord()->getConnectionString().toString())
			    .detail("SourcePrefix", movementItr->second.getSourcePrefix())
			    .detail("DestinationPrefix", movementItr->second.getDestinationPrefix());

			movementItr->second.setPeerDatabase(externalDb.get());
			self->externalDatabases.addDatabaseRef(movementItr->second.getPeerDatabaseName());
			unusedDatabases.erase(movementItr->second.getPeerDatabaseName());

			try {
				wait(recoverSourceMovement(self, &movementItr->second));
			} catch (Error& e) {
				TraceEvent(SevWarn, "TenantBalancerRecoverMovementError", self->tbi.id())
				    .error(e)
				    .detail("MovementId", movementItr->second.getMovementId())
				    .detail("MovementState",
				            TenantBalancerInterface::movementStateToString(movementItr->second.movementState))
				    .detail("SourcePrefix", movementItr->second.getSourcePrefix())
				    .detail("DestinationPrefix", movementItr->second.getDestinationPrefix());

				// TODO: store error string in movement record
				movementItr->second.movementState = MovementState::ERROR;
				wait(self->saveMovementRecord(movementItr->second));
			}

			++movementItr;
		}

		movementItr = self->incomingMovements.begin();
		while (movementItr != self->incomingMovements.end()) {
			Optional<Database> externalDb = self->externalDatabases.get(movementItr->second.getPeerDatabaseName());
			ASSERT(externalDb.present());

			TraceEvent(SevDebug, "TenantBalancerRecoverIncomingMovementDatabase", self->tbi.id())
			    .detail("MovementId", movementItr->second.getMovementId())
			    .detail("DatabaseName", movementItr->second.getPeerDatabaseName())
			    .detail("SourceConnectionString",
			            externalDb.get()->getConnectionRecord()->getConnectionString().toString())
			    .detail("SourcePrefix", movementItr->second.getSourcePrefix())
			    .detail("DestinationPrefix", movementItr->second.getDestinationPrefix());

			movementItr->second.setPeerDatabase(externalDb.get());
			self->externalDatabases.addDatabaseRef(movementItr->second.getPeerDatabaseName());
			unusedDatabases.erase(movementItr->second.getPeerDatabaseName());

			try {
				wait(recoverDestinationMovement(self, &movementItr->second));
			} catch (Error& e) {
				TraceEvent(SevWarn, "TenantBalancerRecoverMovementError", self->tbi.id())
				    .error(e)
				    .detail("MovementId", movementItr->second.getMovementId())
				    .detail("DatabaseName", movementItr->second.getPeerDatabaseName())
				    .detail("SourcePrefix", movementItr->second.getSourcePrefix())
				    .detail("DestinationPrefix", movementItr->second.getDestinationPrefix());

				movementItr->second.movementState = MovementState::ERROR;
				wait(self->saveMovementRecord(movementItr->second));
			}

			++movementItr;
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

	std::map<Key, MovementRecord> getOutgoingMovements() const { return outgoingMovements; }
	std::map<Key, MovementRecord> getIncomingMovements() const { return incomingMovements; }

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
	std::map<Key, MovementRecord> outgoingMovements;
	std::map<Key, MovementRecord> incomingMovements;
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

Future<Void> abortPeer(TenantBalancer* self, MovementRecord const& record) {
	return success(sendTenantBalancerRequest(
	    record.getPeerDatabase(),
	    AbortMovementRequest(record.getMovementId(),
	                         record.getRemotePrefix(),
	                         record.getMovementLocation() == MovementLocation::SOURCE ? MovementLocation::DEST
	                                                                                  : MovementLocation::SOURCE),
	    &TenantBalancerInterface::abortMovement));
}

ACTOR Future<EBackupState> getDrState(TenantBalancer* self,
                                      Standalone<StringRef> tag,
                                      Reference<ReadYourWritesTransaction> tr) {
	UID logUid = wait(self->agent.getLogUid(tr, tag));
	EBackupState backupState = wait(self->agent.getStateValue(tr, logUid));
	return backupState;
}

ACTOR Future<EBackupState> getDrState(TenantBalancer* self, Standalone<StringRef> tag) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->db);
	loop {
		try {
			EBackupState backupState = wait(getDrState(self, tag, tr));
			return backupState;
		} catch (Error& e) {
			TraceEvent(SevDebug, "TenantBalancerGetDRStateError", self->tbi.id()).error(e).detail("Tag", tag);
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<bool> checkForActiveDr(TenantBalancer* self, Standalone<StringRef> tag) {
	EBackupState backupState = wait(getDrState(self, tag));
	return backupState == EBackupState::STATE_SUBMITTED || backupState == EBackupState::STATE_RUNNING ||
	       backupState == EBackupState::STATE_RUNNING_DIFFERENTIAL;
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

ACTOR Future<ReceiveTenantFromClusterReply> startSourceMovement(TenantBalancer* self, MovementRecord* record) {
	// Send a request to the destination database to prepare for the move
	state ReceiveTenantFromClusterReply reply = wait(
	    sendTenantBalancerRequest(record->getPeerDatabase(),
	                              ReceiveTenantFromClusterRequest(record->getMovementId(),
	                                                              record->getSourcePrefix(),
	                                                              record->getDestinationPrefix(),
	                                                              self->connRecord->getConnectionString().toString()),
	                              &TenantBalancerInterface::receiveTenantFromCluster));

	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	backupRanges.push_back_deep(backupRanges.arena(), prefixRange(record->getSourcePrefix()));

	// Submit a DR to move the target range
	bool replacePrefix = record->getSourcePrefix() != record->getDestinationPrefix();

	wait(self->agent.submitBackup(record->getPeerDatabase(),
	                              record->getTagName(),
	                              backupRanges,
	                              StopWhenDone::False,
	                              replacePrefix ? record->getDestinationPrefix() : StringRef(),
	                              replacePrefix ? record->getSourcePrefix() : StringRef(),
	                              LockDB::False));

	// Update the state of the movement to started
	record->movementState = MovementState::STARTED;
	wait(self->saveMovementRecord(*record));

	return reply;
}

ACTOR Future<Void> moveTenantToCluster(TenantBalancer* self, MoveTenantToClusterRequest req) {
	TraceEvent(SevDebug, "TenantBalancerMoveTenantToCluster", self->tbi.id())
	    .detail("SourcePrefix", req.sourcePrefix)
	    .detail("DestinationPrefix", req.destPrefix)
	    .detail("DestinationConnectionString", req.destConnectionString);

	++self->moveTenantToClusterRequests;

	try {
		state Optional<Database> destDatabase;
		state std::string databaseName = req.destConnectionString;

		loop {
			Optional<Database> db = wait(getOrInsertDatabase(self, req.destConnectionString, req.destConnectionString));
			if (db.present()) {
				destDatabase = db;
				break;
			}

			// This will generate a unique random database name, so we won't get the benefits of sharing
			databaseName = req.destConnectionString + "/" + deterministicRandom()->randomUniqueID().toString();
			TraceEvent(SevDebug, "TenantBalancerCreateDatabaseUniqueNameFallback", self->tbi.id())
			    .detail("ConnectionString", req.destConnectionString)
			    .detail("DatabaseName", databaseName);
		}

		state MovementRecord record(
		    MovementLocation::SOURCE, req.sourcePrefix, req.destPrefix, req.destConnectionString, destDatabase.get());

		wait(self->saveMovementRecord(record));

		// Start the movement
		state ReceiveTenantFromClusterReply replyFromDestinationDatabase = wait(startSourceMovement(self, &record));

		// Check if a DR agent is running to process the move
		state bool agentRunning = wait(self->agent.checkActive(destDatabase.get()));
		if (!agentRunning) {
			throw movement_agent_not_running();
		}

		TraceEvent(SevDebug, "TenantBalancerMoveTenantToClusterComplete", self->tbi.id())
		    .detail("MovementId", record.getMovementId())
		    .detail("SourcePrefix", req.sourcePrefix)
		    .detail("DestinationPrefix", req.destPrefix)
		    .detail("DestinationConnectionString", req.destConnectionString);

		MoveTenantToClusterReply reply(record.getMovementId(), replyFromDestinationDatabase.tenantName);
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

ACTOR Future<Void> receiveTenantFromCluster(TenantBalancer* self, ReceiveTenantFromClusterRequest req) {
	TraceEvent(SevDebug, "TenantBalancerReceiveTenantFromCluster", self->tbi.id())
	    .detail("MovementId", req.movementId)
	    .detail("SourcePrefix", req.sourcePrefix)
	    .detail("DestinationPrefix", req.destPrefix)
	    .detail("SourceConnectionString", req.srcConnectionString);

	++self->receiveTenantFromClusterRequests;

	try {
		state Optional<Database> srcDatabase;
		state std::string databaseName = req.srcConnectionString;
		loop {
			Optional<Database> db = wait(getOrInsertDatabase(self, req.srcConnectionString, req.srcConnectionString));
			if (db.present()) {
				srcDatabase = db;
				break;
			}

			// This will generate a unique random database name, so we won't get the benefits of sharing
			databaseName = req.srcConnectionString + "/" + deterministicRandom()->randomUniqueID().toString();
			TraceEvent(SevDebug, "TenantBalancerCreateDatabaseUniqueNameFallback", self->tbi.id())
			    .detail("ConnectionString", req.srcConnectionString)
			    .detail("DatabaseName", databaseName);
		}

		state MovementRecord destinationMovementRecord;
		try {
			destinationMovementRecord = self->getIncomingMovement(req.destPrefix, req.movementId);
		} catch (Error& e) {
			if (e.code() == error_code_movement_not_found) {
				destinationMovementRecord = MovementRecord(req.movementId,
				                                           MovementLocation::DEST,
				                                           req.sourcePrefix,
				                                           req.destPrefix,
				                                           req.srcConnectionString,
				                                           srcDatabase.get());

				wait(self->saveMovementRecord(destinationMovementRecord));
			} else {
				throw;
			}
		}

		state std::string lockedTenant = "";
		if (destinationMovementRecord.movementState == MovementState::INITIALIZING) {
			// 1. TODO: Lock the destination before we start the movement

			// 2.Check if prefix is empty.
			bool isPrefixEmpty = wait(self->isTenantEmpty(self->db, req.destPrefix));
			if (!isPrefixEmpty) {
				throw movement_dest_prefix_not_empty();
			}

			// 3.Update record
			destinationMovementRecord.movementState = MovementState::STARTED;
			wait(self->saveMovementRecord(destinationMovementRecord));
		}

		TraceEvent(SevDebug, "TenantBalancerReceiveTenantFromClusterComplete", self->tbi.id())
		    .detail("MovementId", req.movementId)
		    .detail("SourcePrefix", req.sourcePrefix)
		    .detail("DestinationPrefix", req.destPrefix)
		    .detail("SourceConnectionString", req.srcConnectionString);

		ReceiveTenantFromClusterReply reply(lockedTenant);
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerReceiveTenantFromClusterError", self->tbi.id())
		    .error(e)
		    .detail("MovementId", req.movementId)
		    .detail("SourcePrefix", req.sourcePrefix)
		    .detail("DestinationPrefix", req.destPrefix)
		    .detail("SourceConnectionString", req.srcConnectionString);

		req.reply.sendError(e);
	}

	return Void();
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
					std::string backupState, secondsBehind;
					tag.tryGet("backup_state", backupState);
					tag.tryGet("seconds_behind", secondsBehind);
					char* end = nullptr;
					double mutationLag = strtod(secondsBehind.c_str(), &end);
					if (end != nullptr) {
						TraceEvent(SevWarn, "TenantBalancerSecondsBehindIllegal", self->tbi.id())
						    .detail("TagName", itr.first)
						    .detail("SecondsBehind", secondsBehind);
						continue;
					}

					// TODO: alternate method to associate prefix with DR status
					// prefixToDRInfo[prefix] = { mutationLag, backupState };
				}
			}
		}

		// Iterate movement records
		std::string curConnectionString = self->db->getConnectionRecord()->getConnectionString().toString();
		for (const auto& [prefix, record] : (isSrc ? self->getOutgoingMovements() : self->getIncomingMovements())) {
			TenantMovementInfo tenantMovementInfo;
			tenantMovementInfo.movementLocation = isSrc ? MovementLocation::SOURCE : MovementLocation::DEST;
			tenantMovementInfo.sourceConnectionString =
			    isSrc ? curConnectionString
			          : record.getPeerDatabase()->getConnectionRecord()->getConnectionString().toString();
			tenantMovementInfo.destinationConnectionString =
			    !isSrc ? curConnectionString
			           : record.getPeerDatabase()->getConnectionRecord()->getConnectionString().toString();
			tenantMovementInfo.sourcePrefix = record.getSourcePrefix();
			tenantMovementInfo.destPrefix = record.getDestinationPrefix();
			// TODO update isSourceLocked and isDestinationLocked
			tenantMovementInfo.isSourceLocked = false;
			tenantMovementInfo.isDestinationLocked = false;
			tenantMovementInfo.movementState = record.movementState;
			tenantMovementInfo.mutationLag = prefixToDRInfo[prefix.toString()].first;
			tenantMovementInfo.databaseBackupStatus = prefixToDRInfo[prefix.toString()].second;
			// TODO assign databaseTimingDelay
			tenantMovementInfo.switchVersion = record.switchVersion;
			// errorMessage
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		throw;
	}
	return recorder;
}

void filterActiveMove(const std::vector<TenantMovementInfo>& originStatus,
                      std::vector<TenantMovementInfo>& targetStatus,
                      Optional<Key> prefixFilter,
                      Optional<std::string> peerDatabaseConnectionStringFilter) {
	for (const auto& status : originStatus) {
		const std::string& localPrefix =
		    (status.movementLocation == MovementLocation::SOURCE ? status.sourcePrefix : status.destPrefix).toString();
		if (prefixFilter.present() && prefixFilter.get() != localPrefix) {
			continue;
		}
		const std::string& remoteDatabaseConnectionString = status.movementLocation == MovementLocation::SOURCE
		                                                        ? status.destinationConnectionString
		                                                        : status.sourceConnectionString;
		if (peerDatabaseConnectionStringFilter.present() &&
		    peerDatabaseConnectionStringFilter.get() != remoteDatabaseConnectionString) {
			continue;
		}
		targetStatus.push_back(status);
	}
}

ACTOR Future<std::vector<TenantMovementInfo>> getFilteredMovements(
    TenantBalancer* self,
    Optional<Key> prefixFilter,
    Optional<std::string> peerDatabaseConnectionStringFilter,
    Optional<MovementLocation> locationFilter) {
	state std::vector<TenantMovementInfo> recorder;
	if (!locationFilter.present() || locationFilter.get() == MovementLocation::SOURCE) {
		state std::vector<TenantMovementInfo> statusAsSrc = wait(fetchDBMove(self, true));
		recorder.insert(recorder.end(), statusAsSrc.begin(), statusAsSrc.end());
	}
	if (!locationFilter.present() || locationFilter.get() == MovementLocation::DEST) {
		state std::vector<TenantMovementInfo> statusAsDest = wait(fetchDBMove(self, false));
		recorder.insert(recorder.end(), statusAsDest.begin(), statusAsDest.end());
	}
	std::vector<TenantMovementInfo> resultAfterFilter;
	filterActiveMove(recorder, resultAfterFilter, prefixFilter, peerDatabaseConnectionStringFilter);
	return recorder;
}

ACTOR Future<Void> getActiveMovements(TenantBalancer* self, GetActiveMovementsRequest req) {
	++self->getActiveMovementsRequests;

	TraceEvent(SevDebug, "TenantBalancerGetActiveMovements", self->tbi.id())
	    .detail("PrefixFilter", req.prefixFilter)
	    .detail("LocationFilter",
	            req.locationFilter.present()
	                ? TenantBalancerInterface::movementLocationToString(req.locationFilter.get())
	                : "[not set]")
	    .detail("PeerClusterFilter", req.peerDatabaseConnectionStringFilter);

	try {
		state std::vector<TenantMovementInfo> status = wait(
		    getFilteredMovements(self, req.prefixFilter, req.peerDatabaseConnectionStringFilter, req.locationFilter));

		GetActiveMovementsReply reply;
		reply.activeMovements.insert(reply.activeMovements.end(), status.begin(), status.end());

		TraceEvent(SevDebug, "TenantBalancerGetActiveMovementsComplete", self->tbi.id())
		    .detail("PrefixFilter", req.prefixFilter)
		    .detail("LocationFilter",
		            req.locationFilter.present()
		                ? TenantBalancerInterface::movementLocationToString(req.locationFilter.get())
		                : "[not set]")
		    .detail("PeerClusterFilter", req.peerDatabaseConnectionStringFilter);
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerGetActiveMovementsError", self->tbi.id())
		    .error(e)
		    .detail("PrefixFilter", req.prefixFilter)
		    .detail("LocationFilter",
		            req.locationFilter.present()
		                ? TenantBalancerInterface::movementLocationToString(req.locationFilter.get())
		                : "[not set]")
		    .detail("PeerClusterFilter", req.peerDatabaseConnectionStringFilter);

		req.reply.sendError(e);
	}

	return Void();
}

Future<Version> lockSourceTenant(TenantBalancer* self, Key prefix) {
	TraceEvent("TenantBalancerLockSourceTenant", self->tbi.id()).detail("Prefix", prefix);
	return runTenantBalancerTransaction<Version>(
	    self->db, self->tbi.id(), "LockSourceTenant", [](Reference<ReadYourWritesTransaction> tr) {
		    tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		    return tr->getReadVersion();
	    });
}

Future<Void> sendFinishRequestToDestination(TenantBalancer* self, MovementRecord* record) {
	// TODO when finish is ready
	return Void();
}

ACTOR Future<Void> finishSourceMovement(TenantBalancer* self, FinishSourceMovementRequest req) {
	++self->finishSourceMovementRequests;

	TraceEvent(SevDebug, "TenantBalancerFinishSourceMovement", self->tbi.id())
	    .detail("SourcePrefix", req.sourcePrefix)
	    .detail("MaxLagSeconds", req.maxLagSeconds);

	try {
		state MovementRecord record = self->getOutgoingMovement(req.sourcePrefix);
		state std::vector<TenantMovementInfo> filteredMovements =
		    wait(getFilteredMovements(self, req.sourcePrefix, Optional<std::string>(), Optional<MovementLocation>()));
		if (filteredMovements.size() != 1) {
			throw movement_not_found();
		}
		state TenantMovementInfo targetMovementInfo = filteredMovements[0];

		// Check that the DR is ready to switch
		if (targetMovementInfo.databaseBackupStatus == "has errored") {
			record.movementState = MovementState::ERROR;
			wait(self->saveMovementRecord(record));

			TraceEvent(SevWarn, "TenantBalancerBackupError", self->tbi.id())
			    .detail("MovementId", record.getMovementId())
			    .detail("MovementLocation",
			            TenantBalancerInterface::movementLocationToString(record.getMovementLocation()))
			    .detail("SourcePrefix", record.getSourcePrefix())
			    .detail("DestinationPrefix", record.getDestinationPrefix())
			    .detail("PeerConnectionString",
			            record.getPeerDatabase()->getConnectionRecord()->getConnectionString().toString());

			throw movement_error();
		}
		if (targetMovementInfo.databaseBackupStatus != "is differential") {
			throw movement_not_ready_for_operation();
		}
		if (targetMovementInfo.mutationLag > req.maxLagSeconds) {
			TraceEvent(SevDebug, "TenantBalancerLagCheckFailed", self->tbi.id())
			    .detail("MaxLagSeconds", req.maxLagSeconds)
			    .detail("CurrentLagSeconds", targetMovementInfo.mutationLag);
			throw movement_lag_too_large();
		}

		TraceEvent(SevDebug, "TenantBalancerStartingSwitch", self->tbi.id())
		    .detail("MovementId", record.getMovementId())
		    .detail("SourcePrefix", record.getSourcePrefix())
		    .detail("DestinationPrefix", record.getDestinationPrefix())
		    .detail("DestinationConnectionString",
		            record.getPeerDatabase()->getConnectionRecord()->getConnectionString().toString())
		    .detail("MaxLagSeconds", req.maxLagSeconds)
		    .detail("CurrentLagSeconds", targetMovementInfo.mutationLag);

		state std::string destinationConnectionString =
		    record.getPeerDatabase()->getConnectionRecord()->getConnectionString().toString();

		state Version version = wait(lockSourceTenant(self, req.sourcePrefix));
		// TODO: get a locked tenant
		state std::string lockedTenant = "";
		record.switchVersion = version;

		// Update movement record
		record.movementState = MovementState::SWITCHING;
		wait(self->saveMovementRecord(record));

		FinishDestinationMovementReply destinationReply = wait(sendTenantBalancerRequest(
		    record.getPeerDatabase(),
		    FinishDestinationMovementRequest(record.getMovementId(), record.getDestinationPrefix(), version),
		    &TenantBalancerInterface::finishDestinationMovement));

		record.movementState = MovementState::COMPLETED;
		wait(self->saveMovementRecord(record));

		wait(abortPeer(self, record));

		TraceEvent(SevDebug, "TenantBalancerFinishSourceMovementComplete", self->tbi.id())
		    .detail("MovementId", record.getMovementId())
		    .detail("SourcePrefix", record.getSourcePrefix())
		    .detail("DestinationPrefix", record.getDestinationPrefix())
		    .detail("DestinationConnectionString",
		            record.getPeerDatabase()->getConnectionRecord()->getConnectionString().toString());

		FinishSourceMovementReply reply(lockedTenant, version);
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerFinishSourceMovementError", self->tbi.id())
		    .error(e)
		    .detail("SourcePrefix", req.sourcePrefix)
		    .detail("MaxLagSeconds", req.maxLagSeconds);

		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> flushMovement(TenantBalancer* self, MovementRecord* record) {
	DatabaseBackupAgent sourceBackupAgent(record->getPeerDatabase());
	wait(sourceBackupAgent.flushBackup(self->db, record->getTagName(), record->switchVersion));
	// TODO: unlock DR prefix

	record->movementState = MovementState::COMPLETED;
	wait(self->saveMovementRecord(*record));

	return Void();
}

ACTOR Future<Void> flushAndNotifySourceCluster(TenantBalancer* self, MovementRecord* record) {
	EBackupState backupState = wait(getDrState(self, record->getTagName()));

	if (backupState == EBackupState::STATE_RUNNING_DIFFERENTIAL) {
		wait(flushMovement(self, record));
	} else if (backupState == EBackupState::STATE_COMPLETED) {
		// Do nothing
	} else {
		bool locked = false;
		if (locked) {
			// TODO: If destination is locked, we are in an unexpected state and the movement should be aborted
			wait(abortPeer(self, *record));
			TraceEvent(SevWarn, "TenantBalancerRecoverMovementUnexpectedDRState", self->tbi.id())
			    .detail("MovementId", record->getMovementId())
			    .detail("MovementLocation",
			            TenantBalancerInterface::movementLocationToString(record->getMovementLocation()))
			    .detail("SourcePrefix", record->getSourcePrefix())
			    .detail("DestinationPrefix", record->getDestinationPrefix());

			throw movement_error();
		}
	}

	// TODO: Notify source of completion

	return Void();
}

ACTOR Future<Void> finishDestinationMovement(TenantBalancer* self, FinishDestinationMovementRequest req) {
	++self->finishDestinationMovementRequests;

	TraceEvent(SevDebug, "TenantBalancerFinishDestinationMovement", self->tbi.id())
	    .detail("MovementId", req.movementId)
	    .detail("DestinationPrefix", req.destinationPrefix)
	    .detail("SwitchVersion", req.version);

	try {
		state MovementRecord record = self->getIncomingMovement(Key(req.destinationPrefix), req.movementId);
		record.movementState = MovementState::SWITCHING;
		record.switchVersion = req.version;
		wait(self->saveMovementRecord(record));

		TraceEvent(SevDebug, "TenantBalancerFinishDestinationMovementComplete", self->tbi.id())
		    .detail("MovementId", record.getMovementId())
		    .detail("SourcePrefix", record.getSourcePrefix())
		    .detail("DestinationPrefix", record.getDestinationPrefix())
		    .detail("SourceConnectionString",
		            record.getPeerDatabase()->getConnectionRecord()->getConnectionString().toString())
		    .detail("SwitchVersion", req.version);

		FinishDestinationMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerFinishDestinationMovementError", self->tbi.id())
		    .error(e)
		    .detail("MovementId", req.movementId)
		    .detail("SourcePrefix", req.destinationPrefix)
		    .detail("SwitchVersion", req.version);

		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> abortDr(TenantBalancer* self, MovementRecord const* record) {
	if (record->getMovementLocation() == MovementLocation::SOURCE) {
		wait(self->agent.abortBackup(record->getPeerDatabase(),
		                             record->getTagName(),
		                             PartialBackup{ false },
		                             AbortOldBackup::False,
		                             DstOnly{ false }));
	} else {
		DatabaseBackupAgent sourceAgent(record->getPeerDatabase());
		wait(sourceAgent.abortBackup(
		    self->db, record->getTagName(), PartialBackup{ false }, AbortOldBackup::False, DstOnly{ false }));
	}

	return Void();
}

ACTOR Future<Void> abortMovement(TenantBalancer* self, AbortMovementRequest req) {
	++self->abortMovementRequests;

	TraceEvent(SevDebug, "TenantBalancerAbortMovement", self->tbi.id())
	    .detail("MovementId", req.movementId)
	    .detail("MovementLocation", TenantBalancerInterface::movementLocationToString(req.movementLocation))
	    .detail("Prefix", req.prefix);

	try {
		state MovementRecord record = self->getMovement(req.movementLocation, req.prefix, req.movementId);

		wait(abortDr(self, &record));
		wait(self->clearMovementRecord(record));

		TraceEvent(SevDebug, "TenantBalancerAbortComplete", self->tbi.id())
		    .detail("MovementId", record.getMovementId())
		    .detail("MovementLocation", TenantBalancerInterface::movementLocationToString(record.getMovementLocation()))
		    .detail("SourcePrefix", record.getSourcePrefix())
		    .detail("DestinationPrefix", record.getDestinationPrefix())
		    .detail("PeerConnectionString",
		            record.getPeerDatabase()->getConnectionRecord()->getConnectionString().toString());

		AbortMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerAbortError", self->tbi.id())
		    .error(e)
		    .detail("MovementId", req.movementId)
		    .detail("MovementLocation", TenantBalancerInterface::movementLocationToString(req.movementLocation))
		    .detail("Prefix", req.prefix);

		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> cleanupMovementSource(TenantBalancer* self, CleanupMovementSourceRequest req) {
	++self->cleanupMovementSourceRequests;

	TraceEvent(SevDebug, "TenantBalancerCleanupMovementSource", self->tbi.id())
	    .detail("Prefix", req.prefix)
	    .detail("CleanupType", req.cleanupType);

	try {
		state MovementRecord record = self->getOutgoingMovement(req.prefix);
		if (record.movementState != MovementState::COMPLETED) {
			TraceEvent(SevDebug, "TenantBalancerMovementNotReadyForCleanup", self->tbi.id())
			    .detail("Prefix", req.prefix)
			    .detail("CurrentState", TenantBalancerInterface::movementStateToString(record.movementState));

			throw movement_not_ready_for_operation();
		}

		// Erase the moved data, if desired
		if (req.cleanupType != CleanupMovementSourceRequest::CleanupType::UNLOCK) {
			KeyRange rangeToErase = prefixRange(req.prefix);
			wait(runTenantBalancerTransaction<Void>(self->db,
			                                        self->tbi.id(),
			                                        "CleanupMovementSourceErase",
			                                        [rangeToErase](Reference<ReadYourWritesTransaction> tr) {
				                                        tr->clear(rangeToErase);
				                                        return tr->commit();
			                                        }));

			TraceEvent("TenantBalancerPrefixErased", self->tbi.id())
			    .detail("MovementId", record.getMovementId())
			    .detail("Prefix", req.prefix);
		}

		// Unlock the moved range, if desired
		if (req.cleanupType != CleanupMovementSourceRequest::CleanupType::ERASE) {
			// TODO unlock tenant

			wait(self->clearMovementRecord(record));

			TraceEvent(SevDebug, "TenantBalancerPrefixUnlocked", self->tbi.id())
			    .detail("MovementId", record.getMovementId())
			    .detail("Prefix", req.prefix);
		}

		TraceEvent(SevDebug, "TenantBalancerCleanupMovementSourceComplete", self->tbi.id())
		    .detail("MovementId", record.getMovementId())
		    .detail("Prefix", req.prefix)
		    .detail("CleanupType", req.cleanupType);

		CleanupMovementSourceReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerCleanupMovementSourceError", self->tbi.id())
		    .error(e)
		    .detail("Prefix", req.prefix)
		    .detail("CleanupType", req.cleanupType);

		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> abortMovementDueToFailedDr(TenantBalancer* self, MovementRecord* record) {
	TraceEvent(SevWarn, "TenantBalancerRecoverMovementAborted", self->tbi.id())
	    .detail("Reason", "DR is not running")
	    .detail("MovementId", record->getMovementId())
	    .detail("MovementLocation", TenantBalancerInterface::movementLocationToString(record->getMovementLocation()))
	    .detail("MovementState", TenantBalancerInterface::movementStateToString(record->movementState))
	    .detail("DestinationDatabase",
	            record->getPeerDatabase()->getConnectionRecord()->getConnectionString().toString())
	    .detail("SourcePrefix", record->getSourcePrefix())
	    .detail("DestinationPrefix", record->getDestinationPrefix());

	wait(abortPeer(self, *record));

	record->movementState = MovementState::ERROR;
	wait(self->saveMovementRecord(*record));

	return Void();
}

ACTOR Future<Void> TenantBalancer::recoverSourceMovement(TenantBalancer* self, MovementRecord* record) {
	bool activeDr = wait(checkForActiveDr(self, record->getTagName()));

	if (record->movementState == MovementState::INITIALIZING) {
		// If DR is already running, then we can just move to the started phase.
		if (activeDr) {
			record->movementState = MovementState::STARTED;
			wait(self->saveMovementRecord(*record));
		}
		// Otherwise, attempt to start the movement
		else {
			ReceiveTenantFromClusterReply reply = wait(startSourceMovement(self, record));
		}
	} else if (record->movementState == MovementState::STARTED) {
		if (!activeDr) {
			wait(abortMovementDueToFailedDr(self, record));
		}
	} else if (record->movementState == MovementState::READY_FOR_SWITCH) {
		// TODO: unlock the source
		if (!activeDr) {
			wait(abortMovementDueToFailedDr(self, record));
		}
	} else if (record->movementState == MovementState::SWITCHING) {
		wait(sendFinishRequestToDestination(self, record));
	} else if (record->movementState == MovementState::COMPLETED) {
		wait(abortPeer(self, *record));
	} else if (record->movementState == MovementState::ERROR) {
		// Do nothing
	} else {
		ASSERT(false);
	}

	return Void();
}

ACTOR Future<Void> TenantBalancer::recoverDestinationMovement(TenantBalancer* self, MovementRecord* record) {
	bool activeDr = wait(checkForActiveDr(self, record->getTagName()));

	if (record->movementState == MovementState::INITIALIZING) {
		// Do nothing
	} else if (record->movementState == MovementState::STARTED) {
		// Do nothing
	} else if (record->movementState == MovementState::READY_FOR_SWITCH) {
		if (!activeDr) {
			wait(abortMovementDueToFailedDr(self, record));
		}
	} else if (record->movementState == MovementState::SWITCHING) {
		wait(flushAndNotifySourceCluster(self, record));
	} else if (record->movementState == MovementState::COMPLETED) {
		wait(flushAndNotifySourceCluster(self, record));
	} else if (record->movementState == MovementState::ERROR) {
		// Do nothing
	} else {
		ASSERT(false);
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