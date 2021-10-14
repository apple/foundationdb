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
#include "fdbserver/Knobs.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/ITrace.h"
#include "flow/Trace.h"
#include "fdbclient/StatusClient.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include <string>
#include <unordered_map>
#include <vector>

// TODO: do we need any recoverable error states?
enum class MovementState { INITIALIZING, STARTED, READY_FOR_SWITCH, COMPLETED };
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

struct TenantBalancer {
	TenantBalancer(TenantBalancerInterface tbi, Reference<AsyncVar<ServerDBInfo> const> dbInfo)
	  : tbi(tbi), dbInfo(dbInfo), actors(false), tenantBalancerMetrics("TenantBalancer", tbi.id().toString()),
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
		specialCounter(tenantBalancerMetrics, "ActiveSourceMoves", [this]() { return externalDatabases.size(); });
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
				TraceEvent(SevDebug, "TenantBalancerPersistMovementRecordError", self->tbi.id())
				    .error(e)
				    .detail("SourcePrefix", record.getSourcePrefix())
				    .detail("DestinationPrefix", record.getDestinationPrefix());

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
				TraceEvent(SevDebug, "TenantBalancerClearMovementRecordError", self->tbi.id())
				    .error(e)
				    .detail("SourcePrefix", record.getSourcePrefix())
				    .detail("DestinationPrefix", record.getDestinationPrefix());

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
				TraceEvent(SevDebug, "TenantBalancerReuseDatabase", self->tbi.id())
				    .detail("Name", name)
				    .detail("ConnectionString", connectionString);

				return itr->second;
			}

			TraceEvent("TenantBalancerExternalDatabaseMismatch", self->tbi.id())
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
				TraceEvent(SevDebug, "TenantBalancerAddExternalDatabaseError", self->tbi.id())
				    .error(e)
				    .detail("Name", name)
				    .detail("ConnectionString", connectionString);

				wait(tr.onError(e));
			}
		}

		Database externalDb = Database::createDatabase(
		    makeReference<ClusterConnectionKey>(self->db, dbKey, ClusterConnectionString(connectionString), true),
		    Database::API_VERSION_LATEST,
		    IsInternal::True,
		    self->tbi.locality);

		TraceEvent("TenantBalancerAddedExternalDatabase", self->tbi.id())
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
						self->externalDatabases[name] = Database::createDatabase(
						    makeReference<ClusterConnectionKey>(
						        self->db, kv.key, ClusterConnectionString(kv.value.toString()), true),
						    Database::API_VERSION_LATEST,
						    IsInternal::True,
						    self->tbi.locality);

						TraceEvent(SevDebug, "TenantBalancerRecoverDatabaseConnection", self->tbi.id())
						    .detail("Name", name)
						    .detail("ConnectionString", kv.value);
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
				TraceEvent(SevDebug, "TenantBalancerRecoveryError", self->tbi.id()).error(e);
				wait(tr.onError(e));
			}
		}

		for (auto itr : self->outgoingMovements) {
			auto dbItr = self->externalDatabases.find(itr.second.getDestinationDatabaseName());
			ASSERT(dbItr != self->externalDatabases.end());
			itr.second.setDestinationDatabase(dbItr->second);
		}

		TraceEvent("TenantBalancerRecovered", self->tbi.id());
		return Void();
	}

	Future<Void> recover() { return recoverImpl(this); }

	ACTOR static Future<bool> isTenantEmpty(Reference<ReadYourWritesTransaction> tr, Key prefix) {
		state RangeResult rangeResult = wait(tr->getRange(prefixRange(prefix), 1));
		return rangeResult.empty();
	}

	Future<bool> static isTenantEmpty(Database db, Key prefix) {
		return runRYWTransaction(db,
		                         [=](Reference<ReadYourWritesTransaction> tr) { return isTenantEmpty(tr, prefix); });
	}

	CounterCollection tenantBalancerMetrics;

	Counter moveTenantToClusterRequests;
	Counter receiveTenantFromClusterRequests;
	Counter getActiveMovementsRequests;
	Counter finishSourceMovementRequests;
	Counter finishDestinationMovementRequests;
	Counter abortMovementRequests;
	Counter cleanupMovementSourceRequests;

private:
	// TODO: ref count external databases and delete when all references are gone
	std::unordered_map<std::string, Database> externalDatabases;
	std::map<Key, SourceMovementRecord> outgoingMovements;
	std::map<Key, DestinationMovementRecord> incomingMovements;
};

// src
ACTOR Future<Void> moveTenantToCluster(TenantBalancer* self, MoveTenantToClusterRequest req) {
	TraceEvent(SevDebug, "TenantBalancerMoveTenantToCluster", self->tbi.id())
	    .detail("SourcePrefix", req.sourcePrefix)
	    .detail("DestinationPrefix", req.destPrefix)
	    .detail("DestinationConnectionString", req.destConnectionString);

	++self->moveTenantToClusterRequests;

	try {
		// 1.Extract necessary data from metadata
		state Optional<Database> destDatabase =
		    wait(self->addExternalDatabase(req.destConnectionString, req.destConnectionString));
		if (!destDatabase.present()) {
			// TODO: how to handle this?
			ASSERT(false);
		}

		state SourceMovementRecord sourceMovementRecord(
		    req.sourcePrefix, req.destPrefix, req.destConnectionString, destDatabase.get());
		Standalone<VectorRef<KeyRangeRef>> backupRanges;
		backupRanges.push_back_deep(backupRanges.arena(), prefixRange(req.sourcePrefix));
		// TODO we'll need to log the metadata once here and then again after we submit the backup,
		// this is going to require having a movement state field in our record that we can update as we make progress;

		// 2.Use DR to do datamovement
		wait(self->agent.submitBackup(self->getExternalDatabase(req.destConnectionString).get(),
		                              KeyRef(sourceMovementRecord.getTagName()),
		                              backupRanges,
		                              StopWhenDone::False,
		                              req.destPrefix,
		                              req.sourcePrefix,
		                              LockDB::False));

		// Check if a backup agent is running
		state bool agentRunning = wait(self->agent.checkActive(destDatabase.get()));

		// 3.Do record
		wait(self->saveOutgoingMovement(sourceMovementRecord));

		MoveTenantToClusterReply reply;
		if (!agentRunning) {
			throw movement_agent_not_running();
		}

		TraceEvent(SevDebug, "TenantBalancerMoveTenantToClusterComplete", self->tbi.id())
		    .detail("SourcePrefix", req.sourcePrefix)
		    .detail("DestinationPrefix", req.destPrefix)
		    .detail("DestinationConnectionString", req.destConnectionString);

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
		// 0.Extract necessary variables
		state Optional<Database> srcDatabase =
		    wait(self->addExternalDatabase(req.srcConnectionString, req.srcConnectionString));

		if (!srcDatabase.present()) {
			// TODO: how to handle this?
			ASSERT(false);
		}

		// 1.Lock the destination before we start the movement
		// TODO

		// 2.Check if prefix is empty.
		bool isPrefixEmpty = wait(self->isTenantEmpty(self->db, req.destPrefix));
		if (!isPrefixEmpty) {
			throw movement_dest_prefix_not_empty();
		}

		// 3.Do record
		DestinationMovementRecord destinationMovementRecord(
		    req.sourcePrefix, req.destPrefix, req.srcConnectionString, srcDatabase.get());
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
		// TODO distinguish dr and data movement
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

		// TODO: populate results from our own records (outgoing/incoming movements), add extra data from DR status

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
					if (running) {
						std::string backup_state, secondsBehind;
						tag.tryGet("backup_state", backup_state);
						tag.tryGet("seconds_behind", secondsBehind);
						TenantMovementInfo tenantMovementInfo;
						tenantMovementInfo.movementLocation =
						    isSrc ? TenantMovementInfo::Location::SOURCE : TenantMovementInfo::Location::DEST;
						tenantMovementInfo.tenantMovementStatus = backup_state;
						tenantMovementInfo.secondsBehind = secondsBehind;

						Key sourcePrefix = Key(getPrefixFromTagName(itr.first));
						SourceMovementRecord sourceMovementRecord = self->getOutgoingMovement(sourcePrefix);
						tenantMovementInfo.sourcePrefix = sourcePrefix;
						tenantMovementInfo.destPrefix = sourceMovementRecord.getDestinationPrefix();
						tenantMovementInfo.destConnectionString = sourceMovementRecord.getDestinationDatabase()
						                                              ->getConnectionRecord()
						                                              ->getConnectionString()
						                                              .toString();
						recorder.push_back(tenantMovementInfo);
					}
				}
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

ACTOR Future<Void> finishSourceMovement(TenantBalancer* self, FinishSourceMovementRequest req) {
	++self->finishSourceMovementRequests;

	try {
		// 1.Get target tenant and version
		// TODO

		// 2. Finish movement
		Standalone<VectorRef<KeyRangeRef>> backupRanges;
		backupRanges.push_back_deep(backupRanges.arena(), prefixRange(req.sourceTenant));
		Database dest = self->getOutgoingMovement(Key(req.sourceTenant)).getDestinationDatabase();
		// TODO check if arguments here are correct - ForceAction especially
		// TODO check if maxLagSeconds is exceeded
		wait(self->agent.atomicSwitchover(dest,
		                                  KeyRef(dest->getConnectionRecord()->getConnectionString().toString()),
		                                  backupRanges,
		                                  StringRef(),
		                                  StringRef(),
		                                  ForceAction{ true },
		                                  false));

		// 3.Remove metadata
		self->clearOutgoingMovement(self->getOutgoingMovement(Key(req.sourceTenant)));

		FinishSourceMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> finishDestinationMovement(TenantBalancer* self, FinishDestinationMovementRequest req) {
	++self->finishDestinationMovementRequests;
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		// 1.Unlock the prefix of dest db
		// TODO

		// 2.Remove metadata
		self->clearIncomingMovement(self->getIncomingMovement(Key(req.destinationTenant)));

		// 3.Finish movement based on prefix and version
		// TODO
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
		if (req.isSrc) {
			SourceMovementRecord srcRecord = self->getOutgoingMovement(Key(req.tenantName));
			targetDB = srcRecord.getDestinationDatabase();
			tagName = srcRecord.getTagName();
			self->clearOutgoingMovement(self->getOutgoingMovement(Key(req.tenantName)));
		} else {
			DestinationMovementRecord destRecord = self->getIncomingMovement(Key(req.tenantName));
			targetDB = destRecord.getSourceDatabase();
			tagName = destRecord.getTagName();
			self->clearIncomingMovement(self->getIncomingMovement(Key(req.tenantName)));
		}
		// TODO: make sure the parameters in abortBackup() are correct
		wait(self->agent.abortBackup(
		    targetDB, Key(tagName), PartialBackup{ false }, AbortOldBackup::False, DstOnly{ false }));
		wait(self->agent.unlockBackup(targetDB, Key(tagName)));
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
		// TODO once the range has been unlocked, it will no longer be legal to run cleanup
		state std::string tenantName = req.tenantName;
		if (req.cleanupType != CleanupMovementSourceRequest::CleanupType::UNLOCK) {
			// erase
			wait(self->agent.clearPrefix(self->db, Key(tenantName)));
		}
		if (req.cleanupType != CleanupMovementSourceRequest::CleanupType::ERASE) {
			// TODO unlock
			self->clearOutgoingMovement(self->getOutgoingMovement(Key(req.tenantName)));
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