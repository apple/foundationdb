/*
 * ClusterControllerDBInfo.actor.cpp
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

#include "fdbclient/DatabaseContext.h"
#include "fdbserver/ClusterControllerDBInfo.h"
#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h" // must be last include

class ClusterControllerDBInfoImpl {
public:
	ACTOR static Future<Void> countClients(ClusterControllerDBInfo* self) {
		loop {
			wait(delay(SERVER_KNOBS->CC_PRUNE_CLIENTS_INTERVAL));

			self->clientCount = 0;
			for (auto itr = self->clientStatus.begin(); itr != self->clientStatus.end();) {
				if (now() - itr->second.first < 2 * SERVER_KNOBS->COORDINATOR_REGISTER_INTERVAL) {
					self->clientCount += itr->second.second.clientCount;
					++itr;
				} else {
					itr = self->clientStatus.erase(itr);
				}
			}
		}
	}

	ACTOR static Future<Void> clusterGetServerInfo(ClusterControllerDBInfo* self,
	                                               UID knownServerInfoID,
	                                               ReplyPromise<ServerDBInfo> reply) {
		while (self->serverInfo->get().id == knownServerInfoID) {
			choose {
				when(wait(yieldedFuture(self->serverInfo->onChange()))) {}
				when(wait(delayJittered(300))) {
					break;
				} // The server might be long gone!
			}
		}
		reply.send(self->serverInfo->get());
		return Void();
	}

	ACTOR static Future<Void> clusterOpenDatabase(ClusterControllerDBInfo* self, OpenDatabaseRequest req) {
		self->clientStatus[req.reply.getEndpoint().getPrimaryAddress()] = std::make_pair(now(), req);
		if (self->clientStatus.size() > 10000) {
			TraceEvent(SevWarnAlways, "TooManyClientStatusEntries").suppressFor(1.0);
		}

		while (self->clientInfo->get().id == req.knownClientInfoID) {
			choose {
				when(wait(self->clientInfo->onChange())) {}
				when(wait(delayJittered(SERVER_KNOBS->COORDINATOR_REGISTER_INTERVAL))) {
					break;
				} // The client might be long gone!
			}
		}

		req.reply.send(self->clientInfo->get());
		return Void();
	}

	ACTOR static Future<Void> monitorServerInfoConfig(ClusterControllerDBInfo* self) {
		loop {
			state ReadYourWritesTransaction tr(self->db);
			loop {
				try {
					tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);

					Optional<Value> configVal = wait(tr.get(latencyBandConfigKey));
					Optional<LatencyBandConfig> config;
					if (configVal.present()) {
						config = LatencyBandConfig::parse(configVal.get());
					}

					auto serverInfo = self->serverInfo->get();
					if (config != serverInfo.latencyBandConfig) {
						TraceEvent("LatencyBandConfigChanged").detail("Present", config.present());
						serverInfo.id = deterministicRandom()->randomUniqueID();
						serverInfo.infoGeneration = ++self->dbInfoCount;
						serverInfo.latencyBandConfig = config;
						self->serverInfo->set(serverInfo);
					}

					state Future<Void> configChangeFuture = tr.watch(latencyBandConfigKey);

					wait(tr.commit());
					wait(configChangeFuture);

					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}
	}

	ACTOR static Future<Void> monitorGlobalConfig(ClusterControllerDBInfo* self) {
		loop {
			state ReadYourWritesTransaction tr(self->db);
			loop {
				try {
					tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					state Optional<Value> globalConfigVersion = wait(tr.get(globalConfigVersionKey));
					state ClientDBInfo clientInfo = self->serverInfo->get().client;

					if (globalConfigVersion.present()) {
						// Since the history keys end with versionstamps, they
						// should be sorted correctly (versionstamps are stored in
						// big-endian order).
						RangeResult globalConfigHistory =
						    wait(tr.getRange(globalConfigHistoryKeys, CLIENT_KNOBS->TOO_MANY));
						// If the global configuration version key has been set,
						// the history should contain at least one item.
						ASSERT(globalConfigHistory.size() > 0);
						clientInfo.history.clear();

						for (const auto& kv : globalConfigHistory) {
							ObjectReader reader(kv.value.begin(), IncludeVersion());
							if (reader.protocolVersion() != g_network->protocolVersion()) {
								// If the protocol version has changed, the
								// GlobalConfig actor should refresh its view by
								// reading the entire global configuration key
								// range.  Setting the version to the max int64_t
								// will always cause the global configuration
								// updater to refresh its view of the configuration
								// keyspace.
								clientInfo.history.clear();
								clientInfo.history.emplace_back(std::numeric_limits<Version>::max());
								break;
							}

							VersionHistory vh;
							reader.deserialize(vh);

							// Read commit version out of versionstamp at end of key.
							BinaryReader versionReader =
							    BinaryReader(kv.key.removePrefix(globalConfigHistoryPrefix), Unversioned());
							Version historyCommitVersion;
							versionReader >> historyCommitVersion;
							historyCommitVersion = bigEndian64(historyCommitVersion);
							vh.version = historyCommitVersion;

							clientInfo.history.push_back(std::move(vh));
						}

						if (clientInfo.history.size() > 0) {
							// The first item in the historical list of mutations
							// is only used to:
							//   a) Recognize that some historical changes may have
							//      been missed, and the entire global
							//      configuration keyspace needs to be read, or..
							//   b) Check which historical updates have already
							//      been applied. If this is the case, the first
							//      history item must have a version greater than
							//      or equal to whatever version the global
							//      configuration was last updated at, and
							//      therefore won't need to be applied again.
							clientInfo.history[0].mutations = Standalone<VectorRef<MutationRef>>();
						}

						clientInfo.id = deterministicRandom()->randomUniqueID();
						// Update ServerDBInfo so fdbserver processes receive updated history.
						ServerDBInfo serverInfo = self->serverInfo->get();
						serverInfo.id = deterministicRandom()->randomUniqueID();
						serverInfo.infoGeneration = ++self->dbInfoCount;
						serverInfo.client = clientInfo;
						self->serverInfo->set(serverInfo);

						// Update ClientDBInfo so client processes receive updated history.
						self->clientInfo->set(clientInfo);
					}

					state Future<Void> globalConfigFuture = tr.watch(globalConfigVersionKey);
					wait(tr.commit());
					wait(globalConfigFuture);
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}
	}

}; // class ClusterControllerDBInfoImpl

ClusterControllerDBInfo::ClusterControllerDBInfo()
  : clientInfo(new AsyncVar<ClientDBInfo>()), serverInfo(new AsyncVar<ServerDBInfo>()), masterRegistrationCount(0),
    dbInfoCount(0), recoveryStalled(false), forceRecovery(false),
    db(DatabaseContext::create(clientInfo,
                               Future<Void>(),
                               LocalityData(),
                               EnableLocalityLoadBalance::True,
                               TaskPriority::DefaultEndpoint,
                               LockAware::True)), // SOMEDAY: Locality!
    unfinishedRecoveries(0), logGenerations(0), clientCount(0), blobGranulesEnabled(config.blobGranulesEnabled),
    blobRestoreEnabled(false) {
	clientCounter = countClients();
}

void ClusterControllerDBInfo::setDistributor(const DataDistributorInterface& interf) {
	auto newInfo = serverInfo->get();
	newInfo.id = deterministicRandom()->randomUniqueID();
	newInfo.infoGeneration = ++dbInfoCount;
	newInfo.distributor = interf;
	serverInfo->set(newInfo);
}

void ClusterControllerDBInfo::setRatekeeper(const RatekeeperInterface& interf) {
	auto newInfo = serverInfo->get();
	newInfo.id = deterministicRandom()->randomUniqueID();
	newInfo.infoGeneration = ++dbInfoCount;
	newInfo.ratekeeper = interf;
	serverInfo->set(newInfo);
}

void ClusterControllerDBInfo::setBlobManager(const BlobManagerInterface& interf) {
	auto newInfo = serverInfo->get();
	newInfo.id = deterministicRandom()->randomUniqueID();
	newInfo.infoGeneration = ++dbInfoCount;
	newInfo.blobManager = interf;
	serverInfo->set(newInfo);
}

void ClusterControllerDBInfo::setBlobMigrator(const BlobMigratorInterface& interf) {
	auto newInfo = serverInfo->get();
	newInfo.id = deterministicRandom()->randomUniqueID();
	newInfo.infoGeneration = ++dbInfoCount;
	newInfo.blobMigrator = interf;
	serverInfo->set(newInfo);
}

void ClusterControllerDBInfo::setEncryptKeyProxy(const EncryptKeyProxyInterface& interf) {
	auto newInfo = serverInfo->get();
	auto newClientInfo = clientInfo->get();
	newClientInfo.id = deterministicRandom()->randomUniqueID();
	newInfo.id = deterministicRandom()->randomUniqueID();
	newInfo.infoGeneration = ++dbInfoCount;
	newInfo.encryptKeyProxy = interf;
	newInfo.client.encryptKeyProxy = interf;
	newClientInfo.encryptKeyProxy = interf;
	serverInfo->set(newInfo);
	clientInfo->set(newClientInfo);
}

void ClusterControllerDBInfo::setConsistencyScan(const ConsistencyScanInterface& interf) {
	auto newInfo = serverInfo->get();
	newInfo.id = deterministicRandom()->randomUniqueID();
	newInfo.infoGeneration = ++dbInfoCount;
	newInfo.consistencyScan = interf;
	serverInfo->set(newInfo);
}

void ClusterControllerDBInfo::clearInterf(ProcessClass::ClassType t) {
	auto newInfo = serverInfo->get();
	auto newClientInfo = clientInfo->get();
	newInfo.id = deterministicRandom()->randomUniqueID();
	newClientInfo.id = deterministicRandom()->randomUniqueID();
	newInfo.infoGeneration = ++dbInfoCount;
	if (t == ProcessClass::DataDistributorClass) {
		newInfo.distributor = Optional<DataDistributorInterface>();
	} else if (t == ProcessClass::RatekeeperClass) {
		newInfo.ratekeeper = Optional<RatekeeperInterface>();
	} else if (t == ProcessClass::BlobManagerClass) {
		newInfo.blobManager = Optional<BlobManagerInterface>();
	} else if (t == ProcessClass::BlobMigratorClass) {
		newInfo.blobMigrator = Optional<BlobMigratorInterface>();
	} else if (t == ProcessClass::EncryptKeyProxyClass) {
		newInfo.encryptKeyProxy = Optional<EncryptKeyProxyInterface>();
		newInfo.client.encryptKeyProxy = Optional<EncryptKeyProxyInterface>();
		newClientInfo.encryptKeyProxy = Optional<EncryptKeyProxyInterface>();
	} else if (t == ProcessClass::ConsistencyScanClass) {
		newInfo.consistencyScan = Optional<ConsistencyScanInterface>();
	}
	serverInfo->set(newInfo);
	clientInfo->set(newClientInfo);
}

Future<Void> ClusterControllerDBInfo::countClients() {
	return ClusterControllerDBInfoImpl::countClients(this);
}

Future<Void> ClusterControllerDBInfo::monitorServerInfoConfig() {
	return ClusterControllerDBInfoImpl::monitorServerInfoConfig(this);
}

Future<Void> ClusterControllerDBInfo::clusterOpenDatabase(OpenDatabaseRequest req) {
	return ClusterControllerDBInfoImpl::clusterOpenDatabase(this, req);
}

Future<Void> ClusterControllerDBInfo::clusterGetServerInfo(UID knownServerInfoID, ReplyPromise<ServerDBInfo> reply) {
	return ClusterControllerDBInfoImpl::clusterGetServerInfo(this, knownServerInfoID, reply);
}

Future<Void> ClusterControllerDBInfo::monitorGlobalConfig() {
	return ClusterControllerDBInfoImpl::monitorGlobalConfig(this);
}

void ClusterControllerDBInfo::markConnectionIncompatible(NetworkAddress address) {
	incompatibleConnections[address] = now() + SERVER_KNOBS->INCOMPATIBLE_PEERS_LOGGING_INTERVAL;
}

std::vector<NetworkAddress> ClusterControllerDBInfo::getIncompatibleConnections() {
	std::vector<NetworkAddress> result;
	for (auto it = incompatibleConnections.begin(); it != incompatibleConnections.end();) {
		auto const& [address, expirationTime] = *it;
		if (expirationTime < now()) {
			it = incompatibleConnections.erase(it);
		} else {
			result.push_back(address);
			it++;
		}
	}
	return result;
}
