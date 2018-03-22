/*
 * ManagementAPI.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "flow/actorcompiler.h"
#include "ManagementAPI.h"

#include "SystemData.h"
#include "NativeAPI.h"
#include "CoordinationInterface.h"
#include "DatabaseContext.h"
#include "fdbrpc/simulator.h"
#include "StatusClient.h"
#include "flow/UnitTest.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbrpc/Replication.h"

static Future<vector<AddressExclusion>> getExcludedServers( Transaction* const& tr );

bool isInteger(const std::string& s) {
	if( s.empty() ) return false;
	char *p;
	auto ign = strtol(s.c_str(), &p, 10);
	return (*p == 0);
}

// Defines the mapping between configuration names (as exposed by fdbcli, buildConfiguration()) and actual configuration parameters
std::map<std::string, std::string> configForToken( std::string const& mode ) {
	std::map<std::string, std::string> out;
	std::string p = configKeysPrefix.toString();

	if (mode == "new") {
		out[p+"initialized"]="1";
		return out;
	}

	size_t pos;

	// key:=value is unvalidated and unchecked
	pos = mode.find( ":=" );
	if( pos != std::string::npos ) {
		out[p+mode.substr(0, pos)] = mode.substr(pos+2);
		return out;
	}

	// key=value is constrained to a limited set of options and basic validation is performed
	pos = mode.find( "=" );
	if( pos != std::string::npos ) {
		std::string key = mode.substr(0, pos);
		std::string value = mode.substr(pos+1);

		if( (key == "logs" || key == "proxies" || key == "resolvers") && isInteger(value) ) {
			out[p+key] = value;
		}

		return out;
	}

	Optional<KeyValueStoreType> storeType;
	if (mode == "ssd-1") {
		storeType= KeyValueStoreType::SSD_BTREE_V1;
	} else if (mode == "ssd" || mode == "ssd-2") {
		storeType = KeyValueStoreType::SSD_BTREE_V2;
	} else if (mode == "memory") {
		storeType= KeyValueStoreType::MEMORY;
	}
	// Add any new store types to fdbserver/workloads/ConfigureDatabase, too

	if (storeType.present()) {
		out[p+"log_engine"] = out[p+"storage_engine"] = format("%d", storeType.get());
		return out;
	}

	std::string redundancy, log_replicas;
	IRepPolicyRef storagePolicy;
	IRepPolicyRef tLogPolicy;

	bool redundancySpecified = true;
	if (mode == "single") {
		redundancy="1";
		log_replicas="1";
		storagePolicy = tLogPolicy = IRepPolicyRef(new PolicyOne());

	} else if(mode == "double" || mode == "fast_recovery_double") {
		redundancy="2";
		log_replicas="2";
		storagePolicy = tLogPolicy = IRepPolicyRef(new PolicyAcross(2, "zoneid", IRepPolicyRef(new PolicyOne())));
	} else if(mode == "triple" || mode == "fast_recovery_triple") {
		redundancy="3";
		log_replicas="3";
		storagePolicy = tLogPolicy = IRepPolicyRef(new PolicyAcross(3, "zoneid", IRepPolicyRef(new PolicyOne())));
	} else if(mode == "three_datacenter" || mode == "multi_dc") {
		redundancy="6";
		log_replicas="4";
		storagePolicy = IRepPolicyRef(new PolicyAcross(3, "dcid",
			IRepPolicyRef(new PolicyAcross(2, "zoneid", IRepPolicyRef(new PolicyOne())))
		));
		tLogPolicy = IRepPolicyRef(new PolicyAcross(2, "dcid",
			IRepPolicyRef(new PolicyAcross(2, "zoneid", IRepPolicyRef(new PolicyOne())))
		));
	} else if(mode == "three_data_hall") {
		redundancy="3";
		log_replicas="4";
		storagePolicy = IRepPolicyRef(new PolicyAcross(3, "data_hall", IRepPolicyRef(new PolicyOne())));
		tLogPolicy = IRepPolicyRef(new PolicyAcross(2, "data_hall",
			IRepPolicyRef(new PolicyAcross(2, "zoneid", IRepPolicyRef(new PolicyOne())))
		));
	} else
		redundancySpecified = false;
	if (redundancySpecified) {
		out[p+"storage_replicas"] =
			out[p+"storage_quorum"] = redundancy;
		out[p+"log_replicas"] = log_replicas;
		out[p+"log_anti_quorum"] = "0";

		BinaryWriter policyWriter(IncludeVersion());
		serializeReplicationPolicy(policyWriter, storagePolicy);
		out[p+"storage_replication_policy"] = policyWriter.toStringRef().toString();

		policyWriter = BinaryWriter(IncludeVersion());
		serializeReplicationPolicy(policyWriter, tLogPolicy);
		out[p+"log_replication_policy"] = policyWriter.toStringRef().toString();

		return out;
	}

	return out;
}

ConfigurationResult::Type buildConfiguration( std::vector<StringRef> const& modeTokens, std::map<std::string, std::string>& outConf ) {
	for(auto it : modeTokens) {
		std::string mode = it.toString();

		auto m = configForToken( mode );
		if( !m.size() )
			return ConfigurationResult::UNKNOWN_OPTION;

		for( auto t = m.begin(); t != m.end(); ++t ) {
			if( outConf.count( t->first ) ) {
				return ConfigurationResult::CONFLICTING_OPTIONS;
			}
			outConf[t->first] = t->second;
		}
	}
	auto p = configKeysPrefix.toString();
	if(!outConf.count(p + "storage_replication_policy") && outConf.count(p + "storage_replicas")) {
		int storageCount = stoi(outConf[p + "storage_replicas"]);
		IRepPolicyRef storagePolicy = IRepPolicyRef(new PolicyAcross(storageCount, "zoneid", IRepPolicyRef(new PolicyOne())));
		BinaryWriter policyWriter(IncludeVersion());
		serializeReplicationPolicy(policyWriter, storagePolicy);
		outConf[p+"storage_replication_policy"] = policyWriter.toStringRef().toString();
	}

	if(!outConf.count(p + "log_replication_policy") && outConf.count(p + "log_replicas")) {
		int logCount = stoi(outConf[p + "log_replicas"]);
		IRepPolicyRef logPolicy = IRepPolicyRef(new PolicyAcross(logCount, "zoneid", IRepPolicyRef(new PolicyOne())));
		BinaryWriter policyWriter(IncludeVersion());
		serializeReplicationPolicy(policyWriter, logPolicy);
		outConf[p+"log_replication_policy"] = policyWriter.toStringRef().toString();
	}

	return ConfigurationResult::SUCCESS;
}

ConfigurationResult::Type buildConfiguration( std::string const& configMode, std::map<std::string, std::string>& outConf ) {
	std::vector<StringRef> modes;

	int p = 0;
	while ( p < configMode.size() ) {
		int end = configMode.find_first_of(' ', p);
		if (end == configMode.npos) end = configMode.size();
		modes.push_back( StringRef(configMode).substr(p, end-p) );
		p = end+1;
	}

	return buildConfiguration( modes, outConf );
}

bool isCompleteConfiguration( std::map<std::string, std::string> const& options ) {
	std::string p = configKeysPrefix.toString();

	return 	options.count( p+"log_replicas" ) == 1 &&
			options.count( p+"log_anti_quorum" ) == 1 &&
			options.count( p+"storage_quorum" ) == 1 &&
			options.count( p+"storage_replicas" ) == 1 &&
			options.count( p+"log_engine" ) == 1 &&
			options.count( p+"storage_engine" ) == 1;
}

ACTOR Future<ConfigurationResult::Type> changeConfig( Database cx, std::map<std::string, std::string> m ) {
	state StringRef initIdKey = LiteralStringRef( "\xff/init_id" );
	state Transaction tr(cx);

	if (!m.size())
		return ConfigurationResult::NO_OPTIONS_PROVIDED;

	// make sure we have essential configuration options
	std::string initKey = configKeysPrefix.toString() + "initialized";
	state bool creating = m.count( initKey ) != 0;
	if (creating) {
		m[initIdKey.toString()] = g_random->randomUniqueID().toString();
		if (!isCompleteConfiguration(m))
			return ConfigurationResult::INCOMPLETE_CONFIGURATION;
	}

	loop {
		try {
			tr.setOption( FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE );
			tr.setOption( FDBTransactionOptions::LOCK_AWARE );
			if (creating) {
				tr.setOption( FDBTransactionOptions::INITIALIZE_NEW_DATABASE );
				tr.addReadConflictRange( singleKeyRange( initIdKey ) );
			} else if (m.size()) {
				// might be used in an emergency transaction, so make sure it is retry-self-conflicting and CAUSAL_WRITE_RISKY
				tr.setOption( FDBTransactionOptions::CAUSAL_WRITE_RISKY );
				tr.addReadConflictRange( singleKeyRange(m.begin()->first) );
			}

			for(auto i=m.begin(); i!=m.end(); ++i)
				tr.set( StringRef(i->first), StringRef(i->second) );

			Void _ = wait( tr.commit() );
			break;
		} catch (Error& e) {
			state Error e1(e);
			if ( (e.code() == error_code_not_committed || e.code() == error_code_transaction_too_old ) && creating) {
				// The database now exists.  Determine whether we created it or it was already existing/created by someone else.  The latter is an error.
				tr.reset();
				loop {
					try {
						tr.setOption( FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE );
						tr.setOption(FDBTransactionOptions::LOCK_AWARE);

						Optional<Value> v = wait( tr.get( initIdKey ) );
						if (v != m[initIdKey.toString()])
							return ConfigurationResult::DATABASE_ALREADY_CREATED;
						else
							return ConfigurationResult::DATABASE_CREATED;
					} catch (Error& e2) {
						Void _ = wait( tr.onError(e2) );
					}
				}
			}
			Void _ = wait( tr.onError(e1) );
		}
	}
	return ConfigurationResult::SUCCESS;
}

ConfigureAutoResult parseConfig( StatusObject const& status ) {
	ConfigureAutoResult result;
	StatusObjectReader statusObj(status);

	StatusObjectReader statusObjCluster;
	if (!statusObj.get("cluster", statusObjCluster))
		return ConfigureAutoResult();

	StatusObjectReader statusObjConfig;
	if (!statusObjCluster.get("configuration", statusObjConfig))
		return ConfigureAutoResult();

	if (!statusObjConfig.get("redundancy.factor", result.old_replication))
		return ConfigureAutoResult();

	result.auto_replication = result.old_replication;

	int storage_replication;
	int log_replication;
	if( result.old_replication == "single" ) {
		result.auto_replication = "double";
		storage_replication = 2;
		log_replication = 2;
	} else if( result.old_replication == "double" || result.old_replication == "fast_recovery_double" ) {
		storage_replication = 2;
		log_replication = 2;
	} else if( result.old_replication == "triple" || result.old_replication == "fast_recovery_triple" ) {
		storage_replication = 3;
		log_replication = 3;
	} else if( result.old_replication == "three_datacenter" ) {
		storage_replication = 6;
		log_replication = 4;
	} else
		return ConfigureAutoResult();

	StatusObjectReader machinesMap;
	if (!statusObjCluster.get("machines", machinesMap))
		return ConfigureAutoResult();

	std::map<std::string, std::string> machineid_dcid;
	std::set<std::string> datacenters;
	int machineCount = 0;
	for (auto mach : machinesMap.obj()) {
		StatusObjectReader machine(mach.second);
		std::string dcId;
		if (machine.get("datacenter_id", dcId)) {
			machineid_dcid[mach.first] = dcId;
			datacenters.insert(dcId);
		}
		machineCount++;
	}

	result.machines = machineCount;

	if(datacenters.size() > 1)
		return ConfigureAutoResult();

	StatusObjectReader processesMap;
	if (!statusObjCluster.get("processes", processesMap))
		return ConfigureAutoResult();

	std::set<std::string> oldMachinesWithTransaction;
	int oldTransactionProcesses = 0;
	std::map<std::string, std::vector<std::pair<NetworkAddress, ProcessClass>>> machine_processes;
	int processCount = 0;
	for (auto proc : processesMap.obj()){
		StatusObjectReader process(proc.second);
		if (!process.has("excluded") || !process.last().get_bool()) {
			std::string addrStr;
			if (!process.get("address", addrStr))
				return ConfigureAutoResult();
			std::string class_source;
			if (!process.get("class_source", class_source))
				return ConfigureAutoResult();
			std::string class_type;
			if (!process.get("class_type", class_type))
				return ConfigureAutoResult();
			std::string machineId;
			if (!process.get("machine_id", machineId))
				return ConfigureAutoResult();

			NetworkAddress addr = NetworkAddress::parse(addrStr);
			ProcessClass processClass(class_type, class_source);

			if(processClass.classType() == ProcessClass::TransactionClass || processClass.classType() == ProcessClass::LogClass) {
				oldMachinesWithTransaction.insert(machineId);
			}

			if(processClass.classType() == ProcessClass::TransactionClass || processClass.classType() == ProcessClass::ProxyClass || processClass.classType() == ProcessClass::ResolutionClass || processClass.classType() == ProcessClass::StatelessClass || processClass.classType() == ProcessClass::LogClass) {
				oldTransactionProcesses++;
			}

			if( processClass.classSource() == ProcessClass::AutoSource ) {
				processClass = ProcessClass(ProcessClass::UnsetClass, ProcessClass::CommandLineSource);
				result.address_class[addr] = processClass;
			}

			if( processClass.classType() != ProcessClass::TesterClass ) {
				machine_processes[machineId].push_back(std::make_pair(addr, processClass));
				processCount++;
			}
		}
	}

	result.processes = processCount;
	result.old_processes_with_transaction = oldTransactionProcesses;
	result.old_machines_with_transaction = oldMachinesWithTransaction.size();

	std::map<std::pair<int, std::string>, std::vector<std::pair<NetworkAddress, ProcessClass>>> count_processes;
	for( auto& it : machine_processes ) {
		count_processes[std::make_pair(it.second.size(), it.first)] = it.second;
	}

	std::set<std::string> machinesWithTransaction;
	std::set<std::string> machinesWithStorage;
	int totalTransactionProcesses = 0;
	int existingProxyCount = 0;
	int existingResolverCount = 0;
	int existingStatelessCount = 0;
	for( auto& it : machine_processes ) {
		for(auto& proc : it.second ) {
			if(proc.second == ProcessClass::TransactionClass || proc.second == ProcessClass::LogClass) {
				totalTransactionProcesses++;
				machinesWithTransaction.insert(it.first);
			}
			if(proc.second == ProcessClass::StatelessClass) {
				existingStatelessCount++;
			}
			if(proc.second == ProcessClass::ProxyClass) {
				existingProxyCount++;
			}
			if(proc.second == ProcessClass::ResolutionClass) {
				existingResolverCount++;
			}
			if(proc.second == ProcessClass::StorageClass) {
				machinesWithStorage.insert(it.first);
			}
			if(proc.second == ProcessClass::UnsetClass && proc.second.classSource() == ProcessClass::DBSource) {
				machinesWithStorage.insert(it.first);
			}
		}
	}

	if( processCount < 10 )
		return ConfigureAutoResult();

	result.desired_resolvers = 1;
	int resolverCount;
	if (!statusObjConfig.get("resolvers", result.old_resolvers)) {
		result.old_resolvers = CLIENT_KNOBS->DEFAULT_AUTO_RESOLVERS;
		statusObjConfig.get("auto_resolvers", result.old_resolvers);
		result.auto_resolvers = result.desired_resolvers;
		resolverCount = result.auto_resolvers;
	} else {
		result.auto_resolvers = result.old_resolvers;
		resolverCount = result.old_resolvers;
	}

	result.desired_proxies = std::min( 12, processCount / 15 );
	int proxyCount;
	if (!statusObjConfig.get("proxies", result.old_proxies)) {
		result.old_proxies = CLIENT_KNOBS->DEFAULT_AUTO_PROXIES;
		statusObjConfig.get("auto_proxies", result.old_proxies);
		result.auto_proxies = result.desired_proxies;
		proxyCount = result.auto_proxies;
	} else {
		result.auto_proxies = result.old_proxies;
		proxyCount = result.old_proxies;
	}

	result.desired_logs = std::min( 12, processCount / 20 );
	result.desired_logs = std::max( result.desired_logs, log_replication + 1 );
	result.desired_logs = std::min<int>( result.desired_logs, machine_processes.size() );
	int logCount;
	if (!statusObjConfig.get("logs", result.old_logs)) {
		result.old_logs = CLIENT_KNOBS->DEFAULT_AUTO_LOGS;
		statusObjConfig.get("auto_logs", result.old_logs);
		result.auto_logs = result.desired_logs;
		logCount = result.auto_logs;
	} else {
		result.auto_logs = result.old_logs;
		logCount = result.old_logs;
	}

	logCount = std::max(logCount, log_replication);

	totalTransactionProcesses += std::min(existingProxyCount, proxyCount);
	totalTransactionProcesses += std::min(existingResolverCount, resolverCount);
	totalTransactionProcesses += existingStatelessCount;

	//if one process on a machine is transaction class, make them all transaction class
	for( auto& it : count_processes ) {
		if( machinesWithTransaction.count(it.first.second) && !machinesWithStorage.count(it.first.second) ) {
			for(auto& proc : it.second ) {
				if(proc.second == ProcessClass::UnsetClass && proc.second.classSource() == ProcessClass::CommandLineSource) {
					result.address_class[proc.first] = ProcessClass(ProcessClass::TransactionClass, ProcessClass::AutoSource);
					totalTransactionProcesses++;
				}
			}
		}
	}

	int desiredTotalTransactionProcesses = logCount + resolverCount + proxyCount;

	//add machines with all transaction class until we have enough processes and enough machines
	for( auto& it : count_processes ) {
		if( machinesWithTransaction.size() >= logCount && totalTransactionProcesses >= desiredTotalTransactionProcesses )
			break;

		if( !machinesWithTransaction.count(it.first.second) && !machinesWithStorage.count(it.first.second) ) {
			for(auto& proc : it.second ) {
				if(proc.second == ProcessClass::UnsetClass && proc.second.classSource() == ProcessClass::CommandLineSource) {
					ASSERT(proc.second != ProcessClass::TransactionClass);
					result.address_class[proc.first] = ProcessClass(ProcessClass::TransactionClass, ProcessClass::AutoSource);
					totalTransactionProcesses++;
					machinesWithTransaction.insert(it.first.second);
				}
			}
		}
	}

	if( machinesWithTransaction.size() < logCount || totalTransactionProcesses < desiredTotalTransactionProcesses )
		return ConfigureAutoResult();

	result.auto_processes_with_transaction = totalTransactionProcesses;
	result.auto_machines_with_transaction = machinesWithTransaction.size();

	if( 3*totalTransactionProcesses > processCount )
		return ConfigureAutoResult();

	return result;
}

ACTOR Future<ConfigurationResult::Type> autoConfig( Database cx, ConfigureAutoResult conf ) {
	state Transaction tr(cx);

	if(!conf.address_class.size())
		return ConfigurationResult::INCOMPLETE_CONFIGURATION; //FIXME: correct return type

	loop {
		try {
			tr.setOption( FDBTransactionOptions::ACCESS_SYSTEM_KEYS );
			tr.setOption( FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE );
			tr.setOption( FDBTransactionOptions::LOCK_AWARE );

			vector<ProcessData> workers = wait( getWorkers(&tr) );
			std::map<NetworkAddress, Optional<Standalone<StringRef>>> address_processId;
			for(auto& w : workers) {
				address_processId[w.address] = w.locality.processId();
			}

			for(auto& it : conf.address_class) {
				if( it.second.classSource() == ProcessClass::CommandLineSource ) {
					tr.clear(processClassKeyFor(address_processId[it.first].get()));
				} else {
					tr.set(processClassKeyFor(address_processId[it.first].get()), processClassValue(it.second));
				}
			}

			if(conf.address_class.size())
				tr.set(processClassChangeKey, g_random->randomUniqueID().toString());

			if(conf.auto_logs != conf.old_logs)
				tr.set(configKeysPrefix.toString() + "auto_logs", format("%d", conf.auto_logs));

			if(conf.auto_proxies != conf.old_proxies)
				tr.set(configKeysPrefix.toString() + "auto_proxies", format("%d", conf.auto_proxies));

			if(conf.auto_resolvers != conf.old_resolvers)
				tr.set(configKeysPrefix.toString() + "auto_resolvers", format("%d", conf.auto_resolvers));


			if( conf.auto_replication != conf.old_replication ) {
				std::vector<StringRef> modes;
				modes.push_back(conf.auto_replication);
				std::map<std::string,std::string> m;
				auto r = buildConfiguration( modes, m );
				if (r != ConfigurationResult::SUCCESS)
					return r;

				for(auto& kv : m)
					tr.set(kv.first, kv.second);
			}

			Void _ = wait( tr.commit() );
			return ConfigurationResult::SUCCESS;
		} catch( Error &e ) {
			Void _ = wait( tr.onError(e));
		}
	}
}

Future<ConfigurationResult::Type> changeConfig( Database const& cx, std::vector<StringRef> const& modes, Optional<ConfigureAutoResult> const& conf ) {
	if( modes.size() && modes[0] == LiteralStringRef("auto") && conf.present() ) {
		return autoConfig(cx, conf.get());
	}

	std::map<std::string,std::string> m;
	auto r = buildConfiguration( modes, m );
	if (r != ConfigurationResult::SUCCESS)
		return r;
	return changeConfig(cx, m);
}

Future<ConfigurationResult::Type> changeConfig( Database const& cx, std::string const& modes ) {
	TraceEvent("ChangeConfig").detail("Mode", modes);
	std::map<std::string,std::string> m;
	auto r = buildConfiguration( modes, m );
	if (r != ConfigurationResult::SUCCESS)
		return r;
	return changeConfig(cx, m);
}

ACTOR Future<vector<ProcessData>> getWorkers( Transaction* tr ) {
	state Future<Standalone<RangeResultRef>> processClasses = tr->getRange( processClassKeys, CLIENT_KNOBS->TOO_MANY );
	state Future<Standalone<RangeResultRef>> processData = tr->getRange( workerListKeys, CLIENT_KNOBS->TOO_MANY );

	Void _ = wait( success(processClasses) && success(processData) );
	ASSERT( !processClasses.get().more && processClasses.get().size() < CLIENT_KNOBS->TOO_MANY );
	ASSERT( !processData.get().more && processData.get().size() < CLIENT_KNOBS->TOO_MANY );

	std::map<Optional<Standalone<StringRef>>,ProcessClass> id_class;
	for( int i = 0; i < processClasses.get().size(); i++ ) {
		id_class[decodeProcessClassKey(processClasses.get()[i].key)] = decodeProcessClassValue(processClasses.get()[i].value);
	}

	std::vector<ProcessData> results;

	for( int i = 0; i < processData.get().size(); i++ ) {
		ProcessData data = decodeWorkerListValue(processData.get()[i].value);
		ProcessClass processClass = id_class[data.locality.processId()];

		if(processClass.classSource() == ProcessClass::DBSource || data.processClass.classType() == ProcessClass::UnsetClass)
			data.processClass = processClass;

		if(data.processClass.classType() != ProcessClass::TesterClass)
			results.push_back(data);
	}

	return results;
}

ACTOR Future<vector<ProcessData>> getWorkers( Database cx ) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption( FDBTransactionOptions::READ_SYSTEM_KEYS );
			tr.setOption( FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE );  // necessary?
			tr.setOption( FDBTransactionOptions::LOCK_AWARE );
			vector<ProcessData> workers = wait( getWorkers(&tr) );
			return workers;
		} catch (Error& e) {
			Void _ = wait( tr.onError(e) );
		}
	}
}

ACTOR Future<std::vector<NetworkAddress>> getCoordinators( Database cx ) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption( FDBTransactionOptions::LOCK_AWARE );
			Optional<Value> currentKey = wait( tr.get( coordinatorsKey ) );
			if (!currentKey.present())
				return std::vector<NetworkAddress>();

			return ClusterConnectionString( currentKey.get().toString() ).coordinators();
		} catch (Error& e) {
			Void _ = wait( tr.onError(e) );
		}
	}
}

ACTOR Future<CoordinatorsResult::Type> changeQuorum( Database cx, Reference<IQuorumChange> change ) {
	state Transaction tr(cx);
	state int retries = 0;

	//quorum changes do not balance coordinators evenly across datacenters
	if(g_network->isSimulated())
		g_simulator.maxCoordinatorsInDatacenter = g_simulator.killableMachines + 1;

	loop {
		try {
			tr.setOption( FDBTransactionOptions::LOCK_AWARE );
			Optional<Value> currentKey = wait( tr.get( coordinatorsKey ) );

			if (!currentKey.present())
				return CoordinatorsResult::BAD_DATABASE_STATE;  // Someone deleted this key entirely?

			state ClusterConnectionString old( currentKey.get().toString() );
			if ( cx->cluster && old.clusterKeyName().toString() != cx->cluster->getConnectionFile()->getConnectionString().clusterKeyName() )
				return CoordinatorsResult::BAD_DATABASE_STATE;  // Someone changed the "name" of the database??

			state CoordinatorsResult::Type result = CoordinatorsResult::SUCCESS;
			std::vector<NetworkAddress> _desiredCoordinators = wait( change->getDesiredCoordinators( &tr, old.coordinators(), Reference<ClusterConnectionFile>(new ClusterConnectionFile(old)), result ) );
			std::vector<NetworkAddress> desiredCoordinators = _desiredCoordinators;
			if (result != CoordinatorsResult::SUCCESS)
				return result;
			if (!desiredCoordinators.size())
				return CoordinatorsResult::INVALID_NETWORK_ADDRESSES;
			std::sort(desiredCoordinators.begin(), desiredCoordinators.end());

			std::string newName = change->getDesiredClusterKeyName();
			if (newName.empty()) newName = old.clusterKeyName().toString();

			if ( old.coordinators() == desiredCoordinators && old.clusterKeyName() == newName)
				return retries ? CoordinatorsResult::SUCCESS : CoordinatorsResult::SAME_NETWORK_ADDRESSES;

			state ClusterConnectionString conn( desiredCoordinators, StringRef( newName + ':' + g_random->randomAlphaNumeric( 32 ) ) );

			if(g_network->isSimulated()) {
				for(int i = 0; i < (desiredCoordinators.size()/2)+1; i++) {
					auto address = NetworkAddress(desiredCoordinators[i].ip,desiredCoordinators[i].port,true,false);
					g_simulator.protectedAddresses.insert(address);
					TraceEvent("ProtectCoordinator").detail("Address", address).backtrace();
				}
			}

			TraceEvent("AttemptingQuorumChange").detail("FromCS", old.toString()).detail("ToCS", conn.toString());
			TEST(old.clusterKeyName() != conn.clusterKeyName());  // Quorum change with new name
			TEST(old.clusterKeyName() == conn.clusterKeyName()); // Quorum change with unchanged name

			vector<Future<Optional<LeaderInfo>>> leaderServers;
			ClientCoordinators coord( Reference<ClusterConnectionFile>( new ClusterConnectionFile( conn ) ) );
			for( int i = 0; i < coord.clientLeaderServers.size(); i++ )
				leaderServers.push_back( retryBrokenPromise( coord.clientLeaderServers[i].getLeader, GetLeaderRequest( coord.clusterKey, UID() ), TaskCoordinationReply ) );

			choose {
				when( Void _ = wait( waitForAll( leaderServers ) ) ) {}
				when( Void _ = wait( delay(5.0) ) ) {
					return CoordinatorsResult::COORDINATOR_UNREACHABLE;
				}
			}

			tr.set( coordinatorsKey, conn.toString() );

			Void _ = wait( tr.commit() );
			ASSERT( false ); //commit should fail, but the value has changed
		} catch (Error& e) {
			TraceEvent("RetryQuorumChange").error(e).detail("Retries", retries);
			Void _ = wait( tr.onError(e) );
			++retries;
		}
	}
}

struct SpecifiedQuorumChange : IQuorumChange {
	vector<NetworkAddress> desired;
	explicit SpecifiedQuorumChange( vector<NetworkAddress> const& desired ) : desired(desired) {}
	virtual Future<vector<NetworkAddress>> getDesiredCoordinators( Transaction* tr, vector<NetworkAddress> oldCoordinators, Reference<ClusterConnectionFile>, CoordinatorsResult::Type& ) {
		return desired;
	}
};
Reference<IQuorumChange> specifiedQuorumChange(vector<NetworkAddress> const& addresses) { return Reference<IQuorumChange>(new SpecifiedQuorumChange(addresses)); }

struct NoQuorumChange : IQuorumChange {
	virtual Future<vector<NetworkAddress>> getDesiredCoordinators( Transaction* tr, vector<NetworkAddress> oldCoordinators, Reference<ClusterConnectionFile>, CoordinatorsResult::Type& ) {
		return oldCoordinators;
	}
};
Reference<IQuorumChange> noQuorumChange() { return Reference<IQuorumChange>(new NoQuorumChange); }

struct NameQuorumChange : IQuorumChange {
	std::string newName;
	Reference<IQuorumChange> otherChange;
	explicit NameQuorumChange( std::string const& newName, Reference<IQuorumChange> const& otherChange ) : newName(newName), otherChange(otherChange) {}
	virtual Future<vector<NetworkAddress>> getDesiredCoordinators( Transaction* tr, vector<NetworkAddress> oldCoordinators, Reference<ClusterConnectionFile> cf, CoordinatorsResult::Type& t ) {
		return otherChange->getDesiredCoordinators(tr, oldCoordinators, cf, t);
	}
	virtual std::string getDesiredClusterKeyName() {
		return newName;
	}
};
Reference<IQuorumChange> nameQuorumChange(std::string const& name, Reference<IQuorumChange> const& other) {
	return Reference<IQuorumChange>(new NameQuorumChange( name, other ));
}

struct AutoQuorumChange : IQuorumChange {
	int desired;
	explicit AutoQuorumChange( int desired ) : desired(desired) {}

	virtual Future<vector<NetworkAddress>> getDesiredCoordinators( Transaction* tr, vector<NetworkAddress> oldCoordinators, Reference<ClusterConnectionFile> ccf, CoordinatorsResult::Type& err ) {
		return getDesired( this, tr, oldCoordinators, ccf, &err );
	}

	ACTOR static Future<int> getRedundancy( AutoQuorumChange* self, Transaction* tr ) {
		state Future<Optional<Value>> fStorageReplicas = tr->get( LiteralStringRef("storage_replicas").withPrefix( configKeysPrefix ) );
		state Future<Optional<Value>> fLogReplicas = tr->get( LiteralStringRef("log_replicas").withPrefix( configKeysPrefix ) );
		Void _ = wait( success( fStorageReplicas ) && success( fLogReplicas ) );
		int redundancy = std::min(
			atoi( fStorageReplicas.get().get().toString().c_str() ),
			atoi( fLogReplicas.get().get().toString().c_str() ) );

		return redundancy;
	}

	ACTOR static Future<bool> isAcceptable( AutoQuorumChange* self, Transaction* tr, vector<NetworkAddress> oldCoordinators, Reference<ClusterConnectionFile> ccf, int desiredCount, std::set<AddressExclusion>* excluded ) {
		// Are there enough coordinators for the redundancy level?
		if (oldCoordinators.size() < desiredCount) return false;
		if (oldCoordinators.size() % 2 != 1) return false;

		// Check availability
		ClientCoordinators coord(ccf);
		vector<Future<Optional<LeaderInfo>>> leaderServers;
		for( int i = 0; i < coord.clientLeaderServers.size(); i++ )
			leaderServers.push_back( retryBrokenPromise( coord.clientLeaderServers[i].getLeader, GetLeaderRequest( coord.clusterKey, UID() ), TaskCoordinationReply ) );
		Optional<vector<Optional<LeaderInfo>>> results = wait( timeout( getAll(leaderServers), CLIENT_KNOBS->IS_ACCEPTABLE_DELAY ) );
		if (!results.present()) return false;  // Not all responded
		for(auto& r : results.get())
			if (!r.present())
				return false;   // Coordinator doesn't know about this database?

		// Check exclusions
		for(auto& c : oldCoordinators) {
			if (addressExcluded(*excluded, c)) return false;
		}

		// Check locality
		// FIXME: Actual locality!
		std::sort( oldCoordinators.begin(), oldCoordinators.end() );
		for(int i=1; i<oldCoordinators.size(); i++)
			if (oldCoordinators[i-1].ip == oldCoordinators[i].ip)
				return false;  // Multiple coordinators share an IP

		return true; // The status quo seems fine
	}

	ACTOR static Future<vector<NetworkAddress>> getDesired( AutoQuorumChange* self, Transaction* tr, vector<NetworkAddress> oldCoordinators, Reference<ClusterConnectionFile> ccf, CoordinatorsResult::Type* err ) {
		state int desiredCount = self->desired;

		if(desiredCount == -1) {
			int redundancy = wait( getRedundancy( self, tr ) );
			desiredCount = redundancy*2 - 1;
		}

		std::vector<AddressExclusion> excl = wait( getExcludedServers( tr ) );
		state std::set<AddressExclusion> excluded( excl.begin(), excl.end() );

		vector<ProcessData> _workers = wait(getWorkers(tr));
		state vector<ProcessData> workers = _workers;

		std::map<NetworkAddress, LocalityData> addr_locality;
		for(auto w : workers)
			addr_locality[w.address] = w.locality;

		// since we don't have the locality data for oldCoordinators:
		//     check if every old coordinator is in the workers vector and
		//     check if multiple old coordinators map to the same locality data (same machine)
		bool checkAcceptable = true;
		std::set<Optional<Standalone<StringRef>>> checkDuplicates;
		if (workers.size()){
			for (auto addr : oldCoordinators) {
				auto findResult = addr_locality.find(addr);
				if (findResult == addr_locality.end() || checkDuplicates.count(findResult->second.zoneId())){
					checkAcceptable = false;
					break;
				}
				checkDuplicates.insert(findResult->second.zoneId());
			}
		}

		if (checkAcceptable){
			bool ok = wait(isAcceptable(self, tr, oldCoordinators, ccf, desiredCount, &excluded));
			if (ok) return oldCoordinators;
		}

		std::vector<NetworkAddress> chosen;
		self->addDesiredWorkers(chosen, workers, desiredCount, excluded);

		if (chosen.size() < desiredCount) {
			if (chosen.size() < oldCoordinators.size()) {
				TraceEvent("NotEnoughMachinesForCoordinators").detail("EligibleWorkers", workers.size()).detail("DesiredCoordinators", desiredCount).detail("CurrentCoordinators", oldCoordinators.size());
				*err = CoordinatorsResult::NOT_ENOUGH_MACHINES;
				return vector<NetworkAddress>();
			}
			desiredCount = std::max(oldCoordinators.size(), (workers.size() - 1) | 1);
			chosen.resize(desiredCount);
		}

		return chosen;
	}

	void addDesiredWorkers(vector<NetworkAddress>& chosen, const vector<ProcessData>& workers, int desiredCount, const std::set<AddressExclusion>& excluded) {
		vector<ProcessData> remainingWorkers(workers);
		g_random->randomShuffle(remainingWorkers);

		std::map<StringRef, int> maxCounts;
		std::map<StringRef, std::map<StringRef, int>> currentCounts;
		std::map<StringRef, int> hardLimits;

		vector<StringRef> fields({
			LiteralStringRef("dcid"),
			LiteralStringRef("data_hall"),
			LiteralStringRef("zoneid"),
			LiteralStringRef("machineid")
		});

		for(auto field = fields.begin(); field != fields.end(); field++) {
			if(field->toString() == "machineid") {
				hardLimits[*field] = 1;
			}
			else {
				hardLimits[*field] = desiredCount;
			}
		}

		while(chosen.size() < desiredCount) {
			bool found = false;
			for (auto worker = remainingWorkers.begin(); worker != remainingWorkers.end(); worker++) {
				if(addressExcluded(excluded, worker->address)) {
					continue;
				}
				bool valid = true;
				for(auto field = fields.begin(); field != fields.end(); field++) {
					if(maxCounts[*field] == 0) {
						maxCounts[*field] = 1;
					}
					auto value = worker->locality.get(*field).orDefault(LiteralStringRef(""));
					auto currentCount = currentCounts[*field][value];
					if(currentCount >= maxCounts[*field]) {
						valid = false;
						break;
					}
				}
				if(valid) {
					for(auto field = fields.begin(); field != fields.end(); field++) {
						auto value = worker->locality.get(*field).orDefault(LiteralStringRef(""));
						currentCounts[*field][value] += 1;
					}
					chosen.push_back(worker->address);
					remainingWorkers.erase(worker);
					found = true;
					break;
				}
			}
			if(!found) {
				bool canIncrement = false;
				for(auto field = fields.begin(); field != fields.end(); field++) {
					if(maxCounts[*field] < hardLimits[*field]) {
						maxCounts[*field] += 1;
						canIncrement = true;
						break;
					}
				}
				if(!canIncrement) {
					break;
				}
			}
		}
	}
};
Reference<IQuorumChange> autoQuorumChange( int desired ) { return Reference<IQuorumChange>(new AutoQuorumChange(desired)); }

ACTOR Future<Void> excludeServers( Database cx, vector<AddressExclusion> servers ) {
	state Transaction tr(cx);
	state std::string versionKey = g_random->randomUniqueID().toString();
	loop {
		try {
			tr.setOption( FDBTransactionOptions::ACCESS_SYSTEM_KEYS );
			tr.setOption( FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE );
			tr.setOption( FDBTransactionOptions::LOCK_AWARE );

			tr.addReadConflictRange( singleKeyRange(excludedServersVersionKey) ); //To conflict with parallel includeServers
			tr.set( excludedServersVersionKey, versionKey );
			for(auto& s : servers)
				tr.set( encodeExcludedServersKey(s), StringRef() );

			TraceEvent("ExcludeServersCommit").detail("Servers", describe(servers));

			Void _ = wait( tr.commit() );
			return Void();
		} catch (Error& e) {
			Void _ = wait( tr.onError(e) );
		}
	}
}

ACTOR Future<Void> includeServers( Database cx, vector<AddressExclusion> servers ) {
	state bool includeAll = false;
	state Transaction tr(cx);
	state std::string versionKey = g_random->randomUniqueID().toString();
	loop {
		try {
			tr.setOption( FDBTransactionOptions::ACCESS_SYSTEM_KEYS );
			tr.setOption( FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE );
			tr.setOption( FDBTransactionOptions::LOCK_AWARE );

			// includeServers might be used in an emergency transaction, so make sure it is retry-self-conflicting and CAUSAL_WRITE_RISKY
			tr.setOption( FDBTransactionOptions::CAUSAL_WRITE_RISKY );
			tr.addReadConflictRange( singleKeyRange(excludedServersVersionKey) );

			tr.set( excludedServersVersionKey, versionKey );
			for(auto& s : servers ) {
				if (!s.isValid()) {
					tr.clear( excludedServersKeys );
					includeAll = true;
				} else if (s.isWholeMachine()) {
					// Eliminate both any ip-level exclusion (1.2.3.4) and any port-level exclusions (1.2.3.4:5)
					tr.clear( KeyRangeRef( encodeExcludedServersKey(s), encodeExcludedServersKey(s) + char(':'+1) ) );
				} else {
					tr.clear( encodeExcludedServersKey(s) );
				}
			}

			TraceEvent("IncludeServersCommit").detail("Servers", describe(servers));

			Void _ = wait( tr.commit() );
			return Void();
		} catch (Error& e) {
			TraceEvent("IncludeServersError").error(e, true);
			Void _ = wait( tr.onError(e) );
		}
	}
}

ACTOR Future<Void> setClass( Database cx, AddressExclusion server, ProcessClass processClass ) {
	state Transaction tr(cx);

	loop {
		try {
			tr.setOption( FDBTransactionOptions::ACCESS_SYSTEM_KEYS );
			tr.setOption( FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE );
			tr.setOption( FDBTransactionOptions::LOCK_AWARE );

			vector<ProcessData> workers = wait( getWorkers(&tr) );

			bool foundChange = false;
			for(int i = 0; i < workers.size(); i++) {
				if( server.excludes(workers[i].address) ) {
					if(processClass.classType() != ProcessClass::InvalidClass)
						tr.set(processClassKeyFor(workers[i].locality.processId().get()), processClassValue(processClass));
					else
						tr.clear(processClassKeyFor(workers[i].locality.processId().get()));
					foundChange = true;
				}
			}

			if(foundChange)
				tr.set(processClassChangeKey, g_random->randomUniqueID().toString());

			Void _ = wait( tr.commit() );
			return Void();
		} catch (Error& e) {
			Void _ = wait( tr.onError(e) );
		}
	}
}

ACTOR static Future<vector<AddressExclusion>> getExcludedServers( Transaction* tr ) {
	Standalone<RangeResultRef> r = wait( tr->getRange( excludedServersKeys, CLIENT_KNOBS->TOO_MANY ) );
	ASSERT( !r.more && r.size() < CLIENT_KNOBS->TOO_MANY );

	vector<AddressExclusion> exclusions;
	for(auto i = r.begin(); i != r.end(); ++i) {
		auto a = decodeExcludedServersKey( i->key );
		if (a.isValid())
			exclusions.push_back( a );
	}
	return exclusions;
}

ACTOR Future<vector<AddressExclusion>> getExcludedServers( Database cx ) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption( FDBTransactionOptions::READ_SYSTEM_KEYS );
			tr.setOption( FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE );  // necessary?
			tr.setOption( FDBTransactionOptions::LOCK_AWARE );
			vector<AddressExclusion> exclusions = wait( getExcludedServers(&tr) );
			return exclusions;
		} catch (Error& e) {
			Void _ = wait( tr.onError(e) );
		}
	}
}

ACTOR Future<int> setDDMode( Database cx, int mode ) {
	state Transaction tr(cx);
	state int oldMode = -1;
	state BinaryWriter wr(Unversioned());
	wr << mode;

	loop {
		try {
			Optional<Value> old = wait( tr.get( dataDistributionModeKey ) );
			if (oldMode < 0) {
				oldMode = 1;
				if (old.present()) {
					BinaryReader rd(old.get(), Unversioned());
					rd >> oldMode;
				}
			}
			if (!mode) {
				BinaryWriter wrMyOwner(Unversioned());
				wrMyOwner << dataDistributionModeLock;
				tr.set( moveKeysLockOwnerKey, wrMyOwner.toStringRef() );
			}

			tr.set( dataDistributionModeKey, wr.toStringRef() );

			Void _ = wait( tr.commit() );
			return oldMode;
		} catch (Error& e) {
			TraceEvent("setDDModeRetrying").error(e);
			Void _ = wait (tr.onError(e));
		}
	}
}

ACTOR Future<Void> waitForExcludedServers( Database cx, vector<AddressExclusion> excl ) {
	state std::set<AddressExclusion> exclusions( excl.begin(), excl.end() );

	if (!excl.size()) return Void();

	loop {
		state Transaction tr(cx);
		try {
			tr.setOption( FDBTransactionOptions::READ_SYSTEM_KEYS );
			tr.setOption( FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE );  // necessary?
			tr.setOption( FDBTransactionOptions::LOCK_AWARE );

			// Just getting a consistent read version proves that a set of tlogs satisfying the exclusions has completed recovery

			// Check that there aren't any storage servers with addresses violating the exclusions
			Standalone<RangeResultRef> serverList = wait( tr.getRange( serverListKeys, CLIENT_KNOBS->TOO_MANY ) );
			ASSERT( !serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY );

			state bool ok = true;
			for(auto& s : serverList) {
				auto addr = decodeServerListValue( s.value ).address();
				if ( addressExcluded(exclusions, addr) ) {
					ok = false;
					break;
				}
			}

			if (ok) {
				Optional<Standalone<StringRef>> value = wait( tr.get(logsKey) );
				ASSERT(value.present());
				auto logs = decodeLogsValue(value.get());
				for( auto const& log : logs.first ) {
					if (log.second == NetworkAddress() || addressExcluded(exclusions, log.second)) {
						ok = false;
						break;
					}
				}
				for( auto const& log : logs.second ) {
					if (log.second == NetworkAddress() || addressExcluded(exclusions, log.second)) {
						ok = false;
						break;
					}
				}
			}

			if (ok) return Void();

			Void _ = wait( delayJittered( 1.0 ) );  // SOMEDAY: watches!
		} catch (Error& e) {
			Void _ = wait( tr.onError(e) );
		}
	}
}

ACTOR Future<Void> timeKeeperSetDisable(Database cx) {
	loop {
		state Transaction tr(cx);
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.set(timeKeeperDisableKey, StringRef());
			Void _ = wait(tr.commit());
			return Void();
		} catch (Error &e) {
			Void _ = wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> lockDatabase( Transaction* tr, UID id ) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	Optional<Value> val = wait( tr->get(databaseLockedKey) );

	if(val.present()) {
		if(BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()) == id) {
			return Void();
		} else {
			//TraceEvent("DBA_lock_locked").detail("expecting", id).detail("lock", BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()));
			throw database_locked();
		}
	}

	tr->atomicOp(databaseLockedKey, BinaryWriter::toValue(id, Unversioned()).withPrefix(LiteralStringRef("0123456789")).withSuffix(LiteralStringRef("\x00\x00\x00\x00")), MutationRef::SetVersionstampedValue);
	tr->addWriteConflictRange(normalKeys);
	return Void();
}

ACTOR Future<Void> lockDatabase( Reference<ReadYourWritesTransaction> tr, UID id ) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	Optional<Value> val = wait( tr->get(databaseLockedKey) );

	if(val.present()) {
		if(BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()) == id) {
			return Void();
		} else {
			//TraceEvent("DBA_lock_locked").detail("expecting", id).detail("lock", BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()));
			throw database_locked();
		}
	}

	tr->atomicOp(databaseLockedKey, BinaryWriter::toValue(id, Unversioned()).withPrefix(LiteralStringRef("0123456789")).withSuffix(LiteralStringRef("\x00\x00\x00\x00")), MutationRef::SetVersionstampedValue);
	tr->addWriteConflictRange(normalKeys);
	return Void();
}

ACTOR Future<Void> lockDatabase( Database cx, UID id ) {
	state Transaction tr(cx);
	loop {
		try {
			Void _ = wait( lockDatabase(&tr, id) );
			Void _ = wait( tr.commit() );
			return Void();
		} catch( Error &e ) {
			if(e.code() == error_code_database_locked)
				throw e;
			Void _ = wait( tr.onError(e) );
		}
	}
}

ACTOR Future<Void> unlockDatabase( Transaction* tr, UID id ) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	Optional<Value> val = wait( tr->get(databaseLockedKey) );

	if(!val.present())
		return Void();

	if(val.present() && BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()) != id) {
		//TraceEvent("DBA_unlock_locked").detail("expecting", id).detail("lock", BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()));
		throw database_locked();
	}

	tr->clear(singleKeyRange(databaseLockedKey));
	return Void();
}

ACTOR Future<Void> unlockDatabase( Reference<ReadYourWritesTransaction> tr, UID id ) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	Optional<Value> val = wait( tr->get(databaseLockedKey) );

	if(!val.present())
		return Void();

	if(val.present() && BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()) != id) {
		//TraceEvent("DBA_unlock_locked").detail("expecting", id).detail("lock", BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()));
		throw database_locked();
	}

	tr->clear(singleKeyRange(databaseLockedKey));
	return Void();
}

ACTOR Future<Void> unlockDatabase( Database cx, UID id ) {
	state Transaction tr(cx);
	loop {
		try {
			Void _ = wait( unlockDatabase(&tr, id) );
			Void _ = wait( tr.commit() );
			return Void();
		} catch( Error &e ) {
			if(e.code() == error_code_database_locked)
				throw e;
			Void _ = wait( tr.onError(e) );
		}
	}
}

ACTOR Future<Void> checkDatabaseLock( Transaction* tr, UID id ) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	Optional<Value> val = wait( tr->get(databaseLockedKey) );

	if (val.present() && BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()) != id) {
		//TraceEvent("DBA_check_locked").detail("expecting", id).detail("lock", BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned())).backtrace();
		throw database_locked();
	}

	return Void();
}

ACTOR Future<Void> checkDatabaseLock( Reference<ReadYourWritesTransaction> tr, UID id ) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	Optional<Value> val = wait( tr->get(databaseLockedKey) );

	if (val.present() && BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()) != id) {
		//TraceEvent("DBA_check_locked").detail("expecting", id).detail("lock", BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned())).backtrace();
		throw database_locked();
	}

	return Void();
}

TEST_CASE("ManagementAPI/AutoQuorumChange/checkLocality") {
	Void _ = wait(Future<Void>(Void()));

	std::vector<ProcessData> workers;
	std::vector<NetworkAddress> chosen;
	std::set<AddressExclusion> excluded;
	AutoQuorumChange change(5);

	for(int i = 0; i < 10; i++) {
		ProcessData data;
		auto dataCenter = std::to_string(i / 4 % 2);
		auto dataHall = dataCenter + std::to_string(i / 2 % 2);
		auto rack = dataHall + std::to_string(i % 2);
		auto machineId = rack + std::to_string(i);
		data.locality.set(LiteralStringRef("dcid"), StringRef(dataCenter));
		data.locality.set(LiteralStringRef("data_hall"), StringRef(dataHall));
		data.locality.set(LiteralStringRef("rack"), StringRef(rack));
		data.locality.set(LiteralStringRef("zoneid"), StringRef(rack));
		data.locality.set(LiteralStringRef("machineid"), StringRef(machineId));
		data.address.ip = i;

		workers.push_back(data);
	}

	change.addDesiredWorkers(chosen, workers, 5, excluded);
	std::map<StringRef, std::set<StringRef>> chosenValues;

	ASSERT(chosen.size() == 5);
	std::vector<StringRef> fields({
		LiteralStringRef("dcid"),
		LiteralStringRef("data_hall"),
		LiteralStringRef("zoneid"),
		LiteralStringRef("machineid")
	});
	for(auto worker = chosen.begin(); worker != chosen.end(); worker++) {
		ASSERT(worker->ip < workers.size());
		LocalityData data = workers[worker->ip].locality;
		for(auto field = fields.begin(); field != fields.end(); field++) {
			chosenValues[*field].insert(data.get(*field).get());
		}
	}

	ASSERT(chosenValues[LiteralStringRef("dcid")].size() == 2);
	ASSERT(chosenValues[LiteralStringRef("data_hall")].size() == 4);
	ASSERT(chosenValues[LiteralStringRef("zoneid")].size() == 5);
	ASSERT(chosenValues[LiteralStringRef("machineid")].size() == 5);

	return Void();
}
