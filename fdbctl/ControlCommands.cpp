/*
 * ControlCommands.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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
#ifdef FLOW_GRPC_ENABLED
#include "fdbctl/ControlCommands.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/Schemas.h"
#include "fmt/format.h"
#include <boost/algorithm/string.hpp>

namespace fdbctl {

//-- Coordinators ----

Future<grpc::Status> getCoordinators(Reference<IDatabase> db,
                                     const GetCoordinatorsRequest* req,
                                     GetCoordinatorsReply* rep) {
	Reference<ITransaction> tr = db->createTransaction();

	loop {
		Error err;
		try {
			ThreadFuture<Optional<Value>> processesF = tr->get(special_keys::coordinatorsProcessSpecialKey);
			Optional<Value> processes = co_await safeThreadFutureToFuture(processesF);
			ASSERT(processes.present());

			std::vector<std::string> process_addresses;
			boost::split(process_addresses, processes.get().toString(), [](char c) { return c == ','; });
			for (const auto& p : process_addresses) {
				rep->add_coordinators(p);
			}

			co_return grpc::Status::OK;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}

			err = e;
		}

		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

Future<grpc::Status> changeCoordinators(Reference<IDatabase> db,
                                        const ChangeCoordinatorsRequest* req,
                                        ChangeCoordinatorsReply* rep) {
	int retries = 0;
	int notEnoughMachineResults = 0;
	std::string auto_coordinators_str;

	// TODO: Reject request if intersection(new coordinators, old coordinators) = {}.
	Reference<ITransaction> tr = db->createTransaction();
	Error err;
	loop {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		try {
			if (req->has_cluster_description() && req->cluster_description().size()) {
				tr->set(special_keys::clusterDescriptionSpecialKey, req->cluster_description());
			}

			if (req->automatic_coordinators()) {
				ASSERT(req->new_coordinator_addresses_size() == 0);
				if (!auto_coordinators_str.size()) {
					ThreadFuture<Optional<Value>> auto_coordinatorsF =
					    tr->get(special_keys::coordinatorsAutoSpecialKey);
					Optional<Value> auto_coordinators = co_await safeThreadFutureToFuture(auto_coordinatorsF);
					ASSERT(auto_coordinators.present());
					auto_coordinators_str = auto_coordinators.get().toString();
				}
				tr->set(special_keys::coordinatorsProcessSpecialKey, auto_coordinators_str);
			} else {
				std::set<NetworkAddress> new_coordinators_addresses;
				std::set<Hostname> new_coordinators_hostnames;
				std::vector<std::string> newCoordinatorsList;

				for (const auto& new_coord : req->new_coordinator_addresses()) {
					try {
						if (Hostname::isHostname(new_coord)) {
							const auto& hostname = Hostname::parse(new_coord);
							if (new_coordinators_hostnames.count(hostname)) {
								co_return grpc::Status(
								    grpc::StatusCode::INVALID_ARGUMENT,
								    fmt::format("passed redundant coordinators: {}", hostname.toString()));
							}
							new_coordinators_hostnames.insert(hostname);
							newCoordinatorsList.push_back(hostname.toString());
						} else {
							const auto& addr = NetworkAddress::parse(new_coord);
							if (new_coordinators_addresses.count(addr)) {
								co_return grpc::Status(
								    grpc::StatusCode::INVALID_ARGUMENT,
								    fmt::format("passed redundant coordinators: {}", addr.toString()));
							}
							new_coordinators_addresses.insert(addr);
							newCoordinatorsList.push_back(addr.toString());
						}
					} catch (Error& e) {
						if (e.code() == error_code_connection_string_invalid) {
							co_return grpc::Status(
							    grpc::StatusCode::INVALID_ARGUMENT,
							    fmt::format("'{}' is not a valid network endpoint address", new_coord));
						}

						throw;
					}
				}

				std::string new_coordinators_str = boost::algorithm::join(newCoordinatorsList, ",");
				tr->set(special_keys::coordinatorsProcessSpecialKey, new_coordinators_str);
			}
			co_await safeThreadFutureToFuture(tr->commit());

			// Commit should always fail here. If the commit succeeds, the coordinators change and
			// the commit will fail with commit_unknown_result().
			ASSERT(false);
		} catch (Error& e) {
			if (err.code() == error_code_actor_cancelled) {
				throw err;
			}

			err = Error(e);
		}

		if (err.code() == error_code_special_keys_api_failure) {
			std::string errorMsgStr = co_await utils::getSpecialKeysFailureErrorMessage(tr);
			if (errorMsgStr == ManagementAPI::generateErrorMessage(CoordinatorsResult::NOT_ENOUGH_MACHINES) &&
			    notEnoughMachineResults < 1) {
				// We could get not_enough_machines if we happen to see the database while the
				// cluster controller is updating the worker list, so make sure it happens twice
				// before returning a failure
				notEnoughMachineResults++;
				co_await delay(1.0);
				tr->reset();
				continue;
			} else if (errorMsgStr == ManagementAPI::generateErrorMessage(CoordinatorsResult::SAME_NETWORK_ADDRESSES)) {
				GetCoordinatorsRequest get_req;
				GetCoordinatorsReply get_rep;
				auto ret = co_await getCoordinators(db, &get_req, &get_rep);
				ASSERT(ret.ok());
				for (auto& c : get_rep.coordinators()) {
					rep->add_coordinators(c);
				}

				rep->set_changed(retries > 0);
				co_return grpc::Status::OK;
			} else {
				throw err;
			}
		}

		++retries;
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

//-- Database Configure ----

Reference<ITransaction> getTransaction(Reference<IDatabase> db, Reference<ITransaction>& tr) {
	return tr ? tr : db->createTransaction();
}

//-- Status ----

Future<grpc::Status> getStatus(Reference<IDatabase> db, const GetStatusRequest* req, GetStatusReply* rep) {
	Reference<ITransaction> tr = db->createTransaction();

	loop {
		Error err;
		try {
			ThreadFuture<Optional<Value>> statusValueF = tr->get("\xff\xff/status/json"_sr);
			Optional<Value> statusValue = co_await safeThreadFutureToFuture(statusValueF);

			if (!statusValue.present()) {
				co_return grpc::Status(grpc::StatusCode::INTERNAL, "Failed to get status json from the cluster");
			}

			rep->set_result(statusValue.get().toString());
			co_return grpc::Status::OK;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}

			err = e;
		}

		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

//-- Workers ----

void localityDataToProto(const LocalityData& loc, Worker::Locality* proto) {
	if (loc.isPresent(LocalityData::keyProcessId))
		proto->set_process_id(loc.processId()->toString());

	if (loc.isPresent(LocalityData::keyZoneId))
		proto->set_zone_id(loc.zoneId()->toString());

	if (loc.isPresent(LocalityData::keyMachineId))
		proto->set_machine_id(loc.machineId()->toString());

	if (loc.isPresent(LocalityData::keyDcId))
		proto->set_dc_id(loc.dcId()->toString());

	if (loc.isPresent(LocalityData::keyDataHallId))
		proto->set_data_hall_id(loc.dataHallId()->toString());
}

Future<grpc::Status> getWorkers(Reference<IDatabase> db, const GetWorkersRequest* req, GetWorkersReply* rep) {
	Reference<ITransaction> tr = db->createTransaction();

	int numRetries = 0;
	while (true) {
		Error err;

		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			ThreadFuture<RangeResult> processClasses = tr->getRange(processClassKeys, CLIENT_KNOBS->TOO_MANY);
			ThreadFuture<RangeResult> processData = tr->getRange(workerListKeys, CLIENT_KNOBS->TOO_MANY);

			co_await (success(safeThreadFutureToFuture(processClasses)) &&
			          success(safeThreadFutureToFuture(processData)));

			ASSERT(!processClasses.get().more && processClasses.get().size() < CLIENT_KNOBS->TOO_MANY);
			ASSERT(!processData.get().more && processData.get().size() < CLIENT_KNOBS->TOO_MANY);

			if ((processData.get().empty() || processClasses.get().empty()) && numRetries < 5) {
				co_await delay((1 << numRetries++) * 0.01 * deterministicRandom()->random01());
				tr->reset();
				continue;
			}

			std::map<Optional<Standalone<StringRef>>, ProcessClass> id_class;
			for (int i = 0; i < processClasses.get().size(); i++) {
				try {
					id_class[decodeProcessClassKey(processClasses.get()[i].key)] =
					    decodeProcessClassValue(processClasses.get()[i].value);
				} catch (Error& e) {
					co_return grpc::Status(grpc::StatusCode::ABORTED,
					                       fmt::format("error decoding process class: {}", e.name()));
				}
			}

			for (int i = 0; i < processData.get().size(); i++) {
				ProcessData data = decodeWorkerListValue(processData.get()[i].value);
				ProcessClass processClass = id_class[data.locality.processId()];

				if (processClass.classSource() == ProcessClass::DBSource ||
				    data.processClass.classType() == ProcessClass::UnsetClass)
					data.processClass = processClass;

				if (data.processClass.classType() != ProcessClass::TesterClass) {
					Worker* w = rep->add_workers();
					w->set_address(data.address.toString());
					w->set_process_class(data.processClass.toString());
					if (data.grpcAddress.present())
						w->set_grpc_address(data.grpcAddress->toString());

					auto* lc = w->mutable_locality();
					localityDataToProto(data.locality, lc);
				}
			}

			co_return grpc::Status::OK;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}

			++numRetries;
			err = e;
		}

		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

Future<grpc::Status> include(Reference<IDatabase> db, const IncludeRequest* req, IncludeReply* rep) {
	std::vector<AddressExclusion> addresses;
	std::vector<std::string> localities;

	for (const auto& addr_str : req->addresses()) {
		auto a = AddressExclusion::parse(StringRef(addr_str));
		if (!a.isValid()) {
			co_return grpc::Status(
			    grpc::StatusCode::INVALID_ARGUMENT,
			    fmt::format("'{}' is neither a valid network endpoint address nor a locality", addr_str));
		}
		addresses.push_back(a);
	}

	Reference<ITransaction> tr = db->createTransaction();
	loop {
		Error err;
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		try {
			if (req->all()) {
				// Include all - clear the entire exclusion ranges
				if (req->failed()) {
					tr->clear(special_keys::failedServersSpecialKeyRange);
					tr->clear(special_keys::failedLocalitySpecialKeyRange);
				} else {
					tr->clear(special_keys::excludedServersSpecialKeyRange);
					tr->clear(special_keys::excludedLocalitySpecialKeyRange);
				}
			} else {
				// Include specific addresses
				for (const auto& s : addresses) {
					Key addr = req->failed()
					               ? special_keys::failedServersSpecialKeyRange.begin.withSuffix(s.toString())
					               : special_keys::excludedServersSpecialKeyRange.begin.withSuffix(s.toString());
					tr->clear(addr);
					// Clear both IP-level and port-level exclusions
					if (s.isWholeMachine()) {
						tr->clear(KeyRangeRef(addr.withSuffix(":"_sr), addr.withSuffix(";"_sr)));
					}
				}

				// Include specific localities
				for (const auto& l : localities) {
					Key locality = req->failed() ? special_keys::failedLocalitySpecialKeyRange.begin.withSuffix(l)
					                             : special_keys::excludedLocalitySpecialKeyRange.begin.withSuffix(l);
					tr->clear(locality);
				}
			}

			co_await safeThreadFutureToFuture(tr->commit());
			co_return grpc::Status::OK;
		} catch (Error& e) {
			if (err.code() == error_code_actor_cancelled) {
				throw err;
			}

			err = e;
		}

		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

void addInterfacesFromKVs(RangeResult& kvs,
                          std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface) {
	for (const auto& kv : kvs) {
		ClientWorkerInterface workerInterf =
		    BinaryReader::fromStringRef<ClientWorkerInterface>(kv.value, IncludeVersion());
		ClientLeaderRegInterface leaderInterf(workerInterf.address());
		StringRef ip_port = (kv.key.endsWith(":tls"_sr) ? kv.key.removeSuffix(":tls"_sr) : kv.key)
		                        .removePrefix("\xff\xff/worker_interfaces/"_sr);
		(*address_interface)[ip_port] = std::make_pair(kv.value, leaderInterf);

		if (workerInterf.reboot.getEndpoint().addresses.secondaryAddress.present()) {
			Key full_ip_port2 =
			    StringRef(workerInterf.reboot.getEndpoint().addresses.secondaryAddress.get().toString());
			StringRef ip_port2 =
			    full_ip_port2.endsWith(":tls"_sr) ? full_ip_port2.removeSuffix(":tls"_sr) : full_ip_port2;
			(*address_interface)[ip_port2] = std::make_pair(kv.value, leaderInterf);
		}
	}
}

Future<Void> getWorkerInterfaces(Reference<ITransaction> tr,
                                 std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface,
                                 bool verify) {
	if (verify) {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		tr->set(special_keys::workerInterfacesVerifyOptionSpecialKey, ValueRef());
	}

	ThreadFuture<RangeResult> kvsFuture = tr->getRange(
	    KeyRangeRef("\xff\xff/worker_interfaces/"_sr, "\xff\xff/worker_interfaces0"_sr), CLIENT_KNOBS->TOO_MANY);
	RangeResult kvs = co_await safeThreadFutureToFuture(kvsFuture);
	ASSERT(!kvs.more);

	if (verify) {
		// remove the option if set
		tr->clear(special_keys::workerInterfacesVerifyOptionSpecialKey);
	}

	addInterfacesFromKVs(kvs, address_interface);
	co_return;
}

Future<grpc::Status> kill(Reference<IDatabase> db, const KillRequest* req, KillReply* rep) {
	// TODO: Handle duration_seconds
	// TODO: What if asked to kill itself?
	if (req->all() && req->addresses().size() > 0) {
		co_return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "ERROR: both fields `all` and `addresses` set");
	}

	Reference<ITransaction> tr = getTransaction(db, tr);
	std::map<Key, std::pair<Value, ClientLeaderRegInterface>> address_interface;
	co_await getWorkerInterfaces(tr, &address_interface, true);

	if (address_interface.size() == 0) {
		// TODO: Return more elaborate result.
		co_return grpc::Status::OK;
	}

	std::vector<std::string> addressesVec;
	if (req->all()) {
		for (const auto& [address, _] : address_interface) {
			addressesVec.push_back(address.toString());
		}
	} else if (req->addresses().size() > 0) {
		for (const auto& addr : req->addresses()) {
			if (!address_interface.count(StringRef(addr))) {
				co_return grpc::Status(grpc::StatusCode::UNKNOWN, fmt::format("process `{}' not recognized", addr));
			}
			addressesVec.push_back(addr);
		}
	} else {
		co_return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid request");
	}

	auto addressesStr = boost::algorithm::join(addressesVec, ",");
	int64_t killRequestsSent = co_await safeThreadFutureToFuture(db->rebootWorker(addressesStr, false, 0));
	if (!killRequestsSent) {
		co_return grpc::Status(grpc::StatusCode::UNKNOWN,
		                       "failed to kill all processes, please call `kill’ command again to fetch");
	}

	co_return grpc::Status::OK;
}

namespace utils {
inline const KeyRef errorMsgSpecialKey = "\xff\xff/error_message"_sr;
Future<std::string> getSpecialKeysFailureErrorMessage(Reference<ITransaction> tr) {
	ThreadFuture<Optional<Value>> errorMsgF = tr->get(errorMsgSpecialKey);
	Optional<Value> errorMsg = co_await safeThreadFutureToFuture(errorMsgF);
	ASSERT(errorMsg.present());

	auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
	auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();

	std::string errorStr;
	ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
	co_return valueObj["message"].get_str();
}

Future<Void> getStorageServerInterfaces(Reference<IDatabase> db,
                                        std::map<std::string, StorageServerInterface>* interfaces) {
	Reference<ITransaction> tr = db->createTransaction();
	loop {
		Error err;
		interfaces->clear();
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			ThreadFuture<RangeResult> serverListF = tr->getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY);
			co_await success(safeThreadFutureToFuture(serverListF));
			ASSERT(!serverListF.get().more);
			ASSERT_LT(serverListF.get().size(), CLIENT_KNOBS->TOO_MANY);
			RangeResult serverList = serverListF.get();
			// decode server interfaces
			for (int i = 0; i < serverList.size(); i++) {
				auto ssi = decodeServerListValue(serverList[i].value);
				(*interfaces)[ssi.address().toString()] = ssi;
			}
			co_return;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}

			err = e;
			TraceEvent(SevWarn, "GetStorageServerInterfacesError").error(e);
		}

		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

Future<bool> getWorkersProcessData(Reference<IDatabase> db, std::vector<ProcessData>* workers) {
	Reference<ITransaction> tr = db->createTransaction();
	loop {
		Error err;
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			ThreadFuture<RangeResult> processClasses = tr->getRange(processClassKeys, CLIENT_KNOBS->TOO_MANY);
			ThreadFuture<RangeResult> processData = tr->getRange(workerListKeys, CLIENT_KNOBS->TOO_MANY);

			co_await success(safeThreadFutureToFuture(processClasses));
			co_await success(safeThreadFutureToFuture(processData));

			ASSERT(!processClasses.get().more && processClasses.get().size() < CLIENT_KNOBS->TOO_MANY);
			ASSERT(!processData.get().more && processData.get().size() < CLIENT_KNOBS->TOO_MANY);

			std::map<Optional<Standalone<StringRef>>, ProcessClass> id_class;
			for (int i = 0; i < processClasses.get().size(); i++) {
				try {
					id_class[decodeProcessClassKey(processClasses.get()[i].key)] =
					    decodeProcessClassValue(processClasses.get()[i].value);
				} catch (Error& e) {
					co_return false;
				}
			}

			for (int i = 0; i < processData.get().size(); i++) {
				ProcessData data = decodeWorkerListValue(processData.get()[i].value);
				ProcessClass processClass = id_class[data.locality.processId()];

				if (processClass.classSource() == ProcessClass::DBSource ||
				    data.processClass.classType() == ProcessClass::UnsetClass)
					data.processClass = processClass;

				if (data.processClass.classType() != ProcessClass::TesterClass)
					workers->push_back(data);
			}

			co_return true;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}

			err = e;
		}

		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

} // namespace utils
} // namespace fdbctl
#endif // FLOW_GRPC_ENABLED
