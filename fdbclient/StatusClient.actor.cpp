/*
 * StatusClient.actor.cpp
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

#include "flow/flow.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/MonitorLeader.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/StatusClient.h"
#include "fdbclient/Status.h"
#include "fdbclient/json_spirit/json_spirit_writer_template.h"
#include "fdbclient/json_spirit/json_spirit_reader_template.h"
#include "fdbrpc/genericactors.actor.h"
#include <cstdint>

#include "flow/actorcompiler.h" // has to be last include

json_spirit::mValue readJSONStrictly(const std::string& s) {
	json_spirit::mValue val;
	std::string::const_iterator i = s.begin();
	if (!json_spirit::read_range(i, s.end(), val)) {
		if (g_network->isSimulated()) {
			printf("MALFORMED: %s\n", s.c_str());
		}
		throw json_malformed();
	}

	// Allow trailing whitespace
	while (i != s.end()) {
		if (!isspace(*i)) {
			if (g_network->isSimulated()) {
				printf(
				    "EXPECTED EOF: %s\n^^^\n%s\n", std::string(s.begin(), i).c_str(), std::string(i, s.end()).c_str());
			}
			throw json_eof_expected();
		}
		++i;
	}

	return val;
}

uint64_t JSONDoc::expires_reference_version = std::numeric_limits<uint64_t>::max();

// Template specializations for mergeOperator
template <>
json_spirit::mObject JSONDoc::mergeOperator<bool>(const std::string& op,
                                                  const json_spirit::mObject& op_a,
                                                  const json_spirit::mObject& op_b,
                                                  bool const& a,
                                                  bool const& b) {
	if (op == "$and")
		return { { op, a && b } };
	if (op == "$or")
		return { { op, a || b } };
	throw std::exception();
}

template <>
json_spirit::mObject JSONDoc::mergeOperator<json_spirit::mArray>(const std::string& op,
                                                                 const json_spirit::mObject& op_a,
                                                                 const json_spirit::mObject& op_b,
                                                                 json_spirit::mArray const& a,
                                                                 json_spirit::mArray const& b) {
	throw std::exception();
}

template <>
json_spirit::mObject JSONDoc::mergeOperator<json_spirit::mObject>(const std::string& op,
                                                                  const json_spirit::mObject& op_a,
                                                                  const json_spirit::mObject& op_b,
                                                                  json_spirit::mObject const& a,
                                                                  json_spirit::mObject const& b) {
	if (op == "$count_keys") {
		json_spirit::mObject combined;
		for (auto& e : a)
			combined[e.first] = json_spirit::mValue();
		for (auto& e : b)
			combined[e.first] = json_spirit::mValue();
		return { { op, combined } };
	}
	throw std::exception();
}

// If the types for a and B differ then pass them as mValues to this specialization.
template <>
json_spirit::mObject JSONDoc::mergeOperator<json_spirit::mValue>(const std::string& op,
                                                                 const json_spirit::mObject& op_a,
                                                                 const json_spirit::mObject& op_b,
                                                                 json_spirit::mValue const& a,
                                                                 json_spirit::mValue const& b) {
	// Returns { $latest : <a or b>, timestamp: <a or b timestamp> }
	// where the thing (a or b) with the highest timestamp operator arg will be chosen
	if (op == "$latest") {
		double ts_a = 0, ts_b = 0;
		JSONDoc(op_a).tryGet("timestamp", ts_a);
		JSONDoc(op_b).tryGet("timestamp", ts_b);
		if (ts_a > ts_b)
			return { { op, a }, { "timestamp", ts_a } };
		return { { op, b }, { "timestamp", ts_b } };
	}

	// Simply selects the last thing to be merged.
	// Returns { $last : b }
	if (op == "$last")
		return { { op, b } };

	// $expires will reduce its value to null if the "version" operator argument is present, nonzero, and has a value
	// that is less than JSONDoc::expires_reference_version.  This DOES mean that if the "version" argument
	// is not present or has a value of 0 then the operator's value will be considered NOT expired.
	// When two $expires operations are merged, the result is
	// { $expires : <value> } where value is the result of a merger between null and any unexpired
	// values for a or b.
	if (op == "$expires") {
		uint64_t ver_a = 0, ver_b = 0;
		// Whichever has the most recent "timestamp" in its operator object will be used
		JSONDoc(op_a).tryGet("version", ver_a);
		JSONDoc(op_b).tryGet("version", ver_b);

		json_spirit::mValue r;
		// If version is 0 or greater than the current reference version then use the value
		if (ver_a == 0 || ver_a > JSONDoc::expires_reference_version)
			r = a;
		if (ver_b == 0 || ver_b > JSONDoc::expires_reference_version)
			mergeValueInto(r, b);

		return { { op, r } };
	}

	throw std::exception();
}

void JSONDoc::cleanOps(json_spirit::mObject& obj) {
	auto kv = obj.begin();
	while (kv != obj.end()) {
		if (kv->second.type() == json_spirit::obj_type) {
			json_spirit::mObject& o = kv->second.get_obj();
			std::string op = getOperator(o);
			// If an operator was found, replace object with its value.
			if (!op.empty()) {
				// The "count_keys" operator needs special handling
				if (op == "$count_keys") {
					int count = 1;
					if (o.at(op).type() == json_spirit::obj_type)
						count = o.at(op).get_obj().size();
					kv->second = count;
				} else if (op == "$expires") {
					uint64_t version = 0;
					JSONDoc(o).tryGet("version", version);
					if (version == 0 || version > JSONDoc::expires_reference_version)
						kv->second = o.at(op);
					else {
						// Thing is expired so competely remove its key from the enclosing Object
						auto tmp = kv;
						++kv;
						obj.erase(tmp);
					}
				} else // For others just move the value to replace the operator object
					kv->second = o.at(op);
				// Don't advance kv because the new value could also be an operator
				continue;
			} else {
				// It's not an operator, just a regular object so clean it too.
				cleanOps(o);
			}
		}
		++kv;
	}
}

void JSONDoc::mergeInto(json_spirit::mObject& dst, const json_spirit::mObject& src) {
	for (auto& i : src) {
		// printf("Merging key: %s\n", i.first.c_str());
		mergeValueInto(dst[i.first], i.second);
	}
}

void JSONDoc::mergeValueInto(json_spirit::mValue& dst, const json_spirit::mValue& src) {
	if (src.is_null())
		return;

	if (dst.is_null()) {
		dst = src;
		return;
	}

	// Do nothing if d is already an error
	if (dst.type() == json_spirit::obj_type && dst.get_obj().count("ERROR"))
		return;

	if (dst.type() != src.type()) {
		dst = json_spirit::mObject({ { "ERROR", "Incompatible types." }, { "a", dst }, { "b", src } });
		return;
	}

	switch (dst.type()) {
	case json_spirit::obj_type: {
		// Refs to the objects, for convenience.
		json_spirit::mObject& aObj = dst.get_obj();
		const json_spirit::mObject& bObj = src.get_obj();

		const std::string& op = getOperator(aObj);
		const std::string& opB = getOperator(bObj);

		// Operators must be the same, which could mean both are empty (if these objects are not operators)
		if (op != opB) {
			dst = json_spirit::mObject({ { "ERROR", "Operators do not match" }, { "a", dst }, { "b", src } });
			break;
		}

		// If objects are not operators then defer to mergeInto
		if (op.empty()) {
			mergeInto(dst.get_obj(), src.get_obj());
			break;
		}

		// Get the operator values
		json_spirit::mValue& a = aObj.at(op);
		const json_spirit::mValue& b = bObj.at(op);

		// First try the operators that are type-agnostic
		try {
			dst = mergeOperator<json_spirit::mValue>(op, aObj, bObj, a, b);
			return;
		} catch (std::exception&) {
		}

		// Now try type and type pair specific operators
		// First, if types are incompatible try to make them compatible or return an error
		if (a.type() != b.type()) {
			// It's actually okay if the type mismatch is double vs int since once can be converted to the other.
			if ((a.type() == json_spirit::int_type && b.type() == json_spirit::real_type) ||
			    (b.type() == json_spirit::int_type && a.type() == json_spirit::real_type)) {
				// Convert d's op value (which a is a reference to) to a double so that the
				// switch block below will do the operation with doubles.
				a = a.get_real();
			} else {
				// Otherwise, output an error as the types do not match
				dst = json_spirit::mObject(
				    { { "ERROR", "Incompatible operator value types" }, { "a", dst }, { "b", src } });
				return;
			}
		}

		// Now try the type-specific operators.
		try {
			switch (a.type()) {
			case json_spirit::bool_type:
				dst = mergeOperatorWrapper<bool>(op, aObj, bObj, a, b);
				break;
			case json_spirit::int_type:
				dst = mergeOperatorWrapper<int64_t>(op, aObj, bObj, a, b);
				break;
			case json_spirit::real_type:
				dst = mergeOperatorWrapper<double>(op, aObj, bObj, a, b);
				break;
			case json_spirit::str_type:
				dst = mergeOperatorWrapper<std::string>(op, aObj, bObj, a, b);
				break;
			case json_spirit::array_type:
				dst = mergeOperatorWrapper<json_spirit::mArray>(op, aObj, bObj, a, b);
				break;
			case json_spirit::obj_type:
				dst = mergeOperatorWrapper<json_spirit::mObject>(op, aObj, bObj, a, b);
				break;
			case json_spirit::null_type:
				break;
			}
		} catch (...) {
			dst = json_spirit::mObject({ { "ERROR", "Unsupported operator / value type combination." },
			                             { "operator", op },
			                             { "type", a.type() } });
		}
		break;
	}

	case json_spirit::array_type:
		for (auto& ai : src.get_array())
			dst.get_array().push_back(ai);
		break;

	default:
		if (!(dst == src))
			dst = json_spirit::mObject({ { "ERROR", "Values do not match." }, { "a", dst }, { "b", src } });
	}
}

// Check if a quorum of coordination servers is reachable
// Will not throw, will just return non-present Optional if error
ACTOR Future<Optional<StatusObject>> clientCoordinatorsStatusFetcher(Reference<IClusterConnectionRecord> connRecord,
                                                                     bool* quorum_reachable,
                                                                     int* coordinatorsFaultTolerance) {
	try {
		wait(connRecord->resolveHostnames());
		state ClientCoordinators coord(connRecord);
		state StatusObject statusObj;

		state std::vector<Future<Optional<LeaderInfo>>> leaderServers;
		leaderServers.reserve(coord.clientLeaderServers.size());
		for (int i = 0; i < coord.clientLeaderServers.size(); i++)
			leaderServers.push_back(retryBrokenPromise(coord.clientLeaderServers[i].getLeader,
			                                           GetLeaderRequest(coord.clusterKey, UID()),
			                                           TaskPriority::CoordinationReply));

		state std::vector<Future<ProtocolInfoReply>> coordProtocols;
		coordProtocols.reserve(coord.clientLeaderServers.size());
		for (int i = 0; i < coord.clientLeaderServers.size(); i++) {
			RequestStream<ProtocolInfoRequest> requestStream{ Endpoint::wellKnown(
				{ coord.clientLeaderServers[i].getLeader.getEndpoint().addresses }, WLTOKEN_PROTOCOL_INFO) };
			coordProtocols.push_back(retryBrokenPromise(requestStream, ProtocolInfoRequest{}));
		}

		wait(smartQuorum(leaderServers, leaderServers.size() / 2 + 1, 1.5) &&
		         smartQuorum(coordProtocols, coordProtocols.size() / 2 + 1, 1.5) ||
		     delay(2.0));

		statusObj["quorum_reachable"] = *quorum_reachable =
		    quorum(leaderServers, leaderServers.size() / 2 + 1).isReady();

		StatusArray coordsStatus;
		int coordinatorsUnavailable = 0;
		for (int i = 0; i < leaderServers.size(); i++) {
			StatusObject coordStatus;
			coordStatus["address"] =
			    coord.clientLeaderServers[i].getLeader.getEndpoint().getPrimaryAddress().toString();

			if (leaderServers[i].isReady()) {
				coordStatus["reachable"] = true;
			} else {
				coordinatorsUnavailable++;
				coordStatus["reachable"] = false;
			}
			if (coordProtocols[i].isReady()) {
				uint64_t protocolVersionInt = coordProtocols[i].get().version.version();
				std::stringstream hexSs;
				hexSs << std::hex << std::setw(2 * sizeof(protocolVersionInt)) << std::setfill('0')
				      << protocolVersionInt;
				coordStatus["protocol"] = hexSs.str();
			}
			coordsStatus.push_back(coordStatus);
		}
		statusObj["coordinators"] = coordsStatus;

		*coordinatorsFaultTolerance = (leaderServers.size() - 1) / 2 - coordinatorsUnavailable;
		return statusObj;
	} catch (Error& e) {
		*quorum_reachable = false;
		return Optional<StatusObject>();
	}
}

// Client section of the json output
// Will NOT throw, errors will be put into messages array
ACTOR Future<StatusObject> clientStatusFetcher(Reference<IClusterConnectionRecord> connRecord,
                                               StatusArray* messages,
                                               bool* quorum_reachable,
                                               int* coordinatorsFaultTolerance) {
	state StatusObject statusObj;

	state Optional<StatusObject> coordsStatusObj =
	    wait(clientCoordinatorsStatusFetcher(connRecord, quorum_reachable, coordinatorsFaultTolerance));
	state bool contentsUpToDate = wait(connRecord->upToDate());

	if (coordsStatusObj.present()) {
		statusObj["coordinators"] = coordsStatusObj.get();
		if (!*quorum_reachable)
			messages->push_back(makeMessage("quorum_not_reachable", "Unable to reach a quorum of coordinators."));
	} else
		messages->push_back(makeMessage("status_incomplete_coordinators", "Could not fetch coordinator info."));

	StatusObject statusObjClusterFile;
	statusObjClusterFile["path"] = connRecord->getLocation();
	statusObjClusterFile["up_to_date"] = contentsUpToDate;
	statusObj["cluster_file"] = statusObjClusterFile;

	if (!contentsUpToDate) {
		ClusterConnectionString storedConnectionString = wait(connRecord->getStoredConnectionString());
		std::string description = "Cluster file contents do not match current cluster connection string.";
		description += "\nThe file contains the connection string: ";
		description += storedConnectionString.toString().c_str();
		description += "\nThe current connection string is: ";
		description += connRecord->getConnectionString().toString().c_str();
		description += "\nVerify the cluster file and its parent directory are writable and that the cluster file has "
		               "not been overwritten externally. To change coordinators without manual intervention, the "
		               "cluster file and its containing folder must be writable by all servers and clients. If a "
		               "majority of the coordinators referenced by the old connection string are lost, the database "
		               "will stop working until the correct cluster file is distributed to all processes.";
		messages->push_back(makeMessage("incorrect_cluster_file_contents", description.c_str()));
	}

	return statusObj;
}

// Cluster section of json output
ACTOR Future<Optional<StatusObject>> clusterStatusFetcher(ClusterInterface cI, StatusArray* messages) {
	state StatusRequest req;
	state Future<Void> clusterTimeout = delay(30.0);
	state Optional<StatusObject> oStatusObj;

	wait(delay(0.0)); // make sure the cluster controller is marked as not failed

	state Future<ErrorOr<StatusReply>> statusReply = cI.databaseStatus.tryGetReply(req);
	loop {
		choose {
			when(ErrorOr<StatusReply> result = wait(statusReply)) {
				if (result.isError()) {
					if (result.getError().code() == error_code_request_maybe_delivered)
						messages->push_back(makeMessage("unreachable_cluster_controller",
						                                ("Unable to communicate with the cluster controller at " +
						                                 cI.address().toString() + " to get status.")
						                                    .c_str()));
					else if (result.getError().code() == error_code_server_overloaded)
						messages->push_back(makeMessage("server_overloaded",
						                                "The cluster controller is currently processing too many "
						                                "status requests and is unable to respond"));
					else
						messages->push_back(
						    makeMessage("status_incomplete_error", "Cluster encountered an error fetching status."));
				} else {
					oStatusObj = result.get().statusObj;
				}
				break;
			}
			when(wait(clusterTimeout)) {
				messages->push_back(makeMessage("status_incomplete_timeout", "Timed out fetching cluster status."));
				break;
			}
		}
	}

	return oStatusObj;
}

// Create and return a database_status section.
// Will not throw, will not return an empty section.
StatusObject getClientDatabaseStatus(StatusObjectReader client, StatusObjectReader cluster) {
	bool isAvailable = false;
	bool isHealthy = false;

	try {
		// Lots of the JSON reads in this code could throw, and that's OK, isAvailable and isHealthy will be
		// at the states we want them to be in (currently)
		std::string recoveryStateName = cluster.at("recovery_state.name").get_str();
		isAvailable = client.at("coordinators.quorum_reachable").get_bool() &&
		              (recoveryStateName == "accepting_commits" || recoveryStateName == "all_logs_recruited" ||
		               recoveryStateName == "storage_recovered" || recoveryStateName == "fully_recovered") &&
		              cluster.at("database_available").get_bool();

		if (isAvailable) {
			bool procMessagesPresent = false;
			// OK to throw if processes doesn't exist, can't have an available database without processes
			for (auto p : cluster.at("processes").get_obj()) {
				StatusObjectReader proc(p.second);
				if (proc.has("messages") && proc.last().get_array().size()) {
					procMessagesPresent = true;
					break;
				}
			}

			bool data_state_present = cluster.has("data.state");

			bool data_state_unhealthy =
			    data_state_present && cluster.has("data.state.healthy") && !cluster.last().get_bool();

			int cluster_messages = cluster.has("messages") ? cluster.last().get_array().size() : 0;
			int configuration_messages = client.has("configuration.messages") ? client.last().get_array().size() : 0;

			isHealthy =
			    !(cluster_messages > 0 || configuration_messages > 0 || procMessagesPresent || data_state_unhealthy ||
			      !data_state_present || !client.at("cluster_file.up_to_date").get_bool());
		}
	} catch (std::exception&) {
		// As documented above, exceptions leave isAvailable and isHealthy in the right state
	}

	StatusObject databaseStatus;
	databaseStatus["healthy"] = isHealthy;
	databaseStatus["available"] = isAvailable;
	return databaseStatus;
}

ACTOR Future<StatusObject> statusFetcherImpl(Reference<IClusterConnectionRecord> connRecord,
                                             Reference<AsyncVar<Optional<ClusterInterface>>> clusterInterface) {
	if (!g_network)
		throw network_not_setup();

	state StatusObject statusObj;
	state StatusObject statusObjClient;
	state StatusArray clientMessages;

	// This could be read from the JSON but doing so safely is ugly so using a real var.
	state bool quorum_reachable = false;
	state int coordinatorsFaultTolerance = 0;

	try {
		state int64_t clientTime = g_network->timer();

		StatusObject _statusObjClient =
		    wait(clientStatusFetcher(connRecord, &clientMessages, &quorum_reachable, &coordinatorsFaultTolerance));
		statusObjClient = _statusObjClient;

		if (clientTime != -1)
			statusObjClient["timestamp"] = clientTime;
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		TraceEvent(SevError, "ClientStatusFetchError").error(e);
		clientMessages.push_back(
		    makeMessage("status_incomplete_client", "Could not retrieve client status information."));

		// quorum_reachable will be false because clientStatusFetcher won't throw and it's the only thing would change
		// it.
	}

	state StatusObject statusObjCluster;

	if (quorum_reachable) {
		try {

			state Future<Void> interfaceTimeout = delay(2.0);

			loop {
				if (clusterInterface->get().present()) {
					Optional<StatusObject> _statusObjCluster =
					    wait(clusterStatusFetcher(clusterInterface->get().get(), &clientMessages));
					if (_statusObjCluster.present()) {
						statusObjCluster = _statusObjCluster.get();
						// TODO: this is a temporary fix, getting the number of available coordinators should move to
						// the server side
						if (statusObjCluster.count("fault_tolerance")) {
							StatusObject::Map& faultToleranceWriteable = statusObjCluster["fault_tolerance"].get_obj();
							StatusObjectReader faultToleranceReader(faultToleranceWriteable);
							int maxDataLoss, maxAvailLoss;
							if (faultToleranceReader.get("max_zone_failures_without_losing_data", maxDataLoss) &&
							    faultToleranceReader.get("max_zone_failures_without_losing_availability",
							                             maxAvailLoss)) {
								// max_zone_failures_without_losing_availability <=
								// max_zone_failures_without_losing_data
								faultToleranceWriteable["max_zone_failures_without_losing_data"] =
								    std::min(maxDataLoss, coordinatorsFaultTolerance);
								faultToleranceWriteable["max_zone_failures_without_losing_availability"] =
								    std::min(maxAvailLoss, coordinatorsFaultTolerance);
							}
						}
					}
					// else clusterStatusFetcher added a message
					break;
				}
				choose {
					when(wait(clusterInterface->onChange())) {}
					when(wait(interfaceTimeout)) {
						clientMessages.push_back(makeMessage("no_cluster_controller",
						                                     "Unable to locate a cluster controller within 2 seconds.  "
						                                     "Check that there are server processes running."));
						break;
					}
				}
			}

			statusObj["cluster"] = statusObjCluster;
		} catch (Error& e) {
			TraceEvent(e.code() == error_code_all_alternatives_failed ? SevInfo : SevError, "ClusterStatusFetchError")
			    .error(e);
			// Set client.messages to an array of one message
			clientMessages.push_back(
			    makeMessage("status_incomplete_cluster", "Could not retrieve cluster status information."));
		}
	}

	// Put clientMessages into Client section.
	statusObjClient["messages"] = clientMessages;

	// Create database_status section, place into statusObjClient
	statusObjClient["database_status"] = getClientDatabaseStatus(statusObjClient, statusObjCluster);

	// Put finalized client section into final document.  Cluster section was created above if it was possible.
	statusObj["client"] = statusObjClient;

	// Make sure that if a document is being returned at all it has a cluster.layers._valid path.
	JSONDoc doc(statusObj); // doc will modify statusObj with a convenient interface
	auto& layers_valid = doc.create("cluster.layers._valid");
	if (layers_valid.is_null())
		layers_valid = false;

	return statusObj;
}

ACTOR Future<Void> timeoutMonitorLeader(Database db) {
	state Future<Void> leadMon = monitorLeader<ClusterInterface>(db->getConnectionRecord(), db->statusClusterInterface);
	loop {
		wait(delay(CLIENT_KNOBS->STATUS_IDLE_TIMEOUT + 0.00001 + db->lastStatusFetch - now()));
		if (now() - db->lastStatusFetch > CLIENT_KNOBS->STATUS_IDLE_TIMEOUT) {
			db->statusClusterInterface = Reference<AsyncVar<Optional<ClusterInterface>>>();
			return Void();
		}
	}
}

Future<StatusObject> StatusClient::statusFetcher(Database db) {
	db->lastStatusFetch = now();
	if (!db->statusClusterInterface) {
		db->statusClusterInterface = makeReference<AsyncVar<Optional<ClusterInterface>>>();
		db->statusLeaderMon = timeoutMonitorLeader(db);
	}

	return statusFetcherImpl(db->getConnectionRecord(), db->statusClusterInterface);
}
